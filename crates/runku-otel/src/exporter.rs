use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use runku_core::EnvironmentScope;
use runku_observability::{LogCursor, LogQuery, LogRepository, LogRepositoryError};
use thiserror::Error;
use tokio::sync::{Mutex, watch};

use crate::{
    CheckpointAdvance, CheckpointError, ExportCheckpointRepository, OtlpDestinationDigest,
    OtlpExporterName, OtlpTransport, OtlpTransportError, OtlpTransportOutcome, encode_otlp_logs,
};

/// Hard upper bound for one exporter request.
pub const OTLP_EXPORT_MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// Validated batching, polling, and retry policy for one named exporter.
#[derive(Clone, Debug)]
pub struct OtlpExporterConfig {
    /// Exact Operational Logs scope.
    pub scope: EnvironmentScope,
    /// Stable operator-selected exporter identity.
    pub name: OtlpExporterName,
    /// Digest binding endpoint and safe configuration identity.
    pub destination: OtlpDestinationDigest,
    /// Maximum records read for one request.
    pub maximum_batch_records: u16,
    /// Maximum encoded Protobuf request bytes.
    pub maximum_request_bytes: usize,
    /// Empty-source polling interval in follow mode.
    pub poll_interval: Duration,
    /// Total transport attempts for one batch, including the first.
    pub maximum_attempts: u8,
    /// Initial retry delay before deterministic jitter.
    pub retry_initial: Duration,
    /// Maximum retry delay, including a server `Retry-After` value.
    pub retry_maximum: Duration,
}

impl OtlpExporterConfig {
    fn validate(&self) -> Result<(), OtlpExportError> {
        if !(1..=1_000).contains(&self.maximum_batch_records)
            || !(1..=OTLP_EXPORT_MAX_REQUEST_BYTES).contains(&self.maximum_request_bytes)
            || !(Duration::from_millis(50)..=Duration::from_mins(1)).contains(&self.poll_interval)
            || !(1..=10).contains(&self.maximum_attempts)
            || !(Duration::from_millis(10)..=Duration::from_mins(1)).contains(&self.retry_initial)
            || self.retry_maximum < self.retry_initial
            || self.retry_maximum > Duration::from_mins(2)
        {
            return Err(OtlpExportError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Result of one source/checkpoint cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpExportOutcome {
    /// No records exist after the durable checkpoint.
    Idle {
        /// Current durable position.
        cursor: LogCursor,
    },
    /// A complete batch was acknowledged and checkpointed.
    Exported {
        /// Number of records in the acknowledged request.
        records: u16,
        /// Last cursor included in that request.
        cursor: LogCursor,
        /// Whether another process had already persisted the same exact cursor.
        replayed_checkpoint: bool,
    },
}

/// Exporter process behavior after one completed cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpExporterMode {
    /// Execute exactly one bounded cycle and return.
    Once,
    /// Continue draining and poll when the source is idle until shutdown.
    Follow,
}

/// Sanitized terminal state for an exporter run.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OtlpExportError {
    /// Batching, polling, or retry bounds are invalid.
    #[error("OTLP exporter configuration is invalid")]
    InvalidConfiguration,
    /// Another cycle is active in this process.
    #[error("OTLP exporter is already running")]
    AlreadyRunning,
    /// Shutdown interrupted polling, transport, or backoff.
    #[error("OTLP exporter was cancelled")]
    Cancelled,
    /// Operational Logs could not be queried safely.
    #[error("OTLP exporter source is unavailable")]
    Source,
    /// Existing exporter name is bound to a different endpoint/header-name configuration.
    #[error("OTLP exporter destination conflicts with its checkpoint")]
    ConfigurationDrift,
    /// Another exporter advanced to a different cursor concurrently.
    #[error("OTLP exporter checkpoint changed concurrently")]
    CheckpointConflict,
    /// Checkpoint storage is temporarily unavailable.
    #[error("OTLP exporter checkpoint is unavailable")]
    CheckpointUnavailable,
    /// Checkpoint storage contents or migration checksum are corrupt.
    #[error("OTLP exporter checkpoint is corrupt")]
    CheckpointCorrupt,
    /// A record or batch cannot be represented inside configured limits.
    #[error("OTLP exporter payload is invalid")]
    Payload,
    /// Collector permanently or partially rejected the batch.
    #[error("OTLP collector rejected the batch")]
    Rejected,
    /// Bounded retry attempts were exhausted; checkpoint remains unchanged.
    #[error("OTLP exporter retries were exhausted")]
    RetryExhausted,
    /// Collector returned a malformed or oversized successful response.
    #[error("OTLP collector response is invalid")]
    InvalidResponse,
}

impl OtlpExportError {
    /// Stable machine-readable status code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "OTLP_EXPORT_CONFIGURATION_INVALID",
            Self::AlreadyRunning => "OTLP_EXPORT_ALREADY_RUNNING",
            Self::Cancelled => "OTLP_EXPORT_CANCELLED",
            Self::Source => "OTLP_EXPORT_SOURCE",
            Self::ConfigurationDrift => "OTLP_EXPORT_CONFIGURATION_DRIFT",
            Self::CheckpointConflict => "OTLP_EXPORT_CHECKPOINT_CONFLICT",
            Self::CheckpointUnavailable => "OTLP_EXPORT_CHECKPOINT_UNAVAILABLE",
            Self::CheckpointCorrupt => "OTLP_EXPORT_CHECKPOINT_CORRUPT",
            Self::Payload => "OTLP_EXPORT_PAYLOAD",
            Self::Rejected => "OTLP_EXPORT_REJECTED",
            Self::RetryExhausted => "OTLP_EXPORT_RETRY_EXHAUSTED",
            Self::InvalidResponse => "OTLP_EXPORT_RESPONSE_INVALID",
        }
    }
}

/// Aggregate non-sensitive exporter counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OtlpExporterTelemetrySnapshot {
    /// Cycles admitted for execution.
    pub cycles: u64,
    /// Requests attempted, including retries.
    pub requests: u64,
    /// Records fully acknowledged and checkpointed.
    pub exported_records: u64,
    /// Retry delays entered.
    pub retries: u64,
    /// Batches whose remote ACK may be duplicated due to checkpoint replay.
    pub duplicates_possible: u64,
    /// Exact checkpoint CAS replays.
    pub checkpoint_replays: u64,
    /// Terminal cycle failures.
    pub failures: u64,
}

#[derive(Debug, Default)]
struct Telemetry {
    cycles: AtomicU64,
    requests: AtomicU64,
    exported_records: AtomicU64,
    retries: AtomicU64,
    duplicates_possible: AtomicU64,
    checkpoint_replays: AtomicU64,
    failures: AtomicU64,
}

/// Durable sequential exporter from Operational Event v1 to OTLP Logs.
pub struct OtlpLogExporter {
    config: OtlpExporterConfig,
    source: Arc<dyn LogRepository>,
    checkpoints: Arc<dyn ExportCheckpointRepository>,
    transport: Arc<dyn OtlpTransport>,
    active: Mutex<()>,
    telemetry: Telemetry,
}

impl fmt::Debug for OtlpLogExporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpLogExporter")
            .field("config", &self.config)
            .field("telemetry", &self.telemetry())
            .finish_non_exhaustive()
    }
}

impl OtlpLogExporter {
    /// Constructs an exporter after validating every local policy bound.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration before any repository or network operation.
    pub fn new(
        config: OtlpExporterConfig,
        source: Arc<dyn LogRepository>,
        checkpoints: Arc<dyn ExportCheckpointRepository>,
        transport: Arc<dyn OtlpTransport>,
    ) -> Result<Self, OtlpExportError> {
        config.validate()?;
        Ok(Self {
            config,
            source,
            checkpoints,
            transport,
            active: Mutex::new(()),
            telemetry: Telemetry::default(),
        })
    }

    /// Returns aggregate counters without endpoint, headers, or event content.
    #[must_use]
    pub fn telemetry(&self) -> OtlpExporterTelemetrySnapshot {
        OtlpExporterTelemetrySnapshot {
            cycles: self.telemetry.cycles.load(Ordering::Relaxed),
            requests: self.telemetry.requests.load(Ordering::Relaxed),
            exported_records: self.telemetry.exported_records.load(Ordering::Relaxed),
            retries: self.telemetry.retries.load(Ordering::Relaxed),
            duplicates_possible: self.telemetry.duplicates_possible.load(Ordering::Relaxed),
            checkpoint_replays: self.telemetry.checkpoint_replays.load(Ordering::Relaxed),
            failures: self.telemetry.failures.load(Ordering::Relaxed),
        }
    }

    /// Runs one bounded cycle or a cancelable continuous drain loop.
    ///
    /// # Errors
    ///
    /// Returns on the first terminal cycle failure. A requested shutdown is a successful exit.
    pub async fn run(
        &self,
        mode: OtlpExporterMode,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<OtlpExporterTelemetrySnapshot, OtlpExportError> {
        loop {
            match self.export_once(&mut shutdown).await {
                Ok(OtlpExportOutcome::Idle { .. }) if mode == OtlpExporterMode::Follow => {
                    tokio::select! {
                        biased;
                        () = wait_for_shutdown(&mut shutdown) => return Ok(self.telemetry()),
                        () = tokio::time::sleep(self.config.poll_interval) => {}
                    }
                }
                Ok(_) => {
                    if mode == OtlpExporterMode::Once {
                        return Ok(self.telemetry());
                    }
                }
                Err(OtlpExportError::Cancelled) => return Ok(self.telemetry()),
                Err(error) => return Err(error),
            }
        }
    }

    /// Executes one query/send/checkpoint cycle.
    ///
    /// # Errors
    ///
    /// Fails closed with the checkpoint unchanged unless the collector fully acknowledged the
    /// request. Cancellation is observed while querying, sending, and waiting for retry.
    pub async fn export_once(
        &self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<OtlpExportOutcome, OtlpExportError> {
        let Ok(_active) = self.active.try_lock() else {
            return Err(OtlpExportError::AlreadyRunning);
        };
        self.telemetry.cycles.fetch_add(1, Ordering::Relaxed);
        let result = self.export_once_inner(shutdown).await;
        if result.is_err() {
            self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn export_once_inner(
        &self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<OtlpExportOutcome, OtlpExportError> {
        ensure_running(shutdown)?;
        let checkpoint = self
            .checkpoints
            .load_or_create(
                self.config.scope,
                &self.config.name,
                self.config.destination,
            )
            .await
            .map_err(map_checkpoint)?;
        ensure_running(shutdown)?;
        let query = LogQuery {
            scope: self.config.scope,
            after: checkpoint.cursor,
            limit: self.config.maximum_batch_records,
            stream: None,
            minimum_level: None,
            function_id: None,
            request_id: None,
            invocation_id: None,
            client_id: None,
            credential_id: None,
            release_id: None,
        };
        let mut page = self.source.query(&query).await.map_err(map_source)?;
        if page.records.is_empty() {
            return Ok(OtlpExportOutcome::Idle {
                cursor: checkpoint.cursor,
            });
        }
        let payload = loop {
            match encode_otlp_logs(&page.records, self.config.maximum_request_bytes) {
                Ok(payload) => break payload,
                Err(OtlpTransportError::LimitExceeded) if page.records.len() > 1 => {
                    page.records.pop();
                }
                Err(_) => return Err(OtlpExportError::Payload),
            }
        };
        let next = page
            .records
            .last()
            .map(|record| record.cursor)
            .ok_or(OtlpExportError::Payload)?;
        self.send_with_retry(payload, next, shutdown).await?;
        let advance = self
            .checkpoints
            .advance(&checkpoint, next)
            .await
            .map_err(map_checkpoint)?;
        let records = u16::try_from(page.records.len()).map_err(|_| OtlpExportError::Payload)?;
        self.telemetry
            .exported_records
            .fetch_add(u64::from(records), Ordering::Relaxed);
        let replayed_checkpoint = advance == CheckpointAdvance::Replayed;
        if replayed_checkpoint {
            self.telemetry
                .checkpoint_replays
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(OtlpExportOutcome::Exported {
            records,
            cursor: next,
            replayed_checkpoint,
        })
    }

    async fn send_with_retry(
        &self,
        payload: Vec<u8>,
        cursor: LogCursor,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), OtlpExportError> {
        for attempt in 0..self.config.maximum_attempts {
            ensure_running(shutdown)?;
            self.telemetry.requests.fetch_add(1, Ordering::Relaxed);
            let outcome = tokio::select! {
                biased;
                () = wait_for_shutdown(shutdown) => return Err(OtlpExportError::Cancelled),
                result = self.transport.send(payload.clone()) => result,
            };
            let retry_after = match outcome {
                Ok(OtlpTransportOutcome::Accepted) => {
                    if attempt > 0 {
                        self.telemetry
                            .duplicates_possible
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(());
                }
                Ok(OtlpTransportOutcome::Retryable { retry_after }) => retry_after,
                Ok(OtlpTransportOutcome::Terminal) => return Err(OtlpExportError::Rejected),
                Err(OtlpTransportError::Unavailable) => None,
                Err(OtlpTransportError::InvalidResponse | OtlpTransportError::LimitExceeded) => {
                    return Err(OtlpExportError::InvalidResponse);
                }
                Err(
                    OtlpTransportError::InvalidConfiguration | OtlpTransportError::InvalidInput,
                ) => return Err(OtlpExportError::Payload),
            };
            if attempt + 1 >= self.config.maximum_attempts {
                return Err(OtlpExportError::RetryExhausted);
            }
            self.telemetry.retries.fetch_add(1, Ordering::Relaxed);
            let delay = retry_delay(&self.config, cursor, attempt, retry_after);
            tokio::select! {
                biased;
                () = wait_for_shutdown(shutdown) => return Err(OtlpExportError::Cancelled),
                () = tokio::time::sleep(delay) => {}
            }
        }
        Err(OtlpExportError::RetryExhausted)
    }
}

fn retry_delay(
    config: &OtlpExporterConfig,
    cursor: LogCursor,
    attempt: u8,
    retry_after: Option<Duration>,
) -> Duration {
    let multiplier = 1_u32.checked_shl(u32::from(attempt)).unwrap_or(u32::MAX);
    let exponential = config
        .retry_initial
        .checked_mul(multiplier)
        .unwrap_or(config.retry_maximum)
        .min(config.retry_maximum);
    let jitter_percent = 80 + ((cursor.get() ^ (u64::from(attempt) * 17)) % 41);
    let jittered = exponential
        .checked_mul(u32::try_from(jitter_percent).unwrap_or(100))
        .and_then(|duration| duration.checked_div(100))
        .unwrap_or(exponential)
        .min(config.retry_maximum);
    retry_after
        .unwrap_or_default()
        .max(jittered)
        .min(config.retry_maximum)
}

fn ensure_running(shutdown: &watch::Receiver<bool>) -> Result<(), OtlpExportError> {
    if *shutdown.borrow() {
        Err(OtlpExportError::Cancelled)
    } else {
        Ok(())
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        if shutdown.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

fn map_source(_error: LogRepositoryError) -> OtlpExportError {
    OtlpExportError::Source
}

fn map_checkpoint(error: CheckpointError) -> OtlpExportError {
    match error {
        CheckpointError::ConfigurationDrift => OtlpExportError::ConfigurationDrift,
        CheckpointError::Conflict => OtlpExportError::CheckpointConflict,
        CheckpointError::Unavailable => OtlpExportError::CheckpointUnavailable,
        CheckpointError::Corruption => OtlpExportError::CheckpointCorrupt,
        CheckpointError::InvalidRequest | CheckpointError::Unsupported => {
            OtlpExportError::InvalidConfiguration
        }
    }
}
