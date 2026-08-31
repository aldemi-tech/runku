use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::sync::{mpsc, watch};

use crate::{
    LOG_APPEND_MAX_RECORDS, LogRepository, LogSinkError, OperationalEventV1, OperationalLogSink,
};

const MAX_CAPACITY: usize = 65_536;
const MAX_RETRY_ATTEMPTS: u8 = 5;
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(20);

/// Bounded in-process spool policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogSpoolConfig {
    /// Maximum admitted records awaiting persistence.
    pub capacity: usize,
    /// Maximum records in one atomic repository append.
    pub maximum_batch: usize,
}

impl LogSpoolConfig {
    /// Product Base local defaults: bounded memory and repository-sized batches.
    pub const LOCAL: Self = Self {
        capacity: 8_192,
        maximum_batch: 128,
    };

    fn validate(self) -> Result<Self, LogSinkError> {
        if !(1..=MAX_CAPACITY).contains(&self.capacity)
            || !(1..=LOG_APPEND_MAX_RECORDS).contains(&self.maximum_batch)
        {
            return Err(LogSinkError::Unavailable);
        }
        Ok(self)
    }
}

#[derive(Debug, Default)]
struct Counters {
    accepted: AtomicU64,
    dropped_full: AtomicU64,
    dropped_unavailable: AtomicU64,
    persisted: AtomicU64,
    persistence_failures: AtomicU64,
    retries: AtomicU64,
}

/// Bounded process-local spool counters without request-controlled labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogSpoolTelemetrySnapshot {
    /// Records admitted without blocking.
    pub accepted: u64,
    /// Records rejected because bounded capacity was full.
    pub dropped_full: u64,
    /// Records rejected after writer shutdown/disconnection.
    pub dropped_unavailable: u64,
    /// Records durably appended.
    pub persisted: u64,
    /// Batches exhausted after bounded retry.
    pub persistence_failures: u64,
    /// Repository retries attempted.
    pub retries: u64,
}

/// Cloneable nonblocking execution-side log sink.
#[derive(Clone)]
pub struct BufferedLogSink {
    sender: mpsc::Sender<OperationalEventV1>,
    counters: Arc<Counters>,
}

impl fmt::Debug for BufferedLogSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BufferedLogSink")
            .field("capacity", &self.sender.max_capacity())
            .finish_non_exhaustive()
    }
}

impl BufferedLogSink {
    /// Creates one bounded sink and its single durable writer.
    ///
    /// # Errors
    ///
    /// Rejects zero or unsupported capacity/batch limits.
    pub fn new(
        config: LogSpoolConfig,
        repository: Arc<dyn LogRepository>,
    ) -> Result<(Self, LogSpoolWriter), LogSinkError> {
        let config = config.validate()?;
        let (sender, receiver) = mpsc::channel(config.capacity);
        let counters = Arc::new(Counters::default());
        Ok((
            Self {
                sender,
                counters: Arc::clone(&counters),
            },
            LogSpoolWriter {
                config,
                receiver,
                repository,
                counters,
            },
        ))
    }

    /// Returns bounded aggregate telemetry.
    #[must_use]
    pub fn telemetry(&self) -> LogSpoolTelemetrySnapshot {
        snapshot(&self.counters)
    }
}

impl OperationalLogSink for BufferedLogSink {
    fn try_emit(&self, event: OperationalEventV1) -> Result<(), LogSinkError> {
        event.validate().map_err(|_| LogSinkError::InvalidEvent)?;
        match self.sender.try_send(event) {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.counters.dropped_full.fetch_add(1, Ordering::Relaxed);
                Err(LogSinkError::Full)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.counters
                    .dropped_unavailable
                    .fetch_add(1, Ordering::Relaxed);
                Err(LogSinkError::Unavailable)
            }
        }
    }
}

/// Single-consumer durable batch writer; run as one supervised Product Base task.
pub struct LogSpoolWriter {
    config: LogSpoolConfig,
    receiver: mpsc::Receiver<OperationalEventV1>,
    repository: Arc<dyn LogRepository>,
    counters: Arc<Counters>,
}

impl fmt::Debug for LogSpoolWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogSpoolWriter")
            .field("config", &self.config)
            .field("repository_backend", &self.repository.backend())
            .finish_non_exhaustive()
    }
}

impl LogSpoolWriter {
    /// Runs until explicit shutdown or all senders disconnect. Shutdown closes admission first,
    /// drains the channel, and performs bounded retries for every admitted batch.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> LogSpoolTelemetrySnapshot {
        loop {
            let first = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.receiver.close();
                    }
                    self.receiver.recv().await
                }
                event = self.receiver.recv() => event,
            };
            let Some(first) = first else {
                break;
            };
            let mut batch = Vec::with_capacity(self.config.maximum_batch);
            batch.push(first);
            while batch.len() < self.config.maximum_batch {
                match self.receiver.try_recv() {
                    Ok(event) => batch.push(event),
                    Err(
                        mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected,
                    ) => {
                        break;
                    }
                }
            }
            self.persist(&batch).await;
        }
        self.repository.close().await;
        snapshot(&self.counters)
    }

    async fn persist(&self, batch: &[OperationalEventV1]) {
        let mut delay = INITIAL_RETRY_DELAY;
        for attempt in 0..MAX_RETRY_ATTEMPTS {
            match self.repository.append(batch).await {
                Ok(_) => {
                    self.counters.persisted.fetch_add(
                        u64::try_from(batch.len()).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    return;
                }
                Err(_) if attempt + 1 < MAX_RETRY_ATTEMPTS => {
                    self.counters.retries.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
                Err(_) => {
                    self.counters
                        .persistence_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        }
    }
}

fn snapshot(counters: &Counters) -> LogSpoolTelemetrySnapshot {
    LogSpoolTelemetrySnapshot {
        accepted: counters.accepted.load(Ordering::Relaxed),
        dropped_full: counters.dropped_full.load(Ordering::Relaxed),
        dropped_unavailable: counters.dropped_unavailable.load(Ordering::Relaxed),
        persisted: counters.persisted.load(Ordering::Relaxed),
        persistence_failures: counters.persistence_failures.load(Ordering::Relaxed),
        retries: counters.retries.load(Ordering::Relaxed),
    }
}
