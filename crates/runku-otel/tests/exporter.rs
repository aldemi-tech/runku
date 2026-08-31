//! End-to-end exporter reliability over real Operational Logs and checkpoint repositories.

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    str::FromStr as _,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message as _;
use runku_core::{
    EnvironmentId, EnvironmentScope, FunctionId, FunctionName, InvocationId, OperationalEventId,
    ProjectId, ReleaseId, RequestId,
};
use runku_observability::{
    LogCursor, LogEventKind, LogLevel, LogPrincipalKind, LogQuery, LogRepository,
    LogRepositoryConfig, LogStream, OperationalEventV1, SqlLogRepository,
};
use runku_otel::{
    CheckpointAdvance, CheckpointError, ExportCheckpoint, ExportCheckpointRepository,
    OtlpDestinationDigest, OtlpEndpoint, OtlpExportError, OtlpExportOutcome, OtlpExporterConfig,
    OtlpExporterMode, OtlpExporterName, OtlpHeaders, OtlpLogExporter, OtlpRepositoryConfig,
    OtlpTransport, OtlpTransportError, OtlpTransportOutcome, SqlExportCheckpointRepository,
    encode_otlp_logs,
};
use runku_releases::FunctionType;
use runku_value::TimestampMicros;
use tempfile::TempDir;
use tokio::sync::{Mutex, watch};
use ulid::Ulid;

type TestResult = Result<(), Box<dyn Error>>;

fn scope() -> EnvironmentScope {
    EnvironmentScope::new(
        ProjectId::from_ulid(Ulid::from(31_000_u128)),
        EnvironmentId::from_ulid(Ulid::from(31_001_u128)),
    )
}

fn event(sequence: u128) -> Result<OperationalEventV1, Box<dyn Error>> {
    Ok(OperationalEventV1 {
        id: OperationalEventId::from_ulid(Ulid::from(sequence)),
        occurred_at: TimestampMicros::new(1_800_000_000_000_000 + i64::try_from(sequence)?),
        scope: scope(),
        request_id: RequestId::from_ulid(Ulid::from(31_010_u128)),
        invocation_id: InvocationId::from_ulid(Ulid::from(31_011_u128)),
        parent_invocation_id: None,
        release_id: ReleaseId::from_ulid(Ulid::from(31_012_u128)),
        dev_revision_id: None,
        function_id: FunctionId::from_ulid(Ulid::from(31_013_u128)),
        function_name: FunctionName::from_str("orders.export")?,
        function_type: FunctionType::Action,
        client_id: None,
        credential_id: None,
        principal_kind: LogPrincipalKind::Service,
        stream: LogStream::Platform,
        level: LogLevel::Info,
        kind: LogEventKind::InvocationStarted,
        message: None,
        fields: None,
        duration_micros: None,
        outcome_code: None,
    })
}

#[derive(Debug)]
struct QueueTransport {
    outcomes: Mutex<VecDeque<Result<OtlpTransportOutcome, OtlpTransportError>>>,
    payloads: Mutex<Vec<Vec<u8>>>,
    block: bool,
}

impl QueueTransport {
    fn new(outcomes: Vec<Result<OtlpTransportOutcome, OtlpTransportError>>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into()),
            payloads: Mutex::new(Vec::new()),
            block: false,
        }
    }

    fn blocked() -> Self {
        Self {
            outcomes: Mutex::new(VecDeque::new()),
            payloads: Mutex::new(Vec::new()),
            block: true,
        }
    }
}

#[async_trait]
impl OtlpTransport for QueueTransport {
    async fn send(&self, payload: Vec<u8>) -> Result<OtlpTransportOutcome, OtlpTransportError> {
        self.payloads.lock().await.push(payload);
        if self.block {
            std::future::pending::<()>().await;
        }
        self.outcomes
            .lock()
            .await
            .pop_front()
            .unwrap_or(Err(OtlpTransportError::Unavailable))
    }
}

#[derive(Debug)]
struct FailAdvanceCheckpoint {
    inner: Arc<SqlExportCheckpointRepository>,
}

#[async_trait]
impl ExportCheckpointRepository for FailAdvanceCheckpoint {
    async fn load_or_create(
        &self,
        environment: EnvironmentScope,
        exporter: &OtlpExporterName,
        destination: OtlpDestinationDigest,
    ) -> Result<ExportCheckpoint, CheckpointError> {
        self.inner
            .load_or_create(environment, exporter, destination)
            .await
    }

    async fn advance(
        &self,
        _checkpoint: &ExportCheckpoint,
        _next: LogCursor,
    ) -> Result<CheckpointAdvance, CheckpointError> {
        Err(CheckpointError::Unavailable)
    }

    async fn close(&self) {}
}

async fn repositories(
    directory: &TempDir,
) -> Result<(Arc<SqlLogRepository>, Arc<SqlExportCheckpointRepository>), Box<dyn Error>> {
    let logs = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("logs.sqlite3").display()
    );
    let checkpoints = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("otel.sqlite3").display()
    );
    Ok((
        Arc::new(SqlLogRepository::connect_sqlite(&logs, LogRepositoryConfig::LOCAL).await?),
        Arc::new(
            SqlExportCheckpointRepository::connect_sqlite(
                &checkpoints,
                OtlpRepositoryConfig::LOCAL,
            )
            .await?,
        ),
    ))
}

fn exporter_config() -> Result<OtlpExporterConfig, Box<dyn Error>> {
    let endpoint = OtlpEndpoint::from_str("http://127.0.0.1:4318/v1/logs")?;
    let headers = OtlpHeaders::new(BTreeMap::new())?;
    Ok(OtlpExporterConfig {
        scope: scope(),
        name: OtlpExporterName::from_str("primary")?,
        destination: OtlpDestinationDigest::new(&endpoint, &headers),
        maximum_batch_records: 2,
        maximum_request_bytes: 1024 * 1024,
        poll_interval: Duration::from_millis(50),
        maximum_attempts: 3,
        retry_initial: Duration::from_millis(10),
        retry_maximum: Duration::from_millis(20),
    })
}

#[tokio::test]
async fn acknowledged_batches_advance_exactly_and_retry_without_skipping() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (logs, checkpoints) = repositories(&directory).await?;
    logs.append(&[event(31_100)?, event(31_101)?, event(31_102)?])
        .await?;
    let transport = Arc::new(QueueTransport::new(vec![
        Err(OtlpTransportError::Unavailable),
        Ok(OtlpTransportOutcome::Retryable { retry_after: None }),
        Ok(OtlpTransportOutcome::Accepted),
        Ok(OtlpTransportOutcome::Accepted),
    ]));
    let source: Arc<dyn LogRepository> = logs.clone();
    let durable: Arc<dyn ExportCheckpointRepository> = checkpoints.clone();
    let boundary: Arc<dyn OtlpTransport> = transport.clone();
    let exporter = OtlpLogExporter::new(exporter_config()?, source, durable, boundary)?;
    let (_shutdown, mut receiver) = watch::channel(false);

    assert_eq!(
        exporter.export_once(&mut receiver).await?,
        OtlpExportOutcome::Exported {
            records: 2,
            cursor: LogCursor::new(2),
            replayed_checkpoint: false,
        }
    );
    assert_eq!(
        exporter.export_once(&mut receiver).await?,
        OtlpExportOutcome::Exported {
            records: 1,
            cursor: LogCursor::new(3),
            replayed_checkpoint: false,
        }
    );
    assert_eq!(
        exporter.export_once(&mut receiver).await?,
        OtlpExportOutcome::Idle {
            cursor: LogCursor::new(3)
        }
    );
    let payloads = transport.payloads.lock().await;
    assert_eq!(payloads.len(), 4);
    let first = ExportLogsServiceRequest::decode(payloads[0].as_slice())?;
    assert_eq!(first.resource_logs[0].scope_logs[0].log_records.len(), 2);
    let telemetry = exporter.telemetry();
    assert_eq!(telemetry.requests, 4);
    assert_eq!(telemetry.retries, 2);
    assert_eq!(telemetry.exported_records, 3);
    assert_eq!(telemetry.failures, 0);
    logs.close().await;
    checkpoints.close().await;
    Ok(())
}

#[tokio::test]
async fn rejection_and_retry_exhaustion_leave_checkpoint_unchanged() -> TestResult {
    for outcomes in [
        vec![Ok(OtlpTransportOutcome::Terminal)],
        vec![
            Err(OtlpTransportError::Unavailable),
            Err(OtlpTransportError::Unavailable),
            Err(OtlpTransportError::Unavailable),
        ],
    ] {
        let directory = tempfile::tempdir()?;
        let (logs, checkpoints) = repositories(&directory).await?;
        logs.append(&[event(31_200)?]).await?;
        let expected = if outcomes.len() == 1 {
            OtlpExportError::Rejected
        } else {
            OtlpExportError::RetryExhausted
        };
        let source: Arc<dyn LogRepository> = logs.clone();
        let durable: Arc<dyn ExportCheckpointRepository> = checkpoints.clone();
        let exporter = OtlpLogExporter::new(
            exporter_config()?,
            source,
            durable,
            Arc::new(QueueTransport::new(outcomes)),
        )?;
        let (_shutdown, mut receiver) = watch::channel(false);
        assert_eq!(exporter.export_once(&mut receiver).await, Err(expected));
        let checkpoint = checkpoints
            .load_or_create(
                scope(),
                &exporter_config()?.name,
                exporter_config()?.destination,
            )
            .await?;
        assert_eq!(checkpoint.cursor, LogCursor::START);
        logs.close().await;
        checkpoints.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn crash_after_remote_ack_replays_same_batch_then_checkpoints() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (logs, checkpoints) = repositories(&directory).await?;
    logs.append(&[event(31_300)?]).await?;
    let transport = Arc::new(QueueTransport::new(vec![
        Ok(OtlpTransportOutcome::Accepted),
        Ok(OtlpTransportOutcome::Accepted),
    ]));
    let source: Arc<dyn LogRepository> = logs.clone();
    let failing: Arc<dyn ExportCheckpointRepository> = Arc::new(FailAdvanceCheckpoint {
        inner: checkpoints.clone(),
    });
    let boundary: Arc<dyn OtlpTransport> = transport.clone();
    let first = OtlpLogExporter::new(exporter_config()?, source, failing, boundary)?;
    let (_shutdown, mut receiver) = watch::channel(false);
    assert_eq!(
        first.export_once(&mut receiver).await,
        Err(OtlpExportError::CheckpointUnavailable)
    );

    let source: Arc<dyn LogRepository> = logs.clone();
    let durable: Arc<dyn ExportCheckpointRepository> = checkpoints.clone();
    let boundary: Arc<dyn OtlpTransport> = transport.clone();
    let restarted = OtlpLogExporter::new(exporter_config()?, source, durable, boundary)?;
    assert_eq!(
        restarted.export_once(&mut receiver).await?,
        OtlpExportOutcome::Exported {
            records: 1,
            cursor: LogCursor::new(1),
            replayed_checkpoint: false,
        }
    );
    let payloads = transport.payloads.lock().await;
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0], payloads[1]);
    logs.close().await;
    checkpoints.close().await;
    Ok(())
}

#[tokio::test]
async fn in_flight_transport_is_cancelled_without_checkpoint_advance() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (logs, checkpoints) = repositories(&directory).await?;
    logs.append(&[event(31_400)?]).await?;
    let source: Arc<dyn LogRepository> = logs.clone();
    let durable: Arc<dyn ExportCheckpointRepository> = checkpoints.clone();
    let exporter = Arc::new(OtlpLogExporter::new(
        exporter_config()?,
        source,
        durable,
        Arc::new(QueueTransport::blocked()),
    )?);
    let (shutdown, mut receiver) = watch::channel(false);
    let task = {
        let exporter = Arc::clone(&exporter);
        tokio::spawn(async move { exporter.export_once(&mut receiver).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    shutdown.send(true)?;
    assert_eq!(task.await?, Err(OtlpExportError::Cancelled));
    let checkpoint = checkpoints
        .load_or_create(
            scope(),
            &exporter_config()?.name,
            exporter_config()?.destination,
        )
        .await?;
    assert_eq!(checkpoint.cursor, LogCursor::START);
    logs.close().await;
    checkpoints.close().await;
    Ok(())
}

#[tokio::test]
async fn byte_bound_shrinks_batch_without_skipping_and_follow_stops_while_idle() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (logs, checkpoints) = repositories(&directory).await?;
    logs.append(&[event(31_500)?, event(31_501)?]).await?;
    let page = logs
        .query(&LogQuery {
            scope: scope(),
            after: LogCursor::START,
            limit: 2,
            stream: None,
            minimum_level: None,
            function_id: None,
            request_id: None,
            invocation_id: None,
            client_id: None,
            credential_id: None,
            release_id: None,
        })
        .await?;
    let one_record_bytes = encode_otlp_logs(&page.records[..1], usize::MAX)?.len();
    assert!(encode_otlp_logs(&page.records, one_record_bytes).is_err());
    let mut config = exporter_config()?;
    config.maximum_request_bytes = one_record_bytes;
    let source: Arc<dyn LogRepository> = logs.clone();
    let durable: Arc<dyn ExportCheckpointRepository> = checkpoints.clone();
    let exporter = OtlpLogExporter::new(
        config,
        source,
        durable,
        Arc::new(QueueTransport::new(vec![Ok(
            OtlpTransportOutcome::Accepted,
        )])),
    )?;
    let (_shutdown, mut receiver) = watch::channel(false);
    assert_eq!(
        exporter.export_once(&mut receiver).await?,
        OtlpExportOutcome::Exported {
            records: 1,
            cursor: LogCursor::new(1),
            replayed_checkpoint: false,
        }
    );
    logs.close().await;
    checkpoints.close().await;

    let idle_directory = tempfile::tempdir()?;
    let (idle_logs, idle_checkpoints) = repositories(&idle_directory).await?;
    let source: Arc<dyn LogRepository> = idle_logs.clone();
    let durable: Arc<dyn ExportCheckpointRepository> = idle_checkpoints.clone();
    let idle = Arc::new(OtlpLogExporter::new(
        exporter_config()?,
        source,
        durable,
        Arc::new(QueueTransport::new(vec![])),
    )?);
    let (shutdown, receiver) = watch::channel(false);
    let task = {
        let idle = Arc::clone(&idle);
        tokio::spawn(async move { idle.run(OtlpExporterMode::Follow, receiver).await })
    };
    tokio::time::sleep(Duration::from_millis(70)).await;
    shutdown.send(true)?;
    let telemetry = task.await??;
    assert!(telemetry.cycles >= 1);
    assert_eq!(telemetry.requests, 0);
    idle_logs.close().await;
    idle_checkpoints.close().await;
    Ok(())
}
