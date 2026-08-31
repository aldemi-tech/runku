//! Bounded spool saturation, retry, and graceful-drain conformance.

use std::{error::Error, fmt, str::FromStr, sync::Arc};

use async_trait::async_trait;
use runku_core::{
    EnvironmentId, EnvironmentScope, FunctionId, FunctionName, InvocationId, OperationalEventId,
    ProjectId, ReleaseId, RequestId,
};
use runku_observability::{
    BufferedLogSink, LogCursor, LogEventKind, LogLevel, LogPage, LogPrincipalKind, LogQuery,
    LogRepository, LogRepositoryBackend, LogRepositoryError, LogSinkError, LogSpoolConfig,
    OperationalEventV1, OperationalLogSink, PruneResult,
};
use runku_releases::FunctionType;
use runku_value::TimestampMicros;
use tokio::sync::Mutex;
use ulid::Ulid;

type TestResult = Result<(), Box<dyn Error>>;

#[derive(Default)]
struct RecordingRepository {
    state: Mutex<RecordingState>,
    failures_before_success: u32,
}

#[derive(Default)]
struct RecordingState {
    attempts: u32,
    records: Vec<OperationalEventV1>,
    closed: bool,
}

impl fmt::Debug for RecordingRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingRepository")
            .field("failures_before_success", &self.failures_before_success)
            .finish_non_exhaustive()
    }
}

impl RecordingRepository {
    fn new(failures_before_success: u32) -> Self {
        Self {
            failures_before_success,
            ..Self::default()
        }
    }
}

#[async_trait]
impl LogRepository for RecordingRepository {
    fn backend(&self) -> LogRepositoryBackend {
        LogRepositoryBackend::SQLite
    }

    async fn append(&self, events: &[OperationalEventV1]) -> Result<LogCursor, LogRepositoryError> {
        let mut state = self.state.lock().await;
        state.attempts = state.attempts.saturating_add(1);
        if state.attempts <= self.failures_before_success {
            return Err(LogRepositoryError::Unavailable);
        }
        state.records.extend_from_slice(events);
        Ok(LogCursor::new(
            u64::try_from(state.records.len()).map_err(|_| LogRepositoryError::LimitExceeded)?,
        ))
    }

    async fn query(&self, _query: &LogQuery) -> Result<LogPage, LogRepositoryError> {
        Err(LogRepositoryError::Unsupported)
    }

    async fn prune_before(
        &self,
        _scope: EnvironmentScope,
        _cutoff: TimestampMicros,
        _maximum: u32,
        _dry_run: bool,
    ) -> Result<PruneResult, LogRepositoryError> {
        Err(LogRepositoryError::Unsupported)
    }

    async fn close(&self) {
        self.state.lock().await.closed = true;
    }
}

fn event(sequence: u128) -> Result<OperationalEventV1, Box<dyn Error>> {
    Ok(OperationalEventV1 {
        id: OperationalEventId::from_ulid(Ulid::from(sequence)),
        occurred_at: TimestampMicros::new(1_800_000_000_000_000),
        scope: EnvironmentScope::new(
            ProjectId::from_ulid(Ulid::from(1)),
            EnvironmentId::from_ulid(Ulid::from(2)),
        ),
        request_id: RequestId::from_ulid(Ulid::from(3)),
        invocation_id: InvocationId::from_ulid(Ulid::from(4)),
        parent_invocation_id: None,
        release_id: ReleaseId::from_ulid(Ulid::from(5)),
        dev_revision_id: None,
        function_id: FunctionId::from_ulid(Ulid::from(6)),
        function_name: FunctionName::from_str("logs.test")?,
        function_type: FunctionType::Action,
        client_id: None,
        credential_id: None,
        principal_kind: LogPrincipalKind::None,
        stream: runku_observability::LogStream::Platform,
        level: LogLevel::Info,
        kind: LogEventKind::InvocationStarted,
        message: None,
        fields: None,
        duration_micros: None,
        outcome_code: None,
    })
}

#[tokio::test]
async fn full_spool_rejects_without_blocking_and_shutdown_drains_admitted_records() -> TestResult {
    let repository = Arc::new(RecordingRepository::default());
    let (sink, writer) = BufferedLogSink::new(
        LogSpoolConfig {
            capacity: 1,
            maximum_batch: 1,
        },
        Arc::clone(&repository) as Arc<dyn LogRepository>,
    )?;
    sink.try_emit(event(10)?)?;
    assert_eq!(sink.try_emit(event(11)?), Err(LogSinkError::Full));
    assert_eq!(sink.telemetry().accepted, 1);
    assert_eq!(sink.telemetry().dropped_full, 1);

    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    shutdown.send(true)?;
    let final_telemetry = writer.run(receiver).await;
    assert_eq!(final_telemetry.persisted, 1);

    let state = repository.state.lock().await;
    assert_eq!(state.records.len(), 1);
    assert!(state.closed);
    drop(state);
    assert_eq!(sink.try_emit(event(12)?), Err(LogSinkError::Unavailable));
    assert_eq!(sink.telemetry().dropped_unavailable, 1);
    Ok(())
}

#[tokio::test]
async fn transient_repository_failures_retry_then_persist_each_admitted_record() -> TestResult {
    let repository = Arc::new(RecordingRepository::new(2));
    let (sink, writer) = BufferedLogSink::new(
        LogSpoolConfig {
            capacity: 4,
            maximum_batch: 2,
        },
        Arc::clone(&repository) as Arc<dyn LogRepository>,
    )?;
    sink.try_emit(event(20)?)?;
    sink.try_emit(event(21)?)?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    shutdown.send(true)?;

    let final_telemetry = writer.run(receiver).await;
    assert_eq!(final_telemetry.accepted, 2);
    assert_eq!(final_telemetry.persisted, 2);
    assert_eq!(final_telemetry.retries, 2);
    assert_eq!(final_telemetry.persistence_failures, 0);
    let state = repository.state.lock().await;
    assert_eq!(state.attempts, 3);
    assert_eq!(state.records.len(), 2);
    Ok(())
}

#[tokio::test]
async fn persistent_repository_failure_is_bounded_and_counted() -> TestResult {
    let repository = Arc::new(RecordingRepository::new(u32::MAX));
    let (sink, writer) = BufferedLogSink::new(
        LogSpoolConfig {
            capacity: 1,
            maximum_batch: 1,
        },
        Arc::clone(&repository) as Arc<dyn LogRepository>,
    )?;
    sink.try_emit(event(30)?)?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    shutdown.send(true)?;

    let final_telemetry = writer.run(receiver).await;
    assert_eq!(final_telemetry.persisted, 0);
    assert_eq!(final_telemetry.retries, 4);
    assert_eq!(final_telemetry.persistence_failures, 1);
    let state = repository.state.lock().await;
    assert_eq!(state.attempts, 5);
    assert!(state.records.is_empty());
    assert!(state.closed);
    Ok(())
}
