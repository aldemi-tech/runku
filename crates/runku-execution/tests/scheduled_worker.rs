//! Scheduled worker retry, timeout, pinning, and lease-expiry behavior.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{
    EnvironmentId, EnvironmentScope, OperationId, ProjectId, ReleaseId, ScheduledInvocationId,
    WorkerId,
};
use runku_data::{
    CommitBatch, LogicalStore, PinnedCode, ScheduleStatus, ScheduledInvocationInsert,
    ScheduledInvocationRecord,
};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::{
    ScheduledInvocationRunner, ScheduledRunFailure, ScheduledWorker, ScheduledWorkerConfig,
    ScheduledWorkerError, SchedulerClock,
};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::TempDir;

#[derive(Debug)]
struct TestClock(AtomicI64);

impl TestClock {
    fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

impl SchedulerClock for TestClock {
    fn now(&self) -> Result<TimestampMicros, ScheduledWorkerError> {
        Ok(TimestampMicros::new(self.0.load(Ordering::Relaxed)))
    }
}

#[derive(Debug)]
struct TestRunner {
    expected_release: ReleaseId,
    calls: Mutex<BTreeMap<String, u32>>,
}

#[async_trait]
impl ScheduledInvocationRunner for TestRunner {
    async fn execute(
        &self,
        _scope: EnvironmentScope,
        record: &ScheduledInvocationRecord,
    ) -> Result<(), ScheduledRunFailure> {
        if record.pinned_code != PinnedCode::Release(self.expected_release) {
            return Err(ScheduledRunFailure::new("PINNED_CODE_MISMATCH", false)
                .map_err(|_| fallback_failure())?);
        }
        let call = {
            let mut calls = self.calls.lock().map_err(|_| fallback_failure())?;
            let call = calls.entry(record.function.to_string()).or_default();
            *call = call.saturating_add(1);
            *call
        };
        match record.function.as_str() {
            "jobs.retry" if call == 1 => {
                Err(ScheduledRunFailure::new("TEMPORARY_UNAVAILABLE", true)
                    .map_err(|_| fallback_failure())?)
            }
            "jobs.success" | "jobs.crash" | "jobs.retry" => Ok(()),
            "jobs.terminal" => Err(ScheduledRunFailure::new("INVALID_DESTINATION", false)
                .map_err(|_| fallback_failure())?),
            "jobs.timeout" => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }
            _ => Err(fallback_failure()),
        }
    }
}

fn fallback_failure() -> ScheduledRunFailure {
    ScheduledRunFailure::internal()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_retries_times_out_fences_crashes_and_preserves_pinning()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("scheduled-worker.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let release = ReleaseId::generate();
    let ids = [
        ScheduledInvocationId::generate(),
        ScheduledInvocationId::generate(),
        ScheduledInvocationId::generate(),
        ScheduledInvocationId::generate(),
    ];
    let mut batch = CommitBatch::new(scope, OperationId::generate());
    for (id, function) in ids.into_iter().zip([
        "jobs.success",
        "jobs.retry",
        "jobs.terminal",
        "jobs.timeout",
    ]) {
        batch.push_schedule(schedule(id, release, function, 100)?);
    }
    store.commit(&batch).await?;

    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let runner = Arc::new(TestRunner {
        expected_release: release,
        calls: Mutex::new(BTreeMap::new()),
    });
    let config = ScheduledWorkerConfig {
        batch_limit: 10,
        lease_micros: 1_100_000,
        invocation_timeout: Duration::from_millis(5),
        max_attempts: 2,
        retry_base_micros: 10,
        retry_max_micros: 40,
    };
    let worker = ScheduledWorker::with_clock(
        store.clone(),
        runner,
        clock.clone(),
        WorkerId::generate(),
        config,
    )?;
    let first = worker.poll_once(scope).await?;
    assert_eq!(first.claimed, 4);
    assert_eq!(first.succeeded, 1);
    assert_eq!(first.retried, 2);
    assert_eq!(first.failed, 1);

    clock.set(109);
    assert_eq!(worker.poll_once(scope).await?.claimed, 0);
    clock.set(110);
    let retry = worker.poll_once(scope).await?;
    assert_eq!(retry.claimed, 2);
    assert_eq!(retry.succeeded, 1);
    assert_eq!(retry.failed, 1);

    assert_status(store.as_ref(), scope, ids[0], ScheduleStatus::Succeeded, 1).await?;
    assert_status(store.as_ref(), scope, ids[1], ScheduleStatus::Succeeded, 2).await?;
    assert_status(store.as_ref(), scope, ids[2], ScheduleStatus::Failed, 1).await?;
    assert_status(store.as_ref(), scope, ids[3], ScheduleStatus::Failed, 2).await?;

    let crash_id = ScheduledInvocationId::generate();
    let mut crash_batch = CommitBatch::new(scope, OperationId::generate());
    crash_batch.push_schedule(schedule(crash_id, release, "jobs.crash", 200)?);
    store.commit(&crash_batch).await?;
    let abandoned = store
        .claim_due_scheduled(
            scope,
            WorkerId::generate(),
            TimestampMicros::new(200),
            TimestampMicros::new(201),
            1,
        )
        .await?;
    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].record.lease_generation, 1);
    clock.set(202);
    let recovered = worker.poll_once(scope).await?;
    assert_eq!(recovered.succeeded, 1);
    assert_status(
        store.as_ref(),
        scope,
        crash_id,
        ScheduleStatus::Succeeded,
        2,
    )
    .await?;

    let telemetry = worker.telemetry();
    assert_eq!(telemetry.succeeded, 3);
    assert_eq!(telemetry.retried, 2);
    assert_eq!(telemetry.failed, 2);
    assert_eq!(telemetry.timeouts, 2);
    assert!(telemetry.max_lag_micros >= 2);
    Ok(())
}

fn schedule(
    id: ScheduledInvocationId,
    release: ReleaseId,
    function: &str,
    execute_at: i64,
) -> Result<ScheduledInvocationInsert, Box<dyn Error>> {
    Ok(ScheduledInvocationInsert {
        id,
        pinned_code: PinnedCode::Release(release),
        function: function.parse()?,
        args: CanonicalValue::Null,
        execute_at: TimestampMicros::new(execute_at),
        idempotency_key: None,
    })
}

async fn assert_status(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    id: ScheduledInvocationId,
    status: ScheduleStatus,
    attempts: u32,
) -> Result<(), Box<dyn Error>> {
    let mut snapshot = store.begin_read(scope).await?;
    let record = snapshot
        .get_scheduled(id)
        .await?
        .ok_or("schedule missing")?;
    snapshot.close().await?;
    assert_eq!(record.status, status);
    assert_eq!(record.attempts, attempts);
    Ok(())
}
