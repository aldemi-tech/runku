//! Reproducible Scheduled Worker baseline over durable `SQLite`.

use std::{error::Error, sync::Arc, time::Duration};

use async_trait::async_trait;
use runku_core::{
    EnvironmentId, EnvironmentScope, OperationId, ProjectId, ReleaseId, ScheduledInvocationId,
    WorkerId,
};
use runku_data::{
    CommitBatch, LogicalStore, PinnedCode, ScheduledInvocationInsert, ScheduledInvocationRecord,
};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::{
    ScheduledInvocationRunner, ScheduledRunFailure, ScheduledWorker, ScheduledWorkerConfig,
    ScheduledWorkerError, SchedulerClock,
};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::TempDir;

const RECORDS: u64 = 10_000;

#[derive(Debug)]
struct FixedClock;

impl SchedulerClock for FixedClock {
    fn now(&self) -> Result<TimestampMicros, ScheduledWorkerError> {
        Ok(TimestampMicros::new(1))
    }
}

#[derive(Debug)]
struct NoopRunner;

#[async_trait]
impl ScheduledInvocationRunner for NoopRunner {
    async fn execute(
        &self,
        _scope: EnvironmentScope,
        _record: &ScheduledInvocationRecord,
    ) -> Result<(), ScheduledRunFailure> {
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("scheduling-baseline.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let release = ReleaseId::generate();
    for _ in 0..(RECORDS / 100) {
        let mut batch = CommitBatch::new(scope, OperationId::generate());
        for _ in 0..100 {
            batch.push_schedule(ScheduledInvocationInsert {
                id: ScheduledInvocationId::generate(),
                pinned_code: PinnedCode::Release(release),
                function: "bench.execute".parse()?,
                args: CanonicalValue::Null,
                execute_at: TimestampMicros::new(0),
                idempotency_key: None,
            });
        }
        store.commit(&batch).await?;
    }
    let worker = ScheduledWorker::with_clock(
        store,
        Arc::new(NoopRunner),
        Arc::new(FixedClock),
        WorkerId::generate(),
        ScheduledWorkerConfig {
            batch_limit: 100,
            lease_micros: 1_200_000,
            invocation_timeout: Duration::from_millis(1),
            max_attempts: 1,
            retry_base_micros: 1,
            retry_max_micros: 1,
        },
    )?;
    let started = std::time::Instant::now();
    let mut completed = 0_u64;
    while completed < RECORDS {
        completed = completed.saturating_add(u64::from(worker.poll_once(scope).await?.succeeded));
    }
    let elapsed = started.elapsed();
    let per_second = u128::from(completed)
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed.as_nanos())
        .unwrap_or(0);
    println!(
        "scheduling_baseline records={completed} elapsed_us={} records_per_second={per_second}",
        elapsed.as_micros()
    );
    Ok(())
}
