//! Real SQLite/PostgreSQL Cron activation, recovery, and execution conformance.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{
    BuildId, EnvironmentDescriptor, EnvironmentId, EnvironmentLocation, EnvironmentScope,
    FunctionId, OperationId, PinnedCode, ProjectId, ReleaseId, WorkerId,
};
use runku_cron::{
    ClaimedCronActivation, CronBackend, CronCommand, CronCommandResult, CronContext, CronError,
    CronMaterializer, CronMaterializerConfig, CronRepository, CronRepositoryConfig, CronSnapshot,
    CronTelemetrySnapshot, SqlCronRepository,
};
use runku_data::{LogicalStore, ScheduledInvocationRecord};
use runku_data_postgres::{PostgresStore, PostgresStoreConfig};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::{
    ScheduledInvocationRunner, ScheduledRunFailure, ScheduledWorker, ScheduledWorkerConfig,
    ScheduledWorkerError, SchedulerClock,
};
use runku_releases::{
    ArtifactDescriptor, ArtifactFormat, AuthPolicy, Capability, CronDefinition, FunctionManifest,
    FunctionType, FunctionVisibility, ReleaseManifestV1, RuntimeClass, Sha256Digest,
    encode_release_manifest,
};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::tempdir;

const MINUTE: i64 = 60_000_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_reopen_replay_recovery_two_workers_and_execution() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let denied_path = directory.path().join("production.sqlite3");
    let denied_url = format!("sqlite://{}?mode=rwc", denied_path.display());
    let denied_environment = EnvironmentId::generate();
    let denied_context = CronContext {
        scope: EnvironmentScope::new(ProjectId::generate(), denied_environment),
        environment: EnvironmentDescriptor::production(
            denied_environment,
            EnvironmentLocation::SelfHosted,
        ),
    };
    assert!(matches!(
        SqlCronRepository::connect_sqlite(&denied_url, CronRepositoryConfig::LOCAL, denied_context)
            .await,
        Err(CronError::Unsupported)
    ));
    assert!(!denied_path.exists());
    let cron_path = directory.path().join("cron.sqlite3");
    let cron_url = format!("sqlite://{}?mode=rwc", cron_path.display());
    let data_path = directory.path().join("data.sqlite3");
    let context = local_context();
    let repository = Arc::new(
        SqlCronRepository::connect_sqlite(&cron_url, CronRepositoryConfig::LOCAL, context).await?,
    );
    let store = Arc::new(
        SqliteStore::open(
            data_path,
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    run_conformance(repository.clone(), store, context, CronBackend::SQLite).await?;
    repository.close().await;
    let reopened =
        SqlCronRepository::connect_sqlite(&cron_url, CronRepositoryConfig::LOCAL, context).await?;
    assert_eq!(reopened.snapshot(context).await?.repository_revision, 3);
    reopened.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_replay_recovery_two_workers_and_execution() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let context = local_context();
    let repository = Arc::new(
        SqlCronRepository::connect_postgres(&url, CronRepositoryConfig::AUTHORITATIVE, context)
            .await?,
    );
    let store = Arc::new(PostgresStore::connect(&url, PostgresStoreConfig::TEST).await?);
    let result = run_conformance(
        repository.clone(),
        store.clone(),
        context,
        CronBackend::PostgreSQL,
    )
    .await;
    repository.close().await;
    store.close().await;
    result
}

#[allow(clippy::too_many_lines)]
async fn run_conformance(
    repository: Arc<SqlCronRepository>,
    store: Arc<dyn LogicalStore>,
    context: CronContext,
    backend: CronBackend,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(repository.backend(), backend);
    repository.health().await?;
    assert_eq!(repository.snapshot(context).await?.repository_revision, 0);
    let wrong_environment = EnvironmentId::generate();
    let wrong_context = CronContext {
        scope: EnvironmentScope::new(context.scope.project_id(), wrong_environment),
        environment: EnvironmentDescriptor::local_development(wrong_environment),
    };
    assert_eq!(
        repository.snapshot(wrong_context).await,
        Err(CronError::InvalidInput)
    );
    let release_one = ReleaseId::generate();
    let activate = activation_command(context, release_one, 0, 0)?;
    let operation = OperationId::generate();
    let applied = repository.apply(context, operation, &activate).await?;
    assert_eq!(applied.repository_revision, 1);
    assert_eq!(applied.active_definitions, 1);
    assert!(!applied.replayed);
    assert!(
        repository
            .apply(context, operation, &activate)
            .await?
            .replayed
    );
    let divergent = activation_command(context, release_one, 0, 1)?;
    assert_eq!(
        repository.apply(context, operation, &divergent).await,
        Err(CronError::Conflict)
    );

    let config = CronMaterializerConfig {
        batch_size: 8,
        lease_duration: Duration::from_micros(10),
    };
    let first = CronMaterializer::new(
        repository.clone(),
        store.clone(),
        context,
        WorkerId::generate(),
        config,
    )?;
    assert_eq!(
        first
            .poll_at(TimestampMicros::new(MINUTE))
            .await?
            .materialized,
        1
    );

    let worker_a = CronMaterializer::new(
        repository.clone(),
        store.clone(),
        context,
        WorkerId::generate(),
        config,
    )?;
    let worker_b = CronMaterializer::new(
        repository.clone(),
        store.clone(),
        context,
        WorkerId::generate(),
        config,
    )?;
    let (a, b) = tokio::join!(
        worker_a.poll_at(TimestampMicros::new(2 * MINUTE)),
        worker_b.poll_at(TimestampMicros::new(2 * MINUTE))
    );
    let (a, b) = (a?, b?);
    assert_eq!(a.materialized + b.materialized, 1);
    assert_eq!(a.completed + b.completed, 1);

    let fault_repository: Arc<dyn CronRepository> = Arc::new(FailCompletionOnce {
        inner: repository.clone(),
        fail: AtomicBool::new(true),
    });
    let faulting = CronMaterializer::new(
        fault_repository,
        store.clone(),
        context,
        WorkerId::generate(),
        config,
    )?;
    let fault = faulting.poll_at(TimestampMicros::new(3 * MINUTE)).await?;
    assert_eq!((fault.materialized, fault.lease_lost), (1, 1));
    let recovering = CronMaterializer::new(
        repository.clone(),
        store.clone(),
        context,
        WorkerId::generate(),
        config,
    )?;
    assert_eq!(
        recovering
            .poll_at(TimestampMicros::new(3 * MINUTE + 11))
            .await?
            .replayed,
        1
    );

    let deactivate = CronCommand::DeactivateAll {
        expected_revision: 1,
        deactivated_at: TimestampMicros::new(4 * MINUTE),
    };
    assert_eq!(
        repository
            .apply(context, OperationId::generate(), &deactivate)
            .await?
            .active_definitions,
        0
    );
    assert_eq!(
        recovering
            .poll_at(TimestampMicros::new(100 * MINUTE))
            .await?
            .claimed,
        0
    );

    let release_two = ReleaseId::generate();
    let reactivate = activation_command(context, release_two, 2, 4 * MINUTE)?;
    repository
        .apply(context, OperationId::generate(), &reactivate)
        .await?;
    assert_eq!(
        recovering
            .poll_at(TimestampMicros::new(5 * MINUTE))
            .await?
            .materialized,
        1
    );
    let snapshot = repository.snapshot(context).await?;
    assert_eq!(snapshot.repository_revision, 3);
    assert_eq!(snapshot.activations[0].activation_revision, 3);
    assert_eq!(
        snapshot.activations[0].pinned_code,
        PinnedCode::Release(release_two)
    );

    let clock = Arc::new(TestClock(AtomicI64::new(5 * MINUTE)));
    let runner = Arc::new(RecordingRunner::default());
    let scheduled = ScheduledWorker::with_clock(
        store,
        runner.clone(),
        clock,
        WorkerId::generate(),
        ScheduledWorkerConfig {
            batch_limit: 10,
            lease_micros: 2_000_000,
            invocation_timeout: Duration::from_millis(10),
            max_attempts: 2,
            retry_base_micros: 1,
            retry_max_micros: 2,
        },
    )?;
    let executed = scheduled.poll_once(context.scope).await?;
    assert_eq!((executed.claimed, executed.succeeded), (4, 4));
    let pins = runner.pins.lock().map_err(|_| "runner mutex poisoned")?;
    assert_eq!(
        pins.iter()
            .filter(|pin| **pin == PinnedCode::Release(release_one))
            .count(),
        3
    );
    assert_eq!(
        pins.iter()
            .filter(|pin| **pin == PinnedCode::Release(release_two))
            .count(),
        1
    );
    assert!(repository.telemetry().conflicts >= 1);
    Ok(())
}

fn local_context() -> CronContext {
    let environment = EnvironmentId::generate();
    CronContext {
        scope: EnvironmentScope::new(ProjectId::generate(), environment),
        environment: EnvironmentDescriptor::local_development(environment),
    }
}

fn activation_command(
    context: CronContext,
    release_id: ReleaseId,
    expected_revision: u64,
    activated_at: i64,
) -> Result<CronCommand, Box<dyn Error>> {
    let function = FunctionManifest {
        id: FunctionId::generate(),
        name: "jobs.minute".parse()?,
        function_type: FunctionType::Action,
        visibility: FunctionVisibility::Internal,
        auth_policy: AuthPolicy::None,
        runtime_class: RuntimeClass::SafeV8,
        implementation_hash: Sha256Digest::from_bytes([4; 32]),
        arguments_contract_hash: Sha256Digest::from_bytes([5; 32]),
        result_contract_hash: Sha256Digest::from_bytes([6; 32]),
        capabilities: vec![Capability::NetworkHttps],
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: context.scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(activated_at),
        runtime_version: "platform-js-1".parse()?,
        artifact: ArtifactDescriptor {
            format: ArtifactFormat::SafeEsmBundleV1,
            digest: Sha256Digest::from_bytes([1; 32]),
            size_bytes: 1,
        },
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![function],
        cron_definitions: vec![CronDefinition {
            name: "minute".parse()?,
            schedule: "* * * * *".parse()?,
            function: "jobs.minute".parse()?,
            args: CanonicalValue::String("tick".to_owned()),
        }],
    };
    Ok(CronCommand::ActivateManifest {
        expected_revision,
        pinned_code: PinnedCode::Release(release_id),
        manifest_bytes: encode_release_manifest(&manifest)?,
        activated_at: TimestampMicros::new(activated_at),
    })
}

struct FailCompletionOnce {
    inner: Arc<SqlCronRepository>,
    fail: AtomicBool,
}

#[async_trait]
impl CronRepository for FailCompletionOnce {
    fn backend(&self) -> CronBackend {
        self.inner.backend()
    }

    async fn apply(
        &self,
        context: CronContext,
        operation_id: OperationId,
        command: &CronCommand,
    ) -> Result<CronCommandResult, CronError> {
        self.inner.apply(context, operation_id, command).await
    }

    async fn snapshot(&self, context: CronContext) -> Result<CronSnapshot, CronError> {
        self.inner.snapshot(context).await
    }

    async fn claim_due(
        &self,
        context: CronContext,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<Vec<ClaimedCronActivation>, CronError> {
        self.inner
            .claim_due(context, worker_id, now, lease_until, limit)
            .await
    }

    async fn complete_tick(
        &self,
        context: CronContext,
        name: &runku_releases::CronName,
        worker_id: WorkerId,
        lease_generation: u64,
        expected_tick: TimestampMicros,
        next_tick: TimestampMicros,
        completed_at: TimestampMicros,
    ) -> Result<(), CronError> {
        if self.fail.swap(false, Ordering::Relaxed) {
            return Err(CronError::LeaseLost);
        }
        self.inner
            .complete_tick(
                context,
                name,
                worker_id,
                lease_generation,
                expected_tick,
                next_tick,
                completed_at,
            )
            .await
    }

    async fn health(&self) -> Result<(), CronError> {
        self.inner.health().await
    }

    fn telemetry(&self) -> CronTelemetrySnapshot {
        self.inner.telemetry()
    }
}

#[derive(Debug, Default)]
struct RecordingRunner {
    pins: Mutex<Vec<PinnedCode>>,
}

#[async_trait]
impl ScheduledInvocationRunner for RecordingRunner {
    async fn execute(
        &self,
        _scope: EnvironmentScope,
        record: &ScheduledInvocationRecord,
    ) -> Result<(), ScheduledRunFailure> {
        if record.function.as_str() != "jobs.minute"
            || record.args != CanonicalValue::String("tick".to_owned())
            || !record
                .idempotency_key
                .as_deref()
                .is_some_and(|key| key.starts_with("cron:minute:"))
        {
            return Err(ScheduledRunFailure::internal());
        }
        self.pins
            .lock()
            .map_err(|_| ScheduledRunFailure::internal())?
            .push(record.pinned_code);
        Ok(())
    }
}

#[derive(Debug)]
struct TestClock(AtomicI64);

impl SchedulerClock for TestClock {
    fn now(&self) -> Result<TimestampMicros, ScheduledWorkerError> {
        Ok(TimestampMicros::new(self.0.load(Ordering::Relaxed)))
    }
}
