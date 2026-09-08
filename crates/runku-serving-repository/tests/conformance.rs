//! Shared SQLite/PostgreSQL serving-policy conformance and adversarial coverage.

use std::{error::Error, sync::Arc};

use runku_core::{
    BuildId, EnvironmentId, EnvironmentScope, FunctionId, OperationId, OperatorId, ProjectId,
    ReleaseId,
};
use runku_releases::{
    ArtifactDescriptor, ArtifactFormat, AuthPolicy, Capability, FunctionManifest, FunctionType,
    FunctionVisibility, ReleaseManifestV1, RuntimeClass, Sha256Digest,
};
use runku_serving::{
    ServingAuditPageRequest, ServingCommand, ServingMaterializationOutcome, ServingMode,
    ServingObservedState, ServingPolicy, ServingPolicyError, ServingPolicyRepository,
    ServingPolicyService, ServingRepositoryBackend,
};
use runku_serving_repository::{
    ServingRepositoryConfig, ServingRepositoryRole, SqlServingPolicyRepository,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;
use tokio::sync::Barrier;
use ulid::Ulid;

#[tokio::test]
async fn sqlite_conformance_reopen_and_role_rejection() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("serving.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    assert!(matches!(
        SqlServingPolicyRepository::connect_sqlite(&url, ServingRepositoryConfig::PRODUCTION).await,
        Err(ServingPolicyError::ProductionBackendUnsupported)
    ));
    let repository =
        SqlServingPolicyRepository::connect_sqlite(&url, ServingRepositoryConfig::LOCAL).await?;
    let (scope, create_operation) =
        run_conformance(&repository, ServingRepositoryBackend::SQLite).await?;
    assert_concurrent_cas(&repository).await?;
    repository.close().await;

    let reopened =
        SqlServingPolicyRepository::connect_sqlite(&url, ServingRepositoryConfig::LOCAL).await?;
    let restored = reopened.get(scope).await?.ok_or("policy missing")?;
    assert_eq!(restored.policy_revision, 2);
    assert!(restored.is_converged());
    assert!(reopened.operation(scope, create_operation).await?.is_some());
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn postgres_conformance_and_concurrency() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let repository =
        SqlServingPolicyRepository::connect_postgres(&url, ServingRepositoryConfig::PRODUCTION)
            .await?;
    run_conformance(&repository, ServingRepositoryBackend::PostgreSQL).await?;
    assert_concurrent_cas(&repository).await?;
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn sqlite_rejects_migration_checksum_drift() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("migration.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let repository =
        SqlServingPolicyRepository::connect_sqlite(&url, ServingRepositoryConfig::LOCAL).await?;
    repository.close().await;

    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(&url).await?;
    sqlx::query("UPDATE runku_serving_schema_migrations SET checksum='tampered' WHERE version=1")
        .execute(&pool)
        .await?;
    pool.close().await;
    assert!(matches!(
        SqlServingPolicyRepository::connect_sqlite(&url, ServingRepositoryConfig::LOCAL).await,
        Err(ServingPolicyError::Corruption)
    ));
    Ok(())
}

#[tokio::test]
async fn sqlite_rejects_tampered_weighted_set() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("tampered-policy.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let repository =
        SqlServingPolicyRepository::connect_sqlite(&url, ServingRepositoryConfig::LOCAL).await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let manifest = manifest(scope.project_id(), 51, 10, 11)?;
    repository
        .apply(
            scope,
            OperationId::generate(),
            &ServingCommand::SetDesired {
                actor: OperatorId::generate(),
                expected_revision: None,
                policy: ServingPolicy::from_manifests(
                    scope,
                    ServingMode::Atomic,
                    [(&manifest, 100)],
                )?,
                changed_at: TimestampMicros::new(10),
            },
        )
        .await?;
    repository.close().await;

    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(&url).await?;
    sqlx::query("UPDATE runku_serving_releases SET weight_percent=99")
        .execute(&pool)
        .await?;
    pool.close().await;
    let reopened =
        SqlServingPolicyRepository::connect_sqlite(&url, ServingRepositoryConfig::LOCAL).await?;
    assert_eq!(
        reopened.get(scope).await,
        Err(ServingPolicyError::Corruption)
    );
    reopened.close().await;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_conformance(
    repository: &SqlServingPolicyRepository,
    backend: ServingRepositoryBackend,
) -> Result<(EnvironmentScope, OperationId), Box<dyn Error>> {
    assert_eq!(repository.backend(), backend);
    repository.health().await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let service = ServingPolicyService::new(Arc::new(repository.clone()));
    let r1 = manifest(scope.project_id(), 1, 10, 11)?;
    let r2 = manifest(scope.project_id(), 2, 10, 11)?;
    let actor = OperatorId::generate();
    let initial = ServingPolicy::from_manifests(scope, ServingMode::Atomic, [(&r1, 100)])?;
    let create_operation = OperationId::generate();
    let created = service
        .set_desired(
            scope,
            create_operation,
            actor,
            None,
            initial.clone(),
            TimestampMicros::new(100),
        )
        .await?;
    assert!(!created.replayed);
    assert_eq!(created.operation.policy_revision, 1);
    assert_eq!(
        created.operation.observed_state,
        ServingObservedState::Pending
    );

    let replay = service
        .set_desired(
            scope,
            create_operation,
            actor,
            None,
            initial.clone(),
            TimestampMicros::new(100),
        )
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.operation, created.operation);
    let alternate = ServingPolicy::from_manifests(scope, ServingMode::Atomic, [(&r2, 100)])?;
    assert_eq!(
        service
            .set_desired(
                scope,
                create_operation,
                actor,
                None,
                alternate,
                TimestampMicros::new(100),
            )
            .await,
        Err(ServingPolicyError::OperationIdReused)
    );

    let wrong_environment = EnvironmentScope::new(scope.project_id(), EnvironmentId::generate());
    let wrong_project = EnvironmentScope::new(ProjectId::generate(), scope.environment_id());
    for wrong_scope in [wrong_environment, wrong_project] {
        assert!(service.get(wrong_scope).await?.is_none());
        assert!(
            service
                .operation(wrong_scope, create_operation)
                .await?
                .is_none()
        );
        assert!(
            service
                .audit(wrong_scope, ServingAuditPageRequest::new(None, 10)?)
                .await?
                .events
                .is_empty()
        );
    }
    let wrong_scope = wrong_environment;
    let unknown_operation = OperationId::generate();
    assert_eq!(
        service
            .materialize(
                wrong_scope,
                unknown_operation,
                1,
                ServingMaterializationOutcome::Ready,
                TimestampMicros::new(101),
            )
            .await,
        Err(ServingPolicyError::NotFound)
    );
    assert!(service.get(wrong_scope).await?.is_none());
    assert!(
        service
            .operation(wrong_scope, unknown_operation)
            .await?
            .is_none()
    );

    let materialize_operation = OperationId::generate();
    service
        .materialize(
            scope,
            materialize_operation,
            1,
            ServingMaterializationOutcome::Ready,
            TimestampMicros::new(101),
        )
        .await?;
    assert!(
        service
            .get(scope)
            .await?
            .ok_or("policy missing")?
            .is_converged()
    );

    let schema_variant = manifest(scope.project_id(), 3, 12, 11)?;
    assert!(
        ServingPolicy::from_manifests(
            scope,
            ServingMode::Gradual,
            [(&r1, 50), (&schema_variant, 50)]
        )
        .is_ok()
    );
    let incompatible = manifest(scope.project_id(), 4, 12, 13)?;
    assert_eq!(
        ServingPolicy::from_manifests(
            scope,
            ServingMode::Gradual,
            [(&r1, 50), (&incompatible, 50)]
        ),
        Err(ServingPolicyError::IncompatibleContracts)
    );
    assert_eq!(
        service
            .get(scope)
            .await?
            .ok_or("policy missing")?
            .policy_revision,
        1
    );

    let gradual =
        ServingPolicy::from_manifests(scope, ServingMode::Gradual, [(&r1, 80), (&r2, 20)])?;
    let update_operation = OperationId::generate();
    let updated = service
        .set_desired(
            scope,
            update_operation,
            actor,
            Some(1),
            gradual.clone(),
            TimestampMicros::new(102),
        )
        .await?;
    assert_eq!(updated.operation.policy_revision, 2);
    assert_eq!(
        updated.operation.observed_state,
        ServingObservedState::Pending
    );
    assert_eq!(updated.operation.observed_policy_revision, Some(1));
    assert_eq!(
        service
            .set_desired(
                scope,
                OperationId::generate(),
                actor,
                Some(1),
                gradual.clone(),
                TimestampMicros::new(103),
            )
            .await,
        Err(ServingPolicyError::Conflict)
    );
    assert_eq!(
        service
            .set_desired(
                scope,
                OperationId::generate(),
                actor,
                Some(2),
                gradual,
                TimestampMicros::new(103),
            )
            .await,
        Err(ServingPolicyError::InvalidInput)
    );

    service
        .materialize(
            scope,
            OperationId::generate(),
            2,
            ServingMaterializationOutcome::Failed,
            TimestampMicros::new(103),
        )
        .await?;
    service
        .materialize(
            scope,
            OperationId::generate(),
            2,
            ServingMaterializationOutcome::Ready,
            TimestampMicros::new(104),
        )
        .await?;
    let loaded = service.get(scope).await?.ok_or("policy missing")?;
    assert_eq!(loaded.policy_revision, 2);
    assert!(loaded.is_converged());

    let first_audit = service
        .audit(scope, ServingAuditPageRequest::new(None, 2)?)
        .await?;
    assert_eq!(first_audit.events.len(), 2);
    let cursor = first_audit.next.ok_or("audit cursor missing")?;
    let final_audit = service
        .audit(scope, ServingAuditPageRequest::new(Some(cursor), 10)?)
        .await?;
    assert_eq!(final_audit.events.len(), 3);
    assert!(final_audit.next.is_none());
    assert_eq!(first_audit.events[0].previous_policy_revision, None);
    assert_eq!(first_audit.events[0].actor_operator_id, Some(actor));
    assert_eq!(first_audit.events[1].previous_policy_revision, Some(1));
    assert_eq!(first_audit.events[1].actor_operator_id, None);
    assert_eq!(final_audit.events[0].policy_revision, 2);
    assert_eq!(final_audit.events[0].actor_operator_id, Some(actor));
    assert_eq!(
        repository
            .audit(
                scope,
                ServingAuditPageRequest {
                    after: None,
                    limit: 0,
                }
            )
            .await,
        Err(ServingPolicyError::LimitExceeded)
    );

    let telemetry = repository.telemetry();
    assert!(telemetry.commands >= 5);
    assert!(telemetry.replays >= 1);
    assert!(telemetry.conflicts >= 2);
    assert!(telemetry.reads >= 5);
    assert!(telemetry.operation_reads >= 2);
    assert!(telemetry.audit_reads >= 3);
    assert!(telemetry.pool_size >= 1);
    Ok((scope, create_operation))
}

async fn assert_concurrent_cas(
    repository: &SqlServingPolicyRepository,
) -> Result<(), Box<dyn Error>> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let initial_manifest = manifest(scope.project_id(), 11, 21, 22)?;
    repository
        .apply(
            scope,
            OperationId::generate(),
            &ServingCommand::SetDesired {
                actor: OperatorId::generate(),
                expected_revision: None,
                policy: ServingPolicy::from_manifests(
                    scope,
                    ServingMode::Atomic,
                    [(&initial_manifest, 100)],
                )?,
                changed_at: TimestampMicros::new(200),
            },
        )
        .await?;
    let barrier = Arc::new(Barrier::new(3));
    let mut tasks = Vec::new();
    for seed in [12_u8, 13] {
        let repository = repository.clone();
        let barrier = Arc::clone(&barrier);
        let candidate = manifest(scope.project_id(), u128::from(seed), 21, 22)?;
        let policy =
            ServingPolicy::from_manifests(scope, ServingMode::Atomic, [(&candidate, 100)])?;
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repository
                .apply(
                    scope,
                    OperationId::generate(),
                    &ServingCommand::SetDesired {
                        actor: OperatorId::generate(),
                        expected_revision: Some(1),
                        policy,
                        changed_at: TimestampMicros::new(201),
                    },
                )
                .await
        }));
    }
    barrier.wait().await;
    let mut successes = 0;
    let mut conflicts = 0;
    for task in tasks {
        match task.await? {
            Ok(_) => successes += 1,
            Err(ServingPolicyError::Conflict | ServingPolicyError::Busy) => conflicts += 1,
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!((successes, conflicts), (1, 1));
    assert_eq!(
        repository
            .get(scope)
            .await?
            .ok_or("concurrent policy missing")?
            .policy_revision,
        2
    );
    assert_eq!(
        repository
            .audit(scope, ServingAuditPageRequest::new(None, 10)?)
            .await?
            .events
            .len(),
        2
    );
    Ok(())
}

fn manifest(
    project_id: ProjectId,
    seed: u128,
    schema: u8,
    indexes: u8,
) -> Result<ReleaseManifestV1, Box<dyn Error>> {
    let seed_byte = u8::try_from(seed)?;
    Ok(ReleaseManifestV1 {
        release_id: ReleaseId::from_ulid(Ulid::from(seed)),
        project_id,
        build_id: BuildId::from_ulid(Ulid::from(seed + 1_000)),
        created_at: TimestampMicros::new(i64::try_from(seed)?),
        runtime_version: "runku-js-1".parse()?,
        artifact: ArtifactDescriptor {
            format: ArtifactFormat::SafeEsmBundleV1,
            digest: Sha256Digest::from_bytes([seed_byte; 32]),
            size_bytes: 1024,
        },
        function_contract_hash: Sha256Digest::from_bytes([seed_byte.saturating_add(1); 32]),
        schema_contract_hash: Sha256Digest::from_bytes([schema; 32]),
        index_contract_hash: Sha256Digest::from_bytes([indexes; 32]),
        functions: vec![FunctionManifest {
            id: FunctionId::from_ulid(Ulid::from(seed + 2_000)),
            name: "tasks.run".parse()?,
            function_type: FunctionType::Mutation,
            visibility: FunctionVisibility::Internal,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::from_bytes([seed_byte.saturating_add(2); 32]),
            arguments_contract_hash: Sha256Digest::from_bytes([31; 32]),
            result_contract_hash: Sha256Digest::from_bytes([32; 32]),
            capabilities: vec![Capability::DbRead, Capability::DbWrite],
        }],
        cron_definitions: Vec::new(),
    })
}

#[test]
fn local_role_cannot_open_postgres() {
    let config = ServingRepositoryConfig {
        role: ServingRepositoryRole::Local,
        ..ServingRepositoryConfig::LOCAL
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let Ok(runtime) = runtime else {
        return;
    };
    assert!(matches!(
        runtime.block_on(SqlServingPolicyRepository::connect_postgres(
            "postgres://localhost/runku",
            config,
        )),
        Err(ServingPolicyError::ProductionBackendUnsupported)
    ));
}
