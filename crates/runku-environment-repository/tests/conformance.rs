//! Shared SQLite/PostgreSQL Environment registry conformance and adversarial coverage.

use std::{error::Error, sync::Arc};

use runku_core::{
    EnvironmentId, EnvironmentLocation, EnvironmentProtection, EnvironmentPurpose,
    EnvironmentScope, OperationId, ProjectId,
};
use runku_environment_repository::{
    EnvironmentRepositoryConfig, RepositoryRole, SqlEnvironmentRepository,
};
use runku_environments::{
    EnvironmentCommand, EnvironmentConfiguration, EnvironmentDesiredState, EnvironmentError,
    EnvironmentMaterializationOutcome, EnvironmentObservedState, EnvironmentPageRequest,
    EnvironmentRepository, EnvironmentRepositoryBackend, EnvironmentService,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;
use tokio::sync::Barrier;
use ulid::Ulid;

#[tokio::test]
async fn sqlite_conformance_reopen_and_role_rejection() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("environments.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    assert!(matches!(
        SqlEnvironmentRepository::connect_sqlite(&url, EnvironmentRepositoryConfig::PRODUCTION)
            .await,
        Err(EnvironmentError::ProductionBackendUnsupported)
    ));
    let repository =
        SqlEnvironmentRepository::connect_sqlite(&url, EnvironmentRepositoryConfig::LOCAL).await?;
    let (scope, operation_id) =
        run_conformance(&repository, EnvironmentRepositoryBackend::SQLite).await?;
    assert_concurrent_update(&repository).await?;
    repository.close().await;

    let reopened =
        SqlEnvironmentRepository::connect_sqlite(&url, EnvironmentRepositoryConfig::LOCAL).await?;
    let restored = reopened.get(scope).await?.ok_or("environment missing")?;
    assert_eq!(restored.configuration_revision, 4);
    assert!(restored.is_converged());
    assert_eq!(
        reopened
            .operation(scope, operation_id)
            .await?
            .ok_or("operation missing")?
            .configuration_revision,
        1
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn postgres_conformance_and_concurrency() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let repository =
        SqlEnvironmentRepository::connect_postgres(&url, EnvironmentRepositoryConfig::PRODUCTION)
            .await?;
    run_conformance(&repository, EnvironmentRepositoryBackend::PostgreSQL).await?;
    assert_concurrent_update(&repository).await?;
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn sqlite_rejects_migration_checksum_drift() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("migration.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let repository =
        SqlEnvironmentRepository::connect_sqlite(&url, EnvironmentRepositoryConfig::LOCAL).await?;
    repository.close().await;

    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(&url).await?;
    sqlx::query(
        "UPDATE runku_environment_schema_migrations SET checksum='tampered' WHERE version=1",
    )
    .execute(&pool)
    .await?;
    pool.close().await;
    assert!(matches!(
        SqlEnvironmentRepository::connect_sqlite(&url, EnvironmentRepositoryConfig::LOCAL).await,
        Err(EnvironmentError::Corruption)
    ));
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_conformance(
    repository: &SqlEnvironmentRepository,
    backend: EnvironmentRepositoryBackend,
) -> Result<(EnvironmentScope, OperationId), Box<dyn Error>> {
    assert_eq!(repository.backend(), backend);
    repository.health().await?;
    let project_id = ProjectId::generate();
    let scope = EnvironmentScope::new(project_id, EnvironmentId::generate());
    let service = EnvironmentService::new(Arc::new(repository.clone()));
    let create_operation = OperationId::generate();
    let initial = configuration("Production", "production", "us-east-1")?;
    let created = service
        .create(
            scope,
            create_operation,
            initial.clone(),
            TimestampMicros::new(100),
        )
        .await?;
    assert!(!created.replayed);
    assert_eq!(created.operation.configuration_revision, 1);
    assert_eq!(
        created.operation.observed_state,
        EnvironmentObservedState::Pending
    );

    let replay = service
        .create(
            scope,
            create_operation,
            initial.clone(),
            TimestampMicros::new(100),
        )
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.operation, created.operation);
    assert_eq!(
        service
            .create(
                scope,
                create_operation,
                configuration("Staging", "staging", "us-east-1")?,
                TimestampMicros::new(100),
            )
            .await,
        Err(EnvironmentError::OperationIdReused)
    );

    let wrong_project = EnvironmentScope::new(ProjectId::generate(), scope.environment_id());
    assert!(service.get(wrong_project).await?.is_none());
    assert!(
        service
            .operation(wrong_project, create_operation)
            .await?
            .is_none()
    );
    let unknown = EnvironmentScope::new(project_id, EnvironmentId::generate());
    let unknown_operation = OperationId::generate();
    assert_eq!(
        service
            .materialize(
                unknown,
                unknown_operation,
                1,
                EnvironmentMaterializationOutcome::Ready,
                TimestampMicros::new(101),
            )
            .await,
        Err(EnvironmentError::NotFound)
    );
    assert!(service.get(unknown).await?.is_none());
    assert!(
        service
            .operation(unknown, unknown_operation)
            .await?
            .is_none()
    );
    let unknown_update_operation = OperationId::generate();
    assert_eq!(
        service
            .update(
                unknown,
                unknown_update_operation,
                1,
                configuration("Unknown", "unknown", "us-east-1")?,
                TimestampMicros::new(101),
            )
            .await,
        Err(EnvironmentError::NotFound)
    );
    assert!(service.get(unknown).await?.is_none());
    assert!(
        service
            .operation(unknown, unknown_update_operation)
            .await?
            .is_none()
    );

    let materialize_operation = OperationId::generate();
    let ready = service
        .materialize(
            scope,
            materialize_operation,
            1,
            EnvironmentMaterializationOutcome::Ready,
            TimestampMicros::new(101),
        )
        .await?;
    assert!(!ready.replayed);
    assert_eq!(ready.operation.observed_configuration_revision, Some(1));
    assert!(service.get(scope).await?.ok_or("missing")?.is_converged());
    assert!(
        service
            .materialize(
                scope,
                materialize_operation,
                1,
                EnvironmentMaterializationOutcome::Ready,
                TimestampMicros::new(101),
            )
            .await?
            .replayed
    );

    let update_operation = OperationId::generate();
    let updated_configuration = configuration("Production East", "production-east", "us-east-2")?;
    let updated = service
        .update(
            scope,
            update_operation,
            1,
            updated_configuration.clone(),
            TimestampMicros::new(102),
        )
        .await?;
    assert_eq!(updated.operation.configuration_revision, 2);
    assert_eq!(
        updated.operation.observed_state,
        EnvironmentObservedState::Pending
    );
    assert_eq!(updated.operation.observed_configuration_revision, Some(1));
    let stale_operation = OperationId::generate();
    assert_eq!(
        service
            .update(
                scope,
                stale_operation,
                1,
                configuration("Stale", "stale", "us-west-1")?,
                TimestampMicros::new(103),
            )
            .await,
        Err(EnvironmentError::Conflict)
    );

    assert!(service.operation(scope, stale_operation).await?.is_none());

    service
        .materialize(
            scope,
            OperationId::generate(),
            2,
            EnvironmentMaterializationOutcome::Failed,
            TimestampMicros::new(103),
        )
        .await?;
    service
        .materialize(
            scope,
            OperationId::generate(),
            2,
            EnvironmentMaterializationOutcome::Ready,
            TimestampMicros::new(104),
        )
        .await?;
    let loaded = service.get(scope).await?.ok_or("environment missing")?;
    assert_eq!(loaded.configuration, updated_configuration);
    assert!(loaded.is_converged());
    assert_eq!(
        service
            .materialize(
                scope,
                OperationId::generate(),
                2,
                EnvironmentMaterializationOutcome::Ready,
                TimestampMicros::new(105),
            )
            .await,
        Err(EnvironmentError::Conflict)
    );

    let archive_operation = OperationId::generate();
    let archived = service
        .archive(scope, archive_operation, 2, TimestampMicros::new(106))
        .await?;
    assert_eq!(archived.operation.kind.as_str(), "archive");
    assert_eq!(archived.operation.configuration_revision, 3);
    assert_eq!(
        archived.operation.desired_state,
        EnvironmentDesiredState::Archived
    );
    assert!(
        service
            .archive(scope, archive_operation, 2, TimestampMicros::new(106))
            .await?
            .replayed
    );
    service
        .materialize(
            scope,
            OperationId::generate(),
            3,
            EnvironmentMaterializationOutcome::Ready,
            TimestampMicros::new(107),
        )
        .await?;
    assert!(
        service
            .get(scope)
            .await?
            .ok_or("archived missing")?
            .is_converged()
    );
    let restored = service
        .restore(scope, OperationId::generate(), 3, TimestampMicros::new(108))
        .await?;
    assert_eq!(restored.operation.kind.as_str(), "restore");
    assert_eq!(restored.operation.configuration_revision, 4);
    assert_eq!(
        restored.operation.desired_state,
        EnvironmentDesiredState::Active
    );
    service
        .materialize(
            scope,
            OperationId::generate(),
            4,
            EnvironmentMaterializationOutcome::Ready,
            TimestampMicros::new(109),
        )
        .await?;

    let same_slug = EnvironmentScope::new(project_id, EnvironmentId::generate());
    assert_eq!(
        service
            .create(
                same_slug,
                OperationId::generate(),
                configuration("Duplicate", "production-east", "eu-west-1")?,
                TimestampMicros::new(110),
            )
            .await,
        Err(EnvironmentError::Conflict)
    );
    assert!(service.get(same_slug).await?.is_none());
    let other_project_scope =
        EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    service
        .create(
            other_project_scope,
            OperationId::generate(),
            configuration("Same slug, other project", "production-east", "eu-west-1")?,
            TimestampMicros::new(110),
        )
        .await?;

    let page_project = ProjectId::generate();
    let page_scopes = [1_u128, 2, 3].map(|value| {
        EnvironmentScope::new(page_project, EnvironmentId::from_ulid(Ulid::from(value)))
    });
    for (index, page_scope) in page_scopes.into_iter().enumerate() {
        service
            .create(
                page_scope,
                OperationId::generate(),
                configuration(
                    &format!("Page {index}"),
                    &format!("page-{index}"),
                    "eu-central-1",
                )?,
                TimestampMicros::new(120 + i64::try_from(index)?),
            )
            .await?;
    }
    let page = service
        .list(page_project, EnvironmentPageRequest::new(None, 2)?)
        .await?;
    assert_eq!(page.project_id, page_project);
    assert_eq!(page.environments.len(), 2);
    let cursor = page.next.ok_or("missing continuation cursor")?;
    assert_eq!(cursor, page.environments[1].scope.environment_id());
    let final_page = service
        .list(page_project, EnvironmentPageRequest::new(Some(cursor), 2)?)
        .await?;
    assert_eq!(final_page.environments.len(), 1);
    assert!(final_page.next.is_none());
    assert!(
        service
            .list(
                ProjectId::generate(),
                EnvironmentPageRequest::new(None, 10)?
            )
            .await?
            .environments
            .is_empty()
    );
    assert_eq!(
        repository
            .list(
                project_id,
                EnvironmentPageRequest {
                    after: None,
                    limit: 0,
                },
            )
            .await,
        Err(EnvironmentError::LimitExceeded)
    );

    let telemetry = repository.telemetry();
    assert!(telemetry.commands >= 6);
    assert!(telemetry.replays >= 2);
    assert!(telemetry.conflicts >= 4);
    assert!(telemetry.reads >= 4);
    assert!(telemetry.lists >= 2);
    assert!(telemetry.operation_reads >= 3);
    assert!(telemetry.pool_size >= 1);
    Ok((scope, create_operation))
}

async fn assert_concurrent_update(
    repository: &SqlEnvironmentRepository,
) -> Result<(), Box<dyn Error>> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    repository
        .apply(
            scope,
            OperationId::generate(),
            &EnvironmentCommand::Create {
                configuration: configuration("Concurrent", "concurrent", "us-east-1")?,
                created_at: TimestampMicros::new(200),
            },
        )
        .await?;
    let barrier = Arc::new(Barrier::new(3));
    let mut tasks = Vec::new();
    for (name, slug) in [
        ("Concurrent A", "concurrent-a"),
        ("Concurrent B", "concurrent-b"),
    ] {
        let repository = repository.clone();
        let barrier = Arc::clone(&barrier);
        let configuration = configuration(name, slug, "us-west-1")?;
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repository
                .apply(
                    scope,
                    OperationId::generate(),
                    &EnvironmentCommand::Update {
                        expected_revision: 1,
                        configuration,
                        updated_at: TimestampMicros::new(201),
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
            Err(EnvironmentError::Conflict | EnvironmentError::Busy) => conflicts += 1,
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!((successes, conflicts), (1, 1));
    assert_eq!(
        repository
            .get(scope)
            .await?
            .ok_or("concurrent environment missing")?
            .configuration_revision,
        2
    );
    Ok(())
}

fn configuration(
    name: &str,
    slug: &str,
    region: &str,
) -> Result<EnvironmentConfiguration, EnvironmentError> {
    Ok(EnvironmentConfiguration {
        name: name.parse()?,
        slug: slug.parse()?,
        region: region.parse()?,
        purpose: EnvironmentPurpose::Production,
        protection: EnvironmentProtection::Production,
        location: EnvironmentLocation::SelfHosted,
        workspace_targets_enabled: false,
    })
}

#[test]
fn deterministic_ids_keep_page_order_stable() {
    let first = EnvironmentId::from_ulid(Ulid::from(1_u128));
    let second = EnvironmentId::from_ulid(Ulid::from(2_u128));
    assert!(first < second);
}

#[test]
fn local_role_cannot_open_postgres() {
    let config = EnvironmentRepositoryConfig {
        role: RepositoryRole::Local,
        ..EnvironmentRepositoryConfig::LOCAL
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let Ok(runtime) = runtime else {
        return;
    };
    assert!(matches!(
        runtime.block_on(SqlEnvironmentRepository::connect_postgres(
            "postgres://localhost/runku",
            config,
        )),
        Err(EnvironmentError::ProductionBackendUnsupported)
    ));
}
