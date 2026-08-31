//! Shared SQLite/PostgreSQL Release Repository behavior.

use std::{error::Error, sync::Arc};

use runku_core::{
    BuildId, ChannelName, CodeTarget, EnvironmentId, EnvironmentScope, FunctionId, OperationId,
    ProjectId, ReleaseId,
};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{
    ArtifactDescriptor, ArtifactFormat, AuthPolicy, Capability, FunctionManifest, FunctionType,
    FunctionVisibility, ReleaseCommand, ReleaseError, ReleaseManifestV1, ReleaseRepository,
    ReleaseRepositoryBackend, ReleaseRouter, ReleaseStatus, RuntimeClass, Sha256Digest,
    encode_release_manifest,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;
use tokio::sync::Barrier;
use ulid::Ulid;

#[tokio::test]
async fn sqlite_conformance_reopen_and_role_rejection() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("releases.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    assert!(matches!(
        SqlReleaseRepository::connect_sqlite(&url, RepositoryConfig::PRODUCTION).await,
        Err(ReleaseError::ProductionBackendUnsupported)
    ));
    let repository = SqlReleaseRepository::connect_sqlite(&url, RepositoryConfig::LOCAL).await?;
    let scope = run_conformance(&repository, ReleaseRepositoryBackend::SQLite).await?;
    repository.close().await;
    let reopened = SqlReleaseRepository::connect_sqlite(&url, RepositoryConfig::LOCAL).await?;
    assert_eq!(reopened.snapshot(scope).await?.revision(), 17);
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn postgres_conformance() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let repository =
        SqlReleaseRepository::connect_postgres(&url, RepositoryConfig::PRODUCTION).await?;
    run_conformance(&repository, ReleaseRepositoryBackend::PostgreSQL).await?;
    assert_concurrent_promotion(&repository).await?;
    repository.close().await;
    Ok(())
}

async fn assert_concurrent_promotion(
    repository: &SqlReleaseRepository,
) -> Result<(), Box<dyn Error>> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let r1 = ReleaseId::generate();
    let r2 = ReleaseId::generate();
    repository
        .apply(scope, OperationId::generate(), &register(scope, r1, 11)?)
        .await?;
    transition_to_servable(repository, scope, r1, 1).await?;
    repository
        .apply(scope, OperationId::generate(), &register(scope, r2, 21)?)
        .await?;
    transition_to_servable(repository, scope, r2, 6).await?;
    let stable: ChannelName = "stable".parse()?;
    repository
        .apply(
            scope,
            OperationId::generate(),
            &ReleaseCommand::SetChannel {
                channel: stable.clone(),
                expected_release: None,
                target_release: Some(r1),
            },
        )
        .await?;
    let barrier = Arc::new(Barrier::new(3));
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let task_repository = repository.clone();
        let task_barrier = Arc::clone(&barrier);
        let channel = stable.clone();
        tasks.push(tokio::spawn(async move {
            task_barrier.wait().await;
            task_repository
                .apply(
                    scope,
                    OperationId::generate(),
                    &ReleaseCommand::SetChannel {
                        channel,
                        expected_release: Some(r1),
                        target_release: Some(r2),
                    },
                )
                .await
        }));
    }
    barrier.wait().await;
    let mut successes = 0;
    let mut failures = 0;
    for task in tasks {
        match task.await? {
            Ok(_) => successes += 1,
            Err(ReleaseError::RepositoryConflict | ReleaseError::Busy) => failures += 1,
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!((successes, failures), (1, 1));
    let router = ReleaseRouter::new(repository.snapshot(scope).await?);
    assert_eq!(router.resolve(&CodeTarget::Channel(stable))?.release_id, r2);
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_conformance(
    repository: &dyn ReleaseRepository,
    backend: ReleaseRepositoryBackend,
) -> Result<EnvironmentScope, Box<dyn Error>> {
    assert_eq!(repository.backend(), backend);
    repository.health().await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let r1 = ReleaseId::generate();
    let r2 = ReleaseId::generate();
    let register_r1 = register(scope, r1, 1)?;
    let register_operation = OperationId::generate();
    let first = repository
        .apply(scope, register_operation, &register_r1)
        .await?;
    assert_eq!(first.serving_revision, 1);
    assert!(!first.replayed);
    let replay = repository
        .apply(scope, register_operation, &register_r1)
        .await?;
    assert_eq!(replay.serving_revision, 1);
    assert!(replay.replayed);
    assert_eq!(
        repository
            .apply(scope, register_operation, &register(scope, r2, 2)?)
            .await,
        Err(ReleaseError::OperationIdReused)
    );
    let loaded_manifest = repository.manifest(scope, r1).await?;
    assert_eq!(loaded_manifest.release_id, r1);
    assert_eq!(loaded_manifest.project_id, scope.project_id());
    assert_eq!(
        repository.manifest(scope, ReleaseId::generate()).await,
        Err(ReleaseError::ReleaseNotFound)
    );

    transition_to_servable(repository, scope, r1, 1).await?;
    assert_eq!(
        repository
            .apply(scope, OperationId::generate(), &register(scope, r2, 2)?)
            .await?
            .serving_revision,
        6
    );
    transition_to_servable(repository, scope, r2, 6).await?;

    let stable: ChannelName = "stable".parse()?;
    assert_eq!(
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::SetChannel {
                    channel: stable.clone(),
                    expected_release: None,
                    target_release: Some(r1),
                }
            )
            .await?
            .serving_revision,
        11
    );
    assert_eq!(
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::SetDefaultChannel {
                    expected_channel: None,
                    target_channel: Some(stable.clone()),
                }
            )
            .await?
            .serving_revision,
        12
    );
    let router = ReleaseRouter::new(repository.snapshot(scope).await?);
    assert_eq!(router.resolve_default()?.release_id, r1);
    assert_eq!(router.resolve(&CodeTarget::Release(r2))?.release_id, r2);

    assert_eq!(
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::SetChannel {
                    channel: stable.clone(),
                    expected_release: Some(r1),
                    target_release: Some(r2),
                }
            )
            .await?
            .serving_revision,
        13
    );
    let conflicted = ReleaseCommand::SetChannel {
        channel: stable.clone(),
        expected_release: Some(r1),
        target_release: Some(r2),
    };
    assert_eq!(
        repository
            .apply(scope, OperationId::generate(), &conflicted)
            .await,
        Err(ReleaseError::RepositoryConflict)
    );
    assert_eq!(repository.snapshot(scope).await?.revision(), 13);
    assert_eq!(
        ReleaseRouter::new(repository.snapshot(scope).await?)
            .resolve_default()?
            .release_id,
        r2
    );

    assert_eq!(
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::SetChannel {
                    channel: stable.clone(),
                    expected_release: Some(r2),
                    target_release: Some(r1),
                }
            )
            .await?
            .serving_revision,
        14
    );
    assert_eq!(
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::SetChannel {
                    channel: stable.clone(),
                    expected_release: Some(r1),
                    target_release: None,
                },
            )
            .await,
        Err(ReleaseError::RepositoryConflict)
    );
    assert_eq!(repository.snapshot(scope).await?.revision(), 14);
    assert_eq!(
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::SetDefaultChannel {
                    expected_channel: Some(stable.clone()),
                    target_channel: None,
                }
            )
            .await?
            .serving_revision,
        15
    );
    assert_eq!(
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::SetChannel {
                    channel: stable,
                    expected_release: Some(r1),
                    target_release: None,
                }
            )
            .await?
            .serving_revision,
        16
    );
    assert_eq!(
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::Transition {
                    release_id: r1,
                    expected: ReleaseStatus::Servable,
                    next: ReleaseStatus::Deprecated,
                }
            )
            .await?
            .serving_revision,
        17
    );
    assert!(matches!(
        repository
            .snapshot(EnvironmentScope::new(
                scope.project_id(),
                EnvironmentId::generate()
            ))
            .await,
        Err(ReleaseError::ReleaseNotFound)
    ));
    let telemetry = repository.telemetry();
    assert_eq!(telemetry.commands, 17);
    assert!(telemetry.replays >= 1);
    assert!(telemetry.conflicts >= 1);
    assert!(telemetry.snapshots >= 3);
    assert!(telemetry.pool_size >= 1);
    Ok(scope)
}

async fn transition_to_servable(
    repository: &dyn ReleaseRepository,
    scope: EnvironmentScope,
    release_id: ReleaseId,
    starting_revision: u64,
) -> Result<(), ReleaseError> {
    let transitions = [
        (ReleaseStatus::Created, ReleaseStatus::Building),
        (ReleaseStatus::Building, ReleaseStatus::Validating),
        (ReleaseStatus::Validating, ReleaseStatus::Ready),
        (ReleaseStatus::Ready, ReleaseStatus::Servable),
    ];
    for (offset, (expected, next)) in transitions.into_iter().enumerate() {
        let result = repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::Transition {
                    release_id,
                    expected,
                    next,
                },
            )
            .await?;
        assert_eq!(
            result.serving_revision,
            starting_revision + u64::try_from(offset).map_err(|_| ReleaseError::Internal)? + 1
        );
    }
    Ok(())
}

fn register(
    scope: EnvironmentScope,
    release_id: ReleaseId,
    seed: u8,
) -> Result<ReleaseCommand, Box<dyn Error>> {
    let function = FunctionManifest {
        id: FunctionId::from_ulid(Ulid::from(u128::from(seed))),
        name: format!("function{seed}").parse()?,
        function_type: FunctionType::Query,
        visibility: FunctionVisibility::Public,
        auth_policy: AuthPolicy::None,
        runtime_class: RuntimeClass::SafeV8,
        implementation_hash: Sha256Digest::from_bytes([seed; 32]),
        arguments_contract_hash: Sha256Digest::from_bytes([seed + 1; 32]),
        result_contract_hash: Sha256Digest::from_bytes([seed + 2; 32]),
        capabilities: vec![Capability::DbRead],
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(i64::from(seed)),
        runtime_version: "platform-js-1".parse()?,
        artifact: ArtifactDescriptor {
            format: ArtifactFormat::SafeEsmBundleV1,
            digest: Sha256Digest::from_bytes([seed; 32]),
            size_bytes: 1024,
        },
        function_contract_hash: Sha256Digest::from_bytes([seed + 3; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([seed + 4; 32]),
        index_contract_hash: Sha256Digest::from_bytes([seed + 5; 32]),
        functions: vec![function],
        cron_definitions: Vec::new(),
    };
    Ok(ReleaseCommand::Register {
        manifest_bytes: encode_release_manifest(&manifest)?,
    })
}
