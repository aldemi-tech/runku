//! Shared `SQLite`/`PostgreSQL` Development Repository conformance.

use std::{error::Error, sync::Arc};

use runku_core::{
    BuildId, DevRevisionId, EnvironmentDescriptor, EnvironmentId, EnvironmentLocation,
    EnvironmentScope, FunctionId, OperationId, ProjectId, ReleaseId, WorkspaceId,
};
use runku_development::{
    DevelopmentActor, DevelopmentBackend, DevelopmentCommand, DevelopmentContext, DevelopmentError,
    DevelopmentRepository, DevelopmentRepositoryConfig, SqlDevelopmentRepository,
};
use runku_releases::{
    ArtifactDescriptor, ArtifactFormat, AuthPolicy, Capability, FunctionManifest, FunctionType,
    FunctionVisibility, ReleaseManifestV1, RuntimeClass, Sha256Digest, encode_release_manifest,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;
use tokio::sync::Barrier;

#[tokio::test]
async fn sqlite_conformance_reopens_and_policy_precedes_file_creation() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let denied_path = directory.path().join("denied.sqlite3");
    let denied_url = format!("sqlite://{}?mode=rwc", denied_path.display());
    let production_environment = EnvironmentId::generate();
    let denied = DevelopmentContext {
        scope: EnvironmentScope::new(ProjectId::generate(), production_environment),
        environment: EnvironmentDescriptor::production(
            production_environment,
            EnvironmentLocation::SelfHosted,
        ),
    };
    assert!(matches!(
        SqlDevelopmentRepository::connect_sqlite(
            &denied_url,
            DevelopmentRepositoryConfig::LOCAL,
            denied,
        )
        .await,
        Err(DevelopmentError::PolicyDenied)
    ));
    assert!(!denied_path.exists());

    let path = directory.path().join("development.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let context = local_context();
    let repository =
        SqlDevelopmentRepository::connect_sqlite(&url, DevelopmentRepositoryConfig::LOCAL, context)
            .await?;
    run_conformance(&repository, context, DevelopmentBackend::SQLite).await?;
    repository.close().await;

    let reopened =
        SqlDevelopmentRepository::connect_sqlite(&url, DevelopmentRepositoryConfig::LOCAL, context)
            .await?;
    assert!(reopened.snapshot(context).await?.revision() >= 4);
    reopened.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_conformance_and_concurrent_cas() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let context = local_context();
    let repository = SqlDevelopmentRepository::connect_postgres(
        &url,
        DevelopmentRepositoryConfig::AUTHORITATIVE,
        context,
    )
    .await?;
    run_conformance(&repository, context, DevelopmentBackend::PostgreSQL).await?;
    repository.close().await;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_conformance(
    repository: &SqlDevelopmentRepository,
    context: DevelopmentContext,
    backend: DevelopmentBackend,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(repository.backend(), backend);
    repository.health().await?;
    let actor: DevelopmentActor = "manuel.local".parse()?;
    let first_workspace = WorkspaceId::generate();
    let create = DevelopmentCommand::CreateWorkspace {
        workspace_id: first_workspace,
        workspace_ref: "dev/manuel/feature".parse()?,
        actor: actor.clone(),
        created_at: TimestampMicros::new(100),
    };
    let create_operation = OperationId::generate();
    let first = repository.apply(context, create_operation, &create).await?;
    assert_eq!(first.serving_revision, 1);
    assert!(!first.replayed);
    assert_eq!(first.head_revision, None);
    let replay = repository.apply(context, create_operation, &create).await?;
    assert!(replay.replayed);
    assert_eq!(replay.serving_revision, 1);

    let second_create = DevelopmentCommand::CreateWorkspace {
        workspace_id: WorkspaceId::generate(),
        workspace_ref: "dev/ana/fix".parse()?,
        actor: "ana".parse()?,
        created_at: TimestampMicros::new(101),
    };
    assert_eq!(
        repository
            .apply(context, create_operation, &second_create)
            .await,
        Err(DevelopmentError::Conflict)
    );
    repository
        .apply(context, OperationId::generate(), &second_create)
        .await?;

    let revision_one = revision(context, actor.clone(), 1)?;
    let publish_one = DevelopmentCommand::PublishRevision {
        workspace_ref: "dev/manuel/feature".parse()?,
        expected_head: None,
        revision: revision_one.clone(),
    };
    repository
        .apply(context, OperationId::generate(), &publish_one)
        .await?;
    let stable_snapshot = repository.snapshot(context).await?;
    let resolved = stable_snapshot.resolve(&"dev/manuel/feature".parse()?)?;
    assert_eq!(resolved.revision, revision_one);
    assert_eq!(
        resolved.pinned_code().to_string(),
        format!("dev_revision:{}", revision_one.revision_id)
    );
    assert_eq!(resolved.manifest.release_id, revision_one.release_id);
    assert_eq!(
        stable_snapshot.resolve(&"dev/ana/fix".parse()?),
        Err(DevelopmentError::WorkspaceEmpty)
    );

    let wrong_environment = EnvironmentId::generate();
    let wrong_context = DevelopmentContext {
        scope: EnvironmentScope::new(context.scope.project_id(), wrong_environment),
        environment: EnvironmentDescriptor::local_development(wrong_environment),
    };
    assert_eq!(
        repository.snapshot(wrong_context).await,
        Err(DevelopmentError::PolicyDenied)
    );

    let next_a = revision(context, actor.clone(), 2)?;
    let next_b = revision(context, actor, 3)?;
    let barrier = Arc::new(Barrier::new(3));
    let mut tasks = Vec::new();
    for next in [next_a.clone(), next_b.clone()] {
        let repository = repository.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            let command = DevelopmentCommand::PublishRevision {
                workspace_ref: "dev/manuel/feature"
                    .parse()
                    .map_err(|_| DevelopmentError::InvalidInput)?,
                expected_head: Some(revision_one.revision_id),
                revision: next,
            };
            barrier.wait().await;
            repository
                .apply(context, OperationId::generate(), &command)
                .await
        }));
    }
    barrier.wait().await;
    let mut success = 0;
    let mut conflict = 0;
    for task in tasks {
        match task.await? {
            Ok(_) => success += 1,
            Err(DevelopmentError::Conflict | DevelopmentError::Unavailable) => conflict += 1,
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!((success, conflict), (1, 1));
    let current = repository
        .snapshot(context)
        .await?
        .resolve(&"dev/manuel/feature".parse()?)?;
    assert!(current.revision == next_a || current.revision == next_b);
    assert_eq!(
        stable_snapshot
            .resolve(&"dev/manuel/feature".parse()?)?
            .revision,
        revision_one
    );
    assert!(repository.telemetry().commands >= 4);
    assert!(repository.telemetry().conflicts >= 2);
    Ok(())
}

fn local_context() -> DevelopmentContext {
    let environment = EnvironmentId::generate();
    DevelopmentContext {
        scope: EnvironmentScope::new(ProjectId::generate(), environment),
        environment: EnvironmentDescriptor::local_development(environment),
    }
}

fn revision(
    context: DevelopmentContext,
    actor: DevelopmentActor,
    sequence: u128,
) -> Result<runku_development::DevelopmentRevisionEntry, Box<dyn Error>> {
    let revision_id = DevRevisionId::generate();
    let release_id = ReleaseId::generate();
    let hash = Sha256Digest::of(&sequence.to_be_bytes());
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: context.scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(i64::try_from(sequence)?),
        runtime_version: "platform-js-1".parse()?,
        artifact: ArtifactDescriptor {
            format: ArtifactFormat::SafeEsmBundleV1,
            digest: hash,
            size_bytes: 1,
        },
        function_contract_hash: hash,
        schema_contract_hash: hash,
        index_contract_hash: hash,
        functions: vec![FunctionManifest {
            id: FunctionId::generate(),
            name: "queries.version".parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: hash,
            arguments_contract_hash: hash,
            result_contract_hash: hash,
            capabilities: vec![Capability::DbRead],
        }],
        cron_definitions: Vec::new(),
    };
    let manifest_bytes = encode_release_manifest(&manifest)?;
    Ok(runku_development::DevelopmentRevisionEntry {
        revision_id,
        release_id,
        manifest_digest: Sha256Digest::of(&manifest_bytes),
        manifest_bytes,
        actor,
        created_at: TimestampMicros::new(i64::try_from(sequence)?),
    })
}
