//! Read-only local consistency and dependency health diagnosis.

use std::path::Path;

use runku_core::{DevRevisionId, ReleaseId};
use runku_cron::{CronContext, CronRepository, CronRepositoryConfig, SqlCronRepository};
use runku_data::LogicalStore;
use runku_data_sqlite::{SqliteStore, SqliteStoreConfig};
use runku_development::{
    DevelopmentContext, DevelopmentRepository, DevelopmentRepositoryConfig,
    SqlDevelopmentRepository,
};
use runku_development_access::{
    DevelopmentAccessRepository, DevelopmentAccessRepositoryConfig, SqlDevelopmentAccessRepository,
};
use runku_identity::{ApplicationIdentityRepository, EnvironmentScope};
use runku_identity_repository::{IdentityRepositoryConfig, SqlApplicationIdentityRepository};
use runku_observability::{
    LogArchive, LogCursor, LogQuery, LogRepository, LogRepositoryConfig, SqlLogRepository,
};
use runku_otel::{ExportCheckpointRepository, OtlpRepositoryConfig, SqlExportCheckpointRepository};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{
    ArtifactStore, FilesystemArtifactStore, FilesystemStoreRole, ReleaseRepository,
};
use thiserror::Error;

use crate::{
    LocalStateError, load_local,
    publish::cron_matches,
    state::{load_development_access_pepper, load_identity_pepper},
};

/// Successful coherent local dependency report without secrets or user data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalDoctorReport {
    /// Exact checked Environment scope.
    pub scope: EnvironmentScope,
    /// Default Workspace immutable HEAD.
    pub revision_id: DevRevisionId,
    /// Candidate Release embedded in that revision.
    pub release_id: ReleaseId,
    /// Monotonic Development serving catalog revision.
    pub development_revision: u64,
    /// Monotonic Release repository revision.
    pub release_repository_revision: u64,
    /// Monotonic Cron activation repository revision; zero is valid before first activation.
    pub cron_repository_revision: u64,
    /// Number of active Cron definitions, already checked against Workspace HEAD.
    pub active_cron_definitions: usize,
    /// Operational log repository opened, migrated, and answered a bounded scoped query.
    pub operational_logs_healthy: bool,
    /// OTLP checkpoint repository opened and migration checksum verified.
    pub otlp_checkpoints_healthy: bool,
    /// Development Access repository and dedicated pepper are healthy.
    pub development_access_healthy: bool,
}

/// Stable failure returned by local diagnosis.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalDoctorError {
    /// State/path/permissions or expected durable files are invalid.
    #[error("local doctor found invalid state")]
    InvalidState,
    /// A dependency could not answer its bounded health/read request.
    #[error("local doctor dependency is unavailable")]
    Unavailable,
    /// Workspace, Release, artifact, or Cron records disagree.
    #[error("local doctor found inconsistent durable state")]
    Inconsistent,
}

impl LocalDoctorError {
    /// Stable machine-readable category.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidState => "LOCAL_DOCTOR_STATE_INVALID",
            Self::Unavailable => "LOCAL_DOCTOR_UNAVAILABLE",
            Self::Inconsistent => "LOCAL_DOCTOR_INCONSISTENT",
        }
    }

    /// Whether the same diagnosis may succeed after an external dependency recovers.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

/// Checks one initialized and published local project without repairing or moving any pointer.
///
/// Repository adapters may perform their normal connection pragmas, but this function issues no
/// application mutation, Release command, Development command, Cron command, or artifact write.
///
/// # Errors
///
/// Rejects missing/symlinked files, invalid pepper permissions, unhealthy stores, empty/invalid
/// Workspace HEAD, candidate manifest drift, missing artifact, or Cron activation drift.
#[allow(clippy::too_many_lines)]
pub async fn doctor_local(root: &Path) -> Result<LocalDoctorReport, LocalDoctorError> {
    let (state, paths) = load_local(root).await.map_err(map_state)?;
    for path in [
        &paths.data_database,
        &paths.release_database,
        &paths.identity_database,
        &paths.development_access_database,
        &paths.development_database,
        &paths.cron_database,
        &paths.observability_database,
        &paths.otlp_database,
    ] {
        validate_regular_file(path).await?;
    }
    validate_directory(&paths.observability_archive).await?;
    load_identity_pepper(&paths).await.map_err(map_state)?;
    load_development_access_pepper(&paths)
        .await
        .map_err(map_state)?;

    let releases = SqlReleaseRepository::connect_sqlite(
        &sqlite_url(&paths.release_database),
        RepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalDoctorError::Unavailable)?;
    releases
        .health()
        .await
        .map_err(|_| LocalDoctorError::Unavailable)?;
    let release_snapshot = releases
        .snapshot(state.scope())
        .await
        .map_err(|_| LocalDoctorError::Inconsistent)?;

    let development_context = DevelopmentContext {
        scope: state.scope(),
        environment: state.environment(),
    };
    let development = SqlDevelopmentRepository::connect_sqlite(
        &sqlite_url(&paths.development_database),
        DevelopmentRepositoryConfig::LOCAL,
        development_context,
    )
    .await
    .map_err(|_| LocalDoctorError::Unavailable)?;
    development
        .health()
        .await
        .map_err(|_| LocalDoctorError::Unavailable)?;
    let development_snapshot = development
        .snapshot(development_context)
        .await
        .map_err(|_| LocalDoctorError::Inconsistent)?;
    let resolution = development_snapshot
        .resolve(&state.workspace_ref)
        .map_err(|_| LocalDoctorError::Inconsistent)?;
    let release_manifest = releases
        .manifest(state.scope(), resolution.manifest.release_id)
        .await
        .map_err(|_| LocalDoctorError::Inconsistent)?;
    if release_manifest != resolution.manifest {
        return Err(LocalDoctorError::Inconsistent);
    }

    let artifacts =
        FilesystemArtifactStore::open(&paths.artifacts, FilesystemStoreRole::LocalDevelopment)
            .await
            .map_err(|_| LocalDoctorError::Unavailable)?;
    artifacts
        .get(&resolution.manifest.artifact)
        .await
        .map_err(|_| LocalDoctorError::Inconsistent)?;

    let identity = SqlApplicationIdentityRepository::connect_sqlite(
        &sqlite_url(&paths.identity_database),
        IdentityRepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalDoctorError::Unavailable)?;
    identity
        .health()
        .await
        .map_err(|_| LocalDoctorError::Unavailable)?;
    let development_access = SqlDevelopmentAccessRepository::connect_sqlite(
        &sqlite_url(&paths.development_access_database),
        DevelopmentAccessRepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalDoctorError::Unavailable)?;
    development_access
        .health()
        .await
        .map_err(|_| LocalDoctorError::Unavailable)?;
    development_access.close().await;
    let data = SqliteStore::open(&paths.data_database, SqliteStoreConfig::LOCAL)
        .await
        .map_err(|_| LocalDoctorError::Unavailable)?;
    data.health()
        .await
        .map_err(|_| LocalDoctorError::Unavailable)?;
    let logs = SqlLogRepository::connect_sqlite(
        &sqlite_url(&paths.observability_database),
        LogRepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalDoctorError::Unavailable)?;
    logs.query(&LogQuery {
        scope: state.scope(),
        after: LogCursor::START,
        limit: 1,
        stream: None,
        minimum_level: None,
        function_id: None,
        request_id: None,
        invocation_id: None,
        client_id: None,
        credential_id: None,
        release_id: None,
    })
    .await
    .map_err(|_| LocalDoctorError::Unavailable)?;
    logs.close().await;
    LogArchive::open_filesystem(paths.observability_archive.clone())
        .await
        .map_err(|_| LocalDoctorError::Unavailable)?
        .status(state.scope())
        .await
        .map_err(|_| LocalDoctorError::Inconsistent)?;
    let otlp = SqlExportCheckpointRepository::connect_sqlite(
        &sqlite_url(&paths.otlp_database),
        OtlpRepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalDoctorError::Unavailable)?;
    otlp.close().await;

    let cron_context = CronContext {
        scope: state.scope(),
        environment: state.environment(),
    };
    let cron = SqlCronRepository::connect_sqlite(
        &sqlite_url(&paths.cron_database),
        CronRepositoryConfig::LOCAL,
        cron_context,
    )
    .await
    .map_err(|_| LocalDoctorError::Unavailable)?;
    cron.health()
        .await
        .map_err(|_| LocalDoctorError::Unavailable)?;
    let cron_snapshot = cron
        .snapshot(cron_context)
        .await
        .map_err(|_| LocalDoctorError::Inconsistent)?;
    if !cron_matches(
        &cron_snapshot,
        runku_core::PinnedCode::DevRevision(resolution.revision.revision_id),
        &resolution.manifest,
    ) {
        return Err(LocalDoctorError::Inconsistent);
    }

    Ok(LocalDoctorReport {
        scope: state.scope(),
        revision_id: resolution.revision.revision_id,
        release_id: resolution.manifest.release_id,
        development_revision: development_snapshot.revision(),
        release_repository_revision: release_snapshot.revision(),
        cron_repository_revision: cron_snapshot.repository_revision,
        active_cron_definitions: cron_snapshot.activations.len(),
        operational_logs_healthy: true,
        otlp_checkpoints_healthy: true,
        development_access_healthy: true,
    })
}

async fn validate_regular_file(path: &Path) -> Result<(), LocalDoctorError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| LocalDoctorError::InvalidState)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
        return Err(LocalDoctorError::InvalidState);
    }
    Ok(())
}

async fn validate_directory(path: &Path) -> Result<(), LocalDoctorError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| LocalDoctorError::InvalidState)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(LocalDoctorError::InvalidState);
    }
    Ok(())
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display())
}

fn map_state(error: LocalStateError) -> LocalDoctorError {
    match error {
        LocalStateError::Unavailable => LocalDoctorError::Unavailable,
        LocalStateError::InvalidPath
        | LocalStateError::InvalidState
        | LocalStateError::Conflict
        | LocalStateError::Corruption => LocalDoctorError::InvalidState,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr};

    use runku_core::{BuildId, FunctionId, ReleaseId, WorkspaceRef};
    use runku_development::DevelopmentActor;
    use runku_releases::{
        AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility,
        ReleaseManifestV1, RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_release_manifest,
        encode_safe_esm_bundle,
    };
    use runku_value::TimestampMicros;
    use tempfile::tempdir;

    use super::{LocalDoctorError, doctor_local};
    use crate::{initialize_local, publish_local};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn doctor_checks_published_graph_and_rejects_symlinked_repository() -> TestResult {
        let directory = tempdir()?;
        let workspace = WorkspaceRef::from_str("default")?;
        let (state, paths) = initialize_local(
            directory.path(),
            workspace.clone(),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            TimestampMicros::new(100),
        )
        .await?;
        let source = "export default async (_ctx, value) => value;";
        let bundle = SafeEsmBundleV1::from_sources([source])?;
        let artifact = encode_safe_esm_bundle(&bundle)?;
        let contract = Sha256Digest::of(b"doctor-contract");
        let release_id = ReleaseId::generate();
        let manifest = encode_release_manifest(&ReleaseManifestV1 {
            release_id,
            project_id: state.project_id,
            build_id: BuildId::generate(),
            created_at: TimestampMicros::new(101),
            runtime_version: "platform-js-1".parse()?,
            artifact: bundle.descriptor()?,
            function_contract_hash: contract,
            schema_contract_hash: contract,
            index_contract_hash: contract,
            functions: vec![FunctionManifest {
                id: FunctionId::generate(),
                name: "queries.echo".parse()?,
                function_type: FunctionType::Query,
                visibility: FunctionVisibility::Public,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(source.as_bytes()),
                arguments_contract_hash: contract,
                result_contract_hash: contract,
                capabilities: vec![Capability::DbRead],
            }],
            cron_definitions: vec![],
        })?;
        let published = publish_local(
            directory.path(),
            &workspace,
            &DevelopmentActor::from_str("doctor-test")?,
            &manifest,
            &artifact,
        )
        .await?;
        let report = doctor_local(directory.path()).await?;
        assert_eq!(report.scope, state.scope());
        assert_eq!(report.revision_id, published.revision_id);
        assert_eq!(report.release_id, release_id);
        assert_eq!(report.active_cron_definitions, 0);

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let backup = paths.state.join("development-backup.sqlite3");
            tokio::fs::rename(&paths.development_database, &backup).await?;
            symlink(&backup, &paths.development_database)?;
            assert_eq!(
                doctor_local(directory.path()).await,
                Err(LocalDoctorError::InvalidState)
            );
        }
        Ok(())
    }
}
