//! Validated artifact-first publication into a local Development Workspace.

use std::path::Path;

use runku_core::{DevRevisionId, OperationId, PinnedCode, ReleaseId, WorkspaceRef};
use runku_cron::{
    CronCommand, CronContext, CronError, CronRepository, CronRepositoryConfig, CronSnapshot,
    SqlCronRepository,
};
use runku_development::{
    DevelopmentActor, DevelopmentCommand, DevelopmentContext, DevelopmentError,
    DevelopmentRepository, DevelopmentRepositoryConfig, DevelopmentRevisionEntry,
    SqlDevelopmentRepository,
};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{
    ArtifactFormat, ArtifactStore, FilesystemArtifactStore, FilesystemStoreRole, ReleaseCommand,
    ReleaseError, ReleaseManifestV1, ReleaseRepository, Sha256Digest, decode_node_esm_bundle,
    decode_release_manifest, decode_safe_esm_bundle, encode_node_esm_bundle,
    encode_release_manifest, encode_safe_esm_bundle,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use ulid::Ulid;

use crate::{LocalStateError, load_local};

/// Stable failure returned by local package publication.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalPublishError {
    /// Local project state/path is absent, invalid, or conflicting.
    #[error("local project state is invalid")]
    InvalidState,
    /// Manifest/artifact bytes are malformed, noncanonical, unsupported, or inconsistent.
    #[error("local release package is invalid")]
    InvalidPackage,
    /// The manifest belongs to a different Project.
    #[error("release package belongs to another project")]
    ProjectMismatch,
    /// Workspace is absent or its exact expected HEAD did not match.
    #[error("development workspace publication conflicted")]
    Conflict,
    /// Durable local storage is temporarily unavailable or the result is uncertain.
    #[error("local publication is temporarily unavailable")]
    Unavailable,
    /// Existing durable bytes violate an immutable invariant.
    #[error("local publication state is corrupt")]
    Corruption,
}

impl LocalPublishError {
    /// Stable machine-readable category.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidState => "LOCAL_PUBLISH_STATE_INVALID",
            Self::InvalidPackage => "LOCAL_PUBLISH_PACKAGE_INVALID",
            Self::ProjectMismatch => "LOCAL_PUBLISH_PROJECT_MISMATCH",
            Self::Conflict => "LOCAL_PUBLISH_CONFLICT",
            Self::Unavailable => "LOCAL_PUBLISH_UNAVAILABLE",
            Self::Corruption => "LOCAL_PUBLISH_CORRUPT",
        }
    }

    /// Whether a retry after external recovery may succeed unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

/// Successful immutable revision publication and Workspace pointer outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalPublishResult {
    /// Deterministic immutable revision installed by this package.
    pub revision_id: DevRevisionId,
    /// Candidate Release identity from the canonical manifest.
    pub release_id: ReleaseId,
    /// Exact Workspace HEAD required by the successful CAS.
    pub previous_head: Option<DevRevisionId>,
    /// The exact revision was already the Workspace HEAD.
    pub replayed: bool,
}

/// Publishes a canonical package using the Workspace HEAD observed in one coherent snapshot.
///
/// This convenience path remains CAS-safe against concurrent publishers. Use
/// [`publish_local_if_head`] when the caller must enforce an earlier explicit observation.
///
/// # Errors
///
/// Rejects invalid state/package/scope, missing Workspaces, CAS conflicts, and storage failures.
pub async fn publish_local(
    root: &Path,
    workspace_ref: &WorkspaceRef,
    actor: &DevelopmentActor,
    manifest_bytes: &[u8],
    artifact_bytes: &[u8],
) -> Result<LocalPublishResult, LocalPublishError> {
    let (state, paths) = load_local(root).await.map_err(map_state)?;
    let context = DevelopmentContext {
        scope: state.scope(),
        environment: state.environment(),
    };
    let repository = open_repository(&paths.development_database, context).await?;
    let snapshot = repository
        .snapshot(context)
        .await
        .map_err(map_development)?;
    let expected_head = snapshot
        .workspace_binding(workspace_ref)
        .ok_or(LocalPublishError::Conflict)?
        .head_revision;
    publish(
        &state,
        &paths,
        &repository,
        context,
        workspace_ref,
        actor,
        expected_head,
        manifest_bytes,
        artifact_bytes,
    )
    .await
}

/// Publishes only if the Workspace has the caller-provided exact HEAD (`None` means empty).
///
/// # Errors
///
/// Returns [`LocalPublishError::Conflict`] without moving HEAD when the precondition differs.
#[allow(clippy::too_many_arguments)]
pub async fn publish_local_if_head(
    root: &Path,
    workspace_ref: &WorkspaceRef,
    actor: &DevelopmentActor,
    expected_head: Option<DevRevisionId>,
    manifest_bytes: &[u8],
    artifact_bytes: &[u8],
) -> Result<LocalPublishResult, LocalPublishError> {
    let (state, paths) = load_local(root).await.map_err(map_state)?;
    let context = DevelopmentContext {
        scope: state.scope(),
        environment: state.environment(),
    };
    let repository = open_repository(&paths.development_database, context).await?;
    publish(
        &state,
        &paths,
        &repository,
        context,
        workspace_ref,
        actor,
        expected_head,
        manifest_bytes,
        artifact_bytes,
    )
    .await
}

async fn open_repository(
    path: &Path,
    context: DevelopmentContext,
) -> Result<SqlDevelopmentRepository, LocalPublishError> {
    SqlDevelopmentRepository::connect_sqlite(
        &format!("sqlite://{}?mode=rwc", path.display()),
        DevelopmentRepositoryConfig::LOCAL,
        context,
    )
    .await
    .map_err(map_development)
}

#[allow(clippy::too_many_arguments)]
async fn publish(
    state: &crate::LocalProjectState,
    paths: &crate::LocalPaths,
    repository: &SqlDevelopmentRepository,
    context: DevelopmentContext,
    workspace_ref: &WorkspaceRef,
    actor: &DevelopmentActor,
    expected_head: Option<DevRevisionId>,
    manifest_bytes: &[u8],
    artifact_bytes: &[u8],
) -> Result<LocalPublishResult, LocalPublishError> {
    let manifest = validate_local_package(state, manifest_bytes, artifact_bytes)?;

    let manifest_digest = Sha256Digest::of(manifest_bytes);
    let revision_id = deterministic_revision_id(manifest_digest, actor);
    let revision = DevelopmentRevisionEntry {
        revision_id,
        release_id: manifest.release_id,
        manifest_digest,
        manifest_bytes: manifest_bytes.to_vec(),
        actor: actor.clone(),
        created_at: manifest.created_at,
    };
    let artifacts =
        FilesystemArtifactStore::open(&paths.artifacts, FilesystemStoreRole::LocalDevelopment)
            .await
            .map_err(map_release_storage)?;
    artifacts
        .put(&manifest.artifact, artifact_bytes)
        .await
        .map_err(map_release_storage)?;
    let releases = SqlReleaseRepository::connect_sqlite(
        &format!("sqlite://{}?mode=rwc", paths.release_database.display()),
        RepositoryConfig::LOCAL,
    )
    .await
    .map_err(map_release_repository)?;
    releases
        .apply(
            state.scope(),
            deterministic_release_operation_id(state, manifest_digest),
            &ReleaseCommand::Register {
                manifest_bytes: manifest_bytes.to_vec(),
            },
        )
        .await
        .map_err(map_release_repository)?;

    let snapshot = repository
        .snapshot(context)
        .await
        .map_err(map_development)?;
    let binding = snapshot
        .workspace_binding(workspace_ref)
        .ok_or(LocalPublishError::Conflict)?;
    if binding.head_revision != expected_head {
        return Err(LocalPublishError::Conflict);
    }
    if binding.head_revision == Some(revision_id) {
        let existing = snapshot
            .resolve_revision(revision_id)
            .map_err(map_development)?;
        if existing.revision != revision {
            return Err(LocalPublishError::Corruption);
        }
        reconcile_cron(state, paths, revision_id, &manifest, manifest_bytes).await?;
        return Ok(LocalPublishResult {
            revision_id,
            release_id: manifest.release_id,
            previous_head: expected_head,
            replayed: true,
        });
    }

    let command = DevelopmentCommand::PublishRevision {
        workspace_ref: workspace_ref.clone(),
        expected_head,
        revision,
    };
    let operation_id =
        deterministic_operation_id(state, workspace_ref, actor, expected_head, manifest_digest);
    let applied = repository
        .apply(context, operation_id, &command)
        .await
        .map_err(map_development)?;
    if applied.head_revision != Some(revision_id) {
        return Err(LocalPublishError::Corruption);
    }
    reconcile_cron(state, paths, revision_id, &manifest, manifest_bytes).await?;
    Ok(LocalPublishResult {
        revision_id,
        release_id: manifest.release_id,
        previous_head: expected_head,
        replayed: applied.replayed,
    })
}

fn validate_local_package(
    state: &crate::LocalProjectState,
    manifest_bytes: &[u8],
    artifact_bytes: &[u8],
) -> Result<ReleaseManifestV1, LocalPublishError> {
    let manifest = decode_release_manifest(manifest_bytes).map_err(map_release_package)?;
    if encode_release_manifest(&manifest).map_err(map_release_package)? != manifest_bytes {
        return Err(LocalPublishError::InvalidPackage);
    }
    if manifest.project_id != state.project_id {
        return Err(LocalPublishError::ProjectMismatch);
    }
    match manifest.artifact.format {
        ArtifactFormat::SafeEsmBundleV1 => {
            let bundle = decode_safe_esm_bundle(artifact_bytes).map_err(map_release_package)?;
            if encode_safe_esm_bundle(&bundle).map_err(map_release_package)? != artifact_bytes {
                return Err(LocalPublishError::InvalidPackage);
            }
            bundle
                .verify_manifest(&manifest, artifact_bytes)
                .map_err(map_release_package)?;
        }
        ArtifactFormat::NodeEsmBundleV1 => {
            let bundle = decode_node_esm_bundle(artifact_bytes).map_err(map_release_package)?;
            if encode_node_esm_bundle(&bundle).map_err(map_release_package)? != artifact_bytes {
                return Err(LocalPublishError::InvalidPackage);
            }
            bundle
                .verify_manifest(&manifest, artifact_bytes)
                .map_err(map_release_package)?;
        }
        ArtifactFormat::NodeOciDescriptorV1 | ArtifactFormat::HybridOciArtifactV1 => {
            return Err(LocalPublishError::InvalidPackage);
        }
    }
    Ok(manifest)
}

pub(crate) async fn reconcile_cron_head(
    state: &crate::LocalProjectState,
    paths: &crate::LocalPaths,
) -> Result<(), LocalPublishError> {
    let context = DevelopmentContext {
        scope: state.scope(),
        environment: state.environment(),
    };
    let development = open_repository(&paths.development_database, context).await?;
    let resolution = development
        .snapshot(context)
        .await
        .map_err(map_development)?
        .resolve(&state.workspace_ref)
        .map_err(map_development)?;
    reconcile_cron(
        state,
        paths,
        resolution.revision.revision_id,
        &resolution.manifest,
        &resolution.revision.manifest_bytes,
    )
    .await
}

async fn reconcile_cron(
    state: &crate::LocalProjectState,
    paths: &crate::LocalPaths,
    revision_id: DevRevisionId,
    manifest: &runku_releases::ReleaseManifestV1,
    manifest_bytes: &[u8],
) -> Result<(), LocalPublishError> {
    reconcile_cron_pinned(
        state,
        paths,
        PinnedCode::DevRevision(revision_id),
        manifest,
        manifest_bytes,
        manifest.created_at,
    )
    .await
}

pub(crate) async fn reconcile_release_cron(
    state: &crate::LocalProjectState,
    paths: &crate::LocalPaths,
    manifest: &runku_releases::ReleaseManifestV1,
    manifest_bytes: &[u8],
    activated_at: runku_value::TimestampMicros,
) -> Result<(), LocalPublishError> {
    reconcile_cron_pinned(
        state,
        paths,
        PinnedCode::Release(manifest.release_id),
        manifest,
        manifest_bytes,
        activated_at,
    )
    .await
}

async fn reconcile_cron_pinned(
    state: &crate::LocalProjectState,
    paths: &crate::LocalPaths,
    pinned: PinnedCode,
    manifest: &runku_releases::ReleaseManifestV1,
    manifest_bytes: &[u8],
    activated_at: runku_value::TimestampMicros,
) -> Result<(), LocalPublishError> {
    let context = CronContext {
        scope: state.scope(),
        environment: state.environment(),
    };
    let repository = SqlCronRepository::connect_sqlite(
        &format!("sqlite://{}?mode=rwc", paths.cron_database.display()),
        CronRepositoryConfig::LOCAL,
        context,
    )
    .await
    .map_err(map_cron)?;
    let snapshot = repository.snapshot(context).await.map_err(map_cron)?;
    if cron_matches(&snapshot, pinned, manifest) {
        return Ok(());
    }
    let command = if manifest.cron_definitions.is_empty() {
        CronCommand::DeactivateAll {
            expected_revision: snapshot.repository_revision,
            deactivated_at: activated_at,
        }
    } else {
        CronCommand::ActivateManifest {
            expected_revision: snapshot.repository_revision,
            pinned_code: pinned,
            manifest_bytes: manifest_bytes.to_vec(),
            activated_at,
        }
    };
    let digest = command.digest(context).map_err(map_cron)?;
    repository
        .apply(
            context,
            OperationId::from_ulid(ulid_from_digest(digest)),
            &command,
        )
        .await
        .map(|_| ())
        .map_err(map_cron)
}

pub(crate) fn cron_matches(
    snapshot: &CronSnapshot,
    pinned: PinnedCode,
    manifest: &runku_releases::ReleaseManifestV1,
) -> bool {
    snapshot.activations.len() == manifest.cron_definitions.len()
        && snapshot
            .activations
            .iter()
            .zip(&manifest.cron_definitions)
            .all(|(activation, definition)| {
                activation.name == definition.name
                    && activation.pinned_code == pinned
                    && activation.release_id == manifest.release_id
                    && activation.schedule == definition.schedule
                    && activation.function == definition.function
                    && activation.args == definition.args
            })
}

fn deterministic_revision_id(
    manifest_digest: Sha256Digest,
    actor: &DevelopmentActor,
) -> DevRevisionId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_LOCAL_DEV_REVISION_V1\0");
    digest.update(manifest_digest.as_bytes());
    digest.update(actor.as_str().as_bytes());
    DevRevisionId::from_ulid(ulid_from_digest(digest.finalize().into()))
}

fn deterministic_operation_id(
    state: &crate::LocalProjectState,
    workspace_ref: &WorkspaceRef,
    actor: &DevelopmentActor,
    expected_head: Option<DevRevisionId>,
    manifest_digest: Sha256Digest,
) -> OperationId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_LOCAL_PUBLISH_OPERATION_V1\0");
    digest.update(state.project_id.to_string().as_bytes());
    digest.update(state.environment_id.to_string().as_bytes());
    digest.update(workspace_ref.as_str().as_bytes());
    digest.update(actor.as_str().as_bytes());
    match expected_head {
        Some(head) => {
            digest.update([1]);
            digest.update(head.to_string().as_bytes());
        }
        None => digest.update([0]),
    }
    digest.update(manifest_digest.as_bytes());
    OperationId::from_ulid(ulid_from_digest(digest.finalize().into()))
}

fn deterministic_release_operation_id(
    state: &crate::LocalProjectState,
    manifest_digest: Sha256Digest,
) -> OperationId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_LOCAL_RELEASE_REGISTER_V1\0");
    digest.update(state.project_id.to_string().as_bytes());
    digest.update(state.environment_id.to_string().as_bytes());
    digest.update(manifest_digest.as_bytes());
    OperationId::from_ulid(ulid_from_digest(digest.finalize().into()))
}

fn ulid_from_digest(digest: [u8; 32]) -> Ulid {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Ulid::from_bytes(bytes)
}

fn map_state(error: LocalStateError) -> LocalPublishError {
    match error {
        LocalStateError::Unavailable => LocalPublishError::Unavailable,
        LocalStateError::Corruption => LocalPublishError::Corruption,
        LocalStateError::InvalidPath
        | LocalStateError::InvalidState
        | LocalStateError::Conflict => LocalPublishError::InvalidState,
    }
}

fn map_release_package(_: ReleaseError) -> LocalPublishError {
    LocalPublishError::InvalidPackage
}

fn map_release_storage(error: ReleaseError) -> LocalPublishError {
    match error {
        ReleaseError::Corruption => LocalPublishError::Corruption,
        ReleaseError::Busy | ReleaseError::Unavailable | ReleaseError::ResultUncertain => {
            LocalPublishError::Unavailable
        }
        _ => LocalPublishError::InvalidPackage,
    }
}

fn map_release_repository(error: ReleaseError) -> LocalPublishError {
    match error {
        ReleaseError::Corruption | ReleaseError::InvalidSnapshot => LocalPublishError::Corruption,
        ReleaseError::RepositoryConflict | ReleaseError::OperationIdReused => {
            LocalPublishError::Conflict
        }
        ReleaseError::Busy | ReleaseError::Unavailable | ReleaseError::ResultUncertain => {
            LocalPublishError::Unavailable
        }
        _ => LocalPublishError::InvalidPackage,
    }
}

fn map_development(error: DevelopmentError) -> LocalPublishError {
    match error {
        DevelopmentError::Conflict | DevelopmentError::WorkspaceNotFound => {
            LocalPublishError::Conflict
        }
        DevelopmentError::Unavailable | DevelopmentError::ResultUncertain => {
            LocalPublishError::Unavailable
        }
        DevelopmentError::Corruption | DevelopmentError::InvalidSnapshot => {
            LocalPublishError::Corruption
        }
        DevelopmentError::InvalidInput
        | DevelopmentError::PolicyDenied
        | DevelopmentError::InvalidRevision
        | DevelopmentError::WorkspaceEmpty
        | DevelopmentError::RevisionNotFound
        | DevelopmentError::LimitExceeded
        | DevelopmentError::Unsupported => LocalPublishError::InvalidPackage,
    }
}

fn map_cron(error: CronError) -> LocalPublishError {
    match error {
        CronError::Conflict | CronError::LeaseLost => LocalPublishError::Conflict,
        CronError::Unavailable | CronError::ResultUncertain => LocalPublishError::Unavailable,
        CronError::Corruption => LocalPublishError::Corruption,
        CronError::InvalidInput
        | CronError::InvalidManifest
        | CronError::LimitExceeded
        | CronError::Unsupported => LocalPublishError::InvalidPackage,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr};

    use runku_core::{BuildId, FunctionId, OperationId, ProjectId, ReleaseId, WorkspaceRef};
    use runku_cron::{
        CronCommand, CronContext, CronRepository, CronRepositoryConfig, SqlCronRepository,
    };
    use runku_development::{
        DevelopmentActor, DevelopmentContext, DevelopmentRepository, DevelopmentRepositoryConfig,
        SqlDevelopmentRepository,
    };
    use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
    use runku_releases::{
        ArtifactStore, AuthPolicy, CronDefinition, FilesystemArtifactStore, FilesystemStoreRole,
        FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1, ReleaseRepository,
        RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_release_manifest,
        encode_safe_esm_bundle,
    };
    use runku_value::TimestampMicros;
    use tempfile::tempdir;

    use super::{LocalPublishError, publish_local, publish_local_if_head, reconcile_cron_head};
    use crate::initialize_local;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    struct Package {
        manifest: ReleaseManifestV1,
        manifest_bytes: Vec<u8>,
        artifact_bytes: Vec<u8>,
    }

    fn package(
        project_id: ProjectId,
        sequence: u128,
        with_cron: bool,
    ) -> Result<Package, Box<dyn std::error::Error>> {
        let source = format!("export default () => ({sequence});");
        let implementation_hash = Sha256Digest::of(source.as_bytes());
        let bundle = SafeEsmBundleV1::from_sources([source])?;
        let artifact_bytes = encode_safe_esm_bundle(&bundle)?;
        let contract = Sha256Digest::of(&sequence.to_be_bytes());
        let manifest = ReleaseManifestV1 {
            release_id: ReleaseId::from_ulid(ulid::Ulid::from(sequence + 100)),
            project_id,
            build_id: BuildId::from_ulid(ulid::Ulid::from(sequence + 200)),
            created_at: TimestampMicros::new(i64::try_from(sequence)?),
            runtime_version: "platform-js-1".parse()?,
            artifact: bundle.descriptor()?,
            function_contract_hash: contract,
            schema_contract_hash: contract,
            index_contract_hash: contract,
            functions: vec![
                FunctionManifest {
                    id: FunctionId::from_ulid(ulid::Ulid::from(sequence + 300)),
                    name: "actions.cron".parse()?,
                    function_type: FunctionType::Action,
                    visibility: FunctionVisibility::Internal,
                    auth_policy: AuthPolicy::None,
                    runtime_class: RuntimeClass::SafeV8,
                    implementation_hash,
                    arguments_contract_hash: contract,
                    result_contract_hash: contract,
                    capabilities: vec![],
                },
                FunctionManifest {
                    id: FunctionId::from_ulid(ulid::Ulid::from(sequence + 301)),
                    name: "queries.version".parse()?,
                    function_type: FunctionType::Query,
                    visibility: FunctionVisibility::Public,
                    auth_policy: AuthPolicy::None,
                    runtime_class: RuntimeClass::SafeV8,
                    implementation_hash,
                    arguments_contract_hash: contract,
                    result_contract_hash: contract,
                    capabilities: vec![],
                },
            ],
            cron_definitions: if with_cron {
                vec![CronDefinition {
                    name: "minute".parse()?,
                    schedule: "* * * * *".parse()?,
                    function: "actions.cron".parse()?,
                    args: runku_value::CanonicalValue::Null,
                }]
            } else {
                vec![]
            },
        };
        let manifest_bytes = encode_release_manifest(&manifest)?;
        Ok(Package {
            manifest,
            manifest_bytes,
            artifact_bytes,
        })
    }

    async fn initialized(
        port: u16,
    ) -> Result<
        (
            tempfile::TempDir,
            crate::LocalProjectState,
            crate::LocalPaths,
            WorkspaceRef,
        ),
        Box<dyn std::error::Error>,
    > {
        let directory = tempdir()?;
        let workspace = WorkspaceRef::from_str("default")?;
        let (state, paths) = initialize_local(
            directory.path(),
            workspace.clone(),
            SocketAddr::from(([127, 0, 0, 1], port)),
            TimestampMicros::new(10),
        )
        .await?;
        Ok((directory, state, paths, workspace))
    }

    #[tokio::test]
    async fn publication_is_artifact_first_cas_and_exactly_replayable() -> TestResult {
        let (directory, state, paths, workspace) = initialized(3220).await?;
        let package = package(state.project_id, 20, false)?;
        let actor = DevelopmentActor::from_str("manuel.local")?;
        let first = publish_local_if_head(
            directory.path(),
            &workspace,
            &actor,
            None,
            &package.manifest_bytes,
            &package.artifact_bytes,
        )
        .await?;
        assert!(!first.replayed);
        assert_eq!(first.release_id, package.manifest.release_id);
        assert_eq!(first.previous_head, None);
        let releases = SqlReleaseRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", paths.release_database.display()),
            RepositoryConfig::LOCAL,
        )
        .await?;
        assert_eq!(
            releases.manifest(state.scope(), first.release_id).await?,
            package.manifest
        );

        let artifacts =
            FilesystemArtifactStore::open(&paths.artifacts, FilesystemStoreRole::LocalDevelopment)
                .await?;
        assert_eq!(
            artifacts.get(&package.manifest.artifact).await?,
            package.artifact_bytes
        );
        drop(artifacts);
        tokio::fs::remove_dir_all(&paths.artifacts).await?;

        let replay = publish_local(
            directory.path(),
            &workspace,
            &actor,
            &package.manifest_bytes,
            &package.artifact_bytes,
        )
        .await?;
        assert!(replay.replayed);
        assert_eq!(replay.revision_id, first.revision_id);
        assert_eq!(replay.previous_head, Some(first.revision_id));
        let repaired =
            FilesystemArtifactStore::open(&paths.artifacts, FilesystemStoreRole::LocalDevelopment)
                .await?;
        assert_eq!(
            repaired.get(&package.manifest.artifact).await?,
            package.artifact_bytes
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_head_conflict_never_moves_workspace_pointer() -> TestResult {
        let (directory, state, paths, workspace) = initialized(3221).await?;
        let first_package = package(state.project_id, 21, false)?;
        let second_package = package(state.project_id, 22, false)?;
        let actor = DevelopmentActor::from_str("publisher")?;
        let first = publish_local_if_head(
            directory.path(),
            &workspace,
            &actor,
            None,
            &first_package.manifest_bytes,
            &first_package.artifact_bytes,
        )
        .await?;

        assert_eq!(
            publish_local_if_head(
                directory.path(),
                &workspace,
                &actor,
                None,
                &second_package.manifest_bytes,
                &second_package.artifact_bytes,
            )
            .await,
            Err(LocalPublishError::Conflict)
        );
        let context = DevelopmentContext {
            scope: state.scope(),
            environment: state.environment(),
        };
        let repository = SqlDevelopmentRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", paths.development_database.display()),
            DevelopmentRepositoryConfig::LOCAL,
            context,
        )
        .await?;
        assert_eq!(
            repository
                .snapshot(context)
                .await?
                .workspace_binding(&workspace)
                .ok_or("workspace disappeared")?
                .head_revision,
            Some(first.revision_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_cross_project_or_tampered_packages_fail_before_head_move() -> TestResult {
        let (directory, state, paths, workspace) = initialized(3222).await?;
        let actor = DevelopmentActor::from_str("publisher")?;
        let valid = package(state.project_id, 23, false)?;
        let other = package(ProjectId::generate(), 24, false)?;
        let mut tampered_artifact = valid.artifact_bytes.clone();
        let last = tampered_artifact
            .last_mut()
            .ok_or("empty artifact fixture")?;
        *last ^= 1;

        assert_eq!(
            publish_local(
                directory.path(),
                &workspace,
                &actor,
                &other.manifest_bytes,
                &other.artifact_bytes,
            )
            .await,
            Err(LocalPublishError::ProjectMismatch)
        );
        assert_eq!(
            publish_local(
                directory.path(),
                &workspace,
                &actor,
                &valid.manifest_bytes,
                &tampered_artifact,
            )
            .await,
            Err(LocalPublishError::InvalidPackage)
        );
        let context = DevelopmentContext {
            scope: state.scope(),
            environment: state.environment(),
        };
        let repository = SqlDevelopmentRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", paths.development_database.display()),
            DevelopmentRepositoryConfig::LOCAL,
            context,
        )
        .await?;
        assert_eq!(
            repository
                .snapshot(context)
                .await?
                .workspace_binding(&workspace)
                .ok_or("workspace disappeared")?
                .head_revision,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn cron_activation_tracks_exact_workspace_head_and_replays() -> TestResult {
        let (directory, state, paths, workspace) = initialized(3223).await?;
        let actor = DevelopmentActor::from_str("publisher")?;
        let with_cron = package(state.project_id, 25, true)?;
        let published = publish_local(
            directory.path(),
            &workspace,
            &actor,
            &with_cron.manifest_bytes,
            &with_cron.artifact_bytes,
        )
        .await?;
        let context = CronContext {
            scope: state.scope(),
            environment: state.environment(),
        };
        let repository = SqlCronRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", paths.cron_database.display()),
            CronRepositoryConfig::LOCAL,
            context,
        )
        .await?;
        let active = repository.snapshot(context).await?;
        assert_eq!(active.repository_revision, 1);
        assert_eq!(active.activations.len(), 1);
        assert_eq!(
            active.activations[0].pinned_code,
            runku_core::PinnedCode::DevRevision(published.revision_id)
        );
        repository
            .apply(
                context,
                OperationId::generate(),
                &CronCommand::DeactivateAll {
                    expected_revision: 1,
                    deactivated_at: TimestampMicros::new(30),
                },
            )
            .await?;
        assert!(repository.snapshot(context).await?.activations.is_empty());
        reconcile_cron_head(&state, &paths).await?;
        let repaired = repository.snapshot(context).await?;
        assert_eq!(repaired.repository_revision, 3);
        assert_eq!(repaired.activations.len(), 1);

        assert!(
            publish_local(
                directory.path(),
                &workspace,
                &actor,
                &with_cron.manifest_bytes,
                &with_cron.artifact_bytes,
            )
            .await?
            .replayed
        );
        assert_eq!(repository.snapshot(context).await?.repository_revision, 3);

        let without_cron = package(state.project_id, 26, false)?;
        publish_local(
            directory.path(),
            &workspace,
            &actor,
            &without_cron.manifest_bytes,
            &without_cron.artifact_bytes,
        )
        .await?;
        let inactive = repository.snapshot(context).await?;
        assert_eq!(inactive.repository_revision, 4);
        assert!(inactive.activations.is_empty());
        Ok(())
    }
}
