//! Safe local Release validation, compatibility, promotion, rollback, and status.

use std::{path::Path, time::SystemTime};

use runku_compatibility::{CompatibilityEngine, CompatibilityReport, ReleasePackage};
use runku_core::{ChannelName, OperationId, ReleaseId};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{
    ArtifactStore, FilesystemArtifactStore, FilesystemStoreRole, ReleaseCommand, ReleaseError,
    ReleaseRepository, ReleaseStatus, ServingSnapshot, encode_release_manifest,
};
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};
use thiserror::Error;
use ulid::Ulid;

use crate::{
    LocalPaths, LocalProjectState, load_local, publish::reconcile_release_cron, state::sqlite_url,
};

/// Stable local Release lifecycle failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalReleaseError {
    /// Root, target, status, or package is invalid for the requested operation.
    #[error("local release request is invalid")]
    InvalidRequest,
    /// A Release or Channel does not exist.
    #[error("local release target was not found")]
    NotFound,
    /// A compare-and-set precondition or concurrent operation conflicted.
    #[error("local release operation conflicted")]
    Conflict,
    /// Local durable storage is temporarily unavailable.
    #[error("local release storage is unavailable")]
    Unavailable,
    /// Durable metadata or artifact bytes violate integrity invariants.
    #[error("local release state is corrupt")]
    Corruption,
}

impl LocalReleaseError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "LOCAL_RELEASE_INVALID",
            Self::NotFound => "LOCAL_RELEASE_NOT_FOUND",
            Self::Conflict => "LOCAL_RELEASE_CONFLICT",
            Self::Unavailable => "LOCAL_RELEASE_UNAVAILABLE",
            Self::Corruption => "LOCAL_RELEASE_CORRUPT",
        }
    }
}

/// Safe compatibility diagnostic projected for local and CLI consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalCompatibilityDiagnostic {
    /// Stable machine-readable reason.
    pub code: &'static str,
    /// Canonical bounded logical subject.
    pub subject: String,
}

/// Result of validating or moving one Release.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalReleaseOutcome {
    /// Release operated on.
    pub release_id: ReleaseId,
    /// Channel moved, if this was promotion/rollback.
    pub channel: Option<ChannelName>,
    /// Final lifecycle state observed.
    pub status: ReleaseStatus,
    /// Final durable serving configuration revision.
    pub serving_revision: u64,
    /// Whether the desired final state already existed.
    pub replayed: bool,
    /// Ordered blockers; nonempty means no Channel move occurred.
    pub diagnostics: Vec<LocalCompatibilityDiagnostic>,
}

/// Read-only Release entry for local status output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalReleaseStatus {
    /// Immutable Release identity.
    pub release_id: ReleaseId,
    /// Current lifecycle state.
    pub status: ReleaseStatus,
    /// Platform runtime/API version.
    pub runtime_version: String,
}

/// Read-only Channel entry for local status output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalChannelStatus {
    /// Moving Channel name.
    pub channel: ChannelName,
    /// Exact selected Release.
    pub release_id: ReleaseId,
    /// Whether this Channel is the environment default.
    pub default: bool,
}

/// Coherent local Release/Channel status at one repository revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalReleaseStatusReport {
    /// Serving configuration revision. A freshly initialized Environment is revision zero.
    pub serving_revision: u64,
    /// Default Channel, if configured.
    pub default_channel: Option<ChannelName>,
    /// Releases in stable identity order.
    pub releases: Vec<LocalReleaseStatus>,
    /// Channels in stable name order.
    pub channels: Vec<LocalChannelStatus>,
}

/// Optional operator precondition for a Channel promotion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalChannelExpectation {
    /// Use the freshly read durable binding and repository CAS only.
    #[default]
    Current,
    /// Require the Channel to be absent.
    Empty,
    /// Require one exact current Release.
    Release(ReleaseId),
}

/// Product Base local lifecycle service over durable Release metadata and artifacts.
pub struct LocalReleaseManager {
    state: LocalProjectState,
    paths: LocalPaths,
    repository: SqlReleaseRepository,
    artifacts: FilesystemArtifactStore,
}

impl LocalReleaseManager {
    /// Opens an initialized local project without mutating lifecycle state.
    ///
    /// # Errors
    ///
    /// Rejects invalid roots and unavailable/corrupt repositories.
    pub async fn open(root: &Path) -> Result<Self, LocalReleaseError> {
        let (state, paths) = load_local(root).await.map_err(|error| match error {
            crate::LocalStateError::Unavailable => LocalReleaseError::Unavailable,
            crate::LocalStateError::Corruption => LocalReleaseError::Corruption,
            crate::LocalStateError::Conflict
            | crate::LocalStateError::InvalidPath
            | crate::LocalStateError::InvalidState => LocalReleaseError::InvalidRequest,
        })?;
        let repository = SqlReleaseRepository::connect_sqlite(
            &sqlite_url(&paths.release_database),
            RepositoryConfig::LOCAL,
        )
        .await
        .map_err(map_repository)?;
        let artifacts =
            FilesystemArtifactStore::open(&paths.artifacts, FilesystemStoreRole::LocalDevelopment)
                .await
                .map_err(map_repository)?;
        Ok(Self {
            state,
            paths,
            repository,
            artifacts,
        })
    }

    /// Validates one published candidate and advances it to `SERVABLE` if compatible.
    ///
    /// `against` selects an exact Channel baseline. When absent, the configured default Channel
    /// is used; an environment with Channels but no default requires an explicit baseline.
    ///
    /// # Errors
    ///
    /// Rejects missing/invalid candidates, ambiguous baselines, repository conflicts, corruption,
    /// or unavailable storage. Semantic blockers are returned in a successful outcome whose final
    /// state is `COMPATIBILITY_BLOCKED`.
    pub async fn release(
        &self,
        release_id: ReleaseId,
        against: Option<&ChannelName>,
    ) -> Result<LocalReleaseOutcome, LocalReleaseError> {
        let initial = self.snapshot().await?;
        let entry = initial
            .release(release_id)
            .ok_or(LocalReleaseError::NotFound)?;
        if matches!(
            entry.status,
            ReleaseStatus::Servable | ReleaseStatus::Active
        ) {
            return Ok(outcome(
                release_id,
                None,
                entry.status,
                initial.revision(),
                true,
                Vec::new(),
            ));
        }
        let baseline = select_baseline(&initial, against)?;
        self.advance_if(release_id, entry.status, ReleaseStatus::Building)
            .await?;
        let mut snapshot = self.snapshot().await?;
        let mut status = snapshot
            .release(release_id)
            .ok_or(LocalReleaseError::Corruption)?
            .status;
        if matches!(
            status,
            ReleaseStatus::Building | ReleaseStatus::CompatibilityBlocked
        ) {
            if status == ReleaseStatus::CompatibilityBlocked {
                let report = self.compatibility_report(release_id, baseline).await?;
                if !report.compatible {
                    return Ok(outcome(
                        release_id,
                        None,
                        status,
                        snapshot.revision(),
                        true,
                        diagnostics(report),
                    ));
                }
            }
            self.transition(release_id, status, ReleaseStatus::Validating)
                .await?;
            status = ReleaseStatus::Validating;
        }
        if status == ReleaseStatus::Validating {
            let report = self.compatibility_report(release_id, baseline).await?;
            if !report.compatible {
                self.transition(
                    release_id,
                    ReleaseStatus::Validating,
                    ReleaseStatus::CompatibilityBlocked,
                )
                .await?;
                snapshot = self.snapshot().await?;
                return Ok(outcome(
                    release_id,
                    None,
                    ReleaseStatus::CompatibilityBlocked,
                    snapshot.revision(),
                    false,
                    diagnostics(report),
                ));
            }
            self.transition(release_id, ReleaseStatus::Validating, ReleaseStatus::Ready)
                .await?;
            status = ReleaseStatus::Ready;
        }
        if status == ReleaseStatus::Ready {
            self.transition(release_id, status, ReleaseStatus::Servable)
                .await?;
            status = ReleaseStatus::Servable;
        }
        if status != ReleaseStatus::Servable {
            return Err(LocalReleaseError::Conflict);
        }
        snapshot = self.snapshot().await?;
        Ok(outcome(
            release_id,
            None,
            status,
            snapshot.revision(),
            false,
            Vec::new(),
        ))
    }

    /// Moves one Channel to a compatible servable Release using repository CAS.
    ///
    /// The first promoted Channel becomes default when no default exists. `expected` optionally
    /// adds an operator-supplied precondition on top of the repository CAS.
    ///
    /// # Errors
    ///
    /// Rejects invalid states/preconditions and storage failures. Compatibility blockers are
    /// returned without moving the Channel.
    pub async fn promote(
        &self,
        channel: ChannelName,
        release_id: ReleaseId,
        expected: LocalChannelExpectation,
    ) -> Result<LocalReleaseOutcome, LocalReleaseError> {
        self.move_channel("promote", channel, release_id, expected)
            .await
    }

    /// Rolls a Channel back to an older compatible servable Release using exact expected-current
    /// CAS. Rollback never bypasses compatibility.
    ///
    /// # Errors
    ///
    /// Rejects stale expected Releases, invalid targets, incompatibility, or storage failures.
    pub async fn rollback(
        &self,
        channel: ChannelName,
        expected_current: ReleaseId,
        target: ReleaseId,
    ) -> Result<LocalReleaseOutcome, LocalReleaseError> {
        self.move_channel(
            "rollback",
            channel,
            target,
            LocalChannelExpectation::Release(expected_current),
        )
        .await
    }

    /// Returns one coherent read-only Release/Channel status report.
    ///
    /// # Errors
    ///
    /// Returns unavailable/corrupt state failures.
    pub async fn status(&self) -> Result<LocalReleaseStatusReport, LocalReleaseError> {
        let snapshot = match self.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(LocalReleaseError::NotFound) => {
                return Ok(LocalReleaseStatusReport {
                    serving_revision: 0,
                    default_channel: None,
                    releases: Vec::new(),
                    channels: Vec::new(),
                });
            }
            Err(error) => return Err(error),
        };
        let default_channel = snapshot.default_channel().cloned();
        let releases = snapshot
            .releases()
            .map(|release| LocalReleaseStatus {
                release_id: release.release_id,
                status: release.status,
                runtime_version: release.runtime_version.to_string(),
            })
            .collect();
        let channels = snapshot
            .channels()
            .map(|binding| LocalChannelStatus {
                default: default_channel.as_ref() == Some(&binding.channel),
                channel: binding.channel,
                release_id: binding.release_id,
            })
            .collect();
        Ok(LocalReleaseStatusReport {
            serving_revision: snapshot.revision(),
            default_channel,
            releases,
            channels,
        })
    }

    async fn move_channel(
        &self,
        operation: &'static str,
        channel: ChannelName,
        release_id: ReleaseId,
        expected: LocalChannelExpectation,
    ) -> Result<LocalReleaseOutcome, LocalReleaseError> {
        let snapshot = self.snapshot().await?;
        let current = snapshot.channel_release(&channel);
        let expectation_matches = match expected {
            LocalChannelExpectation::Current => true,
            LocalChannelExpectation::Empty => current.is_none(),
            LocalChannelExpectation::Release(expected) => current == Some(expected),
        };
        if !expectation_matches {
            return Err(LocalReleaseError::Conflict);
        }
        let target = snapshot
            .release(release_id)
            .ok_or(LocalReleaseError::NotFound)?;
        if !matches!(
            target.status,
            ReleaseStatus::Servable | ReleaseStatus::Active
        ) {
            return Err(LocalReleaseError::InvalidRequest);
        }
        if current == Some(release_id) {
            let revision = self.ensure_default(&snapshot, &channel).await?;
            self.reconcile_cron(release_id).await?;
            return Ok(outcome(
                release_id,
                Some(channel),
                ReleaseStatus::Active,
                revision,
                true,
                Vec::new(),
            ));
        }
        if let Some(base_id) = current {
            let base = self.package(base_id).await?;
            let candidate = self.package(release_id).await?;
            let report =
                CompatibilityEngine::compare(&base, &candidate).map_err(map_compatibility)?;
            if !report.compatible {
                return Ok(outcome(
                    release_id,
                    Some(channel),
                    target.status,
                    snapshot.revision(),
                    false,
                    diagnostics(report),
                ));
            }
        }
        self.repository
            .apply(
                self.state.scope(),
                operation_id(
                    operation,
                    &channel,
                    current,
                    Some(release_id),
                    snapshot.revision(),
                ),
                &ReleaseCommand::SetChannel {
                    channel: channel.clone(),
                    expected_release: current,
                    target_release: Some(release_id),
                },
            )
            .await
            .map_err(map_repository)?;
        let moved = self.snapshot().await?;
        let revision = self.ensure_default(&moved, &channel).await?;
        self.reconcile_cron(release_id).await?;
        Ok(outcome(
            release_id,
            Some(channel),
            ReleaseStatus::Active,
            revision,
            false,
            Vec::new(),
        ))
    }

    async fn ensure_default(
        &self,
        snapshot: &ServingSnapshot,
        channel: &ChannelName,
    ) -> Result<u64, LocalReleaseError> {
        if snapshot.default_channel().is_some() {
            return Ok(snapshot.revision());
        }
        let result = self
            .repository
            .apply(
                self.state.scope(),
                operation_id("default", channel, None, None, snapshot.revision()),
                &ReleaseCommand::SetDefaultChannel {
                    expected_channel: None,
                    target_channel: Some(channel.clone()),
                },
            )
            .await
            .map_err(map_repository)?;
        Ok(result.serving_revision)
    }

    async fn advance_if(
        &self,
        release_id: ReleaseId,
        current: ReleaseStatus,
        next: ReleaseStatus,
    ) -> Result<(), LocalReleaseError> {
        match current {
            ReleaseStatus::Created if next == ReleaseStatus::Building => {
                self.transition(release_id, current, next).await
            }
            ReleaseStatus::Building
            | ReleaseStatus::Validating
            | ReleaseStatus::CompatibilityBlocked
            | ReleaseStatus::Ready => Ok(()),
            _ => Err(LocalReleaseError::Conflict),
        }
    }

    async fn transition(
        &self,
        release_id: ReleaseId,
        expected: ReleaseStatus,
        next: ReleaseStatus,
    ) -> Result<(), LocalReleaseError> {
        let label = format!("{}:{}", expected.as_str(), next.as_str());
        let revision = self.snapshot().await?.revision();
        self.repository
            .apply(
                self.state.scope(),
                release_operation_id(&label, release_id, revision),
                &ReleaseCommand::Transition {
                    release_id,
                    expected,
                    next,
                },
            )
            .await
            .map_err(map_repository)?;
        Ok(())
    }

    async fn package(&self, release_id: ReleaseId) -> Result<ReleasePackage, LocalReleaseError> {
        let manifest = self
            .repository
            .manifest(self.state.scope(), release_id)
            .await
            .map_err(map_repository)?;
        let bytes = self
            .artifacts
            .get(&manifest.artifact)
            .await
            .map_err(map_repository)?;
        ReleasePackage::load(manifest, &bytes).map_err(map_compatibility)
    }

    async fn compatibility_report(
        &self,
        release_id: ReleaseId,
        baseline: Option<ReleaseId>,
    ) -> Result<CompatibilityReport, LocalReleaseError> {
        let candidate = self.package(release_id).await?;
        match baseline {
            Some(base_id) if base_id != release_id => {
                let base = self.package(base_id).await?;
                CompatibilityEngine::compare(&base, &candidate).map_err(map_compatibility)
            }
            _ => Ok(CompatibilityReport {
                compatible: true,
                diagnostics: Vec::new(),
            }),
        }
    }

    async fn reconcile_cron(&self, release_id: ReleaseId) -> Result<(), LocalReleaseError> {
        let manifest = self
            .repository
            .manifest(self.state.scope(), release_id)
            .await
            .map_err(map_repository)?;
        let bytes = encode_release_manifest(&manifest).map_err(map_repository)?;
        reconcile_release_cron(
            &self.state,
            &self.paths,
            &manifest,
            &bytes,
            current_timestamp()?,
        )
        .await
        .map_err(map_publish)
    }

    async fn snapshot(&self) -> Result<ServingSnapshot, LocalReleaseError> {
        self.repository
            .snapshot(self.state.scope())
            .await
            .map_err(map_repository)
    }
}

fn select_baseline(
    snapshot: &ServingSnapshot,
    selected: Option<&ChannelName>,
) -> Result<Option<ReleaseId>, LocalReleaseError> {
    if let Some(channel) = selected {
        return snapshot
            .channel_release(channel)
            .map(Some)
            .ok_or(LocalReleaseError::NotFound);
    }
    if let Some(channel) = snapshot.default_channel() {
        return snapshot
            .channel_release(channel)
            .map(Some)
            .ok_or(LocalReleaseError::Corruption);
    }
    if snapshot.channels().len() == 0 {
        Ok(None)
    } else {
        Err(LocalReleaseError::InvalidRequest)
    }
}

fn diagnostics(report: CompatibilityReport) -> Vec<LocalCompatibilityDiagnostic> {
    report
        .diagnostics
        .into_iter()
        .map(|diagnostic| LocalCompatibilityDiagnostic {
            code: diagnostic.code,
            subject: diagnostic.subject,
        })
        .collect()
}

fn outcome(
    release_id: ReleaseId,
    channel: Option<ChannelName>,
    status: ReleaseStatus,
    serving_revision: u64,
    replayed: bool,
    diagnostics: Vec<LocalCompatibilityDiagnostic>,
) -> LocalReleaseOutcome {
    LocalReleaseOutcome {
        release_id,
        channel,
        status,
        serving_revision,
        replayed,
        diagnostics,
    }
}

fn operation_id(
    operation: &str,
    channel: &ChannelName,
    from: Option<ReleaseId>,
    to: Option<ReleaseId>,
    observed_revision: u64,
) -> OperationId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_LOCAL_CHANNEL_OPERATION_V1");
    digest.update(operation.as_bytes());
    digest.update([0]);
    digest.update(channel.as_str().as_bytes());
    digest_optional_release(&mut digest, from);
    digest_optional_release(&mut digest, to);
    digest.update(observed_revision.to_be_bytes());
    operation_from_digest(digest.finalize().into())
}

fn release_operation_id(
    operation: &str,
    release_id: ReleaseId,
    observed_revision: u64,
) -> OperationId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_LOCAL_RELEASE_OPERATION_V1");
    digest.update(operation.as_bytes());
    digest.update([0]);
    digest.update(release_id.to_string().as_bytes());
    digest.update(observed_revision.to_be_bytes());
    operation_from_digest(digest.finalize().into())
}

fn digest_optional_release(digest: &mut Sha256, release_id: Option<ReleaseId>) {
    if let Some(release_id) = release_id {
        digest.update([1]);
        digest.update(release_id.to_string().as_bytes());
    } else {
        digest.update([0]);
    }
}

fn operation_from_digest(digest: [u8; 32]) -> OperationId {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    OperationId::from_ulid(Ulid::from_bytes(bytes))
}

fn map_repository(error: ReleaseError) -> LocalReleaseError {
    match error {
        ReleaseError::ReleaseNotFound | ReleaseError::ChannelNotFound | ReleaseError::NotFound => {
            LocalReleaseError::NotFound
        }
        ReleaseError::RepositoryConflict
        | ReleaseError::OperationIdReused
        | ReleaseError::InvalidTransition => LocalReleaseError::Conflict,
        ReleaseError::Unavailable | ReleaseError::ResultUncertain | ReleaseError::Busy => {
            LocalReleaseError::Unavailable
        }
        ReleaseError::Corruption | ReleaseError::InvalidSnapshot => LocalReleaseError::Corruption,
        ReleaseError::InvalidManifest
        | ReleaseError::InvalidArtifact
        | ReleaseError::DigestMismatch
        | ReleaseError::DescriptorMismatch
        | ReleaseError::LimitExceeded
        | ReleaseError::Unsupported
        | ReleaseError::ProductionBackendUnsupported
        | ReleaseError::DefaultChannelMissing
        | ReleaseError::ReleaseRetired
        | ReleaseError::ReleaseNotServable
        | ReleaseError::WorkspaceUnsupported
        | ReleaseError::Internal => LocalReleaseError::InvalidRequest,
    }
}

fn map_compatibility(error: runku_compatibility::CompatibilityError) -> LocalReleaseError {
    match error {
        runku_compatibility::CompatibilityError::InvalidRelease
        | runku_compatibility::CompatibilityError::LimitExceeded => {
            LocalReleaseError::InvalidRequest
        }
        runku_compatibility::CompatibilityError::InvalidArtifact
        | runku_compatibility::CompatibilityError::InvalidContract => LocalReleaseError::Corruption,
    }
}

fn map_publish(error: crate::LocalPublishError) -> LocalReleaseError {
    match error {
        crate::LocalPublishError::Conflict => LocalReleaseError::Conflict,
        crate::LocalPublishError::Unavailable => LocalReleaseError::Unavailable,
        crate::LocalPublishError::Corruption => LocalReleaseError::Corruption,
        crate::LocalPublishError::InvalidState
        | crate::LocalPublishError::InvalidPackage
        | crate::LocalPublishError::ProjectMismatch => LocalReleaseError::InvalidRequest,
    }
}

fn current_timestamp() -> Result<TimestampMicros, LocalReleaseError> {
    let elapsed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| LocalReleaseError::Unavailable)?;
    i64::try_from(elapsed.as_micros())
        .map(TimestampMicros::new)
        .map_err(|_| LocalReleaseError::Unavailable)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr};

    use runku_core::{
        BuildId, ChannelName, FunctionId, PinnedCode, ProjectId, ReleaseId, WorkspaceRef,
    };
    use runku_cron::{CronContext, CronRepository, CronRepositoryConfig, SqlCronRepository};
    use runku_development::DevelopmentActor;
    use runku_releases::{
        AuthPolicy, CronDefinition, FunctionManifest, FunctionType, FunctionVisibility,
        ReleaseManifestV1, ReleaseStatus, RuntimeClass, SafeEsmBundleV1, Sha256Digest,
        encode_release_manifest, encode_safe_esm_bundle,
    };
    use runku_value::TimestampMicros;
    use tempfile::tempdir;

    use super::{LocalChannelExpectation, LocalReleaseError, LocalReleaseManager};
    use crate::{initialize_local, publish_local};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    struct Package {
        release_id: ReleaseId,
        manifest: Vec<u8>,
        artifact: Vec<u8>,
    }

    #[tokio::test]
    async fn freshly_initialized_environment_has_an_empty_status_snapshot() -> TestResult {
        let directory = tempdir()?;
        initialize_local(
            directory.path(),
            WorkspaceRef::from_str("default")?,
            SocketAddr::from(([127, 0, 0, 1], 3289)),
            TimestampMicros::new(1),
        )
        .await?;

        let manager = LocalReleaseManager::open(directory.path()).await?;
        let status = manager.status().await?;
        assert_eq!(status.serving_revision, 0);
        assert_eq!(status.default_channel, None);
        assert!(status.releases.is_empty());
        assert!(status.channels.is_empty());
        Ok(())
    }

    fn package(
        project_id: ProjectId,
        sequence: u128,
        compatible_identity: bool,
    ) -> Result<Package, Box<dyn std::error::Error>> {
        let source = format!("export default () => ({sequence});");
        let implementation_hash = Sha256Digest::of(source.as_bytes());
        let bundle = SafeEsmBundleV1::from_sources([source])?;
        let artifact = encode_safe_esm_bundle(&bundle)?;
        let stable = Sha256Digest::of(b"stable-contract");
        let release_id = ReleaseId::from_ulid(ulid::Ulid::from(sequence + 100));
        let manifest = ReleaseManifestV1 {
            release_id,
            project_id,
            build_id: BuildId::from_ulid(ulid::Ulid::from(sequence + 200)),
            created_at: TimestampMicros::new(i64::try_from(sequence)?),
            runtime_version: "platform-js-1".parse()?,
            artifact: bundle.descriptor()?,
            function_contract_hash: stable,
            schema_contract_hash: stable,
            index_contract_hash: stable,
            functions: vec![
                FunctionManifest {
                    id: FunctionId::from_ulid(ulid::Ulid::from(850)),
                    name: "actions.cron".parse()?,
                    function_type: FunctionType::Action,
                    visibility: FunctionVisibility::Internal,
                    auth_policy: AuthPolicy::None,
                    runtime_class: RuntimeClass::SafeV8,
                    implementation_hash,
                    arguments_contract_hash: stable,
                    result_contract_hash: stable,
                    capabilities: Vec::new(),
                },
                FunctionManifest {
                    id: FunctionId::from_ulid(ulid::Ulid::from(if compatible_identity {
                        900
                    } else {
                        sequence + 900
                    })),
                    name: "queries.version".parse()?,
                    function_type: FunctionType::Query,
                    visibility: FunctionVisibility::Public,
                    auth_policy: AuthPolicy::None,
                    runtime_class: RuntimeClass::SafeV8,
                    implementation_hash,
                    arguments_contract_hash: stable,
                    result_contract_hash: stable,
                    capabilities: Vec::new(),
                },
            ],
            cron_definitions: vec![CronDefinition {
                name: "minute".parse()?,
                schedule: "* * * * *".parse()?,
                function: "actions.cron".parse()?,
                args: runku_value::CanonicalValue::Null,
            }],
        };
        Ok(Package {
            release_id,
            manifest: encode_release_manifest(&manifest)?,
            artifact,
        })
    }

    async fn publish(
        root: &std::path::Path,
        workspace: &WorkspaceRef,
        package: &Package,
    ) -> TestResult {
        publish_local(
            root,
            workspace,
            &DevelopmentActor::from_str("release-test")?,
            &package.manifest,
            &package.artifact,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn release_promote_rollback_status_and_blockers_are_durable() -> TestResult {
        let directory = tempdir()?;
        let workspace = WorkspaceRef::from_str("default")?;
        let (state, paths) = initialize_local(
            directory.path(),
            workspace.clone(),
            SocketAddr::from(([127, 0, 0, 1], 3290)),
            TimestampMicros::new(10),
        )
        .await?;
        let stable = ChannelName::from_str("stable")?;
        let first = package(state.project_id, 20, true)?;
        publish(directory.path(), &workspace, &first).await?;

        let manager = LocalReleaseManager::open(directory.path()).await?;
        let released = manager.release(first.release_id, None).await?;
        assert_eq!(released.status, ReleaseStatus::Servable);
        let promoted = manager
            .promote(
                stable.clone(),
                first.release_id,
                LocalChannelExpectation::Empty,
            )
            .await?;
        assert_eq!(promoted.status, ReleaseStatus::Active);
        let cron_context = CronContext {
            scope: state.scope(),
            environment: state.environment(),
        };
        let cron = SqlCronRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", paths.cron_database.display()),
            CronRepositoryConfig::LOCAL,
            cron_context,
        )
        .await?;
        assert_eq!(
            cron.snapshot(cron_context).await?.activations[0].pinned_code,
            PinnedCode::Release(first.release_id)
        );
        let status = manager.status().await?;
        assert_eq!(status.default_channel, Some(stable.clone()));
        assert_eq!(status.channels.len(), 1);
        assert!(status.channels[0].default);

        let second = package(state.project_id, 21, true)?;
        publish(directory.path(), &workspace, &second).await?;
        assert_eq!(
            manager.release(second.release_id, None).await?.status,
            ReleaseStatus::Servable
        );
        assert_eq!(
            manager
                .promote(
                    stable.clone(),
                    second.release_id,
                    LocalChannelExpectation::Release(first.release_id),
                )
                .await?
                .status,
            ReleaseStatus::Active
        );
        assert_eq!(
            cron.snapshot(cron_context).await?.activations[0].pinned_code,
            PinnedCode::Release(second.release_id)
        );
        assert_eq!(
            manager
                .rollback(stable.clone(), second.release_id, first.release_id)
                .await?
                .status,
            ReleaseStatus::Active
        );
        assert_eq!(
            cron.snapshot(cron_context).await?.activations[0].pinned_code,
            PinnedCode::Release(first.release_id)
        );
        assert!(
            manager
                .promote(
                    stable.clone(),
                    first.release_id,
                    LocalChannelExpectation::Release(first.release_id),
                )
                .await?
                .replayed
        );
        assert_eq!(
            manager
                .promote(
                    stable.clone(),
                    second.release_id,
                    LocalChannelExpectation::Release(first.release_id),
                )
                .await?
                .status,
            ReleaseStatus::Active
        );

        let incompatible = package(state.project_id, 22, false)?;
        publish(directory.path(), &workspace, &incompatible).await?;
        let blocked = manager.release(incompatible.release_id, None).await?;
        assert_eq!(blocked.status, ReleaseStatus::CompatibilityBlocked);
        assert_eq!(blocked.diagnostics.len(), 1);
        assert_eq!(
            blocked.diagnostics[0].code,
            "PUBLIC_FUNCTION_METADATA_CHANGED"
        );
        let blocked_revision = blocked.serving_revision;
        let blocked_replay = manager.release(incompatible.release_id, None).await?;
        assert!(blocked_replay.replayed);
        assert_eq!(blocked_replay.serving_revision, blocked_revision);
        assert_eq!(
            manager
                .promote(
                    stable,
                    second.release_id,
                    LocalChannelExpectation::Release(first.release_id),
                )
                .await,
            Err(LocalReleaseError::Conflict)
        );
        Ok(())
    }
}
