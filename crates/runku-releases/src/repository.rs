//! Durable Release Repository boundary and commands.

use async_trait::async_trait;
use runku_core::{ChannelName, EnvironmentScope, OperationId, ReleaseId};
use sha2::{Digest, Sha256};

use crate::{
    ReleaseError, ReleaseManifestV1, ReleaseStatus, ServingSnapshot, decode_release_manifest,
};

/// One idempotent management mutation scoped to an Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseCommand {
    /// Associate an immutable validated manifest with this Environment in `CREATED` state.
    Register {
        /// Complete canonical Release Manifest v1 bytes.
        manifest_bytes: Vec<u8>,
    },
    /// Advance one Release through an explicit non-Channel lifecycle edge.
    Transition {
        /// Release association to mutate.
        release_id: ReleaseId,
        /// Required current state.
        expected: ReleaseStatus,
        /// Required next state.
        next: ReleaseStatus,
    },
    /// Create, move, or remove one Channel using compare-and-set semantics.
    SetChannel {
        /// Channel to mutate.
        channel: ChannelName,
        /// Required current binding; `None` requires absence.
        expected_release: Option<ReleaseId>,
        /// New binding; `None` removes the Channel.
        target_release: Option<ReleaseId>,
    },
    /// Select or clear the default Channel using compare-and-set semantics.
    SetDefaultChannel {
        /// Required current default; `None` requires absence.
        expected_channel: Option<ChannelName>,
        /// New default; `None` clears it.
        target_channel: Option<ChannelName>,
    },
}

impl ReleaseCommand {
    /// Validates pure command invariants before opening a transaction.
    ///
    /// # Errors
    ///
    /// Returns stable invalid/limit/transition errors.
    pub fn validate(&self, scope: EnvironmentScope) -> Result<(), ReleaseError> {
        match self {
            Self::Register { manifest_bytes } => {
                let manifest = decode_release_manifest(manifest_bytes)?;
                if manifest.project_id != scope.project_id() {
                    return Err(ReleaseError::InvalidManifest);
                }
            }
            Self::Transition { expected, next, .. } => {
                crate::ReleaseLifecycle::advance(*expected, *next)?;
            }
            Self::SetChannel {
                expected_release,
                target_release,
                ..
            } => {
                if expected_release == target_release {
                    return Err(ReleaseError::InvalidTransition);
                }
            }
            Self::SetDefaultChannel {
                expected_channel,
                target_channel,
            } => {
                if expected_channel == target_channel {
                    return Err(ReleaseError::InvalidTransition);
                }
            }
        }
        Ok(())
    }

    /// Computes a canonical SHA-256 digest used by the operation journal.
    ///
    /// # Errors
    ///
    /// Returns a stable validation/limit error.
    pub fn digest(&self, scope: EnvironmentScope) -> Result<[u8; 32], ReleaseError> {
        self.validate(scope)?;
        let mut digest = Sha256::new();
        digest.update(b"RUNKU_RELEASE_COMMAND_V1");
        digest.update(scope.project_id().to_string().as_bytes());
        digest.update([0]);
        digest.update(scope.environment_id().to_string().as_bytes());
        digest.update([0]);
        match self {
            Self::Register { manifest_bytes } => {
                digest.update([1]);
                digest.update(
                    u64::try_from(manifest_bytes.len())
                        .map_err(|_| ReleaseError::LimitExceeded)?
                        .to_be_bytes(),
                );
                digest.update(manifest_bytes);
            }
            Self::Transition {
                release_id,
                expected,
                next,
            } => {
                digest.update([2]);
                digest.update(release_id.to_string().as_bytes());
                digest.update([status_tag(*expected), status_tag(*next)]);
            }
            Self::SetChannel {
                channel,
                expected_release,
                target_release,
            } => {
                digest.update([3]);
                digest.update(channel.as_str().as_bytes());
                digest_optional_release(&mut digest, *expected_release);
                digest_optional_release(&mut digest, *target_release);
            }
            Self::SetDefaultChannel {
                expected_channel,
                target_channel,
            } => {
                digest.update([4]);
                digest_optional_channel(&mut digest, expected_channel.as_ref());
                digest_optional_channel(&mut digest, target_channel.as_ref());
            }
        }
        Ok(digest.finalize().into())
    }
}

/// Successful idempotent command result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleaseCommandResult {
    /// Environment serving revision produced or recovered by this operation.
    pub serving_revision: u64,
    /// Whether the result came from the durable operation journal.
    pub replayed: bool,
}

/// Physical repository backend selected by composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseRepositoryBackend {
    /// Embedded local `SQLite`.
    SQLite,
    /// Authoritative `PostgreSQL`.
    PostgreSQL,
}

/// Bounded process-local repository telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReleaseRepositoryTelemetrySnapshot {
    /// New commands committed.
    pub commands: u64,
    /// Idempotent command results replayed.
    pub replays: u64,
    /// Compare/state conflicts.
    pub conflicts: u64,
    /// Snapshots loaded.
    pub snapshots: u64,
    /// Retryable backend failures.
    pub retryable_errors: u64,
    /// Current physical pool size.
    pub pool_size: u32,
    /// Current idle physical connections.
    pub pool_idle: u32,
}

/// Durable repository for Release associations and Environment serving configuration.
#[async_trait]
pub trait ReleaseRepository: Send + Sync {
    /// Returns the selected physical backend.
    fn backend(&self) -> ReleaseRepositoryBackend;

    /// Atomically applies one validated idempotent command.
    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &ReleaseCommand,
    ) -> Result<ReleaseCommandResult, ReleaseError>;

    /// Loads and validates one complete immutable serving snapshot.
    async fn snapshot(&self, scope: EnvironmentScope) -> Result<ServingSnapshot, ReleaseError>;

    /// Loads and revalidates the canonical immutable manifest for one scoped Release.
    async fn manifest(
        &self,
        scope: EnvironmentScope,
        release_id: ReleaseId,
    ) -> Result<ReleaseManifestV1, ReleaseError>;

    /// Performs a lightweight backend health query.
    async fn health(&self) -> Result<(), ReleaseError>;

    /// Returns bounded process-local counters/pool gauges.
    fn telemetry(&self) -> ReleaseRepositoryTelemetrySnapshot;
}

fn digest_optional_release(digest: &mut Sha256, value: Option<ReleaseId>) {
    if let Some(value) = value {
        digest.update([1]);
        digest.update(value.to_string().as_bytes());
    } else {
        digest.update([0]);
    }
}

fn digest_optional_channel(digest: &mut Sha256, value: Option<&ChannelName>) {
    if let Some(value) = value {
        digest.update([1]);
        digest.update(value.as_str().as_bytes());
    } else {
        digest.update([0]);
    }
}

const fn status_tag(status: ReleaseStatus) -> u8 {
    match status {
        ReleaseStatus::Created => 1,
        ReleaseStatus::Building => 2,
        ReleaseStatus::BuildFailed => 3,
        ReleaseStatus::Validating => 4,
        ReleaseStatus::ValidationFailed => 5,
        ReleaseStatus::CompatibilityBlocked => 6,
        ReleaseStatus::MigrationRequired => 7,
        ReleaseStatus::Ready => 8,
        ReleaseStatus::Servable => 9,
        ReleaseStatus::Active => 10,
        ReleaseStatus::Deprecated => 11,
        ReleaseStatus::Retired => 12,
        ReleaseStatus::GarbageCollected => 13,
    }
}
