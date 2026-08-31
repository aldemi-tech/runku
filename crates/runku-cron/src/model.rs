//! Infrastructure-independent Cron activation contracts.

use std::fmt;

use async_trait::async_trait;
use runku_core::{
    EnvironmentDescriptor, EnvironmentScope, OperationId, PinnedCode, ReleaseId, WorkerId,
};
use runku_releases::{
    CronDefinition, CronName, CronSchedule, ReleaseManifestV1, decode_release_manifest,
    encode_release_manifest,
};
use runku_value::{CanonicalValue, TimestampMicros, encode_stored_value};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Trusted Environment metadata paired with the exact storage scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CronContext {
    /// Exact Project/Environment scope.
    pub scope: EnvironmentScope,
    /// Server-authoritative Environment descriptor.
    pub environment: EnvironmentDescriptor,
}

impl CronContext {
    /// Validates descriptor identity without excluding Production.
    ///
    /// # Errors
    ///
    /// Rejects a descriptor for another Environment.
    pub fn validate(self) -> Result<(), CronError> {
        if self.scope.environment_id() != self.environment.id() {
            return Err(CronError::InvalidInput);
        }
        Ok(())
    }
}

/// One enabled Cron activation, including its durable cursor and optional lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronActivation {
    /// Stable logical definition name.
    pub name: CronName,
    /// Repository revision that created this activation epoch.
    pub activation_revision: u64,
    /// Exact immutable code identity copied into every tick.
    pub pinned_code: PinnedCode,
    /// Candidate/stable Release that supplied function contracts.
    pub release_id: ReleaseId,
    /// Canonical UTC schedule.
    pub schedule: CronSchedule,
    /// Internal Mutation or Action destination.
    pub function: runku_core::FunctionName,
    /// Canonical arguments copied into every tick.
    pub args: CanonicalValue,
    /// Next logical tick not yet durably acknowledged.
    pub next_tick: TimestampMicros,
    /// Monotonic lease fence.
    pub lease_generation: u64,
    /// Current owner, if claimed.
    pub lease_owner: Option<WorkerId>,
    /// Current lease deadline, if claimed.
    pub lease_until: Option<TimestampMicros>,
}

impl CronActivation {
    /// Validates persisted cross-field invariants.
    ///
    /// # Errors
    ///
    /// Rejects zero revisions, malformed args, or half-present leases.
    pub fn validate(&self) -> Result<(), CronError> {
        if self.activation_revision == 0
            || self.next_tick.get() < 0
            || encode_stored_value(&self.args).is_err()
            || self.lease_owner.is_some() != self.lease_until.is_some()
            || self.lease_owner.is_some() && self.lease_generation == 0
        {
            return Err(CronError::Corruption);
        }
        Ok(())
    }
}

/// Activation returned under a fenced worker lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedCronActivation {
    /// Complete activation including owner/generation/deadline.
    pub activation: CronActivation,
}

/// Coherent local activation snapshot for status and outage-safe introspection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronSnapshot {
    /// Positive repository state revision, or zero before the first command.
    pub repository_revision: u64,
    /// Enabled activations in strict name order.
    pub activations: Vec<CronActivation>,
}

impl CronSnapshot {
    /// Validates ordering and every activation.
    ///
    /// # Errors
    ///
    /// Rejects duplicates, unordered names, or corrupt records.
    pub fn validate(&self) -> Result<(), CronError> {
        let mut previous: Option<&CronName> = None;
        for activation in &self.activations {
            activation.validate()?;
            if activation.activation_revision > self.repository_revision
                || previous.is_some_and(|name| name >= &activation.name)
            {
                return Err(CronError::Corruption);
            }
            previous = Some(&activation.name);
        }
        Ok(())
    }
}

/// Idempotent activation state command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CronCommand {
    /// Atomically replaces enabled definitions from one exact manifest/code pin.
    ActivateManifest {
        /// Required current repository revision; zero means no prior command.
        expected_revision: u64,
        /// Release or Dev Revision copied into future ticks.
        pinned_code: PinnedCode,
        /// Exact canonical Release Manifest bytes.
        manifest_bytes: Vec<u8>,
        /// Trusted activation time; first tick is strictly later.
        activated_at: TimestampMicros,
    },
    /// Atomically disables all definitions while preserving already materialized ticks.
    DeactivateAll {
        /// Required current repository revision.
        expected_revision: u64,
        /// Trusted audit timestamp.
        deactivated_at: TimestampMicros,
    },
}

impl CronCommand {
    /// Validates scope, pinning, canonical manifest, and timestamp before repository mutation.
    ///
    /// # Errors
    ///
    /// Rejects empty definition sets, project/pin drift, or noncanonical bytes.
    pub fn validate(&self, context: CronContext) -> Result<Option<ReleaseManifestV1>, CronError> {
        context.validate()?;
        match self {
            Self::ActivateManifest {
                pinned_code,
                manifest_bytes,
                activated_at,
                ..
            } => {
                if activated_at.get() < 0 {
                    return Err(CronError::InvalidInput);
                }
                let manifest = decode_release_manifest(manifest_bytes)
                    .map_err(|_| CronError::InvalidManifest)?;
                let canonical =
                    encode_release_manifest(&manifest).map_err(|_| CronError::InvalidManifest)?;
                if canonical != *manifest_bytes
                    || manifest.project_id != context.scope.project_id()
                    || manifest.cron_definitions.is_empty()
                    || matches!(pinned_code, PinnedCode::Release(id) if *id != manifest.release_id)
                {
                    return Err(CronError::InvalidManifest);
                }
                Ok(Some(manifest))
            }
            Self::DeactivateAll { deactivated_at, .. } => {
                if deactivated_at.get() < 0 {
                    return Err(CronError::InvalidInput);
                }
                Ok(None)
            }
        }
    }

    /// Returns the caller's compare-and-set repository revision.
    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        match self {
            Self::ActivateManifest {
                expected_revision, ..
            }
            | Self::DeactivateAll {
                expected_revision, ..
            } => *expected_revision,
        }
    }

    /// Computes a domain-separated exact command digest.
    ///
    /// # Errors
    ///
    /// Returns validation errors before hashing.
    pub fn digest(&self, context: CronContext) -> Result<[u8; 32], CronError> {
        self.validate(context)?;
        let mut digest = Sha256::new();
        digest.update(b"RUNKU_CRON_COMMAND_V1\0");
        digest.update(context.scope.project_id().to_string().as_bytes());
        digest.update([0]);
        digest.update(context.scope.environment_id().to_string().as_bytes());
        digest.update(self.expected_revision().to_be_bytes());
        match self {
            Self::ActivateManifest {
                pinned_code,
                manifest_bytes,
                activated_at,
                ..
            } => {
                digest.update([1]);
                digest.update(pinned_code.to_string().as_bytes());
                digest.update(activated_at.get().to_be_bytes());
                digest.update(manifest_bytes);
            }
            Self::DeactivateAll { deactivated_at, .. } => {
                digest.update([2]);
                digest.update(deactivated_at.get().to_be_bytes());
            }
        }
        Ok(digest.finalize().into())
    }
}

/// Successful activation command result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CronCommandResult {
    /// Repository revision produced by the original command.
    pub repository_revision: u64,
    /// Number of definitions enabled after the command.
    pub active_definitions: u32,
    /// Result recovered from the operation journal.
    pub replayed: bool,
}

/// Physical activation repository backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CronBackend {
    /// Embedded local `SQLite`.
    SQLite,
    /// Authoritative `PostgreSQL`.
    PostgreSQL,
}

/// Bounded process-local repository counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CronTelemetrySnapshot {
    /// Newly committed activation commands.
    pub commands: u64,
    /// Exact operation replays.
    pub replays: u64,
    /// Successfully claimed activations.
    pub claims: u64,
    /// Successfully advanced tick cursors.
    pub completions: u64,
    /// CAS or lease conflicts.
    pub conflicts: u64,
    /// Retryable repository errors.
    pub retryable_errors: u64,
}

/// Durable local Cron activation repository.
#[async_trait]
pub trait CronRepository: Send + Sync {
    /// Returns the physical backend.
    fn backend(&self) -> CronBackend;

    /// Applies one exact idempotent activation command.
    async fn apply(
        &self,
        context: CronContext,
        operation_id: OperationId,
        command: &CronCommand,
    ) -> Result<CronCommandResult, CronError>;

    /// Loads one coherent status snapshot.
    async fn snapshot(&self, context: CronContext) -> Result<CronSnapshot, CronError>;

    /// Claims due/expired activations in deterministic tick/name order.
    async fn claim_due(
        &self,
        context: CronContext,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<Vec<ClaimedCronActivation>, CronError>;

    /// Advances exactly one tick iff worker/generation/expected cursor still own the lease.
    #[allow(clippy::too_many_arguments)]
    async fn complete_tick(
        &self,
        context: CronContext,
        name: &CronName,
        worker_id: WorkerId,
        lease_generation: u64,
        expected_tick: TimestampMicros,
        next_tick: TimestampMicros,
        completed_at: TimestampMicros,
    ) -> Result<(), CronError>;

    /// Performs a bounded backend health check.
    async fn health(&self) -> Result<(), CronError>;

    /// Returns aggregate non-cardinal telemetry.
    fn telemetry(&self) -> CronTelemetrySnapshot;
}

/// Stable Cron activation/materialization failure taxonomy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CronError {
    /// Input shape, scope, timestamp, or configuration is invalid.
    #[error("cron input is invalid")]
    InvalidInput,
    /// Release manifest or code pin is invalid.
    #[error("cron release manifest is invalid")]
    InvalidManifest,
    /// Activation revision or operation identity conflicts.
    #[error("cron activation state changed concurrently")]
    Conflict,
    /// Persisted activation violates an invariant.
    #[error("cron repository state is corrupt")]
    Corruption,
    /// Worker no longer owns the exact activation lease.
    #[error("cron activation lease was lost")]
    LeaseLost,
    /// Configured count/size/time bound was exceeded.
    #[error("cron limit was exceeded")]
    LimitExceeded,
    /// Repository or `LogicalStore` is temporarily unavailable.
    #[error("cron dependency is unavailable")]
    Unavailable,
    /// Commit outcome is unknown and must be replayed.
    #[error("cron operation result is uncertain")]
    ResultUncertain,
    /// Backend/role/version is not supported.
    #[error("cron backend is unsupported")]
    Unsupported,
}

impl CronError {
    /// Stable machine code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "CRON_INPUT_INVALID",
            Self::InvalidManifest => "CRON_MANIFEST_INVALID",
            Self::Conflict => "CRON_CONFLICT",
            Self::Corruption => "CRON_CORRUPTION",
            Self::LeaseLost => "CRON_LEASE_LOST",
            Self::LimitExceeded => "CRON_LIMIT_EXCEEDED",
            Self::Unavailable => "CRON_UNAVAILABLE",
            Self::ResultUncertain => "CRON_RESULT_UNCERTAIN",
            Self::Unsupported => "CRON_BACKEND_UNSUPPORTED",
        }
    }

    /// Whether an unchanged operation may succeed later.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable | Self::ResultUncertain)
    }
}

impl fmt::Display for CronActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}@{}:{}",
            self.name,
            self.activation_revision,
            self.next_tick.get()
        )
    }
}

pub(crate) fn definitions(manifest: &ReleaseManifestV1) -> &[CronDefinition] {
    &manifest.cron_definitions
}
