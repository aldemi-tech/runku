//! Infrastructure-independent Workspace repository contracts and pure snapshot router.

use std::{collections::BTreeMap, fmt, str::FromStr};

use async_trait::async_trait;
use runku_core::{
    DevRevisionId, EnvironmentDescriptor, EnvironmentScope, OperationId, PinnedCode, ReleaseId,
    WorkspaceId, WorkspaceRef,
};
use runku_releases::{
    ReleaseManifestV1, Sha256Digest, decode_release_manifest, encode_release_manifest,
};
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAX_ACTOR_BYTES: usize = 64;
const MAX_ENTRIES: usize = 10_000;

/// Bounded non-secret actor label recorded for local/shared development attribution.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DevelopmentActor(String);

impl DevelopmentActor {
    /// Returns the canonical actor label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DevelopmentActor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for DevelopmentActor {
    type Err = DevelopmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = value.bytes();
        let first = bytes.next().ok_or(DevelopmentError::InvalidInput)?;
        if value.len() > MAX_ACTOR_BYTES
            || !first.is_ascii_lowercase() && !first.is_ascii_digit()
            || !bytes.all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'-' | b'_' | b'.')
            })
        {
            return Err(DevelopmentError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Trusted Environment metadata paired with its tenant scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DevelopmentContext {
    /// Exact Project/Environment scope.
    pub scope: EnvironmentScope,
    /// Server-authoritative purpose/protection/location policy.
    pub environment: EnvironmentDescriptor,
}

impl DevelopmentContext {
    /// Validates scope/descriptor identity and development sync policy.
    ///
    /// # Errors
    ///
    /// Rejects cross-Environment descriptors and production/workspace-disabled contexts.
    pub fn validate(self) -> Result<(), DevelopmentError> {
        if self.scope.environment_id() != self.environment.id()
            || self.environment.validate_development_sync().is_err()
        {
            return Err(DevelopmentError::PolicyDenied);
        }
        Ok(())
    }
}

/// Immutable revision record containing exact candidate Release manifest bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentRevisionEntry {
    /// Immutable Dev Revision identity used for scheduling and attribution.
    pub revision_id: DevRevisionId,
    /// Candidate Release identity embedded in the manifest and returned by HTTP v1.
    pub release_id: ReleaseId,
    /// SHA-256 of canonical manifest bytes.
    pub manifest_digest: Sha256Digest,
    /// Canonical Release Manifest bytes reusable during freeze.
    pub manifest_bytes: Vec<u8>,
    /// Actor that published this immutable revision.
    pub actor: DevelopmentActor,
    /// Trusted publication timestamp.
    pub created_at: TimestampMicros,
}

impl DevelopmentRevisionEntry {
    /// Decodes and revalidates the embedded candidate manifest.
    ///
    /// # Errors
    ///
    /// Rejects project/Release/digest or canonical byte drift.
    pub fn manifest(&self, scope: EnvironmentScope) -> Result<ReleaseManifestV1, DevelopmentError> {
        let manifest = decode_release_manifest(&self.manifest_bytes)
            .map_err(|_| DevelopmentError::InvalidRevision)?;
        let canonical =
            encode_release_manifest(&manifest).map_err(|_| DevelopmentError::InvalidRevision)?;
        if manifest.project_id != scope.project_id()
            || manifest.release_id != self.release_id
            || canonical != self.manifest_bytes
            || Sha256Digest::of(&self.manifest_bytes) != self.manifest_digest
        {
            return Err(DevelopmentError::InvalidRevision);
        }
        Ok(manifest)
    }
}

/// Exact mutable Workspace pointer captured in a coherent snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceBinding {
    /// Durable Workspace resource identity.
    pub workspace_id: WorkspaceId,
    /// Human-readable target reference.
    pub workspace_ref: WorkspaceRef,
    /// Current immutable HEAD.
    pub head_revision: Option<DevRevisionId>,
    /// Actor that created or last moved this pointer.
    pub updated_by: DevelopmentActor,
    /// Trusted last-update timestamp.
    pub updated_at: TimestampMicros,
}

/// Result pinned from one immutable Development serving snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentResolution {
    /// Trusted Environment scope.
    pub scope: EnvironmentScope,
    /// Development serving revision used for lookup.
    pub serving_revision: u64,
    /// Workspace that was resolved.
    pub workspace_ref: WorkspaceRef,
    /// Immutable Dev Revision pin.
    pub revision: DevelopmentRevisionEntry,
    /// Fully decoded and revalidated candidate Release manifest.
    pub manifest: ReleaseManifestV1,
}

impl DevelopmentResolution {
    /// Returns the immutable scheduler/telemetry identity.
    #[must_use]
    pub const fn pinned_code(&self) -> PinnedCode {
        PinnedCode::DevRevision(self.revision.revision_id)
    }
}

/// Result resolved directly from one immutable Development Revision identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentRevisionResolution {
    /// Trusted Environment scope.
    pub scope: EnvironmentScope,
    /// Development serving revision containing the immutable entry.
    pub serving_revision: u64,
    /// Exact immutable revision entry.
    pub revision: DevelopmentRevisionEntry,
    /// Fully decoded and revalidated candidate Release manifest.
    pub manifest: ReleaseManifestV1,
}

/// Complete coherent serving view for Development Workspaces in one Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentSnapshot {
    scope: EnvironmentScope,
    revision: u64,
    revisions: BTreeMap<DevRevisionId, DevelopmentRevisionEntry>,
    workspaces: BTreeMap<WorkspaceRef, WorkspaceBinding>,
}

impl DevelopmentSnapshot {
    /// Constructs and validates a closed snapshot.
    ///
    /// # Errors
    ///
    /// Rejects zero revision, limits, duplicates, invalid manifests, and dangling HEADs.
    pub fn new(
        scope: EnvironmentScope,
        revision: u64,
        revisions: Vec<DevelopmentRevisionEntry>,
        workspaces: Vec<WorkspaceBinding>,
    ) -> Result<Self, DevelopmentError> {
        if revision == 0 || revisions.len() > MAX_ENTRIES || workspaces.len() > MAX_ENTRIES {
            return Err(DevelopmentError::InvalidSnapshot);
        }
        let mut revision_map = BTreeMap::new();
        let mut releases = BTreeMap::new();
        for entry in revisions {
            entry
                .manifest(scope)
                .map_err(|_| DevelopmentError::InvalidSnapshot)?;
            if releases
                .insert(entry.release_id, entry.revision_id)
                .is_some()
                || revision_map.insert(entry.revision_id, entry).is_some()
            {
                return Err(DevelopmentError::InvalidSnapshot);
            }
        }
        let mut workspace_ids = BTreeMap::new();
        let mut workspace_map = BTreeMap::new();
        for workspace in workspaces {
            if workspace
                .head_revision
                .is_some_and(|head| !revision_map.contains_key(&head))
                || workspace_ids
                    .insert(workspace.workspace_id, workspace.workspace_ref.clone())
                    .is_some()
                || workspace_map
                    .insert(workspace.workspace_ref.clone(), workspace)
                    .is_some()
            {
                return Err(DevelopmentError::InvalidSnapshot);
            }
        }
        Ok(Self {
            scope,
            revision,
            revisions: revision_map,
            workspaces: workspace_map,
        })
    }

    /// Returns the fixed Environment scope.
    #[must_use]
    pub const fn scope(&self) -> EnvironmentScope {
        self.scope
    }

    /// Returns the positive monotonic serving revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns one immutable Workspace binding without resolving its optional HEAD.
    #[must_use]
    pub fn workspace_binding(&self, workspace_ref: &WorkspaceRef) -> Option<&WorkspaceBinding> {
        self.workspaces.get(workspace_ref)
    }

    /// Resolves one Workspace HEAD exactly once.
    ///
    /// # Errors
    ///
    /// Returns distinct unknown/empty/corrupt failures with no Release fallback.
    pub fn resolve(
        &self,
        workspace_ref: &WorkspaceRef,
    ) -> Result<DevelopmentResolution, DevelopmentError> {
        let workspace = self
            .workspaces
            .get(workspace_ref)
            .ok_or(DevelopmentError::WorkspaceNotFound)?;
        let head = workspace
            .head_revision
            .ok_or(DevelopmentError::WorkspaceEmpty)?;
        let revision = self
            .revisions
            .get(&head)
            .ok_or(DevelopmentError::Corruption)?
            .clone();
        let manifest = revision.manifest(self.scope)?;
        Ok(DevelopmentResolution {
            scope: self.scope,
            serving_revision: self.revision,
            workspace_ref: workspace_ref.clone(),
            revision,
            manifest,
        })
    }

    /// Resolves an immutable Development Revision without consulting any Workspace HEAD.
    ///
    /// # Errors
    ///
    /// Returns not-found or corruption when the exact revision is unavailable or invalid.
    pub fn resolve_revision(
        &self,
        revision_id: DevRevisionId,
    ) -> Result<DevelopmentRevisionResolution, DevelopmentError> {
        let revision = self
            .revisions
            .get(&revision_id)
            .ok_or(DevelopmentError::RevisionNotFound)?
            .clone();
        let manifest = revision.manifest(self.scope)?;
        Ok(DevelopmentRevisionResolution {
            scope: self.scope,
            serving_revision: self.revision,
            revision,
            manifest,
        })
    }

    /// Resolves the immutable Development Revision registered for one exact candidate Release.
    ///
    /// # Errors
    ///
    /// Returns not-found or corruption when the candidate is not a Development publication in
    /// this scope or its immutable revision is unavailable.
    pub fn resolve_release(
        &self,
        release_id: ReleaseId,
    ) -> Result<DevelopmentRevisionResolution, DevelopmentError> {
        let revision_id = self
            .revisions
            .values()
            .find(|revision| revision.release_id == release_id)
            .map(|revision| revision.revision_id)
            .ok_or(DevelopmentError::RevisionNotFound)?;
        self.resolve_revision(revision_id)
    }
}

/// Idempotent Development Workspace state command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DevelopmentCommand {
    /// Creates one empty Workspace pointer.
    CreateWorkspace {
        /// Durable Workspace identity.
        workspace_id: WorkspaceId,
        /// Human-readable target reference.
        workspace_ref: WorkspaceRef,
        /// Creating actor.
        actor: DevelopmentActor,
        /// Trusted creation time.
        created_at: TimestampMicros,
    },
    /// Atomically registers an immutable revision and compare-and-sets Workspace HEAD.
    PublishRevision {
        /// Existing Workspace reference.
        workspace_ref: WorkspaceRef,
        /// Required current HEAD; `None` requires an empty Workspace.
        expected_head: Option<DevRevisionId>,
        /// New immutable revision.
        revision: DevelopmentRevisionEntry,
    },
}

impl DevelopmentCommand {
    /// Validates command and candidate manifest before repository mutation.
    ///
    /// # Errors
    ///
    /// Rejects policy, scope, manifest, timestamp, or no-op violations.
    pub fn validate(&self, context: DevelopmentContext) -> Result<(), DevelopmentError> {
        context.validate()?;
        match self {
            Self::CreateWorkspace { created_at, .. } => {
                if created_at.get() < 0 {
                    return Err(DevelopmentError::InvalidInput);
                }
            }
            Self::PublishRevision {
                expected_head,
                revision,
                ..
            } => {
                if expected_head == &Some(revision.revision_id) || revision.created_at.get() < 0 {
                    return Err(DevelopmentError::InvalidInput);
                }
                revision.manifest(context.scope)?;
            }
        }
        Ok(())
    }

    /// Computes the domain-separated canonical command digest for the operation journal.
    ///
    /// # Errors
    ///
    /// Returns validation errors before hashing.
    pub fn digest(&self, context: DevelopmentContext) -> Result<[u8; 32], DevelopmentError> {
        self.validate(context)?;
        let mut digest = Sha256::new();
        digest.update(b"RUNKU_DEVELOPMENT_COMMAND_V1\0");
        digest.update(context.scope.project_id().to_string().as_bytes());
        digest.update([0]);
        digest.update(context.scope.environment_id().to_string().as_bytes());
        digest.update([0]);
        match self {
            Self::CreateWorkspace {
                workspace_id,
                workspace_ref,
                actor,
                created_at,
            } => {
                digest.update([1]);
                digest.update(workspace_id.to_string().as_bytes());
                digest.update([0]);
                digest.update(workspace_ref.as_str().as_bytes());
                digest.update([0]);
                digest.update(actor.as_str().as_bytes());
                digest.update(created_at.get().to_be_bytes());
            }
            Self::PublishRevision {
                workspace_ref,
                expected_head,
                revision,
            } => {
                digest.update([2]);
                digest.update(workspace_ref.as_str().as_bytes());
                digest.update([0]);
                if let Some(head) = expected_head {
                    digest.update([1]);
                    digest.update(head.to_string().as_bytes());
                } else {
                    digest.update([0]);
                }
                digest.update(revision.revision_id.to_string().as_bytes());
                digest.update(revision.release_id.to_string().as_bytes());
                digest.update(revision.manifest_digest.as_bytes());
                digest.update(revision.actor.as_str().as_bytes());
                digest.update(revision.created_at.get().to_be_bytes());
            }
        }
        Ok(digest.finalize().into())
    }
}

/// Successful journaled Development command outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DevelopmentCommandResult {
    /// Monotonic serving revision produced by the original command.
    pub serving_revision: u64,
    /// Result was recovered from the operation journal.
    pub replayed: bool,
    /// HEAD after the command, absent for an empty Workspace create.
    pub head_revision: Option<DevRevisionId>,
}

/// Physical repository backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentBackend {
    /// Embedded local `SQLite`.
    SQLite,
    /// Authoritative shared `PostgreSQL`.
    PostgreSQL,
}

/// Bounded process-local repository counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DevelopmentTelemetrySnapshot {
    /// Newly committed commands.
    pub commands: u64,
    /// Exact journal replays.
    pub replays: u64,
    /// CAS/identity conflicts.
    pub conflicts: u64,
    /// Coherent snapshots loaded.
    pub snapshots: u64,
    /// Retryable backend failures.
    pub retryable_errors: u64,
}

/// Durable Development Workspace repository.
#[async_trait]
pub trait DevelopmentRepository: Send + Sync {
    /// Returns the explicit physical backend.
    fn backend(&self) -> DevelopmentBackend;

    /// Atomically applies one validated idempotent command.
    async fn apply(
        &self,
        context: DevelopmentContext,
        operation_id: OperationId,
        command: &DevelopmentCommand,
    ) -> Result<DevelopmentCommandResult, DevelopmentError>;

    /// Loads one coherent immutable serving snapshot.
    async fn snapshot(
        &self,
        context: DevelopmentContext,
    ) -> Result<DevelopmentSnapshot, DevelopmentError>;

    /// Performs a bounded backend health check.
    async fn health(&self) -> Result<(), DevelopmentError>;

    /// Returns aggregate non-cardinal telemetry.
    fn telemetry(&self) -> DevelopmentTelemetrySnapshot;
}

/// Stable Development Workspace failure taxonomy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DevelopmentError {
    /// Input grammar, timestamp, or command shape is invalid.
    #[error("development input is invalid")]
    InvalidInput,
    /// Trusted Environment policy forbids Workspace operations.
    #[error("development operation is forbidden by Environment policy")]
    PolicyDenied,
    /// Candidate manifest/revision binding is invalid.
    #[error("development revision is invalid")]
    InvalidRevision,
    /// Snapshot invariants do not hold.
    #[error("development snapshot is invalid")]
    InvalidSnapshot,
    /// Workspace does not exist in this Environment.
    #[error("development workspace was not found")]
    WorkspaceNotFound,
    /// Workspace exists but has no valid build HEAD.
    #[error("development workspace has no revision")]
    WorkspaceEmpty,
    /// Immutable Development Revision does not exist in this Environment snapshot.
    #[error("development revision was not found")]
    RevisionNotFound,
    /// Compare-and-set, duplicate identity, or divergent replay conflict.
    #[error("development state changed concurrently")]
    Conflict,
    /// Stored state violates an invariant.
    #[error("development repository state is corrupt")]
    Corruption,
    /// Configured limits were exceeded.
    #[error("development repository limit exceeded")]
    LimitExceeded,
    /// Backend is unavailable or busy.
    #[error("development repository is unavailable")]
    Unavailable,
    /// Commit outcome is unknown and must be recovered by operation ID.
    #[error("development command result is uncertain")]
    ResultUncertain,
    /// Selected backend/role combination is unsupported.
    #[error("development repository backend is unsupported")]
    Unsupported,
}

impl DevelopmentError {
    /// Stable machine code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "DEVELOPMENT_INPUT_INVALID",
            Self::PolicyDenied => "DEVELOPMENT_POLICY_DENIED",
            Self::InvalidRevision => "DEV_REVISION_INVALID",
            Self::InvalidSnapshot => "DEVELOPMENT_SNAPSHOT_INVALID",
            Self::WorkspaceNotFound => "WORKSPACE_NOT_FOUND",
            Self::WorkspaceEmpty => "WORKSPACE_EMPTY",
            Self::RevisionNotFound => "DEV_REVISION_NOT_FOUND",
            Self::Conflict => "DEVELOPMENT_CONFLICT",
            Self::Corruption => "DEVELOPMENT_CORRUPTION",
            Self::LimitExceeded => "DEVELOPMENT_LIMIT_EXCEEDED",
            Self::Unavailable => "DEVELOPMENT_UNAVAILABLE",
            Self::ResultUncertain => "DEVELOPMENT_RESULT_UNCERTAIN",
            Self::Unsupported => "DEVELOPMENT_BACKEND_UNSUPPORTED",
        }
    }

    /// Whether an unchanged request may succeed later.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable | Self::ResultUncertain)
    }
}
