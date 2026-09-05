//! Weighted serving policy values, canonical compatibility evidence, and transitions.

use std::collections::BTreeSet;

use runku_core::{EnvironmentScope, OperationId, OperatorId, ProjectId, ReleaseId};
use runku_releases::{ReleaseManifestV1, Sha256Digest};
use runku_value::{TimestampMicros, encode_stored_value};
use sha2::{Digest, Sha256};

use crate::ServingPolicyError;

const MAX_SERVING_RELEASES: usize = 16;
const MAX_AUDIT_PAGE_SIZE: u16 = 100;

/// Strategy used to replace or distribute future serving traffic.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ServingMode {
    /// All future traffic moves to one Release in one policy revision.
    Atomic,
    /// Future traffic is distributed between at least two compatible Releases.
    Gradual,
}

impl ServingMode {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Atomic => "atomic",
            Self::Gradual => "gradual",
        }
    }

    /// Parses the canonical persisted spelling.
    ///
    /// # Errors
    ///
    /// Unknown values fail closed as repository corruption.
    pub fn from_persisted(value: &str) -> Result<Self, ServingPolicyError> {
        match value {
            "atomic" => Ok(Self::Atomic),
            "gradual" => Ok(Self::Gradual),
            _ => Err(ServingPolicyError::Corruption),
        }
    }
}

/// Canonical data and Cron compatibility evidence for one immutable Release.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ServingContractHashes {
    /// Canonical logical schema contract hash from the Release Manifest.
    pub schema: Sha256Digest,
    /// Canonical logical index contract hash from the Release Manifest.
    pub indexes: Sha256Digest,
    /// Canonical hash of the complete ordered Cron declarations.
    pub cron_declarations: Sha256Digest,
}

/// One Release and positive integral percentage in a serving set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServingRelease {
    release_id: ReleaseId,
    weight_percent: u8,
    contracts: ServingContractHashes,
}

impl ServingRelease {
    /// Creates an entry from already authenticated canonical Release contract hashes.
    ///
    /// This constructor is used by durable adapters when restoring validated state. Management
    /// callers should prefer [`Self::from_manifest`] so hashes cannot be supplied independently.
    ///
    /// # Errors
    ///
    /// Zero or greater-than-100 weights are rejected.
    pub fn new(
        release_id: ReleaseId,
        weight_percent: u8,
        contracts: ServingContractHashes,
    ) -> Result<Self, ServingPolicyError> {
        if weight_percent == 0 || weight_percent > 100 {
            return Err(ServingPolicyError::InvalidInput);
        }
        Ok(Self {
            release_id,
            weight_percent,
            contracts,
        })
    }

    /// Derives one entry from a validated canonical Release Manifest.
    ///
    /// # Errors
    ///
    /// Rejects invalid manifests, cross-Project manifests, or invalid weights.
    pub fn from_manifest(
        scope: EnvironmentScope,
        manifest: &ReleaseManifestV1,
        weight_percent: u8,
    ) -> Result<Self, ServingPolicyError> {
        manifest.validate().map_err(map_manifest_error)?;
        if manifest.project_id != scope.project_id() {
            return Err(ServingPolicyError::InvalidInput);
        }
        Self::new(
            manifest.release_id,
            weight_percent,
            ServingContractHashes {
                schema: manifest.schema_contract_hash,
                indexes: manifest.index_contract_hash,
                cron_declarations: canonical_cron_declarations_hash(manifest)?,
            },
        )
    }

    /// Exact immutable Release identity.
    #[must_use]
    pub const fn release_id(self) -> ReleaseId {
        self.release_id
    }

    /// Positive integral percentage assigned to this Release.
    #[must_use]
    pub const fn weight_percent(self) -> u8 {
        self.weight_percent
    }

    /// Canonical compatibility evidence.
    #[must_use]
    pub const fn contracts(self) -> ServingContractHashes {
        self.contracts
    }
}

/// Complete desired weighted policy for one Project's Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingPolicy {
    project_id: ProjectId,
    mode: ServingMode,
    releases: Vec<ServingRelease>,
}

impl ServingPolicy {
    /// Creates, canonicalizes, and validates a policy from authenticated hash evidence.
    ///
    /// # Errors
    ///
    /// Rejects duplicates, invalid weights/counts, invalid mode shape, and incompatible contracts.
    pub fn new(
        project_id: ProjectId,
        mode: ServingMode,
        mut releases: Vec<ServingRelease>,
    ) -> Result<Self, ServingPolicyError> {
        releases.sort_by_key(|entry| entry.release_id);
        let policy = Self {
            project_id,
            mode,
            releases,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Creates a policy while deriving all contract hashes from canonical Release Manifests.
    ///
    /// The manifests must have been loaded through the exact Environment-scoped Release authority;
    /// manifests themselves carry Project but not Environment identity.
    ///
    /// # Errors
    ///
    /// Returns manifest, scope, weight, duplicate, shape, or compatibility errors.
    pub fn from_manifests<'a, I>(
        scope: EnvironmentScope,
        mode: ServingMode,
        manifests: I,
    ) -> Result<Self, ServingPolicyError>
    where
        I: IntoIterator<Item = (&'a ReleaseManifestV1, u8)>,
    {
        let releases = manifests
            .into_iter()
            .map(|(manifest, weight)| ServingRelease::from_manifest(scope, manifest, weight))
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(scope.project_id(), mode, releases)
    }

    /// Revalidates all cross-field and canonical-order invariants.
    ///
    /// # Errors
    ///
    /// Rejects invalid persisted or caller-constructed policy state.
    pub fn validate(&self) -> Result<(), ServingPolicyError> {
        if self.releases.is_empty() || self.releases.len() > MAX_SERVING_RELEASES {
            return Err(ServingPolicyError::LimitExceeded);
        }
        let mut total = 0_u16;
        let mut seen = BTreeSet::new();
        let mut previous = None;
        let baseline = self.releases[0].contracts;
        for entry in &self.releases {
            if entry.weight_percent == 0
                || entry.weight_percent > 100
                || !seen.insert(entry.release_id)
                || previous.is_some_and(|value| value >= entry.release_id)
            {
                return Err(ServingPolicyError::InvalidInput);
            }
            total = total
                .checked_add(u16::from(entry.weight_percent))
                .ok_or(ServingPolicyError::LimitExceeded)?;
            previous = Some(entry.release_id);
            if entry.contracts != baseline {
                return Err(ServingPolicyError::IncompatibleContracts);
            }
        }
        if total != 100
            || matches!(self.mode, ServingMode::Atomic) && self.releases.len() != 1
            || matches!(self.mode, ServingMode::Gradual) && self.releases.len() < 2
        {
            return Err(ServingPolicyError::InvalidInput);
        }
        Ok(())
    }

    /// Project owning every referenced Release.
    #[must_use]
    pub const fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// Selected rollout strategy.
    #[must_use]
    pub const fn mode(&self) -> ServingMode {
        self.mode
    }

    /// Canonically Release-ID-ordered weighted entries.
    #[must_use]
    pub fn releases(&self) -> &[ServingRelease] {
        &self.releases
    }

    /// Selects exactly one Release for a deterministic percentile in `0..100`.
    ///
    /// # Errors
    ///
    /// Rejects an out-of-range percentile or corrupt policy weights.
    pub fn select_percentile(&self, percentile: u8) -> Result<ReleaseId, ServingPolicyError> {
        self.validate()?;
        if percentile >= 100 {
            return Err(ServingPolicyError::InvalidInput);
        }
        let mut upper = 0_u16;
        for release in &self.releases {
            upper = upper
                .checked_add(u16::from(release.weight_percent))
                .ok_or(ServingPolicyError::LimitExceeded)?;
            if u16::from(percentile) < upper {
                return Ok(release.release_id);
            }
        }
        Err(ServingPolicyError::Corruption)
    }

    /// Canonical digest of the complete desired policy and compatibility evidence.
    #[must_use]
    pub fn digest(&self) -> Sha256Digest {
        let mut digest = Sha256::new();
        digest.update(b"RUNKU_SERVING_POLICY_V1\0");
        digest.update(self.project_id.to_string().as_bytes());
        digest.update([0]);
        digest.update([mode_tag(self.mode)]);
        digest.update(
            u64::try_from(self.releases.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for entry in &self.releases {
            digest.update(entry.release_id.to_string().as_bytes());
            digest.update([0, entry.weight_percent]);
            digest.update(entry.contracts.schema.as_bytes());
            digest.update(entry.contracts.indexes.as_bytes());
            digest.update(entry.contracts.cron_declarations.as_bytes());
        }
        Sha256Digest::from_bytes(digest.finalize().into())
    }
}

/// Last observed application of one desired serving policy revision.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ServingObservedState {
    /// The desired policy has not been confirmed on the serving path.
    Pending,
    /// The exact policy revision was applied successfully.
    Ready,
    /// Applying the exact policy revision failed.
    Failed,
}

impl ServingObservedState {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }

    /// Parses the canonical persisted spelling.
    ///
    /// # Errors
    ///
    /// Unknown values fail closed as corruption.
    pub fn from_persisted(value: &str) -> Result<Self, ServingPolicyError> {
        match value {
            "pending" => Ok(Self::Pending),
            "ready" => Ok(Self::Ready),
            "failed" => Ok(Self::Failed),
            _ => Err(ServingPolicyError::Corruption),
        }
    }
}

/// Trusted serving-path materialization outcome.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ServingMaterializationOutcome {
    /// The exact desired policy is active on the serving path.
    Ready,
    /// Applying the exact desired policy failed.
    Failed,
}

impl ServingMaterializationOutcome {
    const fn observed_state(self) -> ServingObservedState {
        match self {
            Self::Ready => ServingObservedState::Ready,
            Self::Failed => ServingObservedState::Failed,
        }
    }
}

/// Authoritative desired/observed policy record for one exact Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingPolicyRecord {
    /// Exact Project/Environment identity.
    pub scope: EnvironmentScope,
    /// Current desired weighted policy.
    pub desired_policy: ServingPolicy,
    /// Positive compare-and-set desired policy revision.
    pub policy_revision: u64,
    /// Last observed serving-path state.
    pub observed_state: ServingObservedState,
    /// Exact policy revision represented by the last observation.
    pub observed_policy_revision: Option<u64>,
    /// Initial policy creation timestamp.
    pub created_at: TimestampMicros,
    /// Last desired policy change timestamp.
    pub updated_at: TimestampMicros,
    /// Last materializer observation timestamp.
    pub observed_at: Option<TimestampMicros>,
}

impl ServingPolicyRecord {
    /// Validates persisted desired/observed and timestamp relationships.
    ///
    /// # Errors
    ///
    /// Rejects scope, revision, compatibility, and observation drift.
    pub fn validate(&self) -> Result<(), ServingPolicyError> {
        self.desired_policy
            .validate()
            .map_err(|_| ServingPolicyError::Corruption)?;
        if self.desired_policy.project_id != self.scope.project_id()
            || self.policy_revision == 0
            || self.created_at.get() < 0
            || self.updated_at < self.created_at
            || self
                .observed_at
                .is_some_and(|timestamp| timestamp < self.created_at)
            || self.observed_at.is_some() != self.observed_policy_revision.is_some()
            || self
                .observed_policy_revision
                .is_some_and(|revision| revision == 0 || revision > self.policy_revision)
        {
            return Err(ServingPolicyError::Corruption);
        }
        match self.observed_state {
            ServingObservedState::Pending => {
                if self
                    .observed_policy_revision
                    .is_some_and(|revision| revision >= self.policy_revision)
                    || self
                        .observed_at
                        .is_some_and(|timestamp| timestamp > self.updated_at)
                {
                    return Err(ServingPolicyError::Corruption);
                }
            }
            ServingObservedState::Ready | ServingObservedState::Failed => {
                if self.observed_policy_revision != Some(self.policy_revision)
                    || self
                        .observed_at
                        .is_none_or(|timestamp| timestamp < self.updated_at)
                {
                    return Err(ServingPolicyError::Corruption);
                }
            }
        }
        Ok(())
    }

    /// Whether the exact desired policy revision is observed ready.
    #[must_use]
    pub fn is_converged(&self) -> bool {
        self.observed_state == ServingObservedState::Ready
            && self.observed_policy_revision == Some(self.policy_revision)
    }
}

/// One durable idempotent serving-policy command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServingCommand {
    /// Creates or replaces the complete desired policy using compare-and-set.
    SetDesired {
        /// Authenticated operator attributed in the immutable audit trail.
        actor: OperatorId,
        /// `None` requires absence; `Some` requires that exact current revision.
        expected_revision: Option<u64>,
        /// Complete replacement policy.
        policy: ServingPolicy,
        /// Trusted management timestamp.
        changed_at: TimestampMicros,
    },
    /// Records serving-path application of one exact desired revision.
    Materialize {
        /// Required current desired revision.
        expected_revision: u64,
        /// Trusted application outcome.
        outcome: ServingMaterializationOutcome,
        /// Trusted observation timestamp.
        observed_at: TimestampMicros,
    },
}

impl ServingCommand {
    /// Validates command-local invariants for an exact Environment scope.
    ///
    /// # Errors
    ///
    /// Rejects mismatched Projects, zero revisions, invalid policies, or negative timestamps.
    pub fn validate(&self, scope: EnvironmentScope) -> Result<(), ServingPolicyError> {
        match self {
            Self::SetDesired {
                actor: _,
                expected_revision,
                policy,
                changed_at,
            } => {
                policy.validate()?;
                if policy.project_id != scope.project_id()
                    || expected_revision.is_some_and(|revision| revision == 0)
                    || changed_at.get() < 0
                {
                    return Err(ServingPolicyError::InvalidInput);
                }
            }
            Self::Materialize {
                expected_revision,
                observed_at,
                ..
            } => {
                if *expected_revision == 0 || observed_at.get() < 0 {
                    return Err(ServingPolicyError::InvalidInput);
                }
            }
        }
        Ok(())
    }

    /// Computes the exact-scope canonical operation-journal digest.
    ///
    /// # Errors
    ///
    /// Returns the same input failures as [`Self::validate`].
    pub fn digest(&self, scope: EnvironmentScope) -> Result<[u8; 32], ServingPolicyError> {
        self.validate(scope)?;
        let mut digest = Sha256::new();
        digest.update(b"RUNKU_SERVING_COMMAND_V1\0");
        digest.update(scope.project_id().to_string().as_bytes());
        digest.update([0]);
        digest.update(scope.environment_id().to_string().as_bytes());
        digest.update([0]);
        match self {
            Self::SetDesired {
                actor,
                expected_revision,
                policy,
                changed_at,
            } => {
                digest.update([1]);
                digest.update(actor.to_string().as_bytes());
                digest.update([0]);
                digest_optional_revision(&mut digest, *expected_revision);
                digest.update(policy.digest().as_bytes());
                digest.update(changed_at.get().to_be_bytes());
            }
            Self::Materialize {
                expected_revision,
                outcome,
                observed_at,
            } => {
                digest.update([2]);
                digest.update(expected_revision.to_be_bytes());
                digest.update([match outcome {
                    ServingMaterializationOutcome::Ready => 1,
                    ServingMaterializationOutcome::Failed => 2,
                }]);
                digest.update(observed_at.get().to_be_bytes());
            }
        }
        Ok(digest.finalize().into())
    }

    /// Semantic kind persisted in the journal and audit trail.
    #[must_use]
    pub const fn kind(&self) -> ServingCommandKind {
        match self {
            Self::SetDesired { .. } => ServingCommandKind::SetDesired,
            Self::Materialize { .. } => ServingCommandKind::Materialize,
        }
    }

    /// Trusted event time carried by this command.
    #[must_use]
    pub const fn occurred_at(&self) -> TimestampMicros {
        match self {
            Self::SetDesired { changed_at, .. } => *changed_at,
            Self::Materialize { observed_at, .. } => *observed_at,
        }
    }
}

/// Pure serving-policy lifecycle transition policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct ServingLifecycle;

impl ServingLifecycle {
    /// Creates or replaces the complete desired policy under CAS.
    ///
    /// # Errors
    ///
    /// Returns not-found/conflict for CAS drift and invalid input for a no-op or time regression.
    pub fn set_desired(
        scope: EnvironmentScope,
        current: Option<&ServingPolicyRecord>,
        expected_revision: Option<u64>,
        policy: ServingPolicy,
        changed_at: TimestampMicros,
    ) -> Result<ServingPolicyRecord, ServingPolicyError> {
        policy.validate()?;
        if policy.project_id != scope.project_id() || changed_at.get() < 0 {
            return Err(ServingPolicyError::InvalidInput);
        }
        if current.is_some_and(|record| record.scope != scope) {
            return Err(ServingPolicyError::InvalidInput);
        }
        match current {
            None => {
                if expected_revision.is_some() {
                    return Err(ServingPolicyError::NotFound);
                }
                let record = ServingPolicyRecord {
                    scope,
                    desired_policy: policy,
                    policy_revision: 1,
                    observed_state: ServingObservedState::Pending,
                    observed_policy_revision: None,
                    created_at: changed_at,
                    updated_at: changed_at,
                    observed_at: None,
                };
                record.validate()?;
                Ok(record)
            }
            Some(current) => {
                current.validate()?;
                if expected_revision != Some(current.policy_revision) {
                    return Err(ServingPolicyError::Conflict);
                }
                if policy == current.desired_policy
                    || changed_at < current.updated_at
                    || current.observed_at.is_some_and(|value| changed_at < value)
                {
                    return Err(ServingPolicyError::InvalidInput);
                }
                let mut next = current.clone();
                next.desired_policy = policy;
                next.policy_revision = next
                    .policy_revision
                    .checked_add(1)
                    .ok_or(ServingPolicyError::LimitExceeded)?;
                next.observed_state = ServingObservedState::Pending;
                next.updated_at = changed_at;
                next.validate()?;
                Ok(next)
            }
        }
    }

    /// Records a trusted serving-path outcome for the current desired policy revision.
    ///
    /// # Errors
    ///
    /// Returns conflict for revision/no-op drift and invalid input for time regression.
    pub fn materialize(
        current: &ServingPolicyRecord,
        expected_revision: u64,
        outcome: ServingMaterializationOutcome,
        observed_at: TimestampMicros,
    ) -> Result<ServingPolicyRecord, ServingPolicyError> {
        current.validate()?;
        let observed_state = outcome.observed_state();
        if expected_revision != current.policy_revision
            || current.observed_state == observed_state
                && current.observed_policy_revision == Some(expected_revision)
        {
            return Err(ServingPolicyError::Conflict);
        }
        if observed_at < current.updated_at
            || current
                .observed_at
                .is_some_and(|previous| observed_at < previous)
        {
            return Err(ServingPolicyError::InvalidInput);
        }
        let mut next = current.clone();
        next.observed_state = observed_state;
        next.observed_policy_revision = Some(expected_revision);
        next.observed_at = Some(observed_at);
        next.validate()?;
        Ok(next)
    }
}

/// Durable serving command kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ServingCommandKind {
    /// Desired policy replacement.
    SetDesired,
    /// Serving-path observation.
    Materialize,
}

impl ServingCommandKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SetDesired => "set_desired",
            Self::Materialize => "materialize",
        }
    }

    /// Parses the canonical persisted spelling.
    ///
    /// # Errors
    ///
    /// Unknown values fail closed as corruption.
    pub fn from_persisted(value: &str) -> Result<Self, ServingPolicyError> {
        match value {
            "set_desired" => Ok(Self::SetDesired),
            "materialize" => Ok(Self::Materialize),
            _ => Err(ServingPolicyError::Corruption),
        }
    }
}

/// Immutable successful operation metadata for uncertain-result reconciliation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingOperation {
    /// Exact operated scope.
    pub scope: EnvironmentScope,
    /// Caller-generated idempotency identity.
    pub operation_id: OperationId,
    /// Semantic command kind.
    pub kind: ServingCommandKind,
    /// Policy revision produced or observed.
    pub policy_revision: u64,
    /// Desired policy digest after completion.
    pub desired_policy_digest: Sha256Digest,
    /// Observed state after completion.
    pub observed_state: ServingObservedState,
    /// Observed policy revision after completion.
    pub observed_policy_revision: Option<u64>,
    /// Trusted completion timestamp.
    pub completed_at: TimestampMicros,
}

impl ServingOperation {
    /// Validates immutable operation metadata.
    ///
    /// # Errors
    ///
    /// Rejects invalid revisions, timestamps, or observed relationships.
    pub fn validate(&self) -> Result<(), ServingPolicyError> {
        validate_observation(
            self.policy_revision,
            self.observed_state,
            self.observed_policy_revision,
        )?;
        if self.completed_at.get() < 0
            || self.kind == ServingCommandKind::SetDesired
                && self.observed_state != ServingObservedState::Pending
            || self.kind == ServingCommandKind::Materialize
                && self.observed_state == ServingObservedState::Pending
        {
            return Err(ServingPolicyError::Corruption);
        }
        Ok(())
    }
}

/// Result of one successfully committed or replayed command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingOperationResult {
    /// Immutable operation outcome.
    pub operation: ServingOperation,
    /// Whether the operation was loaded from the durable journal.
    pub replayed: bool,
}

/// Immutable audit event for one committed serving operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingAuditEvent {
    /// Exact audited scope.
    pub scope: EnvironmentScope,
    /// Correlated operation identity.
    pub operation_id: OperationId,
    /// Authenticated operator for desired changes; absent for reconciler observations.
    pub actor_operator_id: Option<OperatorId>,
    /// Semantic command kind.
    pub kind: ServingCommandKind,
    /// Previous policy revision, absent only for initial creation.
    pub previous_policy_revision: Option<u64>,
    /// Policy revision after completion.
    pub policy_revision: u64,
    /// Desired policy digest after completion.
    pub desired_policy_digest: Sha256Digest,
    /// Observed state after completion.
    pub observed_state: ServingObservedState,
    /// Observed revision after completion.
    pub observed_policy_revision: Option<u64>,
    /// Trusted event timestamp.
    pub occurred_at: TimestampMicros,
}

impl ServingAuditEvent {
    /// Validates immutable audit metadata.
    ///
    /// # Errors
    ///
    /// Rejects revision, timestamp, or initial/update relationship drift.
    pub fn validate(&self) -> Result<(), ServingPolicyError> {
        validate_observation(
            self.policy_revision,
            self.observed_state,
            self.observed_policy_revision,
        )?;
        let revision_relationship_valid = match (self.kind, self.previous_policy_revision) {
            (ServingCommandKind::SetDesired, None) => self.policy_revision == 1,
            (ServingCommandKind::SetDesired, Some(previous)) => {
                previous.checked_add(1) == Some(self.policy_revision)
            }
            (ServingCommandKind::Materialize, Some(previous)) => previous == self.policy_revision,
            (ServingCommandKind::Materialize, None) => false,
        };
        if self.occurred_at.get() < 0
            || !revision_relationship_valid
            || self.kind == ServingCommandKind::SetDesired && self.actor_operator_id.is_none()
            || self.kind == ServingCommandKind::Materialize && self.actor_operator_id.is_some()
            || self.kind == ServingCommandKind::SetDesired
                && self.observed_state != ServingObservedState::Pending
            || self.kind == ServingCommandKind::Materialize
                && self.observed_state == ServingObservedState::Pending
        {
            return Err(ServingPolicyError::Corruption);
        }
        Ok(())
    }
}

/// Stable exclusive cursor for audit pagination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServingAuditCursor {
    /// Trusted event timestamp of the last returned row.
    pub occurred_at: TimestampMicros,
    /// Operation identity breaking timestamp ties.
    pub operation_id: OperationId,
}

/// Bounded exact-scope audit request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServingAuditPageRequest {
    /// Exclusive `(occurred_at, operation_id)` cursor.
    pub after: Option<ServingAuditCursor>,
    /// Number of rows in `1..=100`.
    pub limit: u16,
}

impl ServingAuditPageRequest {
    /// Creates a validated request.
    ///
    /// # Errors
    ///
    /// Rejects zero, excessive limits, or negative cursor timestamps.
    pub fn new(after: Option<ServingAuditCursor>, limit: u16) -> Result<Self, ServingPolicyError> {
        let request = Self { after, limit };
        request.validate()?;
        Ok(request)
    }

    /// Validates a request constructed by struct literal.
    ///
    /// # Errors
    ///
    /// Rejects zero, excessive limits, or negative cursor timestamps.
    pub const fn validate(self) -> Result<(), ServingPolicyError> {
        if self.limit == 0
            || self.limit > MAX_AUDIT_PAGE_SIZE
            || match self.after {
                Some(cursor) => cursor.occurred_at.get() < 0,
                None => false,
            }
        {
            return Err(ServingPolicyError::LimitExceeded);
        }
        Ok(())
    }
}

/// Stable ascending audit page for one exact Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingAuditPage {
    /// Exact audited scope.
    pub scope: EnvironmentScope,
    /// Ordered immutable events.
    pub events: Vec<ServingAuditEvent>,
    /// Exclusive cursor for the next page, absent at the end.
    pub next: Option<ServingAuditCursor>,
}

/// Computes a canonical hash over all ordered Cron declarations in a validated manifest.
///
/// Function implementation and general Function contract changes deliberately do not affect this
/// hash. Name, normalized UTC schedule, destination, and canonical Stored Value arguments do.
///
/// # Errors
///
/// Rejects invalid manifests or values exceeding the existing manifest/value limits.
pub fn canonical_cron_declarations_hash(
    manifest: &ReleaseManifestV1,
) -> Result<Sha256Digest, ServingPolicyError> {
    manifest.validate().map_err(map_manifest_error)?;
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_CRON_DECLARATIONS_V1\0");
    digest.update(
        u64::try_from(manifest.cron_definitions.len())
            .map_err(|_| ServingPolicyError::LimitExceeded)?
            .to_be_bytes(),
    );
    for definition in &manifest.cron_definitions {
        for text in [
            definition.name.as_str(),
            definition.schedule.as_str(),
            definition.function.as_str(),
        ] {
            digest.update(
                u64::try_from(text.len())
                    .map_err(|_| ServingPolicyError::LimitExceeded)?
                    .to_be_bytes(),
            );
            digest.update(text.as_bytes());
        }
        let args =
            encode_stored_value(&definition.args).map_err(|_| ServingPolicyError::InvalidInput)?;
        digest.update(
            u64::try_from(args.len())
                .map_err(|_| ServingPolicyError::LimitExceeded)?
                .to_be_bytes(),
        );
        digest.update(args);
    }
    Ok(Sha256Digest::from_bytes(digest.finalize().into()))
}

fn validate_observation(
    policy_revision: u64,
    observed_state: ServingObservedState,
    observed_policy_revision: Option<u64>,
) -> Result<(), ServingPolicyError> {
    if policy_revision == 0
        || observed_policy_revision
            .is_some_and(|revision| revision == 0 || revision > policy_revision)
    {
        return Err(ServingPolicyError::Corruption);
    }
    match observed_state {
        ServingObservedState::Pending => {
            if observed_policy_revision.is_some_and(|revision| revision >= policy_revision) {
                return Err(ServingPolicyError::Corruption);
            }
        }
        ServingObservedState::Ready | ServingObservedState::Failed => {
            if observed_policy_revision != Some(policy_revision) {
                return Err(ServingPolicyError::Corruption);
            }
        }
    }
    Ok(())
}

fn digest_optional_revision(digest: &mut Sha256, revision: Option<u64>) {
    if let Some(revision) = revision {
        digest.update([1]);
        digest.update(revision.to_be_bytes());
    } else {
        digest.update([0]);
    }
}

const fn mode_tag(mode: ServingMode) -> u8 {
    match mode {
        ServingMode::Atomic => 1,
        ServingMode::Gradual => 2,
    }
}

fn map_manifest_error(error: runku_releases::ReleaseError) -> ServingPolicyError {
    match error {
        runku_releases::ReleaseError::LimitExceeded => ServingPolicyError::LimitExceeded,
        runku_releases::ReleaseError::Unsupported => ServingPolicyError::Unsupported,
        runku_releases::ReleaseError::Internal => ServingPolicyError::Internal,
        _ => ServingPolicyError::InvalidInput,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use runku_core::{BuildId, EnvironmentId, FunctionId, ProjectId, ReleaseId};
    use runku_releases::{
        ArtifactDescriptor, ArtifactFormat, AuthPolicy, Capability, CronDefinition,
        FunctionManifest, FunctionType, FunctionVisibility, RuntimeClass,
    };
    use runku_value::CanonicalValue;
    use ulid::Ulid;

    use super::*;

    fn scope() -> EnvironmentScope {
        EnvironmentScope::new(
            ProjectId::from_ulid(Ulid::from(10_u128)),
            EnvironmentId::from_ulid(Ulid::from(20_u128)),
        )
    }

    fn manifest(
        release: u128,
        schema: u8,
        indexes: u8,
        cron_schedule: Option<&str>,
    ) -> Result<ReleaseManifestV1, Box<dyn Error>> {
        let function = FunctionManifest {
            id: FunctionId::from_ulid(Ulid::from(release + 100)),
            name: "tasks.run".parse()?,
            function_type: FunctionType::Mutation,
            visibility: FunctionVisibility::Internal,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::from_bytes([u8::try_from(release)?; 32]),
            arguments_contract_hash: Sha256Digest::from_bytes([31; 32]),
            result_contract_hash: Sha256Digest::from_bytes([32; 32]),
            capabilities: vec![Capability::DbRead, Capability::DbWrite],
        };
        let cron_definitions = cron_schedule
            .map(|schedule| {
                Ok::<_, Box<dyn Error>>(vec![CronDefinition {
                    name: "daily".parse()?,
                    schedule: schedule.parse()?,
                    function: "tasks.run".parse()?,
                    args: CanonicalValue::Null,
                }])
            })
            .transpose()?
            .unwrap_or_default();
        Ok(ReleaseManifestV1 {
            release_id: ReleaseId::from_ulid(Ulid::from(release)),
            project_id: scope().project_id(),
            build_id: BuildId::from_ulid(Ulid::from(release + 200)),
            created_at: TimestampMicros::new(i64::try_from(release)?),
            runtime_version: "runku-js-1".parse()?,
            artifact: ArtifactDescriptor {
                format: ArtifactFormat::SafeEsmBundleV1,
                digest: Sha256Digest::from_bytes([u8::try_from(release + 1)?; 32]),
                size_bytes: 1024,
            },
            function_contract_hash: Sha256Digest::from_bytes([u8::try_from(release + 2)?; 32]),
            schema_contract_hash: Sha256Digest::from_bytes([schema; 32]),
            index_contract_hash: Sha256Digest::from_bytes([indexes; 32]),
            functions: vec![function],
            cron_definitions,
        })
    }

    #[test]
    fn atomic_and_gradual_shapes_are_strict_and_canonical() -> Result<(), Box<dyn Error>> {
        let first = manifest(1, 10, 11, None)?;
        let second = manifest(2, 10, 11, None)?;
        let atomic = ServingPolicy::from_manifests(scope(), ServingMode::Atomic, [(&first, 100)])?;
        assert_eq!(atomic.releases().len(), 1);
        assert!(
            ServingPolicy::from_manifests(
                scope(),
                ServingMode::Atomic,
                [(&first, 50), (&second, 50)]
            )
            .is_err()
        );
        assert!(
            ServingPolicy::from_manifests(scope(), ServingMode::Gradual, [(&first, 99)]).is_err()
        );
        assert!(
            ServingPolicy::from_manifests(
                scope(),
                ServingMode::Gradual,
                [(&first, 60), (&second, 39)]
            )
            .is_err()
        );
        let left = ServingPolicy::from_manifests(
            scope(),
            ServingMode::Gradual,
            [(&first, 40), (&second, 60)],
        )?;
        let right = ServingPolicy::from_manifests(
            scope(),
            ServingMode::Gradual,
            [(&second, 60), (&first, 40)],
        )?;
        assert_eq!(left, right);
        assert_eq!(left.digest(), right.digest());
        assert_eq!(left.select_percentile(0)?, first.release_id);
        assert_eq!(left.select_percentile(39)?, first.release_id);
        assert_eq!(left.select_percentile(40)?, second.release_id);
        assert_eq!(left.select_percentile(99)?, second.release_id);
        assert_eq!(
            left.select_percentile(100),
            Err(ServingPolicyError::InvalidInput)
        );
        Ok(())
    }

    #[test]
    fn every_required_contract_hash_blocks_mixed_serving() -> Result<(), Box<dyn Error>> {
        let baseline = manifest(1, 10, 11, Some("0 0 * * *"))?;
        for incompatible in [
            manifest(2, 12, 11, Some("0 0 * * *"))?,
            manifest(3, 10, 13, Some("0 0 * * *"))?,
            manifest(4, 10, 11, Some("0 1 * * *"))?,
        ] {
            assert_eq!(
                ServingPolicy::from_manifests(
                    scope(),
                    ServingMode::Gradual,
                    [(&baseline, 50), (&incompatible, 50)],
                ),
                Err(ServingPolicyError::IncompatibleContracts)
            );
        }
        Ok(())
    }

    #[test]
    fn cron_hash_uses_declarations_not_release_or_implementation() -> Result<(), Box<dyn Error>> {
        let first = manifest(1, 10, 11, Some("0 0 * * *"))?;
        let second = manifest(2, 10, 11, Some("0 0 * * *"))?;
        let changed = manifest(3, 10, 11, Some("0 1 * * *"))?;
        assert_eq!(
            canonical_cron_declarations_hash(&first)?,
            canonical_cron_declarations_hash(&second)?
        );
        assert_ne!(
            canonical_cron_declarations_hash(&first)?,
            canonical_cron_declarations_hash(&changed)?
        );
        Ok(())
    }

    #[test]
    fn lifecycle_tracks_revisions_and_refuses_stale_or_noop() -> Result<(), Box<dyn Error>> {
        let first = manifest(1, 10, 11, None)?;
        let second = manifest(2, 10, 11, None)?;
        let initial = ServingPolicy::from_manifests(scope(), ServingMode::Atomic, [(&first, 100)])?;
        let created = ServingLifecycle::set_desired(
            scope(),
            None,
            None,
            initial.clone(),
            TimestampMicros::new(10),
        )?;
        assert_eq!(created.policy_revision, 1);
        assert!(!created.is_converged());
        let ready = ServingLifecycle::materialize(
            &created,
            1,
            ServingMaterializationOutcome::Ready,
            TimestampMicros::new(11),
        )?;
        assert!(ready.is_converged());
        let gradual = ServingPolicy::from_manifests(
            scope(),
            ServingMode::Gradual,
            [(&first, 90), (&second, 10)],
        )?;
        let updated = ServingLifecycle::set_desired(
            scope(),
            Some(&ready),
            Some(1),
            gradual,
            TimestampMicros::new(12),
        )?;
        assert_eq!(updated.policy_revision, 2);
        assert_eq!(updated.observed_state, ServingObservedState::Pending);
        assert_eq!(updated.observed_policy_revision, Some(1));
        assert_eq!(
            ServingLifecycle::set_desired(
                scope(),
                Some(&updated),
                Some(1),
                initial,
                TimestampMicros::new(13),
            ),
            Err(ServingPolicyError::Conflict)
        );
        Ok(())
    }

    #[test]
    fn command_digest_binds_exact_scope_and_intent() -> Result<(), Box<dyn Error>> {
        let candidate_manifest = manifest(1, 10, 11, None)?;
        let mut foreign_manifest = manifest(2, 10, 11, None)?;
        foreign_manifest.project_id = ProjectId::generate();
        assert_eq!(
            ServingPolicy::from_manifests(scope(), ServingMode::Atomic, [(&foreign_manifest, 100)]),
            Err(ServingPolicyError::InvalidInput)
        );
        let policy = ServingPolicy::from_manifests(
            scope(),
            ServingMode::Atomic,
            [(&candidate_manifest, 100)],
        )?;
        let command = ServingCommand::SetDesired {
            actor: OperatorId::generate(),
            expected_revision: None,
            policy,
            changed_at: TimestampMicros::new(10),
        };
        let other = EnvironmentScope::new(scope().project_id(), EnvironmentId::generate());
        assert_ne!(command.digest(scope())?, command.digest(other)?);
        Ok(())
    }

    #[test]
    fn stable_errors_expose_retry_policy() {
        assert_eq!(
            ServingPolicyError::IncompatibleContracts.code(),
            "SERVING_POLICY_INCOMPATIBLE_CONTRACTS"
        );
        assert!(ServingPolicyError::ResultUncertain.retryable());
        assert!(!ServingPolicyError::Conflict.retryable());
    }
}
