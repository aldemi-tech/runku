//! Pure Environment lifecycle values and transitions.

use std::{fmt, str::FromStr};

use runku_core::{
    EnvironmentDescriptor, EnvironmentLocation, EnvironmentProtection, EnvironmentPurpose,
    EnvironmentScope, OperationId, ProjectId,
};
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};

use crate::EnvironmentError;

const MAX_NAME_BYTES: usize = 120;
const MAX_SLUG_BYTES: usize = 63;
const MAX_REGION_BYTES: usize = 64;
const MAX_PAGE_SIZE: u16 = 100;

/// Human-readable Environment name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EnvironmentName(String);

impl EnvironmentName {
    /// Returns the validated name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EnvironmentName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for EnvironmentName {
    type Err = EnvironmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > MAX_NAME_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(EnvironmentError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// DNS-label-shaped Environment slug unique within one Project.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EnvironmentSlug(String);

impl EnvironmentSlug {
    /// Returns the canonical slug.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EnvironmentSlug {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for EnvironmentSlug {
    type Err = EnvironmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !valid_dns_label(value, MAX_SLUG_BYTES) {
            return Err(EnvironmentError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Logical deployment region selected for an Environment.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EnvironmentRegion(String);

impl EnvironmentRegion {
    /// Returns the canonical logical region.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EnvironmentRegion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for EnvironmentRegion {
    type Err = EnvironmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !valid_dns_label(value, MAX_REGION_BYTES) {
            return Err(EnvironmentError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Revisioned operator intent for one Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentConfiguration {
    /// Human-readable display name.
    pub name: EnvironmentName,
    /// Project-unique stable slug.
    pub slug: EnvironmentSlug,
    /// Logical region, independent from provider topology.
    pub region: EnvironmentRegion,
    /// Operational purpose.
    pub purpose: EnvironmentPurpose,
    /// Server-authoritative protection level.
    pub protection: EnvironmentProtection,
    /// Product operating location, not a provider placement identifier.
    pub location: EnvironmentLocation,
    /// Whether Workspace targets are allowed by Environment policy.
    pub workspace_targets_enabled: bool,
}

impl EnvironmentConfiguration {
    /// Validates configuration policy for an exact Environment identity.
    ///
    /// # Errors
    ///
    /// Rejects production purpose/protection combined with Workspace targeting.
    pub fn validate(&self, scope: EnvironmentScope) -> Result<(), EnvironmentError> {
        EnvironmentDescriptor::new(
            scope.environment_id(),
            self.purpose,
            self.protection,
            self.location,
            self.workspace_targets_enabled,
        )
        .map(|_| ())
        .map_err(|_| EnvironmentError::InvalidInput)
    }

    /// Builds the existing target-policy descriptor from this configuration.
    ///
    /// # Errors
    ///
    /// Returns invalid input if the configuration contradicts production policy.
    pub fn descriptor(
        &self,
        scope: EnvironmentScope,
    ) -> Result<EnvironmentDescriptor, EnvironmentError> {
        EnvironmentDescriptor::new(
            scope.environment_id(),
            self.purpose,
            self.protection,
            self.location,
            self.workspace_targets_enabled,
        )
        .map_err(|_| EnvironmentError::InvalidInput)
    }
}

/// Desired lifecycle state persisted by the Product control plane.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EnvironmentDesiredState {
    /// The Environment should be materialized and usable.
    Active,
    /// Reserved terminal intent for the later archive lifecycle slice.
    Archived,
}

impl EnvironmentDesiredState {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }
}

impl FromStr for EnvironmentDesiredState {
    type Err = EnvironmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "archived" => Ok(Self::Archived),
            _ => Err(EnvironmentError::Corruption),
        }
    }
}

/// Last observed reconciliation state for the desired configuration.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EnvironmentObservedState {
    /// The desired configuration has not been fully materialized.
    Pending,
    /// The exact observed configuration revision is ready.
    Ready,
    /// Reconciliation of the exact observed revision failed.
    Failed,
}

impl EnvironmentObservedState {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for EnvironmentObservedState {
    type Err = EnvironmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "ready" => Ok(Self::Ready),
            "failed" => Ok(Self::Failed),
            _ => Err(EnvironmentError::Corruption),
        }
    }
}

/// Trusted materializer outcome for one exact configuration revision.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EnvironmentMaterializationOutcome {
    /// The desired configuration is ready.
    Ready,
    /// Reconciliation failed and may be retried by a new idempotent operation.
    Failed,
}

impl EnvironmentMaterializationOutcome {
    const fn observed_state(self) -> EnvironmentObservedState {
        match self {
            Self::Ready => EnvironmentObservedState::Ready,
            Self::Failed => EnvironmentObservedState::Failed,
        }
    }
}

/// Authoritative Product record for one exact Project/Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Environment {
    /// Exact Project and Environment identity.
    pub scope: EnvironmentScope,
    /// Current desired configuration.
    pub configuration: EnvironmentConfiguration,
    /// Positive compare-and-set revision of the desired configuration.
    pub configuration_revision: u64,
    /// Desired lifecycle state.
    pub desired_state: EnvironmentDesiredState,
    /// Last observed reconciliation state.
    pub observed_state: EnvironmentObservedState,
    /// Configuration revision represented by the last materializer observation.
    pub observed_configuration_revision: Option<u64>,
    /// Creation timestamp.
    pub created_at: TimestampMicros,
    /// Last desired-configuration update timestamp.
    pub updated_at: TimestampMicros,
    /// Last materializer observation timestamp.
    pub observed_at: Option<TimestampMicros>,
}

impl Environment {
    /// Validates persisted cross-field invariants.
    ///
    /// # Errors
    ///
    /// Rejects invalid configuration, revision, timestamp, or observed-state relationships.
    pub fn validate(&self) -> Result<(), EnvironmentError> {
        self.configuration
            .validate(self.scope)
            .map_err(|_| EnvironmentError::Corruption)?;
        if self.configuration_revision == 0
            || self.created_at.get() < 0
            || self.updated_at < self.created_at
            || self
                .observed_at
                .is_some_and(|value| value < self.created_at)
            || self
                .observed_configuration_revision
                .is_some_and(|revision| revision == 0 || revision > self.configuration_revision)
            || self.observed_at.is_some() != self.observed_configuration_revision.is_some()
        {
            return Err(EnvironmentError::Corruption);
        }
        match self.observed_state {
            EnvironmentObservedState::Pending => {
                if self
                    .observed_configuration_revision
                    .is_some_and(|revision| revision >= self.configuration_revision)
                {
                    return Err(EnvironmentError::Corruption);
                }
            }
            EnvironmentObservedState::Ready | EnvironmentObservedState::Failed => {
                if self.observed_configuration_revision != Some(self.configuration_revision)
                    || self
                        .observed_at
                        .is_none_or(|observed_at| observed_at < self.updated_at)
                {
                    return Err(EnvironmentError::Corruption);
                }
            }
        }
        Ok(())
    }

    /// Whether the exact desired revision is observed ready.
    #[must_use]
    pub fn is_converged(&self) -> bool {
        self.desired_state == EnvironmentDesiredState::Active
            && self.observed_state == EnvironmentObservedState::Ready
            && self.observed_configuration_revision == Some(self.configuration_revision)
    }

    /// Builds the trusted target-policy descriptor.
    ///
    /// # Errors
    ///
    /// Returns corruption if persisted configuration is contradictory.
    pub fn descriptor(&self) -> Result<EnvironmentDescriptor, EnvironmentError> {
        self.configuration
            .descriptor(self.scope)
            .map_err(|_| EnvironmentError::Corruption)
    }
}

/// One durable idempotent Environment mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnvironmentCommand {
    /// Creates an active desired Environment in pending observed state.
    Create {
        /// Initial configuration.
        configuration: EnvironmentConfiguration,
        /// Trusted creation timestamp.
        created_at: TimestampMicros,
    },
    /// Replaces desired configuration using compare-and-set.
    Update {
        /// Required current configuration revision.
        expected_revision: u64,
        /// Complete replacement configuration.
        configuration: EnvironmentConfiguration,
        /// Trusted update timestamp.
        updated_at: TimestampMicros,
    },
    /// Records the materializer result for one exact desired revision.
    Materialize {
        /// Required current desired configuration revision.
        expected_revision: u64,
        /// Trusted reconciliation outcome.
        outcome: EnvironmentMaterializationOutcome,
        /// Trusted observation timestamp.
        observed_at: TimestampMicros,
    },
}

impl EnvironmentCommand {
    /// Validates command-local invariants for an exact scope.
    ///
    /// # Errors
    ///
    /// Rejects invalid configuration, zero revisions, or negative timestamps.
    pub fn validate(&self, scope: EnvironmentScope) -> Result<(), EnvironmentError> {
        match self {
            Self::Create {
                configuration,
                created_at,
            } => {
                configuration.validate(scope)?;
                if created_at.get() < 0 {
                    return Err(EnvironmentError::InvalidInput);
                }
            }
            Self::Update {
                expected_revision,
                configuration,
                updated_at,
            } => {
                configuration.validate(scope)?;
                if *expected_revision == 0 || updated_at.get() < 0 {
                    return Err(EnvironmentError::InvalidInput);
                }
            }
            Self::Materialize {
                expected_revision,
                observed_at,
                ..
            } => {
                if *expected_revision == 0 || observed_at.get() < 0 {
                    return Err(EnvironmentError::InvalidInput);
                }
            }
        }
        Ok(())
    }

    /// Computes the canonical operation-journal digest.
    ///
    /// # Errors
    ///
    /// Rejects the same invalid input as [`Self::validate`].
    pub fn digest(&self, scope: EnvironmentScope) -> Result<[u8; 32], EnvironmentError> {
        self.validate(scope)?;
        let mut digest = Sha256::new();
        digest.update(b"RUNKU_ENVIRONMENT_COMMAND_V1\0");
        digest.update(scope.project_id().to_string().as_bytes());
        digest.update([0]);
        digest.update(scope.environment_id().to_string().as_bytes());
        digest.update([0]);
        match self {
            Self::Create {
                configuration,
                created_at,
            } => {
                digest.update([1]);
                digest_configuration(&mut digest, configuration);
                digest.update(created_at.get().to_be_bytes());
            }
            Self::Update {
                expected_revision,
                configuration,
                updated_at,
            } => {
                digest.update([2]);
                digest.update(expected_revision.to_be_bytes());
                digest_configuration(&mut digest, configuration);
                digest.update(updated_at.get().to_be_bytes());
            }
            Self::Materialize {
                expected_revision,
                outcome,
                observed_at,
            } => {
                digest.update([3]);
                digest.update(expected_revision.to_be_bytes());
                digest.update([match outcome {
                    EnvironmentMaterializationOutcome::Ready => 1,
                    EnvironmentMaterializationOutcome::Failed => 2,
                }]);
                digest.update(observed_at.get().to_be_bytes());
            }
        }
        Ok(digest.finalize().into())
    }

    /// Operation kind persisted with a successful command.
    #[must_use]
    pub const fn kind(&self) -> EnvironmentOperationKind {
        match self {
            Self::Create { .. } => EnvironmentOperationKind::Create,
            Self::Update { .. } => EnvironmentOperationKind::Update,
            Self::Materialize { .. } => EnvironmentOperationKind::Materialize,
        }
    }
}

/// Pure Environment lifecycle transition policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvironmentLifecycle;

impl EnvironmentLifecycle {
    /// Constructs the initial pending record.
    ///
    /// # Errors
    ///
    /// Rejects invalid configuration or timestamps.
    pub fn create(
        scope: EnvironmentScope,
        configuration: EnvironmentConfiguration,
        created_at: TimestampMicros,
    ) -> Result<Environment, EnvironmentError> {
        let environment = Environment {
            scope,
            configuration,
            configuration_revision: 1,
            desired_state: EnvironmentDesiredState::Active,
            observed_state: EnvironmentObservedState::Pending,
            observed_configuration_revision: None,
            created_at,
            updated_at: created_at,
            observed_at: None,
        };
        environment
            .validate()
            .map_err(|_| EnvironmentError::InvalidInput)?;
        Ok(environment)
    }

    /// Applies one full desired-configuration replacement.
    ///
    /// # Errors
    ///
    /// Returns conflict for revision/state drift and invalid input for no-op or timestamp drift.
    pub fn update(
        current: &Environment,
        expected_revision: u64,
        configuration: EnvironmentConfiguration,
        updated_at: TimestampMicros,
    ) -> Result<Environment, EnvironmentError> {
        current.validate()?;
        configuration.validate(current.scope)?;
        if current.desired_state != EnvironmentDesiredState::Active
            || expected_revision != current.configuration_revision
        {
            return Err(EnvironmentError::Conflict);
        }
        if configuration == current.configuration || updated_at < current.updated_at {
            return Err(EnvironmentError::InvalidInput);
        }
        let mut next = current.clone();
        next.configuration = configuration;
        next.configuration_revision = next
            .configuration_revision
            .checked_add(1)
            .ok_or(EnvironmentError::LimitExceeded)?;
        next.observed_state = EnvironmentObservedState::Pending;
        next.updated_at = updated_at;
        next.validate()?;
        Ok(next)
    }

    /// Records a trusted materializer result for the current desired revision.
    ///
    /// # Errors
    ///
    /// Returns conflict for revision/state/no-op drift and invalid input for timestamp drift.
    pub fn materialize(
        current: &Environment,
        expected_revision: u64,
        outcome: EnvironmentMaterializationOutcome,
        observed_at: TimestampMicros,
    ) -> Result<Environment, EnvironmentError> {
        current.validate()?;
        let observed_state = outcome.observed_state();
        if current.desired_state != EnvironmentDesiredState::Active
            || expected_revision != current.configuration_revision
            || (current.observed_state == observed_state
                && current.observed_configuration_revision == Some(expected_revision))
        {
            return Err(EnvironmentError::Conflict);
        }
        if observed_at < current.updated_at
            || current
                .observed_at
                .is_some_and(|previous| observed_at < previous)
        {
            return Err(EnvironmentError::InvalidInput);
        }
        let mut next = current.clone();
        next.observed_state = observed_state;
        next.observed_configuration_revision = Some(expected_revision);
        next.observed_at = Some(observed_at);
        next.validate()?;
        Ok(next)
    }
}

/// Durable Environment operation kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EnvironmentOperationKind {
    /// Initial Environment creation.
    Create,
    /// Desired configuration replacement.
    Update,
    /// Materializer observation.
    Materialize,
}

impl EnvironmentOperationKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Materialize => "materialize",
        }
    }
}

impl FromStr for EnvironmentOperationKind {
    type Err = EnvironmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "create" => Ok(Self::Create),
            "update" => Ok(Self::Update),
            "materialize" => Ok(Self::Materialize),
            _ => Err(EnvironmentError::Corruption),
        }
    }
}

/// Immutable successful operation metadata used to reconcile uncertain results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentOperation {
    /// Exact Project/Environment operated on.
    pub scope: EnvironmentScope,
    /// Caller-generated idempotency identity.
    pub operation_id: OperationId,
    /// Semantic command kind.
    pub kind: EnvironmentOperationKind,
    /// Desired configuration revision produced or observed.
    pub configuration_revision: u64,
    /// Desired state at operation completion.
    pub desired_state: EnvironmentDesiredState,
    /// Observed state at operation completion.
    pub observed_state: EnvironmentObservedState,
    /// Observed configuration revision at operation completion.
    pub observed_configuration_revision: Option<u64>,
    /// Trusted completion timestamp.
    pub completed_at: TimestampMicros,
}

impl EnvironmentOperation {
    /// Validates immutable operation metadata.
    ///
    /// # Errors
    ///
    /// Rejects zero revisions, negative timestamps, or invalid observed revision relationships.
    pub fn validate(&self) -> Result<(), EnvironmentError> {
        if self.configuration_revision == 0
            || self.completed_at.get() < 0
            || self
                .observed_configuration_revision
                .is_some_and(|revision| revision == 0 || revision > self.configuration_revision)
        {
            return Err(EnvironmentError::Corruption);
        }
        match self.observed_state {
            EnvironmentObservedState::Pending => {
                if self
                    .observed_configuration_revision
                    .is_some_and(|revision| revision >= self.configuration_revision)
                {
                    return Err(EnvironmentError::Corruption);
                }
            }
            EnvironmentObservedState::Ready | EnvironmentObservedState::Failed => {
                if self.observed_configuration_revision != Some(self.configuration_revision) {
                    return Err(EnvironmentError::Corruption);
                }
            }
        }
        Ok(())
    }
}

/// Result of applying one idempotent command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentOperationResult {
    /// Immutable operation outcome.
    pub operation: EnvironmentOperation,
    /// Whether the operation was loaded from the durable journal.
    pub replayed: bool,
}

/// Bounded Project-scoped Environment list request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnvironmentPageRequest {
    /// Exclusive Environment ID cursor.
    pub after: Option<runku_core::EnvironmentId>,
    /// Number of records in `1..=100`.
    pub limit: u16,
}

impl EnvironmentPageRequest {
    /// Creates a validated bounded page request.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive limits.
    pub const fn new(
        after: Option<runku_core::EnvironmentId>,
        limit: u16,
    ) -> Result<Self, EnvironmentError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(EnvironmentError::LimitExceeded);
        }
        Ok(Self { after, limit })
    }

    /// Validates a page request constructed through a struct literal.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive limits.
    pub const fn validate(self) -> Result<(), EnvironmentError> {
        if self.limit == 0 || self.limit > MAX_PAGE_SIZE {
            return Err(EnvironmentError::LimitExceeded);
        }
        Ok(())
    }
}

/// One stable Environment page in ascending ID order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentPage {
    /// Exact Project owner of every record.
    pub project_id: ProjectId,
    /// Ordered Environment records.
    pub environments: Vec<Environment>,
    /// Exclusive cursor for the next page, absent at the end.
    pub next: Option<runku_core::EnvironmentId>,
}

fn valid_dns_label(value: &str, maximum: usize) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= maximum
        && bytes.first().is_some_and(u8::is_ascii_lowercase)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

fn digest_configuration(digest: &mut Sha256, configuration: &EnvironmentConfiguration) {
    for value in [
        configuration.name.as_str(),
        configuration.slug.as_str(),
        configuration.region.as_str(),
    ] {
        digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(value.as_bytes());
    }
    digest.update([
        purpose_tag(configuration.purpose),
        protection_tag(configuration.protection),
        location_tag(configuration.location),
        u8::from(configuration.workspace_targets_enabled),
    ]);
}

const fn purpose_tag(value: EnvironmentPurpose) -> u8 {
    match value {
        EnvironmentPurpose::Development => 1,
        EnvironmentPurpose::Preview => 2,
        EnvironmentPurpose::Staging => 3,
        EnvironmentPurpose::Production => 4,
    }
}

const fn protection_tag(value: EnvironmentProtection) -> u8 {
    match value {
        EnvironmentProtection::Open => 1,
        EnvironmentProtection::Protected => 2,
        EnvironmentProtection::Production => 3,
    }
}

const fn location_tag(value: EnvironmentLocation) -> u8 {
    match value {
        EnvironmentLocation::Local => 1,
        EnvironmentLocation::Managed => 2,
        EnvironmentLocation::SelfHosted => 3,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use runku_core::{EnvironmentId, ProjectId};
    use ulid::Ulid;

    use super::*;

    fn scope() -> EnvironmentScope {
        EnvironmentScope::new(
            ProjectId::from_ulid(Ulid::from(1_u128)),
            EnvironmentId::from_ulid(Ulid::from(2_u128)),
        )
    }

    fn configuration(slug: &str) -> Result<EnvironmentConfiguration, EnvironmentError> {
        Ok(EnvironmentConfiguration {
            name: "Production".parse()?,
            slug: slug.parse()?,
            region: "us-east-1".parse()?,
            purpose: EnvironmentPurpose::Production,
            protection: EnvironmentProtection::Production,
            location: EnvironmentLocation::SelfHosted,
            workspace_targets_enabled: false,
        })
    }

    #[test]
    fn names_slugs_and_regions_are_bounded() {
        assert!(" Production".parse::<EnvironmentName>().is_err());
        assert!("production-".parse::<EnvironmentSlug>().is_err());
        assert!("US-EAST-1".parse::<EnvironmentRegion>().is_err());
        assert!("production".parse::<EnvironmentSlug>().is_ok());
        assert!("us-east-1".parse::<EnvironmentRegion>().is_ok());
    }

    #[test]
    fn lifecycle_tracks_desired_and_observed_revisions() -> Result<(), Box<dyn Error>> {
        let created = EnvironmentLifecycle::create(
            scope(),
            configuration("production")?,
            TimestampMicros::new(10),
        )?;
        assert!(!created.is_converged());
        let ready = EnvironmentLifecycle::materialize(
            &created,
            1,
            EnvironmentMaterializationOutcome::Ready,
            TimestampMicros::new(11),
        )?;
        assert!(ready.is_converged());
        let updated = EnvironmentLifecycle::update(
            &ready,
            1,
            configuration("production-v2")?,
            TimestampMicros::new(12),
        )?;
        assert_eq!(updated.configuration_revision, 2);
        assert_eq!(updated.observed_state, EnvironmentObservedState::Pending);
        assert_eq!(updated.observed_configuration_revision, Some(1));
        assert!(!updated.is_converged());
        Ok(())
    }

    #[test]
    fn lifecycle_rejects_stale_revision_noop_and_time_regression() -> Result<(), Box<dyn Error>> {
        let created = EnvironmentLifecycle::create(
            scope(),
            configuration("production")?,
            TimestampMicros::new(10),
        )?;
        assert_eq!(
            EnvironmentLifecycle::update(
                &created,
                2,
                configuration("production-v2")?,
                TimestampMicros::new(11),
            ),
            Err(EnvironmentError::Conflict)
        );
        assert_eq!(
            EnvironmentLifecycle::update(
                &created,
                1,
                configuration("production")?,
                TimestampMicros::new(11),
            ),
            Err(EnvironmentError::InvalidInput)
        );
        assert_eq!(
            EnvironmentLifecycle::materialize(
                &created,
                1,
                EnvironmentMaterializationOutcome::Ready,
                TimestampMicros::new(9),
            ),
            Err(EnvironmentError::InvalidInput)
        );
        Ok(())
    }

    #[test]
    fn operation_digest_binds_scope_and_complete_intent() -> Result<(), Box<dyn Error>> {
        let command = EnvironmentCommand::Create {
            configuration: configuration("production")?,
            created_at: TimestampMicros::new(10),
        };
        let other_scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        assert_ne!(command.digest(scope())?, command.digest(other_scope)?);
        let changed = EnvironmentCommand::Create {
            configuration: configuration("staging")?,
            created_at: TimestampMicros::new(10),
        };
        assert_ne!(command.digest(scope())?, changed.digest(scope())?);
        Ok(())
    }

    #[test]
    fn production_workspace_contradiction_is_rejected() -> Result<(), Box<dyn Error>> {
        let mut invalid = configuration("production")?;
        invalid.workspace_targets_enabled = true;
        assert_eq!(
            invalid.validate(scope()),
            Err(EnvironmentError::InvalidInput)
        );
        Ok(())
    }

    #[test]
    fn stable_errors_expose_retry_policy() {
        assert_eq!(
            EnvironmentError::OperationIdReused.code(),
            "ENVIRONMENT_OPERATION_ID_REUSED"
        );
        assert!(EnvironmentError::ResultUncertain.retryable());
        assert!(!EnvironmentError::Conflict.retryable());
    }
}
