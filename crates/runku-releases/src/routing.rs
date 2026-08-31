//! Pure Release lifecycle and immutable serving routing.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use runku_core::{ChannelName, CodeTarget, EnvironmentScope, ProjectId, ReleaseId};

use crate::{ArtifactDescriptor, ReleaseError, RuntimeVersion, Sha256Digest};

/// Mutable management lifecycle state of one immutable Release.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReleaseStatus {
    /// Identity reserved; build has not begun.
    Created,
    /// Build worker is producing content.
    Building,
    /// Build attempt failed terminally.
    BuildFailed,
    /// Integrity/contracts/runtime are being validated.
    Validating,
    /// Validation failed terminally for this Release.
    ValidationFailed,
    /// Compatibility policy blocked progression.
    CompatibilityBlocked,
    /// Data/index migration is required before progression.
    MigrationRequired,
    /// Valid artifact exists but serving has not confirmed loadability.
    Ready,
    /// Explicit invocation is permitted and no Channel references it.
    Servable,
    /// One or more Channels reference the Release.
    Active,
    /// Explicit invocation remains possible but new binding is discouraged.
    Deprecated,
    /// New invocations are rejected; retention may keep artifact bytes.
    Retired,
    /// Nonessential artifact/metadata has been collected; tombstone remains.
    GarbageCollected,
}

/// Pure lifecycle transition policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReleaseLifecycle;

impl ReleaseLifecycle {
    /// Applies one explicit management transition that is not derived from Channel references.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseError::InvalidTransition`] for skipped, reversed, identical, terminal, or
    /// direct `ACTIVE` transitions.
    pub const fn advance(
        current: ReleaseStatus,
        next: ReleaseStatus,
    ) -> Result<ReleaseStatus, ReleaseError> {
        let allowed = matches!(
            (current, next),
            (ReleaseStatus::Created, ReleaseStatus::Building)
                | (
                    ReleaseStatus::Building,
                    ReleaseStatus::BuildFailed | ReleaseStatus::Validating
                )
                | (
                    ReleaseStatus::Validating,
                    ReleaseStatus::ValidationFailed
                        | ReleaseStatus::CompatibilityBlocked
                        | ReleaseStatus::MigrationRequired
                        | ReleaseStatus::Ready
                )
                | (
                    ReleaseStatus::CompatibilityBlocked | ReleaseStatus::MigrationRequired,
                    ReleaseStatus::Validating
                )
                | (ReleaseStatus::Ready, ReleaseStatus::Servable)
                | (ReleaseStatus::Servable, ReleaseStatus::Deprecated)
                | (ReleaseStatus::Deprecated, ReleaseStatus::Retired)
                | (ReleaseStatus::Retired, ReleaseStatus::GarbageCollected)
        );
        if allowed {
            Ok(next)
        } else {
            Err(ReleaseError::InvalidTransition)
        }
    }

    /// Derives `ACTIVE`/`SERVABLE` from whether any Channel references the Release.
    ///
    /// # Errors
    ///
    /// Only `SERVABLE` and `ACTIVE` participate in Channel activity.
    pub const fn with_channel_reference(
        current: ReleaseStatus,
        referenced: bool,
    ) -> Result<ReleaseStatus, ReleaseError> {
        match (current, referenced) {
            (ReleaseStatus::Servable | ReleaseStatus::Active, true) => Ok(ReleaseStatus::Active),
            (ReleaseStatus::Servable | ReleaseStatus::Active, false) => Ok(ReleaseStatus::Servable),
            _ => Err(ReleaseError::InvalidTransition),
        }
    }
}

impl ReleaseStatus {
    /// Stable repository text representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Building => "building",
            Self::BuildFailed => "build_failed",
            Self::Validating => "validating",
            Self::ValidationFailed => "validation_failed",
            Self::CompatibilityBlocked => "compatibility_blocked",
            Self::MigrationRequired => "migration_required",
            Self::Ready => "ready",
            Self::Servable => "servable",
            Self::Active => "active",
            Self::Deprecated => "deprecated",
            Self::Retired => "retired",
            Self::GarbageCollected => "garbage_collected",
        }
    }

    /// Whether new explicit invocations may target this status.
    #[must_use]
    pub const fn explicitly_invocable(self) -> bool {
        matches!(self, Self::Servable | Self::Active | Self::Deprecated)
    }
}

impl FromStr for ReleaseStatus {
    type Err = ReleaseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "created" => Ok(Self::Created),
            "building" => Ok(Self::Building),
            "build_failed" => Ok(Self::BuildFailed),
            "validating" => Ok(Self::Validating),
            "validation_failed" => Ok(Self::ValidationFailed),
            "compatibility_blocked" => Ok(Self::CompatibilityBlocked),
            "migration_required" => Ok(Self::MigrationRequired),
            "ready" => Ok(Self::Ready),
            "servable" => Ok(Self::Servable),
            "active" => Ok(Self::Active),
            "deprecated" => Ok(Self::Deprecated),
            "retired" => Ok(Self::Retired),
            "garbage_collected" => Ok(Self::GarbageCollected),
            _ => Err(ReleaseError::Corruption),
        }
    }
}

/// Minimal immutable Release entry consumed on the serving hot path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingReleaseEntry {
    /// Exact Release.
    pub release_id: ReleaseId,
    /// Owning Project, validated against snapshot scope.
    pub project_id: ProjectId,
    /// Digest of canonical Release Manifest bytes.
    pub manifest_digest: Sha256Digest,
    /// Artifact descriptor copied from the validated manifest.
    pub artifact: ArtifactDescriptor,
    /// Platform runtime/API version.
    pub runtime_version: RuntimeVersion,
    /// Current serving lifecycle state.
    pub status: ReleaseStatus,
}

/// Exact Channel pointer inside one Environment snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelBinding {
    /// Stable Channel name.
    pub channel: ChannelName,
    /// Exact Release selected by the Channel.
    pub release_id: ReleaseId,
}

/// Coherent immutable serving configuration for one Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingSnapshot {
    scope: EnvironmentScope,
    revision: u64,
    releases: BTreeMap<ReleaseId, ServingReleaseEntry>,
    channels: BTreeMap<ChannelName, ReleaseId>,
    default_channel: Option<ChannelName>,
}

impl ServingSnapshot {
    /// Constructs and validates a complete snapshot.
    ///
    /// # Errors
    ///
    /// Rejects zero revision, duplicate/cross-project Releases, duplicate/invalid Channels,
    /// `ACTIVE` drift, and an unbound default Channel.
    pub fn new(
        scope: EnvironmentScope,
        revision: u64,
        releases: Vec<ServingReleaseEntry>,
        channels: Vec<ChannelBinding>,
        default_channel: Option<ChannelName>,
    ) -> Result<Self, ReleaseError> {
        if revision == 0 {
            return Err(ReleaseError::InvalidSnapshot);
        }
        let mut release_map = BTreeMap::new();
        for release in releases {
            if release.project_id != scope.project_id()
                || release_map.insert(release.release_id, release).is_some()
            {
                return Err(ReleaseError::InvalidSnapshot);
            }
        }
        let mut channel_map = BTreeMap::new();
        let mut referenced = BTreeSet::new();
        for binding in channels {
            if channel_map
                .insert(binding.channel, binding.release_id)
                .is_some()
            {
                return Err(ReleaseError::InvalidSnapshot);
            }
            let release = release_map
                .get(&binding.release_id)
                .ok_or(ReleaseError::InvalidSnapshot)?;
            if release.status != ReleaseStatus::Active {
                return Err(ReleaseError::InvalidSnapshot);
            }
            referenced.insert(binding.release_id);
        }
        for release in release_map.values() {
            if (release.status == ReleaseStatus::Active) != referenced.contains(&release.release_id)
            {
                return Err(ReleaseError::InvalidSnapshot);
            }
        }
        if default_channel
            .as_ref()
            .is_some_and(|channel| !channel_map.contains_key(channel))
        {
            return Err(ReleaseError::InvalidSnapshot);
        }
        Ok(Self {
            scope,
            revision,
            releases: release_map,
            channels: channel_map,
            default_channel,
        })
    }

    /// Returns the snapshot scope.
    #[must_use]
    pub const fn scope(&self) -> EnvironmentScope {
        self.scope
    }

    /// Returns the positive serving configuration revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the configured default Channel, if any.
    #[must_use]
    pub fn default_channel(&self) -> Option<&ChannelName> {
        self.default_channel.as_ref()
    }

    /// Returns one immutable Release entry by exact identity.
    #[must_use]
    pub fn release(&self, release_id: ReleaseId) -> Option<&ServingReleaseEntry> {
        self.releases.get(&release_id)
    }

    /// Iterates Releases in stable identity order.
    pub fn releases(&self) -> impl ExactSizeIterator<Item = &ServingReleaseEntry> {
        self.releases.values()
    }

    /// Returns the exact Release currently selected by a Channel.
    #[must_use]
    pub fn channel_release(&self, channel: &ChannelName) -> Option<ReleaseId> {
        self.channels.get(channel).copied()
    }

    /// Iterates Channel bindings in stable name order.
    pub fn channels(&self) -> impl ExactSizeIterator<Item = ChannelBinding> + '_ {
        self.channels
            .iter()
            .map(|(channel, release_id)| ChannelBinding {
                channel: channel.clone(),
                release_id: *release_id,
            })
    }
}

/// Release context pinned for the complete invocation/session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectiveRelease {
    /// Exact tenant/environment scope.
    pub scope: EnvironmentScope,
    /// Snapshot revision used for resolution.
    pub serving_revision: u64,
    /// Exact immutable Release.
    pub release_id: ReleaseId,
    /// Canonical manifest digest.
    pub manifest_digest: Sha256Digest,
    /// Content-addressed artifact.
    pub artifact: ArtifactDescriptor,
    /// Required platform runtime/API version.
    pub runtime_version: RuntimeVersion,
}

/// O(1)/O(log n) local router over one coherent snapshot.
#[derive(Clone, Debug)]
pub struct ReleaseRouter {
    snapshot: ServingSnapshot,
}

impl ReleaseRouter {
    /// Creates a router from an already validated immutable snapshot.
    #[must_use]
    pub const fn new(snapshot: ServingSnapshot) -> Self {
        Self { snapshot }
    }

    /// Resolves an explicit Release or Channel target without remote lookup/fallback.
    ///
    /// # Errors
    ///
    /// Returns stable not-found/servable/retired/workspace errors.
    pub fn resolve(&self, target: &CodeTarget) -> Result<EffectiveRelease, ReleaseError> {
        let release_id = match target {
            CodeTarget::Release(release_id) => *release_id,
            CodeTarget::Channel(channel) => *self
                .snapshot
                .channels
                .get(channel)
                .ok_or(ReleaseError::ChannelNotFound)?,
            CodeTarget::Workspace(_) => return Err(ReleaseError::WorkspaceUnsupported),
        };
        self.resolve_release(release_id)
    }

    /// Resolves the explicitly configured default Channel for unbound legacy traffic.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseError::DefaultChannelMissing`] when absent and otherwise the same stable
    /// Release-state errors as [`Self::resolve`].
    pub fn resolve_default(&self) -> Result<EffectiveRelease, ReleaseError> {
        let channel = self
            .snapshot
            .default_channel
            .as_ref()
            .ok_or(ReleaseError::DefaultChannelMissing)?;
        let release_id = *self
            .snapshot
            .channels
            .get(channel)
            .ok_or(ReleaseError::InvalidSnapshot)?;
        self.resolve_release(release_id)
    }

    fn resolve_release(&self, release_id: ReleaseId) -> Result<EffectiveRelease, ReleaseError> {
        let release = self
            .snapshot
            .releases
            .get(&release_id)
            .ok_or(ReleaseError::ReleaseNotFound)?;
        if matches!(
            release.status,
            ReleaseStatus::Retired | ReleaseStatus::GarbageCollected
        ) {
            return Err(ReleaseError::ReleaseRetired);
        }
        if !release.status.explicitly_invocable() {
            return Err(ReleaseError::ReleaseNotServable);
        }
        Ok(EffectiveRelease {
            scope: self.snapshot.scope,
            serving_revision: self.snapshot.revision,
            release_id,
            manifest_digest: release.manifest_digest,
            artifact: release.artifact,
            runtime_version: release.runtime_version.clone(),
        })
    }
}
