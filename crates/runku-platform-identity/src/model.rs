//! Canonical operator, grant, invitation, and session models.

use std::{collections::BTreeSet, fmt, str::FromStr};

use runku_core::{
    EnvironmentScope, OperationId, OperatorId, OperatorInvitationId, OperatorSessionId, ProjectId,
};
use runku_value::TimestampMicros;

use crate::PlatformIdentityError;

/// Maximum capabilities stored in one grant.
pub const MAX_GRANT_CAPABILITIES: usize = 32;

macro_rules! bounded_name {
    ($(#[$meta:meta])* $name:ident, $max:expr) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Returns the validated value.
            #[must_use]
            pub fn as_str(&self) -> &str { &self.0 }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = PlatformIdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                if value.is_empty()
                    || value.len() > $max
                    || value.trim() != value
                    || value.chars().any(char::is_control)
                {
                    return Err(PlatformIdentityError::InvalidInput);
                }
                Ok(Self(value.to_owned()))
            }
        }
    };
}

bounded_name!(
    /// Human-readable operator name.
    OperatorName,
    120
);

/// Canonical HTTPS origin that owns one independently versioned managed grant set.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ManagedSourceAuthority(String);

impl ManagedSourceAuthority {
    /// Returns the exact configured canonical origin.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ManagedSourceAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ManagedSourceAuthority {
    type Err = PlatformIdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || value.len() > 2_048 {
            return Err(PlatformIdentityError::InvalidInput);
        }
        let origin = url::Url::parse(value).map_err(|_| PlatformIdentityError::InvalidInput)?;
        if origin.scheme() != "https"
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || origin.origin().ascii_serialization() != value
        {
            return Err(PlatformIdentityError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Durable outcome of reconciling one source-owned managed grant set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedGrantReconciliation {
    /// Operator whose effective grants were reconciled.
    pub operator_id: OperatorId,
    /// Exact source authority owning the replaced subset.
    pub source_authority: ManagedSourceAuthority,
    /// Applied or replayed monotonic source revision.
    pub source_revision: u64,
    /// Current operator authorization revision after reconciliation.
    pub authorization_revision: u64,
    /// True when a greater revision committed.
    pub applied: bool,
    /// True when an equal revision and identical digest was replayed.
    pub replayed: bool,
}
bounded_name!(
    /// Human-readable name of one enrolled CLI device.
    DeviceName,
    120
);

/// Closed, versioned Platform Identity capability catalog.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PlatformCapability {
    /// Manage installation-wide configuration and lifecycle.
    InstallationManage,
    /// Create, update, or retire projects.
    ProjectsManage,
    /// Create, update, or retire environments.
    EnvironmentsManage,
    /// Read portable Product Environment configuration and lifecycle state.
    EnvironmentsRead,
    /// Invite operators, change grants, and revoke sessions.
    OperatorsManage,
    /// Read releases and channels.
    ReleasesRead,
    /// Publish candidate releases.
    ReleasesPublish,
    /// Promote or roll back channels.
    ChannelsPromote,
    /// Read logical application documents through the administrative boundary.
    DataRead,
    /// Insert, replace, or delete logical application documents.
    DataWrite,
    /// Read non-secret credential metadata.
    CredentialsRead,
    /// Create, rotate, revoke, or delete credentials.
    CredentialsManage,
    /// Read logical bucket configuration and non-secret Object Storage key metadata.
    StorageRead,
    /// Create, configure, archive buckets and manage scoped Product storage keys.
    StorageManage,
    /// Query historical operational logs.
    LogsRead,
    /// Follow an operational log stream.
    LogsFollow,
    /// Apply operational-log retention.
    LogsPrune,
    /// Read durable usage records and aggregates.
    UsageRead,
    /// Manage backup and restore operations.
    BackupsManage,
}

impl PlatformCapability {
    /// Canonical storage and protocol spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InstallationManage => "installation:manage",
            Self::ProjectsManage => "projects:manage",
            Self::EnvironmentsManage => "environments:manage",
            Self::EnvironmentsRead => "environments:read",
            Self::OperatorsManage => "operators:manage",
            Self::ReleasesRead => "releases:read",
            Self::ReleasesPublish => "releases:publish",
            Self::ChannelsPromote => "channels:promote",
            Self::DataRead => "data:read",
            Self::DataWrite => "data:write",
            Self::CredentialsRead => "credentials:read",
            Self::CredentialsManage => "credentials:manage",
            Self::StorageRead => "storage:read",
            Self::StorageManage => "storage:manage",
            Self::LogsRead => "logs:read",
            Self::LogsFollow => "logs:follow",
            Self::LogsPrune => "logs:prune",
            Self::UsageRead => "usage:read",
            Self::BackupsManage => "backups:manage",
        }
    }

    /// Parses a canonical capability.
    ///
    /// # Errors
    ///
    /// Rejects unknown or non-canonical capability strings.
    pub fn parse(value: &str) -> Result<Self, PlatformIdentityError> {
        match value {
            "installation:manage" => Ok(Self::InstallationManage),
            "projects:manage" => Ok(Self::ProjectsManage),
            "environments:manage" => Ok(Self::EnvironmentsManage),
            "environments:read" => Ok(Self::EnvironmentsRead),
            "operators:manage" => Ok(Self::OperatorsManage),
            "releases:read" => Ok(Self::ReleasesRead),
            "releases:publish" => Ok(Self::ReleasesPublish),
            "channels:promote" => Ok(Self::ChannelsPromote),
            "data:read" => Ok(Self::DataRead),
            "data:write" => Ok(Self::DataWrite),
            "credentials:read" => Ok(Self::CredentialsRead),
            "credentials:manage" => Ok(Self::CredentialsManage),
            "storage:read" => Ok(Self::StorageRead),
            "storage:manage" => Ok(Self::StorageManage),
            "logs:read" => Ok(Self::LogsRead),
            "logs:follow" => Ok(Self::LogsFollow),
            "logs:prune" => Ok(Self::LogsPrune),
            "usage:read" => Ok(Self::UsageRead),
            "backups:manage" => Ok(Self::BackupsManage),
            _ => Err(PlatformIdentityError::Corruption),
        }
    }

    /// Complete owner capability set for one installation.
    #[must_use]
    pub fn owner_set() -> BTreeSet<Self> {
        [
            Self::InstallationManage,
            Self::ProjectsManage,
            Self::EnvironmentsManage,
            Self::EnvironmentsRead,
            Self::OperatorsManage,
            Self::ReleasesRead,
            Self::ReleasesPublish,
            Self::ChannelsPromote,
            Self::DataRead,
            Self::DataWrite,
            Self::CredentialsRead,
            Self::CredentialsManage,
            Self::StorageRead,
            Self::StorageManage,
            Self::LogsRead,
            Self::LogsFollow,
            Self::LogsPrune,
            Self::UsageRead,
            Self::BackupsManage,
        ]
        .into_iter()
        .collect()
    }
}

/// Resource boundary to which capabilities apply.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AccessScope {
    /// Every current and future resource in this installation.
    Installation,
    /// One project and every environment owned by it.
    Project(ProjectId),
    /// One exact project/environment pair.
    Environment(EnvironmentScope),
}

impl AccessScope {
    /// Whether this scope contains the requested exact scope.
    #[must_use]
    pub fn contains(self, requested: Self) -> bool {
        match (self, requested) {
            (Self::Installation, _) => true,
            (Self::Project(granted), Self::Project(requested)) => granted == requested,
            (Self::Project(granted), Self::Environment(requested)) => {
                granted == requested.project_id()
            }
            (Self::Environment(granted), Self::Environment(requested)) => granted == requested,
            (Self::Project(_) | Self::Environment(_), Self::Installation)
            | (Self::Environment(_), Self::Project(_)) => false,
        }
    }
}

/// A set of capabilities delegated at one resource boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorGrant {
    /// Resource boundary.
    pub scope: AccessScope,
    /// Non-empty capability set.
    pub capabilities: BTreeSet<PlatformCapability>,
}

impl OperatorGrant {
    /// Validates bounded non-empty capabilities.
    ///
    /// # Errors
    ///
    /// Rejects empty or oversized capability sets.
    pub fn validate(&self) -> Result<(), PlatformIdentityError> {
        if self.capabilities.is_empty() || self.capabilities.len() > MAX_GRANT_CAPABILITIES {
            return Err(PlatformIdentityError::InvalidInput);
        }
        Ok(())
    }
}

/// Convenience role expanded to explicit grants before persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperatorRole {
    /// Full authority inside the selected scope.
    Owner,
    /// Operational authority without operator/installation ownership.
    Operator,
    /// Development and release publication authority.
    Developer,
    /// Read-only releases, logs, usage, and credential metadata.
    Observer,
}

impl OperatorRole {
    /// Expands this presentation role into the authoritative capability set.
    #[must_use]
    pub fn capabilities(self) -> BTreeSet<PlatformCapability> {
        use PlatformCapability as C;
        match self {
            Self::Owner => C::owner_set(),
            Self::Operator => [
                C::EnvironmentsManage,
                C::EnvironmentsRead,
                C::ReleasesRead,
                C::ReleasesPublish,
                C::ChannelsPromote,
                C::DataRead,
                C::DataWrite,
                C::CredentialsRead,
                C::CredentialsManage,
                C::StorageRead,
                C::StorageManage,
                C::LogsRead,
                C::LogsFollow,
                C::LogsPrune,
                C::UsageRead,
                C::BackupsManage,
            ]
            .into_iter()
            .collect(),
            Self::Developer => [
                C::EnvironmentsRead,
                C::ReleasesRead,
                C::ReleasesPublish,
                C::ChannelsPromote,
                C::DataRead,
                C::DataWrite,
                C::CredentialsRead,
                C::StorageRead,
                C::LogsRead,
                C::LogsFollow,
            ]
            .into_iter()
            .collect(),
            Self::Observer => [
                C::EnvironmentsRead,
                C::ReleasesRead,
                C::CredentialsRead,
                C::StorageRead,
                C::LogsRead,
                C::LogsFollow,
                C::DataRead,
                C::UsageRead,
            ]
            .into_iter()
            .collect(),
        }
    }
}

/// Durable operator lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperatorStatus {
    /// New sessions and authorized operations are allowed.
    Active,
    /// Every existing session is denied while history is retained.
    Disabled,
}

/// Human operator record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operator {
    /// Stable operator identity.
    pub id: OperatorId,
    /// Display name, not an authentication claim.
    pub name: OperatorName,
    /// Current lifecycle state.
    pub status: OperatorStatus,
    /// Creation time.
    pub created_at: TimestampMicros,
    /// Monotonic authorization revision for stream/session revalidation.
    pub authorization_revision: u64,
}

/// Proven external identity after a configured OIDC verifier validates its token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalOperatorIdentity {
    /// Stable local provider configuration ID.
    pub provider_id: String,
    /// Opaque keyed subject identifier; raw OIDC `sub` is not retained here.
    pub subject_id: String,
}

impl ExternalOperatorIdentity {
    /// Validates bounded provider and opaque subject identifiers.
    ///
    /// # Errors
    ///
    /// Rejects unsafe provider IDs or empty/oversized subject identifiers.
    pub fn validate(&self) -> Result<(), PlatformIdentityError> {
        if self.provider_id.is_empty()
            || self.provider_id.len() > 80
            || self.provider_id.starts_with(['.', '-'])
            || self.provider_id.ends_with(['.', '-'])
            || !self.provider_id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
            })
            || self.subject_id.is_empty()
            || self.subject_id.len() > 128
            || !self.subject_id.is_ascii()
        {
            return Err(PlatformIdentityError::InvalidInput);
        }
        Ok(())
    }
}

/// Why an invitation exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvitationKind {
    /// Server-generated, first-owner-only bootstrap credential.
    Bootstrap,
    /// Invitation delegated by an authenticated operator.
    Operator,
}

/// Invitation lifecycle, including the time-derived expired view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvitationStatus {
    /// May be exchanged exactly once before expiry.
    Pending,
    /// Was exchanged successfully.
    Consumed,
    /// Was irreversibly revoked.
    Revoked,
    /// Remained unconsumed beyond its absolute expiry.
    Expired,
}

/// Non-secret metadata for one delegated operator invitation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorInvitation {
    /// Stable invitation identity embedded in the one-time bearer.
    pub id: OperatorInvitationId,
    /// Durable idempotent operation identity, when automation supplied one.
    pub operation_id: Option<OperationId>,
    /// Name assigned if the invitation is consumed.
    pub operator_name: OperatorName,
    /// Exact grants assigned if the invitation is consumed.
    pub grants: Vec<OperatorGrant>,
    /// Current durable lifecycle state.
    pub status: InvitationStatus,
    /// Operator that created the invitation.
    pub created_by: OperatorId,
    /// Server-owned creation timestamp.
    pub created_at: TimestampMicros,
    /// Absolute single-use expiry.
    pub expires_at: TimestampMicros,
    /// Successful consumption time, when consumed.
    pub consumed_at: Option<TimestampMicros>,
    /// Irreversible revocation time, when revoked.
    pub revoked_at: Option<TimestampMicros>,
}

impl OperatorInvitation {
    /// Returns the externally visible state at the supplied server time.
    #[must_use]
    pub fn status_at(&self, now: TimestampMicros) -> InvitationStatus {
        if self.status == InvitationStatus::Pending && self.expires_at <= now {
            InvitationStatus::Expired
        } else {
            self.status
        }
    }
}

/// Durable operator session lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionStatus {
    /// Access and refresh credentials may be accepted before their respective expiry.
    Active,
    /// Every credential for this device is denied.
    Revoked,
}

/// Non-secret metadata for one independently revocable device session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorSession {
    /// Stable device session identity.
    pub id: OperatorSessionId,
    /// Owning operator.
    pub operator_id: OperatorId,
    /// Operator-selected device label.
    pub device_name: DeviceName,
    /// Current lifecycle state.
    pub status: SessionStatus,
    /// Creation timestamp.
    pub created_at: TimestampMicros,
    /// Last successful authentication or refresh timestamp.
    pub last_used_at: TimestampMicros,
    /// Access credential expiry.
    pub access_expires_at: TimestampMicros,
    /// Refresh credential expiry.
    pub refresh_expires_at: TimestampMicros,
}

/// Token-free authenticated operator and current grants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorContext {
    /// Current operator record.
    pub operator: Operator,
    /// Exact device session used for authentication.
    pub session: OperatorSession,
    /// Current authoritative grants.
    pub grants: Vec<OperatorGrant>,
}

impl OperatorContext {
    /// Requires one capability at an exact requested scope.
    ///
    /// # Errors
    ///
    /// Returns forbidden when no current grant contains the requested scope and capability.
    pub fn authorize(
        &self,
        requested: AccessScope,
        capability: PlatformCapability,
    ) -> Result<(), PlatformIdentityError> {
        if self.operator.status != OperatorStatus::Active
            || self.session.status != SessionStatus::Active
        {
            return Err(PlatformIdentityError::Unauthenticated);
        }
        if self.grants.iter().any(|grant| {
            grant.scope.contains(requested) && grant.capabilities.contains(&capability)
        }) {
            Ok(())
        } else {
            Err(PlatformIdentityError::Forbidden)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runku_core::{EnvironmentId, OperatorId, OperatorSessionId};

    fn context(
        capabilities: impl IntoIterator<Item = PlatformCapability>,
    ) -> Result<OperatorContext, PlatformIdentityError> {
        let operator_id = OperatorId::generate();
        let now = TimestampMicros::new(1_800_000_000_000_000);
        Ok(OperatorContext {
            operator: Operator {
                id: operator_id,
                name: "bounded operator".parse()?,
                status: OperatorStatus::Active,
                created_at: now,
                authorization_revision: 1,
            },
            session: OperatorSession {
                id: OperatorSessionId::generate(),
                operator_id,
                device_name: "device".parse()?,
                status: SessionStatus::Active,
                created_at: now,
                last_used_at: now,
                access_expires_at: TimestampMicros::new(now.get() + 1),
                refresh_expires_at: TimestampMicros::new(now.get() + 2),
            },
            grants: vec![OperatorGrant {
                scope: AccessScope::Environment(EnvironmentScope::new(
                    ProjectId::generate(),
                    EnvironmentId::generate(),
                )),
                capabilities: capabilities.into_iter().collect(),
            }],
        })
    }

    #[test]
    fn data_authority_is_not_implied_by_environment_management_or_read_access()
    -> Result<(), PlatformIdentityError> {
        let environment_manager = context([PlatformCapability::EnvironmentsManage])?;
        let scope = environment_manager.grants[0].scope;
        assert_eq!(
            environment_manager.authorize(scope, PlatformCapability::DataRead),
            Err(PlatformIdentityError::Forbidden)
        );
        assert_eq!(
            environment_manager.authorize(scope, PlatformCapability::DataWrite),
            Err(PlatformIdentityError::Forbidden)
        );
        assert_eq!(
            environment_manager.authorize(scope, PlatformCapability::StorageRead),
            Err(PlatformIdentityError::Forbidden)
        );
        assert_eq!(
            environment_manager.authorize(scope, PlatformCapability::StorageManage),
            Err(PlatformIdentityError::Forbidden)
        );

        let reader = context([PlatformCapability::DataRead])?;
        let scope = reader.grants[0].scope;
        assert!(
            reader
                .authorize(scope, PlatformCapability::DataRead)
                .is_ok()
        );
        assert_eq!(
            reader.authorize(scope, PlatformCapability::DataWrite),
            Err(PlatformIdentityError::Forbidden)
        );
        let storage_reader = context([PlatformCapability::StorageRead])?;
        let scope = storage_reader.grants[0].scope;
        assert!(
            storage_reader
                .authorize(scope, PlatformCapability::StorageRead)
                .is_ok()
        );
        assert_eq!(
            storage_reader.authorize(scope, PlatformCapability::StorageManage),
            Err(PlatformIdentityError::Forbidden)
        );
        Ok(())
    }

    #[test]
    fn managed_source_authority_requires_an_exact_canonical_https_origin()
    -> Result<(), PlatformIdentityError> {
        assert_eq!(
            ManagedSourceAuthority::from_str("https://cloud.runku.example")?.as_str(),
            "https://cloud.runku.example"
        );
        for invalid in [
            "http://cloud.runku.example",
            "https://cloud.runku.example/",
            "https://cloud.runku.example/path",
            "https://cloud.runku.example:443",
            "https://user@cloud.runku.example",
            "HTTPS://cloud.runku.example",
        ] {
            assert_eq!(
                ManagedSourceAuthority::from_str(invalid),
                Err(PlatformIdentityError::InvalidInput),
                "accepted non-canonical authority {invalid}"
            );
        }
        Ok(())
    }
}
