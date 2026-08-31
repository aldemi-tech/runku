//! Canonical Application Client and credential model.

use std::{collections::BTreeSet, fmt, str::FromStr};

use runku_core::{ApplicationClientId, CredentialId, EnvironmentScope};
use runku_value::TimestampMicros;

use crate::{CredentialDigest, IdentityError};

/// Maximum scopes on a client or credential in protocol v1.
pub const MAX_APPLICATION_SCOPES: usize = 64;

macro_rules! bounded_name {
    ($(#[$meta:meta])* $name:ident, $max:expr) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Returns the validated canonical string.
            #[must_use]
            pub fn as_str(&self) -> &str { &self.0 }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                if value.is_empty()
                    || value.len() > $max
                    || value.trim() != value
                    || value.chars().any(char::is_control)
                {
                    return Err(IdentityError::InvalidInput);
                }
                Ok(Self(value.to_owned()))
            }
        }
    };
}

bounded_name!(
    /// Human-readable stable name of a logical application caller.
    ApplicationClientName,
    80
);
bounded_name!(
    /// Operator label for one replaceable credential.
    CredentialLabel,
    80
);

/// Canonical least-privilege scope name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApplicationScope(String);

impl ApplicationScope {
    /// Returns the canonical scope text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ApplicationScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ApplicationScope {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 64
            || value.starts_with([':', '.', '-'])
            || value.ends_with([':', '.', '-'])
            || value.contains("::")
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b":.-".contains(&byte)
            })
        {
            return Err(IdentityError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Whether the logical client runs in an untrusted distributed binary or trusted server.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ClientKind {
    /// Browser, mobile, desktop, or other caller that cannot hold a secret.
    Public,
    /// Backend, worker, integration, or agent running in trusted infrastructure.
    Confidential,
}

/// Administrative availability of an Application Client.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ApplicationClientStatus {
    /// New requests may resolve active credentials.
    Active,
    /// Every credential is denied without deleting audit history.
    Disabled,
}

/// Replaceable credential type.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CredentialKind {
    /// Public declaration token with no authentication assurance.
    Publishable,
    /// High-entropy bearer secret for a confidential service.
    Secret,
}

/// Durable lifecycle state of a credential.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CredentialStatus {
    /// Accepted while the client is active and optional expiry has not elapsed.
    Active,
    /// Permanently denied but retained for audit and attribution.
    Revoked,
    /// Hidden from normal listings while a tombstone remains durable.
    Deleted,
}

/// Stable logical application caller, independent from replaceable credentials.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationClient {
    /// Exact Project/Environment owner.
    pub scope: EnvironmentScope,
    /// Stable client identity.
    pub id: ApplicationClientId,
    /// Unique operator-facing name inside the Environment.
    pub name: ApplicationClientName,
    /// Public or confidential execution context.
    pub kind: ClientKind,
    /// Administrative status.
    pub status: ApplicationClientStatus,
    /// Maximum scopes any credential under this client may receive.
    pub scope_ceiling: BTreeSet<ApplicationScope>,
    /// Creation timestamp.
    pub created_at: TimestampMicros,
}

impl ApplicationClient {
    /// Validates protocol-v1 limits.
    ///
    /// # Errors
    ///
    /// Rejects empty or oversized scope ceilings and invalid timestamps.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.scope_ceiling.is_empty()
            || self.scope_ceiling.len() > MAX_APPLICATION_SCOPES
            || self.created_at.get() < 0
        {
            return Err(IdentityError::InvalidInput);
        }
        Ok(())
    }
}

/// One replaceable key under an Application Client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCredential {
    /// Exact Project/Environment owner, repeated to enforce tenant scoping in storage.
    pub scope: EnvironmentScope,
    /// Stable credential identity embedded in its external key.
    pub id: CredentialId,
    /// Stable owning client.
    pub client_id: ApplicationClientId,
    /// Public declaration or confidential bearer secret.
    pub kind: CredentialKind,
    /// Operator-facing deployment/instance label.
    pub label: CredentialLabel,
    /// Current irreversible lifecycle state.
    pub status: CredentialStatus,
    /// Keyed digest used for constant-time verification.
    pub digest: CredentialDigest,
    /// Effective scopes, always a subset of the client ceiling.
    pub scopes: BTreeSet<ApplicationScope>,
    /// Creation timestamp.
    pub created_at: TimestampMicros,
    /// Optional absolute expiration; active credentials fail at or after this instant.
    pub expires_at: Option<TimestampMicros>,
    /// Timestamp of irreversible revocation.
    pub revoked_at: Option<TimestampMicros>,
    /// Timestamp of tombstoning after revoke.
    pub deleted_at: Option<TimestampMicros>,
}

impl ApplicationCredential {
    /// Validates internal lifecycle, limits, and timestamps.
    ///
    /// # Errors
    ///
    /// Returns a stable validation error before persistence.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.scopes.is_empty()
            || self.scopes.len() > MAX_APPLICATION_SCOPES
            || self.created_at.get() < 0
            || self.expires_at.is_some_and(|time| time <= self.created_at)
        {
            return Err(IdentityError::InvalidInput);
        }
        match self.status {
            CredentialStatus::Active if self.revoked_at.is_none() && self.deleted_at.is_none() => {}
            CredentialStatus::Revoked if self.revoked_at.is_some() && self.deleted_at.is_none() => {
            }
            CredentialStatus::Deleted if self.revoked_at.is_some() && self.deleted_at.is_some() => {
            }
            _ => return Err(IdentityError::InvalidInput),
        }
        if self.revoked_at.is_some_and(|time| time < self.created_at)
            || self
                .deleted_at
                .is_some_and(|time| self.revoked_at.is_none_or(|revoked| time < revoked))
        {
            return Err(IdentityError::InvalidInput);
        }
        Ok(())
    }
}

/// Assurance assigned to an application context independently from its functional principal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ApplicationAssurance {
    /// A publishable key selected context but could have been copied by anyone.
    Declared,
    /// A confidential secret proved possession of service credentials.
    Verified,
}

/// Non-secret application context produced after successful key resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationContext {
    /// Stable logical caller.
    pub client_id: ApplicationClientId,
    /// Exact credential used for attribution and precise revoke.
    pub credential_id: CredentialId,
    /// Credential type.
    pub credential_kind: CredentialKind,
    /// Declared for publishable, verified for secret.
    pub assurance: ApplicationAssurance,
    /// Effective least-privilege scopes.
    pub scopes: BTreeSet<ApplicationScope>,
    /// Complete identity configuration revision used for this resolution.
    pub configuration_revision: u64,
}

/// Result of an idempotent irreversible lifecycle command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialLifecycleResult {
    /// This command performed the transition.
    Changed,
    /// The requested target state was already durable.
    Replayed,
}
