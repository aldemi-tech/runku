use std::{fmt, str::FromStr};

use runku_core::{DevelopmentCredentialId, EnvironmentScope};
use runku_development::DevelopmentActor;
use runku_value::TimestampMicros;
use thiserror::Error;

use crate::DevelopmentKeyDigest;

const LABEL_MAX_BYTES: usize = 64;

/// Operator-facing canonical label for one independently revocable development key.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DevelopmentCredentialLabel(String);

impl DevelopmentCredentialLabel {
    /// Returns the canonical non-secret text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DevelopmentCredentialLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for DevelopmentCredentialLabel {
    type Err = DevelopmentAccessError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > LABEL_MAX_BYTES
            || !value
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'-' | b'_' | b'.')
            })
        {
            return Err(DevelopmentAccessError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Irreversible development credential lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentCredentialStatus {
    /// Credential may authenticate before its optional expiry.
    Active,
    /// Credential bearer is permanently rejected but metadata remains listable.
    Revoked,
    /// Credential is tombstoned and excluded from normal listing.
    Deleted,
}

impl DevelopmentCredentialStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Revoked => "revoked",
            Self::Deleted => "deleted",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, DevelopmentAccessError> {
        match value {
            "active" => Ok(Self::Active),
            "revoked" => Ok(Self::Revoked),
            "deleted" => Ok(Self::Deleted),
            _ => Err(DevelopmentAccessError::Corruption),
        }
    }
}

/// Persisted non-secret metadata and verifier for one Development Access key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentCredential {
    /// Replaceable credential identity embedded in its bearer key.
    pub id: DevelopmentCredentialId,
    /// Exact Project/Environment boundary.
    pub scope: EnvironmentScope,
    /// Trusted actor attributed to successful remote development commands.
    pub actor: DevelopmentActor,
    /// Operator-facing key label.
    pub label: DevelopmentCredentialLabel,
    /// Keyed digest; never complete key material.
    pub digest: DevelopmentKeyDigest,
    /// Irreversible lifecycle status.
    pub status: DevelopmentCredentialStatus,
    /// Trusted creation time.
    pub created_at: TimestampMicros,
    /// Optional absolute expiry; authentication rejects at or after this instant.
    pub expires_at: Option<TimestampMicros>,
    /// First revocation time.
    pub revoked_at: Option<TimestampMicros>,
    /// Tombstone time after revocation.
    pub deleted_at: Option<TimestampMicros>,
}

impl DevelopmentCredential {
    /// Validates lifecycle and timestamp relationships.
    ///
    /// # Errors
    ///
    /// Rejects negative/non-increasing times and state/timestamp mismatches.
    pub fn validate(&self) -> Result<(), DevelopmentAccessError> {
        if self.created_at.get() < 0
            || self
                .expires_at
                .is_some_and(|expires| expires <= self.created_at)
        {
            return Err(DevelopmentAccessError::InvalidInput);
        }
        match self.status {
            DevelopmentCredentialStatus::Active => {
                if self.revoked_at.is_some() || self.deleted_at.is_some() {
                    return Err(DevelopmentAccessError::InvalidInput);
                }
            }
            DevelopmentCredentialStatus::Revoked => {
                if self
                    .revoked_at
                    .is_none_or(|revoked| revoked < self.created_at)
                    || self.deleted_at.is_some()
                {
                    return Err(DevelopmentAccessError::InvalidInput);
                }
            }
            DevelopmentCredentialStatus::Deleted => {
                if self
                    .revoked_at
                    .is_none_or(|revoked| revoked < self.created_at)
                    || !matches!(
                        (self.revoked_at, self.deleted_at),
                        (Some(revoked), Some(deleted)) if deleted >= revoked
                    )
                {
                    return Err(DevelopmentAccessError::InvalidInput);
                }
            }
        }
        Ok(())
    }
}

/// Verified Development Access identity passed to administrative handlers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentIdentity {
    /// Exact authenticated Environment.
    pub scope: EnvironmentScope,
    /// Credential used, safe for audit attribution and targeted revocation diagnosis.
    pub credential_id: DevelopmentCredentialId,
    /// Server-owned actor; requests cannot override it.
    pub actor: DevelopmentActor,
    /// Configuration revision observed during verification.
    pub configuration_revision: u64,
}

/// Idempotent lifecycle mutation outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentLifecycleResult {
    /// State changed and configuration revision advanced.
    Applied,
    /// Target already had the requested irreversible state.
    Replayed,
}

/// Stable failure taxonomy for Development Access identity and persistence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DevelopmentAccessError {
    /// Management input or lifecycle timestamps are invalid.
    #[error("development access input is invalid")]
    InvalidInput,
    /// Presented key is malformed, absent, expired, inactive, cross-scope, or unverifiable.
    #[error("development access credential is invalid")]
    InvalidCredential,
    /// Requested management credential does not exist in the exact scope.
    #[error("development access credential was not found")]
    NotFound,
    /// ID/label/replay or concurrent state conflicts with the request.
    #[error("development access state conflicts")]
    Conflict,
    /// A supported bounded limit was exceeded.
    #[error("development access limit exceeded")]
    LimitExceeded,
    /// Backend is temporarily unavailable.
    #[error("development access repository is unavailable")]
    Unavailable,
    /// Commit outcome is unknown and must be reconciled before changing intent.
    #[error("development access result is uncertain")]
    ResultUncertain,
    /// Durable contents or migration checksums violate the contract.
    #[error("development access repository is corrupt")]
    Corruption,
    /// Backend/role/version is unsupported.
    #[error("development access backend is unsupported")]
    Unsupported,
    /// Operating-system entropy was unavailable.
    #[error("development access entropy is unavailable")]
    EntropyUnavailable,
}

impl DevelopmentAccessError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "DEVELOPMENT_ACCESS_INPUT_INVALID",
            Self::InvalidCredential => "DEVELOPMENT_ACCESS_CREDENTIAL_INVALID",
            Self::NotFound => "DEVELOPMENT_ACCESS_NOT_FOUND",
            Self::Conflict => "DEVELOPMENT_ACCESS_CONFLICT",
            Self::LimitExceeded => "DEVELOPMENT_ACCESS_LIMIT",
            Self::Unavailable => "DEVELOPMENT_ACCESS_UNAVAILABLE",
            Self::ResultUncertain => "DEVELOPMENT_ACCESS_RESULT_UNCERTAIN",
            Self::Corruption => "DEVELOPMENT_ACCESS_CORRUPT",
            Self::Unsupported => "DEVELOPMENT_ACCESS_UNSUPPORTED",
            Self::EntropyUnavailable => "DEVELOPMENT_ACCESS_ENTROPY_UNAVAILABLE",
        }
    }

    /// Whether an unchanged operation can be retried after external recovery.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable | Self::ResultUncertain)
    }
}
