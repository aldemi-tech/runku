//! Stable failures for operator authentication and authorization.

use thiserror::Error;

/// Sanitized Platform Identity failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PlatformIdentityError {
    /// A caller-controlled value or model combination is invalid.
    #[error("platform identity input is invalid")]
    InvalidInput,
    /// A configured or protocol bound was exceeded.
    #[error("platform identity limit exceeded")]
    LimitExceeded,
    /// A presented invitation, access token, refresh token, or external identity is invalid.
    #[error("platform authentication failed")]
    Unauthenticated,
    /// A verified operator lacks the required capability in the exact resource scope.
    #[error("platform access is denied")]
    Forbidden,
    /// A requested operator, invitation, or session does not exist.
    #[error("platform identity resource was not found")]
    NotFound,
    /// Durable state conflicts with the requested operation.
    #[error("platform identity state conflicts")]
    Conflict,
    /// The installation already has its initial owner.
    #[error("platform identity bootstrap is already complete")]
    AlreadyInitialized,
    /// An invitation or session is expired, consumed, revoked, or otherwise inactive.
    #[error("platform identity credential is inactive")]
    Inactive,
    /// The operating system could not provide cryptographic entropy.
    #[error("cryptographic entropy is unavailable")]
    EntropyUnavailable,
    /// Persistent data or migration metadata violates a trusted invariant.
    #[error("platform identity storage is corrupt")]
    Corruption,
    /// The repository is temporarily unavailable.
    #[error("platform identity repository is unavailable")]
    Unavailable,
    /// A write may have committed and must be reconciled before retrying with new material.
    #[error("platform identity write result is uncertain")]
    ResultUncertain,
    /// The selected database backend or version is unsupported for the declared role.
    #[error("platform identity backend is unsupported")]
    Unsupported,
}

impl PlatformIdentityError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "PLATFORM_IDENTITY_INPUT_INVALID",
            Self::LimitExceeded => "PLATFORM_IDENTITY_LIMIT_EXCEEDED",
            Self::Unauthenticated => "PLATFORM_AUTHENTICATION_FAILED",
            Self::Forbidden => "PLATFORM_ACCESS_DENIED",
            Self::NotFound => "PLATFORM_IDENTITY_NOT_FOUND",
            Self::Conflict => "PLATFORM_IDENTITY_CONFLICT",
            Self::AlreadyInitialized => "PLATFORM_BOOTSTRAP_COMPLETE",
            Self::Inactive => "PLATFORM_CREDENTIAL_INACTIVE",
            Self::EntropyUnavailable => "PLATFORM_IDENTITY_ENTROPY_UNAVAILABLE",
            Self::Corruption => "PLATFORM_IDENTITY_STORAGE_CORRUPT",
            Self::Unavailable => "PLATFORM_IDENTITY_UNAVAILABLE",
            Self::ResultUncertain => "PLATFORM_IDENTITY_RESULT_UNCERTAIN",
            Self::Unsupported => "PLATFORM_IDENTITY_BACKEND_UNSUPPORTED",
        }
    }

    /// Whether an exact retry may succeed after dependency recovery.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::EntropyUnavailable | Self::Unavailable | Self::ResultUncertain
        )
    }
}
