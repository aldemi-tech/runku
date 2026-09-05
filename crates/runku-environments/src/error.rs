//! Stable Environment lifecycle and repository failures.

use thiserror::Error;

/// Stable failure returned by Environment lifecycle and persistence operations.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EnvironmentError {
    /// A name, slug, region, timestamp, state, or command is invalid.
    #[error("environment input is invalid")]
    InvalidInput,
    /// A bounded input or result exceeds the supported limit.
    #[error("environment limit exceeded")]
    LimitExceeded,
    /// The exact Project/Environment record does not exist.
    #[error("environment not found")]
    NotFound,
    /// A slug, configuration revision, or lifecycle precondition conflicts.
    #[error("environment operation conflicts with current state")]
    Conflict,
    /// An operation identity was reused for a different command.
    #[error("environment operation identity was reused")]
    OperationIdReused,
    /// The repository is temporarily contended.
    #[error("environment repository is busy")]
    Busy,
    /// The repository or one of its dependencies is unavailable.
    #[error("environment repository is unavailable")]
    Unavailable,
    /// The commit result is unknown and must be reconciled by operation identity.
    #[error("environment operation result is uncertain")]
    ResultUncertain,
    /// Durable state violates a schema or lifecycle invariant.
    #[error("environment repository is corrupt")]
    Corruption,
    /// The selected physical backend is not permitted for the declared role.
    #[error("environment repository backend is unsupported for this role")]
    ProductionBackendUnsupported,
    /// The backend version is outside the supported compatibility window.
    #[error("environment repository backend version is unsupported")]
    Unsupported,
    /// An internal invariant failed without exposing implementation details.
    #[error("environment operation failed internally")]
    Internal,
}

impl EnvironmentError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "ENVIRONMENT_INPUT_INVALID",
            Self::LimitExceeded => "ENVIRONMENT_LIMIT_EXCEEDED",
            Self::NotFound => "ENVIRONMENT_NOT_FOUND",
            Self::Conflict => "ENVIRONMENT_CONFLICT",
            Self::OperationIdReused => "ENVIRONMENT_OPERATION_ID_REUSED",
            Self::Busy => "ENVIRONMENT_REPOSITORY_BUSY",
            Self::Unavailable => "ENVIRONMENT_REPOSITORY_UNAVAILABLE",
            Self::ResultUncertain => "ENVIRONMENT_RESULT_UNCERTAIN",
            Self::Corruption => "ENVIRONMENT_REPOSITORY_CORRUPT",
            Self::ProductionBackendUnsupported => "ENVIRONMENT_PRODUCTION_BACKEND_UNSUPPORTED",
            Self::Unsupported => "ENVIRONMENT_BACKEND_UNSUPPORTED",
            Self::Internal => "ENVIRONMENT_INTERNAL",
        }
    }

    /// Whether retrying or reconciling the operation may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Busy | Self::Unavailable | Self::ResultUncertain)
    }
}
