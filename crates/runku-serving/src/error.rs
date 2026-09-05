//! Stable serving-policy lifecycle and repository failures.

use thiserror::Error;

/// Stable failure returned by serving-policy operations.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ServingPolicyError {
    /// A policy, timestamp, state, or command is invalid.
    #[error("serving policy input is invalid")]
    InvalidInput,
    /// A bounded input or result exceeds the supported limit.
    #[error("serving policy limit exceeded")]
    LimitExceeded,
    /// The exact Project/Environment policy does not exist.
    #[error("serving policy not found")]
    NotFound,
    /// A policy revision or lifecycle precondition conflicts.
    #[error("serving policy operation conflicts with current state")]
    Conflict,
    /// Weighted Releases do not share identical schema, index, and Cron contracts.
    #[error("serving Releases have incompatible data or Cron contracts")]
    IncompatibleContracts,
    /// An operation identity was reused for a different command.
    #[error("serving operation identity was reused")]
    OperationIdReused,
    /// The repository is temporarily contended.
    #[error("serving policy repository is busy")]
    Busy,
    /// The repository or one of its dependencies is unavailable.
    #[error("serving policy repository is unavailable")]
    Unavailable,
    /// The commit result is unknown and must be reconciled by operation identity.
    #[error("serving policy operation result is uncertain")]
    ResultUncertain,
    /// Durable state violates a schema or lifecycle invariant.
    #[error("serving policy repository is corrupt")]
    Corruption,
    /// The selected physical backend is not permitted for the declared role.
    #[error("serving policy repository backend is unsupported for this role")]
    ProductionBackendUnsupported,
    /// The backend or persisted version is outside the supported window.
    #[error("serving policy backend or format is unsupported")]
    Unsupported,
    /// An internal invariant failed without exposing implementation details.
    #[error("serving policy operation failed internally")]
    Internal,
}

impl ServingPolicyError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "SERVING_POLICY_INPUT_INVALID",
            Self::LimitExceeded => "SERVING_POLICY_LIMIT_EXCEEDED",
            Self::NotFound => "SERVING_POLICY_NOT_FOUND",
            Self::Conflict => "SERVING_POLICY_CONFLICT",
            Self::IncompatibleContracts => "SERVING_POLICY_INCOMPATIBLE_CONTRACTS",
            Self::OperationIdReused => "SERVING_POLICY_OPERATION_ID_REUSED",
            Self::Busy => "SERVING_POLICY_REPOSITORY_BUSY",
            Self::Unavailable => "SERVING_POLICY_REPOSITORY_UNAVAILABLE",
            Self::ResultUncertain => "SERVING_POLICY_RESULT_UNCERTAIN",
            Self::Corruption => "SERVING_POLICY_REPOSITORY_CORRUPT",
            Self::ProductionBackendUnsupported => "SERVING_POLICY_PRODUCTION_BACKEND_UNSUPPORTED",
            Self::Unsupported => "SERVING_POLICY_BACKEND_UNSUPPORTED",
            Self::Internal => "SERVING_POLICY_INTERNAL",
        }
    }

    /// Whether bounded retry or exact operation reconciliation may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Busy | Self::Unavailable | Self::ResultUncertain)
    }
}
