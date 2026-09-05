//! Stable Object Storage failures.

use thiserror::Error;

/// Stable failures returned by the logical Object Storage registry.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectStorageError {
    /// Input is malformed or violates a domain invariant.
    #[error("object storage input is invalid")]
    InvalidInput,
    /// A bounded collection or numeric quota exceeds the supported limit.
    #[error("object storage limit exceeded")]
    LimitExceeded,
    /// The exact-scoped resource does not exist.
    #[error("object storage resource not found")]
    NotFound,
    /// A name, revision precondition, or state transition conflicts.
    #[error("object storage conflict")]
    Conflict,
    /// An idempotency identity was reused with a different command.
    #[error("object storage operation identity reused")]
    OperationIdReused,
    /// The repository is temporarily busy.
    #[error("object storage repository busy")]
    Busy,
    /// The repository is unavailable before commit.
    #[error("object storage repository unavailable")]
    Unavailable,
    /// Commit acknowledgement was lost and operation lookup is required.
    #[error("object storage result uncertain")]
    ResultUncertain,
    /// Persisted state or migration history is corrupt.
    #[error("object storage repository corrupt")]
    Corruption,
    /// The selected backend or persisted version is unsupported.
    #[error("object storage backend or version unsupported")]
    Unsupported,
    /// Production role was paired with a non-production backend.
    #[error("object storage production backend unsupported")]
    ProductionBackendUnsupported,
    /// Secret generation or another internal invariant failed.
    #[error("object storage internal failure")]
    Internal,
}

impl ObjectStorageError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "OBJECT_STORAGE_INVALID_INPUT",
            Self::LimitExceeded => "OBJECT_STORAGE_LIMIT_EXCEEDED",
            Self::NotFound => "OBJECT_STORAGE_NOT_FOUND",
            Self::Conflict => "OBJECT_STORAGE_CONFLICT",
            Self::OperationIdReused => "OBJECT_STORAGE_OPERATION_ID_REUSED",
            Self::Busy => "OBJECT_STORAGE_BUSY",
            Self::Unavailable => "OBJECT_STORAGE_UNAVAILABLE",
            Self::ResultUncertain => "OBJECT_STORAGE_RESULT_UNCERTAIN",
            Self::Corruption => "OBJECT_STORAGE_CORRUPTION",
            Self::Unsupported => "OBJECT_STORAGE_UNSUPPORTED",
            Self::ProductionBackendUnsupported => "OBJECT_STORAGE_PRODUCTION_BACKEND_UNSUPPORTED",
            Self::Internal => "OBJECT_STORAGE_INTERNAL",
        }
    }

    /// Whether retrying or operation reconciliation can make progress.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Busy | Self::Unavailable | Self::ResultUncertain)
    }
}
