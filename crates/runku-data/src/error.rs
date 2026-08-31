//! Stable storage error taxonomy.

use thiserror::Error;

/// Error returned by the `LogicalStore` boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StoreError {
    /// A commit contains no durable mutation.
    #[error("commit batch must contain at least one durable mutation")]
    EmptyBatch,
    /// A v1 count or byte limit was exceeded.
    #[error("storage operation exceeds a v1 limit")]
    LimitExceeded,
    /// The same logical key appears more than once in a batch.
    #[error("storage operation contains a duplicate logical mutation")]
    DuplicateMutation,
    /// Range bounds or limit are invalid.
    #[error("storage range is invalid")]
    InvalidRange,
    /// An operation ID already exists with different content.
    #[error("operation ID was reused with different content")]
    OperationIdReused,
    /// Expected document revision does not match current state.
    #[error("document mutation conflicted with current revision")]
    MutationConflict,
    /// Requested logical resource does not exist in the supplied scope.
    #[error("storage resource was not found")]
    NotFound,
    /// Scheduled Invocation completion does not own the active lease.
    #[error("scheduled invocation lease is no longer owned")]
    LeaseLost,
    /// Outbox acknowledgement does not own the active fenced consumer lease.
    #[error("outbox consumer lease is no longer owned")]
    OutboxLeaseLost,
    /// A production role was requested from a local-only backend.
    #[error("storage backend is not allowed for production")]
    ProductionBackendUnsupported,
    /// Storage is temporarily busy or its bounded pool is exhausted.
    #[error("storage is temporarily busy")]
    Busy,
    /// Transaction must be retried because serialization/deadlock aborted it.
    #[error("storage transaction requires retry")]
    SerializationFailure,
    /// Commit outcome could not be determined by the adapter.
    #[error("storage commit result is uncertain")]
    ResultUncertain,
    /// Persisted bytes or relational invariants are corrupt.
    #[error("stored data violates a persistent invariant")]
    Corruption,
    /// Schema migration could not be validated or applied.
    #[error("storage schema migration failed")]
    MigrationFailed,
    /// Backend is unavailable.
    #[error("storage backend is unavailable")]
    Unavailable,
    /// Unexpected implementation failure without safe public detail.
    #[error("storage operation failed internally")]
    Internal,
}

impl StoreError {
    /// Stable machine-readable public error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::EmptyBatch => "STORAGE_BATCH_EMPTY",
            Self::LimitExceeded => "STORAGE_LIMIT_EXCEEDED",
            Self::DuplicateMutation => "STORAGE_MUTATION_DUPLICATE",
            Self::InvalidRange => "STORAGE_RANGE_INVALID",
            Self::OperationIdReused => "OPERATION_ID_REUSED",
            Self::MutationConflict => "MUTATION_CONFLICT",
            Self::NotFound => "STORAGE_NOT_FOUND",
            Self::LeaseLost => "SCHEDULE_LEASE_LOST",
            Self::OutboxLeaseLost => "OUTBOX_LEASE_LOST",
            Self::ProductionBackendUnsupported => "STORAGE_BACKEND_PRODUCTION_UNSUPPORTED",
            Self::Busy => "STORAGE_BUSY",
            Self::SerializationFailure => "STORAGE_SERIALIZATION_FAILURE",
            Self::ResultUncertain => "STORAGE_RESULT_UNCERTAIN",
            Self::Corruption => "STORAGE_CORRUPTION",
            Self::MigrationFailed => "STORAGE_MIGRATION_FAILED",
            Self::Unavailable => "STORAGE_UNAVAILABLE",
            Self::Internal => "STORAGE_INTERNAL_ERROR",
        }
    }

    /// Whether retrying may succeed without changing logical input.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::MutationConflict
                | Self::Busy
                | Self::SerializationFailure
                | Self::ResultUncertain
                | Self::OutboxLeaseLost
                | Self::Unavailable
        )
    }
}
