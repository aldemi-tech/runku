//! Stable Safe Runtime error taxonomy.

use thiserror::Error;

/// Public, sanitized failure returned by the Safe Runtime boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeError {
    /// Runtime limits or worker/queue dimensions are outside safe supported bounds.
    #[error("safe runtime configuration is invalid")]
    InvalidConfiguration,
    /// Invocation scope, Release, Function, or budget is inconsistent.
    #[error("runtime invocation is invalid")]
    InvalidInvocation,
    /// Canonical caller arguments do not satisfy the selected Function contract.
    #[error("runtime arguments do not satisfy the function contract")]
    InvalidArguments,
    /// Runtime class or platform JavaScript version is not implemented by this node.
    #[error("requested runtime is unsupported")]
    UnsupportedRuntime,
    /// Immutable artifact or selected implementation failed validation.
    #[error("runtime artifact validation failed")]
    InvalidArtifact,
    /// Selected Function does not exist in the exact Release manifest.
    #[error("runtime function was not found")]
    FunctionNotFound,
    /// Admission queue is currently full.
    #[error("safe runtime is busy")]
    Busy,
    /// Runtime supervisor has no available worker channel.
    #[error("safe runtime is unavailable")]
    Unavailable,
    /// Wall deadline elapsed before the invocation completed.
    #[error("runtime invocation deadline exceeded")]
    DeadlineExceeded,
    /// Explicit caller cancellation won the termination race.
    #[error("runtime invocation was cancelled")]
    Cancelled,
    /// The isolate approached its configured V8 heap limit.
    #[error("runtime invocation exceeded its heap limit")]
    HeapLimitExceeded,
    /// User module parse/evaluation/handler code threw or rejected.
    #[error("runtime JavaScript execution failed")]
    JavaScript,
    /// Handler export or returned JavaScript value violates `platform-js-1`.
    #[error("runtime result is invalid")]
    InvalidResult,
    /// A worker failed internally without exposing private engine detail.
    #[error("safe runtime failed internally")]
    Internal,
}

impl RuntimeError {
    /// Stable machine-readable public error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "RUNTIME_CONFIGURATION_INVALID",
            Self::InvalidInvocation => "RUNTIME_INVOCATION_INVALID",
            Self::InvalidArguments => "RUNTIME_ARGUMENTS_INVALID",
            Self::UnsupportedRuntime => "RUNTIME_VERSION_UNSUPPORTED",
            Self::InvalidArtifact => "RUNTIME_ARTIFACT_INVALID",
            Self::FunctionNotFound => "RUNTIME_FUNCTION_NOT_FOUND",
            Self::Busy => "RUNTIME_BUSY",
            Self::Unavailable => "RUNTIME_UNAVAILABLE",
            Self::DeadlineExceeded => "RUNTIME_DEADLINE_EXCEEDED",
            Self::Cancelled => "RUNTIME_CANCELLED",
            Self::HeapLimitExceeded => "RUNTIME_HEAP_LIMIT_EXCEEDED",
            Self::JavaScript => "RUNTIME_JAVASCRIPT_ERROR",
            Self::InvalidResult => "RUNTIME_RESULT_INVALID",
            Self::Internal => "RUNTIME_INTERNAL_ERROR",
        }
    }

    /// Whether retrying may succeed without changing logical input.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Busy | Self::Unavailable)
    }
}
