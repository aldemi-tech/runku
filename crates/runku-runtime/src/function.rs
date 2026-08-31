//! Mediated nested Function invocation boundary.

use std::{fmt, time::Instant};

use async_trait::async_trait;
use runku_core::FunctionName;
use runku_value::CanonicalValue;
use thiserror::Error;

use crate::CancellationToken;

/// Exact nested Function execution kind requested by a Platform Op.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FunctionCallKind {
    /// Read-only reactive Function.
    Query,
    /// Transactional state-changing Function.
    Mutation,
    /// Non-transactional external-effect Function.
    Action,
}

/// Canonical bounded input passed from Runtime to a trusted nested-call broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionCallRequest {
    /// Required target type.
    pub kind: FunctionCallKind,
    /// Logical Function name inside the already-pinned manifest.
    pub function: FunctionName,
    /// Canonical arguments after the JS/Rust value bridge.
    pub arguments: CanonicalValue,
}

/// Sanitized nested Function call failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FunctionCallError {
    /// Name, arguments, target type, or derived identity is invalid.
    #[error("nested function request is invalid")]
    InvalidRequest,
    /// Caller capability or target visibility/auth policy denied the call.
    #[error("nested function call is denied")]
    Denied,
    /// No Function with the requested logical name exists in the pinned manifest.
    #[error("nested function was not found")]
    NotFound,
    /// Bounded nested execution capacity is currently exhausted.
    #[error("nested function execution is busy")]
    Busy,
    /// A required execution coordinator is unavailable.
    #[error("nested function execution is unavailable")]
    Unavailable,
    /// The inherited invocation deadline elapsed.
    #[error("nested function call timed out")]
    Timeout,
    /// The inherited invocation was cancelled.
    #[error("nested function call was cancelled")]
    Cancelled,
    /// Depth, call count, bytes, or another trusted limit was exceeded.
    #[error("nested function call exceeds a limit")]
    LimitExceeded,
    /// Target Runtime/storage/egress failed after admission.
    #[error("nested function execution failed")]
    Execution,
}

impl FunctionCallError {
    /// Stable machine-readable code exposed only as a sanitized Platform Op rejection.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "FUNCTION_CALL_INVALID",
            Self::Denied => "FUNCTION_CALL_DENIED",
            Self::NotFound => "FUNCTION_CALL_NOT_FOUND",
            Self::Busy => "FUNCTION_CALL_BUSY",
            Self::Unavailable => "FUNCTION_CALL_UNAVAILABLE",
            Self::Timeout => "FUNCTION_CALL_TIMEOUT",
            Self::Cancelled => "FUNCTION_CALL_CANCELLED",
            Self::LimitExceeded => "FUNCTION_CALL_LIMIT_EXCEEDED",
            Self::Execution => "FUNCTION_CALL_EXECUTION_FAILED",
        }
    }
}

/// Trusted authority for nested calls inside one already-resolved invocation tree.
#[async_trait]
pub trait FunctionInvoke: fmt::Debug + Send + Sync {
    /// Executes one call without re-resolving a mutable Code Target or raw credentials.
    async fn invoke(
        &self,
        request: FunctionCallRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CanonicalValue, FunctionCallError>;
}
