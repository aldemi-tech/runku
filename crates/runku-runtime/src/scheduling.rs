//! Typed scheduling authority exposed only to capability-authorized invocations.

use std::{fmt, time::Instant};

use async_trait::async_trait;
use runku_core::{FunctionName, ScheduledInvocationId};
use runku_value::{CanonicalValue, TimestampMicros};
use thiserror::Error;

use crate::CancellationToken;

/// Absolute or invocation-relative execution time requested by user code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleTime {
    /// Non-negative delay in microseconds from the coordinator's fixed invocation base.
    AfterMicros(u64),
    /// Exact UTC timestamp in microseconds.
    At(TimestampMicros),
}

/// Canonical request crossing the Safe Runtime scheduling boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleRequest {
    /// Logical internal Mutation or Action destination.
    pub function: FunctionName,
    /// Canonical destination arguments.
    pub arguments: CanonicalValue,
    /// Relative or absolute execution time.
    pub time: ScheduleTime,
    /// Optional caller deduplication key.
    pub idempotency_key: Option<String>,
}

/// Sanitized scheduling broker failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ScheduleError {
    /// Destination, time, key, or arguments are invalid.
    #[error("schedule request is invalid")]
    InvalidRequest,
    /// Per-invocation scheduling limits were exceeded.
    #[error("schedule request exceeds a limit")]
    LimitExceeded,
    /// Durable scheduling storage rejected the request.
    #[error("schedule persistence failed")]
    Storage,
    /// Scheduling broker is temporarily unavailable.
    #[error("schedule broker is unavailable")]
    Unavailable,
    /// Invocation deadline elapsed before persistence completed.
    #[error("schedule persistence timed out")]
    Timeout,
    /// Persistence may have committed but its exact result could not be recovered before deadline.
    #[error("schedule persistence result is uncertain")]
    ResultUncertain,
    /// Caller cancelled before persistence completed.
    #[error("schedule persistence was cancelled")]
    Cancelled,
}

impl ScheduleError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "SCHEDULE_REQUEST_INVALID",
            Self::LimitExceeded => "SCHEDULE_LIMIT_EXCEEDED",
            Self::Storage => "SCHEDULE_STORAGE_FAILED",
            Self::Unavailable => "SCHEDULE_UNAVAILABLE",
            Self::Timeout => "SCHEDULE_TIMEOUT",
            Self::ResultUncertain => "SCHEDULE_RESULT_UNCERTAIN",
            Self::Cancelled => "SCHEDULE_CANCELLED",
        }
    }
}

/// Durable schedule creation authority injected into one invocation.
#[async_trait]
pub trait ScheduleCreate: fmt::Debug + Send + Sync {
    /// Persists or transactionally buffers one invocation and returns its stable identity.
    async fn create(
        &self,
        request: ScheduleRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ScheduledInvocationId, ScheduleError>;
}
