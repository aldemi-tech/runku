//! Framework-independent service support contracts.

use std::{fmt, time::SystemTime};

use runku_core::{DevelopmentCredentialId, EnvironmentScope, OperationId, RequestId};
use runku_protocol::DevelopmentAdminErrorCodeV1;
use runku_value::TimestampMicros;
use thiserror::Error;

/// Remote administrative operation recorded by bounded audit sinks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentAuditOperation {
    /// Read trusted Environment and Workspace state.
    State,
    /// Create one empty Workspace.
    Create,
    /// Publish a canonical package by CAS.
    Publish,
    /// Validate and make one candidate Release explicitly servable.
    Freeze,
}

/// Sanitized terminal audit outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentAuditOutcome {
    /// Operation completed or replayed successfully.
    Succeeded,
    /// Operation was rejected by auth, policy, input, or state.
    Rejected,
    /// Operation ended in a retryable or uncertain condition.
    Retryable,
    /// An invariant/corruption/internal condition failed closed.
    Failed,
}

/// Token-free bounded audit record produced once per semantic operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DevelopmentAuditEvent {
    /// Server correlation identity.
    pub request_id: RequestId,
    /// Exact trusted Environment scope.
    pub scope: EnvironmentScope,
    /// Operation category.
    pub operation: DevelopmentAuditOperation,
    /// Optional client-supplied idempotency identity.
    pub operation_id: Option<OperationId>,
    /// Verified key identity, absent when authentication failed.
    pub credential_id: Option<DevelopmentCredentialId>,
    /// Stable sanitized terminal code.
    pub error: Option<DevelopmentAdminErrorCodeV1>,
    /// Coarse outcome suitable for aggregate metrics.
    pub outcome: DevelopmentAuditOutcome,
    /// Server-owned occurrence timestamp.
    pub occurred_at: TimestampMicros,
}

/// Nonblocking audit boundary. Implementations must bound memory and cardinality.
pub trait DevelopmentAuditSink: fmt::Debug + Send + Sync {
    /// Attempts to admit one already-sanitized event; audit backpressure never changes the
    /// functional result and must be observable by the sink.
    fn try_emit(&self, event: DevelopmentAuditEvent);
}

/// Injected wall clock for authentication, trusted timestamps, and deterministic tests.
pub trait DevelopmentServiceClock: fmt::Debug + Send + Sync {
    /// Returns current Unix UTC microseconds.
    ///
    /// # Errors
    ///
    /// Fails when the host clock is before the epoch or cannot fit the canonical timestamp.
    fn now(&self) -> Result<TimestampMicros, DevelopmentServiceError>;
}

/// Operating-system clock implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemDevelopmentServiceClock;

impl DevelopmentServiceClock for SystemDevelopmentServiceClock {
    fn now(&self) -> Result<TimestampMicros, DevelopmentServiceError> {
        let micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| DevelopmentServiceError::Internal)?
            .as_micros();
        Ok(TimestampMicros::new(
            i64::try_from(micros).map_err(|_| DevelopmentServiceError::Internal)?,
        ))
    }
}

/// Sanitized semantic failure independent from HTTP status mapping.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DevelopmentServiceError {
    /// Request or package is invalid.
    #[error("development request is invalid")]
    InvalidRequest,
    /// Development bearer is absent, malformed, inactive, expired, or unverifiable.
    #[error("development authentication failed")]
    Unauthenticated,
    /// Verified identity cannot perform the requested operation.
    #[error("development access is denied")]
    Forbidden,
    /// Exact scoped resource does not exist.
    #[error("development resource was not found")]
    NotFound,
    /// CAS, idempotency, or identity state conflicts.
    #[error("development state conflicts")]
    Conflict,
    /// Environment policy denies Workspace synchronization.
    #[error("development policy denied the operation")]
    PolicyDenied,
    /// A bounded service/protocol limit was exceeded.
    #[error("development limit was exceeded")]
    LimitExceeded,
    /// Admission capacity is exhausted.
    #[error("development service is busy")]
    Busy,
    /// A required dependency is temporarily unavailable.
    #[error("development service is unavailable")]
    Unavailable,
    /// A durable commit may have succeeded and must be replayed/reconciled.
    #[error("development result is uncertain")]
    ResultUncertain,
    /// Durable state violates a trusted invariant.
    #[error("development state is corrupt")]
    Corruption,
    /// Unexpected trusted service failure.
    #[error("development service failed internally")]
    Internal,
}

impl DevelopmentServiceError {
    /// Wire error selected without exposing dependency details.
    #[must_use]
    pub const fn wire(self) -> DevelopmentAdminErrorCodeV1 {
        match self {
            Self::InvalidRequest => DevelopmentAdminErrorCodeV1::InvalidRequest,
            Self::Unauthenticated => DevelopmentAdminErrorCodeV1::Unauthenticated,
            Self::Forbidden => DevelopmentAdminErrorCodeV1::Forbidden,
            Self::NotFound => DevelopmentAdminErrorCodeV1::NotFound,
            Self::Conflict => DevelopmentAdminErrorCodeV1::Conflict,
            Self::PolicyDenied => DevelopmentAdminErrorCodeV1::PolicyDenied,
            Self::LimitExceeded => DevelopmentAdminErrorCodeV1::LimitExceeded,
            Self::Busy => DevelopmentAdminErrorCodeV1::Busy,
            Self::Unavailable => DevelopmentAdminErrorCodeV1::Unavailable,
            Self::ResultUncertain => DevelopmentAdminErrorCodeV1::ResultUncertain,
            Self::Corruption => DevelopmentAdminErrorCodeV1::Corruption,
            Self::Internal => DevelopmentAdminErrorCodeV1::Internal,
        }
    }

    /// Whether an exact retry may complete after recovery.
    #[must_use]
    pub const fn retryable(self) -> bool {
        self.wire().retryable()
    }
}

/// Aggregate process-local counters without actor/Workspace labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DevelopmentServiceTelemetrySnapshot {
    /// Successful authenticated state reads.
    pub state_successes: u64,
    /// Successful/replayed creates.
    pub create_successes: u64,
    /// Successful/replayed publishes.
    pub publish_successes: u64,
    /// Successful/replayed freeze evaluations, including compatibility-blocked outcomes.
    pub freeze_successes: u64,
    /// Authentication failures.
    pub authentication_failures: u64,
    /// Policy rejections.
    pub policy_rejections: u64,
    /// CAS/idempotency conflicts.
    pub conflicts: u64,
    /// Retryable dependency/outcome failures.
    pub retryable_failures: u64,
    /// Admission rejections at the HTTP boundary.
    pub admission_rejections: u64,
    /// HTTP deadline responses while detached work continued.
    pub deadline_responses: u64,
}
