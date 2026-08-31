//! Durable execution state, result, and cancellation contracts.

use std::{fmt, time::Duration};

use async_trait::async_trait;
use runku_core::InvocationId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version of the durable execution-control record.
pub const EXECUTION_CONTROL_FORMAT_VERSION: u16 = 1;
/// Maximum opaque terminal result accepted by the control plane.
pub const EXECUTION_RESULT_PAYLOAD_MAX_BYTES: usize = 524_288;

/// Durable execution lifecycle. Terminal states never transition again.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    /// Gateway registered the invocation before queue publication.
    Queued,
    /// An agent is validating immutable dependencies before ACK.
    Preparing,
    /// Queue admission was `ACKed` and user code may be running.
    Running,
    /// A caller requested cancellation; an agent must not start new user code.
    CancelRequested,
    /// Invocation completed successfully with a result payload.
    Succeeded,
    /// Invocation completed with a known sanitized runtime error.
    Failed,
    /// Cancellation won before a successful result became visible.
    Cancelled,
    /// The agent lost the ability to determine a post-admission outcome.
    Uncertain,
}

impl ExecutionState {
    /// Whether no further state transition is valid.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Uncertain
        )
    }
}

/// Terminal outcome written by an execution agent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionCompletion {
    /// Canonical runtime-specific result bytes.
    Succeeded(Vec<u8>),
    /// Known sanitized stable error code.
    Failed(String),
    /// Cancellation won.
    Cancelled,
    /// Post-admission outcome cannot be proven.
    Uncertain,
}

/// One latest-value durable execution record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRecordV1 {
    /// Wire format version.
    pub format_version: u16,
    /// Correlation and idempotency identity.
    pub invocation_id: InvocationId,
    /// Absolute Unix deadline in milliseconds.
    pub deadline_unix_ms: u64,
    /// Current lifecycle state.
    pub state: ExecutionState,
    /// Opaque successful result, present only for `succeeded`.
    pub result: Option<Vec<u8>>,
    /// Sanitized stable failure code, present only for `failed`.
    pub error_code: Option<String>,
}

impl ExecutionRecordV1 {
    /// Validates structural and terminal payload invariants.
    ///
    /// # Errors
    ///
    /// Rejects unsupported formats, absent deadlines, oversized output, and state/payload drift.
    pub fn validate(&self) -> Result<(), ExecutionControlError> {
        let valid_error = self.error_code.as_ref().is_none_or(|code| {
            !code.is_empty()
                && code.len() <= 128
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        });
        let valid_payload = self.result.as_ref().is_none_or(|payload| {
            !payload.is_empty() && payload.len() <= EXECUTION_RESULT_PAYLOAD_MAX_BYTES
        });
        let valid_shape = match self.state {
            ExecutionState::Succeeded => self.result.is_some() && self.error_code.is_none(),
            ExecutionState::Failed => self.result.is_none() && self.error_code.is_some(),
            _ => self.result.is_none() && self.error_code.is_none(),
        };
        if self.format_version != EXECUTION_CONTROL_FORMAT_VERSION
            || self.deadline_unix_ms == 0
            || !valid_error
            || !valid_payload
            || !valid_shape
        {
            return Err(ExecutionControlError::InvalidRecord);
        }
        Ok(())
    }
}

/// Record plus backend revision used to wait without missing changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedExecutionRecord {
    /// Monotonic backend revision.
    pub revision: u64,
    /// Validated current record.
    pub record: ExecutionRecordV1,
}

/// Stable control-plane failure taxonomy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionControlError {
    /// Record, transition, or configuration is invalid.
    #[error("execution control input is invalid")]
    InvalidRecord,
    /// Invocation was not registered or its retained state expired.
    #[error("execution record was not found")]
    NotFound,
    /// A concurrent state transition won; the caller should reload state.
    #[error("execution state changed concurrently")]
    Conflict,
    /// Durable state backend is unavailable.
    #[error("execution control plane is unavailable")]
    Unavailable,
}

impl ExecutionControlError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRecord => "EXECUTION_CONTROL_INVALID",
            Self::NotFound => "EXECUTION_NOT_FOUND",
            Self::Conflict => "EXECUTION_STATE_CONFLICT",
            Self::Unavailable => "EXECUTION_CONTROL_UNAVAILABLE",
        }
    }
}

/// Durable latest-state boundary shared by Gateways and execution agents.
#[async_trait]
pub trait ExecutionControlPlane: fmt::Debug + Send + Sync {
    /// Registers one queued invocation idempotently before queue publication.
    async fn register(
        &self,
        invocation_id: InvocationId,
        deadline_unix_ms: u64,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError>;

    /// Marks an agent as preparing immutable dependencies.
    async fn begin_preparing(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError>;

    /// Marks the post-ACK point immediately before user code starts.
    async fn begin_running(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError>;

    /// Requests cancellation durably. Terminal records remain unchanged.
    async fn request_cancel(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError>;

    /// Publishes one terminal completion. Repeating the exact completion is idempotent.
    async fn complete(
        &self,
        invocation_id: InvocationId,
        completion: ExecutionCompletion,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError>;

    /// Reads the current retained record.
    async fn get(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError>;

    /// Waits efficiently for a revision newer than `after`, returning `None` on wait expiry.
    async fn wait_changed(
        &self,
        invocation_id: InvocationId,
        after: u64,
        wait: Duration,
    ) -> Result<Option<VersionedExecutionRecord>, ExecutionControlError>;
}

pub(crate) fn transition(
    current: &ExecutionRecordV1,
    next: Transition,
) -> Result<ExecutionRecordV1, ExecutionControlError> {
    current.validate()?;
    if current.state.is_terminal() {
        if let Transition::Complete(ref completion) = next {
            let candidate = completed_record(current, completion.clone())?;
            return (candidate == *current)
                .then_some(candidate)
                .ok_or(ExecutionControlError::Conflict);
        }
        return Ok(current.clone());
    }
    let mut record = current.clone();
    match next {
        Transition::Preparing => match current.state {
            ExecutionState::Queued | ExecutionState::Preparing => {
                record.state = ExecutionState::Preparing;
            }
            ExecutionState::CancelRequested => return Ok(record),
            _ => return Err(ExecutionControlError::Conflict),
        },
        Transition::Running => match current.state {
            ExecutionState::Preparing | ExecutionState::Running => {
                record.state = ExecutionState::Running;
            }
            ExecutionState::CancelRequested => return Ok(record),
            _ => return Err(ExecutionControlError::Conflict),
        },
        Transition::Cancel => {
            record.state = ExecutionState::CancelRequested;
        }
        Transition::Complete(completion) => return completed_record(current, completion),
    }
    record.validate()?;
    Ok(record)
}

#[derive(Clone)]
pub(crate) enum Transition {
    Preparing,
    Running,
    Cancel,
    Complete(ExecutionCompletion),
}

fn completed_record(
    current: &ExecutionRecordV1,
    completion: ExecutionCompletion,
) -> Result<ExecutionRecordV1, ExecutionControlError> {
    let (state, result, error_code) = match completion {
        ExecutionCompletion::Succeeded(payload) => (ExecutionState::Succeeded, Some(payload), None),
        ExecutionCompletion::Failed(code) => (ExecutionState::Failed, None, Some(code)),
        ExecutionCompletion::Cancelled => (ExecutionState::Cancelled, None, None),
        ExecutionCompletion::Uncertain => (ExecutionState::Uncertain, None, None),
    };
    let record = ExecutionRecordV1 {
        state,
        result,
        error_code,
        ..current.clone()
    };
    record.validate()?;
    Ok(record)
}
