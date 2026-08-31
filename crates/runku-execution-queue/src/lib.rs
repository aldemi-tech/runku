//! Execution queue contracts and in-process/NATS `JetStream` adapters.
//!
//! Runners issue bounded pull requests only when they own a free execution slot. If a runner is
//! already waiting, `JetStream` delivers a newly published job immediately; otherwise it persists
//! the job until capacity becomes available.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod agent;
mod control;
mod memory;
mod memory_control;
mod nats;
mod nats_control;

use std::{fmt, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use runku_core::{EnvironmentId, InvocationId, ProjectId, ReleaseId, RequestId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use agent::{
    ExecutionAgent, ExecutionAgentConfig, ExecutionAgentTelemetrySnapshot, ExecutionHandler,
    ExecutionHandlerError, ExecutionPreparationError, PreparedExecution,
};
pub use control::{
    EXECUTION_CONTROL_FORMAT_VERSION, EXECUTION_RESULT_PAYLOAD_MAX_BYTES, ExecutionCompletion,
    ExecutionControlError, ExecutionControlPlane, ExecutionRecordV1, ExecutionState,
    VersionedExecutionRecord,
};
pub use memory::InMemoryExecutionQueue;
pub use memory_control::InMemoryExecutionControlPlane;
pub use nats::{NatsExecutionQueue, NatsExecutionQueueConfig};
pub use nats_control::{NatsExecutionControlConfig, NatsExecutionControlPlane};

/// Version of the durable execution job envelope.
pub const EXECUTION_JOB_FORMAT_VERSION: u16 = 1;
/// Maximum opaque invocation payload accepted by the queue.
pub const EXECUTION_JOB_PAYLOAD_MAX_BYTES: usize = 524_288;

/// Startup-only queue backend selection for a server composition.
#[derive(Clone, Debug)]
pub enum ServerExecutionQueueConfig {
    /// Bounded, non-durable queue for one development/self-hosted process.
    InMemory {
        /// Maximum number of waiting jobs.
        capacity: usize,
    },
    /// Shared durable NATS `JetStream` queue for horizontal runner agents.
    NatsJetStream(NatsExecutionQueueConfig),
}

impl ServerExecutionQueueConfig {
    /// Opens the selected queue. A pre-authenticated NATS client is required only for `JetStream`.
    ///
    /// # Errors
    ///
    /// Rejects a missing/unexpected NATS client or invalid backend configuration.
    pub async fn open(
        self,
        nats_client: Option<async_nats::Client>,
    ) -> Result<Arc<dyn ExecutionQueue>, ExecutionQueueError> {
        match (self, nats_client) {
            (Self::InMemory { capacity }, None) => InMemoryExecutionQueue::new(capacity)
                .map(|queue| Arc::new(queue) as Arc<dyn ExecutionQueue>),
            (Self::NatsJetStream(config), Some(client)) => NatsExecutionQueue::open(client, config)
                .await
                .map(|queue| Arc::new(queue) as Arc<dyn ExecutionQueue>),
            _ => Err(ExecutionQueueError::InvalidJob),
        }
    }
}

/// Exact pool of compatible runners, encoded as one safe NATS subject token.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ExecutionClass(String);

impl ExecutionClass {
    /// Creates a class such as `node_oci_v1` or `node_host_v1`.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, or unsafe NATS subject tokens.
    pub fn new(value: impl Into<String>) -> Result<Self, ExecutionQueueError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ExecutionQueueError::InvalidJob);
        }
        Ok(Self(value))
    }

    /// Returns the canonical NATS-safe token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExecutionClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ExecutionClass {
    type Err = ExecutionQueueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl From<ExecutionClass> for String {
    fn from(value: ExecutionClass) -> Self {
        value.0
    }
}

impl TryFrom<String> for ExecutionClass {
    type Error = ExecutionQueueError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Durable queue envelope. It contains routing metadata and arguments, never source or artifacts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionJobV1 {
    /// Wire format version.
    pub format_version: u16,
    /// Globally unique execution identity used for deduplication and correlation.
    pub invocation_id: InvocationId,
    /// Originating request identity.
    pub request_id: RequestId,
    /// Owning project.
    pub project_id: ProjectId,
    /// Owning environment.
    pub environment_id: EnvironmentId,
    /// Immutable release selected before enqueueing.
    pub release_id: ReleaseId,
    /// Absolute Unix deadline in milliseconds.
    pub deadline_unix_ms: u64,
    /// Bounded runtime-specific invocation envelope; artifacts are forbidden here.
    pub payload: Vec<u8>,
}

impl ExecutionJobV1 {
    /// Validates bounds and format before crossing a queue boundary.
    ///
    /// # Errors
    ///
    /// Rejects unsupported versions, absent deadlines, and oversized/empty payloads.
    pub fn validate(&self) -> Result<(), ExecutionQueueError> {
        if self.format_version != EXECUTION_JOB_FORMAT_VERSION
            || self.deadline_unix_ms == 0
            || self.payload.is_empty()
            || self.payload.len() > EXECUTION_JOB_PAYLOAD_MAX_BYTES
        {
            return Err(ExecutionQueueError::InvalidJob);
        }
        Ok(())
    }
}

/// Stable execution queue failure taxonomy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionQueueError {
    /// Queue class, envelope, or configuration is invalid.
    #[error("execution queue input is invalid")]
    InvalidJob,
    /// Encoded job is malformed or exceeds the wire limit.
    #[error("execution queue payload is invalid")]
    InvalidPayload,
    /// Queue is at its configured durable capacity.
    #[error("execution queue is full")]
    Full,
    /// Queue operation timed out and may be retried.
    #[error("execution queue operation timed out")]
    Timeout,
    /// Queue backend is unavailable.
    #[error("execution queue is unavailable")]
    Unavailable,
}

impl ExecutionQueueError {
    /// Stable machine-readable public code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidJob => "EXECUTION_QUEUE_JOB_INVALID",
            Self::InvalidPayload => "EXECUTION_QUEUE_PAYLOAD_INVALID",
            Self::Full => "EXECUTION_QUEUE_FULL",
            Self::Timeout => "EXECUTION_QUEUE_TIMEOUT",
            Self::Unavailable => "EXECUTION_QUEUE_UNAVAILABLE",
        }
    }

    /// Whether retrying may succeed without changing the logical job.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Full | Self::Timeout | Self::Unavailable)
    }
}

/// One leased job that must be explicitly acknowledged, retried, or terminated.
#[async_trait]
pub trait ExecutionDelivery: Send + Sync {
    /// Returns the validated immutable job.
    fn job(&self) -> &ExecutionJobV1;

    /// Extends the lease while immutable dependencies are still being prepared.
    async fn progress(&self) -> Result<(), ExecutionQueueError>;

    /// Confirms durable admission by this runner before user code starts.
    async fn ack(self: Box<Self>) -> Result<(), ExecutionQueueError>;

    /// Releases the job for another delivery because execution has not started.
    async fn retry(self: Box<Self>, delay: Option<Duration>) -> Result<(), ExecutionQueueError>;

    /// Permanently removes an invalid/expired poison job.
    async fn terminate(self: Box<Self>) -> Result<(), ExecutionQueueError>;
}

/// Queue used by publishers and capacity-aware runner agents.
#[async_trait]
pub trait ExecutionQueue: Send + Sync {
    /// Durably publishes one validated job to an exact runner class.
    async fn enqueue(
        &self,
        class: &ExecutionClass,
        job: &ExecutionJobV1,
    ) -> Result<(), ExecutionQueueError>;

    /// Waits for at most one job while the caller owns one free execution slot.
    async fn pull(
        &self,
        class: &ExecutionClass,
        wait: Duration,
    ) -> Result<Option<Box<dyn ExecutionDelivery>>, ExecutionQueueError>;
}
