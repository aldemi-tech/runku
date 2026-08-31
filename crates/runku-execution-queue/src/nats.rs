//! NATS `JetStream` work-queue adapter.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_nats::jetstream::{
    self, AckKind,
    consumer::{AckPolicy, DeliverPolicy, PullConsumer, ReplayPolicy, pull},
    stream::{DiscardPolicy, RetentionPolicy, StorageType},
};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use runku_core::{EnvironmentId, InvocationId, ProjectId, ReleaseId, RequestId};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    EXECUTION_JOB_PAYLOAD_MAX_BYTES, ExecutionClass, ExecutionDelivery, ExecutionJobV1,
    ExecutionQueue, ExecutionQueueError,
};

const ENVELOPE_OVERHEAD_MAX_BYTES: usize = 2_048;
const ENCODED_PAYLOAD_MAX_BYTES: usize = EXECUTION_JOB_PAYLOAD_MAX_BYTES.div_ceil(3) * 4;
const MESSAGE_MAX_BYTES: usize = ENCODED_PAYLOAD_MAX_BYTES + ENVELOPE_OVERHEAD_MAX_BYTES;

#[derive(Serialize)]
struct NatsJobWire<'a> {
    format_version: u16,
    invocation_id: InvocationId,
    request_id: RequestId,
    project_id: ProjectId,
    environment_id: EnvironmentId,
    release_id: ReleaseId,
    deadline_unix_ms: u64,
    payload_base64url: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedNatsJobWire {
    format_version: u16,
    invocation_id: InvocationId,
    request_id: RequestId,
    project_id: ProjectId,
    environment_id: EnvironmentId,
    release_id: ReleaseId,
    deadline_unix_ms: u64,
    payload_base64url: String,
}

/// Durable `JetStream` sizing and namespace configuration.
#[derive(Clone, Debug)]
pub struct NatsExecutionQueueConfig {
    /// `JetStream` stream name.
    pub stream_name: String,
    /// Subject prefix owned by this queue.
    pub subject_prefix: String,
    /// Maximum number of queued messages before publishers receive backpressure.
    pub max_messages: i64,
    /// Maximum aggregate persisted bytes.
    pub max_bytes: i64,
    /// Maximum time a job may remain in the queue.
    pub max_age: Duration,
    /// Stream replica count; use three for a production cluster.
    pub replicas: usize,
    /// Consumer acknowledgement window before redelivery.
    pub ack_wait: Duration,
    /// Maximum delivery attempts before `JetStream` stops automatic redelivery.
    pub max_deliver: i64,
    /// Maximum simultaneous outstanding pull requests per execution class.
    pub max_waiting: i64,
}

impl Default for NatsExecutionQueueConfig {
    fn default() -> Self {
        Self {
            stream_name: "RUNKU_EXECUTIONS".to_owned(),
            subject_prefix: "runku.execution.v1".to_owned(),
            max_messages: 100_000,
            max_bytes: 1_073_741_824,
            max_age: Duration::from_mins(15),
            replicas: 1,
            ack_wait: Duration::from_secs(60),
            max_deliver: 5,
            max_waiting: 10_000,
        }
    }
}

/// Shared NATS `JetStream` queue. Authentication and TLS are configured on the supplied client.
#[derive(Clone)]
pub struct NatsExecutionQueue {
    context: jetstream::Context,
    stream: jetstream::stream::Stream,
    config: NatsExecutionQueueConfig,
    consumers: Arc<RwLock<HashMap<ExecutionClass, PullConsumer>>>,
}

impl std::fmt::Debug for NatsExecutionQueue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NatsExecutionQueue")
            .field("stream_name", &self.config.stream_name)
            .field("subject_prefix", &self.config.subject_prefix)
            .finish_non_exhaustive()
    }
}

impl NatsExecutionQueue {
    /// Opens or creates the durable stream. The client must already enforce the deployment's
    /// authentication, TLS, and account isolation policy.
    ///
    /// # Errors
    ///
    /// Rejects unsafe sizing/namespaces or an unavailable `JetStream` backend.
    pub async fn open(
        client: async_nats::Client,
        config: NatsExecutionQueueConfig,
    ) -> Result<Self, ExecutionQueueError> {
        validate_config(&config)?;
        let context = jetstream::new(client);
        let max_message_size =
            i32::try_from(MESSAGE_MAX_BYTES).map_err(|_| ExecutionQueueError::InvalidJob)?;
        let stream = context
            .get_or_create_stream(jetstream::stream::Config {
                name: config.stream_name.clone(),
                description: Some("Runku capacity-aware execution work queue".to_owned()),
                subjects: vec![format!("{}.>", config.subject_prefix)],
                retention: RetentionPolicy::WorkQueue,
                max_messages: config.max_messages,
                max_bytes: config.max_bytes,
                max_age: config.max_age,
                max_message_size,
                discard: DiscardPolicy::New,
                storage: StorageType::File,
                num_replicas: config.replicas,
                duplicate_window: config.max_age.min(Duration::from_secs(120)),
                ..Default::default()
            })
            .await
            .map_err(|_| ExecutionQueueError::Unavailable)?;
        let existing = &stream.cached_info().config;
        if existing.retention != RetentionPolicy::WorkQueue
            || existing.discard != DiscardPolicy::New
            || existing.subjects != [format!("{}.>", config.subject_prefix)]
            || existing.max_messages != config.max_messages
            || existing.max_bytes != config.max_bytes
            || existing.max_age != config.max_age
            || existing.max_message_size != max_message_size
            || existing.storage != StorageType::File
            || existing.num_replicas != config.replicas
        {
            return Err(ExecutionQueueError::InvalidJob);
        }
        Ok(Self {
            context,
            stream,
            config,
            consumers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn consumer(&self, class: &ExecutionClass) -> Result<PullConsumer, ExecutionQueueError> {
        if let Some(consumer) = self.consumers.read().await.get(class).cloned() {
            return Ok(consumer);
        }
        let durable = format!("runku_{}", class.as_str());
        let filter_subject = self.subject_for(class);
        let consumer = self
            .stream
            .get_or_create_consumer(
                &durable,
                pull::Config {
                    durable_name: Some(durable.clone()),
                    name: Some(durable.clone()),
                    description: Some(format!("Runku runner pool {}", class.as_str())),
                    deliver_policy: DeliverPolicy::All,
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: self.config.ack_wait,
                    max_deliver: self.config.max_deliver,
                    filter_subject,
                    replay_policy: ReplayPolicy::Instant,
                    max_waiting: self.config.max_waiting,
                    max_ack_pending: self.config.max_waiting,
                    max_batch: 1,
                    max_bytes: i64::try_from(MESSAGE_MAX_BYTES)
                        .map_err(|_| ExecutionQueueError::InvalidJob)?,
                    max_expires: self.config.ack_wait,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| ExecutionQueueError::Unavailable)?;
        let existing = &consumer.cached_info().config;
        if existing.durable_name.as_deref() != Some(durable.as_str())
            || existing.ack_policy != AckPolicy::Explicit
            || existing.ack_wait != self.config.ack_wait
            || existing.max_deliver != self.config.max_deliver
            || existing.filter_subject != self.subject_for(class)
            || existing.max_waiting != self.config.max_waiting
            || existing.max_ack_pending != self.config.max_waiting
        {
            return Err(ExecutionQueueError::InvalidJob);
        }
        self.consumers
            .write()
            .await
            .insert(class.clone(), consumer.clone());
        Ok(consumer)
    }

    fn subject_for(&self, class: &ExecutionClass) -> String {
        format!("{}.{}", self.config.subject_prefix, class.as_str())
    }
}

#[async_trait]
impl ExecutionQueue for NatsExecutionQueue {
    async fn enqueue(
        &self,
        class: &ExecutionClass,
        job: &ExecutionJobV1,
    ) -> Result<(), ExecutionQueueError> {
        job.validate()?;
        let payload_base64url = URL_SAFE_NO_PAD.encode(&job.payload);
        let payload = serde_json::to_vec(&NatsJobWire {
            format_version: job.format_version,
            invocation_id: job.invocation_id,
            request_id: job.request_id,
            project_id: job.project_id,
            environment_id: job.environment_id,
            release_id: job.release_id,
            deadline_unix_ms: job.deadline_unix_ms,
            payload_base64url: &payload_base64url,
        })
        .map_err(|_| ExecutionQueueError::InvalidPayload)?;
        if payload.len() > MESSAGE_MAX_BYTES {
            return Err(ExecutionQueueError::InvalidPayload);
        }
        self.context
            .send_publish(
                self.subject_for(class),
                jetstream::message::PublishMessage::build()
                    .payload(payload.into())
                    .message_id(job.invocation_id.to_string()),
            )
            .await
            .map_err(|_| ExecutionQueueError::Unavailable)?
            .await
            .map_err(map_publish_error)?;
        Ok(())
    }

    async fn pull(
        &self,
        class: &ExecutionClass,
        wait: Duration,
    ) -> Result<Option<Box<dyn ExecutionDelivery>>, ExecutionQueueError> {
        if wait.is_zero() || wait > self.config.ack_wait {
            return Err(ExecutionQueueError::InvalidJob);
        }
        let consumer = self.consumer(class).await?;
        let mut messages = consumer
            .fetch()
            .max_messages(1)
            .expires(wait)
            .messages()
            .await
            .map_err(|_| ExecutionQueueError::Unavailable)?;
        let Some(message) = messages.next().await else {
            return Ok(None);
        };
        let message = message.map_err(|_| ExecutionQueueError::Unavailable)?;
        let Ok(job) = decode_job(&message.payload) else {
            message
                .ack_with(AckKind::Term)
                .await
                .map_err(|_| ExecutionQueueError::Unavailable)?;
            return Err(ExecutionQueueError::InvalidPayload);
        };
        Ok(Some(Box::new(NatsDelivery { message, job })))
    }
}

fn decode_job(payload: &[u8]) -> Result<ExecutionJobV1, ExecutionQueueError> {
    if payload.len() > MESSAGE_MAX_BYTES {
        return Err(ExecutionQueueError::InvalidPayload);
    }
    let wire: OwnedNatsJobWire =
        serde_json::from_slice(payload).map_err(|_| ExecutionQueueError::InvalidPayload)?;
    if wire.payload_base64url.len() > ENCODED_PAYLOAD_MAX_BYTES {
        return Err(ExecutionQueueError::InvalidPayload);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(wire.payload_base64url)
        .map_err(|_| ExecutionQueueError::InvalidPayload)?;
    let job = ExecutionJobV1 {
        format_version: wire.format_version,
        invocation_id: wire.invocation_id,
        request_id: wire.request_id,
        project_id: wire.project_id,
        environment_id: wire.environment_id,
        release_id: wire.release_id,
        deadline_unix_ms: wire.deadline_unix_ms,
        payload: decoded,
    };
    job.validate()?;
    Ok(job)
}

struct NatsDelivery {
    message: jetstream::Message,
    job: ExecutionJobV1,
}

#[async_trait]
impl ExecutionDelivery for NatsDelivery {
    fn job(&self) -> &ExecutionJobV1 {
        &self.job
    }

    async fn progress(&self) -> Result<(), ExecutionQueueError> {
        self.message
            .ack_with(AckKind::Progress)
            .await
            .map_err(|_| ExecutionQueueError::Unavailable)
    }

    async fn ack(self: Box<Self>) -> Result<(), ExecutionQueueError> {
        self.message
            .double_ack()
            .await
            .map_err(|_| ExecutionQueueError::Unavailable)
    }

    async fn retry(self: Box<Self>, delay: Option<Duration>) -> Result<(), ExecutionQueueError> {
        self.message
            .ack_with(AckKind::Nak(delay))
            .await
            .map_err(|_| ExecutionQueueError::Unavailable)
    }

    async fn terminate(self: Box<Self>) -> Result<(), ExecutionQueueError> {
        self.message
            .ack_with(AckKind::Term)
            .await
            .map_err(|_| ExecutionQueueError::Unavailable)
    }
}

fn validate_config(config: &NatsExecutionQueueConfig) -> Result<(), ExecutionQueueError> {
    let valid_stream_name = !config.stream_name.is_empty()
        && config.stream_name.len() <= 64
        && config
            .stream_name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_');
    let valid_prefix = !config.subject_prefix.is_empty()
        && config.subject_prefix.len() <= 128
        && config.subject_prefix.split('.').all(|token| {
            !token.is_empty()
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        });
    if !valid_stream_name
        || !valid_prefix
        || config.max_messages <= 0
        || config.max_bytes <= 0
        || config.max_age.is_zero()
        || !(1..=5).contains(&config.replicas)
        || config.ack_wait < Duration::from_secs(2)
        || config.max_deliver <= 0
        || config.max_waiting <= 0
    {
        return Err(ExecutionQueueError::InvalidJob);
    }
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn map_publish_error(error: jetstream::context::PublishError) -> ExecutionQueueError {
    let message = error.to_string();
    if message.contains("maximum") || message.contains("limit") || message.contains("discard") {
        ExecutionQueueError::Full
    } else {
        ExecutionQueueError::Unavailable
    }
}
