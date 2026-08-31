//! NATS `JetStream` KV execution ledger and durable result/cancellation bus.

use std::{sync::Arc, time::Duration};

use async_nats::jetstream::{self, kv, stream::StorageType};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use runku_core::InvocationId;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::{
    EXECUTION_CONTROL_FORMAT_VERSION, EXECUTION_RESULT_PAYLOAD_MAX_BYTES, ExecutionCompletion,
    ExecutionControlError, ExecutionControlPlane, ExecutionRecordV1, ExecutionState,
    VersionedExecutionRecord,
    control::{Transition, transition},
};

const CONTROL_OVERHEAD_MAX_BYTES: usize = 2_048;
const ENCODED_RESULT_MAX_BYTES: usize = EXECUTION_RESULT_PAYLOAD_MAX_BYTES.div_ceil(3) * 4;
const CONTROL_VALUE_MAX_BYTES: usize = ENCODED_RESULT_MAX_BYTES + CONTROL_OVERHEAD_MAX_BYTES;
const CAS_ATTEMPTS: usize = 16;
const CHANGE_BUS_CAPACITY: usize = 16_384;

/// Durable execution ledger sizing and retention.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsExecutionControlConfig {
    /// `JetStream` KV bucket name.
    pub bucket: String,
    /// Maximum retained state/result bytes across invocations.
    pub max_bytes: i64,
    /// Retention for completed and abandoned records.
    pub max_age: Duration,
    /// KV replicas; production clusters use three.
    pub replicas: usize,
}

impl Default for NatsExecutionControlConfig {
    fn default() -> Self {
        Self {
            bucket: "RUNKU_EXECUTION_STATE".to_owned(),
            max_bytes: 1_073_741_824,
            max_age: Duration::from_hours(1),
            replicas: 1,
        }
    }
}

/// Shared `JetStream` KV control plane with optimistic state transitions.
#[derive(Clone)]
pub struct NatsExecutionControlPlane {
    store: kv::Store,
    config: NatsExecutionControlConfig,
    change_bus: Arc<ChangeBus>,
}

struct ChangeBus {
    changes: broadcast::Sender<VersionedExecutionRecord>,
    shutdown: watch::Sender<bool>,
}

impl Drop for ChangeBus {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

impl std::fmt::Debug for NatsExecutionControlPlane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NatsExecutionControlPlane")
            .field("bucket", &self.config.bucket)
            .finish_non_exhaustive()
    }
}

impl NatsExecutionControlPlane {
    /// Opens or creates the exact durable KV bucket and rejects configuration drift.
    ///
    /// # Errors
    ///
    /// Rejects unsafe bounds or an unavailable/incompatible `JetStream` bucket.
    pub async fn open(
        client: async_nats::Client,
        config: NatsExecutionControlConfig,
    ) -> Result<Self, ExecutionControlError> {
        validate_config(&config)?;
        let context = jetstream::new(client);
        let desired = kv::Config {
            bucket: config.bucket.clone(),
            description: "Runku durable execution state, result, and cancellation".to_owned(),
            max_value_size: i32::try_from(CONTROL_VALUE_MAX_BYTES)
                .map_err(|_| ExecutionControlError::InvalidRecord)?,
            history: 16,
            max_age: config.max_age,
            max_bytes: config.max_bytes,
            storage: StorageType::File,
            num_replicas: config.replicas,
            ..Default::default()
        };
        let store = match context.get_key_value(config.bucket.clone()).await {
            Ok(store) => store,
            Err(_) => match context.create_key_value(desired.clone()).await {
                Ok(store) => store,
                Err(_) => context
                    .get_key_value(config.bucket.clone())
                    .await
                    .map_err(|_| ExecutionControlError::Unavailable)?,
            },
        };
        let existing = &store.stream.cached_info().config;
        if existing.max_messages_per_subject != desired.history
            || existing.max_age != desired.max_age
            || existing.max_bytes != desired.max_bytes
            || existing.max_message_size != desired.max_value_size
            || existing.storage != desired.storage
            || existing.num_replicas != desired.num_replicas
        {
            return Err(ExecutionControlError::InvalidRecord);
        }
        let (changes, _) = broadcast::channel(CHANGE_BUS_CAPACITY);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        spawn_change_bus(store.clone(), changes.clone(), shutdown_receiver);
        Ok(Self {
            store,
            config,
            change_bus: Arc::new(ChangeBus { changes, shutdown }),
        })
    }

    async fn apply(
        &self,
        invocation_id: InvocationId,
        next: Transition,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        let key = key(invocation_id);
        for _ in 0..CAS_ATTEMPTS {
            let current = self.get(invocation_id).await?;
            let candidate = transition(&current.record, next.clone())?;
            if candidate == current.record {
                return Ok(current);
            }
            let bytes = encode_record(&candidate)?;
            if let Ok(revision) = self
                .store
                .update(&key, bytes.into(), current.revision)
                .await
            {
                return Ok(VersionedExecutionRecord {
                    revision,
                    record: candidate,
                });
            }
        }
        Err(ExecutionControlError::Conflict)
    }
}

#[async_trait]
impl ExecutionControlPlane for NatsExecutionControlPlane {
    async fn register(
        &self,
        invocation_id: InvocationId,
        deadline_unix_ms: u64,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        let record = ExecutionRecordV1 {
            format_version: EXECUTION_CONTROL_FORMAT_VERSION,
            invocation_id,
            deadline_unix_ms,
            state: ExecutionState::Queued,
            result: None,
            error_code: None,
        };
        record.validate()?;
        let key = key(invocation_id);
        if let Ok(revision) = self
            .store
            .create(&key, encode_record(&record)?.into())
            .await
        {
            Ok(VersionedExecutionRecord { revision, record })
        } else {
            let existing = self.get(invocation_id).await?;
            if existing.record.deadline_unix_ms == deadline_unix_ms {
                Ok(existing)
            } else {
                Err(ExecutionControlError::Conflict)
            }
        }
    }

    async fn begin_preparing(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.apply(invocation_id, Transition::Preparing).await
    }

    async fn begin_running(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.apply(invocation_id, Transition::Running).await
    }

    async fn request_cancel(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.apply(invocation_id, Transition::Cancel).await
    }

    async fn complete(
        &self,
        invocation_id: InvocationId,
        completion: ExecutionCompletion,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.apply(invocation_id, Transition::Complete(completion))
            .await
    }

    async fn get(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        let entry = self
            .store
            .entry(key(invocation_id))
            .await
            .map_err(|_| ExecutionControlError::Unavailable)?
            .ok_or(ExecutionControlError::NotFound)?;
        let record = decode_record(&entry.value)?;
        if record.invocation_id != invocation_id {
            return Err(ExecutionControlError::InvalidRecord);
        }
        Ok(VersionedExecutionRecord {
            revision: entry.revision,
            record,
        })
    }

    async fn wait_changed(
        &self,
        invocation_id: InvocationId,
        after: u64,
        wait: Duration,
    ) -> Result<Option<VersionedExecutionRecord>, ExecutionControlError> {
        if wait.is_zero() {
            return Ok(None);
        }
        let mut receiver = self.change_bus.changes.subscribe();
        let current = self.get(invocation_id).await?;
        if current.revision > after {
            return Ok(Some(current));
        }
        match tokio::time::timeout(wait, async {
            loop {
                match receiver.recv().await {
                    Ok(event)
                        if event.record.invocation_id == invocation_id
                            && event.revision > after =>
                    {
                        return Ok(Some(event));
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let current = self.get(invocation_id).await?;
                        if current.revision > after {
                            return Ok(Some(current));
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(None),
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Ok(None),
        }
    }
}

fn spawn_change_bus(
    store: kv::Store,
    changes: broadcast::Sender<VersionedExecutionRecord>,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let mut watcher = tokio::select! {
                result = store.watch_all() => if let Ok(watcher) = result {
                    watcher
                } else {
                    if wait_or_shutdown(&mut shutdown).await {
                        return;
                    }
                    continue;
                },
                _ = shutdown.changed() => return,
            };
            loop {
                let entry = tokio::select! {
                    entry = watcher.next() => entry,
                    _ = shutdown.changed() => return,
                };
                let Some(entry) = entry else { break };
                let Ok(entry) = entry else { break };
                let Ok(record) = decode_record(&entry.value) else {
                    continue;
                };
                if entry.key != key(record.invocation_id) {
                    continue;
                }
                let _ = changes.send(VersionedExecutionRecord {
                    revision: entry.revision,
                    record,
                });
            }
            if wait_or_shutdown(&mut shutdown).await {
                return;
            }
        }
    });
}

async fn wait_or_shutdown(shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        () = tokio::time::sleep(Duration::from_millis(100)) => false,
        _ = shutdown.changed() => true,
    }
}

#[derive(Serialize)]
struct NatsRecordWire<'a> {
    format_version: u16,
    invocation_id: InvocationId,
    deadline_unix_ms: u64,
    state: ExecutionState,
    result_base64url: Option<&'a str>,
    error_code: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedNatsRecordWire {
    format_version: u16,
    invocation_id: InvocationId,
    deadline_unix_ms: u64,
    state: ExecutionState,
    result_base64url: Option<String>,
    error_code: Option<String>,
}

fn encode_record(record: &ExecutionRecordV1) -> Result<Vec<u8>, ExecutionControlError> {
    record.validate()?;
    let encoded = record
        .result
        .as_ref()
        .map(|result| URL_SAFE_NO_PAD.encode(result));
    let bytes = serde_json::to_vec(&NatsRecordWire {
        format_version: record.format_version,
        invocation_id: record.invocation_id,
        deadline_unix_ms: record.deadline_unix_ms,
        state: record.state,
        result_base64url: encoded.as_deref(),
        error_code: record.error_code.as_deref(),
    })
    .map_err(|_| ExecutionControlError::InvalidRecord)?;
    if bytes.len() > CONTROL_VALUE_MAX_BYTES {
        return Err(ExecutionControlError::InvalidRecord);
    }
    Ok(bytes)
}

fn decode_record(bytes: &[u8]) -> Result<ExecutionRecordV1, ExecutionControlError> {
    if bytes.len() > CONTROL_VALUE_MAX_BYTES {
        return Err(ExecutionControlError::InvalidRecord);
    }
    let wire: OwnedNatsRecordWire =
        serde_json::from_slice(bytes).map_err(|_| ExecutionControlError::InvalidRecord)?;
    let result = wire
        .result_base64url
        .map(|encoded| {
            if encoded.len() > ENCODED_RESULT_MAX_BYTES {
                return Err(ExecutionControlError::InvalidRecord);
            }
            URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| ExecutionControlError::InvalidRecord)
        })
        .transpose()?;
    let record = ExecutionRecordV1 {
        format_version: wire.format_version,
        invocation_id: wire.invocation_id,
        deadline_unix_ms: wire.deadline_unix_ms,
        state: wire.state,
        result,
        error_code: wire.error_code,
    };
    record.validate()?;
    Ok(record)
}

fn key(invocation_id: InvocationId) -> String {
    format!("i.{invocation_id}").to_ascii_lowercase()
}

fn validate_config(config: &NatsExecutionControlConfig) -> Result<(), ExecutionControlError> {
    let valid_bucket = !config.bucket.is_empty()
        && config.bucket.len() <= 64
        && config
            .bucket
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_');
    if !valid_bucket
        || config.max_bytes < i64::try_from(CONTROL_VALUE_MAX_BYTES).unwrap_or(i64::MAX)
        || config.max_age < Duration::from_secs(60)
        || config.max_age > Duration::from_hours(168)
        || !(1..=5).contains(&config.replicas)
    {
        return Err(ExecutionControlError::InvalidRecord);
    }
    Ok(())
}
