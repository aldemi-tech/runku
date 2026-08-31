//! Durable Gateway-to-agent Full Node execution vertical.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_core::{EnvironmentScope, FunctionId, InvocationId};
use runku_execution_queue::{
    EXECUTION_JOB_FORMAT_VERSION, ExecutionClass, ExecutionCompletion, ExecutionControlError,
    ExecutionControlPlane, ExecutionHandler, ExecutionHandlerError, ExecutionJobV1,
    ExecutionPreparationError, ExecutionQueue, ExecutionState, PreparedExecution,
};
use runku_observability::{
    InvocationPerformanceRecorder, InvocationPerformanceSink, PerformanceComponent,
    PerformanceOperation, PerformanceRuntime,
};
use runku_protocol::WireValueV1;
use runku_releases::{ArtifactStore, ReleaseManifestV1, ReleaseRepository, Sha256Digest};
use runku_runtime::{CancellationToken, InvocationRequest, RuntimeError};
use serde::{Deserialize, Serialize};

use crate::{FullNodeActionOutcome, FullNodeActionRuntime};

/// Version of the runtime-specific payload stored inside [`ExecutionJobV1`].
pub const REMOTE_NODE_INVOCATION_FORMAT_VERSION: u16 = 1;

/// Gateway queue selection and bounded result-wait settings.
#[derive(Clone, Debug)]
pub struct QueuedNodeRuntimeConfig {
    /// Exact compatible agent pool.
    pub class: ExecutionClass,
    /// Maximum duration of one durable wait subscription before refreshing it.
    pub result_wait: Duration,
}

impl QueuedNodeRuntimeConfig {
    fn validate(&self) -> Result<(), RuntimeError> {
        if self.result_wait < Duration::from_millis(10)
            || self.result_wait > Duration::from_secs(30)
        {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Full Node runtime adapter used by a Gateway: it enqueues and waits for durable agent output.
pub struct QueuedNodeRuntime {
    queue: Arc<dyn ExecutionQueue>,
    control: Arc<dyn ExecutionControlPlane>,
    config: QueuedNodeRuntimeConfig,
}

impl std::fmt::Debug for QueuedNodeRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueuedNodeRuntime")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl QueuedNodeRuntime {
    /// Creates a Gateway-side queued Full Node runtime.
    ///
    /// # Errors
    ///
    /// Rejects unsafe wait bounds.
    pub fn new(
        queue: Arc<dyn ExecutionQueue>,
        control: Arc<dyn ExecutionControlPlane>,
        config: QueuedNodeRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        config.validate()?;
        Ok(Self {
            queue,
            control,
            config,
        })
    }

    async fn execute_inner(
        &self,
        request: InvocationRequest,
    ) -> Result<FullNodeActionOutcome, RuntimeError> {
        let total_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Gateway,
                PerformanceOperation::Invocation,
                runku_value::encode_stored_value(request.arguments())
                    .ok()
                    .and_then(|bytes| u64::try_from(bytes.len()).ok()),
            )
        });
        let result = self.execute_measured(&request).await;
        let output_bytes = result.as_ref().ok().and_then(|outcome| {
            runku_value::encode_stored_value(&outcome.value)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok())
        });
        crate::performance::finish(total_timer, &result, output_bytes, None);
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_measured(
        &self,
        request: &InvocationRequest,
    ) -> Result<FullNodeActionOutcome, RuntimeError> {
        let encode_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Gateway,
                PerformanceOperation::EncodeInput,
                None,
            )
        });
        let validation = self.validate_manifest(request.manifest());
        crate::performance::finish(encode_timer, &validation, None, None);
        validation?;
        let deadline_unix_ms = absolute_deadline(request.wall_timeout())?;
        let payload = encode_invocation(request.function_id(), request.arguments())?;
        let job = ExecutionJobV1 {
            format_version: EXECUTION_JOB_FORMAT_VERSION,
            invocation_id: request.invocation_id(),
            request_id: request.request_id(),
            project_id: request.scope().project_id(),
            environment_id: request.scope().environment_id(),
            release_id: request.release_id(),
            deadline_unix_ms,
            payload,
        };
        let register_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::ControlPlane,
                PerformanceOperation::Register,
                None,
            )
        });
        let registration = self
            .control
            .register(job.invocation_id, deadline_unix_ms)
            .await
            .map_err(map_control);
        crate::performance::finish(register_timer, &registration, None, None);
        let mut record = registration?;
        let publish_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Queue,
                PerformanceOperation::Publish,
                u64::try_from(job.payload.len()).ok(),
            )
        });
        let published = self
            .queue
            .enqueue(&self.config.class, &job)
            .await
            .map_err(map_queue);
        crate::performance::finish(publish_timer, &published, None, None);
        published?;
        let mut queue_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Queue,
                PerformanceOperation::QueueWait,
                None,
            )
        });
        let mut result_timer = None;
        let cancellation = request.cancellation();
        let local_deadline = tokio::time::Instant::now() + request.wall_timeout();
        loop {
            if let Some(outcome) = terminal_outcome(&record.record)? {
                if let Some(timer) = queue_timer.take() {
                    crate::performance::finish(
                        Some(timer),
                        &outcome,
                        record
                            .record
                            .result
                            .as_ref()
                            .and_then(|bytes| u64::try_from(bytes.len()).ok()),
                        None,
                    );
                }
                if let Some(timer) = result_timer.take() {
                    crate::performance::finish(
                        Some(timer),
                        &outcome,
                        record
                            .record
                            .result
                            .as_ref()
                            .and_then(|bytes| u64::try_from(bytes.len()).ok()),
                        None,
                    );
                }
                return outcome;
            }
            if record.record.state != ExecutionState::Queued
                && let Some(timer) = queue_timer.take()
            {
                crate::performance::finish(Some(timer), &Ok::<(), RuntimeError>(()), None, None);
                result_timer = request.performance().map(|recorder| {
                    recorder.start(
                        PerformanceComponent::Result,
                        PerformanceOperation::ResultWait,
                        None,
                    )
                });
            }
            let remaining = local_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let _ = self.control.request_cancel(job.invocation_id).await;
                crate::performance::finish(
                    queue_timer.take(),
                    &Err::<(), RuntimeError>(RuntimeError::DeadlineExceeded),
                    None,
                    None,
                );
                crate::performance::finish(
                    result_timer.take(),
                    &Err::<(), RuntimeError>(RuntimeError::DeadlineExceeded),
                    None,
                    None,
                );
                return Err(RuntimeError::DeadlineExceeded);
            }
            let wait = remaining.min(self.config.result_wait);
            tokio::select! {
                () = cancellation.cancelled() => {
                    self.control
                        .request_cancel(job.invocation_id)
                        .await
                        .map_err(map_control)?;
                    crate::performance::finish(
                        queue_timer.take(),
                        &Err::<(), RuntimeError>(RuntimeError::Cancelled),
                        None,
                        None,
                    );
                    crate::performance::finish(
                        result_timer.take(),
                        &Err::<(), RuntimeError>(RuntimeError::Cancelled),
                        None,
                        None,
                    );
                    return Err(RuntimeError::Cancelled);
                }
                changed = self.control.wait_changed(job.invocation_id, record.revision, wait) => {
                    if let Some(changed) = changed.map_err(map_control)? {
                        record = changed;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl FullNodeActionRuntime for QueuedNodeRuntime {
    fn validate_manifest(&self, manifest: &ReleaseManifestV1) -> Result<(), RuntimeError> {
        manifest
            .ensure_full_node_v1_supported()
            .map_err(|_| RuntimeError::UnsupportedRuntime)
    }

    async fn execute(
        &self,
        request: InvocationRequest,
    ) -> Result<FullNodeActionOutcome, RuntimeError> {
        self.execute_inner(request).await
    }
}

/// Agent-side materializer connecting Release Repository, Artifact Store, control plane, and Node.
pub struct FullNodeExecutionHandler {
    releases: Arc<dyn ReleaseRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    runtime: Arc<dyn FullNodeActionRuntime>,
    control: Arc<dyn ExecutionControlPlane>,
    prepared_artifacts: tokio::sync::Mutex<PreparedArtifactCache>,
    performance: Option<(PerformanceRuntime, Arc<dyn InvocationPerformanceSink>)>,
}

#[derive(Debug)]
struct PreparedArtifactCache {
    entries: HashMap<Sha256Digest, Arc<[u8]>>,
    insertion_order: VecDeque<Sha256Digest>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl PreparedArtifactCache {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn get(&self, digest: Sha256Digest) -> Option<Arc<[u8]>> {
        self.entries.get(&digest).cloned()
    }

    fn insert(&mut self, digest: Sha256Digest, artifact: Arc<[u8]>) {
        if artifact.len() > self.max_bytes || self.entries.contains_key(&digest) {
            return;
        }
        while self.entries.len() >= self.max_entries
            || self.bytes.saturating_add(artifact.len()) > self.max_bytes
        {
            let Some(oldest) = self.insertion_order.pop_front() else {
                return;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed.len());
            }
        }
        self.bytes = self.bytes.saturating_add(artifact.len());
        self.insertion_order.push_back(digest);
        self.entries.insert(digest, artifact);
    }
}

impl std::fmt::Debug for FullNodeExecutionHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FullNodeExecutionHandler")
            .field("release_backend", &self.releases.backend())
            .finish_non_exhaustive()
    }
}

impl FullNodeExecutionHandler {
    /// Composes one agent-side immutable execution handler.
    #[must_use]
    pub fn new(
        releases: Arc<dyn ReleaseRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        runtime: Arc<dyn FullNodeActionRuntime>,
        control: Arc<dyn ExecutionControlPlane>,
    ) -> Self {
        Self {
            releases,
            artifacts,
            runtime,
            control,
            prepared_artifacts: tokio::sync::Mutex::new(PreparedArtifactCache::new(
                1_024,
                8 * 1024 * 1024,
            )),
            performance: None,
        }
    }

    /// Replaces the bounded process-local cache of verified descriptor bytes and prepared images.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive limits.
    pub fn with_prepared_artifact_cache_limits(
        mut self,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Self, RuntimeError> {
        if !(1..=65_536).contains(&max_entries) || !(1..=1024 * 1024 * 1024).contains(&max_bytes) {
            return Err(RuntimeError::InvalidConfiguration);
        }
        self.prepared_artifacts =
            tokio::sync::Mutex::new(PreparedArtifactCache::new(max_entries, max_bytes));
        Ok(self)
    }

    /// Enables bounded per-invocation Agent and runtime spans.
    #[must_use]
    pub fn with_performance_sink(
        mut self,
        runtime: PerformanceRuntime,
        sink: Arc<dyn InvocationPerformanceSink>,
    ) -> Self {
        self.performance = Some((runtime, sink));
        self
    }

    async fn reject(
        &self,
        invocation_id: InvocationId,
        error: RuntimeError,
    ) -> ExecutionPreparationError {
        let _ = self
            .control
            .complete(
                invocation_id,
                ExecutionCompletion::Failed(error.code().to_owned()),
            )
            .await;
        ExecutionPreparationError::Invalid
    }
}

#[async_trait]
impl ExecutionHandler for FullNodeExecutionHandler {
    async fn expire(&self, job: &ExecutionJobV1) -> Result<(), ExecutionPreparationError> {
        self.control
            .complete(
                job.invocation_id,
                ExecutionCompletion::Failed(RuntimeError::DeadlineExceeded.code().to_owned()),
            )
            .await
            .map_err(map_control_preparation)?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn prepare(
        &self,
        job: ExecutionJobV1,
    ) -> Result<Box<dyn PreparedExecution>, ExecutionPreparationError> {
        let agent_recorder = self.performance.as_ref().map(|(_, sink)| {
            InvocationPerformanceRecorder::new(
                job.request_id,
                job.invocation_id,
                PerformanceRuntime::RemoteAgent,
                Arc::clone(sink),
            )
        });
        let preparing_timer = agent_recorder.as_ref().map(|recorder| {
            recorder.start(
                PerformanceComponent::ControlPlane,
                PerformanceOperation::BeginPreparing,
                None,
            )
        });
        let state = self
            .control
            .begin_preparing(job.invocation_id)
            .await
            .map_err(map_control_preparation);
        finish_preparation_timer(preparing_timer, &state);
        let state = state?;
        if state.record.state == ExecutionState::CancelRequested {
            self.control
                .complete(job.invocation_id, ExecutionCompletion::Cancelled)
                .await
                .map_err(map_control_preparation)?;
            return Err(ExecutionPreparationError::Invalid);
        }
        let payload = match decode_invocation(&job.payload) {
            Ok(payload) => payload,
            Err(error) => return Err(self.reject(job.invocation_id, error).await),
        };
        let scope = EnvironmentScope::new(job.project_id, job.environment_id);
        let release_timer = agent_recorder.as_ref().map(|recorder| {
            recorder.start(
                PerformanceComponent::ReleaseRepository,
                PerformanceOperation::ResolveRelease,
                None,
            )
        });
        let manifest_result = self.releases.manifest(scope, job.release_id).await;
        finish_preparation_timer(release_timer, &manifest_result);
        let manifest = match manifest_result {
            Ok(manifest) => manifest,
            Err(error) if error.retryable() => return Err(ExecutionPreparationError::Unavailable),
            Err(_) => {
                return Err(self
                    .reject(job.invocation_id, RuntimeError::InvalidArtifact)
                    .await);
            }
        };
        if manifest.project_id != job.project_id || manifest.release_id != job.release_id {
            return Err(self
                .reject(job.invocation_id, RuntimeError::InvalidArtifact)
                .await);
        }
        if let Err(error) = self.runtime.validate_manifest(&manifest) {
            return Err(self.reject(job.invocation_id, error).await);
        }
        let artifact = if let Some(artifact) = self
            .prepared_artifacts
            .lock()
            .await
            .get(manifest.artifact.digest)
        {
            artifact
        } else {
            let artifact_timer = agent_recorder.as_ref().map(|recorder| {
                recorder.start(
                    PerformanceComponent::ArtifactStore,
                    PerformanceOperation::FetchArtifact,
                    None,
                )
            });
            let artifact_result = self.artifacts.get(&manifest.artifact).await;
            finish_preparation_timer(artifact_timer, &artifact_result);
            let artifact = match artifact_result {
                Ok(artifact) => artifact,
                Err(error) if error.retryable() => {
                    return Err(ExecutionPreparationError::Unavailable);
                }
                Err(_) => {
                    return Err(self
                        .reject(job.invocation_id, RuntimeError::InvalidArtifact)
                        .await);
                }
            };
            if artifact.len() != usize::try_from(manifest.artifact.size_bytes).unwrap_or(usize::MAX)
                || Sha256Digest::of(&artifact) != manifest.artifact.digest
            {
                return Err(self
                    .reject(job.invocation_id, RuntimeError::InvalidArtifact)
                    .await);
            }
            let runtime_timer = agent_recorder.as_ref().map(|recorder| {
                recorder.start(
                    PerformanceComponent::OciImage,
                    PerformanceOperation::PrepareRuntime,
                    u64::try_from(artifact.len()).ok(),
                )
            });
            let runtime_prepared = self.runtime.prepare(&manifest, &artifact).await;
            if let Err(error) = &runtime_prepared {
                crate::performance::finish(
                    runtime_timer,
                    &Err::<(), RuntimeError>(*error),
                    None,
                    None,
                );
                return if matches!(error, RuntimeError::Unavailable | RuntimeError::Busy) {
                    Err(ExecutionPreparationError::Unavailable)
                } else {
                    Err(self.reject(job.invocation_id, *error).await)
                };
            }
            crate::performance::finish(runtime_timer, &Ok::<(), RuntimeError>(()), None, None);
            let artifact: Arc<[u8]> = artifact.into();
            self.prepared_artifacts
                .lock()
                .await
                .insert(manifest.artifact.digest, Arc::clone(&artifact));
            artifact
        };
        let Some(remaining) = remaining_duration(job.deadline_unix_ms) else {
            return Err(self
                .reject(job.invocation_id, RuntimeError::DeadlineExceeded)
                .await);
        };
        let cancellation = CancellationToken::new();
        let mut request = InvocationRequest::new(
            scope,
            job.release_id,
            job.request_id,
            job.invocation_id,
            payload.function_id,
            Arc::new(manifest),
            artifact,
            payload.arguments,
            remaining,
            cancellation.clone(),
        )
        .map_err(|_| ExecutionPreparationError::Invalid)?;
        if let Some((runtime, sink)) = &self.performance {
            request = request.with_performance_sink(*runtime, Arc::clone(sink));
        }
        Ok(Box::new(PreparedFullNodeExecution {
            invocation_id: job.invocation_id,
            runtime: Arc::clone(&self.runtime),
            control: Arc::clone(&self.control),
            request,
            cancellation,
            control_revision: state.revision,
            agent_recorder,
        }))
    }
}

struct PreparedFullNodeExecution {
    invocation_id: InvocationId,
    runtime: Arc<dyn FullNodeActionRuntime>,
    control: Arc<dyn ExecutionControlPlane>,
    request: InvocationRequest,
    cancellation: CancellationToken,
    control_revision: u64,
    agent_recorder: Option<InvocationPerformanceRecorder>,
}

#[async_trait]
impl PreparedExecution for PreparedFullNodeExecution {
    async fn execute(self: Box<Self>) -> Result<(), ExecutionHandlerError> {
        let running_timer = self.agent_recorder.as_ref().map(|recorder| {
            recorder.start(
                PerformanceComponent::ControlPlane,
                PerformanceOperation::BeginRunning,
                None,
            )
        });
        let running = self.control.begin_running(self.invocation_id).await;
        finish_control_timer(running_timer, &running);
        let Ok(running) = running else {
            let _ = self
                .control
                .complete(self.invocation_id, ExecutionCompletion::Uncertain)
                .await;
            return Err(ExecutionHandlerError::OutcomeUncertain);
        };
        if running.record.state == ExecutionState::CancelRequested {
            self.control
                .complete(self.invocation_id, ExecutionCompletion::Cancelled)
                .await
                .map_err(|_| ExecutionHandlerError::Unavailable)?;
            return Ok(());
        }
        let control = Arc::clone(&self.control);
        let cancellation = self.cancellation.clone();
        let invocation_id = self.invocation_id;
        let mut revision = running.revision.max(self.control_revision);
        let cancellation_watcher = tokio::spawn(async move {
            loop {
                if let Ok(Some(changed)) = control
                    .wait_changed(invocation_id, revision, Duration::from_secs(1))
                    .await
                {
                    revision = changed.revision;
                    if changed.record.state == ExecutionState::CancelRequested {
                        cancellation.cancel();
                        return;
                    }
                    if changed.record.state.is_terminal() {
                        return;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
        let completion = match self.runtime.execute(self.request).await {
            Ok(outcome) => ExecutionCompletion::Succeeded(
                serde_json::to_vec(
                    &WireValueV1::from_canonical(&outcome.value)
                        .map_err(|_| ExecutionHandlerError::OutcomeUncertain)?,
                )
                .map_err(|_| ExecutionHandlerError::OutcomeUncertain)?,
            ),
            Err(RuntimeError::Cancelled) => ExecutionCompletion::Cancelled,
            Err(error) => ExecutionCompletion::Failed(error.code().to_owned()),
        };
        cancellation_watcher.abort();
        let complete_timer = self.agent_recorder.as_ref().map(|recorder| {
            recorder.start(
                PerformanceComponent::ControlPlane,
                PerformanceOperation::Complete,
                None,
            )
        });
        let completed = self
            .control
            .complete(self.invocation_id, completion)
            .await
            .map_err(|_| ExecutionHandlerError::OutcomeUncertain);
        finish_control_timer(complete_timer, &completed);
        completed?;
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeInvocationWire<'a> {
    format_version: u16,
    function_id: FunctionId,
    arguments: &'a WireValueV1,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OwnedNodeInvocationWire {
    format_version: u16,
    function_id: FunctionId,
    arguments: WireValueV1,
}

struct DecodedInvocation {
    function_id: FunctionId,
    arguments: runku_value::CanonicalValue,
}

fn encode_invocation(
    function_id: FunctionId,
    arguments: &runku_value::CanonicalValue,
) -> Result<Vec<u8>, RuntimeError> {
    let arguments =
        WireValueV1::from_canonical(arguments).map_err(|_| RuntimeError::InvalidArguments)?;
    serde_json::to_vec(&NodeInvocationWire {
        format_version: REMOTE_NODE_INVOCATION_FORMAT_VERSION,
        function_id,
        arguments: &arguments,
    })
    .map_err(|_| RuntimeError::Internal)
}

fn decode_invocation(bytes: &[u8]) -> Result<DecodedInvocation, RuntimeError> {
    let wire: OwnedNodeInvocationWire =
        serde_json::from_slice(bytes).map_err(|_| RuntimeError::InvalidInvocation)?;
    if wire.format_version != REMOTE_NODE_INVOCATION_FORMAT_VERSION {
        return Err(RuntimeError::InvalidInvocation);
    }
    Ok(DecodedInvocation {
        function_id: wire.function_id,
        arguments: wire
            .arguments
            .into_canonical()
            .map_err(|_| RuntimeError::InvalidArguments)?,
    })
}

fn terminal_outcome(
    record: &runku_execution_queue::ExecutionRecordV1,
) -> Result<Option<Result<FullNodeActionOutcome, RuntimeError>>, RuntimeError> {
    let outcome = match record.state {
        ExecutionState::Succeeded => {
            let bytes = record.result.as_ref().ok_or(RuntimeError::Internal)?;
            let value: WireValueV1 =
                serde_json::from_slice(bytes).map_err(|_| RuntimeError::InvalidResult)?;
            Some(Ok(FullNodeActionOutcome {
                value: value
                    .into_canonical()
                    .map_err(|_| RuntimeError::InvalidResult)?,
                resource_usage: None,
            }))
        }
        ExecutionState::Failed => Some(Err(runtime_error_from_code(
            record.error_code.as_deref().ok_or(RuntimeError::Internal)?,
        ))),
        ExecutionState::Cancelled => Some(Err(RuntimeError::Cancelled)),
        ExecutionState::Uncertain => Some(Err(RuntimeError::Unavailable)),
        _ => None,
    };
    Ok(outcome)
}

fn runtime_error_from_code(code: &str) -> RuntimeError {
    match code {
        "RUNTIME_CONFIGURATION_INVALID" => RuntimeError::InvalidConfiguration,
        "RUNTIME_INVOCATION_INVALID" => RuntimeError::InvalidInvocation,
        "RUNTIME_ARGUMENTS_INVALID" => RuntimeError::InvalidArguments,
        "RUNTIME_VERSION_UNSUPPORTED" => RuntimeError::UnsupportedRuntime,
        "RUNTIME_ARTIFACT_INVALID" => RuntimeError::InvalidArtifact,
        "RUNTIME_FUNCTION_NOT_FOUND" => RuntimeError::FunctionNotFound,
        "RUNTIME_BUSY" => RuntimeError::Busy,
        "RUNTIME_UNAVAILABLE" => RuntimeError::Unavailable,
        "RUNTIME_DEADLINE_EXCEEDED" => RuntimeError::DeadlineExceeded,
        "RUNTIME_CANCELLED" => RuntimeError::Cancelled,
        "RUNTIME_HEAP_LIMIT_EXCEEDED" => RuntimeError::HeapLimitExceeded,
        "RUNTIME_JAVASCRIPT_ERROR" => RuntimeError::JavaScript,
        "RUNTIME_RESULT_INVALID" => RuntimeError::InvalidResult,
        _ => RuntimeError::Internal,
    }
}

fn absolute_deadline(timeout: Duration) -> Result<u64, RuntimeError> {
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RuntimeError::Internal)?
        .checked_add(timeout)
        .ok_or(RuntimeError::InvalidInvocation)?;
    u64::try_from(deadline.as_millis()).map_err(|_| RuntimeError::InvalidInvocation)
}

fn remaining_duration(deadline_unix_ms: u64) -> Option<Duration> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();
    let remaining = u128::from(deadline_unix_ms).checked_sub(now)?;
    if remaining == 0 {
        return None;
    }
    u64::try_from(remaining).ok().map(Duration::from_millis)
}

const fn map_control(error: ExecutionControlError) -> RuntimeError {
    match error {
        ExecutionControlError::Conflict | ExecutionControlError::Unavailable => {
            RuntimeError::Unavailable
        }
        ExecutionControlError::InvalidRecord | ExecutionControlError::NotFound => {
            RuntimeError::Internal
        }
    }
}

const fn map_control_preparation(error: ExecutionControlError) -> ExecutionPreparationError {
    match error {
        ExecutionControlError::Unavailable | ExecutionControlError::Conflict => {
            ExecutionPreparationError::Unavailable
        }
        ExecutionControlError::InvalidRecord | ExecutionControlError::NotFound => {
            ExecutionPreparationError::Invalid
        }
    }
}

const fn map_queue(error: runku_execution_queue::ExecutionQueueError) -> RuntimeError {
    match error {
        runku_execution_queue::ExecutionQueueError::Full => RuntimeError::Busy,
        runku_execution_queue::ExecutionQueueError::Timeout
        | runku_execution_queue::ExecutionQueueError::Unavailable => RuntimeError::Unavailable,
        runku_execution_queue::ExecutionQueueError::InvalidJob
        | runku_execution_queue::ExecutionQueueError::InvalidPayload => RuntimeError::Internal,
    }
}

fn finish_preparation_timer<T, E>(
    timer: Option<runku_observability::InvocationPerformanceTimer>,
    result: &Result<T, E>,
) {
    let result = result
        .as_ref()
        .map(|_| ())
        .map_err(|_| RuntimeError::Unavailable);
    crate::performance::finish(timer, &result, None, None);
}

fn finish_control_timer<T>(
    timer: Option<runku_observability::InvocationPerformanceTimer>,
    result: &Result<T, impl std::fmt::Debug>,
) {
    finish_preparation_timer(timer, result);
}
