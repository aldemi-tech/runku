//! Trusted invocation envelope, resource limits, cancellation, and bounded telemetry.

use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use runku_core::{EnvironmentScope, FunctionId, InvocationId, PinnedCode, ReleaseId, RequestId};
use runku_identity::RequestIdentity;
use runku_observability::{
    InvocationPerformanceRecorder, InvocationPerformanceSink, OperationalLogSink,
    PerformanceRuntime,
};
use runku_releases::{ReleaseManifestV1, Sha256Digest};
use runku_value::{CanonicalValue, encode_stored_value};

use crate::{DataRead, DataWrite, FunctionInvoke, HttpsEgress, RuntimeError, ScheduleCreate};

const MIB: usize = 1024 * 1024;
const MIN_HEAP_BYTES: usize = 16 * MIB;
const MAX_HEAP_BYTES: usize = 512 * MIB;
const MAX_WORKERS: usize = 64;
const MAX_QUEUE_CAPACITY: usize = 100_000;
const MAX_NESTED_CONCURRENCY: usize = 1_024;
const MAX_NESTED_DEPTH: u8 = 32;
const MAX_NESTED_CALLS: u64 = 10_000;
const MAX_OPS: u64 = 1_000_000;
const MAX_WALL_TIME: Duration = Duration::from_mins(5);

/// Validated, process-local limits for one Safe Runtime supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeLimits {
    pub(crate) worker_count: usize,
    pub(crate) queue_capacity: usize,
    pub(crate) heap_bytes: usize,
    pub(crate) max_wall_time: Duration,
    pub(crate) max_ops: u64,
    pub(crate) max_nested_concurrency: usize,
    pub(crate) max_nested_depth: u8,
    pub(crate) max_nested_calls: u64,
}

impl RuntimeLimits {
    /// Starts a builder with bounded worker and admission queue dimensions.
    #[must_use]
    pub const fn builder(worker_count: usize, queue_capacity: usize) -> RuntimeLimitsBuilder {
        RuntimeLimitsBuilder {
            worker_count,
            queue_capacity,
            heap_bytes: 64 * MIB,
            max_wall_time: Duration::from_secs(30),
            max_ops: 10_000,
            max_nested_concurrency: worker_count.saturating_mul(4),
            max_nested_depth: 8,
            max_nested_calls: 100,
        }
    }

    /// Number of host worker threads.
    #[must_use]
    pub const fn worker_count(self) -> usize {
        self.worker_count
    }

    /// Maximum invocations waiting for a worker.
    #[must_use]
    pub const fn queue_capacity(self) -> usize {
        self.queue_capacity
    }

    /// Maximum V8 heap bytes per invocation isolate.
    #[must_use]
    pub const fn heap_bytes(self) -> usize {
        self.heap_bytes
    }

    /// Maximum accepted invocation wall timeout, including queue wait.
    #[must_use]
    pub const fn max_wall_time(self) -> Duration {
        self.max_wall_time
    }

    /// Maximum Platform Op calls allowed in one invocation.
    #[must_use]
    pub const fn max_ops(self) -> u64 {
        self.max_ops
    }

    /// Maximum nested isolates executing concurrently outside the primary worker pool.
    #[must_use]
    pub const fn max_nested_concurrency(self) -> usize {
        self.max_nested_concurrency
    }

    /// Maximum number of nested edges below a root invocation.
    #[must_use]
    pub const fn max_nested_depth(self) -> u8 {
        self.max_nested_depth
    }

    /// Maximum number of nested calls admitted across one invocation tree.
    #[must_use]
    pub const fn max_nested_calls(self) -> u64 {
        self.max_nested_calls
    }
}

/// Builder for validated Safe Runtime limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeLimitsBuilder {
    worker_count: usize,
    queue_capacity: usize,
    heap_bytes: usize,
    max_wall_time: Duration,
    max_ops: u64,
    max_nested_concurrency: usize,
    max_nested_depth: u8,
    max_nested_calls: u64,
}

impl RuntimeLimitsBuilder {
    /// Sets the hard V8 heap limit applied to each fresh isolate.
    #[must_use]
    pub const fn heap_bytes(mut self, heap_bytes: usize) -> Self {
        self.heap_bytes = heap_bytes;
        self
    }

    /// Sets the largest caller wall timeout accepted by this supervisor.
    #[must_use]
    pub const fn max_wall_time(mut self, max_wall_time: Duration) -> Self {
        self.max_wall_time = max_wall_time;
        self
    }

    /// Sets the maximum explicit Platform Op calls per invocation.
    #[must_use]
    pub const fn max_ops(mut self, max_ops: u64) -> Self {
        self.max_ops = max_ops;
        self
    }

    /// Sets the process-wide concurrent nested-isolate capacity.
    #[must_use]
    pub const fn max_nested_concurrency(mut self, capacity: usize) -> Self {
        self.max_nested_concurrency = capacity;
        self
    }

    /// Sets the maximum nested-call depth below a root invocation.
    #[must_use]
    pub const fn max_nested_depth(mut self, depth: u8) -> Self {
        self.max_nested_depth = depth;
        self
    }

    /// Sets the aggregate nested-call budget shared by one invocation tree.
    #[must_use]
    pub const fn max_nested_calls(mut self, calls: u64) -> Self {
        self.max_nested_calls = calls;
        self
    }

    /// Validates all dimensions and constructs immutable limits.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive workers or queue, heap outside 16–512 MiB, and wall time outside
    /// 1 millisecond–5 minutes.
    pub fn build(self) -> Result<RuntimeLimits, RuntimeError> {
        if !(1..=MAX_WORKERS).contains(&self.worker_count)
            || !(1..=MAX_QUEUE_CAPACITY).contains(&self.queue_capacity)
            || !(MIN_HEAP_BYTES..=MAX_HEAP_BYTES).contains(&self.heap_bytes)
            || self.max_wall_time < Duration::from_millis(1)
            || self.max_wall_time > MAX_WALL_TIME
            || !(1..=MAX_OPS).contains(&self.max_ops)
            || !(1..=MAX_NESTED_CONCURRENCY).contains(&self.max_nested_concurrency)
            || !(1..=MAX_NESTED_DEPTH).contains(&self.max_nested_depth)
            || !(1..=MAX_NESTED_CALLS).contains(&self.max_nested_calls)
        {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(RuntimeLimits {
            worker_count: self.worker_count,
            queue_capacity: self.queue_capacity,
            heap_bytes: self.heap_bytes,
            max_wall_time: self.max_wall_time,
            max_ops: self.max_ops,
            max_nested_concurrency: self.max_nested_concurrency,
            max_nested_depth: self.max_nested_depth,
            max_nested_calls: self.max_nested_calls,
        })
    }
}

/// Cooperative caller cancellation shared with the out-of-isolate termination watchdog.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

#[derive(Debug)]
pub(crate) struct CancellationState {
    cancelled: Mutex<bool>,
    changed: Condvar,
    async_changed: tokio::sync::watch::Sender<bool>,
}

impl Default for CancellationState {
    fn default() -> Self {
        let (async_changed, _receiver) = tokio::sync::watch::channel(false);
        Self {
            cancelled: Mutex::new(false),
            changed: Condvar::new(),
            async_changed,
        }
    }
}

impl CancellationToken {
    /// Creates an active token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically marks every invocation using this token cancelled and wakes its watchdog.
    pub fn cancel(&self) {
        if let Ok(mut cancelled) = self.inner.cancelled.lock() {
            *cancelled = true;
            self.inner.changed.notify_all();
            self.inner.async_changed.send_replace(true);
        }
    }

    /// Returns whether cancellation was requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner
            .cancelled
            .lock()
            .map_or(true, |cancelled| *cancelled)
    }

    /// Waits asynchronously until cancellation is requested.
    pub async fn cancelled(&self) {
        self.inner.cancelled().await;
    }

    pub(crate) fn state(&self) -> &Arc<CancellationState> {
        &self.inner
    }
}

impl CancellationState {
    pub(crate) fn wait_until_changed_or(
        &self,
        completed: &std::sync::atomic::AtomicBool,
        timeout: Duration,
    ) -> bool {
        let Ok(cancelled) = self.cancelled.lock() else {
            return true;
        };
        let result = self
            .changed
            .wait_timeout_while(cancelled, timeout, |cancelled| {
                !*cancelled && !completed.load(std::sync::atomic::Ordering::Acquire)
            });
        result.map_or(true, |(cancelled, _)| *cancelled)
    }

    pub(crate) fn wake(&self) {
        self.changed.notify_all();
    }

    pub(crate) async fn cancelled(&self) {
        let mut changed = self.async_changed.subscribe();
        if *changed.borrow() {
            return;
        }
        while changed.changed().await.is_ok() {
            if *changed.borrow() {
                return;
            }
        }
    }
}

/// Complete immutable input accepted by a Runtime Supervisor.
#[derive(Clone, Debug)]
pub struct InvocationRequest {
    pub(crate) scope: EnvironmentScope,
    pub(crate) release_id: ReleaseId,
    pub(crate) pinned_code: PinnedCode,
    pub(crate) request_id: RequestId,
    pub(crate) invocation_id: InvocationId,
    pub(crate) parent_invocation_id: Option<InvocationId>,
    pub(crate) function_id: FunctionId,
    pub(crate) manifest: Arc<ReleaseManifestV1>,
    pub(crate) artifact_bytes: Arc<[u8]>,
    pub(crate) arguments: CanonicalValue,
    pub(crate) wall_timeout: Duration,
    pub(crate) cancellation: CancellationToken,
    pub(crate) identity: Option<Arc<RequestIdentity>>,
    pub(crate) https: Option<Arc<dyn HttpsEgress>>,
    pub(crate) data: Option<Arc<dyn DataRead>>,
    pub(crate) data_write: Option<Arc<dyn DataWrite>>,
    pub(crate) scheduler: Option<Arc<dyn ScheduleCreate>>,
    pub(crate) functions: Option<Arc<dyn FunctionInvoke>>,
    pub(crate) operational_logs: Option<Arc<dyn OperationalLogSink>>,
    pub(crate) performance: Option<InvocationPerformanceRecorder>,
    pub(crate) nested_depth: u8,
    pub(crate) nested_calls: Arc<AtomicU64>,
    pub(crate) nested_call_admitted: Arc<AtomicBool>,
    pub(crate) telemetry: Option<Arc<RuntimeTelemetry>>,
}

impl InvocationRequest {
    /// Constructs an immutable trusted envelope and validates cheap cross-field invariants.
    ///
    /// Artifact codec/digest and implementation selection are deliberately revalidated inside the
    /// worker immediately before V8 creation.
    ///
    /// # Errors
    ///
    /// Rejects scope/Release drift, absent Function, zero timeout, or non-encodable arguments.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scope: EnvironmentScope,
        release_id: ReleaseId,
        request_id: RequestId,
        invocation_id: InvocationId,
        function_id: FunctionId,
        manifest: Arc<ReleaseManifestV1>,
        artifact_bytes: Arc<[u8]>,
        arguments: CanonicalValue,
        wall_timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Self, RuntimeError> {
        if manifest.project_id != scope.project_id()
            || manifest.release_id != release_id
            || wall_timeout.is_zero()
            || !manifest
                .functions
                .iter()
                .any(|function| function.id == function_id)
            || encode_stored_value(&arguments).is_err()
        {
            return Err(RuntimeError::InvalidInvocation);
        }
        Ok(Self {
            scope,
            release_id,
            pinned_code: PinnedCode::Release(release_id),
            request_id,
            invocation_id,
            parent_invocation_id: None,
            function_id,
            manifest,
            artifact_bytes,
            arguments,
            wall_timeout,
            cancellation,
            identity: None,
            https: None,
            data: None,
            data_write: None,
            scheduler: None,
            functions: None,
            operational_logs: None,
            performance: None,
            nested_depth: 0,
            nested_calls: Arc::new(AtomicU64::new(0)),
            nested_call_admitted: Arc::new(AtomicBool::new(false)),
            telemetry: None,
        })
    }

    /// Creates a child envelope pinned to the exact same immutable execution context.
    ///
    /// Brokers must attach only the capabilities valid for the selected child after this call.
    /// Mutable Channel/Workspace resolution and raw-credential authentication are intentionally
    /// absent from this boundary.
    ///
    /// # Errors
    ///
    /// Rejects an unknown Function, invalid arguments, a zero/larger remaining timeout, or depth
    /// overflow. Trusted tree limits are admitted separately by the supervisor.
    pub fn nested_child(
        &self,
        function_id: FunctionId,
        arguments: CanonicalValue,
        wall_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let nested_depth = self
            .nested_depth
            .checked_add(1)
            .ok_or(RuntimeError::InvalidInvocation)?;
        if wall_timeout.is_zero()
            || wall_timeout > self.wall_timeout
            || !self
                .manifest
                .functions
                .iter()
                .any(|function| function.id == function_id)
            || encode_stored_value(&arguments).is_err()
        {
            return Err(RuntimeError::InvalidInvocation);
        }
        Ok(Self {
            scope: self.scope,
            release_id: self.release_id,
            pinned_code: self.pinned_code,
            request_id: self.request_id,
            invocation_id: InvocationId::generate(),
            parent_invocation_id: Some(self.invocation_id),
            function_id,
            manifest: Arc::clone(&self.manifest),
            artifact_bytes: Arc::clone(&self.artifact_bytes),
            arguments,
            wall_timeout,
            cancellation: self.cancellation.clone(),
            identity: self.identity.clone(),
            https: None,
            data: None,
            data_write: None,
            scheduler: None,
            functions: None,
            operational_logs: self.operational_logs.clone(),
            performance: self.performance.clone(),
            nested_depth,
            nested_calls: Arc::clone(&self.nested_calls),
            nested_call_admitted: Arc::new(AtomicBool::new(false)),
            telemetry: self.telemetry.clone(),
        })
    }

    pub(crate) fn admit_nested_call(&self, limits: RuntimeLimits) -> Result<(), RuntimeError> {
        if self.nested_depth == 0 || self.nested_depth > limits.max_nested_depth {
            return Err(RuntimeError::InvalidInvocation);
        }
        if self.nested_call_admitted.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let admitted = self
            .nested_calls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < limits.max_nested_calls).then_some(current + 1)
            })
            .map(|_| ())
            .map_err(|_| RuntimeError::InvalidInvocation);
        if admitted.is_err() {
            self.nested_call_admitted.store(false, Ordering::Release);
        }
        admitted
    }

    /// Attaches the already-authorized, token-free identity for `ctx.auth` and isolation keys.
    ///
    /// The worker exposes it only when the selected Function declares `auth:read`; attaching it
    /// never grants authority and raw credentials are not representable by [`RequestIdentity`].
    #[must_use]
    pub fn with_identity(mut self, identity: Arc<RequestIdentity>) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Overrides the scheduling/observability identity after a Workspace resolved to a candidate
    /// Release manifest.
    ///
    /// # Errors
    ///
    /// A Release pin must match the manifest Release exactly. A Dev Revision remains a distinct
    /// immutable identity while the candidate Release continues to identify manifest bytes.
    pub fn with_pinned_code(mut self, pinned_code: PinnedCode) -> Result<Self, RuntimeError> {
        if matches!(pinned_code, PinnedCode::Release(value) if value != self.release_id) {
            return Err(RuntimeError::InvalidInvocation);
        }
        self.pinned_code = pinned_code;
        Ok(self)
    }

    /// Injects the HTTPS capability broker for this invocation.
    ///
    /// # Errors
    ///
    /// Rejects attachment unless the selected Function is an Action that declares
    /// `network:https`. The worker independently validates the same invariant before exposing the
    /// Platform Op.
    pub fn with_https(mut self, https: Arc<dyn HttpsEgress>) -> Result<Self, RuntimeError> {
        let authorized = self.manifest.functions.iter().any(|function| {
            function.id == self.function_id
                && function.function_type == runku_releases::FunctionType::Action
                && function
                    .capabilities
                    .contains(&runku_releases::Capability::NetworkHttps)
        });
        if !authorized {
            return Err(RuntimeError::InvalidInvocation);
        }
        self.https = Some(https);
        Ok(self)
    }

    /// Injects a read-only data broker for this invocation.
    ///
    /// # Errors
    ///
    /// Rejects attachment unless the selected Function is a Query that declares `db:read`.
    /// The worker independently validates the same invariant before exposing the Platform Ops.
    pub fn with_data(mut self, data: Arc<dyn DataRead>) -> Result<Self, RuntimeError> {
        let authorized = self.manifest.functions.iter().any(|function| {
            function.id == self.function_id
                && function.function_type == runku_releases::FunctionType::Query
                && function
                    .capabilities
                    .contains(&runku_releases::Capability::DbRead)
        });
        if !authorized {
            return Err(RuntimeError::InvalidInvocation);
        }
        self.data = Some(data);
        Ok(self)
    }

    /// Injects one broker implementing read and buffered-write authority for a Mutation.
    ///
    /// # Errors
    ///
    /// Rejects attachment unless the selected Function is a Mutation declaring `db:write`.
    pub fn with_mutation_data<T>(mut self, data: Arc<T>) -> Result<Self, RuntimeError>
    where
        T: DataRead + DataWrite + 'static,
    {
        let authorized = self.manifest.functions.iter().any(|function| {
            function.id == self.function_id
                && function.function_type == runku_releases::FunctionType::Mutation
                && function
                    .capabilities
                    .contains(&runku_releases::Capability::DbWrite)
        });
        if !authorized {
            return Err(RuntimeError::InvalidInvocation);
        }
        self.data = Some(data.clone());
        self.data_write = Some(data);
        Ok(self)
    }

    /// Injects read-only document authority for a Mutation declaring `db:read` without requiring
    /// write authority.
    ///
    /// # Errors
    ///
    /// Rejects non-Mutations and Functions without the explicit `db:read` capability.
    pub fn with_mutation_read<T>(mut self, data: Arc<T>) -> Result<Self, RuntimeError>
    where
        T: DataRead + 'static,
    {
        let authorized = self.manifest.functions.iter().any(|function| {
            function.id == self.function_id
                && function.function_type == runku_releases::FunctionType::Mutation
                && function
                    .capabilities
                    .contains(&runku_releases::Capability::DbRead)
        });
        if !authorized {
            return Err(RuntimeError::InvalidInvocation);
        }
        self.data = Some(data);
        Ok(self)
    }

    /// Injects durable schedule creation into a Mutation or Action declaring `scheduler:create`.
    ///
    /// # Errors
    ///
    /// Rejects Queries and Functions without the explicit capability.
    pub fn with_scheduler(
        mut self,
        scheduler: Arc<dyn ScheduleCreate>,
    ) -> Result<Self, RuntimeError> {
        let authorized = self.manifest.functions.iter().any(|function| {
            function.id == self.function_id
                && matches!(
                    function.function_type,
                    runku_releases::FunctionType::Mutation | runku_releases::FunctionType::Action
                )
                && function
                    .capabilities
                    .contains(&runku_releases::Capability::SchedulerCreate)
        });
        if !authorized {
            return Err(RuntimeError::InvalidInvocation);
        }
        self.scheduler = Some(scheduler);
        Ok(self)
    }

    /// Injects the trusted nested Function broker for a caller declaring at least one matching
    /// `function:*` capability.
    ///
    /// # Errors
    ///
    /// Rejects attachment when the selected Function has no nested-call capability. The worker
    /// independently enforces the caller-type/capability matrix for every individual Op.
    pub fn with_functions(
        mut self,
        functions: Arc<dyn FunctionInvoke>,
    ) -> Result<Self, RuntimeError> {
        let authorized = self.manifest.functions.iter().any(|function| {
            function.id == self.function_id
                && function.capabilities.iter().any(|capability| {
                    matches!(
                        capability,
                        runku_releases::Capability::FunctionQuery
                            | runku_releases::Capability::FunctionMutation
                            | runku_releases::Capability::FunctionAction
                    )
                })
        });
        if !authorized {
            return Err(RuntimeError::InvalidInvocation);
        }
        self.functions = Some(functions);
        Ok(self)
    }

    /// Attaches the nonblocking Product Base operational log sink.
    ///
    /// Nested child envelopes inherit the same sink while maintaining independent per-invocation
    /// Function-log budgets. Sink backpressure never changes a functional execution result.
    #[must_use]
    pub fn with_operational_logs(mut self, sink: Arc<dyn OperationalLogSink>) -> Self {
        self.operational_logs = Some(sink);
        self
    }

    /// Returns the immutable Function execution identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the immediate parent for nested invocations.
    #[must_use]
    pub const fn parent_invocation_id(&self) -> Option<InvocationId> {
        self.parent_invocation_id
    }

    /// Returns the transport/request correlation identifier shared by the invocation tree.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the selected immutable Function identity.
    #[must_use]
    pub const fn function_id(&self) -> FunctionId {
        self.function_id
    }

    /// Returns the trusted Project/Environment scope.
    #[must_use]
    pub const fn scope(&self) -> EnvironmentScope {
        self.scope
    }

    /// Returns the immutable Release captured for this invocation.
    #[must_use]
    pub const fn release_id(&self) -> ReleaseId {
        self.release_id
    }

    /// Returns the immutable code identity used by scheduling and execution attribution.
    #[must_use]
    pub const fn pinned_code(&self) -> PinnedCode {
        self.pinned_code
    }

    /// Returns the validated immutable Release manifest.
    #[must_use]
    pub fn manifest(&self) -> &Arc<ReleaseManifestV1> {
        &self.manifest
    }

    /// Returns the exact immutable artifact bytes pinned to this invocation.
    #[must_use]
    pub fn artifact_bytes(&self) -> &Arc<[u8]> {
        &self.artifact_bytes
    }

    /// Returns the canonical invocation arguments.
    #[must_use]
    pub const fn arguments(&self) -> &CanonicalValue {
        &self.arguments
    }

    /// Returns the complete invocation wall timeout.
    #[must_use]
    pub const fn wall_timeout(&self) -> Duration {
        self.wall_timeout
    }

    /// Returns the immutable logical index contract expected by the Release.
    #[must_use]
    pub fn index_contract_hash(&self) -> Sha256Digest {
        self.manifest.index_contract_hash
    }

    /// Returns the caller cancellation token.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Returns the token-free identity already authorized for this invocation, when attached.
    #[must_use]
    pub fn identity(&self) -> Option<&Arc<RequestIdentity>> {
        self.identity.as_ref()
    }

    /// Returns the attached mediated HTTPS broker, when the current Function has one.
    #[must_use]
    pub fn https_egress(&self) -> Option<Arc<dyn HttpsEgress>> {
        self.https.clone()
    }

    /// Returns the trusted nested Function broker attached to this invocation, when authorized.
    #[must_use]
    pub fn function_invoker(&self) -> Option<Arc<dyn FunctionInvoke>> {
        self.functions.clone()
    }

    /// Returns the durable scheduling broker attached to this invocation, when authorized.
    #[must_use]
    pub fn schedule_creator(&self) -> Option<Arc<dyn ScheduleCreate>> {
        self.scheduler.clone()
    }

    /// Attaches an opt-in bounded performance sink for this invocation.
    ///
    /// Payload bodies are never passed to the sink. Runtime adapters emit only stable operation
    /// names, timings, byte counts, sanitized outcomes, and backend resource counters.
    #[must_use]
    pub fn with_performance_sink(
        mut self,
        runtime: PerformanceRuntime,
        sink: Arc<dyn InvocationPerformanceSink>,
    ) -> Self {
        self.performance = Some(InvocationPerformanceRecorder::new(
            self.request_id,
            self.invocation_id,
            runtime,
            sink,
        ));
        self
    }

    /// Returns the opt-in performance recorder, when detailed diagnostics are enabled.
    #[must_use]
    pub fn performance(&self) -> Option<&InvocationPerformanceRecorder> {
        self.performance.as_ref()
    }

    /// Returns the current edge depth below the root invocation.
    #[must_use]
    pub const fn nested_depth(&self) -> u8 {
        self.nested_depth
    }
}

/// Process-local counters with no Project, user, Function, or credential labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelemetrySnapshot {
    /// Requests admitted to the bounded queue.
    pub admitted: u64,
    /// Requests rejected because the admission queue was full.
    pub busy: u64,
    /// Invocations that returned a canonical value.
    pub succeeded: u64,
    /// Invalid invocation/artifact/result failures.
    pub invalid: u64,
    /// User JavaScript parse/evaluation/throw failures.
    pub javascript_errors: u64,
    /// Wall deadline terminations.
    pub deadline_exceeded: u64,
    /// Explicit cancellation terminations.
    pub cancelled: u64,
    /// V8 near-heap terminations.
    pub heap_exceeded: u64,
    /// Internal/worker channel failures.
    pub internal_errors: u64,
    /// Nested invocations admitted to the separate bounded capacity.
    pub nested_admitted: u64,
    /// Nested invocations rejected because all nested capacity was active.
    pub nested_busy: u64,
    /// Nested invocations that completed successfully.
    pub nested_succeeded: u64,
    /// Nested invocations that failed after admission or exceeded a tree limit.
    pub nested_failed: u64,
    /// Nested Platform Ops attempted after caller capability admission.
    pub function_calls: u64,
    /// Nested Platform Ops that returned a canonical child result.
    pub function_call_succeeded: u64,
    /// Nested Platform Ops rejected by target type, visibility, or derived auth policy.
    pub function_call_denied: u64,
    /// Nested Platform Ops rejected by bounded execution capacity.
    pub function_call_busy: u64,
    /// Nested Platform Ops rejected by depth/call/value limits.
    pub function_call_limited: u64,
    /// Nested Platform Ops that failed validation or execution for another reason.
    pub function_call_failed: u64,
    /// Function-authored records admitted to an operational sink.
    pub function_logs_emitted: u64,
    /// Function-authored records dropped by sink backpressure/unavailability.
    pub function_logs_dropped: u64,
    /// Function-authored records rejected by per-invocation budgets.
    pub function_logs_limited: u64,
    /// Platform lifecycle records dropped by sink validation/backpressure.
    pub platform_logs_dropped: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeTelemetry {
    admitted: AtomicU64,
    busy: AtomicU64,
    succeeded: AtomicU64,
    invalid: AtomicU64,
    javascript_errors: AtomicU64,
    deadline_exceeded: AtomicU64,
    cancelled: AtomicU64,
    heap_exceeded: AtomicU64,
    internal_errors: AtomicU64,
    nested_admitted: AtomicU64,
    nested_busy: AtomicU64,
    nested_succeeded: AtomicU64,
    nested_failed: AtomicU64,
    function_calls: AtomicU64,
    function_call_succeeded: AtomicU64,
    function_call_denied: AtomicU64,
    function_call_busy: AtomicU64,
    function_call_limited: AtomicU64,
    function_call_failed: AtomicU64,
    function_logs_emitted: AtomicU64,
    function_logs_dropped: AtomicU64,
    function_logs_limited: AtomicU64,
    platform_logs_dropped: AtomicU64,
}

impl RuntimeTelemetry {
    pub(crate) fn admitted(&self) {
        self.admitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn busy(&self) {
        self.busy.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn nested_admitted(&self) {
        self.nested_admitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn nested_busy(&self) {
        self.nested_busy.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn nested_result(&self, result: &Result<CanonicalValue, RuntimeError>) {
        let counter = if result.is_ok() {
            &self.nested_succeeded
        } else {
            &self.nested_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn function_call(&self) {
        self.function_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn function_call_result(
        &self,
        result: &Result<CanonicalValue, crate::FunctionCallError>,
    ) {
        let counter = match result {
            Ok(_) => &self.function_call_succeeded,
            Err(crate::FunctionCallError::Denied) => &self.function_call_denied,
            Err(crate::FunctionCallError::Busy) => &self.function_call_busy,
            Err(crate::FunctionCallError::LimitExceeded) => &self.function_call_limited,
            Err(
                crate::FunctionCallError::InvalidRequest
                | crate::FunctionCallError::NotFound
                | crate::FunctionCallError::Unavailable
                | crate::FunctionCallError::Timeout
                | crate::FunctionCallError::Cancelled
                | crate::FunctionCallError::Execution,
            ) => &self.function_call_failed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn function_log_emitted(&self) {
        self.function_logs_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn function_log_dropped(&self) {
        self.function_logs_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn function_log_limited(&self) {
        self.function_logs_limited.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn platform_log_dropped(&self) {
        self.platform_logs_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record(&self, result: &Result<CanonicalValue, RuntimeError>) {
        let counter = match result {
            Ok(_) => &self.succeeded,
            Err(RuntimeError::JavaScript) => &self.javascript_errors,
            Err(RuntimeError::DeadlineExceeded) => &self.deadline_exceeded,
            Err(RuntimeError::Cancelled) => &self.cancelled,
            Err(RuntimeError::HeapLimitExceeded) => &self.heap_exceeded,
            Err(
                RuntimeError::InvalidConfiguration
                | RuntimeError::InvalidInvocation
                | RuntimeError::InvalidArguments
                | RuntimeError::UnsupportedRuntime
                | RuntimeError::InvalidArtifact
                | RuntimeError::FunctionNotFound
                | RuntimeError::InvalidResult,
            ) => &self.invalid,
            Err(RuntimeError::Busy | RuntimeError::Unavailable | RuntimeError::Internal) => {
                &self.internal_errors
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> RuntimeTelemetrySnapshot {
        RuntimeTelemetrySnapshot {
            admitted: self.admitted.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            invalid: self.invalid.load(Ordering::Relaxed),
            javascript_errors: self.javascript_errors.load(Ordering::Relaxed),
            deadline_exceeded: self.deadline_exceeded.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            heap_exceeded: self.heap_exceeded.load(Ordering::Relaxed),
            internal_errors: self.internal_errors.load(Ordering::Relaxed),
            nested_admitted: self.nested_admitted.load(Ordering::Relaxed),
            nested_busy: self.nested_busy.load(Ordering::Relaxed),
            nested_succeeded: self.nested_succeeded.load(Ordering::Relaxed),
            nested_failed: self.nested_failed.load(Ordering::Relaxed),
            function_calls: self.function_calls.load(Ordering::Relaxed),
            function_call_succeeded: self.function_call_succeeded.load(Ordering::Relaxed),
            function_call_denied: self.function_call_denied.load(Ordering::Relaxed),
            function_call_busy: self.function_call_busy.load(Ordering::Relaxed),
            function_call_limited: self.function_call_limited.load(Ordering::Relaxed),
            function_call_failed: self.function_call_failed.load(Ordering::Relaxed),
            function_logs_emitted: self.function_logs_emitted.load(Ordering::Relaxed),
            function_logs_dropped: self.function_logs_dropped.load(Ordering::Relaxed),
            function_logs_limited: self.function_logs_limited.load(Ordering::Relaxed),
            platform_logs_dropped: self.platform_logs_dropped.load(Ordering::Relaxed),
        }
    }
}
