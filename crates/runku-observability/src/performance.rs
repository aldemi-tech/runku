//! Bounded per-invocation performance spans for diagnostics and capacity modelling.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use runku_core::{InvocationId, RequestId};
use serde::{Deserialize, Serialize};

/// Wire version for invocation performance spans.
pub const INVOCATION_PERFORMANCE_FORMAT_VERSION: u16 = 1;
/// Maximum spans retained by one in-memory sink.
pub const INVOCATION_PERFORMANCE_MAX_SPANS: usize = 100_000;
/// Fixed upper bounds, in microseconds, used by aggregate latency histograms.
pub const INVOCATION_PERFORMANCE_DURATION_BUCKETS_MICROS: [u64; 14] = [
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    u64::MAX,
];

/// Runtime/backend that produced a performance span.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceRuntime {
    /// Safe isolate-per-invocation V8 runtime.
    SafeV8,
    /// Developer-machine Node process.
    NodeLocal,
    /// Dedicated single-tenant host Node process.
    NodeHost,
    /// Docker conformance sandbox.
    NodeDocker,
    /// Production Node runner inside a prewarmed Firecracker microVM.
    NodeFirecracker,
    /// Gateway side of a durable remote Node invocation.
    RemoteGateway,
    /// Execution Agent side of a durable remote Node invocation.
    RemoteAgent,
}

/// Stable component attribution used in traces and commercial capacity reports.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceComponent {
    /// Public/product invocation boundary.
    Gateway,
    /// Durable execution state and result bus.
    ControlPlane,
    /// Durable work queue.
    Queue,
    /// Execution Agent admission and orchestration.
    Agent,
    /// Release manifest lookup.
    ReleaseRepository,
    /// Artifact descriptor lookup and verification.
    ArtifactStore,
    /// Runtime-level validation and admission.
    Runtime,
    /// Safe V8 isolate lifecycle.
    V8,
    /// Native Node process lifecycle.
    NodeProcess,
    /// OCI image availability/pull.
    OciImage,
    /// Ephemeral sandbox lifecycle.
    Sandbox,
    /// Network namespace policy installation.
    NetworkPolicy,
    /// User handler execution.
    Function,
    /// Result encoding, validation and delivery.
    Result,
    /// Container/process/scratch cleanup.
    Cleanup,
}

/// Stable operation within a performance component.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceOperation {
    /// Complete caller-visible invocation.
    Invocation,
    /// Runtime or queue admission wait.
    Admission,
    /// Input, manifest, artifact and contract validation.
    Validate,
    /// Serialize the invocation envelope.
    EncodeInput,
    /// Register durable execution state.
    Register,
    /// Publish a durable execution job.
    Publish,
    /// Wait for an Agent to claim the job.
    QueueWait,
    /// Wait for the claimed/running job to publish its terminal result.
    ResultWait,
    /// Transition the Agent into preparation.
    BeginPreparing,
    /// Resolve the immutable Release.
    ResolveRelease,
    /// Fetch immutable artifact bytes.
    FetchArtifact,
    /// Ensure the runtime/image is ready before ACK.
    PrepareRuntime,
    /// Transition to post-ACK running state.
    BeginRunning,
    /// Acknowledge the durable queue lease immediately before user code.
    Acknowledge,
    /// Check the native Node installation.
    CheckNode,
    /// Create an isolate, process or sandbox.
    Create,
    /// Acquire or provision a matching bounded warm sandbox worker.
    AcquireWarm,
    /// Load and evaluate user code.
    LoadModule,
    /// Execute the user handler.
    ExecuteHandler,
    /// Encode and validate the handler result.
    EncodeResult,
    /// Install the sandbox egress policy.
    ApplyPolicy,
    /// Start the sandboxed Node container.
    Start,
    /// Execute the fixed runner contract.
    ExecuteRunner,
    /// Publish/read the terminal durable result.
    Complete,
    /// Remove ephemeral resources.
    Cleanup,
    /// Kill residual processes and clear invocation scratch before warm reuse.
    ResetWarm,
}

/// Stable span outcome without tenant-controlled text.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceOutcome {
    /// Operation completed successfully.
    Succeeded,
    /// Operation failed with a separately reported stable error code.
    Failed,
    /// Operation was rejected by bounded admission.
    Busy,
    /// Operation exceeded its deadline.
    DeadlineExceeded,
    /// Operation was explicitly cancelled.
    Cancelled,
    /// Post-admission result could not be proven.
    Uncertain,
    /// A timer was dropped before the operation reported an outcome.
    Abandoned,
}

/// Resource usage attributed to one process or sandbox when the backend exposes it.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceResourceUsage {
    /// Total CPU consumed in microseconds when the backend cannot split user/system time.
    pub cpu_total_micros: Option<u64>,
    /// User CPU consumed in microseconds.
    pub user_cpu_micros: Option<u64>,
    /// Kernel CPU consumed in microseconds.
    pub system_cpu_micros: Option<u64>,
    /// Peak resident memory in bytes.
    pub peak_memory_bytes: Option<u64>,
    /// Resident/working-set memory at the sample point in bytes.
    pub memory_bytes: Option<u64>,
    /// Peak process count observed by the sandbox cgroup.
    pub peak_pids: Option<u64>,
    /// Bytes read according to the backend accounting source.
    pub io_read_bytes: Option<u64>,
    /// Bytes written according to the backend accounting source.
    pub io_write_bytes: Option<u64>,
}

/// One completed, bounded performance span.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationPerformanceSpanV1 {
    /// Wire format version.
    pub format_version: u16,
    /// Request correlation identity.
    pub request_id: RequestId,
    /// Invocation correlation identity.
    pub invocation_id: InvocationId,
    /// Runtime/backend attribution.
    pub runtime: PerformanceRuntime,
    /// Component attribution.
    pub component: PerformanceComponent,
    /// Stable operation attribution.
    pub operation: PerformanceOperation,
    /// Wall-clock start timestamp in Unix microseconds.
    pub started_unix_micros: u64,
    /// Monotonic elapsed time in microseconds.
    pub duration_micros: u64,
    /// Serialized input bytes crossing this boundary, when applicable.
    pub input_bytes: Option<u64>,
    /// Serialized output bytes crossing this boundary, when applicable.
    pub output_bytes: Option<u64>,
    /// Stable span outcome.
    pub outcome: PerformanceOutcome,
    /// Sanitized platform error code; never user-authored text.
    pub error_code: Option<String>,
    /// Backend resource usage when available.
    pub resources: Option<PerformanceResourceUsage>,
}

/// Non-blocking/infallible span destination used by hot execution paths.
pub trait InvocationPerformanceSink: fmt::Debug + Send + Sync {
    /// Records one already-bounded span. Implementations must not panic or block indefinitely.
    fn record(&self, span: InvocationPerformanceSpanV1);
}

/// Bounded process-local sink used by benchmarks, diagnostics and adapter tests.
#[derive(Debug)]
pub struct MemoryInvocationPerformanceSink {
    capacity: usize,
    spans: Mutex<VecDeque<InvocationPerformanceSpanV1>>,
    dropped: AtomicU64,
}

impl MemoryInvocationPerformanceSink {
    /// Creates a sink with an explicit bounded span capacity.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive capacities.
    pub fn new(capacity: usize) -> Result<Self, PerformanceSinkError> {
        if !(1..=INVOCATION_PERFORMANCE_MAX_SPANS).contains(&capacity) {
            return Err(PerformanceSinkError::InvalidCapacity);
        }
        Ok(Self {
            capacity,
            spans: Mutex::new(VecDeque::with_capacity(capacity)),
            dropped: AtomicU64::new(0),
        })
    }

    /// Returns retained spans in emission order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<InvocationPerformanceSpanV1> {
        self.spans
            .lock()
            .map_or_else(|_| Vec::new(), |spans| spans.iter().cloned().collect())
    }

    /// Returns retained spans for one invocation in emission order.
    #[must_use]
    pub fn invocation(&self, invocation_id: InvocationId) -> Vec<InvocationPerformanceSpanV1> {
        self.spans.lock().map_or_else(
            |_| Vec::new(),
            |spans| {
                spans
                    .iter()
                    .filter(|span| span.invocation_id == invocation_id)
                    .cloned()
                    .collect()
            },
        )
    }

    /// Number of spans dropped because the bounded sink was full or poisoned.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl InvocationPerformanceSink for MemoryInvocationPerformanceSink {
    fn record(&self, span: InvocationPerformanceSpanV1) {
        let Ok(mut spans) = self.spans.lock() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if spans.len() == self.capacity {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        spans.push_back(span);
    }
}

/// Fixed-cardinality key for one aggregate performance metric series.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationPerformanceMetricKey {
    /// Runtime/backend attribution.
    pub runtime: PerformanceRuntime,
    /// Component attribution.
    pub component: PerformanceComponent,
    /// Stable operation attribution.
    pub operation: PerformanceOperation,
}

/// Aggregate counters suitable for a low-cardinality metrics endpoint.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationPerformanceMetricValue {
    /// Number of completed spans.
    pub spans: u64,
    /// Sum of span durations in microseconds.
    pub duration_micros_sum: u64,
    /// Largest observed span duration in microseconds.
    pub duration_micros_max: u64,
    /// Cumulative counts for `INVOCATION_PERFORMANCE_DURATION_BUCKETS_MICROS`.
    pub duration_micros_buckets: [u64; INVOCATION_PERFORMANCE_DURATION_BUCKETS_MICROS.len()],
    /// Sum of serialized input bytes when reported.
    pub input_bytes_sum: u64,
    /// Sum of serialized output bytes when reported.
    pub output_bytes_sum: u64,
    /// Sum of attributed CPU microseconds when reported.
    pub cpu_micros_sum: u64,
    /// Largest observed resident/peak memory sample in bytes.
    pub memory_bytes_max: u64,
    /// Outcome counters with a fixed enum keyspace.
    pub outcomes: BTreeMap<PerformanceOutcome, u64>,
}

/// One JSON-exportable aggregate metric series.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationPerformanceMetricSeries {
    /// Fixed-cardinality runtime/component/operation labels.
    pub key: InvocationPerformanceMetricKey,
    /// Counters, histogram buckets and resource totals for the labels.
    pub value: InvocationPerformanceMetricValue,
}

/// Snapshot of every fixed-cardinality runtime/component/operation series.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationPerformanceMetricsSnapshot {
    /// Aggregate metric series.
    pub series: Vec<InvocationPerformanceMetricSeries>,
    /// Spans dropped because the aggregate sink was contended or poisoned.
    pub dropped: u64,
}

/// Low-cardinality aggregate sink for continuous SaaS/self-host monitoring.
#[derive(Debug, Default)]
pub struct AggregateInvocationPerformanceSink {
    series: Mutex<BTreeMap<InvocationPerformanceMetricKey, InvocationPerformanceMetricValue>>,
    dropped: AtomicU64,
}

impl AggregateInvocationPerformanceSink {
    /// Returns a consistent aggregate snapshot without resetting counters.
    #[must_use]
    pub fn snapshot(&self) -> InvocationPerformanceMetricsSnapshot {
        InvocationPerformanceMetricsSnapshot {
            series: self.series.lock().map_or_else(
                |_| Vec::new(),
                |series| {
                    series
                        .iter()
                        .map(|(key, value)| InvocationPerformanceMetricSeries {
                            key: *key,
                            value: value.clone(),
                        })
                        .collect()
                },
            ),
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }
}

impl InvocationPerformanceSink for AggregateInvocationPerformanceSink {
    fn record(&self, span: InvocationPerformanceSpanV1) {
        let Ok(mut series) = self.series.try_lock() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let value = series
            .entry(InvocationPerformanceMetricKey {
                runtime: span.runtime,
                component: span.component,
                operation: span.operation,
            })
            .or_default();
        value.spans = value.spans.saturating_add(1);
        value.duration_micros_sum = value
            .duration_micros_sum
            .saturating_add(span.duration_micros);
        value.duration_micros_max = value.duration_micros_max.max(span.duration_micros);
        for (upper_bound, count) in INVOCATION_PERFORMANCE_DURATION_BUCKETS_MICROS
            .iter()
            .zip(&mut value.duration_micros_buckets)
        {
            if span.duration_micros <= *upper_bound {
                *count = count.saturating_add(1);
            }
        }
        value.input_bytes_sum = value
            .input_bytes_sum
            .saturating_add(span.input_bytes.unwrap_or_default());
        value.output_bytes_sum = value
            .output_bytes_sum
            .saturating_add(span.output_bytes.unwrap_or_default());
        if let Some(resources) = span.resources {
            let cpu = resources.cpu_total_micros.unwrap_or_else(|| {
                resources
                    .user_cpu_micros
                    .unwrap_or_default()
                    .saturating_add(resources.system_cpu_micros.unwrap_or_default())
            });
            value.cpu_micros_sum = value.cpu_micros_sum.saturating_add(cpu);
            value.memory_bytes_max = value.memory_bytes_max.max(
                resources
                    .peak_memory_bytes
                    .or(resources.memory_bytes)
                    .unwrap_or_default(),
            );
        }
        let outcome = value.outcomes.entry(span.outcome).or_default();
        *outcome = outcome.saturating_add(1);
    }
}

/// Invalid in-memory performance sink configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PerformanceSinkError {
    /// Capacity is zero or exceeds the hard process bound.
    InvalidCapacity,
}

impl fmt::Display for PerformanceSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid invocation performance sink capacity")
    }
}

impl std::error::Error for PerformanceSinkError {}

/// Cloneable per-invocation recorder attached only when detailed diagnostics are enabled.
#[derive(Clone)]
pub struct InvocationPerformanceRecorder {
    request_id: RequestId,
    invocation_id: InvocationId,
    runtime: PerformanceRuntime,
    sink: Arc<dyn InvocationPerformanceSink>,
}

impl fmt::Debug for InvocationPerformanceRecorder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvocationPerformanceRecorder")
            .field("request_id", &self.request_id)
            .field("invocation_id", &self.invocation_id)
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

impl InvocationPerformanceRecorder {
    /// Creates a recorder for one trusted invocation identity and runtime.
    #[must_use]
    pub fn new(
        request_id: RequestId,
        invocation_id: InvocationId,
        runtime: PerformanceRuntime,
        sink: Arc<dyn InvocationPerformanceSink>,
    ) -> Self {
        Self {
            request_id,
            invocation_id,
            runtime,
            sink,
        }
    }

    /// Starts one monotonic operation timer.
    pub fn start(
        &self,
        component: PerformanceComponent,
        operation: PerformanceOperation,
        input_bytes: Option<u64>,
    ) -> InvocationPerformanceTimer {
        InvocationPerformanceTimer {
            recorder: self.clone(),
            component,
            operation,
            started: Instant::now(),
            started_unix_micros: unix_micros(),
            input_bytes,
            finished: false,
        }
    }

    /// Returns the configured runtime attribution.
    #[must_use]
    pub const fn runtime(&self) -> PerformanceRuntime {
        self.runtime
    }
}

/// In-progress performance span which emits `abandoned` if a return path forgets to finish it.
#[must_use]
pub struct InvocationPerformanceTimer {
    recorder: InvocationPerformanceRecorder,
    component: PerformanceComponent,
    operation: PerformanceOperation,
    started: Instant,
    started_unix_micros: u64,
    input_bytes: Option<u64>,
    finished: bool,
}

impl fmt::Debug for InvocationPerformanceTimer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvocationPerformanceTimer")
            .field("component", &self.component)
            .field("operation", &self.operation)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl InvocationPerformanceTimer {
    /// Completes and emits the span.
    pub fn finish(
        mut self,
        outcome: PerformanceOutcome,
        error_code: Option<&str>,
        output_bytes: Option<u64>,
        resources: Option<PerformanceResourceUsage>,
    ) {
        self.emit(outcome, error_code, output_bytes, resources);
        self.finished = true;
    }

    fn emit(
        &self,
        outcome: PerformanceOutcome,
        error_code: Option<&str>,
        output_bytes: Option<u64>,
        resources: Option<PerformanceResourceUsage>,
    ) {
        self.recorder.sink.record(InvocationPerformanceSpanV1 {
            format_version: INVOCATION_PERFORMANCE_FORMAT_VERSION,
            request_id: self.recorder.request_id,
            invocation_id: self.recorder.invocation_id,
            runtime: self.recorder.runtime,
            component: self.component,
            operation: self.operation,
            started_unix_micros: self.started_unix_micros,
            duration_micros: u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX),
            input_bytes: self.input_bytes,
            output_bytes,
            outcome,
            error_code: error_code.map(str::to_owned),
            resources,
        });
    }
}

impl Drop for InvocationPerformanceTimer {
    fn drop(&mut self) {
        if !self.finished {
            self.emit(PerformanceOutcome::Abandoned, None, None, None);
        }
    }
}

fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|value| u64::try_from(value.as_micros()).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    #[test]
    fn bounded_sink_records_completed_and_abandoned_spans() -> Result<(), Box<dyn std::error::Error>>
    {
        let sink = Arc::new(MemoryInvocationPerformanceSink::new(2)?);
        let recorder = InvocationPerformanceRecorder::new(
            RequestId::from_ulid(Ulid::from(1_u128)),
            InvocationId::from_ulid(Ulid::from(2_u128)),
            PerformanceRuntime::SafeV8,
            sink.clone(),
        );
        recorder
            .start(
                PerformanceComponent::Runtime,
                PerformanceOperation::Validate,
                Some(10),
            )
            .finish(PerformanceOutcome::Succeeded, None, Some(20), None);
        drop(recorder.start(
            PerformanceComponent::Function,
            PerformanceOperation::ExecuteHandler,
            None,
        ));
        drop(recorder.start(
            PerformanceComponent::Cleanup,
            PerformanceOperation::Cleanup,
            None,
        ));
        let spans = sink.snapshot();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].outcome, PerformanceOutcome::Succeeded);
        assert_eq!(spans[1].outcome, PerformanceOutcome::Abandoned);
        assert_eq!(sink.dropped(), 1);
        Ok(())
    }

    #[test]
    fn aggregate_sink_has_fixed_keys_and_resource_totals() -> Result<(), Box<dyn std::error::Error>>
    {
        let aggregate = AggregateInvocationPerformanceSink::default();
        aggregate.record(InvocationPerformanceSpanV1 {
            format_version: INVOCATION_PERFORMANCE_FORMAT_VERSION,
            request_id: RequestId::from_ulid(Ulid::from(3_u128)),
            invocation_id: InvocationId::from_ulid(Ulid::from(4_u128)),
            runtime: PerformanceRuntime::NodeHost,
            component: PerformanceComponent::NodeProcess,
            operation: PerformanceOperation::ExecuteRunner,
            started_unix_micros: 1,
            duration_micros: 40,
            input_bytes: Some(25),
            output_bytes: Some(30),
            outcome: PerformanceOutcome::Succeeded,
            error_code: None,
            resources: Some(PerformanceResourceUsage {
                user_cpu_micros: Some(7),
                system_cpu_micros: Some(3),
                peak_memory_bytes: Some(1024),
                ..PerformanceResourceUsage::default()
            }),
        });
        let snapshot = aggregate.snapshot();
        let value = &snapshot.series.first().ok_or("series missing")?.value;
        assert_eq!(value.spans, 1);
        assert_eq!(value.cpu_micros_sum, 10);
        assert_eq!(value.memory_bytes_max, 1024);
        assert_eq!(value.duration_micros_buckets.last(), Some(&1));
        assert_eq!(value.outcomes.get(&PerformanceOutcome::Succeeded), Some(&1));
        serde_json::to_string(&snapshot)?;
        Ok(())
    }
}
