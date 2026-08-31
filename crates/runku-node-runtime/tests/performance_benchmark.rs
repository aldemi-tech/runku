//! Opt-in end-to-end latency/resource benchmark for every supported execution mode.

use std::{
    collections::BTreeSet,
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use runku_artifact_s3::{
    S3ArtifactStore, S3ArtifactStoreConfig, S3Credentials, S3StaticCredentials,
};
use runku_build::{BuildMetadata, build_project};
use runku_core::{
    BuildId, EnvironmentId, EnvironmentScope, FunctionId, InvocationId, OperationId, ProjectId,
    ReleaseId, RequestId,
};
use runku_execution_queue::{
    ExecutionAgent, ExecutionAgentConfig, ExecutionClass, ExecutionControlPlane, ExecutionQueue,
    NatsExecutionControlConfig, NatsExecutionControlPlane, NatsExecutionQueue,
    NatsExecutionQueueConfig,
};
use runku_node_runtime::{
    DedicatedHostPolicy, DockerNodeRuntime, DockerNodeRuntimeConfig, FirecrackerNodeRuntime,
    FirecrackerNodeRuntimeConfig, FullNodeActionRuntime, FullNodeExecutionHandler,
    HostNodeArtifactCache, HostNodeRuntime, HostNodeRuntimeConfig, LocalNodeRuntime,
    LocalNodeRuntimeConfig, QueuedNodeRuntime, QueuedNodeRuntimeConfig,
};
use runku_observability::{
    InvocationPerformanceSink, InvocationPerformanceSpanV1, MemoryInvocationPerformanceSink,
    PerformanceOperation, PerformanceOutcome, PerformanceRuntime,
};
use runku_releases::{
    ArtifactStore, AuthPolicy, Capability, FullNodeEgressPolicy, FunctionManifest, FunctionType,
    FunctionVisibility, NodeOciDescriptorV1, ReleaseCommand, ReleaseCommandResult, ReleaseError,
    ReleaseManifestV1, ReleaseRepository, ReleaseRepositoryBackend,
    ReleaseRepositoryTelemetrySnapshot, RuntimeClass, SafeEsmBundleV1, ServingSnapshot,
    Sha256Digest, decode_release_manifest, encode_node_oci_descriptor, encode_safe_esm_bundle,
};
use runku_runtime::{
    CancellationToken, InvocationRequest, RuntimeError, RuntimeLimits, RuntimeSupervisor,
};
use runku_value::{CanonicalValue, TimestampMicros, encode_stored_value};
use serde::Serialize;
use tempfile::tempdir;
use tokio::{sync::watch, task::JoinSet};

const NODE_SOURCE: &str = r#""use runku node"
import { action, v } from "@runku/server"
export const echo = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), handler(_ctx, value) { return value } })
export const create = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), async handler(ctx, value) { await new Promise(resolve => setTimeout(resolve, 10 + value.charCodeAt(value.length - 1) % 7)); return `created:${value}:${ctx.invocation.invocationId}:${ctx.invocation.function}` } })
export const deleteItem = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), async handler(ctx, value) { await new Promise(resolve => setTimeout(resolve, 10 + value.charCodeAt(value.length - 1) % 5)); return `deleted:${value}:${ctx.invocation.invocationId}:${ctx.invocation.function}` } })
export const inspect = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), async handler(ctx, value) { await new Promise(resolve => setTimeout(resolve, 10 + value.charCodeAt(value.length - 1) % 3)); return `inspected:${value}:${ctx.invocation.invocationId}:${ctx.invocation.function}` } })"#;
const SAFE_SOURCE: &str = "export default (_ctx, value) => value;";
const INPUT: &str = "benchmark-payload";

#[derive(Clone)]
struct RequestTemplate {
    scope: EnvironmentScope,
    release_id: ReleaseId,
    function_id: FunctionId,
    manifest: Arc<ReleaseManifestV1>,
    artifact: Arc<[u8]>,
    wall_timeout: Duration,
}

impl RequestTemplate {
    fn request(
        &self,
        runtime: PerformanceRuntime,
        sink: Option<Arc<dyn InvocationPerformanceSink>>,
    ) -> Result<InvocationRequest, RuntimeError> {
        self.request_for(
            self.function_id,
            CanonicalValue::String(INPUT.to_owned()),
            runtime,
            sink,
        )
    }

    fn request_for(
        &self,
        function_id: FunctionId,
        arguments: CanonicalValue,
        runtime: PerformanceRuntime,
        sink: Option<Arc<dyn InvocationPerformanceSink>>,
    ) -> Result<InvocationRequest, RuntimeError> {
        let request = InvocationRequest::new(
            self.scope,
            self.release_id,
            RequestId::generate(),
            InvocationId::generate(),
            function_id,
            Arc::clone(&self.manifest),
            Arc::clone(&self.artifact),
            arguments,
            self.wall_timeout,
            CancellationToken::new(),
        )?;
        Ok(match sink {
            Some(sink) => request.with_performance_sink(runtime, sink),
            None => request,
        })
    }

    fn request_for_with_cancellation(
        &self,
        function_id: FunctionId,
        arguments: CanonicalValue,
        _runtime: PerformanceRuntime,
        cancellation: CancellationToken,
    ) -> Result<InvocationRequest, RuntimeError> {
        InvocationRequest::new(
            self.scope,
            self.release_id,
            RequestId::generate(),
            InvocationId::generate(),
            function_id,
            Arc::clone(&self.manifest),
            Arc::clone(&self.artifact),
            arguments,
            self.wall_timeout,
            cancellation,
        )
    }
}

#[async_trait]
trait BenchmarkExecutor: Send + Sync {
    async fn invoke(&self, request: InvocationRequest) -> Result<CanonicalValue, RuntimeError>;
}

struct SafeExecutor(RuntimeSupervisor);

#[async_trait]
impl BenchmarkExecutor for SafeExecutor {
    async fn invoke(&self, request: InvocationRequest) -> Result<CanonicalValue, RuntimeError> {
        self.0.invoke(request).await
    }
}

struct NodeExecutor(Arc<dyn FullNodeActionRuntime>);

#[async_trait]
impl BenchmarkExecutor for NodeExecutor {
    async fn invoke(&self, request: InvocationRequest) -> Result<CanonicalValue, RuntimeError> {
        self.0.execute(request).await.map(|outcome| outcome.value)
    }
}

#[derive(Serialize)]
struct BenchmarkCaseReport {
    case: String,
    runtime: PerformanceRuntime,
    cold_micros: u64,
    warm_iterations: usize,
    warm_p50_micros: u64,
    warm_p95_micros: u64,
    warm_p99_micros: u64,
    concurrent_requests: usize,
    concurrency: usize,
    concurrent_total_micros: u64,
    throughput_requests_per_second: u64,
    input_bytes: u64,
    output_bytes: u64,
    max_peak_memory_bytes: Option<u64>,
    average_cpu_micros: Option<u64>,
    spans: usize,
    abandoned_spans: usize,
    warm_pool: Option<WarmPoolReport>,
}

#[derive(Serialize)]
struct WarmPoolReport {
    hits: u64,
    reconnects: u64,
    failed: u64,
    replacements: u64,
    replacement_failures: u64,
    workers: usize,
    idle: usize,
}

#[derive(Serialize)]
struct BenchmarkReport {
    format_version: u16,
    input_fixture: &'static str,
    warmups: usize,
    iterations: usize,
    concurrent_requests: usize,
    concurrency: usize,
    cases: Vec<BenchmarkCaseReport>,
    routing_checks: Vec<RoutingCorrectnessReport>,
    open_loop_checks: Vec<OpenLoopReport>,
}

#[derive(Serialize)]
struct RoutingCorrectnessReport {
    case: String,
    requests: usize,
    functions: Vec<String>,
    configured_slots: usize,
    peak_agent_concurrency: u64,
    unique_request_ids: usize,
    unique_invocation_ids: usize,
    warm_pool_hits: u64,
    warm_pool_misses: u64,
    mismatches: usize,
    elapsed_micros: u64,
}

#[derive(Serialize)]
struct OpenLoopReport {
    case: String,
    target_requests_per_second: usize,
    injection_duration_secs: usize,
    scheduled_requests: usize,
    succeeded: usize,
    failed: usize,
    mismatches: usize,
    configured_slots: usize,
    peak_agent_concurrency: u64,
    warm_pool_hits: u64,
    warm_pool_misses: u64,
    injection_elapsed_micros: u64,
    completion_elapsed_micros: u64,
    completion_throughput_requests_per_second: u64,
    latency_p50_micros: u64,
    latency_p95_micros: u64,
    latency_p99_micros: u64,
}

#[derive(Clone, Copy)]
struct BenchmarkPoolTelemetrySnapshot {
    hits: u64,
    misses: u64,
}

trait BenchmarkPoolTelemetry: Send + Sync {
    fn benchmark_pool_telemetry(&self) -> BenchmarkPoolTelemetrySnapshot;
}

impl BenchmarkPoolTelemetry for FirecrackerNodeRuntime {
    fn benchmark_pool_telemetry(&self) -> BenchmarkPoolTelemetrySnapshot {
        let snapshot = self.telemetry();
        BenchmarkPoolTelemetrySnapshot {
            hits: snapshot.hits,
            misses: snapshot.reconnects,
        }
    }
}

struct NodeFixture {
    _directory: tempfile::TempDir,
    local: RequestTemplate,
    local_runtime: Option<Arc<LocalNodeRuntime>>,
    host_runtime: Option<Arc<HostNodeRuntime>>,
    host: RequestTemplate,
    cache: HostNodeArtifactCache,
}

impl NodeFixture {
    fn remote(&self, image: &str) -> Result<RequestTemplate, Box<dyn Error>> {
        let descriptor = NodeOciDescriptorV1::new(image.to_owned())?
            .with_egress_policy(FullNodeEgressPolicy::none());
        self.cache.materialize(
            &self.local.manifest,
            &self.local.artifact,
            &descriptor,
            None,
        )?;
        let artifact: Arc<[u8]> = encode_node_oci_descriptor(&descriptor)?.into();
        let mut manifest = (*self.local.manifest).clone();
        manifest.artifact = descriptor.descriptor()?;
        Ok(RequestTemplate {
            scope: self.local.scope,
            release_id: self.local.release_id,
            function_id: self.local.function_id,
            manifest: Arc::new(manifest),
            artifact,
            wall_timeout: self.local.wall_timeout,
        })
    }
}

struct AgentGuard {
    shutdown: watch::Sender<bool>,
    agent: Arc<ExecutionAgent>,
}

impl Drop for AgentGuard {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn full_execution_flow_performance_baseline() -> Result<(), Box<dyn Error>> {
    if std::env::var_os("RUNKU_PERFORMANCE_BENCHMARK").is_none() {
        return Ok(());
    }
    let iterations = env_usize("RUNKU_BENCH_ITERATIONS", 20, 1, 10_000)?;
    let warmups = env_usize("RUNKU_BENCH_WARMUPS", 3, 0, 1_000)?;
    let concurrency = env_usize("RUNKU_BENCH_CONCURRENCY", 8, 1, 128)?;
    let concurrent_requests = env_usize("RUNKU_BENCH_CONCURRENT_REQUESTS", 32, 1, 10_000)?;
    let request_timeout = Duration::from_secs(u64::try_from(env_usize(
        "RUNKU_BENCH_REQUEST_TIMEOUT_SECS",
        15,
        1,
        600,
    )?)?);
    let routing_requests = env_usize(
        "RUNKU_BENCH_ROUTING_REQUESTS",
        concurrent_requests
            .max(concurrency.saturating_mul(4))
            .min(1_024),
        1,
        20_000,
    )?;
    let open_loop_rps = env_usize("RUNKU_BENCH_OPEN_LOOP_RPS", 0, 0, 10_000)?;
    let open_loop_duration_secs = env_usize("RUNKU_BENCH_OPEN_LOOP_DURATION_SECS", 0, 0, 600)?;
    if (open_loop_rps == 0) != (open_loop_duration_secs == 0)
        || open_loop_rps.saturating_mul(open_loop_duration_secs) > 20_000
    {
        return Err("open-loop rate/duration configuration is invalid".into());
    }
    let modes = modes();
    let node = node_fixture(concurrency, &modes, request_timeout)
        .map_err(|error| format!("node fixture: {error:?}"))?;
    let safe = safe_fixture(request_timeout).map_err(|error| format!("safe fixture: {error:?}"))?;
    let mut reports = Vec::new();
    let mut routing_checks = Vec::new();
    let mut open_loop_checks = Vec::new();

    if modes.contains("safe_v8") {
        let supervisor = RuntimeSupervisor::start(
            RuntimeLimits::builder(concurrency.min(64), concurrent_requests.max(32))
                .max_wall_time(request_timeout)
                .build()?,
        )?;
        reports.push(
            run_case(
                "safe_v8",
                PerformanceRuntime::SafeV8,
                Arc::new(SafeExecutor(supervisor)),
                safe,
                warmups,
                iterations,
                concurrency,
                concurrent_requests,
                None,
            )
            .await?,
        );
    }
    if modes.contains("node_local") {
        let runtime: Arc<dyn FullNodeActionRuntime> = node
            .local_runtime
            .clone()
            .ok_or("node local runtime fixture missing")?;
        reports.push(
            run_case(
                "node_local",
                PerformanceRuntime::NodeLocal,
                Arc::new(NodeExecutor(runtime)),
                node.local.clone(),
                warmups,
                iterations,
                concurrency,
                concurrent_requests,
                None,
            )
            .await?,
        );
    }
    if modes.contains("node_host") {
        let runtime: Arc<dyn FullNodeActionRuntime> = node
            .host_runtime
            .clone()
            .ok_or("node host runtime fixture missing")?;
        reports.push(
            run_case(
                "node_host",
                PerformanceRuntime::NodeHost,
                Arc::new(NodeExecutor(runtime)),
                node.host.clone(),
                warmups,
                iterations,
                concurrency,
                concurrent_requests,
                None,
            )
            .await?,
        );
    }

    if let Some(image) = std::env::var_os("RUNKU_BENCH_OCI_IMAGE") {
        let image = image.into_string().map_err(|_| "OCI image is not UTF-8")?;
        let remote = node
            .remote(&image)
            .map_err(|error| format!("remote OCI fixture: {error:?}"))?;
        if modes.contains("node_docker") {
            let runtime: Arc<dyn FullNodeActionRuntime> = Arc::new(DockerNodeRuntime::new(
                DockerNodeRuntimeConfig::new(concurrency)?,
            )?);
            reports.push(
                run_case(
                    "node_docker_oci",
                    PerformanceRuntime::NodeDocker,
                    Arc::new(NodeExecutor(runtime)),
                    remote.clone(),
                    warmups,
                    iterations,
                    concurrency,
                    concurrent_requests,
                    None,
                )
                .await?,
            );
        }
        if modes.contains("node_firecracker_warm") || modes.contains("remote_firecracker_warm") {
            let firecracker = firecracker_runtime(&remote, &image)?;
            firecracker
                .prepare(&remote.manifest, &remote.artifact)
                .await?;
            if modes.contains("node_firecracker_warm") {
                let runtime: Arc<dyn FullNodeActionRuntime> = firecracker.clone();
                let mut report = run_case(
                    "node_firecracker_prewarmed",
                    PerformanceRuntime::NodeFirecracker,
                    Arc::new(NodeExecutor(runtime)),
                    remote.clone(),
                    warmups,
                    iterations,
                    concurrency,
                    concurrent_requests,
                    None,
                )
                .await?;
                report.warm_pool = Some(firecracker_report_telemetry(&firecracker));
                reports.push(report);
            }
            if modes.contains("remote_firecracker_warm") {
                let runtime: Arc<dyn FullNodeActionRuntime> = firecracker.clone();
                let (queued, guard, sink) = remote_runtime(
                    &remote,
                    runtime,
                    PerformanceRuntime::NodeFirecracker,
                    concurrency,
                )
                .await?;
                verify_firecracker_gateway_cancellation(&remote, Arc::clone(&queued), &guard)
                    .await?;
                let case = "gateway_nats_s3_agent_firecracker_prewarmed";
                let mut report = run_case(
                    case,
                    PerformanceRuntime::RemoteGateway,
                    Arc::new(NodeExecutor(Arc::clone(&queued))),
                    remote.clone(),
                    warmups,
                    iterations,
                    concurrency,
                    concurrent_requests,
                    Some(Arc::clone(&sink)),
                )
                .await?;
                report.warm_pool = Some(firecracker_report_telemetry(&firecracker));
                routing_checks.push(
                    verify_scaled_warm_routing(
                        &format!("{case}_routing"),
                        &remote,
                        Arc::clone(&queued),
                        &guard,
                        firecracker.as_ref(),
                        &sink,
                        concurrency,
                        routing_requests,
                    )
                    .await?,
                );
                if open_loop_rps > 0 {
                    open_loop_checks.push(
                        verify_open_loop_warm_capacity(
                            &format!("{case}_open_loop"),
                            &remote,
                            Arc::clone(&queued),
                            &guard,
                            firecracker.as_ref(),
                            &sink,
                            concurrency,
                            open_loop_rps,
                            open_loop_duration_secs,
                        )
                        .await?,
                    );
                }
                reports.push(report);
                drop(guard);
            }
        }
        if modes.contains("remote_host") {
            let runtime: Arc<dyn FullNodeActionRuntime> = node
                .host_runtime
                .clone()
                .ok_or("remote host runtime fixture missing")?;
            let (queued, guard, sink) =
                remote_runtime(&remote, runtime, PerformanceRuntime::NodeHost, concurrency).await?;
            reports.push(
                run_case(
                    "gateway_nats_s3_agent_host",
                    PerformanceRuntime::RemoteGateway,
                    Arc::new(NodeExecutor(queued)),
                    remote,
                    warmups,
                    iterations,
                    concurrency,
                    concurrent_requests,
                    Some(sink),
                )
                .await?,
            );
            drop(guard);
        }
    }

    let report = BenchmarkReport {
        format_version: 1,
        input_fixture: INPUT,
        warmups,
        iterations,
        concurrent_requests,
        concurrency,
        cases: reports,
        routing_checks,
        open_loop_checks,
    };
    println!("RUNKU_BENCHMARK_REPORT {}", serde_json::to_string(&report)?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kubernetes_distributed_execution_agent() -> Result<(), Box<dyn Error>> {
    if std::env::var_os("RUNKU_KUBERNETES_EXECUTION_AGENT").is_none() {
        return Ok(());
    }
    let slots = env_usize("RUNKU_BENCH_CONCURRENCY", 2, 1, 32)?;
    let timeout = Duration::from_secs(u64::try_from(env_usize(
        "RUNKU_BENCH_REQUEST_TIMEOUT_SECS",
        120,
        1,
        600,
    )?)?);
    let fixture = node_fixture(
        slots,
        &BTreeSet::from(["remote_firecracker_warm".to_owned()]),
        timeout,
    )?;
    let image = std::env::var("RUNKU_BENCH_OCI_IMAGE")?;
    let remote = fixture.remote(&image)?;
    let firecracker = firecracker_runtime(&remote, &image)?;
    firecracker
        .prepare(&remote.manifest, &remote.artifact)
        .await?;
    let (queue, control, class) = remote_boundaries().await?;
    let releases = Arc::new(StaticReleases {
        scope: remote.scope,
        manifest: (*remote.manifest).clone(),
    });
    let handler = Arc::new(FullNodeExecutionHandler::new(
        releases,
        s3_store()?,
        firecracker.clone(),
        control,
    ));
    let agent = Arc::new(ExecutionAgent::new(
        queue,
        handler,
        ExecutionAgentConfig {
            class,
            slots,
            max_concurrent_per_project: slots,
            pull_wait: Duration::from_millis(100),
        },
    )?);
    let (shutdown, receiver) = watch::channel(false);
    let running = tokio::spawn(Arc::clone(&agent).run(receiver));
    std::fs::write("/tmp/runku-agent-ready", b"ready\n")?;
    println!(
        "RUNKU_KUBERNETES_AGENT_READY {}",
        serde_json::json!({ "slots": slots, "scope": shared_benchmark_scope()? })
    );

    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal?,
        _ = terminate.recv() => {}
    }
    shutdown.send_replace(true);
    tokio::time::timeout(Duration::from_secs(30), running).await???;
    firecracker.shutdown().await;
    let telemetry = agent.telemetry();
    println!(
        "RUNKU_KUBERNETES_AGENT_FINAL {}",
        serde_json::json!({
            "deliveries": telemetry.deliveries,
            "completed": telemetry.completed,
            "active": telemetry.active_executions,
            "peak": telemetry.peak_concurrent_executions,
            "uncertain": telemetry.uncertain,
            "rejected": telemetry.rejected
        })
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn kubernetes_distributed_gateway_load() -> Result<(), Box<dyn Error>> {
    if std::env::var_os("RUNKU_KUBERNETES_GATEWAY_BENCHMARK").is_none() {
        return Ok(());
    }
    let slots = env_usize("RUNKU_BENCH_CONCURRENCY", 2, 1, 64)?;
    let iterations = env_usize("RUNKU_BENCH_ITERATIONS", 20, 1, 10_000)?;
    let warmups = env_usize("RUNKU_BENCH_WARMUPS", 5, 0, 1_000)?;
    let requests = env_usize("RUNKU_BENCH_CONCURRENT_REQUESTS", 1_000, 1, 20_000)?;
    let routing_requests = env_usize("RUNKU_BENCH_ROUTING_REQUESTS", 1_000, 1, 20_000)?;
    let open_loop_rps = env_usize("RUNKU_BENCH_OPEN_LOOP_RPS", 0, 0, 10_000)?;
    let open_loop_duration = env_usize("RUNKU_BENCH_OPEN_LOOP_DURATION_SECS", 0, 0, 600)?;
    let timeout = Duration::from_secs(u64::try_from(env_usize(
        "RUNKU_BENCH_REQUEST_TIMEOUT_SECS",
        120,
        1,
        600,
    )?)?);
    let fixture = node_fixture(
        slots,
        &BTreeSet::from(["remote_firecracker_warm".to_owned()]),
        timeout,
    )?;
    let image = std::env::var("RUNKU_BENCH_OCI_IMAGE")?;
    let remote = fixture.remote(&image)?;
    let (queue, control, class) = remote_boundaries().await?;
    let artifacts = s3_store()?;
    artifacts
        .put(&remote.manifest.artifact, &remote.artifact)
        .await?;
    let gateway: Arc<dyn FullNodeActionRuntime> = Arc::new(QueuedNodeRuntime::new(
        queue,
        control,
        QueuedNodeRuntimeConfig {
            class,
            result_wait: Duration::from_millis(100),
        },
    )?);
    let case = "kubernetes_gateway_nats_s3_agents_firecracker";
    let report = run_case(
        case,
        PerformanceRuntime::RemoteGateway,
        Arc::new(NodeExecutor(Arc::clone(&gateway))),
        remote.clone(),
        warmups,
        iterations,
        slots,
        requests,
        None,
    )
    .await?;
    let routing = verify_external_routing(
        &format!("{case}_routing"),
        &remote,
        Arc::clone(&gateway),
        slots,
        routing_requests,
    )
    .await?;
    let open_loop_checks = if open_loop_rps == 0 {
        Vec::new()
    } else {
        vec![
            verify_external_open_loop(
                &format!("{case}_open_loop"),
                &remote,
                gateway,
                slots,
                open_loop_rps,
                open_loop_duration,
            )
            .await?,
        ]
    };
    println!(
        "RUNKU_BENCHMARK_REPORT {}",
        serde_json::to_string(&BenchmarkReport {
            format_version: 1,
            input_fixture: INPUT,
            warmups,
            iterations,
            concurrent_requests: requests,
            concurrency: slots,
            cases: vec![report],
            routing_checks: vec![routing],
            open_loop_checks,
        })?
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_case(
    name: &str,
    runtime: PerformanceRuntime,
    executor: Arc<dyn BenchmarkExecutor>,
    template: RequestTemplate,
    warmups: usize,
    iterations: usize,
    concurrency: usize,
    concurrent_requests: usize,
    sink: Option<Arc<MemoryInvocationPerformanceSink>>,
) -> Result<BenchmarkCaseReport, Box<dyn Error>> {
    let sink = match sink {
        Some(sink) => sink,
        None => Arc::new(MemoryInvocationPerformanceSink::new(
            (iterations + concurrent_requests + 1)
                .saturating_mul(64)
                .min(100_000),
        )?),
    };
    let sink_dyn: Arc<dyn InvocationPerformanceSink> = sink.clone();

    let cold_started = Instant::now();
    let request = template.request(runtime, Some(Arc::clone(&sink_dyn)))?;
    mark_request(name, "cold", &request)?;
    verify_output(&executor.invoke(request).await?)?;
    let cold_micros = micros(cold_started.elapsed());

    for _ in 0..warmups {
        let request = template.request(runtime, None)?;
        mark_request(name, "warmup", &request)?;
        verify_output(&executor.invoke(request).await?)?;
    }
    let mut warm = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let request = template.request(runtime, Some(Arc::clone(&sink_dyn)))?;
        mark_request(name, "warm", &request)?;
        verify_output(&executor.invoke(request).await?)?;
        warm.push(micros(started.elapsed()));
    }

    let concurrent_started = Instant::now();
    let mut tasks = JoinSet::new();
    for _ in 0..concurrent_requests {
        let executor = Arc::clone(&executor);
        let request = template.request(runtime, Some(Arc::clone(&sink_dyn)))?;
        mark_request(name, "concurrent", &request)?;
        tasks.spawn(async move { executor.invoke(request).await });
    }
    while let Some(result) = tasks.join_next().await {
        verify_output(&result??)?;
    }
    let concurrent_total_micros = micros(concurrent_started.elapsed());
    let spans = sink.snapshot();
    let abandoned_spans = spans
        .iter()
        .filter(|span| span.outcome == PerformanceOutcome::Abandoned)
        .count();
    if abandoned_spans != 0 || sink.dropped() != 0 {
        return Err(format!(
            "{name} produced {abandoned_spans} abandoned and {} dropped spans",
            sink.dropped()
        )
        .into());
    }
    for span in &spans {
        println!("RUNKU_PERFORMANCE_SPAN {}", serde_json::to_string(span)?);
    }
    warm.sort_unstable();
    let cpu = resource_cpu_samples(&spans);
    let max_peak_memory_bytes = spans
        .iter()
        .filter_map(|span| span.resources)
        .filter_map(|resources| resources.peak_memory_bytes.or(resources.memory_bytes))
        .max();
    let input_bytes = encoded_len(&CanonicalValue::String(INPUT.to_owned()))?;
    let throughput_requests_per_second = u64::try_from(concurrent_requests)?
        .saturating_mul(1_000_000)
        .checked_div(concurrent_total_micros)
        .unwrap_or_default();
    Ok(BenchmarkCaseReport {
        case: name.to_owned(),
        runtime,
        cold_micros,
        warm_iterations: iterations,
        warm_p50_micros: percentile(&warm, 50),
        warm_p95_micros: percentile(&warm, 95),
        warm_p99_micros: percentile(&warm, 99),
        concurrent_requests,
        concurrency,
        concurrent_total_micros,
        throughput_requests_per_second,
        input_bytes,
        output_bytes: input_bytes,
        max_peak_memory_bytes,
        average_cpu_micros: (!cpu.is_empty())
            .then(|| cpu.iter().sum::<u64>() / u64::try_from(cpu.len()).unwrap_or(1)),
        spans: spans.len(),
        abandoned_spans,
        warm_pool: None,
    })
}

async fn verify_external_routing(
    case: &str,
    template: &RequestTemplate,
    gateway: Arc<dyn FullNodeActionRuntime>,
    slots: usize,
    requests: usize,
) -> Result<RoutingCorrectnessReport, Box<dyn Error>> {
    let routes = [
        ("actions.create", "created"),
        ("actions.deleteItem", "deleted"),
        ("actions.inspect", "inspected"),
    ];
    let functions = routes
        .iter()
        .map(|(name, tag)| {
            template
                .manifest
                .functions
                .iter()
                .find(|function| function.name.as_str() == *name)
                .map(|function| (*name, *tag, function.id))
                .ok_or_else(|| format!("routing fixture function {name} missing"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let started = Instant::now();
    let mut request_ids = BTreeSet::new();
    let mut invocation_ids = BTreeSet::new();
    let mut tasks = JoinSet::new();
    for index in 0..requests {
        let (function_name, result_tag, function_id) = functions[index % functions.len()];
        let token = format!("routing-{index:05}");
        let request = template.request_for(
            function_id,
            CanonicalValue::String(token.clone()),
            PerformanceRuntime::RemoteGateway,
            None,
        )?;
        let invocation_id = request.invocation_id();
        if !request_ids.insert(request.request_id().to_string())
            || !invocation_ids.insert(invocation_id.to_string())
        {
            return Err("routing fixture generated duplicate identifiers".into());
        }
        let expected = CanonicalValue::String(format!(
            "{result_tag}:{token}:{invocation_id}:{function_name}"
        ));
        let gateway = Arc::clone(&gateway);
        tasks.spawn(async move { (expected, gateway.execute(request).await) });
    }
    let mut mismatches = 0;
    while let Some(joined) = tasks.join_next().await {
        let (expected, result) = joined?;
        if !matches!(result, Ok(ref outcome) if outcome.value == expected) {
            mismatches += 1;
        }
    }
    if mismatches != 0 {
        return Err(format!("external routing produced {mismatches} mismatches").into());
    }
    Ok(RoutingCorrectnessReport {
        case: case.to_owned(),
        requests,
        functions: functions
            .iter()
            .map(|(name, _, _)| (*name).to_owned())
            .collect(),
        configured_slots: slots,
        peak_agent_concurrency: 0,
        unique_request_ids: request_ids.len(),
        unique_invocation_ids: invocation_ids.len(),
        warm_pool_hits: 0,
        warm_pool_misses: 0,
        mismatches,
        elapsed_micros: micros(started.elapsed()),
    })
}

async fn verify_external_open_loop(
    case: &str,
    template: &RequestTemplate,
    gateway: Arc<dyn FullNodeActionRuntime>,
    slots: usize,
    target_rps: usize,
    duration_secs: usize,
) -> Result<OpenLoopReport, Box<dyn Error>> {
    let routes = [
        ("actions.create", "created"),
        ("actions.deleteItem", "deleted"),
        ("actions.inspect", "inspected"),
    ];
    let functions = routes
        .iter()
        .map(|(name, tag)| {
            template
                .manifest
                .functions
                .iter()
                .find(|function| function.name.as_str() == *name)
                .map(|function| (*name, *tag, function.id))
                .ok_or_else(|| format!("open-loop fixture function {name} missing"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let requests = target_rps.saturating_mul(duration_secs);
    let origin = tokio::time::Instant::now();
    let mut tasks = JoinSet::new();
    for index in 0..requests {
        let due_nanos = u64::try_from(
            u128::try_from(index)?
                .saturating_mul(1_000_000_000)
                .checked_div(u128::try_from(target_rps)?)
                .unwrap_or_default(),
        )?;
        tokio::time::sleep_until(origin + Duration::from_nanos(due_nanos)).await;
        let (function_name, result_tag, function_id) = functions[index % functions.len()];
        let token = format!("open-loop-{index:05}");
        let request = template.request_for(
            function_id,
            CanonicalValue::String(token.clone()),
            PerformanceRuntime::RemoteGateway,
            None,
        )?;
        let invocation_id = request.invocation_id();
        let expected = CanonicalValue::String(format!(
            "{result_tag}:{token}:{invocation_id}:{function_name}"
        ));
        let gateway = Arc::clone(&gateway);
        tasks.spawn(async move {
            let started = Instant::now();
            (
                expected,
                gateway.execute(request).await,
                micros(started.elapsed()),
            )
        });
    }
    let injection_elapsed_micros = micros(origin.elapsed());
    let mut succeeded = 0;
    let mut failed = 0;
    let mut mismatches = 0;
    let mut latencies = Vec::with_capacity(requests);
    while let Some(joined) = tasks.join_next().await {
        let (expected, result, latency) = joined?;
        latencies.push(latency);
        match result {
            Ok(outcome) if outcome.value == expected => succeeded += 1,
            Ok(_) => mismatches += 1,
            Err(_) => failed += 1,
        }
    }
    let completion_elapsed_micros = micros(origin.elapsed());
    latencies.sort_unstable();
    if succeeded != requests || failed != 0 || mismatches != 0 {
        return Err(format!(
            "external open-loop failed: success={succeeded}/{requests} failed={failed} mismatches={mismatches}"
        )
        .into());
    }
    Ok(OpenLoopReport {
        case: case.to_owned(),
        target_requests_per_second: target_rps,
        injection_duration_secs: duration_secs,
        scheduled_requests: requests,
        succeeded,
        failed,
        mismatches,
        configured_slots: slots,
        peak_agent_concurrency: 0,
        warm_pool_hits: 0,
        warm_pool_misses: 0,
        injection_elapsed_micros,
        completion_elapsed_micros,
        completion_throughput_requests_per_second: u64::try_from(requests)?
            .saturating_mul(1_000_000)
            .checked_div(completion_elapsed_micros)
            .unwrap_or_default(),
        latency_p50_micros: percentile(&latencies, 50),
        latency_p95_micros: percentile(&latencies, 95),
        latency_p99_micros: percentile(&latencies, 99),
    })
}

async fn verify_firecracker_gateway_cancellation(
    template: &RequestTemplate,
    gateway: Arc<dyn FullNodeActionRuntime>,
    guard: &AgentGuard,
) -> Result<(), Box<dyn Error>> {
    let function = template
        .manifest
        .functions
        .iter()
        .find(|function| function.name.as_str() == "actions.create")
        .ok_or("Firecracker cancellation fixture function missing")?;
    let cancellation = CancellationToken::new();
    let request = template.request_for_with_cancellation(
        function.id,
        CanonicalValue::String("cancel-me".to_owned()),
        PerformanceRuntime::RemoteGateway,
        cancellation.clone(),
    )?;
    let execution = tokio::spawn({
        let gateway = Arc::clone(&gateway);
        async move { gateway.execute(request).await }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    cancellation.cancel();
    if !matches!(execution.await?, Err(RuntimeError::Cancelled)) {
        return Err("Firecracker active Gateway cancellation was not controlled".into());
    }
    for _ in 0..1_000 {
        if guard.agent.telemetry().active_executions == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    if guard.agent.telemetry().active_executions != 0 {
        return Err("Firecracker cancelled execution remained active".into());
    }
    let follow_up = template.request(PerformanceRuntime::RemoteGateway, None)?;
    verify_output(&gateway.execute(follow_up).await?.value)?;
    println!(
        "RUNKU_FIRECRACKER_CONFORMANCE {}",
        serde_json::json!({
            "gateway_active_cancellation": "controlled",
            "agent_active_after_cancel": 0,
            "post_cancel_execution": "succeeded"
        })
    );
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn verify_scaled_warm_routing(
    case: &str,
    template: &RequestTemplate,
    gateway: Arc<dyn FullNodeActionRuntime>,
    guard: &AgentGuard,
    pool: &dyn BenchmarkPoolTelemetry,
    sink: &Arc<MemoryInvocationPerformanceSink>,
    concurrency: usize,
    requests: usize,
) -> Result<RoutingCorrectnessReport, Box<dyn Error>> {
    let distributed = shared_benchmark_scope()?.is_some();
    let routes = [
        ("actions.create", "created"),
        ("actions.deleteItem", "deleted"),
        ("actions.inspect", "inspected"),
    ];
    let functions = routes
        .iter()
        .map(|(name, tag)| {
            template
                .manifest
                .functions
                .iter()
                .find(|function| function.name.as_str() == *name)
                .map(|function| (*name, *tag, function.id))
                .ok_or_else(|| format!("routing fixture function {name} missing"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let before_pool = pool.benchmark_pool_telemetry();
    let before_agent = guard.agent.telemetry();
    let before_spans = sink.snapshot().len();
    let sink_dyn: Arc<dyn InvocationPerformanceSink> = sink.clone();
    let started = Instant::now();
    let mut request_ids = BTreeSet::new();
    let mut invocation_ids = BTreeSet::new();
    let mut tasks = JoinSet::new();
    for index in 0..requests {
        let (function_name, result_tag, function_id) = functions[index % functions.len()];
        let token = format!("routing-{index:04}");
        let request = template.request_for(
            function_id,
            CanonicalValue::String(token.clone()),
            PerformanceRuntime::RemoteGateway,
            Some(Arc::clone(&sink_dyn)),
        )?;
        let request_id = request.request_id();
        let invocation_id = request.invocation_id();
        if !request_ids.insert(request_id.to_string())
            || !invocation_ids.insert(invocation_id.to_string())
        {
            return Err("routing fixture generated duplicate correlation identifiers".into());
        }
        mark_load_request(case, "correctness", index, function_name, &request)?;
        let expected = CanonicalValue::String(format!(
            "{result_tag}:{token}:{invocation_id}:{function_name}"
        ));
        let gateway = Arc::clone(&gateway);
        tasks.spawn(async move {
            let result = gateway.execute(request).await.map(|outcome| outcome.value);
            (index, function_name, invocation_id, expected, result)
        });
    }
    let mut mismatches = 0;
    while let Some(joined) = tasks.join_next().await {
        let (index, function_name, invocation_id, expected, result) = joined?;
        match result {
            Ok(actual) if actual == expected => {}
            Ok(actual) => {
                mismatches += 1;
                eprintln!(
                    "routing mismatch index={index} function={function_name} invocation={invocation_id} expected={expected:?} actual={actual:?}"
                );
            }
            Err(error) => {
                mismatches += 1;
                eprintln!(
                    "routing failure index={index} function={function_name} invocation={invocation_id} error={error}"
                );
            }
        }
    }
    let elapsed_micros = micros(started.elapsed());
    if !distributed {
        for _ in 0..100 {
            let telemetry = guard.agent.telemetry();
            if telemetry.completed.saturating_sub(before_agent.completed)
                == u64::try_from(requests)?
                && telemetry.active_executions == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    let after_pool = pool.benchmark_pool_telemetry();
    let after_agent = guard.agent.telemetry();
    let new_spans = sink.snapshot();
    if new_spans[before_spans..]
        .iter()
        .any(|span| span.outcome == PerformanceOutcome::Abandoned)
    {
        return Err("scaled warm routing produced an abandoned span".into());
    }
    for span in &new_spans[before_spans..] {
        println!("RUNKU_PERFORMANCE_SPAN {}", serde_json::to_string(span)?);
    }
    let warm_pool_hits = after_pool.hits.saturating_sub(before_pool.hits);
    let warm_pool_misses = after_pool.misses.saturating_sub(before_pool.misses);
    let completed = after_agent.completed.saturating_sub(before_agent.completed);
    let expected_parallelism = u64::try_from(concurrency.min(requests))?;
    if mismatches != 0
        || (!distributed
            && (completed != u64::try_from(requests)?
                || after_agent.active_executions != 0
                || after_agent.peak_concurrent_executions < expected_parallelism
                || warm_pool_hits != u64::try_from(requests)?
                || warm_pool_misses != 0))
    {
        return Err(format!(
            "scaled warm routing failed: mismatches={mismatches} completed={completed}/{requests} active={} peak={}/{} pool_hit/miss={warm_pool_hits}/{warm_pool_misses}",
            after_agent.active_executions,
            after_agent.peak_concurrent_executions,
            expected_parallelism,
        )
        .into());
    }
    Ok(RoutingCorrectnessReport {
        case: case.to_owned(),
        requests,
        functions: functions
            .iter()
            .map(|(name, _, _)| (*name).to_owned())
            .collect(),
        configured_slots: concurrency,
        peak_agent_concurrency: after_agent.peak_concurrent_executions,
        unique_request_ids: request_ids.len(),
        unique_invocation_ids: invocation_ids.len(),
        warm_pool_hits,
        warm_pool_misses,
        mismatches,
        elapsed_micros,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn verify_open_loop_warm_capacity(
    case: &str,
    template: &RequestTemplate,
    gateway: Arc<dyn FullNodeActionRuntime>,
    guard: &AgentGuard,
    pool: &dyn BenchmarkPoolTelemetry,
    sink: &Arc<MemoryInvocationPerformanceSink>,
    concurrency: usize,
    target_rps: usize,
    duration_secs: usize,
) -> Result<OpenLoopReport, Box<dyn Error>> {
    let distributed = shared_benchmark_scope()?.is_some();
    let routes = [
        ("actions.create", "created"),
        ("actions.deleteItem", "deleted"),
        ("actions.inspect", "inspected"),
    ];
    let functions = routes
        .iter()
        .map(|(name, tag)| {
            template
                .manifest
                .functions
                .iter()
                .find(|function| function.name.as_str() == *name)
                .map(|function| (*name, *tag, function.id))
                .ok_or_else(|| format!("open-loop fixture function {name} missing"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let requests = target_rps.saturating_mul(duration_secs);
    let before_pool = pool.benchmark_pool_telemetry();
    let before_agent = guard.agent.telemetry();
    let before_spans = sink.snapshot().len();
    let sink_dyn: Arc<dyn InvocationPerformanceSink> = sink.clone();
    let origin = tokio::time::Instant::now();
    let mut tasks = JoinSet::new();
    for index in 0..requests {
        let due_nanos = u64::try_from(
            u128::try_from(index)?
                .saturating_mul(1_000_000_000)
                .checked_div(u128::try_from(target_rps)?)
                .unwrap_or_default(),
        )?;
        tokio::time::sleep_until(origin + Duration::from_nanos(due_nanos)).await;
        let (function_name, result_tag, function_id) = functions[index % functions.len()];
        let token = format!("open-loop-{index:05}");
        let request = template.request_for(
            function_id,
            CanonicalValue::String(token.clone()),
            PerformanceRuntime::RemoteGateway,
            Some(Arc::clone(&sink_dyn)),
        )?;
        let invocation_id = request.invocation_id();
        mark_load_request(case, "open_loop", index, function_name, &request)?;
        let expected = CanonicalValue::String(format!(
            "{result_tag}:{token}:{invocation_id}:{function_name}"
        ));
        let gateway = Arc::clone(&gateway);
        tasks.spawn(async move {
            let started = Instant::now();
            let result = gateway.execute(request).await.map(|outcome| outcome.value);
            (expected, result, micros(started.elapsed()))
        });
    }
    let injection_elapsed_micros = micros(origin.elapsed());
    let mut succeeded = 0;
    let mut failed = 0;
    let mut mismatches = 0;
    let mut latencies = Vec::with_capacity(requests);
    while let Some(joined) = tasks.join_next().await {
        let (expected, result, latency) = joined?;
        latencies.push(latency);
        match result {
            Ok(actual) if actual == expected => succeeded += 1,
            Ok(_) => mismatches += 1,
            Err(_) => failed += 1,
        }
    }
    let completion_elapsed_micros = micros(origin.elapsed());
    if !distributed {
        for _ in 0..100 {
            let telemetry = guard.agent.telemetry();
            if telemetry.completed.saturating_sub(before_agent.completed)
                == u64::try_from(requests)?
                && telemetry.active_executions == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    latencies.sort_unstable();
    let after_pool = pool.benchmark_pool_telemetry();
    let after_agent = guard.agent.telemetry();
    let new_spans = sink.snapshot();
    if new_spans[before_spans..]
        .iter()
        .any(|span| span.outcome == PerformanceOutcome::Abandoned)
    {
        return Err("open-loop warm load produced an abandoned span".into());
    }
    for span in &new_spans[before_spans..] {
        println!("RUNKU_PERFORMANCE_SPAN {}", serde_json::to_string(span)?);
    }
    let warm_pool_hits = after_pool.hits.saturating_sub(before_pool.hits);
    let warm_pool_misses = after_pool.misses.saturating_sub(before_pool.misses);
    let completed = after_agent.completed.saturating_sub(before_agent.completed);
    if succeeded != requests
        || failed != 0
        || mismatches != 0
        || (!distributed
            && (completed != u64::try_from(requests)?
                || after_agent.active_executions != 0
                || after_agent.peak_concurrent_executions
                    < u64::try_from(concurrency.min(requests))?
                || warm_pool_hits != u64::try_from(requests)?
                || warm_pool_misses != 0))
    {
        return Err(format!(
            "open-loop warm load failed: success={succeeded}/{requests} failed={failed} mismatches={mismatches} completed={completed} active={} peak={} pool_hit/miss={warm_pool_hits}/{warm_pool_misses}",
            after_agent.active_executions, after_agent.peak_concurrent_executions,
        )
        .into());
    }
    Ok(OpenLoopReport {
        case: case.to_owned(),
        target_requests_per_second: target_rps,
        injection_duration_secs: duration_secs,
        scheduled_requests: requests,
        succeeded,
        failed,
        mismatches,
        configured_slots: concurrency,
        peak_agent_concurrency: after_agent.peak_concurrent_executions,
        warm_pool_hits,
        warm_pool_misses,
        injection_elapsed_micros,
        completion_elapsed_micros,
        completion_throughput_requests_per_second: u64::try_from(requests)?
            .saturating_mul(1_000_000)
            .checked_div(completion_elapsed_micros)
            .unwrap_or_default(),
        latency_p50_micros: percentile(&latencies, 50),
        latency_p95_micros: percentile(&latencies, 95),
        latency_p99_micros: percentile(&latencies, 99),
    })
}

fn firecracker_runtime(
    template: &RequestTemplate,
    image: &str,
) -> Result<Arc<FirecrackerNodeRuntime>, Box<dyn Error>> {
    let endpoints = std::env::var("RUNKU_BENCH_FIRECRACKER_ENDPOINTS")?
        .split(',')
        .map(str::parse)
        .collect::<Result<Vec<_>, _>>()?;
    let token = std::env::var("RUNKU_BENCH_FIRECRACKER_TOKEN")?;
    let controller = std::env::var_os("RUNKU_BENCH_FIRECRACKER_CONTROLLER")
        .map(PathBuf::from)
        .ok_or("RUNKU_BENCH_FIRECRACKER_CONTROLLER is required")?;
    let mut config = FirecrackerNodeRuntimeConfig::new(
        endpoints,
        image,
        FullNodeEgressPolicy::none(),
        token,
        controller,
    )?;
    config.connect_timeout = template.wall_timeout.min(Duration::from_secs(30));
    Ok(Arc::new(FirecrackerNodeRuntime::new(config)?))
}

fn firecracker_report_telemetry(runtime: &FirecrackerNodeRuntime) -> WarmPoolReport {
    let snapshot = runtime.telemetry();
    WarmPoolReport {
        hits: snapshot.hits,
        reconnects: snapshot.reconnects,
        failed: snapshot.failed,
        replacements: snapshot.replacements,
        replacement_failures: snapshot.replacement_failures,
        workers: snapshot.workers,
        idle: snapshot.idle,
    }
}

fn resource_cpu_samples(spans: &[InvocationPerformanceSpanV1]) -> Vec<u64> {
    spans
        .iter()
        .filter(|span| span.operation == PerformanceOperation::ExecuteRunner)
        .filter_map(|span| span.resources)
        .filter_map(|resources| {
            resources.cpu_total_micros.or_else(|| {
                match (resources.user_cpu_micros, resources.system_cpu_micros) {
                    (None, None) => None,
                    (user, system) => Some(user.unwrap_or_default() + system.unwrap_or_default()),
                }
            })
        })
        .collect()
}

fn safe_fixture(wall_timeout: Duration) -> Result<RequestTemplate, Box<dyn Error>> {
    let bundle = SafeEsmBundleV1::from_sources([SAFE_SOURCE])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let project_id = ProjectId::generate();
    let release_id = ReleaseId::generate();
    let function_id = FunctionId::generate();
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id,
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_800_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "benchmark.echo".parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Internal,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(SAFE_SOURCE.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
            result_contract_hash: Sha256Digest::from_bytes([5; 32]),
            capabilities: Vec::<Capability>::new(),
        }],
        cron_definitions: Vec::new(),
    };
    Ok(RequestTemplate {
        scope: EnvironmentScope::new(project_id, EnvironmentId::generate()),
        release_id,
        function_id,
        manifest: Arc::new(manifest),
        artifact,
        wall_timeout,
    })
}

fn node_fixture(
    concurrency: usize,
    modes: &BTreeSet<String>,
    wall_timeout: Duration,
) -> Result<NodeFixture, Box<dyn Error>> {
    let directory = tempdir()?;
    let source = directory.path().join("runku");
    let cache_root = directory.path().join("cache");
    let scratch = directory.path().join("scratch");
    std::fs::create_dir(&source)?;
    std::fs::create_dir(&cache_root)?;
    std::fs::create_dir(&scratch)?;
    std::fs::write(
        source.join("schema.ts"),
        "import { defineSchema } from '@runku/server'; export default defineSchema({});",
    )?;
    std::fs::write(source.join("actions.ts"), NODE_SOURCE)?;
    let distributed = distributed_fixture_ids()?;
    let project_id = distributed.map_or_else(ProjectId::generate, |ids| ids.project);
    let release_id = distributed.map_or_else(ReleaseId::generate, |ids| ids.release);
    let build = build_project(
        directory.path(),
        Path::new("runku"),
        project_id,
        BuildMetadata {
            release_id,
            build_id: distributed.map_or_else(BuildId::generate, |ids| ids.build),
            created_at: TimestampMicros::new(1_800_000_000_000_000),
        },
    )?;
    let manifest = decode_release_manifest(&std::fs::read(build.manifest_path)?)?;
    let artifact: Arc<[u8]> = std::fs::read(build.artifact_path)?.into();
    let function_id = manifest
        .functions
        .iter()
        .find(|function| function.name.as_str() == "actions.echo")
        .ok_or("echo function missing")?
        .id;
    let scope = EnvironmentScope::new(
        project_id,
        distributed.map_or_else(EnvironmentId::generate, |ids| ids.environment),
    );
    let local = RequestTemplate {
        scope,
        release_id,
        function_id,
        manifest: Arc::new(manifest.clone()),
        artifact: Arc::clone(&artifact),
        wall_timeout,
    };
    let local_runtime = modes
        .contains("node_local")
        .then(|| {
            LocalNodeRuntimeConfig::new(directory.path(), concurrency)
                .and_then(LocalNodeRuntime::new)
                .map(Arc::new)
        })
        .transpose()?;
    let host_descriptor = NodeOciDescriptorV1::new(format!("sha256:{}", "a".repeat(64)))?
        .with_egress_policy(FullNodeEgressPolicy::none());
    let cache = HostNodeArtifactCache::open(cache_root)?;
    cache.materialize(&manifest, &artifact, &host_descriptor, None)?;
    let host_artifact: Arc<[u8]> = encode_node_oci_descriptor(&host_descriptor)?.into();
    let mut host_manifest = manifest;
    host_manifest.artifact = host_descriptor.descriptor()?;
    let host = RequestTemplate {
        scope,
        release_id,
        function_id,
        manifest: Arc::new(host_manifest),
        artifact: host_artifact,
        wall_timeout,
    };
    let host_runtime = (modes.contains("node_host") || modes.contains("remote_host"))
        .then(|| {
            let policy = DedicatedHostPolicy::new(
                8_000,
                2 * 1024 * 1024 * 1024,
                512,
                FullNodeEgressPolicy::none(),
            )?;
            HostNodeRuntimeConfig::dedicated(cache.clone(), scratch, policy, concurrency)
                .map(|config| config.with_queue_timeout(wall_timeout))
                .and_then(HostNodeRuntime::new)
                .map(Arc::new)
        })
        .transpose()?;
    Ok(NodeFixture {
        _directory: directory,
        local,
        local_runtime,
        host_runtime,
        host,
        cache,
    })
}

async fn remote_runtime(
    template: &RequestTemplate,
    runtime: Arc<dyn FullNodeActionRuntime>,
    runtime_kind: PerformanceRuntime,
    concurrency: usize,
) -> Result<
    (
        Arc<dyn FullNodeActionRuntime>,
        AgentGuard,
        Arc<MemoryInvocationPerformanceSink>,
    ),
    Box<dyn Error>,
> {
    let (queue, control, class) = remote_boundaries().await?;
    let artifacts = s3_store()?;
    artifacts
        .put(&template.manifest.artifact, &template.artifact)
        .await?;
    let releases = Arc::new(StaticReleases {
        scope: template.scope,
        manifest: (*template.manifest).clone(),
    });
    let sink = Arc::new(MemoryInvocationPerformanceSink::new(100_000)?);
    let sink_dyn: Arc<dyn InvocationPerformanceSink> = sink.clone();
    let handler = Arc::new(
        FullNodeExecutionHandler::new(releases, artifacts, runtime, Arc::clone(&control))
            .with_performance_sink(runtime_kind, Arc::clone(&sink_dyn)),
    );
    let agent = Arc::new(
        ExecutionAgent::new(
            Arc::clone(&queue),
            handler,
            ExecutionAgentConfig {
                class: class.clone(),
                slots: concurrency,
                max_concurrent_per_project: concurrency,
                pull_wait: Duration::from_millis(100),
            },
        )?
        .with_performance_sink(sink_dyn),
    );
    let gateway: Arc<dyn FullNodeActionRuntime> = Arc::new(QueuedNodeRuntime::new(
        queue,
        control,
        QueuedNodeRuntimeConfig {
            class,
            result_wait: Duration::from_millis(100),
        },
    )?);
    let (shutdown, receiver) = watch::channel(false);
    tokio::spawn(Arc::clone(&agent).run(receiver));
    Ok((gateway, AgentGuard { shutdown, agent }, sink))
}

async fn remote_boundaries() -> Result<
    (
        Arc<dyn ExecutionQueue>,
        Arc<dyn ExecutionControlPlane>,
        ExecutionClass,
    ),
    Box<dyn Error>,
> {
    let client = async_nats::connect(std::env::var("RUNKU_TEST_NATS_URL")?).await?;
    let suffix = shared_benchmark_scope()?.unwrap_or_else(|| {
        InvocationId::generate()
            .to_string()
            .replace('-', "_")
            .to_ascii_uppercase()
    });
    let class = ExecutionClass::new("node_benchmark_v1")?;
    let queue: Arc<dyn ExecutionQueue> = Arc::new(
        NatsExecutionQueue::open(
            client.clone(),
            NatsExecutionQueueConfig {
                stream_name: format!("RUNKU_BENCH_{suffix}"),
                subject_prefix: format!("runku.bench.{}", suffix.to_ascii_lowercase()),
                max_messages: 20_000,
                max_bytes: 64 * 1024 * 1024,
                max_age: Duration::from_secs(300),
                replicas: 1,
                ack_wait: Duration::from_secs(30),
                max_deliver: 3,
                max_waiting: 256,
            },
        )
        .await?,
    );
    let control: Arc<dyn ExecutionControlPlane> = Arc::new(
        NatsExecutionControlPlane::open(
            client,
            NatsExecutionControlConfig {
                bucket: format!("RUNKU_BENCH_STATE_{suffix}"),
                max_bytes: 64 * 1024 * 1024,
                max_age: Duration::from_secs(300),
                replicas: 1,
            },
        )
        .await?,
    );
    Ok((queue, control, class))
}

fn s3_store() -> Result<Arc<dyn ArtifactStore>, Box<dyn Error>> {
    let mut config =
        S3ArtifactStoreConfig::new(std::env::var("RUNKU_TEST_S3_BUCKET")?, "us-east-1");
    config.endpoint = Some(std::env::var("RUNKU_TEST_S3_ENDPOINT")?);
    config.prefix = shared_benchmark_scope()?.map_or_else(
        || format!("performance/{}", InvocationId::generate()),
        |scope| format!("performance/{}", scope.to_ascii_lowercase()),
    );
    config.allow_http = true;
    config.operation_timeout = Duration::from_secs(10);
    config.credentials = S3Credentials::Static(S3StaticCredentials::new(
        std::env::var("RUNKU_TEST_S3_ACCESS_KEY")?,
        std::env::var("RUNKU_TEST_S3_SECRET_KEY")?,
    ));
    Ok(Arc::new(S3ArtifactStore::open(&config)?))
}

#[derive(Debug)]
struct StaticReleases {
    scope: EnvironmentScope,
    manifest: ReleaseManifestV1,
}

#[async_trait]
impl ReleaseRepository for StaticReleases {
    fn backend(&self) -> ReleaseRepositoryBackend {
        ReleaseRepositoryBackend::SQLite
    }

    async fn apply(
        &self,
        _scope: EnvironmentScope,
        _operation_id: OperationId,
        _command: &ReleaseCommand,
    ) -> Result<ReleaseCommandResult, ReleaseError> {
        Err(ReleaseError::Unsupported)
    }

    async fn snapshot(&self, _scope: EnvironmentScope) -> Result<ServingSnapshot, ReleaseError> {
        Err(ReleaseError::Unsupported)
    }

    async fn manifest(
        &self,
        scope: EnvironmentScope,
        release_id: ReleaseId,
    ) -> Result<ReleaseManifestV1, ReleaseError> {
        if scope == self.scope && release_id == self.manifest.release_id {
            Ok(self.manifest.clone())
        } else {
            Err(ReleaseError::ReleaseNotFound)
        }
    }

    async fn health(&self) -> Result<(), ReleaseError> {
        Ok(())
    }

    fn telemetry(&self) -> ReleaseRepositoryTelemetrySnapshot {
        ReleaseRepositoryTelemetrySnapshot::default()
    }
}

fn modes() -> BTreeSet<String> {
    std::env::var("RUNKU_BENCH_MODES")
        .unwrap_or_else(|_| "safe_v8,node_local,node_host".to_owned())
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

#[derive(Clone, Copy)]
struct DistributedFixtureIds {
    project: ProjectId,
    environment: EnvironmentId,
    release: ReleaseId,
    build: BuildId,
}

fn distributed_fixture_ids() -> Result<Option<DistributedFixtureIds>, Box<dyn Error>> {
    if shared_benchmark_scope()?.is_none() {
        return Ok(None);
    }
    Ok(Some(DistributedFixtureIds {
        project: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?,
        environment: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?,
        release: "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?,
        build: "bld_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?,
    }))
}

fn shared_benchmark_scope() -> Result<Option<String>, Box<dyn Error>> {
    let Some(value) = std::env::var_os("RUNKU_BENCH_SHARED_SCOPE") else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| "RUNKU_BENCH_SHARED_SCOPE is not UTF-8")?
        .to_ascii_uppercase();
    if value.is_empty()
        || value.len() > 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err("RUNKU_BENCH_SHARED_SCOPE is invalid".into());
    }
    Ok(Some(value))
}

fn env_usize(
    name: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, Box<dyn Error>> {
    let value = std::env::var(name).map_or(Ok(default), |value| value.parse::<usize>())?;
    if !(minimum..=maximum).contains(&value) {
        return Err(format!("{name} outside {minimum}..={maximum}").into());
    }
    Ok(value)
}

fn verify_output(value: &CanonicalValue) -> Result<(), Box<dyn Error>> {
    if value == &CanonicalValue::String(INPUT.to_owned()) {
        Ok(())
    } else {
        Err("benchmark output mismatch".into())
    }
}

fn mark_request(
    case: &str,
    phase: &str,
    request: &InvocationRequest,
) -> Result<(), Box<dyn Error>> {
    println!(
        "RUNKU_BENCHMARK_INVOCATION {}",
        serde_json::to_string(&serde_json::json!({
            "case": case,
            "phase": phase,
            "invocation_id": request.invocation_id(),
        }))?
    );
    Ok(())
}

fn mark_load_request(
    case: &str,
    phase: &str,
    sequence: usize,
    function: &str,
    request: &InvocationRequest,
) -> Result<(), Box<dyn Error>> {
    println!(
        "RUNKU_BENCHMARK_INVOCATION {}",
        serde_json::to_string(&serde_json::json!({
            "case": case,
            "phase": phase,
            "sequence": sequence,
            "request_id": request.request_id(),
            "invocation_id": request.invocation_id(),
            "function_id": request.function_id(),
            "function": function,
        }))?
    );
    Ok(())
}

fn encoded_len(value: &CanonicalValue) -> Result<u64, Box<dyn Error>> {
    Ok(u64::try_from(encode_stored_value(value)?.len())?)
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}
