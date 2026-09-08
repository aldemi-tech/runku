//! Gateway-style queued Full Node execution through a real agent handler.

use std::{error::Error, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use runku_build::{BuildMetadata, build_project};
use runku_core::{
    BuildId, EnvironmentId, EnvironmentScope, InvocationId, OperationId, ProjectId, ReleaseId,
    RequestId,
};
use runku_execution_queue::{
    ExecutionAgent, ExecutionAgentConfig, ExecutionClass, ExecutionControlPlane, ExecutionQueue,
    InMemoryExecutionControlPlane, InMemoryExecutionQueue, NatsExecutionControlConfig,
    NatsExecutionControlPlane, NatsExecutionQueue, NatsExecutionQueueConfig,
};
use runku_node_runtime::{
    DedicatedHostPolicy, FullNodeActionRuntime, FullNodeExecutionHandler,
    FullNodeExecutionResources, FullNodeFilesystemResources, HostNodeArtifactCache,
    HostNodeRuntime, HostNodeRuntimeConfig, QueuedNodeRuntime, QueuedNodeRuntimeConfig,
};
use runku_observability::{
    InvocationPerformanceSink, MemoryInvocationPerformanceSink, PerformanceComponent,
    PerformanceOutcome, PerformanceRuntime,
};
use runku_releases::{
    ArtifactDescriptor, ArtifactStore, FullNodeEgressPolicy, NodeOciDescriptorV1, ReleaseCommand,
    ReleaseCommandResult, ReleaseError, ReleaseManifestV1, ReleaseRepository,
    ReleaseRepositoryBackend, ReleaseRepositoryTelemetrySnapshot, ServingSnapshot,
    decode_release_manifest, encode_node_oci_descriptor, encode_release_manifest,
};
use runku_runtime::{
    CancellationToken, ConfigurationRead, ConfigurationReadError, ConfigurationValueKind,
    InvocationRequest, RuntimeError,
};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::tempdir;
use tokio::{sync::watch, task::JoinSet};
use zeroize::Zeroizing;

const SOURCE: &str = r#"
"use runku node"
import { action, v } from "@runku/server"
import { createHash } from "node:crypto"
export const hash = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.string(),
  handler(_ctx, input) { return createHash("sha256").update(input).digest("hex") },
})
export const create = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.string(),
  async handler(ctx, input) {
    await new Promise(resolve => setTimeout(resolve, 5 + input.charCodeAt(input.length - 1) % 7))
    return `created:${input}:${ctx.invocation.invocationId}:${ctx.invocation.function}`
  },
})
export const deleteItem = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.string(),
  async handler(ctx, input) {
    await new Promise(resolve => setTimeout(resolve, 5 + input.charCodeAt(input.length - 1) % 5))
    return `deleted:${input}:${ctx.invocation.invocationId}:${ctx.invocation.function}`
  },
})
export const inspect = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.string(),
  async handler(ctx, input) {
    await new Promise(resolve => setTimeout(resolve, 5 + input.charCodeAt(input.length - 1) % 3))
    return `inspected:${input}:${ctx.invocation.invocationId}:${ctx.invocation.function}`
  },
})
"#;

const CONFIGURED_ACTION: &str = r#"
export const configured = action({
  auth: "none", visibility: "public", capabilities: ["secret:API_KEY", "variable:REGION"],
  args: v.null(), returns: v.string(),
  async handler(ctx) {
    const region = await ctx.env.get("REGION")
    const secret = await ctx.secrets.get("API_KEY")
    return `${region}:${secret.length}:${Object.isFrozen(ctx.env)}:${Object.isFrozen(ctx.secrets)}`
  },
})
"#;

const LOOP_ACTION: &str = r#"
export const loop = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.null(), returns: v.null(), handler() { for (;;) {} },
})
"#;

struct Fixture {
    _directory: tempfile::TempDir,
    scope: EnvironmentScope,
    manifest: Arc<ReleaseManifestV1>,
    descriptor: Arc<[u8]>,
    runtime: Arc<HostNodeRuntime>,
}

impl Fixture {
    fn request(
        &self,
        function_name: &str,
        arguments: CanonicalValue,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<InvocationRequest, RuntimeError> {
        let function = self
            .manifest
            .functions
            .iter()
            .find(|function| function.name.as_str() == function_name)
            .ok_or(RuntimeError::FunctionNotFound)?;
        InvocationRequest::new(
            self.scope,
            self.manifest.release_id,
            RequestId::generate(),
            InvocationId::generate(),
            function.id,
            Arc::clone(&self.manifest),
            Arc::clone(&self.descriptor),
            arguments,
            timeout,
            cancellation,
        )
    }
}

fn fixture() -> Result<Fixture, Box<dyn Error>> {
    fixture_with_oci_descriptor(true, true)
}

fn direct_fixture() -> Result<Fixture, Box<dyn Error>> {
    fixture_with_oci_descriptor(false, false)
}

fn fixture_with_oci_descriptor(
    oci_descriptor: bool,
    configuration_capability: bool,
) -> Result<Fixture, Box<dyn Error>> {
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
    let mut source_code = SOURCE.to_owned();
    if configuration_capability {
        source_code.push_str(CONFIGURED_ACTION);
    }
    source_code.push_str(LOOP_ACTION);
    std::fs::write(source.join("actions.ts"), source_code)?;
    let project_id = ProjectId::generate();
    let release_id = ReleaseId::generate();
    let output = build_project(
        directory.path(),
        Path::new("runku"),
        project_id,
        BuildMetadata {
            release_id,
            build_id: BuildId::generate(),
            created_at: TimestampMicros::new(1_800_000_000_000_000),
        },
    )?;
    let mut manifest = decode_release_manifest(&std::fs::read(output.manifest_path)?)?;
    let bundle = std::fs::read(output.artifact_path)?;
    let cache = HostNodeArtifactCache::open(cache_root)?;
    let descriptor = if oci_descriptor {
        let target = NodeOciDescriptorV1::new(format!("sha256:{}", "a".repeat(64)))?;
        cache.materialize(&manifest, &bundle, &target, None)?;
        let descriptor = encode_node_oci_descriptor(&target)?;
        manifest.artifact = target.descriptor()?;
        descriptor
    } else {
        bundle
    };
    let runtime = HostNodeRuntime::new(HostNodeRuntimeConfig::dedicated(
        cache,
        scratch,
        DedicatedHostPolicy::new(2_000, 512 * 1024 * 1024, 64, FullNodeEgressPolicy::none())?,
        4,
    )?)?;
    Ok(Fixture {
        _directory: directory,
        scope: EnvironmentScope::new(project_id, EnvironmentId::generate()),
        manifest: Arc::new(manifest),
        descriptor: descriptor.into(),
        runtime: Arc::new(runtime),
    })
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

#[derive(Debug)]
struct StaticArtifact {
    descriptor: ArtifactDescriptor,
    bytes: Vec<u8>,
}

#[async_trait]
impl ArtifactStore for StaticArtifact {
    async fn put(
        &self,
        _descriptor: &ArtifactDescriptor,
        _bytes: &[u8],
    ) -> Result<(), ReleaseError> {
        Err(ReleaseError::Unsupported)
    }

    async fn get(&self, descriptor: &ArtifactDescriptor) -> Result<Vec<u8>, ReleaseError> {
        if descriptor == &self.descriptor {
            Ok(self.bytes.clone())
        } else {
            Err(ReleaseError::NotFound)
        }
    }
}

struct Vertical {
    _resources: Option<tempfile::TempDir>,
    runtime: Arc<QueuedNodeRuntime>,
    agent: Arc<ExecutionAgent>,
    performance: Arc<MemoryInvocationPerformanceSink>,
    shutdown: watch::Sender<bool>,
}

#[derive(Debug)]
struct TestConfiguration;

#[async_trait]
impl ConfigurationRead for TestConfiguration {
    async fn read(
        &self,
        kind: ConfigurationValueKind,
        name: &str,
        _deadline: std::time::Instant,
        _cancellation: CancellationToken,
    ) -> Result<Zeroizing<String>, ConfigurationReadError> {
        match (kind, name) {
            (ConfigurationValueKind::Variable, "REGION") => {
                Ok(Zeroizing::new("south-1".to_owned()))
            }
            (ConfigurationValueKind::Secret, "API_KEY") => {
                Ok(Zeroizing::new("never-log-this-secret".to_owned()))
            }
            _ => Err(ConfigurationReadError::NotFound),
        }
    }
}

async fn wait_for_agent_settled(
    agent: &ExecutionAgent,
    expected_completed: u64,
) -> Result<runku_execution_queue::ExecutionAgentTelemetrySnapshot, tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let telemetry = agent.telemetry();
            if telemetry.completed == expected_completed && telemetry.active_executions == 0 {
                return telemetry;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}

fn vertical(fixture: &Fixture) -> Result<Vertical, Box<dyn Error>> {
    let queue = Arc::new(InMemoryExecutionQueue::new(32)?);
    let control: Arc<dyn ExecutionControlPlane> =
        Arc::new(InMemoryExecutionControlPlane::default());
    let releases = Arc::new(StaticReleases {
        scope: fixture.scope,
        manifest: (*fixture.manifest).clone(),
    });
    let artifacts = Arc::new(StaticArtifact {
        descriptor: fixture.manifest.artifact,
        bytes: fixture.descriptor.to_vec(),
    });
    let node: Arc<dyn FullNodeActionRuntime> = fixture.runtime.clone();
    let performance = Arc::new(MemoryInvocationPerformanceSink::new(256)?);
    let performance_sink: Arc<dyn InvocationPerformanceSink> = performance.clone();
    let handler = Arc::new(
        FullNodeExecutionHandler::new(releases, artifacts, node, Arc::clone(&control))
            .with_configuration(fixture.scope, Arc::new(TestConfiguration))
            .with_performance_sink(PerformanceRuntime::NodeHost, Arc::clone(&performance_sink)),
    );
    let class = ExecutionClass::new("node_host_v1")?;
    let agent = Arc::new(
        ExecutionAgent::new(
            queue.clone(),
            handler,
            ExecutionAgentConfig {
                class: class.clone(),
                slots: 4,
                max_concurrent_per_project: 4,
                pull_wait: Duration::from_millis(50),
            },
        )?
        .with_performance_sink(performance_sink),
    );
    let runtime = Arc::new(QueuedNodeRuntime::new(
        queue,
        control,
        QueuedNodeRuntimeConfig {
            class,
            result_wait: Duration::from_millis(50),
            configuration_available: true,
        },
    )?);
    let (shutdown, receiver) = watch::channel(false);
    tokio::spawn(Arc::clone(&agent).run(receiver));
    Ok(Vertical {
        _resources: None,
        runtime,
        agent,
        performance,
        shutdown,
    })
}

async fn nats_vertical(fixture: &Fixture, url: &str) -> Result<Vertical, Box<dyn Error>> {
    let resources_directory = tempdir()?;
    let writer = FullNodeFilesystemResources::open_writer(resources_directory.path()).await?;
    writer
        .stage(
            fixture.scope,
            &encode_release_manifest(&fixture.manifest)?,
            &fixture.descriptor,
        )
        .await?;
    writer
        .stage(
            fixture.scope,
            &encode_release_manifest(&fixture.manifest)?,
            &fixture.descriptor,
        )
        .await?;
    let resources: Arc<dyn FullNodeExecutionResources> =
        Arc::new(FullNodeFilesystemResources::open_reader(resources_directory.path()).await?);
    let client = async_nats::connect(url).await?;
    let suffix = InvocationId::generate()
        .to_string()
        .replace('-', "_")
        .to_ascii_uppercase();
    let queue_config = NatsExecutionQueueConfig {
        stream_name: format!("RUNKU_VERTICAL_{suffix}"),
        subject_prefix: format!("runku.vertical.{}", suffix.to_ascii_lowercase()),
        max_messages: 32,
        max_bytes: 4 * 1024 * 1024,
        max_age: Duration::from_secs(60),
        replicas: 1,
        ack_wait: Duration::from_secs(10),
        max_deliver: 3,
        max_waiting: 16,
    };
    let control_config = NatsExecutionControlConfig {
        bucket: format!("RUNKU_VERTICAL_STATE_{suffix}"),
        max_bytes: 4 * 1024 * 1024,
        max_age: Duration::from_secs(60),
        replicas: 1,
    };
    let queue: Arc<dyn ExecutionQueue> =
        Arc::new(NatsExecutionQueue::open(client.clone(), queue_config).await?);
    let control: Arc<dyn ExecutionControlPlane> =
        Arc::new(NatsExecutionControlPlane::open(client, control_config).await?);
    let node: Arc<dyn FullNodeActionRuntime> = fixture.runtime.clone();
    let performance = Arc::new(MemoryInvocationPerformanceSink::new(256)?);
    let performance_sink: Arc<dyn InvocationPerformanceSink> = performance.clone();
    let handler = Arc::new(
        FullNodeExecutionHandler::from_resources(resources, node, Arc::clone(&control))
            .with_configuration(fixture.scope, Arc::new(TestConfiguration))
            .with_performance_sink(PerformanceRuntime::NodeHost, Arc::clone(&performance_sink)),
    );
    let class = ExecutionClass::new("node_host_v1")?;
    let agent = Arc::new(
        ExecutionAgent::new(
            Arc::clone(&queue),
            handler,
            ExecutionAgentConfig {
                class: class.clone(),
                slots: 4,
                max_concurrent_per_project: 4,
                pull_wait: Duration::from_millis(50),
            },
        )?
        .with_performance_sink(performance_sink),
    );
    let runtime = Arc::new(QueuedNodeRuntime::new(
        queue,
        control,
        QueuedNodeRuntimeConfig {
            class,
            result_wait: Duration::from_millis(50),
            configuration_available: true,
        },
    )?);
    let (shutdown, receiver) = watch::channel(false);
    tokio::spawn(Arc::clone(&agent).run(receiver));
    Ok(Vertical {
        _resources: Some(resources_directory),
        runtime,
        agent,
        performance,
        shutdown,
    })
}

async fn filesystem_vertical(fixture: &Fixture) -> Result<Vertical, Box<dyn Error>> {
    let resources_directory = tempdir()?;
    let writer = FullNodeFilesystemResources::open_writer(resources_directory.path()).await?;
    writer
        .stage(
            fixture.scope,
            &encode_release_manifest(&fixture.manifest)?,
            &fixture.descriptor,
        )
        .await?;
    let resources: Arc<dyn FullNodeExecutionResources> =
        Arc::new(FullNodeFilesystemResources::open_reader(resources_directory.path()).await?);
    let queue = Arc::new(InMemoryExecutionQueue::new(32)?);
    let control: Arc<dyn ExecutionControlPlane> =
        Arc::new(InMemoryExecutionControlPlane::default());
    let node: Arc<dyn FullNodeActionRuntime> = fixture.runtime.clone();
    let handler = Arc::new(FullNodeExecutionHandler::from_resources(
        resources,
        node,
        Arc::clone(&control),
    ));
    let class = ExecutionClass::new("node_host_v1")?;
    let agent = Arc::new(ExecutionAgent::new(
        queue.clone(),
        handler,
        ExecutionAgentConfig {
            class: class.clone(),
            slots: 2,
            max_concurrent_per_project: 2,
            pull_wait: Duration::from_millis(50),
        },
    )?);
    let runtime = Arc::new(QueuedNodeRuntime::new(
        queue,
        control,
        QueuedNodeRuntimeConfig {
            class,
            result_wait: Duration::from_millis(50),
            configuration_available: false,
        },
    )?);
    let performance = Arc::new(MemoryInvocationPerformanceSink::new(64)?);
    let (shutdown, receiver) = watch::channel(false);
    tokio::spawn(Arc::clone(&agent).run(receiver));
    Ok(Vertical {
        _resources: Some(resources_directory),
        runtime,
        agent,
        performance,
        shutdown,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sanitized_filesystem_projection_executes_without_product_state()
-> Result<(), Box<dyn Error>> {
    let fixture = direct_fixture()?;
    let vertical = filesystem_vertical(&fixture).await?;
    let outcome = vertical
        .runtime
        .execute(fixture.request(
            "actions.hash",
            CanonicalValue::String("isolated".to_owned()),
            Duration::from_secs(3),
            CancellationToken::new(),
        )?)
        .await?;
    assert_eq!(
        outcome.value,
        CanonicalValue::String(
            "afed2f77cefe58d821f08e334aeb42e52facc63c4f97e9d5d18f53fd0e285953".to_owned()
        )
    );
    assert_eq!(
        wait_for_agent_settled(&vertical.agent, 1).await?.completed,
        1
    );
    vertical.shutdown.send_replace(true);
    Ok(())
}

#[tokio::test]
async fn filesystem_projection_is_scope_bound_and_detects_modified_artifacts()
-> Result<(), Box<dyn Error>> {
    let fixture = fixture()?;
    let directory = tempdir()?;
    let writer = FullNodeFilesystemResources::open_writer(directory.path()).await?;
    writer
        .stage(
            fixture.scope,
            &encode_release_manifest(&fixture.manifest)?,
            &fixture.descriptor,
        )
        .await?;
    let reader = FullNodeFilesystemResources::open_reader(directory.path()).await?;
    let package = reader
        .load(fixture.scope, fixture.manifest.release_id)
        .await?;
    assert_eq!(package.manifest, *fixture.manifest);
    assert_eq!(package.artifact, fixture.descriptor.as_ref());
    let other_scope = EnvironmentScope::new(fixture.scope.project_id(), EnvironmentId::generate());
    assert!(
        reader
            .load(other_scope, fixture.manifest.release_id)
            .await
            .is_err()
    );
    let digest = fixture.manifest.artifact.digest.to_string();
    let artifact_path = directory
        .path()
        .join("full-node-execution-v1/artifacts")
        .join(&digest[..2])
        .join(&digest[2..4])
        .join(format!("{}.artifact", &digest[4..]));
    std::fs::write(artifact_path, b"modified")?;
    assert!(
        reader
            .load(fixture.scope, fixture.manifest.release_id)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_gateway_agent_executes_real_node_and_returns_durable_result()
-> Result<(), Box<dyn Error>> {
    let fixture = fixture()?;
    let vertical = vertical(&fixture)?;
    let performance_sink: Arc<dyn InvocationPerformanceSink> = vertical.performance.clone();
    let request = fixture
        .request(
            "actions.hash",
            CanonicalValue::String("runku".to_owned()),
            Duration::from_secs(3),
            CancellationToken::new(),
        )?
        .with_performance_sink(PerformanceRuntime::RemoteGateway, performance_sink);
    let outcome = vertical.runtime.execute(request).await?;
    assert_eq!(
        outcome.value,
        CanonicalValue::String(
            "ae57fe8872aa84461538f8ed7c54dd3eb8f7bdd2398744aef598287746259bc9".to_owned()
        )
    );
    let telemetry = wait_for_agent_settled(&vertical.agent, 1).await?;
    assert_eq!(telemetry.completed, 1);
    let expected_components = [
        PerformanceComponent::Gateway,
        PerformanceComponent::Queue,
        PerformanceComponent::Agent,
        PerformanceComponent::ReleaseRepository,
        PerformanceComponent::ArtifactStore,
        PerformanceComponent::NodeProcess,
    ];
    let spans = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let spans = vertical.performance.snapshot();
            if expected_components
                .iter()
                .all(|component| spans.iter().any(|span| span.component == *component))
            {
                return spans;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        spans
            .iter()
            .all(|span| span.outcome != PerformanceOutcome::Abandoned)
    );
    vertical.shutdown.send_replace(true);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_agent_resolves_only_declared_environment_configuration()
-> Result<(), Box<dyn Error>> {
    let fixture = fixture()?;
    assert_eq!(fixture.manifest.runtime_version.as_str(), "runku-node");
    let vertical = vertical(&fixture)?;
    let outcome = vertical
        .runtime
        .execute(fixture.request(
            "actions.configured",
            CanonicalValue::Null,
            Duration::from_secs(3),
            CancellationToken::new(),
        )?)
        .await?;
    assert_eq!(
        outcome.value,
        CanonicalValue::String("south-1:21:true:true".to_owned())
    );
    assert!(!format!("{:?}", vertical.performance.snapshot()).contains("never-log-this-secret"));
    vertical.shutdown.send_replace(true);
    Ok(())
}

#[test]
fn unbrokered_queue_rejects_node_configuration_capabilities_at_admission()
-> Result<(), Box<dyn Error>> {
    let fixture = fixture()?;
    let queue = Arc::new(InMemoryExecutionQueue::new(8)?);
    let control: Arc<dyn ExecutionControlPlane> =
        Arc::new(InMemoryExecutionControlPlane::default());
    let runtime = QueuedNodeRuntime::new(
        queue,
        control,
        QueuedNodeRuntimeConfig {
            class: ExecutionClass::new("node_host_v1")?,
            result_wait: Duration::from_millis(50),
            configuration_available: false,
        },
    )?;
    assert_eq!(
        runtime.validate_manifest(&fixture.manifest),
        Err(RuntimeError::UnsupportedRuntime)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queued_gateway_never_crosses_heterogeneous_results_under_load()
-> Result<(), Box<dyn Error>> {
    let fixture = fixture()?;
    let vertical = vertical(&fixture)?;
    let routes = [
        ("actions.create", "created"),
        ("actions.deleteItem", "deleted"),
        ("actions.inspect", "inspected"),
    ];
    let mut tasks = JoinSet::new();
    for index in 0..24 {
        let (function, tag) = routes[index % routes.len()];
        let token = format!("correlation-{index:02}");
        let request = fixture.request(
            function,
            CanonicalValue::String(token.clone()),
            Duration::from_secs(5),
            CancellationToken::new(),
        )?;
        let invocation_id = request.invocation_id();
        let expected = CanonicalValue::String(format!("{tag}:{token}:{invocation_id}:{function}"));
        let runtime = vertical.runtime.clone();
        tasks.spawn(async move { (expected, runtime.execute(request).await) });
    }
    while let Some(joined) = tasks.join_next().await {
        let (expected, result) = joined?;
        assert_eq!(result?.value, expected);
    }
    let telemetry = wait_for_agent_settled(&vertical.agent, 24).await?;
    assert_eq!(telemetry.completed, 24);
    assert_eq!(telemetry.active_executions, 0);
    assert_eq!(telemetry.peak_concurrent_executions, 4);
    vertical.shutdown.send_replace(true);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_cancellation_reaches_the_active_node_process() -> Result<(), Box<dyn Error>> {
    let fixture = fixture()?;
    let vertical = vertical(&fixture)?;
    let cancellation = CancellationToken::new();
    let request = fixture.request(
        "actions.loop",
        CanonicalValue::Null,
        Duration::from_secs(5),
        cancellation.clone(),
    )?;
    let task = tokio::spawn(async move { vertical.runtime.execute(request).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    cancellation.cancel();
    assert_eq!(task.await?, Err(RuntimeError::Cancelled));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nats_gateway_agent_result_and_cancellation_vertical() -> Result<(), Box<dyn Error>> {
    let Ok(url) = std::env::var("RUNKU_TEST_NATS_URL") else {
        eprintln!("skipping NATS vertical: RUNKU_TEST_NATS_URL is unset");
        return Ok(());
    };
    let fixture = fixture()?;
    let vertical = nats_vertical(&fixture, &url).await?;
    let outcome = vertical
        .runtime
        .execute(fixture.request(
            "actions.hash",
            CanonicalValue::String("runku".to_owned()),
            Duration::from_secs(3),
            CancellationToken::new(),
        )?)
        .await?;
    assert_eq!(
        outcome.value,
        CanonicalValue::String(
            "ae57fe8872aa84461538f8ed7c54dd3eb8f7bdd2398744aef598287746259bc9".to_owned()
        )
    );
    let cancellation = CancellationToken::new();
    let request = fixture.request(
        "actions.loop",
        CanonicalValue::Null,
        Duration::from_secs(5),
        cancellation.clone(),
    )?;
    let task = tokio::spawn(async move { vertical.runtime.execute(request).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    cancellation.cancel();
    assert_eq!(task.await?, Err(RuntimeError::Cancelled));
    Ok(())
}
