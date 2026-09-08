//! Separate-process Full Node worker acceptance over real NATS `JetStream`.

use std::{
    error::Error,
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use runku_build::{BuildMetadata, build_project};
use runku_core::{BuildId, EnvironmentId, EnvironmentScope, InvocationId, ProjectId, RequestId};
use runku_execution_queue::{
    ExecutionClass, ExecutionControlPlane, ExecutionQueue, NatsExecutionControlConfig,
    NatsExecutionControlPlane, NatsExecutionQueue, NatsExecutionQueueConfig,
};
use runku_node_runtime::{
    FullNodeActionRuntime, FullNodeFilesystemResources, QueuedNodeRuntime, QueuedNodeRuntimeConfig,
};
use runku_releases::decode_release_manifest;
use runku_runtime::{CancellationToken, InvocationRequest};
use runku_value::{CanonicalValue, TimestampMicros};

const SOURCE: &str = r#"
"use runku node"
import { action, v } from "@runku/server"
import { createHash } from "node:crypto"
export const hash = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.string(),
  handler(_ctx, input) { return createHash("sha256").update(input).digest("hex") },
})
"#;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn gateway_executes_direct_bundle_in_separate_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(nats_url) = std::env::var("RUNKU_TEST_NATS_URL") else {
        eprintln!("skipping separate worker acceptance: RUNKU_TEST_NATS_URL is unset");
        return Ok(());
    };
    let node = Command::new("node")
        .args(["--print", "process.execPath"])
        .output()?;
    if !node.status.success() {
        eprintln!("skipping separate worker acceptance: Node is unavailable");
        return Ok(());
    }
    let node_binary = String::from_utf8(node.stdout)?.trim().to_owned();
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source/runku");
    let resources_root = directory.path().join("resources");
    let runtime_root = directory.path().join("runtime");
    std::fs::create_dir_all(&source)?;
    std::fs::create_dir_all(&runtime_root)?;
    std::fs::write(
        source.join("schema.ts"),
        "import { defineSchema } from '@runku/server'; export default defineSchema({});",
    )?;
    std::fs::write(source.join("actions.ts"), SOURCE)?;
    let project_id = ProjectId::generate();
    let environment_id = EnvironmentId::generate();
    let scope = EnvironmentScope::new(project_id, environment_id);
    let output = build_project(
        directory.path().join("source").as_path(),
        Path::new("runku"),
        project_id,
        BuildMetadata {
            release_id: runku_core::ReleaseId::generate(),
            build_id: BuildId::generate(),
            created_at: TimestampMicros::new(1_800_000_000_000_000),
        },
    )?;
    let manifest_bytes = std::fs::read(output.manifest_path)?;
    let artifact = std::fs::read(output.artifact_path)?;
    let manifest = decode_release_manifest(&manifest_bytes)?;
    FullNodeFilesystemResources::open_writer(&resources_root)
        .await?
        .stage(scope, &manifest_bytes, &artifact)
        .await?;

    let suffix = InvocationId::generate()
        .to_string()
        .replace('-', "_")
        .to_ascii_uppercase();
    let stream = format!("RUNKU_PROCESS_{suffix}");
    let subject = format!("runku.process.{}", suffix.to_ascii_lowercase());
    let bucket = format!("RUNKU_PROCESS_STATE_{suffix}");
    let mut worker = Command::new(env!("CARGO_BIN_EXE_runku-server"));
    worker
        .arg("full-node-worker")
        .env_clear()
        .env("RUNKU_FULL_NODE_PROFILE", "dedicated-worker")
        .env("RUNKU_FULL_NODE_RESOURCE_ROOT", &resources_root)
        .env("RUNKU_FULL_NODE_RUNTIME_ROOT", &runtime_root)
        .env("RUNKU_FULL_NODE_BINARY", node_binary)
        .env("RUNKU_FULL_NODE_MAX_CONCURRENCY", "1")
        .env("RUNKU_FULL_NODE_MAX_CONCURRENT_PER_PROJECT", "1")
        .env("RUNKU_FULL_NODE_HEAP_MEGABYTES", "128")
        .env("RUNKU_FULL_NODE_INSTANCE_CPU_MILLIS", "1000")
        .env("RUNKU_FULL_NODE_INSTANCE_MEMORY_BYTES", "536870912")
        .env("RUNKU_FULL_NODE_INSTANCE_PIDS", "64")
        .env("RUNKU_EXECUTION_NATS_URL", &nats_url)
        .env("RUNKU_EXECUTION_NATS_STREAM", &stream)
        .env("RUNKU_EXECUTION_NATS_SUBJECT_PREFIX", &subject)
        .env("RUNKU_EXECUTION_NATS_CONTROL_BUCKET", &bucket)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = ChildGuard(worker.spawn()?);

    let client = async_nats::connect(&nats_url).await?;
    let queue_config = NatsExecutionQueueConfig {
        stream_name: stream,
        subject_prefix: subject,
        max_messages: 100_000,
        max_bytes: 1_073_741_824,
        max_age: Duration::from_mins(15),
        replicas: 1,
        ack_wait: Duration::from_secs(60),
        max_deliver: 5,
        max_waiting: 10_000,
    };
    let control_config = NatsExecutionControlConfig {
        bucket,
        max_bytes: 1_073_741_824,
        max_age: Duration::from_hours(1),
        replicas: 1,
    };
    let queue: Arc<dyn ExecutionQueue> =
        Arc::new(NatsExecutionQueue::open(client.clone(), queue_config).await?);
    let control: Arc<dyn ExecutionControlPlane> =
        Arc::new(NatsExecutionControlPlane::open(client, control_config).await?);
    let class = ExecutionClass::new("node_host_v1")?;
    let gateway = QueuedNodeRuntime::new(
        queue,
        control,
        QueuedNodeRuntimeConfig {
            class,
            result_wait: Duration::from_millis(50),
            configuration_available: false,
        },
    )?;
    let function = manifest
        .functions
        .iter()
        .find(|function| function.name.as_str() == "actions.hash")
        .ok_or("missing action")?;
    let function_id = function.id;
    let manifest = Arc::new(manifest);
    let artifact: Arc<[u8]> = artifact.into();
    let request = InvocationRequest::new(
        scope,
        manifest.release_id,
        RequestId::generate(),
        InvocationId::generate(),
        function_id,
        Arc::clone(&manifest),
        Arc::clone(&artifact),
        CanonicalValue::String("separate-worker".to_owned()),
        Duration::from_secs(5),
        CancellationToken::new(),
    )?;
    let started = Instant::now();
    let result = gateway.execute(request).await;
    let elapsed = started.elapsed();
    if let Err(error) = &result
        && let Some(status) = child.0.try_wait()?
    {
        return Err(format!("worker exited {status}: {error}").into());
    }
    assert_eq!(
        result?.value,
        CanonicalValue::String(
            "6d4d1d41d3965f1233599fe462043624fadad8acbf6bc3f2cab8fa5d8b646a41".to_owned()
        )
    );
    println!(
        "separate worker cold invocation: {} ms",
        elapsed.as_millis()
    );
    assert!(elapsed < Duration::from_secs(4));
    let warm_request = InvocationRequest::new(
        scope,
        manifest.release_id,
        RequestId::generate(),
        InvocationId::generate(),
        function_id,
        manifest,
        artifact,
        CanonicalValue::String("separate-worker".to_owned()),
        Duration::from_secs(3),
        CancellationToken::new(),
    )?;
    let warm_started = Instant::now();
    assert_eq!(
        gateway.execute(warm_request).await?.value,
        CanonicalValue::String(
            "6d4d1d41d3965f1233599fe462043624fadad8acbf6bc3f2cab8fa5d8b646a41".to_owned()
        )
    );
    let warm_elapsed = warm_started.elapsed();
    println!(
        "separate worker warm invocation: {} ms",
        warm_elapsed.as_millis()
    );
    assert!(warm_elapsed < Duration::from_secs(2));
    Ok(())
}
