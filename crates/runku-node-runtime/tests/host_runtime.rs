//! Dedicated host Node execution, immutable cache, server selection and controlled failures.

use std::{error::Error, path::Path, sync::Arc, time::Duration};

use runku_build::{BuildMetadata, build_project};
use runku_core::{
    BuildId, EnvironmentId, EnvironmentScope, InvocationId, ProjectId, ReleaseId, RequestId,
};
use runku_node_runtime::{
    DedicatedHostPolicy, FullNodeActionRuntime, HostNodeArtifactCache, HostNodeRuntimeConfig,
    ServerNodeRuntimeConfig,
};
use runku_observability::{
    InvocationPerformanceSink, MemoryInvocationPerformanceSink, PerformanceOperation,
    PerformanceOutcome, PerformanceRuntime,
};
use runku_releases::{
    FullNodeEgressPolicy, FullNodeNetworkMode, FullNodeTcpRule, NodeOciDescriptorV1,
    ReleaseManifestV1, decode_release_manifest, encode_node_oci_descriptor,
};
use runku_runtime::{CancellationToken, InvocationRequest, RuntimeError};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::tempdir;

const SOURCE: &str = r#"
"use runku node"
import { action, v } from "@runku/server"
import { createHash } from "node:crypto"
import { deflateSync } from "node:zlib"

let warmInvocations = 0

export const workerIdentity = action({
  auth: "none", visibility: "internal", capabilities: [],
  args: v.null(), returns: v.string(),
  handler(ctx) {
    warmInvocations += 1
    return `${process.pid}:${warmInvocations}:${ctx.invocation.invocationId}`
  },
})

export const encrypt = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.string(),
  handler(_ctx, input) { return createHash("sha256").update(input).digest("hex") },
})

export const image = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.bytes(),
  handler(_ctx, input) {
    const compressed = deflateSync(Buffer.from(input));
    return new Uint8Array(Buffer.concat([Buffer.from("89504e470d0a1a0a", "hex"), compressed]));
  },
})

export const loop = action({
  auth: "none", visibility: "internal", capabilities: [],
  args: v.null(), returns: v.null(), handler() { for (;;) {} },
})

"#;

const POSTGRES_SOURCE: &str = r#"
"use runku node"
import { action, v } from "@runku/server"
import pg from "pg"

export const postgres = action({
  auth: "none", visibility: "internal", capabilities: [],
  args: v.string(), returns: v.string(),
  async handler(_ctx, connectionString) {
    const client = new pg.Client({ connectionString, connectionTimeoutMillis: 1000 });
    await client.connect();
    try {
      const result = await client.query("select 'tcp-ok'::text as value");
      return result.rows[0].value;
    } finally {
      await client.end();
    }
  },
})
"#;

struct Fixture {
    _directory: tempfile::TempDir,
    project_id: ProjectId,
    release_id: ReleaseId,
    manifest: Arc<ReleaseManifestV1>,
    artifact: Arc<[u8]>,
    cache: HostNodeArtifactCache,
    scratch: std::path::PathBuf,
}

impl Fixture {
    fn request(
        &self,
        name: &str,
        arguments: CanonicalValue,
        timeout: Duration,
    ) -> Result<InvocationRequest, Box<dyn Error>> {
        let function = self
            .manifest
            .functions
            .iter()
            .find(|function| function.name.as_str() == name)
            .ok_or("function missing")?;
        Ok(InvocationRequest::new(
            EnvironmentScope::new(self.project_id, EnvironmentId::generate()),
            self.release_id,
            RequestId::generate(),
            InvocationId::generate(),
            function.id,
            Arc::clone(&self.manifest),
            Arc::clone(&self.artifact),
            arguments,
            timeout,
            CancellationToken::new(),
        )?)
    }
}

fn fixture() -> Result<Fixture, Box<dyn Error>> {
    fixture_with(FullNodeEgressPolicy::none(), None)
}

fn fixture_with(
    egress: FullNodeEgressPolicy,
    production_node_modules: Option<&Path>,
) -> Result<Fixture, Box<dyn Error>> {
    let directory = tempdir()?;
    let source = directory.path().join("runku");
    let cache_root = directory.path().join("artifacts");
    let scratch = directory.path().join("scratch");
    std::fs::create_dir(&source)?;
    std::fs::create_dir(&cache_root)?;
    std::fs::create_dir(&scratch)?;
    std::fs::write(
        source.join("schema.ts"),
        "import { defineSchema } from '@runku/server'; export default defineSchema({});",
    )?;
    std::fs::write(source.join("actions.ts"), SOURCE)?;
    std::fs::write(source.join("postgres.ts"), POSTGRES_SOURCE)?;
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
    let source_artifact = std::fs::read(output.artifact_path)?;
    let target =
        NodeOciDescriptorV1::new(format!("sha256:{}", "a".repeat(64)))?.with_egress_policy(egress);
    let cache = HostNodeArtifactCache::open(&cache_root)?;
    cache.materialize(
        &manifest,
        &source_artifact,
        &target,
        production_node_modules,
    )?;
    let artifact = encode_node_oci_descriptor(&target)?;
    manifest.artifact = target.descriptor()?;
    Ok(Fixture {
        _directory: directory,
        project_id,
        release_id,
        manifest: Arc::new(manifest),
        artifact: artifact.into(),
        cache,
        scratch,
    })
}

fn runtime(fixture: &Fixture) -> Result<runku_node_runtime::ServerNodeRuntime, RuntimeError> {
    runtime_with_policy(fixture, FullNodeEgressPolicy::none())
}

fn runtime_with_policy(
    fixture: &Fixture,
    egress: FullNodeEgressPolicy,
) -> Result<runku_node_runtime::ServerNodeRuntime, RuntimeError> {
    let policy = DedicatedHostPolicy::new(2_000, 1024 * 1024 * 1024, 128, egress)?;
    ServerNodeRuntimeConfig::DedicatedHost(HostNodeRuntimeConfig::dedicated(
        fixture.cache.clone(),
        &fixture.scratch,
        policy,
        2,
    )?)
    .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedicated_host_connects_to_external_postgres_under_exact_policy()
-> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var_os("RUNKU_HOST_NODE_POSTGRES_URL") else {
        return Ok(());
    };
    let destination = std::env::var("RUNKU_HOST_NODE_POSTGRES_DESTINATION")?;
    let port = std::env::var("RUNKU_HOST_NODE_POSTGRES_PORT")?.parse::<u16>()?;
    let modules = std::env::var("RUNKU_HOST_NODE_MODULES")?;
    let policy = FullNodeEgressPolicy::new(
        FullNodeNetworkMode::Restricted,
        vec![FullNodeTcpRule::new(destination, vec![port])?],
        vec![],
    )?;
    let fixture = fixture_with(policy.clone(), Some(Path::new(&modules)))?;
    let outcome = runtime_with_policy(&fixture, policy)?
        .execute(fixture.request(
            "postgres.postgres",
            CanonicalValue::String(url.to_string_lossy().into_owned()),
            Duration::from_secs(5),
        )?)
        .await?;
    assert_eq!(outcome.value, CanonicalValue::String("tcp-ok".to_owned()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedicated_host_runs_node_crypto_image_and_controls_failures() -> Result<(), Box<dyn Error>>
{
    let fixture = fixture()?;
    let runtime = runtime(&fixture)?;
    let encrypted = runtime
        .execute(fixture.request(
            "actions.encrypt",
            CanonicalValue::String("runku".to_owned()),
            Duration::from_secs(3),
        )?)
        .await?;
    assert_eq!(
        encrypted.value,
        CanonicalValue::String(
            "ae57fe8872aa84461538f8ed7c54dd3eb8f7bdd2398744aef598287746259bc9".to_owned()
        )
    );
    let image = runtime
        .execute(fixture.request(
            "actions.image",
            CanonicalValue::String("pixel-data".to_owned()),
            Duration::from_secs(3),
        )?)
        .await?;
    let CanonicalValue::Bytes(image) = image.value else {
        return Err("image did not return bytes".into());
    };
    assert_eq!(&image[..8], b"\x89PNG\r\n\x1a\n");
    let first_request = fixture.request(
        "actions.workerIdentity",
        CanonicalValue::Null,
        Duration::from_secs(3),
    )?;
    let first_invocation = first_request.invocation_id();
    let first = runtime.execute(first_request).await?.value;
    let second_request = fixture.request(
        "actions.workerIdentity",
        CanonicalValue::Null,
        Duration::from_secs(3),
    )?;
    let second_invocation = second_request.invocation_id();
    let second = runtime.execute(second_request).await?.value;
    let CanonicalValue::String(first) = first else {
        return Err("first worker identity was not a string".into());
    };
    let CanonicalValue::String(second) = second else {
        return Err("second worker identity was not a string".into());
    };
    let first_pid = first.split(':').next().ok_or("first worker PID missing")?;
    let second_pid = second
        .split(':')
        .next()
        .ok_or("second worker PID missing")?;
    assert_eq!(first_pid, second_pid, "sequential requests changed worker");
    assert_eq!(first, format!("{first_pid}:1:{first_invocation}"));
    assert_eq!(second, format!("{second_pid}:2:{second_invocation}"));
    let performance = Arc::new(MemoryInvocationPerformanceSink::new(32)?);
    let performance_sink: Arc<dyn InvocationPerformanceSink> = performance.clone();
    let deadline_request = fixture
        .request(
            "actions.loop",
            CanonicalValue::Null,
            Duration::from_millis(100),
        )?
        .with_performance_sink(PerformanceRuntime::NodeHost, performance_sink);
    assert_eq!(
        runtime.execute(deadline_request).await,
        Err(RuntimeError::DeadlineExceeded)
    );
    let spans = performance.snapshot();
    assert!(spans.iter().any(|span| {
        span.operation == PerformanceOperation::ExecuteRunner
            && span.outcome == PerformanceOutcome::DeadlineExceeded
    }));
    assert!(
        spans
            .iter()
            .all(|span| span.outcome != PerformanceOutcome::Abandoned)
    );
    assert!(
        std::fs::read_dir(&fixture.scratch)?.next().is_none(),
        "host scratch leaked after timeout"
    );
    Ok(())
}

#[tokio::test]
async fn dedicated_host_rejects_public_egress_and_missing_cache() -> Result<(), Box<dyn Error>> {
    assert!(
        FullNodeEgressPolicy::new(runku_releases::FullNodeNetworkMode::Public, vec![], vec![])
            .is_ok()
    );
    let public =
        FullNodeEgressPolicy::new(runku_releases::FullNodeNetworkMode::Public, vec![], vec![])?;
    assert_eq!(
        DedicatedHostPolicy::new(1_000, 512 * 1024 * 1024, 64, public),
        Err(RuntimeError::InvalidConfiguration)
    );
    let fixture = fixture()?;
    let missing = NodeOciDescriptorV1::new(format!("sha256:{}", "b".repeat(64)))?;
    let mut manifest = (*fixture.manifest).clone();
    let artifact = encode_node_oci_descriptor(&missing)?;
    manifest.artifact = missing.descriptor()?;
    let function = manifest.functions[0].id;
    let request = InvocationRequest::new(
        EnvironmentScope::new(fixture.project_id, EnvironmentId::generate()),
        fixture.release_id,
        RequestId::generate(),
        InvocationId::generate(),
        function,
        Arc::new(manifest),
        artifact.into(),
        CanonicalValue::String("runku".to_owned()),
        Duration::from_secs(1),
        CancellationToken::new(),
    )?;
    assert_eq!(
        runtime(&fixture)?.execute(request).await,
        Err(RuntimeError::InvalidArtifact)
    );
    Ok(())
}

#[tokio::test]
async fn dedicated_host_rejects_valid_contract_substitution_in_cache() -> Result<(), Box<dyn Error>>
{
    let fixture = fixture()?;
    let function = fixture
        .manifest
        .functions
        .iter()
        .find(|function| function.name.as_str() == "actions.image")
        .ok_or("image function missing")?;
    let image_root = std::fs::read_dir(
        fixture
            .scratch
            .parent()
            .ok_or("fixture root missing")?
            .join("artifacts"),
    )?
    .next()
    .ok_or("materialized image missing")??
    .path();
    let argument_path = image_root.join(format!("{}.resource", function.arguments_contract_hash));
    let result_path = image_root.join(format!("{}.resource", function.result_contract_hash));
    let mut permissions = std::fs::metadata(&argument_path)?.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        permissions.set_mode(0o600);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(false);
    std::fs::set_permissions(&argument_path, permissions)?;
    std::fs::write(&argument_path, std::fs::read(result_path)?)?;

    assert_eq!(
        runtime(&fixture)?
            .execute(fixture.request(
                "actions.image",
                CanonicalValue::String("pixel-data".to_owned()),
                Duration::from_secs(1),
            )?)
            .await,
        Err(RuntimeError::InvalidArtifact)
    );
    Ok(())
}
