//! Local Node execution through the declarative build and immutable invocation envelope.

use std::{error::Error, path::Path, sync::Arc, time::Duration};

use runku_build::{BuildMetadata, build_project};
use runku_core::{
    BuildId, EnvironmentId, EnvironmentScope, InvocationId, ProjectId, ReleaseId, RequestId,
};
use runku_node_runtime::{FullNodeActionRuntime, LocalNodeRuntime, LocalNodeRuntimeConfig};
use runku_observability::{
    InvocationPerformanceSink, MemoryInvocationPerformanceSink, PerformanceOperation,
    PerformanceOutcome, PerformanceRuntime,
};
use runku_releases::{ArtifactFormat, RuntimeClass, decode_release_manifest};
use runku_runtime::{CancellationToken, InvocationRequest};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn declarative_full_node_action_uses_the_machine_node_binary() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let source = directory.path().join("runku");
    std::fs::create_dir(&source)?;
    std::fs::write(
        source.join("schema.ts"),
        r#"
import { defineSchema } from "@runku/server"
export default defineSchema({})
"#,
    )?;
    let package = directory
        .path()
        .join("node_modules/runku-local-test-package");
    std::fs::create_dir_all(&package)?;
    std::fs::write(
        package.join("package.json"),
        r#"{"name":"runku-local-test-package","version":"1.0.0","main":"index.cjs"}"#,
    )?;
    std::fs::write(
        package.join("index.cjs"),
        r"module.exports = (value) => `npm:${value}`;",
    )?;
    std::fs::write(
        source.join("functions.ts"),
        r#"
"use runku node"
import { action, v } from "@runku/server"
import path from "node:path"
import { createHash } from "node:crypto"
import { deflateSync } from "node:zlib"
import fromPackage from "runku-local-test-package"
export const basename = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.string(),
  handler(_ctx, input) {
    const name = fromPackage(path.basename(input));
    const digest = createHash("sha256").update("runku").digest("hex");
    const image = Buffer.concat([Buffer.from("89504e470d0a1a0a", "hex"), deflateSync(Buffer.from(input))]);
    return `${name}:${digest}:${image.subarray(0, 8).toString("hex")}`;
  },
})
export const loop = action({
  auth: "none", visibility: "internal", capabilities: [],
  args: v.null(), returns: v.null(), handler() { for (;;) {} },
})
"#,
    )?;
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
    let manifest = Arc::new(decode_release_manifest(&std::fs::read(
        output.manifest_path,
    )?)?);
    assert_eq!(manifest.artifact.format, ArtifactFormat::NodeEsmBundleV1);
    assert_eq!(manifest.functions[0].runtime_class, RuntimeClass::FullNode);
    let basename_id = manifest
        .functions
        .iter()
        .find(|function| function.name.as_str() == "functions.basename")
        .ok_or("basename function missing")?
        .id;
    let loop_id = manifest
        .functions
        .iter()
        .find(|function| function.name.as_str() == "functions.loop")
        .ok_or("loop function missing")?
        .id;
    let artifact: Arc<[u8]> = std::fs::read(output.artifact_path)?.into();
    let request = |function_id, arguments, timeout| {
        InvocationRequest::new(
            EnvironmentScope::new(project_id, EnvironmentId::generate()),
            release_id,
            RequestId::generate(),
            InvocationId::generate(),
            function_id,
            Arc::clone(&manifest),
            Arc::clone(&artifact),
            arguments,
            timeout,
            CancellationToken::new(),
        )
    };
    let unavailable = LocalNodeRuntime::new(
        LocalNodeRuntimeConfig::new(directory.path(), 1)?
            .with_node_binary("/definitely/missing/runku-node"),
    )?;
    assert_eq!(
        unavailable
            .execute(request(
                basename_id,
                CanonicalValue::String("/tmp/runku/result.txt".to_owned()),
                Duration::from_secs(1),
            )?)
            .await,
        Err(runku_runtime::RuntimeError::Unavailable)
    );
    let runtime = LocalNodeRuntime::new(LocalNodeRuntimeConfig::new(directory.path(), 4)?)?;
    let outcome = runtime
        .execute(request(
            basename_id,
            CanonicalValue::String("/tmp/runku/result.txt".to_owned()),
            Duration::from_secs(5),
        )?)
        .await?;
    assert_eq!(
        outcome.value,
        CanonicalValue::String(
            "npm:result.txt:ae57fe8872aa84461538f8ed7c54dd3eb8f7bdd2398744aef598287746259bc9:89504e470d0a1a0a"
                .to_owned()
        )
    );
    let performance = Arc::new(MemoryInvocationPerformanceSink::new(32)?);
    let performance_sink: Arc<dyn InvocationPerformanceSink> = performance.clone();
    let deadline_request = request(loop_id, CanonicalValue::Null, Duration::from_millis(100))?
        .with_performance_sink(PerformanceRuntime::NodeLocal, performance_sink);
    assert_eq!(
        runtime.execute(deadline_request).await,
        Err(runku_runtime::RuntimeError::DeadlineExceeded)
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
    Ok(())
}
