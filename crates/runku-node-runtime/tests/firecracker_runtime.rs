//! Firecracker production adapter lifecycle, cancellation and resource-attribution conformance.

use std::{error::Error, sync::Arc, time::Duration};

use runku_core::{
    BuildId, EnvironmentId, EnvironmentScope, FunctionId, InvocationId, ProjectId, ReleaseId,
    RequestId,
};
use runku_node_runtime::{
    FirecrackerNodeRuntimeConfig, FullNodeActionRuntime, ServerNodeRuntimeConfig,
};
use runku_observability::{
    InvocationPerformanceSink, MemoryInvocationPerformanceSink, PerformanceOperation,
    PerformanceRuntime,
};
use runku_releases::{
    AuthPolicy, Capability, FullNodeEgressPolicy, FunctionManifest, FunctionType,
    FunctionVisibility, NodeOciDescriptorV1, ReleaseManifestV1, RuntimeClass, Sha256Digest,
    encode_node_oci_descriptor,
};
use runku_runtime::{CancellationToken, InvocationRequest, RuntimeError};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::tempdir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const IMAGE: &str = "registry.invalid/runku/function@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

struct Fixture {
    project_id: ProjectId,
    release_id: ReleaseId,
    function_id: FunctionId,
    manifest: Arc<ReleaseManifestV1>,
    artifact: Arc<[u8]>,
}

impl Fixture {
    fn request(
        &self,
        value: &str,
        cancellation: CancellationToken,
    ) -> Result<InvocationRequest, RuntimeError> {
        InvocationRequest::new(
            EnvironmentScope::new(self.project_id, EnvironmentId::generate()),
            self.release_id,
            RequestId::generate(),
            InvocationId::generate(),
            self.function_id,
            Arc::clone(&self.manifest),
            Arc::clone(&self.artifact),
            CanonicalValue::String(value.to_owned()),
            Duration::from_secs(2),
            cancellation,
        )
    }
}

fn fixture() -> Result<Fixture, Box<dyn Error>> {
    let descriptor = NodeOciDescriptorV1::new(IMAGE)?;
    let artifact: Arc<[u8]> = encode_node_oci_descriptor(&descriptor)?.into();
    let project_id = ProjectId::generate();
    let release_id = ReleaseId::generate();
    let function_id = FunctionId::generate();
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id,
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_800_000_000_000_000),
        runtime_version: "runku-node-1".parse()?,
        artifact: descriptor.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "actions.echo".parse()?,
            function_type: FunctionType::Action,
            visibility: FunctionVisibility::Internal,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::FullNode,
            implementation_hash: Sha256Digest::from_bytes([4; 32]),
            arguments_contract_hash: Sha256Digest::from_bytes([5; 32]),
            result_contract_hash: Sha256Digest::from_bytes([6; 32]),
            capabilities: Vec::<Capability>::new(),
        }],
        cron_definitions: Vec::new(),
    };
    manifest.validate()?;
    Ok(Fixture {
        project_id,
        release_id,
        function_id,
        manifest: Arc::new(manifest),
        artifact,
    })
}

#[cfg(unix)]
fn controller(directory: &std::path::Path) -> Result<std::path::PathBuf, Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt as _;

    let log = directory.join("controller.log");
    let script = directory.join("controller.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nset -eu\nprintf '%s %s\\n' \"$1\" \"$2\" >> '{}'\n",
            log.display()
        ),
    )?;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700))?;
    Ok(script)
}

async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let mut bytes = vec![0_u8; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

async fn write_frame(
    stream: &mut TcpStream,
    bytes: &[u8],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    stream
        .write_all(&u32::try_from(bytes.len())?.to_be_bytes())
        .await?;
    stream.write_all(bytes).await?;
    Ok(())
}

async fn serve_connection(mut stream: TcpStream) -> Result<(), Box<dyn Error + Send + Sync>> {
    if read_frame(&mut stream).await? != TOKEN.as_bytes() {
        return Err("unexpected IPC token".into());
    }
    write_frame(&mut stream, b"READY").await?;
    loop {
        let Ok(request) = read_frame(&mut stream).await else {
            return Ok(());
        };
        if String::from_utf8_lossy(&request).contains("hang") {
            tokio::time::sleep(Duration::from_secs(5)).await;
            return Ok(());
        }
        write_frame(
            &mut stream,
            br#"{"protocolVersion":1,"ok":true,"value":{"type":"string","value":"ok"},"performance":{"userCpuMicros":17,"systemCpuMicros":3,"peakMemoryBytes":1048576,"memoryBytes":524288}}"#,
        )
        .await?;
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn firecracker_is_server_selectable_measures_resources_and_replaces_cancelled_vm()
-> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?;
    let server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _ = serve_connection(stream).await;
            });
        }
    });
    let directory = tempdir()?;
    let controller = controller(directory.path())?;
    let config = FirecrackerNodeRuntimeConfig::new(
        vec![endpoint],
        IMAGE,
        FullNodeEgressPolicy::none(),
        TOKEN,
        controller,
    )?;
    let runtime = ServerNodeRuntimeConfig::Firecracker(config).build()?;
    let fixture = fixture()?;
    runtime
        .prepare(&fixture.manifest, &fixture.artifact)
        .await?;

    let spans = Arc::new(MemoryInvocationPerformanceSink::new(32)?);
    let sink: Arc<dyn InvocationPerformanceSink> = spans.clone();
    let measured = fixture
        .request("ok", CancellationToken::new())?
        .with_performance_sink(PerformanceRuntime::NodeFirecracker, sink);
    let outcome = runtime.execute(measured).await?;
    assert_eq!(outcome.value, CanonicalValue::String("ok".to_owned()));
    assert_eq!(
        outcome.resource_usage.and_then(|usage| usage.memory_bytes),
        Some(524_288)
    );
    assert!(spans.snapshot().iter().any(|span| {
        span.operation == PerformanceOperation::ExecuteRunner
            && span.resources.is_some_and(|usage| {
                usage.user_cpu_micros == Some(17)
                    && usage.system_cpu_micros == Some(3)
                    && usage.peak_memory_bytes == Some(1_048_576)
            })
    }));

    let cancellation = CancellationToken::new();
    let cancel_signal = cancellation.clone();
    let cancelled = fixture.request("hang", cancellation)?;
    let execution = runtime.execute(cancelled);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancel_signal.cancel();
    });
    assert_eq!(execution.await, Err(RuntimeError::Cancelled));
    assert_eq!(
        runtime
            .execute(fixture.request("after", CancellationToken::new())?)
            .await?
            .value,
        CanonicalValue::String("ok".to_owned())
    );
    let lifecycle = std::fs::read_to_string(directory.path().join("controller.log"))?;
    assert!(lifecycle.lines().any(|line| line == "ensure 0"));
    assert!(lifecycle.lines().any(|line| line == "replace 0"));
    server.abort();
    Ok(())
}
