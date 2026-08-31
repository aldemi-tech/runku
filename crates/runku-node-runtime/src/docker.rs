use std::{process::Stdio, sync::Arc, time::Duration};

use async_trait::async_trait;
use runku_observability::{PerformanceComponent, PerformanceOperation};
use runku_protocol::WireValueV1;
use runku_releases::{
    ArtifactFormat, FullNodeEgressPolicy, FullNodeNetworkMode, FunctionType, RuntimeClass,
    Sha256Digest, decode_node_oci_descriptor,
};
use runku_runtime::{InvocationRequest, RuntimeError};
use runku_value::encode_stored_value;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::Semaphore,
};

use crate::{FullNodeActionOutcome, FullNodeActionRuntime};

const MIB: u64 = 1024 * 1024;
const MAX_CONCURRENCY: usize = 1_024;
const MAX_MEMORY_BYTES: u64 = 4 * 1024 * MIB;
const MAX_OUTPUT_BYTES: usize = 2 * 1024 * 1024;

/// Validated resource and admission settings for the Docker Full Node adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerNodeRuntimeConfig {
    docker_binary: String,
    max_concurrency: usize,
    queue_timeout: Duration,
    memory_bytes: u64,
    cpu_millis: u16,
    pids_limit: u16,
    tmpfs_bytes: u64,
    max_output_bytes: usize,
    restricted_network: Option<DockerRestrictedNetwork>,
}

/// Pre-provisioned Docker network whose reachable services enforce one exact restricted policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerRestrictedNetwork {
    name: String,
    policy: FullNodeEgressPolicy,
}

impl DockerRestrictedNetwork {
    /// Binds one exact restricted policy to an operator-created isolated Docker network.
    ///
    /// # Errors
    ///
    /// Rejects unsafe network names and policies other than `restricted`.
    pub fn new(
        name: impl Into<String>,
        policy: FullNodeEgressPolicy,
    ) -> Result<Self, RuntimeError> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            || policy.mode() != FullNodeNetworkMode::Restricted
        {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(Self { name, policy })
    }

    /// Exact operator-created Docker network name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Exact restricted policy enforced by network membership/topology.
    #[must_use]
    pub const fn policy(&self) -> &FullNodeEgressPolicy {
        &self.policy
    }
}

impl DockerNodeRuntimeConfig {
    /// Creates conservative defaults for a local/CI Docker validation node.
    ///
    /// # Errors
    ///
    /// Rejects zero concurrency.
    pub fn new(max_concurrency: usize) -> Result<Self, RuntimeError> {
        let config = Self {
            docker_binary: "docker".to_owned(),
            max_concurrency,
            queue_timeout: Duration::from_secs(2),
            memory_bytes: 128 * MIB,
            cpu_millis: 500,
            pids_limit: 64,
            tmpfs_bytes: 16 * MIB,
            max_output_bytes: MAX_OUTPUT_BYTES,
            restricted_network: None,
        };
        config.validate()?;
        Ok(config)
    }

    /// Overrides the Docker-compatible command path, primarily for testing.
    #[must_use]
    pub fn with_docker_binary(mut self, binary: impl Into<String>) -> Self {
        self.docker_binary = binary.into();
        self
    }

    /// Sets the maximum wait for process admission.
    #[must_use]
    pub const fn with_queue_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = timeout;
        self
    }

    /// Sets the hard container memory limit in bytes.
    #[must_use]
    pub const fn with_memory_bytes(mut self, bytes: u64) -> Self {
        self.memory_bytes = bytes;
        self
    }

    /// Sets the container CPU quota in millicores.
    #[must_use]
    pub const fn with_cpu_millis(mut self, millis: u16) -> Self {
        self.cpu_millis = millis;
        self
    }

    /// Allows one exact restricted policy through a pre-provisioned isolated network.
    #[must_use]
    pub fn with_restricted_network(mut self, network: DockerRestrictedNetwork) -> Self {
        self.restricted_network = Some(network);
        self
    }

    /// Validates every configured boundary.
    ///
    /// # Errors
    ///
    /// Rejects empty commands and resource dimensions outside the experimental safe bounds.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.docker_binary.is_empty()
            || !(1..=MAX_CONCURRENCY).contains(&self.max_concurrency)
            || self.queue_timeout.is_zero()
            || !(16 * MIB..=MAX_MEMORY_BYTES).contains(&self.memory_bytes)
            || !(10..=64_000).contains(&self.cpu_millis)
            || self.pids_limit == 0
            || self.tmpfs_bytes == 0
            || !(1..=MAX_OUTPUT_BYTES).contains(&self.max_output_bytes)
        {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Docker-backed ephemeral Full Node runtime used for remote-model conformance.
pub struct DockerNodeRuntime {
    config: DockerNodeRuntimeConfig,
    permits: Arc<Semaphore>,
}

impl std::fmt::Debug for DockerNodeRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DockerNodeRuntime")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DockerNodeRuntime {
    /// Creates an executor with a process-wide concurrent container ceiling.
    ///
    /// # Errors
    ///
    /// Rejects invalid resource or admission settings.
    pub fn new(config: DockerNodeRuntimeConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.max_concurrency)),
            config,
        })
    }

    async fn execute_inner(
        &self,
        request: InvocationRequest,
    ) -> Result<FullNodeActionOutcome, RuntimeError> {
        let total_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Runtime,
                PerformanceOperation::Invocation,
                encode_stored_value(request.arguments())
                    .ok()
                    .and_then(|bytes| u64::try_from(bytes.len()).ok()),
            )
        });
        let result = self.execute_measured(&request).await;
        let output_bytes = result.as_ref().ok().and_then(|outcome| {
            encode_stored_value(&outcome.value)
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
        let deadline = tokio::time::Instant::now() + request.wall_timeout();
        let validation_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Runtime,
                PerformanceOperation::Validate,
                u64::try_from(request.artifact_bytes().len()).ok(),
            )
        });
        let prepared = prepare_request(&self.config, request);
        let input_bytes = prepared
            .as_ref()
            .ok()
            .and_then(|(_, input, _)| u64::try_from(input.len()).ok());
        crate::performance::finish(validation_timer, &prepared, input_bytes, None);
        let (image_reference, input, network) = prepared?;
        let cancellation = request.cancellation();
        let admission_deadline = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + self.config.queue_timeout,
        );
        let admission_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Runtime,
                PerformanceOperation::Admission,
                None,
            )
        });
        let permit = tokio::select! {
            permit = Arc::clone(&self.permits).acquire_owned() => {
                permit.map_err(|_| RuntimeError::Unavailable)
            }
            () = cancellation.cancelled() => Err(RuntimeError::Cancelled),
            () = tokio::time::sleep_until(admission_deadline) => {
                Err(if admission_deadline == deadline {
                    RuntimeError::DeadlineExceeded
                } else {
                    RuntimeError::Busy
                })
            }
        };
        crate::performance::finish(admission_timer, &permit, None, None);
        let permit = permit?;
        let container_name = format!("runku-node-{}", request.invocation_id()).to_lowercase();
        let mut command = self.command(&container_name, &image_reference, &network);
        let create_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Sandbox,
                PerformanceOperation::Create,
                None,
            )
        });
        let child = command.spawn().map_err(|_| RuntimeError::Unavailable);
        crate::performance::finish(create_timer, &child, None, None);
        let mut child = child?;
        let stdout = child.stdout.take().ok_or(RuntimeError::Internal)?;
        let stderr = child.stderr.take().ok_or(RuntimeError::Internal)?;
        let mut stdin = child.stdin.take().ok_or(RuntimeError::Internal)?;
        let output_limit = self.config.max_output_bytes;
        let stdout_task = tokio::spawn(read_bounded(stdout, output_limit));
        let stderr_task = tokio::spawn(read_bounded(stderr, output_limit));
        let runner_input_bytes = u64::try_from(input.len()).ok();
        let input_task = tokio::spawn(async move {
            stdin.write_all(&input).await?;
            stdin.shutdown().await
        });
        let runner_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::NodeProcess,
                PerformanceOperation::ExecuteRunner,
                runner_input_bytes,
            )
        });
        let wait = tokio::select! {
            status = child.wait() => status.map_err(|_| RuntimeError::Unavailable),
            () = cancellation.cancelled() => {
                terminate(&mut child, &self.config.docker_binary, &container_name).await;
                Err(RuntimeError::Cancelled)
            }
            () = tokio::time::sleep_until(deadline) => {
                terminate(&mut child, &self.config.docker_binary, &container_name).await;
                Err(RuntimeError::DeadlineExceeded)
            }
        };
        drop(permit);
        let status = match wait {
            Ok(status) => status,
            Err(error) => {
                crate::performance::finish(
                    runner_timer,
                    &Err::<(), RuntimeError>(error),
                    None,
                    None,
                );
                return Err(error);
            }
        };
        input_task
            .await
            .map_err(|_| RuntimeError::Internal)?
            .map_err(|_| RuntimeError::Unavailable)?;
        let stdout = stdout_task.await.map_err(|_| RuntimeError::Internal)??;
        let _stderr = stderr_task.await.map_err(|_| RuntimeError::Internal)??;
        if !status.success() {
            return Err(if status.code() == Some(125) {
                RuntimeError::Unavailable
            } else {
                RuntimeError::JavaScript
            });
        }
        let response: NodeResponseV1 =
            serde_json::from_slice(&stdout).map_err(|_| RuntimeError::InvalidResult)?;
        if response.protocol_version != 1 {
            return Err(RuntimeError::InvalidResult);
        }
        let result = match (response.ok, response.value, response.error) {
            (true, Some(value), None) => Ok(FullNodeActionOutcome {
                value: value
                    .into_canonical()
                    .map_err(|_| RuntimeError::InvalidResult)?,
                resource_usage: response.performance.map(Into::into),
            }),
            (false, None, Some(error)) => {
                let _ = error.code;
                Err(RuntimeError::JavaScript)
            }
            _ => Err(RuntimeError::InvalidResult),
        };
        let output_bytes = result.as_ref().ok().and_then(|outcome| {
            encode_stored_value(&outcome.value)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok())
        });
        let resources = result
            .as_ref()
            .ok()
            .and_then(|outcome| outcome.resource_usage);
        crate::performance::finish(runner_timer, &result, output_bytes, resources);
        result
    }

    fn command(&self, container_name: &str, image: &str, network: &str) -> Command {
        let mut command = Command::new(&self.config.docker_binary);
        command
            .arg("run")
            .arg("--rm")
            .arg("--name")
            .arg(container_name)
            .arg("--network")
            .arg(network)
            .arg("--read-only")
            .arg("--tmpfs")
            .arg(format!(
                "/tmp:rw,noexec,nosuid,nodev,size={}",
                self.config.tmpfs_bytes
            ))
            .arg("--memory")
            .arg(self.config.memory_bytes.to_string())
            .arg("--cpus")
            .arg(format!(
                "{}.{:03}",
                self.config.cpu_millis / 1000,
                self.config.cpu_millis % 1000
            ))
            .arg("--pids-limit")
            .arg(self.config.pids_limit.to_string())
            .arg("--cap-drop")
            .arg("ALL")
            .arg("--security-opt")
            .arg("no-new-privileges")
            .arg("--user")
            .arg("65532:65532")
            .arg("--interactive")
            .arg("--entrypoint")
            .arg("node")
            .arg(image)
            .arg("/opt/runku/runner.mjs")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
    }
}

fn prepare_request(
    config: &DockerNodeRuntimeConfig,
    request: &InvocationRequest,
) -> Result<(String, Vec<u8>, String), RuntimeError> {
    request
        .manifest()
        .ensure_full_node_v1_supported()
        .map_err(|_| RuntimeError::UnsupportedRuntime)?;
    let function = request
        .manifest()
        .functions
        .iter()
        .find(|function| function.id == request.function_id())
        .ok_or(RuntimeError::FunctionNotFound)?;
    if function.runtime_class != RuntimeClass::FullNode
        || function.function_type != FunctionType::Action
    {
        return Err(RuntimeError::UnsupportedRuntime);
    }
    let artifact = request.artifact_bytes();
    if request.manifest().artifact.size_bytes
        != u64::try_from(artifact.len()).map_err(|_| RuntimeError::InvalidArtifact)?
        || request.manifest().artifact.digest != Sha256Digest::of(artifact)
    {
        return Err(RuntimeError::InvalidArtifact);
    }
    let descriptor_bytes = match request.manifest().artifact.format {
        ArtifactFormat::NodeOciDescriptorV1 => artifact.as_ref(),
        ArtifactFormat::HybridOciArtifactV1 => {
            runku_releases::decode_hybrid_oci_artifact(artifact)
                .map_err(|_| RuntimeError::InvalidArtifact)?
                .1
        }
        _ => return Err(RuntimeError::UnsupportedRuntime),
    };
    let descriptor =
        decode_node_oci_descriptor(descriptor_bytes).map_err(|_| RuntimeError::InvalidArtifact)?;
    let network = match descriptor.egress_policy().mode() {
        FullNodeNetworkMode::None => "none".to_owned(),
        FullNodeNetworkMode::Restricted => config
            .restricted_network
            .as_ref()
            .filter(|network| network.policy() == descriptor.egress_policy())
            .map(|network| network.name().to_owned())
            .ok_or(RuntimeError::UnsupportedRuntime)?,
        FullNodeNetworkMode::Public => return Err(RuntimeError::UnsupportedRuntime),
    };
    let arguments = WireValueV1::from_canonical(request.arguments())
        .map_err(|_| RuntimeError::InvalidArguments)?;
    let envelope = NodeRequestV1 {
        protocol_version: 1,
        collect_performance: request.performance().is_some(),
        release_id: request.release_id().to_string(),
        invocation_id: request.invocation_id().to_string(),
        function: function.name.as_str().to_owned(),
        implementation_hash: function.implementation_hash.to_string(),
        arguments_contract_hash: function.arguments_contract_hash.to_string(),
        result_contract_hash: function.result_contract_hash.to_string(),
        arguments,
    };
    let input = serde_json::to_vec(&envelope).map_err(|_| RuntimeError::Internal)?;
    Ok((descriptor.image_reference().to_owned(), input, network))
}

#[async_trait]
impl FullNodeActionRuntime for DockerNodeRuntime {
    fn validate_manifest(
        &self,
        manifest: &runku_releases::ReleaseManifestV1,
    ) -> Result<(), RuntimeError> {
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

async fn read_bounded(
    reader: impl AsyncRead + Unpin,
    limit: usize,
) -> Result<Vec<u8>, RuntimeError> {
    let take_limit = u64::try_from(limit)
        .map_err(|_| RuntimeError::Internal)?
        .saturating_add(1);
    let mut reader = reader.take(take_limit);
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    if bytes.len() > limit {
        return Err(RuntimeError::InvalidResult);
    }
    Ok(bytes)
}

async fn terminate(child: &mut Child, docker_binary: &str, container_name: &str) {
    let _ = child.kill().await;
    let _ = Command::new(docker_binary)
        .arg("rm")
        .arg("--force")
        .arg(container_name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeRequestV1 {
    protocol_version: u8,
    collect_performance: bool,
    release_id: String,
    invocation_id: String,
    function: String,
    implementation_hash: String,
    arguments_contract_hash: String,
    result_contract_hash: String,
    arguments: WireValueV1,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeResponseV1 {
    protocol_version: u8,
    ok: bool,
    value: Option<WireValueV1>,
    error: Option<NodeErrorV1>,
    performance: Option<NodePerformanceV1>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodePerformanceV1 {
    user_cpu_micros: u64,
    system_cpu_micros: u64,
    peak_memory_bytes: u64,
    memory_bytes: u64,
}

impl From<NodePerformanceV1> for runku_observability::PerformanceResourceUsage {
    fn from(value: NodePerformanceV1) -> Self {
        Self {
            user_cpu_micros: Some(value.user_cpu_micros),
            system_cpu_micros: Some(value.system_cpu_micros),
            peak_memory_bytes: Some(value.peak_memory_bytes),
            memory_bytes: Some(value.memory_bytes),
            ..Self::default()
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeErrorV1 {
    code: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_and_docker_isolation_arguments_are_bounded() -> Result<(), RuntimeError> {
        assert_eq!(
            DockerNodeRuntimeConfig::new(0),
            Err(RuntimeError::InvalidConfiguration)
        );
        let runtime = DockerNodeRuntime::new(DockerNodeRuntimeConfig::new(4)?)?;
        let command = runtime.command(
            "runku-node-test",
            &format!("sha256:{}", "a".repeat(64)),
            "none",
        );
        let arguments = command
            .as_std()
            .get_args()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        for required in [
            "--network",
            "none",
            "--read-only",
            "--memory",
            "--cpus",
            "--pids-limit",
            "--cap-drop",
            "ALL",
            "no-new-privileges",
            "65532:65532",
            "--entrypoint",
            "node",
            "/opt/runku/runner.mjs",
        ] {
            assert!(arguments.iter().any(|argument| argument == required));
        }
        Ok(())
    }
}
