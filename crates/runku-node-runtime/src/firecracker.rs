//! Production Full Node transport for a supervised pool of prewarmed Firecracker microVMs.
//!
//! An operator-owned controller starts each worker through `jailer`, installs its network policy,
//! and replaces the complete microVM after cancellation, deadline, transport failure, malformed
//! output, or JavaScript failure. The Rust adapter owns bounded admission, immutable image/policy
//! binding, authenticated IPC, resource attribution, and fail-closed controller coordination.

use std::{
    collections::{HashSet, VecDeque},
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::atomic::AtomicBool,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use runku_observability::{PerformanceComponent, PerformanceOperation};
use runku_releases::{
    FullNodeEgressPolicy, NodeOciDescriptorV1, ReleaseManifestV1, decode_node_oci_descriptor,
};
use runku_runtime::{InvocationRequest, RuntimeError};
use runku_value::encode_stored_value;
use tokio::{
    process::Command,
    sync::{Mutex, Notify},
};

use crate::{
    FullNodeActionOutcome, FullNodeActionRuntime,
    mailbox::TcpMailbox,
    protocol::{decode_response_measured, prepare_request, validate_artifact},
};

const MAX_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_WORKERS: usize = 32;
const MAX_TOKEN_BYTES: usize = 256;
const MAX_CONTROLLER_TIMEOUT: Duration = Duration::from_secs(120);

/// One immutable, supervised Firecracker pool exposed to an Execution Agent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FirecrackerNodeRuntimeConfig {
    /// Authenticated runner addresses, one per single-flight microVM.
    pub endpoints: Vec<SocketAddr>,
    /// Exact OCI digest already loaded into every microVM root filesystem.
    pub image_reference: String,
    /// Exact egress policy installed for every worker in this homogeneous pool.
    pub egress_policy: FullNodeEgressPolicy,
    /// Per-pool random IPC authentication secret.
    pub ipc_token: String,
    /// Root-owned controller invoked as `ensure|replace|shutdown WORKER_INDEX`.
    pub controller_path: PathBuf,
    /// Maximum time for one controller lifecycle operation.
    pub controller_timeout: Duration,
    /// Maximum time allowed to establish a runner connection.
    pub connect_timeout: Duration,
    /// Maximum runner response frame accepted by the host.
    pub max_output_bytes: usize,
}

impl FirecrackerNodeRuntimeConfig {
    /// Creates a production configuration for one homogeneous immutable worker pool.
    ///
    /// # Errors
    ///
    /// Rejects empty or unbounded pools, mutable images and unsafe IPC settings.
    pub fn new(
        endpoints: Vec<SocketAddr>,
        image_reference: impl Into<String>,
        egress_policy: FullNodeEgressPolicy,
        ipc_token: impl Into<String>,
        controller_path: impl Into<PathBuf>,
    ) -> Result<Self, RuntimeError> {
        let config = Self {
            endpoints,
            image_reference: image_reference.into(),
            egress_policy,
            ipc_token: ipc_token.into(),
            controller_path: controller_path.into(),
            controller_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_output_bytes: MAX_OUTPUT_BYTES,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), RuntimeError> {
        if self.endpoints.is_empty()
            || self.endpoints.len() > MAX_WORKERS
            || self.image_reference.split_once("@sha256:").is_none()
            || NodeOciDescriptorV1::new(&self.image_reference).is_err()
            || self.endpoints.iter().collect::<HashSet<_>>().len() != self.endpoints.len()
            || self.ipc_token.len() < 32
            || self.ipc_token.len() > MAX_TOKEN_BYTES
            || !self.controller_path.is_absolute()
            || !controller_is_executable(&self.controller_path)
            || self.controller_timeout.is_zero()
            || self.controller_timeout > MAX_CONTROLLER_TIMEOUT
            || self.connect_timeout.is_zero()
            || self.max_output_bytes == 0
            || self.max_output_bytes > MAX_OUTPUT_BYTES
        {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct FirecrackerTelemetry {
    hits: AtomicU64,
    reconnects: AtomicU64,
    failed: AtomicU64,
    replacements: AtomicU64,
    replacement_failures: AtomicU64,
    idle: AtomicUsize,
}

/// Process-local counters for the prewarmed Firecracker transport pool.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FirecrackerNodeRuntimeTelemetrySnapshot {
    /// Invocations served over an already-authenticated connection.
    pub hits: u64,
    /// Connections established or re-established to a prewarmed microVM.
    pub reconnects: u64,
    /// Invocations whose worker connection or response failed.
    pub failed: u64,
    /// Complete microVM replacements requested after an unsafe outcome.
    pub replacements: u64,
    /// Replacement operations that did not complete successfully.
    pub replacement_failures: u64,
    /// Configured single-flight worker count.
    pub workers: usize,
    /// Workers currently idle and available for admission.
    pub idle: usize,
}

struct Worker {
    address: SocketAddr,
    mailbox: Option<TcpMailbox>,
}

/// Bounded production runtime backed by single-flight, prewarmed Firecracker microVMs.
pub struct FirecrackerNodeRuntime {
    config: FirecrackerNodeRuntimeConfig,
    workers: Vec<Mutex<Worker>>,
    available: Mutex<VecDeque<usize>>,
    available_changed: Notify,
    telemetry: FirecrackerTelemetry,
    prewarmed: AtomicBool,
    prewarm_lock: Mutex<()>,
}

impl std::fmt::Debug for FirecrackerNodeRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FirecrackerNodeRuntime")
            .field("endpoints", &self.config.endpoints)
            .field("image_reference", &self.config.image_reference)
            .field("ipc_token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl FirecrackerNodeRuntime {
    /// Creates a runtime for an externally provisioned microVM pool.
    ///
    /// # Errors
    ///
    /// Rejects invalid, mutable or unbounded configuration.
    pub fn new(config: FirecrackerNodeRuntimeConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        let workers = config
            .endpoints
            .iter()
            .copied()
            .map(|address| {
                Mutex::new(Worker {
                    address,
                    mailbox: None,
                })
            })
            .collect::<Vec<_>>();
        Ok(Self {
            available: Mutex::new((0..workers.len()).collect()),
            config,
            telemetry: FirecrackerTelemetry {
                idle: AtomicUsize::new(workers.len()),
                ..FirecrackerTelemetry::default()
            },
            workers,
            available_changed: Notify::new(),
            prewarmed: AtomicBool::new(false),
            prewarm_lock: Mutex::new(()),
        })
    }

    /// Returns bounded process-local pool counters.
    pub fn telemetry(&self) -> FirecrackerNodeRuntimeTelemetrySnapshot {
        FirecrackerNodeRuntimeTelemetrySnapshot {
            hits: self.telemetry.hits.load(Ordering::Relaxed),
            reconnects: self.telemetry.reconnects.load(Ordering::Relaxed),
            failed: self.telemetry.failed.load(Ordering::Relaxed),
            replacements: self.telemetry.replacements.load(Ordering::Relaxed),
            replacement_failures: self.telemetry.replacement_failures.load(Ordering::Relaxed),
            workers: self.workers.len(),
            idle: self.telemetry.idle.load(Ordering::Relaxed),
        }
    }

    /// Ensures that every configured worker is running and authenticated before traffic is pulled.
    ///
    /// # Errors
    ///
    /// Fails closed when the lifecycle controller or any runner readiness handshake fails.
    pub async fn prewarm(&self) -> Result<(), RuntimeError> {
        if self.prewarmed.load(Ordering::Acquire) {
            return Ok(());
        }
        let _guard = self.prewarm_lock.lock().await;
        if self.prewarmed.load(Ordering::Acquire) {
            return Ok(());
        }
        for index in 0..self.workers.len() {
            self.run_controller("ensure", index).await?;
            let worker = self.workers[index].lock().await;
            let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
            let ready = TcpMailbox::connect(worker.address, &self.config.ipc_token, deadline).await;
            drop(worker);
            if ready.is_err() {
                self.replace_worker(index).await?;
            }
        }
        self.prewarmed.store(true, Ordering::Release);
        Ok(())
    }

    /// Stops every worker owned by this exact runtime pool.
    pub async fn shutdown(&self) {
        for index in 0..self.workers.len() {
            self.workers[index].lock().await.mailbox = None;
            let _ = self.run_controller("shutdown", index).await;
        }
        self.prewarmed.store(false, Ordering::Release);
    }

    async fn run_controller(&self, action: &str, index: usize) -> Result<(), RuntimeError> {
        let mut command = Command::new(&self.config.controller_path);
        command
            .arg(action)
            .arg(index.to_string())
            .env(
                "RUNKU_FIRECRACKER_IMAGE_REFERENCE",
                &self.config.image_reference,
            )
            .env(
                "RUNKU_FIRECRACKER_EGRESS_MODE",
                match self.config.egress_policy.mode() {
                    runku_releases::FullNodeNetworkMode::None => "none",
                    runku_releases::FullNodeNetworkMode::Public => "public",
                    runku_releases::FullNodeNetworkMode::Restricted => "restricted",
                },
            )
            .env(
                "RUNKU_FIRECRACKER_EGRESS_ALLOW",
                encode_rules(self.config.egress_policy.allow()),
            )
            .env(
                "RUNKU_FIRECRACKER_EGRESS_DENY",
                encode_rules(self.config.egress_policy.deny()),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let status = tokio::time::timeout(self.config.controller_timeout, command.status())
            .await
            .map_err(|_| RuntimeError::Unavailable)?
            .map_err(|_| RuntimeError::Unavailable)?;
        if status.success() {
            Ok(())
        } else {
            Err(RuntimeError::Unavailable)
        }
    }

    async fn replace_worker(&self, index: usize) -> Result<(), RuntimeError> {
        self.telemetry.replacements.fetch_add(1, Ordering::Relaxed);
        let replaced = self.run_controller("replace", index).await;
        if replaced.is_err() {
            self.telemetry
                .replacement_failures
                .fetch_add(1, Ordering::Relaxed);
            self.prewarmed.store(false, Ordering::Release);
            return replaced;
        }
        let worker = self.workers[index].lock().await;
        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let ready = TcpMailbox::connect(worker.address, &self.config.ipc_token, deadline)
            .await
            .map(|_| ());
        if ready.is_err() {
            self.telemetry
                .replacement_failures
                .fetch_add(1, Ordering::Relaxed);
            self.prewarmed.store(false, Ordering::Release);
        }
        ready
    }

    async fn acquire_worker(
        &self,
        deadline: tokio::time::Instant,
        cancellation: &runku_runtime::CancellationToken,
    ) -> Result<usize, RuntimeError> {
        loop {
            let changed = self.available_changed.notified();
            if let Some(index) = self.available.lock().await.pop_front() {
                self.telemetry.idle.fetch_sub(1, Ordering::Relaxed);
                return Ok(index);
            }
            tokio::select! {
                () = changed => {}
                () = cancellation.cancelled() => return Err(RuntimeError::Cancelled),
                () = tokio::time::sleep_until(deadline) => {
                    return Err(RuntimeError::DeadlineExceeded);
                }
            }
        }
    }

    async fn release_worker(&self, index: usize) {
        self.available.lock().await.push_back(index);
        self.telemetry.idle.fetch_add(1, Ordering::Relaxed);
        self.available_changed.notify_one();
    }

    async fn execute_measured(
        &self,
        request: &InvocationRequest,
    ) -> Result<FullNodeActionOutcome, RuntimeError> {
        self.prewarm().await?;
        let deadline = tokio::time::Instant::now() + request.wall_timeout();
        let validation_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Runtime,
                PerformanceOperation::Validate,
                u64::try_from(request.artifact_bytes().len()).ok(),
            )
        });
        let prepared = prepare_request(request);
        let validated = prepared.and_then(|prepared| {
            if prepared.image_reference != self.config.image_reference
                || prepared.egress != self.config.egress_policy
            {
                return Err(RuntimeError::InvalidArtifact);
            }
            Ok(prepared)
        });
        crate::performance::finish(validation_timer, &validated, None, None);
        let prepared = validated?;
        let cancellation = request.cancellation();
        let admission_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Runtime,
                PerformanceOperation::Admission,
                None,
            )
        });
        let worker_index = self.acquire_worker(deadline, &cancellation).await;
        crate::performance::finish(admission_timer, &worker_index, None, None);
        let worker_index = worker_index?;

        let runner_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::NodeProcess,
                PerformanceOperation::ExecuteRunner,
                u64::try_from(prepared.input.len()).ok(),
            )
        });
        let mut worker = self.workers[worker_index].lock().await;
        let result = async {
            if worker.mailbox.is_none() {
                let connect_deadline =
                    deadline.min(tokio::time::Instant::now() + self.config.connect_timeout);
                worker.mailbox = Some(
                    TcpMailbox::connect(worker.address, &self.config.ipc_token, connect_deadline)
                        .await?,
                );
                self.telemetry.reconnects.fetch_add(1, Ordering::Relaxed);
            } else {
                self.telemetry.hits.fetch_add(1, Ordering::Relaxed);
            }
            let response = worker
                .mailbox
                .as_mut()
                .ok_or(RuntimeError::Unavailable)?
                .invoke(
                    &prepared.input,
                    self.config.max_output_bytes,
                    deadline,
                    &cancellation,
                )
                .await?;
            let decoded = decode_response_measured(&response);
            Ok::<_, RuntimeError>((decoded.result, decoded.resources))
        }
        .await;
        let (result, resources) = match result {
            Ok((result, resources)) => (result, resources),
            Err(error) => (Err(error), None),
        };
        if result.is_err() {
            worker.mailbox = None;
            self.telemetry.failed.fetch_add(1, Ordering::Relaxed);
        }
        drop(worker);
        if result.is_err() {
            let _ = self.replace_worker(worker_index).await;
        }
        self.release_worker(worker_index).await;
        let output_bytes = result.as_ref().ok().and_then(|outcome| {
            encode_stored_value(&outcome.value)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok())
        });
        crate::performance::finish(runner_timer, &result, output_bytes, resources);
        result
    }
}

fn encode_rules(rules: &[runku_releases::FullNodeTcpRule]) -> String {
    rules
        .iter()
        .map(|rule| {
            let ports = rule
                .ports()
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(",");
            format!("{}|{ports}", rule.destination())
        })
        .collect::<Vec<_>>()
        .join(";")
}

#[cfg(unix)]
fn controller_is_executable(path: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    path.metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn controller_is_executable(path: &std::path::Path) -> bool {
    path.is_file()
}

#[async_trait]
impl FullNodeActionRuntime for FirecrackerNodeRuntime {
    fn validate_manifest(&self, manifest: &ReleaseManifestV1) -> Result<(), RuntimeError> {
        manifest
            .ensure_full_node_v1_supported()
            .map_err(|_| RuntimeError::UnsupportedRuntime)
    }

    async fn prepare(
        &self,
        manifest: &ReleaseManifestV1,
        artifact_bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        validate_artifact(manifest, artifact_bytes)?;
        let descriptor_bytes =
            if manifest.artifact.format == runku_releases::ArtifactFormat::HybridOciArtifactV1 {
                runku_releases::decode_hybrid_oci_artifact(artifact_bytes)
                    .map_err(|_| RuntimeError::InvalidArtifact)?
                    .1
            } else {
                artifact_bytes
            };
        let descriptor = decode_node_oci_descriptor(descriptor_bytes)
            .map_err(|_| RuntimeError::InvalidArtifact)?;
        if descriptor.image_reference() != self.config.image_reference
            || descriptor.egress_policy() != &self.config.egress_policy
        {
            return Err(RuntimeError::InvalidArtifact);
        }
        self.prewarm().await
    }

    async fn execute(
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
        let resources = result
            .as_ref()
            .ok()
            .and_then(|outcome| outcome.resource_usage);
        crate::performance::finish(total_timer, &result, output_bytes, resources);
        result
    }
}
