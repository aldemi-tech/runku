//! Dedicated-tenant host Node execution over verified materialized OCI application artifacts.

use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use runku_contracts::{Contract, decode_contract};
use runku_core::InvocationId;
use runku_observability::{PerformanceComponent, PerformanceOperation};
use runku_protocol::WireValueV1;
use runku_releases::{
    ArtifactFormat, FullNodeEgressPolicy, FullNodeNetworkMode, FunctionType, NodeOciDescriptorV1,
    ReleaseManifestV1, RuntimeClass, Sha256Digest, decode_node_esm_bundle,
    decode_node_oci_descriptor,
};
use runku_runtime::{InvocationRequest, RuntimeError};
use runku_value::encode_stored_value;
use serde::{Deserialize, Serialize};
use tokio::{
    process::{Child, Command},
    sync::{Mutex, OnceCell, Semaphore},
};

use crate::{FullNodeActionOutcome, FullNodeActionRuntime};

const HOST_RUNNER: &str = include_str!("local_runner.mjs");
const MAX_CONCURRENCY: usize = 128;
const MAX_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_DEPENDENCY_FILES: usize = 50_000;
const MAX_DEPENDENCY_BYTES: u64 = 512 * 1024 * 1024;

/// Instance-level resources and egress that the dedicated VM/host must enforce externally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DedicatedHostPolicy {
    cpu_millis: u32,
    memory_bytes: u64,
    pids: u32,
    egress: FullNodeEgressPolicy,
}

impl DedicatedHostPolicy {
    /// Declares the hard boundary assigned to one tenant instance.
    ///
    /// The runtime validates this declaration and applies a smaller V8 heap/concurrency ceiling.
    /// VM, cgroup and firewall provisioning remain responsible for enforcing the declared whole
    /// instance CPU, memory, PID and network boundary.
    ///
    /// # Errors
    ///
    /// Rejects unsafe dimensions and unrestricted public egress.
    pub fn new(
        cpu_millis: u32,
        memory_bytes: u64,
        pids: u32,
        egress: FullNodeEgressPolicy,
    ) -> Result<Self, RuntimeError> {
        if !(100..=256_000).contains(&cpu_millis)
            || !(128 * 1024 * 1024..=1024_u64.pow(4)).contains(&memory_bytes)
            || !(8..=1_000_000).contains(&pids)
            || egress.mode() == FullNodeNetworkMode::Public
        {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(Self {
            cpu_millis,
            memory_bytes,
            pids,
            egress,
        })
    }

    /// Exact externally enforced egress policy.
    #[must_use]
    pub const fn egress(&self) -> &FullNodeEgressPolicy {
        &self.egress
    }

    /// Whole-instance CPU allocation in millicores.
    #[must_use]
    pub const fn cpu_millis(&self) -> u32 {
        self.cpu_millis
    }

    /// Whole-instance hard memory allocation.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    /// Whole-instance hard PID allocation.
    #[must_use]
    pub const fn pids(&self) -> u32 {
        self.pids
    }
}

/// Trusted content-addressed cache populated by an OCI-aware publisher/agent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostNodeArtifactCache {
    root: PathBuf,
}

impl HostNodeArtifactCache {
    /// Opens an existing private cache root.
    ///
    /// # Errors
    ///
    /// Rejects missing, non-directory or noncanonical roots.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, RuntimeError> {
        let root = fs::canonicalize(root.into()).map_err(|_| RuntimeError::InvalidConfiguration)?;
        if !root.is_dir() {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(Self { root })
    }

    /// Materializes one verified local build under the immutable OCI image identity.
    ///
    /// This is the trusted publisher/agent bridge used after the OCI image digest has been
    /// resolved. Implementations and contracts are revalidated by content hash. An optional
    /// production `node_modules` tree is copied without following symlinks and with strict bounds.
    /// Existing exact materializations replay without mutation.
    ///
    /// # Errors
    ///
    /// Rejects invalid manifests/artifacts, symlinks, special files, bounds and cache conflicts.
    pub fn materialize(
        &self,
        source_manifest: &ReleaseManifestV1,
        source_artifact: &[u8],
        target: &NodeOciDescriptorV1,
        production_node_modules: Option<&Path>,
    ) -> Result<PathBuf, RuntimeError> {
        source_manifest
            .ensure_local_full_node_supported()
            .map_err(|_| RuntimeError::UnsupportedRuntime)?;
        let bundle =
            decode_node_esm_bundle(source_artifact).map_err(|_| RuntimeError::InvalidArtifact)?;
        bundle
            .verify_manifest(source_manifest, source_artifact)
            .map_err(|_| RuntimeError::InvalidArtifact)?;
        let final_path = self.image_root(target)?;
        if final_path.exists() {
            verify_materialized(&final_path, source_manifest, &bundle, target)?;
            return Ok(final_path);
        }
        let staging = self.root.join(format!(
            ".install-{}-{}",
            target
                .image_digest()
                .map_err(|_| RuntimeError::InvalidArtifact)?,
            InvocationId::generate()
        ));
        fs::create_dir(&staging).map_err(|_| RuntimeError::Unavailable)?;
        let result = (|| {
            write_resources(&staging, source_manifest, &bundle)?;
            if let Some(dependencies) = production_node_modules {
                copy_dependency_tree(dependencies, &staging.join("node_modules"))?;
            }
            fs::write(staging.join("image-reference"), target.image_reference())
                .map_err(|_| RuntimeError::Unavailable)?;
            fs::rename(&staging, &final_path).map_err(|error| {
                if final_path.exists() {
                    RuntimeError::Busy
                } else {
                    let _ = error;
                    RuntimeError::Unavailable
                }
            })?;
            if let Err(error) = make_tree_read_only(&final_path) {
                make_tree_writable(&final_path);
                let _ = fs::remove_dir_all(&final_path);
                return Err(error);
            }
            Ok(())
        })();
        if result.is_err() && staging.exists() {
            make_tree_writable(&staging);
            let _ = fs::remove_dir_all(&staging);
        }
        if result == Err(RuntimeError::Busy) && final_path.exists() {
            verify_materialized(&final_path, source_manifest, &bundle, target)?;
            return Ok(final_path);
        }
        result?;
        Ok(final_path)
    }

    fn image_root(&self, target: &NodeOciDescriptorV1) -> Result<PathBuf, RuntimeError> {
        Ok(self.root.join(
            target
                .image_digest()
                .map_err(|_| RuntimeError::InvalidArtifact)?
                .to_string(),
        ))
    }
}

/// Production host-process settings for one dedicated tenant instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostNodeRuntimeConfig {
    node_binary: String,
    cache: HostNodeArtifactCache,
    scratch_root: PathBuf,
    instance_policy: DedicatedHostPolicy,
    max_concurrency: usize,
    queue_timeout: Duration,
    heap_megabytes: u16,
    max_output_bytes: usize,
}

impl HostNodeRuntimeConfig {
    /// Creates a dedicated-instance configuration.
    ///
    /// # Errors
    ///
    /// Rejects invalid roots, bounds or a heap larger than the instance allocation.
    pub fn dedicated(
        cache: HostNodeArtifactCache,
        scratch_root: impl Into<PathBuf>,
        instance_policy: DedicatedHostPolicy,
        max_concurrency: usize,
    ) -> Result<Self, RuntimeError> {
        let scratch_root = fs::canonicalize(scratch_root.into())
            .map_err(|_| RuntimeError::InvalidConfiguration)?;
        let config = Self {
            node_binary: "node".to_owned(),
            cache,
            scratch_root,
            instance_policy,
            max_concurrency,
            queue_timeout: Duration::from_secs(2),
            heap_megabytes: 256,
            max_output_bytes: MAX_OUTPUT_BYTES,
        };
        config.validate()?;
        Ok(config)
    }

    /// Overrides the pinned Node executable.
    #[must_use]
    pub fn with_node_binary(mut self, binary: impl Into<String>) -> Self {
        self.node_binary = binary.into();
        self
    }

    /// Overrides per-process V8 heap memory.
    #[must_use]
    pub const fn with_heap_megabytes(mut self, heap_megabytes: u16) -> Self {
        self.heap_megabytes = heap_megabytes;
        self
    }

    /// Overrides bounded admission wait.
    #[must_use]
    pub const fn with_queue_timeout(mut self, queue_timeout: Duration) -> Self {
        self.queue_timeout = queue_timeout;
        self
    }

    /// Validates every dedicated-host boundary.
    ///
    /// # Errors
    ///
    /// Rejects invalid dimensions, roots and policy contradictions.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        let heap_bytes = u64::from(self.heap_megabytes) * 1024 * 1024;
        if self.node_binary.is_empty()
            || !self.cache.root.is_absolute()
            || !self.scratch_root.is_absolute()
            || !self.scratch_root.is_dir()
            || !(1..=MAX_CONCURRENCY).contains(&self.max_concurrency)
            || self.queue_timeout.is_zero()
            || !(64..=4096).contains(&self.heap_megabytes)
            || heap_bytes >= self.instance_policy.memory_bytes
            || !(1..=MAX_OUTPUT_BYTES).contains(&self.max_output_bytes)
        {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Native Node executor restricted to one externally isolated tenant VM/instance.
pub struct HostNodeRuntime {
    config: HostNodeRuntimeConfig,
    permits: Arc<Semaphore>,
    node_ready: OnceCell<()>,
    workers: Mutex<HostWorkerPool>,
}

#[derive(Default)]
struct HostWorkerPool {
    idle: HashMap<PathBuf, Vec<HostWorker>>,
    workers: usize,
}

struct HostWorker {
    child: Child,
    root: PathBuf,
    image_root: PathBuf,
    uses: u32,
}

impl std::fmt::Debug for HostNodeRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostNodeRuntime")
            .field("config", &self.config)
            .field("node_checked", &self.node_ready.initialized())
            .finish_non_exhaustive()
    }
}

impl HostNodeRuntime {
    /// Creates a native executor for a dedicated tenant instance.
    ///
    /// # Errors
    ///
    /// Rejects an invalid configuration.
    pub fn new(mut config: HostNodeRuntimeConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        config.node_binary = resolve_executable(&config.node_binary)?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.max_concurrency)),
            config,
            node_ready: OnceCell::new(),
            workers: Mutex::new(HostWorkerPool::default()),
        })
    }

    async fn ensure_node(&self) -> Result<(), RuntimeError> {
        self.node_ready
            .get_or_try_init(|| async {
                let output = tokio::time::timeout(
                    Duration::from_secs(2),
                    Command::new(&self.config.node_binary)
                        .arg("--version")
                        .stdin(Stdio::null())
                        .stderr(Stdio::null())
                        .output(),
                )
                .await
                .map_err(|_| RuntimeError::Unavailable)?
                .map_err(|_| RuntimeError::Unavailable)?;
                if !output.status.success() || !supported_node_version(&output.stdout) {
                    return Err(RuntimeError::UnsupportedRuntime);
                }
                Ok(())
            })
            .await
            .copied()
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

    async fn checkout_worker(
        &self,
        image_root: &Path,
        deadline: tokio::time::Instant,
    ) -> Result<HostWorker, RuntimeError> {
        let mut pool = self.workers.lock().await;
        if let Some(worker) = pool.idle.get_mut(image_root).and_then(Vec::pop) {
            if pool.idle.get(image_root).is_some_and(Vec::is_empty) {
                pool.idle.remove(image_root);
            }
            return Ok(worker);
        }
        pool.workers = pool.workers.saturating_add(1);
        drop(pool);
        let worker = self.start_worker(image_root, deadline).await;
        if worker.is_err() {
            let mut pool = self.workers.lock().await;
            pool.workers = pool.workers.saturating_sub(1);
        }
        worker
    }

    async fn start_worker(
        &self,
        image_root: &Path,
        deadline: tokio::time::Instant,
    ) -> Result<HostWorker, RuntimeError> {
        let root = self
            .config
            .scratch_root
            .join(format!("worker-{}", InvocationId::generate()));
        tokio::fs::create_dir(&root)
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        let mailbox = root.join("ipc");
        if let Err(error) = crate::mailbox::prepare(&mailbox).await {
            let _ = tokio::fs::remove_dir_all(&root).await;
            return Err(error);
        }
        let user = root.join("user");
        tokio::fs::create_dir(&user)
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        let child = Command::new(&self.config.node_binary)
            .arg(format!(
                "--max-old-space-size={}",
                self.config.heap_megabytes
            ))
            .args(["--input-type=module", "--eval", HOST_RUNNER])
            .arg("--")
            .arg("--serve-directory")
            .arg(&mailbox)
            .arg(image_root)
            .current_dir(image_root)
            .env_clear()
            .env("LANG", "C.UTF-8")
            .env("HOME", &user)
            .env("TMPDIR", &user)
            .env("TMP", &user)
            .env("TEMP", &user)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| RuntimeError::Unavailable)?;
        let ready_deadline = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + Duration::from_secs(2),
        );
        if let Err(error) = crate::mailbox::wait_ready(&mailbox, ready_deadline).await {
            let mut child = child;
            let _ = child.kill().await;
            let _ = tokio::fs::remove_dir_all(&root).await;
            return Err(error);
        }
        Ok(HostWorker {
            child,
            root,
            image_root: image_root.to_path_buf(),
            uses: 0,
        })
    }

    async fn return_worker(&self, mut worker: HostWorker, reusable: bool) {
        worker.uses = worker.uses.saturating_add(1);
        let alive = worker.child.try_wait().is_ok_and(|status| status.is_none());
        if reusable && alive && worker.uses < 10_000 {
            self.workers
                .lock()
                .await
                .idle
                .entry(worker.image_root.clone())
                .or_default()
                .push(worker);
            return;
        }
        let _ = worker.child.kill().await;
        let _ = tokio::fs::remove_dir_all(&worker.root).await;
        let mut pool = self.workers.lock().await;
        pool.workers = pool.workers.saturating_sub(1);
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_measured(
        &self,
        request: &InvocationRequest,
    ) -> Result<FullNodeActionOutcome, RuntimeError> {
        let deadline = tokio::time::Instant::now() + request.wall_timeout();
        let node_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::NodeProcess,
                PerformanceOperation::CheckNode,
                None,
            )
        });
        let node = self.ensure_node().await;
        crate::performance::finish(node_timer, &node, None, None);
        node?;
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
            .and_then(|prepared| u64::try_from(prepared.input.len()).ok());
        crate::performance::finish(validation_timer, &prepared, input_bytes, None);
        let prepared = prepared?;
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
        let create_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::NodeProcess,
                PerformanceOperation::Create,
                None,
            )
        });
        let create = self.checkout_worker(&prepared.image_root, deadline).await;
        crate::performance::finish(create_timer, &create, None, None);
        let worker = create?;
        let runner_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::NodeProcess,
                PerformanceOperation::ExecuteRunner,
                u64::try_from(prepared.input.len()).ok(),
            )
        });
        let cancellation = request.cancellation();
        let response = crate::mailbox::invoke(
            &worker.root.join("ipc"),
            request.invocation_id(),
            &prepared.input,
            self.config.max_output_bytes,
            deadline,
            &cancellation,
        )
        .await;
        let outcome = response.and_then(|bytes| decode_response(&bytes));
        let reusable = outcome.is_ok();
        let runner_output_bytes = outcome.as_ref().ok().and_then(|outcome| {
            encode_stored_value(&outcome.value)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok())
        });
        let resources = outcome
            .as_ref()
            .ok()
            .and_then(|outcome| outcome.resource_usage);
        crate::performance::finish(runner_timer, &outcome, runner_output_bytes, resources);
        let cleanup_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::Cleanup,
                PerformanceOperation::Cleanup,
                None,
            )
        });
        self.return_worker(worker, reusable).await;
        crate::performance::finish(cleanup_timer, &Ok::<(), RuntimeError>(()), None, None);
        drop(permit);
        let outcome = outcome?;
        prepared
            .result_contract
            .validate_value(&outcome.value)
            .map_err(|_| RuntimeError::InvalidResult)?;
        Ok(outcome)
    }
}

fn resolve_executable(binary: &str) -> Result<String, RuntimeError> {
    let output = std::process::Command::new(binary)
        .args(["--print", "process.execPath"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|_| RuntimeError::InvalidConfiguration)?;
    if !output.status.success() {
        return Err(RuntimeError::InvalidConfiguration);
    }
    let path = std::str::from_utf8(&output.stdout)
        .map_err(|_| RuntimeError::InvalidConfiguration)?
        .trim();
    let resolved = fs::canonicalize(path).map_err(|_| RuntimeError::InvalidConfiguration)?;
    if !resolved.is_file() {
        return Err(RuntimeError::InvalidConfiguration);
    }
    resolved
        .into_os_string()
        .into_string()
        .map_err(|_| RuntimeError::InvalidConfiguration)
}

#[async_trait]
impl FullNodeActionRuntime for HostNodeRuntime {
    fn validate_manifest(&self, manifest: &ReleaseManifestV1) -> Result<(), RuntimeError> {
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

struct PreparedInvocation {
    image_root: PathBuf,
    input: Vec<u8>,
    result_contract: Contract,
}

fn prepare_request(
    config: &HostNodeRuntimeConfig,
    request: &InvocationRequest,
) -> Result<PreparedInvocation, RuntimeError> {
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
    if descriptor.egress_policy() != config.instance_policy.egress() {
        return Err(RuntimeError::UnsupportedRuntime);
    }
    let image_root = config.cache.image_root(&descriptor)?;
    validate_cached_root(&config.cache.root, &image_root)?;
    let source_path = image_root.join(format!("{}.mjs", function.implementation_hash));
    validate_cached_file(
        &image_root,
        &source_path,
        Some(function.implementation_hash),
    )?;
    let arguments_contract = read_contract(&image_root, function.arguments_contract_hash)?;
    arguments_contract
        .validate_value(request.arguments())
        .map_err(|_| RuntimeError::InvalidArguments)?;
    let result_contract = read_contract(&image_root, function.result_contract_hash)?;
    let input = serde_json::to_vec(&NodeRequestV1 {
        protocol_version: 1,
        collect_performance: request.performance().is_some(),
        release_id: request.release_id().to_string(),
        invocation_id: request.invocation_id().to_string(),
        function: function.name.as_str().to_owned(),
        implementation_hash: function.implementation_hash.to_string(),
        arguments: WireValueV1::from_canonical(request.arguments())
            .map_err(|_| RuntimeError::InvalidArguments)?,
    })
    .map_err(|_| RuntimeError::Internal)?;
    Ok(PreparedInvocation {
        image_root,
        input,
        result_contract,
    })
}

fn write_resources(
    root: &Path,
    manifest: &ReleaseManifestV1,
    bundle: &runku_releases::NodeEsmBundleV1,
) -> Result<(), RuntimeError> {
    let mut implementations = BTreeSet::new();
    let mut contracts = BTreeSet::new();
    for function in &manifest.functions {
        if implementations.insert(function.implementation_hash) {
            let source = bundle
                .source(function.implementation_hash)
                .ok_or(RuntimeError::InvalidArtifact)?;
            fs::write(
                root.join(format!("{}.mjs", function.implementation_hash)),
                source,
            )
            .map_err(|_| RuntimeError::Unavailable)?;
        }
        contracts.insert(function.arguments_contract_hash);
        contracts.insert(function.result_contract_hash);
    }
    contracts.insert(manifest.schema_contract_hash);
    contracts.insert(manifest.index_contract_hash);
    for digest in contracts {
        let resource = bundle
            .resource(digest)
            .ok_or(RuntimeError::InvalidArtifact)?;
        fs::write(root.join(format!("{digest}.resource")), resource)
            .map_err(|_| RuntimeError::Unavailable)?;
    }
    Ok(())
}

fn verify_materialized(
    root: &Path,
    manifest: &ReleaseManifestV1,
    bundle: &runku_releases::NodeEsmBundleV1,
    target: &NodeOciDescriptorV1,
) -> Result<(), RuntimeError> {
    validate_cached_root(root, root)?;
    let image_reference = root.join("image-reference");
    validate_cached_file(root, &image_reference, None)?;
    if fs::read_to_string(image_reference).map_err(|_| RuntimeError::InvalidArtifact)?
        != target.image_reference()
    {
        return Err(RuntimeError::InvalidArtifact);
    }
    let mut contracts =
        BTreeSet::from([manifest.schema_contract_hash, manifest.index_contract_hash]);
    for function in &manifest.functions {
        let path = root.join(format!("{}.mjs", function.implementation_hash));
        validate_cached_file(root, &path, Some(function.implementation_hash))?;
        if fs::read_to_string(path).map_err(|_| RuntimeError::InvalidArtifact)?
            != bundle
                .source(function.implementation_hash)
                .ok_or(RuntimeError::InvalidArtifact)?
        {
            return Err(RuntimeError::InvalidArtifact);
        }
        contracts.insert(function.arguments_contract_hash);
        contracts.insert(function.result_contract_hash);
    }
    for digest in contracts {
        let path = root.join(format!("{digest}.resource"));
        validate_cached_file(root, &path, Some(digest))?;
        if fs::read_to_string(path).map_err(|_| RuntimeError::InvalidArtifact)?
            != bundle
                .resource(digest)
                .ok_or(RuntimeError::InvalidArtifact)?
        {
            return Err(RuntimeError::InvalidArtifact);
        }
    }
    Ok(())
}

fn read_contract(root: &Path, digest: Sha256Digest) -> Result<Contract, RuntimeError> {
    let path = root.join(format!("{digest}.resource"));
    validate_cached_file(root, &path, Some(digest))?;
    decode_contract(&fs::read(path).map_err(|_| RuntimeError::InvalidArtifact)?)
        .map_err(|_| RuntimeError::InvalidArtifact)
}

fn validate_cached_root(cache: &Path, root: &Path) -> Result<(), RuntimeError> {
    let metadata = fs::symlink_metadata(root).map_err(|_| RuntimeError::InvalidArtifact)?;
    let canonical = fs::canonicalize(root).map_err(|_| RuntimeError::InvalidArtifact)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() || !canonical.starts_with(cache) {
        return Err(RuntimeError::InvalidArtifact);
    }
    Ok(())
}

fn validate_cached_file(
    root: &Path,
    path: &Path,
    digest: Option<Sha256Digest>,
) -> Result<(), RuntimeError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| RuntimeError::InvalidArtifact)?;
    let canonical = fs::canonicalize(path).map_err(|_| RuntimeError::InvalidArtifact)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || !canonical.starts_with(root) {
        return Err(RuntimeError::InvalidArtifact);
    }
    if let Some(digest) = digest
        && Sha256Digest::of(&fs::read(path).map_err(|_| RuntimeError::InvalidArtifact)?) != digest
    {
        return Err(RuntimeError::InvalidArtifact);
    }
    Ok(())
}

fn copy_dependency_tree(source: &Path, target: &Path) -> Result<(), RuntimeError> {
    let source = fs::canonicalize(source).map_err(|_| RuntimeError::InvalidArtifact)?;
    if !source.is_dir() {
        return Err(RuntimeError::InvalidArtifact);
    }
    fs::create_dir(target).map_err(|_| RuntimeError::Unavailable)?;
    let mut stack = vec![(source.clone(), target.to_path_buf())];
    let mut files = 0_usize;
    let mut bytes = 0_u64;
    while let Some((from, to)) = stack.pop() {
        for entry in fs::read_dir(&from).map_err(|_| RuntimeError::InvalidArtifact)? {
            let entry = entry.map_err(|_| RuntimeError::InvalidArtifact)?;
            let metadata =
                fs::symlink_metadata(entry.path()).map_err(|_| RuntimeError::InvalidArtifact)?;
            if metadata.file_type().is_symlink() {
                return Err(RuntimeError::InvalidArtifact);
            }
            let destination = to.join(entry.file_name());
            if metadata.is_dir() {
                fs::create_dir(&destination).map_err(|_| RuntimeError::Unavailable)?;
                stack.push((entry.path(), destination));
            } else if metadata.is_file() {
                files = files.saturating_add(1);
                bytes = bytes.saturating_add(metadata.len());
                if files > MAX_DEPENDENCY_FILES || bytes > MAX_DEPENDENCY_BYTES {
                    return Err(RuntimeError::InvalidArtifact);
                }
                fs::copy(entry.path(), destination).map_err(|_| RuntimeError::Unavailable)?;
            } else {
                return Err(RuntimeError::InvalidArtifact);
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn make_tree_read_only(root: &Path) -> Result<(), RuntimeError> {
    use std::os::unix::fs::PermissionsExt as _;
    for path in tree_paths(root)? {
        let metadata = fs::metadata(&path).map_err(|_| RuntimeError::Unavailable)?;
        let mode = if metadata.is_dir() {
            0o555
        } else if metadata.permissions().mode() & 0o111 == 0 {
            0o444
        } else {
            0o555
        };
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .map_err(|_| RuntimeError::Unavailable)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn make_tree_read_only(root: &Path) -> Result<(), RuntimeError> {
    for path in tree_paths(root)? {
        let mut permissions = fs::metadata(&path)
            .map_err(|_| RuntimeError::Unavailable)?
            .permissions();
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).map_err(|_| RuntimeError::Unavailable)?;
    }
    Ok(())
}

#[cfg(unix)]
fn make_tree_writable(root: &Path) {
    use std::os::unix::fs::PermissionsExt as _;

    for path in tree_paths(root).unwrap_or_default() {
        if let Ok(metadata) = fs::metadata(&path) {
            let mode = if metadata.is_dir() || metadata.permissions().mode() & 0o111 != 0 {
                0o700
            } else {
                0o600
            };
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
        }
    }
}

#[cfg(not(unix))]
fn make_tree_writable(root: &Path) {
    for path in tree_paths(root).unwrap_or_default() {
        if let Ok(metadata) = fs::metadata(&path) {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(false);
            let _ = fs::set_permissions(path, permissions);
        }
    }
}

fn tree_paths(root: &Path) -> Result<Vec<PathBuf>, RuntimeError> {
    let mut paths = vec![root.to_path_buf()];
    let mut index = 0;
    while index < paths.len() {
        let current = paths[index].clone();
        index += 1;
        if fs::metadata(&current)
            .map_err(|_| RuntimeError::Unavailable)?
            .is_dir()
        {
            for entry in fs::read_dir(current).map_err(|_| RuntimeError::Unavailable)? {
                paths.push(entry.map_err(|_| RuntimeError::Unavailable)?.path());
            }
        }
    }
    paths.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    Ok(paths)
}

fn supported_node_version(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.trim().strip_prefix('v'))
        .and_then(|value| value.split('.').next())
        .and_then(|value| value.parse::<u16>().ok())
        .is_some_and(|major| major >= 20)
}

fn decode_response(bytes: &[u8]) -> Result<FullNodeActionOutcome, RuntimeError> {
    let response: NodeResponseV1 =
        serde_json::from_slice(bytes).map_err(|_| RuntimeError::InvalidResult)?;
    if response.protocol_version != 1 {
        return Err(RuntimeError::InvalidResult);
    }
    match (response.ok, response.value, response.error) {
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
    }
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
