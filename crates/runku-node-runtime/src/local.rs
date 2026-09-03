use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use runku_contracts::{Contract, decode_contract};
use runku_observability::{PerformanceComponent, PerformanceOperation};
use runku_protocol::WireValueV1;
use runku_releases::{
    ArtifactFormat, Capability, FunctionType, ReleaseManifestV1, RuntimeClass, Sha256Digest,
    decode_node_esm_bundle,
};
use runku_runtime::{
    FileDownloadGrantRequest, FileStoreRequest, FileUploadGrantRequest, FunctionCallKind,
    FunctionCallRequest, InvocationRequest, RuntimeError, ScheduleRequest, ScheduleTime,
};
use runku_value::{TimestampMicros, encode_stored_value};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::{Child, Command},
    sync::{OnceCell, Semaphore},
};

use crate::{FullNodeActionOutcome, FullNodeActionRuntime};

const LOCAL_RUNNER: &str = include_str!("local_runner.mjs");
const MAX_CONCURRENCY: usize = 128;
const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_PLATFORM_OPS_PER_INVOCATION: u64 = 10_000;

/// Validated developer-machine Node process settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalNodeRuntimeConfig {
    node_binary: String,
    project_root: PathBuf,
    max_concurrency: usize,
    queue_timeout: Duration,
    heap_megabytes: u16,
    max_output_bytes: usize,
}

impl LocalNodeRuntimeConfig {
    /// Creates local defaults rooted at one canonical project directory.
    ///
    /// # Errors
    ///
    /// Rejects missing/non-directory roots and invalid concurrency.
    pub fn new(
        project_root: impl Into<PathBuf>,
        max_concurrency: usize,
    ) -> Result<Self, RuntimeError> {
        let project_root = std::fs::canonicalize(project_root.into())
            .map_err(|_| RuntimeError::InvalidConfiguration)?;
        if !project_root.is_dir() {
            return Err(RuntimeError::InvalidConfiguration);
        }
        let config = Self {
            node_binary: "node".to_owned(),
            project_root,
            max_concurrency,
            queue_timeout: Duration::from_secs(2),
            heap_megabytes: 256,
            max_output_bytes: MAX_OUTPUT_BYTES,
        };
        config.validate()?;
        Ok(config)
    }

    /// Overrides the Node executable resolved by the local process.
    #[must_use]
    pub fn with_node_binary(mut self, binary: impl Into<String>) -> Self {
        self.node_binary = binary.into();
        self
    }

    /// Validates local process and resource bounds.
    ///
    /// # Errors
    ///
    /// Rejects empty commands, invalid roots, and unsafe dimensions.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.node_binary.is_empty()
            || !self.project_root.is_absolute()
            || !self.project_root.is_dir()
            || !(1..=MAX_CONCURRENCY).contains(&self.max_concurrency)
            || self.queue_timeout.is_zero()
            || !(64..=4096).contains(&self.heap_megabytes)
            || !(1..=MAX_OUTPUT_BYTES).contains(&self.max_output_bytes)
        {
            return Err(RuntimeError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Full Node executor that uses the developer machine's installed Node binary.
pub struct LocalNodeRuntime {
    config: LocalNodeRuntimeConfig,
    permits: Arc<Semaphore>,
    node_ready: OnceCell<()>,
}

impl std::fmt::Debug for LocalNodeRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalNodeRuntime")
            .field("config", &self.config)
            .field("node_checked", &self.node_ready.initialized())
            .finish_non_exhaustive()
    }
}

impl LocalNodeRuntime {
    /// Creates a lazy local executor. Node availability/version is checked on first invocation.
    ///
    /// # Errors
    ///
    /// Rejects invalid local paths or process bounds.
    pub fn new(config: LocalNodeRuntimeConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.max_concurrency)),
            config,
            node_ready: OnceCell::new(),
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
        let prepared = prepare_request(request);
        let encoded_bytes = prepared
            .as_ref()
            .ok()
            .and_then(|prepared| u64::try_from(prepared.input.len()).ok());
        crate::performance::finish(validation_timer, &prepared, encoded_bytes, None);
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
        let source_path = self
            .write_source(request.invocation_id(), &prepared.source)
            .await;
        crate::performance::finish(create_timer, &source_path, None, None);
        let source_path = source_path?;
        let runner_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::NodeProcess,
                PerformanceOperation::ExecuteRunner,
                u64::try_from(prepared.input.len()).ok(),
            )
        });
        let outcome = self
            .run_process(
                request,
                &source_path,
                &prepared.export_name,
                prepared.input,
                deadline,
            )
            .await;
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
        let cleanup = tokio::fs::remove_file(&source_path)
            .await
            .map_err(|_| RuntimeError::Unavailable);
        crate::performance::finish(cleanup_timer, &cleanup, None, None);
        drop(permit);
        let outcome = outcome?;
        prepared
            .result_contract
            .validate_value(&outcome.value)
            .map_err(|_| RuntimeError::InvalidResult)?;
        Ok(outcome)
    }

    async fn write_source(
        &self,
        invocation_id: runku_core::InvocationId,
        source: &str,
    ) -> Result<PathBuf, RuntimeError> {
        let directory = self.config.project_root.join(".runku/node-runtime-v1");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        let directory = tokio::fs::canonicalize(directory)
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        if !directory.starts_with(&self.config.project_root) {
            return Err(RuntimeError::InvalidConfiguration);
        }
        let path = directory.join(format!("{invocation_id}.mjs"));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        file.write_all(source.as_bytes())
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        file.sync_all()
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        Ok(path)
    }

    #[allow(clippy::too_many_lines)]
    async fn run_process(
        &self,
        request: &InvocationRequest,
        source_path: &std::path::Path,
        export_name: &str,
        input: Vec<u8>,
        deadline: tokio::time::Instant,
    ) -> Result<FullNodeActionOutcome, RuntimeError> {
        let mut child = Command::new(&self.config.node_binary)
            .arg(format!(
                "--max-old-space-size={}",
                self.config.heap_megabytes
            ))
            .args(["--input-type=module", "--eval", LOCAL_RUNNER])
            .arg(source_path)
            .arg(export_name)
            .current_dir(&self.config.project_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| RuntimeError::Unavailable)?;
        let mut stdout = child.stdout.take().ok_or(RuntimeError::Internal)?;
        let stderr = child.stderr.take().ok_or(RuntimeError::Internal)?;
        let mut stdin = child.stdin.take().ok_or(RuntimeError::Internal)?;
        let limit = self.config.max_output_bytes;
        let stderr_task = tokio::spawn(read_bounded(stderr, limit));
        let cancellation = request.cancellation();
        write_frame(&mut stdin, &input, limit).await?;
        let mut platform_ops = 0_u64;
        let response = loop {
            let frame = tokio::select! {
                frame = read_frame(&mut stdout, limit) => frame,
                () = cancellation.cancelled() => {
                    terminate(&mut child).await;
                    return Err(RuntimeError::Cancelled);
                }
                () = tokio::time::sleep_until(deadline) => {
                    terminate(&mut child).await;
                    return Err(RuntimeError::DeadlineExceeded);
                }
            }?;
            let message = serde_json::from_slice::<NodeMessageV1>(&frame)
                .map_err(|_| RuntimeError::InvalidResult)?;
            if !matches!(&message, NodeMessageV1::Result { .. }) {
                platform_ops = platform_ops
                    .checked_add(1)
                    .ok_or(RuntimeError::InvalidInvocation)?;
                if platform_ops > MAX_PLATFORM_OPS_PER_INVOCATION {
                    terminate(&mut child).await;
                    return Err(RuntimeError::InvalidInvocation);
                }
            }
            match message {
                NodeMessageV1::Result { response } => break response,
                NodeMessageV1::FunctionCall {
                    call_id,
                    kind,
                    function,
                    arguments,
                } => {
                    let result =
                        handle_function_call(request, kind, function, arguments, deadline).await;
                    write_op_result(&mut stdin, call_id, result, limit).await?;
                }
                NodeMessageV1::Schedule {
                    call_id,
                    function,
                    arguments,
                    time,
                    idempotency_key,
                } => {
                    let result = handle_schedule(
                        request,
                        function,
                        arguments,
                        time,
                        idempotency_key,
                        deadline,
                    )
                    .await;
                    write_text_result(&mut stdin, call_id, result, limit).await?;
                }
                NodeMessageV1::StorageCreateUpload {
                    call_id,
                    max_bytes,
                    content_type,
                    sha256,
                } => {
                    let result = handle_storage_create_upload(
                        request,
                        max_bytes,
                        content_type,
                        sha256,
                        deadline,
                    )
                    .await;
                    write_json_result(&mut stdin, call_id, result, limit).await?;
                }
                NodeMessageV1::StorageStore {
                    call_id,
                    bytes,
                    content_type,
                    sha256,
                } => {
                    let result =
                        handle_storage_store(request, bytes, content_type, sha256, deadline).await;
                    write_json_result(&mut stdin, call_id, result, limit).await?;
                }
                NodeMessageV1::StorageMetadata { call_id, file_id } => {
                    let result = handle_storage_metadata(request, file_id, deadline).await;
                    write_json_result(&mut stdin, call_id, result, limit).await?;
                }
                NodeMessageV1::StorageCreateDownload {
                    call_id,
                    file_id,
                    expires_in_micros,
                } => {
                    let result = handle_storage_create_download(
                        request,
                        file_id,
                        expires_in_micros,
                        deadline,
                    )
                    .await;
                    write_json_result(&mut stdin, call_id, result, limit).await?;
                }
                NodeMessageV1::StorageGet { call_id, file_id } => {
                    let result = handle_storage_get(request, file_id, deadline).await;
                    write_json_result(&mut stdin, call_id, result, limit).await?;
                }
                NodeMessageV1::StorageDelete { call_id, file_id } => {
                    let result = handle_storage_delete(request, file_id, deadline).await;
                    write_json_result(&mut stdin, call_id, result, limit).await?;
                }
            }
        };
        stdin
            .shutdown()
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        let status = child.wait().await.map_err(|_| RuntimeError::Unavailable)?;
        let _stderr = stderr_task.await.map_err(|_| RuntimeError::Internal)??;
        if !status.success() {
            return Err(RuntimeError::JavaScript);
        }
        decode_response(&serde_json::to_vec(&response).map_err(|_| RuntimeError::Internal)?)
    }
}

#[async_trait]
impl FullNodeActionRuntime for LocalNodeRuntime {
    fn validate_manifest(&self, manifest: &ReleaseManifestV1) -> Result<(), RuntimeError> {
        manifest
            .ensure_local_full_node_supported()
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
    source: String,
    export_name: String,
    input: Vec<u8>,
    result_contract: Contract,
}

fn prepare_request(request: &InvocationRequest) -> Result<PreparedInvocation, RuntimeError> {
    request
        .manifest()
        .ensure_local_full_node_supported()
        .map_err(|_| RuntimeError::UnsupportedRuntime)?;
    let function = request
        .manifest()
        .functions
        .iter()
        .find(|function| function.id == request.function_id())
        .ok_or(RuntimeError::FunctionNotFound)?;
    if function.runtime_class != RuntimeClass::FullNode
        || function.function_type != FunctionType::Action
        || request.manifest().artifact.format != ArtifactFormat::NodeEsmBundleV1
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
    let bundle = decode_node_esm_bundle(artifact).map_err(|_| RuntimeError::InvalidArtifact)?;
    bundle
        .verify_manifest(request.manifest(), artifact)
        .map_err(|_| RuntimeError::InvalidArtifact)?;
    let arguments_contract = contract(&bundle, function.arguments_contract_hash)?;
    arguments_contract
        .validate_value(request.arguments())
        .map_err(|_| RuntimeError::InvalidArguments)?;
    let result_contract = contract(&bundle, function.result_contract_hash)?;
    let arguments = WireValueV1::from_canonical(request.arguments())
        .map_err(|_| RuntimeError::InvalidArguments)?;
    let input = serde_json::to_vec(&NodeRequestV1 {
        protocol_version: 1,
        collect_performance: request.performance().is_some(),
        release_id: request.release_id().to_string(),
        invocation_id: request.invocation_id().to_string(),
        function: function.name.as_str().to_owned(),
        capabilities: function.capabilities.iter().map(capability_name).collect(),
        arguments,
    })
    .map_err(|_| RuntimeError::Internal)?;
    let export_name = function
        .name
        .as_str()
        .rsplit('.')
        .next()
        .filter(|name| !name.is_empty())
        .ok_or(RuntimeError::InvalidInvocation)?
        .to_owned();
    Ok(PreparedInvocation {
        source: bundle
            .source(function.implementation_hash)
            .ok_or(RuntimeError::InvalidArtifact)?
            .to_owned(),
        export_name,
        input,
        result_contract,
    })
}

fn capability_name(capability: &runku_releases::Capability) -> String {
    match capability {
        runku_releases::Capability::DbRead => "db:read".to_owned(),
        runku_releases::Capability::DbWrite => "db:write".to_owned(),
        runku_releases::Capability::AuthRead => "auth:read".to_owned(),
        runku_releases::Capability::FunctionQuery => "function:query".to_owned(),
        runku_releases::Capability::FunctionMutation => "function:mutation".to_owned(),
        runku_releases::Capability::FunctionAction => "function:action".to_owned(),
        runku_releases::Capability::NetworkHttps => "network:https".to_owned(),
        runku_releases::Capability::SchedulerCreate => "scheduler:create".to_owned(),
        runku_releases::Capability::FileRead => "storage:read".to_owned(),
        runku_releases::Capability::FileWrite => "storage:write".to_owned(),
        runku_releases::Capability::Secret(name) => format!("secret:{name}"),
    }
}

fn contract(
    bundle: &runku_releases::NodeEsmBundleV1,
    digest: Sha256Digest,
) -> Result<Contract, RuntimeError> {
    let bytes = bundle
        .resource(digest)
        .ok_or(RuntimeError::InvalidArtifact)?;
    decode_contract(bytes.as_bytes()).map_err(|_| RuntimeError::InvalidArtifact)
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

fn supported_node_version(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.trim().strip_prefix('v'))
        .and_then(|value| value.split('.').next())
        .and_then(|value| value.parse::<u16>().ok())
        .is_some_and(|major| major >= 20)
}

async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
    limit: usize,
) -> Result<(), RuntimeError> {
    if bytes.is_empty() || bytes.len() > limit {
        return Err(RuntimeError::InvalidInvocation);
    }
    let length = u32::try_from(bytes.len()).map_err(|_| RuntimeError::InvalidInvocation)?;
    writer
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    writer
        .write_all(bytes)
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    writer.flush().await.map_err(|_| RuntimeError::Unavailable)
}

async fn read_frame(
    reader: &mut (impl AsyncRead + Unpin),
    limit: usize,
) -> Result<Vec<u8>, RuntimeError> {
    let mut header = [0_u8; 4];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|_| RuntimeError::InvalidResult)?;
    let length =
        usize::try_from(u32::from_be_bytes(header)).map_err(|_| RuntimeError::InvalidResult)?;
    if length == 0 || length > limit {
        return Err(RuntimeError::InvalidResult);
    }
    let mut frame = vec![0_u8; length];
    reader
        .read_exact(&mut frame)
        .await
        .map_err(|_| RuntimeError::InvalidResult)?;
    Ok(frame)
}

async fn handle_function_call(
    request: &InvocationRequest,
    kind: String,
    function: String,
    arguments: WireValueV1,
    deadline: tokio::time::Instant,
) -> Result<runku_value::CanonicalValue, String> {
    let kind = match kind.as_str() {
        "query" => FunctionCallKind::Query,
        "mutation" => FunctionCallKind::Mutation,
        "action" => FunctionCallKind::Action,
        _ => return Err("FUNCTION_CALL_INVALID".to_owned()),
    };
    let function = function
        .parse()
        .map_err(|_| "FUNCTION_CALL_INVALID".to_owned())?;
    let arguments = arguments
        .into_canonical()
        .map_err(|_| "FUNCTION_CALL_INVALID".to_owned())?;
    request
        .function_invoker()
        .ok_or_else(|| "FUNCTION_CALL_DENIED".to_owned())?
        .invoke(
            FunctionCallRequest {
                kind,
                function,
                arguments,
            },
            deadline.into_std(),
            request.cancellation(),
        )
        .await
        .map_err(|error| error.code().to_owned())
}

async fn handle_schedule(
    request: &InvocationRequest,
    function: String,
    arguments: WireValueV1,
    time: NodeScheduleTimeV1,
    idempotency_key: Option<String>,
    deadline: tokio::time::Instant,
) -> Result<String, String> {
    let function = function
        .parse()
        .map_err(|_| "SCHEDULE_REQUEST_INVALID".to_owned())?;
    let arguments = arguments
        .into_canonical()
        .map_err(|_| "SCHEDULE_REQUEST_INVALID".to_owned())?;
    let time = match time {
        NodeScheduleTimeV1::After { micros } => ScheduleTime::AfterMicros(
            micros
                .parse()
                .map_err(|_| "SCHEDULE_REQUEST_INVALID".to_owned())?,
        ),
        NodeScheduleTimeV1::At { micros } => ScheduleTime::At(TimestampMicros::new(
            micros
                .parse()
                .map_err(|_| "SCHEDULE_REQUEST_INVALID".to_owned())?,
        )),
    };
    request
        .schedule_creator()
        .ok_or_else(|| "SCHEDULE_UNAVAILABLE".to_owned())?
        .create(
            ScheduleRequest {
                function,
                arguments,
                time,
                idempotency_key,
            },
            deadline.into_std(),
            request.cancellation(),
        )
        .await
        .map(|id| id.to_string())
        .map_err(|error| error.code().to_owned())
}

async fn handle_storage_create_upload(
    request: &InvocationRequest,
    max_bytes: u64,
    content_type: Option<String>,
    sha256: Option<String>,
    deadline: tokio::time::Instant,
) -> Result<serde_json::Value, String> {
    require_storage_capability(request, &Capability::FileWrite)?;
    let value = request
        .file_storage()
        .ok_or_else(|| "FILE_STORAGE_FORBIDDEN".to_owned())?
        .create_upload_grant(
            FileUploadGrantRequest {
                max_bytes,
                content_type,
                sha256,
            },
            deadline.into_std(),
            request.cancellation(),
        )
        .await
        .map_err(|error| error.code().to_owned())?;
    serde_json::to_value(value).map_err(|_| "FILE_STORAGE_UNAVAILABLE".to_owned())
}

async fn handle_storage_store(
    request: &InvocationRequest,
    bytes: String,
    content_type: Option<String>,
    sha256: Option<String>,
    deadline: tokio::time::Instant,
) -> Result<serde_json::Value, String> {
    require_storage_capability(request, &Capability::FileWrite)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(bytes)
        .map_err(|_| "FILE_STORAGE_REQUEST_INVALID".to_owned())?;
    let value = request
        .file_storage()
        .ok_or_else(|| "FILE_STORAGE_FORBIDDEN".to_owned())?
        .store(
            FileStoreRequest {
                bytes,
                content_type,
                sha256,
            },
            deadline.into_std(),
            request.cancellation(),
        )
        .await
        .map_err(|error| error.code().to_owned())?;
    serde_json::to_value(value).map_err(|_| "FILE_STORAGE_UNAVAILABLE".to_owned())
}

async fn handle_storage_metadata(
    request: &InvocationRequest,
    file_id: String,
    deadline: tokio::time::Instant,
) -> Result<serde_json::Value, String> {
    require_storage_capability(request, &Capability::FileRead)?;
    let value = request
        .file_storage()
        .ok_or_else(|| "FILE_STORAGE_FORBIDDEN".to_owned())?
        .metadata(file_id, deadline.into_std(), request.cancellation())
        .await
        .map_err(|error| error.code().to_owned())?;
    serde_json::to_value(value).map_err(|_| "FILE_STORAGE_UNAVAILABLE".to_owned())
}

async fn handle_storage_create_download(
    request: &InvocationRequest,
    file_id: String,
    expires_in_micros: String,
    deadline: tokio::time::Instant,
) -> Result<serde_json::Value, String> {
    require_storage_capability(request, &Capability::FileRead)?;
    let value = request
        .file_storage()
        .ok_or_else(|| "FILE_STORAGE_FORBIDDEN".to_owned())?
        .create_download_grant(
            FileDownloadGrantRequest {
                file_id,
                expires_in_micros,
            },
            deadline.into_std(),
            request.cancellation(),
        )
        .await
        .map_err(|error| error.code().to_owned())?;
    serde_json::to_value(value).map_err(|_| "FILE_STORAGE_UNAVAILABLE".to_owned())
}

async fn handle_storage_get(
    request: &InvocationRequest,
    file_id: String,
    deadline: tokio::time::Instant,
) -> Result<serde_json::Value, String> {
    require_storage_capability(request, &Capability::FileRead)?;
    let value = request
        .file_storage()
        .ok_or_else(|| "FILE_STORAGE_FORBIDDEN".to_owned())?
        .get(file_id, deadline.into_std(), request.cancellation())
        .await
        .map_err(|error| error.code().to_owned())?;
    Ok(serde_json::json!({
        "metadata": value.metadata,
        "bytes": URL_SAFE_NO_PAD.encode(value.bytes),
    }))
}

async fn handle_storage_delete(
    request: &InvocationRequest,
    file_id: String,
    deadline: tokio::time::Instant,
) -> Result<serde_json::Value, String> {
    require_storage_capability(request, &Capability::FileWrite)?;
    request
        .file_storage()
        .ok_or_else(|| "FILE_STORAGE_FORBIDDEN".to_owned())?
        .delete(file_id, deadline.into_std(), request.cancellation())
        .await
        .map_err(|error| error.code().to_owned())?;
    Ok(serde_json::Value::Null)
}

fn require_storage_capability(
    request: &InvocationRequest,
    required: &Capability,
) -> Result<(), String> {
    request
        .manifest()
        .functions
        .iter()
        .find(|function| function.id == request.function_id())
        .is_some_and(|function| function.capabilities.contains(required))
        .then_some(())
        .ok_or_else(|| "FILE_STORAGE_FORBIDDEN".to_owned())
}

async fn write_op_result(
    writer: &mut (impl AsyncWrite + Unpin),
    call_id: u64,
    result: Result<runku_value::CanonicalValue, String>,
    limit: usize,
) -> Result<(), RuntimeError> {
    let (ok, value, error) = match result {
        Ok(value) => (
            true,
            Some(WireValueV1::from_canonical(&value).map_err(|_| RuntimeError::Internal)?),
            None,
        ),
        Err(error) => (false, None, Some(error)),
    };
    let bytes = serde_json::to_vec(&NodeOpResultV1 {
        r#type: "opResult",
        call_id,
        ok,
        value,
        text: None,
        json: None,
        error,
    })
    .map_err(|_| RuntimeError::Internal)?;
    write_frame(writer, &bytes, limit).await
}

async fn write_text_result(
    writer: &mut (impl AsyncWrite + Unpin),
    call_id: u64,
    result: Result<String, String>,
    limit: usize,
) -> Result<(), RuntimeError> {
    let (ok, text, error) = match result {
        Ok(text) => (true, Some(text), None),
        Err(error) => (false, None, Some(error)),
    };
    let bytes = serde_json::to_vec(&NodeOpResultV1 {
        r#type: "opResult",
        call_id,
        ok,
        value: None,
        text,
        json: None,
        error,
    })
    .map_err(|_| RuntimeError::Internal)?;
    write_frame(writer, &bytes, limit).await
}

async fn write_json_result(
    writer: &mut (impl AsyncWrite + Unpin),
    call_id: u64,
    result: Result<serde_json::Value, String>,
    limit: usize,
) -> Result<(), RuntimeError> {
    let (ok, json, error) = match result {
        Ok(json) => (true, Some(json), None),
        Err(error) => (false, None, Some(error)),
    };
    let bytes = serde_json::to_vec(&NodeOpResultV1 {
        r#type: "opResult",
        call_id,
        ok,
        value: None,
        text: None,
        json,
        error,
    })
    .map_err(|_| RuntimeError::Internal)?;
    write_frame(writer, &bytes, limit).await
}

async fn read_bounded(
    reader: impl AsyncRead + Unpin,
    limit: usize,
) -> Result<Vec<u8>, RuntimeError> {
    let maximum = u64::try_from(limit)
        .map_err(|_| RuntimeError::Internal)?
        .saturating_add(1);
    let mut reader = reader.take(maximum);
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

async fn terminate(child: &mut Child) {
    let _ = child.kill().await;
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeRequestV1 {
    protocol_version: u8,
    collect_performance: bool,
    release_id: String,
    invocation_id: String,
    function: String,
    capabilities: Vec<String>,
    arguments: WireValueV1,
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum NodeMessageV1 {
    Result {
        response: NodeResponseV1,
    },
    FunctionCall {
        call_id: u64,
        kind: String,
        function: String,
        arguments: WireValueV1,
    },
    Schedule {
        call_id: u64,
        function: String,
        arguments: WireValueV1,
        time: NodeScheduleTimeV1,
        idempotency_key: Option<String>,
    },
    StorageCreateUpload {
        call_id: u64,
        max_bytes: u64,
        content_type: Option<String>,
        sha256: Option<String>,
    },
    StorageStore {
        call_id: u64,
        bytes: String,
        content_type: Option<String>,
        sha256: Option<String>,
    },
    StorageMetadata {
        call_id: u64,
        file_id: String,
    },
    StorageCreateDownload {
        call_id: u64,
        file_id: String,
        expires_in_micros: String,
    },
    StorageGet {
        call_id: u64,
        file_id: String,
    },
    StorageDelete {
        call_id: u64,
        file_id: String,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum NodeScheduleTimeV1 {
    After { micros: String },
    At { micros: String },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeOpResultV1 {
    r#type: &'static str,
    call_id: u64,
    ok: bool,
    value: Option<WireValueV1>,
    text: Option<String>,
    json: Option<serde_json::Value>,
    error: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeResponseV1 {
    protocol_version: u8,
    ok: bool,
    value: Option<WireValueV1>,
    error: Option<NodeErrorV1>,
    performance: Option<NodePerformanceV1>,
}

#[derive(Deserialize, Serialize)]
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

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeErrorV1 {
    code: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_version_policy_accepts_supported_majors_only() {
        assert!(supported_node_version(b"v20.18.1\n"));
        assert!(supported_node_version(b"v22.13.0\n"));
        assert!(!supported_node_version(b"v18.20.0\n"));
        assert!(!supported_node_version(b"not-node\n"));
    }
}
