//! One-isolate invocation executor owned by bounded supervisor workers.

use std::{
    borrow::Cow,
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    thread,
    time::Instant,
};

use deno_core::{
    Extension, ExtensionFileSource, JsRuntime, NoopModuleLoader, OpDecl, OpState,
    PollEventLoopOptions, RuntimeOptions, op2, serde_v8, v8,
};
use runku_contracts::{Contract, DocumentSchemaV1, decode_contract, decode_document_schema};
use runku_identity::{ApplicationAssurance, PrincipalContext, PrincipalKind, RequestIdentity};
use runku_observability::{
    InvocationPerformanceTimer, LogLevel, PerformanceComponent, PerformanceOperation,
    PerformanceOutcome, PerformanceResourceUsage,
};
use runku_releases::{
    Capability, FunctionManifest, FunctionType, RuntimeClass, decode_safe_esm_bundle,
};
use runku_value::{CanonicalValue, TimestampMicros, encode_stored_value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    DataDocument, DataGetRequest, DataIndexEntry, DataRead, DataScanRequest, DataWrite,
    FunctionCallError, FunctionCallKind, FunctionCallRequest, FunctionInvoke, HttpsEgress,
    HttpsRequest, HttpsResponse, RuntimeError, ScheduleCreate, ScheduleRequest, ScheduleTime,
    invocation::{InvocationRequest, RuntimeLimits},
    logging::InvocationLogContext,
    value_bridge::{WireValue, from_wire, to_wire},
};

const TERMINATION_NONE: u8 = 0;
const TERMINATION_DEADLINE: u8 = 1;
const TERMINATION_CANCELLED: u8 = 2;
const TERMINATION_HEAP: u8 = 3;

#[derive(Debug)]
struct OpBudget {
    used: AtomicU64,
    maximum: u64,
}

#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
struct PlatformState {
    budget: OpBudget,
    function_type: FunctionType,
    network_https: bool,
    https: Option<Arc<dyn HttpsEgress>>,
    data_read: bool,
    data: Option<Arc<dyn DataRead>>,
    data_write: bool,
    writer: Option<Arc<dyn DataWrite>>,
    document_schema: Option<Arc<DocumentSchemaV1>>,
    scheduling: bool,
    scheduler: Option<Arc<dyn ScheduleCreate>>,
    function_query: bool,
    function_mutation: bool,
    function_action: bool,
    functions: Option<Arc<dyn FunctionInvoke>>,
    logs: Option<Arc<InvocationLogContext>>,
    telemetry: Option<Arc<crate::invocation::RuntimeTelemetry>>,
    deadline: Instant,
    cancellation: crate::CancellationToken,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireDataDocument {
    table_id: String,
    document_id: String,
    revision: String,
    commit_sequence: String,
    created_at: String,
    updated_at: String,
    value: WireValue,
}

impl From<DataDocument> for WireDataDocument {
    fn from(value: DataDocument) -> Self {
        Self {
            table_id: value.table_id.to_string(),
            document_id: value.document_id.to_string(),
            revision: value.revision.to_string(),
            commit_sequence: value.commit_sequence.to_string(),
            created_at: value.created_at.get().to_string(),
            updated_at: value.updated_at.get().to_string(),
            value: to_wire(&value.value),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireDataIndexEntry {
    index_id: String,
    key: Vec<u8>,
    table_id: String,
    document_id: String,
    document_revision: String,
    commit_sequence: String,
}

impl From<DataIndexEntry> for WireDataIndexEntry {
    fn from(value: DataIndexEntry) -> Self {
        Self {
            index_id: value.index_id.to_string(),
            key: value.key,
            table_id: value.table_id.to_string(),
            document_id: value.document_id.to_string(),
            document_revision: value.document_revision.to_string(),
            commit_sequence: value.commit_sequence.to_string(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireDataWrite {
    table_id: runku_core::TableId,
    document_id: runku_core::DocumentId,
    value: WireValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireDocumentId {
    table_id: runku_core::TableId,
    stable_key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireDataRevisionWrite {
    table_id: runku_core::TableId,
    document_id: runku_core::DocumentId,
    expected_revision: String,
    value: WireValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireDataDelete {
    table_id: runku_core::TableId,
    document_id: runku_core::DocumentId,
    expected_revision: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireSchedule {
    function: String,
    arguments: WireValue,
    time_micros: String,
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireFunctionCall {
    function: String,
    arguments: WireValue,
}

#[derive(Deserialize)]
struct WireFunctionLog {
    level: String,
    message: String,
    fields: Option<WireValue>,
}

impl OpBudget {
    fn take(&self) -> Result<(), deno_error::JsErrorBox> {
        let used = self.used.fetch_add(1, Ordering::Relaxed) + 1;
        if used > self.maximum {
            return Err(deno_error::JsErrorBox::generic(
                "Runku Platform Op budget exceeded",
            ));
        }
        Ok(())
    }
}

#[op2]
async fn op_runku_cooperate(state: Rc<RefCell<OpState>>) -> Result<(), deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    tokio::task::yield_now().await;
    Ok(())
}

#[op2]
#[serde]
async fn op_runku_https(
    state: Rc<RefCell<OpState>>,
    #[serde] request: HttpsRequest,
) -> Result<HttpsResponse, deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    if platform.function_type != FunctionType::Action || !platform.network_https {
        return Err(deno_error::JsErrorBox::generic("HTTPS_CAPABILITY_DENIED"));
    }
    let https = platform
        .https
        .as_ref()
        .ok_or_else(|| deno_error::JsErrorBox::generic("HTTPS_BROKER_UNAVAILABLE"))?;
    https
        .execute(request, platform.deadline, platform.cancellation.clone())
        .await
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

#[op2]
#[serde]
async fn op_runku_data_get(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DataGetRequest,
) -> Result<Option<WireDataDocument>, deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    if !matches!(
        platform.function_type,
        FunctionType::Query | FunctionType::Mutation
    ) || !platform.data_read
    {
        return Err(deno_error::JsErrorBox::generic("DATA_CAPABILITY_DENIED"));
    }
    let data = platform
        .data
        .as_ref()
        .ok_or_else(|| deno_error::JsErrorBox::generic("DATA_BROKER_UNAVAILABLE"))?;
    data.get(request, platform.deadline, platform.cancellation.clone())
        .await
        .map(|document| document.map(Into::into))
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

#[op2]
#[serde]
async fn op_runku_data_scan(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DataScanRequest,
) -> Result<Vec<WireDataIndexEntry>, deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    if platform.function_type != FunctionType::Query || !platform.data_read {
        return Err(deno_error::JsErrorBox::generic("DATA_CAPABILITY_DENIED"));
    }
    let data = platform
        .data
        .as_ref()
        .ok_or_else(|| deno_error::JsErrorBox::generic("DATA_BROKER_UNAVAILABLE"))?;
    data.scan(request, platform.deadline, platform.cancellation.clone())
        .await
        .map(|entries| entries.into_iter().map(Into::into).collect())
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

fn data_writer(platform: &PlatformState) -> Result<&Arc<dyn DataWrite>, deno_error::JsErrorBox> {
    if platform.function_type != FunctionType::Mutation || !platform.data_write {
        return Err(deno_error::JsErrorBox::generic(
            "DATA_WRITE_CAPABILITY_DENIED",
        ));
    }
    platform
        .writer
        .as_ref()
        .ok_or_else(|| deno_error::JsErrorBox::generic("DATA_WRITE_BROKER_UNAVAILABLE"))
}

fn positive_revision(value: &str) -> Result<u64, deno_error::JsErrorBox> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| deno_error::JsErrorBox::generic("DATA_WRITE_REVISION_INVALID"))
}

#[op2]
async fn op_runku_data_insert(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireDataWrite,
) -> Result<(), deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    let writer = data_writer(&platform)?;
    let value = from_wire(request.value)
        .map_err(|_| deno_error::JsErrorBox::generic("DATA_WRITE_VALUE_INVALID"))?;
    validate_document(&platform, request.table_id, &value)?;
    writer
        .insert(request.table_id, request.document_id, value)
        .await
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

#[op2]
#[string]
#[allow(clippy::needless_pass_by_value)]
fn op_runku_data_document_id(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireDocumentId,
) -> Result<String, deno_error::JsErrorBox> {
    const DOMAIN: &[u8] = b"RUNKU_DOCUMENT_ID_FROM_KEY_V1\0";
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    if !matches!(
        platform.function_type,
        FunctionType::Query | FunctionType::Mutation
    ) || !platform.data_read
    {
        return Err(deno_error::JsErrorBox::generic("DATA_CAPABILITY_DENIED"));
    }
    if request.stable_key.is_empty() || request.stable_key.len() > 1_024 {
        return Err(deno_error::JsErrorBox::generic(
            "DATA_DOCUMENT_ID_KEY_INVALID",
        ));
    }
    let schema = platform
        .document_schema
        .as_ref()
        .ok_or_else(|| deno_error::JsErrorBox::generic("SCHEMA_CONTRACT_UNAVAILABLE"))?;
    if !schema
        .tables
        .iter()
        .any(|table| table.id == request.table_id)
    {
        return Err(deno_error::JsErrorBox::generic("SCHEMA_TABLE_UNKNOWN"));
    }
    let mut hash = Sha256::new();
    hash.update(DOMAIN);
    hash.update(request.table_id.to_string().as_bytes());
    hash.update(
        u32::try_from(request.stable_key.len())
            .map_err(|_| deno_error::JsErrorBox::generic("DATA_DOCUMENT_ID_KEY_INVALID"))?
            .to_be_bytes(),
    );
    hash.update(request.stable_key.as_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    // Canonical ULIDs reserve the two most-significant bits so their first character is 0..7.
    bytes[0] &= 0x3f;
    Ok(runku_core::DocumentId::from_ulid(Ulid::from(u128::from_be_bytes(bytes))).to_string())
}

#[op2]
async fn op_runku_data_replace(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireDataRevisionWrite,
) -> Result<(), deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    let writer = data_writer(&platform)?;
    let revision = positive_revision(&request.expected_revision)?;
    let value = from_wire(request.value)
        .map_err(|_| deno_error::JsErrorBox::generic("DATA_WRITE_VALUE_INVALID"))?;
    validate_document(&platform, request.table_id, &value)?;
    writer
        .replace(request.table_id, request.document_id, revision, value)
        .await
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

fn validate_document(
    platform: &PlatformState,
    table_id: runku_core::TableId,
    value: &CanonicalValue,
) -> Result<(), deno_error::JsErrorBox> {
    if let Some(schema) = &platform.document_schema {
        schema
            .validate_document(table_id, value)
            .map_err(|_| deno_error::JsErrorBox::generic("DATA_WRITE_DOCUMENT_INVALID"))?;
    }
    Ok(())
}

#[op2]
async fn op_runku_data_delete(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireDataDelete,
) -> Result<(), deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    let writer = data_writer(&platform)?;
    writer
        .delete(
            request.table_id,
            request.document_id,
            positive_revision(&request.expected_revision)?,
        )
        .await
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

fn schedule_broker(
    platform: &PlatformState,
) -> Result<&Arc<dyn ScheduleCreate>, deno_error::JsErrorBox> {
    if !matches!(
        platform.function_type,
        FunctionType::Mutation | FunctionType::Action
    ) || !platform.scheduling
    {
        return Err(deno_error::JsErrorBox::generic(
            "SCHEDULE_CAPABILITY_DENIED",
        ));
    }
    platform
        .scheduler
        .as_ref()
        .ok_or_else(|| deno_error::JsErrorBox::generic("SCHEDULE_BROKER_UNAVAILABLE"))
}

fn decode_schedule(
    request: WireSchedule,
    time: ScheduleTime,
) -> Result<ScheduleRequest, deno_error::JsErrorBox> {
    Ok(ScheduleRequest {
        function: request
            .function
            .parse()
            .map_err(|_| deno_error::JsErrorBox::generic("SCHEDULE_REQUEST_INVALID"))?,
        arguments: from_wire(request.arguments)
            .map_err(|_| deno_error::JsErrorBox::generic("SCHEDULE_REQUEST_INVALID"))?,
        time,
        idempotency_key: request.idempotency_key,
    })
}

#[op2]
#[string]
async fn op_runku_schedule_after(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireSchedule,
) -> Result<String, deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    let delay = request
        .time_micros
        .parse::<u64>()
        .map_err(|_| deno_error::JsErrorBox::generic("SCHEDULE_REQUEST_INVALID"))?;
    let broker = schedule_broker(&platform)?;
    broker
        .create(
            decode_schedule(request, ScheduleTime::AfterMicros(delay))?,
            platform.deadline,
            platform.cancellation.clone(),
        )
        .await
        .map(|id| id.to_string())
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

#[op2]
#[string]
async fn op_runku_schedule_at(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireSchedule,
) -> Result<String, deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    let timestamp = request
        .time_micros
        .parse::<i64>()
        .map(TimestampMicros::new)
        .map_err(|_| deno_error::JsErrorBox::generic("SCHEDULE_REQUEST_INVALID"))?;
    let broker = schedule_broker(&platform)?;
    broker
        .create(
            decode_schedule(request, ScheduleTime::At(timestamp))?,
            platform.deadline,
            platform.cancellation.clone(),
        )
        .await
        .map(|id| id.to_string())
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

async fn invoke_function(
    state: Rc<RefCell<OpState>>,
    request: WireFunctionCall,
    kind: FunctionCallKind,
) -> Result<WireValue, deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    if let Some(telemetry) = &platform.telemetry {
        telemetry.function_call();
    }
    let result = async {
        let allowed = match kind {
            FunctionCallKind::Query => platform.function_query,
            FunctionCallKind::Mutation => platform.function_mutation,
            FunctionCallKind::Action => platform.function_action,
        };
        if !allowed {
            return Err(FunctionCallError::Denied);
        }
        let functions = platform
            .functions
            .as_ref()
            .ok_or(FunctionCallError::Unavailable)?;
        let function = request
            .function
            .parse()
            .map_err(|_| FunctionCallError::InvalidRequest)?;
        let arguments =
            from_wire(request.arguments).map_err(|_| FunctionCallError::InvalidRequest)?;
        functions
            .invoke(
                FunctionCallRequest {
                    kind,
                    function,
                    arguments,
                },
                platform.deadline,
                platform.cancellation.clone(),
            )
            .await
    }
    .await;
    if let Some(telemetry) = &platform.telemetry {
        telemetry.function_call_result(&result);
    }
    result
        .map(|value| to_wire(&value))
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

#[op2]
#[serde]
async fn op_runku_function_query(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireFunctionCall,
) -> Result<WireValue, deno_error::JsErrorBox> {
    invoke_function(state, request, FunctionCallKind::Query).await
}

#[op2]
#[serde]
async fn op_runku_function_mutation(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireFunctionCall,
) -> Result<WireValue, deno_error::JsErrorBox> {
    invoke_function(state, request, FunctionCallKind::Mutation).await
}

#[op2]
#[serde]
async fn op_runku_function_action(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireFunctionCall,
) -> Result<WireValue, deno_error::JsErrorBox> {
    invoke_function(state, request, FunctionCallKind::Action).await
}

#[op2]
#[allow(clippy::needless_pass_by_value)]
fn op_runku_log(
    state: Rc<RefCell<OpState>>,
    #[serde] request: WireFunctionLog,
) -> Result<(), deno_error::JsErrorBox> {
    let platform = state.borrow().borrow::<Arc<PlatformState>>().clone();
    platform.budget.take()?;
    let logs = platform
        .logs
        .as_ref()
        .ok_or_else(|| deno_error::JsErrorBox::generic("LOG_UNAVAILABLE"))?;
    let level = match request.level.as_str() {
        "debug" => LogLevel::Debug,
        "info" => LogLevel::Info,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => return Err(deno_error::JsErrorBox::generic("LOG_LEVEL_INVALID")),
    };
    let fields = request
        .fields
        .map(from_wire)
        .transpose()
        .map_err(|_| deno_error::JsErrorBox::generic("LOG_FIELDS_INVALID"))?;
    logs.function(level, request.message, fields)
        .map_err(|error| deno_error::JsErrorBox::generic(error.code()))
}

fn platform_extension() -> Extension {
    const COOPERATE_OP: OpDecl = op_runku_cooperate();
    const HTTPS_OP: OpDecl = op_runku_https();
    const DATA_GET_OP: OpDecl = op_runku_data_get();
    const DATA_SCAN_OP: OpDecl = op_runku_data_scan();
    const DATA_INSERT_OP: OpDecl = op_runku_data_insert();
    const DATA_DOCUMENT_ID_OP: OpDecl = op_runku_data_document_id();
    const DATA_REPLACE_OP: OpDecl = op_runku_data_replace();
    const DATA_DELETE_OP: OpDecl = op_runku_data_delete();
    const SCHEDULE_AFTER_OP: OpDecl = op_runku_schedule_after();
    const SCHEDULE_AT_OP: OpDecl = op_runku_schedule_at();
    const FUNCTION_QUERY_OP: OpDecl = op_runku_function_query();
    const FUNCTION_MUTATION_OP: OpDecl = op_runku_function_mutation();
    const FUNCTION_ACTION_OP: OpDecl = op_runku_function_action();
    const LOG_OP: OpDecl = op_runku_log();
    const SOURCES: &[ExtensionFileSource] = &[ExtensionFileSource::new(
        "ext:runku_platform_js_1/runtime_bootstrap.js",
        deno_core::ascii_str_include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/runtime_bootstrap.js"
        )),
    )];
    Extension {
        name: "runku_platform_js_1",
        esm_files: Cow::Borrowed(SOURCES),
        esm_entry_point: Some("ext:runku_platform_js_1/runtime_bootstrap.js"),
        ops: Cow::Borrowed(&[
            COOPERATE_OP,
            HTTPS_OP,
            DATA_GET_OP,
            DATA_SCAN_OP,
            DATA_INSERT_OP,
            DATA_DOCUMENT_ID_OP,
            DATA_REPLACE_OP,
            DATA_DELETE_OP,
            SCHEDULE_AFTER_OP,
            SCHEDULE_AT_OP,
            FUNCTION_QUERY_OP,
            FUNCTION_MUTATION_OP,
            FUNCTION_ACTION_OP,
            LOG_OP,
        ]),
        ..Extension::default()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_excessive_bools)]
struct WireInvocationMetadata {
    project_id: String,
    environment_id: String,
    release_id: String,
    request_id: String,
    invocation_id: String,
    function_id: String,
    function_name: String,
    function_type: &'static str,
    capabilities: Vec<String>,
    https_enabled: bool,
    data_enabled: bool,
    data_write_enabled: bool,
    scheduler_enabled: bool,
    function_query_enabled: bool,
    function_mutation_enabled: bool,
    function_action_enabled: bool,
    auth_enabled: bool,
    auth: Option<WireAuthContext>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireAuthContext {
    application: Option<WireApplicationContext>,
    principal: Option<WirePrincipalContext>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WireApplicationContext {
    client_id: String,
    credential_id: String,
    assurance: &'static str,
    scopes: Vec<String>,
    configuration_revision: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WirePrincipalContext {
    id: String,
    kind: &'static str,
    provider_id: String,
    scopes: Vec<String>,
    auth_time: Option<String>,
    expires_at: Option<String>,
    mapping_revision: String,
}

pub(crate) async fn execute(
    request: InvocationRequest,
    limits: RuntimeLimits,
    deadline: Instant,
) -> Result<CanonicalValue, RuntimeError> {
    let validation_timer = request.performance().map(|recorder| {
        recorder.start(
            PerformanceComponent::Runtime,
            PerformanceOperation::Validate,
            u64::try_from(request.artifact_bytes().len()).ok(),
        )
    });
    let validated = validate_input(&request, limits, deadline);
    finish_runtime_timer(validation_timer, &validated, None);
    let validated = validated?;
    execute_validated(request, validated, limits, deadline).await
}

struct ValidatedInvocation {
    function: FunctionManifest,
    source: String,
    result_contract: Option<Contract>,
    document_schema: Option<Arc<DocumentSchemaV1>>,
}

fn validate_input(
    request: &InvocationRequest,
    limits: RuntimeLimits,
    deadline: Instant,
) -> Result<ValidatedInvocation, RuntimeError> {
    if request.wall_timeout > limits.max_wall_time {
        return Err(RuntimeError::InvalidInvocation);
    }
    if request.cancellation.is_cancelled() {
        return Err(RuntimeError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(RuntimeError::DeadlineExceeded);
    }
    if !matches!(
        request.manifest.runtime_version.as_str(),
        "platform-js-1" | "runku-js-1" | "runku-hybrid-1"
    ) {
        return Err(RuntimeError::UnsupportedRuntime);
    }
    let function = request
        .manifest
        .functions
        .iter()
        .find(|function| function.id == request.function_id)
        .cloned()
        .ok_or(RuntimeError::FunctionNotFound)?;
    if function.capabilities.contains(&Capability::AuthRead) && request.identity.is_none() {
        return Err(RuntimeError::InvalidInvocation);
    }
    if function.runtime_class != RuntimeClass::SafeV8 {
        return Err(RuntimeError::UnsupportedRuntime);
    }
    let resource_bytes: &[u8] = if request.manifest.artifact.format
        == runku_releases::ArtifactFormat::HybridOciArtifactV1
    {
        if request.manifest.artifact.size_bytes
            != u64::try_from(request.artifact_bytes.len())
                .map_err(|_| RuntimeError::InvalidArtifact)?
            || request.manifest.artifact.digest
                != runku_releases::Sha256Digest::of(&request.artifact_bytes)
        {
            return Err(RuntimeError::InvalidArtifact);
        }
        request
            .manifest
            .ensure_full_node_v1_supported()
            .map_err(|_| RuntimeError::UnsupportedRuntime)?;
        runku_releases::decode_hybrid_oci_artifact(&request.artifact_bytes)
            .map_err(|_| RuntimeError::InvalidArtifact)?
            .0
    } else {
        &request.artifact_bytes
    };
    let bundle =
        decode_safe_esm_bundle(resource_bytes).map_err(|_| RuntimeError::InvalidArtifact)?;
    if request.manifest.runtime_version.as_str() == "runku-hybrid-1" {
        let node_bundle = runku_releases::decode_node_esm_bundle(resource_bytes)
            .map_err(|_| RuntimeError::InvalidArtifact)?;
        if request.manifest.artifact.format == runku_releases::ArtifactFormat::NodeEsmBundleV1 {
            node_bundle
                .verify_manifest(&request.manifest, &request.artifact_bytes)
                .map_err(|_| RuntimeError::InvalidArtifact)?;
        } else {
            bundle
                .verify_resources(&request.manifest)
                .map_err(|_| RuntimeError::InvalidArtifact)?;
        }
    } else {
        bundle
            .verify_manifest(&request.manifest, &request.artifact_bytes)
            .map_err(|_| RuntimeError::InvalidArtifact)?;
    }
    let source = bundle
        .source(function.implementation_hash)
        .ok_or(RuntimeError::InvalidArtifact)?
        .to_owned();
    let (result_contract, document_schema) = if matches!(
        request.manifest.runtime_version.as_str(),
        "runku-js-1" | "runku-hybrid-1"
    ) {
        let arguments_contract = contract_resource(&bundle, function.arguments_contract_hash)?;
        arguments_contract
            .validate_value(&request.arguments)
            .map_err(|_| RuntimeError::InvalidArguments)?;
        let result_contract = contract_resource(&bundle, function.result_contract_hash)?;
        let schema_source = bundle
            .resource(request.manifest.schema_contract_hash)
            .ok_or(RuntimeError::InvalidArtifact)?;
        let schema = decode_document_schema(schema_source.as_bytes())
            .map_err(|_| RuntimeError::InvalidArtifact)?;
        (Some(result_contract), Some(Arc::new(schema)))
    } else {
        (None, None)
    };
    Ok(ValidatedInvocation {
        function,
        source,
        result_contract,
        document_schema,
    })
}

fn contract_resource(
    bundle: &runku_releases::SafeEsmBundleV1,
    digest: runku_releases::Sha256Digest,
) -> Result<Contract, RuntimeError> {
    let source = bundle
        .resource(digest)
        .ok_or(RuntimeError::InvalidArtifact)?;
    decode_contract(source.as_bytes()).map_err(|_| RuntimeError::InvalidArtifact)
}

#[allow(clippy::too_many_lines)]
async fn execute_validated(
    request: InvocationRequest,
    validated: ValidatedInvocation,
    limits: RuntimeLimits,
    deadline: Instant,
) -> Result<CanonicalValue, RuntimeError> {
    let function = validated.function;
    let logs = InvocationLogContext::new(&request, &function);
    let execution_started = Instant::now();
    if let Some(logs) = &logs {
        logs.started();
    }
    let result = async {
        let create_timer = request.performance().map(|recorder| {
            recorder.start(PerformanceComponent::V8, PerformanceOperation::Create, None)
        });
        let create_params = v8::Isolate::create_params().heap_limits(0, limits.heap_bytes);
        let runtime_result = JsRuntime::try_new(RuntimeOptions {
            module_loader: Some(Rc::new(NoopModuleLoader)),
            extensions: vec![platform_extension()],
            create_params: Some(create_params),
            ..RuntimeOptions::default()
        })
        .map_err(|_| RuntimeError::Internal);
        finish_runtime_timer(create_timer, &runtime_result, None);
        let mut runtime = runtime_result?;
        runtime.op_state().borrow_mut().put(platform_state(
            &request,
            &function,
            limits,
            deadline,
            validated.document_schema,
            logs.clone(),
        ));

        let termination = Arc::new(AtomicU8::new(TERMINATION_NONE));
        let completed = Arc::new(AtomicBool::new(false));
        let heap_handle = runtime.v8_isolate().thread_safe_handle();
        let heap_cause = Arc::clone(&termination);
        runtime.add_near_heap_limit_callback(move |current_limit, _initial_limit| {
            if heap_cause
                .compare_exchange(
                    TERMINATION_NONE,
                    TERMINATION_HEAP,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                heap_handle.terminate_execution();
            }
            current_limit.saturating_mul(2)
        });

        let watchdog_handle = runtime.v8_isolate().thread_safe_handle();
        let watchdog_cause = Arc::clone(&termination);
        let watchdog_completed = Arc::clone(&completed);
        let cancellation = request.cancellation.clone();
        let watchdog = thread::Builder::new()
            .name("runku-v8-watchdog".to_owned())
            .spawn(move || {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let cancelled = cancellation
                    .state()
                    .wait_until_changed_or(&watchdog_completed, remaining);
                if watchdog_completed.load(Ordering::Acquire) {
                    return;
                }
                let cause = if cancelled {
                    TERMINATION_CANCELLED
                } else {
                    TERMINATION_DEADLINE
                };
                if watchdog_cause
                    .compare_exchange(TERMINATION_NONE, cause, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    watchdog_handle.terminate_execution();
                }
            })
            .map_err(|_| RuntimeError::Internal)?;

        let execute_timer = request.performance().map(|recorder| {
            recorder.start(
                PerformanceComponent::V8,
                PerformanceOperation::Invocation,
                None,
            )
        });
        let cpu_started = thread_cpu_micros();
        let result = execute_in_isolate(
            &mut runtime,
            &request,
            &function,
            validated.source,
            validated.result_contract.as_ref(),
        )
        .await;
        let output_bytes = result.as_ref().ok().and_then(|value| {
            encode_stored_value(value)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok())
        });
        let resources = isolate_resource_usage(&mut runtime, cpu_started);
        finish_runtime_timer_with_resources(execute_timer, &result, output_bytes, Some(resources));
        completed.store(true, Ordering::Release);
        request.cancellation.state().wake();
        watchdog.join().map_err(|_| RuntimeError::Internal)?;

        match termination.load(Ordering::Acquire) {
            TERMINATION_NONE => result,
            TERMINATION_DEADLINE => Err(RuntimeError::DeadlineExceeded),
            TERMINATION_CANCELLED => Err(RuntimeError::Cancelled),
            TERMINATION_HEAP => Err(RuntimeError::HeapLimitExceeded),
            _ => Err(RuntimeError::Internal),
        }
    }
    .await;
    if let Some(logs) = &logs {
        logs.completed(&result, execution_started);
    }
    result
}

fn platform_state(
    request: &InvocationRequest,
    function: &FunctionManifest,
    limits: RuntimeLimits,
    deadline: Instant,
    document_schema: Option<Arc<DocumentSchemaV1>>,
    logs: Option<Arc<InvocationLogContext>>,
) -> Arc<PlatformState> {
    let network_https = function.function_type == FunctionType::Action
        && function.capabilities.contains(&Capability::NetworkHttps);
    let data_read = matches!(
        function.function_type,
        FunctionType::Query | FunctionType::Mutation
    ) && function.capabilities.contains(&Capability::DbRead);
    let data_write = function.function_type == FunctionType::Mutation
        && function.capabilities.contains(&Capability::DbWrite);
    Arc::new(PlatformState {
        budget: OpBudget {
            used: AtomicU64::new(0),
            maximum: limits.max_ops,
        },
        function_type: function.function_type,
        network_https,
        https: request.https.clone(),
        data_read,
        data: request.data.clone(),
        data_write,
        writer: request.data_write.clone(),
        document_schema,
        scheduling: matches!(
            function.function_type,
            FunctionType::Mutation | FunctionType::Action
        ) && function.capabilities.contains(&Capability::SchedulerCreate),
        scheduler: request.scheduler.clone(),
        function_query: function.capabilities.contains(&Capability::FunctionQuery),
        function_mutation: function
            .capabilities
            .contains(&Capability::FunctionMutation),
        function_action: function.capabilities.contains(&Capability::FunctionAction),
        functions: request.functions.clone(),
        logs,
        telemetry: request.telemetry.clone(),
        deadline,
        cancellation: request.cancellation.clone(),
    })
}

async fn execute_in_isolate(
    runtime: &mut JsRuntime,
    request: &InvocationRequest,
    function: &FunctionManifest,
    source: String,
    result_contract: Option<&Contract>,
) -> Result<CanonicalValue, RuntimeError> {
    let invoke = platform_function(runtime, "__runkuPlatformInvoke")?;
    let encode = platform_function(runtime, "__runkuPlatformEncode")?;
    runtime
        .execute_script(
            "runku:bootstrap/cleanup",
            "delete globalThis.__runkuPlatformInvoke; delete globalThis.__runkuPlatformEncode;",
        )
        .map_err(|_| RuntimeError::Internal)?;

    let specifier = deno_core::url::Url::parse(&format!(
        "runku:/implementation/{}.js",
        function.implementation_hash
    ))
    .map_err(|_| RuntimeError::Internal)?;
    let module_timer = request.performance().map(|recorder| {
        recorder.start(
            PerformanceComponent::V8,
            PerformanceOperation::LoadModule,
            u64::try_from(source.len()).ok(),
        )
    });
    let module_result = runtime
        .load_main_es_module_from_code(&specifier, source)
        .await
        .map_err(|_| RuntimeError::JavaScript);
    finish_runtime_timer(module_timer, &module_result, None);
    let module_id = module_result?;
    let evaluation = runtime.mod_evaluate(module_id);
    runtime
        .run_event_loop(PollEventLoopOptions::default())
        .await
        .map_err(|_| RuntimeError::JavaScript)?;
    evaluation.await.map_err(|_| RuntimeError::JavaScript)?;
    let handler = named_export(
        runtime,
        module_id,
        function.name.as_str(),
        request.manifest.runtime_version.as_str() == "platform-js-1",
    )?;

    let wire_arguments = to_wire(&request.arguments);
    let metadata = metadata(request, function);
    let handler_argument = function_as_value(runtime, &handler);
    let arguments_argument = serialize_value(runtime, &wire_arguments)?;
    let metadata_argument = serialize_value(runtime, &metadata)?;
    let call = runtime.call_with_args(
        &invoke,
        &[handler_argument, arguments_argument, metadata_argument],
    );
    let handler_timer = request.performance().map(|recorder| {
        recorder.start(
            PerformanceComponent::Function,
            PerformanceOperation::ExecuteHandler,
            None,
        )
    });
    let raw_result = runtime
        .with_event_loop_promise(call, PollEventLoopOptions::default())
        .await
        .map_err(|_| RuntimeError::JavaScript);
    finish_runtime_timer(handler_timer, &raw_result, None);
    let raw_result = raw_result?;

    let raw_argument = raw_result;
    let result_timer = request.performance().map(|recorder| {
        recorder.start(
            PerformanceComponent::Result,
            PerformanceOperation::EncodeResult,
            None,
        )
    });
    let encode_call = runtime.call_with_args(&encode, &[raw_argument]);
    let encoded_result = runtime
        .with_event_loop_promise(encode_call, PollEventLoopOptions::default())
        .await
        .map_err(|_| RuntimeError::InvalidResult)?;
    let wire_result: WireValue = deserialize_value(runtime, &encoded_result)?;
    let result = from_wire(wire_result)?;
    if let Some(contract) = result_contract {
        contract
            .validate_value(&result)
            .map_err(|_| RuntimeError::InvalidResult)?;
    }
    let output_bytes = encode_stored_value(&result)
        .ok()
        .and_then(|bytes| u64::try_from(bytes.len()).ok());
    finish_runtime_timer(result_timer, &Ok::<(), RuntimeError>(()), output_bytes);
    Ok(result)
}

fn finish_runtime_timer<T>(
    timer: Option<InvocationPerformanceTimer>,
    result: &Result<T, RuntimeError>,
    output_bytes: Option<u64>,
) {
    finish_runtime_timer_with_resources(timer, result, output_bytes, None);
}

fn finish_runtime_timer_with_resources<T>(
    timer: Option<InvocationPerformanceTimer>,
    result: &Result<T, RuntimeError>,
    output_bytes: Option<u64>,
    resources: Option<PerformanceResourceUsage>,
) {
    let Some(timer) = timer else { return };
    let (outcome, error_code) = match result {
        Ok(_) => (PerformanceOutcome::Succeeded, None),
        Err(RuntimeError::Busy) => (PerformanceOutcome::Busy, Some(RuntimeError::Busy.code())),
        Err(RuntimeError::DeadlineExceeded) => (
            PerformanceOutcome::DeadlineExceeded,
            Some(RuntimeError::DeadlineExceeded.code()),
        ),
        Err(RuntimeError::Cancelled) => (
            PerformanceOutcome::Cancelled,
            Some(RuntimeError::Cancelled.code()),
        ),
        Err(error) => (PerformanceOutcome::Failed, Some(error.code())),
    };
    timer.finish(outcome, error_code, output_bytes, resources);
}

fn isolate_resource_usage(
    runtime: &mut JsRuntime,
    cpu_started: Option<u64>,
) -> PerformanceResourceUsage {
    let heap = runtime.v8_isolate().get_heap_statistics();
    let current = heap
        .total_physical_size()
        .saturating_add(heap.malloced_memory())
        .saturating_add(heap.external_memory());
    PerformanceResourceUsage {
        cpu_total_micros: thread_cpu_micros()
            .zip(cpu_started)
            .map(|(finished, started)| finished.saturating_sub(started)),
        memory_bytes: Some(u64::try_from(current).unwrap_or(u64::MAX)),
        ..PerformanceResourceUsage::default()
    }
}

#[cfg(target_os = "linux")]
fn thread_cpu_micros() -> Option<u64> {
    static TICKS_PER_SECOND: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    let ticks_per_second = TICKS_PER_SECOND
        .get_or_init(|| {
            std::process::Command::new("getconf")
                .arg("CLK_TCK")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .and_then(|output| output.trim().parse::<u64>().ok())
                .filter(|ticks| *ticks > 0)
        })
        .as_ref()
        .copied()?;
    let stat = std::fs::read_to_string("/proc/thread-self/stat").ok()?;
    let after_name = stat.rsplit_once(')')?.1.trim();
    let fields = after_name.split_whitespace().collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    user_ticks
        .saturating_add(system_ticks)
        .saturating_mul(1_000_000)
        .checked_div(ticks_per_second)
}

#[cfg(not(target_os = "linux"))]
const fn thread_cpu_micros() -> Option<u64> {
    None
}

fn platform_function(
    runtime: &mut JsRuntime,
    name: &'static str,
) -> Result<v8::Global<v8::Function>, RuntimeError> {
    let value = runtime
        .execute_script("runku:bootstrap/function", format!("globalThis.{name}"))
        .map_err(|_| RuntimeError::Internal)?;
    deno_core::scope!(scope, runtime);
    let local = v8::Local::new(scope, value);
    let function =
        v8::Local::<v8::Function>::try_from(local).map_err(|_| RuntimeError::Internal)?;
    Ok(v8::Global::new(scope, function))
}

fn named_export(
    runtime: &mut JsRuntime,
    module_id: deno_core::ModuleId,
    function_name: &str,
    historical_default: bool,
) -> Result<v8::Global<v8::Function>, RuntimeError> {
    let namespace = runtime
        .get_module_namespace(module_id)
        .map_err(|_| RuntimeError::JavaScript)?;
    deno_core::scope!(scope, runtime);
    let namespace = v8::Local::<v8::Object>::new(scope, namespace);
    let export_name = if historical_default {
        "default"
    } else {
        function_name
            .rsplit('.')
            .next()
            .filter(|name| !name.is_empty())
            .ok_or(RuntimeError::InvalidInvocation)?
    };
    let name = v8::String::new(scope, export_name).ok_or(RuntimeError::Internal)?;
    let value = namespace
        .get(scope, name.into())
        .ok_or(RuntimeError::InvalidResult)?;
    let function =
        v8::Local::<v8::Function>::try_from(value).map_err(|_| RuntimeError::InvalidResult)?;
    Ok(v8::Global::new(scope, function))
}

fn serialize_value<T: Serialize>(
    runtime: &mut JsRuntime,
    value: &T,
) -> Result<v8::Global<v8::Value>, RuntimeError> {
    deno_core::scope!(scope, runtime);
    let value = serde_v8::to_v8(scope, value).map_err(|_| RuntimeError::Internal)?;
    Ok(v8::Global::new(scope, value))
}

fn deserialize_value<T: for<'de> serde::Deserialize<'de>>(
    runtime: &mut JsRuntime,
    value: &v8::Global<v8::Value>,
) -> Result<T, RuntimeError> {
    deno_core::scope!(scope, runtime);
    let value = v8::Local::new(scope, value);
    serde_v8::from_v8(scope, value).map_err(|_| RuntimeError::InvalidResult)
}

fn function_as_value(
    runtime: &mut JsRuntime,
    function: &v8::Global<v8::Function>,
) -> v8::Global<v8::Value> {
    deno_core::scope!(scope, runtime);
    let function = v8::Local::new(scope, function);
    let value = v8::Local::<v8::Value>::from(function);
    v8::Global::new(scope, value)
}

fn metadata(request: &InvocationRequest, function: &FunctionManifest) -> WireInvocationMetadata {
    let auth_enabled = function.capabilities.contains(&Capability::AuthRead);
    WireInvocationMetadata {
        project_id: request.scope.project_id().to_string(),
        environment_id: request.scope.environment_id().to_string(),
        release_id: request.release_id.to_string(),
        request_id: request.request_id.to_string(),
        invocation_id: request.invocation_id.to_string(),
        function_id: function.id.to_string(),
        function_name: function.name.to_string(),
        function_type: function_type(function.function_type),
        capabilities: function.capabilities.iter().map(capability).collect(),
        https_enabled: function.function_type == FunctionType::Action
            && function.capabilities.contains(&Capability::NetworkHttps),
        data_enabled: matches!(
            function.function_type,
            FunctionType::Query | FunctionType::Mutation
        ) && function.capabilities.contains(&Capability::DbRead),
        data_write_enabled: function.function_type == FunctionType::Mutation
            && function.capabilities.contains(&Capability::DbWrite),
        scheduler_enabled: matches!(
            function.function_type,
            FunctionType::Mutation | FunctionType::Action
        ) && function.capabilities.contains(&Capability::SchedulerCreate),
        function_query_enabled: function.capabilities.contains(&Capability::FunctionQuery),
        function_mutation_enabled: function
            .capabilities
            .contains(&Capability::FunctionMutation),
        function_action_enabled: function.capabilities.contains(&Capability::FunctionAction),
        auth_enabled,
        auth: if auth_enabled {
            request.identity.as_deref().map(wire_auth)
        } else {
            None
        },
    }
}

fn wire_auth(identity: &RequestIdentity) -> WireAuthContext {
    let application = identity
        .application
        .as_ref()
        .map(|application| WireApplicationContext {
            client_id: application.client_id.to_string(),
            credential_id: application.credential_id.to_string(),
            assurance: match application.assurance {
                ApplicationAssurance::Declared => "declared",
                ApplicationAssurance::Verified => "verified",
            },
            scopes: application.scopes.iter().map(ToString::to_string).collect(),
            configuration_revision: application.configuration_revision.to_string(),
        });
    let principal = match &identity.principal {
        PrincipalContext::None => None,
        PrincipalContext::Authenticated(principal) => Some(WirePrincipalContext {
            id: principal.id().to_string(),
            kind: match principal.kind() {
                PrincipalKind::Guest => "guest",
                PrincipalKind::User => "user",
                PrincipalKind::Service => "service",
                PrincipalKind::System => "system",
            },
            provider_id: principal.provider_id().to_owned(),
            scopes: principal.scopes().iter().map(ToString::to_string).collect(),
            auth_time: principal.auth_time().map(|time| time.get().to_string()),
            expires_at: principal.expires_at().map(|time| time.get().to_string()),
            mapping_revision: principal.mapping_revision().to_string(),
        }),
    };
    WireAuthContext {
        application,
        principal,
    }
}

const fn function_type(value: FunctionType) -> &'static str {
    match value {
        FunctionType::Query => "query",
        FunctionType::Mutation => "mutation",
        FunctionType::Action => "action",
    }
}

fn capability(value: &Capability) -> String {
    match value {
        Capability::DbRead => "db:read".to_owned(),
        Capability::DbWrite => "db:write".to_owned(),
        Capability::AuthRead => "auth:read".to_owned(),
        Capability::FunctionQuery => "function:query".to_owned(),
        Capability::FunctionMutation => "function:mutation".to_owned(),
        Capability::FunctionAction => "function:action".to_owned(),
        Capability::NetworkHttps => "network:https".to_owned(),
        Capability::SchedulerCreate => "scheduler:create".to_owned(),
        Capability::Secret(name) => format!("secret:{name}"),
    }
}
