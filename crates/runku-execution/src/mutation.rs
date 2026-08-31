//! Document Mutation execution and atomic commit coordination.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use runku_core::{
    DocumentId, EnvironmentScope, FunctionName, OperationId, OutboxEventId, ScheduledInvocationId,
    TableId,
};
use runku_data::{
    CommitBatch, CommitResult, DocumentMutation, DocumentReadAssertion, ExpectedRevision,
    IndexMutation, LogicalStore, OutboxAppend, PinnedCode, ReadSnapshot, ScheduledInvocationInsert,
    StoreError,
};
use runku_releases::{
    Capability, FunctionManifest, FunctionType, FunctionVisibility, decode_safe_esm_bundle,
};
use runku_runtime::{
    CancellationToken, DataDocument, DataGetRequest, DataIndexEntry, DataRead, DataReadError,
    DataScanRequest, DataWrite, FunctionCallError, FunctionCallKind, FunctionCallRequest,
    FunctionInvoke, InvocationRequest, RuntimeError, RuntimeSupervisor, ScheduleCreate,
    ScheduleError, ScheduleRequest, ScheduleTime,
};
use runku_schema::{SchemaCatalog, SchemaError, decode_schema_catalog, extract_index_key};
use runku_value::{CanonicalValue, TimestampMicros, encode_stored_value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::nested::{map_runtime_error, prepare_child};

const MAX_ATTEMPTS: u8 = 3;
const MAX_DOCUMENT_READS: usize = 10_000;
const MAX_DOCUMENT_WRITES: usize = 1_000;
const MAX_SCHEDULES: usize = 100;
const MAX_SCHEDULE_DELAY_MICROS: u64 = 10 * 365 * 24 * 60 * 60 * 1_000_000;

/// Successful Mutation result published after a known commit or a document-free execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationOutcome {
    /// Canonical handler result.
    pub value: CanonicalValue,
    /// Commit sequence, or `None` for a Mutation that buffered no writes.
    pub commit_sequence: Option<u64>,
    /// Whether storage recovered the commit from its operation journal.
    pub replayed: bool,
    /// Number of complete Function attempts.
    pub attempts: u8,
}

/// Stable Mutation composition failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MutationExecutionError {
    /// Safe Runtime rejected or failed the invocation.
    #[error("mutation runtime failed")]
    Runtime(RuntimeError),
    /// Logical storage failed with an exact sanitized category.
    #[error("mutation storage failed")]
    Storage(StoreError),
    /// Buffered data broker failed validation, limits, deadline, or cancellation.
    #[error("mutation data broker failed")]
    Data(DataReadError),
    /// Schema catalog or deterministic index extraction failed.
    #[error("mutation schema/index planning failed")]
    Schema(SchemaError),
    /// Transactional scheduling validation or buffering failed.
    #[error("mutation scheduling failed")]
    Schedule(ScheduleError),
}

impl MutationExecutionError {
    /// Stable machine-readable public code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Runtime(error) => error.code(),
            Self::Storage(error) => error.code(),
            Self::Data(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::Schedule(error) => error.code(),
        }
    }

    /// Whether retrying the complete public invocation may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        match self {
            Self::Runtime(error) => error.retryable(),
            Self::Storage(error) => error.retryable(),
            Self::Data(error) => {
                matches!(error, DataReadError::Unavailable | DataReadError::Timeout)
            }
            Self::Schema(error) => error.retryable(),
            Self::Schedule(error) => matches!(
                error,
                ScheduleError::Unavailable
                    | ScheduleError::Storage
                    | ScheduleError::Timeout
                    | ScheduleError::ResultUncertain
            ),
        }
    }
}

/// Bounded process-local Mutation counters with no request-controlled labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MutationTelemetrySnapshot {
    /// Public executions attempted.
    pub executions: u64,
    /// Successful outcomes.
    pub succeeded: u64,
    /// Complete Function attempts, including OCC reruns.
    pub function_attempts: u64,
    /// Physical commit calls, including exact retries.
    pub commit_calls: u64,
    /// Commit calls after the first call for the same immutable batch.
    pub exact_retries: u64,
    /// OCC conflicts observed.
    pub conflicts: u64,
    /// Successful operation-journal replays.
    pub replays: u64,
    /// Successful executions with no buffered writes.
    pub no_op: u64,
    /// Logical index mutations derived from active schema catalogs.
    pub index_mutations: u64,
    /// Scheduled Invocations committed by successful Mutations.
    pub schedules_created: u64,
    /// Runtime failures.
    pub runtime_failures: u64,
    /// Data-broker failures.
    pub data_failures: u64,
    /// Storage failures returned to the caller.
    pub storage_failures: u64,
    /// Aggregate elapsed microseconds, saturating at `u64::MAX`.
    pub elapsed_micros: u64,
}

#[derive(Debug, Default)]
struct MutationTelemetry {
    executions: AtomicU64,
    succeeded: AtomicU64,
    function_attempts: AtomicU64,
    commit_calls: AtomicU64,
    exact_retries: AtomicU64,
    conflicts: AtomicU64,
    replays: AtomicU64,
    no_op: AtomicU64,
    index_mutations: AtomicU64,
    schedules_created: AtomicU64,
    runtime_failures: AtomicU64,
    data_failures: AtomicU64,
    storage_failures: AtomicU64,
    elapsed_micros: AtomicU64,
}

/// Product Base coordinator for document Mutations.
#[derive(Clone)]
pub struct MutationExecutor {
    runtime: RuntimeSupervisor,
    store: Arc<dyn LogicalStore>,
    telemetry: Arc<MutationTelemetry>,
    schema: Option<Arc<SchemaCatalog>>,
}

impl fmt::Debug for MutationExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationExecutor")
            .field("backend", &self.store.backend())
            .finish_non_exhaustive()
    }
}

impl MutationExecutor {
    /// Composes a Runtime Supervisor and Logical Store without a physical-adapter dependency.
    #[must_use]
    pub fn new(runtime: RuntimeSupervisor, store: Arc<dyn LogicalStore>) -> Self {
        Self {
            runtime,
            store,
            telemetry: Arc::new(MutationTelemetry::default()),
            schema: None,
        }
    }

    /// Attaches one immutable active index catalog.
    #[must_use]
    pub fn with_schema_catalog(mut self, schema: Arc<SchemaCatalog>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Executes and atomically commits one Mutation under a durable operation identity.
    ///
    /// # Errors
    ///
    /// Returns only sanitized runtime/data/storage categories. OCC conflicts reexecute the whole
    /// side-effect-free Function up to three times; uncertain/transient commit failures retry the
    /// exact same batch first.
    pub async fn execute(
        &self,
        request: InvocationRequest,
        operation_id: OperationId,
    ) -> Result<MutationOutcome, MutationExecutionError> {
        self.execute_with_deadline(request, operation_id, None)
            .await
    }

    pub(crate) async fn execute_nested(
        &self,
        request: InvocationRequest,
        operation_id: OperationId,
        deadline: Instant,
    ) -> Result<MutationOutcome, MutationExecutionError> {
        self.execute_with_deadline(request, operation_id, Some(deadline))
            .await
    }

    async fn execute_with_deadline(
        &self,
        request: InvocationRequest,
        operation_id: OperationId,
        inherited_deadline: Option<Instant>,
    ) -> Result<MutationOutcome, MutationExecutionError> {
        let started = Instant::now();
        self.telemetry.executions.fetch_add(1, Ordering::Relaxed);
        let result = self
            .execute_inner(request, operation_id, inherited_deadline)
            .await;
        self.telemetry.record(&result, started.elapsed());
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_inner(
        &self,
        request: InvocationRequest,
        operation_id: OperationId,
        inherited_deadline: Option<Instant>,
    ) -> Result<MutationOutcome, MutationExecutionError> {
        let active_schema = if let Some(schema) = &self.schema {
            Some(Arc::clone(schema))
        } else if request.manifest().runtime_version.as_str() == "runku-js-1" {
            let bundle = decode_safe_esm_bundle(request.artifact_bytes())
                .map_err(|_| MutationExecutionError::Schema(SchemaError::InvalidCatalog))?;
            let resource = bundle
                .resource(request.index_contract_hash())
                .ok_or(MutationExecutionError::Schema(SchemaError::InvalidCatalog))?;
            Some(Arc::new(
                decode_schema_catalog(resource.as_bytes())
                    .map_err(MutationExecutionError::Schema)?,
            ))
        } else {
            None
        };
        if let Some(schema) = &active_schema
            && (schema.project_id() != request.scope().project_id()
                || schema.digest().as_slice() != request.index_contract_hash().as_bytes())
        {
            return Err(MutationExecutionError::Schema(SchemaError::InvalidCatalog));
        }
        let event_id = OutboxEventId::from_ulid(operation_id.as_ulid());
        let schedule_base = operation_time_micros(operation_id)?;
        let pinned_code = request.pinned_code();
        let allowed_schedules = request
            .manifest()
            .functions
            .iter()
            .filter(|function| {
                function.visibility == FunctionVisibility::Internal
                    && matches!(
                        function.function_type,
                        FunctionType::Mutation | FunctionType::Action
                    )
            })
            .map(|function| function.name.clone())
            .collect::<BTreeSet<_>>();
        let selected = request
            .manifest()
            .functions
            .iter()
            .find(|function| function.id == request.function_id())
            .cloned()
            .ok_or(MutationExecutionError::Runtime(
                RuntimeError::InvalidInvocation,
            ))?;
        if selected.function_type != FunctionType::Mutation {
            return Err(MutationExecutionError::Runtime(
                RuntimeError::InvalidInvocation,
            ));
        }
        for attempt in 1..=MAX_ATTEMPTS {
            self.telemetry
                .function_attempts
                .fetch_add(1, Ordering::Relaxed);
            let session = Arc::new(MutationSession::new(
                Arc::clone(&self.store),
                request.scope(),
                operation_id,
                schedule_base,
                pinned_code,
                allowed_schedules.clone(),
            ));
            let attached = attach_mutation_capabilities(
                request.clone(),
                &selected,
                self.runtime.clone(),
                session.clone(),
            )
            .map_err(MutationExecutionError::Runtime)?;
            let runtime_result = match inherited_deadline {
                Some(deadline) => self.runtime.invoke_nested_until(attached, deadline).await,
                None => self.runtime.invoke(attached).await,
            };
            let buffered = session.finish().await.map_err(SessionFailure::public)?;
            let value = runtime_result.map_err(MutationExecutionError::Runtime)?;
            if buffered.writes.is_empty() && buffered.schedules.is_empty() {
                self.telemetry.no_op.fetch_add(1, Ordering::Relaxed);
                return Ok(MutationOutcome {
                    value,
                    commit_sequence: None,
                    replayed: false,
                    attempts: attempt,
                });
            }
            let indexes = active_schema
                .as_deref()
                .map_or_else(|| Ok(Vec::new()), |schema| plan_indexes(schema, &buffered))?;
            self.telemetry.index_mutations.fetch_add(
                u64::try_from(indexes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let schedule_count = u64::try_from(buffered.schedules.len()).unwrap_or(u64::MAX);
            let batch = mutation_batch(
                request.scope(),
                operation_id,
                event_id,
                buffered.reads,
                buffered.writes,
                indexes,
                buffered.schedules,
            )?;
            match commit_exact(self.store.as_ref(), &batch, &self.telemetry).await {
                Ok(result) => {
                    if !result.replayed {
                        self.telemetry
                            .schedules_created
                            .fetch_add(schedule_count, Ordering::Relaxed);
                    }
                    return Ok(outcome(value, &result, attempt));
                }
                Err(StoreError::MutationConflict) if attempt < MAX_ATTEMPTS => {
                    self.telemetry.conflicts.fetch_add(1, Ordering::Relaxed);
                }
                Err(StoreError::MutationConflict) => {
                    self.telemetry.conflicts.fetch_add(1, Ordering::Relaxed);
                    return Err(MutationExecutionError::Storage(
                        StoreError::MutationConflict,
                    ));
                }
                Err(error) => return Err(MutationExecutionError::Storage(error)),
            }
        }
        Err(MutationExecutionError::Storage(
            StoreError::MutationConflict,
        ))
    }

    /// Returns bounded aggregate telemetry.
    #[must_use]
    pub fn telemetry(&self) -> MutationTelemetrySnapshot {
        self.telemetry.snapshot()
    }
}

fn attach_mutation_capabilities(
    mut request: InvocationRequest,
    selected: &FunctionManifest,
    runtime: RuntimeSupervisor,
    session: Arc<MutationSession>,
) -> Result<InvocationRequest, RuntimeError> {
    match selected.function_type {
        FunctionType::Query => {
            if selected.capabilities.contains(&Capability::DbRead) {
                request = request.with_data(session.clone())?;
            }
        }
        FunctionType::Mutation => {
            if selected.capabilities.contains(&Capability::DbWrite) {
                request = request.with_mutation_data(session.clone())?;
            } else if selected.capabilities.contains(&Capability::DbRead) {
                request = request.with_mutation_read(session.clone())?;
            }
            if selected.capabilities.contains(&Capability::SchedulerCreate) {
                request = request.with_scheduler(session.clone())?;
            }
        }
        FunctionType::Action => return Err(RuntimeError::InvalidInvocation),
    }
    if selected.capabilities.iter().any(|capability| {
        matches!(
            capability,
            Capability::FunctionQuery | Capability::FunctionMutation
        )
    }) {
        let broker = Arc::new(MutationFunctionBroker {
            runtime,
            root: request.clone(),
            session,
        });
        request = request.with_functions(broker)?;
    }
    Ok(request)
}

struct MutationFunctionBroker {
    runtime: RuntimeSupervisor,
    root: InvocationRequest,
    session: Arc<MutationSession>,
}

impl fmt::Debug for MutationFunctionBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationFunctionBroker")
            .field("scope", &self.root.scope())
            .field("depth", &self.root.nested_depth())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl FunctionInvoke for MutationFunctionBroker {
    async fn invoke(
        &self,
        call: FunctionCallRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CanonicalValue, FunctionCallError> {
        if matches!(call.kind, FunctionCallKind::Action) || cancellation.is_cancelled() {
            return Err(FunctionCallError::Denied);
        }
        let (child, selected) = prepare_child(&self.root, call, deadline)?;
        let attached = attach_mutation_capabilities(
            child,
            &selected,
            self.runtime.clone(),
            Arc::clone(&self.session),
        )
        .map_err(map_runtime_error)?;
        self.runtime
            .invoke_nested_until(attached, deadline)
            .await
            .map_err(map_runtime_error)
    }
}

impl MutationTelemetry {
    fn record(&self, result: &Result<MutationOutcome, MutationExecutionError>, elapsed: Duration) {
        match result {
            Ok(outcome) => {
                self.succeeded.fetch_add(1, Ordering::Relaxed);
                if outcome.replayed {
                    self.replays.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(MutationExecutionError::Runtime(_)) => {
                self.runtime_failures.fetch_add(1, Ordering::Relaxed);
            }
            Err(
                MutationExecutionError::Data(_)
                | MutationExecutionError::Schema(_)
                | MutationExecutionError::Schedule(_),
            ) => {
                self.data_failures.fetch_add(1, Ordering::Relaxed);
            }
            Err(MutationExecutionError::Storage(_)) => {
                self.storage_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        let elapsed = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let _ = self
            .elapsed_micros
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(elapsed))
            });
    }

    fn snapshot(&self) -> MutationTelemetrySnapshot {
        MutationTelemetrySnapshot {
            executions: self.executions.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            function_attempts: self.function_attempts.load(Ordering::Relaxed),
            commit_calls: self.commit_calls.load(Ordering::Relaxed),
            exact_retries: self.exact_retries.load(Ordering::Relaxed),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            replays: self.replays.load(Ordering::Relaxed),
            no_op: self.no_op.load(Ordering::Relaxed),
            index_mutations: self.index_mutations.load(Ordering::Relaxed),
            schedules_created: self.schedules_created.load(Ordering::Relaxed),
            runtime_failures: self.runtime_failures.load(Ordering::Relaxed),
            data_failures: self.data_failures.load(Ordering::Relaxed),
            storage_failures: self.storage_failures.load(Ordering::Relaxed),
            elapsed_micros: self.elapsed_micros.load(Ordering::Relaxed),
        }
    }
}

fn outcome(value: CanonicalValue, result: &CommitResult, attempts: u8) -> MutationOutcome {
    MutationOutcome {
        value,
        commit_sequence: Some(result.commit_sequence),
        replayed: result.replayed,
        attempts,
    }
}

async fn commit_exact(
    store: &dyn LogicalStore,
    batch: &CommitBatch,
    telemetry: &MutationTelemetry,
) -> Result<CommitResult, StoreError> {
    let mut last = StoreError::Internal;
    for retry in 0..MAX_ATTEMPTS {
        telemetry.commit_calls.fetch_add(1, Ordering::Relaxed);
        if retry > 0 {
            telemetry.exact_retries.fetch_add(1, Ordering::Relaxed);
        }
        match store.commit(batch).await {
            Ok(result) => return Ok(result),
            Err(
                error @ (StoreError::Busy
                | StoreError::SerializationFailure
                | StoreError::ResultUncertain
                | StoreError::Unavailable),
            ) if retry + 1 < MAX_ATTEMPTS => {
                last = error;
                tokio::time::sleep(Duration::from_millis(5_u64 << retry)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last)
}

fn mutation_batch(
    scope: EnvironmentScope,
    operation_id: OperationId,
    event_id: OutboxEventId,
    reads: Vec<DocumentReadAssertion>,
    documents: Vec<DocumentMutation>,
    indexes: Vec<IndexMutation>,
    schedules: Vec<ScheduledInvocationInsert>,
) -> Result<CommitBatch, MutationExecutionError> {
    let has_documents = !documents.is_empty();
    let payload = write_set_payload(&documents, &indexes);
    let mut batch = CommitBatch::new(scope, operation_id);
    for read in reads {
        batch.push_read(read);
    }
    for document in documents {
        batch.push_document(document);
    }
    for index in indexes {
        batch.push_index(index);
    }
    if has_documents {
        batch.push_outbox(OutboxAppend { event_id, payload });
    }
    for schedule in schedules {
        batch.push_schedule(schedule);
    }
    batch.validate().map_err(MutationExecutionError::Storage)?;
    Ok(batch)
}

fn write_set_payload(documents: &[DocumentMutation], indexes: &[IndexMutation]) -> CanonicalValue {
    let writes = documents
        .iter()
        .map(|mutation| {
            let kind = match mutation {
                DocumentMutation::Upsert {
                    expected: ExpectedRevision::Absent,
                    ..
                } => "insert",
                DocumentMutation::Upsert {
                    expected: ExpectedRevision::Exact(_),
                    ..
                } => "replace",
                DocumentMutation::Delete { .. } => "delete",
            };
            CanonicalValue::Object(BTreeMap::from([
                (
                    "documentId".to_owned(),
                    CanonicalValue::String(mutation.document_id().to_string()),
                ),
                ("kind".to_owned(), CanonicalValue::String(kind.to_owned())),
                (
                    "tableId".to_owned(),
                    CanonicalValue::String(mutation.table_id().to_string()),
                ),
            ]))
        })
        .collect();
    CanonicalValue::Object(BTreeMap::from([
        (
            "indexes".to_owned(),
            CanonicalValue::Array(indexes.iter().map(index_impact).collect()),
        ),
        (
            "type".to_owned(),
            CanonicalValue::String("document_write_set_v2".to_owned()),
        ),
        ("writes".to_owned(), CanonicalValue::Array(writes)),
    ]))
}

fn index_impact(mutation: &IndexMutation) -> CanonicalValue {
    let (kind, index_id, key, document_id) = match mutation {
        IndexMutation::Put {
            index_id,
            key,
            document_id,
            ..
        } => ("put", index_id, key, document_id),
        IndexMutation::Delete {
            index_id,
            key,
            document_id,
        } => ("delete", index_id, key, document_id),
    };
    CanonicalValue::Object(BTreeMap::from([
        (
            "documentId".to_owned(),
            CanonicalValue::String(document_id.to_string()),
        ),
        (
            "indexId".to_owned(),
            CanonicalValue::String(index_id.to_string()),
        ),
        (
            "key".to_owned(),
            CanonicalValue::Bytes(key.as_bytes().to_vec()),
        ),
        ("kind".to_owned(), CanonicalValue::String(kind.to_owned())),
    ]))
}

fn plan_indexes(
    schema: &SchemaCatalog,
    buffered: &BufferedMutation,
) -> Result<Vec<IndexMutation>, MutationExecutionError> {
    let mut indexes = Vec::new();
    for mutation in &buffered.writes {
        let key = (mutation.table_id(), mutation.document_id());
        let old_value = buffered.old_values.get(&key).and_then(Option::as_ref);
        let (new_value, new_revision) = match mutation {
            DocumentMutation::Upsert {
                expected, value, ..
            } => (
                Some(value),
                match expected {
                    ExpectedRevision::Absent => 1,
                    ExpectedRevision::Exact(revision) => revision
                        .checked_add(1)
                        .ok_or(MutationExecutionError::Storage(StoreError::LimitExceeded))?,
                },
            ),
            DocumentMutation::Delete { .. } => (None, 0),
        };
        for definition in schema.indexes_for_table(mutation.table_id()) {
            let old_key = old_value
                .map(|value| extract_index_key(definition, value))
                .transpose()
                .map_err(MutationExecutionError::Schema)?
                .flatten();
            let new_key = new_value
                .map(|value| extract_index_key(definition, value))
                .transpose()
                .map_err(MutationExecutionError::Schema)?
                .flatten();
            if let Some(old_key) = old_key.as_ref()
                && new_key.as_ref() != Some(old_key)
            {
                indexes.push(IndexMutation::Delete {
                    index_id: definition.index_id(),
                    key: old_key.clone(),
                    document_id: mutation.document_id(),
                });
            }
            if let Some(new_key) = new_key {
                indexes.push(IndexMutation::Put {
                    index_id: definition.index_id(),
                    key: new_key,
                    table_id: mutation.table_id(),
                    document_id: mutation.document_id(),
                    document_revision: new_revision,
                });
            }
        }
    }
    Ok(indexes)
}

#[derive(Clone, Copy)]
enum SessionFailure {
    Store(StoreError),
    Data(DataReadError),
    Schedule(ScheduleError),
}

impl SessionFailure {
    const fn public(self) -> MutationExecutionError {
        match self {
            Self::Store(error) => MutationExecutionError::Storage(error),
            Self::Data(error) => MutationExecutionError::Data(error),
            Self::Schedule(error) => MutationExecutionError::Schedule(error),
        }
    }
}

struct MutationState {
    snapshot: Option<Box<dyn ReadSnapshot>>,
    reads: BTreeMap<(TableId, DocumentId), Option<DataDocument>>,
    writes: BTreeMap<(TableId, DocumentId), DocumentMutation>,
    schedules: Vec<ScheduledInvocationInsert>,
    schedule_keys: BTreeSet<String>,
    failure: Option<SessionFailure>,
    closed: bool,
}

struct MutationSession {
    store: Arc<dyn LogicalStore>,
    scope: EnvironmentScope,
    operation_id: OperationId,
    schedule_base: TimestampMicros,
    pinned_code: PinnedCode,
    allowed_schedules: BTreeSet<FunctionName>,
    state: Mutex<MutationState>,
}

enum BufferedRead {
    NotBuffered,
    Missing,
    Document(DataDocument),
}

impl fmt::Debug for MutationSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationSession")
            .finish_non_exhaustive()
    }
}

impl MutationSession {
    fn new(
        store: Arc<dyn LogicalStore>,
        scope: EnvironmentScope,
        operation_id: OperationId,
        schedule_base: TimestampMicros,
        pinned_code: PinnedCode,
        allowed_schedules: BTreeSet<FunctionName>,
    ) -> Self {
        Self {
            store,
            scope,
            operation_id,
            schedule_base,
            pinned_code,
            allowed_schedules,
            state: Mutex::new(MutationState {
                snapshot: None,
                reads: BTreeMap::new(),
                writes: BTreeMap::new(),
                schedules: Vec::new(),
                schedule_keys: BTreeSet::new(),
                failure: None,
                closed: false,
            }),
        }
    }

    async fn finish(&self) -> Result<BufferedMutation, SessionFailure> {
        let preload_failure =
            match tokio::time::timeout(Duration::from_secs(1), self.load_write_bases()).await {
                Err(_) => Some(SessionFailure::Data(DataReadError::Timeout)),
                Ok(Err(failure)) => Some(failure),
                Ok(Ok(())) => None,
            };
        let (snapshot, reads, old_values, writes, schedules, failure) = {
            let mut state = self.state.lock().await;
            state.closed = true;
            if state.failure.is_none() {
                state.failure = preload_failure;
            }
            let old_values = state
                .reads
                .iter()
                .map(|(key, document)| (*key, document.as_ref().map(|value| value.value.clone())))
                .collect();
            (
                state.snapshot.take(),
                std::mem::take(&mut state.reads)
                    .into_iter()
                    .map(
                        |((table_id, document_id), document)| DocumentReadAssertion {
                            table_id,
                            document_id,
                            observed_revision: document.map(|value| value.revision),
                        },
                    )
                    .collect(),
                old_values,
                std::mem::take(&mut state.writes).into_values().collect(),
                std::mem::take(&mut state.schedules),
                state.failure,
            )
        };
        if let Some(snapshot) = snapshot {
            let close = tokio::time::timeout(Duration::from_secs(1), snapshot.close())
                .await
                .map_err(|_| SessionFailure::Data(DataReadError::Timeout))?;
            close.map_err(SessionFailure::Store)?;
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        Ok(BufferedMutation {
            reads,
            old_values,
            writes,
            schedules,
        })
    }

    async fn load_write_bases(&self) -> Result<(), SessionFailure> {
        let mut state = self.state.lock().await;
        if state.failure.is_some() || state.closed {
            return Ok(());
        }
        let keys = state
            .writes
            .iter()
            .filter_map(|(key, mutation)| {
                let needs_old = matches!(
                    mutation,
                    DocumentMutation::Upsert {
                        expected: ExpectedRevision::Exact(_),
                        ..
                    } | DocumentMutation::Delete { .. }
                );
                (needs_old && !state.reads.contains_key(key)).then_some(*key)
            })
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok(());
        }
        if state.snapshot.is_none() {
            state.snapshot = Some(
                self.store
                    .begin_read(self.scope)
                    .await
                    .map_err(SessionFailure::Store)?,
            );
        }
        for (table_id, document_id) in keys {
            let snapshot = state
                .snapshot
                .as_mut()
                .ok_or(SessionFailure::Data(DataReadError::Unavailable))?;
            let sequence = snapshot.commit_sequence();
            let record = snapshot
                .get_document(table_id, document_id)
                .await
                .map_err(SessionFailure::Store)?;
            if record.as_ref().is_some_and(|record| {
                record.table_id != table_id
                    || record.document_id != document_id
                    || record.revision == 0
                    || record.commit_sequence > sequence
                    || record.created_at > record.updated_at
            }) {
                return Err(SessionFailure::Store(StoreError::Corruption));
            }
            state.reads.insert(
                (table_id, document_id),
                record.map(|record| DataDocument {
                    table_id: record.table_id,
                    document_id: record.document_id,
                    revision: record.revision,
                    commit_sequence: record.commit_sequence,
                    created_at: record.created_at,
                    updated_at: record.updated_at,
                    value: record.value,
                }),
            );
        }
        Ok(())
    }

    fn buffer(state: &mut MutationState, mutation: DocumentMutation) -> Result<(), DataReadError> {
        if state.closed || state.failure.is_some() {
            return Err(DataReadError::Unavailable);
        }
        if state.writes.len() >= MAX_DOCUMENT_WRITES || encode_mutation_value(&mutation).is_err() {
            state.failure = Some(SessionFailure::Data(DataReadError::LimitExceeded));
            return Err(DataReadError::LimitExceeded);
        }
        let key = (mutation.table_id(), mutation.document_id());
        if state.writes.contains_key(&key) {
            state.failure = Some(SessionFailure::Data(DataReadError::InvalidRequest));
            return Err(DataReadError::InvalidRequest);
        }
        state.writes.insert(key, mutation);
        Ok(())
    }

    fn buffered_read(
        &self,
        state: &MutationState,
        request: DataGetRequest,
    ) -> Result<BufferedRead, DataReadError> {
        let Some(buffered) = state
            .writes
            .get(&(request.table_id, request.document_id))
            .cloned()
        else {
            return Ok(BufferedRead::NotBuffered);
        };
        let DocumentMutation::Upsert {
            expected, value, ..
        } = buffered
        else {
            return Ok(BufferedRead::Missing);
        };
        let revision = match expected {
            ExpectedRevision::Absent => 1,
            ExpectedRevision::Exact(revision) => revision
                .checked_add(1)
                .ok_or(DataReadError::LimitExceeded)?,
        };
        Ok(BufferedRead::Document(DataDocument {
            table_id: request.table_id,
            document_id: request.document_id,
            revision,
            // Zero explicitly identifies a buffered, not-yet-committed projection.
            commit_sequence: 0,
            created_at: self.schedule_base,
            updated_at: self.schedule_base,
            value,
        }))
    }
}

struct BufferedMutation {
    reads: Vec<DocumentReadAssertion>,
    old_values: BTreeMap<(TableId, DocumentId), Option<CanonicalValue>>,
    writes: Vec<DocumentMutation>,
    schedules: Vec<ScheduledInvocationInsert>,
}

fn encode_mutation_value(mutation: &DocumentMutation) -> Result<(), ()> {
    match mutation {
        DocumentMutation::Upsert { value, .. } => {
            encode_stored_value(value).map(|_| ()).map_err(|_| ())
        }
        DocumentMutation::Delete { .. } => Ok(()),
    }
}

#[async_trait]
impl DataWrite for MutationSession {
    async fn insert(
        &self,
        table_id: TableId,
        document_id: DocumentId,
        value: CanonicalValue,
    ) -> Result<(), DataReadError> {
        let mut state = self.state.lock().await;
        Self::buffer(
            &mut state,
            DocumentMutation::Upsert {
                table_id,
                document_id,
                expected: ExpectedRevision::Absent,
                value,
            },
        )
    }

    async fn replace(
        &self,
        table_id: TableId,
        document_id: DocumentId,
        expected_revision: u64,
        value: CanonicalValue,
    ) -> Result<(), DataReadError> {
        if expected_revision == 0 || expected_revision == u64::MAX {
            let mut state = self.state.lock().await;
            state.failure = Some(SessionFailure::Data(DataReadError::InvalidRequest));
            return Err(DataReadError::InvalidRequest);
        }
        let mut state = self.state.lock().await;
        Self::buffer(
            &mut state,
            DocumentMutation::Upsert {
                table_id,
                document_id,
                expected: ExpectedRevision::Exact(expected_revision),
                value,
            },
        )
    }

    async fn delete(
        &self,
        table_id: TableId,
        document_id: DocumentId,
        expected_revision: u64,
    ) -> Result<(), DataReadError> {
        if expected_revision == 0 {
            let mut state = self.state.lock().await;
            state.failure = Some(SessionFailure::Data(DataReadError::InvalidRequest));
            return Err(DataReadError::InvalidRequest);
        }
        let mut state = self.state.lock().await;
        Self::buffer(
            &mut state,
            DocumentMutation::Delete {
                table_id,
                document_id,
                expected_revision,
            },
        )
    }
}

#[async_trait]
impl ScheduleCreate for MutationSession {
    async fn create(
        &self,
        request: ScheduleRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ScheduledInvocationId, ScheduleError> {
        if cancellation.is_cancelled() {
            return self.latch_schedule_error(ScheduleError::Cancelled).await;
        }
        if Instant::now() >= deadline {
            return self.latch_schedule_error(ScheduleError::Timeout).await;
        }
        if !self.allowed_schedules.contains(&request.function)
            || encode_stored_value(&request.arguments).is_err()
        {
            return self
                .latch_schedule_error(ScheduleError::InvalidRequest)
                .await;
        }
        let execute_at = match schedule_time(self.schedule_base, request.time) {
            Ok(value) => value,
            Err(error) => return self.latch_schedule_error(error).await,
        };
        let mut state = self.state.lock().await;
        if state.closed || state.failure.is_some() {
            return Err(ScheduleError::Unavailable);
        }
        if state.schedules.len() >= MAX_SCHEDULES {
            state.failure = Some(SessionFailure::Schedule(ScheduleError::LimitExceeded));
            return Err(ScheduleError::LimitExceeded);
        }
        if let Some(key) = request.idempotency_key.as_ref()
            && (key.is_empty() || key.len() > 128 || !state.schedule_keys.insert(key.clone()))
        {
            state.failure = Some(SessionFailure::Schedule(ScheduleError::InvalidRequest));
            return Err(ScheduleError::InvalidRequest);
        }
        let ordinal =
            u64::try_from(state.schedules.len()).map_err(|_| ScheduleError::LimitExceeded)?;
        let id = derived_schedule_id(self.operation_id, ordinal);
        state.schedules.push(ScheduledInvocationInsert {
            id,
            pinned_code: self.pinned_code,
            function: request.function,
            args: request.arguments,
            execute_at,
            idempotency_key: request.idempotency_key,
        });
        Ok(id)
    }
}

impl MutationSession {
    async fn latch_schedule_error<T>(&self, error: ScheduleError) -> Result<T, ScheduleError> {
        let mut state = self.state.lock().await;
        state.failure = Some(SessionFailure::Schedule(error));
        Err(error)
    }
}

pub(crate) fn schedule_time(
    base: TimestampMicros,
    requested: ScheduleTime,
) -> Result<TimestampMicros, ScheduleError> {
    let maximum_delay =
        i64::try_from(MAX_SCHEDULE_DELAY_MICROS).map_err(|_| ScheduleError::LimitExceeded)?;
    let maximum = base
        .get()
        .checked_add(maximum_delay)
        .ok_or(ScheduleError::LimitExceeded)?;
    match requested {
        ScheduleTime::AfterMicros(delay) => {
            if delay > MAX_SCHEDULE_DELAY_MICROS {
                return Err(ScheduleError::LimitExceeded);
            }
            let delay = i64::try_from(delay).map_err(|_| ScheduleError::LimitExceeded)?;
            base.get()
                .checked_add(delay)
                .map(TimestampMicros::new)
                .ok_or(ScheduleError::LimitExceeded)
        }
        ScheduleTime::At(timestamp) if timestamp.get() <= maximum => Ok(timestamp),
        ScheduleTime::At(_) => Err(ScheduleError::LimitExceeded),
    }
}

fn derived_schedule_id(operation_id: OperationId, ordinal: u64) -> ScheduledInvocationId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_MUTATION_SCHEDULE_V1");
    digest.update(operation_id.to_string().as_bytes());
    digest.update(ordinal.to_be_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    ScheduledInvocationId::from_ulid(ulid::Ulid::from(u128::from_be_bytes(bytes)))
}

fn operation_time_micros(
    operation_id: OperationId,
) -> Result<TimestampMicros, MutationExecutionError> {
    let millis = operation_id.as_ulid().timestamp_ms();
    let micros = millis
        .checked_mul(1_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(MutationExecutionError::Storage(StoreError::Internal))?;
    Ok(TimestampMicros::new(micros))
}

#[async_trait]
impl DataRead for MutationSession {
    async fn get(
        &self,
        request: DataGetRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<DataDocument>, DataReadError> {
        let mut state = self.state.lock().await;
        if state.closed || state.failure.is_some() {
            return Err(DataReadError::Unavailable);
        }
        match self.buffered_read(&state, request)? {
            BufferedRead::Missing => return Ok(None),
            BufferedRead::Document(document) => return Ok(Some(document)),
            BufferedRead::NotBuffered => {}
        }
        if state.snapshot.is_none() {
            let begin = self.store.begin_read(self.scope);
            let snapshot = tokio::select! {
                () = cancellation.cancelled() => {
                    state.failure = Some(SessionFailure::Data(DataReadError::Cancelled));
                    return Err(DataReadError::Cancelled);
                }
                result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), begin) => {
                    match result {
                        Err(_) => {
                            state.failure = Some(SessionFailure::Data(DataReadError::Timeout));
                            return Err(DataReadError::Timeout);
                        }
                        Ok(Err(error)) => {
                            state.failure = Some(SessionFailure::Store(error));
                            return Err(DataReadError::Storage);
                        }
                        Ok(Ok(snapshot)) => snapshot,
                    }
                }
            };
            state.snapshot = Some(snapshot);
        }
        let snapshot = state.snapshot.as_mut().ok_or(DataReadError::Unavailable)?;
        let snapshot_sequence = snapshot.commit_sequence();
        let read = snapshot.get_document(request.table_id, request.document_id);
        let result = tokio::select! {
            () = cancellation.cancelled() => {
                state.failure = Some(SessionFailure::Data(DataReadError::Cancelled));
                return Err(DataReadError::Cancelled);
            }
            result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), read) => {
                match result {
                    Err(_) => {
                        state.failure = Some(SessionFailure::Data(DataReadError::Timeout));
                        return Err(DataReadError::Timeout);
                    }
                    Ok(result) => result,
                }
            }
        };
        let result = result.map_err(|error| {
            state.failure = Some(SessionFailure::Store(error));
            DataReadError::Storage
        })?;
        if result.as_ref().is_some_and(|record| {
            record.table_id != request.table_id
                || record.document_id != request.document_id
                || record.revision == 0
                || record.commit_sequence > snapshot_sequence
                || record.created_at > record.updated_at
        }) {
            state.failure = Some(SessionFailure::Store(StoreError::Corruption));
            return Err(DataReadError::Storage);
        }
        let key = (request.table_id, request.document_id);
        let observed = result.as_ref().map(|record| DataDocument {
            table_id: record.table_id,
            document_id: record.document_id,
            revision: record.revision,
            commit_sequence: record.commit_sequence,
            created_at: record.created_at,
            updated_at: record.updated_at,
            value: record.value.clone(),
        });
        if !state.reads.contains_key(&key) && state.reads.len() >= MAX_DOCUMENT_READS {
            state.failure = Some(SessionFailure::Data(DataReadError::LimitExceeded));
            return Err(DataReadError::LimitExceeded);
        }
        if state
            .reads
            .insert(key, observed.clone())
            .is_some_and(|prior| prior != observed)
        {
            state.failure = Some(SessionFailure::Store(StoreError::Corruption));
            return Err(DataReadError::Storage);
        }
        Ok(result.map(|record| DataDocument {
            table_id: record.table_id,
            document_id: record.document_id,
            revision: record.revision,
            commit_sequence: record.commit_sequence,
            created_at: record.created_at,
            updated_at: record.updated_at,
            value: record.value,
        }))
    }

    async fn scan(
        &self,
        _request: DataScanRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<Vec<DataIndexEntry>, DataReadError> {
        Err(DataReadError::InvalidRequest)
    }
}
