//! Read-only Query execution, snapshot ownership, dependencies, and telemetry.

use std::{
    cmp::Ordering as CompareOrdering,
    collections::BTreeSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use runku_core::{DocumentId, EnvironmentScope, IndexId, TableId};
use runku_data::{
    IndexRange, IndexScanCursor, IndexScanDirection, KeyBound, LogicalStore, ReadSnapshot,
    StoreError, TableScanCursor,
};
use runku_releases::{Capability, FunctionManifest, FunctionType, decode_safe_esm_bundle};
use runku_runtime::{
    CancellationToken, DataBoundKind, DataDocument, DataGetRequest, DataIndexEntry, DataKeyBound,
    DataQueryDirection, DataQueryFilter, DataQueryOperator, DataQueryOrder, DataQueryPage,
    DataQueryRequest, DataRead, DataReadError, DataScanRequest, FunctionCallError,
    FunctionCallKind, FunctionCallRequest, FunctionInvoke, InvocationRequest, RuntimeError,
    RuntimeSupervisor,
};
use runku_schema::{
    FieldPath, IndexKind, SchemaCatalog, decode_schema_catalog, normalize_search_term,
};
use runku_value::{CanonicalValue, IndexKey, IndexValue, encode_stored_value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::nested::{map_runtime_error, prepare_child};

const MAX_DEPENDENCIES: usize = 10_000;
const MAX_SCAN_ROWS: usize = 10_000;
const MAX_UNINDEXED_TABLE_ROWS: usize = 2_000;
const MAX_QUERY_LIMIT: u32 = 200;

/// Canonical dependency range endpoint.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DependencyBound {
    /// No bound in this direction.
    Unbounded,
    /// Includes the supplied canonical Index Key v1 bytes.
    Inclusive(Vec<u8>),
    /// Excludes the supplied canonical Index Key v1 bytes.
    Exclusive(Vec<u8>),
}

/// Exact logical read dependency emitted by one Query snapshot.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReadDependency {
    /// One document, including misses represented by `observed_revision = None`.
    Point {
        /// Logical table.
        table_id: TableId,
        /// Opaque document.
        document_id: DocumentId,
        /// Revision observed, or `None` for a miss.
        observed_revision: Option<u64>,
        /// Snapshot commit sequence.
        snapshot_sequence: u64,
    },
    /// One exact logical index range, including empty results.
    Range {
        /// Logical index.
        index_id: IndexId,
        /// Lower endpoint.
        lower: DependencyBound,
        /// Upper endpoint.
        upper: DependencyBound,
        /// Snapshot commit sequence.
        snapshot_sequence: u64,
    },
    /// One bounded whole-table query, invalidated by any write to that table.
    Table {
        /// Logical table.
        table_id: TableId,
        /// Snapshot commit sequence.
        snapshot_sequence: u64,
    },
}

/// Complete successful Query result and its reactive read set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryOutcome {
    /// Canonical handler result.
    pub value: CanonicalValue,
    /// Snapshot sequence, or `None` when the Query made no data read.
    pub snapshot_sequence: Option<u64>,
    /// Canonical sorted, deduplicated dependency set.
    pub dependencies: Vec<ReadDependency>,
}

/// Result of one direct logical query executed without invoking user JavaScript.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalQueryOutcome {
    /// Stable page produced by the shared query planner.
    pub page: DataQueryPage,
    /// Snapshot sequence shared by every returned document.
    pub snapshot_sequence: u64,
}

/// Executes one bounded logical table query through the same planner used by `ctx.db.query`.
///
/// This entry point exists for trusted Product adapters such as the authenticated Management Data
/// Explorer. It does not grant capabilities or execute application code.
///
/// # Errors
///
/// Returns the same sanitized storage and data-planning failures as a Function query.
pub async fn execute_logical_query(
    store: Arc<dyn LogicalStore>,
    scope: EnvironmentScope,
    schema: Arc<SchemaCatalog>,
    request: DataQueryRequest,
    deadline: Instant,
) -> Result<LogicalQueryOutcome, ExecutionError> {
    let session = QueryReadSession::new(
        store,
        scope,
        Arc::new(QueryTelemetry::default()),
        Some(schema),
    );
    let query = session
        .query(request, deadline, CancellationToken::new())
        .await;
    let summary = session
        .finish(None)
        .await
        .map_err(SessionFailure::into_execution)?;
    let page = query.map_err(ExecutionError::Data)?;
    let snapshot_sequence = summary
        .snapshot_sequence
        .ok_or(ExecutionError::Data(DataReadError::Unavailable))?;
    Ok(LogicalQueryOutcome {
        page,
        snapshot_sequence,
    })
}

/// Stable Query composition failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionError {
    /// Safe Runtime rejected or failed the invocation.
    #[error("query runtime failed")]
    Runtime(RuntimeError),
    /// Logical storage failed; the exact sanitized Store category is retained.
    #[error("query storage failed")]
    Storage(StoreError),
    /// Data broker validation, limit, cancellation, or deadline failed.
    #[error("query data broker failed")]
    Data(DataReadError),
}

impl ExecutionError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Runtime(error) => error.code(),
            Self::Storage(error) => error.code(),
            Self::Data(error) => error.code(),
        }
    }

    /// Whether retrying unchanged may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        match self {
            Self::Runtime(error) => error.retryable(),
            Self::Storage(error) => error.retryable(),
            Self::Data(DataReadError::Unavailable | DataReadError::Timeout) => true,
            Self::Data(
                DataReadError::InvalidRequest
                | DataReadError::Storage
                | DataReadError::Cancelled
                | DataReadError::LimitExceeded
                | DataReadError::QueryRequiresIndex,
            ) => false,
        }
    }
}

/// Bounded process-local Query counters with no request-controlled labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryTelemetrySnapshot {
    /// Query executions attempted.
    pub executions: u64,
    /// Successful outcomes.
    pub succeeded: u64,
    /// Point reads attempted.
    pub point_reads: u64,
    /// Range reads attempted.
    pub range_reads: u64,
    /// Index rows returned.
    pub rows: u64,
    /// Dependencies returned.
    pub dependencies: u64,
    /// Runtime failures.
    pub runtime_failures: u64,
    /// Store/broker failures.
    pub data_failures: u64,
    /// Aggregate elapsed microseconds, saturating at `u64::MAX`.
    pub elapsed_micros: u64,
}

#[derive(Debug, Default)]
struct QueryTelemetry {
    executions: AtomicU64,
    succeeded: AtomicU64,
    point_reads: AtomicU64,
    range_reads: AtomicU64,
    rows: AtomicU64,
    dependencies: AtomicU64,
    runtime_failures: AtomicU64,
    data_failures: AtomicU64,
    elapsed_micros: AtomicU64,
}

/// Product Base coordinator for one read-only Query execution.
#[derive(Clone)]
pub struct QueryExecutor {
    runtime: RuntimeSupervisor,
    store: Arc<dyn LogicalStore>,
    telemetry: Arc<QueryTelemetry>,
    schema: Option<Arc<SchemaCatalog>>,
}

impl fmt::Debug for QueryExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryExecutor")
            .field("backend", &self.store.backend())
            .finish_non_exhaustive()
    }
}

impl QueryExecutor {
    /// Composes an existing bounded Runtime Supervisor and Logical Store.
    #[must_use]
    pub fn new(runtime: RuntimeSupervisor, store: Arc<dyn LogicalStore>) -> Self {
        Self {
            runtime,
            store,
            telemetry: Arc::new(QueryTelemetry::default()),
            schema: None,
        }
    }

    /// Attaches one immutable active index catalog for logical query planning.
    #[must_use]
    pub fn with_schema_catalog(mut self, schema: Arc<SchemaCatalog>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Executes one pre-authorized Query and closes its optional snapshot before returning.
    ///
    /// # Errors
    ///
    /// Returns a stable runtime, exact sanitized storage, or data-broker error. A latched Store
    /// failure dominates any value returned after user JavaScript catches an Op rejection.
    pub async fn execute(
        &self,
        request: InvocationRequest,
    ) -> Result<QueryOutcome, ExecutionError> {
        self.execute_with_deadline(request, None).await
    }

    pub(crate) async fn execute_nested(
        &self,
        request: InvocationRequest,
        deadline: Instant,
    ) -> Result<QueryOutcome, ExecutionError> {
        self.execute_with_deadline(request, Some(deadline)).await
    }

    async fn execute_with_deadline(
        &self,
        request: InvocationRequest,
        inherited_deadline: Option<Instant>,
    ) -> Result<QueryOutcome, ExecutionError> {
        let started = Instant::now();
        self.telemetry.executions.fetch_add(1, Ordering::Relaxed);
        let active_schema = if let Some(schema) = &self.schema {
            Some(Arc::clone(schema))
        } else if matches!(
            request.manifest().runtime_version.as_str(),
            "runku-js-1" | "runku-js-2" | "runku-js-3" | "runku-js"
        ) {
            let bundle = decode_safe_esm_bundle(request.artifact_bytes())
                .map_err(|_| ExecutionError::Runtime(RuntimeError::InvalidArtifact))?;
            let resource = bundle
                .resource(request.index_contract_hash())
                .ok_or(ExecutionError::Runtime(RuntimeError::InvalidArtifact))?;
            let schema = decode_schema_catalog(resource.as_bytes())
                .map_err(|_| ExecutionError::Runtime(RuntimeError::InvalidArtifact))?;
            if schema.project_id() != request.scope().project_id()
                || schema.digest().as_slice() != request.index_contract_hash().as_bytes()
            {
                return Err(ExecutionError::Runtime(RuntimeError::InvalidArtifact));
            }
            Some(Arc::new(schema))
        } else {
            None
        };
        let session = Arc::new(QueryReadSession::new(
            Arc::clone(&self.store),
            request.scope(),
            Arc::clone(&self.telemetry),
            active_schema,
        ));
        let selected = selected_query(&request).map_err(ExecutionError::Runtime)?;
        let attached = attach_query_capabilities(
            request.clone(),
            &selected,
            self.runtime.clone(),
            session.clone(),
        )
        .map_err(ExecutionError::Runtime)?;
        let runtime_result = match inherited_deadline {
            Some(deadline) => self.runtime.invoke_nested_until(attached, deadline).await,
            None => self.runtime.invoke(attached).await,
        };
        let summary = session.finish(runtime_result.as_ref().err().copied()).await;
        let result = match summary {
            Err(failure) => Err(failure.into_execution()),
            Ok(summary) => runtime_result
                .map(|value| QueryOutcome {
                    value,
                    snapshot_sequence: summary.snapshot_sequence,
                    dependencies: summary.dependencies,
                })
                .map_err(ExecutionError::Runtime),
        };
        match &result {
            Ok(outcome) => {
                self.telemetry.succeeded.fetch_add(1, Ordering::Relaxed);
                self.telemetry.dependencies.fetch_add(
                    u64::try_from(outcome.dependencies.len()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            Err(ExecutionError::Runtime(_)) => {
                self.telemetry
                    .runtime_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(ExecutionError::Storage(_) | ExecutionError::Data(_)) => {
                self.telemetry.data_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let _ = self.telemetry.elapsed_micros.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(elapsed)),
        );
        result
    }

    /// Returns bounded aggregate telemetry.
    #[must_use]
    pub fn telemetry(&self) -> QueryTelemetrySnapshot {
        self.telemetry.snapshot()
    }
}

fn selected_query(request: &InvocationRequest) -> Result<FunctionManifest, RuntimeError> {
    let selected = request
        .manifest()
        .functions
        .iter()
        .find(|function| function.id == request.function_id())
        .cloned()
        .ok_or(RuntimeError::InvalidInvocation)?;
    if selected.function_type != FunctionType::Query {
        return Err(RuntimeError::InvalidInvocation);
    }
    Ok(selected)
}

fn attach_query_capabilities(
    mut request: InvocationRequest,
    selected: &FunctionManifest,
    runtime: RuntimeSupervisor,
    session: Arc<QueryReadSession>,
) -> Result<InvocationRequest, RuntimeError> {
    if selected.capabilities.contains(&Capability::DbRead) {
        request = request.with_data(session.clone())?;
    }
    if selected.capabilities.contains(&Capability::FunctionQuery) {
        let broker = Arc::new(QueryFunctionBroker {
            runtime,
            root: request.clone(),
            session,
        });
        request = request.with_functions(broker)?;
    }
    Ok(request)
}

struct QueryFunctionBroker {
    runtime: RuntimeSupervisor,
    root: InvocationRequest,
    session: Arc<QueryReadSession>,
}

impl fmt::Debug for QueryFunctionBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryFunctionBroker")
            .field("scope", &self.root.scope())
            .field("depth", &self.root.nested_depth())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl FunctionInvoke for QueryFunctionBroker {
    async fn invoke(
        &self,
        call: FunctionCallRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CanonicalValue, FunctionCallError> {
        if call.kind != FunctionCallKind::Query || cancellation.is_cancelled() {
            return Err(FunctionCallError::Denied);
        }
        let (child, selected) = prepare_child(&self.root, call, deadline)?;
        let attached = attach_query_capabilities(
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

impl QueryTelemetry {
    fn snapshot(&self) -> QueryTelemetrySnapshot {
        QueryTelemetrySnapshot {
            executions: self.executions.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            point_reads: self.point_reads.load(Ordering::Relaxed),
            range_reads: self.range_reads.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            dependencies: self.dependencies.load(Ordering::Relaxed),
            runtime_failures: self.runtime_failures.load(Ordering::Relaxed),
            data_failures: self.data_failures.load(Ordering::Relaxed),
            elapsed_micros: self.elapsed_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SessionFailure {
    Store(StoreError),
    Data(DataReadError),
}

impl SessionFailure {
    const fn into_execution(self) -> ExecutionError {
        match self {
            Self::Store(error) => ExecutionError::Storage(error),
            Self::Data(error) => ExecutionError::Data(error),
        }
    }
}

struct SessionState {
    snapshot: Option<Box<dyn ReadSnapshot>>,
    snapshot_sequence: Option<u64>,
    dependencies: BTreeSet<ReadDependency>,
    scan_rows: usize,
    failure: Option<SessionFailure>,
    closed: bool,
}

struct QueryReadSession {
    store: Arc<dyn LogicalStore>,
    scope: EnvironmentScope,
    state: Mutex<SessionState>,
    telemetry: Arc<QueryTelemetry>,
    active_operations: AtomicU64,
    schema: Option<Arc<SchemaCatalog>>,
}

impl fmt::Debug for QueryReadSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryReadSession")
            .field("backend", &self.store.backend())
            .finish_non_exhaustive()
    }
}

impl QueryReadSession {
    fn new(
        store: Arc<dyn LogicalStore>,
        scope: EnvironmentScope,
        telemetry: Arc<QueryTelemetry>,
        schema: Option<Arc<SchemaCatalog>>,
    ) -> Self {
        Self {
            store,
            scope,
            state: Mutex::new(SessionState {
                snapshot: None,
                snapshot_sequence: None,
                dependencies: BTreeSet::new(),
                scan_rows: 0,
                failure: None,
                closed: false,
            }),
            telemetry,
            active_operations: AtomicU64::new(0),
            schema,
        }
    }

    async fn ensure_snapshot<'a>(
        &'a self,
        state: &'a mut SessionState,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<&'a mut Box<dyn ReadSnapshot>, DataReadError> {
        if state.closed || state.failure.is_some() {
            return Err(DataReadError::Unavailable);
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
            state.snapshot_sequence = Some(snapshot.commit_sequence());
            state.snapshot = Some(snapshot);
        }
        state.snapshot.as_mut().ok_or(DataReadError::Unavailable)
    }

    fn latch_store(state: &mut SessionState, error: StoreError) -> DataReadError {
        state.failure = Some(SessionFailure::Store(error));
        DataReadError::Storage
    }

    fn latch_limit(state: &mut SessionState) -> DataReadError {
        state.failure = Some(SessionFailure::Data(DataReadError::LimitExceeded));
        DataReadError::LimitExceeded
    }

    async fn finish(
        &self,
        runtime_failure: Option<RuntimeError>,
    ) -> Result<SessionSummary, SessionFailure> {
        let (snapshot, summary, existing_failure) = {
            let mut state = self.state.lock().await;
            state.closed = true;
            let summary = SessionSummary {
                snapshot_sequence: state.snapshot_sequence,
                dependencies: state.dependencies.iter().cloned().collect(),
            };
            // The V8 watchdog and the broker share one absolute deadline. If termination drops an
            // in-flight op before its future can latch the same timeout/cancellation, retain the
            // broker classification. This makes the public error independent of scheduler order.
            let abandoned_operation = self.active_operations.load(Ordering::Acquire) != 0;
            let failure = state.failure.or_else(|| {
                if !abandoned_operation {
                    return None;
                }
                match runtime_failure {
                    Some(RuntimeError::DeadlineExceeded) => {
                        Some(SessionFailure::Data(DataReadError::Timeout))
                    }
                    Some(RuntimeError::Cancelled) => {
                        Some(SessionFailure::Data(DataReadError::Cancelled))
                    }
                    None => Some(SessionFailure::Data(DataReadError::Unavailable)),
                    _ => None,
                }
            });
            (state.snapshot.take(), summary, failure)
        };
        if let Some(snapshot) = snapshot {
            let close = snapshot.close();
            let cleanup_deadline = Instant::now()
                .checked_add(Duration::from_secs(1))
                .ok_or(SessionFailure::Data(DataReadError::Timeout))?;
            let close_result = match tokio::time::timeout_at(
                tokio::time::Instant::from_std(cleanup_deadline),
                close,
            )
            .await
            {
                Err(_) => Err(SessionFailure::Data(DataReadError::Timeout)),
                Ok(Err(error)) => Err(SessionFailure::Store(error)),
                Ok(Ok(())) => Ok(()),
            };
            close_result?;
        }
        if let Some(failure) = existing_failure {
            return Err(failure);
        }
        Ok(summary)
    }
}

struct SessionSummary {
    snapshot_sequence: Option<u64>,
    dependencies: Vec<ReadDependency>,
}

impl QueryReadSession {
    async fn get_inner(
        &self,
        request: DataGetRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<DataDocument>, DataReadError> {
        self.telemetry.point_reads.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().await;
        let document = {
            let snapshot = self
                .ensure_snapshot(&mut state, deadline, cancellation.clone())
                .await?;
            let read = snapshot.get_document(request.table_id, request.document_id);
            tokio::select! {
                () = cancellation.cancelled() => Err(SessionFailure::Data(DataReadError::Cancelled)),
                result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), read) => {
                    match result {
                        Err(_) => Err(SessionFailure::Data(DataReadError::Timeout)),
                        Ok(Err(error)) => Err(SessionFailure::Store(error)),
                        Ok(Ok(document)) => Ok(document),
                    }
                }
            }
        };
        let document = match document {
            Ok(document) => document,
            Err(SessionFailure::Store(error)) => {
                return Err(Self::latch_store(&mut state, error));
            }
            Err(SessionFailure::Data(error)) => {
                state.failure = Some(SessionFailure::Data(error));
                return Err(error);
            }
        };
        let sequence = state.snapshot_sequence.ok_or(DataReadError::Unavailable)?;
        if document.as_ref().is_some_and(|value| {
            value.table_id != request.table_id
                || value.document_id != request.document_id
                || value.revision == 0
                || value.commit_sequence > sequence
                || value.created_at > value.updated_at
        }) {
            return Err(Self::latch_store(&mut state, StoreError::Corruption));
        }
        let dependency = ReadDependency::Point {
            table_id: request.table_id,
            document_id: request.document_id,
            observed_revision: document.as_ref().map(|value| value.revision),
            snapshot_sequence: sequence,
        };
        if !state.dependencies.contains(&dependency) && state.dependencies.len() >= MAX_DEPENDENCIES
        {
            return Err(Self::latch_limit(&mut state));
        }
        state.dependencies.insert(dependency);
        Ok(document.map(|value| DataDocument {
            table_id: value.table_id,
            document_id: value.document_id,
            revision: value.revision,
            commit_sequence: value.commit_sequence,
            created_at: value.created_at,
            updated_at: value.updated_at,
            value: value.value,
        }))
    }

    async fn scan_inner(
        &self,
        request: DataScanRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Vec<DataIndexEntry>, DataReadError> {
        self.telemetry.range_reads.fetch_add(1, Ordering::Relaxed);
        let (lower, lower_dependency) = convert_bound(request.lower)?;
        let (upper, upper_dependency) = convert_bound(request.upper)?;
        let range = IndexRange::between(lower, upper);
        range
            .validate(request.limit)
            .map_err(|_| DataReadError::InvalidRequest)?;
        let mut state = self.state.lock().await;
        let entries = {
            let snapshot = self
                .ensure_snapshot(&mut state, deadline, cancellation.clone())
                .await?;
            let read = snapshot.scan_index(request.index_id, &range, request.limit);
            tokio::select! {
                () = cancellation.cancelled() => Err(SessionFailure::Data(DataReadError::Cancelled)),
                result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), read) => {
                    match result {
                        Err(_) => Err(SessionFailure::Data(DataReadError::Timeout)),
                        Ok(Err(error)) => Err(SessionFailure::Store(error)),
                        Ok(Ok(entries)) => Ok(entries),
                    }
                }
            }
        };
        let entries = match entries {
            Ok(entries) => entries,
            Err(SessionFailure::Store(error)) => {
                return Err(Self::latch_store(&mut state, error));
            }
            Err(SessionFailure::Data(error)) => {
                state.failure = Some(SessionFailure::Data(error));
                return Err(error);
            }
        };
        let sequence = state.snapshot_sequence.ok_or(DataReadError::Unavailable)?;
        if !valid_entries(&entries, request.index_id, &range, request.limit, sequence) {
            return Err(Self::latch_store(&mut state, StoreError::Corruption));
        }
        let next_rows = state
            .scan_rows
            .checked_add(entries.len())
            .ok_or_else(|| Self::latch_limit(&mut state))?;
        if next_rows > MAX_SCAN_ROWS {
            return Err(Self::latch_limit(&mut state));
        }
        state.scan_rows = next_rows;
        self.telemetry.rows.fetch_add(
            u64::try_from(entries.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let dependency = ReadDependency::Range {
            index_id: request.index_id,
            lower: lower_dependency,
            upper: upper_dependency,
            snapshot_sequence: sequence,
        };
        if !state.dependencies.contains(&dependency) && state.dependencies.len() >= MAX_DEPENDENCIES
        {
            return Err(Self::latch_limit(&mut state));
        }
        state.dependencies.insert(dependency);
        Ok(entries
            .into_iter()
            .map(|value| DataIndexEntry {
                index_id: value.index_id,
                key: value.key.as_bytes().to_vec(),
                table_id: value.table_id,
                document_id: value.document_id,
                document_revision: value.document_revision,
                commit_sequence: value.commit_sequence,
            })
            .collect())
    }

    #[allow(clippy::too_many_lines)]
    async fn query_inner(
        &self,
        request: DataQueryRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<DataQueryPage, DataReadError> {
        validate_query(&request)?;
        let digest = query_digest(&request)?;
        let default_plan = request.filters.is_empty() && uses_physical_table_order(&request.order);
        if !default_plan
            && let Some(plan) = self
                .schema
                .as_deref()
                .and_then(|schema| indexed_query_plan(schema, &request))
        {
            return self
                .query_indexed(request, digest, plan, deadline, cancellation)
                .await;
        }
        if request
            .filters
            .iter()
            .any(|filter| filter.operator == DataQueryOperator::Search)
        {
            return Err(DataReadError::QueryRequiresIndex);
        }
        let (offset, after) = if default_plan {
            (
                0,
                decode_default_query_cursor(request.cursor.as_deref(), &digest)?,
            )
        } else {
            (
                decode_query_cursor(request.cursor.as_deref(), &digest)?,
                None,
            )
        };
        let scan_limit = if default_plan {
            request.limit.saturating_add(1)
        } else {
            2_001
        };
        self.telemetry.range_reads.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().await;
        let documents = {
            let snapshot = self
                .ensure_snapshot(&mut state, deadline, cancellation.clone())
                .await?;
            let read = snapshot.scan_documents(request.table_id, after, scan_limit);
            tokio::select! {
                () = cancellation.cancelled() => Err(SessionFailure::Data(DataReadError::Cancelled)),
                result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), read) => {
                    match result {
                        Err(_) => Err(SessionFailure::Data(DataReadError::Timeout)),
                        Ok(Err(error)) => Err(SessionFailure::Store(error)),
                        Ok(Ok(documents)) => Ok(documents),
                    }
                }
            }
        };
        let mut documents = match documents {
            Ok(documents) => documents,
            Err(SessionFailure::Store(error)) => {
                return Err(Self::latch_store(&mut state, error));
            }
            Err(SessionFailure::Data(error)) => {
                state.failure = Some(SessionFailure::Data(error));
                return Err(error);
            }
        };
        if documents.len() > MAX_UNINDEXED_TABLE_ROWS {
            return Err(DataReadError::QueryRequiresIndex);
        }
        let sequence = state.snapshot_sequence.ok_or(DataReadError::Unavailable)?;
        if documents.iter().any(|document| {
            document.table_id != request.table_id
                || document.revision == 0
                || document.commit_sequence > sequence
                || document.created_at > document.updated_at
        }) {
            return Err(Self::latch_store(&mut state, StoreError::Corruption));
        }
        if default_plan {
            let has_more = documents.len()
                > usize::try_from(request.limit).map_err(|_| DataReadError::InvalidRequest)?;
            if has_more {
                documents.pop();
            }
            let next_cursor = if has_more {
                documents
                    .last()
                    .map(|last| encode_default_query_cursor(&digest, last))
            } else {
                None
            };
            let page = documents
                .into_iter()
                .map(document_into_runtime)
                .collect::<Vec<_>>();
            self.telemetry.rows.fetch_add(
                u64::try_from(page.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            state.dependencies.insert(ReadDependency::Table {
                table_id: request.table_id,
                snapshot_sequence: sequence,
            });
            return Ok(DataQueryPage {
                documents: page,
                next_cursor,
            });
        }
        let scanned_documents = documents.len();
        documents.retain(|document| {
            request
                .filters
                .iter()
                .all(|filter| filter_matches(document, filter))
        });
        let order = effective_order(&request.order);
        documents.sort_by(|left, right| compare_documents(left, right, &order));
        if offset > documents.len() {
            return Err(DataReadError::InvalidRequest);
        }
        let limit = usize::try_from(request.limit).map_err(|_| DataReadError::InvalidRequest)?;
        let end = offset.saturating_add(limit).min(documents.len());
        let has_more = end < documents.len();
        let page = documents[offset..end]
            .iter()
            .cloned()
            .map(document_into_runtime)
            .collect::<Vec<_>>();
        let next_cursor = has_more.then(|| encode_query_cursor(&digest, end));
        state.scan_rows = state
            .scan_rows
            .checked_add(scanned_documents)
            .ok_or_else(|| Self::latch_limit(&mut state))?;
        if state.scan_rows > MAX_SCAN_ROWS {
            return Err(Self::latch_limit(&mut state));
        }
        self.telemetry.rows.fetch_add(
            u64::try_from(page.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let dependency = ReadDependency::Table {
            table_id: request.table_id,
            snapshot_sequence: sequence,
        };
        if !state.dependencies.contains(&dependency) && state.dependencies.len() >= MAX_DEPENDENCIES
        {
            return Err(Self::latch_limit(&mut state));
        }
        state.dependencies.insert(dependency);
        Ok(DataQueryPage {
            documents: page,
            next_cursor,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn query_indexed(
        &self,
        request: DataQueryRequest,
        digest: [u8; 32],
        plan: IndexedQueryPlan,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<DataQueryPage, DataReadError> {
        let mut after = decode_index_query_cursor(request.cursor.as_deref(), &digest)?;
        if plan.empty {
            return Ok(DataQueryPage {
                documents: Vec::new(),
                next_cursor: None,
            });
        }
        let target = usize::try_from(request.limit)
            .map_err(|_| DataReadError::InvalidRequest)?
            .saturating_add(1);
        let mut matched = Vec::<(DataDocument, IndexScanCursor)>::new();
        let mut scanned = 0_usize;
        let mut exhausted = false;
        let mut state = self.state.lock().await;
        while matched.len() < target && scanned < MAX_UNINDEXED_TABLE_ROWS {
            let remaining = MAX_UNINDEXED_TABLE_ROWS.saturating_sub(scanned);
            let batch_limit = u32::try_from(remaining.min(256)).unwrap_or(256);
            let entries = {
                let snapshot = self
                    .ensure_snapshot(&mut state, deadline, cancellation.clone())
                    .await?;
                let read = snapshot.scan_index_page(
                    plan.index_id,
                    &plan.range,
                    after.clone(),
                    plan.direction,
                    batch_limit,
                );
                tokio::select! {
                    () = cancellation.cancelled() => Err(SessionFailure::Data(DataReadError::Cancelled)),
                    result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), read) => {
                        match result {
                            Err(_) => Err(SessionFailure::Data(DataReadError::Timeout)),
                            Ok(Err(error)) => Err(SessionFailure::Store(error)),
                            Ok(Ok(entries)) => Ok(entries),
                        }
                    }
                }
            };
            let entries = match entries {
                Ok(entries) => entries,
                Err(SessionFailure::Store(error)) => {
                    return Err(Self::latch_store(&mut state, error));
                }
                Err(SessionFailure::Data(error)) => {
                    state.failure = Some(SessionFailure::Data(error));
                    return Err(error);
                }
            };
            if entries.is_empty() {
                exhausted = true;
                break;
            }
            scanned = scanned.saturating_add(entries.len());
            let batch_was_full =
                entries.len() == usize::try_from(batch_limit).unwrap_or(usize::MAX);
            for entry in entries {
                if entry.index_id != plan.index_id || entry.table_id != request.table_id {
                    return Err(Self::latch_store(&mut state, StoreError::Corruption));
                }
                let cursor = IndexScanCursor {
                    key: entry.key.clone(),
                    document_id: entry.document_id,
                };
                after = Some(cursor.clone());
                let read = {
                    let snapshot = self
                        .ensure_snapshot(&mut state, deadline, cancellation.clone())
                        .await?;
                    snapshot
                        .get_document(entry.table_id, entry.document_id)
                        .await
                };
                let document = match read {
                    Ok(Some(document)) => document,
                    Ok(None) => {
                        return Err(Self::latch_store(&mut state, StoreError::Corruption));
                    }
                    Err(error) => return Err(Self::latch_store(&mut state, error)),
                };
                let dependency = ReadDependency::Point {
                    table_id: document.table_id,
                    document_id: document.document_id,
                    observed_revision: Some(document.revision),
                    snapshot_sequence: state.snapshot_sequence.ok_or(DataReadError::Unavailable)?,
                };
                if !state.dependencies.contains(&dependency)
                    && state.dependencies.len() >= MAX_DEPENDENCIES
                {
                    return Err(Self::latch_limit(&mut state));
                }
                state.dependencies.insert(dependency);
                if document.revision != entry.document_revision
                    || request
                        .filters
                        .iter()
                        .any(|filter| !filter_matches(&document, filter))
                {
                    continue;
                }
                matched.push((document_into_runtime(document), cursor));
                if matched.len() == target {
                    break;
                }
            }
            if !batch_was_full {
                exhausted = true;
                break;
            }
        }
        if matched.len() < target && !exhausted && scanned >= MAX_UNINDEXED_TABLE_ROWS {
            return Err(DataReadError::QueryRequiresIndex);
        }
        let has_more = matched.len() == target;
        if has_more {
            matched.pop();
        }
        let next_cursor = if has_more {
            matched
                .last()
                .map(|(_, cursor)| encode_index_query_cursor(&digest, cursor))
        } else {
            None
        };
        let sequence = state.snapshot_sequence.ok_or(DataReadError::Unavailable)?;
        state.scan_rows = state
            .scan_rows
            .checked_add(scanned)
            .ok_or_else(|| Self::latch_limit(&mut state))?;
        if state.scan_rows > MAX_SCAN_ROWS {
            return Err(Self::latch_limit(&mut state));
        }
        state.dependencies.insert(ReadDependency::Range {
            index_id: plan.index_id,
            lower: dependency_bound(plan.range.lower()),
            upper: dependency_bound(plan.range.upper()),
            snapshot_sequence: sequence,
        });
        self.telemetry.rows.fetch_add(
            u64::try_from(matched.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(DataQueryPage {
            documents: matched.into_iter().map(|(document, _)| document).collect(),
            next_cursor,
        })
    }
}

#[async_trait]
impl DataRead for QueryReadSession {
    async fn get(
        &self,
        request: DataGetRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<DataDocument>, DataReadError> {
        self.active_operations.fetch_add(1, Ordering::AcqRel);
        let result = self.get_inner(request, deadline, cancellation).await;
        self.active_operations.fetch_sub(1, Ordering::AcqRel);
        result
    }

    async fn scan(
        &self,
        request: DataScanRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Vec<DataIndexEntry>, DataReadError> {
        self.active_operations.fetch_add(1, Ordering::AcqRel);
        let result = self.scan_inner(request, deadline, cancellation).await;
        self.active_operations.fetch_sub(1, Ordering::AcqRel);
        result
    }

    async fn query(
        &self,
        request: DataQueryRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<DataQueryPage, DataReadError> {
        self.active_operations.fetch_add(1, Ordering::AcqRel);
        let result = self.query_inner(request, deadline, cancellation).await;
        self.active_operations.fetch_sub(1, Ordering::AcqRel);
        result
    }
}

#[derive(Clone)]
struct IndexedQueryPlan {
    index_id: IndexId,
    range: IndexRange,
    direction: IndexScanDirection,
    empty: bool,
}

#[allow(clippy::too_many_lines)]
fn indexed_query_plan(
    schema: &SchemaCatalog,
    request: &DataQueryRequest,
) -> Option<IndexedQueryPlan> {
    if let Some(search) = request
        .filters
        .iter()
        .find(|filter| filter.operator == DataQueryOperator::Search)
    {
        let CanonicalValue::String(term) = &search.value else {
            return None;
        };
        let term = normalize_search_term(term).ok()?;
        let definition = schema
            .indexes_for_table(request.table_id)
            .find(|definition| {
                definition.kind() == IndexKind::Search
                    && definition.fields()[0].segments().join(".") == search.field
            })?;
        if request
            .order
            .iter()
            .any(|component| component.field != "$id")
            || request
                .order
                .windows(2)
                .any(|pair| pair[0].direction != pair[1].direction)
        {
            return None;
        }
        let key = IndexKey::encode(&[IndexValue::String(term)]).ok()?;
        return Some(IndexedQueryPlan {
            index_id: definition.index_id(),
            range: IndexRange::prefix(&key.prefix(1).ok()?).ok()?,
            direction: match request
                .order
                .first()
                .map_or(DataQueryDirection::Ascending, |component| {
                    component.direction
                }) {
                DataQueryDirection::Ascending => IndexScanDirection::Ascending,
                DataQueryDirection::Descending => IndexScanDirection::Descending,
            },
            empty: false,
        });
    }
    'indexes: for definition in schema.indexes_for_table(request.table_id) {
        if definition.kind() != IndexKind::Ordered {
            continue;
        }
        let mut prefix_values = Vec::new();
        for field in definition.fields() {
            let name = field.segments().join(".");
            let Some(filter) = request
                .filters
                .iter()
                .find(|filter| filter.field == name && filter.operator == DataQueryOperator::Equal)
            else {
                break;
            };
            let value = IndexValue::try_from(&filter.value).ok()?;
            prefix_values.push(value);
        }
        let ordered = request
            .order
            .iter()
            .filter(|component| component.field != "$id")
            .collect::<Vec<_>>();
        if ordered
            .iter()
            .any(|component| component.field.starts_with('$'))
        {
            continue;
        }
        let direction = request
            .order
            .first()
            .map_or(DataQueryDirection::Ascending, |component| {
                component.direction
            });
        if request
            .order
            .iter()
            .any(|component| component.direction != direction)
        {
            continue;
        }
        let suffix = &definition.fields()[prefix_values.len()..];
        if ordered.is_empty() {
            if prefix_values.len() != definition.fields().len()
                || request
                    .order
                    .iter()
                    .any(|component| component.field != "$id")
            {
                continue;
            }
        } else if suffix.len() != ordered.len()
            || suffix
                .iter()
                .zip(&ordered)
                .any(|(field, order)| field.segments().join(".") != order.field)
        {
            continue 'indexes;
        }
        let mut range = if prefix_values.is_empty() {
            IndexRange::all()
        } else {
            let key = IndexKey::encode(&prefix_values).ok()?;
            IndexRange::prefix(&key.prefix(prefix_values.len()).ok()?).ok()?
        };
        if let Some(field) = suffix.first() {
            range = constrain_index_range(
                &range,
                &prefix_values,
                &field.segments().join("."),
                &request.filters,
            )?;
        }
        let empty = range_is_empty(&range);
        return Some(IndexedQueryPlan {
            index_id: definition.index_id(),
            range,
            direction: match direction {
                DataQueryDirection::Ascending => IndexScanDirection::Ascending,
                DataQueryDirection::Descending => IndexScanDirection::Descending,
            },
            empty,
        });
    }
    None
}

fn uses_physical_table_order(order: &[DataQueryOrder]) -> bool {
    order.is_empty()
        || matches!(
            order,
            [DataQueryOrder {
                field,
                direction: DataQueryDirection::Descending,
            }] if field == "$createdAt"
        )
        || matches!(
            order,
            [
                DataQueryOrder {
                    field: created_at,
                    direction: DataQueryDirection::Descending,
                },
                DataQueryOrder {
                    field: id,
                    direction: DataQueryDirection::Descending,
                },
            ] if created_at == "$createdAt" && id == "$id"
        )
}

fn constrain_index_range(
    range: &IndexRange,
    prefix_values: &[IndexValue],
    field: &str,
    filters: &[DataQueryFilter],
) -> Option<IndexRange> {
    let mut lower = range.lower().clone();
    let mut upper = range.upper().clone();
    for filter in filters.iter().filter(|filter| filter.field == field) {
        let mut values = prefix_values.to_vec();
        values.push(IndexValue::try_from(&filter.value).ok()?);
        let key = IndexKey::encode(&values).ok()?;
        let key_start = key.as_bytes().to_vec();
        let key_end = key.prefix(values.len()).ok()?.exclusive_end().ok()?;
        match filter.operator {
            DataQueryOperator::GreaterThan => {
                lower = stronger_lower(lower, KeyBound::Inclusive(key_end));
            }
            DataQueryOperator::GreaterThanOrEqual => {
                lower = stronger_lower(lower, KeyBound::Inclusive(key_start));
            }
            DataQueryOperator::LessThan => {
                upper = stronger_upper(upper, KeyBound::Exclusive(key_start));
            }
            DataQueryOperator::LessThanOrEqual => {
                upper = stronger_upper(upper, KeyBound::Exclusive(key_end));
            }
            DataQueryOperator::Equal
            | DataQueryOperator::NotEqual
            | DataQueryOperator::Contains
            | DataQueryOperator::Search => {}
        }
    }
    Some(IndexRange::between(lower, upper))
}

fn stronger_lower(current: KeyBound, candidate: KeyBound) -> KeyBound {
    match (current.bytes(), candidate.bytes()) {
        (None, _) => candidate,
        (_, None) => current,
        (Some(left), Some(right)) => match left.cmp(right) {
            CompareOrdering::Less => candidate,
            CompareOrdering::Greater => current,
            CompareOrdering::Equal => {
                if matches!(current, KeyBound::Exclusive(_)) {
                    current
                } else {
                    candidate
                }
            }
        },
    }
}

fn stronger_upper(current: KeyBound, candidate: KeyBound) -> KeyBound {
    match (current.bytes(), candidate.bytes()) {
        (None, _) => candidate,
        (_, None) => current,
        (Some(left), Some(right)) => match left.cmp(right) {
            CompareOrdering::Less => current,
            CompareOrdering::Greater => candidate,
            CompareOrdering::Equal => {
                if matches!(current, KeyBound::Exclusive(_)) {
                    current
                } else {
                    candidate
                }
            }
        },
    }
}

fn range_is_empty(range: &IndexRange) -> bool {
    match (range.lower(), range.upper()) {
        (KeyBound::Unbounded, _) | (_, KeyBound::Unbounded) => false,
        (lower, upper) => {
            let lower = lower.bytes().unwrap_or_default();
            let upper = upper.bytes().unwrap_or_default();
            lower > upper
                || (lower == upper
                    && (!matches!(range.lower(), KeyBound::Inclusive(_))
                        || !matches!(range.upper(), KeyBound::Inclusive(_))))
        }
    }
}

fn dependency_bound(bound: &KeyBound) -> DependencyBound {
    match bound {
        KeyBound::Unbounded => DependencyBound::Unbounded,
        KeyBound::Inclusive(value) => DependencyBound::Inclusive(value.clone()),
        KeyBound::Exclusive(value) => DependencyBound::Exclusive(value.clone()),
    }
}

fn validate_query(request: &DataQueryRequest) -> Result<(), DataReadError> {
    if request.limit == 0
        || request.limit > MAX_QUERY_LIMIT
        || request.filters.len() > 16
        || request.order.len() > 4
    {
        return Err(DataReadError::InvalidRequest);
    }
    for filter in &request.filters {
        query_field_path(&filter.field)?;
        if filter.operator == DataQueryOperator::Search {
            let CanonicalValue::String(term) = &filter.value else {
                return Err(DataReadError::InvalidRequest);
            };
            normalize_search_term(term).map_err(|_| DataReadError::InvalidRequest)?;
        }
    }
    for order in &request.order {
        if !matches!(order.field.as_str(), "$createdAt" | "$updatedAt" | "$id") {
            query_field_path(&order.field)?;
        }
    }
    Ok(())
}

fn query_field_path(value: &str) -> Result<FieldPath, DataReadError> {
    FieldPath::new(value.split('.').map(str::to_owned).collect())
        .map_err(|_| DataReadError::InvalidRequest)
}

fn resolve_field<'a>(document: &'a CanonicalValue, field: &str) -> Option<&'a CanonicalValue> {
    let mut current = document;
    for segment in field.split('.') {
        let CanonicalValue::Object(object) = current else {
            return None;
        };
        current = object.get(segment)?;
    }
    Some(current)
}

fn filter_matches(document: &runku_data::DocumentRecord, filter: &DataQueryFilter) -> bool {
    let Some(actual) = resolve_field(&document.value, &filter.field) else {
        return matches!(filter.operator, DataQueryOperator::NotEqual);
    };
    match filter.operator {
        DataQueryOperator::Equal => actual == &filter.value,
        DataQueryOperator::NotEqual => actual != &filter.value,
        DataQueryOperator::GreaterThan => {
            compare_values(actual, &filter.value) == Some(CompareOrdering::Greater)
        }
        DataQueryOperator::GreaterThanOrEqual => matches!(
            compare_values(actual, &filter.value),
            Some(CompareOrdering::Greater | CompareOrdering::Equal)
        ),
        DataQueryOperator::LessThan => {
            compare_values(actual, &filter.value) == Some(CompareOrdering::Less)
        }
        DataQueryOperator::LessThanOrEqual => matches!(
            compare_values(actual, &filter.value),
            Some(CompareOrdering::Less | CompareOrdering::Equal)
        ),
        DataQueryOperator::Contains => match (actual, &filter.value) {
            (CanonicalValue::String(value), CanonicalValue::String(needle)) => {
                value.contains(needle)
            }
            (CanonicalValue::Array(values), needle) => values.contains(needle),
            _ => false,
        },
        DataQueryOperator::Search => match (actual, &filter.value) {
            (CanonicalValue::String(value), CanonicalValue::String(term)) => {
                normalize_search_term(term).is_ok_and(|term| {
                    value
                        .split(|character: char| !character.is_alphanumeric())
                        .filter(|word| !word.is_empty())
                        .map(|word| {
                            word.chars()
                                .flat_map(char::to_lowercase)
                                .collect::<String>()
                        })
                        .any(|word| word == term)
                })
            }
            _ => false,
        },
    }
}

fn compare_values(left: &CanonicalValue, right: &CanonicalValue) -> Option<CompareOrdering> {
    match (left, right) {
        (CanonicalValue::Null, CanonicalValue::Null) => Some(CompareOrdering::Equal),
        (CanonicalValue::Boolean(left), CanonicalValue::Boolean(right)) => Some(left.cmp(right)),
        (CanonicalValue::Int64(left), CanonicalValue::Int64(right)) => Some(left.cmp(right)),
        (CanonicalValue::Float64(left), CanonicalValue::Float64(right)) => Some(left.cmp(right)),
        (CanonicalValue::String(left), CanonicalValue::String(right)) => Some(left.cmp(right)),
        (CanonicalValue::Bytes(left), CanonicalValue::Bytes(right)) => Some(left.cmp(right)),
        (CanonicalValue::Timestamp(left), CanonicalValue::Timestamp(right)) => {
            Some(left.cmp(right))
        }
        (CanonicalValue::TypedId(left), CanonicalValue::TypedId(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn effective_order(order: &[DataQueryOrder]) -> Vec<DataQueryOrder> {
    if order.is_empty() {
        vec![
            DataQueryOrder {
                field: "$createdAt".to_owned(),
                direction: DataQueryDirection::Descending,
            },
            DataQueryOrder {
                field: "$id".to_owned(),
                direction: DataQueryDirection::Descending,
            },
        ]
    } else {
        let mut result = order.to_vec();
        if !result.iter().any(|component| component.field == "$id") {
            let direction = result
                .last()
                .map_or(DataQueryDirection::Ascending, |component| {
                    component.direction
                });
            result.push(DataQueryOrder {
                field: "$id".to_owned(),
                direction,
            });
        }
        result
    }
}

fn compare_documents(
    left: &runku_data::DocumentRecord,
    right: &runku_data::DocumentRecord,
    order: &[DataQueryOrder],
) -> CompareOrdering {
    for component in order {
        let comparison = match component.field.as_str() {
            "$createdAt" => left.created_at.cmp(&right.created_at),
            "$updatedAt" => left.updated_at.cmp(&right.updated_at),
            "$id" => left.document_id.cmp(&right.document_id),
            field => match (
                resolve_field(&left.value, field),
                resolve_field(&right.value, field),
            ) {
                (None, None) => CompareOrdering::Equal,
                (None, Some(_)) => CompareOrdering::Less,
                (Some(_), None) => CompareOrdering::Greater,
                (Some(left), Some(right)) => {
                    compare_values(left, right).unwrap_or(CompareOrdering::Equal)
                }
            },
        };
        let comparison = match component.direction {
            DataQueryDirection::Ascending => comparison,
            DataQueryDirection::Descending => comparison.reverse(),
        };
        if comparison != CompareOrdering::Equal {
            return comparison;
        }
    }
    CompareOrdering::Equal
}

fn document_into_runtime(value: runku_data::DocumentRecord) -> DataDocument {
    DataDocument {
        table_id: value.table_id,
        document_id: value.document_id,
        revision: value.revision,
        commit_sequence: value.commit_sequence,
        created_at: value.created_at,
        updated_at: value.updated_at,
        value: value.value,
    }
}

fn query_digest(request: &DataQueryRequest) -> Result<[u8; 32], DataReadError> {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_DATA_QUERY_CURSOR_V1\0");
    digest.update(request.table_id.to_string().as_bytes());
    digest.update([0]);
    for filter in &request.filters {
        digest.update(filter.field.as_bytes());
        digest.update([filter.operator as u8]);
        digest
            .update(encode_stored_value(&filter.value).map_err(|_| DataReadError::InvalidRequest)?);
        digest.update([0]);
    }
    for order in effective_order(&request.order) {
        digest.update(order.field.as_bytes());
        digest.update([order.direction as u8, 0]);
    }
    digest.update(request.limit.to_be_bytes());
    Ok(digest.finalize().into())
}

fn encode_query_cursor(digest: &[u8; 32], offset: usize) -> String {
    let mut output = String::with_capacity(3 + 64 + 1 + 4);
    output.push_str("q1:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output.push(':');
    output.push_str(&offset.to_string());
    output
}

fn decode_query_cursor(cursor: Option<&str>, digest: &[u8; 32]) -> Result<usize, DataReadError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let mut expected = encode_query_cursor(digest, 0);
    expected.truncate(67);
    let (prefix, offset) = cursor
        .rsplit_once(':')
        .ok_or(DataReadError::InvalidRequest)?;
    if prefix != expected.trim_end_matches(':') {
        return Err(DataReadError::InvalidRequest);
    }
    offset
        .parse::<usize>()
        .ok()
        .filter(|value| *value <= MAX_UNINDEXED_TABLE_ROWS)
        .ok_or(DataReadError::InvalidRequest)
}

fn encode_default_query_cursor(digest: &[u8; 32], document: &runku_data::DocumentRecord) -> String {
    let prefix = encode_query_cursor(digest, 0);
    let digest_text = prefix
        .strip_prefix("q1:")
        .and_then(|value| value.strip_suffix(":0"))
        .unwrap_or_default();
    format!(
        "qt1:{digest_text}:{}:{}",
        document.created_at.get(),
        document.document_id
    )
}

fn decode_default_query_cursor(
    cursor: Option<&str>,
    digest: &[u8; 32],
) -> Result<Option<TableScanCursor>, DataReadError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let mut parts = cursor.split(':');
    let version = parts.next();
    let cursor_digest = parts.next();
    let created_at = parts.next();
    let document_id = parts.next();
    if version != Some("qt1") || parts.next().is_some() {
        return Err(DataReadError::InvalidRequest);
    }
    let expected = encode_query_cursor(digest, 0);
    let expected_digest = expected
        .strip_prefix("q1:")
        .and_then(|value| value.strip_suffix(":0"));
    if cursor_digest != expected_digest {
        return Err(DataReadError::InvalidRequest);
    }
    Ok(Some(TableScanCursor {
        created_at: runku_value::TimestampMicros::new(
            created_at
                .ok_or(DataReadError::InvalidRequest)?
                .parse()
                .map_err(|_| DataReadError::InvalidRequest)?,
        ),
        document_id: document_id
            .ok_or(DataReadError::InvalidRequest)?
            .parse()
            .map_err(|_| DataReadError::InvalidRequest)?,
    }))
}

fn encode_index_query_cursor(digest: &[u8; 32], cursor: &IndexScanCursor) -> String {
    let digest_text = digest_hex(digest);
    let mut key = String::with_capacity(cursor.key.as_bytes().len() * 2);
    for byte in cursor.key.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(key, "{byte:02x}");
    }
    format!("qi1:{digest_text}:{key}:{}", cursor.document_id)
}

fn decode_index_query_cursor(
    cursor: Option<&str>,
    digest: &[u8; 32],
) -> Result<Option<IndexScanCursor>, DataReadError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let mut parts = cursor.split(':');
    if parts.next() != Some("qi1") || parts.next() != Some(digest_hex(digest).as_str()) {
        return Err(DataReadError::InvalidRequest);
    }
    let key = decode_hex(parts.next().ok_or(DataReadError::InvalidRequest)?)?;
    let document_id = parts
        .next()
        .ok_or(DataReadError::InvalidRequest)?
        .parse()
        .map_err(|_| DataReadError::InvalidRequest)?;
    if parts.next().is_some() {
        return Err(DataReadError::InvalidRequest);
    }
    Ok(Some(IndexScanCursor {
        key: IndexKey::decode(&key).map_err(|_| DataReadError::InvalidRequest)?,
        document_id,
    }))
}

fn digest_hex(digest: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn decode_hex(value: &str) -> Result<Vec<u8>, DataReadError> {
    if !value.len().is_multiple_of(2) || value.len() > IndexKey::MAX_ENCODED_BYTES * 2 {
        return Err(DataReadError::InvalidRequest);
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Result<u8, DataReadError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(DataReadError::InvalidRequest),
    }
}

fn convert_bound(
    value: Option<DataKeyBound>,
) -> Result<(KeyBound, DependencyBound), DataReadError> {
    let Some(value) = value else {
        return Ok((KeyBound::Unbounded, DependencyBound::Unbounded));
    };
    let canonical = IndexKey::decode(&value.key).map_err(|_| DataReadError::InvalidRequest)?;
    let bytes = canonical.as_bytes().to_vec();
    Ok(match value.kind {
        DataBoundKind::Inclusive => (
            KeyBound::Inclusive(bytes.clone()),
            DependencyBound::Inclusive(bytes),
        ),
        DataBoundKind::Exclusive => (
            KeyBound::Exclusive(bytes.clone()),
            DependencyBound::Exclusive(bytes),
        ),
    })
}

fn valid_entries(
    entries: &[runku_data::IndexEntry],
    index_id: IndexId,
    range: &IndexRange,
    limit: u32,
    snapshot_sequence: u64,
) -> bool {
    if entries.len() > usize::try_from(limit).unwrap_or(usize::MAX) {
        return false;
    }
    let mut previous: Option<(&[u8], DocumentId)> = None;
    for entry in entries {
        let key = entry.key.as_bytes();
        if entry.index_id != index_id
            || entry.document_revision == 0
            || entry.commit_sequence > snapshot_sequence
            || !key_in_range(key, range)
            || previous.is_some_and(|value| value >= (key, entry.document_id))
        {
            return false;
        }
        previous = Some((key, entry.document_id));
    }
    true
}

fn key_in_range(key: &[u8], range: &IndexRange) -> bool {
    let lower = match range.lower() {
        KeyBound::Unbounded => true,
        KeyBound::Inclusive(value) => key >= value.as_slice(),
        KeyBound::Exclusive(value) => key > value.as_slice(),
    };
    let upper = match range.upper() {
        KeyBound::Unbounded => true,
        KeyBound::Inclusive(value) => key <= value.as_slice(),
        KeyBound::Exclusive(value) => key < value.as_slice(),
    };
    lower && upper
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;
    use runku_core::{DocumentId, IndexId, ProjectId, TableId};
    use runku_data::{IndexRange, IndexScanDirection};
    use runku_runtime::{
        DataBoundKind, DataKeyBound, DataQueryDirection, DataQueryFilter, DataQueryOperator,
        DataQueryOrder, DataQueryRequest,
    };
    use runku_schema::{FieldPath, IndexDefinition, SchemaCatalog};
    use runku_value::{CanonicalValue, IndexKey, IndexValue};
    use ulid::Ulid;

    use super::{
        DependencyBound, ReadDependency, convert_bound, indexed_query_plan,
        uses_physical_table_order,
    };

    #[test]
    fn equality_only_query_selects_an_exact_ordered_index() -> Result<(), Box<dyn std::error::Error>>
    {
        let project = ProjectId::from_ulid(Ulid::from(1_u128));
        let table = TableId::from_ulid(Ulid::from(2_u128));
        let index = IndexId::from_ulid(Ulid::from(3_u128));
        let catalog = SchemaCatalog::new(
            project,
            vec![IndexDefinition::new(
                index,
                table,
                "by_owner".to_owned(),
                vec![FieldPath::new(vec!["ownerId".to_owned()])?],
            )?],
        )?;
        let request = DataQueryRequest {
            table_id: table,
            filters: vec![DataQueryFilter {
                field: "ownerId".to_owned(),
                operator: DataQueryOperator::Equal,
                value: CanonicalValue::String("principal_1".to_owned()),
            }],
            order: Vec::new(),
            limit: 100,
            cursor: None,
        };
        let plan = indexed_query_plan(&catalog, &request).ok_or("missing exact index plan")?;
        assert_eq!(plan.index_id, index);
        assert_eq!(plan.direction, IndexScanDirection::Ascending);
        assert_ne!(plan.range, IndexRange::all());
        Ok(())
    }

    #[test]
    fn ordered_index_plan_applies_range_bounds_after_equality_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let project = ProjectId::from_ulid(Ulid::from(1_u128));
        let table = TableId::from_ulid(Ulid::from(2_u128));
        let index = IndexId::from_ulid(Ulid::from(3_u128));
        let catalog = SchemaCatalog::new(
            project,
            vec![IndexDefinition::new(
                index,
                table,
                "by_owner_created".to_owned(),
                vec![
                    FieldPath::new(vec!["ownerId".to_owned()])?,
                    FieldPath::new(vec!["createdAt".to_owned()])?,
                ],
            )?],
        )?;
        let request = DataQueryRequest {
            table_id: table,
            filters: vec![
                DataQueryFilter {
                    field: "ownerId".to_owned(),
                    operator: DataQueryOperator::Equal,
                    value: CanonicalValue::String("principal_1".to_owned()),
                },
                DataQueryFilter {
                    field: "createdAt".to_owned(),
                    operator: DataQueryOperator::GreaterThanOrEqual,
                    value: CanonicalValue::Int64(10),
                },
                DataQueryFilter {
                    field: "createdAt".to_owned(),
                    operator: DataQueryOperator::LessThan,
                    value: CanonicalValue::Int64(20),
                },
            ],
            order: vec![DataQueryOrder {
                field: "createdAt".to_owned(),
                direction: DataQueryDirection::Descending,
            }],
            limit: 100,
            cursor: None,
        };
        let plan = indexed_query_plan(&catalog, &request).ok_or("missing range index plan")?;
        let lower = IndexKey::encode(&[
            IndexValue::String("principal_1".to_owned()),
            IndexValue::Int64(10),
        ])?;
        let upper = IndexKey::encode(&[
            IndexValue::String("principal_1".to_owned()),
            IndexValue::Int64(20),
        ])?;
        assert_eq!(plan.index_id, index);
        assert_eq!(plan.direction, IndexScanDirection::Descending);
        assert_eq!(
            plan.range,
            IndexRange::between(
                runku_data::KeyBound::Inclusive(lower.as_bytes().to_vec()),
                runku_data::KeyBound::Exclusive(upper.as_bytes().to_vec()),
            )
        );
        assert!(!plan.empty);
        Ok(())
    }

    #[test]
    fn explicit_physical_order_uses_the_unbounded_table_cursor() {
        assert!(uses_physical_table_order(&[]));
        assert!(uses_physical_table_order(&[DataQueryOrder {
            field: "$createdAt".to_owned(),
            direction: DataQueryDirection::Descending,
        }]));
        assert!(uses_physical_table_order(&[
            DataQueryOrder {
                field: "$createdAt".to_owned(),
                direction: DataQueryDirection::Descending,
            },
            DataQueryOrder {
                field: "$id".to_owned(),
                direction: DataQueryDirection::Descending,
            },
        ]));
        assert!(!uses_physical_table_order(&[DataQueryOrder {
            field: "$createdAt".to_owned(),
            direction: DataQueryDirection::Ascending,
        }]));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn canonical_string_bounds_round_trip(value in ".{0,128}") {
            let key = IndexKey::encode(&[IndexValue::String(value)])?;
            let (_, dependency) = convert_bound(Some(DataKeyBound {
                kind: DataBoundKind::Inclusive,
                key: key.as_bytes().to_vec(),
            }))?;
            prop_assert_eq!(dependency, DependencyBound::Inclusive(key.as_bytes().to_vec()));
        }

        #[test]
        fn duplicate_point_dependencies_canonicalize(
            revision in proptest::option::of(1_u64..=i64::MAX.cast_unsigned())
        ) {
            let dependency = ReadDependency::Point {
                table_id: TableId::from_ulid(Ulid::from(1_u128)),
                document_id: DocumentId::from_ulid(Ulid::from(2_u128)),
                observed_revision: revision,
                snapshot_sequence: 7,
            };
            let set = [dependency.clone(), dependency]
                .into_iter()
                .collect::<BTreeSet<_>>();
            prop_assert_eq!(set.len(), 1);
        }

        #[test]
        fn scan_limits_outside_v1_are_rejected(
            limit in prop_oneof![Just(0_u32), 1_001_u32..=u32::MAX]
        ) {
            prop_assert!(IndexRange::all().validate(limit).is_err());
        }
    }
}
