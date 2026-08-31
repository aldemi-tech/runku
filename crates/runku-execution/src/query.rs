//! Read-only Query execution, snapshot ownership, dependencies, and telemetry.

use std::{
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
use runku_data::{IndexRange, KeyBound, LogicalStore, ReadSnapshot, StoreError};
use runku_releases::{Capability, FunctionManifest, FunctionType};
use runku_runtime::{
    CancellationToken, DataBoundKind, DataDocument, DataGetRequest, DataIndexEntry, DataKeyBound,
    DataRead, DataReadError, DataScanRequest, FunctionCallError, FunctionCallKind,
    FunctionCallRequest, FunctionInvoke, InvocationRequest, RuntimeError, RuntimeSupervisor,
};
use runku_value::{CanonicalValue, IndexKey};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::nested::{map_runtime_error, prepare_child};

const MAX_DEPENDENCIES: usize = 10_000;
const MAX_SCAN_ROWS: usize = 10_000;

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
                | DataReadError::LimitExceeded,
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
        }
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
        let session = Arc::new(QueryReadSession::new(
            Arc::clone(&self.store),
            request.scope(),
            Arc::clone(&self.telemetry),
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
    use runku_core::{DocumentId, TableId};
    use runku_data::IndexRange;
    use runku_runtime::{DataBoundKind, DataKeyBound};
    use runku_value::{IndexKey, IndexValue};
    use ulid::Ulid;

    use super::{DependencyBound, ReadDependency, convert_bound};

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
