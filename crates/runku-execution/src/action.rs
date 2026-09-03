//! Action execution with immediate durable schedule creation.

use std::{
    collections::BTreeSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_core::{EnvironmentScope, FunctionName, OperationId, ScheduledInvocationId};
use runku_data::{CommitBatch, LogicalStore, PinnedCode, ScheduledInvocationInsert, StoreError};
use runku_releases::{Capability, FunctionType, FunctionVisibility, RuntimeClass};
use runku_runtime::{
    CancellationToken, FileStorage, FunctionCallError, FunctionCallKind, FunctionCallRequest,
    FunctionInvoke, HttpsEgress, InvocationRequest, RuntimeError, RuntimeSupervisor,
    ScheduleCreate, ScheduleError, ScheduleRequest,
};
use runku_value::{CanonicalValue, TimestampMicros, encode_stored_value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    MutationExecutionError, MutationExecutor, QueryExecutor,
    mutation::schedule_time,
    nested::{map_runtime_error, prepare_child},
    query::ExecutionError,
};

/// Out-of-process Action execution boundary used by the runtime-class dispatcher.
#[async_trait]
pub trait NodeActionExecutor: fmt::Debug + Send + Sync {
    /// Executes one already-authorized Full Node invocation with all Platform Ops attached.
    async fn execute_node(
        &self,
        request: InvocationRequest,
    ) -> Result<CanonicalValue, RuntimeError>;
}

const MAX_SCHEDULES: u64 = 100;
const MAX_COMMIT_ATTEMPTS: u8 = 3;

/// Successful Action result after every awaited Platform Op completed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActionOutcome {
    /// Canonical handler result.
    pub value: CanonicalValue,
    /// Durable schedules newly created by this invocation.
    pub schedules_created: u64,
}

/// Stable Action coordinator failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ActionExecutionError {
    /// Safe Runtime rejected or failed the Action.
    #[error("action runtime failed")]
    Runtime(RuntimeError),
    /// Coordinator setup or durable scheduling failed before runtime admission.
    #[error("action scheduling failed")]
    Schedule(ScheduleError),
}

impl ActionExecutionError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Runtime(error) => error.code(),
            Self::Schedule(error) => error.code(),
        }
    }

    /// Whether retrying unchanged may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        match self {
            Self::Runtime(error) => error.retryable(),
            Self::Schedule(
                ScheduleError::Storage
                | ScheduleError::Unavailable
                | ScheduleError::Timeout
                | ScheduleError::ResultUncertain,
            ) => true,
            Self::Schedule(
                ScheduleError::InvalidRequest
                | ScheduleError::LimitExceeded
                | ScheduleError::Cancelled,
            ) => false,
        }
    }
}

/// Bounded Action coordinator counters without request-controlled labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ActionTelemetrySnapshot {
    /// Action executions attempted.
    pub executions: u64,
    /// Successful handler results.
    pub succeeded: u64,
    /// Runtime failures.
    pub runtime_failures: u64,
    /// Newly committed schedules.
    pub schedules_created: u64,
    /// Idempotent schedule replays.
    pub schedule_replays: u64,
    /// Schedule persistence failures.
    pub schedule_failures: u64,
}

#[derive(Debug, Default)]
struct ActionTelemetry {
    executions: AtomicU64,
    succeeded: AtomicU64,
    runtime_failures: AtomicU64,
    schedules_created: AtomicU64,
    schedule_replays: AtomicU64,
    schedule_failures: AtomicU64,
}

/// Product Base coordinator for Actions and their independent durable scheduling Ops.
#[derive(Clone)]
pub struct ActionExecutor {
    runtime: RuntimeSupervisor,
    store: Arc<dyn LogicalStore>,
    telemetry: Arc<ActionTelemetry>,
    query: Option<QueryExecutor>,
    mutation: Option<MutationExecutor>,
    https: Option<Arc<dyn HttpsEgress>>,
    file_storage: Option<Arc<dyn FileStorage>>,
    node: Option<Arc<dyn NodeActionExecutor>>,
}

impl fmt::Debug for ActionExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActionExecutor")
            .field("backend", &self.store.backend())
            .finish_non_exhaustive()
    }
}

impl ActionExecutor {
    /// Composes the Safe Runtime with the logical durable store.
    #[must_use]
    pub fn new(runtime: RuntimeSupervisor, store: Arc<dyn LogicalStore>) -> Self {
        Self {
            runtime,
            store,
            telemetry: Arc::new(ActionTelemetry::default()),
            query: None,
            mutation: None,
            https: None,
            file_storage: None,
            node: None,
        }
    }

    /// Attaches the exact Query and Mutation coordinators used for independent nested calls.
    #[must_use]
    pub fn with_nested_executors(
        mut self,
        query: QueryExecutor,
        mutation: MutationExecutor,
    ) -> Self {
        self.query = Some(query);
        self.mutation = Some(mutation);
        self
    }

    /// Attaches the Product Base HTTPS broker available to nested Actions declaring the
    /// `network:https` capability.
    #[must_use]
    pub fn with_https_egress(mut self, https: Arc<dyn HttpsEgress>) -> Self {
        self.https = Some(https);
        self
    }

    /// Attaches the application file broker used by root and nested Actions.
    #[must_use]
    pub fn with_file_storage(mut self, storage: Arc<dyn FileStorage>) -> Self {
        self.file_storage = Some(storage);
        self
    }

    /// Attaches the Full Node executor used for root and nested Action routing.
    #[must_use]
    pub fn with_node_runtime(mut self, node: Arc<dyn NodeActionExecutor>) -> Self {
        self.node = Some(node);
        self
    }

    /// Executes one pre-authorized Action; HTTPS may already be attached to the request.
    ///
    /// # Errors
    ///
    /// Returns stable runtime or scheduling setup failures.
    pub async fn execute(
        &self,
        request: InvocationRequest,
    ) -> Result<ActionOutcome, ActionExecutionError> {
        self.execute_with_deadline(request, None).await
    }

    async fn execute_nested(
        &self,
        request: InvocationRequest,
        deadline: Instant,
    ) -> Result<ActionOutcome, ActionExecutionError> {
        self.execute_with_deadline(request, Some(deadline)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_with_deadline(
        &self,
        mut request: InvocationRequest,
        inherited_deadline: Option<Instant>,
    ) -> Result<ActionOutcome, ActionExecutionError> {
        self.telemetry.executions.fetch_add(1, Ordering::Relaxed);
        let selected = request
            .manifest()
            .functions
            .iter()
            .find(|function| function.id == request.function_id())
            .cloned()
            .ok_or(ActionExecutionError::Runtime(
                RuntimeError::InvalidInvocation,
            ))?;
        if selected.function_type != FunctionType::Action {
            return Err(ActionExecutionError::Runtime(
                RuntimeError::InvalidInvocation,
            ));
        }
        if selected.capabilities.contains(&Capability::NetworkHttps)
            && request.https_egress().is_none()
        {
            let https = self
                .https
                .as_ref()
                .ok_or(ActionExecutionError::Runtime(RuntimeError::Unavailable))?;
            request = request
                .with_https(Arc::clone(https))
                .map_err(ActionExecutionError::Runtime)?;
        }
        if selected
            .capabilities
            .iter()
            .any(|capability| matches!(capability, Capability::FileRead | Capability::FileWrite))
            && request.file_storage().is_none()
        {
            let storage = self
                .file_storage
                .as_ref()
                .ok_or(ActionExecutionError::Runtime(RuntimeError::Unavailable))?;
            request = request
                .with_file_storage(Arc::clone(storage))
                .map_err(ActionExecutionError::Runtime)?;
        }
        let scheduling = selected.capabilities.contains(&Capability::SchedulerCreate);
        let nested = selected.capabilities.iter().any(|capability| {
            matches!(
                capability,
                Capability::FunctionQuery
                    | Capability::FunctionMutation
                    | Capability::FunctionAction
            )
        });
        let base = now_micros().map_err(ActionExecutionError::Schedule)?;
        let allowed = request
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
            .collect();
        let broker = Arc::new(ActionScheduleBroker {
            store: Arc::clone(&self.store),
            scope: request.scope(),
            pinned_code: request.pinned_code(),
            base,
            allowed,
            created: AtomicU64::new(0),
            calls: AtomicU64::new(0),
            telemetry: Arc::clone(&self.telemetry),
            serial: Mutex::new(()),
        });
        let request = if scheduling {
            request
                .with_scheduler(broker.clone())
                .map_err(ActionExecutionError::Runtime)?
        } else {
            request
        };
        let request = if nested {
            let broker = Arc::new(ActionFunctionBroker {
                executor: self.clone(),
                root: request.clone(),
                calls: AtomicU64::new(0),
            });
            request
                .with_functions(broker)
                .map_err(ActionExecutionError::Runtime)?
        } else {
            request
        };
        let runtime_result = self
            .invoke_runtime(request, selected.runtime_class, inherited_deadline)
            .await;
        match runtime_result {
            Ok(value) => {
                self.telemetry.succeeded.fetch_add(1, Ordering::Relaxed);
                Ok(ActionOutcome {
                    value,
                    schedules_created: broker.created.load(Ordering::Relaxed),
                })
            }
            Err(error) => {
                self.telemetry
                    .runtime_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err(ActionExecutionError::Runtime(error))
            }
        }
    }

    async fn invoke_runtime(
        &self,
        request: InvocationRequest,
        runtime_class: RuntimeClass,
        inherited_deadline: Option<Instant>,
    ) -> Result<CanonicalValue, RuntimeError> {
        match runtime_class {
            RuntimeClass::SafeV8 => match inherited_deadline {
                Some(deadline) => self.runtime.invoke_nested_until(request, deadline).await,
                None => self.runtime.invoke(request).await,
            },
            RuntimeClass::FullNode => match &self.node {
                Some(node) => node.execute_node(request).await,
                None => Err(RuntimeError::UnsupportedRuntime),
            },
        }
    }

    /// Returns bounded aggregate telemetry.
    #[must_use]
    pub fn telemetry(&self) -> ActionTelemetrySnapshot {
        ActionTelemetrySnapshot {
            executions: self.telemetry.executions.load(Ordering::Relaxed),
            succeeded: self.telemetry.succeeded.load(Ordering::Relaxed),
            runtime_failures: self.telemetry.runtime_failures.load(Ordering::Relaxed),
            schedules_created: self.telemetry.schedules_created.load(Ordering::Relaxed),
            schedule_replays: self.telemetry.schedule_replays.load(Ordering::Relaxed),
            schedule_failures: self.telemetry.schedule_failures.load(Ordering::Relaxed),
        }
    }
}

struct ActionFunctionBroker {
    executor: ActionExecutor,
    root: InvocationRequest,
    calls: AtomicU64,
}

impl fmt::Debug for ActionFunctionBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActionFunctionBroker")
            .field("scope", &self.root.scope())
            .field("depth", &self.root.nested_depth())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl FunctionInvoke for ActionFunctionBroker {
    async fn invoke(
        &self,
        call: FunctionCallRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CanonicalValue, FunctionCallError> {
        if cancellation.is_cancelled() {
            return Err(FunctionCallError::Cancelled);
        }
        let (mut child, selected) = prepare_child(&self.root, call.clone(), deadline)?;
        if selected.capabilities.contains(&Capability::NetworkHttps) {
            let https = self
                .executor
                .https
                .clone()
                .or_else(|| self.root.https_egress())
                .ok_or(FunctionCallError::Unavailable)?;
            child = child.with_https(https).map_err(map_runtime_error)?;
        }
        match call.kind {
            FunctionCallKind::Query => self
                .executor
                .query
                .as_ref()
                .ok_or(FunctionCallError::Unavailable)?
                .execute_nested(child, deadline)
                .await
                .map(|outcome| outcome.value)
                .map_err(map_query_error),
            FunctionCallKind::Mutation => {
                let ordinal = self.calls.fetch_add(1, Ordering::AcqRel);
                let operation = nested_operation_id(self.root.invocation_id(), ordinal);
                self.executor
                    .mutation
                    .as_ref()
                    .ok_or(FunctionCallError::Unavailable)?
                    .execute_nested(child, operation, deadline)
                    .await
                    .map(|outcome| outcome.value)
                    .map_err(map_mutation_error)
            }
            FunctionCallKind::Action => self
                .executor
                .execute_nested(child, deadline)
                .await
                .map(|outcome| outcome.value)
                .map_err(map_action_error),
        }
    }
}

fn nested_operation_id(invocation: runku_core::InvocationId, ordinal: u64) -> OperationId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_ACTION_NESTED_MUTATION_OPERATION_V1");
    digest.update(invocation.to_string().as_bytes());
    digest.update(ordinal.to_be_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    OperationId::from_ulid(ulid::Ulid::from(u128::from_be_bytes(bytes)))
}

fn map_query_error(error: ExecutionError) -> FunctionCallError {
    match error {
        ExecutionError::Runtime(error) => map_runtime_error(error),
        ExecutionError::Data(runku_runtime::DataReadError::Cancelled) => {
            FunctionCallError::Cancelled
        }
        ExecutionError::Data(runku_runtime::DataReadError::Timeout) => FunctionCallError::Timeout,
        ExecutionError::Storage(_) | ExecutionError::Data(_) => FunctionCallError::Execution,
    }
}

fn map_mutation_error(error: MutationExecutionError) -> FunctionCallError {
    match error {
        MutationExecutionError::Runtime(error) => map_runtime_error(error),
        MutationExecutionError::Data(runku_runtime::DataReadError::Cancelled)
        | MutationExecutionError::Schedule(ScheduleError::Cancelled) => {
            FunctionCallError::Cancelled
        }
        MutationExecutionError::Data(runku_runtime::DataReadError::Timeout)
        | MutationExecutionError::Schedule(ScheduleError::Timeout) => FunctionCallError::Timeout,
        MutationExecutionError::Storage(_)
        | MutationExecutionError::Data(_)
        | MutationExecutionError::Schema(_)
        | MutationExecutionError::Schedule(_) => FunctionCallError::Execution,
    }
}

fn map_action_error(error: ActionExecutionError) -> FunctionCallError {
    match error {
        ActionExecutionError::Runtime(error) => map_runtime_error(error),
        ActionExecutionError::Schedule(ScheduleError::Cancelled) => FunctionCallError::Cancelled,
        ActionExecutionError::Schedule(ScheduleError::Timeout) => FunctionCallError::Timeout,
        ActionExecutionError::Schedule(_) => FunctionCallError::Execution,
    }
}

struct ActionScheduleBroker {
    store: Arc<dyn LogicalStore>,
    scope: EnvironmentScope,
    pinned_code: PinnedCode,
    base: TimestampMicros,
    allowed: BTreeSet<FunctionName>,
    created: AtomicU64,
    calls: AtomicU64,
    telemetry: Arc<ActionTelemetry>,
    serial: Mutex<()>,
}

impl fmt::Debug for ActionScheduleBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActionScheduleBroker")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ScheduleCreate for ActionScheduleBroker {
    async fn create(
        &self,
        request: ScheduleRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ScheduledInvocationId, ScheduleError> {
        if cancellation.is_cancelled() {
            return Err(ScheduleError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ScheduleError::Timeout);
        }
        if !self.allowed.contains(&request.function)
            || encode_stored_value(&request.arguments).is_err()
            || request
                .idempotency_key
                .as_ref()
                .is_some_and(|key| key.is_empty() || key.len() > 128)
        {
            return Err(ScheduleError::InvalidRequest);
        }
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        if call >= MAX_SCHEDULES {
            return Err(ScheduleError::LimitExceeded);
        }
        let execute_at = schedule_time(self.base, request.time)?;
        let _serial = self.serial.lock().await;
        let (operation, id) = identities(self.scope, request.idempotency_key.as_deref());
        let schedule = ScheduledInvocationInsert {
            id,
            pinned_code: self.pinned_code,
            function: request.function,
            args: request.arguments,
            execute_at,
            idempotency_key: request.idempotency_key,
        };
        let persist = persist_schedule(
            self.store.as_ref(),
            self.scope,
            operation,
            &schedule,
            deadline,
            cancellation,
        )
        .await;
        match persist {
            Ok(replayed) => {
                if replayed {
                    self.telemetry
                        .schedule_replays
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.created.fetch_add(1, Ordering::Relaxed);
                    self.telemetry
                        .schedules_created
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(id)
            }
            Err(error) => {
                self.telemetry
                    .schedule_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }
}

async fn persist_schedule(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    operation: OperationId,
    schedule: &ScheduledInvocationInsert,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<bool, ScheduleError> {
    let mut batch = CommitBatch::new(scope, operation);
    batch.push_schedule(schedule.clone());
    batch.validate().map_err(map_store)?;
    for attempt in 0..MAX_COMMIT_ATTEMPTS {
        let commit = store.commit(&batch);
        let result = tokio::select! {
            () = cancellation.cancelled() => return Err(ScheduleError::Cancelled),
            result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), commit) => {
                result.map_err(|_| ScheduleError::ResultUncertain)?
            }
        };
        match result {
            Ok(result) => return Ok(result.replayed),
            Err(
                StoreError::OperationIdReused
                | StoreError::MutationConflict
                | StoreError::ResultUncertain,
            ) => {
                if let Some(existing) = read_schedule(store, scope, schedule.id).await? {
                    return if same_schedule(&existing, schedule) {
                        Ok(true)
                    } else {
                        Err(ScheduleError::InvalidRequest)
                    };
                }
            }
            Err(error) if error.retryable() && attempt + 1 < MAX_COMMIT_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(5_u64 << attempt)).await;
            }
            Err(error) => return Err(map_store(error)),
        }
    }
    Err(ScheduleError::Unavailable)
}

async fn read_schedule(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    id: ScheduledInvocationId,
) -> Result<Option<runku_data::ScheduledInvocationRecord>, ScheduleError> {
    let mut snapshot = store.begin_read(scope).await.map_err(map_store)?;
    let result = snapshot.get_scheduled(id).await.map_err(map_store);
    let close = snapshot.close().await.map_err(map_store);
    match (result, close) {
        (Ok(record), Ok(())) => Ok(record),
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}

fn same_schedule(
    existing: &runku_data::ScheduledInvocationRecord,
    requested: &ScheduledInvocationInsert,
) -> bool {
    existing.id == requested.id
        && existing.pinned_code == requested.pinned_code
        && existing.function == requested.function
        && existing.args == requested.args
        && existing.idempotency_key == requested.idempotency_key
}

fn identities(
    scope: EnvironmentScope,
    idempotency_key: Option<&str>,
) -> (OperationId, ScheduledInvocationId) {
    let Some(key) = idempotency_key else {
        let operation = OperationId::generate();
        return (
            operation,
            ScheduledInvocationId::from_ulid(operation.as_ulid()),
        );
    };
    let operation = OperationId::from_ulid(derived_ulid(
        b"RUNKU_ACTION_SCHEDULE_OPERATION_V1",
        scope,
        key,
    ));
    let schedule = ScheduledInvocationId::from_ulid(derived_ulid(
        b"RUNKU_ACTION_SCHEDULE_RECORD_V1",
        scope,
        key,
    ));
    (operation, schedule)
}

fn derived_ulid(domain: &[u8], scope: EnvironmentScope, key: &str) -> ulid::Ulid {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(scope.project_id().to_string().as_bytes());
    digest.update(scope.environment_id().to_string().as_bytes());
    digest.update(key.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    ulid::Ulid::from(u128::from_be_bytes(bytes))
}

fn now_micros() -> Result<TimestampMicros, ScheduleError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ScheduleError::Unavailable)?;
    i64::try_from(elapsed.as_micros())
        .map(TimestampMicros::new)
        .map_err(|_| ScheduleError::Unavailable)
}

const fn map_store(error: StoreError) -> ScheduleError {
    match error {
        StoreError::LimitExceeded => ScheduleError::LimitExceeded,
        StoreError::Unavailable
        | StoreError::Busy
        | StoreError::SerializationFailure
        | StoreError::ResultUncertain
        | StoreError::OutboxLeaseLost => ScheduleError::Unavailable,
        StoreError::EmptyBatch
        | StoreError::DuplicateMutation
        | StoreError::InvalidRange
        | StoreError::OperationIdReused
        | StoreError::MutationConflict
        | StoreError::NotFound
        | StoreError::LeaseLost
        | StoreError::ProductionBackendUnsupported
        | StoreError::Corruption
        | StoreError::MigrationFailed
        | StoreError::Internal => ScheduleError::Storage,
    }
}
