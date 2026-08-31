//! Document Mutation Engine integration over durable `SQLite`.

use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{
    BuildId, DevRevisionId, DocumentId, EnvironmentId, EnvironmentScope, FunctionId, InvocationId,
    OperationId, PinnedCode, ProjectId, ReleaseId, RequestId, ScheduledInvocationId, TableId,
    WorkerId,
};
use runku_data::{
    ClaimedOutboxBatch, ClaimedScheduledInvocation, CommitBatch, CommitResult, DocumentMutation,
    ExpectedRevision, IndexRange, LogicalStore, OutboxConsumerName, OutboxCursor, ReadSnapshot,
    ScheduleCancelResult, ScheduleCompletion, StoreBackend, StoreError, StoreTelemetrySnapshot,
};
use runku_data_postgres::{PostgresStore, PostgresStoreConfig};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::{MutationExecutionError, MutationExecutor};
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_safe_esm_bundle,
};
use runku_runtime::{
    CancellationToken, InvocationRequest, RuntimeError, RuntimeLimits, RuntimeSupervisor,
};
use runku_schema::{FieldPath, IndexDefinition, SchemaCatalog, SchemaError, extract_index_key};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug)]
enum CommitFault {
    ConflictBeforeCommit,
    CommitThenUncertain,
    UpdateObservedBeforeCommit {
        scope: EnvironmentScope,
        table: TableId,
        document: DocumentId,
    },
}

struct FaultStore {
    inner: Arc<SqliteStore>,
    faults: Mutex<VecDeque<CommitFault>>,
    commit_calls: AtomicU64,
}

impl fmt::Debug for FaultStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("FaultStore").finish_non_exhaustive()
    }
}

impl FaultStore {
    fn new(inner: Arc<SqliteStore>, faults: impl IntoIterator<Item = CommitFault>) -> Self {
        Self {
            inner,
            faults: Mutex::new(faults.into_iter().collect()),
            commit_calls: AtomicU64::new(0),
        }
    }

    fn commit_calls(&self) -> u64 {
        self.commit_calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl LogicalStore for FaultStore {
    fn backend(&self) -> StoreBackend {
        self.inner.backend()
    }

    async fn begin_read(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Box<dyn ReadSnapshot>, StoreError> {
        self.inner.begin_read(scope).await
    }

    async fn commit(&self, batch: &CommitBatch) -> Result<CommitResult, StoreError> {
        self.commit_calls.fetch_add(1, Ordering::Relaxed);
        let fault = self
            .faults
            .lock()
            .map_err(|_| StoreError::Internal)?
            .pop_front();
        match fault {
            Some(CommitFault::ConflictBeforeCommit) => Err(StoreError::MutationConflict),
            Some(CommitFault::CommitThenUncertain) => {
                self.inner.commit(batch).await?;
                Err(StoreError::ResultUncertain)
            }
            Some(CommitFault::UpdateObservedBeforeCommit {
                scope,
                table,
                document,
            }) => {
                let mut interference = CommitBatch::new(scope, OperationId::generate());
                interference.push_document(DocumentMutation::Upsert {
                    table_id: table,
                    document_id: document,
                    expected: ExpectedRevision::Exact(1),
                    value: CanonicalValue::String("interference".to_owned()),
                });
                self.inner.commit(&interference).await?;
                self.inner.commit(batch).await
            }
            None => self.inner.commit(batch).await,
        }
    }

    async fn claim_outbox(
        &self,
        scope: EnvironmentScope,
        consumer: &OutboxConsumerName,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<ClaimedOutboxBatch, StoreError> {
        self.inner
            .claim_outbox(scope, consumer, worker_id, now, lease_until, limit)
            .await
    }

    async fn ack_outbox(
        &self,
        scope: EnvironmentScope,
        consumer: &OutboxConsumerName,
        worker_id: WorkerId,
        lease_generation: u64,
        through: OutboxCursor,
    ) -> Result<(), StoreError> {
        self.inner
            .ack_outbox(scope, consumer, worker_id, lease_generation, through)
            .await
    }

    async fn claim_due_scheduled(
        &self,
        scope: EnvironmentScope,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<Vec<ClaimedScheduledInvocation>, StoreError> {
        self.inner
            .claim_due_scheduled(scope, worker_id, now, lease_until, limit)
            .await
    }

    async fn complete_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
        worker_id: WorkerId,
        lease_generation: u64,
        completion: &ScheduleCompletion,
    ) -> Result<(), StoreError> {
        self.inner
            .complete_scheduled(scope, id, worker_id, lease_generation, completion)
            .await
    }

    async fn cancel_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
    ) -> Result<ScheduleCancelResult, StoreError> {
        self.inner.cancel_scheduled(scope, id).await
    }

    async fn health(&self) -> Result<(), StoreError> {
        self.inner.health().await
    }

    fn telemetry(&self) -> StoreTelemetrySnapshot {
        self.inner.telemetry()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_replay_replace_delete_and_rollback_are_atomic() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("mutation.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(2, 16).build()?)?,
        store.clone(),
    );

    let insert_source = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert('{table}', '{document}', value); return 'inserted'; }};"
    );
    let insert_request = mutation_request(
        scope,
        &insert_source,
        CanonicalValue::String("v1".to_owned()),
    )?;
    let operation = OperationId::generate();
    let inserted = executor.execute(insert_request.clone(), operation).await?;
    assert_eq!(
        inserted.value,
        CanonicalValue::String("inserted".to_owned())
    );
    assert!(!inserted.replayed);
    let replayed = executor.execute(insert_request, operation).await?;
    assert!(replayed.replayed);
    assert_eq!(replayed.commit_sequence, inserted.commit_sequence);
    assert_document(store.as_ref(), scope, table, document, Some((1, "v1"))).await?;

    let replace_source = format!(
        "export default async (ctx, value) => {{ const current = await ctx.db.get('{table}', '{document}'); await ctx.db.replace('{table}', '{document}', current.revision, value); return current.value; }};"
    );
    let replacement = executor
        .execute(
            mutation_request(
                scope,
                &replace_source,
                CanonicalValue::String("v2".to_owned()),
            )?,
            OperationId::generate(),
        )
        .await?;
    assert_eq!(replacement.value, CanonicalValue::String("v1".to_owned()));
    assert_document(store.as_ref(), scope, table, document, Some((2, "v2"))).await?;

    let rollback_document = DocumentId::generate();
    let rollback_source = format!(
        "export default async (ctx) => {{ await ctx.db.insert('{table}', '{rollback_document}', 'never'); throw new Error('rollback'); }};"
    );
    assert_eq!(
        executor
            .execute(
                mutation_request(scope, &rollback_source, CanonicalValue::Null)?,
                OperationId::generate(),
            )
            .await,
        Err(MutationExecutionError::Runtime(RuntimeError::JavaScript))
    );
    assert_document(store.as_ref(), scope, table, rollback_document, None).await?;

    let delete_source = format!(
        "export default async (ctx) => {{ const current = await ctx.db.get('{table}', '{document}'); await ctx.db.delete('{table}', '{document}', current.revision); return true; }};"
    );
    executor
        .execute(
            mutation_request(scope, &delete_source, CanonicalValue::Null)?,
            OperationId::generate(),
        )
        .await?;
    assert_document(store.as_ref(), scope, table, document, None).await?;
    let export = store.export_environment(scope).await?;
    assert_eq!(export.outbox.len(), 3);
    assert!(export.outbox.iter().all(|event| matches!(
        &event.payload,
        CanonicalValue::Object(value)
            if value.get("type") == Some(&CanonicalValue::String("document_write_set_v2".to_owned()))
    )));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_write_cannot_be_caught_and_committed() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("duplicate.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let source = format!(
        "export default async (ctx) => {{ await ctx.db.insert('{table}', '{document}', 'one'); try {{ await ctx.db.insert('{table}', '{document}', 'two'); }} catch {{ return 'caught'; }} }};"
    );
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?,
        store.clone(),
    );
    assert_eq!(
        executor
            .execute(
                mutation_request(scope, &source, CanonicalValue::Null)?,
                OperationId::generate(),
            )
            .await,
        Err(MutationExecutionError::Data(
            runku_runtime::DataReadError::InvalidRequest
        ))
    );
    assert_document(store.as_ref(), scope, table, document, None).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_mutation_tree_shares_read_your_writes_one_commit_and_occ_replay()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let inner = Arc::new(
        SqliteStore::open(
            directory.path().join("nested-mutation.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let store = Arc::new(FaultStore::new(
        inner.clone(),
        [CommitFault::ConflictBeforeCommit],
    ));
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let parent_document = DocumentId::generate();
    let child_document = DocumentId::generate();
    let runtime = RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?;
    let executor = MutationExecutor::new(runtime, store.clone());
    let outcome = executor
        .execute(
            nested_mutation_request(scope, table, parent_document, child_document)?,
            OperationId::generate(),
        )
        .await?;
    assert_eq!(
        outcome.value,
        CanonicalValue::Array(vec![
            CanonicalValue::String("child".to_owned()),
            CanonicalValue::String("parent".to_owned()),
            CanonicalValue::Int64(0),
        ])
    );
    assert_eq!(outcome.attempts, 2);
    assert_eq!(store.commit_calls(), 2);
    assert_document(
        inner.as_ref(),
        scope,
        table,
        parent_document,
        Some((1, "parent")),
    )
    .await?;
    assert_document(
        inner.as_ref(),
        scope,
        table,
        child_document,
        Some((1, "child")),
    )
    .await?;
    let export = inner.export_environment(scope).await?;
    assert_eq!(export.documents.len(), 2);
    assert_eq!(export.outbox.len(), 1);
    assert_eq!(executor.telemetry().function_attempts, 2);
    assert_eq!(executor.telemetry().conflicts, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_reruns_function_with_a_fresh_snapshot() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let inner = Arc::new(
        SqliteStore::open(
            directory.path().join("conflict.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let store = Arc::new(FaultStore::new(
        inner.clone(),
        [CommitFault::ConflictBeforeCommit],
    ));
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let source = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert('{table}', '{document}', value); return value; }};"
    );
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?,
        store.clone(),
    );

    let outcome = executor
        .execute(
            mutation_request(
                scope,
                &source,
                CanonicalValue::String("after-conflict".to_owned()),
            )?,
            OperationId::generate(),
        )
        .await?;

    assert_eq!(outcome.attempts, 2);
    assert_eq!(store.commit_calls(), 2);
    assert_document(
        inner.as_ref(),
        scope,
        table,
        document,
        Some((1, "after-conflict")),
    )
    .await?;
    let telemetry = executor.telemetry();
    assert_eq!(telemetry.executions, 1);
    assert_eq!(telemetry.succeeded, 1);
    assert_eq!(telemetry.function_attempts, 2);
    assert_eq!(telemetry.commit_calls, 2);
    assert_eq!(telemetry.conflicts, 1);
    assert_eq!(telemetry.exact_retries, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertain_after_commit_retries_exact_batch_without_rerunning_function()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let inner = Arc::new(
        SqliteStore::open(
            directory.path().join("uncertain.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let store = Arc::new(FaultStore::new(
        inner.clone(),
        [CommitFault::CommitThenUncertain],
    ));
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let source = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert('{table}', '{document}', value); return value; }};"
    );
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?,
        store.clone(),
    );

    let outcome = executor
        .execute(
            mutation_request(
                scope,
                &source,
                CanonicalValue::String("committed-once".to_owned()),
            )?,
            OperationId::generate(),
        )
        .await?;

    assert!(outcome.replayed);
    assert_eq!(outcome.attempts, 1);
    assert_eq!(store.commit_calls(), 2);
    let export = inner.export_environment(scope).await?;
    assert_eq!(export.documents.len(), 1);
    assert_eq!(export.outbox.len(), 1);
    let telemetry = executor.telemetry();
    assert_eq!(telemetry.function_attempts, 1);
    assert_eq!(telemetry.commit_calls, 2);
    assert_eq!(telemetry.exact_retries, 1);
    assert_eq!(telemetry.replays, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_set_change_conflicts_even_when_a_different_document_is_written()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let inner = Arc::new(
        SqliteStore::open(
            directory.path().join("read-set.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let observed = DocumentId::generate();
    let written = DocumentId::generate();
    let mut seed = CommitBatch::new(scope, OperationId::generate());
    seed.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: observed,
        expected: ExpectedRevision::Absent,
        value: CanonicalValue::String("original".to_owned()),
    });
    inner.commit(&seed).await?;
    let store = Arc::new(FaultStore::new(
        inner.clone(),
        [CommitFault::UpdateObservedBeforeCommit {
            scope,
            table,
            document: observed,
        }],
    ));
    let source = format!(
        "export default async (ctx) => {{ const observed = await ctx.db.get('{table}', '{observed}'); await ctx.db.insert('{table}', '{written}', observed.value); return observed.value; }};"
    );
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?,
        store,
    );

    let outcome = executor
        .execute(
            mutation_request(scope, &source, CanonicalValue::Null)?,
            OperationId::generate(),
        )
        .await?;

    assert_eq!(outcome.attempts, 2);
    assert_eq!(
        outcome.value,
        CanonicalValue::String("interference".to_owned())
    );
    assert_document(
        inner.as_ref(),
        scope,
        table,
        observed,
        Some((2, "interference")),
    )
    .await?;
    assert_document(
        inner.as_ref(),
        scope,
        table,
        written,
        Some((1, "interference")),
    )
    .await?;
    assert_eq!(executor.telemetry().conflicts, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_budget_and_scope_isolation_fail_closed() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let inner = Arc::new(
        SqliteStore::open(
            directory.path().join("budget.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let conflict_store = Arc::new(FaultStore::new(
        inner.clone(),
        [
            CommitFault::ConflictBeforeCommit,
            CommitFault::ConflictBeforeCommit,
            CommitFault::ConflictBeforeCommit,
        ],
    ));
    let scope_a = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let source = format!(
        "export default async (ctx) => {{ await ctx.db.insert('{table}', '{document}', 'value'); return true; }};"
    );
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?,
        conflict_store.clone(),
    );
    assert_eq!(
        executor
            .execute(
                mutation_request(scope_a, &source, CanonicalValue::Null)?,
                OperationId::generate(),
            )
            .await,
        Err(MutationExecutionError::Storage(
            StoreError::MutationConflict
        ))
    );
    assert_eq!(conflict_store.commit_calls(), 3);
    assert_document(inner.as_ref(), scope_a, table, document, None).await?;
    assert_eq!(executor.telemetry().function_attempts, 3);

    let direct_executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?,
        inner.clone(),
    );
    let operation = OperationId::generate();
    direct_executor
        .execute(
            mutation_request(scope_a, &source, CanonicalValue::Null)?,
            operation,
        )
        .await?;
    let scope_b = EnvironmentScope::new(scope_a.project_id(), EnvironmentId::generate());
    let cross_scope = direct_executor
        .execute(
            mutation_request(scope_b, &source, CanonicalValue::Null)?,
            operation,
        )
        .await?;
    assert!(!cross_scope.replayed);
    assert_document(inner.as_ref(), scope_b, table, document, Some((1, "value"))).await?;
    assert_document(inner.as_ref(), scope_a, table, document, Some((1, "value"))).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn schema_indexes_follow_insert_replace_and_delete_atomically() -> Result<(), Box<dyn Error>>
{
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("schema-index.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let index = runku_core::IndexId::generate();
    let definition = IndexDefinition::new(
        index,
        table,
        "by_group".to_owned(),
        vec![FieldPath::new(vec!["group".to_owned()])?],
    )?;
    let catalog = Arc::new(SchemaCatalog::new(
        scope.project_id(),
        vec![definition.clone()],
    )?);
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?,
        store.clone(),
    )
    .with_schema_catalog(catalog.clone());
    let source = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert('{table}', '{document}', value); return true; }};"
    );
    let alpha = object_value("alpha");
    executor
        .execute(
            mutation_request_with_index_hash(scope, &source, alpha.clone(), catalog.digest())?,
            OperationId::generate(),
        )
        .await?;
    assert_index(
        store.as_ref(),
        scope,
        index,
        extract_index_key(&definition, &alpha)?.ok_or("alpha sparse")?,
        document,
        1,
    )
    .await?;

    let replace_source = format!(
        "export default async (ctx, value) => {{ await ctx.db.replace('{table}', '{document}', 1n, value); return true; }};"
    );
    let beta = object_value("beta");
    executor
        .execute(
            mutation_request_with_index_hash(
                scope,
                &replace_source,
                beta.clone(),
                catalog.digest(),
            )?,
            OperationId::generate(),
        )
        .await?;
    assert_index(
        store.as_ref(),
        scope,
        index,
        extract_index_key(&definition, &beta)?.ok_or("beta sparse")?,
        document,
        2,
    )
    .await?;

    let same_key_source = format!(
        "export default async (ctx, value) => {{ await ctx.db.replace('{table}', '{document}', 2n, value); return true; }};"
    );
    executor
        .execute(
            mutation_request_with_index_hash(
                scope,
                &same_key_source,
                beta.clone(),
                catalog.digest(),
            )?,
            OperationId::generate(),
        )
        .await?;
    assert_index(
        store.as_ref(),
        scope,
        index,
        extract_index_key(&definition, &beta)?.ok_or("beta sparse")?,
        document,
        3,
    )
    .await?;

    let delete_source = format!(
        "export default async (ctx) => {{ await ctx.db.delete('{table}', '{document}', 3n); return true; }};"
    );
    executor
        .execute(
            mutation_request_with_index_hash(
                scope,
                &delete_source,
                CanonicalValue::Null,
                catalog.digest(),
            )?,
            OperationId::generate(),
        )
        .await?;
    let mut snapshot = store.begin_read(scope).await?;
    assert!(
        snapshot
            .scan_index(index, &IndexRange::all(), 100)
            .await?
            .is_empty()
    );
    snapshot.close().await?;
    let export = store.export_environment(scope).await?;
    assert_eq!(export.outbox.len(), 4);
    assert!(export.outbox.iter().all(|event| matches!(
        &event.payload,
        CanonicalValue::Object(value)
            if value.get("type") == Some(&CanonicalValue::String("document_write_set_v2".to_owned()))
                && matches!(value.get("indexes"), Some(CanonicalValue::Array(indexes)) if !indexes.is_empty())
    )));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_mismatch_and_unsupported_index_value_commit_nothing() -> Result<(), Box<dyn Error>>
{
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("schema-errors.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let catalog = Arc::new(SchemaCatalog::new(
        scope.project_id(),
        vec![IndexDefinition::new(
            runku_core::IndexId::generate(),
            table,
            "by_group".to_owned(),
            vec![FieldPath::new(vec!["group".to_owned()])?],
        )?],
    )?);
    let source = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert('{table}', '{document}', value); return true; }};"
    );
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?,
        store.clone(),
    )
    .with_schema_catalog(catalog.clone());

    assert_eq!(
        executor
            .execute(
                mutation_request_with_index_hash(scope, &source, object_value("value"), [9; 32],)?,
                OperationId::generate(),
            )
            .await,
        Err(MutationExecutionError::Schema(SchemaError::InvalidCatalog))
    );
    let invalid = CanonicalValue::Object(std::collections::BTreeMap::from([(
        "group".to_owned(),
        CanonicalValue::Array(vec![CanonicalValue::String("nested".to_owned())]),
    )]));
    assert_eq!(
        executor
            .execute(
                mutation_request_with_index_hash(scope, &source, invalid, catalog.digest(),)?,
                OperationId::generate(),
            )
            .await,
        Err(MutationExecutionError::Schema(
            SchemaError::UnsupportedValue
        ))
    );
    assert_document(store.as_ref(), scope, table, document, None).await?;
    assert!(store.export_environment(scope).await?.outbox.is_empty());
    assert_eq!(executor.telemetry().data_failures, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_uncertain_commit_replays_documents_indexes_and_outbox_once()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let inner = Arc::new(
        SqliteStore::open(
            directory.path().join("indexed-uncertain.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let store = Arc::new(FaultStore::new(
        inner.clone(),
        [CommitFault::CommitThenUncertain],
    ));
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let index = runku_core::IndexId::generate();
    let definition = IndexDefinition::new(
        index,
        table,
        "by_group".to_owned(),
        vec![FieldPath::new(vec!["group".to_owned()])?],
    )?;
    let catalog = Arc::new(SchemaCatalog::new(
        scope.project_id(),
        vec![definition.clone()],
    )?);
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?,
        store.clone(),
    )
    .with_schema_catalog(catalog.clone());
    let source = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert('{table}', '{document}', value); return true; }};"
    );
    let value = object_value("uncertain");
    let outcome = executor
        .execute(
            mutation_request_with_index_hash(scope, &source, value.clone(), catalog.digest())?,
            OperationId::generate(),
        )
        .await?;
    assert!(outcome.replayed);
    assert_eq!(outcome.attempts, 1);
    assert_eq!(store.commit_calls(), 2);
    assert_index(
        inner.as_ref(),
        scope,
        index,
        extract_index_key(&definition, &value)?.ok_or("sparse")?,
        document,
        1,
    )
    .await?;
    let export = inner.export_environment(scope).await?;
    assert_eq!(export.documents.len(), 1);
    assert_eq!(export.indexes.len(), 1);
    assert_eq!(export.outbox.len(), 1);
    assert_eq!(executor.telemetry().index_mutations, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_document_mutation_conformance() -> Result<(), Box<dyn Error>> {
    let Ok(url) = std::env::var("RUNKU_TEST_POSTGRES_URL") else {
        return Ok(());
    };
    let store = Arc::new(PostgresStore::connect(&url, PostgresStoreConfig::TEST).await?);
    let result = async {
        run_postgres_sequence(store.clone()).await?;
        run_postgres_index_sequence(store.clone()).await
    }
    .await;
    store.close().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_after_and_run_at_commit_atomically_replay_and_pin_immutable_code()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("mutation-scheduling.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?,
        store.clone(),
    );
    let source = r#"
        export default async (ctx, value) => {
          const after = await ctx.scheduler.runAfter(0n, "jobs.send", value, { idempotencyKey: "after" });
          const at = await ctx.scheduler.runAt(1700000000000000n, "jobs.send", null, { idempotencyKey: "at" });
          return [after, at];
        };
    "#;
    let dev_revision = DevRevisionId::generate();
    let request = scheduling_mutation_request(scope, source, FunctionVisibility::Internal)?
        .with_pinned_code(PinnedCode::DevRevision(dev_revision))?;
    let operation = OperationId::generate();
    let first = executor.execute(request.clone(), operation).await?;
    let CanonicalValue::Array(ids) = &first.value else {
        return Err("expected schedule IDs".into());
    };
    let [CanonicalValue::String(after), CanonicalValue::String(at)] = ids.as_slice() else {
        return Err("expected two string schedule IDs".into());
    };
    let after: ScheduledInvocationId = after.parse()?;
    let at: ScheduledInvocationId = at.parse()?;
    let replay = executor.execute(request, operation).await?;
    assert!(replay.replayed);
    assert_eq!(replay.value, first.value);
    assert_eq!(replay.commit_sequence, first.commit_sequence);
    let mut snapshot = store.begin_read(scope).await?;
    let after_record = snapshot
        .get_scheduled(after)
        .await?
        .ok_or("runAfter record missing")?;
    let at_record = snapshot
        .get_scheduled(at)
        .await?
        .ok_or("runAt record missing")?;
    snapshot.close().await?;
    assert_eq!(
        after_record.pinned_code,
        PinnedCode::DevRevision(dev_revision)
    );
    assert_eq!(at_record.pinned_code, PinnedCode::DevRevision(dev_revision));
    assert_eq!(after_record.function.as_str(), "jobs.send");
    assert_eq!(
        after_record.args,
        CanonicalValue::String("payload".to_owned())
    );
    assert_eq!(
        at_record.execute_at,
        TimestampMicros::new(1_700_000_000_000_000)
    );
    assert!(after_record.execute_at > at_record.execute_at);
    assert_eq!(executor.telemetry().schedules_created, 2);

    let abort_source = r#"
        export default async (ctx) => {
          await ctx.scheduler.runAfter(0, "jobs.send", null);
          throw new Error("abort");
        };
    "#;
    let aborted = executor
        .execute(
            scheduling_mutation_request(scope, abort_source, FunctionVisibility::Internal)?,
            OperationId::generate(),
        )
        .await;
    assert!(matches!(
        aborted,
        Err(MutationExecutionError::Runtime(RuntimeError::JavaScript))
    ));

    let denied = executor
        .execute(
            scheduling_mutation_request(scope, source, FunctionVisibility::Public)?,
            OperationId::generate(),
        )
        .await;
    assert_eq!(
        denied,
        Err(MutationExecutionError::Schedule(
            runku_runtime::ScheduleError::InvalidRequest
        ))
    );
    assert_eq!(store.export_environment(scope).await?.schedules.len(), 2);
    Ok(())
}

async fn run_postgres_index_sequence(store: Arc<PostgresStore>) -> Result<(), Box<dyn Error>> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let index = runku_core::IndexId::generate();
    let definition = IndexDefinition::new(
        index,
        table,
        "by_group".to_owned(),
        vec![FieldPath::new(vec!["group".to_owned()])?],
    )?;
    let catalog = Arc::new(SchemaCatalog::new(
        scope.project_id(),
        vec![definition.clone()],
    )?);
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?,
        store.clone(),
    )
    .with_schema_catalog(catalog.clone());
    let source = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert('{table}', '{document}', value); return true; }};"
    );
    let value = object_value("postgres-index");
    executor
        .execute(
            mutation_request_with_index_hash(scope, &source, value.clone(), catalog.digest())?,
            OperationId::generate(),
        )
        .await?;
    assert_index(
        store.as_ref(),
        scope,
        index,
        extract_index_key(&definition, &value)?.ok_or("postgres sparse")?,
        document,
        1,
    )
    .await
}

async fn run_postgres_sequence(store: Arc<PostgresStore>) -> Result<(), Box<dyn Error>> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let executor = MutationExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(2, 8).build()?)?,
        store.clone(),
    );
    let insert_source = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert('{table}', '{document}', value); return value; }};"
    );
    let request = mutation_request(
        scope,
        &insert_source,
        CanonicalValue::String("postgres".to_owned()),
    )?;
    let operation = OperationId::generate();
    let first = executor.execute(request.clone(), operation).await?;
    assert!(!first.replayed);
    assert!(executor.execute(request, operation).await?.replayed);
    assert_document(
        store.as_ref(),
        scope,
        table,
        document,
        Some((1, "postgres")),
    )
    .await?;
    let delete_source = format!(
        "export default async (ctx) => {{ const value = await ctx.db.get('{table}', '{document}'); await ctx.db.delete('{table}', '{document}', value.revision); return value.value; }};"
    );
    let deleted = executor
        .execute(
            mutation_request(scope, &delete_source, CanonicalValue::Null)?,
            OperationId::generate(),
        )
        .await?;
    assert_eq!(deleted.value, CanonicalValue::String("postgres".to_owned()));
    assert_document(store.as_ref(), scope, table, document, None).await?;
    Ok(())
}

async fn assert_document(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    table: TableId,
    document: DocumentId,
    expected: Option<(u64, &str)>,
) -> Result<(), Box<dyn Error>> {
    let mut snapshot = store.begin_read(scope).await?;
    let actual = snapshot.get_document(table, document).await?;
    snapshot.close().await?;
    match (actual, expected) {
        (None, None) => Ok(()),
        (Some(actual), Some((revision, value))) => {
            assert_eq!(actual.revision, revision);
            assert_eq!(actual.value, CanonicalValue::String(value.to_owned()));
            Ok(())
        }
        _ => Err("document state mismatch".into()),
    }
}

fn mutation_request(
    scope: EnvironmentScope,
    source: &str,
    arguments: CanonicalValue,
) -> Result<InvocationRequest, Box<dyn Error>> {
    mutation_request_with_index_hash(scope, source, arguments, [3; 32])
}

fn mutation_request_with_index_hash(
    scope: EnvironmentScope,
    source: &str,
    arguments: CanonicalValue,
    index_contract_hash: [u8; 32],
) -> Result<InvocationRequest, Box<dyn Error>> {
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let release_id = ReleaseId::generate();
    let function_id = FunctionId::generate();
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes(index_contract_hash),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "tests.mutation".parse()?,
            function_type: FunctionType::Mutation,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
            result_contract_hash: Sha256Digest::from_bytes([5; 32]),
            capabilities: vec![Capability::DbRead, Capability::DbWrite],
        }],
        cron_definitions: Vec::new(),
    };
    Ok(InvocationRequest::new(
        scope,
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        function_id,
        Arc::new(manifest),
        artifact,
        arguments,
        Duration::from_secs(2),
        CancellationToken::new(),
    )?)
}

#[allow(clippy::too_many_lines)]
fn nested_mutation_request(
    scope: EnvironmentScope,
    table: TableId,
    parent_document: DocumentId,
    child_document: DocumentId,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let child_source = format!(
        r#"
        export default async (ctx) => {{
          await ctx.db.insert("{table}", "{child_document}", "child");
          const projected = await ctx.db.get("{table}", "{child_document}");
          return projected.value;
        }};
        "#
    );
    let observe_source =
        format!("export default async (ctx) => ctx.db.get(\"{table}\", \"{parent_document}\");\n");
    let parent_source = format!(
        r#"
        export default async (ctx) => {{
          await ctx.db.insert("{table}", "{parent_document}", "parent");
          const child = await ctx.runMutation("tests.child", null);
          const observed = await ctx.runQuery("tests.observe", null);
          return [child, observed.value, observed.commitSequence];
        }};
        "#
    );
    let bundle = SafeEsmBundleV1::from_sources([
        child_source.as_str(),
        observe_source.as_str(),
        parent_source.as_str(),
    ])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let release_id = ReleaseId::generate();
    let child_id = FunctionId::generate();
    let observe_id = FunctionId::generate();
    let parent_id = FunctionId::generate();
    let function = |id,
                    name: &str,
                    source: &str,
                    function_type,
                    visibility,
                    capabilities|
     -> Result<_, Box<dyn Error>> {
        Ok(FunctionManifest {
            id,
            name: name.parse()?,
            function_type,
            visibility,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
            result_contract_hash: Sha256Digest::from_bytes([5; 32]),
            capabilities,
        })
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![
            function(
                child_id,
                "tests.child",
                &child_source,
                FunctionType::Mutation,
                FunctionVisibility::Internal,
                vec![Capability::DbRead, Capability::DbWrite],
            )?,
            function(
                observe_id,
                "tests.observe",
                &observe_source,
                FunctionType::Query,
                FunctionVisibility::Internal,
                vec![Capability::DbRead],
            )?,
            function(
                parent_id,
                "tests.parent",
                &parent_source,
                FunctionType::Mutation,
                FunctionVisibility::Public,
                vec![
                    Capability::DbRead,
                    Capability::DbWrite,
                    Capability::FunctionQuery,
                    Capability::FunctionMutation,
                ],
            )?,
        ],
        cron_definitions: Vec::new(),
    };
    Ok(InvocationRequest::new(
        scope,
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        parent_id,
        Arc::new(manifest),
        artifact,
        CanonicalValue::Null,
        Duration::from_secs(2),
        CancellationToken::new(),
    )?)
}

fn scheduling_mutation_request(
    scope: EnvironmentScope,
    source: &str,
    target_visibility: FunctionVisibility,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let release_id = ReleaseId::generate();
    let mutation_id = FunctionId::generate();
    let implementation_hash = Sha256Digest::of(source.as_bytes());
    let function = |id, name, function_type, visibility, capabilities| FunctionManifest {
        id,
        name,
        function_type,
        visibility,
        auth_policy: AuthPolicy::None,
        runtime_class: RuntimeClass::SafeV8,
        implementation_hash,
        arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
        result_contract_hash: Sha256Digest::from_bytes([5; 32]),
        capabilities,
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![
            function(
                FunctionId::generate(),
                "jobs.send".parse()?,
                FunctionType::Action,
                target_visibility,
                Vec::new(),
            ),
            function(
                mutation_id,
                "tests.mutation".parse()?,
                FunctionType::Mutation,
                FunctionVisibility::Public,
                vec![
                    Capability::DbRead,
                    Capability::DbWrite,
                    Capability::SchedulerCreate,
                ],
            ),
        ],
        cron_definitions: Vec::new(),
    };
    Ok(InvocationRequest::new(
        scope,
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        mutation_id,
        Arc::new(manifest),
        artifact,
        CanonicalValue::String("payload".to_owned()),
        Duration::from_secs(2),
        CancellationToken::new(),
    )?)
}

fn object_value(group: &str) -> CanonicalValue {
    CanonicalValue::Object(std::collections::BTreeMap::from([(
        "group".to_owned(),
        CanonicalValue::String(group.to_owned()),
    )]))
}

async fn assert_index(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    index: runku_core::IndexId,
    expected_key: runku_value::IndexKey,
    document: DocumentId,
    revision: u64,
) -> Result<(), Box<dyn Error>> {
    let mut snapshot = store.begin_read(scope).await?;
    let entries = snapshot.scan_index(index, &IndexRange::all(), 100).await?;
    snapshot.close().await?;
    if entries.len() != 1 {
        return Err("expected exactly one active index entry".into());
    }
    assert_eq!(entries[0].key, expected_key);
    assert_eq!(entries[0].document_id, document);
    assert_eq!(entries[0].document_revision, revision);
    Ok(())
}
