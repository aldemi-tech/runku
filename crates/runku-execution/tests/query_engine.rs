//! Query Engine behavior and real-adapter conformance.

use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{
    BuildId, DocumentId, EnvironmentId, EnvironmentScope, FunctionId, IndexId, InvocationId,
    OperationId, OutboxEventId, ProjectId, ReleaseId, RequestId, ScheduledInvocationId, TableId,
    WorkerId,
};
use runku_data::{
    ClaimedOutboxBatch, ClaimedScheduledInvocation, CommitBatch, CommitResult, DocumentMutation,
    DocumentRecord, ExpectedRevision, IndexEntry, IndexMutation, IndexRange, LogicalStore,
    OutboxConsumerName, OutboxCursor, ReadSnapshot, ScheduleCancelResult, ScheduleCompletion,
    ScheduledInvocationRecord, StoreBackend, StoreError, StoreTelemetrySnapshot,
};
use runku_data_postgres::{PostgresStore, PostgresStoreConfig};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::{ExecutionError, QueryExecutor, ReadDependency};
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_safe_esm_bundle,
};
use runku_runtime::{CancellationToken, InvocationRequest, RuntimeLimits, RuntimeSupervisor};
use runku_value::{CanonicalValue, IndexKey, IndexValue, TimestampMicros};
use tempfile::TempDir;
use ulid::Ulid;

#[derive(Debug, Default)]
struct FakeCounts {
    began: u64,
    closed: u64,
}

#[derive(Clone, Debug)]
struct FakeStore {
    counts: Arc<Mutex<FakeCounts>>,
    point_error: Option<StoreError>,
    close_error: Option<StoreError>,
    read_delay: Duration,
}

#[async_trait]
impl LogicalStore for FakeStore {
    fn backend(&self) -> StoreBackend {
        StoreBackend::SQLite
    }

    async fn begin_read(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Box<dyn ReadSnapshot>, StoreError> {
        self.counts.lock().map_err(|_| StoreError::Internal)?.began += 1;
        Ok(Box::new(FakeSnapshot {
            counts: Arc::clone(&self.counts),
            scope,
            point_error: self.point_error,
            close_error: self.close_error,
            read_delay: self.read_delay,
        }))
    }

    async fn commit(&self, _batch: &CommitBatch) -> Result<CommitResult, StoreError> {
        Err(StoreError::Internal)
    }

    async fn claim_outbox(
        &self,
        _scope: EnvironmentScope,
        _consumer: &OutboxConsumerName,
        _worker_id: WorkerId,
        _now: TimestampMicros,
        _lease_until: TimestampMicros,
        _limit: u32,
    ) -> Result<ClaimedOutboxBatch, StoreError> {
        Err(StoreError::Internal)
    }

    async fn ack_outbox(
        &self,
        _scope: EnvironmentScope,
        _consumer: &OutboxConsumerName,
        _worker_id: WorkerId,
        _lease_generation: u64,
        _through: OutboxCursor,
    ) -> Result<(), StoreError> {
        Err(StoreError::Internal)
    }

    async fn claim_due_scheduled(
        &self,
        _scope: EnvironmentScope,
        _worker_id: WorkerId,
        _now: TimestampMicros,
        _lease_until: TimestampMicros,
        _limit: u32,
    ) -> Result<Vec<ClaimedScheduledInvocation>, StoreError> {
        Err(StoreError::Internal)
    }

    async fn complete_scheduled(
        &self,
        _scope: EnvironmentScope,
        _id: ScheduledInvocationId,
        _worker_id: WorkerId,
        _lease_generation: u64,
        _completion: &ScheduleCompletion,
    ) -> Result<(), StoreError> {
        Err(StoreError::Internal)
    }

    async fn cancel_scheduled(
        &self,
        _scope: EnvironmentScope,
        _id: ScheduledInvocationId,
    ) -> Result<ScheduleCancelResult, StoreError> {
        Err(StoreError::Internal)
    }

    async fn health(&self) -> Result<(), StoreError> {
        Ok(())
    }

    fn telemetry(&self) -> StoreTelemetrySnapshot {
        StoreTelemetrySnapshot::default()
    }
}

struct FakeSnapshot {
    counts: Arc<Mutex<FakeCounts>>,
    scope: EnvironmentScope,
    point_error: Option<StoreError>,
    close_error: Option<StoreError>,
    read_delay: Duration,
}

#[async_trait]
impl ReadSnapshot for FakeSnapshot {
    fn commit_sequence(&self) -> u64 {
        7
    }

    async fn get_document(
        &mut self,
        table_id: TableId,
        document_id: DocumentId,
    ) -> Result<Option<DocumentRecord>, StoreError> {
        if !self.read_delay.is_zero() {
            tokio::time::sleep(self.read_delay).await;
        }
        if let Some(error) = self.point_error {
            return Err(error);
        }
        if document_id == document_id_for(12) {
            return Ok(Some(DocumentRecord {
                table_id,
                document_id,
                revision: 3,
                commit_sequence: 7,
                created_at: TimestampMicros::new(10),
                updated_at: TimestampMicros::new(20),
                value: CanonicalValue::String("Ada".to_owned()),
            }));
        }
        Ok(None)
    }

    async fn scan_index(
        &mut self,
        index_id: IndexId,
        range: &IndexRange,
        limit: u32,
    ) -> Result<Vec<IndexEntry>, StoreError> {
        if !self.read_delay.is_zero() {
            tokio::time::sleep(self.read_delay).await;
        }
        range.validate(limit)?;
        let key = IndexKey::encode(&[IndexValue::String("ada".to_owned())])
            .map_err(|_| StoreError::Corruption)?;
        Ok(vec![IndexEntry {
            index_id,
            key,
            table_id: table_id(),
            document_id: document_id_for(12),
            document_revision: 3,
            commit_sequence: 7,
        }])
    }

    async fn get_outbox(
        &mut self,
        _event_id: OutboxEventId,
    ) -> Result<Option<CanonicalValue>, StoreError> {
        Err(StoreError::Internal)
    }

    async fn get_scheduled(
        &mut self,
        _id: ScheduledInvocationId,
    ) -> Result<Option<ScheduledInvocationRecord>, StoreError> {
        Err(StoreError::Internal)
    }

    async fn close(self: Box<Self>) -> Result<(), StoreError> {
        let _scope = self.scope;
        self.counts.lock().map_err(|_| StoreError::Internal)?.closed += 1;
        self.close_error.map_or(Ok(()), Err)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_snapshot_dependencies_concurrency_and_close_are_complete()
-> Result<(), Box<dyn Error>> {
    let counts = Arc::new(Mutex::new(FakeCounts::default()));
    let executor = executor(Arc::new(FakeStore {
        counts: Arc::clone(&counts),
        point_error: None,
        close_error: None,
        read_delay: Duration::ZERO,
    }))?;
    let no_reads = executor
        .execute(query_request(
            "export default (ctx) => typeof ctx.db.get === 'function';",
            CanonicalValue::Null,
        )?)
        .await?;
    assert_eq!(no_reads.value, CanonicalValue::Boolean(true));
    assert_eq!(no_reads.snapshot_sequence, None);
    assert!(no_reads.dependencies.is_empty());
    assert_counts(&counts, 0, 0)?;

    let table = table_id();
    let hit = document_id_for(12);
    let miss = document_id_for(13);
    let index = index_id();
    let source = format!(
        r#"
        export default async (ctx, key) => {{
          const [hit, miss, rows] = await Promise.all([
            ctx.db.get("{table}", "{hit}"),
            ctx.db.get("{table}", "{miss}"),
            ctx.db.scan("{index}", {{
              lower: {{ kind: "inclusive", key }},
              upper: {{ kind: "inclusive", key }},
              limit: 10
            }})
          ]);
          return {{
            hit: hit.value,
            miss: miss === null,
            revision: hit.revision,
            rowKey: rows[0].key,
            frozen: Object.isFrozen(hit) && Object.isFrozen(rows) && Object.isFrozen(rows[0])
          }};
        }};
        "#
    );
    let outcome = executor
        .execute(query_request(
            &source,
            CanonicalValue::Bytes(index_key()?.as_bytes().to_vec()),
        )?)
        .await?;
    assert_eq!(outcome.snapshot_sequence, Some(7));
    assert_eq!(outcome.dependencies.len(), 3);
    assert!(outcome.dependencies.iter().any(|dependency| matches!(
        dependency,
        ReadDependency::Point {
            observed_revision: None,
            ..
        }
    )));
    assert_counts(&counts, 1, 1)?;
    let telemetry = executor.telemetry();
    assert_eq!(telemetry.executions, 2);
    assert_eq!(telemetry.succeeded, 2);
    assert_eq!(telemetry.point_reads, 2);
    assert_eq!(telemetry.range_reads, 1);
    assert_eq!(telemetry.rows, 1);
    assert_eq!(telemetry.dependencies, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_queries_share_one_snapshot_and_one_reactive_dependency_set()
-> Result<(), Box<dyn Error>> {
    let counts = Arc::new(Mutex::new(FakeCounts::default()));
    let store = Arc::new(FakeStore {
        counts: Arc::clone(&counts),
        point_error: None,
        close_error: None,
        read_delay: Duration::ZERO,
    });
    let runtime = RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?;
    let executor = QueryExecutor::new(runtime, store);
    let outcome = executor.execute(nested_query_request()?).await?;
    assert_eq!(outcome.value, CanonicalValue::String("Ada".to_owned()));
    assert_eq!(outcome.snapshot_sequence, Some(7));
    assert_eq!(outcome.dependencies.len(), 2);
    assert_counts(&counts, 1, 1)?;
    let telemetry = executor.telemetry();
    assert_eq!(telemetry.point_reads, 2);
    assert_eq!(telemetry.dependencies, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_query_target_type_visibility_and_auth_fail_closed() -> Result<(), Box<dyn Error>> {
    for visibility in [FunctionVisibility::Internal, FunctionVisibility::Public] {
        let executor = executor(Arc::new(FakeStore {
            counts: Arc::new(Mutex::new(FakeCounts::default())),
            point_error: None,
            close_error: None,
            read_delay: Duration::ZERO,
        }))?;
        assert_eq!(
            executor
                .execute(nested_query_request_target(
                    FunctionType::Query,
                    AuthPolicy::None,
                    visibility,
                )?)
                .await?
                .value,
            CanonicalValue::String("Ada".to_owned())
        );
    }

    for (function_type, auth_policy) in [
        (FunctionType::Mutation, AuthPolicy::None),
        (FunctionType::Query, AuthPolicy::User),
    ] {
        let executor = executor(Arc::new(FakeStore {
            counts: Arc::new(Mutex::new(FakeCounts::default())),
            point_error: None,
            close_error: None,
            read_delay: Duration::ZERO,
        }))?;
        assert_eq!(
            executor
                .execute(nested_query_request_target(
                    function_type,
                    auth_policy,
                    FunctionVisibility::Internal,
                )?)
                .await,
            Err(ExecutionError::Runtime(
                runku_runtime::RuntimeError::JavaScript
            ))
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_recursion_limit_and_shared_cancellation_terminate_the_tree()
-> Result<(), Box<dyn Error>> {
    let recursion_runtime =
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).max_nested_depth(2).build()?)?;
    let recursion_executor = QueryExecutor::new(
        recursion_runtime.clone(),
        Arc::new(FakeStore {
            counts: Arc::new(Mutex::new(FakeCounts::default())),
            point_error: None,
            close_error: None,
            read_delay: Duration::ZERO,
        }),
    );
    assert_eq!(
        recursion_executor
            .execute(nested_control_request(
                "export default async (ctx) => ctx.runQuery('tests.child', null);",
                Duration::from_secs(1),
            )?)
            .await,
        Err(ExecutionError::Runtime(
            runku_runtime::RuntimeError::JavaScript
        ))
    );
    let recursion_telemetry = recursion_runtime.telemetry();
    assert_eq!(recursion_telemetry.function_calls, 3);
    assert_eq!(recursion_telemetry.function_call_limited, 1);

    let cancellation_runtime = RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?;
    let cancellation_executor = QueryExecutor::new(
        cancellation_runtime,
        Arc::new(FakeStore {
            counts: Arc::new(Mutex::new(FakeCounts::default())),
            point_error: None,
            close_error: None,
            read_delay: Duration::ZERO,
        }),
    );
    let request = nested_control_request(
        "export default () => { while (true) {} };",
        Duration::from_secs(1),
    )?;
    let cancellation = request.cancellation();
    let cancel = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();
    });
    assert_eq!(
        cancellation_executor.execute(request).await,
        Err(ExecutionError::Runtime(
            runku_runtime::RuntimeError::Cancelled
        ))
    );
    cancel.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_store_error_and_close_error_cannot_become_success() -> Result<(), Box<dyn Error>> {
    for (point_error, close_error, expected) in [
        (
            Some(StoreError::Unavailable),
            None,
            ExecutionError::Storage(StoreError::Unavailable),
        ),
        (
            None,
            Some(StoreError::Internal),
            ExecutionError::Storage(StoreError::Internal),
        ),
    ] {
        let counts = Arc::new(Mutex::new(FakeCounts::default()));
        let executor = executor(Arc::new(FakeStore {
            counts: Arc::clone(&counts),
            point_error,
            close_error,
            read_delay: Duration::ZERO,
        }))?;
        let source = format!(
            "export default async (ctx) => {{ try {{ await ctx.db.get('{}', '{}'); }} catch {{ return 'caught'; }} return 'read'; }};",
            table_id(),
            document_id_for(12)
        );
        assert_eq!(
            executor
                .execute(query_request(&source, CanonicalValue::Null)?)
                .await,
            Err(expected)
        );
        assert_counts(&counts, 1, 1)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_closes_after_javascript_error_deadline_and_cancel() -> Result<(), Box<dyn Error>>
{
    let table = table_id();
    let document = document_id_for(12);
    for (source, timeout, expected) in [
        (
            format!(
                "export default async (ctx) => {{ await ctx.db.get('{table}', '{document}'); throw new Error('stop'); }};"
            ),
            Duration::from_secs(1),
            ExecutionError::Runtime(runku_runtime::RuntimeError::JavaScript),
        ),
        (
            format!(
                "export default async (ctx) => {{ await ctx.db.get('{table}', '{document}'); while (true) {{}} }};"
            ),
            Duration::from_millis(50),
            ExecutionError::Runtime(runku_runtime::RuntimeError::DeadlineExceeded),
        ),
    ] {
        let counts = Arc::new(Mutex::new(FakeCounts::default()));
        let executor = executor(Arc::new(FakeStore {
            counts: Arc::clone(&counts),
            point_error: None,
            close_error: None,
            read_delay: Duration::ZERO,
        }))?;
        assert_eq!(
            executor
                .execute(query_request_timeout(
                    &source,
                    CanonicalValue::Null,
                    timeout,
                )?)
                .await,
            Err(expected)
        );
        assert_counts(&counts, 1, 1)?;
    }

    let counts = Arc::new(Mutex::new(FakeCounts::default()));
    let executor = executor(Arc::new(FakeStore {
        counts: Arc::clone(&counts),
        point_error: None,
        close_error: None,
        read_delay: Duration::ZERO,
    }))?;
    let source = format!(
        "export default async (ctx) => {{ await ctx.db.get('{table}', '{document}'); while (true) {{}} }};"
    );
    let request = query_request_timeout(&source, CanonicalValue::Null, Duration::from_secs(1))?;
    let cancellation = request.cancellation();
    let cancel = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();
    });
    assert_eq!(
        executor.execute(request).await,
        Err(ExecutionError::Runtime(
            runku_runtime::RuntimeError::Cancelled
        ))
    );
    cancel.await?;
    assert_counts(&counts, 1, 1)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_and_cancel_terminate_inflight_store_reads_and_close_snapshot()
-> Result<(), Box<dyn Error>> {
    let table = table_id();
    let document = document_id_for(12);
    let source = format!("export default async (ctx) => ctx.db.get('{table}', '{document}');");
    for cancelled in [false, true] {
        let counts = Arc::new(Mutex::new(FakeCounts::default()));
        let executor = executor(Arc::new(FakeStore {
            counts: Arc::clone(&counts),
            point_error: None,
            close_error: None,
            read_delay: Duration::from_secs(2),
        }))?;
        let request = query_request_timeout(
            &source,
            CanonicalValue::Null,
            if cancelled {
                Duration::from_secs(3)
            } else {
                Duration::from_millis(500)
            },
        )?;
        let cancellation = request.cancellation();
        let cancel = cancelled.then(|| {
            let counts = Arc::clone(&counts);
            tokio::spawn(async move {
                for _ in 0..200 {
                    if counts.lock().is_ok_and(|counts| counts.began != 0) {
                        cancellation.cancel();
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                cancellation.cancel();
                false
            })
        });
        let expected = if cancelled {
            ExecutionError::Data(runku_runtime::DataReadError::Cancelled)
        } else {
            ExecutionError::Data(runku_runtime::DataReadError::Timeout)
        };
        assert_eq!(executor.execute(request).await, Err(expected));
        if let Some(cancel) = cancel {
            assert!(cancel.await?, "read did not start before cancellation");
        }
        assert_counts(&counts, 1, 1)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_query_engine_conformance() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("query.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    run_real_store(store).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_query_engine_conformance() -> Result<(), Box<dyn Error>> {
    let Ok(url) = std::env::var("RUNKU_TEST_POSTGRES_URL") else {
        return Ok(());
    };
    let store = Arc::new(PostgresStore::connect(&url, PostgresStoreConfig::TEST).await?);
    let result = run_real_store(store.clone()).await;
    store.close().await;
    result
}

async fn run_real_store<S>(store: Arc<S>) -> Result<(), Box<dyn Error>>
where
    S: LogicalStore + 'static,
{
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = table_id();
    let document = document_id_for(21);
    let index = index_id();
    let key = index_key()?;
    let mut batch = CommitBatch::new(scope, OperationId::generate());
    batch.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected: ExpectedRevision::Absent,
        value: CanonicalValue::String("real".to_owned()),
    });
    batch.push_index(IndexMutation::Put {
        index_id: index,
        key: key.clone(),
        table_id: table,
        document_id: document,
        document_revision: 1,
    });
    let commit = store.commit(&batch).await?;
    let source = format!(
        r#"
        export default async (ctx, key) => {{
          const document = await ctx.db.get("{table}", "{document}");
          const rows = await ctx.db.scan("{index}", {{
            lower: {{ kind: "inclusive", key }},
            upper: {{ kind: "inclusive", key }},
            limit: 10
          }});
          return {{ value: document.value, rows: BigInt(rows.length), same: rows[0].documentId.value === document.documentId.value }};
        }};
        "#
    );
    let executor = executor(store)?;
    let outcome = executor
        .execute(query_request_scoped(
            &source,
            CanonicalValue::Bytes(key.as_bytes().to_vec()),
            Duration::from_secs(2),
            scope,
        )?)
        .await?;
    assert_eq!(outcome.snapshot_sequence, Some(commit.commit_sequence));
    assert_eq!(outcome.dependencies.len(), 2);
    let CanonicalValue::Object(value) = outcome.value else {
        return Err("expected query object".into());
    };
    assert_eq!(value["value"], CanonicalValue::String("real".to_owned()));
    assert_eq!(value["rows"], CanonicalValue::Int64(1));
    assert_eq!(value["same"], CanonicalValue::Boolean(true));
    Ok(())
}

fn executor(store: Arc<dyn LogicalStore>) -> Result<QueryExecutor, RuntimeErrorBox> {
    let runtime = RuntimeSupervisor::start(RuntimeLimits::builder(2, 16).build()?)?;
    Ok(QueryExecutor::new(runtime, store))
}

#[derive(Debug)]
struct RuntimeErrorBox(runku_runtime::RuntimeError);

impl std::fmt::Display for RuntimeErrorBox {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for RuntimeErrorBox {}

impl From<runku_runtime::RuntimeError> for RuntimeErrorBox {
    fn from(value: runku_runtime::RuntimeError) -> Self {
        Self(value)
    }
}

fn query_request(
    source: &str,
    arguments: CanonicalValue,
) -> Result<InvocationRequest, Box<dyn Error>> {
    query_request_timeout(source, arguments, Duration::from_secs(2))
}

fn query_request_timeout(
    source: &str,
    arguments: CanonicalValue,
    timeout: Duration,
) -> Result<InvocationRequest, Box<dyn Error>> {
    query_request_scoped(source, arguments, timeout, scope())
}

fn query_request_scoped(
    source: &str,
    arguments: CanonicalValue,
    timeout: Duration,
    scope: EnvironmentScope,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let release_id = ReleaseId::from_ulid(Ulid::from(1_u128));
    let function_id = FunctionId::from_ulid(Ulid::from(4_u128));
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::from_ulid(Ulid::from(3_u128)),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([2; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([3; 32]),
        index_contract_hash: Sha256Digest::from_bytes([4; 32]),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "tests.query".parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([5; 32]),
            result_contract_hash: Sha256Digest::from_bytes([6; 32]),
            capabilities: vec![Capability::DbRead],
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
        timeout,
        CancellationToken::new(),
    )?)
}

fn nested_query_request() -> Result<InvocationRequest, Box<dyn Error>> {
    nested_query_request_target(
        FunctionType::Query,
        AuthPolicy::None,
        FunctionVisibility::Internal,
    )
}

fn nested_query_request_target(
    target_type: FunctionType,
    target_auth: AuthPolicy,
    target_visibility: FunctionVisibility,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let table = table_id();
    let hit = document_id_for(12);
    let miss = document_id_for(13);
    let parent_source = format!(
        r#"
        export default async (ctx) => {{
          const parent = await ctx.db.get("{table}", "{hit}");
          const child = await ctx.runQuery("tests.child", null);
          return child === null ? parent.value : "unexpected";
        }};
        "#
    );
    let child_source =
        format!("export default async (ctx) => ctx.db.get(\"{table}\", \"{miss}\");\n");
    let bundle = SafeEsmBundleV1::from_sources([parent_source.as_str(), child_source.as_str()])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let scope = scope();
    let release_id = ReleaseId::from_ulid(Ulid::from(31_u128));
    let parent_id = FunctionId::from_ulid(Ulid::from(32_u128));
    let child_id = FunctionId::from_ulid(Ulid::from(33_u128));
    let function = |id,
                    name: &str,
                    source: &str,
                    function_type,
                    visibility,
                    auth_policy,
                    capabilities|
     -> Result<_, Box<dyn Error>> {
        Ok(FunctionManifest {
            id,
            name: name.parse()?,
            function_type,
            visibility,
            auth_policy,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([5; 32]),
            result_contract_hash: Sha256Digest::from_bytes([6; 32]),
            capabilities,
        })
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::from_ulid(Ulid::from(34_u128)),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([2; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([3; 32]),
        index_contract_hash: Sha256Digest::from_bytes([4; 32]),
        functions: vec![
            function(
                child_id,
                "tests.child",
                &child_source,
                target_type,
                target_visibility,
                target_auth,
                vec![Capability::DbRead],
            )?,
            function(
                parent_id,
                "tests.parent",
                &parent_source,
                FunctionType::Query,
                FunctionVisibility::Public,
                AuthPolicy::None,
                vec![Capability::DbRead, Capability::FunctionQuery],
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

fn nested_control_request(
    child_source: &str,
    timeout: Duration,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let parent_source = "export default async (ctx) => ctx.runQuery('tests.child', null);\n";
    let bundle = SafeEsmBundleV1::from_sources([parent_source, child_source])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let scope = scope();
    let release_id = ReleaseId::generate();
    let child_id = FunctionId::generate();
    let parent_id = FunctionId::generate();
    let function = |id, name: &str, source: &str| -> Result<_, Box<dyn Error>> {
        Ok(FunctionManifest {
            id,
            name: name.parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Internal,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([5; 32]),
            result_contract_hash: Sha256Digest::from_bytes([6; 32]),
            capabilities: vec![Capability::FunctionQuery],
        })
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([2; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([3; 32]),
        index_contract_hash: Sha256Digest::from_bytes([4; 32]),
        functions: vec![
            function(child_id, "tests.child", child_source)?,
            function(parent_id, "tests.parent", parent_source)?,
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
        timeout,
        CancellationToken::new(),
    )?)
}

fn assert_counts(counts: &Mutex<FakeCounts>, began: u64, closed: u64) -> Result<(), StoreError> {
    let counts = counts.lock().map_err(|_| StoreError::Internal)?;
    assert_eq!(counts.began, began);
    assert_eq!(counts.closed, closed);
    Ok(())
}

fn scope() -> EnvironmentScope {
    EnvironmentScope::new(
        ProjectId::from_ulid(Ulid::from(2_u128)),
        EnvironmentId::from_ulid(Ulid::from(7_u128)),
    )
}

fn table_id() -> TableId {
    TableId::from_ulid(Ulid::from(10_u128))
}

fn index_id() -> IndexId {
    IndexId::from_ulid(Ulid::from(11_u128))
}

fn document_id_for(value: u128) -> DocumentId {
    DocumentId::from_ulid(Ulid::from(value))
}

fn index_key() -> Result<IndexKey, Box<dyn Error>> {
    IndexKey::encode(&[IndexValue::String("ada".to_owned())]).map_err(Into::into)
}
