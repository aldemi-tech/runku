//! Safe Runtime adversarial and behavioral conformance.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use runku_contracts::{
    Contract, DocumentSchemaV1, DocumentTableContract, encode_contract, encode_document_schema,
};
use runku_core::{
    BuildId, DevRevisionId, DocumentId, EnvironmentId, EnvironmentScope, FunctionId, IndexId,
    InvocationId, PinnedCode, ProjectId, ReleaseId, RequestId, ScheduledInvocationId, TableId,
};
use runku_observability::{
    InvocationPerformanceSink, LogEventKind, LogSinkError, LogStream,
    MemoryInvocationPerformanceSink, OperationalEventV1, OperationalLogSink, PerformanceOperation,
    PerformanceOutcome, PerformanceRuntime,
};
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_safe_esm_bundle,
};
use runku_runtime::{
    CancellationToken, DataDocument, DataGetRequest, DataIndexEntry, DataRead, DataReadError,
    DataScanRequest, DataWrite, FileBytes, FileDownloadGrant, FileDownloadGrantRequest,
    FileMetadata, FileStorage, FileStorageError, FileStoreRequest, FileUploadGrant,
    FileUploadGrantRequest, FunctionCallError, FunctionCallKind, FunctionCallRequest,
    FunctionInvoke, HttpsEgress, HttpsError, HttpsRequest, HttpsResponse, InvocationRequest,
    RuntimeError, RuntimeLimits, RuntimeSupervisor, ScheduleCreate, ScheduleError, ScheduleRequest,
    ScheduleTime,
};
use runku_schema::{SchemaCatalog, encode_schema_catalog};
use runku_value::{CanonicalValue, FiniteF64, TimestampMicros, TypedId};
use ulid::Ulid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_and_async_handlers_round_trip_every_canonical_value() -> Result<(), Box<dyn Error>> {
    let limits = RuntimeLimits::builder(2, 8).build()?;
    let supervisor = RuntimeSupervisor::start(limits)?;
    let arguments = complex_value()?;
    for source in [
        "export default (_ctx, args) => args;\n",
        "export default async (ctx, args) => { await ctx.cooperate(); return args; };\n",
        r#"
          Array.prototype.map = () => { throw new Error("patched map"); };
          Array.prototype.sort = () => { throw new Error("patched sort"); };
          globalThis.BigInt = () => { throw new Error("patched bigint"); };
          Uint8Array.from = () => { throw new Error("patched bytes"); };
          globalThis.WeakSet = class { constructor() { throw new Error("patched weakset"); } };
          Number.isFinite = () => false;
          export default (_ctx, args) => args;
        "#,
    ] {
        let result = supervisor
            .invoke(request(
                source,
                arguments.clone(),
                Duration::from_secs(2),
                CancellationToken::new(),
            )?)
            .await?;
        assert_eq!(result, arguments);
    }
    let telemetry = supervisor.telemetry();
    assert_eq!(telemetry.admitted, 3);
    assert_eq!(telemetry.succeeded, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_apis_and_shared_memory_are_absent() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 2).build()?)?;
    let source = r#"
        export default (ctx) => ({
          deno: typeof Deno === "undefined",
          process: typeof process === "undefined",
          require: typeof require === "undefined",
          fetch: typeof fetch === "undefined",
          db: typeof ctx.db === "undefined",
          webAssembly: typeof WebAssembly === "undefined",
          sharedArrayBuffer: typeof SharedArrayBuffer === "undefined",
          atomics: typeof Atomics === "undefined",
          contextFrozen: Object.isFrozen(ctx),
          invocationFrozen: Object.isFrozen(ctx.invocation),
          capabilitiesFrozen: Object.isFrozen(ctx.invocation.capabilities)
        });
    "#;
    let result = supervisor
        .invoke(request(
            source,
            CanonicalValue::Null,
            Duration::from_secs(2),
            CancellationToken::new(),
        )?)
        .await?;
    let CanonicalValue::Object(values) = result else {
        return Err("expected object".into());
    };
    assert_eq!(values.len(), 11);
    assert!(
        values
            .values()
            .all(|value| value == &CanonicalValue::Boolean(true)),
        "unexpected host global: {values:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn imports_default_export_js_throw_and_invalid_results_fail_closed()
-> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    for source in [
        "import './dependency.js'; export default () => null;",
        "import { op_runku_cooperate } from 'ext:core/ops'; export default () => op_runku_cooperate();",
        "export default () => import('ext:core/ops');",
    ] {
        assert_eq!(
            supervisor
                .invoke(request(
                    source,
                    CanonicalValue::Null,
                    Duration::from_secs(2),
                    CancellationToken::new(),
                )?)
                .await,
            Err(RuntimeError::JavaScript)
        );
    }
    assert_eq!(
        supervisor
            .invoke(request(
                "export default 42;",
                CanonicalValue::Null,
                Duration::from_secs(2),
                CancellationToken::new(),
            )?)
            .await,
        Err(RuntimeError::InvalidResult)
    );
    assert_eq!(
        supervisor
            .invoke(request(
                "export default () => { throw new Error('private secret detail'); };",
                CanonicalValue::Null,
                Duration::from_secs(2),
                CancellationToken::new(),
            )?)
            .await,
        Err(RuntimeError::JavaScript)
    );
    for source in [
        "export default () => { const value = {}; value.self = value; return value; };",
        "export default () => Object.create({ inherited: true });",
        "export default () => Number.POSITIVE_INFINITY;",
        "export default () => undefined;",
        "export default () => { let value = null; for (let i = 0; i < 70; i++) value = [value]; return value; };",
        "export default () => 'x'.repeat(1024 * 1024 + 1);",
    ] {
        assert_eq!(
            supervisor
                .invoke(request(
                    source,
                    CanonicalValue::Null,
                    Duration::from_secs(2),
                    CancellationToken::new(),
                )?)
                .await,
            Err(RuntimeError::InvalidResult)
        );
    }
    assert_eq!(
        RuntimeError::JavaScript.to_string(),
        "runtime JavaScript execution failed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn op_budget_is_enforced_and_worker_recovers() -> Result<(), Box<dyn Error>> {
    let limits = RuntimeLimits::builder(1, 2).max_ops(1).build()?;
    let supervisor = RuntimeSupervisor::start(limits)?;
    let too_many = "export default async (ctx) => { await ctx.cooperate(); await ctx.cooperate(); return null; };";
    assert_eq!(
        supervisor
            .invoke(request(
                too_many,
                CanonicalValue::Null,
                Duration::from_secs(2),
                CancellationToken::new(),
            )?)
            .await,
        Err(RuntimeError::JavaScript)
    );
    assert_eq!(
        supervisor
            .invoke(request(
                "export default () => 7;",
                CanonicalValue::Null,
                Duration::from_secs(2),
                CancellationToken::new(),
            )?)
            .await?,
        CanonicalValue::Float64(FiniteF64::new(7.0)?)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn deadline_and_explicit_cancel_terminate_sync_loops_and_recover()
-> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(
        RuntimeLimits::builder(2, 4)
            .max_ops(1_000_000)
            .max_wall_time(Duration::from_secs(2))
            .build()?,
    )?;
    let performance = Arc::new(MemoryInvocationPerformanceSink::new(128)?);
    let performance_sink: Arc<dyn InvocationPerformanceSink> = performance.clone();
    for source in [
        "export default () => { while (true) {} };",
        "while (true) {} export default () => null;",
        "export default async (ctx) => { while (true) await ctx.cooperate(); };",
    ] {
        assert_eq!(
            supervisor
                .invoke(
                    request(
                        source,
                        CanonicalValue::Null,
                        Duration::from_millis(60),
                        CancellationToken::new(),
                    )?
                    .with_performance_sink(
                        PerformanceRuntime::SafeV8,
                        Arc::clone(&performance_sink),
                    ),
                )
                .await,
            Err(RuntimeError::DeadlineExceeded)
        );
    }

    let cancellation = CancellationToken::new();
    let invoke = tokio::spawn({
        let supervisor = supervisor.clone();
        let request = request(
            "export default () => { while (true) {} };",
            CanonicalValue::Null,
            Duration::from_secs(1),
            cancellation.clone(),
        )?
        .with_performance_sink(PerformanceRuntime::SafeV8, performance_sink);
        async move { supervisor.invoke(request).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    cancellation.cancel();
    assert_eq!(invoke.await?, Err(RuntimeError::Cancelled));

    assert_eq!(
        supervisor
            .invoke(request(
                "export default () => 'recovered';",
                CanonicalValue::Null,
                Duration::from_secs(1),
                CancellationToken::new(),
            )?)
            .await?,
        CanonicalValue::String("recovered".to_owned())
    );
    let telemetry = supervisor.telemetry();
    assert_eq!(telemetry.deadline_exceeded, 3);
    assert_eq!(telemetry.cancelled, 1);
    let spans = performance.snapshot();
    assert!(spans.iter().any(|span| {
        span.operation == PerformanceOperation::Invocation
            && span.outcome == PerformanceOutcome::DeadlineExceeded
    }));
    assert!(spans.iter().any(|span| {
        span.operation == PerformanceOperation::Invocation
            && span.outcome == PerformanceOutcome::Cancelled
    }));
    assert!(spans.iter().any(|span| {
        span.operation == PerformanceOperation::Invocation
            && span
                .resources
                .is_some_and(|resources| resources.memory_bytes.is_some())
    }));
    #[cfg(target_os = "linux")]
    assert!(spans.iter().any(|span| {
        span.operation == PerformanceOperation::Invocation
            && span
                .resources
                .is_some_and(|resources| resources.cpu_total_micros.is_some())
    }));
    assert!(
        spans
            .iter()
            .all(|span| span.outcome != PerformanceOutcome::Abandoned)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn near_heap_limit_terminates_isolate_and_worker_recovers() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(
        RuntimeLimits::builder(1, 2)
            .heap_bytes(16 * 1024 * 1024)
            .max_wall_time(Duration::from_secs(3))
            .build()?,
    )?;
    let source = "export default () => { const values = []; while (true) values.push(new Array(10000).fill('xxxxxxxx')); };";
    assert_eq!(
        supervisor
            .invoke(request(
                source,
                CanonicalValue::Null,
                Duration::from_secs(2),
                CancellationToken::new(),
            )?)
            .await,
        Err(RuntimeError::HeapLimitExceeded)
    );
    assert!(
        supervisor
            .invoke(request(
                "export default () => true;",
                CanonicalValue::Null,
                Duration::from_secs(1),
                CancellationToken::new(),
            )?)
            .await
            .is_ok()
    );
    assert_eq!(supervisor.telemetry().heap_exceeded, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_isolates_prevent_global_leakage() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 2).build()?)?;
    supervisor
        .invoke(request(
            "export default () => { globalThis.tenantSecret = 'alpha'; return null; };",
            CanonicalValue::Null,
            Duration::from_secs(1),
            CancellationToken::new(),
        )?)
        .await?;
    let result = supervisor
        .invoke(request(
            "export default () => typeof globalThis.tenantSecret === 'undefined';",
            CanonicalValue::Null,
            Duration::from_secs(1),
            CancellationToken::new(),
        )?)
        .await?;
    assert_eq!(result, CanonicalValue::Boolean(true));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_queue_rejects_overload_without_losing_worker() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(
        RuntimeLimits::builder(1, 1)
            .max_wall_time(Duration::from_secs(2))
            .build()?,
    )?;
    let first_cancel = CancellationToken::new();
    let second_cancel = CancellationToken::new();
    let first = tokio::spawn({
        let supervisor = supervisor.clone();
        let request = request(
            "export default () => { while (true) {} };",
            CanonicalValue::Null,
            Duration::from_secs(1),
            first_cancel.clone(),
        )?;
        async move { supervisor.invoke(request).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let second = tokio::spawn({
        let supervisor = supervisor.clone();
        let request = request(
            "export default () => 'queued';",
            CanonicalValue::Null,
            Duration::from_secs(1),
            second_cancel.clone(),
        )?;
        async move { supervisor.invoke(request).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        supervisor
            .invoke(request(
                "export default () => 'rejected';",
                CanonicalValue::Null,
                Duration::from_secs(1),
                CancellationToken::new(),
            )?)
            .await,
        Err(RuntimeError::Busy)
    );
    first_cancel.cancel();
    second_cancel.cancel();
    assert_eq!(first.await?, Err(RuntimeError::Cancelled));
    assert_eq!(second.await?, Err(RuntimeError::Cancelled));
    assert_eq!(supervisor.telemetry().busy, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_version_class_and_artifact_integrity_fail_closed() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?;
    assert_eq!(
        supervisor
            .invoke(request_options(
                "export default () => null;",
                CanonicalValue::Null,
                Duration::from_secs(1),
                CancellationToken::new(),
                "platform-js-1",
                RuntimeClass::FullNode,
                false,
            )?)
            .await,
        Err(RuntimeError::UnsupportedRuntime)
    );
    assert_eq!(
        supervisor
            .invoke(request_options(
                "export default () => null;",
                CanonicalValue::Null,
                Duration::from_secs(1),
                CancellationToken::new(),
                "runku-js-1",
                RuntimeClass::SafeV8,
                false,
            )?)
            .await,
        Err(RuntimeError::InvalidArtifact)
    );
    assert_eq!(
        supervisor
            .invoke(request_options(
                "export default () => null;",
                CanonicalValue::Null,
                Duration::from_secs(1),
                CancellationToken::new(),
                "platform-js-1",
                RuntimeClass::SafeV8,
                true,
            )?)
            .await,
        Err(RuntimeError::InvalidArtifact)
    );
    assert_eq!(supervisor.telemetry().invalid, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn safe_v8_rejects_node_crypto_and_has_no_node_authority() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?;
    let source = r#"
import { createHash } from "node:crypto";
export default () => createHash("sha256").update("runku").digest("hex");
"#;
    assert_eq!(
        supervisor
            .invoke(request(
                source,
                CanonicalValue::Null,
                Duration::from_secs(1),
                CancellationToken::new(),
            )?)
            .await,
        Err(RuntimeError::JavaScript)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runku_js_enforces_arguments_results_and_document_writes() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    let table_id = TableId::from_ulid(Ulid::from(91_u128));
    let schema = DocumentSchemaV1::new(vec![DocumentTableContract {
        id: table_id,
        name: "events".to_owned(),
        document_contract: Contract::String {
            minimum_bytes: Some(6),
            maximum_bytes: Some(6),
        },
    }])?;
    let text = Contract::String {
        minimum_bytes: Some(1),
        maximum_bytes: Some(8),
    };
    let valid = request_with_contracts(
        "export const contract = (_ctx, value) => value;",
        CanonicalValue::String("ok".to_owned()),
        FunctionType::Query,
        vec![],
        &text,
        &text,
        &schema,
    )?;
    assert_eq!(
        supervisor.invoke(valid).await,
        Ok(CanonicalValue::String("ok".to_owned()))
    );

    let invalid_arguments = request_with_contracts(
        "export const contract = (_ctx, value) => value;",
        CanonicalValue::Null,
        FunctionType::Query,
        vec![],
        &text,
        &text,
        &schema,
    )?;
    assert_eq!(
        supervisor.invoke(invalid_arguments).await,
        Err(RuntimeError::InvalidArguments)
    );
    let invalid_result = request_with_contracts(
        "export const contract = () => 1n;",
        CanonicalValue::String("ok".to_owned()),
        FunctionType::Query,
        vec![],
        &text,
        &text,
        &schema,
    )?;
    assert_eq!(
        supervisor.invoke(invalid_result).await,
        Err(RuntimeError::InvalidResult)
    );

    let broker = Arc::new(MockMutationData::default());
    let document_id = DocumentId::from_ulid(Ulid::from(92_u128));
    let invalid_write = request_with_contracts(
        &format!(
            "export const contract = async (ctx) => {{ await ctx.db.insert('{table_id}', '{document_id}', null); return true; }};"
        ),
        CanonicalValue::Null,
        FunctionType::Mutation,
        vec![Capability::DbWrite],
        &Contract::Null,
        &Contract::Boolean,
        &schema,
    )?
    .with_mutation_data(broker.clone())?;
    assert_eq!(
        supervisor.invoke(invalid_write).await,
        Err(RuntimeError::JavaScript)
    );
    assert_eq!(broker.inserts.load(Ordering::Relaxed), 0);

    let valid_write = request_with_contracts(
        &format!(
            "export const contract = async (ctx) => {{ await ctx.db.insert('{table_id}', '{document_id}', 'insert'); return true; }};"
        ),
        CanonicalValue::Null,
        FunctionType::Mutation,
        vec![Capability::DbWrite],
        &Contract::Null,
        &Contract::Boolean,
        &schema,
    )?
    .with_mutation_data(broker.clone())?;
    assert_eq!(
        supervisor.invoke(valid_write).await,
        Ok(CanonicalValue::Boolean(true))
    );
    assert_eq!(broker.inserts.load(Ordering::Relaxed), 1);
    Ok(())
}

#[derive(Debug)]
struct MockHttpsEgress {
    calls: AtomicU64,
    result: Result<HttpsResponse, HttpsError>,
}

#[derive(Debug)]
struct MockDataRead;

#[derive(Debug, Default)]
struct MockMutationData {
    inserts: AtomicU64,
    replaces: AtomicU64,
    deletes: AtomicU64,
}

#[async_trait]
impl DataRead for MockDataRead {
    async fn get(
        &self,
        request: DataGetRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<Option<DataDocument>, DataReadError> {
        Ok(Some(DataDocument {
            table_id: request.table_id,
            document_id: request.document_id,
            revision: i64::MAX.cast_unsigned(),
            commit_sequence: (i64::MAX - 1).cast_unsigned(),
            created_at: TimestampMicros::new(-10),
            updated_at: TimestampMicros::new(20),
            value: CanonicalValue::Bytes(vec![0, 1, 255]),
        }))
    }

    async fn scan(
        &self,
        request: DataScanRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<Vec<DataIndexEntry>, DataReadError> {
        Ok(vec![DataIndexEntry {
            index_id: request.index_id,
            key: request.lower.map_or_else(|| vec![1], |bound| bound.key),
            table_id: TableId::from_ulid(Ulid::from(10_u128)),
            document_id: DocumentId::from_ulid(Ulid::from(12_u128)),
            document_revision: i64::MAX.cast_unsigned(),
            commit_sequence: (i64::MAX - 1).cast_unsigned(),
        }])
    }
}

#[async_trait]
impl DataRead for MockMutationData {
    async fn get(
        &self,
        request: DataGetRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<Option<DataDocument>, DataReadError> {
        Ok(Some(DataDocument {
            table_id: request.table_id,
            document_id: request.document_id,
            revision: 7,
            commit_sequence: 11,
            created_at: TimestampMicros::new(10),
            updated_at: TimestampMicros::new(20),
            value: CanonicalValue::String("current".to_owned()),
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

#[async_trait]
impl DataWrite for MockMutationData {
    async fn insert(
        &self,
        _table_id: TableId,
        _document_id: DocumentId,
        value: CanonicalValue,
    ) -> Result<(), DataReadError> {
        if value != CanonicalValue::String("insert".to_owned()) {
            return Err(DataReadError::InvalidRequest);
        }
        self.inserts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn replace(
        &self,
        _table_id: TableId,
        _document_id: DocumentId,
        expected_revision: u64,
        value: CanonicalValue,
    ) -> Result<(), DataReadError> {
        if expected_revision != 7 || value != CanonicalValue::Bytes(vec![0, 1, 255]) {
            return Err(DataReadError::InvalidRequest);
        }
        self.replaces.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn delete(
        &self,
        _table_id: TableId,
        _document_id: DocumentId,
        expected_revision: u64,
    ) -> Result<(), DataReadError> {
        if expected_revision != 8 {
            return Err(DataReadError::InvalidRequest);
        }
        self.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_data_bridge_is_typed_capability_scoped_and_primordial_safe()
-> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    let table = TableId::from_ulid(Ulid::from(10_u128));
    let document = DocumentId::from_ulid(Ulid::from(12_u128));
    let index = IndexId::from_ulid(Ulid::from(11_u128));
    let source = format!(
        r#"
        globalThis.BigInt = () => {{ throw new Error("patched bigint"); }};
        Array.prototype.map = () => {{ throw new Error("patched map"); }};
        Uint8Array.from = () => {{ throw new Error("patched bytes"); }};
        export default async (ctx) => {{
          const document = await ctx.db.get("{table}", "{document}");
          const rows = await ctx.db.scan("{index}", {{
            lower: {{ kind: "inclusive", key: new Uint8Array([1, 2]) }},
            limit: 1
          }});
          return {{
            revision: document.revision,
            sequence: document.commitSequence,
            value: document.value,
            rowKey: rows[0].key,
            frozen: Object.isFrozen(ctx.db) && Object.isFrozen(document)
              && Object.isFrozen(rows) && Object.isFrozen(rows[0])
          }};
        }};
        "#
    );
    let request = request_function(
        &source,
        CanonicalValue::Null,
        FunctionType::Query,
        vec![Capability::DbRead],
    )?
    .with_data(Arc::new(MockDataRead))?;
    let CanonicalValue::Object(output) = supervisor.invoke(request).await? else {
        return Err("expected data bridge object".into());
    };
    assert_eq!(output["revision"], CanonicalValue::Int64(i64::MAX));
    assert_eq!(output["sequence"], CanonicalValue::Int64(i64::MAX - 1));
    assert_eq!(output["value"], CanonicalValue::Bytes(vec![0, 1, 255]));
    assert_eq!(output["rowKey"], CanonicalValue::Bytes(vec![1, 2]));
    assert_eq!(output["frozen"], CanonicalValue::Boolean(true));

    let unavailable = request_function(
        &format!("export default async (ctx) => ctx.db.get('{table}', '{document}');"),
        CanonicalValue::Null,
        FunctionType::Query,
        vec![Capability::DbRead],
    )?;
    assert_eq!(
        supervisor.invoke(unavailable).await,
        Err(RuntimeError::JavaScript)
    );
    let invalid = request_function(
        "export default async (ctx) => ctx.db.get('tbl_invalid', 'doc_invalid');",
        CanonicalValue::Null,
        FunctionType::Query,
        vec![Capability::DbRead],
    )?
    .with_data(Arc::new(MockDataRead))?;
    assert_eq!(
        supervisor.invoke(invalid).await,
        Err(RuntimeError::JavaScript)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn document_ids_are_deterministic_table_scoped_and_schema_checked()
-> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    let rooms = TableId::from_ulid(Ulid::from(13_u128));
    let profiles = TableId::from_ulid(Ulid::from(14_u128));
    let schema = DocumentSchemaV1::new(vec![
        DocumentTableContract {
            id: rooms,
            name: "rooms".to_owned(),
            document_contract: Contract::Any,
        },
        DocumentTableContract {
            id: profiles,
            name: "profiles".to_owned(),
            document_contract: Contract::Any,
        },
    ])?;
    let source = format!(
        r#"
        export const contract = (ctx) => {{
          const room = ctx.db.documentId("{rooms}", "stable-key");
          const same = ctx.db.documentId("{rooms}", "stable-key");
          const profile = ctx.db.documentId("{profiles}", "stable-key");
          return {{ room, same: room.value === same.value, profile }};
        }};
        "#
    );
    let invoke = || -> Result<InvocationRequest, Box<dyn Error>> {
        Ok(request_with_contracts(
            &source,
            CanonicalValue::Null,
            FunctionType::Query,
            vec![Capability::DbRead],
            &Contract::Null,
            &Contract::Any,
            &schema,
        )?
        .with_data(Arc::new(MockDataRead))?)
    };
    let first = supervisor.invoke(invoke()?).await?;
    let second = supervisor.invoke(invoke()?).await?;
    assert_eq!(first, second);
    let CanonicalValue::Object(values) = first else {
        return Err("expected deterministic document IDs".into());
    };
    assert_eq!(values["same"], CanonicalValue::Boolean(true));
    assert_ne!(values["room"], values["profile"]);
    for name in ["room", "profile"] {
        let CanonicalValue::TypedId(id) = &values[name] else {
            return Err("expected a typed document ID".into());
        };
        assert_eq!(id.kind(), "doc");
    }

    let invalid = request_with_contracts(
        "export const contract = (ctx) => ctx.db.documentId('tbl_00000000000000000000000000', 'key');",
        CanonicalValue::Null,
        FunctionType::Query,
        vec![Capability::DbRead],
        &Contract::Null,
        &Contract::Any,
        &schema,
    )?
    .with_data(Arc::new(MockDataRead))?;
    assert_eq!(
        supervisor.invoke(invalid).await,
        Err(RuntimeError::JavaScript)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_data_bridge_is_typed_and_capability_scoped() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    let table = TableId::from_ulid(Ulid::from(20_u128));
    let first = DocumentId::from_ulid(Ulid::from(21_u128));
    let second = DocumentId::from_ulid(Ulid::from(22_u128));
    let third = DocumentId::from_ulid(Ulid::from(23_u128));
    let broker = Arc::new(MockMutationData::default());
    let source = format!(
        r#"
        globalThis.BigInt = () => {{ throw new Error("patched bigint"); }};
        Uint8Array.from = () => {{ throw new Error("patched bytes"); }};
        export default async (ctx) => {{
          const current = await ctx.db.get("{table}", "{first}");
          await ctx.db.insert("{table}", "{first}", "insert");
          await ctx.db.replace("{table}", "{second}", current.revision, new Uint8Array([0, 1, 255]));
          await ctx.db.delete("{table}", "{third}", 8n);
          return {{
            frozen: Object.isFrozen(ctx.db),
            scanAbsent: typeof ctx.db.scan === "undefined",
            value: current.value
          }};
        }};
        "#
    );
    let request = request_function(
        &source,
        CanonicalValue::Null,
        FunctionType::Mutation,
        vec![Capability::DbRead, Capability::DbWrite],
    )?
    .with_mutation_data(broker.clone())?;
    let output = supervisor.invoke(request).await?;
    assert_eq!(
        output,
        CanonicalValue::Object(BTreeMap::from([
            ("frozen".to_owned(), CanonicalValue::Boolean(true)),
            ("scanAbsent".to_owned(), CanonicalValue::Boolean(true)),
            (
                "value".to_owned(),
                CanonicalValue::String("current".to_owned()),
            ),
        ]))
    );
    assert_eq!(broker.inserts.load(Ordering::Relaxed), 1);
    assert_eq!(broker.replaces.load(Ordering::Relaxed), 1);
    assert_eq!(broker.deletes.load(Ordering::Relaxed), 1);

    let write_only = Arc::new(MockMutationData::default());
    let request = request_function(
        &format!(
            "export default async (ctx) => {{ if (typeof ctx.db.get !== 'undefined') throw new Error('read leaked'); await ctx.db.insert('{table}', '{first}', 'insert'); return typeof ctx.db.scan === 'undefined'; }};"
        ),
        CanonicalValue::Null,
        FunctionType::Mutation,
        vec![Capability::DbWrite],
    )?
    .with_mutation_data(write_only.clone())?;
    assert_eq!(
        supervisor.invoke(request).await?,
        CanonicalValue::Boolean(true)
    );
    assert_eq!(write_only.inserts.load(Ordering::Relaxed), 1);

    let query = request_function(
        "export default (ctx) => typeof ctx.db.insert === 'undefined';",
        CanonicalValue::Null,
        FunctionType::Query,
        vec![Capability::DbRead],
    )?
    .with_data(Arc::new(MockDataRead))?;
    assert_eq!(
        supervisor.invoke(query).await?,
        CanonicalValue::Boolean(true)
    );
    Ok(())
}

#[async_trait]
impl HttpsEgress for MockHttpsEgress {
    async fn execute(
        &self,
        request: HttpsRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<HttpsResponse, HttpsError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if request.method != runku_runtime::HttpsMethod::Post
            || request.url != "https://api.example.com/events"
            || request.body != [0, 1, 255]
            || request.idempotency_key.as_deref() != Some("event_1")
        {
            return Err(HttpsError::InvalidRequest);
        }
        self.result.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn action_https_is_capability_scoped_typed_and_recovers_after_error()
-> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    for (function_type, capabilities) in [
        (FunctionType::Query, vec![]),
        (FunctionType::Mutation, vec![]),
        (FunctionType::Action, vec![]),
    ] {
        let output = supervisor
            .invoke(request_function(
                "export default (ctx) => typeof ctx.https === 'undefined';",
                CanonicalValue::Null,
                function_type,
                capabilities,
            )?)
            .await?;
        assert_eq!(output, CanonicalValue::Boolean(true));
    }

    let unavailable = request_function(
        "export default async (ctx) => ctx.https.request({ method: 'GET', url: 'https://api.example.com' });",
        CanonicalValue::Null,
        FunctionType::Action,
        vec![Capability::NetworkHttps],
    )?;
    assert_eq!(
        supervisor.invoke(unavailable).await,
        Err(RuntimeError::JavaScript)
    );

    let broker = Arc::new(MockHttpsEgress {
        calls: AtomicU64::new(0),
        result: Ok(HttpsResponse {
            status: 202,
            headers: BTreeMap::from([("x-result".to_owned(), vec!["accepted".to_owned()])]),
            body: vec![9, 8, 7],
        }),
    });
    let source = r#"
        export default async (ctx) => {
          const response = await ctx.https.request({
            method: "POST",
            url: "https://api.example.com/events",
            headers: { "content-type": ["application/octet-stream"] },
            body: new Uint8Array([0, 1, 255]),
            idempotencyKey: "event_1"
          });
          if (!Object.isFrozen(ctx.https) || !Object.isFrozen(response)
              || !Object.isFrozen(response.headers) || !Object.isFrozen(response.headers["x-result"])) {
            throw new Error("HTTPS bridge is mutable");
          }
          return { status: response.status, body: response.body };
        };
    "#;
    let output = supervisor
        .invoke(
            request_function(
                source,
                CanonicalValue::Null,
                FunctionType::Action,
                vec![Capability::NetworkHttps],
            )?
            .with_https(broker.clone())?,
        )
        .await?;
    assert_eq!(broker.calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        output,
        CanonicalValue::Object(BTreeMap::from([
            ("body".to_owned(), CanonicalValue::Bytes(vec![9, 8, 7])),
            (
                "status".to_owned(),
                CanonicalValue::Float64(FiniteF64::new(202.0)?),
            ),
        ]))
    );

    let failing_broker = Arc::new(MockHttpsEgress {
        calls: AtomicU64::new(0),
        result: Err(HttpsError::Timeout),
    });
    assert_eq!(
        supervisor
            .invoke(
                request_function(
                    source,
                    CanonicalValue::Null,
                    FunctionType::Action,
                    vec![Capability::NetworkHttps],
                )?
                .with_https(failing_broker)?,
            )
            .await,
        Err(RuntimeError::JavaScript)
    );
    assert_eq!(
        supervisor
            .invoke(request(
                "export default () => 'recovered';",
                CanonicalValue::Null,
                Duration::from_secs(1),
                CancellationToken::new(),
            )?)
            .await?,
        CanonicalValue::String("recovered".to_owned())
    );

    let query = request_function(
        "export default () => null;",
        CanonicalValue::Null,
        FunctionType::Query,
        vec![],
    )?;
    let unused_broker = Arc::new(MockHttpsEgress {
        calls: AtomicU64::new(0),
        result: Err(HttpsError::Transport),
    });
    assert_eq!(
        query.with_https(unused_broker).map(|_| ()),
        Err(RuntimeError::InvalidInvocation)
    );
    Ok(())
}

#[derive(Debug, Default)]
struct MockFileStorage {
    operations: Mutex<Vec<&'static str>>,
}

impl MockFileStorage {
    fn record(&self, operation: &'static str) -> Result<(), FileStorageError> {
        self.operations
            .lock()
            .map_err(|_| FileStorageError::Unavailable)?
            .push(operation);
        Ok(())
    }
}

fn file_metadata() -> FileMetadata {
    FileMetadata {
        file_id: "fil_00000000000000000000000001".to_owned(),
        size_bytes: "3".to_owned(),
        sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        content_type: "text/plain".to_owned(),
        created_at_micros: "1".to_owned(),
    }
}

#[async_trait]
impl FileStorage for MockFileStorage {
    async fn create_upload_grant(
        &self,
        request: FileUploadGrantRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<FileUploadGrant, FileStorageError> {
        self.record("create_upload")?;
        if request.max_bytes != 3 || request.content_type.as_deref() != Some("text/plain") {
            return Err(FileStorageError::InvalidRequest);
        }
        Ok(FileUploadGrant {
            upload_id: "upl_00000000000000000000000002".to_owned(),
            path: "/v1/files/uploads/upl_00000000000000000000000002".to_owned(),
            token: "upload-token".to_owned(),
            expires_at_micros: "2".to_owned(),
            max_bytes: "3".to_owned(),
        })
    }

    async fn store(
        &self,
        request: FileStoreRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<FileMetadata, FileStorageError> {
        self.record("store")?;
        if request.bytes != [1, 2, 3] {
            return Err(FileStorageError::InvalidRequest);
        }
        Ok(file_metadata())
    }

    async fn metadata(
        &self,
        file_id: String,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<FileMetadata, FileStorageError> {
        self.record("metadata")?;
        if file_id != file_metadata().file_id {
            return Err(FileStorageError::NotFound);
        }
        Ok(file_metadata())
    }

    async fn create_download_grant(
        &self,
        request: FileDownloadGrantRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<FileDownloadGrant, FileStorageError> {
        self.record("create_download")?;
        if request.expires_in_micros != "1000" {
            return Err(FileStorageError::InvalidRequest);
        }
        Ok(FileDownloadGrant {
            path: format!("/v1/files/downloads/{}", request.file_id),
            token: "download-token".to_owned(),
            expires_at_micros: "3".to_owned(),
            metadata: file_metadata(),
        })
    }

    async fn get(
        &self,
        _file_id: String,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<FileBytes, FileStorageError> {
        self.record("get")?;
        Ok(FileBytes {
            metadata: file_metadata(),
            bytes: vec![1, 2, 3],
        })
    }

    async fn delete(
        &self,
        _file_id: String,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<(), FileStorageError> {
        self.record("delete")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_file_storage_is_capability_scoped_and_typed() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    let absent = supervisor
        .invoke(request_function(
            "export default (ctx) => typeof ctx.storage === 'undefined';",
            CanonicalValue::Null,
            FunctionType::Action,
            vec![],
        )?)
        .await?;
    assert_eq!(absent, CanonicalValue::Boolean(true));

    let broker = Arc::new(MockFileStorage::default());
    let source = r#"
        export const contract = async (ctx) => {
          const upload = await ctx.storage.createUpload({ maxBytes: 3, contentType: "text/plain" });
          const stored = await ctx.storage.store(new Uint8Array([1, 2, 3]));
          const metadata = await ctx.storage.getMetadata(stored.fileId);
          const download = await ctx.storage.createDownload(stored.fileId, { expiresInMicros: 1000n });
          const loaded = await ctx.storage.get(stored.fileId);
          await ctx.storage.delete(stored.fileId);
          return Object.isFrozen(ctx.storage) && Object.isFrozen(upload)
            && Object.isFrozen(metadata) && Object.isFrozen(download)
            && Object.isFrozen(download.metadata) && loaded.bytes instanceof Uint8Array
            && loaded.bytes[2] === 3;
        };
    "#;
    let output = supervisor
        .invoke(
            request_with_contracts(
                source,
                CanonicalValue::Null,
                FunctionType::Action,
                vec![Capability::FileRead, Capability::FileWrite],
                &Contract::Null,
                &Contract::Boolean,
                &DocumentSchemaV1::new(Vec::new())?,
            )?
            .with_file_storage(broker.clone())?,
        )
        .await?;
    assert_eq!(output, CanonicalValue::Boolean(true));
    assert_eq!(
        *broker
            .operations
            .lock()
            .map_err(|_| "storage operations lock")?,
        [
            "create_upload",
            "store",
            "metadata",
            "create_download",
            "get",
            "delete"
        ]
    );

    let query = request_function(
        "export default () => null;",
        CanonicalValue::Null,
        FunctionType::Query,
        vec![],
    )?;
    assert_eq!(
        query.with_file_storage(broker).map(|_| ()),
        Err(RuntimeError::InvalidInvocation)
    );
    Ok(())
}

#[derive(Debug, Default)]
struct MockScheduler {
    requests: Mutex<Vec<ScheduleRequest>>,
}

#[async_trait]
impl ScheduleCreate for MockScheduler {
    async fn create(
        &self,
        request: ScheduleRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<ScheduledInvocationId, ScheduleError> {
        self.requests
            .lock()
            .map_err(|_| ScheduleError::Unavailable)?
            .push(request);
        Ok(ScheduledInvocationId::generate())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_is_capability_scoped_canonical_and_typed() -> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    let broker = Arc::new(MockScheduler::default());
    let source = r#"
        export default async (ctx) => {
          const after = await ctx.scheduler.runAfter(0n, "jobs.send", { value: 7n }, { idempotencyKey: "send-7" });
          const at = await ctx.scheduler.runAt(1700000000000000n, "jobs.cleanup", null);
          return [after, at, Object.isFrozen(ctx.scheduler)];
        };
    "#;
    let output = supervisor
        .invoke(
            request_function(
                source,
                CanonicalValue::Null,
                FunctionType::Action,
                vec![Capability::SchedulerCreate],
            )?
            .with_scheduler(broker.clone())?,
        )
        .await?;
    let CanonicalValue::Array(values) = output else {
        return Err("expected scheduler result array".into());
    };
    assert_eq!(values.len(), 3);
    assert_eq!(values[2], CanonicalValue::Boolean(true));
    {
        let requests = broker.requests.lock().map_err(|_| "scheduler lock")?;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].function.as_str(), "jobs.send");
        assert_eq!(requests[0].time, ScheduleTime::AfterMicros(0));
        assert_eq!(requests[0].idempotency_key.as_deref(), Some("send-7"));
        assert_eq!(
            requests[1].time,
            ScheduleTime::At(TimestampMicros::new(1_700_000_000_000_000))
        );
    }

    let unavailable = request_function(
        "export default async (ctx) => ctx.scheduler.runAfter(0, 'jobs.send', null);",
        CanonicalValue::Null,
        FunctionType::Mutation,
        vec![Capability::SchedulerCreate],
    )?;
    assert_eq!(
        supervisor.invoke(unavailable).await,
        Err(RuntimeError::JavaScript)
    );
    let query = request_function(
        "export default () => null;",
        CanonicalValue::Null,
        FunctionType::Query,
        vec![],
    )?;
    assert_eq!(
        query.with_scheduler(broker).map(|_| ()),
        Err(RuntimeError::InvalidInvocation)
    );
    Ok(())
}

#[derive(Debug, Default)]
struct MockFunctionBroker {
    requests: Mutex<Vec<FunctionCallRequest>>,
}

#[derive(Debug, Default)]
struct RecordingLogSink {
    events: Mutex<Vec<OperationalEventV1>>,
}

impl RecordingLogSink {
    fn snapshot(&self) -> Result<Vec<OperationalEventV1>, Box<dyn Error>> {
        Ok(self.events.lock().map_err(|_| "log sink lock")?.clone())
    }
}

impl OperationalLogSink for RecordingLogSink {
    fn try_emit(&self, event: OperationalEventV1) -> Result<(), LogSinkError> {
        self.events
            .lock()
            .map_err(|_| LogSinkError::Unavailable)?
            .push(event);
        Ok(())
    }
}

#[derive(Debug)]
struct FullLogSink;

impl OperationalLogSink for FullLogSink {
    fn try_emit(&self, _event: OperationalEventV1) -> Result<(), LogSinkError> {
        Err(LogSinkError::Full)
    }
}

struct RuntimeFunctionBroker {
    supervisor: RuntimeSupervisor,
    root: InvocationRequest,
    child_id: FunctionId,
}

impl std::fmt::Debug for RuntimeFunctionBroker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeFunctionBroker")
            .field("child_id", &self.child_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl FunctionInvoke for RuntimeFunctionBroker {
    async fn invoke(
        &self,
        request: FunctionCallRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CanonicalValue, FunctionCallError> {
        if request.kind != FunctionCallKind::Query || cancellation.is_cancelled() {
            return Err(FunctionCallError::Denied);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let child = self
            .root
            .nested_child(self.child_id, request.arguments, remaining)
            .map_err(|_| FunctionCallError::LimitExceeded)?;
        self.supervisor
            .invoke_nested(child)
            .await
            .map_err(runtime_function_error)
    }
}

fn runtime_function_error(error: RuntimeError) -> FunctionCallError {
    match error {
        RuntimeError::Busy => FunctionCallError::Busy,
        RuntimeError::Unavailable | RuntimeError::Internal => FunctionCallError::Unavailable,
        RuntimeError::DeadlineExceeded => FunctionCallError::Timeout,
        RuntimeError::Cancelled => FunctionCallError::Cancelled,
        RuntimeError::InvalidInvocation => FunctionCallError::LimitExceeded,
        RuntimeError::FunctionNotFound => FunctionCallError::NotFound,
        RuntimeError::InvalidArguments => FunctionCallError::InvalidRequest,
        RuntimeError::InvalidConfiguration
        | RuntimeError::UnsupportedRuntime
        | RuntimeError::InvalidArtifact
        | RuntimeError::HeapLimitExceeded
        | RuntimeError::JavaScript
        | RuntimeError::InvalidResult => FunctionCallError::Execution,
    }
}

#[async_trait]
impl FunctionInvoke for MockFunctionBroker {
    async fn invoke(
        &self,
        request: FunctionCallRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CanonicalValue, FunctionCallError> {
        if cancellation.is_cancelled() {
            return Err(FunctionCallError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(FunctionCallError::Timeout);
        }
        let value = request.arguments.clone();
        self.requests
            .lock()
            .map_err(|_| FunctionCallError::Unavailable)?
            .push(request);
        Ok(value)
    }
}

#[derive(Debug)]
struct FixedFunctionError(FunctionCallError);

#[async_trait]
impl FunctionInvoke for FixedFunctionError {
    async fn invoke(
        &self,
        _request: FunctionCallRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<CanonicalValue, FunctionCallError> {
        Err(self.0)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_function_ops_are_capability_scoped_canonical_and_typed()
-> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    let broker = Arc::new(MockFunctionBroker::default());
    let query = request_function(
        r#"
          export default async (ctx) => {
            if (typeof ctx.runMutation !== "undefined" || typeof ctx.runAction !== "undefined") {
              throw new Error("nested authority leaked");
            }
            const value = await ctx.runQuery("queries.child", { count: 7n });
            return [value, Object.isFrozen(ctx.runQuery)];
          };
        "#,
        CanonicalValue::Null,
        FunctionType::Query,
        vec![Capability::FunctionQuery],
    )?
    .with_functions(broker.clone())?;
    assert_eq!(
        supervisor.invoke(query).await?,
        CanonicalValue::Array(vec![
            CanonicalValue::Object(BTreeMap::from([(
                "count".to_owned(),
                CanonicalValue::Int64(7),
            )])),
            CanonicalValue::Boolean(true),
        ])
    );
    {
        let requests = broker.requests.lock().map_err(|_| "function lock")?;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].kind, FunctionCallKind::Query);
        assert_eq!(requests[0].function.as_str(), "queries.child");
    }

    let unavailable = request_function(
        "export default async (ctx) => ctx.runMutation('mutations.child', null);",
        CanonicalValue::Null,
        FunctionType::Mutation,
        vec![Capability::FunctionMutation],
    )?;
    assert_eq!(
        supervisor.invoke(unavailable).await,
        Err(RuntimeError::JavaScript)
    );
    let no_capability = request_function(
        "export default () => null;",
        CanonicalValue::Null,
        FunctionType::Action,
        Vec::new(),
    )?;
    assert_eq!(
        no_capability.with_functions(broker).map(|_| ()),
        Err(RuntimeError::InvalidInvocation)
    );
    for error in [
        FunctionCallError::Denied,
        FunctionCallError::Busy,
        FunctionCallError::LimitExceeded,
    ] {
        let request = request_function(
            "export default async (ctx) => ctx.runQuery('queries.child', null);",
            CanonicalValue::Null,
            FunctionType::Query,
            vec![Capability::FunctionQuery],
        )?
        .with_functions(Arc::new(FixedFunctionError(error)))?;
        assert_eq!(
            supervisor.invoke(request).await,
            Err(RuntimeError::JavaScript)
        );
    }
    let telemetry = supervisor.telemetry();
    assert_eq!(telemetry.function_calls, 5);
    assert_eq!(telemetry.function_call_succeeded, 1);
    assert_eq!(telemetry.function_call_denied, 1);
    assert_eq!(telemetry.function_call_busy, 1);
    assert_eq!(telemetry.function_call_limited, 1);
    assert_eq!(telemetry.function_call_failed, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn function_logs_are_structured_redacted_bounded_and_best_effort()
-> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?;
    let sink = Arc::new(RecordingLogSink::default());
    let source = r#"
      export default async (ctx) => {
        const frozen = Object.isFrozen(ctx.log) && Object.isFrozen(ctx.log.info);
        let tamperDenied = false;
        try {
          Object.defineProperty(ctx.log, "info", { value: () => undefined });
        } catch (_error) {
          tamperDenied = true;
        }
        await ctx.log.info("order accepted", {
          orderId: "ord_123",
          accessToken: "must-not-survive",
          nested: { password: "also-secret", safe: 73n }
        });
        for (let index = 0; index < 99; index += 1) {
          await ctx.log.debug("bounded");
        }
        let limited = false;
        try {
          await ctx.log.warn("over budget");
        } catch (_error) {
          return [frozen && tamperDenied, true];
        }
        return [frozen, limited];
      };
    "#;
    let invocation = request(
        source,
        CanonicalValue::Null,
        Duration::from_secs(2),
        CancellationToken::new(),
    )?
    .with_operational_logs(sink.clone());
    assert_eq!(
        supervisor.invoke(invocation).await?,
        CanonicalValue::Array(vec![
            CanonicalValue::Boolean(true),
            CanonicalValue::Boolean(true),
        ])
    );

    let events = sink.snapshot()?;
    assert_eq!(events.len(), 102);
    assert_eq!(events[0].kind, LogEventKind::InvocationStarted);
    assert_eq!(events[101].kind, LogEventKind::InvocationCompleted);
    assert_eq!(
        events[101]
            .outcome_code
            .as_ref()
            .map(runku_observability::OutcomeCode::as_str),
        Some("OK")
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.stream == LogStream::Function)
            .count(),
        100
    );
    let CanonicalValue::Object(fields) = events[1].fields.as_ref().ok_or("missing log fields")?
    else {
        return Err("log fields are not an object".into());
    };
    assert_eq!(
        fields.get("accessToken"),
        Some(&CanonicalValue::String("[REDACTED]".to_owned()))
    );
    let Some(CanonicalValue::Object(nested)) = fields.get("nested") else {
        return Err("nested log fields are missing".into());
    };
    assert_eq!(
        nested.get("password"),
        Some(&CanonicalValue::String("[REDACTED]".to_owned()))
    );
    assert_eq!(nested.get("safe"), Some(&CanonicalValue::Int64(73)));
    let telemetry = supervisor.telemetry();
    assert_eq!(telemetry.function_logs_emitted, 100);
    assert_eq!(telemetry.function_logs_limited, 1);
    assert_eq!(telemetry.function_logs_dropped, 0);
    assert_eq!(telemetry.platform_logs_dropped, 0);

    let full_supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 2).build()?)?;
    let full = request_function(
        "export default async (ctx) => { await ctx.log.error('dropped'); return 7n; }",
        CanonicalValue::Null,
        FunctionType::Query,
        vec![],
    )?
    .with_operational_logs(Arc::new(FullLogSink));
    assert_eq!(
        full_supervisor.invoke(full).await?,
        CanonicalValue::Int64(7)
    );
    let full_telemetry = full_supervisor.telemetry();
    assert_eq!(full_telemetry.function_logs_dropped, 1);
    assert_eq!(full_telemetry.platform_logs_dropped, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_lifecycle_inherits_sink_parent_request_and_dev_revision()
-> Result<(), Box<dyn Error>> {
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?;
    let sink = Arc::new(RecordingLogSink::default());
    let (root, child_id) = nested_query_request_with_child(
        "export default async (ctx, args) => { await ctx.log.info('child'); return args; }\n",
    )?;
    let root = root
        .with_pinned_code(PinnedCode::DevRevision(DevRevisionId::generate()))?
        .with_operational_logs(sink.clone());
    let root_invocation = root.invocation_id();
    let root_request = root.request_id();
    let broker = Arc::new(RuntimeFunctionBroker {
        supervisor: supervisor.clone(),
        root: root.clone(),
        child_id,
    });
    assert_eq!(
        supervisor.invoke(root.with_functions(broker)?).await?,
        CanonicalValue::Int64(73)
    );
    let events = sink.snapshot()?;
    assert_eq!(events.len(), 5);
    let child_started = events
        .iter()
        .find(|event| {
            event.kind == LogEventKind::InvocationStarted
                && event.parent_invocation_id == Some(root_invocation)
        })
        .ok_or("missing child lifecycle")?;
    assert_eq!(child_started.request_id, root_request);
    assert!(child_started.dev_revision_id.is_some());
    assert_ne!(child_started.invocation_id, root_invocation);
    assert!(events.iter().all(|event| event.request_id == root_request));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_execution_cannot_deadlock_the_single_primary_worker_and_is_bounded()
-> Result<(), Box<dyn Error>> {
    let limits = RuntimeLimits::builder(1, 2)
        .max_nested_concurrency(1)
        .max_nested_depth(1)
        .max_nested_calls(1)
        .build()?;
    let supervisor = RuntimeSupervisor::start(limits)?;
    let (root, child_id) = nested_query_request()?;
    let dev_revision = DevRevisionId::generate();
    let dev_root = root
        .clone()
        .with_pinned_code(PinnedCode::DevRevision(dev_revision))?;
    let derived = dev_root.nested_child(child_id, CanonicalValue::Null, Duration::from_secs(1))?;
    assert_eq!(derived.scope(), dev_root.scope());
    assert_eq!(derived.release_id(), dev_root.release_id());
    assert_eq!(derived.request_id(), dev_root.request_id());
    assert_eq!(derived.pinned_code(), PinnedCode::DevRevision(dev_revision));
    assert_ne!(derived.invocation_id(), dev_root.invocation_id());
    assert!(Arc::ptr_eq(derived.manifest(), dev_root.manifest()));
    let broker = Arc::new(RuntimeFunctionBroker {
        supervisor: supervisor.clone(),
        root: root.clone(),
        child_id,
    });
    let output = tokio::time::timeout(
        Duration::from_secs(1),
        supervisor.invoke(root.clone().with_functions(broker)?),
    )
    .await??;
    assert_eq!(output, CanonicalValue::Int64(73));

    let exhausted = root.nested_child(child_id, CanonicalValue::Null, Duration::from_secs(1))?;
    assert_eq!(
        supervisor.invoke_nested(exhausted).await,
        Err(RuntimeError::InvalidInvocation)
    );

    let fresh = nested_query_request()?.0;
    let depth_one = fresh.nested_child(child_id, CanonicalValue::Null, Duration::from_secs(1))?;
    let depth_two =
        depth_one.nested_child(child_id, CanonicalValue::Null, Duration::from_secs(1))?;
    assert_eq!(
        supervisor.invoke_nested(depth_two).await,
        Err(RuntimeError::InvalidInvocation)
    );
    let telemetry = supervisor.telemetry();
    assert_eq!(telemetry.nested_admitted, 1);
    assert_eq!(telemetry.nested_succeeded, 1);
    assert_eq!(telemetry.nested_failed, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_child_revalidates_contracts_and_exact_artifact_bytes() -> Result<(), Box<dyn Error>>
{
    let supervisor = RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?;
    for (argument_expression, tamper_template, expected) in [
        ("'ok'", false, Ok(CanonicalValue::String("ok".to_owned()))),
        ("null", false, Err(RuntimeError::JavaScript)),
        ("'ok'", true, Err(RuntimeError::JavaScript)),
    ] {
        let (parent, template, child_id) =
            nested_contract_requests(argument_expression, tamper_template)?;
        let broker = Arc::new(RuntimeFunctionBroker {
            supervisor: supervisor.clone(),
            root: template,
            child_id,
        });
        assert_eq!(
            supervisor.invoke(parent.with_functions(broker)?).await,
            expected
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_capacity_saturates_without_queue_or_unbounded_threads() -> Result<(), Box<dyn Error>>
{
    let supervisor = RuntimeSupervisor::start(
        RuntimeLimits::builder(1, 2)
            .max_nested_concurrency(1)
            .build()?,
    )?;
    let source = "export default async (ctx) => { while (true) await ctx.cooperate(); };\n";
    let (first_root, child_id) = nested_query_request_with_child(source)?;
    let first_cancellation = first_root.cancellation();
    let first_child =
        first_root.nested_child(child_id, CanonicalValue::Null, Duration::from_secs(1))?;
    let first = tokio::spawn({
        let supervisor = supervisor.clone();
        async move { supervisor.invoke_nested(first_child).await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while supervisor.telemetry().nested_admitted == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let (second_root, second_child_id) = nested_query_request_with_child(source)?;
    let second_child = second_root.nested_child(
        second_child_id,
        CanonicalValue::Null,
        Duration::from_secs(1),
    )?;
    assert_eq!(
        supervisor.invoke_nested(second_child).await,
        Err(RuntimeError::Busy)
    );
    first_cancellation.cancel();
    assert_eq!(first.await?, Err(RuntimeError::Cancelled));
    assert_eq!(supervisor.telemetry().nested_busy, 1);
    Ok(())
}

#[test]
fn limits_and_invocation_validation_fail_before_admission() {
    assert_eq!(
        RuntimeLimits::builder(0, 1).build(),
        Err(RuntimeError::InvalidConfiguration)
    );
    assert_eq!(
        RuntimeLimits::builder(1, 0).build(),
        Err(RuntimeError::InvalidConfiguration)
    );
    assert_eq!(
        RuntimeLimits::builder(1, 1).heap_bytes(1024).build(),
        Err(RuntimeError::InvalidConfiguration)
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(cancellation.is_cancelled());
}

fn request(
    source: &str,
    arguments: CanonicalValue,
    timeout: Duration,
    cancellation: CancellationToken,
) -> Result<InvocationRequest, Box<dyn Error>> {
    request_options(
        source,
        arguments,
        timeout,
        cancellation,
        "platform-js-1",
        RuntimeClass::SafeV8,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn request_options(
    source: &str,
    arguments: CanonicalValue,
    timeout: Duration,
    cancellation: CancellationToken,
    runtime_version: &str,
    runtime_class: RuntimeClass,
    tamper_artifact: bool,
) -> Result<InvocationRequest, Box<dyn Error>> {
    request_function_options(
        source,
        arguments,
        timeout,
        cancellation,
        runtime_version,
        runtime_class,
        tamper_artifact,
        FunctionType::Query,
        vec![],
    )
}

fn request_function(
    source: &str,
    arguments: CanonicalValue,
    function_type: FunctionType,
    capabilities: Vec<Capability>,
) -> Result<InvocationRequest, Box<dyn Error>> {
    request_function_options(
        source,
        arguments,
        Duration::from_secs(2),
        CancellationToken::new(),
        "platform-js-1",
        RuntimeClass::SafeV8,
        false,
        function_type,
        capabilities,
    )
}

#[allow(clippy::too_many_arguments)]
fn request_function_options(
    source: &str,
    arguments: CanonicalValue,
    timeout: Duration,
    cancellation: CancellationToken,
    runtime_version: &str,
    runtime_class: RuntimeClass,
    tamper_artifact: bool,
    function_type: FunctionType,
    capabilities: Vec<Capability>,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let mut encoded_artifact = encode_safe_esm_bundle(&bundle)?;
    if tamper_artifact {
        let last = encoded_artifact
            .last_mut()
            .ok_or("encoded artifact unexpectedly empty")?;
        *last ^= 1;
    }
    let artifact_bytes: Arc<[u8]> = encoded_artifact.into();
    let implementation_hash = Sha256Digest::of(source.as_bytes());
    let project_id = ProjectId::from_ulid(Ulid::from(2_u128));
    let release_id = ReleaseId::from_ulid(Ulid::from(1_u128));
    let function_id = FunctionId::from_ulid(Ulid::from(4_u128));
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id,
        build_id: BuildId::from_ulid(Ulid::from(3_u128)),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: runtime_version.parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([2; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([3; 32]),
        index_contract_hash: Sha256Digest::from_bytes([4; 32]),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "tests.run".parse()?,
            function_type,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class,
            implementation_hash,
            arguments_contract_hash: Sha256Digest::from_bytes([5; 32]),
            result_contract_hash: Sha256Digest::from_bytes([6; 32]),
            capabilities,
        }],
        cron_definitions: Vec::new(),
    };
    InvocationRequest::new(
        EnvironmentScope::new(project_id, EnvironmentId::from_ulid(Ulid::from(7_u128))),
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        function_id,
        Arc::new(manifest),
        artifact_bytes,
        arguments,
        timeout,
        cancellation,
    )
    .map_err(Into::into)
}

fn nested_query_request() -> Result<(InvocationRequest, FunctionId), Box<dyn Error>> {
    nested_query_request_with_child("export default (_ctx, args) => args;\n")
}

fn nested_query_request_with_child(
    child_source: &str,
) -> Result<(InvocationRequest, FunctionId), Box<dyn Error>> {
    let parent_source = r#"
      export default async (ctx) => {
        return await ctx.runQuery("tests.child", 73n);
      };
    "#;
    let bundle = SafeEsmBundleV1::from_sources([parent_source, child_source])?;
    let artifact_bytes: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let project_id = ProjectId::from_ulid(Ulid::from(91_u128));
    let release_id = ReleaseId::from_ulid(Ulid::from(92_u128));
    let parent_id = FunctionId::from_ulid(Ulid::from(93_u128));
    let child_id = FunctionId::from_ulid(Ulid::from(94_u128));
    let function = |id, name: &str, source: &str, capabilities| -> Result<_, Box<dyn Error>> {
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
            capabilities,
        })
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id,
        build_id: BuildId::from_ulid(Ulid::from(95_u128)),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([2; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([3; 32]),
        index_contract_hash: Sha256Digest::from_bytes([4; 32]),
        functions: vec![
            function(child_id, "tests.child", child_source, vec![])?,
            function(
                parent_id,
                "tests.parent",
                parent_source,
                vec![Capability::FunctionQuery],
            )?,
        ],
        cron_definitions: Vec::new(),
    };
    let request = InvocationRequest::new(
        EnvironmentScope::new(project_id, EnvironmentId::from_ulid(Ulid::from(96_u128))),
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        parent_id,
        Arc::new(manifest),
        artifact_bytes,
        CanonicalValue::Null,
        Duration::from_secs(2),
        CancellationToken::new(),
    )?;
    Ok((request, child_id))
}

fn nested_contract_requests(
    argument_expression: &str,
    tamper_template: bool,
) -> Result<(InvocationRequest, InvocationRequest, FunctionId), Box<dyn Error>> {
    let parent_source = format!(
        "export const parent = async (ctx) => ctx.runQuery('tests.child', {argument_expression});\n"
    );
    let child_source = "export const child = (_ctx, value) => value;\n";
    let null_bytes = encode_contract(&Contract::Null)?;
    let text_contract = Contract::String {
        minimum_bytes: Some(1),
        maximum_bytes: Some(8),
    };
    let text_bytes = encode_contract(&text_contract)?;
    let schema_bytes = encode_document_schema(&DocumentSchemaV1::new(Vec::new())?)?;
    let project_id = ProjectId::from_ulid(Ulid::from(101_u128));
    let index_bytes = encode_schema_catalog(&SchemaCatalog::new(project_id, Vec::new())?)?;
    let resources = [
        parent_source.clone(),
        child_source.to_owned(),
        String::from_utf8(null_bytes.clone())?,
        String::from_utf8(text_bytes.clone())?,
        String::from_utf8(schema_bytes.clone())?,
        String::from_utf8(index_bytes.clone())?,
    ];
    let bundle = SafeEsmBundleV1::from_sources(resources)?;
    let encoded = encode_safe_esm_bundle(&bundle)?;
    let artifact: Arc<[u8]> = encoded.clone().into();
    let mut tampered = encoded;
    if tamper_template {
        let last = tampered.last_mut().ok_or("artifact unexpectedly empty")?;
        *last ^= 1;
    }
    let template_artifact: Arc<[u8]> = tampered.into();
    let release_id = ReleaseId::from_ulid(Ulid::from(102_u128));
    let child_id = FunctionId::from_ulid(Ulid::from(103_u128));
    let parent_id = FunctionId::from_ulid(Ulid::from(104_u128));
    let text_hash = Sha256Digest::of(&text_bytes);
    let manifest = Arc::new(ReleaseManifestV1 {
        release_id,
        project_id,
        build_id: BuildId::from_ulid(Ulid::from(105_u128)),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "runku-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([2; 32]),
        schema_contract_hash: Sha256Digest::of(&schema_bytes),
        index_contract_hash: Sha256Digest::of(&index_bytes),
        functions: vec![
            FunctionManifest {
                id: child_id,
                name: "tests.child".parse()?,
                function_type: FunctionType::Query,
                visibility: FunctionVisibility::Internal,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(child_source.as_bytes()),
                arguments_contract_hash: text_hash,
                result_contract_hash: text_hash,
                capabilities: Vec::new(),
            },
            FunctionManifest {
                id: parent_id,
                name: "tests.parent".parse()?,
                function_type: FunctionType::Query,
                visibility: FunctionVisibility::Public,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(parent_source.as_bytes()),
                arguments_contract_hash: Sha256Digest::of(&null_bytes),
                result_contract_hash: text_hash,
                capabilities: vec![Capability::FunctionQuery],
            },
        ],
        cron_definitions: Vec::new(),
    });
    let scope = EnvironmentScope::new(project_id, EnvironmentId::from_ulid(Ulid::from(106_u128)));
    let make_request = |bytes| {
        InvocationRequest::new(
            scope,
            release_id,
            RequestId::generate(),
            InvocationId::generate(),
            parent_id,
            Arc::clone(&manifest),
            bytes,
            CanonicalValue::Null,
            Duration::from_secs(2),
            CancellationToken::new(),
        )
    };
    Ok((
        make_request(artifact)?,
        make_request(template_artifact)?,
        child_id,
    ))
}

#[allow(clippy::too_many_arguments)]
fn request_with_contracts(
    source: &str,
    arguments: CanonicalValue,
    function_type: FunctionType,
    capabilities: Vec<Capability>,
    arguments_contract: &Contract,
    result_contract: &Contract,
    schema: &DocumentSchemaV1,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let arguments_bytes = encode_contract(arguments_contract)?;
    let result_bytes = encode_contract(result_contract)?;
    let schema_bytes = encode_document_schema(schema)?;
    let project_id = ProjectId::from_ulid(Ulid::from(82_u128));
    let index_bytes = encode_schema_catalog(&SchemaCatalog::new(project_id, Vec::new())?)?;
    let resources = [
        source.to_owned(),
        String::from_utf8(arguments_bytes.clone())?,
        String::from_utf8(result_bytes.clone())?,
        String::from_utf8(schema_bytes.clone())?,
        String::from_utf8(index_bytes.clone())?,
    ];
    let bundle = SafeEsmBundleV1::from_sources(resources)?;
    let artifact_bytes: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let release_id = ReleaseId::from_ulid(Ulid::from(83_u128));
    let function_id = FunctionId::from_ulid(Ulid::from(84_u128));
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id,
        build_id: BuildId::from_ulid(Ulid::from(85_u128)),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: if capabilities
            .iter()
            .any(|capability| matches!(capability, Capability::FileRead | Capability::FileWrite))
        {
            "runku-js-2"
        } else {
            "runku-js-1"
        }
        .parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([2; 32]),
        schema_contract_hash: Sha256Digest::of(&schema_bytes),
        index_contract_hash: Sha256Digest::of(&index_bytes),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "tests.contract".parse()?,
            function_type,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::of(&arguments_bytes),
            result_contract_hash: Sha256Digest::of(&result_bytes),
            capabilities,
        }],
        cron_definitions: Vec::new(),
    };
    InvocationRequest::new(
        EnvironmentScope::new(project_id, EnvironmentId::from_ulid(Ulid::from(86_u128))),
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        function_id,
        Arc::new(manifest),
        artifact_bytes,
        arguments,
        Duration::from_secs(2),
        CancellationToken::new(),
    )
    .map_err(Into::into)
}

fn complex_value() -> Result<CanonicalValue, Box<dyn Error>> {
    let mut nested = BTreeMap::new();
    nested.insert(
        "__proto__".to_owned(),
        CanonicalValue::String("safe".to_owned()),
    );
    nested.insert("boolean".to_owned(), CanonicalValue::Boolean(true));
    nested.insert("bytes".to_owned(), CanonicalValue::Bytes(vec![0, 1, 255]));
    nested.insert(
        "float".to_owned(),
        CanonicalValue::Float64(FiniteF64::new(1.25)?),
    );
    nested.insert("int".to_owned(), CanonicalValue::Int64(i64::MIN));
    nested.insert("null".to_owned(), CanonicalValue::Null);
    nested.insert(
        "timestamp".to_owned(),
        CanonicalValue::Timestamp(TimestampMicros::new(-123_456_789)),
    );
    nested.insert(
        "typedId".to_owned(),
        CanonicalValue::TypedId("document_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse::<TypedId>()?),
    );
    Ok(CanonicalValue::Object(nested))
}
