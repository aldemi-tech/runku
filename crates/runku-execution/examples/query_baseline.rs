//! Reproducible local Query Executor baseline over durable `SQLite`.

use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use runku_core::{
    BuildId, DocumentId, EnvironmentId, EnvironmentScope, FunctionId, InvocationId, OperationId,
    ProjectId, ReleaseId, RequestId, TableId,
};
use runku_data::{CommitBatch, DocumentMutation, ExpectedRevision, LogicalStore};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::QueryExecutor;
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_safe_esm_bundle,
};
use runku_runtime::{CancellationToken, InvocationRequest, RuntimeLimits, RuntimeSupervisor};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::TempDir;

struct Fixture {
    scope: EnvironmentScope,
    release_id: ReleaseId,
    function_id: FunctionId,
    manifest: Arc<ReleaseManifestV1>,
    artifact: Arc<[u8]>,
}

fn main() -> Result<(), Box<dyn Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("query-baseline.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table_id = TableId::generate();
    let document_id = DocumentId::generate();
    seed(store.as_ref(), scope, table_id, document_id).await?;
    let limits = RuntimeLimits::builder(2, 32)
        .max_wall_time(Duration::from_secs(2))
        .build()?;
    let executor = QueryExecutor::new(RuntimeSupervisor::start(limits)?, store);
    let pure = fixture(scope, "export default () => true;", Capability::DbRead)?;
    let read = fixture(
        scope,
        &format!(
            "export default async (ctx) => (await ctx.db.get('{table_id}', '{document_id}')).value;"
        ),
        Capability::DbRead,
    )?;

    let cold_started = Instant::now();
    executor.execute(read.request()?).await?;
    let first_query_micros = cold_started.elapsed().as_micros();

    let pure_started = Instant::now();
    for _ in 0..100 {
        executor.execute(pure.request()?).await?;
    }
    let pure_100_total_micros = pure_started.elapsed().as_micros();

    let read_started = Instant::now();
    for _ in 0..100 {
        executor.execute(read.request()?).await?;
    }
    let sqlite_get_100_total_micros = read_started.elapsed().as_micros();
    let telemetry = executor.telemetry();

    println!("first_query_sqlite_get_micros={first_query_micros}");
    println!("pure_queries=100");
    println!("pure_100_total_micros={pure_100_total_micros}");
    println!("sqlite_get_queries=100");
    println!("sqlite_get_100_total_micros={sqlite_get_100_total_micros}");
    println!("point_reads={}", telemetry.point_reads);
    println!("dependencies={}", telemetry.dependencies);
    Ok(())
}

async fn seed(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    table_id: TableId,
    document_id: DocumentId,
) -> Result<(), Box<dyn Error>> {
    let mut batch = CommitBatch::new(scope, OperationId::generate());
    batch.push_document(DocumentMutation::Upsert {
        table_id,
        document_id,
        expected: ExpectedRevision::Absent,
        value: CanonicalValue::String("Ada".to_owned()),
    });
    store.commit(&batch).await?;
    Ok(())
}

impl Fixture {
    fn request(&self) -> Result<InvocationRequest, Box<dyn Error>> {
        Ok(InvocationRequest::new(
            self.scope,
            self.release_id,
            RequestId::generate(),
            InvocationId::generate(),
            self.function_id,
            Arc::clone(&self.manifest),
            Arc::clone(&self.artifact),
            CanonicalValue::Null,
            Duration::from_secs(1),
            CancellationToken::new(),
        )?)
    }
}

fn fixture(
    scope: EnvironmentScope,
    source: &str,
    capability: Capability,
) -> Result<Fixture, Box<dyn Error>> {
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
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "benchmark.query".parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Internal,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
            result_contract_hash: Sha256Digest::from_bytes([5; 32]),
            capabilities: vec![capability],
        }],
        cron_definitions: Vec::new(),
    };
    Ok(Fixture {
        scope,
        release_id,
        function_id,
        manifest: Arc::new(manifest),
        artifact,
    })
}
