//! Reproducible schema/index Mutation planning baseline over durable `SQLite`.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use runku_core::{
    BuildId, DocumentId, EnvironmentId, EnvironmentScope, FunctionId, IndexId, InvocationId,
    OperationId, ProjectId, ReleaseId, RequestId, TableId,
};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::MutationExecutor;
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_safe_esm_bundle,
};
use runku_runtime::{CancellationToken, InvocationRequest, RuntimeLimits, RuntimeSupervisor};
use runku_schema::{FieldPath, IndexDefinition, SchemaCatalog};
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
            directory.path().join("schema-index-baseline.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let indexes = (0..4)
        .map(|ordinal| {
            IndexDefinition::new(
                IndexId::generate(),
                table,
                format!("by_field_{ordinal}"),
                vec![FieldPath::new(vec![format!("field_{ordinal}")])?],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let catalog = Arc::new(SchemaCatalog::new(scope.project_id(), indexes)?);
    let limits = RuntimeLimits::builder(2, 32)
        .max_wall_time(Duration::from_secs(2))
        .build()?;
    let executor = MutationExecutor::new(RuntimeSupervisor::start(limits)?, store)
        .with_schema_catalog(catalog.clone());
    let source = format!(
        "export default async (ctx, args) => {{ await ctx.db.insert('{table}', args.documentId, args.value); return args.documentId; }};"
    );
    let fixture = fixture(scope, &source, catalog.digest())?;

    let cold_started = Instant::now();
    executor
        .execute(fixture.request(arguments())?, OperationId::generate())
        .await?;
    let first_indexed_commit_micros = cold_started.elapsed().as_micros();

    let started = Instant::now();
    for _ in 0..100 {
        executor
            .execute(fixture.request(arguments())?, OperationId::generate())
            .await?;
    }
    let indexed_commit_100_total_micros = started.elapsed().as_micros();
    let telemetry = executor.telemetry();

    println!("active_indexes=4");
    println!("first_indexed_commit_micros={first_indexed_commit_micros}");
    println!("indexed_mutations=100");
    println!("indexed_commit_100_total_micros={indexed_commit_100_total_micros}");
    println!("commit_calls={}", telemetry.commit_calls);
    println!("index_mutations={}", telemetry.index_mutations);
    Ok(())
}

fn arguments() -> CanonicalValue {
    CanonicalValue::Object(BTreeMap::from([
        (
            "documentId".to_owned(),
            CanonicalValue::String(DocumentId::generate().to_string()),
        ),
        (
            "value".to_owned(),
            CanonicalValue::Object(BTreeMap::from([
                ("field_0".to_owned(), CanonicalValue::Int64(0)),
                ("field_1".to_owned(), CanonicalValue::Int64(1)),
                ("field_2".to_owned(), CanonicalValue::Int64(2)),
                ("field_3".to_owned(), CanonicalValue::Int64(3)),
            ])),
        ),
    ]))
}

impl Fixture {
    fn request(&self, arguments: CanonicalValue) -> Result<InvocationRequest, Box<dyn Error>> {
        Ok(InvocationRequest::new(
            self.scope,
            self.release_id,
            RequestId::generate(),
            InvocationId::generate(),
            self.function_id,
            Arc::clone(&self.manifest),
            Arc::clone(&self.artifact),
            arguments,
            Duration::from_secs(1),
            CancellationToken::new(),
        )?)
    }
}

fn fixture(
    scope: EnvironmentScope,
    source: &str,
    index_hash: [u8; 32],
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
        index_contract_hash: Sha256Digest::from_bytes(index_hash),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "benchmark.schema_index".parse()?,
            function_type: FunctionType::Mutation,
            visibility: FunctionVisibility::Internal,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
            result_contract_hash: Sha256Digest::from_bytes([5; 32]),
            capabilities: vec![Capability::DbWrite],
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
