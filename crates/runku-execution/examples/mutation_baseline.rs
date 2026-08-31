//! Reproducible local Mutation Executor baseline over durable `SQLite`.

use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use runku_core::{
    BuildId, DocumentId, EnvironmentId, EnvironmentScope, FunctionId, InvocationId, OperationId,
    ProjectId, ReleaseId, RequestId, TableId,
};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::MutationExecutor;
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
            directory.path().join("mutation-baseline.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let limits = RuntimeLimits::builder(2, 32)
        .max_wall_time(Duration::from_secs(2))
        .build()?;
    let executor = MutationExecutor::new(RuntimeSupervisor::start(limits)?, store);
    let no_op = fixture(scope, "export default (_ctx, value) => value;")?;
    let insert = fixture(
        scope,
        &format!(
            "export default async (ctx, documentId) => {{ await ctx.db.insert('{table}', documentId, 'value'); return documentId; }};"
        ),
    )?;

    let cold_started = Instant::now();
    executor
        .execute(
            insert.request(CanonicalValue::String(DocumentId::generate().to_string()))?,
            OperationId::generate(),
        )
        .await?;
    let first_commit_micros = cold_started.elapsed().as_micros();

    let no_op_started = Instant::now();
    for _ in 0..100 {
        executor
            .execute(
                no_op.request(CanonicalValue::Null)?,
                OperationId::generate(),
            )
            .await?;
    }
    let no_op_100_total_micros = no_op_started.elapsed().as_micros();

    let commit_started = Instant::now();
    for _ in 0..100 {
        executor
            .execute(
                insert.request(CanonicalValue::String(DocumentId::generate().to_string()))?,
                OperationId::generate(),
            )
            .await?;
    }
    let sqlite_commit_100_total_micros = commit_started.elapsed().as_micros();
    let telemetry = executor.telemetry();

    println!("first_mutation_sqlite_commit_micros={first_commit_micros}");
    println!("no_op_mutations=100");
    println!("no_op_100_total_micros={no_op_100_total_micros}");
    println!("sqlite_commit_mutations=100");
    println!("sqlite_commit_100_total_micros={sqlite_commit_100_total_micros}");
    println!("function_attempts={}", telemetry.function_attempts);
    println!("commit_calls={}", telemetry.commit_calls);
    println!("exact_retries={}", telemetry.exact_retries);
    Ok(())
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

fn fixture(scope: EnvironmentScope, source: &str) -> Result<Fixture, Box<dyn Error>> {
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
            name: "benchmark.mutation".parse()?,
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
