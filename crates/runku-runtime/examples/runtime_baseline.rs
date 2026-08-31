//! Reproducible local Safe Runtime isolate-per-invocation baseline.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use runku_core::{
    BuildId, EnvironmentId, EnvironmentScope, FunctionId, InvocationId, ProjectId, ReleaseId,
    RequestId,
};
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_safe_esm_bundle,
};
use runku_runtime::{
    CancellationToken, HttpsEgress, HttpsError, HttpsRequest, HttpsResponse, InvocationRequest,
    RuntimeLimits, RuntimeSupervisor,
};
use runku_value::{CanonicalValue, TimestampMicros};

struct Fixture {
    scope: EnvironmentScope,
    release_id: ReleaseId,
    function_id: FunctionId,
    manifest: Arc<ReleaseManifestV1>,
    artifact: Arc<[u8]>,
    https: Option<Arc<dyn HttpsEgress>>,
}

#[derive(Debug)]
struct BaselineHttps;

#[async_trait]
impl HttpsEgress for BaselineHttps {
    async fn execute(
        &self,
        request: HttpsRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<HttpsResponse, HttpsError> {
        Ok(HttpsResponse {
            status: 202,
            headers: BTreeMap::from([("content-type".to_owned(), vec!["text/plain".to_owned()])]),
            body: request.body,
        })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let limits = RuntimeLimits::builder(2, 32)
        .max_wall_time(Duration::from_secs(2))
        .build()?;
    let supervisor_started = Instant::now();
    let supervisor = RuntimeSupervisor::start(limits)?;
    let supervisor_start_micros = supervisor_started.elapsed().as_micros();
    let sync = fixture("export default (_ctx, args) => args;")?;
    let asynchronous =
        fixture("export default async (ctx, args) => { await ctx.cooperate(); return args; };")?;
    let looping = fixture("export default () => { while (true) {} };")?;
    let action = action_fixture(
        "export default async (ctx) => { const response = await ctx.https.request({ method: 'POST', url: 'https://api.example.com/events', body: new Uint8Array([1, 2, 3]), idempotencyKey: 'baseline_1' }); return response.body; };",
    )?;

    let cold_started = Instant::now();
    supervisor
        .invoke(sync.request(Duration::from_secs(1))?)
        .await?;
    let first_invoke_micros = cold_started.elapsed().as_micros();

    let sync_started = Instant::now();
    for _ in 0..100 {
        supervisor
            .invoke(sync.request(Duration::from_secs(1))?)
            .await?;
    }
    let sync_100_total_micros = sync_started.elapsed().as_micros();

    let async_started = Instant::now();
    for _ in 0..100 {
        supervisor
            .invoke(asynchronous.request(Duration::from_secs(1))?)
            .await?;
    }
    let async_100_total_micros = async_started.elapsed().as_micros();

    let https_started = Instant::now();
    for _ in 0..100 {
        supervisor
            .invoke(action.request(Duration::from_secs(1))?)
            .await?;
    }
    let action_https_100_total_micros = https_started.elapsed().as_micros();

    let timeout_started = Instant::now();
    for _ in 0..10 {
        let result = supervisor
            .invoke(looping.request(Duration::from_millis(5))?)
            .await;
        if result.is_ok() {
            return Err("looping invocation unexpectedly completed".into());
        }
    }
    let timeout_10_total_micros = timeout_started.elapsed().as_micros();

    println!("supervisor_start_micros={supervisor_start_micros}");
    println!("first_invoke_micros={first_invoke_micros}");
    println!("sync_invocations=100");
    println!("sync_100_total_micros={sync_100_total_micros}");
    println!("async_invocations=100");
    println!("async_100_total_micros={async_100_total_micros}");
    println!("action_https_mock_invocations=100");
    println!("action_https_mock_100_total_micros={action_https_100_total_micros}");
    println!("timeouts=10");
    println!("timeout_10_total_micros={timeout_10_total_micros}");
    Ok(())
}

impl Fixture {
    fn request(&self, timeout: Duration) -> Result<InvocationRequest, Box<dyn Error>> {
        let request = InvocationRequest::new(
            self.scope,
            self.release_id,
            RequestId::generate(),
            InvocationId::generate(),
            self.function_id,
            Arc::clone(&self.manifest),
            Arc::clone(&self.artifact),
            CanonicalValue::Int64(42),
            timeout,
            CancellationToken::new(),
        )?;
        if let Some(https) = &self.https {
            request.with_https(Arc::clone(https)).map_err(Into::into)
        } else {
            Ok(request)
        }
    }
}

fn fixture(source: &str) -> Result<Fixture, Box<dyn Error>> {
    fixture_with(source, FunctionType::Query, vec![], None)
}

fn action_fixture(source: &str) -> Result<Fixture, Box<dyn Error>> {
    fixture_with(
        source,
        FunctionType::Action,
        vec![Capability::NetworkHttps],
        Some(Arc::new(BaselineHttps)),
    )
}

fn fixture_with(
    source: &str,
    function_type: FunctionType,
    capabilities: Vec<Capability>,
    https: Option<Arc<dyn HttpsEgress>>,
) -> Result<Fixture, Box<dyn Error>> {
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let project_id = ProjectId::generate();
    let release_id = ReleaseId::generate();
    let function_id = FunctionId::generate();
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id,
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![FunctionManifest {
            id: function_id,
            name: "benchmark.run".parse()?,
            function_type,
            visibility: FunctionVisibility::Internal,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
            result_contract_hash: Sha256Digest::from_bytes([5; 32]),
            capabilities,
        }],
        cron_definitions: Vec::new(),
    };
    Ok(Fixture {
        scope: EnvironmentScope::new(project_id, EnvironmentId::generate()),
        release_id,
        function_id,
        manifest: Arc::new(manifest),
        artifact,
        https,
    })
}
