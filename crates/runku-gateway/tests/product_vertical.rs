//! Real local vertical slice: HTTP → auth → Release/assets → V8 → `SQLite`.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    http::{HeaderValue, Request, StatusCode, header},
};
use futures_util::{SinkExt, StreamExt, future::try_join_all};
use runku_core::{
    ApplicationClientId, BuildId, ChannelName, CodeTarget, CredentialId, DevRevisionId, DocumentId,
    EnvironmentDescriptor, EnvironmentId, EnvironmentScope, FunctionId, InvocationId, OperationId,
    ProjectId, ReleaseId, RequestId, TableId, WorkerId, WorkspaceId, WorkspaceRef,
};
use runku_data::{LogicalStore, OutboxConsumerName};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_development::{
    DevelopmentActor, DevelopmentCommand, DevelopmentContext, DevelopmentRepository,
    DevelopmentRepositoryConfig, DevelopmentRevisionEntry, SqlDevelopmentRepository,
};
use runku_execution::{
    ActionExecutor, MutationExecutor, QueryExecutor, ScheduledInvocationRunner, ScheduledWorker,
    ScheduledWorkerConfig, ScheduledWorkerError, SchedulerClock,
};
use runku_execution_queue::{
    ExecutionAgent, ExecutionAgentConfig, ExecutionClass, ExecutionControlPlane,
    InMemoryExecutionControlPlane, InMemoryExecutionQueue,
};
use runku_gateway::{
    DevelopmentCatalog, GatewayClock, GatewayHttpConfig, PrincipalVerificationError,
    PrincipalVerifier, ProductInvocationConfig, ProductInvocationService, RealtimeGateway,
    RealtimeGatewayConfig, ServingCatalog, build_router, build_router_with_realtime, serve,
};
use runku_identity::{
    ApplicationClient, ApplicationClientStatus, ApplicationCredential,
    ApplicationCredentialResolver, ApplicationIdentityRepository, ApplicationScope,
    AuthenticatedPrincipal, ClientKind, CredentialStatus, KeyringCrypto, ParsedApplicationKey,
    PrincipalEvidence, PrincipalId, PrincipalKind,
};
use runku_identity_repository::{IdentityRepositoryConfig, SqlApplicationIdentityRepository};
use runku_node_runtime::{
    DockerNodeRuntimeConfig, DockerRestrictedNetwork, FullNodeActionRuntime,
    FullNodeExecutionHandler, QueuedNodeRuntime, QueuedNodeRuntimeConfig, ServerNodeRuntimeConfig,
};
use runku_protocol::{
    ActionCallV1, MutationCallV1, PUBLIC_ENVELOPE_MAX_BYTES, QueryCallV1, RealtimeServerMessageV1,
    SuccessMetadataV1, decode_error_v1, decode_realtime_server_v1, decode_success_v1,
    encode_action_call_v1, encode_mutation_call_v1, encode_query_call_v1,
};
use runku_realtime::{
    ChangeDispatcher, DispatcherConfig, RegistryConfig, SubscriptionRegistry, SubscriptionRunner,
};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{
    ArtifactStore, AuthPolicy, Capability, FilesystemArtifactStore, FilesystemStoreRole,
    FullNodeEgressPolicy, FullNodeNetworkMode, FullNodeTcpRule, FunctionManifest, FunctionType,
    FunctionVisibility, NodeOciDescriptorV1, ReleaseCommand, ReleaseManifestV1, ReleaseRepository,
    ReleaseStatus, RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_node_oci_descriptor,
    encode_release_manifest, encode_safe_esm_bundle,
};
use runku_runtime::{
    CancellationToken, InvocationRequest, RuntimeError, RuntimeLimits, RuntimeSupervisor,
};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{
    WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tower::ServiceExt;

const NOW: i64 = 1_800_000_000_000_000;

#[derive(Debug)]
struct TestFailure(&'static str);

impl fmt::Display for TestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestFailure {}

#[derive(Debug)]
struct FixedClock;

impl GatewayClock for FixedClock {
    fn now(&self) -> Result<TimestampMicros, PrincipalVerificationError> {
        Ok(TimestampMicros::new(NOW))
    }
}

#[derive(Debug)]
struct FixedSchedulerClock;

impl SchedulerClock for FixedSchedulerClock {
    fn now(&self) -> Result<TimestampMicros, ScheduledWorkerError> {
        Ok(TimestampMicros::new(NOW + 1))
    }
}

#[derive(Debug)]
struct FixedUserVerifier {
    principal: AuthenticatedPrincipal,
}

#[async_trait]
impl PrincipalVerifier for FixedUserVerifier {
    async fn verify(
        &self,
        _scope: EnvironmentScope,
        token: &str,
        _crypto: &KeyringCrypto,
        _now: TimestampMicros,
    ) -> Result<PrincipalEvidence, PrincipalVerificationError> {
        if token == "valid-user-token" {
            Ok(PrincipalEvidence::Valid(self.principal.clone()))
        } else {
            Err(PrincipalVerificationError::Invalid)
        }
    }
}

struct TestSystem {
    _directory: TempDir,
    _execution_shutdown: tokio::sync::watch::Sender<bool>,
    router: Router,
    service: Arc<ProductInvocationService>,
    catalog: Arc<ServingCatalog>,
    releases: Arc<SqlReleaseRepository>,
    artifacts: Arc<FilesystemArtifactStore>,
    development: Arc<SqlDevelopmentRepository>,
    development_catalog: Arc<DevelopmentCatalog>,
    development_context: DevelopmentContext,
    store: Arc<SqliteStore>,
    scope: EnvironmentScope,
    release_id: ReleaseId,
    channel: ChannelName,
    workspace: WorkspaceRef,
    dev_revision_id: DevRevisionId,
    table_id: TableId,
    document_id: DocumentId,
    service_key: String,
    wrong_scope_key: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn workspace_refresh_changes_only_new_invocations() -> Result<(), Box<dyn Error>> {
    let system = setup().await?;
    assert_eq!(
        invoke_workspace_query(
            system.router.clone(),
            system.workspace.clone(),
            &system.service_key,
        )
        .await?,
        system.release_id
    );
    let current = system
        .development
        .snapshot(system.development_context)
        .await?
        .resolve(&system.workspace)?;
    let mut next_manifest = current.manifest;
    let next_release = ReleaseId::generate();
    next_manifest.release_id = next_release;
    next_manifest.build_id = BuildId::generate();
    next_manifest.created_at = TimestampMicros::new(NOW + 1);
    let next_bytes = encode_release_manifest(&next_manifest)?;
    system
        .development
        .apply(
            system.development_context,
            OperationId::generate(),
            &DevelopmentCommand::PublishRevision {
                workspace_ref: system.workspace.clone(),
                expected_head: Some(system.dev_revision_id),
                revision: DevelopmentRevisionEntry {
                    revision_id: DevRevisionId::generate(),
                    release_id: next_release,
                    manifest_digest: Sha256Digest::of(&next_bytes),
                    manifest_bytes: next_bytes,
                    actor: "local".parse()?,
                    created_at: TimestampMicros::new(NOW + 1),
                },
            },
        )
        .await?;
    assert_eq!(
        invoke_workspace_query(
            system.router.clone(),
            system.workspace.clone(),
            &system.service_key,
        )
        .await?,
        system.release_id
    );
    assert!(matches!(
        system.development_catalog.refresh().await?,
        runku_gateway::ServingRefresh::Published { revision: 3 }
    ));
    assert_eq!(
        invoke_workspace_query(system.router, system.workspace, &system.service_key).await?,
        next_release
    );
    Ok(())
}

async fn invoke_workspace_query(
    router: Router,
    workspace: WorkspaceRef,
    application_key: &str,
) -> Result<ReleaseId, Box<dyn Error>> {
    let call = QueryCallV1 {
        target: CodeTarget::Workspace(workspace),
        function: "queries.me".parse()?,
        arguments: CanonicalValue::Null,
    };
    let mut request = json_post("/v1/query", encode_query_call_v1(&call)?)?;
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, "Bearer valid-user-token".parse()?);
    request
        .headers_mut()
        .insert("x-runku-key", application_key.parse()?);
    let response = router.oneshot(request).await?;
    Ok(decode_success_v1(&response_body(response).await?)?.release_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_vertical_pins_dev_revision_for_scheduling() -> Result<(), Box<dyn Error>> {
    let system = setup().await?;
    let action = ActionCallV1 {
        target: CodeTarget::Workspace(system.workspace.clone()),
        function: "actions.schedule".parse()?,
        arguments: CanonicalValue::String("schedule".to_owned()),
    };
    let mut request = json_post("/v1/action", encode_action_call_v1(&action)?)?;
    request
        .headers_mut()
        .insert("x-runku-key", system.service_key.parse()?);
    let response = system.router.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let success = decode_success_v1(&response_body(response).await?)?;
    assert_eq!(success.release_id, system.release_id);
    assert_eq!(
        success.metadata,
        SuccessMetadataV1::Action {
            schedules_created: 1
        }
    );
    let CanonicalValue::String(scheduled_id) = success.result else {
        return Err(TestFailure("workspace Action did not return schedule ID").into());
    };
    let mut snapshot = system.store.begin_read(system.scope).await?;
    let scheduled = snapshot
        .get_scheduled(scheduled_id.parse()?)
        .await?
        .ok_or(TestFailure("workspace schedule was not persisted"))?;
    snapshot.close().await?;
    assert_eq!(
        scheduled.pinned_code,
        runku_data::PinnedCode::DevRevision(system.dev_revision_id)
    );
    let logical_store: Arc<dyn LogicalStore> = system.store.clone();
    let runner: Arc<dyn ScheduledInvocationRunner> = system.service.clone();
    let worker = ScheduledWorker::with_clock(
        logical_store,
        runner,
        Arc::new(FixedSchedulerClock),
        WorkerId::generate(),
        ScheduledWorkerConfig::PRODUCTION,
    )?;
    let outcome = worker.poll_once(system.scope).await?;
    assert_eq!(outcome.claimed, 1);
    assert_eq!(outcome.succeeded, 1);
    let mut snapshot = system.store.begin_read(system.scope).await?;
    let completed = snapshot
        .get_scheduled(scheduled.id)
        .await?
        .ok_or(TestFailure("completed schedule disappeared"))?;
    snapshot.close().await?;
    assert_eq!(completed.status, runku_data::ScheduleStatus::Succeeded);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_mutation_has_distinct_operation_identity() -> Result<(), Box<dyn Error>> {
    let system = setup().await?;
    let arguments = object([
        (
            "documentId",
            CanonicalValue::String(system.document_id.to_string()),
        ),
        (
            "tableId",
            CanonicalValue::String(system.table_id.to_string()),
        ),
        ("value", CanonicalValue::Int64(91)),
    ]);
    let action = ActionCallV1 {
        target: CodeTarget::Workspace(system.workspace.clone()),
        function: "actions.scheduleMutation".parse()?,
        arguments,
    };
    let mut request = json_post("/v1/action", encode_action_call_v1(&action)?)?;
    request
        .headers_mut()
        .insert("x-runku-key", system.service_key.parse()?);
    let response = system.router.oneshot(request).await?;
    let status = response.status();
    let body = response_body(response).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "schedule request failed: {:?}",
        decode_error_v1(&body)
    );

    let logical_store: Arc<dyn LogicalStore> = system.store.clone();
    let runner: Arc<dyn ScheduledInvocationRunner> = system.service.clone();
    let worker = ScheduledWorker::with_clock(
        logical_store,
        runner,
        Arc::new(FixedSchedulerClock),
        WorkerId::generate(),
        ScheduledWorkerConfig::PRODUCTION,
    )?;
    let outcome = worker.poll_once(system.scope).await?;
    assert_eq!(outcome.claimed, 1);
    assert_eq!(outcome.succeeded, 1);
    assert_eq!(outcome.failed, 0);

    let mut snapshot = system.store.begin_read(system.scope).await?;
    let document = snapshot
        .get_document(system.table_id, system.document_id)
        .await?
        .ok_or(TestFailure(
            "scheduled Mutation did not persist its document",
        ))?;
    snapshot.close().await?;
    assert_eq!(document.value, CanonicalValue::Int64(91));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn real_vertical_executes_all_kinds_auth_and_mutation_replay() -> Result<(), Box<dyn Error>> {
    let system = setup().await?;

    let query = QueryCallV1 {
        target: CodeTarget::Channel(system.channel.clone()),
        function: "queries.me".parse()?,
        arguments: CanonicalValue::String("hello".to_owned()),
    };
    let mut request = json_post("/v1/query", encode_query_call_v1(&query)?)?;
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, "Bearer valid-user-token".parse()?);
    request
        .headers_mut()
        .insert("x-runku-key", system.service_key.parse()?);
    let response = system.router.clone().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let success = decode_success_v1(&response_body(response).await?)?;
    assert_eq!(success.release_id, system.release_id);
    assert_eq!(
        success.result,
        object([
            ("argument", CanonicalValue::String("hello".to_owned()),),
            ("kind", CanonicalValue::String("user".to_owned())),
        ])
    );
    assert_eq!(
        success.metadata,
        SuccessMetadataV1::Query {
            snapshot_sequence: None
        }
    );

    let denied = system
        .router
        .clone()
        .oneshot(json_post("/v1/query", encode_query_call_v1(&query)?)?)
        .await?;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        decode_error_v1(&response_body(denied).await?)?.code,
        "APPLICATION_CREDENTIAL_REQUIRED"
    );

    let mut denied = json_post("/v1/query", encode_query_call_v1(&query)?)?;
    denied
        .headers_mut()
        .insert("x-runku-key", system.service_key.parse()?);
    let denied = system.router.clone().oneshot(denied).await?;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        decode_error_v1(&response_body(denied).await?)?.code,
        "AUTH_POLICY_DENIED"
    );

    let mut denied = json_post("/v1/query", encode_query_call_v1(&query)?)?;
    denied
        .headers_mut()
        .insert("x-runku-key", system.wrong_scope_key.parse()?);
    denied
        .headers_mut()
        .insert(header::AUTHORIZATION, "Bearer valid-user-token".parse()?);
    let denied = system.router.clone().oneshot(denied).await?;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        decode_error_v1(&response_body(denied).await?)?.code,
        "APPLICATION_SCOPE_DENIED"
    );

    let operation_id = OperationId::generate();
    let mutation = MutationCallV1 {
        target: CodeTarget::Release(system.release_id),
        function: "mutations.insert".parse()?,
        arguments: object([
            (
                "documentId",
                CanonicalValue::String(system.document_id.to_string()),
            ),
            (
                "tableId",
                CanonicalValue::String(system.table_id.to_string()),
            ),
            ("value", CanonicalValue::Int64(77)),
        ]),
        operation_id,
    };
    let first = system
        .router
        .clone()
        .oneshot(json_post_with_key(
            "/v1/mutation",
            encode_mutation_call_v1(&mutation)?,
            &system.service_key,
        )?)
        .await?;
    assert_eq!(first.status(), StatusCode::OK);
    let first = decode_success_v1(&response_body(first).await?)?;
    assert!(matches!(
        first.metadata,
        SuccessMetadataV1::Mutation {
            replayed: false,
            attempts: 1,
            commit_sequence: Some(_)
        }
    ));
    let replay = system
        .router
        .clone()
        .oneshot(json_post_with_key(
            "/v1/mutation",
            encode_mutation_call_v1(&mutation)?,
            &system.service_key,
        )?)
        .await?;
    let replay = decode_success_v1(&response_body(replay).await?)?;
    assert!(matches!(
        replay.metadata,
        SuccessMetadataV1::Mutation {
            replayed: true,
            attempts: 1,
            commit_sequence: Some(_)
        }
    ));
    let mut snapshot = system.store.begin_read(system.scope).await?;
    let document = snapshot
        .get_document(system.table_id, system.document_id)
        .await?
        .ok_or(TestFailure("mutation did not persist document"))?;
    snapshot.close().await?;
    assert_eq!(document.value, CanonicalValue::Int64(77));

    let action = ActionCallV1 {
        target: CodeTarget::Release(system.release_id),
        function: "actions.echo".parse()?,
        arguments: CanonicalValue::Boolean(true),
    };
    let mut request = json_post("/v1/action", encode_action_call_v1(&action)?)?;
    request
        .headers_mut()
        .insert("x-runku-key", system.service_key.parse()?);
    let response = system.router.oneshot(request).await?;
    let success = decode_success_v1(&response_body(response).await?)?;
    assert_eq!(
        success.result,
        object([
            ("kind", CanonicalValue::String("service".to_owned())),
            ("value", CanonicalValue::Boolean(true)),
        ])
    );
    assert_eq!(
        success.metadata,
        SuccessMetadataV1::Action {
            schedules_created: 0
        }
    );
    let cache = system.service.artifact_cache_telemetry();
    assert_eq!(cache.misses, 1);
    assert!(cache.hits >= 3);
    assert_eq!(cache.entries, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serving_catalog_refresh_is_explicit_and_monotonic() -> Result<(), Box<dyn Error>> {
    let system = setup().await?;
    assert!(matches!(
        system.catalog.refresh().await?,
        runku_gateway::ServingRefresh::Unchanged { revision: 6 }
    ));
    system
        .releases
        .apply(
            system.scope,
            OperationId::generate(),
            &ReleaseCommand::SetDefaultChannel {
                expected_channel: None,
                target_channel: Some(system.channel.clone()),
            },
        )
        .await?;
    assert!(matches!(
        system.catalog.refresh().await?,
        runku_gateway::ServingRefresh::Published { revision: 7 }
    ));
    assert!(matches!(
        system.catalog.refresh().await?,
        runku_gateway::ServingRefresh::Unchanged { revision: 7 }
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_node_channel_promotion_and_rollback_use_exact_oci_artifacts()
-> Result<(), Box<dyn Error>> {
    if std::env::var_os("RUNKU_FULL_NODE_DOCKER_TEST").is_none() {
        return Ok(());
    }
    let system = setup().await?;
    let release_one = ReleaseId::generate();
    let release_two = ReleaseId::generate();
    let tag_one = format!("runku-full-node-test:{release_one}").to_lowercase();
    let tag_two = format!("runku-full-node-test:{release_two}").to_lowercase();
    let cleanup = DockerImageCleanup(vec![tag_one.clone(), tag_two.clone()]);
    let image_one = build_node_image("R1", &tag_one).await?;
    let image_two = build_node_image("R2", &tag_two).await?;
    register_node_release(&system, release_one, &image_one).await?;
    register_node_release(&system, release_two, &image_two).await?;

    set_channel(&system, Some(system.release_id), Some(release_one)).await?;
    system.catalog.refresh().await?;
    assert_node_action(
        &system,
        CodeTarget::Channel(system.channel.clone()),
        release_one,
        "R1",
    )
    .await?;

    set_channel(&system, Some(release_one), Some(release_two)).await?;
    system.catalog.refresh().await?;
    assert_node_action(
        &system,
        CodeTarget::Channel(system.channel.clone()),
        release_two,
        "R2",
    )
    .await?;
    try_join_all((0..8).map(|_| {
        assert_node_action(
            &system,
            CodeTarget::Channel(system.channel.clone()),
            release_two,
            "R2",
        )
    }))
    .await?;
    assert_node_action(&system, CodeTarget::Release(release_one), release_one, "R1").await?;

    set_channel(&system, Some(release_two), Some(release_one)).await?;
    system.catalog.refresh().await?;
    assert_node_action(
        &system,
        CodeTarget::Channel(system.channel.clone()),
        release_one,
        "R1",
    )
    .await?;
    assert_node_action(&system, CodeTarget::Release(release_two), release_two, "R2").await?;
    drop(cleanup);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn full_node_docker_enforces_crypto_image_tcp_filesystem_memory_and_deadline()
-> Result<(), Box<dyn Error>> {
    if std::env::var_os("RUNKU_FULL_NODE_DOCKER_TEST").is_none() {
        return Ok(());
    }
    let suffix = ReleaseId::generate().to_string().to_lowercase();
    let network = format!("runku-node-net-{suffix}");
    let postgres = format!("runku-node-pg-{suffix}");
    let image_tag = format!("runku-full-node-capabilities:{suffix}");
    let cleanup = DockerCapabilityCleanup {
        image: image_tag.clone(),
        postgres: postgres.clone(),
        network: network.clone(),
    };
    docker_status(&["network", "create", "--internal", &network]).await?;
    docker_status(&[
        "run",
        "--detach",
        "--name",
        &postgres,
        "--network",
        &network,
        "--network-alias",
        "db",
        "--env",
        "POSTGRES_DB=runku_test",
        "--env",
        "POSTGRES_USER=runku",
        "--env",
        "POSTGRES_PASSWORD=runku_node_test_only",
        "postgres:16-alpine@sha256:20edbde7749f822887a1a022ad526fde0a47d6b2be9a8364433605cf65099416",
    ])
    .await?;
    wait_for_postgres(&postgres).await?;
    let image = build_node_image("CAPABILITIES", &image_tag).await?;
    let policy = FullNodeEgressPolicy::new(
        FullNodeNetworkMode::Restricted,
        vec![FullNodeTcpRule::new("db", vec![5432])?],
        vec![],
    )?;
    let restricted = NodeOciDescriptorV1::new(&image)?.with_egress_policy(policy.clone());
    let (manifest, artifact) = node_capability_manifest(&restricted)?;
    let runtime = ServerNodeRuntimeConfig::DockerSandbox(
        DockerNodeRuntimeConfig::new(4)?
            .with_restricted_network(DockerRestrictedNetwork::new(&network, policy.clone())?),
    )
    .build()?;

    let encrypted = runtime
        .execute(node_capability_request(
            Arc::clone(&manifest),
            Arc::clone(&artifact),
            "actions.encrypt",
            CanonicalValue::String("runku".to_owned()),
            Duration::from_secs(3),
        )?)
        .await?;
    assert_eq!(
        encrypted.value,
        CanonicalValue::String(
            "ae57fe8872aa84461538f8ed7c54dd3eb8f7bdd2398744aef598287746259bc9".to_owned()
        )
    );
    let image_result = runtime
        .execute(node_capability_request(
            Arc::clone(&manifest),
            Arc::clone(&artifact),
            "actions.image",
            CanonicalValue::String("pixel-data".to_owned()),
            Duration::from_secs(3),
        )?)
        .await?;
    let CanonicalValue::Bytes(image_bytes) = image_result.value else {
        return Err(TestFailure("image result was not bytes").into());
    };
    assert_eq!(&image_bytes[..8], b"\x89PNG\r\n\x1a\n");
    let postgres_result = runtime
        .execute(node_capability_request(
            Arc::clone(&manifest),
            Arc::clone(&artifact),
            "actions.postgres",
            CanonicalValue::String(
                "postgres://runku:runku_node_test_only@db:5432/runku_test".to_owned(),
            ),
            Duration::from_secs(5),
        )?)
        .await?;
    assert_eq!(
        postgres_result.value,
        CanonicalValue::String("tcp-ok".to_owned())
    );
    let filesystem = runtime
        .execute(node_capability_request(
            Arc::clone(&manifest),
            Arc::clone(&artifact),
            "actions.writeRoot",
            CanonicalValue::Null,
            Duration::from_secs(3),
        )?)
        .await?;
    assert_eq!(filesystem.value, CanonicalValue::String("EROFS".to_owned()));
    assert_eq!(
        runtime
            .execute(node_capability_request(
                Arc::clone(&manifest),
                Arc::clone(&artifact),
                "actions.loop",
                CanonicalValue::Null,
                Duration::from_millis(200),
            )?)
            .await,
        Err(RuntimeError::DeadlineExceeded)
    );
    assert_eq!(
        runtime
            .execute(node_capability_request(
                Arc::clone(&manifest),
                Arc::clone(&artifact),
                "actions.memory",
                CanonicalValue::Null,
                Duration::from_secs(5),
            )?)
            .await,
        Err(RuntimeError::JavaScript)
    );

    let default_runtime =
        ServerNodeRuntimeConfig::DockerSandbox(DockerNodeRuntimeConfig::new(1)?).build()?;
    assert_eq!(
        default_runtime
            .execute(node_capability_request(
                Arc::clone(&manifest),
                Arc::clone(&artifact),
                "actions.postgres",
                CanonicalValue::String(
                    "postgres://runku:runku_node_test_only@db:5432/runku_test".to_owned(),
                ),
                Duration::from_secs(2),
            )?)
            .await,
        Err(RuntimeError::UnsupportedRuntime)
    );
    let public = NodeOciDescriptorV1::new(&image)?.with_egress_policy(FullNodeEgressPolicy::new(
        FullNodeNetworkMode::Public,
        vec![],
        vec![],
    )?);
    let (public_manifest, public_artifact) = node_capability_manifest(&public)?;
    assert_eq!(
        runtime
            .execute(node_capability_request(
                public_manifest,
                public_artifact,
                "actions.encrypt",
                CanonicalValue::String("runku".to_owned()),
                Duration::from_secs(2),
            )?)
            .await,
        Err(RuntimeError::UnsupportedRuntime)
    );
    drop(cleanup);
    Ok(())
}

struct DockerCapabilityCleanup {
    image: String,
    postgres: String,
    network: String,
}

impl Drop for DockerCapabilityCleanup {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", &self.postgres])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = std::process::Command::new("docker")
            .args(["network", "rm", &self.network])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = std::process::Command::new("docker")
            .args(["image", "rm", "--force", &self.image])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

async fn docker_status(arguments: &[&str]) -> Result<(), Box<dyn Error>> {
    if !tokio::process::Command::new("docker")
        .args(arguments)
        .status()
        .await?
        .success()
    {
        return Err(TestFailure("Docker command failed").into());
    }
    Ok(())
}

async fn wait_for_postgres(container: &str) -> Result<(), Box<dyn Error>> {
    for _ in 0..30 {
        if tokio::process::Command::new("docker")
            .args([
                "exec",
                container,
                "pg_isready",
                "-U",
                "runku",
                "-d",
                "runku_test",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await?
            .success()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(TestFailure("PostgreSQL fixture did not become ready").into())
}

type NodeCapabilityPackage = (Arc<ReleaseManifestV1>, Arc<[u8]>);

fn node_capability_manifest(
    descriptor: &NodeOciDescriptorV1,
) -> Result<NodeCapabilityPackage, Box<dyn Error>> {
    let artifact = encode_node_oci_descriptor(descriptor)?;
    let release_id = ReleaseId::generate();
    let implementation = Sha256Digest::of(descriptor.image_reference().as_bytes());
    let functions = [
        "encrypt",
        "image",
        "loop",
        "memory",
        "postgres",
        "writeRoot",
    ]
    .into_iter()
    .map(|name| -> Result<FunctionManifest, Box<dyn Error>> {
        Ok(FunctionManifest {
            id: FunctionId::generate(),
            name: format!("actions.{name}").parse()?,
            function_type: FunctionType::Action,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::FullNode,
            implementation_hash: implementation,
            arguments_contract_hash: Sha256Digest::from_bytes([21; 32]),
            result_contract_hash: Sha256Digest::from_bytes([22; 32]),
            capabilities: vec![],
        })
    })
    .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok((
        Arc::new(ReleaseManifestV1 {
            release_id,
            project_id: ProjectId::generate(),
            build_id: BuildId::generate(),
            created_at: TimestampMicros::new(NOW),
            runtime_version: "runku-node-1".parse()?,
            artifact: descriptor.descriptor()?,
            function_contract_hash: Sha256Digest::from_bytes([18; 32]),
            schema_contract_hash: Sha256Digest::from_bytes([19; 32]),
            index_contract_hash: Sha256Digest::from_bytes([20; 32]),
            functions,
            cron_definitions: vec![],
        }),
        artifact.into(),
    ))
}

fn node_capability_request(
    manifest: Arc<ReleaseManifestV1>,
    artifact: Arc<[u8]>,
    name: &str,
    arguments: CanonicalValue,
    timeout: Duration,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let function = manifest
        .functions
        .iter()
        .find(|function| function.name.as_str() == name)
        .ok_or(TestFailure("Full Node fixture function missing"))?;
    Ok(InvocationRequest::new(
        EnvironmentScope::new(manifest.project_id, EnvironmentId::generate()),
        manifest.release_id,
        RequestId::generate(),
        InvocationId::generate(),
        function.id,
        manifest,
        artifact,
        arguments,
        timeout,
        CancellationToken::new(),
    )?)
}

struct DockerImageCleanup(Vec<String>);

impl Drop for DockerImageCleanup {
    fn drop(&mut self) {
        for tag in &self.0 {
            let _ = std::process::Command::new("docker")
                .args(["image", "rm", "--force", tag])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

async fn build_node_image(label: &str, tag: &str) -> Result<String, Box<dyn Error>> {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/full_node");
    let status = tokio::process::Command::new("docker")
        .args([
            "build",
            "--quiet",
            "--build-arg",
            &format!("RELEASE_LABEL={label}"),
            "--tag",
            tag,
        ])
        .arg(fixture)
        .status()
        .await?;
    if !status.success() {
        return Err(TestFailure("Docker image build failed").into());
    }
    let output = tokio::process::Command::new("docker")
        .args(["image", "inspect", "--format", "{{.Id}}", tag])
        .output()
        .await?;
    if !output.status.success() {
        return Err(TestFailure("Docker image inspection failed").into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

async fn register_node_release(
    system: &TestSystem,
    release_id: ReleaseId,
    image_reference: &str,
) -> Result<(), Box<dyn Error>> {
    let artifact = NodeOciDescriptorV1::new(image_reference)?;
    let artifact_bytes = encode_node_oci_descriptor(&artifact)?;
    let descriptor = artifact.descriptor()?;
    system.artifacts.put(&descriptor, &artifact_bytes).await?;
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: system.scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(NOW),
        runtime_version: "runku-node-1".parse()?,
        artifact: descriptor,
        function_contract_hash: Sha256Digest::from_bytes([11; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([12; 32]),
        index_contract_hash: Sha256Digest::from_bytes([13; 32]),
        functions: vec![FunctionManifest {
            id: FunctionId::generate(),
            name: "actions.echo".parse()?,
            function_type: FunctionType::Action,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::FullNode,
            implementation_hash: Sha256Digest::of(image_reference.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([14; 32]),
            result_contract_hash: Sha256Digest::from_bytes([15; 32]),
            capabilities: vec![],
        }],
        cron_definitions: vec![],
    };
    system
        .releases
        .apply(
            system.scope,
            OperationId::generate(),
            &ReleaseCommand::Register {
                manifest_bytes: encode_release_manifest(&manifest)?,
            },
        )
        .await?;
    for (expected, next) in [
        (ReleaseStatus::Created, ReleaseStatus::Building),
        (ReleaseStatus::Building, ReleaseStatus::Validating),
        (ReleaseStatus::Validating, ReleaseStatus::Ready),
        (ReleaseStatus::Ready, ReleaseStatus::Servable),
    ] {
        system
            .releases
            .apply(
                system.scope,
                OperationId::generate(),
                &ReleaseCommand::Transition {
                    release_id,
                    expected,
                    next,
                },
            )
            .await?;
    }
    Ok(())
}

async fn set_channel(
    system: &TestSystem,
    expected_release: Option<ReleaseId>,
    target_release: Option<ReleaseId>,
) -> Result<(), Box<dyn Error>> {
    system
        .releases
        .apply(
            system.scope,
            OperationId::generate(),
            &ReleaseCommand::SetChannel {
                channel: system.channel.clone(),
                expected_release,
                target_release,
            },
        )
        .await?;
    Ok(())
}

async fn assert_node_action(
    system: &TestSystem,
    target: CodeTarget,
    expected_release: ReleaseId,
    expected_label: &str,
) -> Result<(), Box<dyn Error>> {
    let call = ActionCallV1 {
        target,
        function: "actions.echo".parse()?,
        arguments: CanonicalValue::String("hello".to_owned()),
    };
    let response = system
        .router
        .clone()
        .oneshot(json_post_with_key(
            "/v1/action",
            encode_action_call_v1(&call)?,
            &system.service_key,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let success = decode_success_v1(&response_body(response).await?)?;
    assert_eq!(success.release_id, expected_release);
    assert_eq!(
        success.result,
        object([
            ("argument", CanonicalValue::String("hello".to_owned())),
            (
                "function",
                CanonicalValue::String("actions.echo".to_owned())
            ),
            ("release", CanonicalValue::String(expected_label.to_owned())),
        ])
    );
    assert_eq!(
        success.metadata,
        SuccessMetadataV1::Action {
            schedules_created: 0
        }
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cold_artifact_load_is_single_flight_and_bounded() -> Result<(), Box<dyn Error>>
{
    let system = setup().await?;
    let call = QueryCallV1 {
        target: CodeTarget::Channel(system.channel.clone()),
        function: "queries.me".parse()?,
        arguments: CanonicalValue::Null,
    };
    let body = encode_query_call_v1(&call)?;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let router = system.router.clone();
        let mut request = json_post("/v1/query", body.clone())?;
        request
            .headers_mut()
            .insert(header::AUTHORIZATION, "Bearer valid-user-token".parse()?);
        request
            .headers_mut()
            .insert("x-runku-key", system.service_key.parse()?);
        tasks.push(tokio::spawn(async move { router.oneshot(request).await }));
    }
    for task in tasks {
        let result = task
            .await
            .map_err(|_| TestFailure("concurrent request task failed"))?;
        let response = match result {
            Ok(response) => response,
            Err(error) => match error {},
        };
        assert_eq!(response.status(), StatusCode::OK);
    }
    let cache = system.service.artifact_cache_telemetry();
    assert_eq!(cache.misses, 1);
    assert_eq!(cache.hits, 7);
    assert_eq!(cache.entries, 1);
    assert!(cache.retained_bytes > 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_bearer_and_function_type_fail_before_runtime() -> Result<(), Box<dyn Error>> {
    let system = setup().await?;
    let call = QueryCallV1 {
        target: CodeTarget::Release(system.release_id),
        function: "queries.me".parse()?,
        arguments: CanonicalValue::Null,
    };
    let mut request = json_post("/v1/query", encode_query_call_v1(&call)?)?;
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, "Bearer invalid".parse()?);
    request
        .headers_mut()
        .insert("x-runku-key", system.service_key.parse()?);
    let response = system.router.clone().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        decode_error_v1(&response_body(response).await?)?.code,
        "PRINCIPAL_INVALID"
    );

    let mismatch = QueryCallV1 {
        target: CodeTarget::Release(system.release_id),
        function: "actions.echo".parse()?,
        arguments: CanonicalValue::Null,
    };
    let response = system
        .router
        .clone()
        .oneshot(json_post_with_key(
            "/v1/query",
            encode_query_call_v1(&mismatch)?,
            &system.service_key,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        decode_error_v1(&response_body(response).await?)?.code,
        "FUNCTION_TYPE_MISMATCH"
    );

    let network = ActionCallV1 {
        target: CodeTarget::Release(system.release_id),
        function: "actions.network".parse()?,
        arguments: CanonicalValue::Null,
    };
    let response = system
        .router
        .oneshot(json_post_with_key(
            "/v1/action",
            encode_action_call_v1(&network)?,
            &system.service_key,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        decode_error_v1(&response_body(response).await?)?.code,
        "ACTION_HTTPS_UNAVAILABLE"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vertical_deadline_terminates_v8_and_returns_a_safe_error() -> Result<(), Box<dyn Error>> {
    let system = setup().await?;
    let action = ActionCallV1 {
        target: CodeTarget::Release(system.release_id),
        function: "actions.echo".parse()?,
        arguments: CanonicalValue::String("loop".to_owned()),
    };
    let mut request = json_post("/v1/action", encode_action_call_v1(&action)?)?;
    request
        .headers_mut()
        .insert("x-runku-key", system.service_key.parse()?);
    let response = system.router.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        decode_error_v1(&response_body(response).await?)?.code,
        "RUNTIME_DEADLINE_EXCEEDED"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn websocket_workspace_query_mutation_dispatcher_and_delivery_are_vertical()
-> Result<(), Box<dyn Error>> {
    let system = setup().await?;
    let registry = SubscriptionRegistry::new(RegistryConfig {
        max_subscriptions: 16,
        max_dependencies: 32,
        max_result_bytes: 4_096,
        delivery_buffer: 8,
        retry_base_micros: 10,
        retry_max_micros: 1_000,
        max_consecutive_failures: 3,
    })?;
    let realtime = RealtimeGateway::new(
        RealtimeGatewayConfig {
            max_connections: 8,
            authentication_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(10),
            reauthentication_interval: Duration::from_mins(5),
            command_timeout: Duration::from_secs(3),
            max_subscriptions_per_connection: 4,
            outbound_buffer: 16,
        },
        system.service.clone(),
        registry.clone(),
    )?;
    let router = build_router_with_realtime(
        GatewayHttpConfig {
            allowed_origins: BTreeSet::from(["https://app.example".parse()?]),
            max_concurrent_requests: 16,
            request_timeout: Duration::from_secs(3),
        },
        system.service.clone(),
        realtime,
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = serve(listener, router).await;
    });
    let url = format!("ws://{address}/v1/realtime");
    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        header::ORIGIN,
        HeaderValue::from_static("https://app.example"),
    );
    request.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("runku.realtime.v1"),
    );
    let (mut socket, _) = connect_async(request).await?;
    let auth_request = runku_core::RequestId::generate();
    socket
        .send(Message::Text(
            serde_json::json!({
                "type":"authenticate", "version":1, "requestId":auth_request,
                "applicationKey":system.service_key, "bearer":"valid-user-token"
            })
            .to_string()
            .into(),
        ))
        .await?;
    assert!(matches!(
        receive_realtime(&mut socket).await?,
        RealtimeServerMessageV1::AuthenticationAccepted { request_id } if request_id == auth_request
    ));

    let subscribe_request = runku_core::RequestId::generate();
    socket
        .send(Message::Text(
            serde_json::json!({
                "type":"subscribe", "version":1, "requestId":subscribe_request,
                "target":format!("workspace:{}", system.workspace),
                "function":"queries.document",
                "arguments": {
                    "type":"object", "value":[
                        {"key":"documentId","value":{"type":"string","value":system.document_id}},
                        {"key":"tableId","value":{"type":"string","value":system.table_id}}
                    ]
                }
            })
            .to_string()
            .into(),
        ))
        .await?;
    let subscription_id = match receive_realtime(&mut socket).await? {
        RealtimeServerMessageV1::State {
            request_id: Some(request_id),
            subscription_id,
            release_id,
            delivery_revision: 1,
            value: CanonicalValue::Null,
            snapshot_sequence: Some(_),
            ..
        } if request_id == subscribe_request && release_id == system.release_id => subscription_id,
        message => return Err(format!("unexpected initial state: {message:?}").into()),
    };
    let snapshot = registry.subscribe(subscription_id)?.snapshot;
    assert_eq!(
        snapshot.spec.pinned_code,
        runku_data::PinnedCode::DevRevision(system.dev_revision_id)
    );
    assert_eq!(snapshot.dependencies.len(), 1);

    let release_subscribe_request = runku_core::RequestId::generate();
    socket
        .send(Message::Text(
            serde_json::json!({
                "type":"subscribe", "version":1, "requestId":release_subscribe_request,
                "target":format!("release:{}", system.release_id),
                "function":"queries.document",
                "arguments": {
                    "type":"object", "value":[
                        {"key":"documentId","value":{"type":"string","value":system.document_id}},
                        {"key":"tableId","value":{"type":"string","value":system.table_id}}
                    ]
                }
            })
            .to_string()
            .into(),
        ))
        .await?;
    let release_subscription_id = match receive_realtime(&mut socket).await? {
        RealtimeServerMessageV1::State {
            request_id: Some(request_id),
            subscription_id,
            delivery_revision: 1,
            value: CanonicalValue::Null,
            ..
        } if request_id == release_subscribe_request => subscription_id,
        message => return Err(format!("unexpected Release initial state: {message:?}").into()),
    };
    assert_eq!(
        registry
            .subscribe(release_subscription_id)?
            .snapshot
            .spec
            .pinned_code,
        runku_data::PinnedCode::Release(system.release_id)
    );

    let mutation = MutationCallV1 {
        target: CodeTarget::Release(system.release_id),
        function: "mutations.insert".parse()?,
        arguments: object([
            (
                "documentId",
                CanonicalValue::String(system.document_id.to_string()),
            ),
            (
                "tableId",
                CanonicalValue::String(system.table_id.to_string()),
            ),
            ("value", CanonicalValue::Int64(77)),
        ]),
        operation_id: OperationId::generate(),
    };
    let response = system
        .router
        .clone()
        .oneshot(json_post_with_key(
            "/v1/mutation",
            encode_mutation_call_v1(&mutation)?,
            &system.service_key,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let logical_store: Arc<dyn LogicalStore> = system.store.clone();
    let runner: Arc<dyn SubscriptionRunner> = system.service.clone();
    let dispatcher = ChangeDispatcher::new(
        logical_store,
        registry.clone(),
        runner,
        "websocket-vertical".parse::<OutboxConsumerName>()?,
        WorkerId::generate(),
        DispatcherConfig::PRODUCTION,
    )?;
    let poll = dispatcher
        .poll_once(system.scope, TimestampMicros::new(NOW + 100))
        .await?;
    assert_eq!(poll.events, 1);
    assert_eq!(poll.reruns, 2);
    let expected = BTreeSet::from([subscription_id, release_subscription_id]);
    let mut delivered = BTreeSet::new();
    for _ in 0..2 {
        match receive_realtime(&mut socket).await? {
            RealtimeServerMessageV1::State {
                request_id: None,
                subscription_id: id,
                release_id,
                delivery_revision: 2,
                value: CanonicalValue::Int64(77),
                snapshot_sequence: Some(_),
                ..
            } if release_id == system.release_id => {
                delivered.insert(id);
            }
            message => return Err(format!("unexpected rerun state: {message:?}").into()),
        }
    }
    assert_eq!(delivered, expected);
    socket.close(None).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(registry.telemetry().subscriptions, 0);
    server.abort();
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn setup() -> Result<TestSystem, Box<dyn Error>> {
    let directory = TempDir::new()?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let release_id = ReleaseId::generate();
    let table_id = TableId::generate();
    let document_id = DocumentId::generate();
    let release_url = sqlite_url(&directory.path().join("releases.sqlite3"));
    let identity_url = sqlite_url(&directory.path().join("identity.sqlite3"));
    let development_url = sqlite_url(&directory.path().join("development.sqlite3"));
    let releases = Arc::new(
        SqlReleaseRepository::connect_sqlite(&release_url, RepositoryConfig::LOCAL).await?,
    );
    let identity = Arc::new(
        SqlApplicationIdentityRepository::connect_sqlite(
            &identity_url,
            IdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let artifacts = Arc::new(
        FilesystemArtifactStore::open(
            directory.path().join("artifacts"),
            FilesystemStoreRole::Test,
        )
        .await?,
    );
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("data.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );

    let action_source = r#"
        export default async (ctx, value) => {
          if (value === "loop") while (true) {}
          return { kind: ctx.auth.principal.kind, value };
        };
    "#;
    let internal_action_source = "export default async (_ctx, value) => value;";
    let network_source = "export default async (_ctx, value) => value;";
    let schedule_source = r#"
        export default async (ctx, value) =>
          await ctx.scheduler.runAfter(0n, "actions.internal", value, { idempotencyKey: "workspace-pin" });
    "#;
    let schedule_mutation_source = r#"
        export default async (ctx, value) =>
          await ctx.scheduler.runAfter(0n, "mutations.scheduledInsert", value);
    "#;
    let mutation_source = r"
        export default async (ctx, input) => {
          await ctx.db.insert(input.tableId, input.documentId, input.value);
          return input.value;
        };
    ";
    let query_source = r"
        export default async (ctx, argument) => ({
          argument,
          kind: ctx.auth.principal.kind,
        });
    ";
    let document_query_source = r"
        export default async (ctx, input) => {
          const document = await ctx.db.get(input.tableId, input.documentId);
          return document === null ? null : document.value;
        };
    ";
    let bundle = SafeEsmBundleV1::from_sources([
        action_source,
        internal_action_source,
        mutation_source,
        network_source,
        document_query_source,
        query_source,
        schedule_mutation_source,
        schedule_source,
    ])?;
    let artifact_bytes = encode_safe_esm_bundle(&bundle)?;
    let descriptor = bundle.descriptor()?;
    artifacts.put(&descriptor, &artifact_bytes).await?;
    let function = |name: &str,
                    function_type,
                    auth_policy,
                    source: &str,
                    capabilities|
     -> Result<FunctionManifest, Box<dyn Error>> {
        Ok(FunctionManifest {
            id: FunctionId::generate(),
            name: name.parse()?,
            function_type,
            visibility: FunctionVisibility::Public,
            auth_policy,
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
        created_at: TimestampMicros::new(NOW - 1_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: descriptor,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![
            function(
                "actions.echo",
                FunctionType::Action,
                AuthPolicy::Service,
                action_source,
                vec![Capability::AuthRead],
            )?,
            FunctionManifest {
                id: FunctionId::generate(),
                name: "actions.internal".parse()?,
                function_type: FunctionType::Action,
                visibility: FunctionVisibility::Internal,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(internal_action_source.as_bytes()),
                arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
                result_contract_hash: Sha256Digest::from_bytes([5; 32]),
                capabilities: vec![],
            },
            function(
                "actions.network",
                FunctionType::Action,
                AuthPolicy::None,
                network_source,
                vec![Capability::NetworkHttps],
            )?,
            function(
                "actions.schedule",
                FunctionType::Action,
                AuthPolicy::Service,
                schedule_source,
                vec![Capability::SchedulerCreate],
            )?,
            function(
                "actions.scheduleMutation",
                FunctionType::Action,
                AuthPolicy::Service,
                schedule_mutation_source,
                vec![Capability::SchedulerCreate],
            )?,
            function(
                "mutations.insert",
                FunctionType::Mutation,
                AuthPolicy::None,
                mutation_source,
                vec![Capability::DbWrite],
            )?,
            FunctionManifest {
                id: FunctionId::generate(),
                name: "mutations.scheduledInsert".parse()?,
                function_type: FunctionType::Mutation,
                visibility: FunctionVisibility::Internal,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(mutation_source.as_bytes()),
                arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
                result_contract_hash: Sha256Digest::from_bytes([5; 32]),
                capabilities: vec![Capability::DbWrite],
            },
            function(
                "queries.document",
                FunctionType::Query,
                AuthPolicy::User,
                document_query_source,
                vec![Capability::DbRead],
            )?,
            function(
                "queries.me",
                FunctionType::Query,
                AuthPolicy::User,
                query_source,
                vec![Capability::DbRead, Capability::AuthRead],
            )?,
        ],
        cron_definitions: Vec::new(),
    };
    let manifest_bytes = encode_release_manifest(&manifest)?;
    releases
        .apply(
            scope,
            OperationId::generate(),
            &ReleaseCommand::Register {
                manifest_bytes: manifest_bytes.clone(),
            },
        )
        .await?;
    for (expected, next) in [
        (ReleaseStatus::Created, ReleaseStatus::Building),
        (ReleaseStatus::Building, ReleaseStatus::Validating),
        (ReleaseStatus::Validating, ReleaseStatus::Ready),
        (ReleaseStatus::Ready, ReleaseStatus::Servable),
    ] {
        releases
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::Transition {
                    release_id,
                    expected,
                    next,
                },
            )
            .await?;
    }
    let channel: ChannelName = "stable".parse()?;
    releases
        .apply(
            scope,
            OperationId::generate(),
            &ReleaseCommand::SetChannel {
                channel: channel.clone(),
                expected_release: None,
                target_release: Some(release_id),
            },
        )
        .await?;

    let development_context = DevelopmentContext {
        scope,
        environment: EnvironmentDescriptor::local_development(scope.environment_id()),
    };
    let development = Arc::new(
        SqlDevelopmentRepository::connect_sqlite(
            &development_url,
            DevelopmentRepositoryConfig::LOCAL,
            development_context,
        )
        .await?,
    );
    let workspace: WorkspaceRef = "local/main".parse()?;
    let actor: DevelopmentActor = "local".parse()?;
    development
        .apply(
            development_context,
            OperationId::generate(),
            &DevelopmentCommand::CreateWorkspace {
                workspace_id: WorkspaceId::generate(),
                workspace_ref: workspace.clone(),
                actor: actor.clone(),
                created_at: TimestampMicros::new(NOW - 500_000),
            },
        )
        .await?;
    let dev_revision_id = DevRevisionId::generate();
    development
        .apply(
            development_context,
            OperationId::generate(),
            &DevelopmentCommand::PublishRevision {
                workspace_ref: workspace.clone(),
                expected_head: None,
                revision: DevelopmentRevisionEntry {
                    revision_id: dev_revision_id,
                    release_id,
                    manifest_digest: Sha256Digest::of(&manifest_bytes),
                    manifest_bytes,
                    actor,
                    created_at: TimestampMicros::new(NOW - 400_000),
                },
            },
        )
        .await?;

    let logical_store: Arc<dyn LogicalStore> = store.clone();
    let runtime = RuntimeSupervisor::start(RuntimeLimits::builder(2, 16).build()?)?;
    let query = QueryExecutor::new(runtime.clone(), Arc::clone(&logical_store));
    let mutation = MutationExecutor::new(runtime.clone(), Arc::clone(&logical_store));
    let action = ActionExecutor::new(runtime, logical_store);
    let crypto = Arc::new(KeyringCrypto::new([23; 32]));
    let service_scopes = BTreeSet::from(["functions:invoke".parse::<ApplicationScope>()?]);
    let client_id = ApplicationClientId::generate();
    identity
        .create_client(&ApplicationClient {
            scope,
            id: client_id,
            name: "vertical-worker".parse()?,
            kind: ClientKind::Confidential,
            status: ApplicationClientStatus::Active,
            scope_ceiling: service_scopes.clone(),
            created_at: TimestampMicros::new(NOW - 100),
        })
        .await?;
    let generated = crypto.generate_secret(CredentialId::generate())?;
    let parsed: ParsedApplicationKey = generated.key.expose().parse()?;
    identity
        .create_credential(&ApplicationCredential {
            scope,
            id: parsed.credential_id(),
            client_id,
            kind: generated.kind,
            label: "action-service".parse()?,
            status: CredentialStatus::Active,
            digest: generated.digest,
            scopes: service_scopes,
            created_at: TimestampMicros::new(NOW - 50),
            expires_at: None,
            revoked_at: None,
            deleted_at: None,
        })
        .await?;
    let service_key = generated.key.expose().to_owned();
    let wrong_scopes = BTreeSet::from(["profile:read".parse::<ApplicationScope>()?]);
    let wrong_client_id = ApplicationClientId::generate();
    identity
        .create_client(&ApplicationClient {
            scope,
            id: wrong_client_id,
            name: "vertical-wrong-scope".parse()?,
            kind: ClientKind::Public,
            status: ApplicationClientStatus::Active,
            scope_ceiling: wrong_scopes.clone(),
            created_at: TimestampMicros::new(NOW - 90),
        })
        .await?;
    let generated = crypto.generate_publishable(CredentialId::generate())?;
    let parsed: ParsedApplicationKey = generated.key.expose().parse()?;
    identity
        .create_credential(&ApplicationCredential {
            scope,
            id: parsed.credential_id(),
            client_id: wrong_client_id,
            kind: generated.kind,
            label: "wrong-scope".parse()?,
            status: CredentialStatus::Active,
            digest: generated.digest,
            scopes: wrong_scopes,
            created_at: TimestampMicros::new(NOW - 40),
            expires_at: None,
            revoked_at: None,
            deleted_at: None,
        })
        .await?;
    let wrong_scope_key = generated.key.expose().to_owned();
    let user_scope: ApplicationScope = "profile:read".parse()?;
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::from_bytes([9; 32]),
        PrincipalKind::User,
        "test-idp",
        BTreeSet::from([user_scope]),
        None,
        Some(TimestampMicros::new(NOW - 10)),
        Some(TimestampMicros::new(NOW + 1_000_000)),
        1,
    )?;
    let release_boundary: Arc<dyn ReleaseRepository> = releases.clone();
    let catalog = Arc::new(ServingCatalog::load(scope, Arc::clone(&release_boundary)).await?);
    let development_boundary: Arc<dyn DevelopmentRepository> = development.clone();
    let development_catalog =
        Arc::new(DevelopmentCatalog::load(development_context, development_boundary).await?);
    let artifact_boundary: Arc<dyn ArtifactStore> = artifacts.clone();
    let credential_boundary: Arc<dyn ApplicationCredentialResolver> = identity;
    let queue = Arc::new(InMemoryExecutionQueue::new(128)?);
    let control: Arc<dyn ExecutionControlPlane> =
        Arc::new(InMemoryExecutionControlPlane::default());
    let execution_class = ExecutionClass::new("node_oci_v1")?;
    let docker: Arc<dyn FullNodeActionRuntime> =
        Arc::new(ServerNodeRuntimeConfig::DockerSandbox(DockerNodeRuntimeConfig::new(8)?).build()?);
    let handler = Arc::new(FullNodeExecutionHandler::new(
        release_boundary.clone(),
        artifact_boundary.clone(),
        docker,
        Arc::clone(&control),
    ));
    let agent = Arc::new(ExecutionAgent::new(
        queue.clone(),
        handler,
        ExecutionAgentConfig {
            class: execution_class.clone(),
            slots: 8,
            max_concurrent_per_project: 4,
            pull_wait: Duration::from_millis(50),
        },
    )?);
    let (execution_shutdown, execution_receiver) = tokio::sync::watch::channel(false);
    tokio::spawn(agent.run(execution_receiver));
    let queued_node = QueuedNodeRuntime::new(
        queue,
        control,
        QueuedNodeRuntimeConfig {
            class: execution_class,
            result_wait: Duration::from_millis(50),
        },
    )?;
    let service = Arc::new(
        ProductInvocationService::new(
            ProductInvocationConfig {
                scope,
                execution_timeout: Duration::from_secs(2),
                max_cached_artifact_bytes: 128 * 1024 * 1024,
            },
            Arc::clone(&catalog),
            release_boundary,
            artifact_boundary,
            credential_boundary,
            crypto,
            Arc::new(FixedUserVerifier { principal }),
            Arc::new(FixedClock),
            query,
            mutation,
            action,
            None,
        )
        .map_err(|_| TestFailure("product invocation composition failed"))?
        .with_full_node_runtime(Arc::new(queued_node))
        .with_development_catalog(Arc::clone(&development_catalog))
        .map_err(|_| TestFailure("development catalog composition failed"))?,
    );
    let router = build_router(
        GatewayHttpConfig {
            allowed_origins: BTreeSet::new(),
            max_concurrent_requests: 16,
            request_timeout: Duration::from_secs(3),
        },
        service.clone(),
    )?;
    Ok(TestSystem {
        _directory: directory,
        _execution_shutdown: execution_shutdown,
        router,
        service,
        catalog,
        releases,
        artifacts,
        development,
        development_catalog,
        development_context,
        store,
        scope,
        release_id,
        channel,
        workspace,
        dev_revision_id,
        table_id,
        document_id,
        service_key,
        wrong_scope_key,
    })
}

fn sqlite_url(path: &std::path::Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display())
}

fn object<const N: usize>(entries: [(&str, CanonicalValue); N]) -> CanonicalValue {
    CanonicalValue::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn json_post(path: &str, body: Vec<u8>) -> Result<Request<Body>, Box<dyn Error>> {
    Ok(Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))?)
}

fn json_post_with_key(
    path: &str,
    body: Vec<u8>,
    application_key: &str,
) -> Result<Request<Body>, Box<dyn Error>> {
    let mut request = json_post(path, body)?;
    request
        .headers_mut()
        .insert("x-runku-key", application_key.parse()?);
    Ok(request)
}

async fn response_body(response: axum::response::Response) -> Result<Bytes, Box<dyn Error>> {
    Ok(to_bytes(response.into_body(), PUBLIC_ENVELOPE_MAX_BYTES).await?)
}

type RealtimeSocket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn receive_realtime(
    socket: &mut RealtimeSocket,
) -> Result<RealtimeServerMessageV1, Box<dyn Error>> {
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await?
        .ok_or(TestFailure("Realtime socket closed"))??;
    let Message::Text(text) = message else {
        return Err(TestFailure("Realtime server did not send text").into());
    };
    Ok(decode_realtime_server_v1(text.as_bytes())?)
}
