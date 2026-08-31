//! Real-socket Realtime WebSocket boundary conformance.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::http::{HeaderValue, header};
use futures_util::{SinkExt, StreamExt};
use runku_core::{
    CodeTarget, EnvironmentId, EnvironmentScope, FunctionName, OutboxEventId, ProjectId, ReleaseId,
    RequestId, TableId,
};
use runku_data::OutboxCursor;
use runku_execution::{QueryOutcome, ReadDependency};
use runku_gateway::{
    CorsOrigin, GatewayFailure, GatewayHttpConfig, GatewaySuccess, InvocationContext,
    InvocationService, InvokeCallV1, RealtimeGateway, RealtimeGatewayConfig,
    RealtimePreparedSubscription, RealtimeQueryService, RealtimeSubscribeContext,
    build_router_with_realtime, serve,
};
use runku_identity::{
    ApplicationContext, ApplicationCredentialResolver, AuthBoundary, AuthGateway, AuthInput,
    IdentityError, KeyringCrypto, ParsedApplicationKey, PrincipalEvidence, RequestIdentity,
};
use runku_protocol::{RealtimeServerMessageV1, SuccessMetadataV1, decode_realtime_server_v1};
use runku_realtime::{ChangeImpact, RegistryConfig, SubscriptionRegistry, SubscriptionSpec};
use runku_releases::{AuthPolicy, FunctionVisibility};
use runku_value::{CanonicalValue, TimestampMicros};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_tungstenite::{
    WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

#[derive(Debug)]
struct NoKeys;

#[async_trait]
impl ApplicationCredentialResolver for NoKeys {
    async fn resolve_key(
        &self,
        _scope: EnvironmentScope,
        _key: &ParsedApplicationKey,
        _crypto: &KeyringCrypto,
        _now: TimestampMicros,
    ) -> Result<ApplicationContext, IdentityError> {
        Err(IdentityError::InvalidCredential)
    }
}

#[derive(Clone)]
struct MockService {
    scope: EnvironmentScope,
    release_id: ReleaseId,
    identity: Arc<RequestIdentity>,
    table_id: TableId,
}

impl fmt::Debug for MockService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MockService")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl RealtimeQueryService for MockService {
    async fn prepare_subscription(
        &self,
        context: RealtimeSubscribeContext,
        _target: CodeTarget,
        function: FunctionName,
        arguments: CanonicalValue,
    ) -> Result<RealtimePreparedSubscription, GatewayFailure> {
        Ok(RealtimePreparedSubscription {
            spec: SubscriptionSpec {
                id: context.subscription_id,
                scope: self.scope,
                release_id: self.release_id,
                pinned_code: runku_core::PinnedCode::Release(self.release_id),
                function,
                arguments,
                identity: Arc::clone(&self.identity),
                authorized_until: TimestampMicros::new(i64::MAX),
            },
            outcome: QueryOutcome {
                value: CanonicalValue::String("initial".to_owned()),
                snapshot_sequence: Some(1),
                dependencies: vec![ReadDependency::Point {
                    table_id: self.table_id,
                    document_id: runku_core::DocumentId::generate(),
                    observed_revision: None,
                    snapshot_sequence: 1,
                }],
            },
        })
    }
}

#[async_trait]
impl InvocationService for MockService {
    async fn invoke(
        &self,
        _context: InvocationContext,
        _call: InvokeCallV1,
    ) -> Result<GatewaySuccess, GatewayFailure> {
        Ok(GatewaySuccess {
            release_id: self.release_id,
            value: CanonicalValue::Null,
            metadata: SuccessMetadataV1::Query {
                snapshot_sequence: None,
            },
        })
    }
}

async fn identity(scope: EnvironmentScope) -> Result<Arc<RequestIdentity>, Box<dyn Error>> {
    let resolver = NoKeys;
    let crypto = KeyringCrypto::new([9; 32]);
    Ok(Arc::new(
        AuthGateway::new(&resolver, &crypto)
            .authorize(
                scope,
                FunctionVisibility::Public,
                AuthPolicy::Optional,
                AuthInput::parse(AuthBoundary::External, None, PrincipalEvidence::Absent)?,
                TimestampMicros::new(1),
            )
            .await?,
    ))
}

type ClientSocket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn fixture(
    reauth: Duration,
) -> Result<
    (
        String,
        SubscriptionRegistry,
        RealtimeGateway,
        JoinHandle<()>,
    ),
    Box<dyn Error>,
> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let registry = SubscriptionRegistry::new(RegistryConfig {
        max_subscriptions: 32,
        max_dependencies: 16,
        max_result_bytes: 4_096,
        delivery_buffer: 8,
        retry_base_micros: 10,
        retry_max_micros: 100,
        max_consecutive_failures: 3,
    })?;
    let service = Arc::new(MockService {
        scope,
        release_id: ReleaseId::generate(),
        identity: identity(scope).await?,
        table_id: TableId::generate(),
    });
    let realtime = RealtimeGateway::new(
        RealtimeGatewayConfig {
            max_connections: 4,
            authentication_timeout: Duration::from_millis(250),
            idle_timeout: Duration::from_secs(5),
            reauthentication_interval: reauth,
            command_timeout: Duration::from_secs(1),
            max_subscriptions_per_connection: 2,
            outbound_buffer: 4,
        },
        service.clone(),
        registry.clone(),
    )?;
    let router = build_router_with_realtime(
        GatewayHttpConfig {
            allowed_origins: BTreeSet::from(["https://app.example".parse::<CorsOrigin>()?]),
            max_concurrent_requests: 16,
            request_timeout: Duration::from_secs(2),
        },
        service,
        realtime.clone(),
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let _ = serve(listener, router).await;
    });
    Ok((
        format!("ws://{address}/v1/realtime"),
        registry,
        realtime,
        task,
    ))
}

async fn connect(url: &str, origin: &str) -> Result<ClientSocket, Box<dyn Error>> {
    let mut request = url.into_client_request()?;
    request
        .headers_mut()
        .insert(header::ORIGIN, HeaderValue::from_str(origin)?);
    request.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("runku.realtime.v1"),
    );
    let (socket, response) = connect_async(request).await?;
    assert_eq!(
        response.headers().get(header::SEC_WEBSOCKET_PROTOCOL),
        Some(&HeaderValue::from_static("runku.realtime.v1"))
    );
    Ok(socket)
}

async fn send_json(
    socket: &mut ClientSocket,
    value: serde_json::Value,
) -> Result<(), Box<dyn Error>> {
    socket.send(Message::Text(value.to_string().into())).await?;
    Ok(())
}

async fn receive(socket: &mut ClientSocket) -> Result<RealtimeServerMessageV1, Box<dyn Error>> {
    let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await?
        .ok_or("socket closed")??;
    let Message::Text(text) = message else {
        return Err("expected text".into());
    };
    Ok(decode_realtime_server_v1(text.as_bytes())?)
}

fn authenticate(request: RequestId) -> serde_json::Value {
    serde_json::json!({
        "type": "authenticate", "version": 1, "requestId": request,
        "applicationKey": null, "bearer": null
    })
}

fn subscribe(request: RequestId) -> serde_json::Value {
    serde_json::json!({
        "type": "subscribe", "version": 1, "requestId": request,
        "target": "channel:stable", "function": "messages.list",
        "arguments": {"type": "null"}
    })
}

#[tokio::test]
async fn real_socket_enforces_handshake_auth_order_and_text_only() -> Result<(), Box<dyn Error>> {
    let (url, _registry, realtime, server) = fixture(Duration::from_secs(2)).await?;
    let mut invalid = url.as_str().into_client_request()?;
    invalid.headers_mut().insert(
        header::ORIGIN,
        HeaderValue::from_static("https://evil.example"),
    );
    invalid.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("runku.realtime.v1"),
    );
    assert!(connect_async(invalid).await.is_err());

    let mut socket = connect(&url, "https://app.example").await?;
    let request = RequestId::generate();
    send_json(&mut socket, subscribe(request)).await?;
    assert!(matches!(
        receive(&mut socket).await?,
        RealtimeServerMessageV1::Error { code, .. } if code == "REALTIME_AUTH_REQUIRED"
    ));

    let mut binary = connect(&url, "https://app.example").await?;
    binary.send(Message::Binary(vec![1, 2, 3].into())).await?;
    assert!(matches!(
        receive(&mut binary).await?,
        RealtimeServerMessageV1::Error { code, .. } if code == "REALTIME_TEXT_REQUIRED"
    ));

    let mut oversized = connect(&url, "https://app.example").await?;
    oversized
        .send(Message::Text("x".repeat(64 * 1024 + 1).into()))
        .await?;
    let oversized_result = tokio::time::timeout(Duration::from_secs(2), oversized.next()).await?;
    assert!(matches!(
        oversized_result,
        None | Some(Err(_) | Ok(Message::Close(_)))
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(realtime.telemetry().protocol_failures >= 1);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn delivery_lag_emits_resync_and_removes_subscription() -> Result<(), Box<dyn Error>> {
    let (url, registry, realtime, server) = fixture(Duration::from_secs(2)).await?;
    let mut socket = connect(&url, "https://app.example").await?;
    send_json(&mut socket, authenticate(RequestId::generate())).await?;
    let _ = receive(&mut socket).await?;
    send_json(&mut socket, subscribe(RequestId::generate())).await?;
    let subscription_id = match receive(&mut socket).await? {
        RealtimeServerMessageV1::State {
            subscription_id, ..
        } => subscription_id,
        message => return Err(format!("unexpected initial state: {message:?}").into()),
    };
    let initial = registry.subscribe(subscription_id)?.snapshot;
    let impact = point_impact(&initial.dependencies)?;
    for sequence in 2..=20 {
        let tickets = registry.mark_impacted(
            initial.spec.scope,
            OutboxCursor {
                commit_sequence: sequence,
                event_id: OutboxEventId::generate(),
            },
            &impact,
            TimestampMicros::new(i64::try_from(sequence)?),
        )?;
        let Some(ticket) = tickets.first() else {
            return Err("expected rerun ticket".into());
        };
        registry.complete_success(
            ticket,
            QueryOutcome {
                value: CanonicalValue::Int64(i64::try_from(sequence)?),
                snapshot_sequence: Some(sequence),
                dependencies: initial.dependencies.clone(),
            },
        )?;
    }
    assert!(matches!(
        receive(&mut socket).await?,
        RealtimeServerMessageV1::ResyncRequired { subscription_id: id, code }
            if id == subscription_id && code == "REALTIME_DELIVERY_LAGGED"
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(registry.telemetry().subscriptions, 0);
    assert_eq!(realtime.telemetry().lagged_deliveries, 1);
    socket.close(None).await?;
    server.abort();
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn subscribe_initial_unsubscribe_reconnect_and_cleanup_are_exact()
-> Result<(), Box<dyn Error>> {
    let (url, registry, realtime, server) = fixture(Duration::from_secs(2)).await?;
    let mut socket = connect(&url, "https://app.example").await?;
    let auth_request = RequestId::generate();
    send_json(&mut socket, authenticate(auth_request)).await?;
    assert!(matches!(
        receive(&mut socket).await?,
        RealtimeServerMessageV1::AuthenticationAccepted { request_id } if request_id == auth_request
    ));
    let subscribe_request = RequestId::generate();
    send_json(&mut socket, subscribe(subscribe_request)).await?;
    let subscription_id = match receive(&mut socket).await? {
        RealtimeServerMessageV1::State {
            request_id: Some(request_id),
            subscription_id,
            delivery_revision: 1,
            value: CanonicalValue::String(value),
            ..
        } if request_id == subscribe_request && value == "initial" => subscription_id,
        other => return Err(format!("unexpected state: {other:?}").into()),
    };
    assert_eq!(registry.telemetry().subscriptions, 1);

    let current = registry.subscribe(subscription_id)?.snapshot;
    let ReadDependency::Point {
        table_id,
        document_id,
        ..
    } = current.dependencies[0].clone()
    else {
        return Err("expected point dependency".into());
    };
    let impact = ChangeImpact::decode(&CanonicalValue::Object(BTreeMap::from([
        ("indexes".to_owned(), CanonicalValue::Array(Vec::new())),
        (
            "type".to_owned(),
            CanonicalValue::String("document_write_set_v2".to_owned()),
        ),
        (
            "writes".to_owned(),
            CanonicalValue::Array(vec![CanonicalValue::Object(BTreeMap::from([
                (
                    "documentId".to_owned(),
                    CanonicalValue::String(document_id.to_string()),
                ),
                (
                    "kind".to_owned(),
                    CanonicalValue::String("replace".to_owned()),
                ),
                (
                    "tableId".to_owned(),
                    CanonicalValue::String(table_id.to_string()),
                ),
            ]))]),
        ),
    ])))?;
    let tickets = registry.mark_impacted(
        current.spec.scope,
        OutboxCursor {
            commit_sequence: 2,
            event_id: OutboxEventId::generate(),
        },
        &impact,
        TimestampMicros::new(2),
    )?;
    let Some(ticket) = tickets.first() else {
        return Err("expected rerun ticket".into());
    };
    registry.complete_success(
        ticket,
        QueryOutcome {
            value: CanonicalValue::String("updated".to_owned()),
            snapshot_sequence: Some(2),
            dependencies: current.dependencies,
        },
    )?;
    assert!(matches!(
        receive(&mut socket).await?,
        RealtimeServerMessageV1::State {
            request_id: None,
            subscription_id: id,
            delivery_revision: 2,
            value: CanonicalValue::String(value),
            ..
        } if id == subscription_id && value == "updated"
    ));

    let unsubscribe_request = RequestId::generate();
    send_json(
        &mut socket,
        serde_json::json!({
            "type":"unsubscribe", "version":1, "requestId":unsubscribe_request,
            "subscriptionId":subscription_id
        }),
    )
    .await?;
    assert!(matches!(
        receive(&mut socket).await?,
        RealtimeServerMessageV1::Unsubscribed { request_id, subscription_id: id }
            if request_id == unsubscribe_request && id == subscription_id
    ));
    assert_eq!(registry.telemetry().subscriptions, 0);

    socket.close(None).await?;
    let mut reconnected = connect(&url, "https://app.example").await?;
    send_json(&mut reconnected, authenticate(RequestId::generate())).await?;
    let _ = receive(&mut reconnected).await?;
    let second_request = RequestId::generate();
    send_json(&mut reconnected, subscribe(second_request)).await?;
    let second = receive(&mut reconnected).await?;
    assert!(matches!(
        second,
        RealtimeServerMessageV1::State { request_id: Some(id), delivery_revision: 1, .. }
            if id == second_request
    ));
    reconnected.close(None).await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(registry.telemetry().subscriptions, 0);
    assert_eq!(realtime.telemetry().subscriptions, 2);
    assert_eq!(realtime.telemetry().removals, 2);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn reauthentication_rules_and_deadline_close_fail_closed() -> Result<(), Box<dyn Error>> {
    let (url, registry, realtime, server) = fixture(Duration::from_secs(1)).await?;
    let mut socket = connect(&url, "https://app.example").await?;
    send_json(&mut socket, authenticate(RequestId::generate())).await?;
    let _ = receive(&mut socket).await?;
    send_json(&mut socket, subscribe(RequestId::generate())).await?;
    let _ = receive(&mut socket).await?;
    let reauth = RequestId::generate();
    send_json(&mut socket, authenticate(reauth)).await?;
    assert!(matches!(
        receive(&mut socket).await?,
        RealtimeServerMessageV1::Error { request_id: Some(id), code, .. }
            if id == reauth && code == "REALTIME_REAUTH_REQUIRES_UNSUBSCRIBE"
    ));
    assert!(matches!(
        receive(&mut socket).await?,
        RealtimeServerMessageV1::Error { code, .. } if code == "AUTHORIZATION_EXPIRED"
    ));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(registry.telemetry().subscriptions, 0);
    assert!(realtime.telemetry().deadline_closures >= 1);
    server.abort();
    Ok(())
}

fn point_impact(dependencies: &[ReadDependency]) -> Result<ChangeImpact, Box<dyn Error>> {
    let Some(ReadDependency::Point {
        table_id,
        document_id,
        ..
    }) = dependencies.first()
    else {
        return Err("expected point dependency".into());
    };
    Ok(ChangeImpact::decode(&CanonicalValue::Object(
        BTreeMap::from([
            ("indexes".to_owned(), CanonicalValue::Array(Vec::new())),
            (
                "type".to_owned(),
                CanonicalValue::String("document_write_set_v2".to_owned()),
            ),
            (
                "writes".to_owned(),
                CanonicalValue::Array(vec![CanonicalValue::Object(BTreeMap::from([
                    (
                        "documentId".to_owned(),
                        CanonicalValue::String(document_id.to_string()),
                    ),
                    (
                        "kind".to_owned(),
                        CanonicalValue::String("replace".to_owned()),
                    ),
                    (
                        "tableId".to_owned(),
                        CanonicalValue::String(table_id.to_string()),
                    ),
                ]))]),
            ),
        ]),
    ))?)
}
