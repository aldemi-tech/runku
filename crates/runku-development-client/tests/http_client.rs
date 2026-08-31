//! Adversarial real-socket coverage for the strict Remote Workspace client.

use std::{
    collections::VecDeque,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::Response,
    routing::post,
};
use runku_core::{
    BuildId, DevRevisionId, DevelopmentCredentialId, EnvironmentDescriptor, EnvironmentId,
    EnvironmentScope, FunctionId, OperationId, ProjectId, ReleaseId, RequestId,
};
use runku_development_access::DevelopmentKeyCrypto;
use runku_development_client::{
    DevelopmentClient, DevelopmentClientConfig, DevelopmentClientError, DevelopmentEndpoint,
};
use runku_protocol::{
    DEVELOPMENT_JSON_MAX_BYTES, DevelopmentAdminErrorCodeV1, DevelopmentPublishRequestV1,
    DevelopmentPublishResponseV1, DevelopmentStateRequestV1, DevelopmentStateResponseV1,
    encode_development_error_v1, encode_development_publish_request_v1,
    encode_development_publish_response_v1, encode_development_state_response_v1,
};
use runku_releases::{
    AuthPolicy, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_release_manifest, encode_safe_esm_bundle,
};
use runku_value::TimestampMicros;
use tokio::{net::TcpListener, task::JoinHandle};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const JSON_RESPONSE: &str = "application/json; charset=utf-8";

#[derive(Clone)]
struct MockReply {
    status: StatusCode,
    request_id: RequestId,
    body: Vec<u8>,
    content_type: Option<&'static str>,
    retry_after: Option<&'static str>,
    duplicate_request_id: bool,
    location: Option<&'static str>,
    delay: Duration,
}

impl MockReply {
    fn json(status: StatusCode, request_id: RequestId, body: Vec<u8>) -> Self {
        Self {
            status,
            request_id,
            body,
            content_type: Some(JSON_RESPONSE),
            retry_after: None,
            duplicate_request_id: false,
            location: None,
            delay: Duration::ZERO,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Received {
    uri: String,
    authorization: Option<String>,
    content_type: Option<String>,
    body: Vec<u8>,
}

struct MockState {
    replies: Mutex<VecDeque<MockReply>>,
    received: Mutex<Vec<Received>>,
    redirected: AtomicUsize,
}

struct MockServer {
    endpoint: DevelopmentEndpoint,
    state: Arc<MockState>,
    task: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn mock_handler(
    State(state): State<Arc<MockState>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    if let Ok(mut received) = state.received.lock() {
        received.push(Received {
            uri: uri.to_string(),
            authorization: single_header(&headers, header::AUTHORIZATION),
            content_type: single_header(&headers, header::CONTENT_TYPE),
            body: body.to_vec(),
        });
    }
    let reply = state
        .replies
        .lock()
        .ok()
        .and_then(|mut replies| replies.pop_front());
    let Some(reply) = reply else {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap_or_else(|_| Response::new(Body::empty()));
    };
    if !reply.delay.is_zero() {
        tokio::time::sleep(reply.delay).await;
    }
    let mut response = Response::builder().status(reply.status);
    if let Some(content_type) = reply.content_type {
        response = response.header(header::CONTENT_TYPE, content_type);
    }
    response = response.header("x-runku-request-id", reply.request_id.to_string());
    if reply.duplicate_request_id {
        response = response.header("x-runku-request-id", RequestId::generate().to_string());
    }
    if let Some(retry_after) = reply.retry_after {
        response = response.header(header::RETRY_AFTER, retry_after);
    }
    if let Some(location) = reply.location {
        response = response.header(header::LOCATION, location);
    }
    response
        .body(Body::from(reply.body))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

async fn redirected(State(state): State<Arc<MockState>>) -> StatusCode {
    state.redirected.fetch_add(1, Ordering::Relaxed);
    StatusCode::NO_CONTENT
}

fn single_header(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?.to_str().ok()?.to_owned();
    (values.next().is_none()).then_some(first)
}

async fn spawn(replies: Vec<MockReply>) -> TestResult<MockServer> {
    let state = Arc::new(MockState {
        replies: Mutex::new(replies.into()),
        received: Mutex::new(Vec::new()),
        redirected: AtomicUsize::new(0),
    });
    let app = Router::new()
        .route("/v1/development/state", post(mock_handler))
        .route("/v1/development/workspaces", post(mock_handler))
        .route("/v1/development/publish", post(mock_handler))
        .route("/redirected", post(redirected))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let _result = axum::serve(listener, app).await;
    });
    Ok(MockServer {
        endpoint: format!("http://{address}").parse()?,
        state,
        task,
    })
}

fn token() -> TestResult<String> {
    let generated =
        DevelopmentKeyCrypto::new([73; 32]).generate(DevelopmentCredentialId::generate())?;
    Ok(generated.key.expose().to_owned())
}

fn config(maximum_attempts: u8) -> DevelopmentClientConfig {
    DevelopmentClientConfig {
        request_timeout: Duration::from_secs(1),
        maximum_attempts,
        retry_delay: Duration::ZERO,
    }
}

fn state_response(request_id: RequestId) -> DevelopmentStateResponseV1 {
    let project_id = ProjectId::generate();
    let environment_id = EnvironmentId::generate();
    DevelopmentStateResponseV1 {
        request_id,
        scope: EnvironmentScope::new(project_id, environment_id),
        environment: EnvironmentDescriptor::local_development(environment_id),
        development_revision: 7,
        workspace: None,
    }
}

fn package() -> TestResult<DevelopmentPublishRequestV1> {
    let source = "export default async (_ctx, value) => value;";
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact_bytes = encode_safe_esm_bundle(&bundle)?;
    let contract = Sha256Digest::of(b"client-http-contract");
    let project_id = ProjectId::generate();
    let manifest = ReleaseManifestV1 {
        release_id: ReleaseId::generate(),
        project_id,
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_800_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: contract,
        schema_contract_hash: contract,
        index_contract_hash: contract,
        functions: vec![FunctionManifest {
            id: FunctionId::generate(),
            name: "queries.echo".parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: contract,
            result_contract_hash: contract,
            capabilities: vec![],
        }],
        cron_definitions: vec![],
    };
    Ok(DevelopmentPublishRequestV1 {
        operation_id: OperationId::generate(),
        project_id,
        workspace_ref: "dev/client".parse()?,
        expected_head: None,
        manifest_bytes: encode_release_manifest(&manifest)?,
        manifest,
        artifact_bytes,
    })
}

#[test]
fn endpoint_config_and_debug_are_strict_and_redacted() -> TestResult {
    for rejected in [
        "http://localhost:8000",
        "http://192.168.1.2:8000",
        "https://example.com/",
        "https://example.com/path",
        "https://user@example.com",
        "https://example.com?query=1",
        "HTTPS://example.com",
    ] {
        assert_eq!(
            rejected.parse::<DevelopmentEndpoint>(),
            Err(DevelopmentClientError::InvalidConfig),
            "accepted {rejected}"
        );
    }
    let endpoint: DevelopmentEndpoint = "https://example.com".parse()?;
    let bearer = token()?;
    let secret_fragment = bearer.clone();
    let client = DevelopmentClient::new(endpoint, bearer, config(1))?;
    let debug = format!("{client:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(&secret_fragment));
    assert_eq!(
        DevelopmentClientConfig {
            request_timeout: Duration::ZERO,
            maximum_attempts: 0,
            retry_delay: Duration::from_secs(31),
        }
        .validate(),
        Err(DevelopmentClientError::InvalidConfig)
    );
    Ok(())
}

#[tokio::test]
async fn publish_retries_the_identical_frame_and_headers_after_uncertain() -> TestResult {
    let request = package()?;
    let expected_frame = encode_development_publish_request_v1(&request)?;
    let first_id = RequestId::generate();
    let second_id = RequestId::generate();
    let error_body =
        encode_development_error_v1(first_id, DevelopmentAdminErrorCodeV1::ResultUncertain)?;
    let success_body = encode_development_publish_response_v1(&DevelopmentPublishResponseV1 {
        request_id: second_id,
        revision_id: DevRevisionId::generate(),
        release_id: request.manifest.release_id,
        manifest_digest: Sha256Digest::of(&request.manifest_bytes),
        development_revision: 8,
        replayed: true,
    })?;
    let mut uncertain = MockReply::json(StatusCode::SERVICE_UNAVAILABLE, first_id, error_body);
    uncertain.retry_after = Some("0");
    let server = spawn(vec![
        uncertain,
        MockReply::json(StatusCode::OK, second_id, success_body),
    ])
    .await?;
    let bearer = token()?;
    let expected_authorization = format!("Bearer {bearer}");
    let client = DevelopmentClient::new(server.endpoint.clone(), bearer, config(2))?;
    let response = client.publish(&request).await?;
    assert!(response.replayed);
    let received = server
        .state
        .received
        .lock()
        .map_err(|_| "received mutex poisoned")?;
    assert_eq!(received.len(), 2);
    for attempt in &*received {
        assert_eq!(attempt.uri, "/v1/development/publish");
        assert_eq!(
            attempt.content_type.as_deref(),
            Some("application/vnd.runku.development-publish-v1")
        );
        assert_eq!(
            attempt.authorization.as_deref(),
            Some(expected_authorization.as_str())
        );
        assert_eq!(attempt.body, expected_frame);
    }
    let telemetry = client.telemetry();
    assert_eq!(telemetry.attempts, 2);
    assert_eq!(telemetry.retries, 1);
    assert_eq!(telemetry.successes, 1);
    assert_eq!(telemetry.exhausted, 0);
    Ok(())
}

#[tokio::test]
async fn redirect_is_not_followed_and_response_contract_is_fail_closed() -> TestResult {
    let request_id = RequestId::generate();
    let mut redirect = MockReply::json(StatusCode::TEMPORARY_REDIRECT, request_id, Vec::new());
    redirect.content_type = None;
    redirect.location = Some("/redirected");
    let server = spawn(vec![redirect]).await?;
    let client = DevelopmentClient::new(server.endpoint.clone(), token()?, config(1))?;
    let request = DevelopmentStateRequestV1 {
        workspace_ref: "dev/client".parse()?,
    };
    assert_eq!(
        client.state(&request).await,
        Err(DevelopmentClientError::InvalidResponse)
    );
    assert_eq!(server.state.redirected.load(Ordering::Relaxed), 0);
    assert_eq!(client.telemetry().invalid_responses, 1);
    Ok(())
}

#[tokio::test]
async fn oversized_duplicate_mismatched_and_noncanonical_responses_are_rejected() -> TestResult {
    let request = DevelopmentStateRequestV1 {
        workspace_ref: "dev/client".parse()?,
    };
    let body_id = RequestId::generate();
    let valid_body = encode_development_state_response_v1(&state_response(body_id))?;

    let oversized = MockReply::json(
        StatusCode::OK,
        body_id,
        vec![b'x'; DEVELOPMENT_JSON_MAX_BYTES + 1],
    );
    let mut duplicate = MockReply::json(StatusCode::OK, body_id, valid_body.clone());
    duplicate.duplicate_request_id = true;
    let mismatched = MockReply::json(StatusCode::OK, RequestId::generate(), valid_body.clone());
    let mut wrong_type = MockReply::json(StatusCode::OK, body_id, valid_body.clone());
    wrong_type.content_type = Some("application/json");
    let malformed = MockReply::json(StatusCode::OK, body_id, b"{}".to_vec());

    for reply in [oversized, duplicate, mismatched, wrong_type, malformed] {
        let server = spawn(vec![reply]).await?;
        let client = DevelopmentClient::new(server.endpoint.clone(), token()?, config(1))?;
        assert_eq!(
            client.state(&request).await,
            Err(DevelopmentClientError::InvalidResponse)
        );
    }
    Ok(())
}

#[tokio::test]
async fn timeouts_are_sanitized_and_mutation_outcome_is_uncertain() -> TestResult {
    let request_id = RequestId::generate();
    let body = encode_development_state_response_v1(&state_response(request_id))?;
    let mut slow = MockReply::json(StatusCode::OK, request_id, body);
    slow.delay = Duration::from_millis(100);
    let server = spawn(vec![slow]).await?;
    let mut short = config(1);
    short.request_timeout = Duration::from_millis(10);
    let client = DevelopmentClient::new(server.endpoint.clone(), token()?, short)?;
    assert_eq!(
        client
            .state(&DevelopmentStateRequestV1 {
                workspace_ref: "dev/client".parse()?,
            })
            .await,
        Err(DevelopmentClientError::Unavailable)
    );
    assert_eq!(client.telemetry().exhausted, 1);

    let publish = package()?;
    let response_id = RequestId::generate();
    let response_body = encode_development_publish_response_v1(&DevelopmentPublishResponseV1 {
        request_id: response_id,
        revision_id: DevRevisionId::generate(),
        release_id: publish.manifest.release_id,
        manifest_digest: Sha256Digest::of(&publish.manifest_bytes),
        development_revision: 9,
        replayed: false,
    })?;
    let mut slow = MockReply::json(StatusCode::OK, response_id, response_body);
    slow.delay = Duration::from_millis(100);
    let server = spawn(vec![slow]).await?;
    let client = DevelopmentClient::new(server.endpoint.clone(), token()?, short)?;
    assert_eq!(
        client.publish(&publish).await,
        Err(DevelopmentClientError::ResultUncertain)
    );
    Ok(())
}

#[test]
fn duplicate_header_builder_really_preserves_two_values() {
    let mut headers = HeaderMap::new();
    headers.append(
        "x-runku-request-id",
        HeaderValue::from_static("req_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
    );
    headers.append(
        "x-runku-request-id",
        HeaderValue::from_static("req_01ARZ3NDEKTSV4RRFFQ69G5FAW"),
    );
    assert_eq!(headers.get_all("x-runku-request-id").iter().count(), 2);
}
