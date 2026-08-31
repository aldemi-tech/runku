//! Black-box conformance tests for the public HTTP boundary.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{Body, Bytes, to_bytes},
    http::{Request, StatusCode, header},
};
use runku_core::{CodeTarget, OperationId, ReleaseId, RequestId};
use runku_gateway::{
    CorsOrigin, GatewayFailure, GatewayHttpConfig, GatewaySuccess, InvocationContext,
    InvocationService, InvokeCallV1, build_router,
};
use runku_protocol::{
    ActionCallV1, ErrorClassV1, MutationCallV1, PUBLIC_ENVELOPE_MAX_BYTES, ProtocolError,
    PublicErrorV1, QueryCallV1, SuccessMetadataV1, decode_error_v1, decode_success_v1,
    encode_action_call_v1, encode_mutation_call_v1, encode_query_call_v1,
};
use runku_runtime::CancellationToken;
use runku_value::CanonicalValue;
use tokio::sync::Notify;
use tower::ServiceExt;

const ALLOWED_ORIGIN: &str = "https://app.example";

#[derive(Clone, Debug, Eq, PartialEq)]
struct CapturedCall {
    request_id: RequestId,
    application_key: Option<String>,
    bearer: Option<String>,
    call: InvokeCallV1,
    credentials_debug: String,
}

#[derive(Debug)]
enum Behavior {
    Success,
    Failure(PublicErrorV1),
    Sleep {
        cancellation: Arc<Mutex<Option<CancellationToken>>>,
    },
    Block {
        started: Arc<Notify>,
        release: Arc<Notify>,
    },
}

#[derive(Debug)]
struct MockService {
    behavior: Behavior,
    calls: Arc<Mutex<Vec<CapturedCall>>>,
}

impl MockService {
    fn new(behavior: Behavior) -> Self {
        Self {
            behavior,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Result<Vec<CapturedCall>, TestFailure> {
        self.calls
            .lock()
            .map(|calls| calls.clone())
            .map_err(|_| TestFailure("captured-call mutex poisoned"))
    }
}

#[async_trait]
impl InvocationService for MockService {
    async fn invoke(
        &self,
        context: InvocationContext,
        call: InvokeCallV1,
    ) -> Result<GatewaySuccess, GatewayFailure> {
        let captured = CapturedCall {
            request_id: context.request_id,
            application_key: context.credentials.application_key().map(str::to_owned),
            bearer: context.credentials.bearer().map(str::to_owned),
            credentials_debug: format!("{:?}", context.credentials),
            call: call.clone(),
        };
        if let Ok(mut calls) = self.calls.lock() {
            calls.push(captured);
        } else {
            return Err(internal_failure());
        }

        match &self.behavior {
            Behavior::Success => {}
            Behavior::Failure(error) => return Err(GatewayFailure { error: *error }),
            Behavior::Sleep { cancellation } => {
                if let Ok(mut slot) = cancellation.lock() {
                    *slot = Some(context.cancellation.clone());
                } else {
                    return Err(internal_failure());
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Behavior::Block { started, release } => {
                started.notify_one();
                release.notified().await;
            }
        }

        Ok(GatewaySuccess {
            release_id: release_id(),
            value: CanonicalValue::String("accepted".to_owned()),
            metadata: metadata_for(&call),
        })
    }
}

#[derive(Debug)]
struct TestFailure(&'static str);

impl fmt::Display for TestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestFailure {}

fn internal_failure() -> GatewayFailure {
    GatewayFailure {
        error: ProtocolError::InvalidResponse.public_error(),
    }
}

fn metadata_for(call: &InvokeCallV1) -> SuccessMetadataV1 {
    match call {
        InvokeCallV1::Query(_) => SuccessMetadataV1::Query {
            snapshot_sequence: Some(7),
        },
        InvokeCallV1::Mutation(_) => SuccessMetadataV1::Mutation {
            commit_sequence: Some(8),
            replayed: false,
            attempts: 1,
        },
        InvokeCallV1::Action(_) => SuccessMetadataV1::Action {
            schedules_created: 2,
        },
    }
}

fn release_id() -> ReleaseId {
    ReleaseId::generate()
}

fn config(concurrency: usize, timeout: Duration) -> Result<GatewayHttpConfig, Box<dyn Error>> {
    Ok(GatewayHttpConfig {
        allowed_origins: BTreeSet::from([ALLOWED_ORIGIN.parse::<CorsOrigin>()?]),
        max_concurrent_requests: concurrency,
        request_timeout: timeout,
    })
}

fn query_call() -> Result<QueryCallV1, Box<dyn Error>> {
    Ok(QueryCallV1 {
        target: CodeTarget::Release(release_id()),
        function: "notes/list".parse()?,
        arguments: CanonicalValue::Null,
    })
}

fn mutation_call() -> Result<MutationCallV1, Box<dyn Error>> {
    Ok(MutationCallV1 {
        target: CodeTarget::Release(release_id()),
        function: "notes/create".parse()?,
        arguments: CanonicalValue::Int64(42),
        operation_id: OperationId::generate(),
    })
}

fn action_call() -> Result<ActionCallV1, Box<dyn Error>> {
    Ok(ActionCallV1 {
        target: CodeTarget::Release(release_id()),
        function: "email/send".parse()?,
        arguments: CanonicalValue::Boolean(true),
    })
}

fn post(path: &str, body: Vec<u8>) -> Result<Request<Body>, Box<dyn Error>> {
    Ok(Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))?)
}

async fn body_bytes(response: axum::response::Response) -> Result<Bytes, Box<dyn Error>> {
    Ok(to_bytes(response.into_body(), PUBLIC_ENVELOPE_MAX_BYTES).await?)
}

#[tokio::test]
async fn all_call_kinds_use_v1_envelopes_and_propagate_redacted_credentials()
-> Result<(), Box<dyn Error>> {
    let service = Arc::new(MockService::new(Behavior::Success));
    let router = build_router(
        config(8, Duration::from_secs(2))?,
        Arc::clone(&service) as Arc<dyn InvocationService>,
    )?;
    let requests = [
        ("/v1/query", encode_query_call_v1(&query_call()?)?),
        ("/v1/mutation", encode_mutation_call_v1(&mutation_call()?)?),
        ("/v1/action", encode_action_call_v1(&action_call()?)?),
    ];

    for (path, body) in requests {
        let mut request = post(path, body)?;
        request
            .headers_mut()
            .insert("x-runku-key", "rk_pub_test-key".parse()?);
        request
            .headers_mut()
            .insert(header::AUTHORIZATION, "Bearer signed.jwt.value".parse()?);
        let response = router.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        let correlation = response.headers()["x-runku-request-id"]
            .to_str()?
            .to_owned();
        let decoded = decode_success_v1(&body_bytes(response).await?)?;
        assert_eq!(decoded.request_id.to_string(), correlation);
        assert!(
            decoded
                .release_id
                .to_string()
                .starts_with(ReleaseId::PREFIX)
        );
        assert_eq!(
            decoded.result,
            CanonicalValue::String("accepted".to_owned())
        );
    }

    let calls = service.calls()?;
    assert_eq!(calls.len(), 3);
    for call in calls {
        assert_eq!(call.application_key.as_deref(), Some("rk_pub_test-key"));
        assert_eq!(call.bearer.as_deref(), Some("signed.jwt.value"));
        assert!(!call.credentials_debug.contains("rk_pub_test-key"));
        assert!(!call.credentials_debug.contains("signed.jwt.value"));
        assert_eq!(
            call.request_id.to_string().len(),
            RequestId::PREFIX.len() + 26
        );
    }
    Ok(())
}

#[tokio::test]
async fn semantic_failures_are_sanitized_and_keep_service_status() -> Result<(), Box<dyn Error>> {
    let denied = PublicErrorV1::new(ErrorClassV1::Forbidden, "FUNCTION_FORBIDDEN", false)?;
    let service = Arc::new(MockService::new(Behavior::Failure(denied)));
    let router = build_router(
        config(2, Duration::from_secs(2))?,
        service as Arc<dyn InvocationService>,
    )?;
    let response = router
        .oneshot(post("/v1/query", encode_query_call_v1(&query_call()?)?)?)
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let error = decode_error_v1(&body_bytes(response).await?)?;
    assert_eq!(error.code, "FUNCTION_FORBIDDEN");
    assert_eq!(error.message, "The request is not permitted.");
    assert!(!error.retryable);
    Ok(())
}

#[tokio::test]
async fn cors_is_exact_and_preflight_accepts_only_the_public_contract() -> Result<(), Box<dyn Error>>
{
    let service = Arc::new(MockService::new(Behavior::Success));
    let router = build_router(
        config(4, Duration::from_secs(2))?,
        service as Arc<dyn InvocationService>,
    )?;
    let mut request = post("/v1/action", encode_action_call_v1(&action_call()?)?)?;
    request
        .headers_mut()
        .insert(header::ORIGIN, ALLOWED_ORIGIN.parse()?);
    let response = router.clone().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
        ALLOWED_ORIGIN
    );
    assert_eq!(response.headers()[header::VARY], "Origin");

    let preflight = Request::builder()
        .method("OPTIONS")
        .uri("/v1/query")
        .header(header::ORIGIN, ALLOWED_ORIGIN)
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "authorization, content-type, x-runku-key",
        )
        .body(Body::empty())?;
    let response = router.clone().oneshot(preflight).await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
        ALLOWED_ORIGIN
    );
    assert_eq!(response.headers()[header::ACCESS_CONTROL_MAX_AGE], "600");

    let denied = Request::builder()
        .method("OPTIONS")
        .uri("/v1/query")
        .header(header::ORIGIN, "https://attacker.example")
        .header("access-control-request-method", "POST")
        .body(Body::empty())?;
    let response = router.clone().oneshot(denied).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        !response
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN)
    );

    let invalid_headers = Request::builder()
        .method("OPTIONS")
        .uri("/v1/query")
        .header(header::ORIGIN, ALLOWED_ORIGIN)
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "x-unbounded-custom")
        .body(Body::empty())?;
    let response = router.oneshot(invalid_headers).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn malformed_methods_paths_headers_and_bodies_are_structured() -> Result<(), Box<dyn Error>> {
    let service = Arc::new(MockService::new(Behavior::Success));
    let router = build_router(
        config(4, Duration::from_secs(2))?,
        service as Arc<dyn InvocationService>,
    )?;
    let cases = [
        Request::builder()
            .method("GET")
            .uri("/v1/query")
            .body(Body::empty())?,
        Request::builder()
            .method("GET")
            .uri("/does-not-exist")
            .body(Body::empty())?,
        Request::builder()
            .method("POST")
            .uri("/v1/query")
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("{}"))?,
        Request::builder()
            .method("POST")
            .uri("/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from("{}"))?,
        Request::builder()
            .method("POST")
            .uri("/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer one")
            .header(header::AUTHORIZATION, "Bearer two")
            .body(Body::from(encode_query_call_v1(&query_call()?)?))?,
    ];
    let expected = [400_u16, 404, 400, 400, 400];
    for (request, expected_status) in cases.into_iter().zip(expected) {
        let response = router.clone().oneshot(request).await?;
        assert_eq!(response.status().as_u16(), expected_status);
        assert!(response.headers().contains_key("x-runku-request-id"));
        decode_error_v1(&body_bytes(response).await?)?;
    }

    let oversized = vec![b' '; PUBLIC_ENVELOPE_MAX_BYTES + 1];
    let response = router
        .clone()
        .oneshot(post("/v1/query", oversized)?)
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        decode_error_v1(&body_bytes(response).await?)?.code,
        "PROTOCOL_LIMIT_EXCEEDED"
    );

    let huge_header = "x".repeat(17 * 1024);
    let mut request = post("/v1/query", encode_query_call_v1(&query_call()?)?)?;
    request
        .headers_mut()
        .insert("x-extra", huge_header.parse()?);
    let response = router.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    Ok(())
}

#[tokio::test]
async fn timeout_signals_the_same_cancellation_token() -> Result<(), Box<dyn Error>> {
    let cancellation = Arc::new(Mutex::new(None));
    let service = Arc::new(MockService::new(Behavior::Sleep {
        cancellation: Arc::clone(&cancellation),
    }));
    let router = build_router(
        config(1, Duration::from_millis(20))?,
        service as Arc<dyn InvocationService>,
    )?;
    let response = router
        .oneshot(post("/v1/action", encode_action_call_v1(&action_call()?)?)?)
        .await?;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        decode_error_v1(&body_bytes(response).await?)?.code,
        "GATEWAY_TIMEOUT"
    );
    let cancelled = cancellation
        .lock()
        .map_err(|_| TestFailure("cancellation mutex poisoned"))?
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled);
    assert!(cancelled);
    Ok(())
}

#[tokio::test]
async fn admission_rejects_excess_work_without_queueing() -> Result<(), Box<dyn Error>> {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let service = Arc::new(MockService::new(Behavior::Block {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    }));
    let router = build_router(
        config(1, Duration::from_secs(2))?,
        Arc::clone(&service) as Arc<dyn InvocationService>,
    )?;
    let first_request = post("/v1/query", encode_query_call_v1(&query_call()?)?)?;
    let first_router = router.clone();
    let first = tokio::spawn(async move { first_router.oneshot(first_request).await });
    started.notified().await;

    let response = router
        .oneshot(post("/v1/query", encode_query_call_v1(&query_call()?)?)?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        decode_error_v1(&body_bytes(response).await?)?.code,
        "GATEWAY_BUSY"
    );
    assert_eq!(service.calls()?.len(), 1);

    release.notify_one();
    let first_result = first.await.map_err(|_| TestFailure("first task failed"))?;
    let first_response = match first_result {
        Ok(response) => response,
        Err(error) => match error {},
    };
    assert_eq!(first_response.status(), StatusCode::OK);
    Ok(())
}

#[test]
fn configuration_and_origins_are_strict() -> Result<(), Box<dyn Error>> {
    for origin in [
        "https://app.example/",
        "https://app.example/path",
        "https://user@app.example",
        "ftp://app.example",
    ] {
        assert!(origin.parse::<CorsOrigin>().is_err(), "accepted {origin}");
    }
    assert!(config(0, Duration::from_secs(1))?.validate().is_err());
    assert!(config(1, Duration::ZERO)?.validate().is_err());
    assert!(config(1, Duration::from_secs(301))?.validate().is_err());
    assert!(config(1, Duration::from_secs(1))?.validate().is_ok());
    Ok(())
}
