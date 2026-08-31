//! Strict administrative HTTP transport, admission, deadlines, and response hardening.

use std::{future::Future, sync::Arc, time::Duration};

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Request, State, rejection::BytesRejection},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse as _, Response},
    routing::post,
};
use runku_core::RequestId;
use runku_protocol::{
    DEVELOPMENT_JSON_MAX_BYTES, DEVELOPMENT_PUBLISH_MAX_BYTES, ProtocolError,
    decode_development_create_request_v1, decode_development_freeze_request_v1,
    decode_development_publish_request_v1, decode_development_state_request_v1,
    encode_development_create_response_v1, encode_development_error_v1,
    encode_development_freeze_response_v1, encode_development_publish_response_v1,
    encode_development_state_response_v1,
};
use tokio::{net::TcpListener, sync::Semaphore};
use zeroize::Zeroizing;

use crate::{DevelopmentServiceError, RemoteWorkspaceService};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_HEADER_VALUES: usize = 64;
const MAX_BEARER_BYTES: usize = 256;
const JSON_CONTENT_TYPE: &str = "application/json";
const PUBLISH_CONTENT_TYPE: &str = "application/vnd.runku.development-publish-v1";

/// Explicit network exposure for an administrative listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentHttpExposure {
    /// The passed listener is plaintext and therefore must be bound to a loopback address.
    LoopbackPlaintext,
    /// A trusted operator-owned boundary terminates TLS before this listener.
    TrustedTlsTermination,
}

/// Strict bounded administrative HTTP policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DevelopmentHttpConfig {
    /// Maximum concurrently running semantic tasks. Timed-out mutations retain their permit until
    /// durable reconciliation finishes in the detached task.
    pub max_concurrent_requests: usize,
    /// Client-visible response deadline.
    pub request_timeout: Duration,
    /// Explicit network exposure; plaintext is accepted only on loopback.
    pub exposure: DevelopmentHttpExposure,
}

impl DevelopmentHttpConfig {
    /// Validates fixed v1 safety bounds.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive admission or deadlines outside 1ms through five minutes.
    pub fn validate(self) -> Result<(), DevelopmentServiceError> {
        if !(1..=100_000).contains(&self.max_concurrent_requests)
            || self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > Duration::from_mins(5)
        {
            return Err(DevelopmentServiceError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct HttpState {
    service: Arc<RemoteWorkspaceService>,
    config: DevelopmentHttpConfig,
    admission: Arc<Semaphore>,
}

/// Builds exact state/create/publish routes with per-operation body bounds and no browser CORS.
///
/// # Errors
///
/// Rejects invalid admission/deadline configuration before a listener accepts traffic.
pub fn build_development_router(
    config: DevelopmentHttpConfig,
    service: Arc<RemoteWorkspaceService>,
) -> Result<Router, DevelopmentServiceError> {
    config.validate()?;
    let state = HttpState {
        service,
        config,
        admission: Arc::new(Semaphore::new(config.max_concurrent_requests)),
    };
    Ok(Router::new()
        .route(
            "/v1/development/state",
            post(state_handler).layer(DefaultBodyLimit::max(DEVELOPMENT_JSON_MAX_BYTES)),
        )
        .route(
            "/v1/development/workspaces",
            post(create_handler).layer(DefaultBodyLimit::max(DEVELOPMENT_JSON_MAX_BYTES)),
        )
        .route(
            "/v1/development/publish",
            post(publish_handler).layer(DefaultBodyLimit::max(DEVELOPMENT_PUBLISH_MAX_BYTES)),
        )
        .route(
            "/v1/development/freeze",
            post(freeze_handler).layer(DefaultBodyLimit::max(DEVELOPMENT_JSON_MAX_BYTES)),
        )
        .fallback(fallback)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn(boundary))
        .with_state(state))
}

/// Serves on an already-bound listener and drains accepted requests after shutdown is signalled.
/// Plain HTTP listeners are restricted to loopback; public exposure requires an explicit trusted
/// TLS termination mode.
///
/// # Errors
///
/// Returns invalid input for a non-loopback plaintext listener or the server I/O failure.
pub async fn serve_development<F>(
    listener: TcpListener,
    router: Router,
    exposure: DevelopmentHttpExposure,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    if exposure == DevelopmentHttpExposure::LoopbackPlaintext
        && !listener.local_addr()?.ip().is_loopback()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "plaintext development listener must be loopback",
        ));
    }
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

async fn state_handler(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    if !exact_content_type(&headers, JSON_CONTENT_TYPE) {
        return failure(request_id, DevelopmentServiceError::InvalidRequest);
    }
    let body = match body_bytes(body) {
        Ok(body) => body,
        Err(error) => return failure(request_id, error),
    };
    let request = match decode_development_state_request_v1(&body) {
        Ok(request) => request,
        Err(error) => return protocol_failure(request_id, error),
    };
    let bearer = match bearer(&headers) {
        Ok(value) => Zeroizing::new(value),
        Err(error) => return failure(request_id, error),
    };
    let Some(permit) = acquire(&state) else {
        return failure(request_id, DevelopmentServiceError::Busy);
    };
    let service = Arc::clone(&state.service);
    let task = tokio::spawn(async move {
        let _permit = permit;
        service.state(request_id, &bearer, request).await
    });
    match tokio::time::timeout(state.config.request_timeout, task).await {
        Ok(Ok(Ok(response))) => match encode_development_state_response_v1(&response) {
            Ok(body) => json(StatusCode::OK, body),
            Err(_) => failure(request_id, DevelopmentServiceError::Internal),
        },
        Ok(Ok(Err(error))) => failure(request_id, error),
        Ok(Err(_)) => failure(request_id, DevelopmentServiceError::Internal),
        Err(_) => {
            state.service.record_deadline_response();
            failure(request_id, DevelopmentServiceError::Unavailable)
        }
    }
}

async fn create_handler(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    if !exact_content_type(&headers, JSON_CONTENT_TYPE) {
        return failure(request_id, DevelopmentServiceError::InvalidRequest);
    }
    let body = match body_bytes(body) {
        Ok(body) => body,
        Err(error) => return failure(request_id, error),
    };
    let request = match decode_development_create_request_v1(&body) {
        Ok(request) => request,
        Err(error) => return protocol_failure(request_id, error),
    };
    let bearer = match bearer(&headers) {
        Ok(value) => Zeroizing::new(value),
        Err(error) => return failure(request_id, error),
    };
    let Some(permit) = acquire(&state) else {
        return failure(request_id, DevelopmentServiceError::Busy);
    };
    let service = Arc::clone(&state.service);
    let task = tokio::spawn(async move {
        let _permit = permit;
        service.create_workspace(request_id, &bearer, request).await
    });
    match tokio::time::timeout(state.config.request_timeout, task).await {
        Ok(Ok(Ok(response))) => match encode_development_create_response_v1(&response) {
            Ok(body) => json(StatusCode::CREATED, body),
            Err(_) => failure(request_id, DevelopmentServiceError::Internal),
        },
        Ok(Ok(Err(error))) => failure(request_id, error),
        Ok(Err(_)) => failure(request_id, DevelopmentServiceError::Internal),
        Err(_) => {
            state.service.record_deadline_response();
            failure(request_id, DevelopmentServiceError::ResultUncertain)
        }
    }
}

async fn publish_handler(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    if !exact_content_type(&headers, PUBLISH_CONTENT_TYPE) {
        return failure(request_id, DevelopmentServiceError::InvalidRequest);
    }
    let body = match body_bytes(body) {
        Ok(body) => body,
        Err(error) => return failure(request_id, error),
    };
    let request = match decode_development_publish_request_v1(&body) {
        Ok(request) => request,
        Err(error) => return protocol_failure(request_id, error),
    };
    let bearer = match bearer(&headers) {
        Ok(value) => Zeroizing::new(value),
        Err(error) => return failure(request_id, error),
    };
    let Some(permit) = acquire(&state) else {
        return failure(request_id, DevelopmentServiceError::Busy);
    };
    let service = Arc::clone(&state.service);
    let task = tokio::spawn(async move {
        let _permit = permit;
        service.publish(request_id, &bearer, request).await
    });
    match tokio::time::timeout(state.config.request_timeout, task).await {
        Ok(Ok(Ok(response))) => match encode_development_publish_response_v1(&response) {
            Ok(body) => json(StatusCode::OK, body),
            Err(_) => failure(request_id, DevelopmentServiceError::Internal),
        },
        Ok(Ok(Err(error))) => failure(request_id, error),
        Ok(Err(_)) => failure(request_id, DevelopmentServiceError::Internal),
        Err(_) => {
            state.service.record_deadline_response();
            failure(request_id, DevelopmentServiceError::ResultUncertain)
        }
    }
}

async fn freeze_handler(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    if !exact_content_type(&headers, JSON_CONTENT_TYPE) {
        return failure(request_id, DevelopmentServiceError::InvalidRequest);
    }
    let body = match body_bytes(body) {
        Ok(body) => body,
        Err(error) => return failure(request_id, error),
    };
    let request = match decode_development_freeze_request_v1(&body) {
        Ok(request) => request,
        Err(error) => return protocol_failure(request_id, error),
    };
    let bearer = match bearer(&headers) {
        Ok(value) => Zeroizing::new(value),
        Err(error) => return failure(request_id, error),
    };
    let Some(permit) = acquire(&state) else {
        return failure(request_id, DevelopmentServiceError::Busy);
    };
    let service = Arc::clone(&state.service);
    let task = tokio::spawn(async move {
        let _permit = permit;
        service.freeze(request_id, &bearer, request).await
    });
    match tokio::time::timeout(state.config.request_timeout, task).await {
        Ok(Ok(Ok(response))) => match encode_development_freeze_response_v1(&response) {
            Ok(body) => json(StatusCode::OK, body),
            Err(_) => failure(request_id, DevelopmentServiceError::Internal),
        },
        Ok(Ok(Err(error))) => failure(request_id, error),
        Ok(Err(_)) => failure(request_id, DevelopmentServiceError::Internal),
        Err(_) => {
            state.service.record_deadline_response();
            failure(request_id, DevelopmentServiceError::ResultUncertain)
        }
    }
}

fn acquire(state: &HttpState) -> Option<tokio::sync::OwnedSemaphorePermit> {
    if let Ok(permit) = Arc::clone(&state.admission).try_acquire_owned() {
        Some(permit)
    } else {
        state.service.record_admission_rejection();
        None
    }
}

async fn boundary(mut request: Request, next: Next) -> Response {
    let request_id = RequestId::generate();
    if request.uri().query().is_some()
        || validate_headers(request.headers()).is_err()
        || request.headers().contains_key(header::ORIGIN)
        || request.headers().contains_key(header::COOKIE)
        || request.headers().contains_key(header::CONTENT_ENCODING)
        || request
            .headers()
            .contains_key(HeaderName::from_static("x-runku-key"))
    {
        return decorate(
            failure(request_id, DevelopmentServiceError::InvalidRequest),
            request_id,
        );
    }
    request.extensions_mut().insert(request_id);
    decorate(next.run(request).await, request_id)
}

async fn fallback(Extension(request_id): Extension<RequestId>) -> Response {
    failure(request_id, DevelopmentServiceError::NotFound)
}

async fn method_not_allowed(Extension(request_id): Extension<RequestId>) -> Response {
    failure(request_id, DevelopmentServiceError::InvalidRequest)
}

fn validate_headers(headers: &HeaderMap) -> Result<(), DevelopmentServiceError> {
    let mut bytes = 0_usize;
    let mut values = 0_usize;
    for (name, value) in headers {
        values = values
            .checked_add(1)
            .ok_or(DevelopmentServiceError::LimitExceeded)?;
        bytes = bytes
            .checked_add(name.as_str().len())
            .and_then(|total| total.checked_add(value.as_bytes().len()))
            .ok_or(DevelopmentServiceError::LimitExceeded)?;
        if values > MAX_HEADER_VALUES || bytes > MAX_HEADER_BYTES {
            return Err(DevelopmentServiceError::LimitExceeded);
        }
    }
    Ok(())
}

fn bearer(headers: &HeaderMap) -> Result<String, DevelopmentServiceError> {
    let authorization = single_header(headers, &header::AUTHORIZATION)?
        .ok_or(DevelopmentServiceError::Unauthenticated)?;
    let token = authorization
        .strip_prefix("Bearer ")
        .ok_or(DevelopmentServiceError::Unauthenticated)?;
    if token.is_empty()
        || token.len() > MAX_BEARER_BYTES
        || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(DevelopmentServiceError::Unauthenticated);
    }
    Ok(token.to_owned())
}

fn single_header(
    headers: &HeaderMap,
    name: &HeaderName,
) -> Result<Option<String>, DevelopmentServiceError> {
    let values = headers.get_all(name).iter().collect::<Vec<_>>();
    if values.len() > 1 {
        return Err(DevelopmentServiceError::InvalidRequest);
    }
    values
        .first()
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| DevelopmentServiceError::InvalidRequest)
        })
        .transpose()
}

fn exact_content_type(headers: &HeaderMap, expected: &str) -> bool {
    single_header(headers, &header::CONTENT_TYPE)
        .is_ok_and(|value| value.as_deref() == Some(expected))
}

fn body_bytes(body: Result<Bytes, BytesRejection>) -> Result<Bytes, DevelopmentServiceError> {
    body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            DevelopmentServiceError::LimitExceeded
        } else {
            DevelopmentServiceError::InvalidRequest
        }
    })
}

fn protocol_failure(request_id: RequestId, error: ProtocolError) -> Response {
    let service_error = match error {
        ProtocolError::LimitExceeded => DevelopmentServiceError::LimitExceeded,
        _ => DevelopmentServiceError::InvalidRequest,
    };
    failure(request_id, service_error)
}

fn failure(request_id: RequestId, error: DevelopmentServiceError) -> Response {
    let status = match error {
        DevelopmentServiceError::InvalidRequest => StatusCode::BAD_REQUEST,
        DevelopmentServiceError::Unauthenticated => StatusCode::UNAUTHORIZED,
        DevelopmentServiceError::Forbidden | DevelopmentServiceError::PolicyDenied => {
            StatusCode::FORBIDDEN
        }
        DevelopmentServiceError::NotFound => StatusCode::NOT_FOUND,
        DevelopmentServiceError::Conflict => StatusCode::CONFLICT,
        DevelopmentServiceError::LimitExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        DevelopmentServiceError::Busy => StatusCode::TOO_MANY_REQUESTS,
        DevelopmentServiceError::Unavailable | DevelopmentServiceError::ResultUncertain => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        DevelopmentServiceError::Corruption | DevelopmentServiceError::Internal => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    match encode_development_error_v1(request_id, error.wire()) {
        Ok(body) => {
            let mut response = json(status, body);
            if error == DevelopmentServiceError::Unauthenticated {
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Bearer realm=\"runku-development\""),
                );
            }
            if error.wire().retryable() {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            }
            response
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn json(status: StatusCode, body: Vec<u8>) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

fn decorate(mut response: Response, request_id: RequestId) -> Response {
    if let Ok(value) = HeaderValue::from_str(&request_id.to_string()) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-runku-request-id"), value);
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}
