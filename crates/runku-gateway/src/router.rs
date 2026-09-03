//! Axum router, admission, CORS, timeout, and response hardening.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{
        DefaultBodyLimit, Extension, Path, Request, State, WebSocketUpgrade,
        rejection::BytesRejection,
    },
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures_util::StreamExt;
use runku_core::RequestId;
use runku_file_storage::FileStorageService;
use runku_protocol::{
    ErrorClassV1, PUBLIC_ENVELOPE_MAX_BYTES, ProtocolError, PublicErrorV1, decode_action_call_v1,
    decode_mutation_call_v1, decode_query_call_v1, encode_error_v1, encode_success_v1,
};
use runku_runtime::CancellationToken;
use tokio::{net::TcpListener, sync::Semaphore};

use crate::websocket::UpgradeFailure;
use crate::{
    CorsOrigin, GatewayFailure, InvocationContext, InvocationService, InvokeCallV1,
    PresentedCredentials, RealtimeGateway,
};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_HEADER_VALUES: usize = 64;
const MAX_APPLICATION_KEY_BYTES: usize = 256;
const MAX_BEARER_BYTES: usize = 16 * 1024;

/// Validated process-local HTTP safety and browser-origin policy.
#[derive(Clone, Debug)]
pub struct GatewayHttpConfig {
    /// Exact browser origins; requests without Origin remain valid server-to-server calls.
    pub allowed_origins: BTreeSet<CorsOrigin>,
    /// Maximum concurrently executing HTTP requests; excess fails immediately.
    pub max_concurrent_requests: usize,
    /// End-to-end handler deadline after routing begins.
    pub request_timeout: Duration,
}

impl GatewayHttpConfig {
    /// Validates hard v1 configuration bounds.
    ///
    /// # Errors
    ///
    /// Rejects more than 128 origins, zero/excessive concurrency, or timeout outside 1ms–5min.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.allowed_origins.len() > 128
            || !(1..=100_000).contains(&self.max_concurrent_requests)
            || self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > Duration::from_mins(5)
        {
            return Err(ProtocolError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct GatewayState {
    service: Arc<dyn InvocationService>,
    config: GatewayHttpConfig,
    admission: Arc<Semaphore>,
    realtime: Option<RealtimeGateway>,
    files: Option<Arc<FileStorageService>>,
}

/// Builds a Router with strict endpoints, body limit, boundary middleware, and fallback.
///
/// # Errors
///
/// Rejects invalid HTTP policy before accepting requests.
pub fn build_router(
    config: GatewayHttpConfig,
    service: Arc<dyn InvocationService>,
) -> Result<Router, ProtocolError> {
    build_router_inner(config, service, None, None)
}

/// Builds the same HTTP Router plus the strict public Realtime WebSocket endpoint.
///
/// # Errors
///
/// Rejects invalid HTTP policy before accepting requests.
pub fn build_router_with_realtime(
    config: GatewayHttpConfig,
    service: Arc<dyn InvocationService>,
    realtime: RealtimeGateway,
) -> Result<Router, ProtocolError> {
    build_router_inner(config, service, Some(realtime), None)
}

/// Builds the public Function Router plus capability-token file transfer endpoints.
///
/// # Errors
///
/// Rejects invalid HTTP policy before accepting requests.
pub fn build_router_with_files(
    config: GatewayHttpConfig,
    service: Arc<dyn InvocationService>,
    files: Arc<FileStorageService>,
) -> Result<Router, ProtocolError> {
    build_router_inner(config, service, None, Some(files))
}

/// Builds the public Function/Realtime Router plus capability-token file transfer endpoints.
///
/// # Errors
///
/// Rejects invalid HTTP policy before accepting requests.
pub fn build_router_with_realtime_and_files(
    config: GatewayHttpConfig,
    service: Arc<dyn InvocationService>,
    realtime: RealtimeGateway,
    files: Arc<FileStorageService>,
) -> Result<Router, ProtocolError> {
    build_router_inner(config, service, Some(realtime), Some(files))
}

fn build_router_inner(
    config: GatewayHttpConfig,
    service: Arc<dyn InvocationService>,
    realtime: Option<RealtimeGateway>,
    files: Option<Arc<FileStorageService>>,
) -> Result<Router, ProtocolError> {
    config.validate()?;
    let state = GatewayState {
        admission: Arc::new(Semaphore::new(config.max_concurrent_requests)),
        service,
        config,
        realtime,
        files,
    };
    let mut router = Router::new()
        .route("/v1/query", post(query).options(preflight))
        .route("/v1/mutation", post(mutation).options(preflight))
        .route("/v1/action", post(action).options(preflight));
    if state.realtime.is_some() {
        router = router.route("/v1/realtime", get(realtime_upgrade));
    }
    if state.files.is_some() {
        let file_routes = Router::new()
            .route(
                "/v1/files/uploads/{upload_id}",
                put(file_upload).options(file_preflight),
            )
            .route(
                "/v1/files/downloads/{file_id}",
                get(file_download)
                    .head(file_download_head)
                    .options(file_preflight),
            )
            .layer(DefaultBodyLimit::disable());
        router = router.merge(file_routes);
    }
    Ok(router
        .fallback(fallback)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(PUBLIC_ENVELOPE_MAX_BYTES))
        .layer(middleware::from_fn_with_state(state.clone(), boundary))
        .with_state(state))
}

async fn file_upload(
    State(state): State<GatewayState>,
    Path(upload_id): Path<String>,
    Extension(request_id): Extension<RequestId>,
    Extension(cancellation): Extension<CancellationToken>,
    request: Request,
) -> Response {
    let Some(files) = state.files else {
        return protocol_failure(request_id, ProtocolError::InvalidRequest);
    };
    if request.headers().contains_key(header::CONTENT_ENCODING) {
        return protocol_failure(request_id, ProtocolError::InvalidRequest);
    }
    let token = match parse_file_token(request.headers()) {
        Ok(token) => token,
        Err(error) => return protocol_failure(request_id, error),
    };
    let content_length = match parse_content_length(request.headers()) {
        Ok(value) => value,
        Err(error) => return protocol_failure(request_id, error),
    };
    let content_type = match optional_single_header(request.headers(), &header::CONTENT_TYPE) {
        Ok(value) => value,
        Err(error) => return protocol_failure(request_id, error),
    };
    let stream = request
        .into_body()
        .into_data_stream()
        .map(|result| result.map_err(|_| runku_runtime::FileStorageError::InvalidRequest));
    match files
        .upload_http(
            &upload_id,
            &token,
            content_length,
            content_type.as_deref(),
            Box::pin(stream),
            Instant::now() + state.config.request_timeout,
            cancellation,
        )
        .await
    {
        Ok(metadata) => file_json_response(
            StatusCode::CREATED,
            &serde_json::json!({ "version": 1, "status": "ok", "file": metadata }),
        ),
        Err(error) => file_failure(request_id, error),
    }
}

async fn file_download(
    State(state): State<GatewayState>,
    Path(file_id): Path<String>,
    Extension(request_id): Extension<RequestId>,
    Extension(cancellation): Extension<CancellationToken>,
    headers: HeaderMap,
) -> Response {
    file_download_inner(state, file_id, request_id, cancellation, headers, false).await
}

async fn file_download_head(
    State(state): State<GatewayState>,
    Path(file_id): Path<String>,
    Extension(request_id): Extension<RequestId>,
    Extension(cancellation): Extension<CancellationToken>,
    headers: HeaderMap,
) -> Response {
    file_download_inner(state, file_id, request_id, cancellation, headers, true).await
}

async fn file_download_inner(
    state: GatewayState,
    file_id: String,
    request_id: RequestId,
    cancellation: CancellationToken,
    headers: HeaderMap,
    head_only: bool,
) -> Response {
    let Some(files) = state.files else {
        return protocol_failure(request_id, ProtocolError::InvalidRequest);
    };
    let token = match parse_file_token(&headers) {
        Ok(token) => token,
        Err(error) => return protocol_failure(request_id, error),
    };
    let range = match parse_range(&headers) {
        Ok(range) => range,
        Err(error) => return protocol_failure(request_id, error),
    };
    match files
        .download_http(
            &file_id,
            &token,
            range.clone(),
            Instant::now() + state.config.request_timeout,
            cancellation,
        )
        .await
    {
        Ok(download) => {
            let partial = range.is_some();
            let length = download.range.end.saturating_sub(download.range.start);
            let body = if head_only {
                Body::empty()
            } else {
                Body::from_stream(download.stream)
            };
            let mut response = Response::new(body);
            *response.status_mut() = if partial {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            };
            let response_headers = response.headers_mut();
            if let Ok(value) = HeaderValue::from_str(&download.metadata.content_type) {
                response_headers.insert(header::CONTENT_TYPE, value);
            }
            if let Ok(value) = HeaderValue::from_str(&length.to_string()) {
                response_headers.insert(header::CONTENT_LENGTH, value);
            }
            if let Ok(value) = HeaderValue::from_str(&format!("\"{}\"", download.metadata.sha256)) {
                response_headers.insert(header::ETAG, value);
            }
            response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            response_headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("private, no-store"),
            );
            response_headers.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_static("attachment"),
            );
            if partial
                && let Ok(value) = HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    download.range.start,
                    download.range.end.saturating_sub(1),
                    download.metadata.size_bytes
                ))
            {
                response_headers.insert(header::CONTENT_RANGE, value);
            }
            response
        }
        Err(error) => file_failure(request_id, error),
    }
}

async fn file_preflight(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Response {
    let method = match single_header(
        &headers,
        &HeaderName::from_static("access-control-request-method"),
    ) {
        Ok(value) => value,
        Err(error) => return protocol_failure(request_id, error),
    };
    if headers.get(header::ORIGIN).is_none()
        || !matches!(method.as_deref(), Some("PUT" | "GET" | "HEAD"))
        || !valid_file_preflight_headers(&headers)
    {
        return protocol_failure(request_id, ProtocolError::InvalidRequest);
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("PUT, GET, HEAD, OPTIONS"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type, content-length, range"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("content-length, content-range, etag, x-runku-request-id"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
    response
}

fn valid_file_preflight_headers(headers: &HeaderMap) -> bool {
    let Ok(requested) = single_header(
        headers,
        &HeaderName::from_static("access-control-request-headers"),
    ) else {
        return false;
    };
    requested.is_none_or(|value| {
        value.split(',').all(|name| {
            matches!(
                name.trim().to_ascii_lowercase().as_str(),
                "authorization" | "content-type" | "content-length" | "range"
            )
        })
    })
}

fn parse_file_token(headers: &HeaderMap) -> Result<String, ProtocolError> {
    let authorization =
        single_header(headers, &header::AUTHORIZATION)?.ok_or(ProtocolError::InvalidRequest)?;
    let token = authorization
        .strip_prefix("Bearer ")
        .ok_or(ProtocolError::InvalidRequest)?;
    if token.is_empty() || token.len() > 1024 || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(ProtocolError::InvalidRequest);
    }
    Ok(token.to_owned())
}

fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>, ProtocolError> {
    optional_single_header(headers, &header::CONTENT_LENGTH)?
        .map(|value| {
            if value == "0"
                || value.starts_with('0')
                || !value.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(ProtocolError::InvalidRequest);
            }
            value.parse().map_err(|_| ProtocolError::LimitExceeded)
        })
        .transpose()
}

fn parse_range(headers: &HeaderMap) -> Result<Option<std::ops::Range<u64>>, ProtocolError> {
    let Some(value) = optional_single_header(headers, &header::RANGE)? else {
        return Ok(None);
    };
    let value = value
        .strip_prefix("bytes=")
        .ok_or(ProtocolError::InvalidRequest)?;
    if value.contains(',') {
        return Err(ProtocolError::InvalidRequest);
    }
    let (start, end) = value.split_once('-').ok_or(ProtocolError::InvalidRequest)?;
    if start.is_empty()
        || end.is_empty()
        || start.starts_with('0') && start != "0"
        || end.starts_with('0') && end != "0"
    {
        return Err(ProtocolError::InvalidRequest);
    }
    let start = start
        .parse::<u64>()
        .map_err(|_| ProtocolError::InvalidRequest)?;
    let end = end
        .parse::<u64>()
        .map_err(|_| ProtocolError::InvalidRequest)?;
    if end < start {
        return Err(ProtocolError::InvalidRequest);
    }
    Ok(Some(
        start..end.checked_add(1).ok_or(ProtocolError::LimitExceeded)?,
    ))
}

fn optional_single_header(
    headers: &HeaderMap,
    name: &HeaderName,
) -> Result<Option<String>, ProtocolError> {
    single_header(headers, name)
}

fn file_json_response(status: StatusCode, value: &serde_json::Value) -> Response {
    match serde_json::to_vec(&value) {
        Ok(body) => json_response(status, body),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn file_failure(request_id: RequestId, error: runku_runtime::FileStorageError) -> Response {
    let class = match error {
        runku_runtime::FileStorageError::InvalidRequest => ErrorClassV1::InvalidRequest,
        runku_runtime::FileStorageError::NotFound => ErrorClassV1::NotFound,
        runku_runtime::FileStorageError::Conflict => ErrorClassV1::Conflict,
        runku_runtime::FileStorageError::LimitExceeded => ErrorClassV1::LimitExceeded,
        runku_runtime::FileStorageError::Forbidden => ErrorClassV1::Forbidden,
        runku_runtime::FileStorageError::Unavailable => ErrorClassV1::Unavailable,
        runku_runtime::FileStorageError::Timeout | runku_runtime::FileStorageError::Cancelled => {
            ErrorClassV1::Timeout
        }
        runku_runtime::FileStorageError::Corruption => ErrorClassV1::Internal,
    };
    public_failure(
        request_id,
        public_error(
            class,
            error.code(),
            matches!(
                error,
                runku_runtime::FileStorageError::Unavailable
                    | runku_runtime::FileStorageError::Timeout
            ),
        ),
    )
}

async fn realtime_upgrade(
    State(state): State<GatewayState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Response {
    let Some(realtime) = state.realtime else {
        return protocol_failure(request_id, ProtocolError::InvalidRequest);
    };
    if RealtimeGateway::validate_upgrade(&websocket, &headers).is_err() {
        return protocol_failure(request_id, ProtocolError::InvalidRequest);
    }
    match realtime.upgrade(websocket) {
        Ok(response) => response,
        Err(UpgradeFailure::Busy) => public_failure(
            request_id,
            public_error(ErrorClassV1::Busy, "REALTIME_BUSY", true),
        ),
        Err(UpgradeFailure::Invalid) => protocol_failure(request_id, ProtocolError::InvalidRequest),
    }
}

/// Serves a validated Router on an already-bound listener.
///
/// TLS termination, socket ownership, and graceful-shutdown orchestration remain operator/binary
/// concerns; the Product Base listener does not read ambient addresses.
///
/// # Errors
///
/// Returns the underlying listener/server I/O error.
pub async fn serve(listener: TcpListener, router: Router) -> std::io::Result<()> {
    axum::serve(listener, router).await
}

async fn query(
    State(state): State<GatewayState>,
    Extension(request_id): Extension<RequestId>,
    Extension(cancellation): Extension<CancellationToken>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    invoke(state, request_id, cancellation, headers, body, |bytes| {
        decode_query_call_v1(bytes).map(InvokeCallV1::Query)
    })
    .await
}

async fn mutation(
    State(state): State<GatewayState>,
    Extension(request_id): Extension<RequestId>,
    Extension(cancellation): Extension<CancellationToken>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    invoke(state, request_id, cancellation, headers, body, |bytes| {
        decode_mutation_call_v1(bytes).map(InvokeCallV1::Mutation)
    })
    .await
}

async fn action(
    State(state): State<GatewayState>,
    Extension(request_id): Extension<RequestId>,
    Extension(cancellation): Extension<CancellationToken>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    invoke(state, request_id, cancellation, headers, body, |bytes| {
        decode_action_call_v1(bytes).map(InvokeCallV1::Action)
    })
    .await
}

async fn invoke<F>(
    state: GatewayState,
    request_id: RequestId,
    cancellation: CancellationToken,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
    decode: F,
) -> Response
where
    F: FnOnce(&[u8]) -> Result<InvokeCallV1, ProtocolError>,
{
    if !valid_content_type(&headers) || headers.contains_key(header::CONTENT_ENCODING) {
        return protocol_failure(request_id, ProtocolError::InvalidRequest);
    }
    let body = match body {
        Ok(body) => body,
        Err(rejection) => {
            let error = if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
                ProtocolError::LimitExceeded
            } else {
                ProtocolError::InvalidRequest
            };
            return protocol_failure(request_id, error);
        }
    };
    let call = match decode(&body) {
        Ok(call) => call,
        Err(error) => return protocol_failure(request_id, error),
    };
    let credentials = match parse_credentials(&headers) {
        Ok(credentials) => credentials,
        Err(error) => return protocol_failure(request_id, error),
    };
    let context = InvocationContext {
        request_id,
        credentials,
        cancellation,
    };
    match state.service.invoke(context, call).await {
        Ok(success) => match encode_success_v1(
            request_id,
            success.release_id,
            &success.value,
            success.metadata,
        ) {
            Ok(body) => json_response(StatusCode::OK, body),
            Err(error) => protocol_failure(request_id, error),
        },
        Err(GatewayFailure { error }) => public_failure(request_id, error),
    }
}

async fn boundary(State(state): State<GatewayState>, mut request: Request, next: Next) -> Response {
    let request_id = RequestId::generate();
    let origin = match validate_request_headers(request.headers(), &state.config) {
        Ok(origin) => origin,
        Err(error) => return decorate(protocol_failure(request_id, error), request_id, None),
    };
    let Ok(permit) = Arc::clone(&state.admission).try_acquire_owned() else {
        let error = public_error(ErrorClassV1::Busy, "GATEWAY_BUSY", true);
        return decorate(
            public_failure(request_id, error),
            request_id,
            origin.as_deref(),
        );
    };
    let cancellation = CancellationToken::new();
    request.extensions_mut().insert(request_id);
    request.extensions_mut().insert(cancellation.clone());
    let mut guard = CancelOnDrop::new(cancellation);
    let response = if let Ok(response) =
        tokio::time::timeout(state.config.request_timeout, next.run(request)).await
    {
        guard.disarm();
        response
    } else {
        let error = public_error(ErrorClassV1::Timeout, "GATEWAY_TIMEOUT", true);
        public_failure(request_id, error)
    };
    drop(permit);
    decorate(response, request_id, origin.as_deref())
}

async fn preflight(Extension(request_id): Extension<RequestId>, headers: HeaderMap) -> Response {
    let requested_method = match single_header(
        &headers,
        &HeaderName::from_static("access-control-request-method"),
    ) {
        Ok(value) => value,
        Err(error) => return protocol_failure(request_id, error),
    };
    if headers.get(header::ORIGIN).is_none()
        || requested_method.as_deref() != Some("POST")
        || !valid_preflight_headers(&headers)
    {
        return protocol_failure(request_id, ProtocolError::InvalidRequest);
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("POST, OPTIONS"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type, x-runku-key"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
    response
}

fn valid_preflight_headers(headers: &HeaderMap) -> bool {
    let Ok(requested) = single_header(
        headers,
        &HeaderName::from_static("access-control-request-headers"),
    ) else {
        return false;
    };
    requested.is_none_or(|value| {
        value.split(',').all(|name| {
            matches!(
                name.trim().to_ascii_lowercase().as_str(),
                "authorization" | "content-type" | "x-runku-key"
            )
        })
    })
}

async fn fallback(Extension(request_id): Extension<RequestId>) -> Response {
    public_failure(
        request_id,
        public_error(ErrorClassV1::NotFound, "ROUTE_NOT_FOUND", false),
    )
}

async fn method_not_allowed(Extension(request_id): Extension<RequestId>) -> Response {
    public_failure(
        request_id,
        public_error(ErrorClassV1::InvalidRequest, "METHOD_NOT_ALLOWED", false),
    )
}

fn parse_credentials(headers: &HeaderMap) -> Result<PresentedCredentials, ProtocolError> {
    let application_key = single_header(headers, &HeaderName::from_static("x-runku-key"))?;
    if application_key.as_ref().is_some_and(|value| {
        value.is_empty()
            || value.len() > MAX_APPLICATION_KEY_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
    }) {
        return Err(ProtocolError::InvalidRequest);
    }
    let authorization = single_header(headers, &header::AUTHORIZATION)?;
    let bearer = authorization
        .map(|value| {
            let token = value
                .strip_prefix("Bearer ")
                .ok_or(ProtocolError::InvalidRequest)?;
            if token.is_empty()
                || token.len() > MAX_BEARER_BYTES
                || !token.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(ProtocolError::InvalidRequest);
            }
            Ok(token.to_owned())
        })
        .transpose()?;
    Ok(PresentedCredentials::new(application_key, bearer))
}

fn single_header(headers: &HeaderMap, name: &HeaderName) -> Result<Option<String>, ProtocolError> {
    let values = headers.get_all(name).iter().collect::<Vec<_>>();
    if values.len() > 1 {
        return Err(ProtocolError::InvalidRequest);
    }
    values
        .first()
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| ProtocolError::InvalidRequest)
        })
        .transpose()
}

fn valid_content_type(headers: &HeaderMap) -> bool {
    let values = headers
        .get_all(header::CONTENT_TYPE)
        .iter()
        .collect::<Vec<_>>();
    if values.len() != 1 {
        return false;
    }
    values[0].to_str().is_ok_and(|value| {
        value == "application/json" || value.eq_ignore_ascii_case("application/json; charset=utf-8")
    })
}

fn validate_request_headers(
    headers: &HeaderMap,
    config: &GatewayHttpConfig,
) -> Result<Option<String>, ProtocolError> {
    let mut bytes = 0_usize;
    let mut values = 0_usize;
    for (name, value) in headers {
        values = values.checked_add(1).ok_or(ProtocolError::LimitExceeded)?;
        bytes = bytes
            .checked_add(name.as_str().len())
            .and_then(|total| total.checked_add(value.as_bytes().len()))
            .ok_or(ProtocolError::LimitExceeded)?;
        if values > MAX_HEADER_VALUES || bytes > MAX_HEADER_BYTES {
            return Err(ProtocolError::LimitExceeded);
        }
    }
    let origin = single_header(headers, &header::ORIGIN)?;
    if let Some(origin) = &origin
        && !config
            .allowed_origins
            .iter()
            .any(|allowed| allowed.as_str() == origin)
    {
        return Err(ProtocolError::InvalidRequest);
    }
    Ok(origin)
}

fn protocol_failure(request_id: RequestId, error: ProtocolError) -> Response {
    public_failure(request_id, error.public_error())
}

fn public_failure(request_id: RequestId, error: PublicErrorV1) -> Response {
    let status =
        StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match encode_error_v1(request_id, error) {
        Ok(body) => json_response(status, body),
        Err(_) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "{{\"version\":1,\"status\":\"error\",\"requestId\":\"{request_id}\",\"error\":{{\"code\":\"GATEWAY_INTERNAL\",\"message\":\"The request failed unexpectedly.\",\"retryable\":false}}}}"
            )
            .into_bytes(),
        ),
    }
}

fn public_error(class: ErrorClassV1, code: &'static str, retryable: bool) -> PublicErrorV1 {
    match PublicErrorV1::new(class, code, retryable) {
        Ok(error) => error,
        Err(_) => ProtocolError::InvalidResponse.public_error(),
    }
}

fn json_response(status: StatusCode, body: Vec<u8>) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn decorate(mut response: Response, request_id: RequestId, origin: Option<&str>) -> Response {
    if let Ok(value) = HeaderValue::from_str(&request_id.to_string()) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-runku-request-id"), value);
    }
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    if let Some(origin) = origin
        && let Ok(value) = HeaderValue::from_str(origin)
    {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
        response.headers_mut().insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static(
                "content-length, content-range, content-type, etag, x-runku-file-id, x-runku-file-sha256, x-runku-request-id",
            ),
        );
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Origin"));
    }
    response
}

struct CancelOnDrop {
    cancellation: CancellationToken,
    armed: bool,
}

impl CancelOnDrop {
    const fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            armed: true,
        }
    }

    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}
