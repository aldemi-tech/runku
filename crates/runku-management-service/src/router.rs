//! Platform Identity HTTP routes and strict transport boundary.

use std::{
    collections::BTreeSet, convert::Infallible, future::Future, str::FromStr as _, sync::Arc,
    time::SystemTime,
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse as _, Response},
    routing::{delete, get, post, put},
};
use futures_util::stream;
use runku_core::{EnvironmentId, EnvironmentScope, OperatorSessionId, ProjectId};
use runku_platform_identity::{
    AccessScope, AccessToken, DeviceName, ExternalOperatorIdentity, InvitationCode, LoginResult,
    OperatorContext, OperatorName, OperatorRole, PlatformCapability, PlatformIdentityError,
    PlatformIdentityService, RefreshToken,
};
use runku_protocol::DEVELOPMENT_PUBLISH_MAX_BYTES;
use runku_value::TimestampMicros;
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use zeroize::Zeroizing;

use crate::{
    ManagementLogPruneRequest, ManagementLogQuery, ManagementProduct, ManagementProductError,
    OidcClientConfiguration,
};

const MAX_BODY_BYTES: usize = 16 * 1024;
const MAX_AUTHORIZATION_BYTES: usize = 16 * 1024;

/// Explicit exposure policy for a plaintext listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementHttpExposure {
    /// Plain HTTP accepted only on literal loopback.
    LoopbackPlaintext,
    /// An operator-owned trusted boundary terminates TLS before this listener.
    TrustedTlsTermination,
}

/// Bounded Management API transport configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagementHttpConfig {
    /// Maximum concurrent semantic requests.
    pub max_concurrent_requests: usize,
    /// Listener exposure policy.
    pub exposure: ManagementHttpExposure,
    /// Optional canonical public Management API origin returned during login discovery.
    /// When absent, clients use the exact origin they queried.
    pub public_management_endpoint: Option<String>,
}

impl ManagementHttpConfig {
    fn validate(&self) -> Result<(), PlatformIdentityError> {
        if !(1..=100_000).contains(&self.max_concurrent_requests) {
            return Err(PlatformIdentityError::InvalidInput);
        }
        if let Some(endpoint) = &self.public_management_endpoint {
            validate_public_endpoint(endpoint)?;
        }
        Ok(())
    }
}

/// Optional configured external `IdP` boundary. Implementations must return only a verified,
/// normalized, token-free identity.
#[async_trait]
pub trait ExternalIdentityAuthenticator: std::fmt::Debug + Send + Sync {
    /// Verifies one external bearer using a configured issuer/audience/JWKS policy.
    async fn authenticate(
        &self,
        bearer: &str,
        now: TimestampMicros,
    ) -> Result<ExternalOperatorIdentity, PlatformIdentityError>;
}

#[derive(Clone)]
struct HttpState {
    identity: Arc<PlatformIdentityService>,
    external: Option<Arc<dyn ExternalIdentityAuthenticator>>,
    product: Option<Arc<dyn ManagementProduct>>,
    oidc_client: Option<OidcClientConfiguration>,
    public_management_endpoint: Option<String>,
    admission: Arc<Semaphore>,
}

/// Builds the versioned login, refresh, session, and invitation routes.
///
/// # Errors
///
/// Rejects invalid transport bounds before the listener accepts traffic.
pub fn build_management_router(
    config: ManagementHttpConfig,
    identity: Arc<PlatformIdentityService>,
    external: Option<Arc<dyn ExternalIdentityAuthenticator>>,
) -> Result<Router, PlatformIdentityError> {
    build_management_router_with_product(config, identity, external, None, None)
}

/// Builds Platform Identity plus authenticated product lifecycle and log routes.
///
/// # Errors
///
/// Rejects invalid transport bounds before the listener accepts traffic.
pub fn build_management_router_with_product(
    config: ManagementHttpConfig,
    identity: Arc<PlatformIdentityService>,
    external: Option<Arc<dyn ExternalIdentityAuthenticator>>,
    product: Option<Arc<dyn ManagementProduct>>,
    oidc_client: Option<OidcClientConfiguration>,
) -> Result<Router, PlatformIdentityError> {
    config.validate()?;
    if oidc_client.is_some() && external.is_none() {
        return Err(PlatformIdentityError::InvalidInput);
    }
    if let Some(client) = &oidc_client {
        validate_oidc_client(client)?;
    }
    let state = HttpState {
        identity,
        external,
        product,
        oidc_client,
        public_management_endpoint: config.public_management_endpoint,
        admission: Arc::new(Semaphore::new(config.max_concurrent_requests)),
    };
    Ok(Router::new()
        .route("/v1/auth/exchange", post(exchange))
        .route("/v1/auth/refresh", post(refresh))
        .route("/v1/auth/oidc", post(oidc))
        .route("/v1/auth/config", get(auth_config))
        .route("/v1/auth/oidc/config", get(oidc_config))
        .route("/v1/auth/me", get(me))
        .route("/v1/auth/sessions", get(sessions))
        .route("/v1/auth/sessions/{session_id}", delete(revoke_session))
        .route("/v1/access/invitations", post(invite))
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/workspace/publish",
            post(product_publish).layer(DefaultBodyLimit::max(DEVELOPMENT_PUBLISH_MAX_BYTES)),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/releases/{release_id}",
            post(product_release),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/channels/{channel}",
            put(product_promote),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/channels/{channel}/rollback",
            post(product_rollback),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/status",
            get(product_status),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/logs",
            get(product_logs),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/logs/follow",
            get(product_logs_follow),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/logs/archive-status",
            get(product_log_archive_status),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/logs/prune",
            post(product_log_prune),
        )
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .fallback(fallback)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthenticationConfigurationResponse<'a> {
    version: u8,
    methods: Vec<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    management_endpoint: Option<&'a str>,
}

async fn auth_config(State(state): State<HttpState>) -> Response {
    let mut methods = vec!["invitationCode"];
    if state.external.is_some() {
        methods.push("oidcToken");
        if state.oidc_client.is_some() {
            methods.insert(0, "oidcBrowser");
        }
    }
    json(
        StatusCode::OK,
        &AuthenticationConfigurationResponse {
            version: 1,
            methods,
            management_endpoint: state.public_management_endpoint.as_deref(),
        },
        false,
    )
}

async fn oidc_config(State(state): State<HttpState>) -> Response {
    match (&state.external, &state.oidc_client) {
        (Some(_), Some(config)) => json(StatusCode::OK, config, false),
        _ => failure(PlatformIdentityError::NotFound),
    }
}

fn validate_public_endpoint(value: &str) -> Result<(), PlatformIdentityError> {
    let endpoint = url::Url::parse(value).map_err(|_| PlatformIdentityError::InvalidInput)?;
    let loopback = endpoint
        .host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback());
    if endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
        || endpoint.origin().ascii_serialization() != value
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    Ok(())
}

fn validate_oidc_client(config: &OidcClientConfiguration) -> Result<(), PlatformIdentityError> {
    if config.client_id.is_empty()
        || config.client_id.len() > 256
        || config.scopes.is_empty()
        || config.scopes.len() > 16
        || !config.scopes.iter().any(|scope| scope == "openid")
        || config.scopes.iter().collect::<BTreeSet<_>>().len() != config.scopes.len()
        || config.scopes.iter().any(|scope| {
            scope.is_empty()
                || scope.len() > 128
                || scope.chars().any(char::is_whitespace)
                || scope.chars().any(char::is_control)
        })
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let issuer =
        url::Url::parse(&config.issuer).map_err(|_| PlatformIdentityError::InvalidInput)?;
    if issuer.scheme() != "https"
        || issuer.host_str().is_none()
        || !issuer.username().is_empty()
        || issuer.password().is_some()
        || issuer.query().is_some()
        || issuer.fragment().is_some()
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    for endpoint in [&config.authorization_endpoint, &config.token_endpoint] {
        let endpoint =
            url::Url::parse(endpoint).map_err(|_| PlatformIdentityError::InvalidInput)?;
        let loopback = endpoint
            .host_str()
            .and_then(|host| host.parse::<std::net::IpAddr>().ok())
            .is_some_and(|address| address.is_loopback());
        if endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
        {
            return Err(PlatformIdentityError::InvalidInput);
        }
    }
    if let Some(resource) = &config.resource {
        validate_secure_resource(resource)?;
    }
    Ok(())
}

fn validate_secure_resource(value: &str) -> Result<(), PlatformIdentityError> {
    if value.is_empty() || value.len() > 2_048 {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let resource = url::Url::parse(value).map_err(|_| PlatformIdentityError::InvalidInput)?;
    let loopback = resource
        .host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback());
    if resource.host_str().is_none()
        || !resource.username().is_empty()
        || resource.password().is_some()
        || resource.fragment().is_some()
        || !(resource.scheme() == "https" || resource.scheme() == "http" && loopback)
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReleaseRequest {
    against: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PromoteRequest {
    release_id: String,
    expected: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RollbackRequest {
    expected: String,
    target: String,
}

async fn product_publish(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, context) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::ReleasesPublish,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let actor = format!("operator-{}", context.operator.id).to_ascii_lowercase();
    match product.publish(&actor, &body).await {
        Ok(result) => json(StatusCode::CREATED, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_release(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, release)): Path<(String, String, String)>,
    Json(request): Json<ReleaseRequest>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::ReleasesPublish,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.release(&release, request.against.as_deref()).await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_promote(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, channel)): Path<(String, String, String)>,
    Json(request): Json<PromoteRequest>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::ChannelsPromote,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let expected = request
        .expected
        .as_deref()
        .map(|value| if value == "empty" { None } else { Some(value) });
    match product
        .promote(&channel, &request.release_id, expected)
        .await
    {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_rollback(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, channel)): Path<(String, String, String)>,
    Json(request): Json<RollbackRequest>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::ChannelsPromote,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .rollback(&channel, &request.expected, &request.target)
        .await
    {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_status(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::ReleasesRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.status().await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_logs(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Query(query): Query<ManagementLogQuery>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::LogsRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.logs(&query).await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_log_archive_status(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::LogsRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.log_archive_status().await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_log_prune(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Json(request): Json<ManagementLogPruneRequest>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::LogsPrune,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.log_prune(&request).await {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_logs_follow(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Query(query): Query<ManagementLogQuery>,
) -> Response {
    let Ok(permit) = state.admission.clone().try_acquire_owned() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::LogsFollow,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let token = match bearer(&headers) {
        Ok(token) => Zeroizing::new(token),
        Err(error) => return failure(error),
    };
    let scope = product.scope();
    let stream_state = (state.identity.clone(), product, token, query, false, permit);
    let body_stream = stream::unfold(stream_state, move |mut current| async move {
        if current.3.limit == 0 {
            return None;
        }
        if current.4 {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        current.4 = true;
        let authorized =
            AccessToken::from_str(&current.2).map_err(|_| PlatformIdentityError::Unauthenticated);
        let authorized = match authorized {
            Ok(token) => current.identity().authenticate(&token, now()).await,
            Err(error) => Err(error),
        }
        .and_then(|context| {
            context.authorize(
                AccessScope::Environment(scope),
                PlatformCapability::LogsFollow,
            )
        });
        if authorized.is_err() {
            let bytes =
                Bytes::from_static(b"{\"error\":{\"code\":\"PLATFORM_UNAUTHENTICATED\"}}\n");
            return Some((Ok::<Bytes, Infallible>(bytes), current.with_done()));
        }
        match current.1.logs(&current.3).await {
            Ok(page) => {
                current.3.after = page.next;
                current.4 = page.records.is_empty();
                let mut bytes = Vec::new();
                for record in page.records {
                    if serde_json::to_writer(&mut bytes, &record).is_err() {
                        return None;
                    }
                    bytes.push(b'\n');
                }
                Some((Ok(Bytes::from(bytes)), current))
            }
            Err(_) => Some((
                Ok(Bytes::from_static(
                    b"{\"error\":{\"code\":\"PLATFORM_LOG_STREAM_UNAVAILABLE\"}}\n",
                )),
                current.with_done(),
            )),
        }
    });
    let mut response = Response::new(Body::from_stream(body_stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

trait FollowState {
    fn identity(&self) -> &Arc<PlatformIdentityService>;
    fn with_done(self) -> Self;
}

impl FollowState
    for (
        Arc<PlatformIdentityService>,
        Arc<dyn ManagementProduct>,
        Zeroizing<String>,
        ManagementLogQuery,
        bool,
        OwnedSemaphorePermit,
    )
{
    fn identity(&self) -> &Arc<PlatformIdentityService> {
        &self.0
    }

    fn with_done(mut self) -> Self {
        self.3.limit = 0;
        self
    }
}

async fn product_context(
    state: &HttpState,
    headers: &HeaderMap,
    project: &str,
    environment: &str,
    capability: PlatformCapability,
) -> Result<(Arc<dyn ManagementProduct>, OperatorContext), Box<Response>> {
    let context = authenticate(state, headers)
        .await
        .map_err(|error| Box::new(failure(error)))?;
    let project = project
        .parse::<ProjectId>()
        .map_err(|_| Box::new(failure(PlatformIdentityError::InvalidInput)))?;
    let environment = environment
        .parse::<EnvironmentId>()
        .map_err(|_| Box::new(failure(PlatformIdentityError::InvalidInput)))?;
    let scope = EnvironmentScope::new(project, environment);
    context
        .authorize(AccessScope::Environment(scope), capability)
        .map_err(|error| Box::new(failure(error)))?;
    let product = state
        .product
        .clone()
        .ok_or_else(|| Box::new(failure(PlatformIdentityError::NotFound)))?;
    if product.scope() != scope {
        return Err(Box::new(failure(PlatformIdentityError::NotFound)));
    }
    Ok((product, context))
}

fn product_failure(error: ManagementProductError) -> Response {
    let (status, code) = match error {
        ManagementProductError::Invalid => (StatusCode::BAD_REQUEST, "PRODUCT_REQUEST_INVALID"),
        ManagementProductError::NotFound => (StatusCode::NOT_FOUND, "PRODUCT_NOT_FOUND"),
        ManagementProductError::Conflict => (StatusCode::CONFLICT, "PRODUCT_CONFLICT"),
        ManagementProductError::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "PRODUCT_UNAVAILABLE")
        }
        ManagementProductError::Corruption => {
            (StatusCode::INTERNAL_SERVER_ERROR, "PRODUCT_CORRUPT")
        }
    };
    let mut response = json(status, &serde_json::json!({"code": code}), false);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// Serves a Management API router with bounded graceful shutdown.
///
/// # Errors
///
/// Rejects a non-loopback plaintext listener and propagates listener/server I/O failures.
pub async fn serve_management<F>(
    listener: TcpListener,
    router: Router,
    exposure: ManagementHttpExposure,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    if exposure == ManagementHttpExposure::LoopbackPlaintext
        && !listener.local_addr()?.ip().is_loopback()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "plaintext management listener must be loopback",
        ));
    }
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExchangeRequest {
    code: String,
    device_name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RefreshRequest {
    refresh_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OidcRequest {
    device_name: String,
    invitation_code: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InviteRequest {
    operator_name: String,
    role: String,
    scope: ScopeRequest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScopeRequest {
    kind: String,
    project_id: Option<String>,
    environment_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponse {
    access_token: String,
    refresh_token: String,
    operator_id: String,
    session_id: String,
    authorization_revision: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MeResponse {
    operator_id: String,
    name: String,
    session_id: String,
    device_name: String,
    authorization_revision: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InvitationResponse {
    code: String,
    secret_shown_once: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionResponse {
    session_id: String,
    device_name: String,
    status: &'static str,
    created_at_micros: i64,
    last_used_at_micros: i64,
    access_expires_at_micros: i64,
    refresh_expires_at_micros: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionsResponse {
    sessions: Vec<SessionResponse>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorResponse {
    code: &'static str,
}

async fn exchange(
    State(state): State<HttpState>,
    Json(request): Json<ExchangeRequest>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let code = Zeroizing::new(request.code);
    let code = match InvitationCode::from_str(&code) {
        Ok(code) => code,
        Err(error) => return failure(error),
    };
    let device = match DeviceName::from_str(&request.device_name) {
        Ok(device) => device,
        Err(error) => return failure(error),
    };
    match state
        .identity
        .login_with_invitation(&code, device, None, now())
        .await
    {
        Ok(result) => login_response(&result),
        Err(error) => failure(error),
    }
}

async fn refresh(State(state): State<HttpState>, Json(request): Json<RefreshRequest>) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let token = Zeroizing::new(request.refresh_token);
    let token = match RefreshToken::from_str(&token) {
        Ok(token) => token,
        Err(error) => return failure(error),
    };
    match state.identity.refresh(&token, now()).await {
        Ok(result) => login_response(&result),
        Err(error) => failure(error),
    }
}

async fn oidc(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<OidcRequest>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let Some(authenticator) = &state.external else {
        return failure(PlatformIdentityError::NotFound);
    };
    let bearer = match bearer(&headers) {
        Ok(value) => Zeroizing::new(value),
        Err(error) => return failure(error),
    };
    let timestamp = now();
    let identity = match authenticator.authenticate(&bearer, timestamp).await {
        Ok(identity) => identity,
        Err(error) => return failure(error),
    };
    let device = match DeviceName::from_str(&request.device_name) {
        Ok(device) => device,
        Err(error) => return failure(error),
    };
    let result = if let Some(code) = request.invitation_code {
        let code = Zeroizing::new(code);
        match InvitationCode::from_str(&code) {
            Ok(code) => {
                state
                    .identity
                    .login_with_invitation(&code, device, Some(identity), timestamp)
                    .await
            }
            Err(error) => Err(error),
        }
    } else {
        state
            .identity
            .login_with_external_identity(&identity, device, timestamp)
            .await
    };
    match result {
        Ok(result) => login_response(&result),
        Err(error) => failure(error),
    }
}

async fn me(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    let context = match authenticate(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return failure(error),
    };
    json(
        StatusCode::OK,
        &MeResponse {
            operator_id: context.operator.id.to_string(),
            name: context.operator.name.to_string(),
            session_id: context.session.id.to_string(),
            device_name: context.session.device_name.to_string(),
            authorization_revision: context.operator.authorization_revision,
        },
        false,
    )
}

async fn sessions(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    let actor = match authenticate(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return failure(error),
    };
    match state.identity.list_sessions(&actor).await {
        Ok(sessions) => json(
            StatusCode::OK,
            &SessionsResponse {
                sessions: sessions
                    .into_iter()
                    .map(|session| SessionResponse {
                        session_id: session.id.to_string(),
                        device_name: session.device_name.to_string(),
                        status: match session.status {
                            runku_platform_identity::SessionStatus::Active => "active",
                            runku_platform_identity::SessionStatus::Revoked => "revoked",
                        },
                        created_at_micros: session.created_at.get(),
                        last_used_at_micros: session.last_used_at.get(),
                        access_expires_at_micros: session.access_expires_at.get(),
                        refresh_expires_at_micros: session.refresh_expires_at.get(),
                    })
                    .collect(),
            },
            false,
        ),
        Err(error) => failure(error),
    }
}

async fn revoke_session(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Response {
    let actor = match authenticate(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return failure(error),
    };
    let Ok(session_id) = session_id.parse::<OperatorSessionId>() else {
        return failure(PlatformIdentityError::InvalidInput);
    };
    match state
        .identity
        .revoke_session(&actor, session_id, now())
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => failure(PlatformIdentityError::NotFound),
        Err(error) => failure(error),
    }
}

async fn invite(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<InviteRequest>,
) -> Response {
    let actor = match authenticate(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return failure(error),
    };
    let name = match OperatorName::from_str(&request.operator_name) {
        Ok(name) => name,
        Err(error) => return failure(error),
    };
    let role = match request.role.as_str() {
        "owner" => OperatorRole::Owner,
        "operator" => OperatorRole::Operator,
        "developer" => OperatorRole::Developer,
        "observer" => OperatorRole::Observer,
        _ => return failure(PlatformIdentityError::InvalidInput),
    };
    let scope = match parse_scope(&request.scope) {
        Ok(scope) => scope,
        Err(error) => return failure(error),
    };
    match state
        .identity
        .create_invitation(&actor, name, scope, role, now())
        .await
    {
        Ok(generated) => json(
            StatusCode::CREATED,
            &InvitationResponse {
                code: generated.code.expose().to_owned(),
                secret_shown_once: true,
            },
            true,
        ),
        Err(error) => failure(error),
    }
}

async fn authenticate(
    state: &HttpState,
    headers: &HeaderMap,
) -> Result<OperatorContext, PlatformIdentityError> {
    let bearer = Zeroizing::new(bearer(headers)?);
    let token = AccessToken::from_str(&bearer)?;
    state.identity.authenticate(&token, now()).await
}

fn parse_scope(request: &ScopeRequest) -> Result<AccessScope, PlatformIdentityError> {
    match (
        request.kind.as_str(),
        request.project_id.as_deref(),
        request.environment_id.as_deref(),
    ) {
        ("installation", None, None) => Ok(AccessScope::Installation),
        ("project", Some(project), None) => Ok(AccessScope::Project(
            project
                .parse::<ProjectId>()
                .map_err(|_| PlatformIdentityError::InvalidInput)?,
        )),
        ("environment", Some(project), Some(environment)) => {
            Ok(AccessScope::Environment(EnvironmentScope::new(
                project
                    .parse::<ProjectId>()
                    .map_err(|_| PlatformIdentityError::InvalidInput)?,
                environment
                    .parse::<EnvironmentId>()
                    .map_err(|_| PlatformIdentityError::InvalidInput)?,
            )))
        }
        _ => Err(PlatformIdentityError::InvalidInput),
    }
}

fn bearer(headers: &HeaderMap) -> Result<String, PlatformIdentityError> {
    let values = headers
        .get_all(header::AUTHORIZATION)
        .iter()
        .collect::<Vec<_>>();
    if values.len() != 1 {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    let value = values[0]
        .to_str()
        .map_err(|_| PlatformIdentityError::Unauthenticated)?;
    if value.len() > MAX_AUTHORIZATION_BYTES {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    let token = value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && token.trim() == *token)
        .ok_or(PlatformIdentityError::Unauthenticated)?;
    Ok(token.to_owned())
}

fn now() -> TimestampMicros {
    let value = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_micros()).ok())
        .unwrap_or(-1);
    TimestampMicros::new(value)
}

fn login_response(result: &LoginResult) -> Response {
    json(
        StatusCode::OK,
        &LoginResponse {
            access_token: result.access_token.expose().to_owned(),
            refresh_token: result.refresh_token.expose().to_owned(),
            operator_id: result.context.operator.id.to_string(),
            session_id: result.context.session.id.to_string(),
            authorization_revision: result.context.operator.authorization_revision,
        },
        true,
    )
}

fn failure(error: PlatformIdentityError) -> Response {
    let status = match error {
        PlatformIdentityError::InvalidInput | PlatformIdentityError::LimitExceeded => {
            StatusCode::BAD_REQUEST
        }
        PlatformIdentityError::Unauthenticated | PlatformIdentityError::Inactive => {
            StatusCode::UNAUTHORIZED
        }
        PlatformIdentityError::Forbidden | PlatformIdentityError::AlreadyInitialized => {
            StatusCode::FORBIDDEN
        }
        PlatformIdentityError::NotFound => StatusCode::NOT_FOUND,
        PlatformIdentityError::Conflict => StatusCode::CONFLICT,
        PlatformIdentityError::Unavailable | PlatformIdentityError::EntropyUnavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        PlatformIdentityError::ResultUncertain => StatusCode::GATEWAY_TIMEOUT,
        PlatformIdentityError::Corruption | PlatformIdentityError::Unsupported => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    json(status, &ErrorResponse { code: error.code() }, false)
}

fn json<T: Serialize>(status: StatusCode, value: &T, secret: bool) -> Response {
    let mut response = (status, Json(value)).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if secret {
            "no-store, max-age=0"
        } else {
            "no-store"
        }),
    );
    response
}

async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<HttpState>) -> Response {
    if let Err(error) = state.identity.health().await {
        return failure(error);
    }
    if let Some(product) = &state.product
        && product.health().await.is_err()
    {
        return failure(PlatformIdentityError::Unavailable);
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn fallback() -> Response {
    failure(PlatformIdentityError::NotFound)
}
