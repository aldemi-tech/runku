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
use runku_core::{
    EnvironmentId, EnvironmentScope, OperationId, OperatorInvitationId, OperatorSessionId,
    ProjectId,
};
use runku_platform_identity::{
    AccessScope, AccessToken, DeviceName, ExternalOperatorIdentity, IdempotentInvitationResult,
    InvitationCode, InvitationStatus, LoginResult, ManagedGrantReconciliation, ManagedLoginResult,
    ManagedSourceAuthority, OperatorContext, OperatorInvitation, OperatorName, OperatorRole,
    PlatformCapability, PlatformIdentityError, PlatformIdentityService, RefreshToken,
};
use runku_protocol::DEVELOPMENT_PUBLISH_MAX_BYTES;
use runku_value::TimestampMicros;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use zeroize::Zeroizing;

use crate::{
    ManagementApplicationClientCreate, ManagementApplicationCredentialCreate,
    ManagementApplicationCredentialRotate, ManagementBucketArchive, ManagementBucketCreate,
    ManagementBucketUpdate, ManagementCatalogQuery, ManagementDataDeleteRequest,
    ManagementDataInsertRequest, ManagementDataQuery, ManagementDataReplaceRequest,
    ManagementLogPruneRequest, ManagementLogQuery, ManagementProduct, ManagementProductError,
    ManagementServingPolicySet, ManagementStorageAccessKeyIssue, ManagementStorageAccessKeyRevoke,
    ManagementStorageAccessKeyRotate, OidcClientConfiguration,
};

const MAX_BODY_BYTES: usize = 16 * 1024;
const MAX_DATA_ADMIN_BODY_BYTES: usize = 12 * 1024 * 1024;
const MAX_AUTHORIZATION_BYTES: usize = 16 * 1024;
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const MANAGED_ENROLLMENT_HEADER: &str = "runku-managed-enrollment";

/// Digest-backed credential accepted only from a trusted managed OIDC enrollment gateway.
#[derive(Clone, Eq, PartialEq)]
pub struct ManagedEnrollmentKey([u8; 32]);

impl std::fmt::Debug for ManagedEnrollmentKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ManagedEnrollmentKey([REDACTED])")
    }
}

impl ManagedEnrollmentKey {
    /// Builds a key from at least 32 bytes of high-entropy secret material.
    ///
    /// # Errors
    ///
    /// Rejects short or surrounding-whitespace-bearing secrets.
    pub fn new(secret: &str) -> Result<Self, PlatformIdentityError> {
        if secret.len() < 32 || secret.trim() != secret {
            return Err(PlatformIdentityError::InvalidInput);
        }
        Ok(Self(Sha256::digest(secret.as_bytes()).into()))
    }

    fn matches(&self, secret: &str) -> bool {
        let digest: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
        bool::from(self.0.ct_eq(&digest))
    }
}

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
    /// Optional separate gateway secret enabling managed first-login enrollment and grant sync.
    pub managed_enrollment_key: Option<ManagedEnrollmentKey>,
    /// Exact HTTPS authority owning every reconciliation accepted with the managed token.
    pub managed_source_authority: Option<ManagedSourceAuthority>,
}

impl ManagementHttpConfig {
    fn validate(&self) -> Result<(), PlatformIdentityError> {
        if !(1..=100_000).contains(&self.max_concurrent_requests) {
            return Err(PlatformIdentityError::InvalidInput);
        }
        if let Some(endpoint) = &self.public_management_endpoint {
            validate_public_endpoint(endpoint)?;
        }
        if self.managed_enrollment_key.is_some() != self.managed_source_authority.is_some() {
            return Err(PlatformIdentityError::InvalidInput);
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
    managed_enrollment_key: Option<ManagedEnrollmentKey>,
    managed_source_authority: Option<ManagedSourceAuthority>,
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
#[allow(clippy::too_many_lines)]
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
        managed_enrollment_key: config.managed_enrollment_key,
        managed_source_authority: config.managed_source_authority,
        admission: Arc::new(Semaphore::new(config.max_concurrent_requests)),
    };
    Ok(Router::new()
        .route("/v1/auth/exchange", post(exchange))
        .route("/v1/auth/refresh", post(refresh))
        .route("/v1/auth/oidc", post(oidc))
        .route("/v1/auth/config", get(auth_config))
        .route("/v1/auth/oidc/config", get(oidc_config))
        .route("/v1/auth/me", get(me))
        .route("/v1/auth/resources", get(resources))
        .route("/v1/auth/sessions", get(sessions))
        .route("/v1/auth/sessions/{session_id}", delete(revoke_session))
        .route(
            "/v1/auth/managed/operators/{operator_id}/grants",
            put(reconcile_managed_operator_grants),
        )
        .route("/v1/access/invitations", post(invite))
        .route(
            "/v1/access/invitations/{invitation_id}",
            delete(revoke_invitation),
        )
        .route(
            "/v1/access/invitation-operations/{operation_id}",
            get(invitation_operation),
        )
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
            "/v1/projects/{project_id}/environments/{environment_id}/serving-policy",
            get(product_serving_policy).put(product_serving_policy_set),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/serving-policy-operations/{operation_id}",
            get(product_serving_operation),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/buckets",
            get(product_buckets).post(product_bucket_create),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/buckets/{bucket_id}",
            get(product_bucket)
                .put(product_bucket_update)
                .delete(product_bucket_archive),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/buckets/{bucket_id}/access-keys",
            get(product_storage_access_keys).post(product_storage_access_key_issue),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/buckets/{bucket_id}/access-keys/{access_key_id}/rotate",
            post(product_storage_access_key_rotate),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/buckets/{bucket_id}/access-keys/{access_key_id}/revoke",
            post(product_storage_access_key_revoke),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/storage-operations/{operation_id}",
            get(product_storage_operation),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/application-clients",
            get(product_application_clients).post(product_application_client_create),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/application-clients/{client_id}/credentials",
            get(product_application_credentials).post(product_application_credential_create),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/application-clients/{client_id}/credentials/{credential_id}/reveal",
            post(product_application_credential_reveal),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/application-clients/{client_id}/credentials/{credential_id}/rotate",
            post(product_application_credential_rotate),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/application-clients/{client_id}/credentials/{credential_id}/revoke",
            post(product_application_credential_revoke),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/application-clients/{client_id}/credentials/{credential_id}",
            delete(product_application_credential_delete),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/functions",
            get(product_functions),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/schema/tables",
            get(product_schema_tables),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/data/query",
            post(product_data_query).layer(DefaultBodyLimit::max(MAX_DATA_ADMIN_BODY_BYTES)),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/data/documents/{table}",
            post(product_data_insert).layer(DefaultBodyLimit::max(MAX_DATA_ADMIN_BODY_BYTES)),
        )
        .route(
            "/v1/projects/{project_id}/environments/{environment_id}/data/documents/{table}/{document_id}",
            get(product_data_get)
                .put(product_data_replace)
                .delete(product_data_delete)
                .layer(DefaultBodyLimit::max(MAX_DATA_ADMIN_BODY_BYTES)),
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

async fn product_serving_policy(
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
    match product.serving_policy().await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_serving_policy_set(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Json(request): Json<ManagementServingPolicySet>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let operation_id = match required_operation(&headers) {
        Ok(value) => value,
        Err(error) => return failure(error),
    };
    let (product, context) = match product_context(
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
        .serving_policy_set(operation_id, context.operator.id, &request)
        .await
    {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_serving_operation(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, operation)): Path<(String, String, String)>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let Ok(operation_id) = operation.parse::<OperationId>() else {
        return failure(PlatformIdentityError::InvalidInput);
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
    match product.serving_operation(operation_id).await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoragePageQuery {
    after: Option<String>,
    limit: u16,
}

async fn product_buckets(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Query(query): Query<StoragePageQuery>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.buckets(query.after.as_deref(), query.limit).await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_bucket(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, bucket)): Path<(String, String, String)>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.bucket(&bucket).await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_bucket_create(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Json(request): Json<ManagementBucketCreate>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let operation_id = match required_operation(&headers) {
        Ok(value) => value,
        Err(error) => return failure(error),
    };
    let (product, context) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .bucket_create(operation_id, context.operator.id, &request)
        .await
    {
        Ok(result) => json(StatusCode::CREATED, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_bucket_update(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, bucket)): Path<(String, String, String)>,
    Json(request): Json<ManagementBucketUpdate>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let operation_id = match required_operation(&headers) {
        Ok(value) => value,
        Err(error) => return failure(error),
    };
    let (product, context) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .bucket_update(&bucket, operation_id, context.operator.id, &request)
        .await
    {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_bucket_archive(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, bucket)): Path<(String, String, String)>,
    Json(request): Json<ManagementBucketArchive>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let operation_id = match required_operation(&headers) {
        Ok(value) => value,
        Err(error) => return failure(error),
    };
    let (product, context) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .bucket_archive(&bucket, operation_id, context.operator.id, &request)
        .await
    {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_storage_access_keys(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, bucket)): Path<(String, String, String)>,
    Query(query): Query<StoragePageQuery>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .storage_access_keys(&bucket, query.after.as_deref(), query.limit)
        .await
    {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_storage_access_key_issue(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, bucket)): Path<(String, String, String)>,
    Json(request): Json<ManagementStorageAccessKeyIssue>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let operation_id = match required_operation(&headers) {
        Ok(value) => value,
        Err(error) => return failure(error),
    };
    let (product, context) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .storage_access_key_issue(&bucket, operation_id, context.operator.id, &request)
        .await
    {
        Ok(result) => json(StatusCode::CREATED, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_storage_access_key_rotate(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, bucket, access_key)): Path<(String, String, String, String)>,
    Json(request): Json<ManagementStorageAccessKeyRotate>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let operation_id = match required_operation(&headers) {
        Ok(value) => value,
        Err(error) => return failure(error),
    };
    let (product, context) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .storage_access_key_rotate(
            &bucket,
            &access_key,
            operation_id,
            context.operator.id,
            &request,
        )
        .await
    {
        Ok(result) => json(StatusCode::CREATED, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_storage_access_key_revoke(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, bucket, access_key)): Path<(String, String, String, String)>,
    Json(request): Json<ManagementStorageAccessKeyRevoke>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let operation_id = match required_operation(&headers) {
        Ok(value) => value,
        Err(error) => return failure(error),
    };
    let (product, context) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .storage_access_key_revoke(
            &bucket,
            &access_key,
            operation_id,
            context.operator.id,
            &request,
        )
        .await
    {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_storage_operation(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, operation)): Path<(String, String, String)>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let Ok(operation_id) = operation.parse::<OperationId>() else {
        return failure(PlatformIdentityError::InvalidInput);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::StorageRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.storage_operation(operation_id).await {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_application_clients(
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
        PlatformCapability::CredentialsRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.application_clients().await {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_application_client_create(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Json(request): Json<ManagementApplicationClientCreate>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::CredentialsManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.application_client_create(&request).await {
        Ok(result) => json(StatusCode::CREATED, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_application_credentials(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, client)): Path<(String, String, String)>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::CredentialsRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.application_credentials(&client).await {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_application_credential_create(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, client)): Path<(String, String, String)>,
    Json(request): Json<ManagementApplicationCredentialCreate>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::CredentialsManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .application_credential_create(&client, &request)
        .await
    {
        Ok(result) => json(StatusCode::CREATED, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_application_credential_reveal(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, client, credential)): Path<(String, String, String, String)>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::CredentialsRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .application_credential_reveal(&client, &credential)
        .await
    {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_application_credential_rotate(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, client, credential)): Path<(String, String, String, String)>,
    Json(request): Json<ManagementApplicationCredentialRotate>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::CredentialsManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product
        .application_credential_rotate(&client, &credential, &request)
        .await
    {
        Ok(result) => json(StatusCode::CREATED, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_application_credential_revoke(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, client, credential)): Path<(String, String, String, String)>,
) -> Response {
    product_application_credential_lifecycle(
        &state,
        &headers,
        &project,
        &environment,
        &client,
        &credential,
        false,
    )
    .await
}

async fn product_application_credential_delete(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, client, credential)): Path<(String, String, String, String)>,
) -> Response {
    product_application_credential_lifecycle(
        &state,
        &headers,
        &project,
        &environment,
        &client,
        &credential,
        true,
    )
    .await
}

async fn product_application_credential_lifecycle(
    state: &HttpState,
    headers: &HeaderMap,
    project: &str,
    environment: &str,
    client: &str,
    credential: &str,
    delete: bool,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        state,
        headers,
        project,
        environment,
        PlatformCapability::CredentialsManage,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let timestamp = now().get();
    let result = if delete {
        product
            .application_credential_delete(client, credential, timestamp)
            .await
    } else {
        product
            .application_credential_revoke(client, credential, timestamp)
            .await
    };
    match result {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_functions(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Query(query): Query<ManagementCatalogQuery>,
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
    match product.functions(&query).await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

async fn product_schema_tables(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Query(query): Query<ManagementCatalogQuery>,
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
    match product.schema_tables(&query).await {
        Ok(result) => json(StatusCode::OK, &result, false),
        Err(error) => product_failure(error),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DataGetQuery {
    target: String,
}

async fn product_data_get(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, table, document_id)): Path<(String, String, String, String)>,
    Query(query): Query<DataGetQuery>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::DataRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.data_get(&query.target, &table, &document_id).await {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_data_query(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment)): Path<(String, String)>,
    Json(request): Json<ManagementDataQuery>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let (product, _) = match product_context(
        &state,
        &headers,
        &project,
        &environment,
        PlatformCapability::DataRead,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match product.data_query(&request).await {
        Ok(result) => json(StatusCode::OK, &result, true),
        Err(error) => product_failure(error),
    }
}

async fn product_data_insert(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, table)): Path<(String, String, String)>,
    Json(request): Json<ManagementDataInsertRequest>,
) -> Response {
    product_data_write_context(
        &state,
        &headers,
        &project,
        &environment,
        |product, operation| async move { product.data_insert(operation, &table, &request).await },
    )
    .await
}

async fn product_data_replace(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, table, document_id)): Path<(String, String, String, String)>,
    Json(request): Json<ManagementDataReplaceRequest>,
) -> Response {
    product_data_write_context(
        &state,
        &headers,
        &project,
        &environment,
        |product, operation| async move {
            product
                .data_replace(operation, &table, &document_id, &request)
                .await
        },
    )
    .await
}

async fn product_data_delete(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, environment, table, document_id)): Path<(String, String, String, String)>,
    Json(request): Json<ManagementDataDeleteRequest>,
) -> Response {
    product_data_write_context(
        &state,
        &headers,
        &project,
        &environment,
        |product, operation| async move {
            product
                .data_delete(operation, &table, &document_id, &request)
                .await
        },
    )
    .await
}

async fn product_data_write_context<F, Fut>(
    state: &HttpState,
    headers: &HeaderMap,
    project: &str,
    environment: &str,
    operation: F,
) -> Response
where
    F: FnOnce(Arc<dyn ManagementProduct>, OperationId) -> Fut,
    Fut: Future<Output = Result<crate::ManagementDataWriteResult, ManagementProductError>>,
{
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let operation_id = match required_operation(headers) {
        Ok(value) => value,
        Err(error) => return failure(error),
    };
    let (product, _) = match product_context(
        state,
        headers,
        project,
        environment,
        PlatformCapability::DataWrite,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return *response,
    };
    match operation(product, operation_id).await {
        Ok(result) => json(StatusCode::OK, &result, true),
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
        ManagementProductError::OperationIdReused => {
            (StatusCode::CONFLICT, "PRODUCT_OPERATION_ID_REUSED")
        }
        ManagementProductError::Validation => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "PRODUCT_DATA_VALIDATION_FAILED",
        ),
        ManagementProductError::Incompatible => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "PRODUCT_SERVING_INCOMPATIBLE_CONTRACTS",
        ),
        ManagementProductError::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "PRODUCT_UNAVAILABLE")
        }
        ManagementProductError::Corruption => {
            (StatusCode::INTERNAL_SERVER_ERROR, "PRODUCT_CORRUPT")
        }
        ManagementProductError::ResultUncertain => {
            (StatusCode::GATEWAY_TIMEOUT, "PRODUCT_RESULT_UNCERTAIN")
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
    managed_enrollment: Option<ManagedEnrollmentRequest>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManagedEnrollmentRequest {
    operator_name: String,
    source_revision: u64,
    grants: Vec<ManagedGrantRequest>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManagedGrantReconciliationRequestWire {
    source_revision: u64,
    grants: Vec<ManagedGrantRequest>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManagedGrantRequest {
    role: String,
    scope: ScopeRequest,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    applied: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replayed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_revision: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedGrantReconciliationResponse {
    applied: bool,
    replayed: bool,
    source_revision: u64,
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
    grants: Vec<GrantResponse>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GrantResponse {
    scope: ScopeResponse,
    capabilities: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResourcesResponse {
    version: u8,
    resources: Vec<ResourceResponse>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResourceResponse {
    project_id: String,
    project_name: String,
    environment_id: String,
    environment_name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InvitationResponse {
    code: String,
    secret_shown_once: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InvitationOperationResponse {
    operation_id: String,
    invitation_id: String,
    operator_name: String,
    scope: ScopeResponse,
    capabilities: Vec<&'static str>,
    status: &'static str,
    created_by: String,
    created_at_micros: i64,
    expires_at_micros: i64,
    consumed_at_micros: Option<i64>,
    revoked_at_micros: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    secret_shown_once: bool,
    replayed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ScopeResponse {
    kind: &'static str,
    project_id: Option<String>,
    environment_id: Option<String>,
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
    if request.invitation_code.is_some() && request.managed_enrollment.is_some() {
        return failure(PlatformIdentityError::InvalidInput);
    }
    if let Some(code) = request.invitation_code {
        let code = Zeroizing::new(code);
        let result = match InvitationCode::from_str(&code) {
            Ok(code) => {
                state
                    .identity
                    .login_with_invitation(&code, device, Some(identity), timestamp)
                    .await
            }
            Err(error) => Err(error),
        };
        return match result {
            Ok(result) => login_response(&result),
            Err(error) => failure(error),
        };
    }
    if let Some(managed) = request.managed_enrollment {
        let source_authority = match trusted_managed_source(&state, &headers) {
            Ok(authority) => authority,
            Err(error) => return failure(error),
        };
        let name = match OperatorName::from_str(&managed.operator_name) {
            Ok(name) => name,
            Err(error) => return failure(error),
        };
        let grants = match parse_managed_grants(managed.grants) {
            Ok(grants) => grants,
            Err(error) => return failure(error),
        };
        return match state
            .identity
            .login_with_managed_external_identity(
                identity,
                name,
                source_authority,
                managed.source_revision,
                grants,
                device,
                timestamp,
            )
            .await
        {
            Ok(result) => managed_login_response(&result),
            Err(error) => failure(error),
        };
    }
    let result = state
        .identity
        .login_with_external_identity(&identity, device, timestamp)
        .await;
    match result {
        Ok(result) => login_response(&result),
        Err(error) => failure(error),
    }
}

fn parse_managed_grants(
    grants: Vec<ManagedGrantRequest>,
) -> Result<Vec<runku_platform_identity::OperatorGrant>, PlatformIdentityError> {
    grants
        .into_iter()
        .map(|grant| {
            let role = parse_role(&grant.role)?;
            let scope = parse_scope(&grant.scope)?;
            if !matches!(scope, AccessScope::Project(_)) {
                return Err(PlatformIdentityError::InvalidInput);
            }
            Ok(runku_platform_identity::OperatorGrant {
                scope,
                capabilities: role.capabilities(),
            })
        })
        .collect()
}

fn managed_bearer(headers: &HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(MANAGED_ENROLLMENT_HEADER).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    value.strip_prefix("Bearer ").filter(|secret| {
        !secret.is_empty() && secret.len() <= 16 * 1024 && secret.trim() == *secret
    })
}

fn trusted_managed_source(
    state: &HttpState,
    headers: &HeaderMap,
) -> Result<ManagedSourceAuthority, PlatformIdentityError> {
    let trusted = managed_bearer(headers).is_some_and(|secret| {
        state
            .managed_enrollment_key
            .as_ref()
            .is_some_and(|key| key.matches(secret))
    });
    if !trusted {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    state
        .managed_source_authority
        .clone()
        .ok_or(PlatformIdentityError::Unauthenticated)
}

async fn reconcile_managed_operator_grants(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(operator_id): Path<String>,
    Json(request): Json<ManagedGrantReconciliationRequestWire>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let source_authority = match trusted_managed_source(&state, &headers) {
        Ok(authority) => authority,
        Err(error) => return failure(error),
    };
    let Ok(operator_id) = operator_id.parse() else {
        return failure(PlatformIdentityError::InvalidInput);
    };
    let grants = match parse_managed_grants(request.grants) {
        Ok(grants) => grants,
        Err(error) => return failure(error),
    };
    match state
        .identity
        .reconcile_managed_grants(
            operator_id,
            source_authority,
            request.source_revision,
            grants,
            now(),
        )
        .await
    {
        Ok(result) => reconciliation_response(&result),
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
            grants: context
                .grants
                .into_iter()
                .map(|grant| GrantResponse {
                    scope: scope_response(grant.scope),
                    capabilities: grant
                        .capabilities
                        .iter()
                        .map(|capability| capability.as_str())
                        .collect(),
                })
                .collect(),
        },
        false,
    )
}

async fn resources(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    let context = match authenticate(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return failure(error),
    };
    let resources = state
        .product
        .as_ref()
        .filter(|product| {
            context
                .authorize(
                    AccessScope::Environment(product.scope()),
                    PlatformCapability::ReleasesRead,
                )
                .is_ok()
        })
        .map(|product| {
            let scope = product.scope();
            ResourceResponse {
                project_id: scope.project_id().to_string(),
                project_name: scope.project_id().to_string(),
                environment_id: scope.environment_id().to_string(),
                environment_name: scope.environment_id().to_string(),
            }
        })
        .into_iter()
        .collect();
    json(
        StatusCode::OK,
        &ResourcesResponse {
            version: 1,
            resources,
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
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let actor = match authenticate(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return failure(error),
    };
    let name = match OperatorName::from_str(&request.operator_name) {
        Ok(name) => name,
        Err(error) => return failure(error),
    };
    let role = match parse_role(&request.role) {
        Ok(role) => role,
        Err(error) => return failure(error),
    };
    let scope = match parse_scope(&request.scope) {
        Ok(scope) => scope,
        Err(error) => return failure(error),
    };
    let operation_id = match optional_invitation_operation(&headers) {
        Ok(operation_id) => operation_id,
        Err(error) => return failure(error),
    };
    let timestamp = now();
    if let Some(operation_id) = operation_id {
        return match state
            .identity
            .create_invitation_idempotent(&actor, operation_id, name, scope, role, timestamp)
            .await
        {
            Ok(IdempotentInvitationResult::Created {
                invitation,
                generated,
            }) => invitation_operation_json(
                StatusCode::CREATED,
                &invitation,
                timestamp,
                Some(generated.code.expose().to_owned()),
                true,
                false,
            ),
            Ok(IdempotentInvitationResult::Replayed(invitation)) => {
                invitation_operation_json(StatusCode::OK, &invitation, timestamp, None, false, true)
            }
            Err(error) => failure(error),
        };
    }
    match state
        .identity
        .create_invitation(&actor, name, scope, role, timestamp)
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

fn parse_role(value: &str) -> Result<OperatorRole, PlatformIdentityError> {
    match value {
        "owner" => Ok(OperatorRole::Owner),
        "operator" => Ok(OperatorRole::Operator),
        "developer" => Ok(OperatorRole::Developer),
        "observer" => Ok(OperatorRole::Observer),
        _ => Err(PlatformIdentityError::InvalidInput),
    }
}

async fn invitation_operation(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(operation_id): Path<String>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let actor = match authenticate(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return failure(error),
    };
    let Ok(operation_id) = operation_id.parse::<OperationId>() else {
        return failure(PlatformIdentityError::InvalidInput);
    };
    let timestamp = now();
    match state
        .identity
        .invitation_by_operation(&actor, operation_id)
        .await
    {
        Ok(invitation) => {
            invitation_operation_json(StatusCode::OK, &invitation, timestamp, None, false, true)
        }
        Err(error) => failure(error),
    }
}

async fn revoke_invitation(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(invitation_id): Path<String>,
) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return failure(PlatformIdentityError::Unavailable);
    };
    let actor = match authenticate(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return failure(error),
    };
    let Ok(invitation_id) = invitation_id.parse::<OperatorInvitationId>() else {
        return failure(PlatformIdentityError::InvalidInput);
    };
    match state
        .identity
        .revoke_invitation(&actor, invitation_id, now())
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => failure(error),
    }
}

fn optional_invitation_operation(
    headers: &HeaderMap,
) -> Result<Option<OperationId>, PlatformIdentityError> {
    let values = headers
        .get_all(IDEMPOTENCY_KEY_HEADER)
        .iter()
        .collect::<Vec<_>>();
    match values.as_slice() {
        [] => Ok(None),
        [value] => value
            .to_str()
            .map_err(|_| PlatformIdentityError::InvalidInput)?
            .parse::<OperationId>()
            .map(Some)
            .map_err(|_| PlatformIdentityError::InvalidInput),
        _ => Err(PlatformIdentityError::InvalidInput),
    }
}

fn required_operation(headers: &HeaderMap) -> Result<OperationId, PlatformIdentityError> {
    optional_invitation_operation(headers)?.ok_or(PlatformIdentityError::InvalidInput)
}

fn invitation_operation_json(
    status: StatusCode,
    invitation: &OperatorInvitation,
    timestamp: TimestampMicros,
    code: Option<String>,
    secret_shown_once: bool,
    replayed: bool,
) -> Response {
    let Some(operation_id) = invitation.operation_id else {
        return failure(PlatformIdentityError::Corruption);
    };
    let [grant] = invitation.grants.as_slice() else {
        return failure(PlatformIdentityError::Corruption);
    };
    let response = InvitationOperationResponse {
        operation_id: operation_id.to_string(),
        invitation_id: invitation.id.to_string(),
        operator_name: invitation.operator_name.to_string(),
        scope: scope_response(grant.scope),
        capabilities: grant
            .capabilities
            .iter()
            .map(|capability| capability.as_str())
            .collect(),
        status: invitation_status(invitation.status_at(timestamp)),
        created_by: invitation.created_by.to_string(),
        created_at_micros: invitation.created_at.get(),
        expires_at_micros: invitation.expires_at.get(),
        consumed_at_micros: invitation.consumed_at.map(TimestampMicros::get),
        revoked_at_micros: invitation.revoked_at.map(TimestampMicros::get),
        code,
        secret_shown_once,
        replayed,
    };
    json(status, &response, true)
}

fn scope_response(scope: AccessScope) -> ScopeResponse {
    match scope {
        AccessScope::Installation => ScopeResponse {
            kind: "installation",
            project_id: None,
            environment_id: None,
        },
        AccessScope::Project(project) => ScopeResponse {
            kind: "project",
            project_id: Some(project.to_string()),
            environment_id: None,
        },
        AccessScope::Environment(environment) => ScopeResponse {
            kind: "environment",
            project_id: Some(environment.project_id().to_string()),
            environment_id: Some(environment.environment_id().to_string()),
        },
    }
}

const fn invitation_status(status: InvitationStatus) -> &'static str {
    match status {
        InvitationStatus::Pending => "pending",
        InvitationStatus::Consumed => "consumed",
        InvitationStatus::Revoked => "revoked",
        InvitationStatus::Expired => "expired",
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
            applied: None,
            replayed: None,
            source_revision: None,
        },
        true,
    )
}

fn managed_login_response(result: &ManagedLoginResult) -> Response {
    json(
        StatusCode::OK,
        &LoginResponse {
            access_token: result.login.access_token.expose().to_owned(),
            refresh_token: result.login.refresh_token.expose().to_owned(),
            operator_id: result.login.context.operator.id.to_string(),
            session_id: result.login.context.session.id.to_string(),
            authorization_revision: result.reconciliation.authorization_revision,
            applied: Some(result.reconciliation.applied),
            replayed: Some(result.reconciliation.replayed),
            source_revision: Some(result.reconciliation.source_revision),
        },
        true,
    )
}

fn reconciliation_response(result: &ManagedGrantReconciliation) -> Response {
    json(
        StatusCode::OK,
        &ManagedGrantReconciliationResponse {
            applied: result.applied,
            replayed: result.replayed,
            source_revision: result.source_revision,
            authorization_revision: result.authorization_revision,
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
        PlatformIdentityError::Conflict
        | PlatformIdentityError::InvitationOperationReused
        | PlatformIdentityError::ManagedSourceConflict
        | PlatformIdentityError::ManagedSourceStale => StatusCode::CONFLICT,
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
