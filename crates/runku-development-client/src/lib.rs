//! Strict, bounded, no-proxy/no-redirect Remote Workspace HTTP client.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{
    fmt,
    net::IpAddr,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use bytes::Bytes;
use reqwest::{StatusCode, header};
use runku_core::RequestId;
use runku_development_access::ParsedDevelopmentKey;
use runku_protocol::{
    DEVELOPMENT_JSON_MAX_BYTES, DevelopmentAdminErrorCodeV1, DevelopmentCreateWorkspaceRequestV1,
    DevelopmentCreateWorkspaceResponseV1, DevelopmentFreezeRequestV1, DevelopmentFreezeResponseV1,
    DevelopmentPublishRequestV1, DevelopmentPublishResponseV1, DevelopmentStateRequestV1,
    DevelopmentStateResponseV1, ProtocolError, decode_development_create_response_v1,
    decode_development_error_v1, decode_development_freeze_response_v1,
    decode_development_publish_response_v1, decode_development_state_response_v1,
    encode_development_create_request_v1, encode_development_freeze_request_v1,
    encode_development_publish_request_v1, encode_development_state_request_v1,
};
use thiserror::Error;
use url::Url;

const JSON_CONTENT_TYPE: &str = "application/json";
const JSON_RESPONSE_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const PUBLISH_CONTENT_TYPE: &str = "application/vnd.runku.development-publish-v1";
const REQUEST_ID_HEADER: &str = "x-runku-request-id";
const MAX_RETRY_AFTER_SECONDS: u64 = 30;

/// Canonical administrative origin with HTTPS or literal loopback HTTP.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentEndpoint(String);

impl DevelopmentEndpoint {
    /// Returns the canonical origin without trailing slash.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn route(&self, path: &str) -> String {
        format!("{}{path}", self.0)
    }
}

impl fmt::Display for DevelopmentEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for DevelopmentEndpoint {
    type Err = DevelopmentClientError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(value).map_err(|_| DevelopmentClientError::InvalidConfig)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(DevelopmentClientError::InvalidConfig);
        }
        if url.scheme() == "http"
            && !url
                .host_str()
                .and_then(|host| host.parse::<IpAddr>().ok())
                .is_some_and(|address| address.is_loopback())
        {
            return Err(DevelopmentClientError::InvalidConfig);
        }
        let canonical = url.origin().ascii_serialization();
        if value != canonical {
            return Err(DevelopmentClientError::InvalidConfig);
        }
        Ok(Self(canonical))
    }
}

/// Bounded retry/deadline configuration for one immutable client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DevelopmentClientConfig {
    /// Complete request deadline per attempt.
    pub request_timeout: Duration,
    /// Total attempts, including the first.
    pub maximum_attempts: u8,
    /// Base backoff used when the server omits `Retry-After`.
    pub retry_delay: Duration,
}

impl Default for DevelopmentClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            maximum_attempts: 3,
            retry_delay: Duration::from_millis(250),
        }
    }
}

impl DevelopmentClientConfig {
    /// Validates v1 client resource bounds.
    ///
    /// # Errors
    ///
    /// Rejects deadline outside 1ms..5min, attempts outside 1..10, or delay above 30s.
    pub fn validate(self) -> Result<(), DevelopmentClientError> {
        if self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > Duration::from_mins(5)
            || !(1..=10).contains(&self.maximum_attempts)
            || self.retry_delay > Duration::from_secs(MAX_RETRY_AFTER_SECONDS)
        {
            return Err(DevelopmentClientError::InvalidConfig);
        }
        Ok(())
    }
}

/// Sanitized client/remote failure taxonomy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DevelopmentClientError {
    /// Endpoint, token, deadline, or retry configuration is invalid.
    #[error("development client configuration is invalid")]
    InvalidConfig,
    /// Local request encoding or server invalid-request response.
    #[error("development request is invalid")]
    InvalidRequest,
    /// Development authentication failed.
    #[error("development authentication failed")]
    Unauthenticated,
    /// Authenticated key cannot access this operation/scope.
    #[error("development access is denied")]
    Forbidden,
    /// Exact remote resource does not exist.
    #[error("development resource was not found")]
    NotFound,
    /// CAS or idempotency state conflicts.
    #[error("development state conflicts")]
    Conflict,
    /// Environment policy denies synchronization.
    #[error("development policy denied synchronization")]
    PolicyDenied,
    /// A local/remote protocol limit was exceeded.
    #[error("development limit was exceeded")]
    LimitExceeded,
    /// Server admission is temporarily exhausted.
    #[error("development service is busy")]
    Busy,
    /// Network or service is temporarily unavailable.
    #[error("development service is unavailable")]
    Unavailable,
    /// Mutation outcome must be reconciled using the identical request.
    #[error("development result is uncertain")]
    ResultUncertain,
    /// Remote durable state is corrupt.
    #[error("development state is corrupt")]
    Corruption,
    /// Response status/headers/body violated the protocol.
    #[error("development response is invalid")]
    InvalidResponse,
    /// Unexpected local trusted failure.
    #[error("development client failed internally")]
    Internal,
}

impl DevelopmentClientError {
    /// Stable machine-readable client failure code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfig => "DEVELOPMENT_CLIENT_CONFIG_INVALID",
            Self::InvalidRequest => "DEVELOPMENT_REQUEST_INVALID",
            Self::Unauthenticated => "DEVELOPMENT_AUTH_INVALID",
            Self::Forbidden => "DEVELOPMENT_ACCESS_DENIED",
            Self::NotFound => "DEVELOPMENT_RESOURCE_NOT_FOUND",
            Self::Conflict => "DEVELOPMENT_STATE_CONFLICT",
            Self::PolicyDenied => "DEVELOPMENT_POLICY_DENIED",
            Self::LimitExceeded => "DEVELOPMENT_LIMIT_EXCEEDED",
            Self::Busy => "DEVELOPMENT_SERVICE_BUSY",
            Self::Unavailable => "DEVELOPMENT_SERVICE_UNAVAILABLE",
            Self::ResultUncertain => "DEVELOPMENT_RESULT_UNCERTAIN",
            Self::Corruption => "DEVELOPMENT_STATE_CORRUPT",
            Self::InvalidResponse => "DEVELOPMENT_RESPONSE_INVALID",
            Self::Internal => "DEVELOPMENT_CLIENT_INTERNAL",
        }
    }

    /// Whether the client may automatically repeat the exact encoded request.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Busy | Self::Unavailable | Self::ResultUncertain)
    }
}

/// Aggregate process-local counters without endpoint/Workspace labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DevelopmentClientTelemetrySnapshot {
    /// HTTP attempts sent.
    pub attempts: u64,
    /// Calls that completed successfully.
    pub successes: u64,
    /// Additional attempts after the first.
    pub retries: u64,
    /// Terminal invalid/malformed responses.
    pub invalid_responses: u64,
    /// Terminal retryable failures after budget exhaustion.
    pub exhausted: u64,
}

#[derive(Debug, Default)]
struct Counters {
    attempts: AtomicU64,
    successes: AtomicU64,
    retries: AtomicU64,
    invalid_responses: AtomicU64,
    exhausted: AtomicU64,
}

/// Immutable authenticated client. Debug output redacts the bearer.
pub struct DevelopmentClient {
    endpoint: DevelopmentEndpoint,
    bearer: ParsedDevelopmentKey,
    config: DevelopmentClientConfig,
    http: reqwest::Client,
    counters: Counters,
}

impl fmt::Debug for DevelopmentClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DevelopmentClient")
            .field("endpoint", &self.endpoint)
            .field("bearer", &"[REDACTED]")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DevelopmentClient {
    /// Creates a rustls, no-proxy, no-cookie, no-redirect client with one parsed Development key.
    ///
    /// # Errors
    ///
    /// Rejects invalid configuration/key or failure to construct the trusted HTTP client.
    #[allow(clippy::needless_pass_by_value)] // Ownership drops the caller's secret promptly.
    pub fn new(
        endpoint: DevelopmentEndpoint,
        bearer: String,
        config: DevelopmentClientConfig,
    ) -> Result<Self, DevelopmentClientError> {
        config.validate()?;
        let bearer = bearer
            .parse()
            .map_err(|_| DevelopmentClientError::InvalidConfig)?;
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| DevelopmentClientError::Internal)?;
        Ok(Self {
            endpoint,
            bearer,
            config,
            http,
            counters: Counters::default(),
        })
    }

    /// Fetches trusted remote state with bounded retry of transient failures.
    ///
    /// # Errors
    ///
    /// Returns strict local/network/server/protocol failures.
    pub async fn state(
        &self,
        request: &DevelopmentStateRequestV1,
    ) -> Result<DevelopmentStateResponseV1, DevelopmentClientError> {
        let body = encode_development_state_request_v1(request).map_err(map_request)?;
        self.execute(
            "/v1/development/state",
            JSON_CONTENT_TYPE,
            body,
            StatusCode::OK,
            false,
            decode_development_state_response_v1,
        )
        .await
    }

    /// Creates one Workspace idempotently using the caller's stable Operation ID.
    ///
    /// # Errors
    ///
    /// Returns strict local/network/server/protocol failures.
    pub async fn create_workspace(
        &self,
        request: &DevelopmentCreateWorkspaceRequestV1,
    ) -> Result<DevelopmentCreateWorkspaceResponseV1, DevelopmentClientError> {
        let body = encode_development_create_request_v1(request).map_err(map_request)?;
        self.execute(
            "/v1/development/workspaces",
            JSON_CONTENT_TYPE,
            body,
            StatusCode::CREATED,
            true,
            decode_development_create_response_v1,
        )
        .await
    }

    /// Publishes one prevalidated canonical package and retries the exact frame byte-for-byte.
    ///
    /// # Errors
    ///
    /// Returns strict local/network/server/protocol failures.
    pub async fn publish(
        &self,
        request: &DevelopmentPublishRequestV1,
    ) -> Result<DevelopmentPublishResponseV1, DevelopmentClientError> {
        let body = encode_development_publish_request_v1(request).map_err(map_request)?;
        self.execute(
            "/v1/development/publish",
            PUBLISH_CONTENT_TYPE,
            body,
            StatusCode::OK,
            true,
            decode_development_publish_response_v1,
        )
        .await
    }

    /// Validates and explicitly makes one candidate Release servable, or returns a compatibility
    /// blocked success result.
    ///
    /// # Errors
    ///
    /// Returns strict local/network/server/protocol failures.
    pub async fn freeze(
        &self,
        request: &DevelopmentFreezeRequestV1,
    ) -> Result<DevelopmentFreezeResponseV1, DevelopmentClientError> {
        let body = encode_development_freeze_request_v1(request).map_err(map_request)?;
        self.execute(
            "/v1/development/freeze",
            JSON_CONTENT_TYPE,
            body,
            StatusCode::OK,
            true,
            decode_development_freeze_response_v1,
        )
        .await
    }

    /// Returns aggregate non-cardinal counters.
    #[must_use]
    pub fn telemetry(&self) -> DevelopmentClientTelemetrySnapshot {
        DevelopmentClientTelemetrySnapshot {
            attempts: self.counters.attempts.load(Ordering::Relaxed),
            successes: self.counters.successes.load(Ordering::Relaxed),
            retries: self.counters.retries.load(Ordering::Relaxed),
            invalid_responses: self.counters.invalid_responses.load(Ordering::Relaxed),
            exhausted: self.counters.exhausted.load(Ordering::Relaxed),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute<T, F>(
        &self,
        path: &str,
        content_type: &'static str,
        body: Vec<u8>,
        success_status: StatusCode,
        mutation: bool,
        decode: F,
    ) -> Result<T, DevelopmentClientError>
    where
        F: Fn(&[u8]) -> Result<T, ProtocolError>,
        T: HasRequestId,
    {
        let body = Bytes::from(body);
        for attempt in 0..self.config.maximum_attempts {
            if attempt != 0 {
                self.counters.retries.fetch_add(1, Ordering::Relaxed);
            }
            self.counters.attempts.fetch_add(1, Ordering::Relaxed);
            match self
                .attempt(
                    path,
                    content_type,
                    body.clone(),
                    success_status,
                    mutation,
                    &decode,
                )
                .await
            {
                Ok(value) => {
                    self.counters.successes.fetch_add(1, Ordering::Relaxed);
                    return Ok(value);
                }
                Err((error, retry_after)) => {
                    if !error.retryable() || attempt + 1 == self.config.maximum_attempts {
                        if error.retryable() {
                            self.counters.exhausted.fetch_add(1, Ordering::Relaxed);
                        }
                        if error == DevelopmentClientError::InvalidResponse {
                            self.counters
                                .invalid_responses
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        return Err(error);
                    }
                    tokio::time::sleep(retry_after.unwrap_or(self.config.retry_delay)).await;
                }
            }
        }
        Err(DevelopmentClientError::Internal)
    }

    #[allow(clippy::too_many_arguments)]
    async fn attempt<T, F>(
        &self,
        path: &str,
        content_type: &'static str,
        body: Bytes,
        success_status: StatusCode,
        mutation: bool,
        decode: &F,
    ) -> Result<T, (DevelopmentClientError, Option<Duration>)>
    where
        F: Fn(&[u8]) -> Result<T, ProtocolError>,
        T: HasRequestId,
    {
        let response = self
            .http
            .post(self.endpoint.route(path))
            .header(header::CONTENT_TYPE, content_type)
            .bearer_auth(self.bearer.key().expose())
            .body(body)
            .send()
            .await
            .map_err(|_| {
                (
                    if mutation {
                        DevelopmentClientError::ResultUncertain
                    } else {
                        DevelopmentClientError::Unavailable
                    },
                    None,
                )
            })?;
        let status = response.status();
        let headers = response.headers().clone();
        if exact_header(&headers, header::CONTENT_TYPE.as_str()) != Some(JSON_RESPONSE_CONTENT_TYPE)
        {
            return Err((DevelopmentClientError::InvalidResponse, None));
        }
        let request_id = exact_header(&headers, REQUEST_ID_HEADER)
            .ok_or((DevelopmentClientError::InvalidResponse, None))?
            .parse::<RequestId>()
            .map_err(|_| (DevelopmentClientError::InvalidResponse, None))?;
        let retry_after = parse_retry_after(&headers)?;
        let bytes = read_bounded(response, DEVELOPMENT_JSON_MAX_BYTES).await?;
        if status == success_status {
            let value = decode(&bytes)
                .map_err(|_| (DevelopmentClientError::InvalidResponse, retry_after))?;
            if value.request_id() != request_id {
                return Err((DevelopmentClientError::InvalidResponse, retry_after));
            }
            return Ok(value);
        }
        let error = decode_development_error_v1(&bytes)
            .map_err(|_| (DevelopmentClientError::InvalidResponse, retry_after))?;
        if error.request_id != request_id {
            return Err((DevelopmentClientError::InvalidResponse, retry_after));
        }
        let mapped = map_remote(error.error);
        if status != status_for(mapped) {
            return Err((DevelopmentClientError::InvalidResponse, retry_after));
        }
        Err((mapped, retry_after))
    }
}

trait HasRequestId {
    fn request_id(&self) -> RequestId;
}

impl HasRequestId for DevelopmentStateResponseV1 {
    fn request_id(&self) -> RequestId {
        self.request_id
    }
}

impl HasRequestId for DevelopmentCreateWorkspaceResponseV1 {
    fn request_id(&self) -> RequestId {
        self.request_id
    }
}

impl HasRequestId for DevelopmentPublishResponseV1 {
    fn request_id(&self) -> RequestId {
        self.request_id
    }
}

impl HasRequestId for DevelopmentFreezeResponseV1 {
    fn request_id(&self) -> RequestId {
        self.request_id
    }
}

async fn read_bounded(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, (DevelopmentClientError, Option<Duration>)> {
    if response
        .content_length()
        .is_some_and(|length| length > u64::try_from(limit).unwrap_or(u64::MAX))
    {
        return Err((DevelopmentClientError::InvalidResponse, None));
    }
    let mut output = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| (DevelopmentClientError::InvalidResponse, None))?
    {
        if output
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > limit)
        {
            return Err((DevelopmentClientError::InvalidResponse, None));
        }
        output.extend_from_slice(&chunk);
    }
    if output.is_empty() {
        return Err((DevelopmentClientError::InvalidResponse, None));
    }
    Ok(output)
}

fn exact_header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(first)
}

fn parse_retry_after(
    headers: &reqwest::header::HeaderMap,
) -> Result<Option<Duration>, (DevelopmentClientError, Option<Duration>)> {
    let Some(value) = exact_header(headers, header::RETRY_AFTER.as_str()) else {
        return Ok(None);
    };
    let seconds = value
        .parse::<u64>()
        .ok()
        .filter(|seconds| *seconds <= MAX_RETRY_AFTER_SECONDS)
        .ok_or((DevelopmentClientError::InvalidResponse, None))?;
    Ok(Some(Duration::from_secs(seconds)))
}

fn map_request(error: ProtocolError) -> DevelopmentClientError {
    if error == ProtocolError::LimitExceeded {
        DevelopmentClientError::LimitExceeded
    } else {
        DevelopmentClientError::InvalidRequest
    }
}

const fn map_remote(error: DevelopmentAdminErrorCodeV1) -> DevelopmentClientError {
    match error {
        DevelopmentAdminErrorCodeV1::InvalidRequest => DevelopmentClientError::InvalidRequest,
        DevelopmentAdminErrorCodeV1::Unauthenticated => DevelopmentClientError::Unauthenticated,
        DevelopmentAdminErrorCodeV1::Forbidden => DevelopmentClientError::Forbidden,
        DevelopmentAdminErrorCodeV1::NotFound => DevelopmentClientError::NotFound,
        DevelopmentAdminErrorCodeV1::Conflict => DevelopmentClientError::Conflict,
        DevelopmentAdminErrorCodeV1::PolicyDenied => DevelopmentClientError::PolicyDenied,
        DevelopmentAdminErrorCodeV1::LimitExceeded => DevelopmentClientError::LimitExceeded,
        DevelopmentAdminErrorCodeV1::Busy => DevelopmentClientError::Busy,
        DevelopmentAdminErrorCodeV1::Unavailable => DevelopmentClientError::Unavailable,
        DevelopmentAdminErrorCodeV1::ResultUncertain => DevelopmentClientError::ResultUncertain,
        DevelopmentAdminErrorCodeV1::Corruption => DevelopmentClientError::Corruption,
        DevelopmentAdminErrorCodeV1::Internal => DevelopmentClientError::Internal,
    }
}

const fn status_for(error: DevelopmentClientError) -> StatusCode {
    match error {
        DevelopmentClientError::InvalidRequest => StatusCode::BAD_REQUEST,
        DevelopmentClientError::Unauthenticated => StatusCode::UNAUTHORIZED,
        DevelopmentClientError::Forbidden | DevelopmentClientError::PolicyDenied => {
            StatusCode::FORBIDDEN
        }
        DevelopmentClientError::NotFound => StatusCode::NOT_FOUND,
        DevelopmentClientError::Conflict => StatusCode::CONFLICT,
        DevelopmentClientError::LimitExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        DevelopmentClientError::Busy => StatusCode::TOO_MANY_REQUESTS,
        DevelopmentClientError::Unavailable | DevelopmentClientError::ResultUncertain => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        DevelopmentClientError::Corruption
        | DevelopmentClientError::InvalidConfig
        | DevelopmentClientError::InvalidResponse
        | DevelopmentClientError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
