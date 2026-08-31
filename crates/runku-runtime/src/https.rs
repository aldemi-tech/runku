//! Mediated HTTPS egress for capability-authorized Actions.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use reqwest::{
    Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::CancellationToken;

const MAX_ORIGINS: usize = 128;
const MAX_METHODS: usize = 6;
const MAX_REDIRECTS: u8 = 10;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEADER_VALUES: usize = 256;
const MAX_CALL_TIMEOUT: Duration = Duration::from_mins(1);
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

/// HTTP method supported by Action HTTPS v1.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpsMethod {
    /// Read a resource.
    Get,
    /// Read response metadata without a body.
    Head,
    /// Create/submit a resource or effect.
    Post,
    /// Replace a resource.
    Put,
    /// Partially modify a resource.
    Patch,
    /// Delete a resource.
    Delete,
}

impl HttpsMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }

    const fn has_side_effect_semantics(self) -> bool {
        !matches!(self, Self::Get | Self::Head)
    }
}

/// Exact HTTPS origin authorized by an egress policy.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HttpsOrigin {
    host: String,
    port: u16,
}

impl HttpsOrigin {
    /// Returns the normalized ASCII hostname or IP literal.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the explicit/effective TLS port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    fn allows(&self, url: &Url) -> bool {
        normalized_url_host(url).is_some_and(|host| host == self.host)
            && url.port_or_known_default() == Some(self.port)
    }
}

impl fmt::Display for HttpsOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "https://[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "https://{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for HttpsOrigin {
    type Err = HttpsError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(value).map_err(|_| HttpsError::InvalidRequest)?;
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(HttpsError::InvalidRequest);
        }
        Ok(Self {
            host: normalized_url_host(&url)
                .ok_or(HttpsError::InvalidRequest)?
                .to_owned(),
            port: url
                .port_or_known_default()
                .ok_or(HttpsError::InvalidRequest)?,
        })
    }
}

/// Immutable validated Action HTTPS limits and allowlist.
#[derive(Clone, Debug)]
pub struct HttpsPolicy {
    origins: BTreeSet<HttpsOrigin>,
    methods: BTreeSet<HttpsMethod>,
    max_redirects: u8,
    max_request_body_bytes: usize,
    max_response_body_bytes: usize,
    max_header_bytes: usize,
    max_header_values: usize,
    call_timeout: Duration,
}

impl HttpsPolicy {
    /// Starts a policy builder from non-empty exact origins and methods.
    #[must_use]
    pub fn builder<I, M>(origins: I, methods: M) -> HttpsPolicyBuilder
    where
        I: IntoIterator<Item = HttpsOrigin>,
        M: IntoIterator<Item = HttpsMethod>,
    {
        HttpsPolicyBuilder {
            origins: origins.into_iter().collect(),
            methods: methods.into_iter().collect(),
            max_redirects: 3,
            max_request_body_bytes: 1024 * 1024,
            max_response_body_bytes: 4 * 1024 * 1024,
            max_header_bytes: 16 * 1024,
            max_header_values: 100,
            call_timeout: Duration::from_secs(15),
        }
    }
}

/// Builder for a bounded exact-origin HTTPS policy.
#[derive(Clone, Debug)]
pub struct HttpsPolicyBuilder {
    origins: BTreeSet<HttpsOrigin>,
    methods: BTreeSet<HttpsMethod>,
    max_redirects: u8,
    max_request_body_bytes: usize,
    max_response_body_bytes: usize,
    max_header_bytes: usize,
    max_header_values: usize,
    call_timeout: Duration,
}

impl HttpsPolicyBuilder {
    /// Sets the maximum manual same-origin redirects.
    #[must_use]
    pub const fn max_redirects(mut self, value: u8) -> Self {
        self.max_redirects = value;
        self
    }

    /// Sets the request body byte limit.
    #[must_use]
    pub const fn max_request_body_bytes(mut self, value: usize) -> Self {
        self.max_request_body_bytes = value;
        self
    }

    /// Sets the final response body byte limit.
    #[must_use]
    pub const fn max_response_body_bytes(mut self, value: usize) -> Self {
        self.max_response_body_bytes = value;
        self
    }

    /// Sets the aggregate request/response header byte limit.
    #[must_use]
    pub const fn max_header_bytes(mut self, value: usize) -> Self {
        self.max_header_bytes = value;
        self
    }

    /// Sets the aggregate request/response header value count limit.
    #[must_use]
    pub const fn max_header_values(mut self, value: usize) -> Self {
        self.max_header_values = value;
        self
    }

    /// Sets a timeout capped by the invocation deadline.
    #[must_use]
    pub const fn call_timeout(mut self, value: Duration) -> Self {
        self.call_timeout = value;
        self
    }

    /// Validates and constructs the immutable policy.
    ///
    /// # Errors
    ///
    /// Rejects empty/excessive allowlists or limits outside hard v1 maxima.
    pub fn build(self) -> Result<HttpsPolicy, HttpsError> {
        if self.origins.is_empty()
            || self.origins.len() > MAX_ORIGINS
            || self.methods.is_empty()
            || self.methods.len() > MAX_METHODS
            || self.max_redirects > MAX_REDIRECTS
            || self.max_request_body_bytes > MAX_BODY_BYTES
            || self.max_response_body_bytes == 0
            || self.max_response_body_bytes > MAX_BODY_BYTES
            || self.max_header_bytes == 0
            || self.max_header_bytes > MAX_HEADER_BYTES
            || self.max_header_values == 0
            || self.max_header_values > MAX_HEADER_VALUES
            || self.call_timeout < Duration::from_millis(1)
            || self.call_timeout > MAX_CALL_TIMEOUT
        {
            return Err(HttpsError::InvalidPolicy);
        }
        Ok(HttpsPolicy {
            origins: self.origins,
            methods: self.methods,
            max_redirects: self.max_redirects,
            max_request_body_bytes: self.max_request_body_bytes,
            max_response_body_bytes: self.max_response_body_bytes,
            max_header_bytes: self.max_header_bytes,
            max_header_values: self.max_header_values,
            call_timeout: self.call_timeout,
        })
    }
}

/// Typed request accepted by the mediated broker and `platform-js-1` Op.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpsRequest {
    /// Allowed method.
    pub method: HttpsMethod,
    /// Absolute HTTPS URL within an exact policy origin.
    pub url: String,
    /// Header names to one or more values; normalized by the broker.
    #[serde(default)]
    pub headers: BTreeMap<String, Vec<String>>,
    /// Exact request bytes.
    #[serde(default)]
    pub body: Vec<u8>,
    /// Optional external-provider deduplication key.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

/// Typed final HTTPS response returned to an Action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HttpsResponse {
    /// HTTP status code without implicit success/error conversion.
    pub status: u16,
    /// Lowercase response headers preserving repeated values.
    pub headers: BTreeMap<String, Vec<String>>,
    /// Exact bounded response bytes.
    pub body: Vec<u8>,
}

/// Stable HTTPS broker failure independent of reqwest/DNS implementation details.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum HttpsError {
    /// Policy construction is invalid.
    #[error("HTTPS egress policy is invalid")]
    InvalidPolicy,
    /// Request URL/method/header/idempotency input is malformed.
    #[error("HTTPS request is invalid")]
    InvalidRequest,
    /// Request or redirect is outside the explicit policy.
    #[error("HTTPS request is denied by policy")]
    PolicyDenied,
    /// DNS resolution failed before connecting.
    #[error("HTTPS DNS resolution failed")]
    DnsUnavailable,
    /// An IP literal/answer is not globally routable and permitted.
    #[error("HTTPS destination address is denied")]
    AddressDenied,
    /// Redirect is missing/invalid, cross-origin, looping, excessive, or method-unsafe.
    #[error("HTTPS redirect is denied")]
    RedirectDenied,
    /// Request/header/body exceeds a hard configured limit.
    #[error("HTTPS request exceeds a limit")]
    RequestLimitExceeded,
    /// Response header/body exceeds a hard configured limit.
    #[error("HTTPS response exceeds a limit")]
    ResponseLimitExceeded,
    /// Invocation or egress call deadline elapsed; remote result may be uncertain.
    #[error("HTTPS request timed out")]
    Timeout,
    /// Invocation cancellation won; remote result may be uncertain.
    #[error("HTTPS request was cancelled")]
    Cancelled,
    /// TLS/connect/protocol/body failure; remote result may be uncertain.
    #[error("HTTPS transport failed")]
    Transport,
}

impl HttpsError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidPolicy => "HTTPS_POLICY_INVALID",
            Self::InvalidRequest => "HTTPS_REQUEST_INVALID",
            Self::PolicyDenied => "HTTPS_POLICY_DENIED",
            Self::DnsUnavailable => "HTTPS_DNS_UNAVAILABLE",
            Self::AddressDenied => "HTTPS_ADDRESS_DENIED",
            Self::RedirectDenied => "HTTPS_REDIRECT_DENIED",
            Self::RequestLimitExceeded => "HTTPS_REQUEST_LIMIT_EXCEEDED",
            Self::ResponseLimitExceeded => "HTTPS_RESPONSE_LIMIT_EXCEEDED",
            Self::Timeout => "HTTPS_TIMEOUT",
            Self::Cancelled => "HTTPS_CANCELLED",
            Self::Transport => "HTTPS_TRANSPORT_FAILED",
        }
    }

    /// Only DNS failures are known to occur before request bytes can be sent.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::DnsUnavailable)
    }
}

/// DNS boundary used before address validation/pinning.
#[async_trait]
pub trait DnsResolver: fmt::Debug + Send + Sync {
    /// Resolves all addresses for one canonical hostname.
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, HttpsError>;
}

/// Tokio/system resolver implementation for self-hosted Product Base nodes.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemDnsResolver;

#[async_trait]
impl DnsResolver for SystemDnsResolver {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, HttpsError> {
        let addresses = tokio::net::lookup_host((host, 0))
            .await
            .map_err(|_| HttpsError::DnsUnavailable)?
            .map(|address| address.ip())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(HttpsError::DnsUnavailable);
        }
        Ok(addresses)
    }
}

/// Capability broker injected into one trusted invocation.
#[async_trait]
pub trait HttpsEgress: fmt::Debug + Send + Sync {
    /// Executes exactly one logical request under invocation deadline/cancellation.
    async fn execute(
        &self,
        request: HttpsRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HttpsResponse, HttpsError>;
}

/// Bounded aggregate HTTPS counters with no request-controlled labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpsTelemetrySnapshot {
    /// Logical calls attempted.
    pub calls: u64,
    /// Final non-redirect responses returned.
    pub succeeded: u64,
    /// Redirect hops followed.
    pub redirects: u64,
    /// Request body bytes accepted.
    pub request_bytes: u64,
    /// Final response body bytes returned.
    pub response_bytes: u64,
    /// Final informational responses.
    pub status_1xx: u64,
    /// Final successful responses.
    pub status_2xx: u64,
    /// Final redirect responses (normally zero because redirects are mediated).
    pub status_3xx: u64,
    /// Final caller-error responses.
    pub status_4xx: u64,
    /// Final upstream-error responses.
    pub status_5xx: u64,
    /// Aggregate elapsed microseconds across completed calls, saturating at `u64::MAX`.
    pub elapsed_micros: u64,
    /// Policy/input/address denials.
    pub denied: u64,
    /// DNS resolution failures.
    pub dns_failures: u64,
    /// Timeout/cancellation failures.
    pub terminated: u64,
    /// Request/response limit failures.
    pub limit_failures: u64,
    /// TLS/connect/protocol failures.
    pub transport_failures: u64,
}

#[derive(Debug, Default)]
struct HttpsTelemetry {
    calls: AtomicU64,
    succeeded: AtomicU64,
    redirects: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    status_1xx: AtomicU64,
    status_2xx: AtomicU64,
    status_3xx: AtomicU64,
    status_4xx: AtomicU64,
    status_5xx: AtomicU64,
    elapsed_micros: AtomicU64,
    denied: AtomicU64,
    dns_failures: AtomicU64,
    terminated: AtomicU64,
    limit_failures: AtomicU64,
    transport_failures: AtomicU64,
}

impl HttpsTelemetry {
    fn record_error(&self, error: HttpsError) {
        let counter = match error {
            HttpsError::InvalidPolicy
            | HttpsError::InvalidRequest
            | HttpsError::PolicyDenied
            | HttpsError::AddressDenied
            | HttpsError::RedirectDenied => &self.denied,
            HttpsError::DnsUnavailable => &self.dns_failures,
            HttpsError::RequestLimitExceeded | HttpsError::ResponseLimitExceeded => {
                &self.limit_failures
            }
            HttpsError::Timeout | HttpsError::Cancelled => &self.terminated,
            HttpsError::Transport => &self.transport_failures,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> HttpsTelemetrySnapshot {
        HttpsTelemetrySnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            redirects: self.redirects.load(Ordering::Relaxed),
            request_bytes: self.request_bytes.load(Ordering::Relaxed),
            response_bytes: self.response_bytes.load(Ordering::Relaxed),
            status_1xx: self.status_1xx.load(Ordering::Relaxed),
            status_2xx: self.status_2xx.load(Ordering::Relaxed),
            status_3xx: self.status_3xx.load(Ordering::Relaxed),
            status_4xx: self.status_4xx.load(Ordering::Relaxed),
            status_5xx: self.status_5xx.load(Ordering::Relaxed),
            elapsed_micros: self.elapsed_micros.load(Ordering::Relaxed),
            denied: self.denied.load(Ordering::Relaxed),
            dns_failures: self.dns_failures.load(Ordering::Relaxed),
            terminated: self.terminated.load(Ordering::Relaxed),
            limit_failures: self.limit_failures.load(Ordering::Relaxed),
            transport_failures: self.transport_failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug)]
struct PinnedRequest {
    method: HttpsMethod,
    url: Url,
    headers: BTreeMap<String, Vec<String>>,
    body: Vec<u8>,
    addresses: Vec<SocketAddr>,
}

#[derive(Clone, Debug)]
struct TransportResponse {
    status: u16,
    headers: BTreeMap<String, Vec<String>>,
    body: Vec<u8>,
}

#[async_trait]
trait HttpsTransport: fmt::Debug + Send + Sync {
    async fn send(
        &self,
        request: PinnedRequest,
        policy: &HttpsPolicy,
        timeout: Duration,
    ) -> Result<TransportResponse, HttpsError>;
}

/// Production mediated client using exact-origin policy, validated/pinned DNS, and rustls.
#[derive(Clone, Debug)]
pub struct MediatedHttpsClient {
    policy: HttpsPolicy,
    resolver: Arc<dyn DnsResolver>,
    transport: Arc<dyn HttpsTransport>,
    telemetry: Arc<HttpsTelemetry>,
}

impl MediatedHttpsClient {
    /// Creates a production client with the system resolver and hardened reqwest/rustls transport.
    #[must_use]
    pub fn new(policy: HttpsPolicy) -> Self {
        Self::with_resolver(policy, Arc::new(SystemDnsResolver))
    }

    /// Creates a production client with an operator-provided resolver.
    #[must_use]
    pub fn with_resolver(policy: HttpsPolicy, resolver: Arc<dyn DnsResolver>) -> Self {
        Self {
            policy,
            resolver,
            transport: Arc::new(ReqwestTransport),
            telemetry: Arc::new(HttpsTelemetry::default()),
        }
    }

    /// Returns bounded aggregate counters.
    #[must_use]
    pub fn telemetry(&self) -> HttpsTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    #[cfg(test)]
    fn with_components(
        policy: HttpsPolicy,
        resolver: Arc<dyn DnsResolver>,
        transport: Arc<dyn HttpsTransport>,
    ) -> Self {
        Self {
            policy,
            resolver,
            transport,
            telemetry: Arc::new(HttpsTelemetry::default()),
        }
    }

    async fn execute_inner(
        &self,
        mut request: HttpsRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HttpsResponse, HttpsError> {
        normalize_request(&mut request, &self.policy)?;
        let call_deadline = deadline.min(
            Instant::now()
                .checked_add(self.policy.call_timeout)
                .ok_or(HttpsError::InvalidPolicy)?,
        );
        let mut url = validate_url(&request.url, &self.policy)?;
        let mut method = request.method;
        let mut body = request.body;
        let mut headers = request.headers;
        let mut visited = BTreeSet::new();
        for redirect_count in 0..=self.policy.max_redirects {
            if !visited.insert(url.as_str().to_owned()) {
                return Err(HttpsError::RedirectDenied);
            }
            let addresses = self
                .resolve_and_validate(&url, call_deadline, cancellation.clone())
                .await?;
            let remaining = call_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(HttpsError::Timeout);
            }
            let pinned = PinnedRequest {
                method,
                url: url.clone(),
                headers: headers.clone(),
                body: body.clone(),
                addresses,
            };
            let transport = self.transport.send(pinned, &self.policy, remaining);
            let response = tokio::select! {
                () = cancellation.state().cancelled() => return Err(HttpsError::Cancelled),
                result = tokio::time::timeout_at(tokio::time::Instant::from_std(call_deadline), transport) => {
                    result.map_err(|_| HttpsError::Timeout)??
                }
            };
            validate_transport_response(&response, &self.policy)?;
            if let Some(next) =
                redirect_target(&url, method, &response, redirect_count, &self.policy)?
            {
                self.telemetry.redirects.fetch_add(1, Ordering::Relaxed);
                if response.status == 303 {
                    method = HttpsMethod::Get;
                    body.clear();
                    headers.remove("content-type");
                    headers.remove("content-encoding");
                    headers.remove("idempotency-key");
                }
                url = next;
                continue;
            }
            return Ok(HttpsResponse {
                status: response.status,
                headers: response.headers,
                body: response.body,
            });
        }
        Err(HttpsError::RedirectDenied)
    }

    async fn resolve_and_validate(
        &self,
        url: &Url,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Vec<SocketAddr>, HttpsError> {
        let host = normalized_url_host(url).ok_or(HttpsError::InvalidRequest)?;
        let port = url
            .port_or_known_default()
            .ok_or(HttpsError::InvalidRequest)?;
        let ips = if let Ok(ip) = host.parse::<IpAddr>() {
            vec![ip]
        } else {
            let resolve = self.resolver.resolve(host);
            tokio::select! {
                () = cancellation.state().cancelled() => return Err(HttpsError::Cancelled),
                result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), resolve) => {
                    result.map_err(|_| HttpsError::Timeout)??
                }
            }
        };
        validate_addresses(ips, port)
    }
}

#[async_trait]
impl HttpsEgress for MediatedHttpsClient {
    async fn execute(
        &self,
        request: HttpsRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HttpsResponse, HttpsError> {
        let started = Instant::now();
        self.telemetry.calls.fetch_add(1, Ordering::Relaxed);
        self.telemetry.request_bytes.fetch_add(
            u64::try_from(request.body.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let result = self.execute_inner(request, deadline, cancellation).await;
        match &result {
            Ok(response) => {
                self.telemetry.succeeded.fetch_add(1, Ordering::Relaxed);
                let status_counter = match response.status / 100 {
                    1 => Some(&self.telemetry.status_1xx),
                    2 => Some(&self.telemetry.status_2xx),
                    3 => Some(&self.telemetry.status_3xx),
                    4 => Some(&self.telemetry.status_4xx),
                    5 => Some(&self.telemetry.status_5xx),
                    _ => None,
                };
                if let Some(counter) = status_counter {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                self.telemetry.response_bytes.fetch_add(
                    u64::try_from(response.body.len()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            Err(error) => self.telemetry.record_error(*error),
        }
        let _ = self.telemetry.elapsed_micros.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| {
                let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                Some(current.saturating_add(elapsed))
            },
        );
        result
    }
}

#[derive(Clone, Copy, Debug)]
struct ReqwestTransport;

#[async_trait]
impl HttpsTransport for ReqwestTransport {
    async fn send(
        &self,
        request: PinnedRequest,
        policy: &HttpsPolicy,
        timeout: Duration,
    ) -> Result<TransportResponse, HttpsError> {
        let host = normalized_url_host(&request.url).ok_or(HttpsError::InvalidRequest)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .tls_backend_rustls()
            .connect_timeout(timeout.min(Duration::from_secs(10)))
            .timeout(timeout)
            .resolve_to_addrs(host, &request.addresses)
            .build()
            .map_err(|_| HttpsError::Transport)?;
        let method = reqwest::Method::from_bytes(request.method.as_str().as_bytes())
            .map_err(|_| HttpsError::InvalidRequest)?;
        let mut builder = client.request(method, request.url).body(request.body);
        for (name, values) in request.headers {
            let name =
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| HttpsError::InvalidRequest)?;
            for value in values {
                let value =
                    HeaderValue::from_str(&value).map_err(|_| HttpsError::InvalidRequest)?;
                builder = builder.header(&name, value);
            }
        }
        let mut response = builder.send().await.map_err(|_| HttpsError::Transport)?;
        let status = response.status().as_u16();
        let headers = collect_response_headers(response.headers(), policy)?;
        if redirect_status(status) {
            return Ok(TransportResponse {
                status,
                headers,
                body: Vec::new(),
            });
        }
        if response.content_length().is_some_and(|length| {
            length > u64::try_from(policy.max_response_body_bytes).unwrap_or(u64::MAX)
        }) {
            return Err(HttpsError::ResponseLimitExceeded);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| HttpsError::Transport)? {
            let next = body
                .len()
                .checked_add(chunk.len())
                .ok_or(HttpsError::ResponseLimitExceeded)?;
            if next > policy.max_response_body_bytes {
                return Err(HttpsError::ResponseLimitExceeded);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(TransportResponse {
            status,
            headers,
            body,
        })
    }
}

fn normalize_request(request: &mut HttpsRequest, policy: &HttpsPolicy) -> Result<(), HttpsError> {
    if !policy.methods.contains(&request.method)
        || request.body.len() > policy.max_request_body_bytes
    {
        return Err(if request.body.len() > policy.max_request_body_bytes {
            HttpsError::RequestLimitExceeded
        } else {
            HttpsError::PolicyDenied
        });
    }
    let mut normalized = BTreeMap::new();
    let mut header_bytes = 0_usize;
    let mut header_values = 0_usize;
    for (name, values) in std::mem::take(&mut request.headers) {
        let parsed =
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| HttpsError::InvalidRequest)?;
        let canonical = parsed.as_str().to_owned();
        if denied_request_header(&canonical)
            || values.is_empty()
            || normalized.contains_key(&canonical)
        {
            return Err(HttpsError::InvalidRequest);
        }
        for value in &values {
            HeaderValue::from_str(value).map_err(|_| HttpsError::InvalidRequest)?;
            header_bytes = header_bytes
                .checked_add(canonical.len())
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(HttpsError::RequestLimitExceeded)?;
            header_values = header_values
                .checked_add(1)
                .ok_or(HttpsError::RequestLimitExceeded)?;
        }
        normalized.insert(canonical, values);
    }
    if let Some(key) = &request.idempotency_key {
        if key.is_empty()
            || key.len() > MAX_IDEMPOTENCY_KEY_BYTES
            || !key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
            || normalized.contains_key("idempotency-key")
        {
            return Err(HttpsError::InvalidRequest);
        }
        header_bytes = header_bytes
            .checked_add("idempotency-key".len() + key.len())
            .ok_or(HttpsError::RequestLimitExceeded)?;
        header_values = header_values
            .checked_add(1)
            .ok_or(HttpsError::RequestLimitExceeded)?;
        normalized.insert("idempotency-key".to_owned(), vec![key.clone()]);
    }
    if header_bytes > policy.max_header_bytes || header_values > policy.max_header_values {
        return Err(HttpsError::RequestLimitExceeded);
    }
    request.headers = normalized;
    Ok(())
}

fn validate_url(value: &str, policy: &HttpsPolicy) -> Result<Url, HttpsError> {
    let url = Url::parse(value).map_err(|_| HttpsError::InvalidRequest)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err(HttpsError::InvalidRequest);
    }
    if !policy.origins.iter().any(|origin| origin.allows(&url)) {
        return Err(HttpsError::PolicyDenied);
    }
    Ok(url)
}

fn normalized_url_host(url: &Url) -> Option<&str> {
    let host = url.host_str()?;
    Some(
        host.strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .unwrap_or(host),
    )
}

fn denied_request_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "content-length"
            | "expect"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    ) || name.starts_with("proxy-")
}

fn collect_response_headers(
    headers: &HeaderMap,
    policy: &HttpsPolicy,
) -> Result<BTreeMap<String, Vec<String>>, HttpsError> {
    let mut output = BTreeMap::<String, Vec<String>>::new();
    let mut bytes = 0_usize;
    let mut values = 0_usize;
    for (name, value) in headers {
        let value = value.to_str().map_err(|_| HttpsError::Transport)?;
        bytes = bytes
            .checked_add(name.as_str().len())
            .and_then(|size| size.checked_add(value.len()))
            .ok_or(HttpsError::ResponseLimitExceeded)?;
        values = values
            .checked_add(1)
            .ok_or(HttpsError::ResponseLimitExceeded)?;
        if bytes > policy.max_header_bytes || values > policy.max_header_values {
            return Err(HttpsError::ResponseLimitExceeded);
        }
        output
            .entry(name.as_str().to_owned())
            .or_default()
            .push(value.to_owned());
    }
    Ok(output)
}

fn validate_transport_response(
    response: &TransportResponse,
    policy: &HttpsPolicy,
) -> Result<(), HttpsError> {
    if !(100..=599).contains(&response.status) {
        return Err(HttpsError::Transport);
    }
    let mut bytes = 0_usize;
    let mut values = 0_usize;
    for (name, header_values) in &response.headers {
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| HttpsError::Transport)?;
        if header_values.is_empty() {
            return Err(HttpsError::Transport);
        }
        for value in header_values {
            HeaderValue::from_str(value).map_err(|_| HttpsError::Transport)?;
            bytes = bytes
                .checked_add(name.len())
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(HttpsError::ResponseLimitExceeded)?;
            values = values
                .checked_add(1)
                .ok_or(HttpsError::ResponseLimitExceeded)?;
        }
    }
    if bytes > policy.max_header_bytes
        || values > policy.max_header_values
        || response.body.len() > policy.max_response_body_bytes
    {
        return Err(HttpsError::ResponseLimitExceeded);
    }
    Ok(())
}

fn redirect_target(
    current: &Url,
    method: HttpsMethod,
    response: &TransportResponse,
    redirect_count: u8,
    policy: &HttpsPolicy,
) -> Result<Option<Url>, HttpsError> {
    if !redirect_status(response.status) {
        return Ok(None);
    }
    if redirect_count >= policy.max_redirects
        || matches!(response.status, 301 | 302) && method.has_side_effect_semantics()
    {
        return Err(HttpsError::RedirectDenied);
    }
    let locations = response
        .headers
        .get("location")
        .ok_or(HttpsError::RedirectDenied)?;
    if locations.len() != 1 {
        return Err(HttpsError::RedirectDenied);
    }
    let next = current
        .join(&locations[0])
        .map_err(|_| HttpsError::RedirectDenied)?;
    validate_url(next.as_str(), policy).map_err(|_| HttpsError::RedirectDenied)?;
    let current_origin = (
        normalized_url_host(current),
        current.port_or_known_default(),
    );
    let next_origin = (normalized_url_host(&next), next.port_or_known_default());
    if current_origin != next_origin {
        return Err(HttpsError::RedirectDenied);
    }
    Ok(Some(next))
}

const fn redirect_status(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}

fn validate_addresses(ips: Vec<IpAddr>, port: u16) -> Result<Vec<SocketAddr>, HttpsError> {
    if ips.is_empty() {
        return Err(HttpsError::DnsUnavailable);
    }
    let unique = ips.into_iter().collect::<BTreeSet<_>>();
    if unique.iter().any(|ip| !public_ip(*ip)) {
        return Err(HttpsError::AddressDenied);
    }
    Ok(unique
        .into_iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect())
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => public_ipv4(ip),
        IpAddr::V6(ip) => public_ipv6(ip),
    }
}

fn public_ipv4(ip: Ipv4Addr) -> bool {
    let value = u32::from(ip);
    ![
        (0x0000_0000, 8),
        (0x0a00_0000, 8),
        (0x6440_0000, 10),
        (0x7f00_0000, 8),
        (0xa9fe_0000, 16),
        (0xac10_0000, 12),
        (0xc000_0000, 24),
        (0xc000_0200, 24),
        (0xc01f_c400, 24),
        (0xc034_c100, 24),
        (0xc058_6300, 24),
        (0xc0a8_0000, 16),
        (0xc0af_3000, 24),
        (0xc612_0000, 15),
        (0xc633_6400, 24),
        (0xcb00_7100, 24),
        (0xe000_0000, 4),
        (0xf000_0000, 4),
    ]
    .into_iter()
    .any(|(network, prefix)| in_ipv4_prefix(value, network, prefix))
}

const fn in_ipv4_prefix(value: u32, network: u32, prefix: u32) -> bool {
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    value & mask == network & mask
}

fn public_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return public_ipv4(mapped);
    }
    let value = u128::from(ip);
    value != 0
        && value != 1
        && ![
            (0x0064_ff9b_0000_0000_0000_0000_0000_0000_u128, 96),
            (0x0064_ff9b_0001_0000_0000_0000_0000_0000_u128, 48),
            (0x0100_0000_0000_0000_0000_0000_0000_0000_u128, 64),
            (0x2001_0000_0000_0000_0000_0000_0000_0000_u128, 23),
            (0x2001_0db8_0000_0000_0000_0000_0000_0000_u128, 32),
            (0x2002_0000_0000_0000_0000_0000_0000_0000_u128, 16),
            (0x3fff_0000_0000_0000_0000_0000_0000_0000_u128, 20),
            (0xfc00_0000_0000_0000_0000_0000_0000_0000_u128, 7),
            (0xfe80_0000_0000_0000_0000_0000_0000_0000_u128, 10),
            (0xfec0_0000_0000_0000_0000_0000_0000_0000_u128, 10),
            (0xff00_0000_0000_0000_0000_0000_0000_0000_u128, 8),
        ]
        .into_iter()
        .any(|(network, prefix)| in_ipv6_prefix(value, network, prefix))
        && in_ipv6_prefix(value, 0x2000_0000_0000_0000_0000_0000_0000_0000_u128, 3)
}

const fn in_ipv6_prefix(value: u128, network: u128, prefix: u32) -> bool {
    let mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    };
    value & mask == network & mask
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use proptest::prelude::*;

    use super::{
        DnsResolver, HttpsEgress, HttpsError, HttpsMethod, HttpsOrigin, HttpsPolicy, HttpsRequest,
        HttpsTransport, MediatedHttpsClient, PinnedRequest, TransportResponse, public_ip,
        validate_addresses,
    };
    use crate::CancellationToken;

    #[derive(Debug)]
    struct FakeResolver {
        answers: BTreeMap<String, Vec<IpAddr>>,
        delay: Duration,
    }

    #[async_trait]
    impl DnsResolver for FakeResolver {
        async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, HttpsError> {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.answers
                .get(host)
                .cloned()
                .ok_or(HttpsError::DnsUnavailable)
        }
    }

    #[derive(Debug)]
    struct FakeTransport {
        responses: Mutex<VecDeque<Result<TransportResponse, HttpsError>>>,
        seen: Mutex<Vec<PinnedRequest>>,
        delay: Duration,
    }

    impl FakeTransport {
        fn new(responses: Vec<Result<TransportResponse, HttpsError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                seen: Mutex::new(Vec::new()),
                delay: Duration::ZERO,
            }
        }

        fn delayed(response: Result<TransportResponse, HttpsError>, delay: Duration) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from([response])),
                seen: Mutex::new(Vec::new()),
                delay,
            }
        }

        fn seen(&self) -> Result<Vec<PinnedRequest>, HttpsError> {
            self.seen
                .lock()
                .map(|requests| requests.clone())
                .map_err(|_| HttpsError::Transport)
        }
    }

    #[async_trait]
    impl HttpsTransport for FakeTransport {
        async fn send(
            &self,
            request: PinnedRequest,
            _policy: &HttpsPolicy,
            _timeout: Duration,
        ) -> Result<TransportResponse, HttpsError> {
            self.seen
                .lock()
                .map_err(|_| HttpsError::Transport)?
                .push(request);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.responses
                .lock()
                .map_err(|_| HttpsError::Transport)?
                .pop_front()
                .ok_or(HttpsError::Transport)?
        }
    }

    fn origin() -> Result<HttpsOrigin, HttpsError> {
        "https://api.example.com".parse()
    }

    fn policy() -> Result<HttpsPolicy, HttpsError> {
        HttpsPolicy::builder([origin()?], [HttpsMethod::Get, HttpsMethod::Post])
            .max_redirects(3)
            .max_request_body_bytes(32)
            .max_response_body_bytes(32)
            .max_header_bytes(128)
            .max_header_values(8)
            .call_timeout(Duration::from_secs(1))
            .build()
    }

    fn resolver(answers: Vec<IpAddr>) -> Arc<dyn DnsResolver> {
        Arc::new(FakeResolver {
            answers: BTreeMap::from([("api.example.com".to_owned(), answers)]),
            delay: Duration::ZERO,
        })
    }

    fn response(status: u16, location: Option<&str>, body: &[u8]) -> TransportResponse {
        let headers = location.map_or_else(BTreeMap::new, |location| {
            BTreeMap::from([("location".to_owned(), vec![location.to_owned()])])
        });
        TransportResponse {
            status,
            headers,
            body: body.to_vec(),
        }
    }

    fn request(method: HttpsMethod, url: &str) -> HttpsRequest {
        HttpsRequest {
            method,
            url: url.to_owned(),
            headers: BTreeMap::new(),
            body: Vec::new(),
            idempotency_key: None,
        }
    }

    #[tokio::test]
    async fn policy_origin_request_and_idempotency_boundaries_fail_closed() -> Result<(), HttpsError>
    {
        assert_eq!(origin()?.to_string(), "https://api.example.com:443");
        let ipv6_origin = "https://[2606:4700:4700::1111]".parse::<HttpsOrigin>()?;
        assert_eq!(ipv6_origin.host(), "2606:4700:4700::1111");
        assert_eq!(
            ipv6_origin.to_string(),
            "https://[2606:4700:4700::1111]:443"
        );
        for invalid in [
            "http://api.example.com",
            "https://user@api.example.com",
            "https://api.example.com/path",
            "https://api.example.com?x=1",
            "https://api.example.com#fragment",
        ] {
            assert_eq!(
                invalid.parse::<HttpsOrigin>(),
                Err(HttpsError::InvalidRequest)
            );
        }
        assert!(matches!(
            HttpsPolicy::builder([], [HttpsMethod::Get]).build(),
            Err(HttpsError::InvalidPolicy)
        ));
        assert!(matches!(
            HttpsPolicy::builder([origin()?], []).build(),
            Err(HttpsError::InvalidPolicy)
        ));

        let transport = Arc::new(FakeTransport::new(vec![Ok(response(200, None, b"ok"))]));
        let client = MediatedHttpsClient::with_components(
            policy()?,
            resolver(vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]),
            transport,
        );
        let mut invalid_header = request(HttpsMethod::Get, "https://api.example.com/x");
        invalid_header
            .headers
            .insert("Host".to_owned(), vec!["attacker".to_owned()]);
        assert_eq!(
            client
                .execute(
                    invalid_header,
                    Instant::now() + Duration::from_secs(1),
                    CancellationToken::new(),
                )
                .await,
            Err(HttpsError::InvalidRequest)
        );
        let mut invalid_key = request(HttpsMethod::Post, "https://api.example.com/x");
        invalid_key.idempotency_key = Some("contains space".to_owned());
        assert_eq!(
            client
                .execute(
                    invalid_key,
                    Instant::now() + Duration::from_secs(1),
                    CancellationToken::new(),
                )
                .await,
            Err(HttpsError::InvalidRequest)
        );
        Ok(())
    }

    #[test]
    fn public_address_filter_rejects_special_and_mixed_answers() -> Result<(), HttpsError> {
        let denied = [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.168.0.1",
            "192.0.2.1",
            "224.0.0.1",
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
            "fc00::1",
            "fe80::1",
            "ff00::1",
        ];
        for value in denied {
            let ip = value
                .parse::<IpAddr>()
                .map_err(|_| HttpsError::InvalidRequest)?;
            assert!(!public_ip(ip), "unexpectedly public: {value}");
        }
        for value in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
            let ip = value
                .parse::<IpAddr>()
                .map_err(|_| HttpsError::InvalidRequest)?;
            assert!(public_ip(ip), "unexpectedly denied: {value}");
        }
        assert_eq!(
            validate_addresses(
                vec![
                    IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                ],
                443,
            ),
            Err(HttpsError::AddressDenied)
        );
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn all_rfc1918_ipv4_values_are_denied(a in any::<u8>(), b in any::<u8>()) {
            prop_assert!(!public_ip(IpAddr::V4(Ipv4Addr::new(10, a, b, a))));
            prop_assert!(!public_ip(IpAddr::V4(Ipv4Addr::new(192, 168, a, b))));
            prop_assert!(!public_ip(IpAddr::V4(Ipv4Addr::new(172, 16 + (a % 16), b, a))));
        }

        #[test]
        fn all_unique_public_answers_are_pinned(a in 1_u8..=223) {
            let ip = Ipv4Addr::new(8, 8, 8, a);
            let result = validate_addresses(vec![IpAddr::V4(ip), IpAddr::V4(ip)], 8443);
            prop_assert_eq!(result, Ok(vec![SocketAddr::new(IpAddr::V4(ip), 8443)]));
        }
    }

    #[tokio::test]
    async fn success_pins_dns_normalizes_request_and_records_safe_telemetry()
    -> Result<(), HttpsError> {
        let transport = Arc::new(FakeTransport::new(vec![Ok(TransportResponse {
            status: 202,
            headers: BTreeMap::from([("x-result".to_owned(), vec!["yes".to_owned()])]),
            body: vec![0, 1, 255],
        })]));
        let client = MediatedHttpsClient::with_components(
            policy()?,
            resolver(vec![
                IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
                IpAddr::V6(
                    "2606:4700:4700::1111"
                        .parse::<Ipv6Addr>()
                        .map_err(|_| HttpsError::InvalidRequest)?,
                ),
            ]),
            transport.clone(),
        );
        let mut input = request(HttpsMethod::Post, "https://api.example.com/v1?q=redacted");
        input.headers.insert(
            "Content-Type".to_owned(),
            vec!["application/octet-stream".to_owned()],
        );
        input.body = vec![9, 8, 7];
        input.idempotency_key = Some("event_01.test".to_owned());
        let output = client
            .execute(
                input,
                Instant::now() + Duration::from_secs(2),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(output.status, 202);
        assert_eq!(output.body, vec![0, 1, 255]);
        let seen = transport.seen()?;
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, HttpsMethod::Post);
        assert_eq!(seen[0].body, vec![9, 8, 7]);
        assert_eq!(
            seen[0].headers["content-type"],
            ["application/octet-stream"]
        );
        assert_eq!(seen[0].headers["idempotency-key"], ["event_01.test"]);
        assert_eq!(seen[0].addresses.len(), 2);
        let telemetry = client.telemetry();
        assert_eq!(telemetry.calls, 1);
        assert_eq!(telemetry.succeeded, 1);
        assert_eq!(telemetry.status_2xx, 1);
        assert_eq!(telemetry.request_bytes, 3);
        assert_eq!(telemetry.response_bytes, 3);
        Ok(())
    }

    #[tokio::test]
    async fn redirects_are_manual_same_origin_method_safe_and_loop_bounded()
    -> Result<(), HttpsError> {
        let transport = Arc::new(FakeTransport::new(vec![
            Ok(response(303, Some("/done"), &[])),
            Ok(response(200, None, b"done")),
        ]));
        let client = MediatedHttpsClient::with_components(
            policy()?,
            resolver(vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]),
            transport.clone(),
        );
        let mut input = request(HttpsMethod::Post, "https://api.example.com/start");
        input.body = b"effect".to_vec();
        input.idempotency_key = Some("effect_1".to_owned());
        input
            .headers
            .insert("content-type".to_owned(), vec!["text/plain".to_owned()]);
        let output = client
            .execute(
                input,
                Instant::now() + Duration::from_secs(2),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(output.body, b"done");
        let seen = transport.seen()?;
        assert_eq!(seen[0].method, HttpsMethod::Post);
        assert_eq!(seen[1].method, HttpsMethod::Get);
        assert!(seen[1].body.is_empty());
        assert!(seen[1].headers.is_empty());

        for (status, location) in [(301, "/next"), (307, "https://other.example.com/next")] {
            let fake = Arc::new(FakeTransport::new(vec![Ok(response(
                status,
                Some(location),
                &[],
            ))]));
            let denied = MediatedHttpsClient::with_components(
                policy()?,
                resolver(vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]),
                fake.clone(),
            );
            assert_eq!(
                denied
                    .execute(
                        request(HttpsMethod::Post, "https://api.example.com/start"),
                        Instant::now() + Duration::from_secs(1),
                        CancellationToken::new(),
                    )
                    .await,
                Err(HttpsError::RedirectDenied)
            );
            assert_eq!(fake.seen()?.len(), 1);
        }

        let loop_transport = Arc::new(FakeTransport::new(vec![
            Ok(response(307, Some("/b"), &[])),
            Ok(response(307, Some("/a"), &[])),
        ]));
        let loop_client = MediatedHttpsClient::with_components(
            policy()?,
            resolver(vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]),
            loop_transport,
        );
        assert_eq!(
            loop_client
                .execute(
                    request(HttpsMethod::Get, "https://api.example.com/a"),
                    Instant::now() + Duration::from_secs(1),
                    CancellationToken::new(),
                )
                .await,
            Err(HttpsError::RedirectDenied)
        );
        Ok(())
    }

    #[tokio::test]
    async fn policy_dns_response_and_transport_limits_are_stable() -> Result<(), HttpsError> {
        let public = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
        let client = MediatedHttpsClient::with_components(
            policy()?,
            resolver(vec![public, IpAddr::V4(Ipv4Addr::LOCALHOST)]),
            Arc::new(FakeTransport::new(vec![])),
        );
        assert_eq!(
            client
                .execute(
                    request(HttpsMethod::Get, "https://api.example.com"),
                    Instant::now() + Duration::from_secs(1),
                    CancellationToken::new(),
                )
                .await,
            Err(HttpsError::AddressDenied)
        );

        let oversized = Arc::new(FakeTransport::new(vec![Ok(response(200, None, &[1; 33]))]));
        let client =
            MediatedHttpsClient::with_components(policy()?, resolver(vec![public]), oversized);
        assert_eq!(
            client
                .execute(
                    request(HttpsMethod::Get, "https://api.example.com"),
                    Instant::now() + Duration::from_secs(1),
                    CancellationToken::new(),
                )
                .await,
            Err(HttpsError::ResponseLimitExceeded)
        );

        let transport_failure = Arc::new(FakeTransport::new(vec![Err(HttpsError::Transport)]));
        let client = MediatedHttpsClient::with_components(
            policy()?,
            resolver(vec![public]),
            transport_failure,
        );
        assert_eq!(
            client
                .execute(
                    request(HttpsMethod::Get, "https://api.example.com"),
                    Instant::now() + Duration::from_secs(1),
                    CancellationToken::new(),
                )
                .await,
            Err(HttpsError::Transport)
        );
        assert_eq!(client.telemetry().transport_failures, 1);
        Ok(())
    }

    #[tokio::test]
    async fn deadline_and_cancellation_cover_dns_and_transport() -> Result<(), HttpsError> {
        let slow_resolver = Arc::new(FakeResolver {
            answers: BTreeMap::from([(
                "api.example.com".to_owned(),
                vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))],
            )]),
            delay: Duration::from_millis(100),
        });
        let client = MediatedHttpsClient::with_components(
            policy()?,
            slow_resolver,
            Arc::new(FakeTransport::new(vec![])),
        );
        assert_eq!(
            client
                .execute(
                    request(HttpsMethod::Get, "https://api.example.com"),
                    Instant::now() + Duration::from_millis(10),
                    CancellationToken::new(),
                )
                .await,
            Err(HttpsError::Timeout)
        );

        let cancellation = CancellationToken::new();
        let cancel_later = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel_later.cancel();
        });
        let slow_transport = Arc::new(FakeTransport::delayed(
            Ok(response(200, None, b"late")),
            Duration::from_millis(100),
        ));
        let client = MediatedHttpsClient::with_components(
            policy()?,
            resolver(vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]),
            slow_transport,
        );
        assert_eq!(
            client
                .execute(
                    request(HttpsMethod::Get, "https://api.example.com"),
                    Instant::now() + Duration::from_secs(1),
                    cancellation,
                )
                .await,
            Err(HttpsError::Cancelled)
        );
        assert_eq!(client.telemetry().terminated, 1);
        Ok(())
    }
}
