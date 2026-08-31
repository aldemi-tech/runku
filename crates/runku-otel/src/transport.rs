use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt as _;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;
use prost::Message as _;
use reqwest::{
    Client, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use thiserror::Error;
use url::{Host, Url};
use zeroize::Zeroizing;

const MAX_HEADERS: usize = 32;
const MAX_HEADER_VALUE_BYTES: usize = 8 * 1024;

/// Validated full OTLP/HTTP logs endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OtlpEndpoint(Url);

impl OtlpEndpoint {
    /// Returns the validated endpoint URL.
    #[must_use]
    pub const fn as_url(&self) -> &Url {
        &self.0
    }
}

impl FromStr for OtlpEndpoint {
    type Err = OtlpTransportError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(value).map_err(|_| OtlpTransportError::InvalidConfiguration)?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/v1/logs"
        {
            return Err(OtlpTransportError::InvalidConfiguration);
        }
        let loopback = match url.host().ok_or(OtlpTransportError::InvalidConfiguration)? {
            Host::Domain(host) => host.eq_ignore_ascii_case("localhost"),
            Host::Ipv4(address) => address.is_loopback(),
            Host::Ipv6(address) => address.is_loopback(),
        };
        if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
            return Err(OtlpTransportError::InvalidConfiguration);
        }
        Ok(Self(url))
    }
}

/// Sensitive OTLP headers with a permanently redacted debug representation.
#[derive(Clone, Default)]
pub struct OtlpHeaders(Vec<(HeaderName, Zeroizing<String>)>);

impl OtlpHeaders {
    /// Builds validated bounded headers from exact names/values.
    ///
    /// # Errors
    ///
    /// Rejects forbidden transport headers, invalid syntax, duplicates, or size/count limits.
    pub fn new(values: BTreeMap<String, String>) -> Result<Self, OtlpTransportError> {
        if values.len() > MAX_HEADERS {
            return Err(OtlpTransportError::InvalidConfiguration);
        }
        let mut headers = Vec::with_capacity(values.len());
        let mut names = BTreeSet::new();
        for (name, value) in values {
            if value.is_empty() || value.len() > MAX_HEADER_VALUE_BYTES {
                return Err(OtlpTransportError::InvalidConfiguration);
            }
            let name = HeaderName::from_str(&name)
                .map_err(|_| OtlpTransportError::InvalidConfiguration)?;
            if !names.insert(name.as_str().to_owned()) {
                return Err(OtlpTransportError::InvalidConfiguration);
            }
            if matches!(
                name.as_str(),
                "content-type"
                    | "content-length"
                    | "content-encoding"
                    | "host"
                    | "connection"
                    | "transfer-encoding"
            ) {
                return Err(OtlpTransportError::InvalidConfiguration);
            }
            HeaderValue::from_str(&value).map_err(|_| OtlpTransportError::InvalidConfiguration)?;
            headers.push((name, Zeroizing::new(value)));
        }
        Ok(Self(headers))
    }

    /// Iterates safe header names without exposing their sensitive values.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|(name, _)| name.as_str())
    }

    fn header_map(&self) -> Result<HeaderMap, OtlpTransportError> {
        let mut headers = HeaderMap::with_capacity(self.0.len());
        for (name, value) in &self.0 {
            headers.insert(
                name.clone(),
                HeaderValue::from_str(value)
                    .map_err(|_| OtlpTransportError::InvalidConfiguration)?,
            );
        }
        Ok(headers)
    }
}

impl fmt::Debug for OtlpHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpHeaders")
            .field("count", &self.0.len())
            .field("values", &"[REDACTED]")
            .finish()
    }
}

/// Bounded OTLP/HTTP client policy.
#[derive(Clone, Debug)]
pub struct OtlpTransportConfig {
    /// Full `/v1/logs` endpoint.
    pub endpoint: OtlpEndpoint,
    /// Operator-supplied authentication/routing headers.
    pub headers: OtlpHeaders,
    /// Complete request deadline.
    pub request_timeout: Duration,
    /// Maximum Protobuf response bytes accepted.
    pub maximum_response_bytes: usize,
}

impl OtlpTransportConfig {
    fn validate(&self) -> Result<(), OtlpTransportError> {
        if !(Duration::from_millis(100)..=Duration::from_mins(2)).contains(&self.request_timeout)
            || !(1..=1024 * 1024).contains(&self.maximum_response_bytes)
        {
            return Err(OtlpTransportError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// One exact OTLP server outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpTransportOutcome {
    /// Entire batch was accepted; checkpoint may advance.
    Accepted,
    /// Retryable transport/status response; checkpoint must remain unchanged.
    Retryable {
        /// Optional bounded delay requested by a delta-seconds `Retry-After` header.
        retry_after: Option<Duration>,
    },
    /// Server permanently rejected the request or partially rejected records.
    Terminal,
}

/// Sanitized OTLP mapping/transport failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OtlpTransportError {
    /// Endpoint, header, or bound is invalid.
    #[error("OTLP transport configuration is invalid")]
    InvalidConfiguration,
    /// Operational Event page cannot be represented safely.
    #[error("OTLP input is invalid")]
    InvalidInput,
    /// Request or response exceeds a configured bound.
    #[error("OTLP payload exceeds a limit")]
    LimitExceeded,
    /// Network operation failed or timed out before a usable response.
    #[error("OTLP transport is unavailable")]
    Unavailable,
    /// HTTP 200 body is not a valid bounded OTLP response.
    #[error("OTLP response is invalid")]
    InvalidResponse,
}

impl OtlpTransportError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "OTLP_CONFIGURATION_INVALID",
            Self::InvalidInput => "OTLP_INPUT_INVALID",
            Self::LimitExceeded => "OTLP_LIMIT_EXCEEDED",
            Self::Unavailable => "OTLP_UNAVAILABLE",
            Self::InvalidResponse => "OTLP_RESPONSE_INVALID",
        }
    }
}

/// Reusable redirect-free OTLP/HTTP binary Protobuf transport.
#[derive(Clone, Debug)]
pub struct OtlpHttpTransport {
    client: Client,
    config: OtlpTransportConfig,
}

/// Injectable OTLP request boundary used by the durable exporter and deterministic tests.
#[async_trait]
pub trait OtlpTransport: fmt::Debug + Send + Sync {
    /// Sends one binary Protobuf request and classifies the complete server response.
    async fn send(&self, payload: Vec<u8>) -> Result<OtlpTransportOutcome, OtlpTransportError>;
}

impl OtlpHttpTransport {
    /// Builds a bounded client with no redirects, cookies, proxy discovery, or implicit auth.
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds or failure to construct the TLS/HTTP client.
    pub fn new(config: OtlpTransportConfig) -> Result<Self, OtlpTransportError> {
        config.validate()?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.request_timeout)
            .no_proxy()
            .build()
            .map_err(|_| OtlpTransportError::InvalidConfiguration)?;
        Ok(Self { client, config })
    }

    /// Sends one already-bounded `ExportLogsServiceRequest` payload.
    ///
    /// # Errors
    ///
    /// Returns unavailable for transport/deadline failure and invalid response for malformed or
    /// oversized HTTP 200 bodies. Retryable HTTP status remains a normal classified outcome.
    async fn send_inner(
        &self,
        payload: Vec<u8>,
    ) -> Result<OtlpTransportOutcome, OtlpTransportError> {
        if payload.is_empty() {
            return Err(OtlpTransportError::InvalidInput);
        }
        let response = self
            .client
            .post(self.config.endpoint.as_url().clone())
            .headers(self.config.headers.header_map()?)
            .header("content-type", "application/x-protobuf")
            .body(payload)
            .send()
            .await
            .map_err(|_| OtlpTransportError::Unavailable)?;
        let status = response.status();
        if status == StatusCode::OK {
            let body = bounded_body(response, self.config.maximum_response_bytes).await?;
            let decoded = ExportLogsServiceResponse::decode(body.as_slice())
                .map_err(|_| OtlpTransportError::InvalidResponse)?;
            return Ok(
                if decoded
                    .partial_success
                    .is_some_and(|partial| partial.rejected_log_records != 0)
                {
                    OtlpTransportOutcome::Terminal
                } else {
                    OtlpTransportOutcome::Accepted
                },
            );
        }
        if matches!(status.as_u16(), 429 | 502 | 503 | 504) {
            return Ok(OtlpTransportOutcome::Retryable {
                retry_after: parse_retry_after(response.headers()),
            });
        }
        Ok(OtlpTransportOutcome::Terminal)
    }
}

#[async_trait]
impl OtlpTransport for OtlpHttpTransport {
    async fn send(&self, payload: Vec<u8>) -> Result<OtlpTransportOutcome, OtlpTransportError> {
        self.send_inner(payload).await
    }
}

impl OtlpHttpTransport {
    /// Sends one already-bounded payload without requiring trait import at call sites.
    ///
    /// # Errors
    ///
    /// Returns the same sanitized failures documented by [`OtlpTransport::send`].
    pub async fn send(&self, payload: Vec<u8>) -> Result<OtlpTransportOutcome, OtlpTransportError> {
        self.send_inner(payload).await
    }
}

async fn bounded_body(
    response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, OtlpTransportError> {
    if response
        .content_length()
        .is_some_and(|length| length > u64::try_from(maximum).unwrap_or(u64::MAX))
    {
        return Err(OtlpTransportError::LimitExceeded);
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| OtlpTransportError::Unavailable)?;
        if body.len().saturating_add(chunk.len()) > maximum {
            return Err(OtlpTransportError::LimitExceeded);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds = headers
        .get("retry-after")?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_secs(seconds.min(120)))
}
