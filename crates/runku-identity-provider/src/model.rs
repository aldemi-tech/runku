//! Validated network policy and bounded transport values.

use std::{collections::BTreeSet, fmt, net::IpAddr, str::FromStr, time::Duration};

use runku_identity::JwtProviderConfig;
use url::Url;

use crate::ProviderError;

const MAX_ORIGINS: usize = 8;
const MAX_ETAG_BYTES: usize = 256;
const MIN_CACHE_TTL: Duration = Duration::from_secs(5);
const MAX_CACHE_TTL: Duration = Duration::from_hours(24);
const MIN_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_COOLDOWN: Duration = Duration::from_secs(1);
const MAX_COOLDOWN: Duration = Duration::from_mins(1);

/// Exact normalized HTTPS origin allowed for discovery or JWKS fetches.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AllowedHttpsOrigin {
    host: String,
    port: u16,
}

impl AllowedHttpsOrigin {
    /// Normalized ASCII hostname or IP literal.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Explicit/effective TLS port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn allows(&self, url: &Url) -> bool {
        url.host_str() == Some(self.host.as_str()) && url.port_or_known_default() == Some(self.port)
    }
}

impl fmt::Display for AllowedHttpsOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "https://[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "https://{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for AllowedHttpsOrigin {
    type Err = ProviderError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let url = parse_https_url(value)?;
        if url.path() != "/" || url.query().is_some() {
            return Err(ProviderError::InvalidConfig);
        }
        Ok(Self {
            host: url
                .host_str()
                .ok_or(ProviderError::InvalidConfig)?
                .to_owned(),
            port: url
                .port_or_known_default()
                .ok_or(ProviderError::InvalidConfig)?,
        })
    }
}

/// Exact literal-loopback HTTP origin allowed only by local Product Base composition.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AllowedLoopbackOrigin {
    ip: IpAddr,
    port: u16,
}

impl AllowedLoopbackOrigin {
    /// Literal loopback IP; DNS names such as `localhost` are intentionally not accepted.
    #[must_use]
    pub const fn ip(&self) -> IpAddr {
        self.ip
    }

    /// Explicit HTTP port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn allows(&self, url: &Url) -> bool {
        url.scheme() == "http"
            && url.host_str().and_then(|host| host.parse::<IpAddr>().ok()) == Some(self.ip)
            && url.port_or_known_default() == Some(self.port)
    }
}

impl fmt::Display for AllowedLoopbackOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.ip {
            IpAddr::V4(ip) => write!(formatter, "http://{ip}:{}", self.port),
            IpAddr::V6(ip) => write!(formatter, "http://[{ip}]:{}", self.port),
        }
    }
}

impl FromStr for AllowedLoopbackOrigin {
    type Err = ProviderError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let url = parse_local_url(value)?;
        if url.path() != "/" || url.query().is_some() {
            return Err(ProviderError::InvalidConfig);
        }
        let ip = url
            .host_str()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .filter(IpAddr::is_loopback)
            .ok_or(ProviderError::InvalidConfig)?;
        Ok(Self {
            ip,
            port: url
                .port_or_known_default()
                .ok_or(ProviderError::InvalidConfig)?,
        })
    }
}

/// Complete bounded network/cache policy for one offline JWT provider mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderNetworkConfig {
    /// Offline cryptographic/claim mapping.
    pub provider: JwtProviderConfig,
    /// Explicit OIDC discovery document URL.
    pub discovery_url: String,
    /// Exact HTTPS origins allowed for discovery and discovered `jwks_uri`.
    pub allowed_origins: BTreeSet<AllowedHttpsOrigin>,
    /// TTL used when a valid response omits `Cache-Control: max-age`.
    pub default_cache_ttl: Duration,
    /// Local upper bound on remote `max-age`.
    pub max_cache_ttl: Duration,
    /// End-to-end deadline for each HTTPS request.
    pub request_timeout: Duration,
    /// Minimum interval between attacker-triggerable unknown-`kid` refreshes.
    pub unknown_kid_cooldown: Duration,
}

impl ProviderNetworkConfig {
    /// Validates local policy independently from remote metadata.
    ///
    /// # Errors
    ///
    /// Rejects invalid provider mapping, URLs, origins, TTLs, timeout, or cooldown.
    pub fn validate(&self) -> Result<(), ProviderError> {
        self.provider.validate().map_err(ProviderError::Identity)?;
        if self.allowed_origins.is_empty()
            || self.allowed_origins.len() > MAX_ORIGINS
            || self.default_cache_ttl < MIN_CACHE_TTL
            || self.default_cache_ttl > self.max_cache_ttl
            || self.max_cache_ttl > MAX_CACHE_TTL
            || self.request_timeout < MIN_TIMEOUT
            || self.request_timeout > MAX_TIMEOUT
            || self.unknown_kid_cooldown < MIN_COOLDOWN
            || self.unknown_kid_cooldown > MAX_COOLDOWN
        {
            return Err(ProviderError::InvalidConfig);
        }
        let discovery = parse_https_url(&self.discovery_url)?;
        if discovery.query().is_some() || !self.url_allowed(&discovery) {
            return Err(ProviderError::InvalidConfig);
        }
        Ok(())
    }

    pub(crate) fn parse_allowed_url(&self, value: &str) -> Result<Url, ProviderError> {
        let url = parse_https_url(value).map_err(|_| ProviderError::UrlDenied)?;
        if !self.url_allowed(&url) {
            return Err(ProviderError::UrlDenied);
        }
        Ok(url)
    }

    fn url_allowed(&self, url: &Url) -> bool {
        self.allowed_origins.iter().any(|origin| origin.allows(url))
    }

    pub(crate) fn effective_ttl(&self, remote: Option<Duration>) -> Duration {
        remote
            .unwrap_or(self.default_cache_ttl)
            .min(self.max_cache_ttl)
    }
}

/// Bounded discovery/JWKS policy for one explicit local loopback identity provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalProviderNetworkConfig {
    /// Offline cryptographic/claim mapping. Its JWT issuer remains an HTTPS identifier.
    pub provider: JwtProviderConfig,
    /// Explicit discovery URL served by a local sidecar/application.
    pub discovery_url: String,
    /// The only literal-loopback HTTP origin allowed for discovery and JWKS.
    pub allowed_origin: AllowedLoopbackOrigin,
    /// TTL used when a valid response omits `Cache-Control: max-age`.
    pub default_cache_ttl: Duration,
    /// Local upper bound on remote `max-age`.
    pub max_cache_ttl: Duration,
    /// End-to-end deadline for each local request.
    pub request_timeout: Duration,
    /// Minimum interval between attacker-triggerable unknown-`kid` refreshes.
    pub unknown_kid_cooldown: Duration,
}

impl LocalProviderNetworkConfig {
    /// Validates local-only policy without weakening [`ProviderNetworkConfig`].
    ///
    /// # Errors
    ///
    /// Rejects invalid claim mapping, non-loopback URLs, origin mismatch, or unsafe bounds.
    pub fn validate(&self) -> Result<(), ProviderError> {
        self.provider.validate().map_err(ProviderError::Identity)?;
        validate_durations(
            self.default_cache_ttl,
            self.max_cache_ttl,
            self.request_timeout,
            self.unknown_kid_cooldown,
        )?;
        let discovery = parse_local_url(&self.discovery_url)?;
        if discovery.query().is_some() || !self.allowed_origin.allows(&discovery) {
            return Err(ProviderError::InvalidConfig);
        }
        Ok(())
    }

    pub(crate) fn parse_allowed_url(&self, value: &str) -> Result<Url, ProviderError> {
        let url = parse_local_url(value).map_err(|_| ProviderError::UrlDenied)?;
        if !self.allowed_origin.allows(&url) {
            return Err(ProviderError::UrlDenied);
        }
        Ok(url)
    }

    pub(crate) fn effective_ttl(&self, remote: Option<Duration>) -> Duration {
        remote
            .unwrap_or(self.default_cache_ttl)
            .min(self.max_cache_ttl)
    }
}

/// Bounded request passed to an injected provider HTTP transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderHttpRequest {
    /// Exact validated HTTPS URL.
    pub url: String,
    /// Optional validator from the prior successful representation.
    pub if_none_match: Option<String>,
    /// Maximum accepted response body bytes.
    pub max_body_bytes: usize,
    /// Complete request deadline duration.
    pub timeout: Duration,
}

/// Sanitized bounded response returned by a provider HTTP transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderHttpResponse {
    /// Only 200 or 304 is accepted from the hardened production transport.
    pub status: u16,
    /// Optional validated `ETag`.
    pub etag: Option<String>,
    /// Optional parsed `Cache-Control: max-age`.
    pub max_age: Option<Duration>,
    /// Exact bounded response bytes; empty for 304.
    pub body: Vec<u8>,
}

pub(crate) fn validate_etag(value: &str) -> Result<(), ProviderError> {
    if value.is_empty()
        || value.len() > MAX_ETAG_BYTES
        || value.trim() != value
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return Err(ProviderError::InvalidResponse);
    }
    Ok(())
}

fn parse_https_url(value: &str) -> Result<Url, ProviderError> {
    if value.is_empty() || value.len() > 2_048 || value.trim() != value {
        return Err(ProviderError::InvalidConfig);
    }
    let url = Url::parse(value).map_err(|_| ProviderError::InvalidConfig)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(ProviderError::InvalidConfig);
    }
    Ok(url)
}

fn parse_local_url(value: &str) -> Result<Url, ProviderError> {
    if value.is_empty() || value.len() > 2_048 || value.trim() != value {
        return Err(ProviderError::InvalidConfig);
    }
    let url = Url::parse(value).map_err(|_| ProviderError::InvalidConfig)?;
    let loopback = url
        .host_str()
        .and_then(|host| host.parse::<IpAddr>().ok())
        .is_some_and(|ip| ip.is_loopback());
    if url.scheme() != "http"
        || !loopback
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(ProviderError::InvalidConfig);
    }
    Ok(url)
}

fn validate_durations(
    default_cache_ttl: Duration,
    max_cache_ttl: Duration,
    request_timeout: Duration,
    unknown_kid_cooldown: Duration,
) -> Result<(), ProviderError> {
    if default_cache_ttl < MIN_CACHE_TTL
        || default_cache_ttl > max_cache_ttl
        || max_cache_ttl > MAX_CACHE_TTL
        || request_timeout < MIN_TIMEOUT
        || request_timeout > MAX_TIMEOUT
        || unknown_kid_cooldown < MIN_COOLDOWN
        || unknown_kid_cooldown > MAX_COOLDOWN
    {
        Err(ProviderError::InvalidConfig)
    } else {
        Ok(())
    }
}
