//! HTTPS-only, DNS-pinned provider metadata transport.

use std::{
    collections::{BTreeSet, HashSet},
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use reqwest::header::{ACCEPT, CACHE_CONTROL, ETAG, IF_NONE_MATCH};
use tokio::net::lookup_host;
use url::Url;

use crate::{
    AllowedHttpsOrigin, AllowedLoopbackOrigin, ProviderError, ProviderHttpRequest,
    ProviderHttpResponse, model::validate_etag,
};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_HEADER_VALUES: usize = 64;

/// Injectable bounded fetch boundary used by the refresh coordinator.
#[async_trait]
pub trait ProviderHttpTransport: fmt::Debug + Send + Sync {
    /// Fetches one already-policy-validated discovery/JWKS representation.
    async fn get(
        &self,
        request: ProviderHttpRequest,
    ) -> Result<ProviderHttpResponse, ProviderError>;
}

/// Production HTTPS transport with public-address validation and DNS pinning.
#[derive(Clone, Debug)]
pub struct HardenedProviderTransport {
    origins: Arc<BTreeSet<AllowedHttpsOrigin>>,
}

impl HardenedProviderTransport {
    /// Creates a transport bound to a non-empty set of exact HTTPS origins.
    ///
    /// # Errors
    ///
    /// Rejects an empty or oversized origin set.
    pub fn new(origins: BTreeSet<AllowedHttpsOrigin>) -> Result<Self, ProviderError> {
        if origins.is_empty() || origins.len() > 8 {
            return Err(ProviderError::InvalidConfig);
        }
        Ok(Self {
            origins: Arc::new(origins),
        })
    }

    fn validate_url(&self, value: &str) -> Result<Url, ProviderError> {
        let url = Url::parse(value).map_err(|_| ProviderError::UrlDenied)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !self.origins.iter().any(|origin| origin.allows(&url))
        {
            return Err(ProviderError::UrlDenied);
        }
        Ok(url)
    }

    async fn get_inner(
        &self,
        request: ProviderHttpRequest,
    ) -> Result<ProviderHttpResponse, ProviderError> {
        if request.max_body_bytes == 0 || request.max_body_bytes > 64 * 1024 {
            return Err(ProviderError::InvalidConfig);
        }
        if let Some(etag) = &request.if_none_match {
            validate_etag(etag).map_err(|_| ProviderError::InvalidConfig)?;
        }
        let url = self.validate_url(&request.url)?;
        let host = url.host_str().ok_or(ProviderError::UrlDenied)?;
        let port = url
            .port_or_known_default()
            .ok_or(ProviderError::UrlDenied)?;
        let addresses = resolve_and_validate(host, port).await?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .tls_backend_rustls()
            .connect_timeout(request.timeout.min(Duration::from_secs(10)))
            .timeout(request.timeout)
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|_| ProviderError::TransportUnavailable)?;
        let mut builder = client
            .get(url)
            .header(ACCEPT, "application/json, application/jwk-set+json");
        if let Some(etag) = request.if_none_match {
            builder = builder.header(IF_NONE_MATCH, etag);
        }
        let mut response = builder
            .send()
            .await
            .map_err(|_| ProviderError::TransportUnavailable)?;
        let status = response.status().as_u16();
        if !matches!(status, 200 | 304) {
            return Err(ProviderError::InvalidResponse);
        }
        validate_headers(response.headers())?;
        let etag = response
            .headers()
            .get_all(ETAG)
            .iter()
            .map(|value| value.to_str().map_err(|_| ProviderError::InvalidResponse))
            .collect::<Result<Vec<_>, _>>()?;
        if etag.len() > 1 {
            return Err(ProviderError::InvalidResponse);
        }
        let etag = etag.first().map(|value| (*value).to_owned());
        if let Some(value) = &etag {
            validate_etag(value)?;
        }
        let max_age = parse_max_age(response.headers())?;
        if status == 304 {
            if response.content_length().is_some_and(|length| length != 0) {
                return Err(ProviderError::InvalidResponse);
            }
            return Ok(ProviderHttpResponse {
                status,
                etag,
                max_age,
                body: Vec::new(),
            });
        }
        if response.content_length().is_some_and(|length| {
            length > u64::try_from(request.max_body_bytes).unwrap_or(u64::MAX)
        }) {
            return Err(ProviderError::LimitExceeded);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ProviderError::TransportUnavailable)?
        {
            let next = body
                .len()
                .checked_add(chunk.len())
                .ok_or(ProviderError::LimitExceeded)?;
            if next > request.max_body_bytes {
                return Err(ProviderError::LimitExceeded);
            }
            body.extend_from_slice(&chunk);
        }
        if body.is_empty() {
            return Err(ProviderError::InvalidResponse);
        }
        Ok(ProviderHttpResponse {
            status,
            etag,
            max_age,
            body,
        })
    }
}

#[async_trait]
impl ProviderHttpTransport for HardenedProviderTransport {
    async fn get(
        &self,
        request: ProviderHttpRequest,
    ) -> Result<ProviderHttpResponse, ProviderError> {
        let timeout = request.timeout;
        tokio::time::timeout(timeout, self.get_inner(request))
            .await
            .map_err(|_| ProviderError::Timeout)?
    }
}

/// HTTP transport restricted to one exact literal-loopback origin for local development.
#[derive(Clone, Debug)]
pub struct LoopbackProviderTransport {
    origin: AllowedLoopbackOrigin,
}

impl LoopbackProviderTransport {
    /// Creates a transport bound to one already-validated local origin.
    #[must_use]
    pub const fn new(origin: AllowedLoopbackOrigin) -> Self {
        Self { origin }
    }

    fn validate_url(&self, value: &str) -> Result<Url, ProviderError> {
        let url = Url::parse(value).map_err(|_| ProviderError::UrlDenied)?;
        if !self.origin.allows(&url)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(ProviderError::UrlDenied);
        }
        Ok(url)
    }

    async fn get_inner(
        &self,
        request: ProviderHttpRequest,
    ) -> Result<ProviderHttpResponse, ProviderError> {
        if request.max_body_bytes == 0 || request.max_body_bytes > 64 * 1024 {
            return Err(ProviderError::InvalidConfig);
        }
        if let Some(etag) = &request.if_none_match {
            validate_etag(etag).map_err(|_| ProviderError::InvalidConfig)?;
        }
        let url = self.validate_url(&request.url)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(request.timeout.min(Duration::from_secs(10)))
            .timeout(request.timeout)
            .build()
            .map_err(|_| ProviderError::TransportUnavailable)?;
        let mut builder = client
            .get(url)
            .header(ACCEPT, "application/json, application/jwk-set+json");
        if let Some(etag) = request.if_none_match {
            builder = builder.header(IF_NONE_MATCH, etag);
        }
        let mut response = builder
            .send()
            .await
            .map_err(|_| ProviderError::TransportUnavailable)?;
        let status = response.status().as_u16();
        if !matches!(status, 200 | 304) {
            return Err(ProviderError::InvalidResponse);
        }
        validate_headers(response.headers())?;
        let etag = response
            .headers()
            .get_all(ETAG)
            .iter()
            .map(|value| value.to_str().map_err(|_| ProviderError::InvalidResponse))
            .collect::<Result<Vec<_>, _>>()?;
        if etag.len() > 1 {
            return Err(ProviderError::InvalidResponse);
        }
        let etag = etag.first().map(|value| (*value).to_owned());
        if let Some(value) = &etag {
            validate_etag(value)?;
        }
        let max_age = parse_max_age(response.headers())?;
        if status == 304 {
            if response.content_length().is_some_and(|length| length != 0) {
                return Err(ProviderError::InvalidResponse);
            }
            return Ok(ProviderHttpResponse {
                status,
                etag,
                max_age,
                body: Vec::new(),
            });
        }
        if response.content_length().is_some_and(|length| {
            length > u64::try_from(request.max_body_bytes).unwrap_or(u64::MAX)
        }) {
            return Err(ProviderError::LimitExceeded);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ProviderError::TransportUnavailable)?
        {
            let next = body
                .len()
                .checked_add(chunk.len())
                .ok_or(ProviderError::LimitExceeded)?;
            if next > request.max_body_bytes {
                return Err(ProviderError::LimitExceeded);
            }
            body.extend_from_slice(&chunk);
        }
        if body.is_empty() {
            return Err(ProviderError::InvalidResponse);
        }
        Ok(ProviderHttpResponse {
            status,
            etag,
            max_age,
            body,
        })
    }
}

#[async_trait]
impl ProviderHttpTransport for LoopbackProviderTransport {
    async fn get(
        &self,
        request: ProviderHttpRequest,
    ) -> Result<ProviderHttpResponse, ProviderError> {
        let timeout = request.timeout;
        tokio::time::timeout(timeout, self.get_inner(request))
            .await
            .map_err(|_| ProviderError::Timeout)?
    }
}

fn validate_headers(headers: &reqwest::header::HeaderMap) -> Result<(), ProviderError> {
    let mut bytes = 0_usize;
    let mut values = 0_usize;
    for (name, value) in headers {
        values = values.checked_add(1).ok_or(ProviderError::LimitExceeded)?;
        bytes = bytes
            .checked_add(name.as_str().len())
            .and_then(|total| total.checked_add(value.as_bytes().len()))
            .ok_or(ProviderError::LimitExceeded)?;
        if values > MAX_HEADER_VALUES || bytes > MAX_HEADER_BYTES {
            return Err(ProviderError::LimitExceeded);
        }
    }
    Ok(())
}

fn parse_max_age(headers: &reqwest::header::HeaderMap) -> Result<Option<Duration>, ProviderError> {
    let mut found = None;
    for value in headers.get_all(CACHE_CONTROL) {
        let text = value.to_str().map_err(|_| ProviderError::InvalidResponse)?;
        for directive in text.split(',').map(str::trim) {
            if matches!(directive, "no-store" | "no-cache") {
                found = Some(Duration::ZERO);
                continue;
            }
            let Some(raw) = directive.strip_prefix("max-age=") else {
                continue;
            };
            if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(ProviderError::InvalidResponse);
            }
            let seconds = raw
                .parse::<u64>()
                .map_err(|_| ProviderError::InvalidResponse)?;
            let candidate = Duration::from_secs(seconds);
            found = Some(found.map_or(candidate, |current: Duration| current.min(candidate)));
        }
    }
    Ok(found)
}

async fn resolve_and_validate(host: &str, port: u16) -> Result<Vec<SocketAddr>, ProviderError> {
    let ips = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![ip]
    } else {
        lookup_host((host, 0))
            .await
            .map_err(|_| ProviderError::DnsUnavailable)?
            .map(|address| address.ip())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    validate_addresses(ips, port)
}

fn validate_addresses(ips: Vec<IpAddr>, port: u16) -> Result<Vec<SocketAddr>, ProviderError> {
    if ips.is_empty() {
        return Err(ProviderError::DnsUnavailable);
    }
    let unique = ips.into_iter().collect::<HashSet<_>>();
    if unique.iter().any(|ip| !public_ip(*ip)) {
        return Err(ProviderError::AddressDenied);
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
    use std::error::Error;

    use reqwest::header::{HeaderMap, HeaderValue};

    use super::*;

    #[test]
    fn private_special_and_mixed_addresses_fail_closed() -> Result<(), Box<dyn Error>> {
        let public: IpAddr = "8.8.8.8".parse()?;
        let private: IpAddr = "10.0.0.1".parse()?;
        assert_eq!(
            validate_addresses(Vec::new(), 443),
            Err(ProviderError::DnsUnavailable)
        );
        assert_eq!(
            validate_addresses(vec![public, private], 443),
            Err(ProviderError::AddressDenied)
        );
        assert_eq!(
            validate_addresses(vec!["::1".parse()?], 443),
            Err(ProviderError::AddressDenied)
        );
        assert!(validate_addresses(vec![public, public], 443).is_ok());
        Ok(())
    }

    #[test]
    fn cache_and_header_metadata_are_bounded_and_unambiguous() -> Result<(), Box<dyn Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=60"),
        );
        assert_eq!(parse_max_age(&headers)?, Some(Duration::from_mins(1)));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        assert_eq!(parse_max_age(&headers)?, Some(Duration::ZERO));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=invalid"));
        assert_eq!(parse_max_age(&headers), Err(ProviderError::InvalidResponse));

        let mut oversized = HeaderMap::new();
        oversized.insert(
            "x-large",
            HeaderValue::from_str(&"x".repeat(MAX_HEADER_BYTES))?,
        );
        assert_eq!(
            validate_headers(&oversized),
            Err(ProviderError::LimitExceeded)
        );
        Ok(())
    }

    #[test]
    fn loopback_transport_is_bound_to_one_literal_http_origin() -> Result<(), Box<dyn Error>> {
        let transport = LoopbackProviderTransport::new("http://127.0.0.1:3000".parse()?);
        assert!(
            transport
                .validate_url("http://127.0.0.1:3000/api/jwks")
                .is_ok()
        );
        for denied in [
            "http://localhost:3000/api/jwks",
            "http://127.0.0.1:3001/api/jwks",
            "https://127.0.0.1:3000/api/jwks",
            "http://user@127.0.0.1:3000/api/jwks",
            "http://127.0.0.1:3000/api/jwks#fragment",
        ] {
            assert_eq!(
                transport.validate_url(denied),
                Err(ProviderError::UrlDenied)
            );
        }
        Ok(())
    }
}
