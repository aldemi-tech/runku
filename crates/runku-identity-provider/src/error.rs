//! Stable provider-network errors without remote or secret detail.

use runku_identity::IdentityError;
use thiserror::Error;

/// Failure while loading or using OIDC/JWKS provider metadata.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderError {
    /// Local provider/network policy is contradictory or unsafe.
    #[error("identity provider configuration is invalid")]
    InvalidConfig,
    /// Discovery/JWKS URL falls outside the configured HTTPS boundary.
    #[error("identity provider URL is denied")]
    UrlDenied,
    /// DNS failed or returned no addresses.
    #[error("identity provider DNS is unavailable")]
    DnsUnavailable,
    /// DNS/IP target is private, special, mixed, or otherwise denied.
    #[error("identity provider address is denied")]
    AddressDenied,
    /// HTTPS connect, TLS, protocol, or body streaming failed.
    #[error("identity provider transport is unavailable")]
    TransportUnavailable,
    /// The complete bounded fetch deadline elapsed.
    #[error("identity provider request timed out")]
    Timeout,
    /// HTTP status, headers, cache metadata, JSON, issuer, or JWKS metadata is invalid.
    #[error("identity provider response is invalid")]
    InvalidResponse,
    /// A response or configured collection exceeds its hard limit.
    #[error("identity provider limit exceeded")]
    LimitExceeded,
    /// Offline identity verification rejected the evidence or snapshot.
    #[error("identity provider evidence is invalid")]
    Identity(IdentityError),
    /// An internal synchronization primitive was poisoned.
    #[error("identity provider cache is unavailable")]
    Unavailable,
}

impl ProviderError {
    /// Stable machine-readable operational code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfig => "PROVIDER_CONFIG_INVALID",
            Self::UrlDenied => "PROVIDER_URL_DENIED",
            Self::DnsUnavailable => "PROVIDER_DNS_UNAVAILABLE",
            Self::AddressDenied => "PROVIDER_ADDRESS_DENIED",
            Self::TransportUnavailable => "PROVIDER_TRANSPORT_UNAVAILABLE",
            Self::Timeout => "PROVIDER_TIMEOUT",
            Self::InvalidResponse => "PROVIDER_RESPONSE_INVALID",
            Self::LimitExceeded => "PROVIDER_LIMIT_EXCEEDED",
            Self::Identity(error) => error.code(),
            Self::Unavailable => "PROVIDER_CACHE_UNAVAILABLE",
        }
    }

    /// Whether retrying later without changing the caller token may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::DnsUnavailable
                | Self::TransportUnavailable
                | Self::Timeout
                | Self::Unavailable
                | Self::Identity(
                    IdentityError::JwksRefreshRequired | IdentityError::JwksSnapshotExpired
                )
        )
    }
}

impl From<IdentityError> for ProviderError {
    fn from(value: IdentityError) -> Self {
        Self::Identity(value)
    }
}
