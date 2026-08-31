//! Hardened OIDC discovery and JWKS refresh around Runku's offline JWT verifier.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod cache;
mod error;
mod model;
mod transport;

pub use cache::{JwtProviderManager, ProviderTelemetrySnapshot};
pub use error::ProviderError;
pub use model::{
    AllowedHttpsOrigin, AllowedLoopbackOrigin, LocalProviderNetworkConfig, ProviderHttpRequest,
    ProviderHttpResponse, ProviderNetworkConfig,
};
pub use transport::{HardenedProviderTransport, LoopbackProviderTransport, ProviderHttpTransport};
