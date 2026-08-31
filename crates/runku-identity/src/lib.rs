//! Application identity contracts independent from an account provider or `SaaS` control plane.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod gateway;
mod guest;
mod jwt;
mod key;
mod model;
mod repository;

pub use error::IdentityError;
pub use gateway::{
    AuthBoundary, AuthGateway, AuthGatewayTelemetrySnapshot, AuthInput, AuthenticatedPrincipal,
    EffectiveIdentityHash, PrincipalContext, PrincipalEvidence, PrincipalId, PrincipalKind,
    RequestIdentity,
};
pub use guest::{
    GuestKeyId, GuestKeyMode, GuestKeyring, GuestSigningKey, GuestToken, GuestTokenPolicy,
    GuestTokenTelemetrySnapshot,
};
pub use jwt::{
    JwtAlgorithm, JwtPrincipalProfile, JwtProviderConfig, JwtVerifierSnapshot,
    JwtVerifierTelemetrySnapshot,
};
pub use key::{
    ApplicationKey, CredentialDigest, GeneratedCredentialKey, KeyringCrypto, ParsedApplicationKey,
};
pub use model::{
    ApplicationAssurance, ApplicationClient, ApplicationClientName, ApplicationClientStatus,
    ApplicationContext, ApplicationCredential, ApplicationScope, ClientKind, CredentialKind,
    CredentialLabel, CredentialLifecycleResult, CredentialStatus,
};
pub use repository::{
    ApplicationCredentialResolver, ApplicationIdentityRepository, IdentityRepositoryBackend,
    IdentityTelemetrySnapshot,
};
pub use runku_core::{ApplicationClientId, CredentialId, EnvironmentScope};
