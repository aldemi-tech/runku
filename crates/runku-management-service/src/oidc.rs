//! Adapter from the hardened Runku OIDC/JWKS verifier to Platform Identity.

use std::sync::Arc;

use async_trait::async_trait;
use runku_identity::{KeyringCrypto, PrincipalEvidence, PrincipalKind};
use runku_identity_provider::JwtProviderManager;
use runku_platform_identity::{ExternalOperatorIdentity, PlatformIdentityError};
use runku_value::TimestampMicros;

use crate::ExternalIdentityAuthenticator;

/// Configured external `OIDC` verifier producing token-free operator identity evidence.
pub struct JwtExternalIdentityAuthenticator {
    manager: Arc<JwtProviderManager>,
    subject_crypto: Arc<KeyringCrypto>,
}

impl std::fmt::Debug for JwtExternalIdentityAuthenticator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JwtExternalIdentityAuthenticator")
            .field("manager", &self.manager)
            .field("subject_crypto", &"[REDACTED]")
            .finish()
    }
}

impl JwtExternalIdentityAuthenticator {
    /// Composes one provider manager and a dedicated opaque-subject derivation secret.
    #[must_use]
    pub fn new(manager: Arc<JwtProviderManager>, subject_crypto: Arc<KeyringCrypto>) -> Self {
        Self {
            manager,
            subject_crypto,
        }
    }
}

#[async_trait]
impl ExternalIdentityAuthenticator for JwtExternalIdentityAuthenticator {
    async fn authenticate(
        &self,
        bearer: &str,
        now: TimestampMicros,
    ) -> Result<ExternalOperatorIdentity, PlatformIdentityError> {
        let evidence = self
            .manager
            .verify(bearer, &self.subject_crypto, now)
            .await
            .map_err(|_| PlatformIdentityError::Unauthenticated)?;
        let PrincipalEvidence::Valid(principal) = evidence else {
            return Err(PlatformIdentityError::Unauthenticated);
        };
        if principal.kind() != PrincipalKind::User {
            return Err(PlatformIdentityError::Unauthenticated);
        }
        let identity = ExternalOperatorIdentity {
            provider_id: principal.provider_id().to_owned(),
            subject_id: principal.id().to_string(),
        };
        identity.validate()?;
        Ok(identity)
    }
}
