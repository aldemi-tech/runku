//! Shared token-free identity fixture for integration tests.

use std::{error::Error, sync::Arc};

use async_trait::async_trait;
use runku_core::EnvironmentScope;
use runku_identity::{
    ApplicationContext, ApplicationCredentialResolver, AuthBoundary, AuthGateway, AuthInput,
    IdentityError, KeyringCrypto, ParsedApplicationKey, PrincipalEvidence, RequestIdentity,
};
use runku_releases::{AuthPolicy, FunctionVisibility};
use runku_value::TimestampMicros;

#[derive(Debug)]
struct NoKeys;

#[async_trait]
impl ApplicationCredentialResolver for NoKeys {
    async fn resolve_key(
        &self,
        _scope: EnvironmentScope,
        _key: &ParsedApplicationKey,
        _crypto: &KeyringCrypto,
        _now: TimestampMicros,
    ) -> Result<ApplicationContext, IdentityError> {
        Err(IdentityError::InvalidCredential)
    }
}

pub async fn anonymous_identity(
    scope: EnvironmentScope,
) -> Result<Arc<RequestIdentity>, Box<dyn Error>> {
    let resolver = NoKeys;
    let crypto = KeyringCrypto::new([7; 32]);
    let identity = AuthGateway::new(&resolver, &crypto)
        .authorize(
            scope,
            FunctionVisibility::Public,
            AuthPolicy::Optional,
            AuthInput::parse(AuthBoundary::External, None, PrincipalEvidence::Absent)?,
            TimestampMicros::new(1),
        )
        .await?;
    Ok(Arc::new(identity))
}
