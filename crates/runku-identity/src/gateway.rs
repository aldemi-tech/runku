//! Canonical request identity composition and Function policy enforcement.

use std::{
    collections::BTreeSet,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use runku_core::{ApplicationClientId, EnvironmentScope};
use runku_releases::{AuthPolicy, FunctionVisibility};
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};

use crate::{
    ApplicationContext, ApplicationCredentialResolver, ApplicationScope, CredentialKind,
    IdentityError, KeyringCrypto, ParsedApplicationKey,
};

const MAX_PROVIDER_ID_BYTES: usize = 80;
const MAX_PRINCIPAL_SCOPES: usize = 64;

/// Opaque stable identifier derived by a trusted verifier from provider/issuer/subject.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PrincipalId([u8; 32]);

impl PrincipalId {
    /// Creates an opaque ID from a trusted keyed derivation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns canonical bytes for cache keys and protocol codecs.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "pri_v1_{}", URL_SAFE_NO_PAD.encode(self.0))
    }
}

impl fmt::Debug for PrincipalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PrincipalId")
            .field(&self.to_string())
            .finish()
    }
}

/// Functional caller class after verification and normalization.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PrincipalKind {
    /// Stable anonymous/guest session with low assurance.
    Guest,
    /// Identified end user from a configured provider.
    User,
    /// Machine/service caller from a secret key or M2M provider.
    Service,
    /// Trusted internal platform execution, never accepted from the external boundary.
    System,
}

/// Bounded, token-free functional principal produced by a trusted verifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedPrincipal {
    id: PrincipalId,
    kind: PrincipalKind,
    provider_id: String,
    scopes: BTreeSet<ApplicationScope>,
    bound_application: Option<ApplicationClientId>,
    auth_time: Option<TimestampMicros>,
    expires_at: Option<TimestampMicros>,
    mapping_revision: u64,
}

impl AuthenticatedPrincipal {
    /// Constructs and validates a normalized principal without storing raw tokens or subjects.
    ///
    /// # Errors
    ///
    /// Rejects unsafe provider IDs, empty/oversized scopes, contradictory timestamps, or revision 0.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: PrincipalId,
        kind: PrincipalKind,
        provider_id: &str,
        scopes: BTreeSet<ApplicationScope>,
        bound_application: Option<ApplicationClientId>,
        auth_time: Option<TimestampMicros>,
        expires_at: Option<TimestampMicros>,
        mapping_revision: u64,
    ) -> Result<Self, IdentityError> {
        if provider_id.is_empty()
            || provider_id.len() > MAX_PROVIDER_ID_BYTES
            || provider_id.starts_with(['.', '-'])
            || provider_id.ends_with(['.', '-'])
            || !provider_id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
            })
            || scopes.is_empty()
            || scopes.len() > MAX_PRINCIPAL_SCOPES
            || mapping_revision == 0
            || auth_time.is_some_and(|time| time.get() < 0)
            || expires_at.is_some_and(|time| time.get() < 0)
            || auth_time
                .zip(expires_at)
                .is_some_and(|(auth, expiry)| auth >= expiry)
        {
            return Err(IdentityError::InvalidPrincipal);
        }
        Ok(Self {
            id,
            kind,
            provider_id: provider_id.to_owned(),
            scopes,
            bound_application,
            auth_time,
            expires_at,
            mapping_revision,
        })
    }

    /// Stable opaque caller ID.
    #[must_use]
    pub const fn id(&self) -> PrincipalId {
        self.id
    }

    /// Normalized caller class.
    #[must_use]
    pub const fn kind(&self) -> PrincipalKind {
        self.kind
    }

    /// Trusted provider configuration ID, not a raw issuer URL.
    #[must_use]
    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    /// Normalized principal scopes.
    #[must_use]
    pub const fn scopes(&self) -> &BTreeSet<ApplicationScope> {
        &self.scopes
    }

    /// Optional Application Client mapping asserted by the verifier.
    #[must_use]
    pub const fn bound_application(&self) -> Option<ApplicationClientId> {
        self.bound_application
    }

    /// Token/session authentication instant if supplied by the verifier.
    #[must_use]
    pub const fn auth_time(&self) -> Option<TimestampMicros> {
        self.auth_time
    }

    /// Absolute evidence expiry, if any.
    #[must_use]
    pub const fn expires_at(&self) -> Option<TimestampMicros> {
        self.expires_at
    }

    /// Claim-mapping/configuration revision used by the verifier.
    #[must_use]
    pub const fn mapping_revision(&self) -> u64 {
        self.mapping_revision
    }

    fn validate_at(&self, now: TimestampMicros) -> Result<(), IdentityError> {
        if now.get() < 0
            || self.auth_time.is_some_and(|time| time > now)
            || self.expires_at.is_some_and(|time| time <= now)
        {
            return Err(IdentityError::InvalidPrincipal);
        }
        Ok(())
    }
}

/// Functional identity exposed to a Function after policy application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrincipalContext {
    /// No functional principal. This is not a shared anonymous user.
    None,
    /// Verified, normalized functional principal.
    Authenticated(AuthenticatedPrincipal),
}

impl PrincipalContext {
    /// Returns the authenticated kind or `None` for an unauthenticated request.
    #[must_use]
    pub const fn kind(&self) -> Option<PrincipalKind> {
        match self {
            Self::None => None,
            Self::Authenticated(principal) => Some(principal.kind),
        }
    }
}

/// Result of parsing/verifying optional functional evidence before policy enforcement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrincipalEvidence {
    /// No bearer/guest evidence was supplied.
    Absent,
    /// A trusted adapter verified and normalized evidence.
    Valid(AuthenticatedPrincipal),
    /// Evidence was supplied but could not be verified; never treated as absent.
    Invalid,
}

/// Whether the invocation entered through the public protocol or trusted infrastructure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthBoundary {
    /// HTTP/WebSocket application boundary.
    External,
    /// Nested call, scheduler, or other pre-authorized infrastructure path.
    TrustedInternal,
}

/// Parsed application key and functional evidence for one authorization decision.
#[derive(Debug)]
pub struct AuthInput {
    /// Trusted boundary classification, never read from request payload.
    pub boundary: AuthBoundary,
    /// Strict parsed key; absence is distinct from invalid parsing.
    pub application_key: Option<ParsedApplicationKey>,
    /// Functional identity evidence state.
    pub principal: PrincipalEvidence,
}

impl AuthInput {
    /// Parses an optional `X-Runku-Key` and preserves explicit principal evidence state.
    ///
    /// # Errors
    ///
    /// A malformed supplied key fails immediately and never becomes absence.
    pub fn parse(
        boundary: AuthBoundary,
        application_key: Option<&str>,
        principal: PrincipalEvidence,
    ) -> Result<Self, IdentityError> {
        Ok(Self {
            boundary,
            application_key: application_key.map(str::parse).transpose()?,
            principal,
        })
    }
}

/// Hash of every identity/configuration dimension that may affect a result.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EffectiveIdentityHash([u8; 32]);

impl EffectiveIdentityHash {
    /// Bytes used in cache/subscription keys.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Canonical identity passed to execution after key resolution and policy enforcement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestIdentity {
    /// Logical application context; independent from functional user/service identity.
    pub application: Option<ApplicationContext>,
    /// Functional principal visible to `ctx.auth`.
    pub principal: PrincipalContext,
    /// Complete cache/subscription isolation hash.
    pub effective_hash: EffectiveIdentityHash,
}

impl RequestIdentity {
    /// Derives the identity visible to one trusted nested Function without re-reading raw
    /// credentials or identity-provider state.
    ///
    /// Application attribution is preserved. Functional principal visibility is recalculated for
    /// the target policy, including expiry validation and `auth:none` redaction. This method does
    /// not grant external access to internal Functions; only a trusted nested-call broker may use
    /// it as part of its independent target validation.
    ///
    /// # Errors
    ///
    /// Rejects expired/invalid principals and policies not satisfied by the existing principal.
    pub fn derive_for_nested(
        &self,
        policy: AuthPolicy,
        now: TimestampMicros,
    ) -> Result<Self, IdentityError> {
        if now.get() < 0 {
            return Err(IdentityError::InvalidPrincipal);
        }
        if let PrincipalContext::Authenticated(principal) = &self.principal {
            principal.validate_at(now)?;
        }
        let principal = apply_policy(
            policy,
            self.principal.clone(),
            AuthBoundary::TrustedInternal,
        )?;
        Ok(Self {
            application: self.application.clone(),
            effective_hash: hash_identity(self.application.as_ref(), &principal),
            principal,
        })
    }
}

/// Bounded process-local authorization counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AuthGatewayTelemetrySnapshot {
    /// Successful authorization decisions.
    pub authorized: u64,
    /// Malformed/invalid/inactive key failures.
    pub application_failures: u64,
    /// Invalid or expired principal evidence failures.
    pub principal_failures: u64,
    /// External attempts to invoke internal Functions.
    pub internal_denials: u64,
    /// Valid principals that did not satisfy Function policy.
    pub policy_denials: u64,
    /// Application/principal binding mismatches.
    pub application_mismatches: u64,
}

#[derive(Debug, Default)]
struct AuthGatewayCounters {
    authorized: AtomicU64,
    application_failures: AtomicU64,
    principal_failures: AtomicU64,
    internal_denials: AtomicU64,
    policy_denials: AtomicU64,
    application_mismatches: AtomicU64,
}

/// Identity composition/policy boundary executed before Runtime admission.
pub struct AuthGateway<'a> {
    resolver: &'a dyn ApplicationCredentialResolver,
    crypto: &'a KeyringCrypto,
    counters: AuthGatewayCounters,
}

impl fmt::Debug for AuthGateway<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthGateway")
            .field("resolver", &"dyn ApplicationCredentialResolver")
            .field("crypto", &self.crypto)
            .field("telemetry", &self.telemetry())
            .finish()
    }
}

impl<'a> AuthGateway<'a> {
    /// Creates a gateway from the hot-path key resolver and operator-managed pepper.
    #[must_use]
    pub fn new(resolver: &'a dyn ApplicationCredentialResolver, crypto: &'a KeyringCrypto) -> Self {
        Self {
            resolver,
            crypto,
            counters: AuthGatewayCounters::default(),
        }
    }

    /// Resolves application context, normalizes service identity, enforces visibility/policy, and hashes the result.
    ///
    /// # Errors
    ///
    /// Fails closed for invalid evidence, key state, mismatches, internal visibility, expiry, or policy denial.
    pub async fn authorize(
        &self,
        scope: EnvironmentScope,
        visibility: FunctionVisibility,
        policy: AuthPolicy,
        input: AuthInput,
        now: TimestampMicros,
    ) -> Result<RequestIdentity, IdentityError> {
        if visibility == FunctionVisibility::Internal && input.boundary == AuthBoundary::External {
            self.counters
                .internal_denials
                .fetch_add(1, Ordering::Relaxed);
            return Err(IdentityError::InternalFunctionDenied);
        }
        let application = match input.application_key.as_ref() {
            Some(key) => match self
                .resolver
                .resolve_key(scope, key, self.crypto, now)
                .await
            {
                Ok(context) => Some(context),
                Err(error) => {
                    self.counters
                        .application_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(error);
                }
            },
            None => None,
        };
        let evidence = match input.principal {
            PrincipalEvidence::Absent => None,
            PrincipalEvidence::Invalid => {
                self.counters
                    .principal_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(IdentityError::InvalidPrincipal);
            }
            PrincipalEvidence::Valid(principal) => {
                if principal.validate_at(now).is_err()
                    || (input.boundary == AuthBoundary::External
                        && principal.kind == PrincipalKind::System)
                {
                    self.counters
                        .principal_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(IdentityError::InvalidPrincipal);
                }
                if let (Some(expected), Some(application)) =
                    (principal.bound_application, application.as_ref())
                    && expected != application.client_id
                {
                    self.counters
                        .application_mismatches
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(IdentityError::ApplicationMismatch);
                }
                Some(principal)
            }
        };
        let principal = match (evidence, application.as_ref()) {
            (Some(principal), _) => PrincipalContext::Authenticated(principal),
            (None, Some(application)) if application.credential_kind == CredentialKind::Secret => {
                PrincipalContext::Authenticated(self.service_principal(application)?)
            }
            (None, _) => PrincipalContext::None,
        };
        let visible_principal =
            apply_policy(policy, principal, input.boundary).inspect_err(|_error| {
                self.counters.policy_denials.fetch_add(1, Ordering::Relaxed);
            })?;
        let effective_hash = hash_identity(application.as_ref(), &visible_principal);
        self.counters.authorized.fetch_add(1, Ordering::Relaxed);
        Ok(RequestIdentity {
            application,
            principal: visible_principal,
            effective_hash,
        })
    }

    fn service_principal(
        &self,
        application: &ApplicationContext,
    ) -> Result<AuthenticatedPrincipal, IdentityError> {
        AuthenticatedPrincipal::new(
            self.crypto.derive_principal_id(
                "runku-keyring",
                "application-client",
                &application.client_id.to_string(),
            )?,
            PrincipalKind::Service,
            "runku-keyring",
            application.scopes.clone(),
            Some(application.client_id),
            None,
            None,
            application.configuration_revision,
        )
    }

    /// Returns bounded counters without per-tenant or credential labels.
    #[must_use]
    pub fn telemetry(&self) -> AuthGatewayTelemetrySnapshot {
        AuthGatewayTelemetrySnapshot {
            authorized: self.counters.authorized.load(Ordering::Relaxed),
            application_failures: self.counters.application_failures.load(Ordering::Relaxed),
            principal_failures: self.counters.principal_failures.load(Ordering::Relaxed),
            internal_denials: self.counters.internal_denials.load(Ordering::Relaxed),
            policy_denials: self.counters.policy_denials.load(Ordering::Relaxed),
            application_mismatches: self.counters.application_mismatches.load(Ordering::Relaxed),
        }
    }
}

fn apply_policy(
    policy: AuthPolicy,
    principal: PrincipalContext,
    boundary: AuthBoundary,
) -> Result<PrincipalContext, IdentityError> {
    let kind = principal.kind();
    match policy {
        AuthPolicy::None => Ok(PrincipalContext::None),
        AuthPolicy::Optional => Ok(principal),
        AuthPolicy::Guest
            if matches!(
                kind,
                Some(PrincipalKind::Guest | PrincipalKind::User | PrincipalKind::Service)
            ) =>
        {
            Ok(principal)
        }
        AuthPolicy::User if kind == Some(PrincipalKind::User) => Ok(principal),
        AuthPolicy::Service if kind == Some(PrincipalKind::Service) => Ok(principal),
        AuthPolicy::Service
            if boundary == AuthBoundary::TrustedInternal && kind == Some(PrincipalKind::System) =>
        {
            Ok(principal)
        }
        AuthPolicy::Guest | AuthPolicy::User | AuthPolicy::Service => {
            Err(IdentityError::PolicyDenied)
        }
    }
}

fn hash_identity(
    application: Option<&ApplicationContext>,
    principal: &PrincipalContext,
) -> EffectiveIdentityHash {
    let mut digest = Sha256::new();
    digest.update(b"runku-request-identity-v1\0");
    match application {
        None => digest.update([0]),
        Some(application) => {
            digest.update([1]);
            field(&mut digest, application.client_id.to_string().as_bytes());
            field(
                &mut digest,
                application.credential_id.to_string().as_bytes(),
            );
            digest.update([match application.credential_kind {
                CredentialKind::Publishable => 1,
                CredentialKind::Secret => 2,
            }]);
            digest.update(application.configuration_revision.to_be_bytes());
            scopes(&mut digest, &application.scopes);
        }
    }
    match principal {
        PrincipalContext::None => digest.update([0]),
        PrincipalContext::Authenticated(principal) => {
            digest.update([match principal.kind {
                PrincipalKind::Guest => 1,
                PrincipalKind::User => 2,
                PrincipalKind::Service => 3,
                PrincipalKind::System => 4,
            }]);
            digest.update(principal.id.0);
            field(&mut digest, principal.provider_id.as_bytes());
            digest.update(principal.mapping_revision.to_be_bytes());
            scopes(&mut digest, &principal.scopes);
        }
    }
    EffectiveIdentityHash(digest.finalize().into())
}

fn scopes(digest: &mut Sha256, scopes: &BTreeSet<ApplicationScope>) {
    digest.update(
        u32::try_from(scopes.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for scope in scopes {
        field(digest, scope.as_str().as_bytes());
    }
}

fn field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, error::Error};

    use async_trait::async_trait;
    use runku_core::{CredentialId, EnvironmentId, ProjectId};

    use super::*;
    use crate::{ApplicationAssurance, CredentialDigest};

    #[derive(Default)]
    struct FakeResolver {
        records: BTreeMap<CredentialId, (ApplicationContext, CredentialDigest, bool)>,
    }

    #[async_trait]
    impl ApplicationCredentialResolver for FakeResolver {
        async fn resolve_key(
            &self,
            _scope: EnvironmentScope,
            key: &ParsedApplicationKey,
            crypto: &KeyringCrypto,
            _now: TimestampMicros,
        ) -> Result<ApplicationContext, IdentityError> {
            let (context, digest, active) = self
                .records
                .get(&key.credential_id())
                .ok_or(IdentityError::InvalidCredential)?;
            if key.kind() != context.credential_kind || !crypto.verify(key.key(), *digest) {
                return Err(IdentityError::InvalidCredential);
            }
            if !active {
                return Err(IdentityError::CredentialInactive);
            }
            Ok(context.clone())
        }
    }

    struct Fixture {
        scope: EnvironmentScope,
        crypto: KeyringCrypto,
        resolver: FakeResolver,
        public_key_one: String,
        public_key_two: String,
        secret_key: String,
        public_client: ApplicationClientId,
    }

    impl Fixture {
        fn new() -> Result<Self, IdentityError> {
            let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
            let crypto = KeyringCrypto::new([55; 32]);
            let public_client = ApplicationClientId::generate();
            let secret_client = ApplicationClientId::generate();
            let scopes = scope_set(&["function:invoke"])?;
            let mut resolver = FakeResolver::default();
            let public_one = crypto.generate_publishable(CredentialId::generate())?;
            let public_one_id: ParsedApplicationKey = public_one.key.expose().parse()?;
            resolver.records.insert(
                public_one_id.credential_id(),
                (
                    application_context(
                        public_client,
                        public_one_id.credential_id(),
                        CredentialKind::Publishable,
                        ApplicationAssurance::Declared,
                        scopes.clone(),
                        10,
                    ),
                    public_one.digest,
                    true,
                ),
            );
            let public_two = crypto.generate_publishable(CredentialId::generate())?;
            let public_two_id: ParsedApplicationKey = public_two.key.expose().parse()?;
            resolver.records.insert(
                public_two_id.credential_id(),
                (
                    application_context(
                        public_client,
                        public_two_id.credential_id(),
                        CredentialKind::Publishable,
                        ApplicationAssurance::Declared,
                        scopes.clone(),
                        11,
                    ),
                    public_two.digest,
                    true,
                ),
            );
            let secret = crypto.generate_secret(CredentialId::generate())?;
            let secret_id: ParsedApplicationKey = secret.key.expose().parse()?;
            resolver.records.insert(
                secret_id.credential_id(),
                (
                    application_context(
                        secret_client,
                        secret_id.credential_id(),
                        CredentialKind::Secret,
                        ApplicationAssurance::Verified,
                        scopes,
                        12,
                    ),
                    secret.digest,
                    true,
                ),
            );
            Ok(Self {
                scope,
                crypto,
                resolver,
                public_key_one: public_one.key.expose().to_owned(),
                public_key_two: public_two.key.expose().to_owned(),
                secret_key: secret.key.expose().to_owned(),
                public_client,
            })
        }

        fn principal(
            &self,
            kind: PrincipalKind,
            bound: Option<ApplicationClientId>,
            expires_at: Option<i64>,
        ) -> Result<AuthenticatedPrincipal, IdentityError> {
            AuthenticatedPrincipal::new(
                self.crypto.derive_principal_id(
                    "test-provider",
                    "https://issuer.example",
                    &format!("subject-{kind:?}"),
                )?,
                kind,
                "test-provider",
                scope_set(&["profile:read"])?,
                bound,
                Some(TimestampMicros::new(10)),
                expires_at.map(TimestampMicros::new),
                7,
            )
        }
    }

    #[tokio::test]
    async fn key_and_principal_axes_compose_without_confusion() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let gateway = AuthGateway::new(&fixture.resolver, &fixture.crypto);
        let now = TimestampMicros::new(50);

        let public_none = gateway
            .authorize(
                fixture.scope,
                FunctionVisibility::Public,
                AuthPolicy::Optional,
                AuthInput::parse(
                    AuthBoundary::External,
                    Some(&fixture.public_key_one),
                    PrincipalEvidence::Absent,
                )?,
                now,
            )
            .await?;
        assert_eq!(public_none.principal, PrincipalContext::None);
        assert_eq!(
            public_none.application.as_ref().map(|app| app.assurance),
            Some(ApplicationAssurance::Declared)
        );

        let secret_service = gateway
            .authorize(
                fixture.scope,
                FunctionVisibility::Public,
                AuthPolicy::Service,
                AuthInput::parse(
                    AuthBoundary::External,
                    Some(&fixture.secret_key),
                    PrincipalEvidence::Absent,
                )?,
                now,
            )
            .await?;
        assert_eq!(
            secret_service.principal.kind(),
            Some(PrincipalKind::Service)
        );
        assert_eq!(
            secret_service.application.as_ref().map(|app| app.assurance),
            Some(ApplicationAssurance::Verified)
        );

        let user =
            fixture.principal(PrincipalKind::User, Some(fixture.public_client), Some(100))?;
        let public_user = gateway
            .authorize(
                fixture.scope,
                FunctionVisibility::Public,
                AuthPolicy::User,
                AuthInput::parse(
                    AuthBoundary::External,
                    Some(&fixture.public_key_one),
                    PrincipalEvidence::Valid(user.clone()),
                )?,
                now,
            )
            .await?;
        assert_eq!(public_user.principal.kind(), Some(PrincipalKind::User));
        assert_eq!(
            public_user.application.as_ref().map(|app| app.client_id),
            Some(fixture.public_client)
        );

        let public_none_with_user = gateway
            .authorize(
                fixture.scope,
                FunctionVisibility::Public,
                AuthPolicy::None,
                AuthInput::parse(
                    AuthBoundary::External,
                    Some(&fixture.public_key_one),
                    PrincipalEvidence::Valid(user),
                )?,
                now,
            )
            .await?;
        assert_eq!(public_none_with_user.principal, PrincipalContext::None);
        assert_eq!(
            public_none_with_user.effective_hash,
            public_none.effective_hash
        );
        assert_ne!(public_user.effective_hash, public_none.effective_hash);

        let other_revision = gateway
            .authorize(
                fixture.scope,
                FunctionVisibility::Public,
                AuthPolicy::Optional,
                AuthInput::parse(
                    AuthBoundary::External,
                    Some(&fixture.public_key_two),
                    PrincipalEvidence::Absent,
                )?,
                now,
            )
            .await?;
        assert_ne!(other_revision.effective_hash, public_none.effective_hash);
        Ok(())
    }

    #[tokio::test]
    async fn nested_derivation_preserves_application_and_reapplies_target_policy()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let gateway = AuthGateway::new(&fixture.resolver, &fixture.crypto);
        let now = TimestampMicros::new(50);
        let user =
            fixture.principal(PrincipalKind::User, Some(fixture.public_client), Some(100))?;
        let parent = gateway
            .authorize(
                fixture.scope,
                FunctionVisibility::Public,
                AuthPolicy::User,
                AuthInput::parse(
                    AuthBoundary::External,
                    Some(&fixture.public_key_one),
                    PrincipalEvidence::Valid(user),
                )?,
                now,
            )
            .await?;

        let optional = parent.derive_for_nested(AuthPolicy::Optional, now)?;
        assert_eq!(optional.principal.kind(), Some(PrincipalKind::User));
        assert_eq!(optional.application, parent.application);
        let redacted = parent.derive_for_nested(AuthPolicy::None, now)?;
        assert_eq!(redacted.principal, PrincipalContext::None);
        assert_ne!(redacted.effective_hash, parent.effective_hash);
        assert_eq!(
            parent.derive_for_nested(AuthPolicy::Service, now),
            Err(IdentityError::PolicyDenied)
        );
        assert_eq!(
            parent.derive_for_nested(AuthPolicy::User, TimestampMicros::new(100)),
            Err(IdentityError::InvalidPrincipal)
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn policy_matrix_visibility_expiry_and_mismatch_fail_closed() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let gateway = AuthGateway::new(&fixture.resolver, &fixture.crypto);
        let now = TimestampMicros::new(50);
        let cases = [
            (AuthPolicy::None, None, true),
            (AuthPolicy::Optional, None, true),
            (AuthPolicy::Guest, None, false),
            (AuthPolicy::Guest, Some(PrincipalKind::Guest), true),
            (AuthPolicy::Guest, Some(PrincipalKind::User), true),
            (AuthPolicy::Guest, Some(PrincipalKind::Service), true),
            (AuthPolicy::User, Some(PrincipalKind::Guest), false),
            (AuthPolicy::User, Some(PrincipalKind::User), true),
            (AuthPolicy::Service, Some(PrincipalKind::User), false),
            (AuthPolicy::Service, Some(PrincipalKind::Service), true),
            (AuthPolicy::Service, Some(PrincipalKind::System), true),
        ];
        for (policy, kind, expected) in cases {
            let evidence = kind.map_or(Ok(PrincipalEvidence::Absent), |kind| {
                fixture
                    .principal(kind, None, Some(100))
                    .map(PrincipalEvidence::Valid)
            })?;
            let result = gateway
                .authorize(
                    fixture.scope,
                    FunctionVisibility::Public,
                    policy,
                    AuthInput::parse(AuthBoundary::TrustedInternal, None, evidence)?,
                    now,
                )
                .await;
            assert_eq!(result.is_ok(), expected, "policy={policy:?} kind={kind:?}");
        }

        let internal = gateway
            .authorize(
                fixture.scope,
                FunctionVisibility::Internal,
                AuthPolicy::None,
                AuthInput::parse(AuthBoundary::External, None, PrincipalEvidence::Absent)?,
                now,
            )
            .await;
        assert_eq!(internal, Err(IdentityError::InternalFunctionDenied));
        let invalid = gateway
            .authorize(
                fixture.scope,
                FunctionVisibility::Public,
                AuthPolicy::None,
                AuthInput::parse(AuthBoundary::External, None, PrincipalEvidence::Invalid)?,
                now,
            )
            .await;
        assert_eq!(invalid, Err(IdentityError::InvalidPrincipal));
        let expired = fixture.principal(PrincipalKind::User, None, Some(50))?;
        assert_eq!(
            gateway
                .authorize(
                    fixture.scope,
                    FunctionVisibility::Public,
                    AuthPolicy::User,
                    AuthInput::parse(
                        AuthBoundary::External,
                        None,
                        PrincipalEvidence::Valid(expired)
                    )?,
                    now,
                )
                .await,
            Err(IdentityError::InvalidPrincipal)
        );
        let mismatch = fixture.principal(
            PrincipalKind::User,
            Some(ApplicationClientId::generate()),
            Some(100),
        )?;
        assert_eq!(
            gateway
                .authorize(
                    fixture.scope,
                    FunctionVisibility::Public,
                    AuthPolicy::User,
                    AuthInput::parse(
                        AuthBoundary::External,
                        Some(&fixture.public_key_one),
                        PrincipalEvidence::Valid(mismatch)
                    )?,
                    now,
                )
                .await,
            Err(IdentityError::ApplicationMismatch)
        );
        assert!(matches!(
            AuthInput::parse(
                AuthBoundary::External,
                Some("rk_sec_v1_invalid"),
                PrincipalEvidence::Absent
            ),
            Err(IdentityError::InvalidCredential)
        ));
        let telemetry = gateway.telemetry();
        assert!(telemetry.authorized >= 7);
        assert!(telemetry.policy_denials >= 3);
        assert_eq!(telemetry.internal_denials, 1);
        assert_eq!(telemetry.principal_failures, 2);
        assert_eq!(telemetry.application_mismatches, 1);
        Ok(())
    }

    #[test]
    fn principal_limits_and_opaque_derivation_are_strict() -> Result<(), Box<dyn Error>> {
        let crypto = KeyringCrypto::new([71; 32]);
        let first = crypto.derive_principal_id("provider", "https://issuer", "subject")?;
        let same = crypto.derive_principal_id("provider", "https://issuer", "subject")?;
        let other = crypto.derive_principal_id("provider", "https://issuer", "other")?;
        assert_eq!(first, same);
        assert_ne!(first, other);
        assert!(first.to_string().starts_with("pri_v1_"));
        assert!(!first.to_string().contains("subject"));

        let valid_scopes = scope_set(&["profile:read"])?;
        for provider in ["", "Uppercase", ".leading", "trailing-"] {
            assert_eq!(
                AuthenticatedPrincipal::new(
                    first,
                    PrincipalKind::User,
                    provider,
                    valid_scopes.clone(),
                    None,
                    None,
                    None,
                    1,
                ),
                Err(IdentityError::InvalidPrincipal)
            );
        }
        let oversized = (0..=MAX_PRINCIPAL_SCOPES)
            .map(|ordinal| format!("scope:{ordinal}").parse())
            .collect::<Result<BTreeSet<ApplicationScope>, _>>()?;
        assert_eq!(
            AuthenticatedPrincipal::new(
                first,
                PrincipalKind::User,
                "provider",
                oversized,
                None,
                None,
                None,
                1,
            ),
            Err(IdentityError::InvalidPrincipal)
        );
        assert_eq!(
            crypto.derive_principal_id("provider", "issuer", &"x".repeat(513)),
            Err(IdentityError::InvalidPrincipal)
        );
        Ok(())
    }

    fn application_context(
        client_id: ApplicationClientId,
        credential_id: CredentialId,
        credential_kind: CredentialKind,
        assurance: ApplicationAssurance,
        scopes: BTreeSet<ApplicationScope>,
        configuration_revision: u64,
    ) -> ApplicationContext {
        ApplicationContext {
            client_id,
            credential_id,
            credential_kind,
            assurance,
            scopes,
            configuration_revision,
        }
    }

    fn scope_set(values: &[&str]) -> Result<BTreeSet<ApplicationScope>, IdentityError> {
        values.iter().map(|value| value.parse()).collect()
    }
}
