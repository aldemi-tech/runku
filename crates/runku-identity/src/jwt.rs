//! Offline JWT verification against a validated, immutable JWKS snapshot.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{AlgorithmParameters, JwkSet, KeyOperations, PublicKeyUse},
};
use runku_core::ApplicationClientId;
use runku_value::TimestampMicros;
use serde::Deserialize;
use serde_json::Value;
use url::Url;

use crate::{
    ApplicationScope, AuthenticatedPrincipal, IdentityError, KeyringCrypto, PrincipalEvidence,
    PrincipalKind,
};

const MAX_JWKS_BYTES: usize = 64 * 1024;
const MAX_JWKS_KEYS: usize = 16;
const MAX_TOKEN_BYTES: usize = 16 * 1024;
const MAX_KID_BYTES: usize = 128;
const MAX_AUDIENCES: usize = 8;
const MAX_MAPPINGS: usize = 64;
const MAX_EXTERNAL_SCOPES: usize = 64;
const MAX_EXTERNAL_SCOPE_BYTES: usize = 128;
const MAX_CLAIM_NAME_BYTES: usize = 64;
const MAX_CLAIM_VALUE_BYTES: usize = 256;
const MIN_TOKEN_TTL: Duration = Duration::from_mins(1);
const MAX_TOKEN_TTL: Duration = Duration::from_hours(168);
const MAX_CLOCK_SKEW: Duration = Duration::from_mins(5);

/// Explicit asymmetric algorithms supported by protocol v1.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum JwtAlgorithm {
    /// RSASSA-PKCS1-v1_5 with SHA-256.
    Rs256,
    /// RSASSA-PSS with SHA-256.
    Ps256,
    /// ECDSA P-256 with SHA-256.
    Es256,
    /// Ed25519/EdDSA.
    EdDsa,
}

impl JwtAlgorithm {
    const fn library(self) -> Algorithm {
        match self {
            Self::Rs256 => Algorithm::RS256,
            Self::Ps256 => Algorithm::PS256,
            Self::Es256 => Algorithm::ES256,
            Self::EdDsa => Algorithm::EdDSA,
        }
    }

    fn from_library(value: Algorithm) -> Result<Self, IdentityError> {
        match value {
            Algorithm::RS256 => Ok(Self::Rs256),
            Algorithm::PS256 => Ok(Self::Ps256),
            Algorithm::ES256 => Ok(Self::Es256),
            Algorithm::EdDSA => Ok(Self::EdDsa),
            _ => Err(IdentityError::InvalidPrincipal),
        }
    }
}

/// Mutually exclusive functional principal profile assigned to one provider configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JwtPrincipalProfile {
    /// Interactive/end-user evidence.
    User,
    /// OAuth client-credentials or another machine identity.
    Service,
}

/// Bounded, revisioned claim mapping for one trusted JWT issuer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JwtProviderConfig {
    /// Stable local provider name used in normalized identity.
    pub provider_id: String,
    /// Exact HTTPS issuer value expected in `iss`.
    pub issuer: String,
    /// One or more accepted exact `aud` values.
    pub audiences: BTreeSet<String>,
    /// Functional identity class produced by this mapping.
    pub profile: JwtPrincipalProfile,
    /// Exact protected-header `typ`; `None` requires the standards-permitted absence of `typ`.
    pub required_type: Option<String>,
    /// Claim name that separates user and service token profiles.
    pub discriminator_claim: String,
    /// Exact discriminator value required by this profile.
    pub discriminator_value: String,
    /// Explicit asymmetric algorithm allowlist.
    pub algorithms: BTreeSet<JwtAlgorithm>,
    /// Scopes granted to every successfully verified principal.
    pub base_scopes: BTreeSet<ApplicationScope>,
    /// Optional external claim containing space-delimited or array scopes.
    pub scope_claim: Option<String>,
    /// Allowlist from external scope strings to Runku scopes.
    pub scope_mapping: BTreeMap<String, ApplicationScope>,
    /// Optional external claim identifying an Application Client.
    pub application_claim: Option<String>,
    /// Exact allowlist mapping external client values to local Application Clients.
    pub application_mapping: BTreeMap<String, ApplicationClientId>,
    /// Maximum accepted `exp - iat` interval.
    pub max_token_ttl: Duration,
    /// Maximum tolerated future `iat`/`nbf` clock skew.
    pub future_clock_skew: Duration,
    /// Monotonic mapping revision included in effective identity hashes.
    pub mapping_revision: u64,
}

impl JwtProviderConfig {
    /// Validates every bound and cross-field invariant before a snapshot can be published.
    ///
    /// # Errors
    ///
    /// Returns `IDENTITY_INPUT_INVALID` or `IDENTITY_LIMIT_EXCEEDED` for unsafe configuration.
    pub fn validate(&self) -> Result<(), IdentityError> {
        validate_provider_id(&self.provider_id)?;
        validate_issuer(&self.issuer)?;
        if self.audiences.is_empty()
            || self.algorithms.is_empty()
            || self.base_scopes.is_empty()
            || self.base_scopes.len() > 64
            || self.mapping_revision == 0
            || self.max_token_ttl < MIN_TOKEN_TTL
            || self.max_token_ttl > MAX_TOKEN_TTL
            || self.future_clock_skew > MAX_CLOCK_SKEW
        {
            return Err(IdentityError::InvalidInput);
        }
        if self.audiences.len() > MAX_AUDIENCES
            || self.scope_mapping.len() > MAX_MAPPINGS
            || self.application_mapping.len() > MAX_MAPPINGS
        {
            return Err(IdentityError::LimitExceeded);
        }
        if let Some(required_type) = &self.required_type {
            validate_header_type(required_type)?;
        }
        validate_claim_name(&self.discriminator_claim)?;
        validate_claim_value(&self.discriminator_value)?;
        if matches!(
            self.discriminator_claim.as_str(),
            "iss" | "sub" | "aud" | "exp" | "iat" | "nbf"
        ) {
            return Err(IdentityError::InvalidInput);
        }
        for audience in &self.audiences {
            validate_claim_value(audience)?;
        }
        if let Some(name) = &self.scope_claim {
            validate_claim_name(name)?;
            if name == &self.discriminator_claim {
                return Err(IdentityError::InvalidInput);
            }
        } else if !self.scope_mapping.is_empty() {
            return Err(IdentityError::InvalidInput);
        }
        if let Some(name) = &self.application_claim {
            validate_claim_name(name)?;
            if self.application_mapping.is_empty()
                || name == &self.discriminator_claim
                || self.scope_claim.as_ref() == Some(name)
            {
                return Err(IdentityError::InvalidInput);
            }
        } else if !self.application_mapping.is_empty() {
            return Err(IdentityError::InvalidInput);
        }
        for external in self
            .scope_mapping
            .keys()
            .chain(self.application_mapping.keys())
        {
            validate_claim_value(external)?;
        }
        let mut maximum_scopes = self.base_scopes.clone();
        maximum_scopes.extend(self.scope_mapping.values().cloned());
        if maximum_scopes.len() > 64 {
            return Err(IdentityError::LimitExceeded);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct VerificationKey {
    algorithm: JwtAlgorithm,
    key: DecodingKey,
}

/// Bounded process-local JWT verification counters; values contain no token or claim data.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JwtVerifierTelemetrySnapshot {
    /// Tokens verified successfully.
    pub verified: u64,
    /// Invalid token/header/signature/claim failures.
    pub invalid: u64,
    /// Unknown `kid` signals requesting one bounded refresh.
    pub key_misses: u64,
    /// Requests rejected because the snapshot was expired.
    pub expired_snapshots: u64,
}

#[derive(Debug, Default)]
struct JwtVerifierCounters {
    verified: AtomicU64,
    invalid: AtomicU64,
    key_misses: AtomicU64,
    expired_snapshots: AtomicU64,
}

/// Immutable, prevalidated JWKS view safe to share across concurrent request verification.
pub struct JwtVerifierSnapshot {
    config: JwtProviderConfig,
    keys: BTreeMap<String, VerificationKey>,
    valid_until: TimestampMicros,
    counters: JwtVerifierCounters,
}

impl fmt::Debug for JwtVerifierSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtVerifierSnapshot")
            .field("provider_id", &self.config.provider_id)
            .field("key_count", &self.keys.len())
            .field("valid_until", &self.valid_until)
            .field("keys", &"[REDACTED]")
            .field("counters", &self.telemetry())
            .finish()
    }
}

impl JwtVerifierSnapshot {
    /// Parses and validates a bounded JWKS document into a reusable immutable snapshot.
    ///
    /// # Errors
    ///
    /// Rejects malformed, private/symmetric, ambiguously purposed, unsupported, duplicate, or
    /// algorithm-incompatible keys and invalid provider configuration.
    pub fn from_jwks_json(
        config: JwtProviderConfig,
        jwks_json: &[u8],
        valid_until: TimestampMicros,
    ) -> Result<Self, IdentityError> {
        config.validate()?;
        if jwks_json.is_empty() || jwks_json.len() > MAX_JWKS_BYTES || valid_until.get() <= 0 {
            return Err(IdentityError::InvalidInput);
        }
        reject_private_or_symmetric_material(jwks_json)?;
        let jwks: JwkSet =
            serde_json::from_slice(jwks_json).map_err(|_| IdentityError::InvalidInput)?;
        if jwks.keys.is_empty() || jwks.keys.len() > MAX_JWKS_KEYS {
            return Err(IdentityError::LimitExceeded);
        }
        let mut keys = BTreeMap::new();
        for jwk in jwks.keys {
            let kid = jwk
                .common
                .key_id
                .as_deref()
                .ok_or(IdentityError::InvalidInput)?;
            validate_kid(kid)?;
            validate_jwk_usage(
                jwk.common.public_key_use.as_ref(),
                jwk.common.key_operations.as_deref(),
            )?;
            let key_algorithm = jwk
                .common
                .key_algorithm
                .ok_or(IdentityError::InvalidInput)?;
            let library_algorithm =
                Algorithm::try_from(key_algorithm).map_err(|_| IdentityError::InvalidInput)?;
            let algorithm = JwtAlgorithm::from_library(library_algorithm)
                .map_err(|_| IdentityError::InvalidInput)?;
            if !config.algorithms.contains(&algorithm)
                || !algorithm_parameters_match(&jwk.algorithm, algorithm)
            {
                return Err(IdentityError::InvalidInput);
            }
            let decoding_key =
                DecodingKey::from_jwk(&jwk).map_err(|_| IdentityError::InvalidInput)?;
            if decoding_key.family() != library_algorithm.family()
                || keys
                    .insert(
                        kid.to_owned(),
                        VerificationKey {
                            algorithm,
                            key: decoding_key,
                        },
                    )
                    .is_some()
            {
                return Err(IdentityError::InvalidInput);
            }
        }
        Ok(Self {
            config,
            keys,
            valid_until,
            counters: JwtVerifierCounters::default(),
        })
    }

    /// Verifies one JWT with an injected clock and returns normalized, token-free evidence.
    ///
    /// # Errors
    ///
    /// Returns a refresh signal for an unknown `kid`, an expiry signal for a stale snapshot, and
    /// `PRINCIPAL_INVALID` for every attacker-controlled verification failure.
    pub fn verify(
        &self,
        token: &str,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<PrincipalEvidence, IdentityError> {
        match self.verify_inner(token, crypto, now) {
            Ok(evidence) => {
                self.counters.verified.fetch_add(1, Ordering::Relaxed);
                Ok(evidence)
            }
            Err(error) => {
                match error {
                    IdentityError::JwksRefreshRequired => {
                        self.counters.key_misses.fetch_add(1, Ordering::Relaxed);
                    }
                    IdentityError::JwksSnapshotExpired => {
                        self.counters
                            .expired_snapshots
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        self.counters.invalid.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(error)
            }
        }
    }

    /// Returns a race-tolerant operational counter snapshot.
    #[must_use]
    pub fn telemetry(&self) -> JwtVerifierTelemetrySnapshot {
        JwtVerifierTelemetrySnapshot {
            verified: self.counters.verified.load(Ordering::Relaxed),
            invalid: self.counters.invalid.load(Ordering::Relaxed),
            key_misses: self.counters.key_misses.load(Ordering::Relaxed),
            expired_snapshots: self.counters.expired_snapshots.load(Ordering::Relaxed),
        }
    }

    /// Returns the immutable validated provider mapping used by this snapshot.
    #[must_use]
    pub const fn provider_config(&self) -> &JwtProviderConfig {
        &self.config
    }

    /// Absolute instant after which requests must obtain a fresher snapshot.
    #[must_use]
    pub const fn valid_until(&self) -> TimestampMicros {
        self.valid_until
    }

    fn verify_inner(
        &self,
        token: &str,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<PrincipalEvidence, IdentityError> {
        if now.get() < 0 || now >= self.valid_until {
            return Err(IdentityError::JwksSnapshotExpired);
        }
        if token.is_empty() || token.len() > MAX_TOKEN_BYTES || token.trim() != token {
            return Err(IdentityError::InvalidPrincipal);
        }
        let header = decode_header(token).map_err(|_| IdentityError::InvalidPrincipal)?;
        validate_header(&header, &self.config)?;
        let kid = header
            .kid
            .as_deref()
            .ok_or(IdentityError::InvalidPrincipal)?;
        validate_kid(kid).map_err(|_| IdentityError::InvalidPrincipal)?;
        let verification_key = self
            .keys
            .get(kid)
            .ok_or(IdentityError::JwksRefreshRequired)?;
        let algorithm = JwtAlgorithm::from_library(header.alg)?;
        if verification_key.algorithm != algorithm || !self.config.algorithms.contains(&algorithm) {
            return Err(IdentityError::InvalidPrincipal);
        }

        // The library verifies the signature and exact algorithm. Time/issuer/audience validation is
        // deliberately manual below so every decision uses the caller-injected clock.
        let mut validation = Validation::new(algorithm.library());
        validation.required_spec_claims.clear();
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.validate_aud = false;
        validation.leeway = 0;
        let claims = decode::<JwtClaims>(token, &verification_key.key, &validation)
            .map_err(|_| IdentityError::InvalidPrincipal)?
            .claims;
        self.normalize_claims(&claims, crypto, now)
            .map(PrincipalEvidence::Valid)
    }

    fn normalize_claims(
        &self,
        claims: &JwtClaims,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<AuthenticatedPrincipal, IdentityError> {
        if claims.iss != self.config.issuer {
            return Err(IdentityError::InvalidPrincipal);
        }
        validate_subject(&claims.sub)?;
        let audiences = claims.aud.values()?;
        if audiences.is_empty()
            || audiences.len() > MAX_AUDIENCES
            || audiences
                .iter()
                .any(|value| validate_claim_value(value).is_err())
            || !audiences
                .iter()
                .any(|value| self.config.audiences.contains(*value))
        {
            return Err(IdentityError::InvalidPrincipal);
        }
        validate_times(
            claims,
            now,
            self.config.max_token_ttl,
            self.config.future_clock_skew,
        )?;
        let discriminator = claims
            .extra
            .get(&self.config.discriminator_claim)
            .and_then(Value::as_str)
            .ok_or(IdentityError::InvalidPrincipal)?;
        if discriminator != self.config.discriminator_value {
            return Err(IdentityError::InvalidPrincipal);
        }

        let mut scopes = self.config.base_scopes.clone();
        if let Some(claim_name) = &self.config.scope_claim
            && let Some(value) = claims.extra.get(claim_name)
        {
            for external in parse_external_scopes(value)? {
                if let Some(mapped) = self.config.scope_mapping.get(external) {
                    scopes.insert(mapped.clone());
                }
            }
        }
        let bound_application = self
            .config
            .application_claim
            .as_ref()
            .map(|claim_name| {
                let external = claims
                    .extra
                    .get(claim_name)
                    .and_then(Value::as_str)
                    .ok_or(IdentityError::InvalidPrincipal)?;
                validate_claim_value(external).map_err(|_| IdentityError::InvalidPrincipal)?;
                self.config
                    .application_mapping
                    .get(external)
                    .copied()
                    .ok_or(IdentityError::InvalidPrincipal)
            })
            .transpose()?;

        let id = crypto.derive_principal_id(
            &self.config.provider_id,
            &self.config.issuer,
            &claims.sub,
        )?;
        let kind = match self.config.profile {
            JwtPrincipalProfile::User => PrincipalKind::User,
            JwtPrincipalProfile::Service => PrincipalKind::Service,
        };
        let issued_at = seconds_to_micros(claims.iat)?;
        AuthenticatedPrincipal::new(
            id,
            kind,
            &self.config.provider_id,
            scopes,
            bound_application,
            // A bounded future `iat` is accepted for issuer clock skew but must not create a
            // principal that the downstream gateway considers future-authenticated.
            Some(issued_at.min(now)),
            Some(seconds_to_micros(claims.exp)?),
            self.config.mapping_revision,
        )
    }
}

#[derive(Deserialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    aud: JwtAudience,
    exp: i64,
    iat: i64,
    #[serde(default)]
    nbf: Option<i64>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JwtAudience {
    One(String),
    Many(Vec<String>),
}

impl JwtAudience {
    fn values(&self) -> Result<Vec<&str>, IdentityError> {
        match self {
            Self::One(value) => Ok(vec![value]),
            Self::Many(values) => {
                let unique: BTreeSet<&str> = values.iter().map(String::as_str).collect();
                if unique.len() != values.len() {
                    return Err(IdentityError::InvalidPrincipal);
                }
                Ok(values.iter().map(String::as_str).collect())
            }
        }
    }
}

fn validate_provider_id(value: &str) -> Result<(), IdentityError> {
    if value.is_empty()
        || value.len() > 80
        || value.starts_with(['.', '-'])
        || value.ends_with(['.', '-'])
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
    {
        return Err(IdentityError::InvalidInput);
    }
    Ok(())
}

fn validate_issuer(value: &str) -> Result<(), IdentityError> {
    if value.is_empty()
        || value.len() > 512
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(IdentityError::InvalidInput);
    }
    let url = Url::parse(value).map_err(|_| IdentityError::InvalidInput)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(IdentityError::InvalidInput);
    }
    Ok(())
}

fn validate_header_type(value: &str) -> Result<(), IdentityError> {
    if value.is_empty()
        || value.len() > 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-'))
    {
        return Err(IdentityError::InvalidInput);
    }
    Ok(())
}

fn validate_claim_name(value: &str) -> Result<(), IdentityError> {
    if value.is_empty()
        || value.len() > MAX_CLAIM_NAME_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b':' | b'/')
        })
    {
        return Err(IdentityError::InvalidInput);
    }
    Ok(())
}

fn validate_claim_value(value: &str) -> Result<(), IdentityError> {
    if value.is_empty()
        || value.len() > MAX_CLAIM_VALUE_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(IdentityError::InvalidInput);
    }
    Ok(())
}

fn validate_subject(value: &str) -> Result<(), IdentityError> {
    if value.is_empty()
        || value.len() > 512
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(IdentityError::InvalidPrincipal);
    }
    Ok(())
}

fn validate_kid(value: &str) -> Result<(), IdentityError> {
    if value.is_empty()
        || value.len() > MAX_KID_BYTES
        || value.trim() != value
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(IdentityError::InvalidInput);
    }
    Ok(())
}

fn validate_jwk_usage(
    public_use: Option<&PublicKeyUse>,
    operations: Option<&[KeyOperations]>,
) -> Result<(), IdentityError> {
    match (public_use, operations) {
        (Some(PublicKeyUse::Signature), None) | (None, Some([KeyOperations::Verify])) => Ok(()),
        _ => Err(IdentityError::InvalidInput),
    }
}

fn algorithm_parameters_match(parameters: &AlgorithmParameters, algorithm: JwtAlgorithm) -> bool {
    matches!(
        (parameters, algorithm),
        (
            AlgorithmParameters::RSA(_),
            JwtAlgorithm::Rs256 | JwtAlgorithm::Ps256
        ) | (AlgorithmParameters::EllipticCurve(_), JwtAlgorithm::Es256)
            | (AlgorithmParameters::OctetKeyPair(_), JwtAlgorithm::EdDsa)
    )
}

fn reject_private_or_symmetric_material(jwks_json: &[u8]) -> Result<(), IdentityError> {
    let root: Value = serde_json::from_slice(jwks_json).map_err(|_| IdentityError::InvalidInput)?;
    let keys = root
        .as_object()
        .filter(|object| object.len() == 1)
        .and_then(|object| object.get("keys"))
        .and_then(Value::as_array)
        .ok_or(IdentityError::InvalidInput)?;
    for key in keys {
        let object = key.as_object().ok_or(IdentityError::InvalidInput)?;
        if object.contains_key("k")
            || ["d", "p", "q", "dp", "dq", "qi", "oth"]
                .iter()
                .any(|name| object.contains_key(*name))
        {
            return Err(IdentityError::InvalidInput);
        }
    }
    Ok(())
}

fn validate_header(
    header: &jsonwebtoken::Header,
    config: &JwtProviderConfig,
) -> Result<(), IdentityError> {
    if header.typ.as_deref() != config.required_type.as_deref()
        || header.cty.is_some()
        || header.jku.is_some()
        || header.jwk.is_some()
        || header.x5u.is_some()
        || header.x5c.is_some()
        || header.x5t.is_some()
        || header.x5t_s256.is_some()
        || header.crit.is_some()
        || header.enc.is_some()
        || header.zip.is_some()
        || header.url.is_some()
        || header.nonce.is_some()
        || !header.extras.inner().is_empty()
    {
        return Err(IdentityError::InvalidPrincipal);
    }
    Ok(())
}

fn validate_times(
    claims: &JwtClaims,
    now: TimestampMicros,
    max_ttl: Duration,
    skew: Duration,
) -> Result<(), IdentityError> {
    let now_seconds = now
        .get()
        .checked_div(1_000_000)
        .ok_or(IdentityError::InvalidPrincipal)?;
    let skew_seconds =
        i64::try_from(skew.as_secs()).map_err(|_| IdentityError::InvalidPrincipal)?;
    let max_ttl_seconds =
        i64::try_from(max_ttl.as_secs()).map_err(|_| IdentityError::InvalidPrincipal)?;
    let latest_future = now_seconds
        .checked_add(skew_seconds)
        .ok_or(IdentityError::InvalidPrincipal)?;
    if claims.iat < 0
        || claims.exp <= now_seconds
        || claims.iat > latest_future
        || claims.exp <= claims.iat
        || claims
            .exp
            .checked_sub(claims.iat)
            .is_none_or(|ttl| ttl > max_ttl_seconds)
        || claims
            .nbf
            .is_some_and(|nbf| nbf < 0 || nbf > latest_future || nbf >= claims.exp)
    {
        return Err(IdentityError::InvalidPrincipal);
    }
    Ok(())
}

fn seconds_to_micros(value: i64) -> Result<TimestampMicros, IdentityError> {
    value
        .checked_mul(1_000_000)
        .map(TimestampMicros::new)
        .ok_or(IdentityError::InvalidPrincipal)
}

fn parse_external_scopes(value: &Value) -> Result<Vec<&str>, IdentityError> {
    let scopes = if let Some(text) = value.as_str() {
        if text.is_empty() || text.trim() != text || text.contains("  ") {
            return Err(IdentityError::InvalidPrincipal);
        }
        text.split(' ').collect::<Vec<_>>()
    } else if let Some(values) = value.as_array() {
        values
            .iter()
            .map(|item| item.as_str().ok_or(IdentityError::InvalidPrincipal))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        return Err(IdentityError::InvalidPrincipal);
    };
    let unique: BTreeSet<&str> = scopes.iter().copied().collect();
    if scopes.len() > MAX_EXTERNAL_SCOPES
        || unique.len() != scopes.len()
        || scopes.iter().any(|scope| {
            scope.is_empty()
                || scope.len() > MAX_EXTERNAL_SCOPE_BYTES
                || scope.chars().any(char::is_control)
        })
    {
        return Err(IdentityError::InvalidPrincipal);
    }
    Ok(scopes)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, error::Error};

    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, jwk::PublicKeyUse};
    use rand::thread_rng;
    use rsa::{RsaPrivateKey, pkcs1::EncodeRsaPrivateKey};
    use serde_json::{Value, json};

    use super::*;

    const NOW_SECONDS: i64 = 1_700_000_000;
    const NOW_MICROS: i64 = NOW_SECONDS * 1_000_000;
    const APP_ID: &str = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV";

    struct SigningFixture {
        encoding: EncodingKey,
        jwks: Vec<u8>,
    }

    fn signing_fixture() -> Result<SigningFixture, Box<dyn Error>> {
        let private = RsaPrivateKey::new(&mut thread_rng(), 2_048)?;
        let der = private.to_pkcs1_der()?;
        let encoding = EncodingKey::from_rsa_der(der.as_bytes());
        let mut jwk = jsonwebtoken::jwk::Jwk::from_encoding_key(&encoding, Algorithm::RS256)?;
        jwk.common.key_id = Some("primary-2026-08".to_owned());
        jwk.common.public_key_use = Some(PublicKeyUse::Signature);
        let jwks = serde_json::to_vec(&JwkSet { keys: vec![jwk] })?;
        Ok(SigningFixture { encoding, jwks })
    }

    fn scope(value: &str) -> Result<ApplicationScope, IdentityError> {
        value.parse()
    }

    fn config(profile: JwtPrincipalProfile) -> Result<JwtProviderConfig, Box<dyn Error>> {
        let mut audiences = BTreeSet::new();
        audiences.insert("runku-api".to_owned());
        let mut algorithms = BTreeSet::new();
        algorithms.insert(JwtAlgorithm::Rs256);
        let mut base_scopes = BTreeSet::new();
        base_scopes.insert(scope("functions:execute")?);
        let mut scope_mapping = BTreeMap::new();
        scope_mapping.insert("documents.read".to_owned(), scope("documents:read")?);
        let mut application_mapping = BTreeMap::new();
        application_mapping.insert("web-main".to_owned(), APP_ID.parse()?);
        let discriminator_value = match profile {
            JwtPrincipalProfile::User => "user",
            JwtPrincipalProfile::Service => "service",
        };
        Ok(JwtProviderConfig {
            provider_id: "acme.identity".to_owned(),
            issuer: "https://identity.example.test/tenant".to_owned(),
            audiences,
            profile,
            required_type: Some("at+jwt".to_owned()),
            discriminator_claim: "token_class".to_owned(),
            discriminator_value: discriminator_value.to_owned(),
            algorithms,
            base_scopes,
            scope_claim: Some("permissions".to_owned()),
            scope_mapping,
            application_claim: Some("client_ref".to_owned()),
            application_mapping,
            max_token_ttl: Duration::from_hours(1),
            future_clock_skew: Duration::from_secs(30),
            mapping_revision: 7,
        })
    }

    fn claims(class: &str) -> Value {
        json!({
            "iss": "https://identity.example.test/tenant",
            "sub": "subject-that-must-never-be-retained",
            "aud": ["other-api", "runku-api"],
            "iat": NOW_SECONDS - 10,
            "nbf": NOW_SECONDS - 10,
            "exp": NOW_SECONDS + 300,
            "token_class": class,
            "permissions": "documents.read ignored.external",
            "client_ref": "web-main",
            "unknown_admin": true
        })
    }

    fn sign(
        encoding: &EncodingKey,
        kid: &str,
        token_type: &str,
        claims: &Value,
    ) -> Result<String, jsonwebtoken::errors::Error> {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_owned());
        header.typ = Some(token_type.to_owned());
        encode(&header, claims, encoding)
    }

    fn snapshot(
        fixture: &SigningFixture,
        profile: JwtPrincipalProfile,
    ) -> Result<JwtVerifierSnapshot, Box<dyn Error>> {
        Ok(JwtVerifierSnapshot::from_jwks_json(
            config(profile)?,
            &fixture.jwks,
            TimestampMicros::new(NOW_MICROS + 3_600_000_000),
        )?)
    }

    #[test]
    fn verifies_real_user_signature_and_maps_only_allowlisted_context() -> Result<(), Box<dyn Error>>
    {
        let fixture = signing_fixture()?;
        let snapshot = snapshot(&fixture, JwtPrincipalProfile::User)?;
        let token = sign(
            &fixture.encoding,
            "primary-2026-08",
            "at+jwt",
            &claims("user"),
        )?;
        let crypto = KeyringCrypto::new([9; 32]);
        let evidence = snapshot.verify(&token, &crypto, TimestampMicros::new(NOW_MICROS))?;
        let PrincipalEvidence::Valid(principal) = evidence else {
            return Err("expected valid principal".into());
        };
        assert_eq!(principal.kind(), PrincipalKind::User);
        assert_eq!(principal.provider_id(), "acme.identity");
        assert_eq!(principal.bound_application(), Some(APP_ID.parse()?));
        assert_eq!(principal.mapping_revision(), 7);
        assert!(principal.scopes().contains(&scope("functions:execute")?));
        assert!(principal.scopes().contains(&scope("documents:read")?));
        assert_eq!(principal.scopes().len(), 2);
        assert!(!format!("{snapshot:?}").contains("subject-that-must-never-be-retained"));
        assert_eq!(snapshot.telemetry().verified, 1);
        Ok(())
    }

    #[test]
    fn optional_type_requires_the_header_to_be_absent_exactly() -> Result<(), Box<dyn Error>> {
        let fixture = signing_fixture()?;
        let mut provider = config(JwtPrincipalProfile::User)?;
        provider.required_type = None;
        let snapshot = JwtVerifierSnapshot::from_jwks_json(
            provider,
            &fixture.jwks,
            TimestampMicros::new(NOW_MICROS + 3_600_000_000),
        )?;
        let crypto = KeyringCrypto::new([8; 32]);

        let mut absent_type = Header::new(Algorithm::RS256);
        absent_type.kid = Some("primary-2026-08".to_owned());
        absent_type.typ = None;
        let token = encode(&absent_type, &claims("user"), &fixture.encoding)?;
        assert!(matches!(
            snapshot.verify(&token, &crypto, TimestampMicros::new(NOW_MICROS))?,
            PrincipalEvidence::Valid(_)
        ));

        let unexpected_type = sign(&fixture.encoding, "primary-2026-08", "JWT", &claims("user"))?;
        assert_eq!(
            snapshot.verify(&unexpected_type, &crypto, TimestampMicros::new(NOW_MICROS)),
            Err(IdentityError::InvalidPrincipal)
        );
        Ok(())
    }

    #[test]
    fn user_and_service_profiles_are_mutually_exclusive() -> Result<(), Box<dyn Error>> {
        let fixture = signing_fixture()?;
        let user = snapshot(&fixture, JwtPrincipalProfile::User)?;
        let service = snapshot(&fixture, JwtPrincipalProfile::Service)?;
        let service_token = sign(
            &fixture.encoding,
            "primary-2026-08",
            "at+jwt",
            &claims("service"),
        )?;
        let crypto = KeyringCrypto::new([3; 32]);
        assert_eq!(
            user.verify(&service_token, &crypto, TimestampMicros::new(NOW_MICROS)),
            Err(IdentityError::InvalidPrincipal)
        );
        let PrincipalEvidence::Valid(principal) =
            service.verify(&service_token, &crypto, TimestampMicros::new(NOW_MICROS))?
        else {
            return Err("expected valid service principal".into());
        };
        assert_eq!(principal.kind(), PrincipalKind::Service);
        Ok(())
    }

    #[test]
    fn rejects_tamper_wrong_header_issuer_audience_and_time() -> Result<(), Box<dyn Error>> {
        let fixture = signing_fixture()?;
        let snapshot = snapshot(&fixture, JwtPrincipalProfile::User)?;
        let crypto = KeyringCrypto::new([4; 32]);
        let valid = claims("user");
        let token = sign(&fixture.encoding, "primary-2026-08", "at+jwt", &valid)?;
        let mut tampered = token.clone();
        let replacement = if tampered.ends_with('A') { "B" } else { "A" };
        tampered.replace_range(tampered.len() - 1.., replacement);
        assert_eq!(
            snapshot.verify(&tampered, &crypto, TimestampMicros::new(NOW_MICROS)),
            Err(IdentityError::InvalidPrincipal)
        );
        let wrong_type = sign(&fixture.encoding, "primary-2026-08", "JWT", &valid)?;
        assert_eq!(
            snapshot.verify(&wrong_type, &crypto, TimestampMicros::new(NOW_MICROS)),
            Err(IdentityError::InvalidPrincipal)
        );
        let mut missing_kid_header = Header::new(Algorithm::RS256);
        missing_kid_header.typ = Some("at+jwt".to_owned());
        let missing_kid = encode(&missing_kid_header, &valid, &fixture.encoding)?;
        assert_eq!(
            snapshot.verify(&missing_kid, &crypto, TimestampMicros::new(NOW_MICROS)),
            Err(IdentityError::InvalidPrincipal)
        );
        let mut hmac_header = Header::new(Algorithm::HS256);
        hmac_header.kid = Some("primary-2026-08".to_owned());
        hmac_header.typ = Some("at+jwt".to_owned());
        let hmac = encode(&hmac_header, &valid, &EncodingKey::from_secret(&[7; 32]))?;
        assert_eq!(
            snapshot.verify(&hmac, &crypto, TimestampMicros::new(NOW_MICROS)),
            Err(IdentityError::InvalidPrincipal)
        );
        assert_eq!(
            snapshot.verify(
                &"x".repeat(MAX_TOKEN_BYTES + 1),
                &crypto,
                TimestampMicros::new(NOW_MICROS)
            ),
            Err(IdentityError::InvalidPrincipal)
        );
        for (field, value) in [
            ("iss", json!("https://attacker.example")),
            ("aud", json!(["other-api"])),
            ("exp", json!(NOW_SECONDS)),
            ("iat", json!(NOW_SECONDS + 31)),
            ("nbf", json!(NOW_SECONDS + 31)),
        ] {
            let mut changed = valid.clone();
            changed[field] = value;
            let invalid = sign(&fixture.encoding, "primary-2026-08", "at+jwt", &changed)?;
            assert_eq!(
                snapshot.verify(&invalid, &crypto, TimestampMicros::new(NOW_MICROS)),
                Err(IdentityError::InvalidPrincipal),
                "field {field} should fail"
            );
        }
        Ok(())
    }

    #[test]
    fn distinguishes_refresh_and_snapshot_expiry_without_fallback() -> Result<(), Box<dyn Error>> {
        let fixture = signing_fixture()?;
        let snapshot = snapshot(&fixture, JwtPrincipalProfile::User)?;
        let crypto = KeyringCrypto::new([5; 32]);
        let unknown = sign(&fixture.encoding, "rotated-key", "at+jwt", &claims("user"))?;
        assert_eq!(
            snapshot.verify(&unknown, &crypto, TimestampMicros::new(NOW_MICROS)),
            Err(IdentityError::JwksRefreshRequired)
        );
        let valid = sign(
            &fixture.encoding,
            "primary-2026-08",
            "at+jwt",
            &claims("user"),
        )?;
        assert_eq!(
            snapshot.verify(
                &valid,
                &crypto,
                TimestampMicros::new(NOW_MICROS + 3_600_000_000)
            ),
            Err(IdentityError::JwksSnapshotExpired)
        );
        assert_eq!(snapshot.telemetry().key_misses, 1);
        assert_eq!(snapshot.telemetry().expired_snapshots, 1);
        Ok(())
    }

    #[test]
    fn rejects_unsafe_jwks_and_provider_configuration() -> Result<(), Box<dyn Error>> {
        let fixture = signing_fixture()?;
        let valid_config = config(JwtPrincipalProfile::User)?;
        assert_eq!(
            JwtVerifierSnapshot::from_jwks_json(
                valid_config.clone(),
                b"{",
                TimestampMicros::new(NOW_MICROS)
            )
            .err(),
            Some(IdentityError::InvalidInput)
        );
        assert_eq!(
            JwtVerifierSnapshot::from_jwks_json(
                valid_config.clone(),
                &vec![b' '; MAX_JWKS_BYTES + 1],
                TimestampMicros::new(NOW_MICROS)
            )
            .err(),
            Some(IdentityError::InvalidInput)
        );
        let root: Value = serde_json::from_slice(&fixture.jwks)?;
        let key = root["keys"][0].clone();
        let duplicate = serde_json::to_vec(&json!({"keys": [key.clone(), key.clone()]}))?;
        assert_eq!(
            JwtVerifierSnapshot::from_jwks_json(
                valid_config.clone(),
                &duplicate,
                TimestampMicros::new(NOW_MICROS)
            )
            .err(),
            Some(IdentityError::InvalidInput)
        );
        let mut private = key.clone();
        private["d"] = json!("private-material");
        let private_jwks = serde_json::to_vec(&json!({"keys": [private]}))?;
        assert_eq!(
            JwtVerifierSnapshot::from_jwks_json(
                valid_config.clone(),
                &private_jwks,
                TimestampMicros::new(NOW_MICROS)
            )
            .err(),
            Some(IdentityError::InvalidInput)
        );
        let symmetric = serde_json::to_vec(&json!({"keys": [{
            "kty": "oct", "k": "c2VjcmV0", "kid": "bad", "alg": "HS256", "use": "sig"
        }]}))?;
        assert_eq!(
            JwtVerifierSnapshot::from_jwks_json(
                valid_config.clone(),
                &symmetric,
                TimestampMicros::new(NOW_MICROS)
            )
            .err(),
            Some(IdentityError::InvalidInput)
        );
        let mut invalid_config = valid_config;
        invalid_config.issuer = "http://identity.example.test".to_owned();
        assert_eq!(invalid_config.validate(), Err(IdentityError::InvalidInput));
        Ok(())
    }
}
