//! Atomic discovery/JWKS cache and refresh coordination.

use std::{
    fmt,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use runku_identity::{IdentityError, JwtVerifierSnapshot, KeyringCrypto, PrincipalEvidence};
use runku_value::TimestampMicros;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::model::validate_etag;
use crate::{
    HardenedProviderTransport, LocalProviderNetworkConfig, LoopbackProviderTransport,
    ProviderError, ProviderHttpRequest, ProviderHttpResponse, ProviderHttpTransport,
    ProviderNetworkConfig,
};

const MAX_DISCOVERY_BYTES: usize = 32 * 1024;
const MAX_JWKS_BYTES: usize = 64 * 1024;

/// Bounded aggregate provider/cache counters with no remote-controlled labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProviderTelemetrySnapshot {
    /// Successful end-to-end token verifications.
    pub verified: u64,
    /// Verifications rejected without a network refresh.
    pub verification_failures: u64,
    /// Requests served from an existing snapshot without refresh.
    pub cache_hits: u64,
    /// Network refresh attempts after single-flight admission.
    pub refresh_attempts: u64,
    /// Fully validated snapshots atomically published.
    pub refresh_successes: u64,
    /// Refreshes that failed before publication.
    pub refresh_failures: u64,
    /// Valid 304 representations reused after revalidation.
    pub not_modified: u64,
    /// Unknown-key decisions observed from the offline verifier.
    pub key_misses: u64,
    /// Unknown-key refreshes suppressed by cooldown.
    pub cooldown_suppressions: u64,
}

#[derive(Debug, Default)]
struct ProviderCounters {
    verified: AtomicU64,
    verification_failures: AtomicU64,
    cache_hits: AtomicU64,
    refresh_attempts: AtomicU64,
    refresh_successes: AtomicU64,
    refresh_failures: AtomicU64,
    not_modified: AtomicU64,
    key_misses: AtomicU64,
    cooldown_suppressions: AtomicU64,
}

impl ProviderCounters {
    fn snapshot(&self) -> ProviderTelemetrySnapshot {
        ProviderTelemetrySnapshot {
            verified: self.verified.load(Ordering::Relaxed),
            verification_failures: self.verification_failures.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            refresh_attempts: self.refresh_attempts.load(Ordering::Relaxed),
            refresh_successes: self.refresh_successes.load(Ordering::Relaxed),
            refresh_failures: self.refresh_failures.load(Ordering::Relaxed),
            not_modified: self.not_modified.load(Ordering::Relaxed),
            key_misses: self.key_misses.load(Ordering::Relaxed),
            cooldown_suppressions: self.cooldown_suppressions.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
struct CachedDiscovery {
    jwks_uri: String,
    etag: Option<String>,
    valid_until: TimestampMicros,
}

#[derive(Clone)]
struct CachedJwks {
    uri: String,
    bytes: Arc<Vec<u8>>,
    etag: Option<String>,
    snapshot: Arc<JwtVerifierSnapshot>,
}

#[derive(Clone, Default)]
struct CacheState {
    generation: u64,
    discovery: Option<CachedDiscovery>,
    jwks: Option<CachedJwks>,
    last_unknown_refresh: Option<TimestampMicros>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefreshReason {
    Cold,
    Expired,
    UnknownKey,
}

#[derive(Clone)]
enum ManagerConfig {
    Production(ProviderNetworkConfig),
    Local(LocalProviderNetworkConfig),
}

impl ManagerConfig {
    fn provider(&self) -> &runku_identity::JwtProviderConfig {
        match self {
            Self::Production(config) => &config.provider,
            Self::Local(config) => &config.provider,
        }
    }

    fn discovery_url(&self) -> &str {
        match self {
            Self::Production(config) => &config.discovery_url,
            Self::Local(config) => &config.discovery_url,
        }
    }

    const fn request_timeout(&self) -> Duration {
        match self {
            Self::Production(config) => config.request_timeout,
            Self::Local(config) => config.request_timeout,
        }
    }

    const fn unknown_kid_cooldown(&self) -> Duration {
        match self {
            Self::Production(config) => config.unknown_kid_cooldown,
            Self::Local(config) => config.unknown_kid_cooldown,
        }
    }

    fn effective_ttl(&self, remote: Option<Duration>) -> Duration {
        match self {
            Self::Production(config) => config.effective_ttl(remote),
            Self::Local(config) => config.effective_ttl(remote),
        }
    }

    fn parse_allowed_url(&self, value: &str) -> Result<url::Url, ProviderError> {
        match self {
            Self::Production(config) => config.parse_allowed_url(value),
            Self::Local(config) => config.parse_allowed_url(value),
        }
    }
}

/// Concurrency-safe local verifier backed by bounded OIDC discovery and JWKS refresh.
pub struct JwtProviderManager {
    config: ManagerConfig,
    transport: Arc<dyn ProviderHttpTransport>,
    state: RwLock<CacheState>,
    refresh_lock: Mutex<()>,
    counters: ProviderCounters,
}

impl fmt::Debug for JwtProviderManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let generation = self.state.read().map_or(0, |state| state.generation);
        formatter
            .debug_struct("JwtProviderManager")
            .field("provider_id", &self.config.provider().provider_id)
            .field("generation", &generation)
            .field("transport", &"dyn ProviderHttpTransport")
            .field("cache", &"[REDACTED]")
            .field("refresh_lock", &"tokio::sync::Mutex")
            .field("counters", &self.telemetry())
            .finish()
    }
}

impl JwtProviderManager {
    /// Creates a manager with an injected transport, primarily for deterministic composition/tests.
    ///
    /// # Errors
    ///
    /// Rejects unsafe or contradictory local provider/network policy.
    pub fn new(
        config: ProviderNetworkConfig,
        transport: Arc<dyn ProviderHttpTransport>,
    ) -> Result<Self, ProviderError> {
        config.validate()?;
        Ok(Self::new_inner(
            ManagerConfig::Production(config),
            transport,
        ))
    }

    fn new_inner(config: ManagerConfig, transport: Arc<dyn ProviderHttpTransport>) -> Self {
        Self {
            config,
            transport,
            state: RwLock::new(CacheState::default()),
            refresh_lock: Mutex::new(()),
            counters: ProviderCounters::default(),
        }
    }

    /// Creates a manager using the production HTTPS/DNS-pinned transport.
    ///
    /// # Errors
    ///
    /// Rejects invalid policy before any network request.
    pub fn production(config: ProviderNetworkConfig) -> Result<Self, ProviderError> {
        config.validate()?;
        let transport = Arc::new(HardenedProviderTransport::new(
            config.allowed_origins.clone(),
        )?);
        Self::new(config, transport)
    }

    /// Creates a manager restricted to one exact literal-loopback HTTP origin.
    ///
    /// # Errors
    ///
    /// Rejects invalid local policy before any network request.
    pub fn local(config: LocalProviderNetworkConfig) -> Result<Self, ProviderError> {
        config.validate()?;
        let transport = Arc::new(LoopbackProviderTransport::new(
            config.allowed_origin.clone(),
        ));
        Ok(Self::new_inner(ManagerConfig::Local(config), transport))
    }

    /// Verifies a token locally, refreshing metadata only for cold start, expiry, or unknown key.
    ///
    /// Unknown-key handling performs at most one single-flight refresh and one verification retry.
    /// Invalid evidence never becomes absence.
    ///
    /// # Errors
    ///
    /// Returns sanitized provider/network or offline identity verification failures.
    pub async fn verify(
        &self,
        token: &str,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<PrincipalEvidence, ProviderError> {
        let (generation, snapshot) = self.current_snapshot()?;
        let reason = if let Some(snapshot) = snapshot {
            match snapshot.verify(token, crypto, now) {
                Ok(evidence) => {
                    self.counters.cache_hits.fetch_add(1, Ordering::Relaxed);
                    self.counters.verified.fetch_add(1, Ordering::Relaxed);
                    return Ok(evidence);
                }
                Err(IdentityError::JwksRefreshRequired) => {
                    self.counters.key_misses.fetch_add(1, Ordering::Relaxed);
                    RefreshReason::UnknownKey
                }
                Err(IdentityError::JwksSnapshotExpired) => RefreshReason::Expired,
                Err(error) => {
                    self.counters
                        .verification_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(error.into());
                }
            }
        } else {
            RefreshReason::Cold
        };

        self.refresh(reason, generation, now).await?;
        let (_, snapshot) = self.current_snapshot()?;
        let snapshot = snapshot.ok_or(ProviderError::Unavailable)?;
        match snapshot.verify(token, crypto, now) {
            Ok(evidence) => {
                self.counters.verified.fetch_add(1, Ordering::Relaxed);
                Ok(evidence)
            }
            Err(error) => {
                self.counters
                    .verification_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err(error.into())
            }
        }
    }

    /// Returns a race-tolerant aggregate counter snapshot.
    #[must_use]
    pub fn telemetry(&self) -> ProviderTelemetrySnapshot {
        self.counters.snapshot()
    }

    fn current_snapshot(&self) -> Result<(u64, Option<Arc<JwtVerifierSnapshot>>), ProviderError> {
        let state = self.state.read().map_err(|_| ProviderError::Unavailable)?;
        Ok((
            state.generation,
            state
                .jwks
                .as_ref()
                .map(|cached| Arc::clone(&cached.snapshot)),
        ))
    }

    async fn refresh(
        &self,
        reason: RefreshReason,
        observed_generation: u64,
        now: TimestampMicros,
    ) -> Result<(), ProviderError> {
        let _guard = self.refresh_lock.lock().await;
        let mut state = self
            .state
            .read()
            .map_err(|_| ProviderError::Unavailable)?
            .clone();
        if state.generation != observed_generation {
            return Ok(());
        }
        if reason == RefreshReason::UnknownKey {
            if cooldown_active(
                state.last_unknown_refresh,
                now,
                self.config.unknown_kid_cooldown(),
            )? {
                self.counters
                    .cooldown_suppressions
                    .fetch_add(1, Ordering::Relaxed);
                return Err(IdentityError::JwksRefreshRequired.into());
            }
            self.state
                .write()
                .map_err(|_| ProviderError::Unavailable)?
                .last_unknown_refresh = Some(now);
            state.last_unknown_refresh = Some(now);
        }

        self.counters
            .refresh_attempts
            .fetch_add(1, Ordering::Relaxed);
        let result = self.load_next(state, now).await;
        match result {
            Ok(mut next) => {
                next.generation = observed_generation
                    .checked_add(1)
                    .ok_or(ProviderError::Unavailable)?;
                *self.state.write().map_err(|_| ProviderError::Unavailable)? = next;
                self.counters
                    .refresh_successes
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.counters
                    .refresh_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    async fn load_next(
        &self,
        prior: CacheState,
        now: TimestampMicros,
    ) -> Result<CacheState, ProviderError> {
        let discovery = if prior
            .discovery
            .as_ref()
            .is_some_and(|cached| cached.valid_until > now)
        {
            prior.discovery.clone().ok_or(ProviderError::Unavailable)?
        } else {
            self.load_discovery(prior.discovery.as_ref(), now).await?
        };
        let jwks = self
            .load_jwks(&discovery.jwks_uri, prior.jwks.as_ref(), now)
            .await?;
        Ok(CacheState {
            generation: prior.generation,
            discovery: Some(discovery),
            jwks: Some(jwks),
            last_unknown_refresh: prior.last_unknown_refresh,
        })
    }

    async fn load_discovery(
        &self,
        prior: Option<&CachedDiscovery>,
        now: TimestampMicros,
    ) -> Result<CachedDiscovery, ProviderError> {
        let response = self
            .transport
            .get(ProviderHttpRequest {
                url: self.config.discovery_url().to_owned(),
                if_none_match: prior.and_then(|cached| cached.etag.clone()),
                max_body_bytes: MAX_DISCOVERY_BYTES,
                timeout: self.config.request_timeout(),
            })
            .await?;
        validate_response(&response, MAX_DISCOVERY_BYTES)?;
        let valid_until = add_duration(now, self.config.effective_ttl(response.max_age))?;
        match response.status {
            200 => {
                let document: DiscoveryDocument = serde_json::from_slice(&response.body)
                    .map_err(|_| ProviderError::InvalidResponse)?;
                if document.issuer != self.config.provider().issuer {
                    return Err(ProviderError::InvalidResponse);
                }
                self.config.parse_allowed_url(&document.jwks_uri)?;
                Ok(CachedDiscovery {
                    jwks_uri: document.jwks_uri,
                    etag: response.etag,
                    valid_until,
                })
            }
            304 => {
                let prior = prior.ok_or(ProviderError::InvalidResponse)?;
                validate_not_modified_etag(&response, prior.etag.as_deref())?;
                self.counters.not_modified.fetch_add(1, Ordering::Relaxed);
                Ok(CachedDiscovery {
                    jwks_uri: prior.jwks_uri.clone(),
                    etag: response.etag.or_else(|| prior.etag.clone()),
                    valid_until,
                })
            }
            _ => Err(ProviderError::InvalidResponse),
        }
    }

    async fn load_jwks(
        &self,
        uri: &str,
        prior: Option<&CachedJwks>,
        now: TimestampMicros,
    ) -> Result<CachedJwks, ProviderError> {
        self.config.parse_allowed_url(uri)?;
        let matching_prior = prior.filter(|cached| cached.uri == uri);
        let response = self
            .transport
            .get(ProviderHttpRequest {
                url: uri.to_owned(),
                if_none_match: matching_prior.and_then(|cached| cached.etag.clone()),
                max_body_bytes: MAX_JWKS_BYTES,
                timeout: self.config.request_timeout(),
            })
            .await?;
        validate_response(&response, MAX_JWKS_BYTES)?;
        let valid_until = add_duration(now, self.config.effective_ttl(response.max_age))?;
        let (bytes, etag) = match response.status {
            200 => (Arc::new(response.body), response.etag),
            304 => {
                let prior = matching_prior.ok_or(ProviderError::InvalidResponse)?;
                validate_not_modified_etag(&response, prior.etag.as_deref())?;
                self.counters.not_modified.fetch_add(1, Ordering::Relaxed);
                (
                    Arc::clone(&prior.bytes),
                    response.etag.or_else(|| prior.etag.clone()),
                )
            }
            _ => return Err(ProviderError::InvalidResponse),
        };
        let snapshot = JwtVerifierSnapshot::from_jwks_json(
            self.config.provider().clone(),
            bytes.as_slice(),
            valid_until,
        )?;
        Ok(CachedJwks {
            uri: uri.to_owned(),
            bytes,
            etag,
            snapshot: Arc::new(snapshot),
        })
    }
}

#[derive(Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    jwks_uri: String,
}

fn add_duration(
    now: TimestampMicros,
    duration: Duration,
) -> Result<TimestampMicros, ProviderError> {
    if now.get() < 0 || duration.is_zero() {
        return Err(ProviderError::InvalidResponse);
    }
    let micros = i64::try_from(duration.as_micros()).map_err(|_| ProviderError::LimitExceeded)?;
    now.get()
        .checked_add(micros)
        .map(TimestampMicros::new)
        .ok_or(ProviderError::LimitExceeded)
}

fn cooldown_active(
    last: Option<TimestampMicros>,
    now: TimestampMicros,
    cooldown: Duration,
) -> Result<bool, ProviderError> {
    if now.get() < 0 {
        return Err(ProviderError::InvalidConfig);
    }
    let Some(last) = last else {
        return Ok(false);
    };
    let threshold =
        i64::try_from(cooldown.as_micros()).map_err(|_| ProviderError::InvalidConfig)?;
    Ok(now
        .get()
        .checked_sub(last.get())
        .is_none_or(|elapsed| elapsed < threshold))
}

fn validate_not_modified_etag(
    response: &ProviderHttpResponse,
    prior: Option<&str>,
) -> Result<(), ProviderError> {
    let prior = prior.ok_or(ProviderError::InvalidResponse)?;
    if response.body.is_empty() && response.etag.as_deref().is_none_or(|etag| etag == prior) {
        Ok(())
    } else {
        Err(ProviderError::InvalidResponse)
    }
}

fn validate_response(
    response: &ProviderHttpResponse,
    max_body_bytes: usize,
) -> Result<(), ProviderError> {
    if response.body.len() > max_body_bytes
        || !matches!(response.status, 200 | 304)
        || response.status == 200 && response.body.is_empty()
        || response.status == 304 && !response.body.is_empty()
    {
        return Err(if response.body.len() > max_body_bytes {
            ProviderError::LimitExceeded
        } else {
            ProviderError::InvalidResponse
        });
    }
    if let Some(etag) = &response.etag {
        validate_etag(etag)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        error::Error,
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, jwk::PublicKeyUse};
    use rand::thread_rng;
    use rsa::{RsaPrivateKey, pkcs1::EncodeRsaPrivateKey};
    use runku_identity::{
        ApplicationScope, JwtAlgorithm, JwtPrincipalProfile, JwtProviderConfig, PrincipalKind,
    };
    use serde_json::{Value, json};

    use super::*;
    use crate::AllowedHttpsOrigin;

    const NOW_SECONDS: i64 = 1_700_000_000;
    const NOW_MICROS: i64 = NOW_SECONDS * 1_000_000;
    const APP_ID: &str = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const DISCOVERY_URL: &str = "https://identity.example.test/.well-known/openid-configuration";
    const JWKS_URL: &str = "https://keys.example.test/jwks.json";

    struct SigningFixture {
        encoding: EncodingKey,
        jwks: Vec<u8>,
        kid: String,
    }

    fn signing_fixture(kid: &str) -> Result<SigningFixture, Box<dyn Error>> {
        let private = RsaPrivateKey::new(&mut thread_rng(), 2_048)?;
        let der = private.to_pkcs1_der()?;
        let encoding = EncodingKey::from_rsa_der(der.as_bytes());
        let mut jwk = jsonwebtoken::jwk::Jwk::from_encoding_key(&encoding, Algorithm::RS256)?;
        jwk.common.key_id = Some(kid.to_owned());
        jwk.common.public_key_use = Some(PublicKeyUse::Signature);
        let jwks = serde_json::to_vec(&jsonwebtoken::jwk::JwkSet { keys: vec![jwk] })?;
        Ok(SigningFixture {
            encoding,
            jwks,
            kid: kid.to_owned(),
        })
    }

    fn provider_config() -> Result<JwtProviderConfig, Box<dyn Error>> {
        let mut audiences = BTreeSet::new();
        audiences.insert("runku-api".to_owned());
        let mut algorithms = BTreeSet::new();
        algorithms.insert(JwtAlgorithm::Rs256);
        let mut base_scopes = BTreeSet::new();
        base_scopes.insert("functions:execute".parse::<ApplicationScope>()?);
        let mut application_mapping = BTreeMap::new();
        application_mapping.insert("web-main".to_owned(), APP_ID.parse()?);
        Ok(JwtProviderConfig {
            provider_id: "acme.identity".to_owned(),
            issuer: "https://identity.example.test/tenant".to_owned(),
            audiences,
            profile: JwtPrincipalProfile::User,
            required_type: Some("at+jwt".to_owned()),
            discriminator_claim: "token_class".to_owned(),
            discriminator_value: "user".to_owned(),
            algorithms,
            base_scopes,
            scope_claim: None,
            scope_mapping: BTreeMap::new(),
            application_claim: Some("client_ref".to_owned()),
            application_mapping,
            max_token_ttl: Duration::from_hours(1),
            future_clock_skew: Duration::from_secs(30),
            mapping_revision: 1,
        })
    }

    fn network_config() -> Result<ProviderNetworkConfig, Box<dyn Error>> {
        let allowed_origins = ["https://identity.example.test", "https://keys.example.test"]
            .into_iter()
            .map(str::parse::<AllowedHttpsOrigin>)
            .collect::<Result<BTreeSet<_>, _>>()?;
        Ok(ProviderNetworkConfig {
            provider: provider_config()?,
            discovery_url: DISCOVERY_URL.to_owned(),
            allowed_origins,
            default_cache_ttl: Duration::from_mins(1),
            max_cache_ttl: Duration::from_hours(1),
            request_timeout: Duration::from_secs(2),
            unknown_kid_cooldown: Duration::from_secs(5),
        })
    }

    fn discovery(issuer: &str, jwks_uri: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "issuer": issuer,
            "jwks_uri": jwks_uri,
            "authorization_endpoint": "ignored"
        }))
        .unwrap_or_default()
    }

    fn response(
        status: u16,
        etag: Option<&str>,
        max_age_seconds: u64,
        body: Vec<u8>,
    ) -> ProviderHttpResponse {
        ProviderHttpResponse {
            status,
            etag: etag.map(str::to_owned),
            max_age: Some(Duration::from_secs(max_age_seconds)),
            body,
        }
    }

    fn token(fixture: &SigningFixture) -> Result<String, jsonwebtoken::errors::Error> {
        let claims: Value = json!({
            "iss": "https://identity.example.test/tenant",
            "sub": "private-subject",
            "aud": "runku-api",
            "iat": NOW_SECONDS - 10,
            "exp": NOW_SECONDS + 600,
            "token_class": "user",
            "client_ref": "web-main"
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(fixture.kid.clone());
        header.typ = Some("at+jwt".to_owned());
        encode(&header, &claims, &fixture.encoding)
    }

    #[derive(Debug)]
    struct FakeTransport {
        responses: StdMutex<VecDeque<Result<ProviderHttpResponse, ProviderError>>>,
        calls: AtomicUsize,
        delay: Duration,
    }

    impl FakeTransport {
        fn new(
            responses: Vec<Result<ProviderHttpResponse, ProviderError>>,
            delay: Duration,
        ) -> Self {
            Self {
                responses: StdMutex::new(responses.into()),
                calls: AtomicUsize::new(0),
                delay,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ProviderHttpTransport for FakeTransport {
        async fn get(
            &self,
            _request: ProviderHttpRequest,
        ) -> Result<ProviderHttpResponse, ProviderError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.responses
                .lock()
                .map_err(|_| ProviderError::Unavailable)?
                .pop_front()
                .ok_or(ProviderError::TransportUnavailable)?
        }
    }

    #[tokio::test]
    async fn cold_cache_rotation_cooldown_and_failure_are_bounded() -> Result<(), Box<dyn Error>> {
        let first = signing_fixture("key-1")?;
        let second = signing_fixture("key-2")?;
        let fake = Arc::new(FakeTransport::new(
            vec![
                Ok(response(
                    200,
                    Some("\"discovery-v1\""),
                    300,
                    discovery("https://identity.example.test/tenant", JWKS_URL),
                )),
                Ok(response(200, Some("\"jwks-v1\""), 300, first.jwks.clone())),
                Ok(response(200, Some("\"jwks-v2\""), 300, second.jwks.clone())),
                Err(ProviderError::TransportUnavailable),
            ],
            Duration::ZERO,
        ));
        let manager = JwtProviderManager::new(network_config()?, fake.clone())?;
        let crypto = KeyringCrypto::new([8; 32]);
        let first_token = token(&first)?;
        let second_token = token(&second)?;

        let PrincipalEvidence::Valid(first_principal) = manager
            .verify(&first_token, &crypto, TimestampMicros::new(NOW_MICROS))
            .await?
        else {
            return Err("expected user".into());
        };
        assert_eq!(first_principal.kind(), PrincipalKind::User);
        manager
            .verify(&first_token, &crypto, TimestampMicros::new(NOW_MICROS + 1))
            .await?;
        assert_eq!(fake.calls(), 2);

        manager
            .verify(&second_token, &crypto, TimestampMicros::new(NOW_MICROS + 2))
            .await?;
        assert_eq!(fake.calls(), 3);

        let third = signing_fixture("key-3")?;
        let third_token = token(&third)?;
        assert_eq!(
            manager
                .verify(&third_token, &crypto, TimestampMicros::new(NOW_MICROS + 3))
                .await,
            Err(ProviderError::Identity(IdentityError::JwksRefreshRequired))
        );
        assert_eq!(fake.calls(), 3);
        assert_eq!(manager.telemetry().cooldown_suppressions, 1);

        assert_eq!(
            manager
                .verify(
                    &third_token,
                    &crypto,
                    TimestampMicros::new(NOW_MICROS + 6_000_003)
                )
                .await,
            Err(ProviderError::TransportUnavailable)
        );
        manager
            .verify(
                &second_token,
                &crypto,
                TimestampMicros::new(NOW_MICROS + 6_000_004),
            )
            .await?;
        assert_eq!(manager.telemetry().refresh_failures, 1);
        assert_eq!(fake.calls(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn expiry_revalidates_discovery_and_jwks_with_etags() -> Result<(), Box<dyn Error>> {
        let fixture = signing_fixture("stable-key")?;
        let fake = Arc::new(FakeTransport::new(
            vec![
                Ok(response(
                    200,
                    Some("\"discovery\""),
                    5,
                    discovery("https://identity.example.test/tenant", JWKS_URL),
                )),
                Ok(response(200, Some("\"jwks\""), 5, fixture.jwks.clone())),
                Ok(response(304, Some("\"discovery\""), 60, Vec::new())),
                Ok(response(304, Some("\"jwks\""), 60, Vec::new())),
            ],
            Duration::ZERO,
        ));
        let manager = JwtProviderManager::new(network_config()?, fake.clone())?;
        let crypto = KeyringCrypto::new([8; 32]);
        let token = token(&fixture)?;
        manager
            .verify(&token, &crypto, TimestampMicros::new(NOW_MICROS))
            .await?;
        manager
            .verify(
                &token,
                &crypto,
                TimestampMicros::new(NOW_MICROS + 6_000_000),
            )
            .await?;
        assert_eq!(fake.calls(), 4);
        assert_eq!(manager.telemetry().not_modified, 2);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_cold_requests_are_single_flight() -> Result<(), Box<dyn Error>> {
        let fixture = signing_fixture("single-flight")?;
        let fake = Arc::new(FakeTransport::new(
            vec![
                Ok(response(
                    200,
                    None,
                    60,
                    discovery("https://identity.example.test/tenant", JWKS_URL),
                )),
                Ok(response(200, None, 60, fixture.jwks.clone())),
            ],
            Duration::from_millis(20),
        ));
        let manager = Arc::new(JwtProviderManager::new(network_config()?, fake.clone())?);
        let token = token(&fixture)?;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let manager = Arc::clone(&manager);
            let token = token.clone();
            tasks.push(tokio::spawn(async move {
                manager
                    .verify(
                        &token,
                        &KeyringCrypto::new([8; 32]),
                        TimestampMicros::new(NOW_MICROS),
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await??;
        }
        assert_eq!(fake.calls(), 2);
        assert_eq!(manager.telemetry().refresh_attempts, 1);
        assert_eq!(manager.telemetry().verified, 8);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_discovery_never_partially_publishes_cache() -> Result<(), Box<dyn Error>> {
        let fixture = signing_fixture("atomic")?;
        let fake = Arc::new(FakeTransport::new(
            vec![
                Ok(response(
                    200,
                    None,
                    60,
                    discovery("https://attacker.example", JWKS_URL),
                )),
                Ok(response(
                    200,
                    None,
                    60,
                    discovery(
                        "https://identity.example.test/tenant",
                        "https://attacker.example/jwks",
                    ),
                )),
                Ok(response(
                    200,
                    None,
                    60,
                    discovery("https://identity.example.test/tenant", JWKS_URL),
                )),
                Ok(response(200, None, 60, b"not-jwks".to_vec())),
                Ok(response(
                    200,
                    None,
                    60,
                    discovery("https://identity.example.test/tenant", JWKS_URL),
                )),
                Ok(response(200, None, 60, fixture.jwks.clone())),
            ],
            Duration::ZERO,
        ));
        let manager = JwtProviderManager::new(network_config()?, fake.clone())?;
        let token = token(&fixture)?;
        let crypto = KeyringCrypto::new([8; 32]);
        assert_eq!(
            manager
                .verify(&token, &crypto, TimestampMicros::new(NOW_MICROS))
                .await,
            Err(ProviderError::InvalidResponse)
        );
        assert_eq!(
            manager
                .verify(&token, &crypto, TimestampMicros::new(NOW_MICROS + 1))
                .await,
            Err(ProviderError::UrlDenied)
        );
        assert!(matches!(
            manager
                .verify(&token, &crypto, TimestampMicros::new(NOW_MICROS + 2))
                .await,
            Err(ProviderError::Identity(IdentityError::InvalidInput))
        ));
        manager
            .verify(&token, &crypto, TimestampMicros::new(NOW_MICROS + 3))
            .await?;
        assert_eq!(fake.calls(), 6);
        assert_eq!(manager.telemetry().refresh_failures, 3);
        assert_eq!(manager.telemetry().refresh_successes, 1);
        Ok(())
    }

    #[test]
    fn fake_transport_responses_are_revalidated_at_cache_boundary() {
        assert_eq!(
            validate_response(&response(500, None, 60, vec![1]), 32),
            Err(ProviderError::InvalidResponse)
        );
        assert_eq!(
            validate_response(&response(200, None, 60, Vec::new()), 32),
            Err(ProviderError::InvalidResponse)
        );
        assert_eq!(
            validate_response(&response(304, None, 60, vec![1]), 32),
            Err(ProviderError::InvalidResponse)
        );
        assert_eq!(
            validate_response(&response(200, None, 60, vec![0; 33]), 32),
            Err(ProviderError::LimitExceeded)
        );
        assert_eq!(
            validate_response(&response(200, Some("bad\netag"), 60, vec![1]), 32),
            Err(ProviderError::InvalidResponse)
        );
        assert_eq!(
            add_duration(TimestampMicros::new(NOW_MICROS), Duration::ZERO),
            Err(ProviderError::InvalidResponse)
        );
    }
}
