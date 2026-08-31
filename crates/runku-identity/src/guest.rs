//! Locally signed, lookup-free guest identity tokens.

use std::{
    collections::BTreeSet,
    fmt,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use runku_core::{ApplicationClientId, EnvironmentId, EnvironmentScope, ProjectId};
use runku_value::TimestampMicros;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::{
    ApplicationScope, AuthenticatedPrincipal, IdentityError, KeyringCrypto, PrincipalEvidence,
    PrincipalKind,
};

const TOKEN_PREFIX: &str = "rk_gst_v1_";
const PAYLOAD_MAGIC: &[u8; 3] = b"RG\x01";
const SUBJECT_BYTES: usize = 32;
const SESSION_BYTES: usize = 16;
const SIGNATURE_BYTES: usize = 32;
const BASE_PAYLOAD_BYTES: usize = 3 + 30 + 30 + SUBJECT_BYTES + SESSION_BYTES + 8 + 8 + 1;
const BOUND_PAYLOAD_BYTES: usize = BASE_PAYLOAD_BYTES + 30;
const MAX_KEYS: usize = 16;

/// Stable, non-secret signing-key locator embedded in a guest token.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GuestKeyId(String);

impl GuestKeyId {
    /// Returns the canonical key locator.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GuestKeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for GuestKeyId {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 32
            || value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(IdentityError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Whether a guest signing key may issue, only verify, or is immediately denied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestKeyMode {
    /// The one key used for new tokens and accepted for verification.
    SignAndVerify,
    /// A rotation-overlap key accepted only for existing tokens.
    VerifyOnly,
    /// A compromised/retired key denied even before token expiry.
    Revoked,
}

/// Zeroizing HMAC-SHA-256 guest signing key.
pub struct GuestSigningKey {
    id: GuestKeyId,
    material: Zeroizing<[u8; 32]>,
    mode: GuestKeyMode,
}

impl GuestSigningKey {
    /// Loads exact operator-managed key material.
    #[must_use]
    pub fn new(id: GuestKeyId, material: [u8; 32], mode: GuestKeyMode) -> Self {
        Self {
            id,
            material: Zeroizing::new(material),
            mode,
        }
    }

    /// Generates new key material using the operating-system CSPRNG.
    ///
    /// # Errors
    ///
    /// Returns `IDENTITY_ENTROPY_UNAVAILABLE` if the system CSPRNG fails.
    pub fn generate(id: GuestKeyId, mode: GuestKeyMode) -> Result<Self, IdentityError> {
        let mut material = Zeroizing::new([0_u8; 32]);
        getrandom::fill(material.as_mut()).map_err(|_| IdentityError::EntropyUnavailable)?;
        Ok(Self { id, material, mode })
    }

    /// Non-secret key locator.
    #[must_use]
    pub const fn id(&self) -> &GuestKeyId {
        &self.id
    }

    /// Current lifecycle mode.
    #[must_use]
    pub const fn mode(&self) -> GuestKeyMode {
        self.mode
    }
}

impl fmt::Debug for GuestSigningKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GuestSigningKey")
            .field("id", &self.id)
            .field("material", &"[REDACTED]")
            .field("mode", &self.mode)
            .finish()
    }
}

/// Issuance/verification limits for local guest identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestTokenPolicy {
    /// Maximum lifetime accepted in issued or presented tokens.
    pub max_ttl: Duration,
    /// Maximum future issued-at clock skew accepted during verification.
    pub future_clock_skew: Duration,
    /// Principal mapping revision included in effective identity hashes.
    pub mapping_revision: u64,
}

impl Default for GuestTokenPolicy {
    fn default() -> Self {
        Self {
            max_ttl: Duration::from_hours(720),
            future_clock_skew: Duration::from_mins(1),
            mapping_revision: 1,
        }
    }
}

impl GuestTokenPolicy {
    fn validate(self) -> Result<(), IdentityError> {
        if self.max_ttl < Duration::from_mins(1)
            || self.max_ttl > Duration::from_hours(8_760)
            || self.future_clock_skew > Duration::from_mins(5)
            || self.mapping_revision == 0
        {
            return Err(IdentityError::InvalidInput);
        }
        Ok(())
    }
}

/// Complete guest token held in zeroizing memory and redacted from debug output.
pub struct GuestToken(Zeroizing<String>);

impl GuestToken {
    /// Returns the bearer token for one-time delivery or Authorization transport.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for GuestToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GuestToken([REDACTED])")
    }
}

/// Bounded guest-token counters without subject/session/key labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GuestTokenTelemetrySnapshot {
    /// Successfully issued tokens.
    pub issued: u64,
    /// Successfully verified tokens.
    pub verified: u64,
    /// Malformed, tampered, expired, mismatched, or revoked tokens.
    pub failures: u64,
}

#[derive(Debug, Default)]
struct GuestCounters {
    issued: AtomicU64,
    verified: AtomicU64,
    failures: AtomicU64,
}

/// Immutable guest signing/verifying configuration snapshot.
#[derive(Debug)]
pub struct GuestKeyring {
    keys: Vec<GuestSigningKey>,
    policy: GuestTokenPolicy,
    counters: GuestCounters,
}

impl GuestKeyring {
    /// Builds a validated keyring with exactly one issuance key.
    ///
    /// # Errors
    ///
    /// Rejects duplicate IDs, invalid policy, empty/oversized keysets, or multiple/no signers.
    pub fn new(
        keys: Vec<GuestSigningKey>,
        policy: GuestTokenPolicy,
    ) -> Result<Self, IdentityError> {
        policy.validate()?;
        if keys.is_empty()
            || keys.len() > MAX_KEYS
            || keys.windows(2).any(|pair| pair[0].id >= pair[1].id)
            || keys
                .iter()
                .filter(|key| key.mode == GuestKeyMode::SignAndVerify)
                .count()
                != 1
        {
            return Err(IdentityError::InvalidInput);
        }
        Ok(Self {
            keys,
            policy,
            counters: GuestCounters::default(),
        })
    }

    /// Issues a new non-PII guest subject/session token for one Environment.
    ///
    /// # Errors
    ///
    /// Rejects invalid time/TTL and fails if the operating-system CSPRNG or HMAC is unavailable.
    pub fn issue(
        &self,
        scope: EnvironmentScope,
        bound_application: Option<ApplicationClientId>,
        now: TimestampMicros,
        ttl: Duration,
    ) -> Result<GuestToken, IdentityError> {
        let result = self.issue_inner(scope, bound_application, now, ttl);
        if result.is_ok() {
            self.counters.issued.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn issue_inner(
        &self,
        scope: EnvironmentScope,
        bound_application: Option<ApplicationClientId>,
        now: TimestampMicros,
        ttl: Duration,
    ) -> Result<GuestToken, IdentityError> {
        if now.get() < 0 || ttl.is_zero() || ttl > self.policy.max_ttl {
            return Err(IdentityError::InvalidInput);
        }
        let ttl_micros = duration_micros(ttl)?;
        let expires_at = now
            .get()
            .checked_add(ttl_micros)
            .ok_or(IdentityError::InvalidInput)?;
        let mut subject = Zeroizing::new([0_u8; SUBJECT_BYTES]);
        let mut session = Zeroizing::new([0_u8; SESSION_BYTES]);
        getrandom::fill(subject.as_mut()).map_err(|_| IdentityError::EntropyUnavailable)?;
        getrandom::fill(session.as_mut()).map_err(|_| IdentityError::EntropyUnavailable)?;
        let mut payload = Zeroizing::new(Vec::with_capacity(if bound_application.is_some() {
            BOUND_PAYLOAD_BYTES
        } else {
            BASE_PAYLOAD_BYTES
        }));
        payload.extend_from_slice(PAYLOAD_MAGIC);
        payload.extend_from_slice(scope.project_id().to_string().as_bytes());
        payload.extend_from_slice(scope.environment_id().to_string().as_bytes());
        payload.extend_from_slice(subject.as_ref());
        payload.extend_from_slice(session.as_ref());
        payload.extend_from_slice(&now.get().to_be_bytes());
        payload.extend_from_slice(&expires_at.to_be_bytes());
        match bound_application {
            None => payload.push(0),
            Some(client_id) => {
                payload.push(1);
                payload.extend_from_slice(client_id.to_string().as_bytes());
            }
        }
        let signer = self
            .keys
            .iter()
            .find(|key| key.mode == GuestKeyMode::SignAndVerify)
            .ok_or(IdentityError::InvalidInput)?;
        let encoded_payload = Zeroizing::new(URL_SAFE_NO_PAD.encode(payload.as_slice()));
        let signed_input = Zeroizing::new(format!(
            "{TOKEN_PREFIX}{}.{encoded_payload}",
            signer.id,
            encoded_payload = encoded_payload.as_str()
        ));
        let signature = sign(signer, signed_input.as_bytes())?;
        Ok(GuestToken(Zeroizing::new(format!(
            "{signed}.{}",
            URL_SAFE_NO_PAD.encode(signature),
            signed = signed_input.as_str()
        ))))
    }

    /// Verifies and normalizes a guest token without persistence/network lookup.
    ///
    /// # Errors
    ///
    /// Fails closed for syntax, signature, key status, Environment, timestamp, TTL, or binding errors.
    pub fn verify(
        &self,
        scope: EnvironmentScope,
        token: &str,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<PrincipalEvidence, IdentityError> {
        let result = self.verify_inner(scope, token, crypto, now);
        if result.is_ok() {
            self.counters.verified.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn verify_inner(
        &self,
        scope: EnvironmentScope,
        token: &str,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<PrincipalEvidence, IdentityError> {
        if now.get() < 0 || token.len() > 512 {
            return Err(IdentityError::InvalidPrincipal);
        }
        let body = token
            .strip_prefix(TOKEN_PREFIX)
            .ok_or(IdentityError::InvalidPrincipal)?;
        let mut segments = body.split('.');
        let key_id: GuestKeyId = segments
            .next()
            .ok_or(IdentityError::InvalidPrincipal)?
            .parse()
            .map_err(|_| IdentityError::InvalidPrincipal)?;
        let encoded_payload = segments.next().ok_or(IdentityError::InvalidPrincipal)?;
        let encoded_signature = segments.next().ok_or(IdentityError::InvalidPrincipal)?;
        if segments.next().is_some() {
            return Err(IdentityError::InvalidPrincipal);
        }
        let key = self
            .keys
            .iter()
            .find(|key| key.id == key_id)
            .ok_or(IdentityError::InvalidPrincipal)?;
        if key.mode == GuestKeyMode::Revoked {
            return Err(IdentityError::InvalidPrincipal);
        }
        let signature = decode_canonical(encoded_signature, SIGNATURE_BYTES)?;
        let signed = Zeroizing::new(format!("{TOKEN_PREFIX}{key_id}.{encoded_payload}"));
        let mut mac = Hmac::<Sha256>::new_from_slice(key.material.as_ref())
            .map_err(|_| IdentityError::InvalidPrincipal)?;
        mac.update(signed.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| IdentityError::InvalidPrincipal)?;
        let payload = decode_payload(encoded_payload)?;
        if payload.project_id != scope.project_id()
            || payload.environment_id != scope.environment_id()
        {
            return Err(IdentityError::InvalidPrincipal);
        }
        let max_ttl = duration_micros(self.policy.max_ttl)?;
        let skew = duration_micros(self.policy.future_clock_skew)?;
        if payload.issued_at < 0
            || payload.expires_at <= payload.issued_at
            || payload.expires_at - payload.issued_at > max_ttl
            || payload.issued_at > now.get().saturating_add(skew)
            || payload.expires_at <= now.get()
        {
            return Err(IdentityError::InvalidPrincipal);
        }
        let subject = Zeroizing::new(URL_SAFE_NO_PAD.encode(payload.subject.as_ref()));
        let principal = AuthenticatedPrincipal::new(
            crypto.derive_principal_id(
                "runku-guest",
                &format!("{}/{}", scope.project_id(), scope.environment_id()),
                &subject,
            )?,
            PrincipalKind::Guest,
            "runku-guest",
            BTreeSet::from(["identity:guest".parse::<ApplicationScope>()?]),
            payload.bound_application,
            Some(TimestampMicros::new(payload.issued_at)),
            Some(TimestampMicros::new(payload.expires_at)),
            self.policy.mapping_revision,
        )?;
        Ok(PrincipalEvidence::Valid(principal))
    }

    /// Returns bounded counters without guest/key/session labels.
    #[must_use]
    pub fn telemetry(&self) -> GuestTokenTelemetrySnapshot {
        GuestTokenTelemetrySnapshot {
            issued: self.counters.issued.load(Ordering::Relaxed),
            verified: self.counters.verified.load(Ordering::Relaxed),
            failures: self.counters.failures.load(Ordering::Relaxed),
        }
    }
}

struct GuestPayload {
    project_id: ProjectId,
    environment_id: EnvironmentId,
    subject: Zeroizing<[u8; SUBJECT_BYTES]>,
    bound_application: Option<ApplicationClientId>,
    issued_at: i64,
    expires_at: i64,
}

fn decode_payload(encoded: &str) -> Result<GuestPayload, IdentityError> {
    let bytes = Zeroizing::new(
        URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| IdentityError::InvalidPrincipal)?,
    );
    if (bytes.len() != BASE_PAYLOAD_BYTES && bytes.len() != BOUND_PAYLOAD_BYTES)
        || URL_SAFE_NO_PAD.encode(&bytes) != encoded
        || bytes.get(..3) != Some(PAYLOAD_MAGIC.as_slice())
    {
        return Err(IdentityError::InvalidPrincipal);
    }
    let project_id: ProjectId = std::str::from_utf8(&bytes[3..33])
        .map_err(|_| IdentityError::InvalidPrincipal)?
        .parse()
        .map_err(|_| IdentityError::InvalidPrincipal)?;
    let environment_id: EnvironmentId = std::str::from_utf8(&bytes[33..63])
        .map_err(|_| IdentityError::InvalidPrincipal)?
        .parse()
        .map_err(|_| IdentityError::InvalidPrincipal)?;
    let mut subject = Zeroizing::new([0_u8; SUBJECT_BYTES]);
    subject.copy_from_slice(&bytes[63..95]);
    let issued_at = i64::from_be_bytes(
        bytes[111..119]
            .try_into()
            .map_err(|_| IdentityError::InvalidPrincipal)?,
    );
    let expires_at = i64::from_be_bytes(
        bytes[119..127]
            .try_into()
            .map_err(|_| IdentityError::InvalidPrincipal)?,
    );
    let bound_application = match bytes[127] {
        0 if bytes.len() == BASE_PAYLOAD_BYTES => None,
        1 if bytes.len() == BOUND_PAYLOAD_BYTES => Some(
            std::str::from_utf8(&bytes[128..158])
                .map_err(|_| IdentityError::InvalidPrincipal)?
                .parse()
                .map_err(|_| IdentityError::InvalidPrincipal)?,
        ),
        _ => return Err(IdentityError::InvalidPrincipal),
    };
    Ok(GuestPayload {
        project_id,
        environment_id,
        subject,
        bound_application,
        issued_at,
        expires_at,
    })
}

fn decode_canonical(value: &str, expected_bytes: usize) -> Result<Vec<u8>, IdentityError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| IdentityError::InvalidPrincipal)?;
    if bytes.len() != expected_bytes || URL_SAFE_NO_PAD.encode(&bytes) != value {
        return Err(IdentityError::InvalidPrincipal);
    }
    Ok(bytes)
}

fn sign(key: &GuestSigningKey, bytes: &[u8]) -> Result<[u8; 32], IdentityError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key.material.as_ref())
        .map_err(|_| IdentityError::InvalidInput)?;
    mac.update(bytes);
    Ok(mac.finalize().into_bytes().into())
}

fn duration_micros(duration: Duration) -> Result<i64, IdentityError> {
    i64::try_from(duration.as_micros()).map_err(|_| IdentityError::InvalidInput)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use runku_core::{EnvironmentId, ProjectId};

    use super::*;
    use crate::PrincipalContext;

    fn key(id: &str, byte: u8, mode: GuestKeyMode) -> Result<GuestSigningKey, IdentityError> {
        Ok(GuestSigningKey::new(id.parse()?, [byte; 32], mode))
    }

    fn policy() -> GuestTokenPolicy {
        GuestTokenPolicy {
            max_ttl: Duration::from_hours(1),
            future_clock_skew: Duration::from_secs(30),
            mapping_revision: 9,
        }
    }

    #[test]
    fn issue_verify_rotation_revoke_and_binding_are_exact() -> Result<(), Box<dyn Error>> {
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let crypto = KeyringCrypto::new([91; 32]);
        let old = GuestKeyring::new(
            vec![key("2026-01", 1, GuestKeyMode::SignAndVerify)?],
            policy(),
        )?;
        let application = ApplicationClientId::generate();
        let token = old.issue(
            scope,
            Some(application),
            TimestampMicros::new(1_000_000),
            Duration::from_mins(10),
        )?;
        assert_eq!(format!("{token:?}"), "GuestToken([REDACTED])");
        assert!(!format!("{:?}", old.keys[0]).contains(&"01".repeat(16)));
        let first = old.verify(
            scope,
            token.expose(),
            &crypto,
            TimestampMicros::new(2_000_000),
        )?;
        let repeated = old.verify(
            scope,
            token.expose(),
            &crypto,
            TimestampMicros::new(2_000_000),
        )?;
        let (PrincipalEvidence::Valid(first), PrincipalEvidence::Valid(repeated)) =
            (first, repeated)
        else {
            return Err(IdentityError::InvalidPrincipal.into());
        };
        assert_eq!(first.kind(), PrincipalKind::Guest);
        assert_eq!(first.id(), repeated.id());
        assert_eq!(first.bound_application(), Some(application));
        assert_eq!(first.mapping_revision(), 9);

        let rotated = GuestKeyring::new(
            vec![
                key("2026-01", 1, GuestKeyMode::VerifyOnly)?,
                key("2026-08", 2, GuestKeyMode::SignAndVerify)?,
            ],
            policy(),
        )?;
        assert!(
            rotated
                .verify(
                    scope,
                    token.expose(),
                    &crypto,
                    TimestampMicros::new(2_000_000)
                )
                .is_ok()
        );
        let new_token = rotated.issue(
            scope,
            None,
            TimestampMicros::new(2_000_000),
            Duration::from_mins(10),
        )?;
        assert!(new_token.expose().starts_with("rk_gst_v1_2026-08."));
        assert_ne!(new_token.expose(), token.expose());

        let revoked = GuestKeyring::new(
            vec![
                key("2026-01", 1, GuestKeyMode::Revoked)?,
                key("2026-08", 2, GuestKeyMode::SignAndVerify)?,
            ],
            policy(),
        )?;
        assert_eq!(
            revoked.verify(
                scope,
                token.expose(),
                &crypto,
                TimestampMicros::new(2_000_000)
            ),
            Err(IdentityError::InvalidPrincipal)
        );
        assert!(
            revoked
                .verify(
                    scope,
                    new_token.expose(),
                    &crypto,
                    TimestampMicros::new(3_000_000)
                )
                .is_ok()
        );
        assert_eq!(old.telemetry().issued, 1);
        assert_eq!(old.telemetry().verified, 2);
        assert_eq!(revoked.telemetry().failures, 1);
        Ok(())
    }

    #[test]
    fn tamper_cross_environment_time_and_parser_fail_closed() -> Result<(), Box<dyn Error>> {
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let other = EnvironmentScope::new(scope.project_id(), EnvironmentId::generate());
        let other_project = EnvironmentScope::new(ProjectId::generate(), scope.environment_id());
        let crypto = KeyringCrypto::new([92; 32]);
        let ring = GuestKeyring::new(
            vec![key("active", 3, GuestKeyMode::SignAndVerify)?],
            policy(),
        )?;
        let token = ring.issue(
            scope,
            None,
            TimestampMicros::new(100_000_000),
            Duration::from_mins(1),
        )?;
        assert_eq!(
            ring.verify(
                other,
                token.expose(),
                &crypto,
                TimestampMicros::new(101_000_000)
            ),
            Err(IdentityError::InvalidPrincipal)
        );
        assert_eq!(
            ring.verify(
                other_project,
                token.expose(),
                &crypto,
                TimestampMicros::new(101_000_000)
            ),
            Err(IdentityError::InvalidPrincipal)
        );
        assert_eq!(
            ring.verify(
                scope,
                token.expose(),
                &crypto,
                TimestampMicros::new(69_000_000)
            ),
            Err(IdentityError::InvalidPrincipal)
        );
        assert_eq!(
            ring.verify(
                scope,
                token.expose(),
                &crypto,
                TimestampMicros::new(160_000_000)
            ),
            Err(IdentityError::InvalidPrincipal)
        );

        let mut tampered = token.expose().to_owned();
        let last = tampered.pop().ok_or(IdentityError::InvalidPrincipal)?;
        tampered.push(if last == 'A' { 'B' } else { 'A' });
        for invalid in [
            tampered,
            format!("{}.extra", token.expose()),
            format!("{}=", token.expose()),
            "rk_gst_v1_missing".to_owned(),
            "x".repeat(513),
        ] {
            assert_eq!(
                ring.verify(scope, &invalid, &crypto, TimestampMicros::new(101_000_000)),
                Err(IdentityError::InvalidPrincipal)
            );
        }
        assert!(matches!(
            ring.issue(
                scope,
                None,
                TimestampMicros::new(1),
                Duration::from_secs(3_601)
            ),
            Err(IdentityError::InvalidInput)
        ));
        Ok(())
    }

    #[test]
    fn keyring_and_policy_configuration_reject_ambiguity() -> Result<(), Box<dyn Error>> {
        assert!(GuestKeyring::new(Vec::new(), policy()).is_err());
        assert!(
            GuestKeyring::new(
                vec![
                    key("b", 1, GuestKeyMode::SignAndVerify)?,
                    key("a", 2, GuestKeyMode::VerifyOnly)?,
                ],
                policy(),
            )
            .is_err()
        );
        assert!(
            GuestKeyring::new(
                vec![
                    key("a", 1, GuestKeyMode::SignAndVerify)?,
                    key("b", 2, GuestKeyMode::SignAndVerify)?,
                ],
                policy(),
            )
            .is_err()
        );
        assert!(
            GuestKeyring::new(vec![key("a", 1, GuestKeyMode::VerifyOnly)?], policy(),).is_err()
        );
        assert!(matches!(
            GuestKeyring::new(
                vec![key("a", 1, GuestKeyMode::SignAndVerify)?],
                GuestTokenPolicy {
                    mapping_revision: 0,
                    ..policy()
                }
            ),
            Err(IdentityError::InvalidInput)
        ));
        for invalid in ["", "UPPER", "-leading", "trailing-", "bad.key"] {
            assert!(invalid.parse::<GuestKeyId>().is_err());
        }
        let context = PrincipalContext::None;
        assert_eq!(context.kind(), None);
        Ok(())
    }
}
