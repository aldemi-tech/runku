use std::{fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit as _, Mac as _};
use runku_core::DevelopmentCredentialId;
use sha2::Sha256;
use subtle::ConstantTimeEq as _;
use zeroize::Zeroizing;

use crate::DevelopmentAccessError;

const PREFIX: &str = "rk_dev_v1_";
const ULID_BYTES: usize = 26;
const TOKEN_BYTES: usize = 32;
const DIGEST_DOMAIN: &[u8] = b"runku-development-access-credential-v1\0";

/// HMAC-SHA-256 verifier safe to persist instead of a bearer key.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct DevelopmentKeyDigest([u8; 32]);

impl DevelopmentKeyDigest {
    /// Creates a digest from exact persisted bytes.
    #[must_use]
    pub const fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }

    /// Returns exact bytes for SQL persistence.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for DevelopmentKeyDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DevelopmentKeyDigest([REDACTED])")
    }
}

/// Complete Development Access bearer held in zeroizing memory.
pub struct DevelopmentKey(Zeroizing<String>);

impl DevelopmentKey {
    /// Exposes the complete key only for one-time delivery or an Authorization header.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for DevelopmentKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DevelopmentKey([REDACTED])")
    }
}

/// Strictly parsed key locator plus redacted complete bearer material.
pub struct ParsedDevelopmentKey {
    id: DevelopmentCredentialId,
    key: DevelopmentKey,
}

impl ParsedDevelopmentKey {
    /// Embedded non-secret lookup identity.
    #[must_use]
    pub const fn credential_id(&self) -> DevelopmentCredentialId {
        self.id
    }

    /// Complete key accessible only to a trusted verifier.
    #[must_use]
    pub fn key(&self) -> &DevelopmentKey {
        &self.key
    }
}

impl fmt::Debug for ParsedDevelopmentKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParsedDevelopmentKey")
            .field("credential_id", &self.id)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

impl FromStr for ParsedDevelopmentKey {
    type Err = DevelopmentAccessError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let body = value
            .strip_prefix(PREFIX)
            .ok_or(DevelopmentAccessError::InvalidCredential)?;
        if body.len() <= ULID_BYTES || body.as_bytes().get(ULID_BYTES) != Some(&b'.') {
            return Err(DevelopmentAccessError::InvalidCredential);
        }
        let ulid = &body[..ULID_BYTES];
        let token = &body[ULID_BYTES + 1..];
        let id = format!("{}{ulid}", DevelopmentCredentialId::PREFIX)
            .parse()
            .map_err(|_| DevelopmentAccessError::InvalidCredential)?;
        let decoded = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| DevelopmentAccessError::InvalidCredential)?;
        if decoded.len() != TOKEN_BYTES || URL_SAFE_NO_PAD.encode(&decoded) != token {
            return Err(DevelopmentAccessError::InvalidCredential);
        }
        Ok(Self {
            id,
            key: DevelopmentKey(Zeroizing::new(value.to_owned())),
        })
    }
}

/// Newly generated bearer and its non-recoverable persisted verifier.
#[derive(Debug)]
pub struct GeneratedDevelopmentKey {
    /// Complete key shown exactly once.
    pub key: DevelopmentKey,
    /// HMAC digest safe to persist.
    pub digest: DevelopmentKeyDigest,
}

/// Process/configuration secret used only for Development Access key verification.
pub struct DevelopmentKeyCrypto(Zeroizing<[u8; 32]>);

impl fmt::Debug for DevelopmentKeyCrypto {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DevelopmentKeyCrypto([REDACTED])")
    }
}

impl DevelopmentKeyCrypto {
    /// Constructs a verifier from a dedicated operator-managed pepper.
    #[must_use]
    pub fn new(pepper: [u8; 32]) -> Self {
        Self(Zeroizing::new(pepper))
    }

    /// Generates one 256-bit CSPRNG-backed bearer.
    ///
    /// # Errors
    ///
    /// Returns entropy unavailable when the operating system RNG fails.
    pub fn generate(
        &self,
        id: DevelopmentCredentialId,
    ) -> Result<GeneratedDevelopmentKey, DevelopmentAccessError> {
        let mut random = Zeroizing::new([0_u8; TOKEN_BYTES]);
        getrandom::fill(random.as_mut()).map_err(|_| DevelopmentAccessError::EntropyUnavailable)?;
        let token = URL_SAFE_NO_PAD.encode(random.as_ref());
        let key = DevelopmentKey(Zeroizing::new(format!("{PREFIX}{}.{token}", id.as_ulid())));
        let digest = self.digest(&key)?;
        Ok(GeneratedDevelopmentKey { key, digest })
    }

    /// Computes the domain-separated keyed digest.
    ///
    /// # Errors
    ///
    /// Fails closed if the HMAC implementation rejects the fixed pepper size.
    pub fn digest(
        &self,
        key: &DevelopmentKey,
    ) -> Result<DevelopmentKeyDigest, DevelopmentAccessError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.0.as_ref())
            .map_err(|_| DevelopmentAccessError::InvalidInput)?;
        mac.update(DIGEST_DOMAIN);
        mac.update(key.expose().as_bytes());
        let bytes = mac.finalize().into_bytes();
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&bytes);
        Ok(DevelopmentKeyDigest(digest))
    }

    /// Verifies a complete bearer against its stored digest in constant time.
    #[must_use]
    pub fn verify(&self, key: &DevelopmentKey, expected: DevelopmentKeyDigest) -> bool {
        self.digest(key)
            .is_ok_and(|candidate| bool::from(candidate.0.ct_eq(&expected.0)))
    }
}
