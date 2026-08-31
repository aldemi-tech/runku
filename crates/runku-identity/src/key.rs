//! Strict external key formats and keyed verification.

use std::{fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use runku_core::CredentialId;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::{CredentialKind, IdentityError, PrincipalId};

const PUB_PREFIX: &str = "rk_pub_v1_";
const SECRET_PREFIX: &str = "rk_sec_v1_";
const ULID_BYTES: usize = 26;
const PUB_TOKEN_BYTES: usize = 16;
const SECRET_TOKEN_BYTES: usize = 32;
const DIGEST_DOMAIN: &[u8] = b"runku-application-credential-v1\0";
const PUB_DOMAIN: &[u8] = b"runku-publishable-key-v1\0";

/// HMAC-SHA-256 digest stored instead of a bearer key.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CredentialDigest([u8; 32]);

impl CredentialDigest {
    /// Creates a digest from exact persisted bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns exact bytes for persistence.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for CredentialDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialDigest([REDACTED])")
    }
}

/// An external application key held in zeroizing memory and redacted from debug output.
pub struct ApplicationKey(Zeroizing<String>);

impl ApplicationKey {
    /// Returns the complete key for one-time delivery or an HTTP header.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for ApplicationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationKey([REDACTED])")
    }
}

/// Parsed key locator plus redacted complete material for verification.
pub struct ParsedApplicationKey {
    id: CredentialId,
    kind: CredentialKind,
    material: ApplicationKey,
}

impl ParsedApplicationKey {
    /// Embedded non-secret lookup identifier.
    #[must_use]
    pub const fn credential_id(&self) -> CredentialId {
        self.id
    }

    /// Type encoded by the external prefix.
    #[must_use]
    pub const fn kind(&self) -> CredentialKind {
        self.kind
    }

    /// Returns complete material only for a trusted verifier.
    #[must_use]
    pub fn key(&self) -> &ApplicationKey {
        &self.material
    }
}

impl fmt::Debug for ParsedApplicationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParsedApplicationKey")
            .field("credential_id", &self.id)
            .field("kind", &self.kind)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl FromStr for ParsedApplicationKey {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (kind, prefix, token_bytes, separator) = if value.starts_with(PUB_PREFIX) {
            (
                CredentialKind::Publishable,
                PUB_PREFIX,
                PUB_TOKEN_BYTES,
                '_',
            )
        } else if value.starts_with(SECRET_PREFIX) {
            (
                CredentialKind::Secret,
                SECRET_PREFIX,
                SECRET_TOKEN_BYTES,
                '.',
            )
        } else {
            return Err(IdentityError::InvalidCredential);
        };
        let body = &value[prefix.len()..];
        if body.len() <= ULID_BYTES || body.as_bytes().get(ULID_BYTES) != Some(&(separator as u8)) {
            return Err(IdentityError::InvalidCredential);
        }
        let ulid = &body[..ULID_BYTES];
        let token = &body[ULID_BYTES + 1..];
        let id: CredentialId = format!("{}{}", CredentialId::PREFIX, ulid)
            .parse()
            .map_err(|_| IdentityError::InvalidCredential)?;
        let decoded = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| IdentityError::InvalidCredential)?;
        if decoded.len() != token_bytes || URL_SAFE_NO_PAD.encode(&decoded) != token {
            return Err(IdentityError::InvalidCredential);
        }
        Ok(Self {
            id,
            kind,
            material: ApplicationKey(Zeroizing::new(value.to_owned())),
        })
    }
}

/// Newly generated credential material and its non-recoverable verifier.
#[derive(Debug)]
pub struct GeneratedCredentialKey {
    /// Complete key. Secret keys must be shown exactly once.
    pub key: ApplicationKey,
    /// Digest safe to persist.
    pub digest: CredentialDigest,
    /// Credential kind encoded in the key.
    pub kind: CredentialKind,
}

/// Process/configuration secret used to derive and verify application credentials.
pub struct KeyringCrypto(Zeroizing<[u8; 32]>);

impl fmt::Debug for KeyringCrypto {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KeyringCrypto([REDACTED])")
    }
}

impl KeyringCrypto {
    /// Creates a verifier from an operator-managed 256-bit pepper.
    #[must_use]
    pub fn new(pepper: [u8; 32]) -> Self {
        Self(Zeroizing::new(pepper))
    }

    /// Generates a publishable key deterministically from its credential ID.
    ///
    /// Deterministic derivation permits an operator to reveal a non-secret publishable key again
    /// without storing full key material. The pepper remains mandatory for unguessability.
    ///
    /// # Errors
    ///
    /// Fails only if the HMAC implementation rejects the fixed key size.
    pub fn generate_publishable(
        &self,
        id: CredentialId,
    ) -> Result<GeneratedCredentialKey, IdentityError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.0.as_ref())
            .map_err(|_| IdentityError::InvalidInput)?;
        mac.update(PUB_DOMAIN);
        mac.update(id.to_string().as_bytes());
        let bytes = mac.finalize().into_bytes();
        let token = URL_SAFE_NO_PAD.encode(&bytes[..PUB_TOKEN_BYTES]);
        self.finish_generated(
            CredentialKind::Publishable,
            format!("{PUB_PREFIX}{}_{token}", id.as_ulid()),
        )
    }

    /// Generates a 256-bit CSPRNG-backed secret key.
    ///
    /// # Errors
    ///
    /// Returns `IDENTITY_ENTROPY_UNAVAILABLE` if the operating system RNG fails.
    pub fn generate_secret(
        &self,
        id: CredentialId,
    ) -> Result<GeneratedCredentialKey, IdentityError> {
        let mut random = Zeroizing::new([0_u8; SECRET_TOKEN_BYTES]);
        getrandom::fill(random.as_mut()).map_err(|_| IdentityError::EntropyUnavailable)?;
        let token = URL_SAFE_NO_PAD.encode(random.as_ref());
        self.finish_generated(
            CredentialKind::Secret,
            format!("{SECRET_PREFIX}{}.{token}", id.as_ulid()),
        )
    }

    fn finish_generated(
        &self,
        kind: CredentialKind,
        key: String,
    ) -> Result<GeneratedCredentialKey, IdentityError> {
        let key = ApplicationKey(Zeroizing::new(key));
        let digest = self.digest(&key)?;
        Ok(GeneratedCredentialKey { key, digest, kind })
    }

    /// Computes the keyed digest persisted by the repository.
    ///
    /// # Errors
    ///
    /// Fails closed if the fixed-size HMAC key cannot be initialized.
    pub fn digest(&self, key: &ApplicationKey) -> Result<CredentialDigest, IdentityError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.0.as_ref())
            .map_err(|_| IdentityError::InvalidInput)?;
        mac.update(DIGEST_DOMAIN);
        mac.update(key.expose().as_bytes());
        let bytes = mac.finalize().into_bytes();
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&bytes);
        Ok(CredentialDigest(digest))
    }

    /// Verifies a parsed key against a stored digest in constant time.
    #[must_use]
    pub fn verify(&self, key: &ApplicationKey, expected: CredentialDigest) -> bool {
        self.digest(key)
            .is_ok_and(|candidate| bool::from(candidate.0.ct_eq(&expected.0)))
    }

    /// Derives a stable opaque principal ID without exposing or reversibly hashing subject data.
    ///
    /// # Errors
    ///
    /// Rejects empty/oversized derivation inputs and fails closed if HMAC initialization fails.
    pub fn derive_principal_id(
        &self,
        provider_id: &str,
        issuer: &str,
        subject: &str,
    ) -> Result<PrincipalId, IdentityError> {
        if provider_id.is_empty()
            || provider_id.len() > 80
            || issuer.is_empty()
            || issuer.len() > 512
            || subject.is_empty()
            || subject.len() > 512
        {
            return Err(IdentityError::InvalidPrincipal);
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(self.0.as_ref())
            .map_err(|_| IdentityError::InvalidInput)?;
        mac.update(b"runku-principal-id-v1\0");
        for value in [provider_id, issuer, subject] {
            let length = u32::try_from(value.len()).map_err(|_| IdentityError::InvalidPrincipal)?;
            mac.update(&length.to_be_bytes());
            mac.update(value.as_bytes());
        }
        Ok(PrincipalId::from_bytes(mac.finalize().into_bytes().into()))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    const ID: &str = "crd_01ARZ3NDEKTSV4RRFFQ69G5FAV";

    #[test]
    fn generated_keys_are_strict_redacted_and_verifiable() -> Result<(), Box<dyn Error>> {
        let crypto = KeyringCrypto::new([7; 32]);
        let id: CredentialId = ID.parse()?;
        let publishable = crypto.generate_publishable(id)?;
        let same = crypto.generate_publishable(id)?;
        assert_eq!(publishable.key.expose(), same.key.expose());
        assert!(crypto.verify(&publishable.key, publishable.digest));
        let parsed: ParsedApplicationKey = publishable.key.expose().parse()?;
        assert_eq!(parsed.credential_id(), id);
        assert_eq!(parsed.kind(), CredentialKind::Publishable);
        assert!(!format!("{parsed:?}").contains(publishable.key.expose()));

        let secret = crypto.generate_secret(id)?;
        assert!(crypto.verify(&secret.key, secret.digest));
        assert_eq!(secret.key.expose().len(), SECRET_PREFIX.len() + 26 + 1 + 43);
        assert_eq!(format!("{:?}", secret.key), "ApplicationKey([REDACTED])");
        Ok(())
    }

    #[test]
    fn malformed_noncanonical_and_wrong_pepper_fail_closed() -> Result<(), Box<dyn Error>> {
        let crypto = KeyringCrypto::new([9; 32]);
        let id: CredentialId = ID.parse()?;
        let secret = crypto.generate_secret(id)?;
        let parsed: ParsedApplicationKey = secret.key.expose().parse()?;
        assert!(!KeyringCrypto::new([8; 32]).verify(parsed.key(), secret.digest));
        for invalid in [
            "rk_sec_v1_bad.value",
            "rk_pub_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV_AA=",
            "rk_secret_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV.value",
        ] {
            assert!(matches!(
                invalid.parse::<ParsedApplicationKey>(),
                Err(IdentityError::InvalidCredential)
            ));
        }
        Ok(())
    }
}
