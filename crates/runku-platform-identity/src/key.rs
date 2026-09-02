//! Domain-separated invitation and session bearer generation.

use std::{fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit as _, Mac as _};
use runku_core::{OperatorInvitationId, OperatorSessionId};
use sha2::Sha256;
use subtle::ConstantTimeEq as _;
use zeroize::Zeroizing;

use crate::PlatformIdentityError;

const INVITATION_PREFIX: &str = "rk_inv_v1_";
const ACCESS_PREFIX: &str = "rk_at_v1_";
const REFRESH_PREFIX: &str = "rk_rt_v1_";
const ULID_BYTES: usize = 26;
const SECRET_BYTES: usize = 32;
const INVITATION_DOMAIN: &[u8] = b"runku-platform-invitation-v1\0";
const ACCESS_DOMAIN: &[u8] = b"runku-platform-access-v1\0";
const REFRESH_DOMAIN: &[u8] = b"runku-platform-refresh-v1\0";

/// Domain-separated HMAC-SHA-256 digest safe to persist instead of a bearer.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PlatformDigest([u8; 32]);

impl PlatformDigest {
    /// Restores an exact persisted digest.
    #[must_use]
    pub const fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }

    /// Returns exact bytes for persistence.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Compares two digests in constant time.
    #[must_use]
    pub fn matches(self, other: Self) -> bool {
        bool::from(self.0.ct_eq(&other.0))
    }
}

impl fmt::Debug for PlatformDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PlatformDigest([REDACTED])")
    }
}

macro_rules! bearer {
    ($(#[$meta:meta])* $name:ident, $id:ty, $prefix:expr, $id_prefix:expr) => {
        $(#[$meta])*
        pub struct $name {
            id: $id,
            value: Zeroizing<String>,
        }

        impl $name {
            /// Embedded non-secret lookup identity.
            #[must_use]
            pub const fn id(&self) -> $id { self.id }

            /// Exposes the bearer only for transport to its intended endpoint.
            #[must_use]
            pub fn expose(&self) -> &str { self.value.as_str() }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("id", &self.id)
                    .field("value", &"[REDACTED]")
                    .finish()
            }
        }

        impl FromStr for $name {
            type Err = PlatformIdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let body = value.strip_prefix($prefix).ok_or(PlatformIdentityError::Unauthenticated)?;
                if body.len() <= ULID_BYTES || body.as_bytes().get(ULID_BYTES) != Some(&b'.') {
                    return Err(PlatformIdentityError::Unauthenticated);
                }
                let ulid = &body[..ULID_BYTES];
                let secret = &body[ULID_BYTES + 1..];
                let id = format!("{}{ulid}", $id_prefix)
                    .parse()
                    .map_err(|_| PlatformIdentityError::Unauthenticated)?;
                let decoded = URL_SAFE_NO_PAD
                    .decode(secret)
                    .map_err(|_| PlatformIdentityError::Unauthenticated)?;
                if decoded.len() != SECRET_BYTES || URL_SAFE_NO_PAD.encode(&decoded) != secret {
                    return Err(PlatformIdentityError::Unauthenticated);
                }
                Ok(Self { id, value: Zeroizing::new(value.to_owned()) })
            }
        }
    };
}

bearer!(
    /// Single-use setup or operator invitation code.
    InvitationCode,
    OperatorInvitationId,
    INVITATION_PREFIX,
    OperatorInvitationId::PREFIX
);
bearer!(
    /// Short-lived operator access token.
    AccessToken,
    OperatorSessionId,
    ACCESS_PREFIX,
    OperatorSessionId::PREFIX
);
bearer!(
    /// Rotating operator refresh token.
    RefreshToken,
    OperatorSessionId,
    REFRESH_PREFIX,
    OperatorSessionId::PREFIX
);

/// Newly generated invitation and its non-recoverable verifier.
#[derive(Debug)]
pub struct GeneratedInvitationCode {
    /// Complete code shown exactly once.
    pub code: InvitationCode,
    pub(crate) digest: PlatformDigest,
}

/// Newly generated access token and its non-recoverable verifier.
#[derive(Debug)]
pub struct GeneratedAccessToken {
    /// Complete access token.
    pub token: AccessToken,
    pub(crate) digest: PlatformDigest,
}

/// Newly generated refresh token and its non-recoverable verifier.
#[derive(Debug)]
pub struct GeneratedRefreshToken {
    /// Complete refresh token shown once to the CLI credential store.
    pub token: RefreshToken,
    pub(crate) digest: PlatformDigest,
}

/// Installation-specific verifier for every Platform Identity bearer class.
pub struct PlatformIdentityCrypto(Zeroizing<[u8; 32]>);

impl fmt::Debug for PlatformIdentityCrypto {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PlatformIdentityCrypto([REDACTED])")
    }
}

impl PlatformIdentityCrypto {
    /// Creates the verifier from a dedicated 256-bit installation pepper.
    #[must_use]
    pub fn new(pepper: [u8; 32]) -> Self {
        Self(Zeroizing::new(pepper))
    }

    /// Generates a single-use invitation code.
    ///
    /// # Errors
    ///
    /// Returns entropy unavailable if the operating-system CSPRNG fails.
    pub fn generate_invitation(
        &self,
        id: OperatorInvitationId,
    ) -> Result<GeneratedInvitationCode, PlatformIdentityError> {
        let id_text = id.as_ulid().to_string();
        let value = generate_value(INVITATION_PREFIX, &id_text)?;
        let code = InvitationCode { id, value };
        let digest = self.digest(INVITATION_DOMAIN, code.expose())?;
        Ok(GeneratedInvitationCode { code, digest })
    }

    /// Generates a short-lived access token.
    ///
    /// # Errors
    ///
    /// Returns entropy unavailable if the operating-system CSPRNG fails.
    pub fn generate_access(
        &self,
        id: OperatorSessionId,
    ) -> Result<GeneratedAccessToken, PlatformIdentityError> {
        let id_text = id.as_ulid().to_string();
        let value = generate_value(ACCESS_PREFIX, &id_text)?;
        let token = AccessToken { id, value };
        let digest = self.digest(ACCESS_DOMAIN, token.expose())?;
        Ok(GeneratedAccessToken { token, digest })
    }

    /// Generates a rotating refresh token.
    ///
    /// # Errors
    ///
    /// Returns entropy unavailable if the operating-system CSPRNG fails.
    pub fn generate_refresh(
        &self,
        id: OperatorSessionId,
    ) -> Result<GeneratedRefreshToken, PlatformIdentityError> {
        let id_text = id.as_ulid().to_string();
        let value = generate_value(REFRESH_PREFIX, &id_text)?;
        let token = RefreshToken { id, value };
        let digest = self.digest(REFRESH_DOMAIN, token.expose())?;
        Ok(GeneratedRefreshToken { token, digest })
    }

    pub(crate) fn invitation_digest(
        &self,
        code: &InvitationCode,
    ) -> Result<PlatformDigest, PlatformIdentityError> {
        self.digest(INVITATION_DOMAIN, code.expose())
    }

    pub(crate) fn access_digest(
        &self,
        token: &AccessToken,
    ) -> Result<PlatformDigest, PlatformIdentityError> {
        self.digest(ACCESS_DOMAIN, token.expose())
    }

    pub(crate) fn refresh_digest(
        &self,
        token: &RefreshToken,
    ) -> Result<PlatformDigest, PlatformIdentityError> {
        self.digest(REFRESH_DOMAIN, token.expose())
    }

    fn digest(&self, domain: &[u8], value: &str) -> Result<PlatformDigest, PlatformIdentityError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.0.as_ref())
            .map_err(|_| PlatformIdentityError::InvalidInput)?;
        mac.update(domain);
        mac.update(value.as_bytes());
        let bytes = mac.finalize().into_bytes();
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&bytes);
        Ok(PlatformDigest(digest))
    }
}

fn generate_value(prefix: &str, id: &str) -> Result<Zeroizing<String>, PlatformIdentityError> {
    let mut random = Zeroizing::new([0_u8; SECRET_BYTES]);
    getrandom::fill(random.as_mut()).map_err(|_| PlatformIdentityError::EntropyUnavailable)?;
    Ok(Zeroizing::new(format!(
        "{prefix}{id}.{}",
        URL_SAFE_NO_PAD.encode(random.as_ref())
    )))
}
