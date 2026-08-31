//! Canonical SHA-256 identifiers.

use std::{fmt, str::FromStr};

use sha2::{Digest, Sha256};
use thiserror::Error;

/// A complete SHA-256 digest with canonical lower-hex text representation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// Hashes complete bytes.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// Creates a digest from its exact 32-byte value.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Sha256Digest {
    type Err = ParseSha256DigestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64 {
            return Err(ParseSha256DigestError::Length);
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            bytes[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
        }
        let parsed = Self(bytes);
        if parsed.to_string() != value {
            return Err(ParseSha256DigestError::NonCanonical);
        }
        Ok(parsed)
    }
}

fn nibble(value: u8) -> Result<u8, ParseSha256DigestError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Err(ParseSha256DigestError::NonCanonical),
        _ => Err(ParseSha256DigestError::Character),
    }
}

/// Error parsing a SHA-256 digest.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParseSha256DigestError {
    /// Text is not exactly 64 bytes.
    #[error("SHA-256 text must contain exactly 64 characters")]
    Length,
    /// Text contains a non-hex character.
    #[error("SHA-256 text contains a non-hex character")]
    Character,
    /// Text uses a valid but non-canonical representation.
    #[error("SHA-256 text is not canonical lower hex")]
    NonCanonical,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_matches_known_vector_and_round_trips() -> Result<(), ParseSha256DigestError> {
        let digest = Sha256Digest::of(b"abc");
        assert_eq!(
            digest.to_string(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(digest.to_string().parse::<Sha256Digest>()?, digest);
        Ok(())
    }

    #[test]
    fn parser_rejects_ambiguous_or_malformed_text() {
        assert_eq!(
            "00".parse::<Sha256Digest>(),
            Err(ParseSha256DigestError::Length)
        );
        assert_eq!(
            "G000000000000000000000000000000000000000000000000000000000000000"
                .parse::<Sha256Digest>(),
            Err(ParseSha256DigestError::Character)
        );
        assert_eq!(
            "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"
                .parse::<Sha256Digest>(),
            Err(ParseSha256DigestError::NonCanonical)
        );
    }
}
