//! In-memory canonical value model.

use std::{cmp::Ordering, collections::BTreeMap, fmt, str::FromStr};

use thiserror::Error;
use ulid::Ulid;

const MAX_ID_KIND_BYTES: usize = 16;
const MAX_TYPED_ID_BYTES: usize = MAX_ID_KIND_BYTES + 1 + 26;

/// A finite IEEE-754 binary64 value with one canonical zero representation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FiniteF64(u64);

impl FiniteF64 {
    /// Creates a finite value and normalizes negative zero to positive zero.
    ///
    /// # Errors
    ///
    /// Returns [`NonFiniteFloatError`] for NaN or either infinity.
    pub fn new(value: f64) -> Result<Self, NonFiniteFloatError> {
        if !value.is_finite() {
            return Err(NonFiniteFloatError);
        }
        let normalized = if value == 0.0 { 0.0 } else { value };
        Ok(Self(normalized.to_bits()))
    }

    /// Reconstructs the finite floating-point number.
    #[must_use]
    pub const fn get(self) -> f64 {
        f64::from_bits(self.0)
    }

    /// Returns canonical IEEE-754 bits.
    #[must_use]
    pub const fn to_bits(self) -> u64 {
        self.0
    }
}

impl PartialOrd for FiniteF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FiniteF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get().total_cmp(&other.get())
    }
}

/// Error returned when NaN or infinity is used as a canonical value.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("canonical float must be finite")]
pub struct NonFiniteFloatError;

impl NonFiniteFloatError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        "VALUE_FLOAT_NON_FINITE"
    }

    /// The same bits always fail, so retrying unchanged cannot succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

/// UTC microseconds since Unix epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimestampMicros(i64);

impl TimestampMicros {
    /// Creates a timestamp from signed Unix-epoch microseconds.
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns signed Unix-epoch microseconds.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// A canonical `<kind>_<ULID>` value.
///
/// Typed IDs are opaque data. Parsing one does not authorize access to any resource.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TypedId {
    kind: String,
    ulid: Ulid,
}

impl TypedId {
    /// Maximum encoded length accepted by v1.
    pub const MAX_ENCODED_BYTES: usize = MAX_TYPED_ID_BYTES;

    /// Creates an ID from a resource kind and ULID.
    ///
    /// # Errors
    ///
    /// Returns [`ParseTypedIdError`] if the kind is empty, too long, or non-canonical.
    pub fn new(kind: &str, ulid: Ulid) -> Result<Self, ParseTypedIdError> {
        validate_kind(kind)?;
        Ok(Self {
            kind: kind.to_owned(),
            ulid,
        })
    }

    /// Returns the resource kind without the separator.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the ULID payload.
    #[must_use]
    pub const fn ulid(&self) -> Ulid {
        self.ulid
    }
}

impl fmt::Display for TypedId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}_{}", self.kind, self.ulid)
    }
}

impl FromStr for TypedId {
    type Err = ParseTypedIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > MAX_TYPED_ID_BYTES {
            return Err(ParseTypedIdError::TooLong);
        }
        let (kind, payload) = value
            .split_once('_')
            .ok_or(ParseTypedIdError::MissingSeparator)?;
        validate_kind(kind)?;
        let ulid = payload
            .parse::<Ulid>()
            .map_err(|_| ParseTypedIdError::InvalidUlid)?;
        if ulid.to_string() != payload {
            return Err(ParseTypedIdError::InvalidUlid);
        }
        Ok(Self {
            kind: kind.to_owned(),
            ulid,
        })
    }
}

fn validate_kind(kind: &str) -> Result<(), ParseTypedIdError> {
    if kind.is_empty() {
        return Err(ParseTypedIdError::EmptyKind);
    }
    if kind.len() > MAX_ID_KIND_BYTES {
        return Err(ParseTypedIdError::TooLong);
    }
    if !kind
        .bytes()
        .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    {
        return Err(ParseTypedIdError::InvalidKind);
    }
    Ok(())
}

/// Error returned when a generic typed ID is not canonical.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParseTypedIdError {
    /// The `_` separator is absent.
    #[error("typed ID must use '<kind>_<ULID>'")]
    MissingSeparator,
    /// The kind before `_` is empty.
    #[error("typed ID kind must not be empty")]
    EmptyKind,
    /// The ID or its kind exceeds the v1 limit.
    #[error("typed ID exceeds the v1 length limit")]
    TooLong,
    /// The kind contains characters other than lowercase ASCII or digits.
    #[error("typed ID kind is not canonical")]
    InvalidKind,
    /// The payload is not a canonical uppercase ULID.
    #[error("typed ID payload is not a canonical ULID")]
    InvalidUlid,
}

impl ParseTypedIdError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingSeparator => "TYPED_ID_FORMAT_INVALID",
            Self::EmptyKind => "TYPED_ID_KIND_EMPTY",
            Self::TooLong => "TYPED_ID_TOO_LONG",
            Self::InvalidKind => "TYPED_ID_KIND_INVALID",
            Self::InvalidUlid => "TYPED_ID_ULID_INVALID",
        }
    }

    /// Parse errors are deterministic and not retryable unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

/// Logical value supported by Runku's v1 persistence contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalValue {
    /// Absence represented explicitly.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Finite 64-bit floating-point number.
    Float64(FiniteF64),
    /// Unicode string encoded as UTF-8 on disk.
    String(String),
    /// Opaque bytes, distinct from string.
    Bytes(Vec<u8>),
    /// UTC Unix-epoch microseconds.
    Timestamp(TimestampMicros),
    /// Opaque typed resource identifier.
    TypedId(TypedId),
    /// Ordered sequence of values.
    Array(Vec<Self>),
    /// Map ordered canonically by UTF-8 key bytes.
    Object(BTreeMap<String, Self>),
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    #[test]
    fn float_rejects_non_finite_and_normalizes_zero() -> Result<(), Box<dyn Error>> {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(FiniteF64::new(value), Err(NonFiniteFloatError));
        }
        assert_eq!(FiniteF64::new(-0.0)?, FiniteF64::new(0.0)?);
        assert_eq!(FiniteF64::new(-0.0)?.to_bits(), 0);
        Ok(())
    }

    #[test]
    fn typed_id_is_strict_and_round_trips() -> Result<(), Box<dyn Error>> {
        let wire = format!("document_{ULID}");
        let id: TypedId = wire.parse()?;
        assert_eq!(id.kind(), "document");
        assert_eq!(id.to_string(), wire);

        for invalid in [
            ULID.to_owned(),
            format!("_{ULID}"),
            format!("Document_{ULID}"),
            format!("document_{}", ULID.to_ascii_lowercase()),
            format!("kind-with-dash_{ULID}"),
        ] {
            assert!(invalid.parse::<TypedId>().is_err(), "accepted {invalid}");
        }
        Ok(())
    }
}
