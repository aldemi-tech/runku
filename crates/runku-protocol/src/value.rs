//! Lossless tagged JSON projection of Canonical Value v1.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use runku_value::{CanonicalValue, FiniteF64, TimestampMicros, TypedId, encode_stored_value};
use serde::{Deserialize, Serialize};

use crate::ProtocolError;

const MAX_DEPTH: usize = 64;
const MAX_CONTAINER_ITEMS: usize = 10_000;

/// Lossless tagged JSON representation of one Canonical Value v1.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireValueV1 {
    /// Null.
    Null {},
    /// Boolean.
    Boolean {
        /// Exact value.
        value: bool,
    },
    /// Signed 64-bit integer encoded as canonical decimal text.
    Int64 {
        /// Canonical decimal value.
        value: String,
    },
    /// Finite IEEE-754 float encoded as 16 lowercase hexadecimal bits.
    Float64 {
        /// Canonical bit representation.
        value: String,
    },
    /// UTF-8 string.
    String {
        /// Exact string value.
        value: String,
    },
    /// Bytes encoded as unpadded URL-safe Base64.
    Bytes {
        /// Canonical encoded bytes.
        value: String,
    },
    /// Timestamp in microseconds encoded as canonical signed decimal text.
    Timestamp {
        /// Canonical timestamp value.
        value: String,
    },
    /// Canonical typed Runku identifier.
    TypedId {
        /// Typed identifier text.
        value: String,
    },
    /// Ordered array.
    Array {
        /// Array elements.
        value: Vec<Self>,
    },
    /// Object encoded as strictly UTF-8-byte-sorted entries.
    Object {
        /// Canonically ordered object entries.
        value: Vec<WireObjectEntryV1>,
    },
}

/// One canonical object entry in [`WireValueV1::Object`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WireObjectEntryV1 {
    /// Object key.
    pub key: String,
    /// Object value.
    pub value: WireValueV1,
}

impl WireValueV1 {
    /// Converts a canonical value into its lossless public JSON projection.
    ///
    /// # Errors
    ///
    /// Rejects values outside protocol structural limits.
    pub fn from_canonical(value: &CanonicalValue) -> Result<Self, ProtocolError> {
        encode_stored_value(value).map_err(|_| ProtocolError::LimitExceeded)?;
        Self::from_canonical_inner(value, 0)
    }

    fn from_canonical_inner(value: &CanonicalValue, depth: usize) -> Result<Self, ProtocolError> {
        if depth > MAX_DEPTH {
            return Err(ProtocolError::LimitExceeded);
        }
        Ok(match value {
            CanonicalValue::Null => Self::Null {},
            CanonicalValue::Boolean(value) => Self::Boolean { value: *value },
            CanonicalValue::Int64(value) => Self::Int64 {
                value: value.to_string(),
            },
            CanonicalValue::Float64(value) => Self::Float64 {
                value: format!("{:016x}", value.to_bits()),
            },
            CanonicalValue::String(value) => Self::String {
                value: value.clone(),
            },
            CanonicalValue::Bytes(value) => Self::Bytes {
                value: URL_SAFE_NO_PAD.encode(value),
            },
            CanonicalValue::Timestamp(value) => Self::Timestamp {
                value: value.get().to_string(),
            },
            CanonicalValue::TypedId(value) => Self::TypedId {
                value: value.to_string(),
            },
            CanonicalValue::Array(values) => Self::Array {
                value: values
                    .iter()
                    .map(|value| Self::from_canonical_inner(value, depth + 1))
                    .collect::<Result<_, _>>()?,
            },
            CanonicalValue::Object(values) => Self::Object {
                value: values
                    .iter()
                    .map(|(key, value)| {
                        Ok(WireObjectEntryV1 {
                            key: key.clone(),
                            value: Self::from_canonical_inner(value, depth + 1)?,
                        })
                    })
                    .collect::<Result<_, ProtocolError>>()?,
            },
        })
    }

    /// Validates and converts the tagged projection into a canonical value.
    ///
    /// # Errors
    ///
    /// Rejects malformed scalar encodings, unsorted/duplicate keys and structural limits.
    pub fn into_canonical(self) -> Result<CanonicalValue, ProtocolError> {
        let value = self.into_canonical_inner(0)?;
        encode_stored_value(&value).map_err(|_| ProtocolError::LimitExceeded)?;
        Ok(value)
    }

    fn into_canonical_inner(self, depth: usize) -> Result<CanonicalValue, ProtocolError> {
        if depth > MAX_DEPTH {
            return Err(ProtocolError::LimitExceeded);
        }
        Ok(match self {
            Self::Null {} => CanonicalValue::Null,
            Self::Boolean { value } => CanonicalValue::Boolean(value),
            Self::Int64 { value } => CanonicalValue::Int64(parse_canonical_i64(&value)?),
            Self::Float64 { value } => {
                if value.len() != 16
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    return Err(ProtocolError::InvalidRequest);
                }
                let bits =
                    u64::from_str_radix(&value, 16).map_err(|_| ProtocolError::InvalidRequest)?;
                let finite = FiniteF64::new(f64::from_bits(bits))
                    .map_err(|_| ProtocolError::InvalidRequest)?;
                if finite.to_bits() != bits {
                    return Err(ProtocolError::InvalidRequest);
                }
                CanonicalValue::Float64(finite)
            }
            Self::String { value } => CanonicalValue::String(value),
            Self::Bytes { value } => {
                let bytes = URL_SAFE_NO_PAD
                    .decode(&value)
                    .map_err(|_| ProtocolError::InvalidRequest)?;
                if URL_SAFE_NO_PAD.encode(&bytes) != value {
                    return Err(ProtocolError::InvalidRequest);
                }
                CanonicalValue::Bytes(bytes)
            }
            Self::Timestamp { value } => {
                CanonicalValue::Timestamp(TimestampMicros::new(parse_canonical_i64(&value)?))
            }
            Self::TypedId { value } => CanonicalValue::TypedId(
                value
                    .parse::<TypedId>()
                    .map_err(|_| ProtocolError::InvalidRequest)?,
            ),
            Self::Array { value } => {
                if value.len() > MAX_CONTAINER_ITEMS {
                    return Err(ProtocolError::LimitExceeded);
                }
                CanonicalValue::Array(
                    value
                        .into_iter()
                        .map(|value| value.into_canonical_inner(depth + 1))
                        .collect::<Result<_, _>>()?,
                )
            }
            Self::Object { value } => {
                if value.len() > MAX_CONTAINER_ITEMS {
                    return Err(ProtocolError::LimitExceeded);
                }
                let mut object = BTreeMap::new();
                let mut previous: Option<String> = None;
                for entry in value {
                    if previous.as_ref().is_some_and(|key| key >= &entry.key) {
                        return Err(ProtocolError::InvalidRequest);
                    }
                    previous = Some(entry.key.clone());
                    object.insert(entry.key, entry.value.into_canonical_inner(depth + 1)?);
                }
                CanonicalValue::Object(object)
            }
        })
    }
}

fn parse_canonical_i64(value: &str) -> Result<i64, ProtocolError> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| ProtocolError::InvalidRequest)?;
    if parsed.to_string() != value {
        return Err(ProtocolError::InvalidRequest);
    }
    Ok(parsed)
}
