//! Cross-language golden vectors for Stored Value v1 and Index Key v1.

use std::{collections::BTreeMap, error::Error};

use runku_value::{
    CanonicalValue, FiniteF64, IndexKey, IndexValue, TimestampMicros, TypedId, decode_stored_value,
    encode_stored_value,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct StoredVectors {
    format_version: u8,
    valid: Vec<StoredVector>,
    invalid_hex: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct StoredVector {
    name: String,
    value: VectorValue,
    encoded_hex: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum VectorValue {
    Null,
    Boolean { value: bool },
    Int64 { value: String },
    Float64 { value: f64 },
    String { value: String },
    Bytes { hex: String },
    Timestamp { microseconds: String },
    TypedId { value: String },
    Array { items: Vec<Self> },
    Object { fields: BTreeMap<String, Self> },
}

impl VectorValue {
    fn into_canonical(self) -> Result<CanonicalValue, Box<dyn Error>> {
        match self {
            Self::Null => Ok(CanonicalValue::Null),
            Self::Boolean { value } => Ok(CanonicalValue::Boolean(value)),
            Self::Int64 { value } => Ok(CanonicalValue::Int64(value.parse()?)),
            Self::Float64 { value } => Ok(CanonicalValue::Float64(FiniteF64::new(value)?)),
            Self::String { value } => Ok(CanonicalValue::String(value)),
            Self::Bytes { hex } => Ok(CanonicalValue::Bytes(decode_hex(&hex)?)),
            Self::Timestamp { microseconds } => Ok(CanonicalValue::Timestamp(
                TimestampMicros::new(microseconds.parse()?),
            )),
            Self::TypedId { value } => Ok(CanonicalValue::TypedId(value.parse()?)),
            Self::Array { items } => items
                .into_iter()
                .map(Self::into_canonical)
                .collect::<Result<Vec<_>, _>>()
                .map(CanonicalValue::Array),
            Self::Object { fields } => fields
                .into_iter()
                .map(|(key, value)| Ok((key, value.into_canonical()?)))
                .collect::<Result<BTreeMap<_, _>, Box<dyn Error>>>()
                .map(CanonicalValue::Object),
        }
    }

    fn into_index(self) -> Result<IndexValue, Box<dyn Error>> {
        match self {
            Self::Null => Ok(IndexValue::Null),
            Self::Boolean { value } => Ok(IndexValue::Boolean(value)),
            Self::Int64 { value } => Ok(IndexValue::Int64(value.parse()?)),
            Self::Float64 { value } => Ok(IndexValue::Float64(FiniteF64::new(value)?)),
            Self::String { value } => Ok(IndexValue::String(value)),
            Self::Bytes { hex } => Ok(IndexValue::Bytes(decode_hex(&hex)?)),
            Self::Timestamp { microseconds } => Ok(IndexValue::Timestamp(TimestampMicros::new(
                microseconds.parse()?,
            ))),
            Self::TypedId { value } => Ok(IndexValue::TypedId(value.parse::<TypedId>()?)),
            Self::Array { .. } | Self::Object { .. } => {
                Err("golden index vector contains a non-indexable type".into())
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct IndexVectors {
    format_version: u8,
    valid: Vec<IndexVector>,
    invalid_hex: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct IndexVector {
    name: String,
    values: Vec<VectorValue>,
    encoded_hex: String,
}

#[test]
fn stored_value_vectors_are_normative() -> Result<(), Box<dyn Error>> {
    let vectors: StoredVectors = serde_json::from_str(include_str!(
        "../../../protocol/v1/stored-value-vectors.json"
    ))?;
    assert_eq!(vectors.format_version, 1);
    for vector in vectors.valid {
        let value = vector.value.into_canonical()?;
        let expected = decode_hex(&vector.encoded_hex)?;
        assert_eq!(encode_stored_value(&value)?, expected, "{}", vector.name);
        assert_eq!(decode_stored_value(&expected)?, value, "{}", vector.name);
    }
    for encoded in vectors.invalid_hex {
        let bytes = decode_hex(&encoded)?;
        assert!(decode_stored_value(&bytes).is_err(), "accepted {encoded}");
    }
    Ok(())
}

#[test]
fn index_key_vectors_are_normative() -> Result<(), Box<dyn Error>> {
    let vectors: IndexVectors =
        serde_json::from_str(include_str!("../../../protocol/v1/index-key-vectors.json"))?;
    assert_eq!(vectors.format_version, IndexKey::FORMAT_VERSION);
    for vector in vectors.valid {
        let values = vector
            .values
            .into_iter()
            .map(VectorValue::into_index)
            .collect::<Result<Vec<_>, _>>()?;
        let expected = decode_hex(&vector.encoded_hex)?;
        assert_eq!(
            IndexKey::encode(&values)?.as_bytes(),
            expected,
            "{}",
            vector.name
        );
        assert_eq!(
            IndexKey::decode(&expected)?.components(),
            values,
            "{}",
            vector.name
        );
    }
    for encoded in vectors.invalid_hex {
        let bytes = decode_hex(&encoded)?;
        assert!(IndexKey::decode(&bytes).is_err(), "accepted {encoded}");
    }
    Ok(())
}

fn decode_hex(value: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    if !value.len().is_multiple_of(2) {
        return Err("hex string has odd length".into());
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(value: u8) -> Result<u8, Box<dyn Error>> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("hex string contains an invalid character".into()),
    }
}
