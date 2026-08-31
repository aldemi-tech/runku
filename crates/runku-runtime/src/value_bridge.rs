//! Typed `platform-js-1` bridge for canonical Runku values.

use std::collections::BTreeMap;

use runku_value::{CanonicalValue, FiniteF64, TimestampMicros, TypedId, encode_stored_value};
use serde::{Deserialize, Serialize};

use crate::RuntimeError;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub(crate) enum WireValue {
    Null,
    Boolean(bool),
    Int64(String),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(String),
    TypedId(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

pub(crate) fn to_wire(value: &CanonicalValue) -> WireValue {
    match value {
        CanonicalValue::Null => WireValue::Null,
        CanonicalValue::Boolean(value) => WireValue::Boolean(*value),
        CanonicalValue::Int64(value) => WireValue::Int64(value.to_string()),
        CanonicalValue::Float64(value) => WireValue::Float64(value.get()),
        CanonicalValue::String(value) => WireValue::String(value.clone()),
        CanonicalValue::Bytes(value) => WireValue::Bytes(value.clone()),
        CanonicalValue::Timestamp(value) => WireValue::Timestamp(value.get().to_string()),
        CanonicalValue::TypedId(value) => WireValue::TypedId(value.to_string()),
        CanonicalValue::Array(values) => WireValue::Array(values.iter().map(to_wire).collect()),
        CanonicalValue::Object(values) => WireValue::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), to_wire(value)))
                .collect(),
        ),
    }
}

pub(crate) fn from_wire(value: WireValue) -> Result<CanonicalValue, RuntimeError> {
    let value = match value {
        WireValue::Null => CanonicalValue::Null,
        WireValue::Boolean(value) => CanonicalValue::Boolean(value),
        WireValue::Int64(value) => CanonicalValue::Int64(
            value
                .parse::<i64>()
                .map_err(|_| RuntimeError::InvalidResult)?,
        ),
        WireValue::Float64(value) => {
            CanonicalValue::Float64(FiniteF64::new(value).map_err(|_| RuntimeError::InvalidResult)?)
        }
        WireValue::String(value) => CanonicalValue::String(value),
        WireValue::Bytes(value) => CanonicalValue::Bytes(value),
        WireValue::Timestamp(value) => CanonicalValue::Timestamp(TimestampMicros::new(
            value
                .parse::<i64>()
                .map_err(|_| RuntimeError::InvalidResult)?,
        )),
        WireValue::TypedId(value) => CanonicalValue::TypedId(
            value
                .parse::<TypedId>()
                .map_err(|_| RuntimeError::InvalidResult)?,
        ),
        WireValue::Array(values) => CanonicalValue::Array(
            values
                .into_iter()
                .map(from_wire)
                .collect::<Result<_, _>>()?,
        ),
        WireValue::Object(values) => CanonicalValue::Object(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, from_wire(value)?)))
                .collect::<Result<BTreeMap<_, _>, RuntimeError>>()?,
        ),
    };
    encode_stored_value(&value).map_err(|_| RuntimeError::InvalidResult)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use proptest::{collection, prelude::*};

    use super::*;

    fn arbitrary_value() -> impl Strategy<Value = CanonicalValue> {
        let scalar = prop_oneof![
            Just(CanonicalValue::Null),
            any::<bool>().prop_map(CanonicalValue::Boolean),
            any::<i64>().prop_map(CanonicalValue::Int64),
            any::<f64>().prop_filter_map("finite", |value| {
                FiniteF64::new(value).ok().map(CanonicalValue::Float64)
            }),
            ".{0,64}".prop_map(CanonicalValue::String),
            collection::vec(any::<u8>(), 0..64).prop_map(CanonicalValue::Bytes),
            any::<i64>().prop_map(|value| CanonicalValue::Timestamp(TimestampMicros::new(value))),
        ];
        scalar.prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                collection::vec(inner.clone(), 0..8).prop_map(CanonicalValue::Array),
                collection::btree_map("[a-z]{1,8}", inner, 0..8).prop_map(CanonicalValue::Object),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn rust_wire_projection_round_trips(value in arbitrary_value()) {
            let decoded = from_wire(to_wire(&value));
            prop_assert_eq!(decoded, Ok(value));
        }
    }
}
