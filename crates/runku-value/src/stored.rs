//! Stored Value v1 binary codec.

use std::{collections::BTreeMap, str};

use thiserror::Error;

use crate::{CanonicalValue, FiniteF64, TimestampMicros, TypedId};

const MAGIC: [u8; 2] = *b"RV";
/// Stored Value format emitted by this crate.
pub const STORED_VALUE_FORMAT_VERSION: u8 = 1;
/// Maximum encoded Stored Value size accepted by v1.
pub const STORED_VALUE_MAX_BYTES: usize = 1024 * 1024;
const MAX_DEPTH: usize = 64;
const MAX_CONTAINER_ITEMS: usize = 10_000;
const MAX_OBJECT_KEY_BYTES: usize = 256;

const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_INT64: u8 = 0x10;
const TAG_FLOAT64: u8 = 0x11;
const TAG_STRING: u8 = 0x20;
const TAG_BYTES: u8 = 0x21;
const TAG_TIMESTAMP: u8 = 0x22;
const TAG_TYPED_ID: u8 = 0x23;
const TAG_ARRAY: u8 = 0x30;
const TAG_OBJECT: u8 = 0x31;

/// Encodes one canonical value using Stored Value v1.
///
/// # Errors
///
/// Returns [`StoredValueError`] when the value exceeds a v1 size, depth, key, or container limit.
pub fn encode_stored_value(value: &CanonicalValue) -> Result<Vec<u8>, StoredValueError> {
    let body_len = encoded_body_len(value, 0)?;
    let total_len = MAGIC
        .len()
        .checked_add(1)
        .and_then(|length| length.checked_add(body_len))
        .ok_or(StoredValueError::ValueTooLarge)?;
    if total_len > STORED_VALUE_MAX_BYTES {
        return Err(StoredValueError::ValueTooLarge);
    }

    let mut output = Vec::with_capacity(total_len);
    output.extend_from_slice(&MAGIC);
    output.push(STORED_VALUE_FORMAT_VERSION);
    encode_body(value, &mut output)?;
    Ok(output)
}

/// Decodes exactly one Stored Value v1 value.
///
/// # Errors
///
/// Returns [`StoredValueError`] for malformed, non-canonical, unsupported, truncated, oversized,
/// or trailing input.
pub fn decode_stored_value(input: &[u8]) -> Result<CanonicalValue, StoredValueError> {
    if input.len() > STORED_VALUE_MAX_BYTES {
        return Err(StoredValueError::ValueTooLarge);
    }
    let mut cursor = Cursor::new(input);
    if cursor.take(2)? != MAGIC {
        return Err(StoredValueError::InvalidMagic);
    }
    let version = cursor.byte()?;
    if version != STORED_VALUE_FORMAT_VERSION {
        return Err(StoredValueError::UnsupportedVersion);
    }
    let value = decode_body(&mut cursor, 0)?;
    if !cursor.is_empty() {
        return Err(StoredValueError::TrailingBytes);
    }
    Ok(value)
}

fn encoded_body_len(value: &CanonicalValue, depth: usize) -> Result<usize, StoredValueError> {
    if depth > MAX_DEPTH {
        return Err(StoredValueError::DepthExceeded);
    }
    let length = match value {
        CanonicalValue::Null | CanonicalValue::Boolean(_) => 1,
        CanonicalValue::Int64(_) | CanonicalValue::Float64(_) | CanonicalValue::Timestamp(_) => 9,
        CanonicalValue::String(value) => {
            ensure_variable_length(value.len())?;
            checked_sum(5, value.len())?
        }
        CanonicalValue::Bytes(value) => {
            ensure_variable_length(value.len())?;
            checked_sum(5, value.len())?
        }
        CanonicalValue::TypedId(value) => {
            let length = value.to_string().len();
            if length > TypedId::MAX_ENCODED_BYTES {
                return Err(StoredValueError::TypedIdInvalid);
            }
            checked_sum(3, length)?
        }
        CanonicalValue::Array(values) => {
            ensure_container_count(values.len())?;
            let mut length = 5_usize;
            for item in values {
                length = checked_sum(length, encoded_body_len(item, depth + 1)?)?;
            }
            length
        }
        CanonicalValue::Object(fields) => {
            ensure_container_count(fields.len())?;
            let mut length = 5_usize;
            for (key, item) in fields {
                if key.len() > MAX_OBJECT_KEY_BYTES || key.len() > usize::from(u16::MAX) {
                    return Err(StoredValueError::ObjectKeyTooLong);
                }
                length = checked_sum(length, 2)?;
                length = checked_sum(length, key.len())?;
                length = checked_sum(length, encoded_body_len(item, depth + 1)?)?;
            }
            length
        }
    };
    if length > STORED_VALUE_MAX_BYTES {
        return Err(StoredValueError::ValueTooLarge);
    }
    Ok(length)
}

fn checked_sum(left: usize, right: usize) -> Result<usize, StoredValueError> {
    left.checked_add(right)
        .filter(|length| *length <= STORED_VALUE_MAX_BYTES)
        .ok_or(StoredValueError::ValueTooLarge)
}

fn ensure_variable_length(length: usize) -> Result<(), StoredValueError> {
    if length > STORED_VALUE_MAX_BYTES || length > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
        return Err(StoredValueError::ValueTooLarge);
    }
    Ok(())
}

fn ensure_container_count(count: usize) -> Result<(), StoredValueError> {
    if count > MAX_CONTAINER_ITEMS {
        return Err(StoredValueError::ContainerTooLarge);
    }
    Ok(())
}

fn encode_body(value: &CanonicalValue, output: &mut Vec<u8>) -> Result<(), StoredValueError> {
    match value {
        CanonicalValue::Null => output.push(TAG_NULL),
        CanonicalValue::Boolean(false) => output.push(TAG_FALSE),
        CanonicalValue::Boolean(true) => output.push(TAG_TRUE),
        CanonicalValue::Int64(value) => {
            output.push(TAG_INT64);
            output.extend_from_slice(&value.to_be_bytes());
        }
        CanonicalValue::Float64(value) => {
            output.push(TAG_FLOAT64);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        CanonicalValue::String(value) => {
            output.push(TAG_STRING);
            push_u32(output, value.len())?;
            output.extend_from_slice(value.as_bytes());
        }
        CanonicalValue::Bytes(value) => {
            output.push(TAG_BYTES);
            push_u32(output, value.len())?;
            output.extend_from_slice(value);
        }
        CanonicalValue::Timestamp(value) => {
            output.push(TAG_TIMESTAMP);
            output.extend_from_slice(&value.get().to_be_bytes());
        }
        CanonicalValue::TypedId(value) => {
            output.push(TAG_TYPED_ID);
            let wire = value.to_string();
            push_u16(output, wire.len())?;
            output.extend_from_slice(wire.as_bytes());
        }
        CanonicalValue::Array(values) => {
            output.push(TAG_ARRAY);
            push_u32(output, values.len())?;
            for item in values {
                encode_body(item, output)?;
            }
        }
        CanonicalValue::Object(fields) => {
            output.push(TAG_OBJECT);
            push_u32(output, fields.len())?;
            for (key, item) in fields {
                push_u16(output, key.len())?;
                output.extend_from_slice(key.as_bytes());
                encode_body(item, output)?;
            }
        }
    }
    Ok(())
}

fn push_u16(output: &mut Vec<u8>, value: usize) -> Result<(), StoredValueError> {
    let value = u16::try_from(value).map_err(|_| StoredValueError::ValueTooLarge)?;
    output.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn push_u32(output: &mut Vec<u8>, value: usize) -> Result<(), StoredValueError> {
    let value = u32::try_from(value).map_err(|_| StoredValueError::ValueTooLarge)?;
    output.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn decode_body(cursor: &mut Cursor<'_>, depth: usize) -> Result<CanonicalValue, StoredValueError> {
    if depth > MAX_DEPTH {
        return Err(StoredValueError::DepthExceeded);
    }
    match cursor.byte()? {
        TAG_NULL => Ok(CanonicalValue::Null),
        TAG_FALSE => Ok(CanonicalValue::Boolean(false)),
        TAG_TRUE => Ok(CanonicalValue::Boolean(true)),
        TAG_INT64 => Ok(CanonicalValue::Int64(i64::from_be_bytes(cursor.array()?))),
        TAG_FLOAT64 => {
            let bits = u64::from_be_bytes(cursor.array()?);
            let value = FiniteF64::new(f64::from_bits(bits))
                .map_err(|_| StoredValueError::FloatNonFinite)?;
            if value.to_bits() != bits {
                return Err(StoredValueError::NonCanonicalFloat);
            }
            Ok(CanonicalValue::Float64(value))
        }
        TAG_STRING => {
            let length = cursor.u32_length()?;
            ensure_variable_length(length)?;
            let bytes = cursor.take(length)?;
            let value = str::from_utf8(bytes).map_err(|_| StoredValueError::InvalidUtf8)?;
            Ok(CanonicalValue::String(value.to_owned()))
        }
        TAG_BYTES => {
            let length = cursor.u32_length()?;
            ensure_variable_length(length)?;
            Ok(CanonicalValue::Bytes(cursor.take(length)?.to_vec()))
        }
        TAG_TIMESTAMP => Ok(CanonicalValue::Timestamp(TimestampMicros::new(
            i64::from_be_bytes(cursor.array()?),
        ))),
        TAG_TYPED_ID => {
            let length = cursor.u16_length()?;
            if length > TypedId::MAX_ENCODED_BYTES {
                return Err(StoredValueError::TypedIdInvalid);
            }
            let wire = str::from_utf8(cursor.take(length)?)
                .map_err(|_| StoredValueError::TypedIdInvalid)?;
            let id = wire
                .parse::<TypedId>()
                .map_err(|_| StoredValueError::TypedIdInvalid)?;
            Ok(CanonicalValue::TypedId(id))
        }
        TAG_ARRAY => {
            let count = cursor.u32_length()?;
            ensure_container_count(count)?;
            if count > cursor.remaining() {
                return Err(StoredValueError::Truncated);
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_body(cursor, depth + 1)?);
            }
            Ok(CanonicalValue::Array(values))
        }
        TAG_OBJECT => {
            let count = cursor.u32_length()?;
            ensure_container_count(count)?;
            if count > cursor.remaining() / 3 {
                return Err(StoredValueError::Truncated);
            }
            let mut fields = BTreeMap::new();
            let mut previous: Option<String> = None;
            for _ in 0..count {
                let key_length = cursor.u16_length()?;
                if key_length > MAX_OBJECT_KEY_BYTES {
                    return Err(StoredValueError::ObjectKeyTooLong);
                }
                let key = str::from_utf8(cursor.take(key_length)?)
                    .map_err(|_| StoredValueError::InvalidUtf8)?
                    .to_owned();
                if previous.as_ref().is_some_and(|last| last >= &key) {
                    return Err(StoredValueError::ObjectKeysNotSorted);
                }
                let value = decode_body(cursor, depth + 1)?;
                previous = Some(key.clone());
                fields.insert(key, value);
            }
            Ok(CanonicalValue::Object(fields))
        }
        _ => Err(StoredValueError::UnknownTag),
    }
}

struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], StoredValueError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(StoredValueError::Truncated)?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(StoredValueError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, StoredValueError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], StoredValueError> {
        self.take(N)?
            .try_into()
            .map_err(|_| StoredValueError::Truncated)
    }

    fn u16_length(&mut self) -> Result<usize, StoredValueError> {
        Ok(usize::from(u16::from_be_bytes(self.array()?)))
    }

    fn u32_length(&mut self) -> Result<usize, StoredValueError> {
        usize::try_from(u32::from_be_bytes(self.array()?))
            .map_err(|_| StoredValueError::ValueTooLarge)
    }

    const fn remaining(&self) -> usize {
        self.input.len() - self.offset
    }

    const fn is_empty(&self) -> bool {
        self.remaining() == 0
    }
}

/// Error returned by the Stored Value v1 codec.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StoredValueError {
    /// Input does not start with the Stored Value magic.
    #[error("stored value magic is invalid")]
    InvalidMagic,
    /// The encoded version is not supported by this reader.
    #[error("stored value version is unsupported")]
    UnsupportedVersion,
    /// A value tag is not defined by v1.
    #[error("stored value tag is unsupported")]
    UnknownTag,
    /// Input ends before the declared value is complete.
    #[error("stored value is truncated")]
    Truncated,
    /// Bytes remain after the single root value.
    #[error("stored value has trailing bytes")]
    TrailingBytes,
    /// A string or object key is not valid UTF-8.
    #[error("stored value contains invalid UTF-8")]
    InvalidUtf8,
    /// Encoded bytes exceed the v1 limit.
    #[error("stored value exceeds the v1 size limit")]
    ValueTooLarge,
    /// Array/object nesting exceeds the v1 limit.
    #[error("stored value exceeds the v1 depth limit")]
    DepthExceeded,
    /// An array or object has too many members.
    #[error("stored value container exceeds the v1 item limit")]
    ContainerTooLarge,
    /// An object key exceeds the v1 byte limit.
    #[error("stored value object key exceeds the v1 limit")]
    ObjectKeyTooLong,
    /// Object keys are duplicated or not strictly sorted.
    #[error("stored value object keys are not canonical")]
    ObjectKeysNotSorted,
    /// Float payload is NaN or infinite.
    #[error("stored value float is not finite")]
    FloatNonFinite,
    /// Float uses a valid but non-canonical bit representation such as negative zero.
    #[error("stored value float representation is not canonical")]
    NonCanonicalFloat,
    /// Typed ID is malformed or exceeds its limit.
    #[error("stored value typed ID is invalid")]
    TypedIdInvalid,
}

impl StoredValueError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidMagic => "STORED_VALUE_MAGIC_INVALID",
            Self::UnsupportedVersion => "STORED_VALUE_VERSION_UNSUPPORTED",
            Self::UnknownTag => "STORED_VALUE_TAG_UNSUPPORTED",
            Self::Truncated => "STORED_VALUE_TRUNCATED",
            Self::TrailingBytes => "STORED_VALUE_TRAILING_BYTES",
            Self::InvalidUtf8 => "STORED_VALUE_UTF8_INVALID",
            Self::ValueTooLarge => "STORED_VALUE_TOO_LARGE",
            Self::DepthExceeded => "STORED_VALUE_DEPTH_EXCEEDED",
            Self::ContainerTooLarge => "STORED_VALUE_CONTAINER_TOO_LARGE",
            Self::ObjectKeyTooLong => "STORED_VALUE_OBJECT_KEY_TOO_LONG",
            Self::ObjectKeysNotSorted => "STORED_VALUE_OBJECT_ORDER_INVALID",
            Self::FloatNonFinite => "STORED_VALUE_FLOAT_NON_FINITE",
            Self::NonCanonicalFloat => "STORED_VALUE_FLOAT_NON_CANONICAL",
            Self::TypedIdInvalid => "STORED_VALUE_TYPED_ID_INVALID",
        }
    }

    /// Codec errors are deterministic and not retryable unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, error::Error};

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn nested_value_round_trips() -> Result<(), Box<dyn Error>> {
        let mut fields = BTreeMap::new();
        fields.insert("bytes".to_owned(), CanonicalValue::Bytes(vec![0, 255]));
        fields.insert(
            "items".to_owned(),
            CanonicalValue::Array(vec![CanonicalValue::Null, CanonicalValue::Boolean(true)]),
        );
        let value = CanonicalValue::Object(fields);
        let encoded = encode_stored_value(&value)?;
        assert_eq!(decode_stored_value(&encoded)?, value);
        Ok(())
    }

    #[test]
    fn malformed_envelopes_fail_closed() {
        let cases = [
            (&b""[..], StoredValueError::Truncated),
            (&b"XX\x01\x00"[..], StoredValueError::InvalidMagic),
            (&b"RV\x02\x00"[..], StoredValueError::UnsupportedVersion),
            (&b"RV\x01\xff"[..], StoredValueError::UnknownTag),
            (&b"RV\x01\x10"[..], StoredValueError::Truncated),
            (&b"RV\x01\x00\x00"[..], StoredValueError::TrailingBytes),
        ];
        for (input, expected) in cases {
            assert_eq!(decode_stored_value(input), Err(expected));
        }
    }

    #[test]
    fn malicious_counts_do_not_allocate_declared_size() {
        let array = b"RV\x01\x30\xff\xff\xff\xff";
        let object = b"RV\x01\x31\xff\xff\xff\xff";
        assert_eq!(
            decode_stored_value(array),
            Err(StoredValueError::ContainerTooLarge)
        );
        assert_eq!(
            decode_stored_value(object),
            Err(StoredValueError::ContainerTooLarge)
        );
    }

    #[test]
    fn duplicate_or_unsorted_object_keys_are_rejected() {
        let duplicate = b"RV\x01\x31\x00\x00\x00\x02\x00\x01a\x00\x00\x01a\x00";
        let unsorted = b"RV\x01\x31\x00\x00\x00\x02\x00\x01b\x00\x00\x01a\x00";
        for input in [duplicate.as_slice(), unsorted.as_slice()] {
            assert_eq!(
                decode_stored_value(input),
                Err(StoredValueError::ObjectKeysNotSorted)
            );
        }
    }

    #[test]
    fn depth_and_size_limits_are_enforced() {
        let mut at_limit = CanonicalValue::Null;
        for _ in 0..MAX_DEPTH {
            at_limit = CanonicalValue::Array(vec![at_limit]);
        }
        assert!(encode_stored_value(&at_limit).is_ok());

        let mut value = CanonicalValue::Null;
        for _ in 0..=MAX_DEPTH {
            value = CanonicalValue::Array(vec![value]);
        }
        assert_eq!(
            encode_stored_value(&value),
            Err(StoredValueError::DepthExceeded)
        );
        let oversized = CanonicalValue::Bytes(vec![0; STORED_VALUE_MAX_BYTES]);
        assert_eq!(
            encode_stored_value(&oversized),
            Err(StoredValueError::ValueTooLarge)
        );
    }

    #[test]
    fn key_and_container_boundaries_are_enforced() -> Result<(), Box<dyn Error>> {
        let mut valid_key = BTreeMap::new();
        valid_key.insert("k".repeat(MAX_OBJECT_KEY_BYTES), CanonicalValue::Null);
        let encoded = encode_stored_value(&CanonicalValue::Object(valid_key))?;
        assert!(matches!(
            decode_stored_value(&encoded)?,
            CanonicalValue::Object(_)
        ));

        let mut invalid_key = BTreeMap::new();
        invalid_key.insert("k".repeat(MAX_OBJECT_KEY_BYTES + 1), CanonicalValue::Null);
        assert_eq!(
            encode_stored_value(&CanonicalValue::Object(invalid_key)),
            Err(StoredValueError::ObjectKeyTooLong)
        );

        let valid_items = CanonicalValue::Array(vec![CanonicalValue::Null; MAX_CONTAINER_ITEMS]);
        assert!(encode_stored_value(&valid_items).is_ok());
        let invalid_items =
            CanonicalValue::Array(vec![CanonicalValue::Null; MAX_CONTAINER_ITEMS + 1]);
        assert_eq!(
            encode_stored_value(&invalid_items),
            Err(StoredValueError::ContainerTooLarge)
        );
        Ok(())
    }

    #[test]
    fn decoder_rejects_invalid_strings_ids_and_floats() {
        let invalid_utf8 = b"RV\x01\x20\x00\x00\x00\x01\xff";
        let invalid_id = b"RV\x01\x23\x00\x03bad";
        let negative_zero = b"RV\x01\x11\x80\x00\x00\x00\x00\x00\x00\x00";
        assert_eq!(
            decode_stored_value(invalid_utf8),
            Err(StoredValueError::InvalidUtf8)
        );
        assert_eq!(
            decode_stored_value(invalid_id),
            Err(StoredValueError::TypedIdInvalid)
        );
        assert_eq!(
            decode_stored_value(negative_zero),
            Err(StoredValueError::NonCanonicalFloat)
        );
    }

    #[test]
    fn stored_errors_have_stable_codes_and_are_not_retryable() {
        let cases = [
            (StoredValueError::InvalidMagic, "STORED_VALUE_MAGIC_INVALID"),
            (
                StoredValueError::UnsupportedVersion,
                "STORED_VALUE_VERSION_UNSUPPORTED",
            ),
            (StoredValueError::ValueTooLarge, "STORED_VALUE_TOO_LARGE"),
            (
                StoredValueError::ObjectKeysNotSorted,
                "STORED_VALUE_OBJECT_ORDER_INVALID",
            ),
        ];
        for (error, code) in cases {
            assert_eq!(error.code(), code);
            assert!(!error.retryable());
        }
    }

    fn leaf_strategy() -> impl Strategy<Value = CanonicalValue> {
        let finite_float = any::<u64>().prop_filter_map("finite f64", |bits| {
            FiniteF64::new(f64::from_bits(bits))
                .ok()
                .map(CanonicalValue::Float64)
        });
        let typed_id = ("[a-z0-9]{1,8}", any::<u128>()).prop_filter_map(
            "valid typed ID",
            |(kind, payload)| {
                TypedId::new(&kind, ulid::Ulid::from(payload))
                    .ok()
                    .map(CanonicalValue::TypedId)
            },
        );
        prop_oneof![
            Just(CanonicalValue::Null),
            any::<bool>().prop_map(CanonicalValue::Boolean),
            any::<i64>().prop_map(CanonicalValue::Int64),
            finite_float,
            any::<i64>().prop_map(|value| CanonicalValue::Timestamp(TimestampMicros::new(value))),
            typed_id,
            ".{0,64}".prop_map(CanonicalValue::String),
            prop::collection::vec(any::<u8>(), 0..64).prop_map(CanonicalValue::Bytes),
        ]
    }

    proptest! {
        #[test]
        fn arbitrary_bounded_values_round_trip(
            value in leaf_strategy().prop_recursive(
                4,
                128,
                8,
                |inner| prop_oneof![
                    prop::collection::vec(inner.clone(), 0..8).prop_map(CanonicalValue::Array),
                    prop::collection::btree_map("[a-z]{1,8}", inner, 0..8)
                        .prop_map(CanonicalValue::Object),
                ],
            )
        ) {
            let encoded = encode_stored_value(&value)?;
            let decoded = decode_stored_value(&encoded)?;
            prop_assert_eq!(decoded, value);
        }
    }
}
