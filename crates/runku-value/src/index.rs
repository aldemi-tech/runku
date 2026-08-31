//! Order-preserving Index Key v1 codec.

use std::{cmp::Ordering, str};

use thiserror::Error;

use crate::{CanonicalValue, FiniteF64, TimestampMicros, TypedId};

const MAGIC: [u8; 2] = *b"RK";
const FORMAT_VERSION: u8 = 1;
const MAX_COMPONENTS: usize = 16;
const MAX_ENCODED_BYTES: usize = 4 * 1024;

const TAG_NULL: u8 = 0x10;
const TAG_FALSE: u8 = 0x20;
const TAG_TRUE: u8 = 0x21;
const TAG_INT64: u8 = 0x30;
const TAG_FLOAT64: u8 = 0x31;
const TAG_TIMESTAMP: u8 = 0x40;
const TAG_STRING: u8 = 0x50;
const TAG_BYTES: u8 = 0x60;
const TAG_TYPED_ID: u8 = 0x70;

/// Scalar value supported by Index Key v1.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum IndexValue {
    /// Null sorts before every other type.
    Null,
    /// Boolean sorts false before true.
    Boolean(bool),
    /// Signed integers sort numerically within the int64 type.
    Int64(i64),
    /// Finite floats sort numerically within the float64 type.
    Float64(FiniteF64),
    /// Timestamps sort by signed UTC microseconds.
    Timestamp(TimestampMicros),
    /// Strings sort by unsigned UTF-8 bytes.
    String(String),
    /// Bytes sort unsigned lexicographically.
    Bytes(Vec<u8>),
    /// IDs sort by their canonical ASCII representation.
    TypedId(TypedId),
}

impl TryFrom<&CanonicalValue> for IndexValue {
    type Error = IndexKeyError;

    fn try_from(value: &CanonicalValue) -> Result<Self, Self::Error> {
        match value {
            CanonicalValue::Null => Ok(Self::Null),
            CanonicalValue::Boolean(value) => Ok(Self::Boolean(*value)),
            CanonicalValue::Int64(value) => Ok(Self::Int64(*value)),
            CanonicalValue::Float64(value) => Ok(Self::Float64(*value)),
            CanonicalValue::Timestamp(value) => Ok(Self::Timestamp(*value)),
            CanonicalValue::String(value) => Ok(Self::String(value.clone())),
            CanonicalValue::Bytes(value) => Ok(Self::Bytes(value.clone())),
            CanonicalValue::TypedId(value) => Ok(Self::TypedId(value.clone())),
            CanonicalValue::Array(_) | CanonicalValue::Object(_) => {
                Err(IndexKeyError::ValueTypeUnsupported)
            }
        }
    }
}

/// A validated Index Key v1 tuple.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct IndexKey {
    encoded: Vec<u8>,
    components: Vec<IndexValue>,
}

impl IndexKey {
    /// Binary format version emitted by this type.
    pub const FORMAT_VERSION: u8 = FORMAT_VERSION;
    /// Maximum number of tuple components.
    pub const MAX_COMPONENTS: usize = MAX_COMPONENTS;
    /// Maximum encoded key size.
    pub const MAX_ENCODED_BYTES: usize = MAX_ENCODED_BYTES;

    /// Encodes a non-empty tuple using Index Key v1.
    ///
    /// # Errors
    ///
    /// Returns [`IndexKeyError`] for an empty tuple or when component/byte limits are exceeded.
    pub fn encode(values: &[IndexValue]) -> Result<Self, IndexKeyError> {
        validate_component_count(values.len())?;
        let mut encoded = Vec::with_capacity(64);
        encoded.extend_from_slice(&MAGIC);
        encoded.push(FORMAT_VERSION);
        encode_components(values, &mut encoded)?;
        ensure_key_size(encoded.len())?;
        Ok(Self {
            encoded,
            components: values.to_vec(),
        })
    }

    /// Validates and adopts already encoded Index Key v1 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`IndexKeyError`] when the bytes are malformed, non-canonical, or over limits.
    pub fn decode(encoded: &[u8]) -> Result<Self, IndexKeyError> {
        let components = decode_components(encoded)?;
        let canonical = Self::encode(&components)?;
        if canonical.encoded != encoded {
            return Err(IndexKeyError::NonCanonical);
        }
        Ok(canonical)
    }

    /// Returns the canonical persisted bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    /// Decodes the logical tuple components.
    ///
    /// This cannot fail because constructors validate and canonicalize the bytes.
    #[must_use]
    pub fn components(&self) -> &[IndexValue] {
        &self.components
    }

    /// Creates a prefix from the first `component_count` components.
    ///
    /// # Errors
    ///
    /// Returns [`IndexKeyError::PrefixComponentCountInvalid`] for zero or a count beyond the key.
    pub fn prefix(&self, component_count: usize) -> Result<IndexKeyPrefix, IndexKeyError> {
        if component_count == 0 || component_count > self.components.len() {
            return Err(IndexKeyError::PrefixComponentCountInvalid);
        }
        let mut encoded = Vec::with_capacity(self.encoded.len());
        encoded.extend_from_slice(&MAGIC);
        encoded.push(FORMAT_VERSION);
        encode_components(&self.components[..component_count], &mut encoded)?;
        Ok(IndexKeyPrefix { encoded })
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.encoded.cmp(&other.encoded)
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Encoded prefix bounds for a compound index tuple.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexKeyPrefix {
    encoded: Vec<u8>,
}

impl IndexKeyPrefix {
    /// Inclusive lower bound containing the encoded prefix.
    #[must_use]
    pub fn inclusive_start(&self) -> &[u8] {
        &self.encoded
    }

    /// Exclusive upper bound that contains every byte string with this prefix.
    ///
    /// # Errors
    ///
    /// Returns [`IndexKeyError::PrefixHasNoSuccessor`] only if every byte is `ff`.
    pub fn exclusive_end(&self) -> Result<Vec<u8>, IndexKeyError> {
        prefix_successor(&self.encoded).ok_or(IndexKeyError::PrefixHasNoSuccessor)
    }
}

fn validate_component_count(count: usize) -> Result<(), IndexKeyError> {
    if count == 0 {
        return Err(IndexKeyError::Empty);
    }
    if count > MAX_COMPONENTS {
        return Err(IndexKeyError::TooManyComponents);
    }
    Ok(())
}

fn ensure_key_size(length: usize) -> Result<(), IndexKeyError> {
    if length > MAX_ENCODED_BYTES {
        return Err(IndexKeyError::TooLarge);
    }
    Ok(())
}

fn encode_components(values: &[IndexValue], output: &mut Vec<u8>) -> Result<(), IndexKeyError> {
    for value in values {
        match value {
            IndexValue::Null => output.push(TAG_NULL),
            IndexValue::Boolean(false) => output.push(TAG_FALSE),
            IndexValue::Boolean(true) => output.push(TAG_TRUE),
            IndexValue::Int64(value) => {
                output.push(TAG_INT64);
                let ordered = value.cast_unsigned() ^ (1_u64 << 63);
                output.extend_from_slice(&ordered.to_be_bytes());
            }
            IndexValue::Float64(value) => {
                output.push(TAG_FLOAT64);
                output.extend_from_slice(&ordered_float_bits(*value).to_be_bytes());
            }
            IndexValue::Timestamp(value) => {
                output.push(TAG_TIMESTAMP);
                let ordered = value.get().cast_unsigned() ^ (1_u64 << 63);
                output.extend_from_slice(&ordered.to_be_bytes());
            }
            IndexValue::String(value) => {
                output.push(TAG_STRING);
                encode_escaped(value.as_bytes(), output);
            }
            IndexValue::Bytes(value) => {
                output.push(TAG_BYTES);
                encode_escaped(value, output);
            }
            IndexValue::TypedId(value) => {
                output.push(TAG_TYPED_ID);
                encode_escaped(value.to_string().as_bytes(), output);
            }
        }
        ensure_key_size(output.len())?;
    }
    Ok(())
}

const fn ordered_float_bits(value: FiniteF64) -> u64 {
    let bits = value.to_bits();
    if bits & (1_u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1_u64 << 63)
    }
}

fn encode_escaped(input: &[u8], output: &mut Vec<u8>) {
    for byte in input {
        if *byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
}

fn decode_components(input: &[u8]) -> Result<Vec<IndexValue>, IndexKeyError> {
    ensure_key_size(input.len())?;
    let mut cursor = Cursor::new(input);
    if cursor.take(2)? != MAGIC {
        return Err(IndexKeyError::InvalidMagic);
    }
    if cursor.byte()? != FORMAT_VERSION {
        return Err(IndexKeyError::UnsupportedVersion);
    }
    let mut values = Vec::new();
    while !cursor.is_empty() {
        if values.len() == MAX_COMPONENTS {
            return Err(IndexKeyError::TooManyComponents);
        }
        values.push(decode_component(&mut cursor)?);
    }
    validate_component_count(values.len())?;
    Ok(values)
}

fn decode_component(cursor: &mut Cursor<'_>) -> Result<IndexValue, IndexKeyError> {
    match cursor.byte()? {
        TAG_NULL => Ok(IndexValue::Null),
        TAG_FALSE => Ok(IndexValue::Boolean(false)),
        TAG_TRUE => Ok(IndexValue::Boolean(true)),
        TAG_INT64 => {
            let ordered = u64::from_be_bytes(cursor.array()?);
            Ok(IndexValue::Int64((ordered ^ (1_u64 << 63)).cast_signed()))
        }
        TAG_FLOAT64 => {
            let ordered = u64::from_be_bytes(cursor.array()?);
            let bits = if ordered & (1_u64 << 63) == 0 {
                !ordered
            } else {
                ordered ^ (1_u64 << 63)
            };
            let value =
                FiniteF64::new(f64::from_bits(bits)).map_err(|_| IndexKeyError::FloatNonFinite)?;
            if value.to_bits() != bits {
                return Err(IndexKeyError::NonCanonical);
            }
            Ok(IndexValue::Float64(value))
        }
        TAG_TIMESTAMP => {
            let ordered = u64::from_be_bytes(cursor.array()?);
            Ok(IndexValue::Timestamp(TimestampMicros::new(
                (ordered ^ (1_u64 << 63)).cast_signed(),
            )))
        }
        TAG_STRING => {
            let bytes = decode_escaped(cursor)?;
            let value = str::from_utf8(&bytes).map_err(|_| IndexKeyError::InvalidUtf8)?;
            Ok(IndexValue::String(value.to_owned()))
        }
        TAG_BYTES => Ok(IndexValue::Bytes(decode_escaped(cursor)?)),
        TAG_TYPED_ID => {
            let bytes = decode_escaped(cursor)?;
            let wire = str::from_utf8(&bytes).map_err(|_| IndexKeyError::TypedIdInvalid)?;
            let id = wire
                .parse::<TypedId>()
                .map_err(|_| IndexKeyError::TypedIdInvalid)?;
            Ok(IndexValue::TypedId(id))
        }
        _ => Err(IndexKeyError::UnknownTag),
    }
}

fn decode_escaped(cursor: &mut Cursor<'_>) -> Result<Vec<u8>, IndexKeyError> {
    let mut output = Vec::new();
    loop {
        let byte = cursor.byte()?;
        if byte != 0 {
            output.push(byte);
            continue;
        }
        match cursor.byte()? {
            0 => return Ok(output),
            0xff => output.push(0),
            _ => return Err(IndexKeyError::InvalidEscape),
        }
        if output.len() > MAX_ENCODED_BYTES {
            return Err(IndexKeyError::TooLarge);
        }
    }
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(last) = successor.pop() {
        if last != u8::MAX {
            successor.push(last + 1);
            return Some(successor);
        }
    }
    None
}

struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], IndexKeyError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(IndexKeyError::Truncated)?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(IndexKeyError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, IndexKeyError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], IndexKeyError> {
        self.take(N)?
            .try_into()
            .map_err(|_| IndexKeyError::Truncated)
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.input.len()
    }
}

/// Error returned by Index Key v1 construction or decoding.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum IndexKeyError {
    /// A persisted key has the wrong magic.
    #[error("index key magic is invalid")]
    InvalidMagic,
    /// A persisted key uses an unsupported version.
    #[error("index key version is unsupported")]
    UnsupportedVersion,
    /// Tuple has no components.
    #[error("index key must contain at least one component")]
    Empty,
    /// Tuple exceeds the v1 component count.
    #[error("index key has too many components")]
    TooManyComponents,
    /// Encoded tuple exceeds 4 KiB.
    #[error("index key exceeds the v1 byte limit")]
    TooLarge,
    /// Array/object cannot be indexed by v1.
    #[error("value type is not indexable in v1")]
    ValueTypeUnsupported,
    /// Persisted input ends inside a component.
    #[error("index key is truncated")]
    Truncated,
    /// Component tag is unknown.
    #[error("index key component tag is unsupported")]
    UnknownTag,
    /// Variable component has an invalid zero escape.
    #[error("index key byte escape is invalid")]
    InvalidEscape,
    /// String component is not UTF-8.
    #[error("index key string is invalid UTF-8")]
    InvalidUtf8,
    /// Float is NaN or infinite.
    #[error("index key float is not finite")]
    FloatNonFinite,
    /// Typed ID component is invalid.
    #[error("index key typed ID is invalid")]
    TypedIdInvalid,
    /// Bytes are valid-shaped but do not use the one canonical representation.
    #[error("index key encoding is not canonical")]
    NonCanonical,
    /// Prefix component count is zero or beyond the tuple.
    #[error("index key prefix component count is invalid")]
    PrefixComponentCountInvalid,
    /// Prefix is all `ff` bytes and has no finite exclusive successor.
    #[error("index key prefix has no exclusive successor")]
    PrefixHasNoSuccessor,
}

impl IndexKeyError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidMagic => "INDEX_KEY_MAGIC_INVALID",
            Self::UnsupportedVersion => "INDEX_KEY_VERSION_UNSUPPORTED",
            Self::Empty => "INDEX_KEY_EMPTY",
            Self::TooManyComponents => "INDEX_KEY_COMPONENT_LIMIT_EXCEEDED",
            Self::TooLarge => "INDEX_KEY_TOO_LARGE",
            Self::ValueTypeUnsupported => "INDEX_VALUE_TYPE_UNSUPPORTED",
            Self::Truncated => "INDEX_KEY_TRUNCATED",
            Self::UnknownTag => "INDEX_KEY_TAG_UNSUPPORTED",
            Self::InvalidEscape => "INDEX_KEY_ESCAPE_INVALID",
            Self::InvalidUtf8 => "INDEX_KEY_UTF8_INVALID",
            Self::FloatNonFinite => "INDEX_KEY_FLOAT_NON_FINITE",
            Self::TypedIdInvalid => "INDEX_KEY_TYPED_ID_INVALID",
            Self::NonCanonical => "INDEX_KEY_NON_CANONICAL",
            Self::PrefixComponentCountInvalid => "INDEX_KEY_PREFIX_COMPONENTS_INVALID",
            Self::PrefixHasNoSuccessor => "INDEX_KEY_PREFIX_SUCCESSOR_ABSENT",
        }
    }

    /// Key errors are deterministic and not retryable unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn compound_key_round_trips_and_prefix_bounds_contain_it() -> Result<(), Box<dyn Error>> {
        let key =
            IndexKey::encode(&[IndexValue::String("team".to_owned()), IndexValue::Int64(42)])?;
        assert_eq!(IndexKey::decode(key.as_bytes())?, key);
        let prefix = key.prefix(1)?;
        assert!(key.as_bytes() >= prefix.inclusive_start());
        assert!(key.as_bytes() < prefix.exclusive_end()?.as_slice());
        Ok(())
    }

    #[test]
    fn zero_bytes_are_escaped_and_decode_canonically() -> Result<(), Box<dyn Error>> {
        let key = IndexKey::encode(&[IndexValue::Bytes(vec![0, 255])])?;
        assert!(key.as_bytes().ends_with(&[0, 255, 255, 0, 0]));
        assert_eq!(key.components(), &[IndexValue::Bytes(vec![0, 255])]);
        Ok(())
    }

    #[test]
    fn invalid_shapes_and_unsupported_values_are_rejected() {
        assert_eq!(IndexKey::encode(&[]), Err(IndexKeyError::Empty));
        assert_eq!(
            IndexValue::try_from(&CanonicalValue::Array(Vec::new())),
            Err(IndexKeyError::ValueTypeUnsupported)
        );
        assert_eq!(
            IndexKey::decode(b"RK\x01\x50\x00\x01"),
            Err(IndexKeyError::InvalidEscape)
        );
        assert_eq!(
            IndexKey::decode(b"RK\x01\x30"),
            Err(IndexKeyError::Truncated)
        );
    }

    #[test]
    fn component_size_and_prefix_boundaries_are_enforced() -> Result<(), Box<dyn Error>> {
        let maximum = vec![IndexValue::Null; MAX_COMPONENTS];
        let key = IndexKey::encode(&maximum)?;
        assert_eq!(key.components().len(), MAX_COMPONENTS);
        assert_eq!(
            IndexKey::encode(&vec![IndexValue::Null; MAX_COMPONENTS + 1]),
            Err(IndexKeyError::TooManyComponents)
        );
        assert_eq!(
            IndexKey::encode(&[IndexValue::String("x".repeat(MAX_ENCODED_BYTES))]),
            Err(IndexKeyError::TooLarge)
        );
        assert_eq!(
            key.prefix(0),
            Err(IndexKeyError::PrefixComponentCountInvalid)
        );
        assert_eq!(
            key.prefix(MAX_COMPONENTS + 1),
            Err(IndexKeyError::PrefixComponentCountInvalid)
        );
        Ok(())
    }

    #[test]
    fn type_order_is_explicit_and_stable() -> Result<(), Box<dyn Error>> {
        let id: TypedId = "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?;
        let values = [
            IndexValue::Null,
            IndexValue::Boolean(false),
            IndexValue::Boolean(true),
            IndexValue::Int64(0),
            IndexValue::Float64(FiniteF64::new(0.0)?),
            IndexValue::Timestamp(TimestampMicros::new(0)),
            IndexValue::String(String::new()),
            IndexValue::Bytes(Vec::new()),
            IndexValue::TypedId(id),
        ];
        let keys = values
            .iter()
            .cloned()
            .map(|value| IndexKey::encode(&[value]))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        Ok(())
    }

    #[test]
    fn index_errors_have_stable_codes_and_are_not_retryable() {
        let cases = [
            (IndexKeyError::Empty, "INDEX_KEY_EMPTY"),
            (IndexKeyError::TooLarge, "INDEX_KEY_TOO_LARGE"),
            (
                IndexKeyError::ValueTypeUnsupported,
                "INDEX_VALUE_TYPE_UNSUPPORTED",
            ),
            (IndexKeyError::InvalidEscape, "INDEX_KEY_ESCAPE_INVALID"),
        ];
        for (error, code) in cases {
            assert_eq!(error.code(), code);
            assert!(!error.retryable());
        }
    }

    proptest! {
        #[test]
        fn int_encoding_preserves_order(left in any::<i64>(), right in any::<i64>()) {
            let left_key = IndexKey::encode(&[IndexValue::Int64(left)])?;
            let right_key = IndexKey::encode(&[IndexValue::Int64(right)])?;
            prop_assert_eq!(left_key.cmp(&right_key), left.cmp(&right));
        }

        #[test]
        fn timestamp_encoding_preserves_order(left in any::<i64>(), right in any::<i64>()) {
            let left_value = TimestampMicros::new(left);
            let right_value = TimestampMicros::new(right);
            let left_key = IndexKey::encode(&[IndexValue::Timestamp(left_value)])?;
            let right_key = IndexKey::encode(&[IndexValue::Timestamp(right_value)])?;
            prop_assert_eq!(left_key.cmp(&right_key), left.cmp(&right));
        }

        #[test]
        fn finite_float_encoding_preserves_order(left in any::<f64>(), right in any::<f64>()) {
            if let (Ok(left), Ok(right)) = (FiniteF64::new(left), FiniteF64::new(right)) {
                let left_key = IndexKey::encode(&[IndexValue::Float64(left)])?;
                let right_key = IndexKey::encode(&[IndexValue::Float64(right)])?;
                prop_assert_eq!(left_key.cmp(&right_key), left.cmp(&right));
            }
        }

        #[test]
        fn string_encoding_preserves_order(left in ".{0,64}", right in ".{0,64}") {
            let left_key = IndexKey::encode(&[IndexValue::String(left.clone())])?;
            let right_key = IndexKey::encode(&[IndexValue::String(right.clone())])?;
            prop_assert_eq!(left_key.cmp(&right_key), left.cmp(&right));
        }

        #[test]
        fn bytes_encoding_preserves_order(
            left in prop::collection::vec(any::<u8>(), 0..64),
            right in prop::collection::vec(any::<u8>(), 0..64),
        ) {
            let left_key = IndexKey::encode(&[IndexValue::Bytes(left.clone())])?;
            let right_key = IndexKey::encode(&[IndexValue::Bytes(right.clone())])?;
            prop_assert_eq!(left_key.cmp(&right_key), left.cmp(&right));
        }
    }
}
