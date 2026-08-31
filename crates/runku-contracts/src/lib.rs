//! Canonical, bounded value contracts and document schemas.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use runku_core::TableId;
use runku_value::CanonicalValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum canonical bytes for one value contract.
pub const CONTRACT_MAX_BYTES: usize = 256 * 1024;
/// Maximum recursive contract depth.
pub const CONTRACT_MAX_DEPTH: usize = 32;
/// Maximum total nodes in one contract.
pub const CONTRACT_MAX_NODES: usize = 10_000;
/// Maximum fields in one object contract.
pub const OBJECT_MAX_FIELDS: usize = 1_000;
/// Maximum variants in one union contract.
pub const UNION_MAX_VARIANTS: usize = 16;
/// Maximum tables in one document schema.
pub const SCHEMA_MAX_TABLES: usize = 1_000;
const NAME_MAX_BYTES: usize = 128;
const VALIDATION_MAX_STEPS: usize = 200_000;

/// Recursive Contract v1 over the complete canonical Runku value algebra.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Contract {
    /// Accepts any canonical Runku value.
    Any,
    /// Accepts only null.
    Null,
    /// Accepts only booleans.
    Boolean,
    /// Accepts signed 64-bit integers, with optional inclusive bounds.
    Int64 {
        /// Inclusive minimum.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum: Option<i64>,
        /// Inclusive maximum.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maximum: Option<i64>,
    },
    /// Accepts finite binary64 values, with optional inclusive bounds.
    Float64 {
        /// Inclusive minimum.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum: Option<FiniteBound>,
        /// Inclusive maximum.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maximum: Option<FiniteBound>,
    },
    /// Accepts Unicode strings measured in UTF-8 bytes.
    String {
        /// Minimum UTF-8 byte length.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum_bytes: Option<u32>,
        /// Maximum UTF-8 byte length.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maximum_bytes: Option<u32>,
    },
    /// Accepts opaque byte strings.
    Bytes {
        /// Minimum byte length.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum_bytes: Option<u32>,
        /// Maximum byte length.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maximum_bytes: Option<u32>,
    },
    /// Accepts canonical timestamps.
    Timestamp,
    /// Accepts canonical typed IDs, optionally restricted to one kind prefix.
    TypedId {
        /// Required kind without the underscore, or any kind when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kind: Option<String>,
    },
    /// Accepts a canonical `doc_*` ID associated with one logical table name.
    DocumentId {
        /// Logical schema table name used for static association and compatibility.
        table: String,
    },
    /// Accepts arrays whose items all match one contract.
    Array {
        /// Item contract.
        items: Box<Self>,
        /// Minimum item count.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum_items: Option<u32>,
        /// Maximum item count.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maximum_items: Option<u32>,
    },
    /// Accepts exact objects. Keys absent from `fields` are rejected.
    Object {
        /// Contract for every declared key, in canonical key order.
        fields: BTreeMap<String, Self>,
        /// Declared keys that may be absent.
        #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
        optional: BTreeSet<String>,
    },
    /// Accepts a value matching at least one of two to sixteen variants.
    Union {
        /// Ordered variants; duplicate canonical variants are rejected.
        variants: Vec<Self>,
    },
}

/// JSON-safe finite float bound encoded as a decimal number.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct FiniteBound(f64);

impl Eq for FiniteBound {}

impl FiniteBound {
    /// Creates a finite canonical bound, normalizing negative zero.
    ///
    /// # Errors
    ///
    /// Rejects NaN and infinities.
    pub fn new(value: f64) -> Result<Self, ContractError> {
        if !value.is_finite() {
            return Err(ContractError::InvalidDefinition);
        }
        Ok(Self(if value == 0.0 { 0.0 } else { value }))
    }

    /// Returns the finite bound.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }
}

/// One named logical table and its document contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DocumentTableContract {
    /// Stable logical table identity.
    pub id: TableId,
    /// Stable code-generation name.
    pub name: String,
    /// Contract enforced for the complete stored document value.
    pub document_contract: Contract,
}

/// Canonical Document Schema v1 embedded in a Release artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DocumentSchemaV1 {
    /// Schema representation version. Must equal one.
    pub version: u8,
    /// Tables sorted strictly by stable ID in canonical encoding.
    pub tables: Vec<DocumentTableContract>,
}

/// Contract definition/codec failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ContractError {
    /// Definition violates shape, ordering, name, or bound invariants.
    #[error("contract definition is invalid")]
    InvalidDefinition,
    /// Definition exceeds a hard v1 size, depth, node, field, or table limit.
    #[error("contract definition exceeds a v1 limit")]
    LimitExceeded,
    /// Encoded bytes are malformed, noncanonical, or contain unknown fields.
    #[error("contract encoding is invalid")]
    InvalidEncoding,
}

impl ContractError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidDefinition => "CONTRACT_DEFINITION_INVALID",
            Self::LimitExceeded => "CONTRACT_LIMIT_EXCEEDED",
            Self::InvalidEncoding => "CONTRACT_ENCODING_INVALID",
        }
    }
}

/// Sanitized reason a canonical value does not satisfy a contract.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ValidationError {
    /// The canonical value variant does not match the declared type.
    #[error("value type does not match contract")]
    TypeMismatch,
    /// A scalar/container bound is violated.
    #[error("value violates contract bounds")]
    BoundViolation,
    /// An exact object is missing a required field or contains an unknown field.
    #[error("object shape does not match contract")]
    ObjectShape,
    /// No union variant accepts the value.
    #[error("value does not match any union variant")]
    UnionMismatch,
    /// Table is not declared by the selected Release schema.
    #[error("table is not declared by document schema")]
    UnknownTable,
}

impl ValidationError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::TypeMismatch => "CONTRACT_TYPE_MISMATCH",
            Self::BoundViolation => "CONTRACT_BOUND_VIOLATION",
            Self::ObjectShape => "CONTRACT_OBJECT_SHAPE_INVALID",
            Self::UnionMismatch => "CONTRACT_UNION_MISMATCH",
            Self::UnknownTable => "SCHEMA_TABLE_UNKNOWN",
        }
    }
}

impl Contract {
    /// Validates definition bounds and recursive invariants.
    ///
    /// # Errors
    ///
    /// Rejects inverted bounds, malformed kinds/keys, invalid optional fields, duplicate union
    /// variants, or structural limits.
    pub fn validate_definition(&self) -> Result<(), ContractError> {
        let mut nodes = 0_usize;
        self.validate_definition_at(0, &mut nodes)?;
        let encoded = serde_json::to_vec(self).map_err(|_| ContractError::InvalidDefinition)?;
        if encoded.len() > CONTRACT_MAX_BYTES {
            return Err(ContractError::LimitExceeded);
        }
        Ok(())
    }

    /// Validates one already-canonical value.
    ///
    /// # Errors
    ///
    /// Returns a sanitized stable mismatch category without echoing values or field names.
    pub fn validate_value(&self, value: &CanonicalValue) -> Result<(), ValidationError> {
        let mut steps = 0_usize;
        self.validate_value_at(value, &mut steps)
    }

    fn validate_definition_at(&self, depth: usize, nodes: &mut usize) -> Result<(), ContractError> {
        *nodes = nodes.saturating_add(1);
        if depth > CONTRACT_MAX_DEPTH || *nodes > CONTRACT_MAX_NODES {
            return Err(ContractError::LimitExceeded);
        }
        match self {
            Self::Int64 { minimum, maximum } => validate_bounds(*minimum, *maximum),
            Self::Float64 { minimum, maximum } => {
                if minimum.is_some_and(|value| !value.get().is_finite())
                    || maximum.is_some_and(|value| !value.get().is_finite())
                    || minimum
                        .is_some_and(|value| value.get() == 0.0 && value.get().is_sign_negative())
                    || maximum
                        .is_some_and(|value| value.get() == 0.0 && value.get().is_sign_negative())
                    || matches!((minimum, maximum), (Some(left), Some(right)) if left.get() > right.get())
                {
                    Err(ContractError::InvalidDefinition)
                } else {
                    Ok(())
                }
            }
            Self::String {
                minimum_bytes,
                maximum_bytes,
            }
            | Self::Bytes {
                minimum_bytes,
                maximum_bytes,
            } => validate_bounds(*minimum_bytes, *maximum_bytes),
            Self::TypedId { kind } => {
                if kind.as_deref().is_some_and(|kind| {
                    kind.is_empty()
                        || kind.len() > 16
                        || !kind
                            .bytes()
                            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
                }) {
                    Err(ContractError::InvalidDefinition)
                } else {
                    Ok(())
                }
            }
            Self::DocumentId { table } => {
                if valid_logical_name(table) {
                    Ok(())
                } else {
                    Err(ContractError::InvalidDefinition)
                }
            }
            Self::Array {
                items,
                minimum_items,
                maximum_items,
            } => {
                validate_bounds(*minimum_items, *maximum_items)?;
                items.validate_definition_at(depth.saturating_add(1), nodes)
            }
            Self::Object { fields, optional } => {
                if fields.len() > OBJECT_MAX_FIELDS
                    || optional.iter().any(|key| !fields.contains_key(key))
                    || fields.keys().any(|key| !valid_field_name(key))
                {
                    return Err(ContractError::InvalidDefinition);
                }
                for contract in fields.values() {
                    contract.validate_definition_at(depth.saturating_add(1), nodes)?;
                }
                Ok(())
            }
            Self::Union { variants } => {
                if !(2..=UNION_MAX_VARIANTS).contains(&variants.len()) {
                    return Err(ContractError::InvalidDefinition);
                }
                let mut encoded = BTreeSet::new();
                for variant in variants {
                    variant.validate_definition_at(depth.saturating_add(1), nodes)?;
                    if !encoded.insert(
                        serde_json::to_vec(variant)
                            .map_err(|_| ContractError::InvalidDefinition)?,
                    ) {
                        return Err(ContractError::InvalidDefinition);
                    }
                }
                Ok(())
            }
            Self::Any | Self::Null | Self::Boolean | Self::Timestamp => Ok(()),
        }
    }

    fn validate_value_at(
        &self,
        value: &CanonicalValue,
        steps: &mut usize,
    ) -> Result<(), ValidationError> {
        *steps = steps.saturating_add(1);
        if *steps > VALIDATION_MAX_STEPS {
            return Err(ValidationError::BoundViolation);
        }
        match (self, value) {
            (Self::Any, _)
            | (Self::Null, CanonicalValue::Null)
            | (Self::Boolean, CanonicalValue::Boolean(_))
            | (Self::Timestamp, CanonicalValue::Timestamp(_)) => Ok(()),
            (Self::Int64 { minimum, maximum }, CanonicalValue::Int64(value)) => {
                validate_value_bounds(value, *minimum, *maximum)
            }
            (Self::Float64 { minimum, maximum }, CanonicalValue::Float64(value)) => {
                validate_value_bounds(
                    &value.get(),
                    minimum.map(FiniteBound::get),
                    maximum.map(FiniteBound::get),
                )
            }
            (
                Self::String {
                    minimum_bytes,
                    maximum_bytes,
                },
                CanonicalValue::String(value),
            ) => validate_length(value.len(), *minimum_bytes, *maximum_bytes),
            (
                Self::Bytes {
                    minimum_bytes,
                    maximum_bytes,
                },
                CanonicalValue::Bytes(value),
            ) => validate_length(value.len(), *minimum_bytes, *maximum_bytes),
            (Self::TypedId { kind }, CanonicalValue::TypedId(value)) => {
                if kind.as_deref().is_none_or(|kind| kind == value.kind()) {
                    Ok(())
                } else {
                    Err(ValidationError::BoundViolation)
                }
            }
            (Self::DocumentId { .. }, CanonicalValue::TypedId(value)) => {
                if value.kind() == "doc" {
                    Ok(())
                } else {
                    Err(ValidationError::BoundViolation)
                }
            }
            (
                Self::Array {
                    items,
                    minimum_items,
                    maximum_items,
                },
                CanonicalValue::Array(values),
            ) => {
                validate_length(values.len(), *minimum_items, *maximum_items)?;
                values
                    .iter()
                    .try_for_each(|value| items.validate_value_at(value, steps))
            }
            (Self::Object { fields, optional }, CanonicalValue::Object(values)) => {
                if values.keys().any(|key| !fields.contains_key(key))
                    || fields
                        .keys()
                        .any(|key| !optional.contains(key) && !values.contains_key(key))
                {
                    return Err(ValidationError::ObjectShape);
                }
                values.iter().try_for_each(|(key, value)| {
                    fields
                        .get(key)
                        .ok_or(ValidationError::ObjectShape)?
                        .validate_value_at(value, steps)
                })
            }
            (Self::Union { variants }, value) => {
                if variants
                    .iter()
                    .any(|variant| variant.validate_value_at(value, steps).is_ok())
                {
                    Ok(())
                } else {
                    Err(ValidationError::UnionMismatch)
                }
            }
            _ => Err(ValidationError::TypeMismatch),
        }
    }
}

impl DocumentSchemaV1 {
    /// Constructs and validates a canonical schema, sorting tables by stable ID.
    ///
    /// # Errors
    ///
    /// Rejects excessive/duplicate IDs or names and invalid document contracts.
    pub fn new(mut tables: Vec<DocumentTableContract>) -> Result<Self, ContractError> {
        tables.sort_by_key(|table| table.id);
        let schema = Self { version: 1, tables };
        schema.validate_definition()?;
        Ok(schema)
    }

    /// Validates schema ordering, uniqueness, names, contracts, and limits.
    ///
    /// # Errors
    ///
    /// Returns a stable definition or limit error.
    pub fn validate_definition(&self) -> Result<(), ContractError> {
        if self.version != 1 || self.tables.len() > SCHEMA_MAX_TABLES {
            return Err(if self.tables.len() > SCHEMA_MAX_TABLES {
                ContractError::LimitExceeded
            } else {
                ContractError::InvalidDefinition
            });
        }
        let mut previous = None;
        let mut names = BTreeSet::new();
        for table in &self.tables {
            if previous.is_some_and(|id| id >= table.id)
                || !valid_logical_name(&table.name)
                || !names.insert(table.name.as_str())
            {
                return Err(ContractError::InvalidDefinition);
            }
            table.document_contract.validate_definition()?;
            previous = Some(table.id);
        }
        let encoded = serde_json::to_vec(self).map_err(|_| ContractError::InvalidDefinition)?;
        if encoded.len() > CONTRACT_MAX_BYTES {
            return Err(ContractError::LimitExceeded);
        }
        Ok(())
    }

    /// Validates one complete document for a declared table.
    ///
    /// # Errors
    ///
    /// Rejects unknown tables or values outside the exact table contract.
    pub fn validate_document(
        &self,
        table_id: TableId,
        value: &CanonicalValue,
    ) -> Result<(), ValidationError> {
        self.tables
            .binary_search_by_key(&table_id, |table| table.id)
            .ok()
            .and_then(|index| self.tables.get(index))
            .ok_or(ValidationError::UnknownTable)?
            .document_contract
            .validate_value(value)
    }
}

/// Encodes one validated contract as strict canonical JSON bytes.
///
/// # Errors
///
/// Rejects invalid definitions or canonical size limits.
pub fn encode_contract(contract: &Contract) -> Result<Vec<u8>, ContractError> {
    contract.validate_definition()?;
    let bytes = serde_json::to_vec(contract).map_err(|_| ContractError::InvalidDefinition)?;
    if bytes.len() > CONTRACT_MAX_BYTES {
        Err(ContractError::LimitExceeded)
    } else {
        Ok(bytes)
    }
}

/// Decodes strict canonical contract JSON and rejects alternate encodings.
///
/// # Errors
///
/// Rejects malformed, noncanonical, unknown-field, invalid, or oversized bytes.
pub fn decode_contract(bytes: &[u8]) -> Result<Contract, ContractError> {
    if bytes.len() > CONTRACT_MAX_BYTES {
        return Err(ContractError::LimitExceeded);
    }
    let contract =
        serde_json::from_slice::<Contract>(bytes).map_err(|_| ContractError::InvalidEncoding)?;
    contract.validate_definition()?;
    if encode_contract(&contract)? != bytes {
        return Err(ContractError::InvalidEncoding);
    }
    Ok(contract)
}

/// Encodes one validated Document Schema as strict canonical JSON bytes.
///
/// # Errors
///
/// Rejects invalid schemas or canonical size limits.
pub fn encode_document_schema(schema: &DocumentSchemaV1) -> Result<Vec<u8>, ContractError> {
    schema.validate_definition()?;
    let bytes = serde_json::to_vec(schema).map_err(|_| ContractError::InvalidDefinition)?;
    if bytes.len() > CONTRACT_MAX_BYTES {
        Err(ContractError::LimitExceeded)
    } else {
        Ok(bytes)
    }
}

/// Decodes strict canonical Document Schema JSON and rejects alternate encodings.
///
/// # Errors
///
/// Rejects malformed, noncanonical, unknown-field, invalid, or oversized bytes.
pub fn decode_document_schema(bytes: &[u8]) -> Result<DocumentSchemaV1, ContractError> {
    if bytes.len() > CONTRACT_MAX_BYTES {
        return Err(ContractError::LimitExceeded);
    }
    let schema = serde_json::from_slice::<DocumentSchemaV1>(bytes)
        .map_err(|_| ContractError::InvalidEncoding)?;
    schema.validate_definition()?;
    if encode_document_schema(&schema)? != bytes {
        return Err(ContractError::InvalidEncoding);
    }
    Ok(schema)
}

fn validate_bounds<T: PartialOrd>(
    minimum: Option<T>,
    maximum: Option<T>,
) -> Result<(), ContractError> {
    if matches!((minimum, maximum), (Some(left), Some(right)) if left > right) {
        Err(ContractError::InvalidDefinition)
    } else {
        Ok(())
    }
}

fn validate_value_bounds<T: PartialOrd>(
    value: &T,
    minimum: Option<T>,
    maximum: Option<T>,
) -> Result<(), ValidationError> {
    if minimum.is_some_and(|minimum| value < &minimum)
        || maximum.is_some_and(|maximum| value > &maximum)
    {
        Err(ValidationError::BoundViolation)
    } else {
        Ok(())
    }
}

fn validate_length(
    length: usize,
    minimum: Option<u32>,
    maximum: Option<u32>,
) -> Result<(), ValidationError> {
    let length = u64::try_from(length).unwrap_or(u64::MAX);
    if minimum.is_some_and(|minimum| length < u64::from(minimum))
        || maximum.is_some_and(|maximum| length > u64::from(maximum))
    {
        Err(ValidationError::BoundViolation)
    } else {
        Ok(())
    }
}

fn valid_field_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= NAME_MAX_BYTES && !name.contains('\0')
}

fn valid_logical_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_alphanumeric() || (index > 0 && byte == b'_'))
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
}
