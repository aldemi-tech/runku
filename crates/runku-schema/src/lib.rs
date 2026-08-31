//! Canonical schema catalog and deterministic logical-index extraction.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use runku_core::{IndexId, ProjectId, TableId};
use runku_value::{CanonicalValue, IndexKey, IndexValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

const CATALOG_RESOURCE_MAGIC: &[u8] = b"RUNKU_INDEX_CATALOG_RESOURCE_V1\n";
const CATALOG_RESOURCE_MAX_BYTES: usize = 1024 * 1024;
const MAX_INDEXES: usize = 1_000;
const MAX_FIELDS: usize = 16;
const MAX_PATH_SEGMENTS: usize = 16;
const MAX_NAME_BYTES: usize = 64;

/// Stable schema/index validation or extraction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchemaError {
    /// Catalog contains duplicate identities/names or non-canonical ordering.
    #[error("schema catalog is invalid")]
    InvalidCatalog,
    /// An index name or field path is malformed.
    #[error("schema path or name is invalid")]
    InvalidName,
    /// A schema/index count or encoded length exceeds v1 limits.
    #[error("schema catalog exceeds a v1 limit")]
    LimitExceeded,
    /// A field reached an Object/Array where an indexable scalar was required.
    #[error("indexed field has an unsupported value type")]
    UnsupportedValue,
}

impl SchemaError {
    /// Stable public machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidCatalog => "SCHEMA_CATALOG_INVALID",
            Self::InvalidName => "SCHEMA_NAME_INVALID",
            Self::LimitExceeded => "SCHEMA_LIMIT_EXCEEDED",
            Self::UnsupportedValue => "INDEX_VALUE_UNSUPPORTED",
        }
    }

    /// Schema failures are deterministic for unchanged inputs.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

/// Validated object-property path used by one index component.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FieldPath(Vec<String>);

impl FieldPath {
    /// Validates a non-empty v1 property path.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::InvalidName`] for unsafe segments and
    /// [`SchemaError::LimitExceeded`] beyond v1 segment limits.
    pub fn new(segments: Vec<String>) -> Result<Self, SchemaError> {
        if segments.is_empty() || segments.len() > MAX_PATH_SEGMENTS {
            return Err(SchemaError::LimitExceeded);
        }
        if segments.iter().any(|segment| !valid_name(segment)) {
            return Err(SchemaError::InvalidName);
        }
        Ok(Self(segments))
    }

    /// Returns validated path segments.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.0
    }
}

/// One immutable logical index definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDefinition {
    index_id: IndexId,
    table_id: TableId,
    name: String,
    fields: Vec<FieldPath>,
}

impl IndexDefinition {
    /// Creates a validated compound sparse index definition.
    ///
    /// # Errors
    ///
    /// Rejects invalid names and field counts outside `1..=16`.
    pub fn new(
        index_id: IndexId,
        table_id: TableId,
        name: String,
        fields: Vec<FieldPath>,
    ) -> Result<Self, SchemaError> {
        if !valid_name(&name) {
            return Err(SchemaError::InvalidName);
        }
        if fields.is_empty() || fields.len() > MAX_FIELDS {
            return Err(SchemaError::LimitExceeded);
        }
        let unique = fields.iter().collect::<BTreeSet<_>>();
        if unique.len() != fields.len() {
            return Err(SchemaError::InvalidCatalog);
        }
        Ok(Self {
            index_id,
            table_id,
            name,
            fields,
        })
    }

    /// Stable logical index ID.
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Table whose documents are indexed.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Canonical logical name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Ordered compound component paths.
    #[must_use]
    pub fn fields(&self) -> &[FieldPath] {
        &self.fields
    }
}

/// Immutable canonical collection of active index definitions for one Project.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaCatalog {
    project_id: ProjectId,
    indexes: Vec<IndexDefinition>,
    by_table: BTreeMap<TableId, Vec<usize>>,
    digest: [u8; 32],
}

impl SchemaCatalog {
    /// Validates, canonicalizes and hashes active index definitions.
    ///
    /// # Errors
    ///
    /// Rejects duplicate IDs/names or more than 1,000 definitions.
    pub fn new(
        project_id: ProjectId,
        mut indexes: Vec<IndexDefinition>,
    ) -> Result<Self, SchemaError> {
        if indexes.len() > MAX_INDEXES {
            return Err(SchemaError::LimitExceeded);
        }
        indexes.sort_by_key(IndexDefinition::index_id);
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut by_table: BTreeMap<TableId, Vec<usize>> = BTreeMap::new();
        for (position, index) in indexes.iter().enumerate() {
            if !ids.insert(index.index_id) || !names.insert((index.table_id, index.name.clone())) {
                return Err(SchemaError::InvalidCatalog);
            }
            by_table.entry(index.table_id).or_default().push(position);
        }
        let mut catalog = Self {
            project_id,
            indexes,
            by_table,
            digest: [0; 32],
        };
        catalog.digest = Sha256::digest(encode_schema_catalog(&catalog)?).into();
        Ok(catalog)
    }

    /// Owning Project.
    #[must_use]
    pub const fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// Canonically ordered definitions.
    #[must_use]
    pub fn indexes(&self) -> &[IndexDefinition] {
        &self.indexes
    }

    /// Canonical definitions for one table.
    pub fn indexes_for_table(&self, table_id: TableId) -> impl Iterator<Item = &IndexDefinition> {
        self.by_table
            .get(&table_id)
            .into_iter()
            .flatten()
            .map(|position| &self.indexes[*position])
    }

    /// Canonical SHA-256 digest of the v1 index contract.
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Encodes one validated catalog into its strict canonical Release resource.
///
/// # Errors
///
/// Returns a limit error if the resource exceeds the v1 bound.
pub fn encode_schema_catalog(catalog: &SchemaCatalog) -> Result<Vec<u8>, SchemaError> {
    let mut output = Vec::new();
    output.extend_from_slice(CATALOG_RESOURCE_MAGIC);
    push_text(&mut output, &catalog.project_id.to_string())?;
    push_count(&mut output, catalog.indexes.len())?;
    for index in &catalog.indexes {
        push_text(&mut output, &index.index_id.to_string())?;
        push_text(&mut output, &index.table_id.to_string())?;
        push_text(&mut output, &index.name)?;
        push_count(&mut output, index.fields.len())?;
        for path in &index.fields {
            push_count(&mut output, path.0.len())?;
            for segment in &path.0 {
                push_text(&mut output, segment)?;
            }
        }
    }
    if output.len() > CATALOG_RESOURCE_MAX_BYTES {
        return Err(SchemaError::LimitExceeded);
    }
    Ok(output)
}

/// Decodes and canonicality-checks one catalog Release resource.
///
/// # Errors
///
/// Rejects malformed, oversized, trailing, noncanonical, or semantically invalid bytes.
pub fn decode_schema_catalog(bytes: &[u8]) -> Result<SchemaCatalog, SchemaError> {
    if bytes.len() > CATALOG_RESOURCE_MAX_BYTES || !bytes.starts_with(CATALOG_RESOURCE_MAGIC) {
        return Err(SchemaError::InvalidCatalog);
    }
    let mut cursor = CatalogCursor {
        bytes,
        offset: CATALOG_RESOURCE_MAGIC.len(),
    };
    let project_id = cursor
        .text()?
        .parse::<ProjectId>()
        .map_err(|_| SchemaError::InvalidCatalog)?;
    let count = cursor.count()?;
    if count > MAX_INDEXES {
        return Err(SchemaError::LimitExceeded);
    }
    let mut indexes = Vec::with_capacity(count);
    for _ in 0..count {
        let index_id = cursor
            .text()?
            .parse::<IndexId>()
            .map_err(|_| SchemaError::InvalidCatalog)?;
        let table_id = cursor
            .text()?
            .parse::<TableId>()
            .map_err(|_| SchemaError::InvalidCatalog)?;
        let name = cursor.text()?;
        let field_count = cursor.count()?;
        if field_count > MAX_FIELDS {
            return Err(SchemaError::LimitExceeded);
        }
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let segment_count = cursor.count()?;
            if segment_count > MAX_PATH_SEGMENTS {
                return Err(SchemaError::LimitExceeded);
            }
            let segments = (0..segment_count)
                .map(|_| cursor.text())
                .collect::<Result<Vec<_>, _>>()?;
            fields.push(FieldPath::new(segments)?);
        }
        indexes.push(IndexDefinition::new(index_id, table_id, name, fields)?);
    }
    if cursor.offset != bytes.len() {
        return Err(SchemaError::InvalidCatalog);
    }
    let catalog = SchemaCatalog::new(project_id, indexes)?;
    if encode_schema_catalog(&catalog)?.as_slice() != bytes {
        return Err(SchemaError::InvalidCatalog);
    }
    Ok(catalog)
}

fn push_count(output: &mut Vec<u8>, count: usize) -> Result<(), SchemaError> {
    let count = u32::try_from(count).map_err(|_| SchemaError::LimitExceeded)?;
    output.extend_from_slice(format!("{count:08x}").as_bytes());
    Ok(())
}

fn push_text(output: &mut Vec<u8>, value: &str) -> Result<(), SchemaError> {
    push_count(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    if output.len() > CATALOG_RESOURCE_MAX_BYTES {
        return Err(SchemaError::LimitExceeded);
    }
    Ok(())
}

struct CatalogCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl CatalogCursor<'_> {
    fn count(&mut self) -> Result<usize, SchemaError> {
        let end = self
            .offset
            .checked_add(8)
            .ok_or(SchemaError::LimitExceeded)?;
        let raw = std::str::from_utf8(
            self.bytes
                .get(self.offset..end)
                .ok_or(SchemaError::InvalidCatalog)?,
        )
        .map_err(|_| SchemaError::InvalidCatalog)?;
        if !raw
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(SchemaError::InvalidCatalog);
        }
        self.offset = end;
        usize::try_from(u32::from_str_radix(raw, 16).map_err(|_| SchemaError::InvalidCatalog)?)
            .map_err(|_| SchemaError::LimitExceeded)
    }

    fn text(&mut self) -> Result<String, SchemaError> {
        let length = self.count()?;
        let end = self
            .offset
            .checked_add(length)
            .ok_or(SchemaError::LimitExceeded)?;
        let value = std::str::from_utf8(
            self.bytes
                .get(self.offset..end)
                .ok_or(SchemaError::InvalidCatalog)?,
        )
        .map_err(|_| SchemaError::InvalidCatalog)?
        .to_owned();
        self.offset = end;
        Ok(value)
    }
}

/// Extracts a sparse compound Index Key v1 from one document.
///
/// # Errors
///
/// Returns [`SchemaError::UnsupportedValue`] for an invalid intermediate/leaf type and a limit
/// error when Index Key v1 rejects the result. A missing property returns `Ok(None)`.
pub fn extract_index_key(
    definition: &IndexDefinition,
    document: &CanonicalValue,
) -> Result<Option<IndexKey>, SchemaError> {
    let mut values = Vec::with_capacity(definition.fields.len());
    for path in &definition.fields {
        let Some(value) = resolve(document, path)? else {
            return Ok(None);
        };
        values.push(IndexValue::try_from(value).map_err(|_| SchemaError::UnsupportedValue)?);
    }
    IndexKey::encode(&values)
        .map(Some)
        .map_err(|_| SchemaError::LimitExceeded)
}

fn resolve<'a>(
    root: &'a CanonicalValue,
    path: &FieldPath,
) -> Result<Option<&'a CanonicalValue>, SchemaError> {
    let mut current = root;
    for segment in &path.0 {
        let CanonicalValue::Object(object) = current else {
            return Err(SchemaError::UnsupportedValue);
        };
        let Some(next) = object.get(segment) else {
            return Ok(None);
        };
        current = next;
    }
    Ok(Some(current))
}

fn valid_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    value.len() <= MAX_NAME_BYTES
        && (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fmt::Write};

    use proptest::prelude::*;
    use runku_core::{IndexId, ProjectId, TableId};
    use runku_value::{CanonicalValue, IndexValue};
    use ulid::Ulid;

    use super::*;

    fn path(segments: &[&str]) -> Result<FieldPath, SchemaError> {
        FieldPath::new(segments.iter().map(|value| (*value).to_owned()).collect())
    }

    #[test]
    fn nested_sparse_null_and_unsupported_are_distinct() -> Result<(), Box<dyn Error>> {
        let definition = IndexDefinition::new(
            IndexId::from_ulid(Ulid::from(1_u128)),
            TableId::from_ulid(Ulid::from(2_u128)),
            "by_team_age".to_owned(),
            vec![path(&["team"])?, path(&["profile", "age"])?],
        )?;
        let document = CanonicalValue::Object(BTreeMap::from([
            ("team".to_owned(), CanonicalValue::Null),
            (
                "profile".to_owned(),
                CanonicalValue::Object(BTreeMap::from([(
                    "age".to_owned(),
                    CanonicalValue::Int64(42),
                )])),
            ),
        ]));
        let key = extract_index_key(&definition, &document)?.ok_or("expected key")?;
        assert_eq!(key.components(), &[IndexValue::Null, IndexValue::Int64(42)]);

        let sparse = CanonicalValue::Object(BTreeMap::new());
        assert_eq!(extract_index_key(&definition, &sparse)?, None);
        let invalid = CanonicalValue::Object(BTreeMap::from([(
            "team".to_owned(),
            CanonicalValue::Array(vec![]),
        )]));
        assert_eq!(
            extract_index_key(&definition, &invalid),
            Err(SchemaError::UnsupportedValue)
        );
        Ok(())
    }

    #[test]
    fn catalog_order_is_canonical_and_duplicates_fail() -> Result<(), Box<dyn Error>> {
        let project = ProjectId::from_ulid(Ulid::from(1_u128));
        let table = TableId::from_ulid(Ulid::from(2_u128));
        let first = IndexDefinition::new(
            IndexId::from_ulid(Ulid::from(4_u128)),
            table,
            "second".to_owned(),
            vec![path(&["value"])?],
        )?;
        let second = IndexDefinition::new(
            IndexId::from_ulid(Ulid::from(3_u128)),
            table,
            "first".to_owned(),
            vec![path(&["value"])?],
        )?;
        let left = SchemaCatalog::new(project, vec![first.clone(), second.clone()])?;
        let right = SchemaCatalog::new(project, vec![second, first])?;
        assert_eq!(left.digest(), right.digest());
        assert_eq!(left, right);
        assert_eq!(left.indexes_for_table(table).count(), 2);
        let encoded = encode_schema_catalog(&left)?;
        assert_eq!(decode_schema_catalog(&encoded)?, left);
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            decode_schema_catalog(&trailing),
            Err(SchemaError::InvalidCatalog)
        );
        Ok(())
    }

    #[test]
    fn unsafe_names_duplicate_contracts_and_complex_leaves_fail_closed()
    -> Result<(), Box<dyn Error>> {
        assert_eq!(
            FieldPath::new(vec!["__proto__-escape".to_owned()]),
            Err(SchemaError::InvalidName)
        );
        let project = ProjectId::from_ulid(Ulid::from(1_u128));
        let table = TableId::from_ulid(Ulid::from(2_u128));
        let id = IndexId::from_ulid(Ulid::from(3_u128));
        let definition =
            IndexDefinition::new(id, table, "by_value".to_owned(), vec![path(&["value"])?])?;
        assert_eq!(
            SchemaCatalog::new(project, vec![definition.clone(), definition.clone()]),
            Err(SchemaError::InvalidCatalog)
        );
        let complex = CanonicalValue::Object(BTreeMap::from([(
            "value".to_owned(),
            CanonicalValue::Object(BTreeMap::new()),
        )]));
        assert_eq!(
            extract_index_key(&definition, &complex),
            Err(SchemaError::UnsupportedValue)
        );
        assert!(!SchemaError::UnsupportedValue.retryable());
        assert_eq!(
            SchemaError::UnsupportedValue.code(),
            "INDEX_VALUE_UNSUPPORTED"
        );
        Ok(())
    }

    #[test]
    fn index_catalog_golden_vector_is_normative() -> Result<(), Box<dyn Error>> {
        let vector: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../protocol/v1/index-catalog-vectors.json"
        )))?;
        assert_eq!(vector["format_version"].as_u64(), Some(1));
        let project = vector["project_id"]
            .as_str()
            .ok_or("missing project")?
            .parse::<ProjectId>()?;
        let mut definitions = Vec::new();
        for raw in vector["indexes"].as_array().ok_or("missing indexes")? {
            let fields = raw["fields"]
                .as_array()
                .ok_or("missing fields")?
                .iter()
                .map(|path| {
                    let segments = path
                        .as_array()
                        .ok_or(SchemaError::InvalidCatalog)?
                        .iter()
                        .map(|segment| {
                            segment
                                .as_str()
                                .map(str::to_owned)
                                .ok_or(SchemaError::InvalidCatalog)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    FieldPath::new(segments)
                })
                .collect::<Result<Vec<_>, _>>()?;
            definitions.push(IndexDefinition::new(
                raw["index_id"]
                    .as_str()
                    .ok_or("missing index ID")?
                    .parse()?,
                raw["table_id"]
                    .as_str()
                    .ok_or("missing table ID")?
                    .parse()?,
                raw["name"].as_str().ok_or("missing name")?.to_owned(),
                fields,
            )?);
        }
        let catalog = SchemaCatalog::new(project, definitions)?;
        let mut actual = String::with_capacity(64);
        for byte in catalog.digest() {
            write!(actual, "{byte:02x}")?;
        }
        assert_eq!(
            actual,
            vector["digest_sha256"].as_str().ok_or("missing digest")?
        );
        Ok(())
    }

    proptest! {
        #[test]
        fn scalar_extraction_matches_index_key_codec(value in any::<i64>()) {
            let definition = IndexDefinition::new(
                IndexId::from_ulid(Ulid::from(1_u128)),
                TableId::from_ulid(Ulid::from(2_u128)),
                "by_value".to_owned(),
                vec![FieldPath::new(vec!["value".to_owned()]).map_err(|error| TestCaseError::fail(error.to_string()))?],
            ).map_err(|error| TestCaseError::fail(error.to_string()))?;
            let document = CanonicalValue::Object(BTreeMap::from([(
                "value".to_owned(),
                CanonicalValue::Int64(value),
            )]));
            let actual = extract_index_key(&definition, &document)
                .map_err(|error| TestCaseError::fail(error.to_string()))?
                .ok_or_else(|| TestCaseError::fail("key missing"))?;
            let expected = IndexKey::encode(&[IndexValue::Int64(value)])
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert_eq!(actual, expected);
        }
    }
}
