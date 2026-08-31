//! Realtime durable impact decoder and dependency matcher conformance.

use std::{collections::BTreeMap, error::Error};

use proptest::prelude::*;
use runku_core::{DocumentId, IndexId, TableId};
use runku_execution::{DependencyBound, ReadDependency};
use runku_realtime::{ChangeImpact, RealtimeError};
use runku_value::{CanonicalValue, IndexKey, IndexValue};

fn payload(key: &IndexKey) -> CanonicalValue {
    let table = TableId::generate();
    let document = DocumentId::generate();
    let index = IndexId::generate();
    CanonicalValue::Object(BTreeMap::from([
        (
            "indexes".to_owned(),
            CanonicalValue::Array(vec![CanonicalValue::Object(BTreeMap::from([
                (
                    "documentId".to_owned(),
                    CanonicalValue::String(document.to_string()),
                ),
                (
                    "indexId".to_owned(),
                    CanonicalValue::String(index.to_string()),
                ),
                (
                    "key".to_owned(),
                    CanonicalValue::Bytes(key.as_bytes().to_vec()),
                ),
                ("kind".to_owned(), CanonicalValue::String("put".to_owned())),
            ]))]),
        ),
        (
            "type".to_owned(),
            CanonicalValue::String("document_write_set_v2".to_owned()),
        ),
        (
            "writes".to_owned(),
            CanonicalValue::Array(vec![CanonicalValue::Object(BTreeMap::from([
                (
                    "documentId".to_owned(),
                    CanonicalValue::String(document.to_string()),
                ),
                (
                    "kind".to_owned(),
                    CanonicalValue::String("insert".to_owned()),
                ),
                (
                    "tableId".to_owned(),
                    CanonicalValue::String(table.to_string()),
                ),
            ]))]),
        ),
    ]))
}

#[test]
fn strict_v2_decode_matches_point_and_range() -> Result<(), Box<dyn Error>> {
    let key = IndexKey::encode(&[IndexValue::Int64(42)])?;
    let impact = ChangeImpact::decode(&payload(&key))?;
    let document = impact.documents()[0];
    assert!(impact.invalidates(&[ReadDependency::Point {
        table_id: document.table_id,
        document_id: document.document_id,
        observed_revision: None,
        snapshot_sequence: 1,
    }]));
    let indexed = &impact.indexes()[0];
    assert!(impact.invalidates(&[ReadDependency::Range {
        index_id: indexed.index_id,
        lower: DependencyBound::Inclusive(key.as_bytes().to_vec()),
        upper: DependencyBound::Inclusive(key.as_bytes().to_vec()),
        snapshot_sequence: 1,
    }]));
    assert!(!impact.invalidates(&[ReadDependency::Range {
        index_id: indexed.index_id,
        lower: DependencyBound::Exclusive(key.as_bytes().to_vec()),
        upper: DependencyBound::Unbounded,
        snapshot_sequence: 1,
    }]));
    Ok(())
}

#[test]
fn unknown_version_extra_fields_and_noncanonical_keys_fail() -> Result<(), Box<dyn Error>> {
    let key = IndexKey::encode(&[IndexValue::String("x".to_owned())])?;
    let mut wrong_version = payload(&key);
    if let CanonicalValue::Object(object) = &mut wrong_version {
        object.insert(
            "type".to_owned(),
            CanonicalValue::String("document_write_set_v3".to_owned()),
        );
    } else {
        return Err("expected object".into());
    }
    assert_eq!(
        ChangeImpact::decode(&wrong_version),
        Err(RealtimeError::InvalidImpact)
    );
    if let CanonicalValue::Object(object) = &mut wrong_version {
        object.insert("extra".to_owned(), CanonicalValue::Null);
    }
    assert_eq!(
        ChangeImpact::decode(&wrong_version),
        Err(RealtimeError::InvalidImpact)
    );
    assert!(!RealtimeError::InvalidImpact.retryable());
    Ok(())
}

proptest! {
    #[test]
    fn inclusive_and_exclusive_range_edges_are_exact(value in any::<i64>()) {
        let key = IndexKey::encode(&[IndexValue::Int64(value)])
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let impact = ChangeImpact::decode(&payload(&key))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let index_id = impact.indexes()[0].index_id;
        let inclusive = ReadDependency::Range {
            index_id,
            lower: DependencyBound::Inclusive(key.as_bytes().to_vec()),
            upper: DependencyBound::Inclusive(key.as_bytes().to_vec()),
            snapshot_sequence: 1,
        };
        let exclusive = ReadDependency::Range {
            index_id,
            lower: DependencyBound::Exclusive(key.as_bytes().to_vec()),
            upper: DependencyBound::Unbounded,
            snapshot_sequence: 1,
        };
        prop_assert!(impact.invalidates(&[inclusive]));
        prop_assert!(!impact.invalidates(&[exclusive]));
    }
}
