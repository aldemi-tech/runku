//! Deterministic decoder/matcher throughput baseline.

use std::{collections::BTreeMap, error::Error, hint::black_box, time::Instant};

use runku_core::{DocumentId, IndexId, TableId};
use runku_execution::{DependencyBound, ReadDependency};
use runku_realtime::ChangeImpact;
use runku_value::{CanonicalValue, IndexKey, IndexValue};

const ITERATIONS: u64 = 100_000;

fn main() -> Result<(), Box<dyn Error>> {
    let table = TableId::generate();
    let document = DocumentId::generate();
    let index = IndexId::generate();
    let key = IndexKey::encode(&[
        IndexValue::String("tenant".to_owned()),
        IndexValue::Int64(42),
    ])?;
    let payload = payload(table, document, index, &key);
    let dependency = ReadDependency::Range {
        index_id: index,
        lower: DependencyBound::Inclusive(key.as_bytes().to_vec()),
        upper: DependencyBound::Inclusive(key.as_bytes().to_vec()),
        snapshot_sequence: 1,
    };
    let started = Instant::now();
    let mut matches = 0_u64;
    for _ in 0..ITERATIONS {
        let impact = ChangeImpact::decode(black_box(&payload))?;
        matches += u64::from(black_box(
            impact.invalidates(black_box(std::slice::from_ref(&dependency))),
        ));
    }
    let elapsed = started.elapsed();
    let per_second = u128::from(ITERATIONS)
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed.as_nanos())
        .unwrap_or(0);
    println!(
        "realtime_baseline iterations={ITERATIONS} matches={matches} elapsed_us={} ops_per_second={per_second}",
        elapsed.as_micros()
    );
    Ok(())
}

fn payload(table: TableId, document: DocumentId, index: IndexId, key: &IndexKey) -> CanonicalValue {
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
                    CanonicalValue::String("replace".to_owned()),
                ),
                (
                    "tableId".to_owned(),
                    CanonicalValue::String(table.to_string()),
                ),
            ]))]),
        ),
    ]))
}
