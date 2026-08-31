//! Contract and document-schema conformance tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
};

use proptest::prelude::*;
use runku_contracts::{
    Contract, ContractError, DocumentSchemaV1, DocumentTableContract, ValidationError,
    decode_contract, decode_document_schema, encode_contract, encode_document_schema,
};
use runku_core::TableId;
use runku_value::{CanonicalValue, TypedId};
use ulid::Ulid;

type TestResult = Result<(), Box<dyn Error>>;

fn table(seed: u128) -> TableId {
    TableId::from_ulid(Ulid::from(seed))
}

fn string(minimum: Option<u32>, maximum: Option<u32>) -> Contract {
    Contract::String {
        minimum_bytes: minimum,
        maximum_bytes: maximum,
    }
}

#[test]
fn canonical_codec_rejects_alternate_unknown_and_invalid_definitions() -> TestResult {
    let contract = Contract::Object {
        fields: BTreeMap::from([
            (
                "age".to_owned(),
                Contract::Int64 {
                    minimum: Some(0),
                    maximum: Some(150),
                },
            ),
            ("name".to_owned(), string(Some(1), Some(100))),
        ]),
        optional: BTreeSet::from(["age".to_owned()]),
    };
    let bytes = encode_contract(&contract)?;
    assert_eq!(decode_contract(&bytes)?, contract);

    let spaced = format!(" {}", String::from_utf8(bytes.clone())?);
    assert_eq!(
        decode_contract(spaced.as_bytes()),
        Err(ContractError::InvalidEncoding)
    );
    assert_eq!(
        decode_contract(br#"{"type":"boolean","extra":true}"#),
        Err(ContractError::InvalidEncoding)
    );
    assert_eq!(
        encode_contract(&Contract::Array {
            items: Box::new(Contract::Any),
            minimum_items: Some(2),
            maximum_items: Some(1),
        }),
        Err(ContractError::InvalidDefinition)
    );
    assert_eq!(
        encode_contract(&Contract::Union {
            variants: vec![Contract::Null, Contract::Null]
        }),
        Err(ContractError::InvalidDefinition)
    );
    Ok(())
}

#[test]
fn exact_objects_optional_fields_unions_bounds_and_ids_are_enforced() -> TestResult {
    let contract = Contract::Object {
        fields: BTreeMap::from([
            (
                "id".to_owned(),
                Contract::TypedId {
                    kind: Some("doc".to_owned()),
                },
            ),
            (
                "label".to_owned(),
                Contract::Union {
                    variants: vec![Contract::Null, string(Some(1), Some(8))],
                },
            ),
        ]),
        optional: BTreeSet::from(["label".to_owned()]),
    };
    let id: TypedId = "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?;
    let valid = CanonicalValue::Object(BTreeMap::from([(
        "id".to_owned(),
        CanonicalValue::TypedId(id),
    )]));
    assert_eq!(contract.validate_value(&valid), Ok(()));

    assert_eq!(
        contract.validate_value(&CanonicalValue::Object(BTreeMap::new())),
        Err(ValidationError::ObjectShape)
    );
    let unknown = CanonicalValue::Object(BTreeMap::from([
        ("id".to_owned(), valid.clone()),
        ("unknown".to_owned(), CanonicalValue::Null),
    ]));
    assert_eq!(
        contract.validate_value(&unknown),
        Err(ValidationError::ObjectShape)
    );
    assert_eq!(
        string(Some(2), Some(3)).validate_value(&CanonicalValue::String("a".to_owned())),
        Err(ValidationError::BoundViolation)
    );
    Ok(())
}

#[test]
fn document_ids_are_canonical_typed_ids_with_a_valid_table_association() -> TestResult {
    let contract = Contract::DocumentId {
        table: "rooms".to_owned(),
    };
    let document: TypedId = "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?;
    let operation: TypedId = "opn_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse()?;

    assert_eq!(contract.validate_definition(), Ok(()));
    assert_eq!(
        contract.validate_value(&CanonicalValue::TypedId(document)),
        Ok(())
    );
    assert_eq!(
        contract.validate_value(&CanonicalValue::TypedId(operation)),
        Err(ValidationError::BoundViolation)
    );
    assert_eq!(
        Contract::DocumentId {
            table: "not-a-table".to_owned(),
        }
        .validate_definition(),
        Err(ContractError::InvalidDefinition)
    );
    assert_eq!(decode_contract(&encode_contract(&contract)?)?, contract);
    Ok(())
}

#[test]
fn document_schema_is_sorted_unique_canonical_and_fail_closed() -> TestResult {
    let schema = DocumentSchemaV1::new(vec![
        DocumentTableContract {
            id: table(2),
            name: "auditLog".to_owned(),
            document_contract: Contract::Any,
        },
        DocumentTableContract {
            id: table(1),
            name: "users".to_owned(),
            document_contract: Contract::Boolean,
        },
    ])?;
    let bytes = encode_document_schema(&schema)?;
    assert_eq!(decode_document_schema(&bytes)?, schema);
    assert_eq!(schema.tables[0].id, table(1));
    assert_eq!(
        schema.validate_document(table(1), &CanonicalValue::Boolean(true)),
        Ok(())
    );
    assert_eq!(
        schema.validate_document(table(1), &CanonicalValue::Null),
        Err(ValidationError::TypeMismatch)
    );
    assert_eq!(
        schema.validate_document(table(3), &CanonicalValue::Boolean(true)),
        Err(ValidationError::UnknownTable)
    );
    assert!(
        DocumentSchemaV1::new(vec![
            DocumentTableContract {
                id: table(1),
                name: "users".to_owned(),
                document_contract: Contract::Any
            },
            DocumentTableContract {
                id: table(1),
                name: "other".to_owned(),
                document_contract: Contract::Any
            },
        ])
        .is_err()
    );
    Ok(())
}

proptest! {
    #[test]
    fn bounded_string_contract_round_trips_and_matches_length(
        minimum in 0_u32..128,
        width in 0_u32..128,
        value in "[a-z]{0,200}",
    ) {
        let maximum = minimum + width;
        let contract = string(Some(minimum), Some(maximum));
        let bytes = encode_contract(&contract).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(decode_contract(&bytes), Ok(contract.clone()));
        let expected = (u64::from(minimum)..=u64::from(maximum)).contains(&(value.len() as u64));
        prop_assert_eq!(contract.validate_value(&CanonicalValue::String(value)).is_ok(), expected);
    }
}
