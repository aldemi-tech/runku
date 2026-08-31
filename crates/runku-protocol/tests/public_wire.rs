//! Public protocol v1 golden and adversarial conformance.

use std::{collections::BTreeMap, error::Error};

use runku_protocol::{
    ActionCallV1, ErrorClassV1, MutationCallV1, PUBLIC_ENVELOPE_MAX_BYTES, ProtocolError,
    PublicErrorV1, QueryCallV1, SuccessMetadataV1, decode_action_call_v1, decode_error_v1,
    decode_mutation_call_v1, decode_query_call_v1, decode_success_v1, encode_action_call_v1,
    encode_error_v1, encode_mutation_call_v1, encode_query_call_v1, encode_success_v1,
};
use runku_value::{CanonicalValue, FiniteF64, TimestampMicros, TypedId};
use serde_json::{Value, json};

const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const RELEASE: &str = "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const REQUEST: &str = "req_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const OPERATION: &str = "opn_01ARZ3NDEKTSV4RRFFQ69G5FAV";

fn golden() -> Result<Value, serde_json::Error> {
    serde_json::from_slice(include_bytes!(
        "../../../protocol/v1/public-wire-vectors.json"
    ))
}

fn all_values() -> Result<CanonicalValue, Box<dyn Error>> {
    let mut object = BTreeMap::new();
    object.insert(
        "array".to_owned(),
        CanonicalValue::Array(vec![CanonicalValue::Null, CanonicalValue::Boolean(true)]),
    );
    object.insert("bytes".to_owned(), CanonicalValue::Bytes(vec![0, 255]));
    object.insert(
        "float".to_owned(),
        CanonicalValue::Float64(FiniteF64::new(1.5)?),
    );
    object.insert(
        "id".to_owned(),
        CanonicalValue::TypedId(format!("doc_{ULID}").parse::<TypedId>()?),
    );
    object.insert("int".to_owned(), CanonicalValue::Int64(i64::MIN));
    object.insert(
        "string".to_owned(),
        CanonicalValue::String("Runku".to_owned()),
    );
    object.insert(
        "time".to_owned(),
        CanonicalValue::Timestamp(TimestampMicros::new(1_700_000_000_123_456)),
    );
    Ok(CanonicalValue::Object(object))
}

#[test]
fn query_and_every_value_match_golden_and_round_trip() -> Result<(), Box<dyn Error>> {
    let call = QueryCallV1 {
        target: format!("release:{RELEASE}").parse()?,
        function: "values.roundtrip".parse()?,
        arguments: all_values()?,
    };
    let encoded = encode_query_call_v1(&call)?;
    assert_eq!(
        serde_json::from_slice::<Value>(&encoded)?,
        golden()?["queryCall"]
    );
    assert_eq!(decode_query_call_v1(&encoded)?, call);
    assert_eq!(
        encode_query_call_v1(&decode_query_call_v1(&encoded)?)?,
        encoded
    );
    Ok(())
}

#[test]
fn distinct_calls_and_success_metadata_round_trip() -> Result<(), Box<dyn Error>> {
    let mutation = MutationCallV1 {
        target: "channel:stable".parse()?,
        function: "users.create".parse()?,
        arguments: CanonicalValue::Boolean(true),
        operation_id: OPERATION.parse()?,
    };
    assert_eq!(
        decode_mutation_call_v1(&encode_mutation_call_v1(&mutation)?)?,
        mutation
    );
    let action = ActionCallV1 {
        target: "workspace:dev/manuel".parse()?,
        function: "email.send".parse()?,
        arguments: CanonicalValue::Null,
    };
    assert_eq!(
        decode_action_call_v1(&encode_action_call_v1(&action)?)?,
        action
    );

    for metadata in [
        SuccessMetadataV1::Query {
            snapshot_sequence: None,
        },
        SuccessMetadataV1::Mutation {
            commit_sequence: Some(9),
            replayed: true,
            attempts: 2,
        },
        SuccessMetadataV1::Action {
            schedules_created: 3,
        },
    ] {
        let encoded = encode_success_v1(
            REQUEST.parse()?,
            RELEASE.parse()?,
            &CanonicalValue::Int64(7),
            metadata,
        )?;
        let decoded = decode_success_v1(&encoded)?;
        assert_eq!(decoded.metadata, metadata);
        assert_eq!(decoded.result, CanonicalValue::Int64(7));
    }
    Ok(())
}

#[test]
fn success_and_error_envelopes_match_golden() -> Result<(), Box<dyn Error>> {
    let success = encode_success_v1(
        REQUEST.parse()?,
        RELEASE.parse()?,
        &CanonicalValue::String("ready".to_owned()),
        SuccessMetadataV1::Query {
            snapshot_sequence: Some(42),
        },
    )?;
    assert_eq!(
        serde_json::from_slice::<Value>(&success)?,
        golden()?["querySuccess"]
    );
    assert_eq!(decode_success_v1(&success)?.request_id.to_string(), REQUEST);

    let public = PublicErrorV1::new(ErrorClassV1::Forbidden, "AUTH_POLICY_DENIED", false)?;
    assert_eq!(public.http_status(), 403);
    let error = encode_error_v1(REQUEST.parse()?, public)?;
    assert_eq!(serde_json::from_slice::<Value>(&error)?, golden()?["error"]);
    let decoded = decode_error_v1(&error)?;
    assert_eq!(decoded.code, "AUTH_POLICY_DENIED");
    assert!(!decoded.retryable);
    Ok(())
}

#[test]
fn request_envelopes_are_strict_versioned_and_bounded() {
    for invalid in [
        json!({}),
        json!({
            "version": 2, "target": format!("release:{RELEASE}"),
            "function": "valid", "arguments": {"type": "null"}
        }),
        json!({
            "version": 1, "target": "latest", "function": "valid",
            "arguments": {"type": "null"}
        }),
        json!({
            "version": 1, "target": format!("release:{RELEASE}"),
            "function": "9invalid", "arguments": {"type": "null"}
        }),
        json!({
            "version": 1, "target": format!("release:{RELEASE}"),
            "function": "valid", "arguments": {"type": "null"}, "extra": true
        }),
    ] {
        assert!(decode_query_call_v1(&serde_json::to_vec(&invalid).unwrap_or_default()).is_err());
    }
    let duplicate = format!(
        "{{\"version\":1,\"version\":1,\"target\":\"release:{RELEASE}\",\"function\":\"valid\",\"arguments\":{{\"type\":\"null\"}}}}"
    );
    assert_eq!(
        decode_query_call_v1(duplicate.as_bytes()),
        Err(ProtocolError::InvalidRequest)
    );
    assert_eq!(
        decode_query_call_v1(&vec![b' '; PUBLIC_ENVELOPE_MAX_BYTES + 1]),
        Err(ProtocolError::LimitExceeded)
    );
    assert_eq!(
        decode_mutation_call_v1(
            &serde_json::to_vec(&json!({
                "version": 1,
                "target": format!("release:{RELEASE}"),
                "function": "valid",
                "arguments": {"type": "null"}
            }))
            .unwrap_or_default()
        ),
        Err(ProtocolError::InvalidRequest)
    );
    assert_eq!(
        decode_action_call_v1(b"{"),
        Err(ProtocolError::InvalidRequest)
    );
}

#[test]
fn noncanonical_or_ambiguous_values_fail_closed() -> Result<(), Box<dyn Error>> {
    for arguments in [
        json!({"type": "int64", "value": "01"}),
        json!({"type": "int64", "value": "-0"}),
        json!({"type": "float64", "value": "3FF8000000000000"}),
        json!({"type": "float64", "value": "7ff8000000000000"}),
        json!({"type": "float64", "value": "8000000000000000"}),
        json!({"type": "bytes", "value": "AP8="}),
        json!({"type": "typed_id", "value": "doc_01arz3ndektsv4rrffq69g5fav"}),
        json!({"type": "object", "value": [
            {"key": "b", "value": {"type": "null"}},
            {"key": "a", "value": {"type": "null"}}
        ]}),
        json!({"type": "object", "value": [
            {"key": "a", "value": {"type": "null"}},
            {"key": "a", "value": {"type": "null"}}
        ]}),
        json!({"type": "null", "value": false}),
    ] {
        let request = json!({
            "version": 1,
            "target": format!("release:{RELEASE}"),
            "function": "valid",
            "arguments": arguments
        });
        assert_eq!(
            decode_query_call_v1(&serde_json::to_vec(&request).unwrap_or_default()),
            Err(ProtocolError::InvalidRequest)
        );
    }

    let mut nested = json!({"type": "null"});
    for _ in 0..=65 {
        nested = json!({"type": "array", "value": [nested]});
    }
    let deep_request = json!({
        "version": 1, "target": format!("release:{RELEASE}"),
        "function": "valid", "arguments": nested
    });
    assert!(decode_query_call_v1(&serde_json::to_vec(&deep_request).unwrap_or_default()).is_err());

    let mut deep_value = CanonicalValue::Null;
    for _ in 0..=65 {
        deep_value = CanonicalValue::Array(vec![deep_value]);
    }
    let deep_call = QueryCallV1 {
        target: format!("release:{RELEASE}").parse()?,
        function: "valid".parse()?,
        arguments: deep_value,
    };
    assert_eq!(
        encode_query_call_v1(&deep_call),
        Err(ProtocolError::LimitExceeded)
    );

    let oversized_array = vec![json!({"type": "null"}); 10_001];
    let item_request = json!({
        "version": 1, "target": format!("release:{RELEASE}"),
        "function": "valid",
        "arguments": {"type": "array", "value": oversized_array}
    });
    assert_eq!(
        decode_query_call_v1(&serde_json::to_vec(&item_request).unwrap_or_default()),
        Err(ProtocolError::LimitExceeded)
    );
    Ok(())
}

#[test]
fn error_codes_messages_and_response_metadata_are_validated() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        PublicErrorV1::new(ErrorClassV1::Internal, "bad-code", false),
        Err(ProtocolError::InvalidResponse)
    );
    assert_eq!(
        encode_success_v1(
            REQUEST.parse()?,
            RELEASE.parse()?,
            &CanonicalValue::Null,
            SuccessMetadataV1::Mutation {
                commit_sequence: None,
                replayed: false,
                attempts: 0,
            },
        ),
        Err(ProtocolError::InvalidResponse)
    );
    let bad_error = json!({
        "version": 1,
        "status": "error",
        "requestId": REQUEST,
        "error": {"code": "bad", "message": "unsafe\nmessage", "retryable": false}
    });
    assert_eq!(
        decode_error_v1(&serde_json::to_vec(&bad_error)?),
        Err(ProtocolError::InvalidResponse)
    );
    assert_eq!(decode_success_v1(b"{"), Err(ProtocolError::InvalidResponse));

    for invalid_metadata in [
        json!({"kind": "query", "snapshotSequence": 42}),
        json!({"kind": "query", "snapshotSequence": "01"}),
        json!({"kind": "query", "snapshotSequence": "18446744073709551616"}),
        json!({"kind": "action", "schedulesCreated": "-1"}),
        json!({"kind": "mutation", "commitSequence": null, "replayed": false, "attempts": 1, "extra": true}),
    ] {
        let response = json!({
            "version": 1,
            "status": "ok",
            "requestId": REQUEST,
            "releaseId": RELEASE,
            "result": {"type": "null"},
            "metadata": invalid_metadata,
        });
        assert_eq!(
            decode_success_v1(&serde_json::to_vec(&response)?),
            Err(ProtocolError::InvalidResponse)
        );
    }

    let maximum = json!({
        "version": 1,
        "status": "ok",
        "requestId": REQUEST,
        "releaseId": RELEASE,
        "result": {"type": "null"},
        "metadata": {"kind": "action", "schedulesCreated": u64::MAX.to_string()},
    });
    assert_eq!(
        decode_success_v1(&serde_json::to_vec(&maximum)?)?.metadata,
        SuccessMetadataV1::Action {
            schedules_created: u64::MAX
        }
    );
    Ok(())
}
