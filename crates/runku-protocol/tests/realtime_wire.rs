//! Realtime WebSocket protocol v1 golden and adversarial conformance.

use std::error::Error;

use runku_protocol::{
    ProtocolError, REALTIME_MESSAGE_MAX_BYTES, RealtimeClientMessageV1, RealtimeServerMessageV1,
    decode_realtime_client_v1, decode_realtime_server_v1, encode_realtime_server_v1,
};
use runku_releases::Sha256Digest;
use runku_value::{CanonicalValue, TimestampMicros};
use serde_json::{Value, json};

const RELEASE: &str = "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const REQUEST: &str = "req_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const SUBSCRIPTION: &str = "sub_01ARZ3NDEKTSV4RRFFQ69G5FAV";

fn golden() -> Result<Value, serde_json::Error> {
    serde_json::from_slice(include_bytes!("../../../protocol/v1/realtime-vectors.json"))
}

#[test]
fn client_commands_decode_strictly_and_redact_credentials() -> Result<(), Box<dyn Error>> {
    let authenticate = serde_json::to_vec(&golden()?["authenticate"])?;
    let decoded = decode_realtime_client_v1(&authenticate)?;
    match &decoded {
        RealtimeClientMessageV1::Authenticate {
            request_id,
            credentials,
        } => {
            assert_eq!(request_id.to_string(), REQUEST);
            assert_eq!(
                credentials.application_key.as_deref(),
                Some("rk_pub_example")
            );
            assert_eq!(credentials.bearer, None);
        }
        _ => return Err("unexpected command".into()),
    }
    let debug = format!("{decoded:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("rk_pub_example"));

    match decode_realtime_client_v1(&serde_json::to_vec(&golden()?["subscribe"])?)? {
        RealtimeClientMessageV1::Subscribe {
            target,
            function,
            arguments,
            ..
        } => {
            assert_eq!(target.to_string(), "channel:stable");
            assert_eq!(function.to_string(), "messages.list");
            assert_eq!(arguments, CanonicalValue::Null);
        }
        _ => return Err("unexpected command".into()),
    }
    Ok(())
}

#[test]
fn server_state_and_resync_match_golden_and_round_trip() -> Result<(), Box<dyn Error>> {
    let state = RealtimeServerMessageV1::State {
        request_id: Some(REQUEST.parse()?),
        subscription_id: SUBSCRIPTION.parse()?,
        release_id: RELEASE.parse()?,
        delivery_revision: 1,
        value: CanonicalValue::String("ready".to_owned()),
        result_hash: Sha256Digest::from_bytes([0; 32]),
        snapshot_sequence: Some(42),
        authorized_until: TimestampMicros::new(1_700_000_000_123_456),
    };
    let bytes = encode_realtime_server_v1(&state)?;
    assert_eq!(serde_json::from_slice::<Value>(&bytes)?, golden()?["state"]);
    assert_eq!(decode_realtime_server_v1(&bytes)?, state);

    let resync = RealtimeServerMessageV1::ResyncRequired {
        subscription_id: SUBSCRIPTION.parse()?,
        code: "REALTIME_DELIVERY_LAGGED".to_owned(),
    };
    let bytes = encode_realtime_server_v1(&resync)?;
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes)?,
        golden()?["resyncRequired"]
    );
    assert_eq!(decode_realtime_server_v1(&bytes)?, resync);
    Ok(())
}

#[test]
fn malformed_unknown_oversized_and_noncanonical_fields_fail_closed() {
    for value in [
        json!({}),
        json!({"type":"ping","version":2,"requestId":REQUEST}),
        json!({"type":"ping","version":1,"requestId":REQUEST,"extra":true}),
        json!({"type":"authenticate","version":1,"requestId":REQUEST,"applicationKey":"","bearer":null}),
        json!({"type":"subscribe","version":1,"requestId":REQUEST,"target":"latest","function":"valid","arguments":{"type":"null"}}),
    ] {
        assert!(
            decode_realtime_client_v1(&serde_json::to_vec(&value).unwrap_or_default()).is_err()
        );
    }
    assert!(matches!(
        decode_realtime_client_v1(&vec![b'x'; REALTIME_MESSAGE_MAX_BYTES + 1]),
        Err(ProtocolError::LimitExceeded)
    ));
    for value in [
        json!({"type":"state","version":1,"requestId":null,"subscriptionId":SUBSCRIPTION,"releaseId":RELEASE,"deliveryRevision":"01","value":{"type":"null"},"resultHash":"0000000000000000000000000000000000000000000000000000000000000000","snapshotSequence":null,"authorizedUntilMicros":"1"}),
        json!({"type":"resync_required","version":1,"subscriptionId":SUBSCRIPTION,"code":"not_stable"}),
    ] {
        assert_eq!(
            decode_realtime_server_v1(&serde_json::to_vec(&value).unwrap_or_default()),
            Err(ProtocolError::InvalidResponse)
        );
    }
}
