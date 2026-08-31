//! Cross-implementation golden vectors for the public Code Target wire format.

use std::{error::Error, str::FromStr};

use runku_core::CodeTarget;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Vectors {
    version: u64,
    valid: Vec<ValidVector>,
    invalid: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ValidVector {
    wire: String,
    kind: String,
}

#[test]
fn code_target_golden_vectors_match_protocol_v1() -> Result<(), Box<dyn Error>> {
    let vectors: Vectors = serde_json::from_str(include_str!(
        "../../../protocol/v1/code-target-vectors.json"
    ))?;
    assert_eq!(vectors.version, 1);

    for vector in vectors.valid {
        let target = CodeTarget::from_str(&vector.wire)?;
        assert_eq!(target.to_string(), vector.wire);
        let actual_kind = match target {
            CodeTarget::Release(_) => "release",
            CodeTarget::Channel(_) => "channel",
            CodeTarget::Workspace(_) => "workspace",
        };
        assert_eq!(actual_kind, vector.kind);
    }

    for wire in vectors.invalid {
        assert!(CodeTarget::from_str(&wire).is_err(), "accepted {wire}");
    }
    Ok(())
}
