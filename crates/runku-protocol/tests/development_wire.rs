//! Remote Workspace administrative protocol golden and adversarial conformance.

use std::error::Error;

use runku_core::{
    EnvironmentDescriptor, EnvironmentLocation, EnvironmentProtection, EnvironmentPurpose,
    EnvironmentScope, OperationId, ReleaseId,
};
use runku_protocol::{
    DEVELOPMENT_JSON_MAX_BYTES, DevelopmentAdminErrorCodeV1, DevelopmentCreateWorkspaceRequestV1,
    DevelopmentCreateWorkspaceResponseV1, DevelopmentFreezeDiagnosticV1,
    DevelopmentFreezeOutcomeV1, DevelopmentFreezeRequestV1, DevelopmentFreezeResponseV1,
    DevelopmentFreezeStageV1, DevelopmentPublishRequestV1, DevelopmentPublishResponseV1,
    DevelopmentStateRequestV1, DevelopmentStateResponseV1, DevelopmentWorkspaceStateV1,
    ProtocolError, decode_development_create_request_v1, decode_development_create_response_v1,
    decode_development_error_v1, decode_development_freeze_request_v1,
    decode_development_freeze_response_v1, decode_development_publish_request_v1,
    decode_development_publish_response_v1, decode_development_state_request_v1,
    decode_development_state_response_v1, derive_development_freeze_operation_id_v1,
    encode_development_create_request_v1, encode_development_create_response_v1,
    encode_development_error_v1, encode_development_freeze_request_v1,
    encode_development_freeze_response_v1, encode_development_publish_request_v1,
    encode_development_publish_response_v1, encode_development_state_request_v1,
    encode_development_state_response_v1,
};
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_release_manifest, encode_safe_esm_bundle,
};
use runku_value::TimestampMicros;
use serde_json::Value;

const PROJECT: &str = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const ENVIRONMENT: &str = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW";
const WORKSPACE_ID: &str = "wsp_01ARZ3NDEKTSV4RRFFQ69G5FAX";
const OPERATION: &str = "opn_01ARZ3NDEKTSV4RRFFQ69G5FAY";
const REQUEST: &str = "req_01ARZ3NDEKTSV4RRFFQ69G5FAZ";
const REVISION: &str = "drv_01ARZ3NDEKTSV4RRFFQ69G5FB0";
const RELEASE: &str = "rel_01ARZ3NDEKTSV4RRFFQ69G5FB1";
const BUILD: &str = "bld_01ARZ3NDEKTSV4RRFFQ69G5FB2";
const FUNCTION: &str = "fnc_01ARZ3NDEKTSV4RRFFQ69G5FB3";
const BASELINE: &str = "rel_01ARZ3NDEKTSV4RRFFQ69G5FB4";

fn golden() -> Result<Value, serde_json::Error> {
    serde_json::from_slice(include_bytes!(
        "../../../protocol/v1/development-admin-vectors.json"
    ))
}

fn golden_text(field: &str) -> Result<String, Box<dyn Error>> {
    Ok(golden()?[field]
        .as_str()
        .ok_or("golden string missing")?
        .to_owned())
}

#[test]
fn state_create_success_and_error_match_golden_exactly() -> Result<(), Box<dyn Error>> {
    let state_request = DevelopmentStateRequestV1 {
        workspace_ref: "dev/manuel".parse()?,
    };
    let encoded = encode_development_state_request_v1(&state_request)?;
    assert_eq!(encoded, golden_text("stateRequest")?.as_bytes());
    assert_eq!(
        decode_development_state_request_v1(&encoded)?,
        state_request
    );

    let scope = EnvironmentScope::new(PROJECT.parse()?, ENVIRONMENT.parse()?);
    let environment = EnvironmentDescriptor::new(
        scope.environment_id(),
        EnvironmentPurpose::Preview,
        EnvironmentProtection::Protected,
        EnvironmentLocation::SelfHosted,
        true,
    )?;
    let workspace = DevelopmentWorkspaceStateV1 {
        workspace_id: WORKSPACE_ID.parse()?,
        workspace_ref: "dev/manuel".parse()?,
        head_revision: Some(REVISION.parse()?),
    };
    let state_response = DevelopmentStateResponseV1 {
        request_id: REQUEST.parse()?,
        scope,
        environment,
        development_revision: 7,
        workspace: Some(workspace.clone()),
    };
    let encoded = encode_development_state_response_v1(&state_response)?;
    assert_eq!(encoded, golden_text("stateResponse")?.as_bytes());
    assert_eq!(
        decode_development_state_response_v1(&encoded)?,
        state_response
    );

    let create_request = DevelopmentCreateWorkspaceRequestV1 {
        operation_id: OPERATION.parse()?,
        workspace_id: WORKSPACE_ID.parse()?,
        workspace_ref: "dev/manuel".parse()?,
    };
    let encoded = encode_development_create_request_v1(&create_request)?;
    assert_eq!(encoded, golden_text("createRequest")?.as_bytes());
    assert_eq!(
        decode_development_create_request_v1(&encoded)?,
        create_request
    );
    let create_response = DevelopmentCreateWorkspaceResponseV1 {
        request_id: REQUEST.parse()?,
        workspace: DevelopmentWorkspaceStateV1 {
            head_revision: None,
            ..workspace
        },
        development_revision: 8,
        replayed: false,
    };
    let encoded = encode_development_create_response_v1(&create_response)?;
    assert_eq!(encoded, golden_text("createResponse")?.as_bytes());
    assert_eq!(
        decode_development_create_response_v1(&encoded)?,
        create_response
    );

    let encoded =
        encode_development_error_v1(REQUEST.parse()?, DevelopmentAdminErrorCodeV1::Conflict)?;
    assert_eq!(encoded, golden_text("conflictError")?.as_bytes());
    assert_eq!(
        decode_development_error_v1(&encoded)?.error,
        DevelopmentAdminErrorCodeV1::Conflict
    );
    Ok(())
}

#[test]
fn publish_frame_is_canonical_bound_and_matches_golden() -> Result<(), Box<dyn Error>> {
    let request = package()?;
    let frame = encode_development_publish_request_v1(&request)?;
    assert_eq!(decode_development_publish_request_v1(&frame)?, request);
    assert_eq!(
        encode_development_publish_request_v1(&decode_development_publish_request_v1(&frame)?)?,
        frame
    );
    let metadata_length = u32::from_be_bytes(frame[5..9].try_into()?) as usize;
    assert_eq!(
        &frame[21..21 + metadata_length],
        golden_text("publishMetadata")?.as_bytes()
    );
    assert_eq!(
        Sha256Digest::of(&frame).to_string(),
        golden_text("publishFrameSha256")?
    );

    let response = DevelopmentPublishResponseV1 {
        request_id: REQUEST.parse()?,
        revision_id: REVISION.parse()?,
        release_id: RELEASE.parse()?,
        manifest_digest: Sha256Digest::from_bytes([0; 32]),
        development_revision: 9,
        replayed: false,
    };
    let encoded = encode_development_publish_response_v1(&response)?;
    assert_eq!(encoded, golden_text("publishResponse")?.as_bytes());
    assert_eq!(decode_development_publish_response_v1(&encoded)?, response);
    Ok(())
}

#[test]
fn freeze_messages_are_golden_bounded_and_stage_ids_are_distinct() -> Result<(), Box<dyn Error>> {
    let request = DevelopmentFreezeRequestV1 {
        operation_id: OPERATION.parse()?,
        release_id: RELEASE.parse()?,
        against_release_id: Some(BASELINE.parse()?),
    };
    let encoded = encode_development_freeze_request_v1(&request)?;
    assert_eq!(encoded, golden_text("freezeRequest")?.as_bytes());
    assert_eq!(decode_development_freeze_request_v1(&encoded)?, request);

    let response = DevelopmentFreezeResponseV1 {
        request_id: REQUEST.parse()?,
        release_id: RELEASE.parse()?,
        outcome: DevelopmentFreezeOutcomeV1::CompatibilityBlocked,
        diagnostics: vec![DevelopmentFreezeDiagnosticV1 {
            code: "FUNCTION_REMOVED".to_owned(),
            subject: "queries.echo".to_owned(),
        }],
        serving_revision: 12,
        replayed: false,
    };
    let encoded = encode_development_freeze_response_v1(&response)?;
    assert_eq!(encoded, golden_text("freezeResponse")?.as_bytes());
    assert_eq!(decode_development_freeze_response_v1(&encoded)?, response);

    let operation: OperationId = OPERATION.parse()?;
    let release: ReleaseId = RELEASE.parse()?;
    let baseline: ReleaseId = BASELINE.parse()?;
    let stages = [
        DevelopmentFreezeStageV1::Building,
        DevelopmentFreezeStageV1::Validating,
        DevelopmentFreezeStageV1::CompatibilityBlocked,
        DevelopmentFreezeStageV1::Ready,
        DevelopmentFreezeStageV1::Servable,
    ]
    .map(|stage| {
        derive_development_freeze_operation_id_v1(operation, release, Some(baseline), stage)
    });
    let unique = stages
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), 5);
    assert_eq!(
        encode_development_freeze_request_v1(&DevelopmentFreezeRequestV1 {
            against_release_id: Some(RELEASE.parse()?),
            ..request
        }),
        Err(ProtocolError::InvalidRequest)
    );
    let unknown = format!(
        "{{\"version\":1,\"operationId\":\"{OPERATION}\",\"releaseId\":\"{RELEASE}\",\"againstReleaseId\":null,\"extra\":true}}"
    );
    assert!(decode_development_freeze_request_v1(unknown.as_bytes()).is_err());
    let duplicate = format!(
        "{{\"version\":1,\"operationId\":\"{OPERATION}\",\"operationId\":\"{OPERATION}\",\"releaseId\":\"{RELEASE}\",\"againstReleaseId\":null}}"
    );
    assert!(decode_development_freeze_request_v1(duplicate.as_bytes()).is_err());
    assert_eq!(
        encode_development_freeze_response_v1(&DevelopmentFreezeResponseV1 {
            request_id: REQUEST.parse()?,
            release_id: RELEASE.parse()?,
            outcome: DevelopmentFreezeOutcomeV1::Servable,
            diagnostics: vec![DevelopmentFreezeDiagnosticV1 {
                code: "UNEXPECTED".to_owned(),
                subject: "release".to_owned(),
            }],
            serving_revision: 1,
            replayed: false,
        }),
        Err(ProtocolError::InvalidResponse)
    );
    assert_eq!(
        encode_development_freeze_response_v1(&DevelopmentFreezeResponseV1 {
            request_id: REQUEST.parse()?,
            release_id: RELEASE.parse()?,
            outcome: DevelopmentFreezeOutcomeV1::CompatibilityBlocked,
            diagnostics: vec![],
            serving_revision: 1,
            replayed: false,
        }),
        Err(ProtocolError::InvalidResponse)
    );
    assert_eq!(
        encode_development_freeze_response_v1(&DevelopmentFreezeResponseV1 {
            request_id: REQUEST.parse()?,
            release_id: RELEASE.parse()?,
            outcome: DevelopmentFreezeOutcomeV1::CompatibilityBlocked,
            diagnostics: (0..129)
                .map(|index| DevelopmentFreezeDiagnosticV1 {
                    code: "LIMIT".to_owned(),
                    subject: format!("item-{index}"),
                })
                .collect(),
            serving_revision: 1,
            replayed: false,
        }),
        Err(ProtocolError::InvalidResponse)
    );
    Ok(())
}

#[test]
fn json_messages_reject_noncanonical_unknown_duplicate_version_and_limits() {
    for invalid in [
        b"".as_slice(),
        b"{}".as_slice(),
        b"{\"version\":2,\"workspace\":\"dev/manuel\"}".as_slice(),
        b"{\"workspace\":\"dev/manuel\",\"version\":1}".as_slice(),
        b"{\"version\":1, \"workspace\":\"dev/manuel\"}".as_slice(),
        b"{\"version\":1,\"version\":1,\"workspace\":\"dev/manuel\"}".as_slice(),
        b"{\"version\":1,\"workspace\":\"dev/manuel\",\"extra\":true}".as_slice(),
        b"\xff".as_slice(),
    ] {
        assert!(decode_development_state_request_v1(invalid).is_err());
    }
    assert_eq!(
        decode_development_state_request_v1(&vec![b'x'; DEVELOPMENT_JSON_MAX_BYTES + 1]),
        Err(ProtocolError::LimitExceeded)
    );
    for invalid in [
        b"{\"version\":1,\"status\":\"error\",\"requestId\":\"req_01ARZ3NDEKTSV4RRFFQ69G5FAZ\",\"error\":{\"code\":\"UNKNOWN\",\"message\":\"x\",\"retryable\":false}}".as_slice(),
        b"{\"version\":1,\"status\":\"error\",\"requestId\":\"req_01ARZ3NDEKTSV4RRFFQ69G5FAZ\",\"error\":{\"code\":\"DEVELOPMENT_STATE_CONFLICT\",\"message\":\"SQL failed\",\"retryable\":false}}".as_slice(),
        b"{\"version\":1,\"status\":\"error\",\"requestId\":\"req_01ARZ3NDEKTSV4RRFFQ69G5FAZ\",\"error\":{\"code\":\"DEVELOPMENT_STATE_CONFLICT\",\"message\":\"The development request conflicts with current state.\",\"retryable\":true}}".as_slice(),
    ] {
        assert_eq!(
            decode_development_error_v1(invalid),
            Err(ProtocolError::InvalidResponse)
        );
    }
}

#[test]
fn publish_rejects_every_truncation_length_magic_metadata_and_package_tamper()
-> Result<(), Box<dyn Error>> {
    let request = package()?;
    let frame = encode_development_publish_request_v1(&request)?;
    for length in 0..frame.len() {
        assert!(decode_development_publish_request_v1(&frame[..length]).is_err());
    }
    let mut trailing = frame.clone();
    trailing.push(0);
    assert_eq!(
        decode_development_publish_request_v1(&trailing),
        Err(ProtocolError::InvalidRequest)
    );
    let mut magic = frame.clone();
    magic[0] ^= 1;
    assert_eq!(
        decode_development_publish_request_v1(&magic),
        Err(ProtocolError::InvalidRequest)
    );
    let mut oversized = frame.clone();
    oversized[13..21].copy_from_slice(&((64_u64 * 1024 * 1024) + 1).to_be_bytes());
    assert_eq!(
        decode_development_publish_request_v1(&oversized),
        Err(ProtocolError::LimitExceeded)
    );
    let metadata_length = u32::from_be_bytes(frame[5..9].try_into()?) as usize;
    let metadata = &frame[21..21 + metadata_length];
    let unknown = String::from_utf8(metadata.to_vec())?.replace('}', ",\"extra\":true}");
    let unknown_frame = reframe(&frame, unknown.as_bytes())?;
    assert_eq!(
        decode_development_publish_request_v1(&unknown_frame),
        Err(ProtocolError::InvalidRequest)
    );
    let current = String::from_utf8(metadata.to_vec())?.replace(
        &format!("\"expectedHead\":\"{REVISION}\""),
        "\"expectedHead\":\"current\"",
    );
    assert_eq!(
        decode_development_publish_request_v1(&reframe(&frame, current.as_bytes())?),
        Err(ProtocolError::InvalidRequest)
    );
    let mut artifact_tamper = frame.clone();
    let last = artifact_tamper.len() - 1;
    artifact_tamper[last] ^= 1;
    assert_eq!(
        decode_development_publish_request_v1(&artifact_tamper),
        Err(ProtocolError::InvalidRequest)
    );
    let mut cross_project = request.clone();
    cross_project.project_id = "prj_01ARZ3NDEKTSV4RRFFQ69G5FB4".parse()?;
    assert_eq!(
        encode_development_publish_request_v1(&cross_project),
        Err(ProtocolError::InvalidRequest)
    );
    Ok(())
}

#[test]
fn error_catalog_is_closed_and_retryability_is_fixed() {
    for error in [
        DevelopmentAdminErrorCodeV1::InvalidRequest,
        DevelopmentAdminErrorCodeV1::Unauthenticated,
        DevelopmentAdminErrorCodeV1::Forbidden,
        DevelopmentAdminErrorCodeV1::NotFound,
        DevelopmentAdminErrorCodeV1::Conflict,
        DevelopmentAdminErrorCodeV1::PolicyDenied,
        DevelopmentAdminErrorCodeV1::LimitExceeded,
        DevelopmentAdminErrorCodeV1::Busy,
        DevelopmentAdminErrorCodeV1::Unavailable,
        DevelopmentAdminErrorCodeV1::ResultUncertain,
        DevelopmentAdminErrorCodeV1::Corruption,
        DevelopmentAdminErrorCodeV1::Internal,
    ] {
        assert!(error.code().starts_with("DEVELOPMENT_"));
        assert!(!error.message().is_empty());
        assert_eq!(
            error.retryable(),
            matches!(
                error,
                DevelopmentAdminErrorCodeV1::Busy
                    | DevelopmentAdminErrorCodeV1::Unavailable
                    | DevelopmentAdminErrorCodeV1::ResultUncertain
            )
        );
    }
}

fn package() -> Result<DevelopmentPublishRequestV1, Box<dyn Error>> {
    let source = "export default async (_ctx, value) => value;";
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact_bytes = encode_safe_esm_bundle(&bundle)?;
    let contract = Sha256Digest::of(b"development-wire-contract");
    let manifest = ReleaseManifestV1 {
        release_id: RELEASE.parse()?,
        project_id: PROJECT.parse()?,
        build_id: BUILD.parse()?,
        created_at: TimestampMicros::new(1_800_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: contract,
        schema_contract_hash: contract,
        index_contract_hash: contract,
        functions: vec![FunctionManifest {
            id: FUNCTION.parse()?,
            name: "queries.echo".parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: contract,
            result_contract_hash: contract,
            capabilities: Vec::<Capability>::new(),
        }],
        cron_definitions: vec![],
    };
    let manifest_bytes = encode_release_manifest(&manifest)?;
    Ok(DevelopmentPublishRequestV1 {
        operation_id: OPERATION.parse()?,
        project_id: PROJECT.parse()?,
        workspace_ref: "dev/manuel".parse()?,
        expected_head: Some(REVISION.parse()?),
        manifest,
        manifest_bytes,
        artifact_bytes,
    })
}

fn reframe(original: &[u8], metadata: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    let old_length = u32::from_be_bytes(original[5..9].try_into()?) as usize;
    let mut output = Vec::new();
    output.extend_from_slice(&original[..5]);
    output.extend_from_slice(&u32::try_from(metadata.len())?.to_be_bytes());
    output.extend_from_slice(&original[9..21]);
    output.extend_from_slice(metadata);
    output.extend_from_slice(&original[21 + old_length..]);
    Ok(output)
}
