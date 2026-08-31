//! Strict call, success, and error JSON envelopes.

use runku_core::{CodeTarget, FunctionName, OperationId, ReleaseId, RequestId};
use runku_value::CanonicalValue;
use serde::{Deserialize, Serialize};

use crate::{
    PUBLIC_ENVELOPE_MAX_BYTES, PUBLIC_PROTOCOL_VERSION, ProtocolError, PublicErrorV1,
    value::WireValueV1,
};

/// Decoded Query call independent from HTTP headers and trusted Environment scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryCallV1 {
    /// Explicit immutable/moving code selector.
    pub target: CodeTarget,
    /// Canonical logical Function name.
    pub function: FunctionName,
    /// Losslessly decoded canonical arguments.
    pub arguments: CanonicalValue,
}

/// Decoded Mutation call with durable caller-generated operation identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationCallV1 {
    /// Explicit immutable/moving code selector.
    pub target: CodeTarget,
    /// Canonical logical Function name.
    pub function: FunctionName,
    /// Losslessly decoded canonical arguments.
    pub arguments: CanonicalValue,
    /// Stable idempotency identity reused for an exact logical Mutation retry.
    pub operation_id: OperationId,
}

/// Decoded non-transactional Action call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActionCallV1 {
    /// Explicit immutable/moving code selector.
    pub target: CodeTarget,
    /// Canonical logical Function name.
    pub function: FunctionName,
    /// Losslessly decoded canonical arguments.
    pub arguments: CanonicalValue,
}

/// Kind-specific successful execution metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SuccessMetadataV1 {
    /// Query snapshot sequence, absent when no data was read.
    Query {
        /// Snapshot sequence, absent when the Query made no data read.
        snapshot_sequence: Option<u64>,
    },
    /// Mutation durable commit/replay information.
    Mutation {
        /// Commit sequence, absent for a no-write Mutation.
        commit_sequence: Option<u64>,
        /// Whether the durable operation journal returned an exact prior commit.
        replayed: bool,
        /// Complete Function execution attempts including OCC reruns.
        attempts: u8,
    },
    /// Action durable schedules created after awaited Ops.
    Action {
        /// Durable schedules newly created by the Action.
        schedules_created: u64,
    },
}

/// Decoded successful response used by Rust SDK/clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SuccessEnvelopeV1 {
    /// Server-generated correlation identity.
    pub request_id: RequestId,
    /// Exact immutable Release that executed.
    pub release_id: ReleaseId,
    /// Lossless canonical Function result.
    pub result: CanonicalValue,
    /// Kind-specific execution metadata.
    pub metadata: SuccessMetadataV1,
}

/// Decoded sanitized public error used by Rust SDK/clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorEnvelopeV1 {
    /// Server-generated correlation identity.
    pub request_id: RequestId,
    /// Stable canonical machine code.
    pub code: String,
    /// Bounded sanitized user-facing message.
    pub message: String,
    /// Whether retrying later may succeed.
    pub retryable: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CallDto {
    version: u8,
    target: CodeTarget,
    function: String,
    arguments: WireValueV1,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MutationCallDto {
    version: u8,
    target: CodeTarget,
    function: String,
    arguments: WireValueV1,
    operation_id: OperationId,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SuccessEnvelopeDto {
    version: u8,
    status: String,
    request_id: RequestId,
    release_id: ReleaseId,
    result: WireValueV1,
    metadata: SuccessMetadataDto,
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum SuccessMetadataDto {
    Query {
        snapshot_sequence: Option<String>,
    },
    Mutation {
        commit_sequence: Option<String>,
        replayed: bool,
        attempts: u8,
    },
    Action {
        schedules_created: String,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ErrorEnvelopeDto {
    version: u8,
    status: String,
    request_id: RequestId,
    error: ErrorBodyDto,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ErrorBodyDto {
    code: String,
    message: String,
    retryable: bool,
}

/// Decodes a strict Query request body.
///
/// # Errors
///
/// Rejects oversized/malformed JSON, unknown fields, unsupported version, or invalid values.
pub fn decode_query_call_v1(bytes: &[u8]) -> Result<QueryCallV1, ProtocolError> {
    let dto: CallDto = decode_json(bytes)?;
    validate_version(dto.version)?;
    Ok(QueryCallV1 {
        target: dto.target,
        function: dto
            .function
            .parse()
            .map_err(|_| ProtocolError::InvalidRequest)?,
        arguments: dto.arguments.into_canonical()?,
    })
}

/// Encodes a deterministic Query request body for SDK/client use.
///
/// # Errors
///
/// Rejects arguments outside canonical value or envelope limits.
pub fn encode_query_call_v1(call: &QueryCallV1) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&CallDto {
        version: PUBLIC_PROTOCOL_VERSION,
        target: call.target.clone(),
        function: call.function.to_string(),
        arguments: WireValueV1::from_canonical(&call.arguments)?,
    })
}

/// Decodes a strict Mutation request body with mandatory operation identity.
///
/// # Errors
///
/// Rejects oversized/malformed JSON, unknown fields, unsupported version, or invalid values/ID.
pub fn decode_mutation_call_v1(bytes: &[u8]) -> Result<MutationCallV1, ProtocolError> {
    let dto: MutationCallDto = decode_json(bytes)?;
    validate_version(dto.version)?;
    Ok(MutationCallV1 {
        target: dto.target,
        function: dto
            .function
            .parse()
            .map_err(|_| ProtocolError::InvalidRequest)?,
        arguments: dto.arguments.into_canonical()?,
        operation_id: dto.operation_id,
    })
}

/// Encodes a deterministic Mutation request with mandatory operation identity.
///
/// # Errors
///
/// Rejects arguments outside canonical value or envelope limits.
pub fn encode_mutation_call_v1(call: &MutationCallV1) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&MutationCallDto {
        version: PUBLIC_PROTOCOL_VERSION,
        target: call.target.clone(),
        function: call.function.to_string(),
        arguments: WireValueV1::from_canonical(&call.arguments)?,
        operation_id: call.operation_id,
    })
}

/// Decodes a strict Action request body.
///
/// # Errors
///
/// Rejects oversized/malformed JSON, unknown fields, unsupported version, or invalid values.
pub fn decode_action_call_v1(bytes: &[u8]) -> Result<ActionCallV1, ProtocolError> {
    let dto: CallDto = decode_json(bytes)?;
    validate_version(dto.version)?;
    Ok(ActionCallV1 {
        target: dto.target,
        function: dto
            .function
            .parse()
            .map_err(|_| ProtocolError::InvalidRequest)?,
        arguments: dto.arguments.into_canonical()?,
    })
}

/// Encodes a deterministic Action request body for SDK/client use.
///
/// # Errors
///
/// Rejects arguments outside canonical value or envelope limits.
pub fn encode_action_call_v1(call: &ActionCallV1) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&CallDto {
        version: PUBLIC_PROTOCOL_VERSION,
        target: call.target.clone(),
        function: call.function.to_string(),
        arguments: WireValueV1::from_canonical(&call.arguments)?,
    })
}

/// Encodes a deterministic successful response envelope.
///
/// # Errors
///
/// Rejects a value/metadata combination outside protocol limits.
pub fn encode_success_v1(
    request_id: RequestId,
    release_id: ReleaseId,
    result: &CanonicalValue,
    metadata: SuccessMetadataV1,
) -> Result<Vec<u8>, ProtocolError> {
    let metadata = match metadata {
        SuccessMetadataV1::Query { snapshot_sequence } => SuccessMetadataDto::Query {
            snapshot_sequence: snapshot_sequence.map(|value| value.to_string()),
        },
        SuccessMetadataV1::Mutation {
            commit_sequence,
            replayed,
            attempts,
        } => {
            if attempts == 0 {
                return Err(ProtocolError::InvalidResponse);
            }
            SuccessMetadataDto::Mutation {
                commit_sequence: commit_sequence.map(|value| value.to_string()),
                replayed,
                attempts,
            }
        }
        SuccessMetadataV1::Action { schedules_created } => SuccessMetadataDto::Action {
            schedules_created: schedules_created.to_string(),
        },
    };
    encode_json(&SuccessEnvelopeDto {
        version: PUBLIC_PROTOCOL_VERSION,
        status: "ok".to_owned(),
        request_id,
        release_id,
        result: WireValueV1::from_canonical(result)?,
        metadata,
    })
}

/// Decodes and validates a successful response for SDK/client use.
///
/// # Errors
///
/// Rejects malformed/oversized envelopes, wrong status/version, invalid values, or metadata.
pub fn decode_success_v1(bytes: &[u8]) -> Result<SuccessEnvelopeV1, ProtocolError> {
    let dto: SuccessEnvelopeDto = decode_response_json(bytes)?;
    validate_version(dto.version)?;
    if dto.status != "ok" {
        return Err(ProtocolError::InvalidResponse);
    }
    let metadata = match dto.metadata {
        SuccessMetadataDto::Query { snapshot_sequence } => SuccessMetadataV1::Query {
            snapshot_sequence: snapshot_sequence
                .map(|value| parse_canonical_u64(&value))
                .transpose()?,
        },
        SuccessMetadataDto::Mutation {
            commit_sequence,
            replayed,
            attempts,
        } => {
            if attempts == 0 {
                return Err(ProtocolError::InvalidResponse);
            }
            SuccessMetadataV1::Mutation {
                commit_sequence: commit_sequence
                    .map(|value| parse_canonical_u64(&value))
                    .transpose()?,
                replayed,
                attempts,
            }
        }
        SuccessMetadataDto::Action { schedules_created } => SuccessMetadataV1::Action {
            schedules_created: parse_canonical_u64(&schedules_created)?,
        },
    };
    Ok(SuccessEnvelopeV1 {
        request_id: dto.request_id,
        release_id: dto.release_id,
        result: dto.result.into_canonical().map_err(response_value_error)?,
        metadata,
    })
}

fn parse_canonical_u64(value: &str) -> Result<u64, ProtocolError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ProtocolError::InvalidResponse);
    }
    value.parse().map_err(|_| ProtocolError::InvalidResponse)
}

/// Encodes a deterministic sanitized error envelope.
///
/// # Errors
///
/// Returns a limit/response error if the fixed envelope cannot be represented.
pub fn encode_error_v1(
    request_id: RequestId,
    error: PublicErrorV1,
) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&ErrorEnvelopeDto {
        version: PUBLIC_PROTOCOL_VERSION,
        status: "error".to_owned(),
        request_id,
        error: ErrorBodyDto {
            code: error.code().to_owned(),
            message: error.message().to_owned(),
            retryable: error.retryable(),
        },
    })
}

/// Decodes and validates a sanitized error response for SDK/client use.
///
/// # Errors
///
/// Rejects malformed/oversized envelopes, wrong status/version, unsafe code, or message text.
pub fn decode_error_v1(bytes: &[u8]) -> Result<ErrorEnvelopeV1, ProtocolError> {
    let dto: ErrorEnvelopeDto = decode_response_json(bytes)?;
    validate_version(dto.version)?;
    if dto.status != "error"
        || !valid_external_error_code(&dto.error.code)
        || dto.error.message.is_empty()
        || dto.error.message.len() > 128
        || dto.error.message.chars().any(char::is_control)
    {
        return Err(ProtocolError::InvalidResponse);
    }
    Ok(ErrorEnvelopeV1 {
        request_id: dto.request_id,
        code: dto.error.code,
        message: dto.error.message,
        retryable: dto.error.retryable,
    })
}

fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ProtocolError> {
    if bytes.is_empty() {
        return Err(ProtocolError::InvalidRequest);
    }
    if bytes.len() > PUBLIC_ENVELOPE_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidRequest)
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let bytes = serde_json::to_vec(value).map_err(|_| ProtocolError::InvalidResponse)?;
    if bytes.len() > PUBLIC_ENVELOPE_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    Ok(bytes)
}

fn decode_response_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ProtocolError> {
    if bytes.len() > PUBLIC_ENVELOPE_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    if bytes.is_empty() {
        return Err(ProtocolError::InvalidResponse);
    }
    serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidResponse)
}

const fn response_value_error(error: ProtocolError) -> ProtocolError {
    match error {
        ProtocolError::LimitExceeded => ProtocolError::LimitExceeded,
        ProtocolError::InvalidRequest
        | ProtocolError::UnsupportedVersion
        | ProtocolError::InvalidResponse => ProtocolError::InvalidResponse,
    }
}

const fn validate_version(version: u8) -> Result<(), ProtocolError> {
    if version == PUBLIC_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion)
    }
}

fn valid_external_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}
