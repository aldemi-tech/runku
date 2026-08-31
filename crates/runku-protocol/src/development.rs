//! Strict Product Base administrative wire for remote Development Workspaces.

use runku_core::{
    DevRevisionId, EnvironmentDescriptor, EnvironmentId, EnvironmentLocation,
    EnvironmentProtection, EnvironmentPurpose, EnvironmentScope, OperationId, ProjectId, ReleaseId,
    RequestId, WorkspaceId, WorkspaceRef,
};
use runku_releases::{
    ARTIFACT_MAX_BYTES, MANIFEST_MAX_BYTES, ReleaseManifestV1, Sha256Digest,
    decode_release_manifest, decode_safe_esm_bundle, encode_release_manifest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use ulid::Ulid;

use crate::ProtocolError;

/// Development administrative protocol version.
pub const DEVELOPMENT_PROTOCOL_VERSION: u8 = 1;
/// Maximum JSON-only administrative message size.
pub const DEVELOPMENT_JSON_MAX_BYTES: usize = 64 * 1024;
/// Maximum bounded metadata JSON inside a publish frame.
pub const DEVELOPMENT_PUBLISH_METADATA_MAX_BYTES: usize = 16 * 1024;
const PUBLISH_MAGIC: &[u8; 5] = b"RWP\0\x01";
const FRAME_LENGTH_BYTES: usize = 4 + 4 + 8;
const FREEZE_DIAGNOSTICS_MAX: usize = 128;
const FREEZE_DIAGNOSTIC_CODE_MAX_BYTES: usize = 64;
const FREEZE_DIAGNOSTIC_SUBJECT_MAX_BYTES: usize = 512;
/// Maximum exact publish frame bytes accepted by v1.
pub const DEVELOPMENT_PUBLISH_MAX_BYTES: usize = PUBLISH_MAGIC.len()
    + FRAME_LENGTH_BYTES
    + DEVELOPMENT_PUBLISH_METADATA_MAX_BYTES
    + MANIFEST_MAX_BYTES
    + ARTIFACT_MAX_BYTES;

/// Derives the immutable Development Revision assigned to one exact publish operation.
///
/// Clients use this public v1 derivation only to reconcile an uncertain mutation against a later
/// authenticated state read; changing any scope, operation, Workspace, or manifest byte changes
/// the result.
#[must_use]
pub fn derive_development_revision_id_v1(
    scope: EnvironmentScope,
    operation_id: OperationId,
    workspace_ref: &WorkspaceRef,
    manifest_digest: Sha256Digest,
) -> DevRevisionId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_REMOTE_DEV_REVISION_V1\0");
    digest.update(scope.project_id().to_string().as_bytes());
    digest.update([0]);
    digest.update(scope.environment_id().to_string().as_bytes());
    digest.update([0]);
    digest.update(operation_id.to_string().as_bytes());
    digest.update([0]);
    digest.update(workspace_ref.as_str().as_bytes());
    digest.update(manifest_digest.as_bytes());
    let bytes: [u8; 32] = digest.finalize().into();
    let mut ulid = [0_u8; 16];
    ulid.copy_from_slice(&bytes[..16]);
    DevRevisionId::from_ulid(Ulid::from_bytes(ulid))
}

/// State request for one optional Workspace binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentStateRequestV1 {
    /// Workspace to inspect; state still returns trusted Environment metadata when absent.
    pub workspace_ref: WorkspaceRef,
}

/// Safe Workspace binding returned by administrative state/create operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentWorkspaceStateV1 {
    /// Durable Workspace resource identity.
    pub workspace_id: WorkspaceId,
    /// Human-readable exact reference.
    pub workspace_ref: WorkspaceRef,
    /// Current immutable HEAD; absent means no successful publication yet.
    pub head_revision: Option<DevRevisionId>,
}

/// Trusted Environment policy and optional Workspace state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentStateResponseV1 {
    /// Correlation identity allocated by the server.
    pub request_id: RequestId,
    /// Exact Project/Environment boundary.
    pub scope: EnvironmentScope,
    /// Server-authoritative Environment policy.
    pub environment: EnvironmentDescriptor,
    /// Monotonic serving catalog revision.
    pub development_revision: u64,
    /// Requested Workspace when it exists.
    pub workspace: Option<DevelopmentWorkspaceStateV1>,
}

/// Idempotent request to create one empty Workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentCreateWorkspaceRequestV1 {
    /// Stable logical operation identity reused for exact retries.
    pub operation_id: OperationId,
    /// Caller-generated durable resource identity.
    pub workspace_id: WorkspaceId,
    /// Exact target reference.
    pub workspace_ref: WorkspaceRef,
}

/// Successful create/replay response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentCreateWorkspaceResponseV1 {
    /// Server correlation identity.
    pub request_id: RequestId,
    /// Created/existing exact binding.
    pub workspace: DevelopmentWorkspaceStateV1,
    /// Monotonic serving catalog revision after the operation.
    pub development_revision: u64,
    /// Whether the operation journal returned an exact prior outcome.
    pub replayed: bool,
}

/// Fully validated canonical package publication intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentPublishRequestV1 {
    /// Stable logical operation identity reused with the same complete frame.
    pub operation_id: OperationId,
    /// Project claimed by the package and checked against its manifest.
    pub project_id: ProjectId,
    /// Exact mutable Workspace pointer to move.
    pub workspace_ref: WorkspaceRef,
    /// Required compare-and-swap precondition; `None` means empty HEAD.
    pub expected_head: Option<DevRevisionId>,
    /// Decoded and revalidated canonical manifest.
    pub manifest: ReleaseManifestV1,
    /// Exact canonical manifest bytes retained for persistence/replay.
    pub manifest_bytes: Vec<u8>,
    /// Exact canonical executable artifact bytes retained for content-addressed put.
    pub artifact_bytes: Vec<u8>,
}

/// Successful artifact-first publication response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentPublishResponseV1 {
    /// Server correlation identity.
    pub request_id: RequestId,
    /// New/existing immutable Development Revision.
    pub revision_id: DevRevisionId,
    /// Candidate Release embedded in the manifest.
    pub release_id: ReleaseId,
    /// Digest of exact canonical manifest bytes.
    pub manifest_digest: Sha256Digest,
    /// Monotonic serving catalog revision after refresh.
    pub development_revision: u64,
    /// Whether the exact logical publish already completed.
    pub replayed: bool,
}

/// Exact request to validate and make one remote candidate Release explicitly servable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentFreezeRequestV1 {
    /// Stable logical freeze identity reused for exact retries and stage derivation.
    pub operation_id: OperationId,
    /// Candidate Release already registered by publish.
    pub release_id: ReleaseId,
    /// Optional exact compatibility baseline Release in the same Environment.
    pub against_release_id: Option<ReleaseId>,
}

/// Closed outcome of a completed freeze evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentFreezeOutcomeV1 {
    /// Candidate passed validation/compatibility and explicit invocation is permitted.
    Servable,
    /// Compatibility diagnostics were persisted and the candidate is not servable.
    CompatibilityBlocked,
}

/// Safe bounded compatibility diagnostic returned by freeze.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentFreezeDiagnosticV1 {
    /// Stable machine-readable compatibility code.
    pub code: String,
    /// Canonical bounded logical subject; never source or artifact bytes.
    pub subject: String,
}

/// Successful durable freeze evaluation response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentFreezeResponseV1 {
    /// Server correlation identity.
    pub request_id: RequestId,
    /// Candidate evaluated.
    pub release_id: ReleaseId,
    /// Final closed outcome.
    pub outcome: DevelopmentFreezeOutcomeV1,
    /// Ordered blockers; nonempty exactly when compatibility is blocked.
    pub diagnostics: Vec<DevelopmentFreezeDiagnosticV1>,
    /// Monotonic serving configuration revision after the final transition.
    pub serving_revision: u64,
    /// Whether the desired terminal result already existed or all stage commands replayed.
    pub replayed: bool,
}

/// Internal durable transition whose Operation ID is derived from one freeze request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentFreezeStageV1 {
    /// `CREATED → BUILDING`.
    Building,
    /// `BUILDING|COMPATIBILITY_BLOCKED → VALIDATING`.
    Validating,
    /// `VALIDATING → COMPATIBILITY_BLOCKED`.
    CompatibilityBlocked,
    /// `VALIDATING → READY`.
    Ready,
    /// `READY → SERVABLE`.
    Servable,
}

/// Derives the stable top-level freeze identity used by stateless CLI retries and restarts.
#[must_use]
pub fn derive_development_freeze_request_operation_id_v1(
    release_id: ReleaseId,
    against_release_id: Option<ReleaseId>,
) -> OperationId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_REMOTE_FREEZE_REQUEST_V1\0");
    digest.update(release_id.to_string().as_bytes());
    match against_release_id {
        Some(baseline) => {
            digest.update([1]);
            digest.update(baseline.to_string().as_bytes());
        }
        None => digest.update([0]),
    }
    let bytes: [u8; 32] = digest.finalize().into();
    let mut ulid = [0_u8; 16];
    ulid.copy_from_slice(&bytes[..16]);
    OperationId::from_ulid(Ulid::from_bytes(ulid))
}

/// Derives a distinct idempotency identity for one exact freeze lifecycle stage.
#[must_use]
pub fn derive_development_freeze_operation_id_v1(
    operation_id: OperationId,
    release_id: ReleaseId,
    against_release_id: Option<ReleaseId>,
    stage: DevelopmentFreezeStageV1,
) -> OperationId {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_REMOTE_FREEZE_OPERATION_V1\0");
    digest.update(operation_id.to_string().as_bytes());
    digest.update([0]);
    digest.update(release_id.to_string().as_bytes());
    match against_release_id {
        Some(baseline) => {
            digest.update([1]);
            digest.update(baseline.to_string().as_bytes());
        }
        None => digest.update([0]),
    }
    digest.update([match stage {
        DevelopmentFreezeStageV1::Building => 1,
        DevelopmentFreezeStageV1::Validating => 2,
        DevelopmentFreezeStageV1::CompatibilityBlocked => 3,
        DevelopmentFreezeStageV1::Ready => 4,
        DevelopmentFreezeStageV1::Servable => 5,
    }]);
    let bytes: [u8; 32] = digest.finalize().into();
    let mut ulid = [0_u8; 16];
    ulid.copy_from_slice(&bytes[..16]);
    OperationId::from_ulid(Ulid::from_bytes(ulid))
}

/// Closed safe error catalog for the Development administrative protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentAdminErrorCodeV1 {
    /// Malformed or semantically invalid input.
    InvalidRequest,
    /// Missing, malformed, inactive, expired, or unverifiable Development key.
    Unauthenticated,
    /// Valid identity lacks authority for the operation.
    Forbidden,
    /// Requested Workspace/revision/resource does not exist.
    NotFound,
    /// CAS, operation replay, or existing content conflicts.
    Conflict,
    /// Trusted Environment policy denies development synchronization.
    PolicyDenied,
    /// Request exceeds a v1 bound.
    LimitExceeded,
    /// Admission capacity is temporarily exhausted.
    Busy,
    /// A required Product Base dependency is temporarily unavailable.
    Unavailable,
    /// Commit outcome is unknown and requires reconciliation.
    ResultUncertain,
    /// Durable state violates a trusted invariant.
    Corruption,
    /// Sanitized unexpected service failure.
    Internal,
}

impl DevelopmentAdminErrorCodeV1 {
    /// Stable wire code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "DEVELOPMENT_REQUEST_INVALID",
            Self::Unauthenticated => "DEVELOPMENT_AUTH_INVALID",
            Self::Forbidden => "DEVELOPMENT_ACCESS_DENIED",
            Self::NotFound => "DEVELOPMENT_RESOURCE_NOT_FOUND",
            Self::Conflict => "DEVELOPMENT_STATE_CONFLICT",
            Self::PolicyDenied => "DEVELOPMENT_POLICY_DENIED",
            Self::LimitExceeded => "DEVELOPMENT_LIMIT_EXCEEDED",
            Self::Busy => "DEVELOPMENT_SERVICE_BUSY",
            Self::Unavailable => "DEVELOPMENT_SERVICE_UNAVAILABLE",
            Self::ResultUncertain => "DEVELOPMENT_RESULT_UNCERTAIN",
            Self::Corruption => "DEVELOPMENT_STATE_CORRUPT",
            Self::Internal => "DEVELOPMENT_INTERNAL",
        }
    }

    /// Fixed non-sensitive message.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "The development request is invalid.",
            Self::Unauthenticated => "Development authentication is required or invalid.",
            Self::Forbidden => "The development request is not permitted.",
            Self::NotFound => "The development resource was not found.",
            Self::Conflict => "The development request conflicts with current state.",
            Self::PolicyDenied => "Environment policy denies development synchronization.",
            Self::LimitExceeded => "The development request exceeds a protocol limit.",
            Self::Busy => "The development service is temporarily busy.",
            Self::Unavailable => "The development service is temporarily unavailable.",
            Self::ResultUncertain => "The development result must be reconciled.",
            Self::Corruption | Self::Internal => "The development request failed unexpectedly.",
        }
    }

    /// Whether an unchanged request may succeed after recovery/reconciliation.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Busy | Self::Unavailable | Self::ResultUncertain)
    }

    fn parse(value: &str) -> Result<Self, ProtocolError> {
        match value {
            "DEVELOPMENT_REQUEST_INVALID" => Ok(Self::InvalidRequest),
            "DEVELOPMENT_AUTH_INVALID" => Ok(Self::Unauthenticated),
            "DEVELOPMENT_ACCESS_DENIED" => Ok(Self::Forbidden),
            "DEVELOPMENT_RESOURCE_NOT_FOUND" => Ok(Self::NotFound),
            "DEVELOPMENT_STATE_CONFLICT" => Ok(Self::Conflict),
            "DEVELOPMENT_POLICY_DENIED" => Ok(Self::PolicyDenied),
            "DEVELOPMENT_LIMIT_EXCEEDED" => Ok(Self::LimitExceeded),
            "DEVELOPMENT_SERVICE_BUSY" => Ok(Self::Busy),
            "DEVELOPMENT_SERVICE_UNAVAILABLE" => Ok(Self::Unavailable),
            "DEVELOPMENT_RESULT_UNCERTAIN" => Ok(Self::ResultUncertain),
            "DEVELOPMENT_STATE_CORRUPT" => Ok(Self::Corruption),
            "DEVELOPMENT_INTERNAL" => Ok(Self::Internal),
            _ => Err(ProtocolError::InvalidResponse),
        }
    }
}

/// Decoded sanitized administrative error response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentErrorResponseV1 {
    /// Server correlation identity.
    pub request_id: RequestId,
    /// Closed stable error code.
    pub error: DevelopmentAdminErrorCodeV1,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateRequestDto {
    version: u8,
    workspace: WorkspaceRef,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EnvironmentDto {
    project_id: ProjectId,
    environment_id: EnvironmentId,
    purpose: EnvironmentPurpose,
    protection: EnvironmentProtection,
    location: EnvironmentLocation,
    workspace_targets_enabled: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkspaceDto {
    workspace_id: WorkspaceId,
    workspace: WorkspaceRef,
    head: Option<DevRevisionId>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateResponseDto {
    version: u8,
    status: String,
    request_id: RequestId,
    environment: EnvironmentDto,
    development_revision: String,
    workspace: Option<WorkspaceDto>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateRequestDto {
    version: u8,
    operation_id: OperationId,
    workspace_id: WorkspaceId,
    workspace: WorkspaceRef,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateResponseDto {
    version: u8,
    status: String,
    request_id: RequestId,
    workspace: WorkspaceDto,
    development_revision: String,
    replayed: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublishMetadataDto {
    version: u8,
    operation_id: OperationId,
    project_id: ProjectId,
    workspace: WorkspaceRef,
    expected_head: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublishResponseDto {
    version: u8,
    status: String,
    request_id: RequestId,
    revision_id: DevRevisionId,
    release_id: ReleaseId,
    manifest_digest: String,
    development_revision: String,
    replayed: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FreezeRequestDto {
    version: u8,
    operation_id: OperationId,
    release_id: ReleaseId,
    against_release_id: Option<ReleaseId>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FreezeResponseDto {
    version: u8,
    status: String,
    request_id: RequestId,
    release_id: ReleaseId,
    outcome: String,
    diagnostics: Vec<FreezeDiagnosticDto>,
    serving_revision: String,
    replayed: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FreezeDiagnosticDto {
    code: String,
    subject: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ErrorResponseDto {
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

/// Encodes canonical state request JSON.
///
/// # Errors
///
/// Returns a protocol error if the request cannot fit the JSON bound.
pub fn encode_development_state_request_v1(
    request: &DevelopmentStateRequestV1,
) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&StateRequestDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        workspace: request.workspace_ref.clone(),
    })
}

/// Decodes canonical state request JSON.
///
/// # Errors
///
/// Rejects empty/oversized/malformed/noncanonical JSON and unsupported versions.
pub fn decode_development_state_request_v1(
    bytes: &[u8],
) -> Result<DevelopmentStateRequestV1, ProtocolError> {
    let dto: StateRequestDto = decode_request_json(bytes)?;
    validate_version(dto.version, false)?;
    ensure_canonical_json(bytes, &dto, false)?;
    Ok(DevelopmentStateRequestV1 {
        workspace_ref: dto.workspace,
    })
}

/// Encodes canonical trusted state response JSON.
///
/// # Errors
///
/// Rejects contradictory Environment/binding state or values outside v1 limits.
pub fn encode_development_state_response_v1(
    response: &DevelopmentStateResponseV1,
) -> Result<Vec<u8>, ProtocolError> {
    if response.scope.environment_id() != response.environment.id()
        || response.environment.validate_development_sync().is_err()
    {
        return Err(ProtocolError::InvalidResponse);
    }
    encode_json(&StateResponseDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        status: "ok".to_owned(),
        request_id: response.request_id,
        environment: environment_dto(response.scope, response.environment),
        development_revision: response.development_revision.to_string(),
        workspace: response.workspace.as_ref().map(workspace_dto),
    })
}

/// Decodes canonical trusted state response JSON.
///
/// # Errors
///
/// Rejects malformed/noncanonical responses and contradictory policy/scope state.
pub fn decode_development_state_response_v1(
    bytes: &[u8],
) -> Result<DevelopmentStateResponseV1, ProtocolError> {
    let dto: StateResponseDto = decode_response_json(bytes)?;
    validate_version(dto.version, true)?;
    ensure_canonical_json(bytes, &dto, true)?;
    if dto.status != "ok" {
        return Err(ProtocolError::InvalidResponse);
    }
    let (scope, environment) = decode_environment(&dto.environment)?;
    environment
        .validate_development_sync()
        .map_err(|_| ProtocolError::InvalidResponse)?;
    Ok(DevelopmentStateResponseV1 {
        request_id: dto.request_id,
        scope,
        environment,
        development_revision: parse_u64(&dto.development_revision, true)?,
        workspace: dto.workspace.map(decode_workspace),
    })
}

/// Encodes canonical create-Workspace request JSON.
///
/// # Errors
///
/// Returns a protocol error if the request cannot fit the JSON bound.
pub fn encode_development_create_request_v1(
    request: &DevelopmentCreateWorkspaceRequestV1,
) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&CreateRequestDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        operation_id: request.operation_id,
        workspace_id: request.workspace_id,
        workspace: request.workspace_ref.clone(),
    })
}

/// Decodes canonical create-Workspace request JSON.
///
/// # Errors
///
/// Rejects empty/oversized/malformed/noncanonical JSON and unsupported versions.
pub fn decode_development_create_request_v1(
    bytes: &[u8],
) -> Result<DevelopmentCreateWorkspaceRequestV1, ProtocolError> {
    let dto: CreateRequestDto = decode_request_json(bytes)?;
    validate_version(dto.version, false)?;
    ensure_canonical_json(bytes, &dto, false)?;
    Ok(DevelopmentCreateWorkspaceRequestV1 {
        operation_id: dto.operation_id,
        workspace_id: dto.workspace_id,
        workspace_ref: dto.workspace,
    })
}

/// Encodes canonical create/replay response JSON.
///
/// # Errors
///
/// Returns a protocol error if the response cannot fit the JSON bound.
pub fn encode_development_create_response_v1(
    response: &DevelopmentCreateWorkspaceResponseV1,
) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&CreateResponseDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        status: "ok".to_owned(),
        request_id: response.request_id,
        workspace: workspace_dto(&response.workspace),
        development_revision: response.development_revision.to_string(),
        replayed: response.replayed,
    })
}

/// Decodes canonical create/replay response JSON.
///
/// # Errors
///
/// Rejects malformed/noncanonical response fields and unsupported versions.
pub fn decode_development_create_response_v1(
    bytes: &[u8],
) -> Result<DevelopmentCreateWorkspaceResponseV1, ProtocolError> {
    let dto: CreateResponseDto = decode_response_json(bytes)?;
    validate_version(dto.version, true)?;
    ensure_canonical_json(bytes, &dto, true)?;
    if dto.status != "ok" {
        return Err(ProtocolError::InvalidResponse);
    }
    Ok(DevelopmentCreateWorkspaceResponseV1 {
        request_id: dto.request_id,
        workspace: decode_workspace(dto.workspace),
        development_revision: parse_u64(&dto.development_revision, true)?,
        replayed: dto.replayed,
    })
}

/// Encodes the canonical binary publish frame without base64 expansion.
///
/// # Errors
///
/// Rejects invalid package binding, noncanonical bytes, size limits, or unsupported runtime.
pub fn encode_development_publish_request_v1(
    request: &DevelopmentPublishRequestV1,
) -> Result<Vec<u8>, ProtocolError> {
    validate_package(
        request.project_id,
        &request.manifest,
        &request.manifest_bytes,
        &request.artifact_bytes,
    )?;
    let metadata = serde_json::to_vec(&PublishMetadataDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        operation_id: request.operation_id,
        project_id: request.project_id,
        workspace: request.workspace_ref.clone(),
        expected_head: encode_expected_head(request.expected_head),
    })
    .map_err(|_| ProtocolError::InvalidRequest)?;
    if metadata.len() > DEVELOPMENT_PUBLISH_METADATA_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    let total = PUBLISH_MAGIC
        .len()
        .checked_add(FRAME_LENGTH_BYTES)
        .and_then(|value| value.checked_add(metadata.len()))
        .and_then(|value| value.checked_add(request.manifest_bytes.len()))
        .and_then(|value| value.checked_add(request.artifact_bytes.len()))
        .ok_or(ProtocolError::LimitExceeded)?;
    if total > DEVELOPMENT_PUBLISH_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    let mut output = Vec::with_capacity(total);
    output.extend_from_slice(PUBLISH_MAGIC);
    output.extend_from_slice(
        &u32::try_from(metadata.len())
            .map_err(|_| ProtocolError::LimitExceeded)?
            .to_be_bytes(),
    );
    output.extend_from_slice(
        &u32::try_from(request.manifest_bytes.len())
            .map_err(|_| ProtocolError::LimitExceeded)?
            .to_be_bytes(),
    );
    output.extend_from_slice(
        &u64::try_from(request.artifact_bytes.len())
            .map_err(|_| ProtocolError::LimitExceeded)?
            .to_be_bytes(),
    );
    output.extend_from_slice(&metadata);
    output.extend_from_slice(&request.manifest_bytes);
    output.extend_from_slice(&request.artifact_bytes);
    Ok(output)
}

/// Decodes and fully validates a canonical binary publish frame.
///
/// # Errors
///
/// Rejects magic/version/length drift, truncation/trailing bytes, invalid package binding,
/// noncanonical manifest/artifact, unsupported runtime, and every declared bound.
pub fn decode_development_publish_request_v1(
    bytes: &[u8],
) -> Result<DevelopmentPublishRequestV1, ProtocolError> {
    if bytes.len() > DEVELOPMENT_PUBLISH_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    let mut cursor = FrameCursor::new(bytes);
    if cursor.take(PUBLISH_MAGIC.len())? != PUBLISH_MAGIC {
        return Err(ProtocolError::InvalidRequest);
    }
    let metadata_len = usize::try_from(cursor.u32()?).map_err(|_| ProtocolError::LimitExceeded)?;
    let manifest_len = usize::try_from(cursor.u32()?).map_err(|_| ProtocolError::LimitExceeded)?;
    let artifact_len = usize::try_from(cursor.u64()?).map_err(|_| ProtocolError::LimitExceeded)?;
    if metadata_len == 0
        || metadata_len > DEVELOPMENT_PUBLISH_METADATA_MAX_BYTES
        || manifest_len == 0
        || manifest_len > MANIFEST_MAX_BYTES
        || artifact_len == 0
        || artifact_len > ARTIFACT_MAX_BYTES
    {
        return Err(ProtocolError::LimitExceeded);
    }
    let expected_remaining = metadata_len
        .checked_add(manifest_len)
        .and_then(|value| value.checked_add(artifact_len))
        .ok_or(ProtocolError::LimitExceeded)?;
    if cursor.remaining_len() != expected_remaining {
        return Err(ProtocolError::InvalidRequest);
    }
    let metadata_bytes = cursor.take(metadata_len)?;
    let metadata: PublishMetadataDto =
        decode_request_json_with_limit(metadata_bytes, DEVELOPMENT_PUBLISH_METADATA_MAX_BYTES)?;
    validate_version(metadata.version, false)?;
    let canonical_metadata =
        serde_json::to_vec(&metadata).map_err(|_| ProtocolError::InvalidRequest)?;
    if canonical_metadata != metadata_bytes {
        return Err(ProtocolError::InvalidRequest);
    }
    let manifest_bytes = cursor.take(manifest_len)?.to_vec();
    let artifact_bytes = cursor.take(artifact_len)?.to_vec();
    if !cursor.is_empty() {
        return Err(ProtocolError::InvalidRequest);
    }
    let manifest = decode_release_manifest(&manifest_bytes).map_err(map_release_request)?;
    validate_package(
        metadata.project_id,
        &manifest,
        &manifest_bytes,
        &artifact_bytes,
    )?;
    let request = DevelopmentPublishRequestV1 {
        operation_id: metadata.operation_id,
        project_id: metadata.project_id,
        workspace_ref: metadata.workspace,
        expected_head: decode_expected_head(&metadata.expected_head)?,
        manifest,
        manifest_bytes,
        artifact_bytes,
    };
    if encode_development_publish_request_v1(&request)? != bytes {
        return Err(ProtocolError::InvalidRequest);
    }
    Ok(request)
}

/// Encodes canonical publish success JSON.
///
/// # Errors
///
/// Returns a protocol error when the response violates JSON bounds.
pub fn encode_development_publish_response_v1(
    response: &DevelopmentPublishResponseV1,
) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&PublishResponseDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        status: "ok".to_owned(),
        request_id: response.request_id,
        revision_id: response.revision_id,
        release_id: response.release_id,
        manifest_digest: response.manifest_digest.to_string(),
        development_revision: response.development_revision.to_string(),
        replayed: response.replayed,
    })
}

/// Decodes canonical publish success JSON.
///
/// # Errors
///
/// Rejects malformed/noncanonical fields, digests, versions, and bounds.
pub fn decode_development_publish_response_v1(
    bytes: &[u8],
) -> Result<DevelopmentPublishResponseV1, ProtocolError> {
    let dto: PublishResponseDto = decode_response_json(bytes)?;
    validate_version(dto.version, true)?;
    ensure_canonical_json(bytes, &dto, true)?;
    if dto.status != "ok" {
        return Err(ProtocolError::InvalidResponse);
    }
    Ok(DevelopmentPublishResponseV1 {
        request_id: dto.request_id,
        revision_id: dto.revision_id,
        release_id: dto.release_id,
        manifest_digest: dto
            .manifest_digest
            .parse()
            .map_err(|_| ProtocolError::InvalidResponse)?,
        development_revision: parse_u64(&dto.development_revision, true)?,
        replayed: dto.replayed,
    })
}

/// Encodes one canonical remote Release freeze request.
///
/// # Errors
///
/// Rejects a candidate used as its own baseline or an oversized envelope.
pub fn encode_development_freeze_request_v1(
    request: &DevelopmentFreezeRequestV1,
) -> Result<Vec<u8>, ProtocolError> {
    validate_freeze_request(request)?;
    encode_json(&FreezeRequestDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        operation_id: request.operation_id,
        release_id: request.release_id,
        against_release_id: request.against_release_id,
    })
}

/// Decodes one canonical remote Release freeze request.
///
/// # Errors
///
/// Rejects malformed, noncanonical, unsupported, contradictory, or oversized input.
pub fn decode_development_freeze_request_v1(
    bytes: &[u8],
) -> Result<DevelopmentFreezeRequestV1, ProtocolError> {
    let dto: FreezeRequestDto = decode_request_json(bytes)?;
    validate_version(dto.version, false)?;
    ensure_canonical_json(bytes, &dto, false)?;
    let request = DevelopmentFreezeRequestV1 {
        operation_id: dto.operation_id,
        release_id: dto.release_id,
        against_release_id: dto.against_release_id,
    };
    validate_freeze_request(&request)?;
    Ok(request)
}

/// Encodes one canonical remote Release freeze result.
///
/// # Errors
///
/// Rejects contradictory outcome/diagnostics, unsafe diagnostics, zero revision, or limits.
pub fn encode_development_freeze_response_v1(
    response: &DevelopmentFreezeResponseV1,
) -> Result<Vec<u8>, ProtocolError> {
    validate_freeze_response(response)?;
    encode_json(&FreezeResponseDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        status: "ok".to_owned(),
        request_id: response.request_id,
        release_id: response.release_id,
        outcome: freeze_outcome_text(response.outcome).to_owned(),
        diagnostics: response
            .diagnostics
            .iter()
            .map(|diagnostic| FreezeDiagnosticDto {
                code: diagnostic.code.clone(),
                subject: diagnostic.subject.clone(),
            })
            .collect(),
        serving_revision: response.serving_revision.to_string(),
        replayed: response.replayed,
    })
}

/// Decodes one canonical remote Release freeze result.
///
/// # Errors
///
/// Rejects malformed/noncanonical responses and outcome/diagnostic drift.
pub fn decode_development_freeze_response_v1(
    bytes: &[u8],
) -> Result<DevelopmentFreezeResponseV1, ProtocolError> {
    let dto: FreezeResponseDto = decode_response_json(bytes)?;
    validate_version(dto.version, true)?;
    ensure_canonical_json(bytes, &dto, true)?;
    if dto.status != "ok" {
        return Err(ProtocolError::InvalidResponse);
    }
    let outcome = match dto.outcome.as_str() {
        "servable" => DevelopmentFreezeOutcomeV1::Servable,
        "compatibility_blocked" => DevelopmentFreezeOutcomeV1::CompatibilityBlocked,
        _ => return Err(ProtocolError::InvalidResponse),
    };
    let response = DevelopmentFreezeResponseV1 {
        request_id: dto.request_id,
        release_id: dto.release_id,
        outcome,
        diagnostics: dto
            .diagnostics
            .into_iter()
            .map(|diagnostic| DevelopmentFreezeDiagnosticV1 {
                code: diagnostic.code,
                subject: diagnostic.subject,
            })
            .collect(),
        serving_revision: parse_u64(&dto.serving_revision, true)?,
        replayed: dto.replayed,
    };
    validate_freeze_response(&response)?;
    Ok(response)
}

/// Encodes a canonical sanitized administrative error response.
///
/// # Errors
///
/// Returns a protocol error only if the fixed response exceeds v1 bounds.
pub fn encode_development_error_v1(
    request_id: RequestId,
    error: DevelopmentAdminErrorCodeV1,
) -> Result<Vec<u8>, ProtocolError> {
    encode_json(&ErrorResponseDto {
        version: DEVELOPMENT_PROTOCOL_VERSION,
        status: "error".to_owned(),
        request_id,
        error: ErrorBodyDto {
            code: error.code().to_owned(),
            message: error.message().to_owned(),
            retryable: error.retryable(),
        },
    })
}

/// Decodes a canonical sanitized administrative error response.
///
/// # Errors
///
/// Rejects unknown codes or any message/retryable drift from the closed catalog.
pub fn decode_development_error_v1(
    bytes: &[u8],
) -> Result<DevelopmentErrorResponseV1, ProtocolError> {
    let dto: ErrorResponseDto = decode_response_json(bytes)?;
    validate_version(dto.version, true)?;
    ensure_canonical_json(bytes, &dto, true)?;
    if dto.status != "error" {
        return Err(ProtocolError::InvalidResponse);
    }
    let error = DevelopmentAdminErrorCodeV1::parse(&dto.error.code)?;
    if dto.error.message != error.message() || dto.error.retryable != error.retryable() {
        return Err(ProtocolError::InvalidResponse);
    }
    Ok(DevelopmentErrorResponseV1 {
        request_id: dto.request_id,
        error,
    })
}

fn validate_package(
    project_id: ProjectId,
    manifest: &ReleaseManifestV1,
    manifest_bytes: &[u8],
    artifact_bytes: &[u8],
) -> Result<(), ProtocolError> {
    let invalid = ProtocolError::InvalidRequest;
    if manifest.project_id != project_id
        || manifest_bytes.is_empty()
        || manifest_bytes.len() > MANIFEST_MAX_BYTES
        || artifact_bytes.is_empty()
        || artifact_bytes.len() > ARTIFACT_MAX_BYTES
        || encode_release_manifest(manifest).map_err(|_| invalid)? != manifest_bytes
    {
        return Err(invalid);
    }
    let bundle = decode_safe_esm_bundle(artifact_bytes).map_err(|_| invalid)?;
    bundle
        .verify_manifest(manifest, artifact_bytes)
        .map_err(|_| invalid)
}

fn validate_freeze_request(request: &DevelopmentFreezeRequestV1) -> Result<(), ProtocolError> {
    if request.against_release_id == Some(request.release_id) {
        Err(ProtocolError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn validate_freeze_response(response: &DevelopmentFreezeResponseV1) -> Result<(), ProtocolError> {
    if response.serving_revision == 0
        || response.diagnostics.len() > FREEZE_DIAGNOSTICS_MAX
        || matches!(response.outcome, DevelopmentFreezeOutcomeV1::Servable)
            && !response.diagnostics.is_empty()
        || matches!(
            response.outcome,
            DevelopmentFreezeOutcomeV1::CompatibilityBlocked
        ) && response.diagnostics.is_empty()
        || response
            .diagnostics
            .iter()
            .any(|diagnostic| !valid_freeze_diagnostic(diagnostic))
    {
        return Err(ProtocolError::InvalidResponse);
    }
    Ok(())
}

fn valid_freeze_diagnostic(diagnostic: &DevelopmentFreezeDiagnosticV1) -> bool {
    !diagnostic.code.is_empty()
        && diagnostic.code.len() <= FREEZE_DIAGNOSTIC_CODE_MAX_BYTES
        && diagnostic
            .code
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_uppercase())
        && diagnostic
            .code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        && !diagnostic.subject.is_empty()
        && diagnostic.subject.len() <= FREEZE_DIAGNOSTIC_SUBJECT_MAX_BYTES
        && diagnostic
            .subject
            .bytes()
            .all(|byte| byte.is_ascii_graphic())
}

const fn freeze_outcome_text(outcome: DevelopmentFreezeOutcomeV1) -> &'static str {
    match outcome {
        DevelopmentFreezeOutcomeV1::Servable => "servable",
        DevelopmentFreezeOutcomeV1::CompatibilityBlocked => "compatibility_blocked",
    }
}

fn environment_dto(scope: EnvironmentScope, environment: EnvironmentDescriptor) -> EnvironmentDto {
    EnvironmentDto {
        project_id: scope.project_id(),
        environment_id: scope.environment_id(),
        purpose: environment.purpose(),
        protection: environment.protection(),
        location: environment.location(),
        workspace_targets_enabled: environment.workspace_targets_enabled(),
    }
}

fn decode_environment(
    dto: &EnvironmentDto,
) -> Result<(EnvironmentScope, EnvironmentDescriptor), ProtocolError> {
    let scope = EnvironmentScope::new(dto.project_id, dto.environment_id);
    let environment = EnvironmentDescriptor::new(
        dto.environment_id,
        dto.purpose,
        dto.protection,
        dto.location,
        dto.workspace_targets_enabled,
    )
    .map_err(|_| ProtocolError::InvalidResponse)?;
    Ok((scope, environment))
}

fn workspace_dto(value: &DevelopmentWorkspaceStateV1) -> WorkspaceDto {
    WorkspaceDto {
        workspace_id: value.workspace_id,
        workspace: value.workspace_ref.clone(),
        head: value.head_revision,
    }
}

fn decode_workspace(dto: WorkspaceDto) -> DevelopmentWorkspaceStateV1 {
    DevelopmentWorkspaceStateV1 {
        workspace_id: dto.workspace_id,
        workspace_ref: dto.workspace,
        head_revision: dto.head,
    }
}

fn encode_expected_head(value: Option<DevRevisionId>) -> String {
    value.map_or_else(|| "empty".to_owned(), |revision| revision.to_string())
}

fn decode_expected_head(value: &str) -> Result<Option<DevRevisionId>, ProtocolError> {
    if value == "empty" {
        Ok(None)
    } else {
        value
            .parse()
            .map(Some)
            .map_err(|_| ProtocolError::InvalidRequest)
    }
}

fn parse_u64(value: &str, response: bool) -> Result<u64, ProtocolError> {
    let invalid = if response {
        ProtocolError::InvalidResponse
    } else {
        ProtocolError::InvalidRequest
    };
    if value.is_empty()
        || value.len() > 20
        || value.len() > 1 && value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid);
    }
    value.parse().map_err(|_| invalid)
}

fn validate_version(version: u8, response: bool) -> Result<(), ProtocolError> {
    if version == DEVELOPMENT_PROTOCOL_VERSION {
        Ok(())
    } else if response {
        Err(ProtocolError::InvalidResponse)
    } else {
        Err(ProtocolError::UnsupportedVersion)
    }
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let bytes = serde_json::to_vec(value).map_err(|_| ProtocolError::InvalidResponse)?;
    if bytes.len() > DEVELOPMENT_JSON_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    Ok(bytes)
}

fn decode_request_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ProtocolError> {
    decode_request_json_with_limit(bytes, DEVELOPMENT_JSON_MAX_BYTES)
}

fn decode_request_json_with_limit<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    limit: usize,
) -> Result<T, ProtocolError> {
    if bytes.is_empty() {
        return Err(ProtocolError::InvalidRequest);
    }
    if bytes.len() > limit {
        return Err(ProtocolError::LimitExceeded);
    }
    serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidRequest)
}

fn decode_response_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ProtocolError> {
    if bytes.is_empty() {
        return Err(ProtocolError::InvalidResponse);
    }
    if bytes.len() > DEVELOPMENT_JSON_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidResponse)
}

fn ensure_canonical_json<T: Serialize>(
    bytes: &[u8],
    value: &T,
    response: bool,
) -> Result<(), ProtocolError> {
    let encoded = serde_json::to_vec(value).map_err(|_| ProtocolError::InvalidResponse)?;
    if encoded == bytes {
        Ok(())
    } else if response {
        Err(ProtocolError::InvalidResponse)
    } else {
        Err(ProtocolError::InvalidRequest)
    }
}

fn map_release_request(_error: runku_releases::ReleaseError) -> ProtocolError {
    ProtocolError::InvalidRequest
}

struct FrameCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> FrameCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    const fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ProtocolError> {
        if self.remaining.len() < count {
            return Err(ProtocolError::InvalidRequest);
        }
        let (value, remaining) = self.remaining.split_at(count);
        self.remaining = remaining;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ProtocolError::InvalidRequest)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, ProtocolError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| ProtocolError::InvalidRequest)?;
        Ok(u64::from_be_bytes(bytes))
    }
}
