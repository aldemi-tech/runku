use runku_observability::PerformanceResourceUsage;
use runku_protocol::WireValueV1;
use runku_releases::{
    ArtifactFormat, FullNodeEgressPolicy, FunctionType, ReleaseManifestV1, RuntimeClass,
    Sha256Digest, decode_node_oci_descriptor,
};
use runku_runtime::{InvocationRequest, RuntimeError};
use serde::{Deserialize, Serialize};

use crate::FullNodeActionOutcome;

pub(crate) struct PreparedRequest {
    pub(crate) image_reference: String,
    pub(crate) input: Vec<u8>,
    pub(crate) egress: FullNodeEgressPolicy,
}

pub(crate) fn prepare_request(
    request: &InvocationRequest,
) -> Result<PreparedRequest, RuntimeError> {
    validate_artifact(request.manifest(), request.artifact_bytes())?;
    let function = request
        .manifest()
        .functions
        .iter()
        .find(|function| function.id == request.function_id())
        .ok_or(RuntimeError::FunctionNotFound)?;
    if function.runtime_class != RuntimeClass::FullNode
        || function.function_type != FunctionType::Action
    {
        return Err(RuntimeError::UnsupportedRuntime);
    }
    let descriptor_bytes =
        if request.manifest().artifact.format == ArtifactFormat::HybridOciArtifactV1 {
            runku_releases::decode_hybrid_oci_artifact(request.artifact_bytes())
                .map_err(|_| RuntimeError::InvalidArtifact)?
                .1
        } else {
            request.artifact_bytes()
        };
    let descriptor =
        decode_node_oci_descriptor(descriptor_bytes).map_err(|_| RuntimeError::InvalidArtifact)?;
    let arguments = WireValueV1::from_canonical(request.arguments())
        .map_err(|_| RuntimeError::InvalidArguments)?;
    let input = serde_json::to_vec(&NodeRequestV1 {
        protocol_version: 1,
        collect_performance: request.performance().is_some(),
        release_id: request.release_id().to_string(),
        invocation_id: request.invocation_id().to_string(),
        function: function.name.as_str().to_owned(),
        implementation_hash: function.implementation_hash.to_string(),
        arguments_contract_hash: function.arguments_contract_hash.to_string(),
        result_contract_hash: function.result_contract_hash.to_string(),
        arguments,
    })
    .map_err(|_| RuntimeError::Internal)?;
    Ok(PreparedRequest {
        image_reference: descriptor.image_reference().to_owned(),
        input,
        egress: descriptor.egress_policy().clone(),
    })
}

pub(crate) fn validate_artifact(
    manifest: &ReleaseManifestV1,
    bytes: &[u8],
) -> Result<(), RuntimeError> {
    manifest
        .ensure_full_node_v1_supported()
        .map_err(|_| RuntimeError::UnsupportedRuntime)?;
    if !matches!(
        manifest.artifact.format,
        ArtifactFormat::NodeOciDescriptorV1 | ArtifactFormat::HybridOciArtifactV1
    ) || manifest.artifact.size_bytes
        != u64::try_from(bytes.len()).map_err(|_| RuntimeError::InvalidArtifact)?
        || manifest.artifact.digest != Sha256Digest::of(bytes)
    {
        return Err(RuntimeError::InvalidArtifact);
    }
    Ok(())
}

pub(crate) struct DecodedNodeResponse {
    pub(crate) result: Result<FullNodeActionOutcome, RuntimeError>,
    pub(crate) resources: Option<PerformanceResourceUsage>,
}

pub(crate) fn decode_response_measured(bytes: &[u8]) -> DecodedNodeResponse {
    let decoded = decode_response_inner(bytes);
    match decoded {
        Ok((result, resources)) => DecodedNodeResponse { result, resources },
        Err(error) => DecodedNodeResponse {
            result: Err(error),
            resources: None,
        },
    }
}

fn decode_response_inner(
    bytes: &[u8],
) -> Result<
    (
        Result<FullNodeActionOutcome, RuntimeError>,
        Option<PerformanceResourceUsage>,
    ),
    RuntimeError,
> {
    let response: NodeResponseV1 =
        serde_json::from_slice(bytes).map_err(|_| RuntimeError::InvalidResult)?;
    if response.protocol_version != 1 {
        return Err(RuntimeError::InvalidResult);
    }
    let resources = response.performance.map(Into::into);
    match (response.ok, response.value, response.error) {
        (true, Some(value), None) => Ok((
            Ok(FullNodeActionOutcome {
                value: value
                    .into_canonical()
                    .map_err(|_| RuntimeError::InvalidResult)?,
                resource_usage: resources,
            }),
            resources,
        )),
        (false, None, Some(error)) => {
            let _ = error.code;
            Ok((Err(RuntimeError::JavaScript), resources))
        }
        _ => Err(RuntimeError::InvalidResult),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeRequestV1 {
    protocol_version: u8,
    collect_performance: bool,
    release_id: String,
    invocation_id: String,
    function: String,
    implementation_hash: String,
    arguments_contract_hash: String,
    result_contract_hash: String,
    arguments: WireValueV1,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeResponseV1 {
    protocol_version: u8,
    ok: bool,
    value: Option<WireValueV1>,
    error: Option<NodeErrorV1>,
    performance: Option<NodePerformanceV1>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodePerformanceV1 {
    user_cpu_micros: u64,
    system_cpu_micros: u64,
    peak_memory_bytes: u64,
    memory_bytes: u64,
}

impl From<NodePerformanceV1> for PerformanceResourceUsage {
    fn from(value: NodePerformanceV1) -> Self {
        Self {
            user_cpu_micros: Some(value.user_cpu_micros),
            system_cpu_micros: Some(value.system_cpu_micros),
            peak_memory_bytes: Some(value.peak_memory_bytes),
            memory_bytes: Some(value.memory_bytes),
            ..Self::default()
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeErrorV1 {
    code: String,
}
