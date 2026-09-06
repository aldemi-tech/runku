use std::{collections::BTreeMap, time::Instant};

use runku_observability::PerformanceResourceUsage;
use runku_protocol::WireValueV1;
use runku_releases::{
    ArtifactFormat, Capability, FullNodeEgressPolicy, FunctionManifest, FunctionType,
    ReleaseManifestV1, RuntimeClass, Sha256Digest, decode_node_oci_descriptor,
};
use runku_runtime::{
    ConfigurationReadError, ConfigurationValueKind, InvocationRequest, RuntimeError,
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::FullNodeActionOutcome;

pub(crate) struct PreparedRequest {
    pub(crate) image_reference: String,
    pub(crate) input: Vec<u8>,
    pub(crate) egress: FullNodeEgressPolicy,
}

pub(crate) async fn prepare_request(
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
    let mut configuration = resolve_configuration(request, function).await?;
    let input = serde_json::to_vec(&NodeRequestV1 {
        protocol_version: 1,
        collect_performance: request.performance().is_some(),
        release_id: request.release_id().to_string(),
        invocation_id: request.invocation_id().to_string(),
        function: function.name.as_str().to_owned(),
        implementation_hash: function.implementation_hash.to_string(),
        arguments_contract_hash: function.arguments_contract_hash.to_string(),
        result_contract_hash: function.result_contract_hash.to_string(),
        capabilities: function.capabilities.iter().map(capability_name).collect(),
        variables: &configuration.variables,
        secrets: &configuration.secrets,
        arguments,
    })
    .map_err(|_| RuntimeError::Internal)?;
    configuration.zeroize();
    Ok(PreparedRequest {
        image_reference: descriptor.image_reference().to_owned(),
        input,
        egress: descriptor.egress_policy().clone(),
    })
}

#[derive(Default)]
pub(crate) struct ResolvedConfiguration {
    pub(crate) variables: BTreeMap<String, String>,
    pub(crate) secrets: BTreeMap<String, String>,
}

impl Zeroize for ResolvedConfiguration {
    fn zeroize(&mut self) {
        for value in self.variables.values_mut() {
            value.zeroize();
        }
        for value in self.secrets.values_mut() {
            value.zeroize();
        }
        self.variables.clear();
        self.secrets.clear();
    }
}

pub(crate) async fn resolve_configuration(
    request: &InvocationRequest,
    function: &FunctionManifest,
) -> Result<ResolvedConfiguration, RuntimeError> {
    let requested = function
        .capabilities
        .iter()
        .any(|capability| matches!(capability, Capability::Variable(_) | Capability::Secret(_)));
    if !requested {
        return Ok(ResolvedConfiguration::default());
    }
    let broker = request.configuration().ok_or(RuntimeError::Unavailable)?;
    let deadline = Instant::now()
        .checked_add(request.wall_timeout())
        .ok_or(RuntimeError::InvalidInvocation)?;
    let mut resolved = ResolvedConfiguration::default();
    for capability in &function.capabilities {
        let (kind, name, destination) = match capability {
            Capability::Variable(name) => (
                ConfigurationValueKind::Variable,
                name,
                &mut resolved.variables,
            ),
            Capability::Secret(name) if function.function_type == FunctionType::Action => {
                (ConfigurationValueKind::Secret, name, &mut resolved.secrets)
            }
            Capability::Secret(_) => return Err(RuntimeError::InvalidInvocation),
            _ => continue,
        };
        let value = broker
            .read(kind, name, deadline, request.cancellation())
            .await
            .map_err(map_configuration_error)?;
        destination.insert(name.clone(), value.to_string());
    }
    Ok(resolved)
}

pub(crate) fn capability_name(capability: &Capability) -> String {
    match capability {
        Capability::DbRead => "db:read".to_owned(),
        Capability::DbWrite => "db:write".to_owned(),
        Capability::AuthRead => "auth:read".to_owned(),
        Capability::FunctionQuery => "function:query".to_owned(),
        Capability::FunctionMutation => "function:mutation".to_owned(),
        Capability::FunctionAction => "function:action".to_owned(),
        Capability::NetworkHttps => "network:https".to_owned(),
        Capability::SchedulerCreate => "scheduler:create".to_owned(),
        Capability::FileRead => "storage:read".to_owned(),
        Capability::FileWrite => "storage:write".to_owned(),
        Capability::Secret(name) => format!("secret:{name}"),
        Capability::Variable(name) => format!("variable:{name}"),
    }
}

const fn map_configuration_error(error: ConfigurationReadError) -> RuntimeError {
    match error {
        ConfigurationReadError::Unavailable => RuntimeError::Unavailable,
        ConfigurationReadError::Timeout => RuntimeError::DeadlineExceeded,
        ConfigurationReadError::Cancelled => RuntimeError::Cancelled,
        ConfigurationReadError::InvalidRequest
        | ConfigurationReadError::NotFound
        | ConfigurationReadError::Corruption => RuntimeError::JavaScript,
    }
}

pub(crate) fn validate_artifact(
    manifest: &ReleaseManifestV1,
    bytes: &[u8],
) -> Result<(), RuntimeError> {
    manifest
        .ensure_full_node_supported()
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
struct NodeRequestV1<'a> {
    protocol_version: u8,
    collect_performance: bool,
    release_id: String,
    invocation_id: String,
    function: String,
    implementation_hash: String,
    arguments_contract_hash: String,
    result_contract_hash: String,
    capabilities: Vec<String>,
    variables: &'a BTreeMap<String, String>,
    secrets: &'a BTreeMap<String, String>,
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
