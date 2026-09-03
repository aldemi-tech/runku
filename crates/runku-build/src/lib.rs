//! Strict, reproducible source-to-Release build toolchain.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod codegen;
mod compiler;
mod declarative;
mod node_oci;
mod output;

use std::{collections::BTreeSet, path::Path};

use runku_contracts::{Contract, encode_contract, encode_document_schema};
use runku_core::{BuildId, FunctionId, ProjectId, ReleaseId};
use runku_releases::{
    AuthPolicy, Capability, CronDefinition, FunctionManifest, FunctionType, FunctionVisibility,
    NodeEsmBundleV1, ReleaseManifestV1, RuntimeClass, SafeEsmBundleV1, Sha256Digest,
    encode_node_esm_bundle, encode_release_manifest, encode_safe_esm_bundle,
};
use runku_schema::encode_schema_catalog;
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};
use thiserror::Error;
use ulid::Ulid;

pub use node_oci::{
    NodeOciPublishConfig, NodeOciPublishError, NodeOciPublishResult, RemoteNodePublisher,
};
pub use output::BuildOutput;

use codegen::generate_types;
use compiler::compile_module;
use declarative::{LoadedCron, LoadedFunction, input_fingerprint, load_project};
use output::publish_output;

const CONTRACT_RUNTIME_VERSION: &str = "runku-js-1";
const NODE_RUNTIME_VERSION: &str = "runku-node-1";
const HYBRID_RUNTIME_VERSION: &str = "runku-hybrid-1";
const CONTRACT_STORAGE_RUNTIME_VERSION: &str = "runku-js-2";
const NODE_STORAGE_RUNTIME_VERSION: &str = "runku-node-2";
const HYBRID_STORAGE_RUNTIME_VERSION: &str = "runku-hybrid-2";
const FUNCTION_ID_DOMAIN: &[u8] = b"RUNKU_FUNCTION_ID_V1";
const FUNCTION_CONTRACT_DOMAIN: &[u8] = b"RUNKU_FUNCTION_CONTRACT_V1";
type CompiledFunctions = Vec<(LoadedFunction, Sha256Digest)>;

enum CompiledArtifact {
    Safe(SafeEsmBundleV1),
    LocalNode(NodeEsmBundleV1),
    Hybrid(NodeEsmBundleV1),
}

/// Stable source build failure category.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BuildError {
    /// Root/config/source/output path is unsafe or unsupported.
    #[error("build path is invalid")]
    InvalidPath,
    /// Descriptor bytes or declared metadata are malformed.
    #[error("build configuration is invalid")]
    InvalidConfig,
    /// A recognized runtime or source construct is unavailable in this gate.
    #[error("build input requests an unsupported feature")]
    Unsupported,
    /// TypeScript/JavaScript source has invalid syntax.
    #[error("function source syntax is invalid")]
    SourceSyntax,
    /// Source crosses an incompatible Safe V8/Full Node module boundary.
    #[error("function source violates runtime module policy")]
    SourcePolicy,
    /// A declared input or emitted artifact exceeds a v1 limit.
    #[error("build input exceeds a v1 limit")]
    LimitExceeded,
    /// An immutable output already exists with different bytes.
    #[error("build output conflicts with existing immutable output")]
    Conflict,
    /// Filesystem state is temporarily unavailable.
    #[error("build dependency is unavailable")]
    Unavailable,
    /// Existing durable output violates its integrity contract.
    #[error("build output is corrupt")]
    Corruption,
    /// Canonical component composition failed unexpectedly.
    #[error("build failed internally")]
    Internal,
}

impl BuildError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidPath => "BUILD_PATH_INVALID",
            Self::InvalidConfig => "BUILD_CONFIG_INVALID",
            Self::Unsupported => "BUILD_FEATURE_UNSUPPORTED",
            Self::SourceSyntax => "BUILD_SOURCE_SYNTAX_INVALID",
            Self::SourcePolicy => "BUILD_SOURCE_POLICY_DENIED",
            Self::LimitExceeded => "BUILD_LIMIT_EXCEEDED",
            Self::Conflict => "BUILD_OUTPUT_CONFLICT",
            Self::Unavailable => "BUILD_UNAVAILABLE",
            Self::Corruption => "BUILD_OUTPUT_CORRUPT",
            Self::Internal => "BUILD_INTERNAL",
        }
    }

    /// Whether unchanged input may succeed after an external dependency recovers.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

/// Immutable metadata embedded into one Release Manifest build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildMetadata {
    /// Release identity allocated for this immutable output.
    pub release_id: ReleaseId,
    /// Build attempt identity.
    pub build_id: BuildId,
    /// Trusted build completion time.
    pub created_at: TimestampMicros,
}

impl BuildMetadata {
    /// Allocates fresh Release/Build identities at one explicit time.
    #[must_use]
    pub fn generate(created_at: TimestampMicros) -> Self {
        Self {
            release_id: ReleaseId::generate(),
            build_id: BuildId::generate(),
            created_at,
        }
    }
}

/// Computes a bounded content fingerprint for the canonical declarative source tree.
///
/// Missing, malformed, oversized, or symlinked files produce distinct stable fingerprints instead
/// of being followed. This lets a watcher suppress duplicate diagnostics and notice recovery. The
/// project root and relative config path themselves must still satisfy the normal safety policy.
///
/// # Errors
///
/// Rejects an unsafe/missing root, an invalid relative config path, or hashing limits.
pub fn source_fingerprint(root: &Path, source_dir: &Path) -> Result<Sha256Digest, BuildError> {
    input_fingerprint(root, source_dir)
}

/// Builds one strict declarative project and atomically publishes immutable package files.
///
/// `source_dir` is relative to the canonical project root. Output always lives under the private
/// `.runku/builds-v1/<release-id>` namespace; the builder may create only these output directories
/// and does not initialize local databases or an Environment.
///
/// # Errors
///
/// Rejects unsafe paths, invalid declarations/source syntax or policy, limits, conflicting output,
/// and canonical Release composition failures.
pub fn build_project(
    root: &Path,
    source_dir: &Path,
    project_id: ProjectId,
    metadata: BuildMetadata,
) -> Result<BuildOutput, BuildError> {
    if metadata.created_at.get() < 0 {
        return Err(BuildError::InvalidConfig);
    }
    let loaded = load_project(root, source_dir, project_id)?;
    let source_fingerprint = loaded.fingerprint;
    let generated_types = generate_types(&loaded.functions, &loaded.schema, &loaded.index_catalog)?;
    let schema = loaded.schema;
    let (mut sources, function_entries) = compile_functions(loaded.functions)?;
    let mut contract_resources = Vec::new();
    let functions =
        build_function_contracts(project_id, &function_entries, &mut contract_resources)?;
    let function_contract_hash = function_contract_digest(&functions)?;
    let schema_bytes = encode_document_schema(&schema).map_err(map_contract)?;
    let schema_contract_hash = Sha256Digest::of(&schema_bytes);
    contract_resources.push(String::from_utf8(schema_bytes).map_err(|_| BuildError::Internal)?);
    let index_bytes =
        encode_schema_catalog(&loaded.index_catalog).map_err(|_| BuildError::InvalidConfig)?;
    let index_contract_hash = Sha256Digest::of(&index_bytes);
    if index_contract_hash.as_bytes() != &loaded.index_catalog.digest() {
        return Err(BuildError::Internal);
    }
    contract_resources.push(String::from_utf8(index_bytes).map_err(|_| BuildError::Internal)?);
    sources.extend(contract_resources);
    let all_safe = functions
        .iter()
        .all(|function| function.runtime_class == RuntimeClass::SafeV8);
    let all_node = functions
        .iter()
        .all(|function| function.runtime_class == RuntimeClass::FullNode);
    let artifact = match (all_safe, all_node) {
        (true, false) => {
            CompiledArtifact::Safe(SafeEsmBundleV1::from_sources(sources).map_err(map_release)?)
        }
        (false, true) => CompiledArtifact::LocalNode(
            NodeEsmBundleV1::from_sources(sources).map_err(map_release)?,
        ),
        (false, false) => {
            CompiledArtifact::Hybrid(NodeEsmBundleV1::from_sources(sources).map_err(map_release)?)
        }
        (true, true) => return Err(BuildError::Internal),
    };
    let storage_runtime = functions.iter().any(|function| {
        function
            .capabilities
            .iter()
            .any(|capability| matches!(capability, Capability::FileRead | Capability::FileWrite))
    });
    let runtime_version = artifact_runtime_version(&artifact, storage_runtime);
    let (artifact_bytes, artifact_descriptor) = match &artifact {
        CompiledArtifact::Safe(bundle) => (
            encode_safe_esm_bundle(bundle).map_err(map_release)?,
            bundle.descriptor().map_err(map_release)?,
        ),
        CompiledArtifact::LocalNode(bundle) | CompiledArtifact::Hybrid(bundle) => (
            encode_node_esm_bundle(bundle).map_err(map_release)?,
            bundle.descriptor().map_err(map_release)?,
        ),
    };
    let cron_definitions = build_crons(loaded.crons, &function_entries)?;
    let manifest = ReleaseManifestV1 {
        release_id: metadata.release_id,
        project_id,
        build_id: metadata.build_id,
        created_at: metadata.created_at,
        runtime_version: runtime_version.parse().map_err(|_| BuildError::Internal)?,
        artifact: artifact_descriptor,
        function_contract_hash,
        schema_contract_hash,
        index_contract_hash,
        functions,
        cron_definitions,
    };
    match artifact {
        CompiledArtifact::Safe(bundle) => bundle
            .verify_manifest(&manifest, &artifact_bytes)
            .map_err(map_release)?,
        CompiledArtifact::LocalNode(bundle) | CompiledArtifact::Hybrid(bundle) => bundle
            .verify_manifest(&manifest, &artifact_bytes)
            .map_err(map_release)?,
    }
    let manifest_bytes = encode_release_manifest(&manifest).map_err(map_release)?;
    publish_output(
        root,
        source_dir,
        &manifest,
        &manifest_bytes,
        &artifact_bytes,
        &generated_types,
        source_fingerprint,
    )
}

fn artifact_runtime_version(artifact: &CompiledArtifact, storage: bool) -> &'static str {
    match (artifact, storage) {
        (CompiledArtifact::Safe(_), false) => CONTRACT_RUNTIME_VERSION,
        (CompiledArtifact::Safe(_), true) => CONTRACT_STORAGE_RUNTIME_VERSION,
        (CompiledArtifact::LocalNode(_), false) => NODE_RUNTIME_VERSION,
        (CompiledArtifact::LocalNode(_), true) => NODE_STORAGE_RUNTIME_VERSION,
        (CompiledArtifact::Hybrid(_), false) => HYBRID_RUNTIME_VERSION,
        (CompiledArtifact::Hybrid(_), true) => HYBRID_STORAGE_RUNTIME_VERSION,
    }
}

fn compile_functions(
    functions: Vec<LoadedFunction>,
) -> Result<(Vec<String>, CompiledFunctions), BuildError> {
    let mut sources = Vec::with_capacity(functions.len());
    let mut entries = Vec::with_capacity(functions.len());
    for function in functions {
        let source = match function.runtime_class {
            RuntimeClass::SafeV8 => compile_module(&function.source_path, &function.source_text)?,
            RuntimeClass::FullNode => function.source_text.clone(),
        };
        let implementation_hash = Sha256Digest::of(source.as_bytes());
        sources.push(source);
        entries.push((function, implementation_hash));
    }
    Ok((sources, entries))
}

fn build_function_contracts(
    project_id: ProjectId,
    entries: &CompiledFunctions,
    resources: &mut Vec<String>,
) -> Result<Vec<FunctionManifest>, BuildError> {
    entries
        .iter()
        .map(|(function, implementation_hash)| {
            let arguments_contract_hash = contract_digest(&function.arguments_contract, resources)?;
            let result_contract_hash = contract_digest(&function.result_contract, resources)?;
            Ok(FunctionManifest {
                id: stable_function_id(project_id, function.name.as_str()),
                name: function.name.clone(),
                function_type: function.function_type,
                visibility: function.visibility,
                auth_policy: function.auth_policy,
                runtime_class: function.runtime_class,
                implementation_hash: *implementation_hash,
                arguments_contract_hash,
                result_contract_hash,
                capabilities: function.capabilities.clone(),
            })
        })
        .collect()
}

fn build_crons(
    crons: Vec<LoadedCron>,
    functions: &CompiledFunctions,
) -> Result<Vec<CronDefinition>, BuildError> {
    crons
        .into_iter()
        .map(|cron| {
            functions
                .iter()
                .find(|(function, _)| function.name == cron.function)
                .map(|(function, _)| &function.arguments_contract)
                .ok_or(BuildError::InvalidConfig)?
                .validate_value(&cron.args)
                .map_err(|_| BuildError::InvalidConfig)?;
            Ok(CronDefinition {
                name: cron.name,
                schedule: cron.schedule,
                function: cron.function,
                args: cron.args,
            })
        })
        .collect()
}

fn contract_digest(
    contract: &Contract,
    resources: &mut Vec<String>,
) -> Result<Sha256Digest, BuildError> {
    let bytes = encode_contract(contract).map_err(map_contract)?;
    let digest = Sha256Digest::of(&bytes);
    resources.push(String::from_utf8(bytes).map_err(|_| BuildError::Internal)?);
    Ok(digest)
}

fn stable_function_id(project_id: ProjectId, name: &str) -> FunctionId {
    let digest = domain_bytes(
        FUNCTION_ID_DOMAIN,
        &[project_id.to_string().as_bytes(), name.as_bytes()],
    );
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    FunctionId::from_ulid(Ulid::from(u128::from_be_bytes(bytes)))
}

fn function_contract_digest(functions: &[FunctionManifest]) -> Result<Sha256Digest, BuildError> {
    let mut hash = Sha256::new();
    hash.update(FUNCTION_CONTRACT_DOMAIN);
    hash.update(
        u32::try_from(functions.len())
            .map_err(|_| BuildError::LimitExceeded)?
            .to_be_bytes(),
    );
    let mut ids = BTreeSet::new();
    for function in functions {
        if !ids.insert(function.id) {
            return Err(BuildError::Internal);
        }
        hash_text(&mut hash, &function.id.to_string())?;
        hash_text(&mut hash, function.name.as_str())?;
        hash.update([
            function_type_tag(function.function_type),
            visibility_tag(function.visibility),
            auth_tag(function.auth_policy),
            runtime_tag(function.runtime_class),
        ]);
        hash.update(function.arguments_contract_hash.as_bytes());
        hash.update(function.result_contract_hash.as_bytes());
        hash.update(
            u32::try_from(function.capabilities.len())
                .map_err(|_| BuildError::LimitExceeded)?
                .to_be_bytes(),
        );
        for capability in &function.capabilities {
            hash_text(&mut hash, &capability_text(capability))?;
        }
    }
    Ok(Sha256Digest::from_bytes(hash.finalize().into()))
}

const fn function_type_tag(value: FunctionType) -> u8 {
    match value {
        FunctionType::Query => 1,
        FunctionType::Mutation => 2,
        FunctionType::Action => 3,
    }
}

const fn visibility_tag(value: FunctionVisibility) -> u8 {
    match value {
        FunctionVisibility::Public => 1,
        FunctionVisibility::Internal => 2,
    }
}

const fn auth_tag(value: AuthPolicy) -> u8 {
    match value {
        AuthPolicy::None => 1,
        AuthPolicy::Optional => 2,
        AuthPolicy::Guest => 3,
        AuthPolicy::User => 4,
        AuthPolicy::Service => 5,
    }
}

const fn runtime_tag(value: RuntimeClass) -> u8 {
    match value {
        RuntimeClass::SafeV8 => 1,
        RuntimeClass::FullNode => 2,
    }
}

fn capability_text(value: &Capability) -> String {
    match value {
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
    }
}

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), BuildError> {
    hash.update(
        u32::try_from(value.len())
            .map_err(|_| BuildError::LimitExceeded)?
            .to_be_bytes(),
    );
    hash.update(value.as_bytes());
    Ok(())
}

fn domain_bytes(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    for part in parts {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        hash.update(part);
    }
    hash.finalize().into()
}

fn map_release(error: runku_releases::ReleaseError) -> BuildError {
    match error {
        runku_releases::ReleaseError::LimitExceeded => BuildError::LimitExceeded,
        runku_releases::ReleaseError::Unsupported => BuildError::Unsupported,
        runku_releases::ReleaseError::InvalidManifest
        | runku_releases::ReleaseError::InvalidArtifact
        | runku_releases::ReleaseError::DigestMismatch
        | runku_releases::ReleaseError::DescriptorMismatch => BuildError::InvalidConfig,
        _ => BuildError::Internal,
    }
}

fn map_contract(error: runku_contracts::ContractError) -> BuildError {
    match error {
        runku_contracts::ContractError::LimitExceeded => BuildError::LimitExceeded,
        runku_contracts::ContractError::InvalidDefinition
        | runku_contracts::ContractError::InvalidEncoding => BuildError::InvalidConfig,
    }
}
