//! Release Manifest v1 model and canonical codec.

use std::{collections::BTreeSet, fmt, str::FromStr};

use runku_core::{BuildId, FunctionId, FunctionName, ProjectId, ReleaseId};
use runku_value::{CanonicalValue, TimestampMicros, decode_stored_value, encode_stored_value};

use crate::{CronName, CronSchedule, ReleaseError, Sha256Digest};

/// Current canonical manifest format version.
pub const MANIFEST_FORMAT_VERSION: u8 = 1;
/// Maximum encoded manifest size.
pub const MANIFEST_MAX_BYTES: usize = 1024 * 1024;
/// Maximum artifact blob size accepted by v1 stores.
pub const ARTIFACT_MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_FUNCTIONS: usize = 1_000;
const MAX_CRON_DEFINITIONS: usize = 128;
const MAX_CRON_ARGS_BYTES: usize = 64 * 1024;
const MAX_CAPABILITIES: usize = 32;
const MAX_RUNTIME_VERSION_BYTES: usize = 32;
const MAX_SECRET_NAME_BYTES: usize = 64;
const MAGIC: &[u8; 3] = b"RM\x01";

/// Artifact representation understood by a runtime loader.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ArtifactFormat {
    /// One deterministic ESM bundle for the safe runtime.
    SafeEsmBundleV1,
    /// Canonical descriptor pointing at one immutable Full Node OCI image.
    NodeOciDescriptorV1,
    /// Self-contained ESM resources executed by the developer machine's local Node binary.
    NodeEsmBundleV1,
    /// Safe ESM resources plus an immutable Full Node OCI descriptor for a mixed Release.
    HybridOciArtifactV1,
}

/// Runtime isolation/capability class declared by a function.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RuntimeClass {
    /// Embedded safe V8 runtime with mediated platform Ops.
    SafeV8,
    /// Out-of-process full Node runtime, recognized but unavailable in the MVP.
    FullNode,
}

/// Stable function execution semantics.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionType {
    /// Read-only reactive computation.
    Query,
    /// Transactional data mutation.
    Mutation,
    /// Non-transactional external-effect function.
    Action,
}

/// Whether a function may be invoked at the public gateway.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FunctionVisibility {
    /// Addressable through public application protocols, subject to auth policy.
    Public,
    /// Addressable only through trusted nested-call capabilities.
    Internal,
}

/// Required functional principal policy at invocation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AuthPolicy {
    /// No functional principal is required or synthesized.
    None,
    /// Missing principal is accepted; a valid supplied principal is exposed.
    Optional,
    /// A guest principal is required or created by the identity layer.
    Guest,
    /// An identified end-user principal is required.
    User,
    /// A service principal is required.
    Service,
}

/// Mediated platform capability requested by one function.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Capability {
    /// Read documents through a transaction/snapshot handle.
    DbRead,
    /// Mutate documents through the active transaction handle.
    DbWrite,
    /// Read normalized principal context.
    AuthRead,
    /// Invoke a Query in the same Release context.
    FunctionQuery,
    /// Invoke a Mutation in the same Release context.
    FunctionMutation,
    /// Invoke an Action in the same Release context.
    FunctionAction,
    /// Perform mediated HTTPS egress.
    NetworkHttps,
    /// Create a durable Scheduled Invocation.
    SchedulerCreate,
    /// Read application files and create bounded download grants.
    FileRead,
    /// Store/delete application files and create bounded upload grants.
    FileWrite,
    /// Read one named secret through the secret provider.
    Secret(String),
}

/// Canonical platform JavaScript API/runtime version.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeVersion(String);

impl RuntimeVersion {
    /// Returns the exact canonical runtime version.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuntimeVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RuntimeVersion {
    type Err = ReleaseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let suffix = value
            .strip_prefix("platform-js-")
            .or_else(|| value.strip_prefix("runku-js-"))
            .or_else(|| value.strip_prefix("runku-node-"))
            .or_else(|| value.strip_prefix("runku-hybrid-"))
            .ok_or(ReleaseError::InvalidManifest)?;
        if value.len() > MAX_RUNTIME_VERSION_BYTES
            || suffix.is_empty()
            || suffix.len() > 5
            || suffix.starts_with('0')
            || !suffix.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(ReleaseError::InvalidManifest);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Immutable artifact reference embedded in a Release manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactDescriptor {
    /// Loader format.
    pub format: ArtifactFormat,
    /// Content digest.
    pub digest: Sha256Digest,
    /// Exact byte length.
    pub size_bytes: u64,
}

/// Complete immutable function entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionManifest {
    /// Stable function identity.
    pub id: FunctionId,
    /// Logical address.
    pub name: FunctionName,
    /// Execution semantics.
    pub function_type: FunctionType,
    /// Gateway visibility.
    pub visibility: FunctionVisibility,
    /// Required functional principal.
    pub auth_policy: AuthPolicy,
    /// Runtime isolation class.
    pub runtime_class: RuntimeClass,
    /// Exact implementation/module digest.
    pub implementation_hash: Sha256Digest,
    /// Canonical arguments contract digest.
    pub arguments_contract_hash: Sha256Digest,
    /// Canonical result contract digest.
    pub result_contract_hash: Sha256Digest,
    /// Strictly ordered, unique requested capabilities.
    pub capabilities: Vec<Capability>,
}

/// Immutable Cron definition compiled into one Release Manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronDefinition {
    /// Logical name unique inside the Release and stable across compatible Releases.
    pub name: CronName,
    /// Canonical five-field UTC schedule.
    pub schedule: CronSchedule,
    /// Internal Mutation or Action destination.
    pub function: FunctionName,
    /// Canonical arguments captured into every materialized invocation.
    pub args: CanonicalValue,
}

/// Canonical immutable Release Manifest v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseManifestV1 {
    /// Immutable Release identity.
    pub release_id: ReleaseId,
    /// Owning Project.
    pub project_id: ProjectId,
    /// Reproducible build identity.
    pub build_id: BuildId,
    /// Build completion/manifest creation time.
    pub created_at: TimestampMicros,
    /// Platform API/runtime version required by the artifact.
    pub runtime_version: RuntimeVersion,
    /// Content-addressed runtime artifact.
    pub artifact: ArtifactDescriptor,
    /// Digest of the complete function contract input.
    pub function_contract_hash: Sha256Digest,
    /// Digest of the logical schema contract.
    pub schema_contract_hash: Sha256Digest,
    /// Digest of the logical index contract.
    pub index_contract_hash: Sha256Digest,
    /// Functions in strict logical-name order.
    pub functions: Vec<FunctionManifest>,
    /// Cron definitions in strict logical-name order.
    pub cron_definitions: Vec<CronDefinition>,
}

impl ReleaseManifestV1 {
    /// Validates all v1 structural, ordering, limit, and capability invariants.
    ///
    /// # Errors
    ///
    /// Returns a stable manifest/limit error before encoding or persistence.
    pub fn validate(&self) -> Result<(), ReleaseError> {
        if self.artifact.size_bytes == 0
            || self.artifact.size_bytes
                > u64::try_from(ARTIFACT_MAX_BYTES).map_err(|_| ReleaseError::Internal)?
            || self.functions.is_empty()
            || self.functions.len() > MAX_FUNCTIONS
            || self.cron_definitions.len() > MAX_CRON_DEFINITIONS
        {
            return Err(ReleaseError::LimitExceeded);
        }
        let mut ids = BTreeSet::new();
        let mut previous_name: Option<&FunctionName> = None;
        for function in &self.functions {
            if !ids.insert(function.id)
                || previous_name.is_some_and(|previous| previous >= &function.name)
                || function.capabilities.len() > MAX_CAPABILITIES
            {
                return Err(ReleaseError::InvalidManifest);
            }
            previous_name = Some(&function.name);
            let mut previous_capability: Option<&Capability> = None;
            for capability in &function.capabilities {
                if previous_capability.is_some_and(|previous| previous >= capability)
                    || !capability_allowed(function.function_type, capability)
                    || !valid_capability(capability)
                {
                    return Err(ReleaseError::InvalidManifest);
                }
                previous_capability = Some(capability);
            }
        }
        let storage_requested = self.functions.iter().any(|function| {
            function.capabilities.iter().any(|capability| {
                matches!(capability, Capability::FileRead | Capability::FileWrite)
            })
        });
        if storage_requested
            && !matches!(
                self.runtime_version.as_str(),
                "runku-js-2" | "runku-node-2" | "runku-hybrid-2"
            )
        {
            return Err(ReleaseError::InvalidManifest);
        }
        let mut previous_cron: Option<&CronName> = None;
        for cron in &self.cron_definitions {
            let target = self
                .functions
                .iter()
                .find(|function| function.name == cron.function)
                .ok_or(ReleaseError::InvalidManifest)?;
            let args =
                encode_stored_value(&cron.args).map_err(|_| ReleaseError::InvalidManifest)?;
            if previous_cron.is_some_and(|previous| previous >= &cron.name)
                || target.visibility != FunctionVisibility::Internal
                || !matches!(
                    target.function_type,
                    FunctionType::Mutation | FunctionType::Action
                )
                || args.len() > MAX_CRON_ARGS_BYTES
            {
                return Err(ReleaseError::InvalidManifest);
            }
            previous_cron = Some(&cron.name);
        }
        Ok(())
    }

    /// Rejects runtime classes intentionally recognized but unavailable in the technical MVP.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseError::Unsupported`] when any function requests `full-node`.
    pub fn ensure_mvp_runtime_supported(&self) -> Result<(), ReleaseError> {
        self.validate()?;
        if !matches!(
            self.runtime_version.as_str(),
            "platform-js-1" | "runku-js-1" | "runku-js-2"
        ) || self
            .functions
            .iter()
            .any(|function| function.runtime_class == RuntimeClass::FullNode)
        {
            return Err(ReleaseError::Unsupported);
        }
        Ok(())
    }

    /// Validates the production Full Node artifact contract.
    ///
    /// Homogeneous Node Releases use `runku-node-1` plus an OCI descriptor. Mixed Safe/Node
    /// Releases use `runku-hybrid-1` plus the canonical resources-and-OCI artifact. Full Node
    /// entrypoints remain Actions; their declared Platform Ops are enforced by the dispatcher.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseError::Unsupported`] for any contract outside the experimental slice.
    pub fn ensure_full_node_v1_supported(&self) -> Result<(), ReleaseError> {
        self.validate()?;
        let supported = match (self.runtime_version.as_str(), self.artifact.format) {
            ("runku-node-1", ArtifactFormat::NodeOciDescriptorV1) => self
                .functions
                .iter()
                .all(|function| function.runtime_class == RuntimeClass::FullNode),
            ("runku-hybrid-1", ArtifactFormat::HybridOciArtifactV1) => {
                self.functions
                    .iter()
                    .any(|function| function.runtime_class == RuntimeClass::SafeV8)
                    && self
                        .functions
                        .iter()
                        .any(|function| function.runtime_class == RuntimeClass::FullNode)
            }
            _ => false,
        };
        if !supported
            || self.functions.iter().any(|function| {
                function.runtime_class == RuntimeClass::FullNode
                    && function.function_type != FunctionType::Action
            })
        {
            return Err(ReleaseError::Unsupported);
        }
        Ok(())
    }

    /// Validates the local JavaScript artifact that may combine Safe V8 and Full Node functions.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseError::Unsupported`] unless every Full Node function is an Action and the
    /// local ESM resource artifact matches the declared homogeneous or hybrid runtime version.
    pub fn ensure_local_full_node_supported(&self) -> Result<(), ReleaseError> {
        self.validate()?;
        let version_supported = match self.runtime_version.as_str() {
            "runku-node-1" | "runku-node-2" => self
                .functions
                .iter()
                .all(|function| function.runtime_class == RuntimeClass::FullNode),
            "runku-hybrid-1" | "runku-hybrid-2" => {
                self.functions
                    .iter()
                    .any(|function| function.runtime_class == RuntimeClass::SafeV8)
                    && self
                        .functions
                        .iter()
                        .any(|function| function.runtime_class == RuntimeClass::FullNode)
            }
            _ => false,
        };
        if !version_supported
            || self.artifact.format != ArtifactFormat::NodeEsmBundleV1
            || self.functions.iter().any(|function| {
                function.runtime_class == RuntimeClass::FullNode
                    && function.function_type != FunctionType::Action
            })
        {
            return Err(ReleaseError::Unsupported);
        }
        Ok(())
    }

    /// Computes SHA-256 over the complete canonical manifest bytes.
    ///
    /// # Errors
    ///
    /// Returns a stable validation/limit error when the manifest is not canonical.
    pub fn digest(&self) -> Result<Sha256Digest, ReleaseError> {
        Ok(Sha256Digest::of(&encode_release_manifest(self)?))
    }
}

/// Encodes a validated manifest into canonical Release Manifest v1 bytes.
///
/// # Errors
///
/// Returns a stable validation/limit error.
pub fn encode_release_manifest(manifest: &ReleaseManifestV1) -> Result<Vec<u8>, ReleaseError> {
    manifest.validate()?;
    let mut output = Vec::with_capacity(1024);
    output.extend_from_slice(MAGIC);
    push_text(&mut output, &manifest.release_id.to_string())?;
    push_text(&mut output, &manifest.project_id.to_string())?;
    push_text(&mut output, &manifest.build_id.to_string())?;
    output.extend_from_slice(&manifest.created_at.get().to_be_bytes());
    push_text(&mut output, manifest.runtime_version.as_str())?;
    output.push(artifact_format_tag(manifest.artifact.format));
    output.extend_from_slice(manifest.artifact.digest.as_bytes());
    output.extend_from_slice(&manifest.artifact.size_bytes.to_be_bytes());
    for digest in [
        manifest.function_contract_hash,
        manifest.schema_contract_hash,
        manifest.index_contract_hash,
    ] {
        output.extend_from_slice(digest.as_bytes());
    }
    push_u16(&mut output, manifest.functions.len())?;
    for function in &manifest.functions {
        push_text(&mut output, &function.id.to_string())?;
        push_text(&mut output, function.name.as_str())?;
        output.extend_from_slice(&[
            function_type_tag(function.function_type),
            visibility_tag(function.visibility),
            auth_policy_tag(function.auth_policy),
            runtime_class_tag(function.runtime_class),
        ]);
        for digest in [
            function.implementation_hash,
            function.arguments_contract_hash,
            function.result_contract_hash,
        ] {
            output.extend_from_slice(digest.as_bytes());
        }
        push_u16(&mut output, function.capabilities.len())?;
        for capability in &function.capabilities {
            output.push(capability_tag(capability));
            if let Capability::Secret(name) = capability {
                push_text(&mut output, name)?;
            }
        }
    }
    push_u16(&mut output, manifest.cron_definitions.len())?;
    for cron in &manifest.cron_definitions {
        push_text(&mut output, cron.name.as_str())?;
        push_text(&mut output, cron.schedule.as_str())?;
        push_text(&mut output, cron.function.as_str())?;
        let args = encode_stored_value(&cron.args).map_err(|_| ReleaseError::InvalidManifest)?;
        push_u32(&mut output, args.len())?;
        output.extend_from_slice(&args);
    }
    if output.len() > MANIFEST_MAX_BYTES {
        return Err(ReleaseError::LimitExceeded);
    }
    Ok(output)
}

/// Decodes strict canonical Release Manifest v1 bytes.
///
/// # Errors
///
/// Rejects unsupported versions/tags, malformed UTF-8/IDs, invalid ordering, excessive counts,
/// truncation, and trailing bytes with stable errors.
pub fn decode_release_manifest(bytes: &[u8]) -> Result<ReleaseManifestV1, ReleaseError> {
    if bytes.len() > MANIFEST_MAX_BYTES {
        return Err(ReleaseError::LimitExceeded);
    }
    if bytes.len() < MAGIC.len() || &bytes[..2] != b"RM" {
        return Err(ReleaseError::InvalidManifest);
    }
    if bytes[2] != MANIFEST_FORMAT_VERSION {
        return Err(ReleaseError::Unsupported);
    }
    let mut cursor = Cursor::new(&bytes[MAGIC.len()..]);
    let release_id = cursor
        .text(64)?
        .parse()
        .map_err(|_| ReleaseError::InvalidManifest)?;
    let project_id = cursor
        .text(64)?
        .parse()
        .map_err(|_| ReleaseError::InvalidManifest)?;
    let build_id = cursor
        .text(64)?
        .parse()
        .map_err(|_| ReleaseError::InvalidManifest)?;
    let created_at = TimestampMicros::new(cursor.i64()?);
    let runtime_version = cursor.text(MAX_RUNTIME_VERSION_BYTES)?.parse()?;
    let artifact = ArtifactDescriptor {
        format: decode_artifact_format(cursor.byte()?)?,
        digest: cursor.digest()?,
        size_bytes: cursor.u64()?,
    };
    let function_contract_hash = cursor.digest()?;
    let schema_contract_hash = cursor.digest()?;
    let index_contract_hash = cursor.digest()?;
    let function_count = usize::from(cursor.u16()?);
    if function_count == 0 || function_count > MAX_FUNCTIONS {
        return Err(ReleaseError::LimitExceeded);
    }
    let mut functions = Vec::with_capacity(function_count);
    for _ in 0..function_count {
        let id = cursor
            .text(64)?
            .parse()
            .map_err(|_| ReleaseError::InvalidManifest)?;
        let name = cursor
            .text(FunctionName::MAX_BYTES)?
            .parse()
            .map_err(|_| ReleaseError::InvalidManifest)?;
        let function_type = decode_function_type(cursor.byte()?)?;
        let visibility = decode_visibility(cursor.byte()?)?;
        let auth_policy = decode_auth_policy(cursor.byte()?)?;
        let runtime_class = decode_runtime_class(cursor.byte()?)?;
        let implementation_hash = cursor.digest()?;
        let arguments_contract_hash = cursor.digest()?;
        let result_contract_hash = cursor.digest()?;
        let capability_count = usize::from(cursor.u16()?);
        if capability_count > MAX_CAPABILITIES {
            return Err(ReleaseError::LimitExceeded);
        }
        let mut capabilities = Vec::with_capacity(capability_count);
        for _ in 0..capability_count {
            let tag = cursor.byte()?;
            capabilities.push(if tag == 9 {
                Capability::Secret(cursor.text(MAX_SECRET_NAME_BYTES)?.to_owned())
            } else {
                decode_capability(tag)?
            });
        }
        functions.push(FunctionManifest {
            id,
            name,
            function_type,
            visibility,
            auth_policy,
            runtime_class,
            implementation_hash,
            arguments_contract_hash,
            result_contract_hash,
            capabilities,
        });
    }
    let cron_definitions = decode_cron_definitions(&mut cursor)?;
    if !cursor.is_empty() {
        return Err(ReleaseError::InvalidManifest);
    }
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id,
        build_id,
        created_at,
        runtime_version,
        artifact,
        function_contract_hash,
        schema_contract_hash,
        index_contract_hash,
        functions,
        cron_definitions,
    };
    manifest.validate()?;
    Ok(manifest)
}

fn decode_cron_definitions(cursor: &mut Cursor<'_>) -> Result<Vec<CronDefinition>, ReleaseError> {
    let count = usize::from(cursor.u16()?);
    if count > MAX_CRON_DEFINITIONS {
        return Err(ReleaseError::LimitExceeded);
    }
    let mut definitions = Vec::with_capacity(count);
    for _ in 0..count {
        let name = cursor
            .text(64)?
            .parse()
            .map_err(|_| ReleaseError::InvalidManifest)?;
        let schedule_text = cursor.text(512)?;
        let schedule: CronSchedule = schedule_text.parse()?;
        if schedule.as_str() != schedule_text {
            return Err(ReleaseError::InvalidManifest);
        }
        let function = cursor
            .text(FunctionName::MAX_BYTES)?
            .parse()
            .map_err(|_| ReleaseError::InvalidManifest)?;
        let args_length = usize::try_from(cursor.u32()?).map_err(|_| ReleaseError::Internal)?;
        if args_length > MAX_CRON_ARGS_BYTES {
            return Err(ReleaseError::LimitExceeded);
        }
        let args_bytes = cursor.take(args_length)?;
        let args = decode_stored_value(args_bytes).map_err(|_| ReleaseError::InvalidManifest)?;
        if encode_stored_value(&args).map_err(|_| ReleaseError::InvalidManifest)? != args_bytes {
            return Err(ReleaseError::InvalidManifest);
        }
        definitions.push(CronDefinition {
            name,
            schedule,
            function,
            args,
        });
    }
    Ok(definitions)
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ReleaseError> {
        if self.remaining.len() < count {
            return Err(ReleaseError::InvalidManifest);
        }
        let (value, remaining) = self.remaining.split_at(count);
        self.remaining = remaining;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, ReleaseError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ReleaseError> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| ReleaseError::Internal)?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, ReleaseError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| ReleaseError::Internal)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32, ReleaseError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ReleaseError::Internal)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn i64(&mut self) -> Result<i64, ReleaseError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| ReleaseError::Internal)?;
        Ok(i64::from_be_bytes(bytes))
    }

    fn text(&mut self, maximum: usize) -> Result<&'a str, ReleaseError> {
        let length = usize::from(self.u16()?);
        if length > maximum {
            return Err(ReleaseError::LimitExceeded);
        }
        std::str::from_utf8(self.take(length)?).map_err(|_| ReleaseError::InvalidManifest)
    }

    fn digest(&mut self) -> Result<Sha256Digest, ReleaseError> {
        let bytes: [u8; 32] = self
            .take(32)?
            .try_into()
            .map_err(|_| ReleaseError::Internal)?;
        Ok(Sha256Digest::from_bytes(bytes))
    }
}

const fn decode_artifact_format(tag: u8) -> Result<ArtifactFormat, ReleaseError> {
    match tag {
        1 => Ok(ArtifactFormat::SafeEsmBundleV1),
        2 => Ok(ArtifactFormat::NodeOciDescriptorV1),
        3 => Ok(ArtifactFormat::NodeEsmBundleV1),
        4 => Ok(ArtifactFormat::HybridOciArtifactV1),
        _ => Err(ReleaseError::Unsupported),
    }
}
const fn decode_function_type(tag: u8) -> Result<FunctionType, ReleaseError> {
    match tag {
        1 => Ok(FunctionType::Query),
        2 => Ok(FunctionType::Mutation),
        3 => Ok(FunctionType::Action),
        _ => Err(ReleaseError::Unsupported),
    }
}
const fn decode_visibility(tag: u8) -> Result<FunctionVisibility, ReleaseError> {
    match tag {
        1 => Ok(FunctionVisibility::Public),
        2 => Ok(FunctionVisibility::Internal),
        _ => Err(ReleaseError::Unsupported),
    }
}
const fn decode_auth_policy(tag: u8) -> Result<AuthPolicy, ReleaseError> {
    match tag {
        1 => Ok(AuthPolicy::None),
        2 => Ok(AuthPolicy::Optional),
        3 => Ok(AuthPolicy::Guest),
        4 => Ok(AuthPolicy::User),
        5 => Ok(AuthPolicy::Service),
        _ => Err(ReleaseError::Unsupported),
    }
}
const fn decode_runtime_class(tag: u8) -> Result<RuntimeClass, ReleaseError> {
    match tag {
        1 => Ok(RuntimeClass::SafeV8),
        2 => Ok(RuntimeClass::FullNode),
        _ => Err(ReleaseError::Unsupported),
    }
}
const fn decode_capability(tag: u8) -> Result<Capability, ReleaseError> {
    match tag {
        1 => Ok(Capability::DbRead),
        2 => Ok(Capability::DbWrite),
        3 => Ok(Capability::AuthRead),
        4 => Ok(Capability::FunctionQuery),
        5 => Ok(Capability::FunctionMutation),
        6 => Ok(Capability::FunctionAction),
        7 => Ok(Capability::NetworkHttps),
        8 => Ok(Capability::SchedulerCreate),
        10 => Ok(Capability::FileRead),
        11 => Ok(Capability::FileWrite),
        _ => Err(ReleaseError::Unsupported),
    }
}

fn capability_allowed(function_type: FunctionType, capability: &Capability) -> bool {
    match function_type {
        FunctionType::Query => matches!(
            capability,
            Capability::DbRead | Capability::AuthRead | Capability::FunctionQuery
        ),
        FunctionType::Mutation => matches!(
            capability,
            Capability::DbRead
                | Capability::DbWrite
                | Capability::AuthRead
                | Capability::FunctionQuery
                | Capability::FunctionMutation
                | Capability::SchedulerCreate
        ),
        FunctionType::Action => matches!(
            capability,
            Capability::AuthRead
                | Capability::FunctionQuery
                | Capability::FunctionMutation
                | Capability::FunctionAction
                | Capability::NetworkHttps
                | Capability::SchedulerCreate
                | Capability::FileRead
                | Capability::FileWrite
                | Capability::Secret(_)
        ),
    }
}

fn valid_capability(capability: &Capability) -> bool {
    match capability {
        Capability::Secret(name) => {
            !name.is_empty()
                && name.len() <= MAX_SECRET_NAME_BYTES
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        }
        _ => true,
    }
}

fn push_text(output: &mut Vec<u8>, value: &str) -> Result<(), ReleaseError> {
    push_u16(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn push_u16(output: &mut Vec<u8>, value: usize) -> Result<(), ReleaseError> {
    output.extend_from_slice(
        &u16::try_from(value)
            .map_err(|_| ReleaseError::LimitExceeded)?
            .to_be_bytes(),
    );
    Ok(())
}

fn push_u32(output: &mut Vec<u8>, value: usize) -> Result<(), ReleaseError> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| ReleaseError::LimitExceeded)?
            .to_be_bytes(),
    );
    Ok(())
}

const fn artifact_format_tag(value: ArtifactFormat) -> u8 {
    match value {
        ArtifactFormat::SafeEsmBundleV1 => 1,
        ArtifactFormat::NodeOciDescriptorV1 => 2,
        ArtifactFormat::NodeEsmBundleV1 => 3,
        ArtifactFormat::HybridOciArtifactV1 => 4,
    }
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
const fn auth_policy_tag(value: AuthPolicy) -> u8 {
    match value {
        AuthPolicy::None => 1,
        AuthPolicy::Optional => 2,
        AuthPolicy::Guest => 3,
        AuthPolicy::User => 4,
        AuthPolicy::Service => 5,
    }
}
const fn runtime_class_tag(value: RuntimeClass) -> u8 {
    match value {
        RuntimeClass::SafeV8 => 1,
        RuntimeClass::FullNode => 2,
    }
}
const fn capability_tag(value: &Capability) -> u8 {
    match value {
        Capability::DbRead => 1,
        Capability::DbWrite => 2,
        Capability::AuthRead => 3,
        Capability::FunctionQuery => 4,
        Capability::FunctionMutation => 5,
        Capability::FunctionAction => 6,
        Capability::NetworkHttps => 7,
        Capability::SchedulerCreate => 8,
        Capability::FileRead => 10,
        Capability::FileWrite => 11,
        Capability::Secret(_) => 9,
    }
}
