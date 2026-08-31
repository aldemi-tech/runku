//! Canonical self-contained ESM bundle consumed by the Safe Runtime.

use std::collections::BTreeMap;

use crate::{
    ARTIFACT_MAX_BYTES, ArtifactDescriptor, ArtifactFormat, ReleaseError, ReleaseManifestV1,
    RuntimeClass, Sha256Digest,
};

/// Current canonical Safe ESM bundle format version.
pub const SAFE_ESM_BUNDLE_FORMAT_VERSION: u8 = 1;
/// Current canonical local Node ESM resource bundle format version.
pub const NODE_ESM_BUNDLE_FORMAT_VERSION: u8 = SAFE_ESM_BUNDLE_FORMAT_VERSION;
/// Maximum UTF-8 bytes in one precompiled Function implementation.
pub const SAFE_ESM_IMPLEMENTATION_MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_IMPLEMENTATIONS: usize = 1_000;
const MAGIC: &[u8; 3] = b"RB\x01";

/// Immutable collection of self-contained ESM Function implementations keyed by source digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SafeEsmBundleV1 {
    implementations: BTreeMap<Sha256Digest, String>,
}

/// Canonical ESM resource bundle consumed by the developer machine's local Node binary.
///
/// It intentionally shares the strict content-addressed resource envelope with
/// [`SafeEsmBundleV1`] while using a distinct manifest artifact tag, preventing local Node output
/// from being confused with a remotely executable OCI artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeEsmBundleV1(SafeEsmBundleV1);

impl NodeEsmBundleV1 {
    /// Builds a canonical, content-deduplicated local Node resource bundle.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, or excessive source collections.
    pub fn from_sources<I, S>(sources: I) -> Result<Self, ReleaseError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        SafeEsmBundleV1::from_sources(sources).map(Self)
    }

    /// Returns one exact implementation source by content digest.
    #[must_use]
    pub fn source(&self, digest: Sha256Digest) -> Option<&str> {
        self.0.source(digest)
    }

    /// Returns one exact resource by content digest.
    #[must_use]
    pub fn resource(&self, digest: Sha256Digest) -> Option<&str> {
        self.0.resource(digest)
    }

    /// Derives the immutable local Node artifact descriptor.
    ///
    /// # Errors
    ///
    /// Returns a stable limit error when canonical bytes exceed Artifact v1 limits.
    pub fn descriptor(&self) -> Result<ArtifactDescriptor, ReleaseError> {
        let bytes = encode_node_esm_bundle(self)?;
        Ok(ArtifactDescriptor {
            format: ArtifactFormat::NodeEsmBundleV1,
            digest: Sha256Digest::of(&bytes),
            size_bytes: u64::try_from(bytes.len()).map_err(|_| ReleaseError::LimitExceeded)?,
        })
    }

    /// Verifies exact bytes and implementation resources against a local Full Node manifest.
    ///
    /// # Errors
    ///
    /// Fails closed on descriptor drift, unsupported Functions, or absent code/contracts.
    pub fn verify_manifest(
        &self,
        manifest: &ReleaseManifestV1,
        artifact_bytes: &[u8],
    ) -> Result<(), ReleaseError> {
        manifest.ensure_local_full_node_supported()?;
        if manifest.artifact.format != ArtifactFormat::NodeEsmBundleV1
            || manifest.artifact.size_bytes
                != u64::try_from(artifact_bytes.len()).map_err(|_| ReleaseError::LimitExceeded)?
        {
            return Err(ReleaseError::DescriptorMismatch);
        }
        if manifest.artifact.digest != Sha256Digest::of(artifact_bytes) {
            return Err(ReleaseError::DigestMismatch);
        }
        if manifest.functions.iter().any(|function| {
            self.source(function.implementation_hash).is_none()
                || (function.runtime_class == RuntimeClass::FullNode
                    && function.function_type != crate::FunctionType::Action)
        }) || self.resource(manifest.schema_contract_hash).is_none()
            || self.resource(manifest.index_contract_hash).is_none()
            || manifest.functions.iter().any(|function| {
                self.resource(function.arguments_contract_hash).is_none()
                    || self.resource(function.result_contract_hash).is_none()
            })
        {
            return Err(ReleaseError::InvalidArtifact);
        }
        Ok(())
    }
}

impl SafeEsmBundleV1 {
    /// Builds a canonical bundle and derives each implementation key from its exact UTF-8 bytes.
    ///
    /// Identical sources are content-deduplicated. Empty, oversized, or excessive inputs fail.
    ///
    /// # Errors
    ///
    /// Returns a stable artifact or limit error.
    pub fn from_sources<I, S>(sources: I) -> Result<Self, ReleaseError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut implementations = BTreeMap::new();
        for source in sources {
            let source = source.into();
            validate_source(&source)?;
            implementations.insert(Sha256Digest::of(source.as_bytes()), source);
            if implementations.len() > MAX_IMPLEMENTATIONS {
                return Err(ReleaseError::LimitExceeded);
            }
        }
        if implementations.is_empty() {
            return Err(ReleaseError::InvalidArtifact);
        }
        let bundle = Self { implementations };
        encode_safe_esm_bundle(&bundle)?;
        Ok(bundle)
    }

    /// Returns the exact implementation source selected by a manifest digest.
    #[must_use]
    pub fn source(&self, digest: Sha256Digest) -> Option<&str> {
        self.resource(digest)
    }

    /// Returns one exact UTF-8 resource selected by its content digest.
    ///
    /// Runtime implementations and `runku-js-1` canonical contract definitions share the
    /// same immutable content-addressed namespace without making contract bytes executable.
    #[must_use]
    pub fn resource(&self, digest: Sha256Digest) -> Option<&str> {
        self.implementations.get(&digest).map(String::as_str)
    }

    /// Returns the number of distinct implementation sources.
    #[must_use]
    pub fn len(&self) -> usize {
        self.implementations.len()
    }

    /// Returns whether no implementation exists. Valid decoded/constructed bundles are non-empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.implementations.is_empty()
    }

    /// Verifies that every implementation and canonical contract referenced by a manifest exists.
    ///
    /// This is used after an outer hybrid artifact has already been authenticated by descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseError::InvalidArtifact`] when any referenced resource is absent.
    pub fn verify_resources(&self, manifest: &ReleaseManifestV1) -> Result<(), ReleaseError> {
        if manifest
            .functions
            .iter()
            .any(|function| self.source(function.implementation_hash).is_none())
            || self.resource(manifest.schema_contract_hash).is_none()
            || self.resource(manifest.index_contract_hash).is_none()
            || manifest.functions.iter().any(|function| {
                self.resource(function.arguments_contract_hash).is_none()
                    || self.resource(function.result_contract_hash).is_none()
            })
        {
            return Err(ReleaseError::InvalidArtifact);
        }
        Ok(())
    }

    /// Encodes the bundle and derives its immutable artifact descriptor.
    ///
    /// # Errors
    ///
    /// Returns a stable limit error if its canonical bytes exceed Artifact v1 limits.
    pub fn descriptor(&self) -> Result<ArtifactDescriptor, ReleaseError> {
        let bytes = encode_safe_esm_bundle(self)?;
        Ok(ArtifactDescriptor {
            format: ArtifactFormat::SafeEsmBundleV1,
            digest: Sha256Digest::of(&bytes),
            size_bytes: u64::try_from(bytes.len()).map_err(|_| ReleaseError::LimitExceeded)?,
        })
    }

    /// Verifies that exact artifact bytes and every Function implementation satisfy a manifest.
    ///
    /// # Errors
    ///
    /// Fails closed on descriptor drift, unsupported runtime class, or missing implementation.
    pub fn verify_manifest(
        &self,
        manifest: &ReleaseManifestV1,
        artifact_bytes: &[u8],
    ) -> Result<(), ReleaseError> {
        manifest.ensure_mvp_runtime_supported()?;
        if manifest.artifact.format != ArtifactFormat::SafeEsmBundleV1
            || manifest.artifact.size_bytes
                != u64::try_from(artifact_bytes.len()).map_err(|_| ReleaseError::LimitExceeded)?
        {
            return Err(ReleaseError::DescriptorMismatch);
        }
        if manifest.artifact.digest != Sha256Digest::of(artifact_bytes) {
            return Err(ReleaseError::DigestMismatch);
        }
        if manifest.functions.iter().any(|function| {
            function.runtime_class != RuntimeClass::SafeV8
                || self.source(function.implementation_hash).is_none()
        }) {
            return Err(ReleaseError::InvalidArtifact);
        }
        if manifest.runtime_version.as_str() == "runku-js-1"
            && (self.resource(manifest.schema_contract_hash).is_none()
                || self.resource(manifest.index_contract_hash).is_none()
                || manifest.functions.iter().any(|function| {
                    self.resource(function.arguments_contract_hash).is_none()
                        || self.resource(function.result_contract_hash).is_none()
                }))
        {
            return Err(ReleaseError::InvalidArtifact);
        }
        Ok(())
    }
}

/// Encodes a Safe ESM bundle into its strict canonical v1 representation.
///
/// # Errors
///
/// Returns an artifact/limit error for invalid in-memory content.
pub fn encode_safe_esm_bundle(bundle: &SafeEsmBundleV1) -> Result<Vec<u8>, ReleaseError> {
    if bundle.implementations.is_empty() || bundle.implementations.len() > MAX_IMPLEMENTATIONS {
        return Err(ReleaseError::InvalidArtifact);
    }
    let mut output = Vec::new();
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(
        &u16::try_from(bundle.implementations.len())
            .map_err(|_| ReleaseError::LimitExceeded)?
            .to_be_bytes(),
    );
    for (digest, source) in &bundle.implementations {
        validate_source(source)?;
        if *digest != Sha256Digest::of(source.as_bytes()) {
            return Err(ReleaseError::DigestMismatch);
        }
        output.extend_from_slice(digest.as_bytes());
        output.extend_from_slice(
            &u32::try_from(source.len())
                .map_err(|_| ReleaseError::LimitExceeded)?
                .to_be_bytes(),
        );
        output.extend_from_slice(source.as_bytes());
        if output.len() > ARTIFACT_MAX_BYTES {
            return Err(ReleaseError::LimitExceeded);
        }
    }
    Ok(output)
}

/// Decodes and revalidates strict canonical Safe ESM bundle bytes.
///
/// # Errors
///
/// Rejects unknown versions, truncation, trailing bytes, invalid UTF-8, noncanonical ordering,
/// duplicate/hash-divergent records, and all declared limits.
pub fn decode_safe_esm_bundle(bytes: &[u8]) -> Result<SafeEsmBundleV1, ReleaseError> {
    if bytes.len() > ARTIFACT_MAX_BYTES {
        return Err(ReleaseError::LimitExceeded);
    }
    if bytes.len() < MAGIC.len() || &bytes[..2] != b"RB" {
        return Err(ReleaseError::InvalidArtifact);
    }
    if bytes[2] != SAFE_ESM_BUNDLE_FORMAT_VERSION {
        return Err(ReleaseError::Unsupported);
    }
    let mut cursor = Cursor::new(&bytes[MAGIC.len()..]);
    let count = usize::from(cursor.u16()?);
    if count == 0 || count > MAX_IMPLEMENTATIONS {
        return Err(ReleaseError::InvalidArtifact);
    }
    let mut implementations = BTreeMap::new();
    let mut previous = None;
    for _ in 0..count {
        let digest = cursor.digest()?;
        if previous.is_some_and(|value| value >= digest) {
            return Err(ReleaseError::InvalidArtifact);
        }
        let length = usize::try_from(cursor.u32()?).map_err(|_| ReleaseError::LimitExceeded)?;
        if length == 0 || length > SAFE_ESM_IMPLEMENTATION_MAX_BYTES {
            return Err(ReleaseError::LimitExceeded);
        }
        let source_bytes = cursor.take(length)?;
        let source = std::str::from_utf8(source_bytes)
            .map_err(|_| ReleaseError::InvalidArtifact)?
            .to_owned();
        if Sha256Digest::of(source_bytes) != digest {
            return Err(ReleaseError::DigestMismatch);
        }
        previous = Some(digest);
        implementations.insert(digest, source);
    }
    if !cursor.is_empty() {
        return Err(ReleaseError::InvalidArtifact);
    }
    let bundle = SafeEsmBundleV1 { implementations };
    if encode_safe_esm_bundle(&bundle)? != bytes {
        return Err(ReleaseError::InvalidArtifact);
    }
    Ok(bundle)
}

/// Encodes the local Node bundle using the shared strict resource envelope.
///
/// # Errors
///
/// Returns the same canonical artifact/limit failures as the underlying resource codec.
pub fn encode_node_esm_bundle(bundle: &NodeEsmBundleV1) -> Result<Vec<u8>, ReleaseError> {
    encode_safe_esm_bundle(&bundle.0)
}

/// Decodes a strict local Node ESM resource bundle.
///
/// # Errors
///
/// Rejects malformed, noncanonical, oversized, or hash-divergent resources.
pub fn decode_node_esm_bundle(bytes: &[u8]) -> Result<NodeEsmBundleV1, ReleaseError> {
    decode_safe_esm_bundle(bytes).map(NodeEsmBundleV1)
}

fn validate_source(source: &str) -> Result<(), ReleaseError> {
    if source.is_empty() {
        return Err(ReleaseError::InvalidArtifact);
    }
    if source.len() > SAFE_ESM_IMPLEMENTATION_MAX_BYTES {
        return Err(ReleaseError::LimitExceeded);
    }
    Ok(())
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
            return Err(ReleaseError::InvalidArtifact);
        }
        let (value, remaining) = self.remaining.split_at(count);
        self.remaining = remaining;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16, ReleaseError> {
        let value: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| ReleaseError::InvalidArtifact)?;
        Ok(u16::from_be_bytes(value))
    }

    fn u32(&mut self) -> Result<u32, ReleaseError> {
        let value: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ReleaseError::InvalidArtifact)?;
        Ok(u32::from_be_bytes(value))
    }

    fn digest(&mut self) -> Result<Sha256Digest, ReleaseError> {
        let value: [u8; 32] = self
            .take(32)?
            .try_into()
            .map_err(|_| ReleaseError::InvalidArtifact)?;
        Ok(Sha256Digest::from_bytes(value))
    }
}
