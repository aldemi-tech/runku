//! Canonical mixed-runtime artifact containing Safe resources and one Node OCI descriptor.

use crate::{
    ARTIFACT_MAX_BYTES, ArtifactDescriptor, ArtifactFormat, ReleaseError, Sha256Digest,
    decode_node_esm_bundle, decode_node_oci_descriptor,
};

const MAGIC: &[u8; 3] = b"RH\x01";

/// Encodes authenticated ESM resources and an immutable Node OCI descriptor.
///
/// # Errors
///
/// Rejects malformed inner artifacts and any combined artifact beyond the canonical size limit.
pub fn encode_hybrid_oci_artifact(
    resources: &[u8],
    node_descriptor: &[u8],
) -> Result<Vec<u8>, ReleaseError> {
    decode_node_esm_bundle(resources)?;
    decode_node_oci_descriptor(node_descriptor)?;
    let mut output = Vec::with_capacity(MAGIC.len() + 8 + resources.len() + node_descriptor.len());
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(
        &u32::try_from(resources.len())
            .map_err(|_| ReleaseError::LimitExceeded)?
            .to_be_bytes(),
    );
    output.extend_from_slice(resources);
    output.extend_from_slice(
        &u32::try_from(node_descriptor.len())
            .map_err(|_| ReleaseError::LimitExceeded)?
            .to_be_bytes(),
    );
    output.extend_from_slice(node_descriptor);
    if output.len() > ARTIFACT_MAX_BYTES {
        return Err(ReleaseError::LimitExceeded);
    }
    Ok(output)
}

/// Decodes and revalidates a canonical mixed-runtime artifact.
///
/// # Errors
///
/// Rejects malformed lengths, trailing bytes, noncanonical inner artifacts and size violations.
pub fn decode_hybrid_oci_artifact(bytes: &[u8]) -> Result<(&[u8], &[u8]), ReleaseError> {
    if bytes.len() > ARTIFACT_MAX_BYTES || !bytes.starts_with(MAGIC) {
        return Err(ReleaseError::InvalidArtifact);
    }
    let mut offset = MAGIC.len();
    let resources_len = read_length(bytes, &mut offset)?;
    let resources_end = offset
        .checked_add(resources_len)
        .filter(|end| *end <= bytes.len())
        .ok_or(ReleaseError::InvalidArtifact)?;
    let resources = &bytes[offset..resources_end];
    offset = resources_end;
    let descriptor_len = read_length(bytes, &mut offset)?;
    let descriptor_end = offset
        .checked_add(descriptor_len)
        .filter(|end| *end == bytes.len())
        .ok_or(ReleaseError::InvalidArtifact)?;
    let descriptor = &bytes[offset..descriptor_end];
    decode_node_esm_bundle(resources)?;
    decode_node_oci_descriptor(descriptor)?;
    if encode_hybrid_oci_artifact(resources, descriptor)? != bytes {
        return Err(ReleaseError::InvalidArtifact);
    }
    Ok((resources, descriptor))
}

/// Derives the outer content-addressed descriptor for a canonical hybrid artifact.
///
/// # Errors
///
/// Rejects a malformed or oversized hybrid artifact.
pub fn hybrid_oci_descriptor(bytes: &[u8]) -> Result<ArtifactDescriptor, ReleaseError> {
    decode_hybrid_oci_artifact(bytes)?;
    Ok(ArtifactDescriptor {
        format: ArtifactFormat::HybridOciArtifactV1,
        digest: Sha256Digest::of(bytes),
        size_bytes: u64::try_from(bytes.len()).map_err(|_| ReleaseError::LimitExceeded)?,
    })
}

fn read_length(bytes: &[u8], offset: &mut usize) -> Result<usize, ReleaseError> {
    let end = offset.checked_add(4).ok_or(ReleaseError::InvalidArtifact)?;
    let raw: [u8; 4] = bytes
        .get(*offset..end)
        .ok_or(ReleaseError::InvalidArtifact)?
        .try_into()
        .map_err(|_| ReleaseError::InvalidArtifact)?;
    *offset = end;
    usize::try_from(u32::from_be_bytes(raw)).map_err(|_| ReleaseError::LimitExceeded)
}
