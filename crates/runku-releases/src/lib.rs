//! Immutable Release contracts and content-addressed artifacts.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod artifact;
mod bundle;
mod cron;
mod digest;
mod error;
mod hybrid;
mod manifest;
mod node;
mod repository;
mod routing;

pub use artifact::{ArtifactStore, FilesystemArtifactStore, FilesystemStoreRole};
pub use bundle::{
    NODE_ESM_BUNDLE_FORMAT_VERSION, NodeEsmBundleV1, SAFE_ESM_BUNDLE_FORMAT_VERSION,
    SAFE_ESM_IMPLEMENTATION_MAX_BYTES, SafeEsmBundleV1, decode_node_esm_bundle,
    decode_safe_esm_bundle, encode_node_esm_bundle, encode_safe_esm_bundle,
};
pub use cron::{CronName, CronSchedule};
pub use digest::{ParseSha256DigestError, Sha256Digest};
pub use error::ReleaseError;
pub use hybrid::{decode_hybrid_oci_artifact, encode_hybrid_oci_artifact, hybrid_oci_descriptor};
pub use manifest::{
    ARTIFACT_MAX_BYTES, ArtifactDescriptor, ArtifactFormat, AuthPolicy, Capability, CronDefinition,
    FunctionManifest, FunctionType, FunctionVisibility, MANIFEST_FORMAT_VERSION,
    MANIFEST_MAX_BYTES, ReleaseManifestV1, RuntimeClass, RuntimeVersion, decode_release_manifest,
    encode_release_manifest,
};
pub use node::{
    FULL_NODE_HARD_DENIED_CIDRS, FULL_NODE_PUBLIC_DENIED_CIDRS, FULL_NODE_TCP_RULES_MAX,
    FullNodeEgressPolicy, FullNodeNetworkMode, FullNodeTcpRule, NODE_OCI_DESCRIPTOR_FORMAT_VERSION,
    NODE_OCI_IMAGE_REFERENCE_MAX_BYTES, NodeOciDescriptorV1, decode_node_oci_descriptor,
    encode_node_oci_descriptor,
};
pub use repository::{
    ReleaseCommand, ReleaseCommandResult, ReleaseRepository, ReleaseRepositoryBackend,
    ReleaseRepositoryTelemetrySnapshot,
};
pub use routing::{
    ChannelBinding, EffectiveRelease, ReleaseLifecycle, ReleaseRouter, ReleaseStatus,
    ServingReleaseEntry, ServingSnapshot,
};
