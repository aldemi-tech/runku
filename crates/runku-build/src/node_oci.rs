//! Reproducible remote Full Node OCI publisher.

use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Weak},
};

use runku_core::{EnvironmentScope, OperationId, ReleaseId};
use runku_releases::{
    ArtifactStore, FullNodeEgressPolicy, NodeOciDescriptorV1, ReleaseCommand, ReleaseManifestV1,
    ReleaseRepository, RuntimeClass, Sha256Digest, decode_node_esm_bundle, decode_release_manifest,
    encode_hybrid_oci_artifact, encode_node_oci_descriptor, encode_release_manifest,
    hybrid_oci_descriptor,
};
use serde::Deserialize;
use tempfile::TempDir;
use thiserror::Error;
use tokio::{process::Command, sync::Mutex};

use crate::BuildOutput;

const RUNNER: &str = include_str!("../assets/node_runner.mjs");
const BUILD_KEY_DOMAIN: &[u8] = b"RUNKU_NODE_OCI_BUILD_KEY_V1";

/// Stable remote Node publication failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum NodeOciPublishError {
    /// Paths, image references, package files, or policy are invalid.
    #[error("remote Node publish input is invalid")]
    InvalidInput,
    /// The local build is not an internally consistent Full Node package.
    #[error("remote Node build package is invalid")]
    InvalidBuild,
    /// Isolated OCI build or registry push failed.
    #[error("remote Node OCI build failed")]
    BuildFailed,
    /// Artifact or Release persistence is temporarily unavailable.
    #[error("remote Node publication is unavailable")]
    Unavailable,
    /// Immutable state disagrees with the publisher output.
    #[error("remote Node publication conflicted")]
    Conflict,
}

/// Immutable builder, base image, target registry, and npm supply-chain policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeOciPublishConfig {
    /// Docker-compatible buildx executable.
    pub builder_binary: PathBuf,
    /// Immutable Node base image pinned by SHA-256 digest.
    pub base_image: String,
    /// OCI repository without tag or digest.
    pub target_repository: String,
    /// Exact Linux platform (`linux/amd64` or `linux/arm64`).
    pub platform: String,
    /// Whether npm lifecycle scripts may execute inside the isolated build.
    pub allow_install_scripts: bool,
    /// Application-requested egress policy embedded in the final descriptor.
    pub egress_policy: FullNodeEgressPolicy,
}

impl NodeOciPublishConfig {
    /// Validates immutable references and bounded builder options.
    ///
    /// # Errors
    ///
    /// Rejects mutable base images, tagged targets, unsafe executables, and unsupported platforms.
    pub fn validate(&self) -> Result<(), NodeOciPublishError> {
        let base = NodeOciDescriptorV1::new(self.base_image.clone())
            .map_err(|_| NodeOciPublishError::InvalidInput)?;
        let _ = base
            .image_digest()
            .map_err(|_| NodeOciPublishError::InvalidInput)?;
        let last = self
            .target_repository
            .rsplit('/')
            .next()
            .unwrap_or_default();
        if !self.builder_binary.is_absolute()
            || self.target_repository.is_empty()
            || self.target_repository.len() > 400
            || self.target_repository.contains(['@', ' ', '\n', '\r'])
            || last.contains(':')
            || !matches!(self.platform.as_str(), "linux/amd64" | "linux/arm64")
        {
            return Err(NodeOciPublishError::InvalidInput);
        }
        Ok(())
    }
}

/// Successful artifact-first OCI publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeOciPublishResult {
    /// Exact Release identity preserved from the local build.
    pub release_id: ReleaseId,
    /// Registry/repository reference pinned to the pushed manifest digest.
    pub image_reference: String,
    /// Content identity used to reuse an existing immutable OCI image.
    pub build_key: Sha256Digest,
    /// Whether the publisher reused an image already indexed by `build_key`.
    pub image_reused: bool,
    /// Canonical remote Release Manifest bytes registered in the repository.
    pub manifest_bytes: Vec<u8>,
    /// Canonical remote artifact bytes stored in Artifact Store.
    ///
    /// Homogeneous Node Releases contain the OCI descriptor directly; mixed Releases contain the
    /// authenticated resources-and-descriptor envelope.
    pub artifact_bytes: Vec<u8>,
    /// Whether repository registration replayed an earlier identical operation.
    pub replayed: bool,
}

/// Builder/publisher composition using an OCI registry plus existing Runku persistence boundaries.
pub struct RemoteNodePublisher {
    config: NodeOciPublishConfig,
    artifacts: Arc<dyn ArtifactStore>,
    releases: Arc<dyn ReleaseRepository>,
    build_locks: Mutex<HashMap<Sha256Digest, Weak<Mutex<()>>>>,
}

impl std::fmt::Debug for RemoteNodePublisher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteNodePublisher")
            .field("config", &self.config)
            .field("release_backend", &self.releases.backend())
            .finish_non_exhaustive()
    }
}

impl RemoteNodePublisher {
    /// Creates an artifact-first remote Node publisher.
    ///
    /// # Errors
    ///
    /// Rejects invalid builder or immutable image configuration.
    pub fn new(
        config: NodeOciPublishConfig,
        artifacts: Arc<dyn ArtifactStore>,
        releases: Arc<dyn ReleaseRepository>,
    ) -> Result<Self, NodeOciPublishError> {
        config.validate()?;
        Ok(Self {
            config,
            artifacts,
            releases,
            build_locks: Mutex::new(HashMap::new()),
        })
    }

    /// Builds and pushes one Full Node image, persists its descriptor, then registers its manifest.
    ///
    /// `package.json` and `package-lock.json` must be regular files from the canonical Project.
    /// The lockfile is mandatory and `npm ci --omit=dev` is always used.
    ///
    /// # Errors
    ///
    /// Rejects package/build drift, registry failures, or persistence failures.
    pub async fn publish(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        build: &BuildOutput,
        package_json: &Path,
        package_lock: &Path,
    ) -> Result<NodeOciPublishResult, NodeOciPublishError> {
        let manifest_bytes = read_regular(&build.manifest_path).await?;
        let local_artifact = read_regular(&build.artifact_path).await?;
        let package_json_bytes = read_regular(package_json).await?;
        let package_lock_bytes = read_regular(package_lock).await?;
        let mut manifest = decode_release_manifest(&manifest_bytes)
            .map_err(|_| NodeOciPublishError::InvalidBuild)?;
        let bundle = decode_node_esm_bundle(&local_artifact)
            .map_err(|_| NodeOciPublishError::InvalidBuild)?;
        bundle
            .verify_manifest(&manifest, &local_artifact)
            .map_err(|_| NodeOciPublishError::InvalidBuild)?;
        if manifest.project_id != scope.project_id()
            || manifest.release_id != build.release_id
            || Sha256Digest::of(&manifest_bytes) != build.manifest_digest
        {
            return Err(NodeOciPublishError::InvalidBuild);
        }
        validate_package_lock(&package_json_bytes, &package_lock_bytes)?;
        let build_key = node_oci_build_key(
            &self.config,
            &local_artifact,
            &package_json_bytes,
            &package_lock_bytes,
        );
        let context = tempfile::tempdir().map_err(|_| NodeOciPublishError::Unavailable)?;
        materialize_context(
            &context,
            &self.config,
            build_key,
            &manifest,
            &bundle,
            &package_json_bytes,
            &package_lock_bytes,
        )
        .await?;
        let build_lock = self.build_lock(build_key).await;
        let build_guard = build_lock.lock().await;
        let (digest, image_reused) = self.build_and_push(&context, build_key).await?;
        drop(build_guard);
        let image_reference = format!("{}@sha256:{digest}", self.config.target_repository);
        let descriptor = NodeOciDescriptorV1::new(image_reference.clone())
            .map_err(|_| NodeOciPublishError::BuildFailed)?
            .with_egress_policy(self.config.egress_policy.clone());
        let descriptor_bytes = encode_node_oci_descriptor(&descriptor)
            .map_err(|_| NodeOciPublishError::InvalidBuild)?;
        let remote_artifact = if manifest.runtime_version.as_str() == "runku-hybrid-1" {
            let artifact = encode_hybrid_oci_artifact(&local_artifact, &descriptor_bytes)
                .map_err(|_| NodeOciPublishError::InvalidBuild)?;
            manifest.artifact =
                hybrid_oci_descriptor(&artifact).map_err(|_| NodeOciPublishError::InvalidBuild)?;
            artifact
        } else {
            manifest.artifact = descriptor
                .descriptor()
                .map_err(|_| NodeOciPublishError::InvalidBuild)?;
            descriptor_bytes
        };
        manifest
            .ensure_full_node_v1_supported()
            .map_err(|_| NodeOciPublishError::InvalidBuild)?;
        let remote_manifest_bytes =
            encode_release_manifest(&manifest).map_err(|_| NodeOciPublishError::InvalidBuild)?;
        self.artifacts
            .put(&manifest.artifact, &remote_artifact)
            .await
            .map_err(map_release)?;
        let registered = self
            .releases
            .apply(
                scope,
                operation_id,
                &ReleaseCommand::Register {
                    manifest_bytes: remote_manifest_bytes.clone(),
                },
            )
            .await
            .map_err(map_release)?;
        Ok(NodeOciPublishResult {
            release_id: manifest.release_id,
            image_reference,
            build_key,
            image_reused,
            manifest_bytes: remote_manifest_bytes,
            artifact_bytes: remote_artifact,
            replayed: registered.replayed,
        })
    }

    async fn build_lock(&self, build_key: Sha256Digest) -> Arc<Mutex<()>> {
        let mut locks = self.build_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&build_key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(build_key, Arc::downgrade(&lock));
        lock
    }

    async fn build_and_push(
        &self,
        context: &TempDir,
        build_key: Sha256Digest,
    ) -> Result<(String, bool), NodeOciPublishError> {
        let tag = format!("{}:runku-build-{build_key}", self.config.target_repository);
        if let Some(digest) = self.resolve_existing_digest(&tag).await? {
            return Ok((digest, true));
        }
        let metadata = context.path().join("metadata.json");
        let status = Command::new(&self.config.builder_binary)
            .arg("buildx")
            .arg("build")
            .arg("--pull")
            .arg("--provenance=true")
            .arg("--sbom=true")
            .arg("--platform")
            .arg(&self.config.platform)
            .arg("--tag")
            .arg(tag)
            .arg("--push")
            .arg("--metadata-file")
            .arg(&metadata)
            .arg(context.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map_err(|_| NodeOciPublishError::BuildFailed)?;
        if !status.success() {
            return Err(NodeOciPublishError::BuildFailed);
        }
        let bytes = read_regular(&metadata).await?;
        let metadata: BuildMetadataWire =
            serde_json::from_slice(&bytes).map_err(|_| NodeOciPublishError::BuildFailed)?;
        let digest = metadata
            .container_image_digest
            .strip_prefix("sha256:")
            .ok_or(NodeOciPublishError::BuildFailed)?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(NodeOciPublishError::BuildFailed);
        }
        Ok((digest.to_owned(), false))
    }

    async fn resolve_existing_digest(
        &self,
        tag: &str,
    ) -> Result<Option<String>, NodeOciPublishError> {
        let output = Command::new(&self.config.builder_binary)
            .arg("buildx")
            .arg("imagetools")
            .arg("inspect")
            .arg(tag)
            .arg("--format")
            .arg("{{.Manifest.Digest}}")
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await
            .map_err(|_| NodeOciPublishError::BuildFailed)?;
        if !output.status.success() {
            return Ok(None);
        }
        let value = std::str::from_utf8(&output.stdout)
            .map_err(|_| NodeOciPublishError::BuildFailed)?
            .trim()
            .strip_prefix("sha256:")
            .ok_or(NodeOciPublishError::BuildFailed)?;
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(NodeOciPublishError::BuildFailed);
        }
        Ok(Some(value.to_owned()))
    }
}

fn node_oci_build_key(
    config: &NodeOciPublishConfig,
    local_artifact: &[u8],
    package_json: &[u8],
    package_lock: &[u8],
) -> Sha256Digest {
    let mut canonical = Vec::with_capacity(
        BUILD_KEY_DOMAIN.len()
            + local_artifact.len()
            + package_json.len()
            + package_lock.len()
            + RUNNER.len()
            + config.base_image.len()
            + config.platform.len()
            + 64,
    );
    for field in [
        BUILD_KEY_DOMAIN,
        config.base_image.as_bytes(),
        config.platform.as_bytes(),
        &[u8::from(config.allow_install_scripts)],
        RUNNER.as_bytes(),
        package_json,
        package_lock,
        local_artifact,
    ] {
        canonical.extend_from_slice(&u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        canonical.extend_from_slice(field);
    }
    Sha256Digest::of(&canonical)
}

#[derive(Deserialize)]
struct BuildMetadataWire {
    #[serde(rename = "containerimage.digest")]
    container_image_digest: String,
}

async fn materialize_context(
    context: &TempDir,
    config: &NodeOciPublishConfig,
    build_key: Sha256Digest,
    manifest: &ReleaseManifestV1,
    bundle: &runku_releases::NodeEsmBundleV1,
    package_json: &[u8],
    package_lock: &[u8],
) -> Result<(), NodeOciPublishError> {
    let functions = context.path().join("functions");
    let contracts = context.path().join("contracts");
    tokio::fs::create_dir(&functions)
        .await
        .map_err(|_| NodeOciPublishError::Unavailable)?;
    tokio::fs::create_dir(&contracts)
        .await
        .map_err(|_| NodeOciPublishError::Unavailable)?;
    let mut implementation_hashes = BTreeSet::new();
    let mut contract_hashes = BTreeSet::new();
    for function in manifest
        .functions
        .iter()
        .filter(|function| function.runtime_class == RuntimeClass::FullNode)
    {
        if implementation_hashes.insert(function.implementation_hash) {
            let source = bundle
                .source(function.implementation_hash)
                .ok_or(NodeOciPublishError::InvalidBuild)?;
            write_new(
                &functions.join(format!("{}.mjs", function.implementation_hash)),
                source.as_bytes(),
            )
            .await?;
        }
        contract_hashes.insert(function.arguments_contract_hash);
        contract_hashes.insert(function.result_contract_hash);
    }
    for digest in contract_hashes {
        let contract = bundle
            .resource(digest)
            .ok_or(NodeOciPublishError::InvalidBuild)?;
        write_new(
            &contracts.join(format!("{digest}.json")),
            contract.as_bytes(),
        )
        .await?;
    }
    write_new(&context.path().join("runner.mjs"), RUNNER.as_bytes()).await?;
    write_new(&context.path().join("package.json"), package_json).await?;
    write_new(&context.path().join("package-lock.json"), package_lock).await?;
    let scripts = if config.allow_install_scripts {
        ""
    } else {
        " --ignore-scripts"
    };
    let dockerfile = format!(
        "FROM {}\nLABEL io.runku.build-key=\"{}\"\nWORKDIR /opt/runku\nCOPY package.json package-lock.json ./\nRUN npm ci --omit=dev{} && npm cache clean --force\nCOPY --chown=65532:65532 runner.mjs ./runner.mjs\nCOPY --chown=65532:65532 functions ./functions\nCOPY --chown=65532:65532 contracts ./contracts\nUSER 65532:65532\nENTRYPOINT [\"node\",\"/opt/runku/runner.mjs\"]\n",
        config.base_image, build_key, scripts
    );
    write_new(&context.path().join("Dockerfile"), dockerfile.as_bytes()).await
}

async fn write_new(path: &Path, bytes: &[u8]) -> Result<(), NodeOciPublishError> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options
        .open(path)
        .await
        .map_err(|_| NodeOciPublishError::Unavailable)?;
    tokio::io::AsyncWriteExt::write_all(&mut file, bytes)
        .await
        .map_err(|_| NodeOciPublishError::Unavailable)?;
    tokio::io::AsyncWriteExt::shutdown(&mut file)
        .await
        .map_err(|_| NodeOciPublishError::Unavailable)
}

async fn read_regular(path: &Path) -> Result<Vec<u8>, NodeOciPublishError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| NodeOciPublishError::InvalidInput)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 64 * 1024 * 1024
    {
        return Err(NodeOciPublishError::InvalidInput);
    }
    tokio::fs::read(path)
        .await
        .map_err(|_| NodeOciPublishError::Unavailable)
}

fn validate_package_lock(
    package_json: &[u8],
    package_lock: &[u8],
) -> Result<(), NodeOciPublishError> {
    let package: serde_json::Value =
        serde_json::from_slice(package_json).map_err(|_| NodeOciPublishError::InvalidInput)?;
    let lock: serde_json::Value =
        serde_json::from_slice(package_lock).map_err(|_| NodeOciPublishError::InvalidInput)?;
    if !package.is_object()
        || lock
            .get("lockfileVersion")
            .and_then(serde_json::Value::as_u64)
            .is_none()
        || lock
            .get("packages")
            .and_then(serde_json::Value::as_object)
            .is_none()
    {
        return Err(NodeOciPublishError::InvalidInput);
    }
    Ok(())
}

const fn map_release(error: runku_releases::ReleaseError) -> NodeOciPublishError {
    if error.retryable() {
        NodeOciPublishError::Unavailable
    } else if matches!(
        error,
        runku_releases::ReleaseError::RepositoryConflict
            | runku_releases::ReleaseError::OperationIdReused
    ) {
        NodeOciPublishError::Conflict
    } else {
        NodeOciPublishError::InvalidBuild
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{os::unix::fs::PermissionsExt, process::Stdio, sync::Mutex};

    use async_trait::async_trait;
    use runku_core::{BuildId, EnvironmentId, ProjectId};
    use runku_releases::{
        ArtifactFormat, FilesystemArtifactStore, FilesystemStoreRole, ReleaseCommandResult,
        ReleaseError, ReleaseRepositoryBackend, ReleaseRepositoryTelemetrySnapshot,
        ServingSnapshot, decode_hybrid_oci_artifact, decode_node_esm_bundle,
        decode_node_oci_descriptor,
    };
    use runku_value::TimestampMicros;

    use super::*;
    use crate::{BuildMetadata, build_project};

    #[test]
    fn build_key_tracks_content_and_supply_chain_policy() {
        let mut config = NodeOciPublishConfig {
            builder_binary: PathBuf::from("/usr/bin/docker"),
            base_image: format!("docker.io/library/node@sha256:{}", "b".repeat(64)),
            target_repository: "registry.example.com/runku/functions".to_owned(),
            platform: "linux/amd64".to_owned(),
            allow_install_scripts: false,
            egress_policy: FullNodeEgressPolicy::none(),
        };
        let key = node_oci_build_key(&config, b"bundle", b"package", b"lock");
        assert_ne!(
            key,
            node_oci_build_key(&config, b"bundle-2", b"package", b"lock")
        );
        config.allow_install_scripts = true;
        assert_ne!(
            key,
            node_oci_build_key(&config, b"bundle", b"package", b"lock")
        );
    }

    #[derive(Debug, Default)]
    struct RecordingReleases {
        manifest: Mutex<Option<ReleaseManifestV1>>,
    }

    #[async_trait]
    impl ReleaseRepository for RecordingReleases {
        fn backend(&self) -> ReleaseRepositoryBackend {
            ReleaseRepositoryBackend::SQLite
        }

        async fn apply(
            &self,
            _scope: EnvironmentScope,
            _operation_id: OperationId,
            command: &ReleaseCommand,
        ) -> Result<ReleaseCommandResult, ReleaseError> {
            let ReleaseCommand::Register { manifest_bytes } = command else {
                return Err(ReleaseError::Unsupported);
            };
            let manifest = decode_release_manifest(manifest_bytes)?;
            *self.manifest.lock().map_err(|_| ReleaseError::Internal)? = Some(manifest);
            Ok(ReleaseCommandResult {
                serving_revision: 0,
                replayed: false,
            })
        }

        async fn snapshot(
            &self,
            _scope: EnvironmentScope,
        ) -> Result<ServingSnapshot, ReleaseError> {
            Err(ReleaseError::Unsupported)
        }

        async fn manifest(
            &self,
            _scope: EnvironmentScope,
            release_id: ReleaseId,
        ) -> Result<ReleaseManifestV1, ReleaseError> {
            self.manifest
                .lock()
                .map_err(|_| ReleaseError::Internal)?
                .clone()
                .filter(|manifest| manifest.release_id == release_id)
                .ok_or(ReleaseError::ReleaseNotFound)
        }

        async fn health(&self) -> Result<(), ReleaseError> {
            Ok(())
        }

        fn telemetry(&self) -> ReleaseRepositoryTelemetrySnapshot {
            ReleaseRepositoryTelemetrySnapshot::default()
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn publisher_builds_digest_descriptor_then_registers_manifest_artifact_first()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("runku");
        std::fs::create_dir(&source)?;
        std::fs::write(
            source.join("schema.ts"),
            "import { defineSchema } from '@runku/server'; export default defineSchema({});",
        )?;
        std::fs::write(
            source.join("actions.ts"),
            r#""use runku node"
import { action, v } from "@runku/server"
export const echo = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), handler(_ctx, value) { return value } })
export const create = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), handler(_ctx, value) { return `created:${value}` } })
export const deleteItem = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), handler(_ctx, value) { return `deleted:${value}` } })"#,
        )?;
        std::fs::write(
            source.join("safe.ts"),
            r#"import { action, v } from "@runku/server"
export const echo = action({ auth: "none", visibility: "internal", capabilities: [], args: v.string(), returns: v.string(), handler(_ctx, value) { return `safe:${value}` } })"#,
        )?;
        let project_id = ProjectId::generate();
        let release_id = ReleaseId::generate();
        let build = build_project(
            directory.path(),
            Path::new("runku"),
            project_id,
            BuildMetadata {
                release_id,
                build_id: BuildId::generate(),
                created_at: TimestampMicros::new(1_800_000_000_000_000),
            },
        )?;
        let package_json = directory.path().join("package.json");
        let package_lock = directory.path().join("package-lock.json");
        std::fs::write(
            &package_json,
            br#"{"name":"fixture","version":"1.0.0","private":true}"#,
        )?;
        std::fs::write(
            &package_lock,
            br#"{"name":"fixture","version":"1.0.0","lockfileVersion":3,"packages":{"":{"name":"fixture","version":"1.0.0"}}}"#,
        )?;
        let builder = directory.path().join("builder");
        std::fs::write(
            &builder,
            r#"#!/bin/sh
set -eu
if test "${1:-}" = "buildx" && test "${2:-}" = "imagetools"; then
  if test -f "$0.cached"; then
    printf '%s\n' 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    exit 0
  fi
  exit 1
fi
while test "$#" -gt 0; do
  if test "$1" = "--metadata-file"; then
    shift
    printf '%s' '{"containerimage.digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}' > "$1"
    : > "$0.cached"
    exit 0
  fi
  shift
done
exit 1
"#,
        )?;
        let mut permissions = std::fs::metadata(&builder)?.permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&builder, permissions)?;
        let artifact_root = directory.path().join("artifacts");
        let artifacts = Arc::new(
            FilesystemArtifactStore::open(&artifact_root, FilesystemStoreRole::Test).await?,
        );
        let releases = Arc::new(RecordingReleases::default());
        let publisher = RemoteNodePublisher::new(
            NodeOciPublishConfig {
                builder_binary: builder,
                base_image: format!("docker.io/library/node@sha256:{}", "b".repeat(64)),
                target_repository: "registry.example.com/runku/functions".to_owned(),
                platform: "linux/amd64".to_owned(),
                allow_install_scripts: false,
                egress_policy: FullNodeEgressPolicy::none(),
            },
            artifacts.clone(),
            releases.clone(),
        )?;
        let scope = EnvironmentScope::new(project_id, EnvironmentId::generate());
        let outcome = publisher
            .publish(
                scope,
                OperationId::generate(),
                &build,
                &package_json,
                &package_lock,
            )
            .await?;
        assert_eq!(outcome.release_id, release_id);
        assert!(!outcome.image_reused);
        assert_eq!(
            outcome.image_reference,
            format!(
                "registry.example.com/runku/functions@sha256:{}",
                "a".repeat(64)
            )
        );
        let manifest = decode_release_manifest(&outcome.manifest_bytes)?;
        assert_eq!(manifest.runtime_version.as_str(), "runku-hybrid-1");
        assert_eq!(
            manifest.artifact.format,
            ArtifactFormat::HybridOciArtifactV1
        );
        let stored = artifacts.get(&manifest.artifact).await?;
        assert_eq!(stored, outcome.artifact_bytes);
        let (resources, descriptor_bytes) = decode_hybrid_oci_artifact(&stored)?;
        let resources = decode_node_esm_bundle(resources)?;
        assert!(
            manifest
                .functions
                .iter()
                .all(|function| resources.source(function.implementation_hash).is_some())
        );
        let descriptor = decode_node_oci_descriptor(descriptor_bytes)?;
        assert_eq!(descriptor.image_reference(), outcome.image_reference);
        assert_eq!(releases.manifest(scope, release_id).await?, manifest);
        let replay = publisher
            .publish(
                scope,
                OperationId::generate(),
                &build,
                &package_json,
                &package_lock,
            )
            .await?;
        assert!(replay.image_reused);
        assert_eq!(replay.build_key, outcome.build_key);
        assert_eq!(replay.image_reference, outcome.image_reference);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn publisher_builds_pushes_and_executes_real_oci_image()
    -> Result<(), Box<dyn std::error::Error>> {
        let (Some(builder), Some(base_image), Some(target_repository)) = (
            std::env::var_os("RUNKU_TEST_OCI_BUILDER"),
            std::env::var_os("RUNKU_TEST_NODE_BASE_IMAGE"),
            std::env::var_os("RUNKU_TEST_OCI_REPOSITORY"),
        ) else {
            eprintln!("skipping real OCI publisher conformance: environment is unset");
            return Ok(());
        };
        let builder = PathBuf::from(builder);
        let base_image = base_image.into_string().map_err(|_| "invalid base image")?;
        let target_repository = target_repository
            .into_string()
            .map_err(|_| "invalid target repository")?;
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("runku");
        std::fs::create_dir(&source)?;
        std::fs::write(
            source.join("schema.ts"),
            "import { defineSchema } from '@runku/server'; export default defineSchema({});",
        )?;
        std::fs::write(
            source.join("actions.ts"),
            r#""use runku node"
import { action, v } from "@runku/server"
export const echo = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), handler(_ctx, value) { return value } })
export const create = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), async handler(ctx, value) { await new Promise(resolve => setTimeout(resolve, 10 + value.charCodeAt(value.length - 1) % 7)); return `created:${value}:${ctx.invocation.invocationId}:${ctx.invocation.function}` } })
export const deleteItem = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), async handler(ctx, value) { await new Promise(resolve => setTimeout(resolve, 10 + value.charCodeAt(value.length - 1) % 5)); return `deleted:${value}:${ctx.invocation.invocationId}:${ctx.invocation.function}` } })
export const inspect = action({ auth: "none", visibility: "public", capabilities: [], args: v.string(), returns: v.string(), async handler(ctx, value) { await new Promise(resolve => setTimeout(resolve, 10 + value.charCodeAt(value.length - 1) % 3)); return `inspected:${value}:${ctx.invocation.invocationId}:${ctx.invocation.function}` } })"#,
        )?;
        let project_id = ProjectId::generate();
        let release_id = ReleaseId::generate();
        let build = build_project(
            directory.path(),
            Path::new("runku"),
            project_id,
            BuildMetadata {
                release_id,
                build_id: BuildId::generate(),
                created_at: TimestampMicros::new(1_800_000_000_000_000),
            },
        )?;
        let package_json = directory.path().join("package.json");
        let package_lock = directory.path().join("package-lock.json");
        std::fs::write(
            &package_json,
            br#"{"name":"runku-publisher-conformance","version":"1.0.0","private":true}"#,
        )?;
        std::fs::write(
            &package_lock,
            br#"{"name":"runku-publisher-conformance","version":"1.0.0","lockfileVersion":3,"packages":{"":{"name":"runku-publisher-conformance","version":"1.0.0"}}}"#,
        )?;
        let artifacts = Arc::new(
            FilesystemArtifactStore::open(
                directory.path().join("artifacts"),
                FilesystemStoreRole::Test,
            )
            .await?,
        );
        let releases = Arc::new(RecordingReleases::default());
        let publisher = RemoteNodePublisher::new(
            NodeOciPublishConfig {
                builder_binary: builder.clone(),
                base_image,
                target_repository,
                platform: if cfg!(target_arch = "aarch64") {
                    "linux/arm64".to_owned()
                } else {
                    "linux/amd64".to_owned()
                },
                allow_install_scripts: false,
                egress_policy: FullNodeEgressPolicy::none(),
            },
            artifacts,
            releases,
        )?;
        let outcome = publisher
            .publish(
                EnvironmentScope::new(project_id, EnvironmentId::generate()),
                OperationId::generate(),
                &build,
                &package_json,
                &package_lock,
            )
            .await?;
        let manifest = decode_release_manifest(&outcome.manifest_bytes)?;
        let function = manifest
            .functions
            .iter()
            .find(|function| function.name.as_str() == "actions.echo")
            .ok_or("echo function missing")?;
        let request = serde_json::to_vec(&serde_json::json!({
            "protocolVersion": 1,
            "releaseId": release_id.to_string(),
            "invocationId": runku_core::InvocationId::generate().to_string(),
            "function": function.name.as_str(),
            "implementationHash": function.implementation_hash.to_string(),
            "argumentsContractHash": function.arguments_contract_hash.to_string(),
            "resultContractHash": function.result_contract_hash.to_string(),
            "arguments": { "type": "string", "value": "publisher-ok" }
        }))?;
        let mut child = tokio::process::Command::new(&builder)
            .arg("run")
            .arg("--rm")
            .arg("--interactive")
            .arg("--network")
            .arg("none")
            .arg("--read-only")
            .arg("--user")
            .arg("65532:65532")
            .arg(&outcome.image_reference)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let mut stdin = child.stdin.take().ok_or("publisher runner stdin missing")?;
        tokio::io::AsyncWriteExt::write_all(&mut stdin, &request).await?;
        tokio::io::AsyncWriteExt::shutdown(&mut stdin).await?;
        drop(stdin);
        let output = child.wait_with_output().await?;
        let _ = tokio::process::Command::new(&builder)
            .arg("image")
            .arg("rm")
            .arg(&outcome.image_reference)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
        assert!(output.status.success());
        let response: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(response.get("ok"), Some(&serde_json::Value::Bool(true)));
        assert_eq!(
            response
                .pointer("/value/type")
                .and_then(|value| value.as_str()),
            Some("string")
        );
        assert_eq!(
            response
                .pointer("/value/value")
                .and_then(|value| value.as_str()),
            Some("publisher-ok")
        );
        println!("RUNKU_PUBLISHED_IMAGE {}", outcome.image_reference);
        Ok(())
    }

    #[test]
    fn publisher_rejects_mutable_images_and_invalid_lockfiles() {
        let invalid = NodeOciPublishConfig {
            builder_binary: PathBuf::from("/usr/bin/docker"),
            base_image: "node:22".to_owned(),
            target_repository: "registry.example.com/runku/functions".to_owned(),
            platform: "linux/amd64".to_owned(),
            allow_install_scripts: false,
            egress_policy: FullNodeEgressPolicy::none(),
        };
        assert_eq!(invalid.validate(), Err(NodeOciPublishError::InvalidInput));
        assert_eq!(
            validate_package_lock(br"{}", br#"{"lockfileVersion":3}"#),
            Err(NodeOciPublishError::InvalidInput)
        );
    }
}
