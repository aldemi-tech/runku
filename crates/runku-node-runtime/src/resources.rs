//! Sanitized immutable resources shared with a separate Full Node worker.

use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId, ReleaseId};
use runku_releases::{
    ARTIFACT_MAX_BYTES, ArtifactDescriptor, ArtifactStore, ReleaseManifestV1, ReleaseRepository,
    Sha256Digest, decode_release_manifest, encode_release_manifest,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

const FORMAT_DIRECTORY: &str = "full-node-execution-v1";

/// One verified immutable package required to prepare a queued Full Node execution.
#[derive(Debug)]
pub struct FullNodeExecutionPackage {
    /// Canonical Release manifest resolved by exact Environment scope and Release ID.
    pub manifest: ReleaseManifestV1,
    /// Exact content-addressed artifact bytes referenced by the manifest.
    pub artifact: Vec<u8>,
}

/// Sanitized failure returned by a Full Node execution resource source.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FullNodeExecutionResourceError {
    /// The exact resource is absent, malformed, mismatched, or was modified.
    #[error("full node execution resource is invalid")]
    Invalid,
    /// The backing resource source is temporarily unavailable.
    #[error("full node execution resource source is unavailable")]
    Unavailable,
}

impl FullNodeExecutionResourceError {
    /// Whether retrying the same read may succeed after external recovery.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

/// Exact immutable package source consumed by a Full Node worker.
#[async_trait]
pub trait FullNodeExecutionResources: Send + Sync {
    /// Loads and verifies one exact scoped Release manifest.
    async fn manifest(
        &self,
        scope: EnvironmentScope,
        release_id: ReleaseId,
    ) -> Result<ReleaseManifestV1, FullNodeExecutionResourceError>;

    /// Loads and verifies one exact content-addressed artifact.
    async fn artifact(
        &self,
        descriptor: &ArtifactDescriptor,
    ) -> Result<Vec<u8>, FullNodeExecutionResourceError>;

    /// Loads one complete package. Runtime handlers may use the separate methods to retain a
    /// bounded verified artifact cache.
    async fn load(
        &self,
        scope: EnvironmentScope,
        release_id: ReleaseId,
    ) -> Result<FullNodeExecutionPackage, FullNodeExecutionResourceError> {
        let manifest = self.manifest(scope, release_id).await?;
        let artifact = self.artifact(&manifest.artifact).await?;
        verify_package(scope, release_id, manifest, artifact)
    }
}

/// Adapter retaining the existing repository and artifact-store composition.
pub(crate) struct RepositoryExecutionResources {
    releases: Arc<dyn ReleaseRepository>,
    artifacts: Arc<dyn ArtifactStore>,
}

impl RepositoryExecutionResources {
    pub(crate) fn new(
        releases: Arc<dyn ReleaseRepository>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            releases,
            artifacts,
        }
    }
}

#[async_trait]
impl FullNodeExecutionResources for RepositoryExecutionResources {
    async fn manifest(
        &self,
        scope: EnvironmentScope,
        release_id: ReleaseId,
    ) -> Result<ReleaseManifestV1, FullNodeExecutionResourceError> {
        self.releases
            .manifest(scope, release_id)
            .await
            .map_err(|error| {
                if error.retryable() {
                    FullNodeExecutionResourceError::Unavailable
                } else {
                    FullNodeExecutionResourceError::Invalid
                }
            })
    }

    async fn artifact(
        &self,
        descriptor: &ArtifactDescriptor,
    ) -> Result<Vec<u8>, FullNodeExecutionResourceError> {
        self.artifacts.get(descriptor).await.map_err(|error| {
            if error.retryable() {
                FullNodeExecutionResourceError::Unavailable
            } else {
                FullNodeExecutionResourceError::Invalid
            }
        })
    }
}

/// Filesystem projection containing only immutable Full Node manifests and artifacts.
///
/// The writer lives in the cell process. A separate worker mounts the projection read-only and
/// therefore does not need access to Product databases, application keys, or identity secrets.
#[derive(Clone, Debug)]
pub struct FullNodeFilesystemResources {
    root: PathBuf,
    manifests: PathBuf,
    artifacts: PathBuf,
    writable: bool,
}

impl FullNodeFilesystemResources {
    /// Opens or creates a writable projection root for the cell process.
    ///
    /// # Errors
    ///
    /// Rejects broad, relative, symlinked, or unavailable paths.
    pub async fn open_writer(
        root: impl AsRef<Path>,
    ) -> Result<Self, FullNodeExecutionResourceError> {
        Self::open(root.as_ref(), true).await
    }

    /// Opens an existing read-only projection for a worker process.
    ///
    /// This constructor performs no writes, so the containing volume can be mounted read-only.
    ///
    /// # Errors
    ///
    /// Rejects broad, relative, symlinked, missing, or unavailable paths.
    pub async fn open_reader(
        root: impl AsRef<Path>,
    ) -> Result<Self, FullNodeExecutionResourceError> {
        Self::open(root.as_ref(), false).await
    }

    async fn open(root: &Path, writable: bool) -> Result<Self, FullNodeExecutionResourceError> {
        if !root.is_absolute() || root == Path::new("/") {
            return Err(FullNodeExecutionResourceError::Invalid);
        }
        if writable {
            tokio::fs::create_dir_all(root).await.map_err(map_io)?;
        }
        reject_symlink_directory(root).await?;
        let root = tokio::fs::canonicalize(root).await.map_err(map_io)?;
        let format_root = root.join(FORMAT_DIRECTORY);
        let manifests = format_root.join("manifests");
        let artifacts = format_root.join("artifacts");
        if writable {
            tokio::fs::create_dir_all(&manifests)
                .await
                .map_err(map_io)?;
            tokio::fs::create_dir_all(&artifacts)
                .await
                .map_err(map_io)?;
        }
        reject_symlink_directory(&format_root).await?;
        reject_symlink_directory(&manifests).await?;
        reject_symlink_directory(&artifacts).await?;
        Ok(Self {
            root,
            manifests,
            artifacts,
            writable,
        })
    }

    /// Returns the canonical mount root; workers should receive this path read-only.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Stages one canonical Full Node package before its Workspace HEAD becomes visible.
    ///
    /// Existing exact bytes are accepted idempotently. Conflicting immutable bytes fail closed.
    ///
    /// # Errors
    ///
    /// Rejects Safe-only, malformed, mismatched, or unavailable packages.
    pub async fn stage(
        &self,
        scope: EnvironmentScope,
        manifest_bytes: &[u8],
        artifact_bytes: &[u8],
    ) -> Result<(), FullNodeExecutionResourceError> {
        if !self.writable {
            return Err(FullNodeExecutionResourceError::Invalid);
        }
        let manifest = decode_release_manifest(manifest_bytes)
            .map_err(|_| FullNodeExecutionResourceError::Invalid)?;
        if encode_release_manifest(&manifest)
            .map_err(|_| FullNodeExecutionResourceError::Invalid)?
            != manifest_bytes
            || manifest.project_id != scope.project_id()
            || !manifest
                .functions
                .iter()
                .any(|function| function.runtime_class == runku_releases::RuntimeClass::FullNode)
            || (manifest.ensure_local_full_node_supported().is_err()
                && manifest.ensure_full_node_supported().is_err())
        {
            return Err(FullNodeExecutionResourceError::Invalid);
        }
        verify_artifact(&manifest, artifact_bytes)?;
        let artifact_path = self.artifact_path(manifest.artifact.digest);
        write_immutable(&self.artifacts, &artifact_path, artifact_bytes).await?;
        let manifest_path = self.manifest_path(scope, manifest.release_id);
        let manifest_parent = manifest_path
            .parent()
            .ok_or(FullNodeExecutionResourceError::Invalid)?;
        tokio::fs::create_dir_all(manifest_parent)
            .await
            .map_err(map_io)?;
        write_immutable(&self.manifests, &manifest_path, manifest_bytes).await
    }

    fn manifest_path(&self, scope: EnvironmentScope, release_id: ReleaseId) -> PathBuf {
        self.manifests
            .join(scope.project_id().to_string())
            .join(scope.environment_id().to_string())
            .join(format!("{release_id}.manifest"))
    }

    fn artifact_path(&self, digest: Sha256Digest) -> PathBuf {
        let digest = digest.to_string();
        self.artifacts
            .join(&digest[..2])
            .join(&digest[2..4])
            .join(format!("{}.artifact", &digest[4..]))
    }
}

#[async_trait]
impl FullNodeExecutionResources for FullNodeFilesystemResources {
    async fn manifest(
        &self,
        scope: EnvironmentScope,
        release_id: ReleaseId,
    ) -> Result<ReleaseManifestV1, FullNodeExecutionResourceError> {
        let manifest_bytes = read_bounded(
            &self.manifest_path(scope, release_id),
            runku_releases::MANIFEST_MAX_BYTES,
        )
        .await?;
        let manifest = decode_release_manifest(&manifest_bytes)
            .map_err(|_| FullNodeExecutionResourceError::Invalid)?;
        if encode_release_manifest(&manifest)
            .map_err(|_| FullNodeExecutionResourceError::Invalid)?
            != manifest_bytes
        {
            return Err(FullNodeExecutionResourceError::Invalid);
        }
        if manifest.project_id != scope.project_id() || manifest.release_id != release_id {
            return Err(FullNodeExecutionResourceError::Invalid);
        }
        Ok(manifest)
    }

    async fn artifact(
        &self,
        descriptor: &ArtifactDescriptor,
    ) -> Result<Vec<u8>, FullNodeExecutionResourceError> {
        let artifact =
            read_bounded(&self.artifact_path(descriptor.digest), ARTIFACT_MAX_BYTES).await?;
        if artifact.len() != usize::try_from(descriptor.size_bytes).unwrap_or(usize::MAX)
            || Sha256Digest::of(&artifact) != descriptor.digest
        {
            return Err(FullNodeExecutionResourceError::Invalid);
        }
        Ok(artifact)
    }
}

fn verify_package(
    scope: EnvironmentScope,
    release_id: ReleaseId,
    manifest: ReleaseManifestV1,
    artifact: Vec<u8>,
) -> Result<FullNodeExecutionPackage, FullNodeExecutionResourceError> {
    if manifest.project_id != scope.project_id() || manifest.release_id != release_id {
        return Err(FullNodeExecutionResourceError::Invalid);
    }
    verify_artifact(&manifest, &artifact)?;
    Ok(FullNodeExecutionPackage { manifest, artifact })
}

fn verify_artifact(
    manifest: &ReleaseManifestV1,
    artifact: &[u8],
) -> Result<(), FullNodeExecutionResourceError> {
    if artifact.is_empty()
        || artifact.len() > ARTIFACT_MAX_BYTES
        || usize::try_from(manifest.artifact.size_bytes).ok() != Some(artifact.len())
        || Sha256Digest::of(artifact) != manifest.artifact.digest
    {
        return Err(FullNodeExecutionResourceError::Invalid);
    }
    Ok(())
}

async fn reject_symlink_directory(path: &Path) -> Result<(), FullNodeExecutionResourceError> {
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(map_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(FullNodeExecutionResourceError::Invalid);
    }
    Ok(())
}

async fn read_bounded(
    path: &Path,
    maximum: usize,
) -> Result<Vec<u8>, FullNodeExecutionResourceError> {
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(map_io)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(FullNodeExecutionResourceError::Invalid);
    }
    tokio::fs::read(path).await.map_err(map_io)
}

async fn write_immutable(
    trusted_root: &Path,
    target: &Path,
    bytes: &[u8],
) -> Result<(), FullNodeExecutionResourceError> {
    if bytes.is_empty() || !target.starts_with(trusted_root) {
        return Err(FullNodeExecutionResourceError::Invalid);
    }
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(map_io)?;
    }
    match read_bounded(target, bytes.len()).await {
        Ok(existing) if existing == bytes => return Ok(()),
        Ok(_) => return Err(FullNodeExecutionResourceError::Invalid),
        Err(FullNodeExecutionResourceError::Invalid) => {
            if tokio::fs::symlink_metadata(target).await.is_ok() {
                return Err(FullNodeExecutionResourceError::Invalid);
            }
        }
        Err(error) => return Err(error),
    }
    let temporary = trusted_root.join(format!(".{}.tmp", OperationId::generate()));
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(map_io)?;
    let result = async {
        file.write_all(bytes).await.map_err(map_io)?;
        file.sync_all().await.map_err(map_io)?;
        drop(file);
        match tokio::fs::hard_link(&temporary, target).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let existing = read_bounded(target, bytes.len()).await?;
                if existing == bytes {
                    Ok(())
                } else {
                    Err(FullNodeExecutionResourceError::Invalid)
                }
            }
            Err(error) => Err(map_io(error)),
        }
    }
    .await;
    let _ = tokio::fs::remove_file(&temporary).await;
    result
}

fn map_io(error: std::io::Error) -> FullNodeExecutionResourceError {
    let kind = error.kind();
    drop(error);
    if kind == ErrorKind::NotFound || kind == ErrorKind::InvalidData {
        FullNodeExecutionResourceError::Invalid
    } else {
        FullNodeExecutionResourceError::Unavailable
    }
}
