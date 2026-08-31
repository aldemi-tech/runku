//! Content-addressed artifact store boundary and local filesystem adapter.

use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use runku_core::OperationId;
use tokio::io::AsyncWriteExt;

use crate::{ARTIFACT_MAX_BYTES, ArtifactDescriptor, ReleaseError, Sha256Digest};

/// Immutable artifact persistence boundary consumed by build and runtime loader layers.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Stores exact bytes under their expected digest, idempotently.
    async fn put(&self, descriptor: &ArtifactDescriptor, bytes: &[u8]) -> Result<(), ReleaseError>;

    /// Loads exact bytes and revalidates size and digest.
    async fn get(&self, descriptor: &ArtifactDescriptor) -> Result<Vec<u8>, ReleaseError>;
}

/// Deployment role requested from the filesystem adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesystemStoreRole {
    /// Persistent local developer store.
    LocalDevelopment,
    /// Ephemeral test store.
    Test,
    /// Persistent artifact store for one self-hosted server instance.
    ///
    /// This role is not safe when multiple server nodes need to observe the same artifacts.
    SingleNodeServer,
    /// Forbidden: distributed production requires an object-store adapter.
    Production,
}

/// Local-only content-addressed artifact storage with atomic no-clobber writes.
#[derive(Clone, Debug)]
pub struct FilesystemArtifactStore {
    root: PathBuf,
    temporary_root: PathBuf,
    role: FilesystemStoreRole,
}

impl FilesystemArtifactStore {
    /// Creates/opens a local artifact root and resolves it to a canonical path.
    ///
    /// # Errors
    ///
    /// Rejects Production before creating directories and returns a sanitized backend error for
    /// invalid or unavailable roots.
    pub async fn open(
        root: impl AsRef<Path>,
        role: FilesystemStoreRole,
    ) -> Result<Self, ReleaseError> {
        if role == FilesystemStoreRole::Production {
            return Err(ReleaseError::ProductionBackendUnsupported);
        }
        tokio::fs::create_dir_all(root.as_ref())
            .await
            .map_err(map_io_error)?;
        let root = tokio::fs::canonicalize(root.as_ref())
            .await
            .map_err(map_io_error)?;
        let metadata = tokio::fs::metadata(&root).await.map_err(map_io_error)?;
        if !metadata.is_dir() {
            return Err(ReleaseError::Unavailable);
        }
        let temporary_root = root.join(".tmp");
        ensure_private_directory(&temporary_root).await?;
        cleanup_temporary_files(&temporary_root).await?;
        Ok(Self {
            root,
            temporary_root,
            role,
        })
    }

    /// Returns the accepted local/test role.
    #[must_use]
    pub const fn role(&self) -> FilesystemStoreRole {
        self.role
    }

    fn path_for(&self, digest: Sha256Digest) -> (PathBuf, PathBuf) {
        let digest = digest.to_string();
        let directory = self.root.join(&digest[..2]).join(&digest[2..4]);
        let target = directory.join(format!("{}.artifact", &digest[4..]));
        (directory, target)
    }

    async fn verify_existing(
        &self,
        target: &Path,
        descriptor: &ArtifactDescriptor,
    ) -> Result<(), ReleaseError> {
        let bytes = read_verified(target, descriptor).await?;
        if bytes.is_empty() {
            return Err(ReleaseError::Corruption);
        }
        Ok(())
    }
}

#[async_trait]
impl ArtifactStore for FilesystemArtifactStore {
    async fn put(&self, descriptor: &ArtifactDescriptor, bytes: &[u8]) -> Result<(), ReleaseError> {
        validate_descriptor(descriptor)?;
        validate_artifact_bytes(bytes)?;
        if usize::try_from(descriptor.size_bytes).map_err(|_| ReleaseError::LimitExceeded)?
            != bytes.len()
        {
            return Err(ReleaseError::DescriptorMismatch);
        }
        if Sha256Digest::of(bytes) != descriptor.digest {
            return Err(ReleaseError::DigestMismatch);
        }
        let (directory, target) = self.path_for(descriptor.digest);
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(map_io_error)?;
        match tokio::fs::symlink_metadata(&target).await {
            Ok(_) => return self.verify_existing(&target, descriptor).await,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(map_io_error(error)),
        }

        let temporary = self
            .temporary_root
            .join(format!("{}.tmp", OperationId::generate()));
        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await
        {
            Ok(file) => file,
            Err(error) => return Err(map_io_error(error)),
        };
        let write_result = async {
            file.write_all(bytes).await.map_err(map_io_error)?;
            file.sync_all().await.map_err(map_io_error)?;
            drop(file);
            match tokio::fs::hard_link(&temporary, &target).await {
                Ok(()) => {
                    tokio::fs::remove_file(&temporary)
                        .await
                        .map_err(map_io_error)?;
                    sync_directory(directory.clone()).await?;
                    sync_directory(self.temporary_root.clone()).await?;
                    Ok(())
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    tokio::fs::remove_file(&temporary)
                        .await
                        .map_err(map_io_error)?;
                    self.verify_existing(&target, descriptor).await
                }
                Err(error) => Err(map_io_error(error)),
            }
        }
        .await;
        if write_result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        write_result
    }

    async fn get(&self, descriptor: &ArtifactDescriptor) -> Result<Vec<u8>, ReleaseError> {
        validate_descriptor(descriptor)?;
        let (_, target) = self.path_for(descriptor.digest);
        read_verified(&target, descriptor).await
    }
}

async fn read_verified(
    path: &Path,
    descriptor: &ArtifactDescriptor,
) -> Result<Vec<u8>, ReleaseError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(map_read_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ReleaseError::Corruption);
    }
    let maximum = u64::try_from(ARTIFACT_MAX_BYTES).map_err(|_| ReleaseError::Internal)?;
    if metadata.len() == 0 || metadata.len() > maximum {
        return Err(ReleaseError::Corruption);
    }
    if metadata.len() != descriptor.size_bytes {
        return Err(ReleaseError::DescriptorMismatch);
    }
    let bytes = tokio::fs::read(path).await.map_err(map_read_error)?;
    validate_artifact_bytes(&bytes).map_err(|_| ReleaseError::Corruption)?;
    if Sha256Digest::of(&bytes) != descriptor.digest {
        return Err(ReleaseError::Corruption);
    }
    Ok(bytes)
}

fn validate_descriptor(descriptor: &ArtifactDescriptor) -> Result<(), ReleaseError> {
    let maximum = u64::try_from(ARTIFACT_MAX_BYTES).map_err(|_| ReleaseError::Internal)?;
    if descriptor.size_bytes == 0 || descriptor.size_bytes > maximum {
        return Err(ReleaseError::LimitExceeded);
    }
    Ok(())
}

fn validate_artifact_bytes(bytes: &[u8]) -> Result<(), ReleaseError> {
    if bytes.is_empty() || bytes.len() > ARTIFACT_MAX_BYTES {
        return Err(ReleaseError::LimitExceeded);
    }
    Ok(())
}

async fn sync_directory(directory: PathBuf) -> Result<(), ReleaseError> {
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(directory)
            .and_then(|file| file.sync_all())
            .map_err(map_io_error)
    })
    .await
    .map_err(|_| ReleaseError::Internal)?
}

async fn ensure_private_directory(path: &Path) -> Result<(), ReleaseError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ReleaseError::Corruption);
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            tokio::fs::create_dir(path).await.map_err(map_io_error)?;
        }
        Err(error) => return Err(map_io_error(error)),
    }
    Ok(())
}

async fn cleanup_temporary_files(path: &Path) -> Result<(), ReleaseError> {
    const MAX_STALE_FILES: usize = 10_000;
    let mut entries = tokio::fs::read_dir(path).await.map_err(map_io_error)?;
    let mut count = 0_usize;
    while let Some(entry) = entries.next_entry().await.map_err(map_io_error)? {
        count = count.checked_add(1).ok_or(ReleaseError::LimitExceeded)?;
        if count > MAX_STALE_FILES {
            return Err(ReleaseError::Busy);
        }
        let metadata = entry.file_type().await.map_err(map_io_error)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !metadata.is_symlink()
            && metadata.is_file()
            && name.starts_with("opn_")
            && name.ends_with(".tmp")
        {
            tokio::fs::remove_file(entry.path())
                .await
                .map_err(map_io_error)?;
        }
    }
    sync_directory(path.to_path_buf()).await
}

fn map_read_error(error: std::io::Error) -> ReleaseError {
    if error.kind() == ErrorKind::NotFound {
        ReleaseError::NotFound
    } else {
        map_io_error(error)
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_io_error(error: std::io::Error) -> ReleaseError {
    match error.kind() {
        ErrorKind::WouldBlock | ErrorKind::TimedOut => ReleaseError::Busy,
        ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::ReadOnlyFilesystem => {
            ReleaseError::Unavailable
        }
        _ => ReleaseError::Internal,
    }
}
