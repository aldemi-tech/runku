use std::{
    fs::{File, OpenOptions, TryLockError},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use runku_core::{BuildId, ReleaseId};
use runku_releases::{ReleaseManifestV1, Sha256Digest};
use serde::Serialize;

use crate::BuildError;

const STATE_DIRECTORY: &str = ".runku";
const BUILDS_DIRECTORY: &str = "builds-v1";
const LOCK_FILE: &str = "builds-v1.lock";
const MANIFEST_FILE: &str = "release-manifest-v1.bin";
const ARTIFACT_FILE: &str = "runtime-artifact-v1.bin";
const RESULT_FILE: &str = "build-result-v1.json";
const GENERATED_TYPES_FILE: &str = "runku.generated.d.ts";
const GENERATED_DIRECTORY: &str = "_generated";
const STABLE_GENERATED_TYPES_FILE: &str = "api.d.ts";
const LOCK_DEADLINE: Duration = Duration::from_secs(5);
const LOCK_RETRY: Duration = Duration::from_millis(20);

/// Immutable on-disk result of one source build.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildOutput {
    /// Release identity embedded in the manifest.
    pub release_id: ReleaseId,
    /// Build attempt identity embedded in the manifest.
    pub build_id: BuildId,
    /// Canonical manifest digest.
    pub manifest_digest: Sha256Digest,
    /// Content-addressed artifact digest.
    pub artifact_digest: Sha256Digest,
    /// Absolute canonical manifest path.
    pub manifest_path: PathBuf,
    /// Absolute canonical artifact path.
    pub artifact_path: PathBuf,
    /// Absolute path to deterministic generated TypeScript declarations.
    pub generated_types_path: PathBuf,
    /// Absolute path to the stable declarations consumed by application source.
    pub stable_generated_types_path: PathBuf,
    /// Digest of exact generated TypeScript declaration bytes.
    pub generated_types_digest: Sha256Digest,
    /// True when the exact immutable directory already existed.
    pub replayed: bool,
    /// Fingerprint of the exact canonical source graph read by this build.
    pub source_fingerprint: Sha256Digest,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResultWire {
    version: u8,
    release_id: ReleaseId,
    build_id: BuildId,
    manifest_digest: String,
    artifact_digest: String,
    manifest_file: &'static str,
    artifact_file: &'static str,
    generated_types_file: &'static str,
    generated_types_digest: String,
}

pub(crate) fn publish_output(
    root: &Path,
    source_dir: &Path,
    manifest: &ReleaseManifestV1,
    manifest_bytes: &[u8],
    artifact_bytes: &[u8],
    generated_types: &[u8],
    source_fingerprint: Sha256Digest,
) -> Result<BuildOutput, BuildError> {
    let root = std::fs::canonicalize(root).map_err(|_| BuildError::InvalidPath)?;
    let source_root = canonical_source_root(&root, source_dir)?;
    let state = root.join(STATE_DIRECTORY);
    let state_was_absent = !state.exists();
    create_private_directory(&state)?;
    if state_was_absent {
        sync_directory(&root)?;
    }
    let _lock = acquire_lock(&state)?;
    let builds = state.join(BUILDS_DIRECTORY);
    create_private_directory(&builds)?;
    let manifest_digest = Sha256Digest::of(manifest_bytes);
    let generated_types_digest = Sha256Digest::of(generated_types);
    let result_bytes = result_bytes(manifest, manifest_digest, generated_types_digest)?;
    let final_directory = builds.join(manifest.release_id.to_string());
    if final_directory.exists() {
        verify_existing(
            &final_directory,
            manifest_bytes,
            artifact_bytes,
            generated_types,
            &result_bytes,
        )?;
        let stable_generated_types_path =
            publish_stable_generated_types(&source_root, generated_types)?;
        return Ok(build_output(
            &final_directory,
            stable_generated_types_path,
            manifest,
            manifest_digest,
            generated_types_digest,
            true,
            source_fingerprint,
        ));
    }
    let staging = builds.join(format!(
        ".staging-{}-{}",
        manifest.release_id, manifest.build_id
    ));
    if staging.exists() {
        require_directory(&staging)?;
        std::fs::remove_dir_all(&staging).map_err(|_| BuildError::Unavailable)?;
    }
    create_private_directory(&staging)?;
    write_new_file(&staging.join(MANIFEST_FILE), manifest_bytes)?;
    write_new_file(&staging.join(ARTIFACT_FILE), artifact_bytes)?;
    write_new_file(&staging.join(GENERATED_TYPES_FILE), generated_types)?;
    write_new_file(&staging.join(RESULT_FILE), &result_bytes)?;
    sync_directory(&staging)?;
    match std::fs::rename(&staging, &final_directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_existing(
                &final_directory,
                manifest_bytes,
                artifact_bytes,
                generated_types,
                &result_bytes,
            )?;
            let _ = std::fs::remove_dir(&staging);
            return Ok(build_output(
                &final_directory,
                publish_stable_generated_types(&source_root, generated_types)?,
                manifest,
                manifest_digest,
                generated_types_digest,
                true,
                source_fingerprint,
            ));
        }
        Err(_) => return Err(BuildError::Unavailable),
    }
    sync_directory(&builds)?;
    let stable_generated_types_path =
        publish_stable_generated_types(&source_root, generated_types)?;
    Ok(build_output(
        &final_directory,
        stable_generated_types_path,
        manifest,
        manifest_digest,
        generated_types_digest,
        false,
        source_fingerprint,
    ))
}

fn result_bytes(
    manifest: &ReleaseManifestV1,
    manifest_digest: Sha256Digest,
    generated_types_digest: Sha256Digest,
) -> Result<Vec<u8>, BuildError> {
    let mut bytes = serde_json::to_vec(&ResultWire {
        version: 1,
        release_id: manifest.release_id,
        build_id: manifest.build_id,
        manifest_digest: manifest_digest.to_string(),
        artifact_digest: manifest.artifact.digest.to_string(),
        manifest_file: MANIFEST_FILE,
        artifact_file: ARTIFACT_FILE,
        generated_types_file: GENERATED_TYPES_FILE,
        generated_types_digest: generated_types_digest.to_string(),
    })
    .map_err(|_| BuildError::Internal)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn build_output(
    directory: &Path,
    stable_generated_types_path: PathBuf,
    manifest: &ReleaseManifestV1,
    manifest_digest: Sha256Digest,
    generated_types_digest: Sha256Digest,
    replayed: bool,
    source_fingerprint: Sha256Digest,
) -> BuildOutput {
    BuildOutput {
        release_id: manifest.release_id,
        build_id: manifest.build_id,
        manifest_digest,
        artifact_digest: manifest.artifact.digest,
        manifest_path: directory.join(MANIFEST_FILE),
        artifact_path: directory.join(ARTIFACT_FILE),
        generated_types_path: directory.join(GENERATED_TYPES_FILE),
        stable_generated_types_path,
        generated_types_digest,
        replayed,
        source_fingerprint,
    }
}

fn canonical_source_root(root: &Path, source_dir: &Path) -> Result<PathBuf, BuildError> {
    let source_dir = if source_dir.as_os_str().is_empty() {
        Path::new("runku")
    } else {
        source_dir
    };
    let source_root =
        std::fs::canonicalize(root.join(source_dir)).map_err(|_| BuildError::InvalidPath)?;
    if !source_root.starts_with(root) {
        return Err(BuildError::InvalidPath);
    }
    require_directory(&source_root)?;
    Ok(source_root)
}

fn publish_stable_generated_types(
    source_root: &Path,
    generated_types: &[u8],
) -> Result<PathBuf, BuildError> {
    let directory = source_root.join(GENERATED_DIRECTORY);
    create_generated_directory(&directory)?;
    let final_path = directory.join(STABLE_GENERATED_TYPES_FILE);
    if let Ok(metadata) = std::fs::symlink_metadata(&final_path)
        && (!metadata.is_file() || metadata.file_type().is_symlink())
    {
        return Err(BuildError::InvalidPath);
    }
    let staging = directory.join(format!(".{STABLE_GENERATED_TYPES_FILE}.tmp"));
    if let Ok(metadata) = std::fs::symlink_metadata(&staging) {
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(BuildError::InvalidPath);
        }
        std::fs::remove_file(&staging).map_err(|_| BuildError::Unavailable)?;
    }
    write_new_file(&staging, generated_types)?;
    std::fs::rename(&staging, &final_path).map_err(|_| BuildError::Unavailable)?;
    sync_directory(&directory)?;
    Ok(final_path)
}

fn create_generated_directory(path: &Path) -> Result<(), BuildError> {
    match std::fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            require_directory(path)?;
        }
        Err(_) => return Err(BuildError::Unavailable),
    }
    Ok(())
}

fn verify_existing(
    directory: &Path,
    manifest: &[u8],
    artifact: &[u8],
    generated_types: &[u8],
    result: &[u8],
) -> Result<(), BuildError> {
    require_directory(directory)?;
    for (name, expected) in [
        (MANIFEST_FILE, manifest),
        (ARTIFACT_FILE, artifact),
        (GENERATED_TYPES_FILE, generated_types),
        (RESULT_FILE, result),
    ] {
        let path = directory.join(name);
        let metadata = std::fs::symlink_metadata(&path).map_err(|_| BuildError::Corruption)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(BuildError::Corruption);
        }
        let actual = std::fs::read(path).map_err(|_| BuildError::Unavailable)?;
        if actual != expected {
            return Err(BuildError::Conflict);
        }
    }
    Ok(())
}

fn acquire_lock(state: &Path) -> Result<File, BuildError> {
    let path = state.join(LOCK_FILE);
    if let Ok(metadata) = std::fs::symlink_metadata(&path)
        && (!metadata.is_file() || metadata.file_type().is_symlink())
    {
        return Err(BuildError::InvalidPath);
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(|_| BuildError::Unavailable)?;
    make_private_file(&file)?;
    let deadline = Instant::now() + LOCK_DEADLINE;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(LOCK_RETRY);
            }
            Err(_) => return Err(BuildError::Unavailable),
        }
    }
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), BuildError> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|_| BuildError::Unavailable)?;
    file.write_all(bytes).map_err(|_| BuildError::Unavailable)?;
    file.sync_all().map_err(|_| BuildError::Unavailable)
}

fn create_private_directory(path: &Path) -> Result<(), BuildError> {
    match std::fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            require_directory(path)?;
        }
        Err(_) => return Err(BuildError::Unavailable),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| BuildError::Unavailable)?;
    }
    Ok(())
}

fn require_directory(path: &Path) -> Result<(), BuildError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| BuildError::InvalidPath)?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        Err(BuildError::InvalidPath)
    }
}

fn make_private_file(file: &File) -> Result<(), BuildError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| BuildError::Unavailable)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), BuildError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| BuildError::Unavailable)
}
