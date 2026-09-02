//! Canonical atomic local project state and recoverable repository initialization.

use std::{
    fs::TryLockError,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant},
};

use runku_core::{
    EnvironmentDescriptor, EnvironmentId, EnvironmentScope, OperationId, ProjectId, WorkspaceId,
    WorkspaceRef,
};
use runku_cron::{CronContext, CronRepositoryConfig, SqlCronRepository};
use runku_data_sqlite::{SqliteStore, SqliteStoreConfig};
use runku_development::{
    DevelopmentActor, DevelopmentCommand, DevelopmentContext, DevelopmentError,
    DevelopmentRepository, DevelopmentRepositoryConfig, SqlDevelopmentRepository,
};
use runku_development_access::{DevelopmentAccessRepositoryConfig, SqlDevelopmentAccessRepository};
use runku_identity_repository::{IdentityRepositoryConfig, SqlApplicationIdentityRepository};
use runku_observability::{LogArchive, LogRepositoryConfig, SqlLogRepository};
use runku_otel::{OtlpRepositoryConfig, SqlExportCheckpointRepository};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{FilesystemArtifactStore, FilesystemStoreRole};
use runku_value::TimestampMicros;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

/// Fixed private directory under one explicit project root.
pub const LOCAL_STATE_DIRECTORY: &str = ".runku";
const STATE_FILE: &str = "local-state-v1.json";
const LOCK_FILE: &str = "local-state-v1.lock";
const PROCESS_LOCK_FILE: &str = "local-process-v1.lock";
const IDENTITY_PEPPER_FILE: &str = "identity-pepper-v1.key";
const DEVELOPMENT_ACCESS_PEPPER_FILE: &str = "development-access-pepper-v1.key";
const STATE_MAX_BYTES: u64 = 16 * 1024;
const IDENTITY_PEPPER_BYTES: usize = 32;
const LOCK_DEADLINE: Duration = Duration::from_secs(5);
const LOCK_RETRY: Duration = Duration::from_millis(20);

/// Stable local state/configuration failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalStateError {
    /// Root/state path is absent, broad, a symlink, or has an unsupported shape.
    #[error("local project path is invalid")]
    InvalidPath,
    /// State bytes or a configured value are malformed/noncanonical.
    #[error("local project state is invalid")]
    InvalidState,
    /// Existing state conflicts with requested initialization parameters.
    #[error("local project state conflicts with requested configuration")]
    Conflict,
    /// Filesystem or local database is temporarily unavailable.
    #[error("local project state is unavailable")]
    Unavailable,
    /// Durable state violates a trusted invariant.
    #[error("local project state is corrupt")]
    Corruption,
}

impl LocalStateError {
    /// Stable machine-readable category.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidPath => "LOCAL_PATH_INVALID",
            Self::InvalidState => "LOCAL_STATE_INVALID",
            Self::Conflict => "LOCAL_STATE_CONFLICT",
            Self::Unavailable => "LOCAL_STATE_UNAVAILABLE",
            Self::Corruption => "LOCAL_STATE_CORRUPT",
        }
    }

    /// Whether retrying after external recovery may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

/// Versioned non-secret identity/configuration persisted for one local project.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalProjectState {
    /// Stable Project identity reused on reopen.
    pub project_id: ProjectId,
    /// Stable local Development Environment identity.
    pub environment_id: EnvironmentId,
    /// Durable default Workspace identity.
    pub workspace_id: WorkspaceId,
    /// Human-readable default Workspace target.
    pub workspace_ref: WorkspaceRef,
    /// Explicit loopback listener address.
    pub listen_address: SocketAddr,
    /// Trusted initialization timestamp.
    pub created_at: TimestampMicros,
}

impl LocalProjectState {
    /// Exact tenant scope owned by every local component.
    #[must_use]
    pub const fn scope(&self) -> EnvironmentScope {
        EnvironmentScope::new(self.project_id, self.environment_id)
    }

    /// Local-only Environment protection/location descriptor.
    #[must_use]
    pub const fn environment(&self) -> EnvironmentDescriptor {
        EnvironmentDescriptor::local_development(self.environment_id)
    }

    fn validate(&self) -> Result<(), LocalStateError> {
        if !self.listen_address.ip().is_loopback() || self.created_at.get() < 0 {
            return Err(LocalStateError::InvalidState);
        }
        Ok(())
    }
}

/// Canonical physical layout derived from one explicit project root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalPaths {
    /// Canonical project root.
    pub root: PathBuf,
    /// Private Runku state directory.
    pub state: PathBuf,
    /// Logical application data database.
    pub data_database: PathBuf,
    /// Release lifecycle database.
    pub release_database: PathBuf,
    /// Application identity/keyring database.
    pub identity_database: PathBuf,
    /// Private persistent pepper used only to verify local application credentials.
    pub identity_pepper: PathBuf,
    /// Development Access keyring database, isolated from application credentials.
    pub development_access_database: PathBuf,
    /// Private persistent pepper used only to verify Development Access credentials.
    pub development_access_pepper: PathBuf,
    /// Development Workspace database.
    pub development_database: PathBuf,
    /// Durable Cron activation/cursor database.
    pub cron_database: PathBuf,
    /// Durable Product Base operational logs database.
    pub observability_database: PathBuf,
    /// Immutable local Parquet Operational Log archive queried with embedded `DuckDB`.
    pub observability_archive: PathBuf,
    /// Durable OTLP exporter checkpoint database, independent from source logs.
    pub otlp_database: PathBuf,
    /// Content-addressed artifact directory.
    pub artifacts: PathBuf,
}

impl LocalPaths {
    async fn resolve(root: &Path, create_state: bool) -> Result<Self, LocalStateError> {
        let metadata = tokio::fs::symlink_metadata(root)
            .await
            .map_err(|_| LocalStateError::InvalidPath)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() || root.parent().is_none() {
            return Err(LocalStateError::InvalidPath);
        }
        let root = tokio::fs::canonicalize(root)
            .await
            .map_err(|_| LocalStateError::InvalidPath)?;
        if broad_root(&root) {
            return Err(LocalStateError::InvalidPath);
        }
        let state = root.join(LOCAL_STATE_DIRECTORY);
        if create_state {
            tokio::fs::create_dir(&state)
                .await
                .or_else(|error| {
                    if error.kind() == std::io::ErrorKind::AlreadyExists {
                        Ok(())
                    } else {
                        Err(error)
                    }
                })
                .map_err(|_| LocalStateError::Unavailable)?;
            make_private_directory(&state).await?;
        }
        let state_metadata = tokio::fs::symlink_metadata(&state)
            .await
            .map_err(|_| LocalStateError::InvalidPath)?;
        if !state_metadata.is_dir() || state_metadata.file_type().is_symlink() {
            return Err(LocalStateError::InvalidPath);
        }
        Ok(Self {
            data_database: state.join("data.sqlite3"),
            release_database: state.join("releases.sqlite3"),
            identity_database: state.join("identity.sqlite3"),
            identity_pepper: state.join(IDENTITY_PEPPER_FILE),
            development_access_database: state.join("development-access.sqlite3"),
            development_access_pepper: state.join(DEVELOPMENT_ACCESS_PEPPER_FILE),
            development_database: state.join("development.sqlite3"),
            cron_database: state.join("cron.sqlite3"),
            observability_database: state.join("observability.sqlite3"),
            observability_archive: state.join("observability-archive"),
            otlp_database: state.join("otel.sqlite3"),
            artifacts: state.join("artifacts"),
            root,
            state,
        })
    }

    fn state_file(&self) -> PathBuf {
        self.state.join(STATE_FILE)
    }

    fn lock_file(&self) -> PathBuf {
        self.state.join(LOCK_FILE)
    }
}

pub(crate) struct LocalLock {
    _file: std::fs::File,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateWire {
    version: u8,
    project_id: ProjectId,
    environment_id: EnvironmentId,
    workspace_id: WorkspaceId,
    workspace_ref: WorkspaceRef,
    listen_address: String,
    created_at_micros: String,
}

/// Initializes or repairs the exact local layout without replacing existing identity/data.
///
/// Exact repetition with the same Workspace/address is idempotent. A crash after state creation is
/// recovered by reopening/initializing each repository and verifying/creating the Workspace.
///
/// # Errors
///
/// Rejects invalid paths/config, divergent existing state, corruption, or repository failures.
pub async fn initialize_local(
    root: &Path,
    workspace_ref: WorkspaceRef,
    listen_address: SocketAddr,
    now: TimestampMicros,
) -> Result<(LocalProjectState, LocalPaths), LocalStateError> {
    if !listen_address.ip().is_loopback() || now.get() < 0 {
        return Err(LocalStateError::InvalidState);
    }
    let paths = LocalPaths::resolve(root, true).await?;
    let _lock = acquire_lock(&paths).await?;
    let proposed = LocalProjectState {
        project_id: ProjectId::generate(),
        environment_id: EnvironmentId::generate(),
        workspace_id: WorkspaceId::generate(),
        workspace_ref: workspace_ref.clone(),
        listen_address,
        created_at: now,
    };
    let state = match write_new_state(&paths, &proposed).await {
        Ok(true) => proposed,
        Ok(false) => {
            let existing = read_state(&paths).await?;
            if existing.workspace_ref != workspace_ref || existing.listen_address != listen_address
            {
                return Err(LocalStateError::Conflict);
            }
            existing
        }
        Err(error) => return Err(error),
    };
    ensure_identity_pepper(&paths).await?;
    ensure_development_access_pepper(&paths).await?;
    initialize_repositories(&paths, &state).await?;
    Ok((state, paths))
}

pub(crate) async fn load_identity_pepper(
    paths: &LocalPaths,
) -> Result<[u8; IDENTITY_PEPPER_BYTES], LocalStateError> {
    load_private_pepper(&paths.identity_pepper).await
}

pub(crate) async fn load_development_access_pepper(
    paths: &LocalPaths,
) -> Result<[u8; IDENTITY_PEPPER_BYTES], LocalStateError> {
    load_private_pepper(&paths.development_access_pepper).await
}

async fn load_private_pepper(path: &Path) -> Result<[u8; IDENTITY_PEPPER_BYTES], LocalStateError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| LocalStateError::Corruption)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != IDENTITY_PEPPER_BYTES as u64
    {
        return Err(LocalStateError::Corruption);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(LocalStateError::Corruption);
        }
    }
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    bytes.try_into().map_err(|_| LocalStateError::Corruption)
}

async fn ensure_identity_pepper(paths: &LocalPaths) -> Result<(), LocalStateError> {
    ensure_private_pepper(paths, &paths.identity_pepper, "identity").await
}

async fn ensure_development_access_pepper(paths: &LocalPaths) -> Result<(), LocalStateError> {
    ensure_private_pepper(
        paths,
        &paths.development_access_pepper,
        "development-access",
    )
    .await
}

async fn ensure_private_pepper(
    paths: &LocalPaths,
    target: &Path,
    temporary_label: &str,
) -> Result<(), LocalStateError> {
    if tokio::fs::symlink_metadata(target).await.is_ok() {
        load_private_pepper(target).await?;
        return Ok(());
    }
    let mut pepper = zeroize::Zeroizing::new([0_u8; IDENTITY_PEPPER_BYTES]);
    getrandom::fill(pepper.as_mut()).map_err(|_| LocalStateError::Unavailable)?;
    let temporary = paths.state.join(format!(
        ".{temporary_label}-pepper-{}.tmp",
        OperationId::generate()
    ));
    let mut options = tokio::fs::OpenOptions::new();
    options.create_new(true).write(true);
    let mut file = options
        .open(&temporary)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|_| LocalStateError::Unavailable)?;
    }
    if file.write_all(pepper.as_ref()).await.is_err() || file.sync_all().await.is_err() {
        drop(file);
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(LocalStateError::Unavailable);
    }
    drop(file);
    match tokio::fs::hard_link(&temporary, target).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(LocalStateError::Unavailable);
        }
    }
    tokio::fs::remove_file(&temporary)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    sync_directory(&paths.state).await?;
    load_private_pepper(target).await.map(|_| ())
}

async fn acquire_lock(paths: &LocalPaths) -> Result<LocalLock, LocalStateError> {
    acquire_file_lock(&paths.lock_file(), true).await
}

pub(crate) async fn acquire_process_lock(paths: &LocalPaths) -> Result<LocalLock, LocalStateError> {
    acquire_file_lock(&paths.state.join(PROCESS_LOCK_FILE), false).await
}

pub(crate) async fn acquire_otel_exporter_lock(
    paths: &LocalPaths,
    exporter: &str,
) -> Result<LocalLock, LocalStateError> {
    acquire_file_lock(
        &paths.state.join(format!("otel-export-{exporter}.lock")),
        false,
    )
    .await
}

async fn acquire_file_lock(lock_path: &Path, wait: bool) -> Result<LocalLock, LocalStateError> {
    if let Ok(metadata) = tokio::fs::symlink_metadata(&lock_path).await
        && (!metadata.is_file() || metadata.file_type().is_symlink())
    {
        return Err(LocalStateError::InvalidPath);
    }
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    let file = options
        .open(lock_path)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|_| LocalStateError::Unavailable)?;
    }
    let file = file.into_std().await;
    let deadline = Instant::now() + LOCK_DEADLINE;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(LocalLock { _file: file }),
            Err(TryLockError::WouldBlock) if wait && Instant::now() < deadline => {
                tokio::time::sleep(LOCK_RETRY).await;
            }
            Err(TryLockError::WouldBlock) => {
                return Err(LocalStateError::Conflict);
            }
            Err(TryLockError::Error(_)) => return Err(LocalStateError::Unavailable),
        }
    }
}

/// Loads and strictly validates an existing local state without creating files.
///
/// # Errors
///
/// Rejects absent/symlink/broad roots, malformed/noncanonical state, or corruption.
pub async fn load_local(root: &Path) -> Result<(LocalProjectState, LocalPaths), LocalStateError> {
    let paths = LocalPaths::resolve(root, false).await?;
    let state = read_state(&paths).await?;
    Ok((state, paths))
}

async fn write_new_state(
    paths: &LocalPaths,
    state: &LocalProjectState,
) -> Result<bool, LocalStateError> {
    state.validate()?;
    let bytes = encode_state(state)?;
    let temporary = paths
        .state
        .join(format!(".state-{}.tmp", OperationId::generate()));
    let mut options = tokio::fs::OpenOptions::new();
    options.create_new(true).write(true);
    let mut file = options
        .open(&temporary)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|_| LocalStateError::Unavailable)?;
    }
    if file.write_all(&bytes).await.is_err() || file.sync_all().await.is_err() {
        drop(file);
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(LocalStateError::Unavailable);
    }
    drop(file);
    let result = match tokio::fs::hard_link(&temporary, paths.state_file()).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(_) => Err(LocalStateError::Unavailable),
    };
    tokio::fs::remove_file(&temporary)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    sync_directory(&paths.state).await?;
    result
}

async fn read_state(paths: &LocalPaths) -> Result<LocalProjectState, LocalStateError> {
    let path = paths.state_file();
    let metadata = tokio::fs::symlink_metadata(&path)
        .await
        .map_err(|_| LocalStateError::InvalidState)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > STATE_MAX_BYTES
    {
        return Err(LocalStateError::InvalidState);
    }
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    let wire: StateWire =
        serde_json::from_slice(&bytes).map_err(|_| LocalStateError::InvalidState)?;
    let state = decode_state(wire)?;
    if encode_state(&state)? != bytes {
        return Err(LocalStateError::InvalidState);
    }
    Ok(state)
}

fn encode_state(state: &LocalProjectState) -> Result<Vec<u8>, LocalStateError> {
    let wire = StateWire {
        version: 1,
        project_id: state.project_id,
        environment_id: state.environment_id,
        workspace_id: state.workspace_id,
        workspace_ref: state.workspace_ref.clone(),
        listen_address: state.listen_address.to_string(),
        created_at_micros: state.created_at.get().to_string(),
    };
    let bytes = serde_json::to_vec(&wire).map_err(|_| LocalStateError::InvalidState)?;
    if bytes.len() > usize::try_from(STATE_MAX_BYTES).map_err(|_| LocalStateError::InvalidState)? {
        return Err(LocalStateError::InvalidState);
    }
    Ok(bytes)
}

fn decode_state(wire: StateWire) -> Result<LocalProjectState, LocalStateError> {
    if wire.version != 1 || !canonical_nonnegative_i64(&wire.created_at_micros) {
        return Err(LocalStateError::InvalidState);
    }
    let state = LocalProjectState {
        project_id: wire.project_id,
        environment_id: wire.environment_id,
        workspace_id: wire.workspace_id,
        workspace_ref: wire.workspace_ref,
        listen_address: SocketAddr::from_str(&wire.listen_address)
            .map_err(|_| LocalStateError::InvalidState)?,
        created_at: TimestampMicros::new(
            wire.created_at_micros
                .parse()
                .map_err(|_| LocalStateError::InvalidState)?,
        ),
    };
    state.validate()?;
    Ok(state)
}

fn canonical_nonnegative_i64(value: &str) -> bool {
    value == "0"
        || value
            .as_bytes()
            .first()
            .is_some_and(|first| (b'1'..=b'9').contains(first))
            && value.as_bytes()[1..].iter().all(u8::is_ascii_digit)
            && value.parse::<i64>().is_ok()
}

async fn initialize_repositories(
    paths: &LocalPaths,
    state: &LocalProjectState,
) -> Result<(), LocalStateError> {
    let context = DevelopmentContext {
        scope: state.scope(),
        environment: state.environment(),
    };
    let development = SqlDevelopmentRepository::connect_sqlite(
        &sqlite_url(&paths.development_database),
        DevelopmentRepositoryConfig::LOCAL,
        context,
    )
    .await
    .map_err(map_development)?;
    let snapshot = development.snapshot(context).await;
    match snapshot {
        Err(DevelopmentError::WorkspaceNotFound) => {
            development
                .apply(
                    context,
                    OperationId::generate(),
                    &DevelopmentCommand::CreateWorkspace {
                        workspace_id: state.workspace_id,
                        workspace_ref: state.workspace_ref.clone(),
                        actor: DevelopmentActor::from_str("local-init").map_err(map_development)?,
                        created_at: state.created_at,
                    },
                )
                .await
                .map_err(map_development)?;
        }
        Ok(snapshot) => match snapshot.workspace_binding(&state.workspace_ref) {
            Some(workspace) if workspace.workspace_id == state.workspace_id => {}
            Some(_) | None => return Err(LocalStateError::Corruption),
        },
        Err(error) => return Err(map_development(error)),
    }
    SqlReleaseRepository::connect_sqlite(
        &sqlite_url(&paths.release_database),
        RepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalStateError::Unavailable)?;
    SqlApplicationIdentityRepository::connect_sqlite(
        &sqlite_url(&paths.identity_database),
        IdentityRepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalStateError::Unavailable)?;
    SqlDevelopmentAccessRepository::connect_sqlite(
        &sqlite_url(&paths.development_access_database),
        DevelopmentAccessRepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalStateError::Unavailable)?;
    SqlCronRepository::connect_sqlite(
        &sqlite_url(&paths.cron_database),
        CronRepositoryConfig::LOCAL,
        CronContext {
            scope: state.scope(),
            environment: state.environment(),
        },
    )
    .await
    .map_err(|_| LocalStateError::Unavailable)?;
    SqlLogRepository::connect_sqlite(
        &sqlite_url(&paths.observability_database),
        LogRepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalStateError::Unavailable)?;
    LogArchive::open_filesystem(paths.observability_archive.clone())
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    SqlExportCheckpointRepository::connect_sqlite(
        &sqlite_url(&paths.otlp_database),
        OtlpRepositoryConfig::LOCAL,
    )
    .await
    .map_err(|_| LocalStateError::Unavailable)?;
    SqliteStore::open(&paths.data_database, SqliteStoreConfig::LOCAL)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    FilesystemArtifactStore::open(&paths.artifacts, FilesystemStoreRole::LocalDevelopment)
        .await
        .map_err(|_| LocalStateError::Unavailable)?;
    Ok(())
}

async fn make_private_directory(path: &Path) -> Result<(), LocalStateError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|_| LocalStateError::Unavailable)?;
    }
    Ok(())
}

async fn sync_directory(path: &Path) -> Result<(), LocalStateError> {
    #[cfg(unix)]
    {
        let directory = tokio::fs::File::open(path)
            .await
            .map_err(|_| LocalStateError::Unavailable)?;
        directory
            .sync_all()
            .await
            .map_err(|_| LocalStateError::Unavailable)?;
    }
    Ok(())
}

fn broad_root(path: &Path) -> bool {
    if path.parent().is_none() {
        return true;
    }
    ["HOME", "USERPROFILE"]
        .into_iter()
        .filter_map(std::env::var_os)
        .filter_map(|candidate| std::fs::canonicalize(candidate).ok())
        .any(|candidate| candidate == path)
}

pub(crate) fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display())
}

fn map_development(error: DevelopmentError) -> LocalStateError {
    match error {
        DevelopmentError::Unavailable | DevelopmentError::ResultUncertain => {
            LocalStateError::Unavailable
        }
        DevelopmentError::Conflict => LocalStateError::Conflict,
        DevelopmentError::Corruption | DevelopmentError::InvalidSnapshot => {
            LocalStateError::Corruption
        }
        DevelopmentError::InvalidInput
        | DevelopmentError::PolicyDenied
        | DevelopmentError::InvalidRevision
        | DevelopmentError::WorkspaceNotFound
        | DevelopmentError::WorkspaceEmpty
        | DevelopmentError::RevisionNotFound
        | DevelopmentError::LimitExceeded
        | DevelopmentError::Unsupported => LocalStateError::InvalidState,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr};

    use runku_core::WorkspaceRef;
    use runku_development::{
        DevelopmentContext, DevelopmentRepository, DevelopmentRepositoryConfig,
        SqlDevelopmentRepository,
    };
    use runku_value::TimestampMicros;
    use tempfile::tempdir;

    use super::{LOCAL_STATE_DIRECTORY, LocalStateError, initialize_local, load_local, sqlite_url};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn address(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn workspace(value: &str) -> Result<WorkspaceRef, Box<dyn std::error::Error>> {
        Ok(WorkspaceRef::from_str(value)?)
    }

    #[tokio::test]
    async fn initialization_is_durable_idempotent_and_complete() -> TestResult {
        let directory = tempdir()?;
        let workspace = workspace("team/default")?;
        let now = TimestampMicros::new(1_800_000_000_000_000);
        let (created, paths) =
            initialize_local(directory.path(), workspace.clone(), address(3210), now).await?;
        let (reopened, reopened_paths) =
            initialize_local(directory.path(), workspace.clone(), address(3210), now).await?;

        assert_eq!(created, reopened);
        assert_eq!(paths, reopened_paths);
        assert_eq!(load_local(directory.path()).await?.0, created);
        for path in [
            &paths.data_database,
            &paths.release_database,
            &paths.identity_database,
            &paths.development_database,
            &paths.cron_database,
        ] {
            assert!(path.is_file(), "missing repository: {}", path.display());
        }
        assert!(paths.artifacts.is_dir());
        let pepper = tokio::fs::read(&paths.identity_pepper).await?;
        assert_eq!(pepper.len(), 32);

        let context = DevelopmentContext {
            scope: created.scope(),
            environment: created.environment(),
        };
        let repository = SqlDevelopmentRepository::connect_sqlite(
            &sqlite_url(&paths.development_database),
            DevelopmentRepositoryConfig::LOCAL,
            context,
        )
        .await?;
        let snapshot = repository.snapshot(context).await?;
        let binding = snapshot
            .workspace_binding(&workspace)
            .ok_or("workspace was not created")?;
        assert_eq!(binding.workspace_id, created.workspace_id);
        assert_eq!(binding.head_revision, None);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let state_mode = std::fs::metadata(&paths.state)?.permissions().mode() & 0o777;
            let file_mode = std::fs::metadata(paths.state.join("local-state-v1.json"))?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(state_mode, 0o700);
            assert_eq!(file_mode, 0o600);
            let pepper_mode = std::fs::metadata(&paths.identity_pepper)?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(pepper_mode, 0o600);
        }
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_initialization_converges_on_one_identity() -> TestResult {
        let directory = tempdir()?;
        let root = directory.path().to_path_buf();
        let workspace = workspace("shared")?;
        let now = TimestampMicros::new(1_800_000_000_000_001);
        let left = initialize_local(&root, workspace.clone(), address(3211), now);
        let right = initialize_local(&root, workspace, address(3211), now);
        let (left, right) = tokio::join!(left, right);

        assert_eq!(left?.0, right?.0);
        let temporary_count = std::fs::read_dir(root.join(LOCAL_STATE_DIRECTORY))?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".state-"))
            .count();
        assert_eq!(temporary_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn divergent_reinitialization_fails_without_overwrite() -> TestResult {
        let directory = tempdir()?;
        let now = TimestampMicros::new(1_800_000_000_000_002);
        let original =
            initialize_local(directory.path(), workspace("default")?, address(3212), now)
                .await?
                .0;

        assert_eq!(
            initialize_local(directory.path(), workspace("other")?, address(3212), now)
                .await
                .map(|_| ()),
            Err(LocalStateError::Conflict)
        );
        assert_eq!(
            initialize_local(directory.path(), workspace("default")?, address(3213), now)
                .await
                .map(|_| ()),
            Err(LocalStateError::Conflict)
        );
        assert_eq!(load_local(directory.path()).await?.0, original);
        Ok(())
    }

    #[tokio::test]
    async fn malformed_or_noncanonical_state_is_never_replaced() -> TestResult {
        let directory = tempdir()?;
        let now = TimestampMicros::new(1_800_000_000_000_003);
        let (_, paths) =
            initialize_local(directory.path(), workspace("default")?, address(3214), now).await?;
        let state_file = paths.state.join("local-state-v1.json");
        let valid = tokio::fs::read(&state_file).await?;

        for invalid in [
            [valid.as_slice(), b"\n"].concat(),
            [
                valid.strip_suffix(b"}").ok_or("invalid test fixture")?,
                br#",\"unknown\":true}"#,
            ]
            .concat(),
            valid
                .windows(b"1800000000000003".len())
                .position(|part| part == b"1800000000000003")
                .map(|index| {
                    let mut changed = valid.clone();
                    changed.splice(index..index + 16, b"01800000000000003".iter().copied());
                    changed
                })
                .ok_or("timestamp was not encoded")?,
        ] {
            tokio::fs::write(&state_file, &invalid).await?;
            assert_eq!(
                load_local(directory.path()).await.map(|_| ()),
                Err(LocalStateError::InvalidState)
            );
            assert_eq!(tokio::fs::read(&state_file).await?, invalid);
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalid_identity_pepper_is_not_rotated_or_repaired_silently() -> TestResult {
        let directory = tempdir()?;
        let now = TimestampMicros::new(1_800_000_000_000_004);
        let (_, paths) =
            initialize_local(directory.path(), workspace("default")?, address(3216), now).await?;
        tokio::fs::write(&paths.identity_pepper, b"short").await?;

        assert_eq!(
            initialize_local(directory.path(), workspace("default")?, address(3216), now)
                .await
                .map(|_| ()),
            Err(LocalStateError::Corruption)
        );
        assert_eq!(tokio::fs::read(paths.identity_pepper).await?, b"short");
        Ok(())
    }

    #[tokio::test]
    async fn invalid_paths_and_non_loopback_listener_fail_closed() -> TestResult {
        let directory = tempdir()?;
        assert_eq!(
            initialize_local(
                directory.path(),
                workspace("default")?,
                SocketAddr::from(([192, 0, 2, 1], 3215)),
                TimestampMicros::new(1),
            )
            .await
            .map(|_| ()),
            Err(LocalStateError::InvalidState)
        );
        assert!(!directory.path().join(LOCAL_STATE_DIRECTORY).exists());
        assert_eq!(
            load_local(std::path::Path::new("/")).await.map(|_| ()),
            Err(LocalStateError::InvalidPath)
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let target = tempdir()?;
            let link = directory.path().join("project-link");
            symlink(target.path(), &link)?;
            assert_eq!(
                initialize_local(
                    &link,
                    workspace("default")?,
                    address(3215),
                    TimestampMicros::new(1),
                )
                .await
                .map(|_| ()),
                Err(LocalStateError::InvalidPath)
            );
        }
        Ok(())
    }
}
