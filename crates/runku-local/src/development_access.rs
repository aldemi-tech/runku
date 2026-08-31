//! Safe local lifecycle for actor-bound Development Access credentials.

use std::path::Path;

use runku_core::DevelopmentCredentialId;
use runku_development::DevelopmentActor;
use runku_development_access::{
    DevelopmentAccessError, DevelopmentAccessRepository, DevelopmentAccessRepositoryConfig,
    DevelopmentCredential, DevelopmentCredentialLabel, DevelopmentCredentialStatus, DevelopmentKey,
    DevelopmentKeyCrypto, DevelopmentLifecycleResult, SqlDevelopmentAccessRepository,
};
use runku_value::TimestampMicros;
use thiserror::Error;

use crate::state::{load_development_access_pepper, sqlite_url};
use crate::{LocalProjectState, LocalStateError, load_local};

/// Stable local Development Access management failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalDevelopmentAccessError {
    /// Initialized state, database path, or pepper is invalid.
    #[error("local Development Access state is invalid")]
    InvalidState,
    /// Actor, label, expiry, timestamp, or transition is invalid.
    #[error("local Development Access input is invalid")]
    InvalidInput,
    /// Requested credential does not exist in the exact Environment.
    #[error("local Development Access credential was not found")]
    NotFound,
    /// Existing durable state conflicts with the operation.
    #[error("local Development Access operation conflicts")]
    Conflict,
    /// Repository, filesystem, or entropy source is unavailable.
    #[error("local Development Access storage is unavailable")]
    Unavailable,
    /// Commit outcome is unknown and must be reconciled.
    #[error("local Development Access result is uncertain")]
    ResultUncertain,
    /// Durable content or migration state violates the contract.
    #[error("local Development Access storage is corrupt")]
    Corruption,
}

impl LocalDevelopmentAccessError {
    /// Stable machine-readable CLI code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidState => "LOCAL_DEVELOPMENT_ACCESS_STATE_INVALID",
            Self::InvalidInput => "LOCAL_DEVELOPMENT_ACCESS_INPUT_INVALID",
            Self::NotFound => "LOCAL_DEVELOPMENT_ACCESS_NOT_FOUND",
            Self::Conflict => "LOCAL_DEVELOPMENT_ACCESS_CONFLICT",
            Self::Unavailable => "LOCAL_DEVELOPMENT_ACCESS_UNAVAILABLE",
            Self::ResultUncertain => "LOCAL_DEVELOPMENT_ACCESS_RESULT_UNCERTAIN",
            Self::Corruption => "LOCAL_DEVELOPMENT_ACCESS_CORRUPT",
        }
    }

    /// Whether the same request may succeed after external recovery.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable | Self::ResultUncertain)
    }
}

/// Non-secret Development Access credential view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalDevelopmentCredentialMetadata {
    /// Independently revocable credential identifier.
    pub id: DevelopmentCredentialId,
    /// Trusted actor attributed to remote Workspace changes.
    pub actor: DevelopmentActor,
    /// Operator-facing usage/deployment label.
    pub label: DevelopmentCredentialLabel,
    /// Current irreversible lifecycle state.
    pub status: DevelopmentCredentialStatus,
    /// Creation timestamp.
    pub created_at: TimestampMicros,
    /// Optional absolute expiry.
    pub expires_at: Option<TimestampMicros>,
    /// Optional first revocation timestamp.
    pub revoked_at: Option<TimestampMicros>,
}

impl From<&DevelopmentCredential> for LocalDevelopmentCredentialMetadata {
    fn from(value: &DevelopmentCredential) -> Self {
        Self {
            id: value.id,
            actor: value.actor.clone(),
            label: value.label.clone(),
            status: value.status,
            created_at: value.created_at,
            expires_at: value.expires_at,
            revoked_at: value.revoked_at,
        }
    }
}

/// Newly persisted credential plus its one-time bearer.
pub struct LocalCreatedDevelopmentCredential {
    /// Safe durable metadata.
    pub credential: LocalDevelopmentCredentialMetadata,
    /// Unrecoverable bearer shown once.
    pub key: DevelopmentKey,
}

impl std::fmt::Debug for LocalCreatedDevelopmentCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalCreatedDevelopmentCredential")
            .field("credential", &self.credential)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// Local management composition using an isolated repository and pepper.
pub struct LocalDevelopmentAccessManager {
    state: LocalProjectState,
    repository: SqlDevelopmentAccessRepository,
    crypto: DevelopmentKeyCrypto,
}

impl std::fmt::Debug for LocalDevelopmentAccessManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalDevelopmentAccessManager")
            .field("state", &self.state)
            .field("repository", &self.repository)
            .field("crypto", &"[REDACTED]")
            .finish()
    }
}

impl LocalDevelopmentAccessManager {
    /// Opens initialized Development Access state without creating or repairing it.
    ///
    /// # Errors
    ///
    /// Rejects missing, empty, symlinked, incorrectly permissioned, or corrupt state.
    pub async fn open(root: &Path) -> Result<Self, LocalDevelopmentAccessError> {
        let (state, paths) = load_local(root).await.map_err(map_state)?;
        let metadata = tokio::fs::symlink_metadata(&paths.development_access_database)
            .await
            .map_err(|_| LocalDevelopmentAccessError::InvalidState)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
            return Err(LocalDevelopmentAccessError::InvalidState);
        }
        let pepper = load_development_access_pepper(&paths)
            .await
            .map_err(map_state)?;
        let repository = SqlDevelopmentAccessRepository::connect_sqlite(
            &sqlite_url(&paths.development_access_database),
            DevelopmentAccessRepositoryConfig::LOCAL,
        )
        .await
        .map_err(map_access)?;
        repository.health().await.map_err(map_access)?;
        Ok(Self {
            state,
            repository,
            crypto: DevelopmentKeyCrypto::new(pepper),
        })
    }

    /// Returns the exact locally owned Environment state.
    #[must_use]
    pub const fn state(&self) -> &LocalProjectState {
        &self.state
    }

    /// Creates one actor-bound, independently revocable credential.
    ///
    /// # Errors
    ///
    /// Rejects invalid metadata, entropy failures, ID conflicts, and persistence failures.
    pub async fn create_credential(
        &self,
        id: DevelopmentCredentialId,
        actor: DevelopmentActor,
        label: DevelopmentCredentialLabel,
        created_at: TimestampMicros,
        expires_at: Option<TimestampMicros>,
    ) -> Result<LocalCreatedDevelopmentCredential, LocalDevelopmentAccessError> {
        let generated = self.crypto.generate(id).map_err(map_access)?;
        let credential = DevelopmentCredential {
            id,
            scope: self.state.scope(),
            actor,
            label,
            digest: generated.digest,
            status: DevelopmentCredentialStatus::Active,
            created_at,
            expires_at,
            revoked_at: None,
            deleted_at: None,
        };
        self.repository
            .create_credential(&credential)
            .await
            .map_err(map_access)?;
        Ok(LocalCreatedDevelopmentCredential {
            credential: LocalDevelopmentCredentialMetadata::from(&credential),
            key: generated.key,
        })
    }

    /// Lists active and revoked metadata in stable ID order; secrets and tombstones are absent.
    ///
    /// # Errors
    ///
    /// Returns repository availability or corruption failures without partial results.
    pub async fn list_credentials(
        &self,
    ) -> Result<Vec<LocalDevelopmentCredentialMetadata>, LocalDevelopmentAccessError> {
        self.repository
            .list_credentials(self.state.scope())
            .await
            .map(|values| {
                values
                    .iter()
                    .map(LocalDevelopmentCredentialMetadata::from)
                    .collect()
            })
            .map_err(map_access)
    }

    /// Creates a replacement with the exact actor of a non-deleted source key.
    ///
    /// The source remains unchanged so operators can validate the replacement before revocation.
    ///
    /// # Errors
    ///
    /// Rejects identical IDs, missing/deleted sources, invalid metadata, or persistence failure.
    pub async fn rotate_credential(
        &self,
        source_id: DevelopmentCredentialId,
        replacement_id: DevelopmentCredentialId,
        label: DevelopmentCredentialLabel,
        created_at: TimestampMicros,
        expires_at: Option<TimestampMicros>,
    ) -> Result<LocalCreatedDevelopmentCredential, LocalDevelopmentAccessError> {
        if source_id == replacement_id {
            return Err(LocalDevelopmentAccessError::InvalidInput);
        }
        let source = self
            .repository
            .get_credential(self.state.scope(), source_id)
            .await
            .map_err(map_access)?
            .filter(|credential| credential.status != DevelopmentCredentialStatus::Deleted)
            .ok_or(LocalDevelopmentAccessError::NotFound)?;
        self.create_credential(replacement_id, source.actor, label, created_at, expires_at)
            .await
    }

    /// Irreversibly revokes one credential, idempotently.
    ///
    /// # Errors
    ///
    /// Returns invalid timestamp, missing target, or persistence failures.
    pub async fn revoke_credential(
        &self,
        id: DevelopmentCredentialId,
        revoked_at: TimestampMicros,
    ) -> Result<DevelopmentLifecycleResult, LocalDevelopmentAccessError> {
        self.repository
            .revoke_credential(self.state.scope(), id, revoked_at)
            .await
            .map_err(map_access)
    }

    /// Tombstones one already-revoked credential, idempotently.
    ///
    /// # Errors
    ///
    /// Rejects active/missing targets, invalid timestamps, or persistence failures.
    pub async fn delete_credential(
        &self,
        id: DevelopmentCredentialId,
        deleted_at: TimestampMicros,
    ) -> Result<DevelopmentLifecycleResult, LocalDevelopmentAccessError> {
        self.repository
            .delete_credential(self.state.scope(), id, deleted_at)
            .await
            .map_err(map_access)
    }

    /// Returns the monotonic Development Access configuration revision.
    ///
    /// # Errors
    ///
    /// Returns availability or corruption failures.
    pub async fn configuration_revision(&self) -> Result<u64, LocalDevelopmentAccessError> {
        self.repository
            .configuration_revision(self.state.scope())
            .await
            .map_err(map_access)
    }
}

const fn map_state(error: LocalStateError) -> LocalDevelopmentAccessError {
    match error {
        LocalStateError::InvalidPath | LocalStateError::InvalidState => {
            LocalDevelopmentAccessError::InvalidState
        }
        LocalStateError::Conflict => LocalDevelopmentAccessError::Conflict,
        LocalStateError::Unavailable => LocalDevelopmentAccessError::Unavailable,
        LocalStateError::Corruption => LocalDevelopmentAccessError::Corruption,
    }
}

const fn map_access(error: DevelopmentAccessError) -> LocalDevelopmentAccessError {
    match error {
        DevelopmentAccessError::NotFound => LocalDevelopmentAccessError::NotFound,
        DevelopmentAccessError::Conflict => LocalDevelopmentAccessError::Conflict,
        DevelopmentAccessError::Unavailable | DevelopmentAccessError::EntropyUnavailable => {
            LocalDevelopmentAccessError::Unavailable
        }
        DevelopmentAccessError::ResultUncertain => LocalDevelopmentAccessError::ResultUncertain,
        DevelopmentAccessError::Corruption => LocalDevelopmentAccessError::Corruption,
        DevelopmentAccessError::InvalidInput
        | DevelopmentAccessError::InvalidCredential
        | DevelopmentAccessError::LimitExceeded
        | DevelopmentAccessError::Unsupported => LocalDevelopmentAccessError::InvalidInput,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr as _};

    use runku_core::{DevelopmentCredentialId, WorkspaceRef};
    use runku_development_access::{DevelopmentCredentialStatus, DevelopmentLifecycleResult};
    use runku_value::TimestampMicros;
    use tempfile::tempdir;

    use super::{LocalDevelopmentAccessError, LocalDevelopmentAccessManager};
    use crate::initialize_local;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    async fn initialized(
        root: &std::path::Path,
    ) -> Result<LocalDevelopmentAccessManager, Box<dyn std::error::Error>> {
        initialize_local(
            root,
            WorkspaceRef::from_str("default")?,
            SocketAddr::from(([127, 0, 0, 1], 3210)),
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?;
        Ok(LocalDevelopmentAccessManager::open(root).await?)
    }

    #[tokio::test]
    async fn complete_multi_key_rotation_revoke_delete_and_reopen() -> TestResult {
        let directory = tempdir()?;
        let manager = initialized(directory.path()).await?;
        let first_id = DevelopmentCredentialId::generate();
        let first = manager
            .create_credential(
                first_id,
                "manuel".parse()?,
                "laptop".parse()?,
                TimestampMicros::new(1_800_000_000_000_010),
                None,
            )
            .await?;
        assert!(first.key.expose().starts_with("rk_dev_v1_"));
        assert!(!format!("{first:?}").contains(first.key.expose()));

        let second_id = DevelopmentCredentialId::generate();
        let second = manager
            .rotate_credential(
                first_id,
                second_id,
                "laptop-rotated".parse()?,
                TimestampMicros::new(1_800_000_000_000_020),
                None,
            )
            .await?;
        assert_eq!(second.credential.actor.as_str(), "manuel");
        assert_ne!(first.key.expose(), second.key.expose());
        assert_eq!(manager.list_credentials().await?.len(), 2);
        assert_eq!(manager.configuration_revision().await?, 2);

        assert_eq!(
            manager
                .revoke_credential(first_id, TimestampMicros::new(1_800_000_000_000_030))
                .await?,
            DevelopmentLifecycleResult::Applied
        );
        assert_eq!(
            manager
                .revoke_credential(first_id, TimestampMicros::new(1_800_000_000_000_031))
                .await?,
            DevelopmentLifecycleResult::Replayed
        );
        assert_eq!(
            manager
                .delete_credential(first_id, TimestampMicros::new(1_800_000_000_000_040))
                .await?,
            DevelopmentLifecycleResult::Applied
        );
        assert_eq!(manager.list_credentials().await?.len(), 1);
        assert_eq!(
            manager.list_credentials().await?[0].status,
            DevelopmentCredentialStatus::Active
        );
        drop(manager);

        let reopened = LocalDevelopmentAccessManager::open(directory.path()).await?;
        assert_eq!(reopened.configuration_revision().await?, 4);
        assert_eq!(reopened.list_credentials().await?.len(), 1);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dedicated_pepper_is_private_and_symlinked_database_is_rejected() -> TestResult {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let directory = tempdir()?;
        let manager = initialized(directory.path()).await?;
        drop(manager);
        let identity = std::fs::read(directory.path().join(".runku/identity-pepper-v1.key"))?;
        let development = std::fs::read(
            directory
                .path()
                .join(".runku/development-access-pepper-v1.key"),
        )?;
        assert_ne!(identity, development);
        let metadata = std::fs::metadata(
            directory
                .path()
                .join(".runku/development-access-pepper-v1.key"),
        )?;
        assert_eq!(metadata.permissions().mode() & 0o077, 0);

        let database = directory.path().join(".runku/development-access.sqlite3");
        let relocated = directory.path().join("development-access-real.sqlite3");
        std::fs::rename(&database, &relocated)?;
        symlink(&relocated, &database)?;
        assert!(matches!(
            LocalDevelopmentAccessManager::open(directory.path()).await,
            Err(LocalDevelopmentAccessError::InvalidState)
        ));
        Ok(())
    }
}
