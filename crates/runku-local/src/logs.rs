//! Safe local Operational Logs query and retention boundary.

use std::{path::Path, sync::Arc};

use runku_core::{EnvironmentId, EnvironmentScope};
use runku_observability::{
    LogArchive, LogArchiveStatus, LogPage, LogQuery, LogRepository, LogRepositoryConfig,
    LogRepositoryError, PruneResult, SqlLogRepository, TieredLogRepository,
};
use runku_value::TimestampMicros;
use thiserror::Error;

use crate::{LocalProjectState, LocalStateError, load_local, state::sqlite_url};

/// Sanitized local log administration failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalLogError {
    /// Project state/path or the dedicated database file is unsafe.
    #[error("local operational log state is invalid")]
    InvalidState,
    /// Query/retention parameters or Environment confirmation are invalid.
    #[error("local operational log request is invalid")]
    InvalidRequest,
    /// Repository is temporarily unavailable.
    #[error("local operational log repository is unavailable")]
    Unavailable,
    /// Durable records or migration state are corrupt.
    #[error("local operational log repository is corrupt")]
    Corruption,
}

impl LocalLogError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidState => "LOCAL_LOG_STATE_INVALID",
            Self::InvalidRequest => "LOCAL_LOG_REQUEST_INVALID",
            Self::Unavailable => "LOCAL_LOG_UNAVAILABLE",
            Self::Corruption => "LOCAL_LOG_CORRUPT",
        }
    }
}

/// Scoped local read/retention manager over the hot SQLite and immutable Parquet tiers.
#[derive(Debug)]
pub struct LocalLogManager {
    state: LocalProjectState,
    repository: Arc<dyn LogRepository>,
    archive: LogArchive,
}

impl LocalLogManager {
    /// Opens an initialized project's existing regular non-symlink log database.
    ///
    /// # Errors
    ///
    /// Rejects absent/unsafe state, a symlink/non-file database, or migration failure.
    pub async fn open(root: &Path) -> Result<Self, LocalLogError> {
        Self::open_with_archive(root, None).await
    }

    /// Opens a project with an optional external archive shared by the serving process.
    ///
    /// # Errors
    ///
    /// Rejects the same unsafe state as [`Self::open`] and propagates archive configuration or
    /// availability failures.
    pub async fn open_with_archive(
        root: &Path,
        external_archive: Option<LogArchive>,
    ) -> Result<Self, LocalLogError> {
        let (state, paths) = load_local(root).await.map_err(map_state)?;
        let metadata = tokio::fs::symlink_metadata(&paths.observability_database)
            .await
            .map_err(|_| LocalLogError::InvalidState)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
            return Err(LocalLogError::InvalidState);
        }
        let hot: Arc<dyn LogRepository> = Arc::new(
            SqlLogRepository::connect_sqlite(
                &sqlite_url(&paths.observability_database),
                LogRepositoryConfig::LOCAL,
            )
            .await
            .map_err(map_repository)?,
        );
        let archive = match external_archive {
            Some(archive) => archive,
            None => LogArchive::open_filesystem(paths.observability_archive)
                .await
                .map_err(map_repository)?,
        };
        let repository: Arc<dyn LogRepository> =
            Arc::new(TieredLogRepository::new(hot, archive.clone()));
        Ok(Self {
            state,
            repository,
            archive,
        })
    }

    /// Queries only this manager's exact Project/Environment scope.
    ///
    /// # Errors
    ///
    /// Rejects cross-scope or invalid filters and propagates sanitized repository failures.
    pub async fn query(&self, query: &LogQuery) -> Result<LogPage, LocalLogError> {
        if query.scope != self.state.scope() {
            return Err(LocalLogError::InvalidRequest);
        }
        self.repository.query(query).await.map_err(map_repository)
    }

    /// Exact Project/Environment scope owned by this manager.
    #[must_use]
    pub const fn scope(&self) -> EnvironmentScope {
        self.state.scope()
    }

    /// Returns verified immutable archive coverage for this exact Environment.
    ///
    /// # Errors
    ///
    /// Fails closed on unavailable storage, malformed manifests, gaps, or modified segments.
    pub async fn archive_status(&self) -> Result<LogArchiveStatus, LocalLogError> {
        self.archive
            .status(self.state.scope())
            .await
            .map_err(map_repository)
    }

    /// Dry-runs or applies one bounded retention transaction.
    ///
    /// Applying requires the exact Environment ID; dry-run rejects a confirmation so scripts
    /// cannot accidentally change meaning by dropping only the `apply` flag.
    ///
    /// # Errors
    ///
    /// Rejects confirmation mismatch, invalid limits/cutoff, or repository failures.
    pub async fn prune_before(
        &self,
        cutoff: TimestampMicros,
        maximum: u32,
        apply: bool,
        confirmed_environment: Option<EnvironmentId>,
    ) -> Result<PruneResult, LocalLogError> {
        if apply && confirmed_environment != Some(self.state.environment_id)
            || !apply && confirmed_environment.is_some()
        {
            return Err(LocalLogError::InvalidRequest);
        }
        self.repository
            .prune_before(self.state.scope(), cutoff, maximum, !apply)
            .await
            .map_err(map_repository)
    }

    /// Exact Environment owned by this manager.
    #[must_use]
    pub const fn environment_id(&self) -> EnvironmentId {
        self.state.environment_id
    }

    /// Closes the local SQL pool after query/follow use.
    pub async fn close(&self) {
        self.repository.close().await;
    }
}

fn map_state(error: LocalStateError) -> LocalLogError {
    match error {
        LocalStateError::Unavailable => LocalLogError::Unavailable,
        LocalStateError::InvalidPath
        | LocalStateError::InvalidState
        | LocalStateError::Conflict
        | LocalStateError::Corruption => LocalLogError::InvalidState,
    }
}

fn map_repository(error: LogRepositoryError) -> LocalLogError {
    match error {
        LogRepositoryError::InvalidRequest
        | LogRepositoryError::LimitExceeded
        | LogRepositoryError::Unsupported => LocalLogError::InvalidRequest,
        LogRepositoryError::Unavailable => LocalLogError::Unavailable,
        LogRepositoryError::Corruption => LocalLogError::Corruption,
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, net::SocketAddr, str::FromStr};

    use runku_core::{EnvironmentId, EnvironmentScope, ProjectId, WorkspaceRef};
    use runku_observability::{LogCursor, LogQuery};
    use runku_value::TimestampMicros;
    use tempfile::tempdir;

    use super::{LocalLogError, LocalLogManager};
    use crate::initialize_local;

    type TestResult = Result<(), Box<dyn Error>>;

    fn query(scope: EnvironmentScope) -> LogQuery {
        LogQuery {
            scope,
            after: LogCursor::START,
            limit: 10,
            stream: None,
            minimum_level: None,
            function_id: None,
            request_id: None,
            invocation_id: None,
            client_id: None,
            credential_id: None,
            release_id: None,
        }
    }

    #[tokio::test]
    async fn manager_enforces_scope_confirmation_and_safe_database_path() -> TestResult {
        let directory = tempdir()?;
        let (_, paths) = initialize_local(
            directory.path(),
            WorkspaceRef::from_str("default")?,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            TimestampMicros::new(100),
        )
        .await?;
        let manager = LocalLogManager::open(directory.path()).await?;
        assert!(
            manager
                .query(&query(manager.scope()))
                .await?
                .records
                .is_empty()
        );
        assert_eq!(
            manager
                .query(&query(EnvironmentScope::new(
                    ProjectId::generate(),
                    EnvironmentId::generate(),
                )))
                .await,
            Err(LocalLogError::InvalidRequest)
        );
        assert_eq!(
            manager
                .prune_before(
                    TimestampMicros::new(101),
                    10,
                    true,
                    Some(EnvironmentId::generate()),
                )
                .await,
            Err(LocalLogError::InvalidRequest)
        );
        assert_eq!(
            manager
                .prune_before(
                    TimestampMicros::new(101),
                    10,
                    false,
                    Some(manager.environment_id()),
                )
                .await,
            Err(LocalLogError::InvalidRequest)
        );
        manager.close().await;

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let backup = directory.path().join("observability-backup.sqlite3");
            std::fs::rename(&paths.observability_database, &backup)?;
            symlink(&backup, &paths.observability_database)?;
            assert!(matches!(
                LocalLogManager::open(directory.path()).await,
                Err(LocalLogError::InvalidState)
            ));
        }
        Ok(())
    }
}
