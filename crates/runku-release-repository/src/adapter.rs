//! SQL-backed repository implementation shared across the two conformance dialects.

use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId};
use runku_releases::{
    ChannelBinding, ReleaseCommand, ReleaseCommandResult, ReleaseError, ReleaseLifecycle,
    ReleaseRepository, ReleaseRepositoryBackend, ReleaseRepositoryTelemetrySnapshot, ReleaseStatus,
    ServingReleaseEntry, ServingSnapshot, decode_release_manifest,
};
use sha2::{Digest, Sha256};
use sqlx::{
    Any, AnyPool, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

const SCHEMA_VERSION: i64 = 1;
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_release_environments (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, serving_revision BIGINT NOT NULL, default_channel TEXT NULL, PRIMARY KEY(project_id, environment_id))",
    "CREATE TABLE IF NOT EXISTS runku_release_records (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, release_id TEXT NOT NULL, status TEXT NOT NULL, manifest_digest BYTEA NOT NULL, manifest_bytes BYTEA NOT NULL, record_revision BIGINT NOT NULL, created_at_micros BIGINT NOT NULL, updated_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, release_id), FOREIGN KEY(project_id, environment_id) REFERENCES runku_release_environments(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE TABLE IF NOT EXISTS runku_release_channels (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, channel_name TEXT NOT NULL, release_id TEXT NOT NULL, PRIMARY KEY(project_id, environment_id, channel_name), FOREIGN KEY(project_id, environment_id, release_id) REFERENCES runku_release_records(project_id, environment_id, release_id) ON DELETE RESTRICT)",
    "CREATE TABLE IF NOT EXISTS runku_release_operations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, command_digest BYTEA NOT NULL, serving_revision BIGINT NOT NULL, created_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, operation_id), FOREIGN KEY(project_id, environment_id) REFERENCES runku_release_environments(project_id, environment_id) ON DELETE CASCADE)",
];

/// Operational role selected for repository composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryRole {
    /// Local/test `SQLite` repository.
    Local,
    /// Authoritative production `PostgreSQL` repository.
    Production,
}

/// Bounded repository pool/timeouts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepositoryConfig {
    /// Declared composition role.
    pub role: RepositoryRole,
    /// Maximum connection count.
    pub max_connections: u32,
    /// Pool acquisition timeout.
    pub acquire_timeout: Duration,
}

impl RepositoryConfig {
    /// Deterministic local/test configuration.
    pub const LOCAL: Self = Self {
        role: RepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };
    /// Bounded production configuration.
    pub const PRODUCTION: Self = Self {
        role: RepositoryRole::Production,
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
    };
}

#[derive(Debug, Default)]
struct Counters {
    commands: AtomicU64,
    replays: AtomicU64,
    conflicts: AtomicU64,
    snapshots: AtomicU64,
    retryable_errors: AtomicU64,
}

/// SQL implementation selected explicitly as `SQLite` or `PostgreSQL`.
#[derive(Clone, Debug)]
pub struct SqlReleaseRepository {
    pool: AnyPool,
    backend: ReleaseRepositoryBackend,
    counters: Arc<Counters>,
}

impl SqlReleaseRepository {
    /// Connects to a `SQLite` URL for local/test use and applies migrations.
    ///
    /// # Errors
    ///
    /// Rejects Production role and returns stable availability/migration errors.
    pub async fn connect_sqlite(url: &str, config: RepositoryConfig) -> Result<Self, ReleaseError> {
        if config.role == RepositoryRole::Production {
            return Err(ReleaseError::ProductionBackendUnsupported);
        }
        if !url.starts_with("sqlite:") {
            return Err(ReleaseError::Unavailable);
        }
        Self::connect(url, config, ReleaseRepositoryBackend::SQLite).await
    }

    /// Connects to `PostgreSQL` for authoritative use and applies migrations.
    ///
    /// # Errors
    ///
    /// Rejects Local role and returns stable availability/migration errors.
    pub async fn connect_postgres(
        url: &str,
        config: RepositoryConfig,
    ) -> Result<Self, ReleaseError> {
        if config.role != RepositoryRole::Production {
            return Err(ReleaseError::ProductionBackendUnsupported);
        }
        if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            return Err(ReleaseError::Unavailable);
        }
        Self::connect(url, config, ReleaseRepositoryBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: RepositoryConfig,
        backend: ReleaseRepositoryBackend,
    ) -> Result<Self, ReleaseError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
        {
            return Err(ReleaseError::LimitExceeded);
        }
        if backend == ReleaseRepositoryBackend::SQLite && config.max_connections != 1 {
            return Err(ReleaseError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options = AnyConnectOptions::from_str(url).map_err(|_| ReleaseError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _metadata| {
                Box::pin(async move {
                    match backend {
                        ReleaseRepositoryBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        ReleaseRepositoryBackend::PostgreSQL => {
                            connection.execute("SET statement_timeout = '30s'").await?;
                            connection.execute("SET lock_timeout = '5s'").await?;
                            connection
                                .execute("SET idle_in_transaction_session_timeout = '30s'")
                                .await?;
                        }
                    }
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(map_sqlx_error)?;
        if backend == ReleaseRepositoryBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(ReleaseError::Unsupported);
            }
        }
        verify_configuration(&pool, backend).await?;
        migrate(&pool, backend).await?;
        Ok(Self {
            pool,
            backend,
            counters: Arc::new(Counters::default()),
        })
    }

    /// Closes the bounded pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

async fn verify_configuration(
    pool: &AnyPool,
    backend: ReleaseRepositoryBackend,
) -> Result<(), ReleaseError> {
    match backend {
        ReleaseRepositoryBackend::SQLite => {
            let journal = sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            let foreign_keys = sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            let synchronous = sqlx::query_scalar::<_, i64>("PRAGMA synchronous")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            let busy_timeout = sqlx::query_scalar::<_, i64>("PRAGMA busy_timeout")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            if !journal.eq_ignore_ascii_case("wal")
                || foreign_keys != 1
                || synchronous != 2
                || busy_timeout != 5_000
            {
                return Err(ReleaseError::Corruption);
            }
        }
        ReleaseRepositoryBackend::PostgreSQL => {
            let row = sqlx::query(
                "SELECT current_setting('statement_timeout') AS statement_timeout, current_setting('lock_timeout') AS lock_timeout, current_setting('idle_in_transaction_session_timeout') AS idle_timeout",
            )
            .fetch_one(pool)
            .await
            .map_err(map_sqlx_error)?;
            let statement: String = row
                .try_get("statement_timeout")
                .map_err(|_| ReleaseError::Corruption)?;
            let lock: String = row
                .try_get("lock_timeout")
                .map_err(|_| ReleaseError::Corruption)?;
            let idle: String = row
                .try_get("idle_timeout")
                .map_err(|_| ReleaseError::Corruption)?;
            if statement != "30s" || lock != "5s" || idle != "30s" {
                return Err(ReleaseError::Corruption);
            }
        }
    }
    Ok(())
}

#[async_trait]
impl ReleaseRepository for SqlReleaseRepository {
    fn backend(&self) -> ReleaseRepositoryBackend {
        self.backend
    }

    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &ReleaseCommand,
    ) -> Result<ReleaseCommandResult, ReleaseError> {
        let digest = command.digest(scope)?;
        let mut transaction = begin_write(&self.pool, self.backend).await?;
        let result = apply_command(&mut transaction, scope, operation_id, command, digest).await;
        match result {
            Ok(result) => {
                if let Err(error) = transaction.commit().await.map_err(map_commit_error) {
                    self.counters
                        .retryable_errors
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(error);
                }
                if result.replayed {
                    self.counters.replays.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.counters.commands.fetch_add(1, Ordering::Relaxed);
                }
                Ok(result)
            }
            Err(error) => {
                if error == ReleaseError::RepositoryConflict {
                    self.counters.conflicts.fetch_add(1, Ordering::Relaxed);
                }
                if error.retryable() {
                    self.counters
                        .retryable_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn snapshot(&self, scope: EnvironmentScope) -> Result<ServingSnapshot, ReleaseError> {
        let mut transaction = self.pool.begin().await.map_err(map_sqlx_error)?;
        let project = scope.project_id().to_string();
        let environment = scope.environment_id().to_string();
        let env = sqlx::query("SELECT serving_revision, default_channel FROM runku_release_environments WHERE project_id = $1 AND environment_id = $2")
            .bind(&project).bind(&environment).fetch_optional(&mut *transaction).await.map_err(map_sqlx_error)?
            .ok_or(ReleaseError::ReleaseNotFound)?;
        let revision = positive_u64(
            env.try_get("serving_revision")
                .map_err(|_| ReleaseError::Corruption)?,
        )?;
        let default_channel = env
            .try_get::<Option<String>, _>("default_channel")
            .map_err(|_| ReleaseError::Corruption)?
            .map(|value| value.parse().map_err(|_| ReleaseError::Corruption))
            .transpose()?;
        let rows = sqlx::query("SELECT release_id, status, manifest_digest, manifest_bytes FROM runku_release_records WHERE project_id = $1 AND environment_id = $2 ORDER BY release_id")
            .bind(&project).bind(&environment).fetch_all(&mut *transaction).await.map_err(map_sqlx_error)?;
        let mut releases = Vec::with_capacity(rows.len());
        for row in rows {
            let bytes: Vec<u8> = row
                .try_get("manifest_bytes")
                .map_err(|_| ReleaseError::Corruption)?;
            let digest: Vec<u8> = row
                .try_get("manifest_digest")
                .map_err(|_| ReleaseError::Corruption)?;
            let manifest = decode_release_manifest(&bytes)?;
            if digest.as_slice() != manifest.digest()?.as_bytes() {
                return Err(ReleaseError::Corruption);
            }
            let release_id = row
                .try_get::<String, _>("release_id")
                .map_err(|_| ReleaseError::Corruption)?
                .parse()
                .map_err(|_| ReleaseError::Corruption)?;
            if release_id != manifest.release_id || manifest.project_id != scope.project_id() {
                return Err(ReleaseError::Corruption);
            }
            releases.push(ServingReleaseEntry {
                release_id,
                project_id: manifest.project_id,
                manifest_digest: manifest.digest()?,
                artifact: manifest.artifact,
                runtime_version: manifest.runtime_version,
                status: row
                    .try_get::<String, _>("status")
                    .map_err(|_| ReleaseError::Corruption)?
                    .parse()?,
            });
        }
        let rows = sqlx::query("SELECT channel_name, release_id FROM runku_release_channels WHERE project_id = $1 AND environment_id = $2 ORDER BY channel_name")
            .bind(&project).bind(&environment).fetch_all(&mut *transaction).await.map_err(map_sqlx_error)?;
        let channels = rows
            .into_iter()
            .map(|row| {
                Ok(ChannelBinding {
                    channel: row
                        .try_get::<String, _>("channel_name")
                        .map_err(|_| ReleaseError::Corruption)?
                        .parse()
                        .map_err(|_| ReleaseError::Corruption)?,
                    release_id: row
                        .try_get::<String, _>("release_id")
                        .map_err(|_| ReleaseError::Corruption)?
                        .parse()
                        .map_err(|_| ReleaseError::Corruption)?,
                })
            })
            .collect::<Result<Vec<_>, ReleaseError>>()?;
        transaction.rollback().await.map_err(map_sqlx_error)?;
        self.counters.snapshots.fetch_add(1, Ordering::Relaxed);
        ServingSnapshot::new(scope, revision, releases, channels, default_channel)
    }

    async fn manifest(
        &self,
        scope: EnvironmentScope,
        release_id: runku_core::ReleaseId,
    ) -> Result<runku_releases::ReleaseManifestV1, ReleaseError> {
        let row = sqlx::query("SELECT manifest_digest, manifest_bytes FROM runku_release_records WHERE project_id = $1 AND environment_id = $2 AND release_id = $3")
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .bind(release_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?
            .ok_or(ReleaseError::ReleaseNotFound)?;
        let bytes: Vec<u8> = row
            .try_get("manifest_bytes")
            .map_err(|_| ReleaseError::Corruption)?;
        let digest: Vec<u8> = row
            .try_get("manifest_digest")
            .map_err(|_| ReleaseError::Corruption)?;
        let manifest = runku_releases::decode_release_manifest(&bytes)?;
        if manifest.project_id != scope.project_id()
            || manifest.release_id != release_id
            || digest.as_slice() != manifest.digest()?.as_bytes()
        {
            return Err(ReleaseError::Corruption);
        }
        Ok(manifest)
    }

    async fn health(&self) -> Result<(), ReleaseError> {
        sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_error)
    }

    fn telemetry(&self) -> ReleaseRepositoryTelemetrySnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        ReleaseRepositoryTelemetrySnapshot {
            commands: load(&self.counters.commands),
            replays: load(&self.counters.replays),
            conflicts: load(&self.counters.conflicts),
            snapshots: load(&self.counters.snapshots),
            retryable_errors: load(&self.counters.retryable_errors),
            pool_size: self.pool.size(),
            pool_idle: u32::try_from(self.pool.num_idle()).unwrap_or(u32::MAX),
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn apply_command(
    transaction: &mut Transaction<'static, Any>,
    scope: EnvironmentScope,
    operation_id: OperationId,
    command: &ReleaseCommand,
    digest: [u8; 32],
) -> Result<ReleaseCommandResult, ReleaseError> {
    let project = scope.project_id().to_string();
    let environment = scope.environment_id().to_string();
    if let Some(row) = sqlx::query("SELECT command_digest, serving_revision FROM runku_release_operations WHERE project_id = $1 AND environment_id = $2 AND operation_id = $3")
        .bind(&project).bind(&environment).bind(operation_id.to_string())
        .fetch_optional(&mut **transaction).await.map_err(map_sqlx_error)? {
        let stored: Vec<u8> = row.try_get("command_digest").map_err(|_| ReleaseError::Corruption)?;
        if stored != digest { return Err(ReleaseError::OperationIdReused); }
        return Ok(ReleaseCommandResult { serving_revision: positive_u64(row.try_get("serving_revision").map_err(|_| ReleaseError::Corruption)?)?, replayed: true });
    }
    sqlx::query("INSERT INTO runku_release_environments(project_id, environment_id, serving_revision, default_channel) VALUES ($1, $2, 0, NULL) ON CONFLICT(project_id, environment_id) DO NOTHING")
        .bind(&project).bind(&environment).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
    let current_revision: i64 = sqlx::query_scalar("SELECT serving_revision FROM runku_release_environments WHERE project_id = $1 AND environment_id = $2")
        .bind(&project).bind(&environment).fetch_one(&mut **transaction).await.map_err(map_sqlx_error)?;
    let next_revision = current_revision
        .checked_add(1)
        .ok_or(ReleaseError::LimitExceeded)?;
    let now = now_micros()?;

    match command {
        ReleaseCommand::Register { manifest_bytes } => {
            let manifest = decode_release_manifest(manifest_bytes)?;
            let manifest_digest = manifest.digest()?;
            let result = sqlx::query("INSERT INTO runku_release_records(project_id, environment_id, release_id, status, manifest_digest, manifest_bytes, record_revision, created_at_micros, updated_at_micros) VALUES ($1, $2, $3, 'created', $4, $5, 1, $6, $7) ON CONFLICT DO NOTHING")
                .bind(&project).bind(&environment).bind(manifest.release_id.to_string()).bind(manifest_digest.as_bytes().as_slice())
                .bind(manifest_bytes).bind(now).bind(now).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
            if result.rows_affected() != 1 {
                return Err(ReleaseError::RepositoryConflict);
            }
        }
        ReleaseCommand::Transition {
            release_id,
            expected,
            next,
        } => {
            ReleaseLifecycle::advance(*expected, *next)?;
            let result = sqlx::query("UPDATE runku_release_records SET status = $1, record_revision = record_revision + 1, updated_at_micros = $2 WHERE project_id = $3 AND environment_id = $4 AND release_id = $5 AND status = $6")
                .bind(next.as_str()).bind(now).bind(&project).bind(&environment).bind(release_id.to_string()).bind(expected.as_str())
                .execute(&mut **transaction).await.map_err(map_sqlx_error)?;
            if result.rows_affected() != 1 {
                return Err(ReleaseError::RepositoryConflict);
            }
        }
        ReleaseCommand::SetChannel {
            channel,
            expected_release,
            target_release,
        } => {
            let current = sqlx::query_scalar::<_, String>("SELECT release_id FROM runku_release_channels WHERE project_id = $1 AND environment_id = $2 AND channel_name = $3")
                .bind(&project).bind(&environment).bind(channel.as_str()).fetch_optional(&mut **transaction).await.map_err(map_sqlx_error)?
                .map(|value| value.parse().map_err(|_| ReleaseError::Corruption)).transpose()?;
            if &current != expected_release {
                return Err(ReleaseError::RepositoryConflict);
            }
            if let Some(target) = target_release {
                let status = sqlx::query_scalar::<_, String>("SELECT status FROM runku_release_records WHERE project_id = $1 AND environment_id = $2 AND release_id = $3")
                    .bind(&project).bind(&environment).bind(target.to_string()).fetch_optional(&mut **transaction).await.map_err(map_sqlx_error)?
                    .ok_or(ReleaseError::RepositoryConflict)?.parse::<ReleaseStatus>()?;
                if !matches!(status, ReleaseStatus::Servable | ReleaseStatus::Active) {
                    return Err(ReleaseError::RepositoryConflict);
                }
            }
            if target_release.is_none() {
                let default_channel: Option<String> = sqlx::query_scalar(
                    "SELECT default_channel FROM runku_release_environments WHERE project_id = $1 AND environment_id = $2",
                )
                .bind(&project)
                .bind(&environment)
                .fetch_one(&mut **transaction)
                .await
                .map_err(map_sqlx_error)?;
                if default_channel.as_deref() == Some(channel.as_str()) {
                    return Err(ReleaseError::RepositoryConflict);
                }
            }
            sqlx::query("DELETE FROM runku_release_channels WHERE project_id = $1 AND environment_id = $2 AND channel_name = $3")
                .bind(&project).bind(&environment).bind(channel.as_str()).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
            if let Some(target) = target_release {
                sqlx::query("INSERT INTO runku_release_channels(project_id, environment_id, channel_name, release_id) VALUES ($1, $2, $3, $4)")
                    .bind(&project).bind(&environment).bind(channel.as_str()).bind(target.to_string()).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
            }
            if let Some(old) = current {
                refresh_activity(transaction, &project, &environment, old, now).await?;
            }
            if let Some(target) = target_release {
                refresh_activity(transaction, &project, &environment, *target, now).await?;
            }
        }
        ReleaseCommand::SetDefaultChannel {
            expected_channel,
            target_channel,
        } => {
            let current: Option<String> = sqlx::query_scalar("SELECT default_channel FROM runku_release_environments WHERE project_id = $1 AND environment_id = $2")
                .bind(&project).bind(&environment).fetch_one(&mut **transaction).await.map_err(map_sqlx_error)?;
            let current = current
                .map(|value| value.parse().map_err(|_| ReleaseError::Corruption))
                .transpose()?;
            if &current != expected_channel {
                return Err(ReleaseError::RepositoryConflict);
            }
            if let Some(target) = target_channel {
                let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM runku_release_channels WHERE project_id = $1 AND environment_id = $2 AND channel_name = $3")
                    .bind(&project).bind(&environment).bind(target.as_str()).fetch_optional(&mut **transaction).await.map_err(map_sqlx_error)?;
                if exists.is_none() {
                    return Err(ReleaseError::RepositoryConflict);
                }
            }
            sqlx::query("UPDATE runku_release_environments SET default_channel = $1 WHERE project_id = $2 AND environment_id = $3")
                .bind(target_channel.as_ref().map(runku_channel_str)).bind(&project).bind(&environment).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
        }
    }
    let updated = sqlx::query("UPDATE runku_release_environments SET serving_revision = $1 WHERE project_id = $2 AND environment_id = $3 AND serving_revision = $4")
        .bind(next_revision).bind(&project).bind(&environment).bind(current_revision).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
    if updated.rows_affected() != 1 {
        return Err(ReleaseError::RepositoryConflict);
    }
    sqlx::query("INSERT INTO runku_release_operations(project_id, environment_id, operation_id, command_digest, serving_revision, created_at_micros) VALUES ($1, $2, $3, $4, $5, $6)")
        .bind(&project).bind(&environment).bind(operation_id.to_string()).bind(digest.as_slice()).bind(next_revision).bind(now)
        .execute(&mut **transaction).await.map_err(map_sqlx_error)?;
    Ok(ReleaseCommandResult {
        serving_revision: positive_u64(next_revision)?,
        replayed: false,
    })
}

fn runku_channel_str(channel: &runku_core::ChannelName) -> &str {
    channel.as_str()
}

async fn refresh_activity(
    transaction: &mut Transaction<'static, Any>,
    project: &str,
    environment: &str,
    release_id: runku_core::ReleaseId,
    now: i64,
) -> Result<(), ReleaseError> {
    let references: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runku_release_channels WHERE project_id = $1 AND environment_id = $2 AND release_id = $3")
        .bind(project).bind(environment).bind(release_id.to_string()).fetch_one(&mut **transaction).await.map_err(map_sqlx_error)?;
    let current: String = sqlx::query_scalar("SELECT status FROM runku_release_records WHERE project_id = $1 AND environment_id = $2 AND release_id = $3")
        .bind(project).bind(environment).bind(release_id.to_string()).fetch_one(&mut **transaction).await.map_err(map_sqlx_error)?;
    let next = ReleaseLifecycle::with_channel_reference(current.parse()?, references > 0)?;
    sqlx::query("UPDATE runku_release_records SET status = $1, record_revision = record_revision + 1, updated_at_micros = $2 WHERE project_id = $3 AND environment_id = $4 AND release_id = $5")
        .bind(next.as_str()).bind(now).bind(project).bind(environment).bind(release_id.to_string())
        .execute(&mut **transaction).await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn migrate(pool: &AnyPool, backend: ReleaseRepositoryBackend) -> Result<(), ReleaseError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_release_schema_migrations(version BIGINT PRIMARY KEY, checksum BYTEA NOT NULL, applied_at_micros BIGINT NOT NULL)")
        .execute(pool).await.map_err(|_| ReleaseError::Unavailable)?;
    let mut transaction = begin_write(pool, backend).await?;
    if backend == ReleaseRepositoryBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(7_224_856_022_i64)
            .execute(&mut *transaction)
            .await
            .map_err(|_| ReleaseError::Unavailable)?;
    }
    let checksum = schema_checksum();
    if let Some(row) =
        sqlx::query("SELECT checksum FROM runku_release_schema_migrations WHERE version = $1")
            .bind(SCHEMA_VERSION)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| ReleaseError::Unavailable)?
    {
        let stored: Vec<u8> = row
            .try_get("checksum")
            .map_err(|_| ReleaseError::Corruption)?;
        if stored != checksum {
            return Err(ReleaseError::Corruption);
        }
        return transaction.commit().await.map_err(map_sqlx_error);
    }
    for statement in SCHEMA {
        sqlx::query(*statement)
            .execute(&mut *transaction)
            .await
            .map_err(|_| ReleaseError::Unavailable)?;
    }
    sqlx::query("INSERT INTO runku_release_schema_migrations(version, checksum, applied_at_micros) VALUES ($1, $2, $3)")
        .bind(SCHEMA_VERSION).bind(checksum.as_slice()).bind(now_micros()?).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
    transaction.commit().await.map_err(map_sqlx_error)
}

fn schema_checksum() -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_RELEASE_REPOSITORY_SCHEMA_V1");
    for statement in SCHEMA {
        digest.update(statement.as_bytes());
        digest.update([0]);
    }
    digest.finalize().into()
}

async fn begin_write(
    pool: &AnyPool,
    backend: ReleaseRepositoryBackend,
) -> Result<Transaction<'static, Any>, ReleaseError> {
    let statement = match backend {
        ReleaseRepositoryBackend::SQLite => "BEGIN IMMEDIATE",
        ReleaseRepositoryBackend::PostgreSQL => "BEGIN ISOLATION LEVEL SERIALIZABLE",
    };
    pool.begin_with(statement).await.map_err(map_sqlx_error)
}

fn now_micros() -> Result<i64, ReleaseError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ReleaseError::Internal)?;
    i64::try_from(duration.as_micros()).map_err(|_| ReleaseError::Internal)
}

fn positive_u64(value: i64) -> Result<u64, ReleaseError> {
    if value <= 0 {
        return Err(ReleaseError::Corruption);
    }
    u64::try_from(value).map_err(|_| ReleaseError::Corruption)
}

fn map_sqlx_error(error: sqlx::Error) -> ReleaseError {
    match error {
        sqlx::Error::PoolTimedOut => ReleaseError::Busy,
        sqlx::Error::PoolClosed | sqlx::Error::Io(_) | sqlx::Error::Tls(_) => {
            ReleaseError::Unavailable
        }
        sqlx::Error::Database(database)
            if database
                .code()
                .is_some_and(|code| matches!(code.as_ref(), "40001" | "40P01" | "5" | "6")) =>
        {
            ReleaseError::Busy
        }
        _ => ReleaseError::Internal,
    }
}

fn map_commit_error(error: sqlx::Error) -> ReleaseError {
    match map_sqlx_error(error) {
        ReleaseError::Unavailable | ReleaseError::Internal => ReleaseError::ResultUncertain,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::map_commit_error;
    use runku_releases::ReleaseError;

    #[test]
    fn disconnected_commit_is_result_uncertain_and_retryable() {
        let error = map_commit_error(sqlx::Error::PoolClosed);
        assert_eq!(error, ReleaseError::ResultUncertain);
        assert!(error.retryable());
    }
}
