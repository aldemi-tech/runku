//! Checksum-protected SQLite/PostgreSQL Development Access repository.

use std::{
    fmt::Write as _,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{DevelopmentCredentialId, EnvironmentScope};
use runku_value::TimestampMicros;
use sha2::{Digest as _, Sha256};
use sqlx::{
    Any, AnyPool, Executor as _, Row as _, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

use crate::{
    DevelopmentAccessBackend, DevelopmentAccessError, DevelopmentAccessRepository,
    DevelopmentAccessResolver, DevelopmentAccessTelemetrySnapshot, DevelopmentCredential,
    DevelopmentCredentialStatus, DevelopmentIdentity, DevelopmentKeyCrypto, DevelopmentKeyDigest,
    DevelopmentLifecycleResult, ParsedDevelopmentKey,
};

const SCHEMA_VERSION: i64 = 1;
const MIGRATION_LOCK: i64 = 5_812_121_011;
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_development_access_environments (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, configuration_revision BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id))",
    "CREATE TABLE IF NOT EXISTS runku_development_access_credentials (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, credential_id TEXT NOT NULL, actor TEXT NOT NULL, label TEXT NOT NULL, digest BYTEA NOT NULL, status TEXT NOT NULL, created_at_micros BIGINT NOT NULL, expires_at_micros BIGINT NULL, revoked_at_micros BIGINT NULL, deleted_at_micros BIGINT NULL, PRIMARY KEY(project_id, environment_id, credential_id), FOREIGN KEY(project_id, environment_id) REFERENCES runku_development_access_environments(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE INDEX IF NOT EXISTS runku_development_credentials_by_scope ON runku_development_access_credentials(project_id, environment_id, status, credential_id)",
];

/// Required deployment role for Development Access persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentAccessRepositoryRole {
    /// Local/test `SQLite` repository.
    Local,
    /// Authoritative shared `PostgreSQL` repository.
    Authoritative,
}

/// Bounded pool and timeout policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DevelopmentAccessRepositoryConfig {
    /// Declared deployment role.
    pub role: DevelopmentAccessRepositoryRole,
    /// Maximum pooled connections.
    pub max_connections: u32,
    /// Maximum pool admission wait.
    pub acquire_timeout: Duration,
}

impl DevelopmentAccessRepositoryConfig {
    /// Deterministic local `SQLite` policy.
    pub const LOCAL: Self = Self {
        role: DevelopmentAccessRepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };
    /// Bounded authoritative `PostgreSQL` policy.
    pub const AUTHORITATIVE: Self = Self {
        role: DevelopmentAccessRepositoryRole::Authoritative,
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
    };
}

#[derive(Debug, Default)]
struct Counters {
    credentials_created: AtomicU64,
    create_replays: AtomicU64,
    resolutions: AtomicU64,
    resolution_failures: AtomicU64,
    credentials_revoked: AtomicU64,
    credentials_deleted: AtomicU64,
    retryable_errors: AtomicU64,
}

/// Durable SQL repository with equivalent `SQLite` and `PostgreSQL` semantics.
#[derive(Clone, Debug)]
pub struct SqlDevelopmentAccessRepository {
    pool: AnyPool,
    backend: DevelopmentAccessBackend,
    counters: Arc<Counters>,
}

impl SqlDevelopmentAccessRepository {
    /// Opens local `SQLite` and applies checksum-protected migrations.
    ///
    /// # Errors
    ///
    /// Rejects incompatible roles, pool policy, storage configuration, or migration drift.
    pub async fn connect_sqlite(
        url: &str,
        config: DevelopmentAccessRepositoryConfig,
    ) -> Result<Self, DevelopmentAccessError> {
        if config.role != DevelopmentAccessRepositoryRole::Local || !url.starts_with("sqlite:") {
            return Err(DevelopmentAccessError::Unsupported);
        }
        Self::connect(url, config, DevelopmentAccessBackend::SQLite).await
    }

    /// Opens authoritative `PostgreSQL` 16+ and applies checksum-protected migrations.
    ///
    /// # Errors
    ///
    /// Rejects incompatible roles, old servers, pool policy, or migration drift.
    pub async fn connect_postgres(
        url: &str,
        config: DevelopmentAccessRepositoryConfig,
    ) -> Result<Self, DevelopmentAccessError> {
        if config.role != DevelopmentAccessRepositoryRole::Authoritative
            || !(url.starts_with("postgres://") || url.starts_with("postgresql://"))
        {
            return Err(DevelopmentAccessError::Unsupported);
        }
        Self::connect(url, config, DevelopmentAccessBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: DevelopmentAccessRepositoryConfig,
        backend: DevelopmentAccessBackend,
    ) -> Result<Self, DevelopmentAccessError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || backend == DevelopmentAccessBackend::SQLite && config.max_connections != 1
        {
            return Err(DevelopmentAccessError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options =
            AnyConnectOptions::from_str(url).map_err(|_| DevelopmentAccessError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    match backend {
                        DevelopmentAccessBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        DevelopmentAccessBackend::PostgreSQL => {
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
        if backend == DevelopmentAccessBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(DevelopmentAccessError::Unsupported);
            }
        }
        if let Err(error) = verify_configuration(&pool, backend).await {
            pool.close().await;
            return Err(error);
        }
        if let Err(error) = migrate(&pool, backend).await {
            pool.close().await;
            return Err(error);
        }
        Ok(Self {
            pool,
            backend,
            counters: Arc::new(Counters::default()),
        })
    }

    fn track<T>(
        &self,
        result: Result<T, DevelopmentAccessError>,
    ) -> Result<T, DevelopmentAccessError> {
        if result.as_ref().is_err_and(|error| error.retryable()) {
            self.counters
                .retryable_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

#[async_trait]
impl DevelopmentAccessRepository for SqlDevelopmentAccessRepository {
    fn backend(&self) -> DevelopmentAccessBackend {
        self.backend
    }

    async fn create_credential(
        &self,
        credential: &DevelopmentCredential,
    ) -> Result<bool, DevelopmentAccessError> {
        credential.validate()?;
        let result = create_credential(&self.pool, self.backend, credential).await;
        match result.as_ref() {
            Ok(true) => {
                self.counters
                    .credentials_created
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {
                self.counters.create_replays.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {}
        }
        self.track(result)
    }

    async fn get_credential(
        &self,
        scope: EnvironmentScope,
        id: DevelopmentCredentialId,
    ) -> Result<Option<DevelopmentCredential>, DevelopmentAccessError> {
        self.track(load_credential(&self.pool, scope, id).await)
    }

    async fn list_credentials(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Vec<DevelopmentCredential>, DevelopmentAccessError> {
        self.track(list_credentials(&self.pool, scope).await)
    }

    async fn revoke_credential(
        &self,
        scope: EnvironmentScope,
        id: DevelopmentCredentialId,
        revoked_at: TimestampMicros,
    ) -> Result<DevelopmentLifecycleResult, DevelopmentAccessError> {
        let result = transition_credential(
            &self.pool,
            self.backend,
            scope,
            id,
            revoked_at,
            DevelopmentCredentialStatus::Revoked,
        )
        .await;
        if matches!(result, Ok(DevelopmentLifecycleResult::Applied)) {
            self.counters
                .credentials_revoked
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn delete_credential(
        &self,
        scope: EnvironmentScope,
        id: DevelopmentCredentialId,
        deleted_at: TimestampMicros,
    ) -> Result<DevelopmentLifecycleResult, DevelopmentAccessError> {
        let result = transition_credential(
            &self.pool,
            self.backend,
            scope,
            id,
            deleted_at,
            DevelopmentCredentialStatus::Deleted,
        )
        .await;
        if matches!(result, Ok(DevelopmentLifecycleResult::Applied)) {
            self.counters
                .credentials_deleted
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn configuration_revision(
        &self,
        scope: EnvironmentScope,
    ) -> Result<u64, DevelopmentAccessError> {
        self.track(configuration_revision(&self.pool, scope).await)
    }

    async fn health(&self) -> Result<(), DevelopmentAccessError> {
        self.track(
            sqlx::query_scalar::<_, i64>("SELECT 1")
                .fetch_one(&self.pool)
                .await
                .map(|_| ())
                .map_err(map_sqlx_error),
        )
    }

    fn telemetry(&self) -> DevelopmentAccessTelemetrySnapshot {
        DevelopmentAccessTelemetrySnapshot {
            credentials_created: self.counters.credentials_created.load(Ordering::Relaxed),
            create_replays: self.counters.create_replays.load(Ordering::Relaxed),
            resolutions: self.counters.resolutions.load(Ordering::Relaxed),
            resolution_failures: self.counters.resolution_failures.load(Ordering::Relaxed),
            credentials_revoked: self.counters.credentials_revoked.load(Ordering::Relaxed),
            credentials_deleted: self.counters.credentials_deleted.load(Ordering::Relaxed),
            retryable_errors: self.counters.retryable_errors.load(Ordering::Relaxed),
        }
    }

    async fn close(&self) {
        self.pool.close().await;
    }
}

#[async_trait]
impl DevelopmentAccessResolver for SqlDevelopmentAccessRepository {
    async fn resolve_key(
        &self,
        scope: EnvironmentScope,
        key: &ParsedDevelopmentKey,
        crypto: &DevelopmentKeyCrypto,
        now: TimestampMicros,
    ) -> Result<DevelopmentIdentity, DevelopmentAccessError> {
        let result = resolve_key(&self.pool, scope, key, crypto, now).await;
        match result.as_ref() {
            Ok(_) => {
                self.counters.resolutions.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.counters
                    .resolution_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        self.track(result)
    }
}

async fn create_credential(
    pool: &AnyPool,
    backend: DevelopmentAccessBackend,
    credential: &DevelopmentCredential,
) -> Result<bool, DevelopmentAccessError> {
    let mut tx = begin_write(pool, backend).await?;
    ensure_environment(&mut tx, credential.scope).await?;
    let result = sqlx::query("INSERT INTO runku_development_access_credentials (project_id, environment_id, credential_id, actor, label, digest, status, created_at_micros, expires_at_micros, revoked_at_micros, deleted_at_micros) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT(project_id, environment_id, credential_id) DO NOTHING")
        .bind(credential.scope.project_id().to_string())
        .bind(credential.scope.environment_id().to_string())
        .bind(credential.id.to_string())
        .bind(credential.actor.as_str())
        .bind(credential.label.as_str())
        .bind(credential.digest.as_bytes().to_vec())
        .bind(credential.status.as_str())
        .bind(credential.created_at.get())
        .bind(credential.expires_at.map(TimestampMicros::get))
        .bind(credential.revoked_at.map(TimestampMicros::get))
        .bind(credential.deleted_at.map(TimestampMicros::get))
        .execute(&mut *tx).await.map_err(map_sqlx_error)?;
    if result.rows_affected() == 0 {
        let existing = load_credential_tx(&mut tx, credential.scope, credential.id)
            .await?
            .ok_or(DevelopmentAccessError::Corruption)?;
        if existing != *credential {
            return Err(DevelopmentAccessError::Conflict);
        }
        tx.commit().await.map_err(map_commit_error)?;
        return Ok(false);
    }
    bump_revision(&mut tx, credential.scope).await?;
    tx.commit().await.map_err(map_commit_error)?;
    Ok(true)
}

async fn resolve_key(
    pool: &AnyPool,
    scope: EnvironmentScope,
    key: &ParsedDevelopmentKey,
    crypto: &DevelopmentKeyCrypto,
    now: TimestampMicros,
) -> Result<DevelopmentIdentity, DevelopmentAccessError> {
    if now.get() < 0 {
        return Err(DevelopmentAccessError::InvalidInput);
    }
    let row = sqlx::query("SELECT k.actor, k.digest, k.status, k.expires_at_micros, e.configuration_revision FROM runku_development_access_credentials k JOIN runku_development_access_environments e ON e.project_id = k.project_id AND e.environment_id = k.environment_id WHERE k.project_id = $1 AND k.environment_id = $2 AND k.credential_id = $3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .bind(key.credential_id().to_string()).fetch_optional(pool).await.map_err(map_sqlx_error)?
        .ok_or(DevelopmentAccessError::InvalidCredential)?;
    let status = DevelopmentCredentialStatus::parse(
        row.try_get("status")
            .map_err(|_| DevelopmentAccessError::Corruption)?,
    )?;
    let digest = decode_digest(
        row.try_get("digest")
            .map_err(|_| DevelopmentAccessError::Corruption)?,
    )?;
    let expires_at: Option<i64> = row
        .try_get("expires_at_micros")
        .map_err(|_| DevelopmentAccessError::Corruption)?;
    if status != DevelopmentCredentialStatus::Active
        || expires_at.is_some_and(|expires| now.get() >= expires)
        || !crypto.verify(key.key(), digest)
    {
        return Err(DevelopmentAccessError::InvalidCredential);
    }
    let revision: i64 = row
        .try_get("configuration_revision")
        .map_err(|_| DevelopmentAccessError::Corruption)?;
    Ok(DevelopmentIdentity {
        scope,
        credential_id: key.credential_id(),
        actor: parse_text(&row, "actor")?,
        configuration_revision: u64::try_from(revision)
            .map_err(|_| DevelopmentAccessError::Corruption)?,
    })
}

async fn transition_credential(
    pool: &AnyPool,
    backend: DevelopmentAccessBackend,
    scope: EnvironmentScope,
    id: DevelopmentCredentialId,
    at: TimestampMicros,
    target: DevelopmentCredentialStatus,
) -> Result<DevelopmentLifecycleResult, DevelopmentAccessError> {
    if at.get() < 0 || target == DevelopmentCredentialStatus::Active {
        return Err(DevelopmentAccessError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let existing = load_credential_tx(&mut tx, scope, id)
        .await?
        .ok_or(DevelopmentAccessError::NotFound)?;
    if at < existing.created_at {
        return Err(DevelopmentAccessError::InvalidInput);
    }
    match (existing.status, target) {
        (DevelopmentCredentialStatus::Active, DevelopmentCredentialStatus::Revoked) => {
            sqlx::query("UPDATE runku_development_access_credentials SET status = 'revoked', revoked_at_micros = $1 WHERE project_id = $2 AND environment_id = $3 AND credential_id = $4 AND status = 'active'")
                .bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
                .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }
        (
            DevelopmentCredentialStatus::Revoked | DevelopmentCredentialStatus::Deleted,
            DevelopmentCredentialStatus::Revoked,
        )
        | (DevelopmentCredentialStatus::Deleted, DevelopmentCredentialStatus::Deleted) => {
            tx.commit().await.map_err(map_commit_error)?;
            return Ok(DevelopmentLifecycleResult::Replayed);
        }
        (DevelopmentCredentialStatus::Revoked, DevelopmentCredentialStatus::Deleted) => {
            if existing.revoked_at.is_none_or(|revoked| at < revoked) {
                return Err(DevelopmentAccessError::InvalidInput);
            }
            sqlx::query("UPDATE runku_development_access_credentials SET status = 'deleted', deleted_at_micros = $1 WHERE project_id = $2 AND environment_id = $3 AND credential_id = $4 AND status = 'revoked'")
                .bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
                .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }
        (DevelopmentCredentialStatus::Active, DevelopmentCredentialStatus::Deleted) => {
            return Err(DevelopmentAccessError::Conflict);
        }
        (_, DevelopmentCredentialStatus::Active) => {
            return Err(DevelopmentAccessError::InvalidInput);
        }
    }
    bump_revision(&mut tx, scope).await?;
    tx.commit().await.map_err(map_commit_error)?;
    Ok(DevelopmentLifecycleResult::Applied)
}

async fn load_credential(
    pool: &AnyPool,
    scope: EnvironmentScope,
    id: DevelopmentCredentialId,
) -> Result<Option<DevelopmentCredential>, DevelopmentAccessError> {
    let row = sqlx::query("SELECT credential_id, actor, label, digest, status, created_at_micros, expires_at_micros, revoked_at_micros, deleted_at_micros FROM runku_development_access_credentials WHERE project_id = $1 AND environment_id = $2 AND credential_id = $3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
        .fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_credential_row(scope, &row))
        .transpose()
}

async fn load_credential_tx(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    id: DevelopmentCredentialId,
) -> Result<Option<DevelopmentCredential>, DevelopmentAccessError> {
    let row = sqlx::query("SELECT credential_id, actor, label, digest, status, created_at_micros, expires_at_micros, revoked_at_micros, deleted_at_micros FROM runku_development_access_credentials WHERE project_id = $1 AND environment_id = $2 AND credential_id = $3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
        .fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_credential_row(scope, &row))
        .transpose()
}

async fn list_credentials(
    pool: &AnyPool,
    scope: EnvironmentScope,
) -> Result<Vec<DevelopmentCredential>, DevelopmentAccessError> {
    let rows = sqlx::query("SELECT credential_id, actor, label, digest, status, created_at_micros, expires_at_micros, revoked_at_micros, deleted_at_micros FROM runku_development_access_credentials WHERE project_id = $1 AND environment_id = $2 AND status <> 'deleted' ORDER BY credential_id")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .fetch_all(pool).await.map_err(map_sqlx_error)?;
    rows.iter()
        .map(|row| decode_credential_row(scope, row))
        .collect()
}

fn decode_credential_row(
    scope: EnvironmentScope,
    row: &sqlx::any::AnyRow,
) -> Result<DevelopmentCredential, DevelopmentAccessError> {
    let credential = DevelopmentCredential {
        id: parse_text(row, "credential_id")?,
        scope,
        actor: parse_text(row, "actor")?,
        label: parse_text(row, "label")?,
        digest: decode_digest(
            row.try_get("digest")
                .map_err(|_| DevelopmentAccessError::Corruption)?,
        )?,
        status: DevelopmentCredentialStatus::parse(
            row.try_get("status")
                .map_err(|_| DevelopmentAccessError::Corruption)?,
        )?,
        created_at: required_time(row, "created_at_micros")?,
        expires_at: optional_time(row, "expires_at_micros")?,
        revoked_at: optional_time(row, "revoked_at_micros")?,
        deleted_at: optional_time(row, "deleted_at_micros")?,
    };
    credential
        .validate()
        .map_err(|_| DevelopmentAccessError::Corruption)?;
    Ok(credential)
}

async fn configuration_revision(
    pool: &AnyPool,
    scope: EnvironmentScope,
) -> Result<u64, DevelopmentAccessError> {
    let revision = sqlx::query_scalar::<_, i64>("SELECT configuration_revision FROM runku_development_access_environments WHERE project_id = $1 AND environment_id = $2")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .fetch_optional(pool).await.map_err(map_sqlx_error)?.unwrap_or(0);
    u64::try_from(revision).map_err(|_| DevelopmentAccessError::Corruption)
}

async fn ensure_environment(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
) -> Result<(), DevelopmentAccessError> {
    sqlx::query("INSERT INTO runku_development_access_environments (project_id, environment_id, configuration_revision) VALUES ($1, $2, 0) ON CONFLICT(project_id, environment_id) DO NOTHING")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .execute(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn bump_revision(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
) -> Result<(), DevelopmentAccessError> {
    let affected = sqlx::query("UPDATE runku_development_access_environments SET configuration_revision = configuration_revision + 1 WHERE project_id = $1 AND environment_id = $2")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .execute(&mut **tx).await.map_err(map_sqlx_error)?.rows_affected();
    if affected != 1 {
        return Err(DevelopmentAccessError::Corruption);
    }
    Ok(())
}

async fn begin_write(
    pool: &AnyPool,
    backend: DevelopmentAccessBackend,
) -> Result<Transaction<'_, Any>, DevelopmentAccessError> {
    let statement = match backend {
        DevelopmentAccessBackend::SQLite => "BEGIN IMMEDIATE",
        DevelopmentAccessBackend::PostgreSQL => "BEGIN ISOLATION LEVEL SERIALIZABLE",
    };
    pool.begin_with(statement).await.map_err(map_sqlx_error)
}

async fn verify_configuration(
    pool: &AnyPool,
    backend: DevelopmentAccessBackend,
) -> Result<(), DevelopmentAccessError> {
    match backend {
        DevelopmentAccessBackend::SQLite => {
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
                return Err(DevelopmentAccessError::Corruption);
            }
        }
        DevelopmentAccessBackend::PostgreSQL => {
            let row = sqlx::query("SELECT current_setting('statement_timeout') AS statement_timeout, current_setting('lock_timeout') AS lock_timeout, current_setting('idle_in_transaction_session_timeout') AS idle_timeout")
                .fetch_one(pool).await.map_err(map_sqlx_error)?;
            let statement: String = row
                .try_get("statement_timeout")
                .map_err(|_| DevelopmentAccessError::Corruption)?;
            let lock: String = row
                .try_get("lock_timeout")
                .map_err(|_| DevelopmentAccessError::Corruption)?;
            let idle: String = row
                .try_get("idle_timeout")
                .map_err(|_| DevelopmentAccessError::Corruption)?;
            if statement != "30s" || lock != "5s" || idle != "30s" {
                return Err(DevelopmentAccessError::Corruption);
            }
        }
    }
    Ok(())
}

async fn migrate(
    pool: &AnyPool,
    backend: DevelopmentAccessBackend,
) -> Result<(), DevelopmentAccessError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_development_access_migrations (version BIGINT PRIMARY KEY, checksum TEXT NOT NULL)")
        .execute(pool).await.map_err(map_sqlx_error)?;
    let mut tx = begin_write(pool, backend).await?;
    if backend == DevelopmentAccessBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(MIGRATION_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
    }
    let checksum = migration_checksum();
    if let Some(existing) = sqlx::query_scalar::<_, String>(
        "SELECT checksum FROM runku_development_access_migrations WHERE version = $1",
    )
    .bind(SCHEMA_VERSION)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?
    {
        if existing != checksum {
            return Err(DevelopmentAccessError::Corruption);
        }
        return tx.commit().await.map_err(map_commit_error);
    }
    for statement in SCHEMA {
        tx.execute(*statement).await.map_err(map_sqlx_error)?;
    }
    sqlx::query(
        "INSERT INTO runku_development_access_migrations (version, checksum) VALUES ($1, $2)",
    )
    .bind(SCHEMA_VERSION)
    .bind(checksum)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_commit_error)
}

fn migration_checksum() -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"runku-development-access-schema-v1\0");
    for statement in SCHEMA {
        hasher.update(statement.as_bytes());
        hasher.update([0]);
    }
    let mut output = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn parse_text<T: FromStr>(
    row: &sqlx::any::AnyRow,
    field: &str,
) -> Result<T, DevelopmentAccessError> {
    let value: String = row
        .try_get(field)
        .map_err(|_| DevelopmentAccessError::Corruption)?;
    value
        .parse()
        .map_err(|_| DevelopmentAccessError::Corruption)
}

fn required_time(
    row: &sqlx::any::AnyRow,
    field: &str,
) -> Result<TimestampMicros, DevelopmentAccessError> {
    row.try_get::<i64, _>(field)
        .map(TimestampMicros::new)
        .map_err(|_| DevelopmentAccessError::Corruption)
}

fn optional_time(
    row: &sqlx::any::AnyRow,
    field: &str,
) -> Result<Option<TimestampMicros>, DevelopmentAccessError> {
    row.try_get::<Option<i64>, _>(field)
        .map(|value| value.map(TimestampMicros::new))
        .map_err(|_| DevelopmentAccessError::Corruption)
}

fn decode_digest(bytes: Vec<u8>) -> Result<DevelopmentKeyDigest, DevelopmentAccessError> {
    Ok(DevelopmentKeyDigest::from_bytes(
        bytes
            .try_into()
            .map_err(|_| DevelopmentAccessError::Corruption)?,
    ))
}

fn map_commit_error(error: sqlx::Error) -> DevelopmentAccessError {
    match error {
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_) => {
            DevelopmentAccessError::ResultUncertain
        }
        other => map_sqlx_error(other),
    }
}

fn map_sqlx_error(error: sqlx::Error) -> DevelopmentAccessError {
    match error {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_) => DevelopmentAccessError::Unavailable,
        sqlx::Error::Database(database)
            if database
                .code()
                .is_some_and(|code| code == "40001" || code == "40P01") =>
        {
            DevelopmentAccessError::Unavailable
        }
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            DevelopmentAccessError::Conflict
        }
        _ => DevelopmentAccessError::Corruption,
    }
}
