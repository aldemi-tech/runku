//! SQL-backed identity repository with identical SQLite/PostgreSQL semantics.

use std::{
    collections::BTreeSet,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{ApplicationClientId, CredentialId, EnvironmentScope};
use runku_identity::{
    ApplicationAssurance, ApplicationClient, ApplicationClientStatus, ApplicationContext,
    ApplicationCredential, ApplicationCredentialResolver, ApplicationIdentityRepository,
    ApplicationScope, ClientKind, CredentialDigest, CredentialKind, CredentialLifecycleResult,
    CredentialStatus, IdentityError, IdentityRepositoryBackend, IdentityTelemetrySnapshot,
    KeyringCrypto, ParsedApplicationKey,
};
use runku_value::TimestampMicros;
use sqlx::{
    Any, AnyPool, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

const SCHEMA_VERSION: i64 = 1;
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_identity_environments (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, configuration_revision BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id))",
    "CREATE TABLE IF NOT EXISTS runku_application_clients (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, client_id TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, scope_ceiling BYTEA NOT NULL, created_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, client_id), UNIQUE(project_id, environment_id, name), FOREIGN KEY(project_id, environment_id) REFERENCES runku_identity_environments(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE TABLE IF NOT EXISTS runku_application_credentials (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, credential_id TEXT NOT NULL, client_id TEXT NOT NULL, kind TEXT NOT NULL, label TEXT NOT NULL, status TEXT NOT NULL, digest BYTEA NOT NULL, scopes BYTEA NOT NULL, created_at_micros BIGINT NOT NULL, expires_at_micros BIGINT NULL, revoked_at_micros BIGINT NULL, deleted_at_micros BIGINT NULL, PRIMARY KEY(project_id, environment_id, credential_id), FOREIGN KEY(project_id, environment_id, client_id) REFERENCES runku_application_clients(project_id, environment_id, client_id) ON DELETE RESTRICT)",
    "CREATE INDEX IF NOT EXISTS runku_credentials_by_client ON runku_application_credentials(project_id, environment_id, client_id, credential_id)",
];

/// Operational role selected for repository composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryRole {
    /// Local/test `SQLite` repository.
    Local,
    /// Authoritative `PostgreSQL` repository.
    Production,
}

/// Bounded connection pool and acquisition policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdentityRepositoryConfig {
    /// Declared operational role.
    pub role: RepositoryRole,
    /// Maximum connection count.
    pub max_connections: u32,
    /// Maximum wait for a connection.
    pub acquire_timeout: Duration,
}

impl IdentityRepositoryConfig {
    /// Deterministic `SQLite` configuration.
    pub const LOCAL: Self = Self {
        role: RepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };
    /// Bounded `PostgreSQL` configuration.
    pub const PRODUCTION: Self = Self {
        role: RepositoryRole::Production,
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
    };
}

#[derive(Debug, Default)]
struct Counters {
    clients_created: AtomicU64,
    credentials_created: AtomicU64,
    create_replays: AtomicU64,
    resolutions: AtomicU64,
    resolution_failures: AtomicU64,
    credentials_revoked: AtomicU64,
    credentials_deleted: AtomicU64,
    retryable_errors: AtomicU64,
}

/// Durable SQL Application Identity repository.
#[derive(Clone, Debug)]
pub struct SqlApplicationIdentityRepository {
    pool: AnyPool,
    backend: IdentityRepositoryBackend,
    counters: Arc<Counters>,
}

impl SqlApplicationIdentityRepository {
    /// Connects local `SQLite` and applies checksum-protected migrations.
    ///
    /// # Errors
    ///
    /// Rejects production role, invalid pool settings, or unsafe database configuration.
    pub async fn connect_sqlite(
        url: &str,
        config: IdentityRepositoryConfig,
    ) -> Result<Self, IdentityError> {
        if config.role == RepositoryRole::Production {
            return Err(IdentityError::ProductionBackendUnsupported);
        }
        if !url.starts_with("sqlite:") {
            return Err(IdentityError::Unavailable);
        }
        Self::connect(url, config, IdentityRepositoryBackend::SQLite).await
    }

    /// Connects authoritative `PostgreSQL` 16+ and applies checksum-protected migrations.
    ///
    /// # Errors
    ///
    /// Rejects local role, invalid pool settings, unsupported versions, or unsafe configuration.
    pub async fn connect_postgres(
        url: &str,
        config: IdentityRepositoryConfig,
    ) -> Result<Self, IdentityError> {
        if config.role != RepositoryRole::Production {
            return Err(IdentityError::ProductionBackendUnsupported);
        }
        if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            return Err(IdentityError::Unavailable);
        }
        Self::connect(url, config, IdentityRepositoryBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: IdentityRepositoryConfig,
        backend: IdentityRepositoryBackend,
    ) -> Result<Self, IdentityError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || (backend == IdentityRepositoryBackend::SQLite && config.max_connections != 1)
        {
            return Err(IdentityError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options = AnyConnectOptions::from_str(url).map_err(|_| IdentityError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    match backend {
                        IdentityRepositoryBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        IdentityRepositoryBackend::PostgreSQL => {
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
        if backend == IdentityRepositoryBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(IdentityError::Unsupported);
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

    /// Closes the bounded pool and waits for checked-out connections.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    fn track<T>(&self, result: Result<T, IdentityError>) -> Result<T, IdentityError> {
        if result.as_ref().is_err_and(|error| error.retryable()) {
            self.counters
                .retryable_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

#[async_trait]
impl ApplicationIdentityRepository for SqlApplicationIdentityRepository {
    fn backend(&self) -> IdentityRepositoryBackend {
        self.backend
    }

    async fn create_client(&self, client: &ApplicationClient) -> Result<bool, IdentityError> {
        client.validate()?;
        let result = create_client(&self.pool, self.backend, client).await;
        match result.as_ref() {
            Ok(true) => {
                self.counters
                    .clients_created
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {
                self.counters.create_replays.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {}
        }
        self.track(result)
    }

    async fn get_client(
        &self,
        scope: EnvironmentScope,
        id: ApplicationClientId,
    ) -> Result<Option<ApplicationClient>, IdentityError> {
        self.track(load_client(&self.pool, scope, id).await)
    }

    async fn list_clients(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Vec<ApplicationClient>, IdentityError> {
        self.track(list_clients(&self.pool, scope).await)
    }

    async fn create_credential(
        &self,
        credential: &ApplicationCredential,
    ) -> Result<bool, IdentityError> {
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

    async fn list_credentials(
        &self,
        scope: EnvironmentScope,
        client_id: ApplicationClientId,
    ) -> Result<Vec<ApplicationCredential>, IdentityError> {
        self.track(list_credentials(&self.pool, scope, client_id).await)
    }

    async fn revoke_credential(
        &self,
        scope: EnvironmentScope,
        id: CredentialId,
        at: TimestampMicros,
    ) -> Result<CredentialLifecycleResult, IdentityError> {
        let result = transition_credential(
            &self.pool,
            self.backend,
            scope,
            id,
            at,
            CredentialStatus::Revoked,
        )
        .await;
        if matches!(result, Ok(CredentialLifecycleResult::Changed)) {
            self.counters
                .credentials_revoked
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn delete_credential(
        &self,
        scope: EnvironmentScope,
        id: CredentialId,
        at: TimestampMicros,
    ) -> Result<CredentialLifecycleResult, IdentityError> {
        let result = transition_credential(
            &self.pool,
            self.backend,
            scope,
            id,
            at,
            CredentialStatus::Deleted,
        )
        .await;
        if matches!(result, Ok(CredentialLifecycleResult::Changed)) {
            self.counters
                .credentials_deleted
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn configuration_revision(&self, scope: EnvironmentScope) -> Result<u64, IdentityError> {
        self.track(configuration_revision(&self.pool, scope).await)
    }

    async fn health(&self) -> Result<(), IdentityError> {
        self.track(
            sqlx::query_scalar::<_, i64>("SELECT 1")
                .fetch_one(&self.pool)
                .await
                .map(|_| ())
                .map_err(map_sqlx_error),
        )
    }

    fn telemetry(&self) -> IdentityTelemetrySnapshot {
        IdentityTelemetrySnapshot {
            clients_created: self.counters.clients_created.load(Ordering::Relaxed),
            credentials_created: self.counters.credentials_created.load(Ordering::Relaxed),
            create_replays: self.counters.create_replays.load(Ordering::Relaxed),
            resolutions: self.counters.resolutions.load(Ordering::Relaxed),
            resolution_failures: self.counters.resolution_failures.load(Ordering::Relaxed),
            credentials_revoked: self.counters.credentials_revoked.load(Ordering::Relaxed),
            credentials_deleted: self.counters.credentials_deleted.load(Ordering::Relaxed),
            retryable_errors: self.counters.retryable_errors.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl ApplicationCredentialResolver for SqlApplicationIdentityRepository {
    async fn resolve_key(
        &self,
        scope: EnvironmentScope,
        key: &ParsedApplicationKey,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<ApplicationContext, IdentityError> {
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

async fn create_client(
    pool: &AnyPool,
    backend: IdentityRepositoryBackend,
    client: &ApplicationClient,
) -> Result<bool, IdentityError> {
    let mut tx = begin_write(pool, backend).await?;
    ensure_environment(&mut tx, client.scope).await?;
    if let Some(existing) = load_client_tx(&mut tx, client.scope, client.id).await? {
        if existing == *client {
            tx.commit().await.map_err(map_commit_error)?;
            return Ok(false);
        }
        return Err(IdentityError::Conflict);
    }
    let inserted = sqlx::query("INSERT INTO runku_application_clients (project_id, environment_id, client_id, name, kind, status, scope_ceiling, created_at_micros) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
        .bind(client.scope.project_id().to_string()).bind(client.scope.environment_id().to_string())
        .bind(client.id.to_string()).bind(client.name.as_str()).bind(encode_client_kind(client.kind))
        .bind(encode_client_status(client.status)).bind(encode_scopes(&client.scope_ceiling))
        .bind(client.created_at.get()).execute(&mut *tx).await;
    if let Err(error) = inserted {
        let constraint = is_constraint_error(&error);
        tx.rollback().await.map_err(map_sqlx_error)?;
        if constraint && load_client(pool, client.scope, client.id).await?.as_ref() == Some(client)
        {
            return Ok(false);
        }
        return Err(map_constraint_error(error));
    }
    bump_revision(&mut tx, client.scope).await?;
    tx.commit().await.map_err(map_commit_error)?;
    Ok(true)
}

async fn create_credential(
    pool: &AnyPool,
    backend: IdentityRepositoryBackend,
    credential: &ApplicationCredential,
) -> Result<bool, IdentityError> {
    let mut tx = begin_write(pool, backend).await?;
    let client = load_client_tx(&mut tx, credential.scope, credential.client_id)
        .await?
        .ok_or(IdentityError::ClientNotFound)?;
    validate_credential_for_client(credential, &client)?;
    if let Some(existing) = load_credential_tx(&mut tx, credential.scope, credential.id).await? {
        if existing == *credential {
            tx.commit().await.map_err(map_commit_error)?;
            return Ok(false);
        }
        return Err(IdentityError::Conflict);
    }
    let inserted = sqlx::query("INSERT INTO runku_application_credentials (project_id, environment_id, credential_id, client_id, kind, label, status, digest, scopes, created_at_micros, expires_at_micros, revoked_at_micros, deleted_at_micros) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)")
        .bind(credential.scope.project_id().to_string()).bind(credential.scope.environment_id().to_string())
        .bind(credential.id.to_string()).bind(credential.client_id.to_string()).bind(encode_credential_kind(credential.kind))
        .bind(credential.label.as_str()).bind(encode_credential_status(credential.status))
        .bind(credential.digest.as_bytes().to_vec()).bind(encode_scopes(&credential.scopes))
        .bind(credential.created_at.get()).bind(credential.expires_at.map(TimestampMicros::get))
        .bind(credential.revoked_at.map(TimestampMicros::get)).bind(credential.deleted_at.map(TimestampMicros::get))
        .execute(&mut *tx).await;
    if let Err(error) = inserted {
        let constraint = is_constraint_error(&error);
        tx.rollback().await.map_err(map_sqlx_error)?;
        if constraint
            && load_credential(pool, credential.scope, credential.id)
                .await?
                .as_ref()
                == Some(credential)
        {
            return Ok(false);
        }
        return Err(map_constraint_error(error));
    }
    bump_revision(&mut tx, credential.scope).await?;
    tx.commit().await.map_err(map_commit_error)?;
    Ok(true)
}

async fn resolve_key(
    pool: &AnyPool,
    scope: EnvironmentScope,
    key: &ParsedApplicationKey,
    crypto: &KeyringCrypto,
    now: TimestampMicros,
) -> Result<ApplicationContext, IdentityError> {
    if now.get() < 0 {
        return Err(IdentityError::InvalidInput);
    }
    let row = sqlx::query("SELECT c.client_id, c.kind AS client_kind, c.status AS client_status, c.scope_ceiling, k.kind AS credential_kind, k.status AS credential_status, k.digest, k.scopes, k.expires_at_micros, e.configuration_revision FROM runku_application_credentials k JOIN runku_application_clients c ON c.project_id = k.project_id AND c.environment_id = k.environment_id AND c.client_id = k.client_id JOIN runku_identity_environments e ON e.project_id = k.project_id AND e.environment_id = k.environment_id WHERE k.project_id = $1 AND k.environment_id = $2 AND k.credential_id = $3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(key.credential_id().to_string())
        .fetch_optional(pool).await.map_err(map_sqlx_error)?.ok_or(IdentityError::InvalidCredential)?;
    let client_id = parse_text::<ApplicationClientId>(&row, "client_id")?;
    let client_kind = decode_client_kind(
        row.try_get("client_kind")
            .map_err(|_| IdentityError::Corruption)?,
    )?;
    let client_status = decode_client_status(
        row.try_get("client_status")
            .map_err(|_| IdentityError::Corruption)?,
    )?;
    let ceiling = decode_scopes(
        row.try_get("scope_ceiling")
            .map_err(|_| IdentityError::Corruption)?,
    )?;
    let credential_kind = decode_credential_kind(
        row.try_get("credential_kind")
            .map_err(|_| IdentityError::Corruption)?,
    )?;
    let credential_status = decode_credential_status(
        row.try_get("credential_status")
            .map_err(|_| IdentityError::Corruption)?,
    )?;
    let digest = decode_digest(
        row.try_get("digest")
            .map_err(|_| IdentityError::Corruption)?,
    )?;
    let scopes = decode_scopes(
        row.try_get("scopes")
            .map_err(|_| IdentityError::Corruption)?,
    )?;
    let expires_at: Option<i64> = row
        .try_get("expires_at_micros")
        .map_err(|_| IdentityError::Corruption)?;
    let revision: i64 = row
        .try_get("configuration_revision")
        .map_err(|_| IdentityError::Corruption)?;
    if key.kind() != credential_kind || !crypto.verify(key.key(), digest) {
        return Err(IdentityError::InvalidCredential);
    }
    if client_status != ApplicationClientStatus::Active {
        return Err(IdentityError::ClientInactive);
    }
    if credential_status != CredentialStatus::Active
        || expires_at.is_some_and(|expiry| now.get() >= expiry)
    {
        return Err(IdentityError::CredentialInactive);
    }
    if !scopes.is_subset(&ceiling) || !kind_matches(client_kind, credential_kind) {
        return Err(IdentityError::Corruption);
    }
    Ok(ApplicationContext {
        client_id,
        credential_id: key.credential_id(),
        credential_kind,
        assurance: match credential_kind {
            CredentialKind::Publishable => ApplicationAssurance::Declared,
            CredentialKind::Secret => ApplicationAssurance::Verified,
        },
        scopes,
        configuration_revision: u64::try_from(revision).map_err(|_| IdentityError::Corruption)?,
    })
}

async fn transition_credential(
    pool: &AnyPool,
    backend: IdentityRepositoryBackend,
    scope: EnvironmentScope,
    id: CredentialId,
    at: TimestampMicros,
    target: CredentialStatus,
) -> Result<CredentialLifecycleResult, IdentityError> {
    if at.get() < 0 || target == CredentialStatus::Active {
        return Err(IdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let existing = load_credential_tx(&mut tx, scope, id)
        .await?
        .ok_or(IdentityError::CredentialNotFound)?;
    if at < existing.created_at {
        return Err(IdentityError::InvalidInput);
    }
    match (existing.status, target) {
        (CredentialStatus::Active, CredentialStatus::Revoked) => {
            sqlx::query("UPDATE runku_application_credentials SET status = 'revoked', revoked_at_micros = $1 WHERE project_id = $2 AND environment_id = $3 AND credential_id = $4 AND status = 'active'")
                .bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
                .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }
        (CredentialStatus::Revoked | CredentialStatus::Deleted, CredentialStatus::Revoked)
        | (CredentialStatus::Deleted, CredentialStatus::Deleted) => {
            tx.commit().await.map_err(map_commit_error)?;
            return Ok(CredentialLifecycleResult::Replayed);
        }
        (CredentialStatus::Revoked, CredentialStatus::Deleted) => {
            if existing.revoked_at.is_none_or(|revoked| at < revoked) {
                return Err(IdentityError::InvalidInput);
            }
            sqlx::query("UPDATE runku_application_credentials SET status = 'deleted', deleted_at_micros = $1 WHERE project_id = $2 AND environment_id = $3 AND credential_id = $4 AND status = 'revoked'")
                .bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
                .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }
        (CredentialStatus::Active, CredentialStatus::Deleted) => {
            return Err(IdentityError::InvalidTransition);
        }
        (_, CredentialStatus::Active) => return Err(IdentityError::InvalidInput),
    }
    bump_revision(&mut tx, scope).await?;
    tx.commit().await.map_err(map_commit_error)?;
    Ok(CredentialLifecycleResult::Changed)
}

async fn load_client(
    pool: &AnyPool,
    scope: EnvironmentScope,
    id: ApplicationClientId,
) -> Result<Option<ApplicationClient>, IdentityError> {
    let row = sqlx::query("SELECT client_id, name, kind, status, scope_ceiling, created_at_micros FROM runku_application_clients WHERE project_id = $1 AND environment_id = $2 AND client_id = $3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
        .fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_client_row(scope, &row)).transpose()
}

async fn load_client_tx(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    id: ApplicationClientId,
) -> Result<Option<ApplicationClient>, IdentityError> {
    let row = sqlx::query("SELECT client_id, name, kind, status, scope_ceiling, created_at_micros FROM runku_application_clients WHERE project_id = $1 AND environment_id = $2 AND client_id = $3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
        .fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_client_row(scope, &row)).transpose()
}

async fn list_clients(
    pool: &AnyPool,
    scope: EnvironmentScope,
) -> Result<Vec<ApplicationClient>, IdentityError> {
    let rows = sqlx::query("SELECT client_id, name, kind, status, scope_ceiling, created_at_micros FROM runku_application_clients WHERE project_id = $1 AND environment_id = $2 ORDER BY client_id")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .fetch_all(pool).await.map_err(map_sqlx_error)?;
    rows.iter()
        .map(|row| decode_client_row(scope, row))
        .collect()
}

async fn load_credential_tx(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    id: CredentialId,
) -> Result<Option<ApplicationCredential>, IdentityError> {
    let row = sqlx::query("SELECT credential_id, client_id, kind, label, status, digest, scopes, created_at_micros, expires_at_micros, revoked_at_micros, deleted_at_micros FROM runku_application_credentials WHERE project_id = $1 AND environment_id = $2 AND credential_id = $3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
        .fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_credential_row(scope, &row))
        .transpose()
}

async fn load_credential(
    pool: &AnyPool,
    scope: EnvironmentScope,
    id: CredentialId,
) -> Result<Option<ApplicationCredential>, IdentityError> {
    let row = sqlx::query("SELECT credential_id, client_id, kind, label, status, digest, scopes, created_at_micros, expires_at_micros, revoked_at_micros, deleted_at_micros FROM runku_application_credentials WHERE project_id = $1 AND environment_id = $2 AND credential_id = $3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string())
        .fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_credential_row(scope, &row))
        .transpose()
}

async fn list_credentials(
    pool: &AnyPool,
    scope: EnvironmentScope,
    client_id: ApplicationClientId,
) -> Result<Vec<ApplicationCredential>, IdentityError> {
    if load_client(pool, scope, client_id).await?.is_none() {
        return Err(IdentityError::ClientNotFound);
    }
    let rows = sqlx::query("SELECT credential_id, client_id, kind, label, status, digest, scopes, created_at_micros, expires_at_micros, revoked_at_micros, deleted_at_micros FROM runku_application_credentials WHERE project_id = $1 AND environment_id = $2 AND client_id = $3 AND status <> 'deleted' ORDER BY credential_id")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(client_id.to_string())
        .fetch_all(pool).await.map_err(map_sqlx_error)?;
    rows.iter()
        .map(|row| decode_credential_row(scope, row))
        .collect()
}

fn decode_client_row(
    scope: EnvironmentScope,
    row: &sqlx::any::AnyRow,
) -> Result<ApplicationClient, IdentityError> {
    let client = ApplicationClient {
        scope,
        id: parse_text(row, "client_id")?,
        name: parse_text(row, "name")?,
        kind: decode_client_kind(row.try_get("kind").map_err(|_| IdentityError::Corruption)?)?,
        status: decode_client_status(
            row.try_get("status")
                .map_err(|_| IdentityError::Corruption)?,
        )?,
        scope_ceiling: decode_scopes(
            row.try_get("scope_ceiling")
                .map_err(|_| IdentityError::Corruption)?,
        )?,
        created_at: TimestampMicros::new(
            row.try_get("created_at_micros")
                .map_err(|_| IdentityError::Corruption)?,
        ),
    };
    client.validate().map_err(|_| IdentityError::Corruption)?;
    Ok(client)
}

fn decode_credential_row(
    scope: EnvironmentScope,
    row: &sqlx::any::AnyRow,
) -> Result<ApplicationCredential, IdentityError> {
    let credential = ApplicationCredential {
        scope,
        id: parse_text(row, "credential_id")?,
        client_id: parse_text(row, "client_id")?,
        kind: decode_credential_kind(row.try_get("kind").map_err(|_| IdentityError::Corruption)?)?,
        label: parse_text(row, "label")?,
        status: decode_credential_status(
            row.try_get("status")
                .map_err(|_| IdentityError::Corruption)?,
        )?,
        digest: decode_digest(
            row.try_get("digest")
                .map_err(|_| IdentityError::Corruption)?,
        )?,
        scopes: decode_scopes(
            row.try_get("scopes")
                .map_err(|_| IdentityError::Corruption)?,
        )?,
        created_at: TimestampMicros::new(
            row.try_get("created_at_micros")
                .map_err(|_| IdentityError::Corruption)?,
        ),
        expires_at: optional_time(row, "expires_at_micros")?,
        revoked_at: optional_time(row, "revoked_at_micros")?,
        deleted_at: optional_time(row, "deleted_at_micros")?,
    };
    credential
        .validate()
        .map_err(|_| IdentityError::Corruption)?;
    Ok(credential)
}

fn validate_credential_for_client(
    credential: &ApplicationCredential,
    client: &ApplicationClient,
) -> Result<(), IdentityError> {
    if client.status != ApplicationClientStatus::Active {
        return Err(IdentityError::ClientInactive);
    }
    if !kind_matches(client.kind, credential.kind) {
        return Err(IdentityError::CredentialTypeMismatch);
    }
    if !credential.scopes.is_subset(&client.scope_ceiling) {
        return Err(IdentityError::ScopeEscalation);
    }
    Ok(())
}

const fn kind_matches(client: ClientKind, credential: CredentialKind) -> bool {
    matches!(
        (client, credential),
        (ClientKind::Public, CredentialKind::Publishable)
            | (ClientKind::Confidential, CredentialKind::Secret)
    )
}

async fn configuration_revision(
    pool: &AnyPool,
    scope: EnvironmentScope,
) -> Result<u64, IdentityError> {
    let revision = sqlx::query_scalar::<_, i64>("SELECT configuration_revision FROM runku_identity_environments WHERE project_id = $1 AND environment_id = $2")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .fetch_optional(pool).await.map_err(map_sqlx_error)?.unwrap_or(0);
    u64::try_from(revision).map_err(|_| IdentityError::Corruption)
}

async fn ensure_environment(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
) -> Result<(), IdentityError> {
    sqlx::query("INSERT INTO runku_identity_environments (project_id, environment_id, configuration_revision) VALUES ($1, $2, 0) ON CONFLICT(project_id, environment_id) DO NOTHING")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .execute(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn bump_revision(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
) -> Result<(), IdentityError> {
    let affected = sqlx::query("UPDATE runku_identity_environments SET configuration_revision = configuration_revision + 1 WHERE project_id = $1 AND environment_id = $2")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .execute(&mut **tx).await.map_err(map_sqlx_error)?.rows_affected();
    if affected != 1 {
        return Err(IdentityError::Corruption);
    }
    Ok(())
}

async fn begin_write(
    pool: &AnyPool,
    backend: IdentityRepositoryBackend,
) -> Result<Transaction<'_, Any>, IdentityError> {
    let statement = match backend {
        IdentityRepositoryBackend::SQLite => "BEGIN IMMEDIATE",
        IdentityRepositoryBackend::PostgreSQL => "BEGIN ISOLATION LEVEL SERIALIZABLE",
    };
    pool.begin_with(statement).await.map_err(map_sqlx_error)
}

async fn verify_configuration(
    pool: &AnyPool,
    backend: IdentityRepositoryBackend,
) -> Result<(), IdentityError> {
    match backend {
        IdentityRepositoryBackend::SQLite => {
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
                return Err(IdentityError::Corruption);
            }
        }
        IdentityRepositoryBackend::PostgreSQL => {
            let row = sqlx::query("SELECT current_setting('statement_timeout') AS statement_timeout, current_setting('lock_timeout') AS lock_timeout, current_setting('idle_in_transaction_session_timeout') AS idle_timeout")
                .fetch_one(pool).await.map_err(map_sqlx_error)?;
            let statement: String = row
                .try_get("statement_timeout")
                .map_err(|_| IdentityError::Corruption)?;
            let lock: String = row
                .try_get("lock_timeout")
                .map_err(|_| IdentityError::Corruption)?;
            let idle: String = row
                .try_get("idle_timeout")
                .map_err(|_| IdentityError::Corruption)?;
            if statement != "30s" || lock != "5s" || idle != "30s" {
                return Err(IdentityError::Corruption);
            }
        }
    }
    Ok(())
}

async fn migrate(pool: &AnyPool, backend: IdentityRepositoryBackend) -> Result<(), IdentityError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_identity_migrations (version BIGINT PRIMARY KEY, checksum TEXT NOT NULL)")
        .execute(pool).await.map_err(map_sqlx_error)?;
    let mut tx = begin_write(pool, backend).await?;
    if backend == IdentityRepositoryBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(7_224_856_023_i64)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
    }
    let checksum = migration_checksum();
    if let Some(existing) = sqlx::query_scalar::<_, String>(
        "SELECT checksum FROM runku_identity_migrations WHERE version = $1",
    )
    .bind(SCHEMA_VERSION)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?
    {
        if existing != checksum {
            return Err(IdentityError::Corruption);
        }
        return tx.commit().await.map_err(map_commit_error);
    }
    for statement in SCHEMA {
        tx.execute(*statement).await.map_err(map_sqlx_error)?;
    }
    sqlx::query("INSERT INTO runku_identity_migrations (version, checksum) VALUES ($1, $2)")
        .bind(SCHEMA_VERSION)
        .bind(checksum)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_commit_error)
}

fn migration_checksum() -> String {
    use sha2::{Digest as _, Sha256};
    use std::fmt::Write as _;
    let mut hasher = Sha256::new();
    hasher.update(b"runku-identity-schema-v1\0");
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

fn encode_scopes(scopes: &BTreeSet<ApplicationScope>) -> Vec<u8> {
    scopes
        .iter()
        .map(ApplicationScope::as_str)
        .collect::<Vec<_>>()
        .join("\n")
        .into_bytes()
}

fn decode_scopes(bytes: Vec<u8>) -> Result<BTreeSet<ApplicationScope>, IdentityError> {
    let text = String::from_utf8(bytes).map_err(|_| IdentityError::Corruption)?;
    let values = text.split('\n').collect::<Vec<_>>();
    let parsed: BTreeSet<_> = values
        .iter()
        .map(|value| value.parse().map_err(|_| IdentityError::Corruption))
        .collect::<Result<_, _>>()?;
    if parsed.is_empty() || parsed.len() != values.len() {
        return Err(IdentityError::Corruption);
    }
    Ok(parsed)
}

fn decode_digest(bytes: Vec<u8>) -> Result<CredentialDigest, IdentityError> {
    Ok(CredentialDigest::from_bytes(
        bytes.try_into().map_err(|_| IdentityError::Corruption)?,
    ))
}

fn parse_text<T: FromStr>(row: &sqlx::any::AnyRow, field: &str) -> Result<T, IdentityError> {
    let text: String = row.try_get(field).map_err(|_| IdentityError::Corruption)?;
    text.parse().map_err(|_| IdentityError::Corruption)
}

fn optional_time(
    row: &sqlx::any::AnyRow,
    field: &str,
) -> Result<Option<TimestampMicros>, IdentityError> {
    let value: Option<i64> = row.try_get(field).map_err(|_| IdentityError::Corruption)?;
    Ok(value.map(TimestampMicros::new))
}

const fn encode_client_kind(value: ClientKind) -> &'static str {
    match value {
        ClientKind::Public => "public",
        ClientKind::Confidential => "confidential",
    }
}
fn decode_client_kind(value: &str) -> Result<ClientKind, IdentityError> {
    match value {
        "public" => Ok(ClientKind::Public),
        "confidential" => Ok(ClientKind::Confidential),
        _ => Err(IdentityError::Corruption),
    }
}
const fn encode_client_status(value: ApplicationClientStatus) -> &'static str {
    match value {
        ApplicationClientStatus::Active => "active",
        ApplicationClientStatus::Disabled => "disabled",
    }
}
fn decode_client_status(value: &str) -> Result<ApplicationClientStatus, IdentityError> {
    match value {
        "active" => Ok(ApplicationClientStatus::Active),
        "disabled" => Ok(ApplicationClientStatus::Disabled),
        _ => Err(IdentityError::Corruption),
    }
}
const fn encode_credential_kind(value: CredentialKind) -> &'static str {
    match value {
        CredentialKind::Publishable => "publishable",
        CredentialKind::Secret => "secret",
    }
}
fn decode_credential_kind(value: &str) -> Result<CredentialKind, IdentityError> {
    match value {
        "publishable" => Ok(CredentialKind::Publishable),
        "secret" => Ok(CredentialKind::Secret),
        _ => Err(IdentityError::Corruption),
    }
}
const fn encode_credential_status(value: CredentialStatus) -> &'static str {
    match value {
        CredentialStatus::Active => "active",
        CredentialStatus::Revoked => "revoked",
        CredentialStatus::Deleted => "deleted",
    }
}
fn decode_credential_status(value: &str) -> Result<CredentialStatus, IdentityError> {
    match value {
        "active" => Ok(CredentialStatus::Active),
        "revoked" => Ok(CredentialStatus::Revoked),
        "deleted" => Ok(CredentialStatus::Deleted),
        _ => Err(IdentityError::Corruption),
    }
}

fn map_constraint_error(error: sqlx::Error) -> IdentityError {
    match &error {
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            IdentityError::Conflict
        }
        _ => map_sqlx_error(error),
    }
}

fn is_constraint_error(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.is_unique_violation() || database.is_foreign_key_violation())
}

fn map_commit_error(error: sqlx::Error) -> IdentityError {
    match error {
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            IdentityError::Conflict
        }
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_) => {
            IdentityError::ResultUncertain
        }
        other => map_sqlx_error(other),
    }
}

fn map_sqlx_error(error: sqlx::Error) -> IdentityError {
    match error {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_) => IdentityError::Unavailable,
        sqlx::Error::Database(database)
            if database
                .code()
                .is_some_and(|code| code == "40001" || code == "40P01") =>
        {
            IdentityError::Unavailable
        }
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            IdentityError::Conflict
        }
        _ => IdentityError::Corruption,
    }
}
