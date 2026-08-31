//! SQL implementation shared by `SQLite` and `PostgreSQL`.

use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{DevRevisionId, OperationId};
use runku_releases::Sha256Digest;
use runku_value::TimestampMicros;
use sqlx::{
    Any, AnyPool, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

use crate::{
    DevelopmentBackend, DevelopmentCommand, DevelopmentCommandResult, DevelopmentContext,
    DevelopmentError, DevelopmentRepository, DevelopmentRevisionEntry, DevelopmentSnapshot,
    DevelopmentTelemetrySnapshot, WorkspaceBinding,
};

const SCHEMA_VERSION: i64 = 1;
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_development_environments (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, serving_revision BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id))",
    "CREATE TABLE IF NOT EXISTS runku_development_revisions (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, revision_id TEXT NOT NULL, release_id TEXT NOT NULL, manifest_digest BYTEA NOT NULL, manifest_bytes BYTEA NOT NULL, actor TEXT NOT NULL, created_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, revision_id), UNIQUE(project_id, environment_id, release_id), FOREIGN KEY(project_id, environment_id) REFERENCES runku_development_environments(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE TABLE IF NOT EXISTS runku_development_workspaces (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, workspace_id TEXT NOT NULL, workspace_ref TEXT NOT NULL, head_revision_id TEXT NULL, updated_by TEXT NOT NULL, updated_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, workspace_ref), UNIQUE(project_id, environment_id, workspace_id), FOREIGN KEY(project_id, environment_id) REFERENCES runku_development_environments(project_id, environment_id) ON DELETE CASCADE, FOREIGN KEY(project_id, environment_id, head_revision_id) REFERENCES runku_development_revisions(project_id, environment_id, revision_id) ON DELETE RESTRICT)",
    "CREATE TABLE IF NOT EXISTS runku_development_operations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, command_digest BYTEA NOT NULL, serving_revision BIGINT NOT NULL, head_revision_id TEXT NULL, created_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, operation_id), FOREIGN KEY(project_id, environment_id) REFERENCES runku_development_environments(project_id, environment_id) ON DELETE CASCADE)",
];

/// Operational adapter role; independent from Environment purpose/protection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentRepositoryRole {
    /// Embedded local development.
    Local,
    /// Shared authoritative repository.
    Authoritative,
}

/// Bounded pool and acquisition configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DevelopmentRepositoryConfig {
    /// Explicit operational role.
    pub role: DevelopmentRepositoryRole,
    /// Maximum physical connections.
    pub max_connections: u32,
    /// Pool acquisition deadline.
    pub acquire_timeout: Duration,
}

impl DevelopmentRepositoryConfig {
    /// Deterministic local `SQLite` configuration.
    pub const LOCAL: Self = Self {
        role: DevelopmentRepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };

    /// Bounded shared `PostgreSQL` configuration.
    pub const AUTHORITATIVE: Self = Self {
        role: DevelopmentRepositoryRole::Authoritative,
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

/// Durable SQL repository fixed to one trusted Environment context.
#[derive(Clone, Debug)]
pub struct SqlDevelopmentRepository {
    pool: AnyPool,
    backend: DevelopmentBackend,
    context: DevelopmentContext,
    counters: Arc<Counters>,
}

impl SqlDevelopmentRepository {
    /// Opens a local `SQLite` repository after validating Environment policy.
    ///
    /// # Errors
    ///
    /// Rejects forbidden policy/role/URL before creating a database file.
    pub async fn connect_sqlite(
        url: &str,
        config: DevelopmentRepositoryConfig,
        context: DevelopmentContext,
    ) -> Result<Self, DevelopmentError> {
        context.validate()?;
        if config.role != DevelopmentRepositoryRole::Local || !url.starts_with("sqlite:") {
            return Err(DevelopmentError::Unsupported);
        }
        Self::connect(url, config, context, DevelopmentBackend::SQLite).await
    }

    /// Opens an authoritative `PostgreSQL` repository after validating Environment policy.
    ///
    /// # Errors
    ///
    /// Rejects forbidden policy/role/URL before network I/O.
    pub async fn connect_postgres(
        url: &str,
        config: DevelopmentRepositoryConfig,
        context: DevelopmentContext,
    ) -> Result<Self, DevelopmentError> {
        context.validate()?;
        if config.role != DevelopmentRepositoryRole::Authoritative
            || !(url.starts_with("postgres://") || url.starts_with("postgresql://"))
        {
            return Err(DevelopmentError::Unsupported);
        }
        Self::connect(url, config, context, DevelopmentBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: DevelopmentRepositoryConfig,
        context: DevelopmentContext,
        backend: DevelopmentBackend,
    ) -> Result<Self, DevelopmentError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || (backend == DevelopmentBackend::SQLite && config.max_connections != 1)
        {
            return Err(DevelopmentError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options =
            AnyConnectOptions::from_str(url).map_err(|_| DevelopmentError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    match backend {
                        DevelopmentBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        DevelopmentBackend::PostgreSQL => {
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
        if backend == DevelopmentBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(DevelopmentError::Unsupported);
            }
        }
        migrate(&pool, backend).await?;
        Ok(Self {
            pool,
            backend,
            context,
            counters: Arc::new(Counters::default()),
        })
    }

    /// Closes the bounded pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    fn validate_context(&self, context: DevelopmentContext) -> Result<(), DevelopmentError> {
        context.validate()?;
        if context != self.context {
            return Err(DevelopmentError::PolicyDenied);
        }
        Ok(())
    }
}

#[async_trait]
impl DevelopmentRepository for SqlDevelopmentRepository {
    fn backend(&self) -> DevelopmentBackend {
        self.backend
    }

    async fn apply(
        &self,
        context: DevelopmentContext,
        operation_id: OperationId,
        command: &DevelopmentCommand,
    ) -> Result<DevelopmentCommandResult, DevelopmentError> {
        self.validate_context(context)?;
        let digest = command.digest(context)?;
        let result = apply_inner(
            &self.pool,
            self.backend,
            context,
            operation_id,
            command,
            digest,
        )
        .await;
        match &result {
            Ok(value) if value.replayed => {
                self.counters.replays.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => {
                self.counters.commands.fetch_add(1, Ordering::Relaxed);
            }
            Err(DevelopmentError::Conflict) => {
                self.counters.conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.retryable() => {
                self.counters
                    .retryable_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {}
        }
        result
    }

    async fn snapshot(
        &self,
        context: DevelopmentContext,
    ) -> Result<DevelopmentSnapshot, DevelopmentError> {
        self.validate_context(context)?;
        let result = load_snapshot(&self.pool, self.backend, context).await;
        if result.is_ok() {
            self.counters.snapshots.fetch_add(1, Ordering::Relaxed);
        } else if result.as_ref().is_err_and(|error| error.retryable()) {
            self.counters
                .retryable_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn health(&self) -> Result<(), DevelopmentError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_error)
    }

    fn telemetry(&self) -> DevelopmentTelemetrySnapshot {
        DevelopmentTelemetrySnapshot {
            commands: self.counters.commands.load(Ordering::Relaxed),
            replays: self.counters.replays.load(Ordering::Relaxed),
            conflicts: self.counters.conflicts.load(Ordering::Relaxed),
            snapshots: self.counters.snapshots.load(Ordering::Relaxed),
            retryable_errors: self.counters.retryable_errors.load(Ordering::Relaxed),
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn apply_inner(
    pool: &AnyPool,
    backend: DevelopmentBackend,
    context: DevelopmentContext,
    operation_id: OperationId,
    command: &DevelopmentCommand,
    digest: [u8; 32],
) -> Result<DevelopmentCommandResult, DevelopmentError> {
    let mut transaction = pool.begin().await.map_err(map_sqlx_error)?;
    let project = context.scope.project_id().to_string();
    let environment = context.scope.environment_id().to_string();
    sqlx::query("INSERT INTO runku_development_environments (project_id, environment_id, serving_revision) VALUES ($1, $2, 0) ON CONFLICT (project_id, environment_id) DO NOTHING")
        .bind(&project).bind(&environment).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
    let lock_query = match backend {
        DevelopmentBackend::SQLite => {
            "SELECT serving_revision FROM runku_development_environments WHERE project_id = $1 AND environment_id = $2"
        }
        DevelopmentBackend::PostgreSQL => {
            "SELECT serving_revision FROM runku_development_environments WHERE project_id = $1 AND environment_id = $2 FOR UPDATE"
        }
    };
    let current: i64 = sqlx::query_scalar(lock_query)
        .bind(&project)
        .bind(&environment)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
    if let Some(row) = sqlx::query("SELECT command_digest, serving_revision, head_revision_id FROM runku_development_operations WHERE project_id = $1 AND environment_id = $2 AND operation_id = $3")
        .bind(&project).bind(&environment).bind(operation_id.to_string()).fetch_optional(&mut *transaction).await.map_err(map_sqlx_error)?
    {
        let stored: Vec<u8> = row.try_get("command_digest").map_err(|_| DevelopmentError::Corruption)?;
        if stored.as_slice() != digest {
            return rollback(transaction, DevelopmentError::Conflict).await;
        }
        let result = DevelopmentCommandResult {
            serving_revision: positive_u64(row.try_get("serving_revision").map_err(|_| DevelopmentError::Corruption)?)?,
            replayed: true,
            head_revision: row.try_get::<Option<String>, _>("head_revision_id").map_err(|_| DevelopmentError::Corruption)?
                .map(|value| value.parse().map_err(|_| DevelopmentError::Corruption)).transpose()?,
        };
        transaction.rollback().await.map_err(map_sqlx_error)?;
        return Ok(result);
    }
    let head = match command {
        DevelopmentCommand::CreateWorkspace {
            workspace_id,
            workspace_ref,
            actor,
            created_at,
        } => {
            let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runku_development_workspaces WHERE project_id = $1 AND environment_id = $2 AND (workspace_ref = $3 OR workspace_id = $4)")
                .bind(&project).bind(&environment).bind(workspace_ref.as_str()).bind(workspace_id.to_string())
                .fetch_one(&mut *transaction).await.map_err(map_sqlx_error)?;
            if existing != 0 {
                return rollback(transaction, DevelopmentError::Conflict).await;
            }
            sqlx::query("INSERT INTO runku_development_workspaces (project_id, environment_id, workspace_id, workspace_ref, head_revision_id, updated_by, updated_at_micros) VALUES ($1, $2, $3, $4, NULL, $5, $6)")
                .bind(&project).bind(&environment).bind(workspace_id.to_string()).bind(workspace_ref.as_str())
                .bind(actor.as_str()).bind(created_at.get()).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
            None
        }
        DevelopmentCommand::PublishRevision {
            workspace_ref,
            expected_head,
            revision,
        } => {
            let workspace_query = match backend {
                DevelopmentBackend::SQLite => {
                    "SELECT head_revision_id FROM runku_development_workspaces WHERE project_id = $1 AND environment_id = $2 AND workspace_ref = $3"
                }
                DevelopmentBackend::PostgreSQL => {
                    "SELECT head_revision_id FROM runku_development_workspaces WHERE project_id = $1 AND environment_id = $2 AND workspace_ref = $3 FOR UPDATE"
                }
            };
            let current_head = sqlx::query_scalar::<_, Option<String>>(workspace_query)
                .bind(&project)
                .bind(&environment)
                .bind(workspace_ref.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(map_sqlx_error)?
                .ok_or(DevelopmentError::WorkspaceNotFound)?
                .map(|value| {
                    value
                        .parse::<DevRevisionId>()
                        .map_err(|_| DevelopmentError::Corruption)
                })
                .transpose()?;
            if &current_head != expected_head {
                return rollback(transaction, DevelopmentError::Conflict).await;
            }
            if let Some(row) = sqlx::query("SELECT revision_id, release_id, manifest_digest, manifest_bytes, actor, created_at_micros FROM runku_development_revisions WHERE project_id = $1 AND environment_id = $2 AND (revision_id = $3 OR release_id = $4)")
                .bind(&project).bind(&environment).bind(revision.revision_id.to_string()).bind(revision.release_id.to_string())
                .fetch_optional(&mut *transaction).await.map_err(map_sqlx_error)?
            {
                if decode_revision(&row)? != *revision { return rollback(transaction, DevelopmentError::Conflict).await; }
            } else {
                sqlx::query("INSERT INTO runku_development_revisions (project_id, environment_id, revision_id, release_id, manifest_digest, manifest_bytes, actor, created_at_micros) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
                    .bind(&project).bind(&environment).bind(revision.revision_id.to_string()).bind(revision.release_id.to_string())
                    .bind(revision.manifest_digest.as_bytes().to_vec()).bind(&revision.manifest_bytes).bind(revision.actor.as_str()).bind(revision.created_at.get())
                    .execute(&mut *transaction).await.map_err(map_sqlx_error)?;
            }
            sqlx::query("UPDATE runku_development_workspaces SET head_revision_id = $1, updated_by = $2, updated_at_micros = $3 WHERE project_id = $4 AND environment_id = $5 AND workspace_ref = $6")
                .bind(revision.revision_id.to_string()).bind(revision.actor.as_str()).bind(revision.created_at.get())
                .bind(&project).bind(&environment).bind(workspace_ref.as_str()).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
            Some(revision.revision_id)
        }
    };
    let next = current
        .checked_add(1)
        .ok_or(DevelopmentError::LimitExceeded)?;
    sqlx::query("UPDATE runku_development_environments SET serving_revision = $1 WHERE project_id = $2 AND environment_id = $3")
        .bind(next).bind(&project).bind(&environment).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
    sqlx::query("INSERT INTO runku_development_operations (project_id, environment_id, operation_id, command_digest, serving_revision, head_revision_id, created_at_micros) VALUES ($1, $2, $3, $4, $5, $6, $7)")
        .bind(&project).bind(&environment).bind(operation_id.to_string()).bind(digest.to_vec()).bind(next)
        .bind(head.map(|value| value.to_string())).bind(command_time(command)).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| DevelopmentError::ResultUncertain)?;
    Ok(DevelopmentCommandResult {
        serving_revision: positive_u64(next)?,
        replayed: false,
        head_revision: head,
    })
}

async fn load_snapshot(
    pool: &AnyPool,
    backend: DevelopmentBackend,
    context: DevelopmentContext,
) -> Result<DevelopmentSnapshot, DevelopmentError> {
    let mut transaction = pool.begin().await.map_err(map_sqlx_error)?;
    if backend == DevelopmentBackend::PostgreSQL {
        transaction
            .execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(map_sqlx_error)?;
    }
    let project = context.scope.project_id().to_string();
    let environment = context.scope.environment_id().to_string();
    let revision: i64 = sqlx::query_scalar("SELECT serving_revision FROM runku_development_environments WHERE project_id = $1 AND environment_id = $2")
        .bind(&project).bind(&environment).fetch_optional(&mut *transaction).await.map_err(map_sqlx_error)?
        .ok_or(DevelopmentError::WorkspaceNotFound)?;
    let revision_rows = sqlx::query("SELECT revision_id, release_id, manifest_digest, manifest_bytes, actor, created_at_micros FROM runku_development_revisions WHERE project_id = $1 AND environment_id = $2 ORDER BY revision_id")
        .bind(&project).bind(&environment).fetch_all(&mut *transaction).await.map_err(map_sqlx_error)?;
    let revisions = revision_rows
        .iter()
        .map(decode_revision)
        .collect::<Result<Vec<_>, _>>()?;
    let workspace_rows = sqlx::query("SELECT workspace_id, workspace_ref, head_revision_id, updated_by, updated_at_micros FROM runku_development_workspaces WHERE project_id = $1 AND environment_id = $2 ORDER BY workspace_ref")
        .bind(&project).bind(&environment).fetch_all(&mut *transaction).await.map_err(map_sqlx_error)?;
    let workspaces = workspace_rows
        .iter()
        .map(decode_workspace)
        .collect::<Result<Vec<_>, _>>()?;
    transaction.rollback().await.map_err(map_sqlx_error)?;
    DevelopmentSnapshot::new(
        context.scope,
        positive_u64(revision)?,
        revisions,
        workspaces,
    )
}

fn decode_revision(row: &sqlx::any::AnyRow) -> Result<DevelopmentRevisionEntry, DevelopmentError> {
    let digest: Vec<u8> = row
        .try_get("manifest_digest")
        .map_err(|_| DevelopmentError::Corruption)?;
    let digest: [u8; 32] = digest
        .try_into()
        .map_err(|_| DevelopmentError::Corruption)?;
    Ok(DevelopmentRevisionEntry {
        revision_id: parse_column(row, "revision_id")?,
        release_id: parse_column(row, "release_id")?,
        manifest_digest: Sha256Digest::from_bytes(digest),
        manifest_bytes: row
            .try_get("manifest_bytes")
            .map_err(|_| DevelopmentError::Corruption)?,
        actor: parse_column(row, "actor")?,
        created_at: TimestampMicros::new(
            row.try_get("created_at_micros")
                .map_err(|_| DevelopmentError::Corruption)?,
        ),
    })
}

fn decode_workspace(row: &sqlx::any::AnyRow) -> Result<WorkspaceBinding, DevelopmentError> {
    Ok(WorkspaceBinding {
        workspace_id: parse_column(row, "workspace_id")?,
        workspace_ref: parse_column(row, "workspace_ref")?,
        head_revision: row
            .try_get::<Option<String>, _>("head_revision_id")
            .map_err(|_| DevelopmentError::Corruption)?
            .map(|value| value.parse().map_err(|_| DevelopmentError::Corruption))
            .transpose()?,
        updated_by: parse_column(row, "updated_by")?,
        updated_at: TimestampMicros::new(
            row.try_get("updated_at_micros")
                .map_err(|_| DevelopmentError::Corruption)?,
        ),
    })
}

fn parse_column<T: FromStr>(row: &sqlx::any::AnyRow, name: &str) -> Result<T, DevelopmentError> {
    row.try_get::<String, _>(name)
        .map_err(|_| DevelopmentError::Corruption)?
        .parse()
        .map_err(|_| DevelopmentError::Corruption)
}

const fn command_time(command: &DevelopmentCommand) -> i64 {
    match command {
        DevelopmentCommand::CreateWorkspace { created_at, .. } => created_at.get(),
        DevelopmentCommand::PublishRevision { revision, .. } => revision.created_at.get(),
    }
}

async fn rollback<T>(
    transaction: Transaction<'_, Any>,
    error: DevelopmentError,
) -> Result<T, DevelopmentError> {
    transaction.rollback().await.map_err(map_sqlx_error)?;
    Err(error)
}

fn positive_u64(value: i64) -> Result<u64, DevelopmentError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(DevelopmentError::Corruption)
}

async fn migrate(pool: &AnyPool, backend: DevelopmentBackend) -> Result<(), DevelopmentError> {
    let mut transaction = pool.begin().await.map_err(map_sqlx_error)?;
    if backend == DevelopmentBackend::PostgreSQL {
        transaction
            .execute("SELECT pg_advisory_xact_lock(5920469865086687794)")
            .await
            .map_err(map_sqlx_error)?;
    }
    transaction.execute("CREATE TABLE IF NOT EXISTS runku_development_schema (singleton INTEGER PRIMARY KEY, version BIGINT NOT NULL)").await.map_err(map_sqlx_error)?;
    let version = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM runku_development_schema WHERE singleton = 1",
    )
    .fetch_optional(&mut *transaction)
    .await
    .map_err(map_sqlx_error)?;
    match version {
        None => {
            for statement in SCHEMA {
                transaction
                    .execute(*statement)
                    .await
                    .map_err(map_sqlx_error)?;
            }
            sqlx::query("INSERT INTO runku_development_schema (singleton, version) VALUES (1, $1)")
                .bind(SCHEMA_VERSION)
                .execute(&mut *transaction)
                .await
                .map_err(map_sqlx_error)?;
        }
        Some(SCHEMA_VERSION) => {
            for statement in SCHEMA {
                transaction
                    .execute(*statement)
                    .await
                    .map_err(map_sqlx_error)?;
            }
        }
        Some(_) => return Err(DevelopmentError::Unsupported),
    }
    transaction
        .commit()
        .await
        .map_err(|_| DevelopmentError::ResultUncertain)
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlx_error(error: sqlx::Error) -> DevelopmentError {
    match error {
        sqlx::Error::RowNotFound
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. } => DevelopmentError::Corruption,
        _ => DevelopmentError::Unavailable,
    }
}
