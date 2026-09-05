//! SQL Cron activation repository shared by `SQLite` and `PostgreSQL`.

use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{EnvironmentLocation, OperationId, WorkerId};
use runku_releases::CronName;
use runku_value::{TimestampMicros, decode_stored_value, encode_stored_value};
use sqlx::{
    Any, AnyPool, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

use crate::{
    ClaimedCronActivation, CronActivation, CronBackend, CronCommand, CronCommandResult,
    CronContext, CronError, CronRepository, CronSnapshot, CronTelemetrySnapshot,
    model::definitions,
};

const SCHEMA_VERSION: i64 = 2;
const MAX_CLAIM: u32 = 1_000;
const SCHEMA_V1: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_cron_environments (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, repository_revision BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id))",
    "CREATE TABLE IF NOT EXISTS runku_cron_activations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, cron_name TEXT NOT NULL, activation_revision BIGINT NOT NULL, pinned_code TEXT NOT NULL, release_id TEXT NOT NULL, schedule TEXT NOT NULL, function_name TEXT NOT NULL, args_bytes BYTEA NOT NULL, next_tick_micros BIGINT NOT NULL, lease_generation BIGINT NOT NULL, lease_owner TEXT NULL, lease_until_micros BIGINT NULL, updated_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, cron_name), FOREIGN KEY(project_id, environment_id) REFERENCES runku_cron_environments(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE INDEX IF NOT EXISTS runku_cron_due ON runku_cron_activations(project_id, environment_id, next_tick_micros, lease_until_micros, cron_name)",
    "CREATE TABLE IF NOT EXISTS runku_cron_operations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, command_digest BYTEA NOT NULL, repository_revision BIGINT NOT NULL, active_definitions BIGINT NOT NULL, created_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, operation_id), FOREIGN KEY(project_id, environment_id) REFERENCES runku_cron_environments(project_id, environment_id) ON DELETE CASCADE)",
];
const SCHEMA_V2: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_cron_disabled_definitions (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, cron_name TEXT NOT NULL, updated_revision BIGINT NOT NULL, updated_at_micros BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, cron_name), FOREIGN KEY(project_id, environment_id) REFERENCES runku_cron_environments(project_id, environment_id) ON DELETE CASCADE)",
];

/// Operational repository role, independent from Environment purpose.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CronRepositoryRole {
    /// Embedded single-process development/installation.
    Local,
    /// Shared authoritative multi-worker installation.
    Authoritative,
}

/// Bounded SQL pool configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CronRepositoryConfig {
    /// Explicit repository role.
    pub role: CronRepositoryRole,
    /// Maximum physical connections.
    pub max_connections: u32,
    /// Pool acquisition deadline.
    pub acquire_timeout: Duration,
}

impl CronRepositoryConfig {
    /// Deterministic local `SQLite` configuration.
    pub const LOCAL: Self = Self {
        role: CronRepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };
    /// Bounded `PostgreSQL` configuration.
    pub const AUTHORITATIVE: Self = Self {
        role: CronRepositoryRole::Authoritative,
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
    };
}

#[derive(Debug, Default)]
struct Counters {
    commands: AtomicU64,
    replays: AtomicU64,
    claims: AtomicU64,
    completions: AtomicU64,
    conflicts: AtomicU64,
    retryable_errors: AtomicU64,
}

/// Durable repository fixed to one trusted Environment context.
#[derive(Clone, Debug)]
pub struct SqlCronRepository {
    pool: AnyPool,
    backend: CronBackend,
    context: CronContext,
    counters: Arc<Counters>,
}

impl SqlCronRepository {
    /// Opens local `SQLite` after validating context, role, URL, and bounds.
    ///
    /// # Errors
    ///
    /// Rejects invalid configuration before creating the database file.
    pub async fn connect_sqlite(
        url: &str,
        config: CronRepositoryConfig,
        context: CronContext,
    ) -> Result<Self, CronError> {
        context.validate()?;
        if config.role != CronRepositoryRole::Local
            || context.environment.location() != EnvironmentLocation::Local
            || !url.starts_with("sqlite:")
        {
            return Err(CronError::Unsupported);
        }
        Self::connect(url, config, context, CronBackend::SQLite).await
    }

    /// Opens authoritative `PostgreSQL` 16+ after validating context, role, URL, and bounds.
    ///
    /// # Errors
    ///
    /// Rejects invalid configuration before network I/O.
    pub async fn connect_postgres(
        url: &str,
        config: CronRepositoryConfig,
        context: CronContext,
    ) -> Result<Self, CronError> {
        context.validate()?;
        if config.role != CronRepositoryRole::Authoritative
            || !(url.starts_with("postgres://") || url.starts_with("postgresql://"))
        {
            return Err(CronError::Unsupported);
        }
        Self::connect(url, config, context, CronBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: CronRepositoryConfig,
        context: CronContext,
        backend: CronBackend,
    ) -> Result<Self, CronError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || (backend == CronBackend::SQLite && config.max_connections != 1)
        {
            return Err(CronError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options = AnyConnectOptions::from_str(url).map_err(|_| CronError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    match backend {
                        CronBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        CronBackend::PostgreSQL => {
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
        if backend == CronBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(CronError::Unsupported);
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

    fn validate_context(&self, context: CronContext) -> Result<(), CronError> {
        context.validate()?;
        if context != self.context {
            return Err(CronError::InvalidInput);
        }
        Ok(())
    }

    fn record<T>(&self, result: &Result<T, CronError>) {
        match result {
            Err(CronError::Conflict | CronError::LeaseLost) => {
                self.counters.conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.retryable() => {
                self.counters
                    .retryable_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

#[async_trait]
impl CronRepository for SqlCronRepository {
    fn backend(&self) -> CronBackend {
        self.backend
    }

    async fn apply(
        &self,
        context: CronContext,
        operation_id: OperationId,
        command: &CronCommand,
    ) -> Result<CronCommandResult, CronError> {
        self.validate_context(context)?;
        let manifest = command.validate(context)?;
        let digest = command.digest(context)?;
        let result = apply_inner(
            &self.pool,
            self.backend,
            context,
            operation_id,
            command,
            manifest.as_ref(),
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
            Err(_) => self.record(&result),
        }
        result
    }

    async fn snapshot(&self, context: CronContext) -> Result<CronSnapshot, CronError> {
        self.validate_context(context)?;
        let result = snapshot_inner(&self.pool, self.backend, context).await;
        self.record(&result);
        result
    }

    async fn operation(
        &self,
        context: CronContext,
        operation_id: OperationId,
    ) -> Result<Option<CronCommandResult>, CronError> {
        self.validate_context(context)?;
        let row = sqlx::query(
            "SELECT repository_revision, active_definitions FROM runku_cron_operations \
             WHERE project_id = $1 AND environment_id = $2 AND operation_id = $3",
        )
        .bind(context.scope.project_id().to_string())
        .bind(context.scope.environment_id().to_string())
        .bind(operation_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        row.map(|row| {
            Ok(CronCommandResult {
                repository_revision: positive_u64(
                    row.try_get("repository_revision")
                        .map_err(|_| CronError::Corruption)?,
                )?,
                active_definitions: nonnegative_u32(
                    row.try_get("active_definitions")
                        .map_err(|_| CronError::Corruption)?,
                )?,
                replayed: true,
            })
        })
        .transpose()
    }

    async fn claim_due(
        &self,
        context: CronContext,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<Vec<ClaimedCronActivation>, CronError> {
        self.validate_context(context)?;
        if now.get() < 0 || lease_until <= now || limit == 0 || limit > MAX_CLAIM {
            return Err(CronError::InvalidInput);
        }
        let result = claim_inner(&self.pool, context, worker_id, now, lease_until, limit).await;
        if let Ok(claimed) = &result {
            self.counters.claims.fetch_add(
                u64::try_from(claimed.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        } else {
            self.record(&result);
        }
        result
    }

    async fn complete_tick(
        &self,
        context: CronContext,
        name: &CronName,
        worker_id: WorkerId,
        lease_generation: u64,
        expected_tick: TimestampMicros,
        next_tick: TimestampMicros,
        completed_at: TimestampMicros,
    ) -> Result<(), CronError> {
        self.validate_context(context)?;
        if lease_generation == 0
            || expected_tick.get() < 0
            || next_tick <= expected_tick
            || completed_at.get() < 0
        {
            return Err(CronError::InvalidInput);
        }
        let result = complete_inner(
            &self.pool,
            context,
            name,
            worker_id,
            lease_generation,
            expected_tick,
            next_tick,
            completed_at,
        )
        .await;
        if result.is_ok() {
            self.counters.completions.fetch_add(1, Ordering::Relaxed);
        } else {
            self.record(&result);
        }
        result
    }

    async fn health(&self) -> Result<(), CronError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_error)
    }

    fn telemetry(&self) -> CronTelemetrySnapshot {
        CronTelemetrySnapshot {
            commands: self.counters.commands.load(Ordering::Relaxed),
            replays: self.counters.replays.load(Ordering::Relaxed),
            claims: self.counters.claims.load(Ordering::Relaxed),
            completions: self.counters.completions.load(Ordering::Relaxed),
            conflicts: self.counters.conflicts.load(Ordering::Relaxed),
            retryable_errors: self.counters.retryable_errors.load(Ordering::Relaxed),
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn apply_inner(
    pool: &AnyPool,
    backend: CronBackend,
    context: CronContext,
    operation_id: OperationId,
    command: &CronCommand,
    manifest: Option<&runku_releases::ReleaseManifestV1>,
    digest: [u8; 32],
) -> Result<CronCommandResult, CronError> {
    let mut transaction = pool.begin().await.map_err(map_sqlx_error)?;
    let project = context.scope.project_id().to_string();
    let environment = context.scope.environment_id().to_string();
    sqlx::query("INSERT INTO runku_cron_environments(project_id, environment_id, repository_revision) VALUES ($1, $2, 0) ON CONFLICT(project_id, environment_id) DO NOTHING").bind(&project).bind(&environment).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
    let lock = if backend == CronBackend::PostgreSQL {
        "SELECT repository_revision FROM runku_cron_environments WHERE project_id = $1 AND environment_id = $2 FOR UPDATE"
    } else {
        "SELECT repository_revision FROM runku_cron_environments WHERE project_id = $1 AND environment_id = $2"
    };
    let current: i64 = sqlx::query_scalar(lock)
        .bind(&project)
        .bind(&environment)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
    if let Some(row) = sqlx::query("SELECT command_digest, repository_revision, active_definitions FROM runku_cron_operations WHERE project_id = $1 AND environment_id = $2 AND operation_id = $3").bind(&project).bind(&environment).bind(operation_id.to_string()).fetch_optional(&mut *transaction).await.map_err(map_sqlx_error)? {
        let stored: Vec<u8> = row.try_get("command_digest").map_err(|_| CronError::Corruption)?;
        if stored.as_slice() != digest { return rollback(transaction, CronError::Conflict).await; }
        let result = CronCommandResult { repository_revision: positive_u64(row.try_get("repository_revision").map_err(|_| CronError::Corruption)?)?, active_definitions: nonnegative_u32(row.try_get("active_definitions").map_err(|_| CronError::Corruption)?)?, replayed: true };
        transaction.rollback().await.map_err(map_sqlx_error)?;
        return Ok(result);
    }
    if nonnegative_u64(current)? != command.expected_revision() {
        return rollback(transaction, CronError::Conflict).await;
    }
    let next = current.checked_add(1).ok_or(CronError::LimitExceeded)?;
    match (command, manifest) {
        (
            CronCommand::ActivateManifest {
                pinned_code,
                activated_at,
                ..
            },
            Some(manifest),
        ) => {
            delete_all(&mut transaction, &project, &environment).await?;
            for definition in definitions(manifest) {
                let disabled = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runku_cron_disabled_definitions WHERE project_id = $1 AND environment_id = $2 AND cron_name = $3")
                    .bind(&project)
                    .bind(&environment)
                    .bind(definition.name.as_str())
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(map_sqlx_error)?;
                if disabled != 0 {
                    continue;
                }
                insert_activation(
                    &mut transaction,
                    &project,
                    &environment,
                    next,
                    *pinned_code,
                    manifest.release_id,
                    definition,
                    *activated_at,
                )
                .await?;
            }
        }
        (CronCommand::DeactivateAll { .. }, None) => {
            delete_all(&mut transaction, &project, &environment).await?;
        }
        (
            CronCommand::SetDefinition {
                pinned_code,
                name,
                enabled,
                changed_at,
                ..
            },
            Some(manifest),
        ) => {
            sqlx::query("DELETE FROM runku_cron_activations WHERE project_id = $1 AND environment_id = $2 AND cron_name = $3")
                .bind(&project)
                .bind(&environment)
                .bind(name.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(map_sqlx_error)?;
            if *enabled {
                sqlx::query("DELETE FROM runku_cron_disabled_definitions WHERE project_id = $1 AND environment_id = $2 AND cron_name = $3")
                    .bind(&project)
                    .bind(&environment)
                    .bind(name.as_str())
                    .execute(&mut *transaction)
                    .await
                    .map_err(map_sqlx_error)?;
                let definition = definitions(manifest)
                    .iter()
                    .find(|definition| definition.name == *name)
                    .ok_or(CronError::InvalidManifest)?;
                insert_activation(
                    &mut transaction,
                    &project,
                    &environment,
                    next,
                    *pinned_code,
                    manifest.release_id,
                    definition,
                    *changed_at,
                )
                .await?;
            } else {
                sqlx::query("INSERT INTO runku_cron_disabled_definitions(project_id, environment_id, cron_name, updated_revision, updated_at_micros) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(project_id, environment_id, cron_name) DO UPDATE SET updated_revision = excluded.updated_revision, updated_at_micros = excluded.updated_at_micros")
                    .bind(&project)
                    .bind(&environment)
                    .bind(name.as_str())
                    .bind(next)
                    .bind(changed_at.get())
                    .execute(&mut *transaction)
                    .await
                    .map_err(map_sqlx_error)?;
            }
        }
        _ => return rollback(transaction, CronError::Corruption).await,
    }
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM runku_cron_activations WHERE project_id = $1 AND environment_id = $2",
    )
    .bind(&project)
    .bind(&environment)
    .fetch_one(&mut *transaction)
    .await
    .map_err(map_sqlx_error)
    .and_then(nonnegative_u32)?;
    sqlx::query("UPDATE runku_cron_environments SET repository_revision = $1 WHERE project_id = $2 AND environment_id = $3").bind(next).bind(&project).bind(&environment).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
    sqlx::query("INSERT INTO runku_cron_operations(project_id, environment_id, operation_id, command_digest, repository_revision, active_definitions, created_at_micros) VALUES ($1,$2,$3,$4,$5,$6,$7)").bind(&project).bind(&environment).bind(operation_id.to_string()).bind(digest.to_vec()).bind(next).bind(i64::from(count)).bind(command_time(command)).execute(&mut *transaction).await.map_err(map_sqlx_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| CronError::ResultUncertain)?;
    Ok(CronCommandResult {
        repository_revision: positive_u64(next)?,
        active_definitions: count,
        replayed: false,
    })
}

async fn delete_all(
    transaction: &mut Transaction<'_, Any>,
    project: &str,
    environment: &str,
) -> Result<(), CronError> {
    sqlx::query("DELETE FROM runku_cron_activations WHERE project_id = $1 AND environment_id = $2")
        .bind(project)
        .bind(environment)
        .execute(&mut **transaction)
        .await
        .map(|_| ())
        .map_err(map_sqlx_error)
}

#[allow(clippy::too_many_arguments)]
async fn insert_activation(
    transaction: &mut Transaction<'_, Any>,
    project: &str,
    environment: &str,
    revision: i64,
    pinned_code: runku_core::PinnedCode,
    release_id: runku_core::ReleaseId,
    definition: &runku_releases::CronDefinition,
    changed_at: TimestampMicros,
) -> Result<(), CronError> {
    let next_tick = definition
        .schedule
        .next_after(changed_at)
        .map_err(|_| CronError::InvalidManifest)?;
    let args = encode_stored_value(&definition.args).map_err(|_| CronError::InvalidManifest)?;
    sqlx::query("INSERT INTO runku_cron_activations(project_id, environment_id, cron_name, activation_revision, pinned_code, release_id, schedule, function_name, args_bytes, next_tick_micros, lease_generation, lease_owner, lease_until_micros, updated_at_micros) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,NULL,NULL,$11)")
        .bind(project)
        .bind(environment)
        .bind(definition.name.as_str())
        .bind(revision)
        .bind(pinned_code.to_string())
        .bind(release_id.to_string())
        .bind(definition.schedule.as_str())
        .bind(definition.function.as_str())
        .bind(args)
        .bind(next_tick.get())
        .bind(changed_at.get())
        .execute(&mut **transaction)
        .await
        .map(|_| ())
        .map_err(map_sqlx_error)
}

async fn snapshot_inner(
    pool: &AnyPool,
    backend: CronBackend,
    context: CronContext,
) -> Result<CronSnapshot, CronError> {
    let mut transaction = pool.begin().await.map_err(map_sqlx_error)?;
    if backend == CronBackend::PostgreSQL {
        transaction
            .execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(map_sqlx_error)?;
    }
    let project = context.scope.project_id().to_string();
    let environment = context.scope.environment_id().to_string();
    let revision = sqlx::query_scalar::<_, i64>("SELECT repository_revision FROM runku_cron_environments WHERE project_id = $1 AND environment_id = $2").bind(&project).bind(&environment).fetch_optional(&mut *transaction).await.map_err(map_sqlx_error)?.unwrap_or(0);
    let rows = sqlx::query("SELECT cron_name, activation_revision, pinned_code, release_id, schedule, function_name, args_bytes, next_tick_micros, lease_generation, lease_owner, lease_until_micros FROM runku_cron_activations WHERE project_id = $1 AND environment_id = $2 ORDER BY cron_name").bind(&project).bind(&environment).fetch_all(&mut *transaction).await.map_err(map_sqlx_error)?;
    let activations = rows
        .iter()
        .map(decode_activation)
        .collect::<Result<Vec<_>, _>>()?;
    let disabled_definitions = sqlx::query_scalar::<_, String>("SELECT cron_name FROM runku_cron_disabled_definitions WHERE project_id = $1 AND environment_id = $2 ORDER BY cron_name")
        .bind(&project)
        .bind(&environment)
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?
        .into_iter()
        .map(|name| name.parse().map_err(|_| CronError::Corruption))
        .collect::<Result<Vec<_>, _>>()?;
    transaction.rollback().await.map_err(map_sqlx_error)?;
    let snapshot = CronSnapshot {
        repository_revision: nonnegative_u64(revision)?,
        activations,
        disabled_definitions,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

async fn claim_inner(
    pool: &AnyPool,
    context: CronContext,
    worker: WorkerId,
    now: TimestampMicros,
    lease_until: TimestampMicros,
    limit: u32,
) -> Result<Vec<ClaimedCronActivation>, CronError> {
    let mut transaction = pool.begin().await.map_err(map_sqlx_error)?;
    let project = context.scope.project_id().to_string();
    let environment = context.scope.environment_id().to_string();
    let names = sqlx::query_scalar::<_, String>("SELECT cron_name FROM runku_cron_activations WHERE project_id = $1 AND environment_id = $2 AND next_tick_micros <= $3 AND (lease_owner IS NULL OR lease_until_micros <= $3) ORDER BY next_tick_micros, cron_name LIMIT $4").bind(&project).bind(&environment).bind(now.get()).bind(i64::from(limit)).fetch_all(&mut *transaction).await.map_err(map_sqlx_error)?;
    let mut claimed = Vec::with_capacity(names.len());
    for name in names {
        let row = sqlx::query("UPDATE runku_cron_activations SET lease_generation = lease_generation + 1, lease_owner = $1, lease_until_micros = $2, updated_at_micros = $3 WHERE project_id = $4 AND environment_id = $5 AND cron_name = $6 AND next_tick_micros <= $3 AND (lease_owner IS NULL OR lease_until_micros <= $3) RETURNING cron_name, activation_revision, pinned_code, release_id, schedule, function_name, args_bytes, next_tick_micros, lease_generation, lease_owner, lease_until_micros").bind(worker.to_string()).bind(lease_until.get()).bind(now.get()).bind(&project).bind(&environment).bind(&name).fetch_optional(&mut *transaction).await.map_err(map_sqlx_error)?;
        if let Some(row) = row {
            claimed.push(ClaimedCronActivation {
                activation: decode_activation(&row)?,
            });
        }
    }
    transaction
        .commit()
        .await
        .map_err(|_| CronError::ResultUncertain)?;
    Ok(claimed)
}

#[allow(clippy::too_many_arguments)]
async fn complete_inner(
    pool: &AnyPool,
    context: CronContext,
    name: &CronName,
    worker: WorkerId,
    generation: u64,
    expected: TimestampMicros,
    next: TimestampMicros,
    completed: TimestampMicros,
) -> Result<(), CronError> {
    let result = sqlx::query("UPDATE runku_cron_activations SET next_tick_micros = $1, lease_owner = NULL, lease_until_micros = NULL, updated_at_micros = $2 WHERE project_id = $3 AND environment_id = $4 AND cron_name = $5 AND lease_owner = $6 AND lease_generation = $7 AND next_tick_micros = $8")
        .bind(next.get()).bind(completed.get()).bind(context.scope.project_id().to_string()).bind(context.scope.environment_id().to_string()).bind(name.as_str()).bind(worker.to_string()).bind(positive_i64(generation)?).bind(expected.get()).execute(pool).await.map_err(map_sqlx_error)?;
    if result.rows_affected() != 1 {
        return Err(CronError::LeaseLost);
    }
    Ok(())
}

fn decode_activation(row: &sqlx::any::AnyRow) -> Result<CronActivation, CronError> {
    let activation = CronActivation {
        name: parse_column(row, "cron_name")?,
        activation_revision: positive_u64(
            row.try_get("activation_revision")
                .map_err(|_| CronError::Corruption)?,
        )?,
        pinned_code: parse_column(row, "pinned_code")?,
        release_id: parse_column(row, "release_id")?,
        schedule: parse_column(row, "schedule")?,
        function: parse_column(row, "function_name")?,
        args: decode_stored_value(
            &row.try_get::<Vec<u8>, _>("args_bytes")
                .map_err(|_| CronError::Corruption)?,
        )
        .map_err(|_| CronError::Corruption)?,
        next_tick: TimestampMicros::new(
            row.try_get("next_tick_micros")
                .map_err(|_| CronError::Corruption)?,
        ),
        lease_generation: nonnegative_u64(
            row.try_get("lease_generation")
                .map_err(|_| CronError::Corruption)?,
        )?,
        lease_owner: row
            .try_get::<Option<String>, _>("lease_owner")
            .map_err(|_| CronError::Corruption)?
            .map(|value| value.parse().map_err(|_| CronError::Corruption))
            .transpose()?,
        lease_until: row
            .try_get::<Option<i64>, _>("lease_until_micros")
            .map_err(|_| CronError::Corruption)?
            .map(TimestampMicros::new),
    };
    activation.validate()?;
    Ok(activation)
}

fn parse_column<T: FromStr>(row: &sqlx::any::AnyRow, name: &str) -> Result<T, CronError> {
    row.try_get::<String, _>(name)
        .map_err(|_| CronError::Corruption)?
        .parse()
        .map_err(|_| CronError::Corruption)
}
const fn command_time(command: &CronCommand) -> i64 {
    match command {
        CronCommand::ActivateManifest { activated_at, .. } => activated_at.get(),
        CronCommand::DeactivateAll { deactivated_at, .. } => deactivated_at.get(),
        CronCommand::SetDefinition { changed_at, .. } => changed_at.get(),
    }
}
async fn rollback<T>(transaction: Transaction<'_, Any>, error: CronError) -> Result<T, CronError> {
    transaction.rollback().await.map_err(map_sqlx_error)?;
    Err(error)
}
fn positive_i64(value: u64) -> Result<i64, CronError> {
    i64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(CronError::LimitExceeded)
}
fn positive_u64(value: i64) -> Result<u64, CronError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(CronError::Corruption)
}
fn nonnegative_u64(value: i64) -> Result<u64, CronError> {
    u64::try_from(value).map_err(|_| CronError::Corruption)
}
fn nonnegative_u32(value: i64) -> Result<u32, CronError> {
    u32::try_from(value).map_err(|_| CronError::Corruption)
}

async fn migrate(pool: &AnyPool, backend: CronBackend) -> Result<(), CronError> {
    let mut transaction = pool.begin().await.map_err(map_sqlx_error)?;
    if backend == CronBackend::PostgreSQL {
        transaction
            .execute("SELECT pg_advisory_xact_lock(4850189907717716907)")
            .await
            .map_err(map_sqlx_error)?;
    }
    transaction.execute("CREATE TABLE IF NOT EXISTS runku_cron_schema(singleton INTEGER PRIMARY KEY, version BIGINT NOT NULL)").await.map_err(map_sqlx_error)?;
    let version =
        sqlx::query_scalar::<_, i64>("SELECT version FROM runku_cron_schema WHERE singleton = 1")
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
    match version {
        None => {
            for statement in SCHEMA_V1.iter().chain(SCHEMA_V2) {
                transaction
                    .execute(*statement)
                    .await
                    .map_err(map_sqlx_error)?;
            }
            sqlx::query("INSERT INTO runku_cron_schema(singleton, version) VALUES (1, $1)")
                .bind(SCHEMA_VERSION)
                .execute(&mut *transaction)
                .await
                .map_err(map_sqlx_error)?;
        }
        Some(1) => {
            for statement in SCHEMA_V2 {
                transaction
                    .execute(*statement)
                    .await
                    .map_err(map_sqlx_error)?;
            }
            sqlx::query("UPDATE runku_cron_schema SET version = $1 WHERE singleton = 1")
                .bind(SCHEMA_VERSION)
                .execute(&mut *transaction)
                .await
                .map_err(map_sqlx_error)?;
        }
        Some(SCHEMA_VERSION) => {
            for statement in SCHEMA_V1.iter().chain(SCHEMA_V2) {
                transaction
                    .execute(*statement)
                    .await
                    .map_err(map_sqlx_error)?;
            }
        }
        Some(_) => return Err(CronError::Unsupported),
    }
    transaction
        .commit()
        .await
        .map_err(|_| CronError::ResultUncertain)
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlx_error(error: sqlx::Error) -> CronError {
    match error {
        sqlx::Error::RowNotFound
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. } => CronError::Corruption,
        _ => CronError::Unavailable,
    }
}
