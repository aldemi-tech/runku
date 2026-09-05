//! SQL-backed Environment repository with identical SQLite/PostgreSQL semantics.

use std::{
    fmt::Write as _,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_core::{
    EnvironmentLocation, EnvironmentProtection, EnvironmentPurpose, EnvironmentScope, OperationId,
    ProjectId,
};
use runku_environments::{
    Environment, EnvironmentCommand, EnvironmentError, EnvironmentLifecycle, EnvironmentOperation,
    EnvironmentOperationResult, EnvironmentPage, EnvironmentPageRequest, EnvironmentRepository,
    EnvironmentRepositoryBackend, EnvironmentRepositoryTelemetrySnapshot,
};
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};
use sqlx::{
    Any, AnyPool, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

const MIGRATION_1: &[&str] = &[
    "CREATE TABLE runku_environments (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, name TEXT NOT NULL, slug TEXT NOT NULL, region TEXT NOT NULL, purpose TEXT NOT NULL CHECK(purpose IN ('development','preview','staging','production')), protection TEXT NOT NULL CHECK(protection IN ('open','protected','production')), location TEXT NOT NULL CHECK(location IN ('local','managed','self_hosted')), workspace_targets_enabled BIGINT NOT NULL CHECK(workspace_targets_enabled IN (0,1)), configuration_revision BIGINT NOT NULL CHECK(configuration_revision > 0), desired_state TEXT NOT NULL CHECK(desired_state IN ('active','archived')), observed_state TEXT NOT NULL CHECK(observed_state IN ('pending','ready','failed')), observed_configuration_revision BIGINT NULL CHECK(observed_configuration_revision IS NULL OR (observed_configuration_revision > 0 AND observed_configuration_revision <= configuration_revision)), created_at_micros BIGINT NOT NULL CHECK(created_at_micros >= 0), updated_at_micros BIGINT NOT NULL CHECK(updated_at_micros >= created_at_micros), observed_at_micros BIGINT NULL CHECK(observed_at_micros IS NULL OR observed_at_micros >= created_at_micros), PRIMARY KEY(project_id, environment_id), UNIQUE(project_id, slug), CHECK((observed_configuration_revision IS NULL) = (observed_at_micros IS NULL)), CHECK(observed_state = 'pending' OR observed_at_micros >= updated_at_micros), CHECK((observed_state = 'pending' AND (observed_configuration_revision IS NULL OR observed_configuration_revision < configuration_revision)) OR (observed_state IN ('ready','failed') AND observed_configuration_revision = configuration_revision)))",
    "CREATE TABLE runku_environment_operations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, command_digest BYTEA NOT NULL CHECK(length(command_digest) = 32), kind TEXT NOT NULL CHECK(kind IN ('create','update','materialize')), configuration_revision BIGINT NOT NULL CHECK(configuration_revision > 0), desired_state TEXT NOT NULL CHECK(desired_state IN ('active','archived')), observed_state TEXT NOT NULL CHECK(observed_state IN ('pending','ready','failed')), observed_configuration_revision BIGINT NULL CHECK(observed_configuration_revision IS NULL OR (observed_configuration_revision > 0 AND observed_configuration_revision <= configuration_revision)), completed_at_micros BIGINT NOT NULL CHECK(completed_at_micros >= 0), PRIMARY KEY(project_id, environment_id, operation_id), FOREIGN KEY(project_id, environment_id) REFERENCES runku_environments(project_id, environment_id) ON DELETE RESTRICT)",
    "CREATE INDEX runku_environments_by_project_id ON runku_environments(project_id, environment_id)",
];
const MIGRATIONS: &[(i64, &[&str])] = &[(1, MIGRATION_1)];

/// Operational role selected for repository composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryRole {
    /// Local/test SQLite repository.
    Local,
    /// Authoritative production PostgreSQL repository.
    Production,
}

/// Bounded connection pool and acquisition policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnvironmentRepositoryConfig {
    /// Declared operational role.
    pub role: RepositoryRole,
    /// Maximum physical connections.
    pub max_connections: u32,
    /// Maximum wait for a pool connection.
    pub acquire_timeout: Duration,
}

impl EnvironmentRepositoryConfig {
    /// Deterministic local/test SQLite configuration.
    pub const LOCAL: Self = Self {
        role: RepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };

    /// Bounded authoritative PostgreSQL configuration.
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
    reads: AtomicU64,
    lists: AtomicU64,
    operation_reads: AtomicU64,
    retryable_errors: AtomicU64,
}

/// Durable SQL Environment repository.
#[derive(Clone, Debug)]
pub struct SqlEnvironmentRepository {
    pool: AnyPool,
    backend: EnvironmentRepositoryBackend,
    counters: Arc<Counters>,
}

impl SqlEnvironmentRepository {
    /// Connects local SQLite and applies checksum-protected append-only migrations.
    ///
    /// # Errors
    ///
    /// Rejects production role, unsafe pool settings, or unavailable/corrupt storage.
    pub async fn connect_sqlite(
        url: &str,
        config: EnvironmentRepositoryConfig,
    ) -> Result<Self, EnvironmentError> {
        if config.role == RepositoryRole::Production {
            return Err(EnvironmentError::ProductionBackendUnsupported);
        }
        if !url.starts_with("sqlite:") {
            return Err(EnvironmentError::Unavailable);
        }
        Self::connect(url, config, EnvironmentRepositoryBackend::SQLite).await
    }

    /// Connects PostgreSQL 16+ and applies checksum-protected append-only migrations.
    ///
    /// # Errors
    ///
    /// Rejects local role, unsupported PostgreSQL, unsafe pool settings, or storage failures.
    pub async fn connect_postgres(
        url: &str,
        config: EnvironmentRepositoryConfig,
    ) -> Result<Self, EnvironmentError> {
        if config.role != RepositoryRole::Production {
            return Err(EnvironmentError::ProductionBackendUnsupported);
        }
        if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            return Err(EnvironmentError::Unavailable);
        }
        Self::connect(url, config, EnvironmentRepositoryBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: EnvironmentRepositoryConfig,
        backend: EnvironmentRepositoryBackend,
    ) -> Result<Self, EnvironmentError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || (backend == EnvironmentRepositoryBackend::SQLite && config.max_connections != 1)
        {
            return Err(EnvironmentError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options =
            AnyConnectOptions::from_str(url).map_err(|_| EnvironmentError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _metadata| {
                Box::pin(async move {
                    match backend {
                        EnvironmentRepositoryBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        EnvironmentRepositoryBackend::PostgreSQL => {
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
        if backend == EnvironmentRepositoryBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(EnvironmentError::Unsupported);
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

#[async_trait]
impl EnvironmentRepository for SqlEnvironmentRepository {
    fn backend(&self) -> EnvironmentRepositoryBackend {
        self.backend
    }

    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &EnvironmentCommand,
    ) -> Result<EnvironmentOperationResult, EnvironmentError> {
        let result = apply(&self.pool, self.backend, scope, operation_id, command).await;
        match &result {
            Ok(result) if result.replayed => {
                self.counters.replays.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => {
                self.counters.commands.fetch_add(1, Ordering::Relaxed);
            }
            Err(EnvironmentError::Conflict | EnvironmentError::OperationIdReused) => {
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

    async fn get(&self, scope: EnvironmentScope) -> Result<Option<Environment>, EnvironmentError> {
        let result = load_environment(&self.pool, scope).await;
        if result.is_ok() {
            self.counters.reads.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn list(
        &self,
        project_id: ProjectId,
        request: EnvironmentPageRequest,
    ) -> Result<EnvironmentPage, EnvironmentError> {
        validate_page_request(request)?;
        let result = list_environments(&self.pool, project_id, request).await;
        if result.is_ok() {
            self.counters.lists.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<EnvironmentOperation>, EnvironmentError> {
        let result = load_operation(&self.pool, scope, operation_id)
            .await
            .map(|value| value.map(|stored| stored.operation));
        if result.is_ok() {
            self.counters
                .operation_reads
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn health(&self) -> Result<(), EnvironmentError> {
        sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_error)
    }

    fn telemetry(&self) -> EnvironmentRepositoryTelemetrySnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        EnvironmentRepositoryTelemetrySnapshot {
            commands: load(&self.counters.commands),
            replays: load(&self.counters.replays),
            conflicts: load(&self.counters.conflicts),
            reads: load(&self.counters.reads),
            lists: load(&self.counters.lists),
            operation_reads: load(&self.counters.operation_reads),
            retryable_errors: load(&self.counters.retryable_errors),
            pool_size: self.pool.size(),
            pool_idle: u32::try_from(self.pool.num_idle()).unwrap_or(u32::MAX),
        }
    }
}

#[derive(Debug)]
struct StoredOperation {
    digest: Vec<u8>,
    operation: EnvironmentOperation,
}

async fn apply(
    pool: &AnyPool,
    backend: EnvironmentRepositoryBackend,
    scope: EnvironmentScope,
    operation_id: OperationId,
    command: &EnvironmentCommand,
) -> Result<EnvironmentOperationResult, EnvironmentError> {
    let digest = command.digest(scope)?;
    let mut transaction = begin_write(pool, backend).await?;
    if let Some(stored) = load_operation_tx(&mut transaction, scope, operation_id).await? {
        if stored.digest.as_slice() != digest {
            return rollback(transaction, EnvironmentError::OperationIdReused).await;
        }
        transaction.commit().await.map_err(map_commit_error)?;
        return Ok(EnvironmentOperationResult {
            operation: stored.operation,
            replayed: true,
        });
    }

    let next = match command {
        EnvironmentCommand::Create {
            configuration,
            created_at,
        } => {
            let next = EnvironmentLifecycle::create(scope, configuration.clone(), *created_at)?;
            insert_environment(&mut transaction, &next).await?;
            next
        }
        EnvironmentCommand::Update {
            expected_revision,
            configuration,
            updated_at,
        } => {
            let current = load_environment_tx(&mut transaction, backend, scope)
                .await?
                .ok_or(EnvironmentError::NotFound)?;
            let next = EnvironmentLifecycle::update(
                &current,
                *expected_revision,
                configuration.clone(),
                *updated_at,
            )?;
            update_environment(&mut transaction, &next, *expected_revision).await?;
            next
        }
        EnvironmentCommand::Materialize {
            expected_revision,
            outcome,
            observed_at,
        } => {
            let current = load_environment_tx(&mut transaction, backend, scope)
                .await?
                .ok_or(EnvironmentError::NotFound)?;
            let next = EnvironmentLifecycle::materialize(
                &current,
                *expected_revision,
                *outcome,
                *observed_at,
            )?;
            update_observation(&mut transaction, &next, *expected_revision).await?;
            next
        }
    };
    let completed_at = command_time(command);
    let operation = EnvironmentOperation {
        scope,
        operation_id,
        kind: command.kind(),
        configuration_revision: next.configuration_revision,
        desired_state: next.desired_state,
        observed_state: next.observed_state,
        observed_configuration_revision: next.observed_configuration_revision,
        completed_at,
    };
    operation.validate()?;
    insert_operation(&mut transaction, &operation, digest).await?;
    transaction.commit().await.map_err(map_commit_error)?;
    Ok(EnvironmentOperationResult {
        operation,
        replayed: false,
    })
}

async fn insert_environment(
    transaction: &mut Transaction<'_, Any>,
    environment: &Environment,
) -> Result<(), EnvironmentError> {
    let result = sqlx::query("INSERT INTO runku_environments(project_id, environment_id, name, slug, region, purpose, protection, location, workspace_targets_enabled, configuration_revision, desired_state, observed_state, observed_configuration_revision, created_at_micros, updated_at_micros, observed_at_micros) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)")
        .bind(environment.scope.project_id().to_string())
        .bind(environment.scope.environment_id().to_string())
        .bind(environment.configuration.name.as_str())
        .bind(environment.configuration.slug.as_str())
        .bind(environment.configuration.region.as_str())
        .bind(encode_purpose(environment.configuration.purpose))
        .bind(encode_protection(environment.configuration.protection))
        .bind(encode_location(environment.configuration.location))
        .bind(i64::from(environment.configuration.workspace_targets_enabled))
        .bind(to_i64(environment.configuration_revision)?)
        .bind(environment.desired_state.as_str())
        .bind(environment.observed_state.as_str())
        .bind(optional_revision(environment.observed_configuration_revision)?)
        .bind(environment.created_at.get())
        .bind(environment.updated_at.get())
        .bind(environment.observed_at.map(TimestampMicros::get))
        .execute(&mut **transaction)
        .await;
    match result {
        Ok(value) if value.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(EnvironmentError::Corruption),
        Err(error) => Err(map_constraint_error(error)),
    }
}

async fn update_environment(
    transaction: &mut Transaction<'_, Any>,
    environment: &Environment,
    expected_revision: u64,
) -> Result<(), EnvironmentError> {
    let result = sqlx::query("UPDATE runku_environments SET name=$1, slug=$2, region=$3, purpose=$4, protection=$5, location=$6, workspace_targets_enabled=$7, configuration_revision=$8, desired_state=$9, observed_state=$10, observed_configuration_revision=$11, updated_at_micros=$12, observed_at_micros=$13 WHERE project_id=$14 AND environment_id=$15 AND configuration_revision=$16")
        .bind(environment.configuration.name.as_str())
        .bind(environment.configuration.slug.as_str())
        .bind(environment.configuration.region.as_str())
        .bind(encode_purpose(environment.configuration.purpose))
        .bind(encode_protection(environment.configuration.protection))
        .bind(encode_location(environment.configuration.location))
        .bind(i64::from(environment.configuration.workspace_targets_enabled))
        .bind(to_i64(environment.configuration_revision)?)
        .bind(environment.desired_state.as_str())
        .bind(environment.observed_state.as_str())
        .bind(optional_revision(environment.observed_configuration_revision)?)
        .bind(environment.updated_at.get())
        .bind(environment.observed_at.map(TimestampMicros::get))
        .bind(environment.scope.project_id().to_string())
        .bind(environment.scope.environment_id().to_string())
        .bind(to_i64(expected_revision)?)
        .execute(&mut **transaction)
        .await;
    match result {
        Ok(value) if value.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(EnvironmentError::Conflict),
        Err(error) => Err(map_constraint_error(error)),
    }
}

async fn update_observation(
    transaction: &mut Transaction<'_, Any>,
    environment: &Environment,
    expected_revision: u64,
) -> Result<(), EnvironmentError> {
    let result = sqlx::query("UPDATE runku_environments SET observed_state=$1, observed_configuration_revision=$2, observed_at_micros=$3 WHERE project_id=$4 AND environment_id=$5 AND configuration_revision=$6")
        .bind(environment.observed_state.as_str())
        .bind(optional_revision(environment.observed_configuration_revision)?)
        .bind(environment.observed_at.map(TimestampMicros::get))
        .bind(environment.scope.project_id().to_string())
        .bind(environment.scope.environment_id().to_string())
        .bind(to_i64(expected_revision)?)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    if result.rows_affected() != 1 {
        return Err(EnvironmentError::Conflict);
    }
    Ok(())
}

async fn insert_operation(
    transaction: &mut Transaction<'_, Any>,
    operation: &EnvironmentOperation,
    digest: [u8; 32],
) -> Result<(), EnvironmentError> {
    sqlx::query("INSERT INTO runku_environment_operations(project_id, environment_id, operation_id, command_digest, kind, configuration_revision, desired_state, observed_state, observed_configuration_revision, completed_at_micros) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind(operation.scope.project_id().to_string())
        .bind(operation.scope.environment_id().to_string())
        .bind(operation.operation_id.to_string())
        .bind(digest.to_vec())
        .bind(operation.kind.as_str())
        .bind(to_i64(operation.configuration_revision)?)
        .bind(operation.desired_state.as_str())
        .bind(operation.observed_state.as_str())
        .bind(optional_revision(operation.observed_configuration_revision)?)
        .bind(operation.completed_at.get())
        .execute(&mut **transaction)
        .await
        .map_err(map_constraint_error)?;
    Ok(())
}

async fn load_environment(
    pool: &AnyPool,
    scope: EnvironmentScope,
) -> Result<Option<Environment>, EnvironmentError> {
    let row = sqlx::query("SELECT name, slug, region, purpose, protection, location, workspace_targets_enabled, configuration_revision, desired_state, observed_state, observed_configuration_revision, created_at_micros, updated_at_micros, observed_at_micros FROM runku_environments WHERE project_id=$1 AND environment_id=$2")
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;
    row.map(|value| decode_environment(scope, &value))
        .transpose()
}

async fn load_environment_tx(
    transaction: &mut Transaction<'_, Any>,
    backend: EnvironmentRepositoryBackend,
    scope: EnvironmentScope,
) -> Result<Option<Environment>, EnvironmentError> {
    let statement = if backend == EnvironmentRepositoryBackend::PostgreSQL {
        "SELECT name, slug, region, purpose, protection, location, workspace_targets_enabled, configuration_revision, desired_state, observed_state, observed_configuration_revision, created_at_micros, updated_at_micros, observed_at_micros FROM runku_environments WHERE project_id=$1 AND environment_id=$2 FOR UPDATE"
    } else {
        "SELECT name, slug, region, purpose, protection, location, workspace_targets_enabled, configuration_revision, desired_state, observed_state, observed_configuration_revision, created_at_micros, updated_at_micros, observed_at_micros FROM runku_environments WHERE project_id=$1 AND environment_id=$2"
    };
    let row = sqlx::query(statement)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    row.map(|value| decode_environment(scope, &value))
        .transpose()
}

async fn list_environments(
    pool: &AnyPool,
    project_id: ProjectId,
    request: EnvironmentPageRequest,
) -> Result<EnvironmentPage, EnvironmentError> {
    let fetch_limit = i64::from(request.limit) + 1;
    let rows = if let Some(after) = request.after {
        sqlx::query("SELECT environment_id, name, slug, region, purpose, protection, location, workspace_targets_enabled, configuration_revision, desired_state, observed_state, observed_configuration_revision, created_at_micros, updated_at_micros, observed_at_micros FROM runku_environments WHERE project_id=$1 AND environment_id>$2 ORDER BY environment_id LIMIT $3")
            .bind(project_id.to_string())
            .bind(after.to_string())
            .bind(fetch_limit)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?
    } else {
        sqlx::query("SELECT environment_id, name, slug, region, purpose, protection, location, workspace_targets_enabled, configuration_revision, desired_state, observed_state, observed_configuration_revision, created_at_micros, updated_at_micros, observed_at_micros FROM runku_environments WHERE project_id=$1 ORDER BY environment_id LIMIT $2")
            .bind(project_id.to_string())
            .bind(fetch_limit)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?
    };
    let mut environments = rows
        .iter()
        .map(|row| {
            let environment_id = row
                .try_get::<String, _>("environment_id")
                .map_err(|_| EnvironmentError::Corruption)?
                .parse()
                .map_err(|_| EnvironmentError::Corruption)?;
            decode_environment(EnvironmentScope::new(project_id, environment_id), row)
        })
        .collect::<Result<Vec<_>, EnvironmentError>>()?;
    let has_more = environments.len() > usize::from(request.limit);
    if has_more {
        let _ = environments.pop();
    }
    let next = if has_more {
        environments
            .last()
            .map(|value| value.scope.environment_id())
    } else {
        None
    };
    Ok(EnvironmentPage {
        project_id,
        environments,
        next,
    })
}

async fn load_operation(
    pool: &AnyPool,
    scope: EnvironmentScope,
    operation_id: OperationId,
) -> Result<Option<StoredOperation>, EnvironmentError> {
    let row = sqlx::query("SELECT command_digest, kind, configuration_revision, desired_state, observed_state, observed_configuration_revision, completed_at_micros FROM runku_environment_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3")
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(operation_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;
    row.map(|value| decode_operation(scope, operation_id, &value))
        .transpose()
}

async fn load_operation_tx(
    transaction: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    operation_id: OperationId,
) -> Result<Option<StoredOperation>, EnvironmentError> {
    let row = sqlx::query("SELECT command_digest, kind, configuration_revision, desired_state, observed_state, observed_configuration_revision, completed_at_micros FROM runku_environment_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3")
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(operation_id.to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    row.map(|value| decode_operation(scope, operation_id, &value))
        .transpose()
}

fn decode_environment(
    scope: EnvironmentScope,
    row: &sqlx::any::AnyRow,
) -> Result<Environment, EnvironmentError> {
    let environment = Environment {
        scope,
        configuration: runku_environments::EnvironmentConfiguration {
            name: parse_domain(row, "name")?,
            slug: parse_domain(row, "slug")?,
            region: parse_domain(row, "region")?,
            purpose: decode_purpose(row.try_get("purpose").map_err(corrupt)?)?,
            protection: decode_protection(row.try_get("protection").map_err(corrupt)?)?,
            location: decode_location(row.try_get("location").map_err(corrupt)?)?,
            workspace_targets_enabled: decode_bool(
                row.try_get("workspace_targets_enabled").map_err(corrupt)?,
            )?,
        },
        configuration_revision: positive_u64(
            row.try_get("configuration_revision").map_err(corrupt)?,
        )?,
        desired_state: parse_domain(row, "desired_state")?,
        observed_state: parse_domain(row, "observed_state")?,
        observed_configuration_revision: optional_positive_u64(
            row.try_get("observed_configuration_revision")
                .map_err(corrupt)?,
        )?,
        created_at: TimestampMicros::new(row.try_get("created_at_micros").map_err(corrupt)?),
        updated_at: TimestampMicros::new(row.try_get("updated_at_micros").map_err(corrupt)?),
        observed_at: row
            .try_get::<Option<i64>, _>("observed_at_micros")
            .map_err(corrupt)?
            .map(TimestampMicros::new),
    };
    environment.validate()?;
    Ok(environment)
}

fn decode_operation(
    scope: EnvironmentScope,
    operation_id: OperationId,
    row: &sqlx::any::AnyRow,
) -> Result<StoredOperation, EnvironmentError> {
    let digest: Vec<u8> = row.try_get("command_digest").map_err(corrupt)?;
    if digest.len() != 32 {
        return Err(EnvironmentError::Corruption);
    }
    let operation = EnvironmentOperation {
        scope,
        operation_id,
        kind: parse_domain(row, "kind")?,
        configuration_revision: positive_u64(
            row.try_get("configuration_revision").map_err(corrupt)?,
        )?,
        desired_state: parse_domain(row, "desired_state")?,
        observed_state: parse_domain(row, "observed_state")?,
        observed_configuration_revision: optional_positive_u64(
            row.try_get("observed_configuration_revision")
                .map_err(corrupt)?,
        )?,
        completed_at: TimestampMicros::new(row.try_get("completed_at_micros").map_err(corrupt)?),
    };
    operation.validate()?;
    Ok(StoredOperation { digest, operation })
}

fn parse_domain<T: FromStr>(row: &sqlx::any::AnyRow, field: &str) -> Result<T, EnvironmentError> {
    row.try_get::<String, _>(field)
        .map_err(corrupt)?
        .parse()
        .map_err(|_| EnvironmentError::Corruption)
}

fn validate_page_request(request: EnvironmentPageRequest) -> Result<(), EnvironmentError> {
    request.validate()
}

const fn command_time(command: &EnvironmentCommand) -> TimestampMicros {
    match command {
        EnvironmentCommand::Create { created_at, .. } => *created_at,
        EnvironmentCommand::Update { updated_at, .. } => *updated_at,
        EnvironmentCommand::Materialize { observed_at, .. } => *observed_at,
    }
}

async fn verify_configuration(
    pool: &AnyPool,
    backend: EnvironmentRepositoryBackend,
) -> Result<(), EnvironmentError> {
    match backend {
        EnvironmentRepositoryBackend::SQLite => {
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
                return Err(EnvironmentError::Corruption);
            }
        }
        EnvironmentRepositoryBackend::PostgreSQL => {
            let row = sqlx::query("SELECT current_setting('statement_timeout') AS statement_timeout, current_setting('lock_timeout') AS lock_timeout, current_setting('idle_in_transaction_session_timeout') AS idle_timeout")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            let statement: String = row.try_get("statement_timeout").map_err(corrupt)?;
            let lock: String = row.try_get("lock_timeout").map_err(corrupt)?;
            let idle: String = row.try_get("idle_timeout").map_err(corrupt)?;
            if statement != "30s" || lock != "5s" || idle != "30s" {
                return Err(EnvironmentError::Corruption);
            }
        }
    }
    Ok(())
}

async fn migrate(
    pool: &AnyPool,
    backend: EnvironmentRepositoryBackend,
) -> Result<(), EnvironmentError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_environment_schema_migrations(version BIGINT PRIMARY KEY, checksum TEXT NOT NULL, applied_at_micros BIGINT NOT NULL)")
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;
    let mut transaction = begin_write(pool, backend).await?;
    if backend == EnvironmentRepositoryBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(7_224_856_025_i64)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
    }
    let stored = sqlx::query(
        "SELECT version, checksum FROM runku_environment_schema_migrations ORDER BY version",
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(map_sqlx_error)?;
    for (index, row) in stored.iter().enumerate() {
        let version: i64 = row.try_get("version").map_err(corrupt)?;
        let checksum: String = row.try_get("checksum").map_err(corrupt)?;
        let Some((expected_version, statements)) = MIGRATIONS.get(index) else {
            return Err(EnvironmentError::Unsupported);
        };
        if version != *expected_version {
            return Err(EnvironmentError::Corruption);
        }
        if checksum != migration_checksum(version, statements) {
            return Err(EnvironmentError::Corruption);
        }
    }
    for (version, statements) in MIGRATIONS {
        if stored
            .iter()
            .any(|row| row.try_get::<i64, _>("version").ok() == Some(*version))
        {
            continue;
        }
        for statement in *statements {
            transaction
                .execute(*statement)
                .await
                .map_err(map_sqlx_error)?;
        }
        sqlx::query("INSERT INTO runku_environment_schema_migrations(version, checksum, applied_at_micros) VALUES ($1,$2,$3)")
            .bind(*version)
            .bind(migration_checksum(*version, statements))
            .bind(now_micros()?)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
    }
    transaction.commit().await.map_err(map_commit_error)
}

fn migration_checksum(version: i64, statements: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_ENVIRONMENT_REPOSITORY_SCHEMA\0");
    digest.update(version.to_be_bytes());
    for statement in statements {
        digest.update(statement.as_bytes());
        digest.update([0]);
    }
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

async fn begin_write(
    pool: &AnyPool,
    backend: EnvironmentRepositoryBackend,
) -> Result<Transaction<'_, Any>, EnvironmentError> {
    let statement = match backend {
        EnvironmentRepositoryBackend::SQLite => "BEGIN IMMEDIATE",
        EnvironmentRepositoryBackend::PostgreSQL => "BEGIN ISOLATION LEVEL SERIALIZABLE",
    };
    pool.begin_with(statement).await.map_err(map_sqlx_error)
}

async fn rollback<T>(
    transaction: Transaction<'_, Any>,
    error: EnvironmentError,
) -> Result<T, EnvironmentError> {
    transaction.rollback().await.map_err(map_sqlx_error)?;
    Err(error)
}

fn now_micros() -> Result<i64, EnvironmentError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| EnvironmentError::Internal)?;
    i64::try_from(duration.as_micros()).map_err(|_| EnvironmentError::Internal)
}

fn to_i64(value: u64) -> Result<i64, EnvironmentError> {
    i64::try_from(value).map_err(|_| EnvironmentError::LimitExceeded)
}

fn optional_revision(value: Option<u64>) -> Result<Option<i64>, EnvironmentError> {
    value.map(to_i64).transpose()
}

fn positive_u64(value: i64) -> Result<u64, EnvironmentError> {
    if value <= 0 {
        return Err(EnvironmentError::Corruption);
    }
    u64::try_from(value).map_err(|_| EnvironmentError::Corruption)
}

fn optional_positive_u64(value: Option<i64>) -> Result<Option<u64>, EnvironmentError> {
    value.map(positive_u64).transpose()
}

fn decode_bool(value: i64) -> Result<bool, EnvironmentError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(EnvironmentError::Corruption),
    }
}

const fn encode_purpose(value: EnvironmentPurpose) -> &'static str {
    match value {
        EnvironmentPurpose::Development => "development",
        EnvironmentPurpose::Preview => "preview",
        EnvironmentPurpose::Staging => "staging",
        EnvironmentPurpose::Production => "production",
    }
}

fn decode_purpose(value: &str) -> Result<EnvironmentPurpose, EnvironmentError> {
    match value {
        "development" => Ok(EnvironmentPurpose::Development),
        "preview" => Ok(EnvironmentPurpose::Preview),
        "staging" => Ok(EnvironmentPurpose::Staging),
        "production" => Ok(EnvironmentPurpose::Production),
        _ => Err(EnvironmentError::Corruption),
    }
}

const fn encode_protection(value: EnvironmentProtection) -> &'static str {
    match value {
        EnvironmentProtection::Open => "open",
        EnvironmentProtection::Protected => "protected",
        EnvironmentProtection::Production => "production",
    }
}

fn decode_protection(value: &str) -> Result<EnvironmentProtection, EnvironmentError> {
    match value {
        "open" => Ok(EnvironmentProtection::Open),
        "protected" => Ok(EnvironmentProtection::Protected),
        "production" => Ok(EnvironmentProtection::Production),
        _ => Err(EnvironmentError::Corruption),
    }
}

const fn encode_location(value: EnvironmentLocation) -> &'static str {
    match value {
        EnvironmentLocation::Local => "local",
        EnvironmentLocation::Managed => "managed",
        EnvironmentLocation::SelfHosted => "self_hosted",
    }
}

fn decode_location(value: &str) -> Result<EnvironmentLocation, EnvironmentError> {
    match value {
        "local" => Ok(EnvironmentLocation::Local),
        "managed" => Ok(EnvironmentLocation::Managed),
        "self_hosted" => Ok(EnvironmentLocation::SelfHosted),
        _ => Err(EnvironmentError::Corruption),
    }
}

fn map_constraint_error(error: sqlx::Error) -> EnvironmentError {
    match &error {
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            EnvironmentError::Conflict
        }
        _ => map_sqlx_error(error),
    }
}

fn map_commit_error(error: sqlx::Error) -> EnvironmentError {
    match error {
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            EnvironmentError::Conflict
        }
        sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_) => EnvironmentError::ResultUncertain,
        other => map_sqlx_error(other),
    }
}

fn map_sqlx_error(error: sqlx::Error) -> EnvironmentError {
    match error {
        sqlx::Error::PoolTimedOut => EnvironmentError::Busy,
        sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_) => EnvironmentError::Unavailable,
        sqlx::Error::Database(database)
            if database
                .code()
                .is_some_and(|code| code == "40001" || code == "40P01" || code == "5") =>
        {
            EnvironmentError::Busy
        }
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            EnvironmentError::Conflict
        }
        _ => EnvironmentError::Corruption,
    }
}

fn corrupt<T>(_error: T) -> EnvironmentError {
    EnvironmentError::Corruption
}

#[cfg(test)]
mod tests {
    use runku_environments::{EnvironmentName, EnvironmentRegion, EnvironmentSlug};

    use super::*;

    #[test]
    fn disconnected_commit_is_uncertain_and_retryable() {
        let error = map_commit_error(sqlx::Error::PoolClosed);
        assert_eq!(error, EnvironmentError::ResultUncertain);
        assert!(error.retryable());
    }

    #[test]
    fn migration_checksums_are_version_bound() {
        assert_ne!(
            migration_checksum(1, MIGRATION_1),
            migration_checksum(2, MIGRATION_1)
        );
    }

    #[test]
    fn domain_text_types_remain_parseable() {
        assert!("Production".parse::<EnvironmentName>().is_ok());
        assert!("production".parse::<EnvironmentSlug>().is_ok());
        assert!("us-east-1".parse::<EnvironmentRegion>().is_ok());
    }
}
