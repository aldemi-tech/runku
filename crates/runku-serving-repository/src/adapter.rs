//! SQL-backed serving-policy repository with shared SQLite/PostgreSQL semantics.

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
use runku_core::{EnvironmentScope, OperationId, OperatorId};
use runku_releases::Sha256Digest;
use runku_serving::{
    ServingAuditCursor, ServingAuditEvent, ServingAuditPage, ServingAuditPageRequest,
    ServingCommand, ServingCommandKind, ServingContractHashes, ServingLifecycle, ServingMode,
    ServingObservedState, ServingOperation, ServingOperationResult, ServingPolicy,
    ServingPolicyError, ServingPolicyRecord, ServingPolicyRepository, ServingRelease,
    ServingRepositoryBackend, ServingRepositoryTelemetrySnapshot,
};
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};
use sqlx::{
    Any, AnyPool, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

const MIGRATION_1: &[&str] = &[
    "CREATE TABLE runku_serving_policies (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, mode TEXT NOT NULL CHECK(mode IN ('atomic','gradual')), policy_digest BYTEA NOT NULL CHECK(length(policy_digest)=32), policy_revision BIGINT NOT NULL CHECK(policy_revision>0), observed_state TEXT NOT NULL CHECK(observed_state IN ('pending','ready','failed')), observed_policy_revision BIGINT NULL CHECK(observed_policy_revision IS NULL OR (observed_policy_revision>0 AND observed_policy_revision<=policy_revision)), created_at_micros BIGINT NOT NULL CHECK(created_at_micros>=0), updated_at_micros BIGINT NOT NULL CHECK(updated_at_micros>=created_at_micros), observed_at_micros BIGINT NULL CHECK(observed_at_micros IS NULL OR observed_at_micros>=created_at_micros), PRIMARY KEY(project_id,environment_id), CHECK((observed_policy_revision IS NULL)=(observed_at_micros IS NULL)), CHECK((observed_state='pending' AND (observed_at_micros IS NULL OR observed_at_micros<=updated_at_micros)) OR (observed_state IN ('ready','failed') AND observed_at_micros>=updated_at_micros)), CHECK((observed_state='pending' AND (observed_policy_revision IS NULL OR observed_policy_revision<policy_revision)) OR (observed_state IN ('ready','failed') AND observed_policy_revision=policy_revision)))",
    "CREATE TABLE runku_serving_releases (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, release_id TEXT NOT NULL, weight_percent BIGINT NOT NULL CHECK(weight_percent>0 AND weight_percent<=100), schema_hash BYTEA NOT NULL CHECK(length(schema_hash)=32), index_hash BYTEA NOT NULL CHECK(length(index_hash)=32), cron_hash BYTEA NOT NULL CHECK(length(cron_hash)=32), PRIMARY KEY(project_id,environment_id,release_id), FOREIGN KEY(project_id,environment_id) REFERENCES runku_serving_policies(project_id,environment_id) ON DELETE CASCADE)",
    "CREATE TABLE runku_serving_operations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, command_digest BYTEA NOT NULL CHECK(length(command_digest)=32), kind TEXT NOT NULL CHECK(kind IN ('set_desired','materialize')), policy_revision BIGINT NOT NULL CHECK(policy_revision>0), policy_digest BYTEA NOT NULL CHECK(length(policy_digest)=32), observed_state TEXT NOT NULL CHECK(observed_state IN ('pending','ready','failed')), observed_policy_revision BIGINT NULL CHECK(observed_policy_revision IS NULL OR (observed_policy_revision>0 AND observed_policy_revision<=policy_revision)), completed_at_micros BIGINT NOT NULL CHECK(completed_at_micros>=0), PRIMARY KEY(project_id,environment_id,operation_id), FOREIGN KEY(project_id,environment_id) REFERENCES runku_serving_policies(project_id,environment_id) ON DELETE RESTRICT)",
    "CREATE TABLE runku_serving_audit (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, actor_operator_id TEXT NULL, kind TEXT NOT NULL CHECK(kind IN ('set_desired','materialize')), previous_policy_revision BIGINT NULL CHECK(previous_policy_revision IS NULL OR previous_policy_revision>0), policy_revision BIGINT NOT NULL CHECK(policy_revision>0), policy_digest BYTEA NOT NULL CHECK(length(policy_digest)=32), observed_state TEXT NOT NULL CHECK(observed_state IN ('pending','ready','failed')), observed_policy_revision BIGINT NULL CHECK(observed_policy_revision IS NULL OR (observed_policy_revision>0 AND observed_policy_revision<=policy_revision)), occurred_at_micros BIGINT NOT NULL CHECK(occurred_at_micros>=0), PRIMARY KEY(project_id,environment_id,operation_id), FOREIGN KEY(project_id,environment_id,operation_id) REFERENCES runku_serving_operations(project_id,environment_id,operation_id) ON DELETE RESTRICT, CHECK((kind='set_desired' AND actor_operator_id IS NOT NULL) OR (kind='materialize' AND actor_operator_id IS NULL)))",
    "CREATE INDEX runku_serving_audit_order ON runku_serving_audit(project_id,environment_id,occurred_at_micros,operation_id)",
];
const MIGRATIONS: &[(i64, &[&str])] = &[(1, MIGRATION_1)];

/// Operational role selected for repository composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServingRepositoryRole {
    /// Local/test SQLite repository.
    Local,
    /// Authoritative production PostgreSQL repository.
    Production,
}

/// Bounded connection-pool and acquisition policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServingRepositoryConfig {
    /// Declared operational role.
    pub role: ServingRepositoryRole,
    /// Maximum physical connections.
    pub max_connections: u32,
    /// Maximum wait for a pool connection.
    pub acquire_timeout: Duration,
}

impl ServingRepositoryConfig {
    /// Deterministic local/test SQLite configuration.
    pub const LOCAL: Self = Self {
        role: ServingRepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };

    /// Bounded authoritative PostgreSQL configuration.
    pub const PRODUCTION: Self = Self {
        role: ServingRepositoryRole::Production,
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
    operation_reads: AtomicU64,
    audit_reads: AtomicU64,
    retryable_errors: AtomicU64,
}

/// Durable SQL serving-policy repository.
#[derive(Clone, Debug)]
pub struct SqlServingPolicyRepository {
    pool: AnyPool,
    backend: ServingRepositoryBackend,
    counters: Arc<Counters>,
}

impl SqlServingPolicyRepository {
    /// Connects local SQLite and applies checksum-protected append-only migrations.
    ///
    /// # Errors
    ///
    /// Rejects Production role, unsafe pool settings, or unavailable/corrupt storage.
    pub async fn connect_sqlite(
        url: &str,
        config: ServingRepositoryConfig,
    ) -> Result<Self, ServingPolicyError> {
        if config.role == ServingRepositoryRole::Production {
            return Err(ServingPolicyError::ProductionBackendUnsupported);
        }
        if !url.starts_with("sqlite:") {
            return Err(ServingPolicyError::Unavailable);
        }
        Self::connect(url, config, ServingRepositoryBackend::SQLite).await
    }

    /// Connects PostgreSQL 16+ and applies checksum-protected append-only migrations.
    ///
    /// # Errors
    ///
    /// Rejects Local role, unsupported PostgreSQL, unsafe pool settings, or storage failures.
    pub async fn connect_postgres(
        url: &str,
        config: ServingRepositoryConfig,
    ) -> Result<Self, ServingPolicyError> {
        if config.role != ServingRepositoryRole::Production {
            return Err(ServingPolicyError::ProductionBackendUnsupported);
        }
        if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            return Err(ServingPolicyError::Unavailable);
        }
        Self::connect(url, config, ServingRepositoryBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: ServingRepositoryConfig,
        backend: ServingRepositoryBackend,
    ) -> Result<Self, ServingPolicyError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || backend == ServingRepositoryBackend::SQLite && config.max_connections != 1
        {
            return Err(ServingPolicyError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options =
            AnyConnectOptions::from_str(url).map_err(|_| ServingPolicyError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _metadata| {
                Box::pin(async move {
                    match backend {
                        ServingRepositoryBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        ServingRepositoryBackend::PostgreSQL => {
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
        if backend == ServingRepositoryBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(ServingPolicyError::Unsupported);
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
impl ServingPolicyRepository for SqlServingPolicyRepository {
    fn backend(&self) -> ServingRepositoryBackend {
        self.backend
    }

    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &ServingCommand,
    ) -> Result<ServingOperationResult, ServingPolicyError> {
        let result = apply(&self.pool, self.backend, scope, operation_id, command).await;
        match &result {
            Ok(value) if value.replayed => {
                self.counters.replays.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => {
                self.counters.commands.fetch_add(1, Ordering::Relaxed);
            }
            Err(ServingPolicyError::Conflict | ServingPolicyError::OperationIdReused) => {
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

    async fn get(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Option<ServingPolicyRecord>, ServingPolicyError> {
        let result = load_policy_snapshot(&self.pool, self.backend, scope).await;
        if result.is_ok() {
            self.counters.reads.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ServingOperation>, ServingPolicyError> {
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

    async fn audit(
        &self,
        scope: EnvironmentScope,
        request: ServingAuditPageRequest,
    ) -> Result<ServingAuditPage, ServingPolicyError> {
        request.validate()?;
        let result = load_audit(&self.pool, scope, request).await;
        if result.is_ok() {
            self.counters.audit_reads.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn health(&self) -> Result<(), ServingPolicyError> {
        sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_error)
    }

    fn telemetry(&self) -> ServingRepositoryTelemetrySnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        ServingRepositoryTelemetrySnapshot {
            commands: load(&self.counters.commands),
            replays: load(&self.counters.replays),
            conflicts: load(&self.counters.conflicts),
            reads: load(&self.counters.reads),
            operation_reads: load(&self.counters.operation_reads),
            audit_reads: load(&self.counters.audit_reads),
            retryable_errors: load(&self.counters.retryable_errors),
            pool_size: self.pool.size(),
            pool_idle: u32::try_from(self.pool.num_idle()).unwrap_or(u32::MAX),
        }
    }
}

#[derive(Debug)]
struct StoredOperation {
    digest: Vec<u8>,
    operation: ServingOperation,
}

async fn apply(
    pool: &AnyPool,
    backend: ServingRepositoryBackend,
    scope: EnvironmentScope,
    operation_id: OperationId,
    command: &ServingCommand,
) -> Result<ServingOperationResult, ServingPolicyError> {
    let command_digest = command.digest(scope)?;
    let mut transaction = begin_write(pool, backend).await?;
    if let Some(stored) = load_operation_tx(&mut transaction, scope, operation_id).await? {
        if stored.digest.as_slice() != command_digest {
            return rollback(transaction, ServingPolicyError::OperationIdReused).await;
        }
        transaction.commit().await.map_err(map_commit_error)?;
        return Ok(ServingOperationResult {
            operation: stored.operation,
            replayed: true,
        });
    }

    let current = load_policy_tx(&mut transaction, backend, scope, true).await?;
    let previous_revision = current.as_ref().map(|record| record.policy_revision);
    let next = match command {
        ServingCommand::SetDesired {
            actor: _,
            expected_revision,
            policy,
            changed_at,
        } => ServingLifecycle::set_desired(
            scope,
            current.as_ref(),
            *expected_revision,
            policy.clone(),
            *changed_at,
        )?,
        ServingCommand::Materialize {
            expected_revision,
            outcome,
            observed_at,
        } => ServingLifecycle::materialize(
            current.as_ref().ok_or(ServingPolicyError::NotFound)?,
            *expected_revision,
            *outcome,
            *observed_at,
        )?,
    };

    if current.is_some() {
        update_policy(&mut transaction, &next, previous_revision.unwrap_or(0)).await?;
        if matches!(command, ServingCommand::SetDesired { .. }) {
            replace_releases(&mut transaction, &next).await?;
        }
    } else {
        insert_policy(&mut transaction, &next).await?;
        insert_releases(&mut transaction, &next).await?;
    }

    let operation = ServingOperation {
        scope,
        operation_id,
        kind: command.kind(),
        policy_revision: next.policy_revision,
        desired_policy_digest: next.desired_policy.digest(),
        observed_state: next.observed_state,
        observed_policy_revision: next.observed_policy_revision,
        completed_at: command.occurred_at(),
    };
    operation.validate()?;
    let audit = ServingAuditEvent {
        scope,
        operation_id,
        actor_operator_id: match command {
            ServingCommand::SetDesired { actor, .. } => Some(*actor),
            ServingCommand::Materialize { .. } => None,
        },
        kind: command.kind(),
        previous_policy_revision: previous_revision,
        policy_revision: next.policy_revision,
        desired_policy_digest: next.desired_policy.digest(),
        observed_state: next.observed_state,
        observed_policy_revision: next.observed_policy_revision,
        occurred_at: command.occurred_at(),
    };
    audit.validate()?;
    insert_operation(&mut transaction, &operation, command_digest).await?;
    insert_audit(&mut transaction, &audit).await?;
    transaction.commit().await.map_err(map_commit_error)?;
    Ok(ServingOperationResult {
        operation,
        replayed: false,
    })
}

async fn insert_policy(
    transaction: &mut Transaction<'_, Any>,
    record: &ServingPolicyRecord,
) -> Result<(), ServingPolicyError> {
    let result = sqlx::query("INSERT INTO runku_serving_policies(project_id,environment_id,mode,policy_digest,policy_revision,observed_state,observed_policy_revision,created_at_micros,updated_at_micros,observed_at_micros) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind(record.scope.project_id().to_string())
        .bind(record.scope.environment_id().to_string())
        .bind(record.desired_policy.mode().as_str())
        .bind(record.desired_policy.digest().as_bytes().as_slice())
        .bind(to_i64(record.policy_revision)?)
        .bind(record.observed_state.as_str())
        .bind(optional_revision(record.observed_policy_revision)?)
        .bind(record.created_at.get())
        .bind(record.updated_at.get())
        .bind(record.observed_at.map(TimestampMicros::get))
        .execute(&mut **transaction)
        .await;
    match result {
        Ok(value) if value.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(ServingPolicyError::Corruption),
        Err(error) => Err(map_constraint_error(error)),
    }
}

async fn update_policy(
    transaction: &mut Transaction<'_, Any>,
    record: &ServingPolicyRecord,
    expected_revision: u64,
) -> Result<(), ServingPolicyError> {
    let result = sqlx::query("UPDATE runku_serving_policies SET mode=$1,policy_digest=$2,policy_revision=$3,observed_state=$4,observed_policy_revision=$5,updated_at_micros=$6,observed_at_micros=$7 WHERE project_id=$8 AND environment_id=$9 AND policy_revision=$10")
        .bind(record.desired_policy.mode().as_str())
        .bind(record.desired_policy.digest().as_bytes().as_slice())
        .bind(to_i64(record.policy_revision)?)
        .bind(record.observed_state.as_str())
        .bind(optional_revision(record.observed_policy_revision)?)
        .bind(record.updated_at.get())
        .bind(record.observed_at.map(TimestampMicros::get))
        .bind(record.scope.project_id().to_string())
        .bind(record.scope.environment_id().to_string())
        .bind(to_i64(expected_revision)?)
        .execute(&mut **transaction)
        .await
        .map_err(map_constraint_error)?;
    if result.rows_affected() != 1 {
        return Err(ServingPolicyError::Conflict);
    }
    Ok(())
}

async fn replace_releases(
    transaction: &mut Transaction<'_, Any>,
    record: &ServingPolicyRecord,
) -> Result<(), ServingPolicyError> {
    sqlx::query("DELETE FROM runku_serving_releases WHERE project_id=$1 AND environment_id=$2")
        .bind(record.scope.project_id().to_string())
        .bind(record.scope.environment_id().to_string())
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    insert_releases(transaction, record).await
}

async fn insert_releases(
    transaction: &mut Transaction<'_, Any>,
    record: &ServingPolicyRecord,
) -> Result<(), ServingPolicyError> {
    for entry in record.desired_policy.releases() {
        let contracts = entry.contracts();
        sqlx::query("INSERT INTO runku_serving_releases(project_id,environment_id,release_id,weight_percent,schema_hash,index_hash,cron_hash) VALUES ($1,$2,$3,$4,$5,$6,$7)")
            .bind(record.scope.project_id().to_string())
            .bind(record.scope.environment_id().to_string())
            .bind(entry.release_id().to_string())
            .bind(i64::from(entry.weight_percent()))
            .bind(contracts.schema.as_bytes().as_slice())
            .bind(contracts.indexes.as_bytes().as_slice())
            .bind(contracts.cron_declarations.as_bytes().as_slice())
            .execute(&mut **transaction)
            .await
            .map_err(map_constraint_error)?;
    }
    Ok(())
}

async fn insert_operation(
    transaction: &mut Transaction<'_, Any>,
    operation: &ServingOperation,
    command_digest: [u8; 32],
) -> Result<(), ServingPolicyError> {
    sqlx::query("INSERT INTO runku_serving_operations(project_id,environment_id,operation_id,command_digest,kind,policy_revision,policy_digest,observed_state,observed_policy_revision,completed_at_micros) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind(operation.scope.project_id().to_string())
        .bind(operation.scope.environment_id().to_string())
        .bind(operation.operation_id.to_string())
        .bind(command_digest.as_slice())
        .bind(operation.kind.as_str())
        .bind(to_i64(operation.policy_revision)?)
        .bind(operation.desired_policy_digest.as_bytes().as_slice())
        .bind(operation.observed_state.as_str())
        .bind(optional_revision(operation.observed_policy_revision)?)
        .bind(operation.completed_at.get())
        .execute(&mut **transaction)
        .await
        .map_err(map_constraint_error)?;
    Ok(())
}

async fn insert_audit(
    transaction: &mut Transaction<'_, Any>,
    event: &ServingAuditEvent,
) -> Result<(), ServingPolicyError> {
    sqlx::query("INSERT INTO runku_serving_audit(project_id,environment_id,operation_id,actor_operator_id,kind,previous_policy_revision,policy_revision,policy_digest,observed_state,observed_policy_revision,occurred_at_micros) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
        .bind(event.scope.project_id().to_string())
        .bind(event.scope.environment_id().to_string())
        .bind(event.operation_id.to_string())
        .bind(event.actor_operator_id.map(|value| value.to_string()))
        .bind(event.kind.as_str())
        .bind(optional_revision(event.previous_policy_revision)?)
        .bind(to_i64(event.policy_revision)?)
        .bind(event.desired_policy_digest.as_bytes().as_slice())
        .bind(event.observed_state.as_str())
        .bind(optional_revision(event.observed_policy_revision)?)
        .bind(event.occurred_at.get())
        .execute(&mut **transaction)
        .await
        .map_err(map_constraint_error)?;
    Ok(())
}

async fn load_policy_snapshot(
    pool: &AnyPool,
    backend: ServingRepositoryBackend,
    scope: EnvironmentScope,
) -> Result<Option<ServingPolicyRecord>, ServingPolicyError> {
    let statement = match backend {
        ServingRepositoryBackend::SQLite => "BEGIN",
        ServingRepositoryBackend::PostgreSQL => "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
    };
    let mut transaction = pool.begin_with(statement).await.map_err(map_sqlx_error)?;
    let record = load_policy_tx(&mut transaction, backend, scope, false).await?;
    transaction.commit().await.map_err(map_sqlx_error)?;
    Ok(record)
}

async fn load_policy_tx(
    transaction: &mut Transaction<'_, Any>,
    backend: ServingRepositoryBackend,
    scope: EnvironmentScope,
    for_update: bool,
) -> Result<Option<ServingPolicyRecord>, ServingPolicyError> {
    let statement = if backend == ServingRepositoryBackend::PostgreSQL && for_update {
        "SELECT mode,policy_digest,policy_revision,observed_state,observed_policy_revision,created_at_micros,updated_at_micros,observed_at_micros FROM runku_serving_policies WHERE project_id=$1 AND environment_id=$2 FOR UPDATE"
    } else {
        "SELECT mode,policy_digest,policy_revision,observed_state,observed_policy_revision,created_at_micros,updated_at_micros,observed_at_micros FROM runku_serving_policies WHERE project_id=$1 AND environment_id=$2"
    };
    let row = sqlx::query(statement)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let release_rows = sqlx::query("SELECT release_id,weight_percent,schema_hash,index_hash,cron_hash FROM runku_serving_releases WHERE project_id=$1 AND environment_id=$2 ORDER BY release_id")
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .fetch_all(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    let releases = release_rows
        .iter()
        .map(decode_serving_release)
        .collect::<Result<Vec<_>, _>>()?;
    let mode_text: String = row.try_get("mode").map_err(corrupt)?;
    let policy = ServingPolicy::new(
        scope.project_id(),
        ServingMode::from_persisted(&mode_text)?,
        releases,
    )
    .map_err(|_| ServingPolicyError::Corruption)?;
    let stored_digest = decode_digest(row.try_get("policy_digest").map_err(corrupt)?)?;
    if stored_digest != policy.digest() {
        return Err(ServingPolicyError::Corruption);
    }
    let record = ServingPolicyRecord {
        scope,
        desired_policy: policy,
        policy_revision: positive_u64(row.try_get("policy_revision").map_err(corrupt)?)?,
        observed_state: ServingObservedState::from_persisted(
            &row.try_get::<String, _>("observed_state")
                .map_err(corrupt)?,
        )?,
        observed_policy_revision: optional_positive_u64(
            row.try_get("observed_policy_revision").map_err(corrupt)?,
        )?,
        created_at: TimestampMicros::new(row.try_get("created_at_micros").map_err(corrupt)?),
        updated_at: TimestampMicros::new(row.try_get("updated_at_micros").map_err(corrupt)?),
        observed_at: row
            .try_get::<Option<i64>, _>("observed_at_micros")
            .map_err(corrupt)?
            .map(TimestampMicros::new),
    };
    record.validate()?;
    Ok(Some(record))
}

fn decode_serving_release(row: &sqlx::any::AnyRow) -> Result<ServingRelease, ServingPolicyError> {
    let release_id = row
        .try_get::<String, _>("release_id")
        .map_err(corrupt)?
        .parse()
        .map_err(corrupt)?;
    let weight = row.try_get::<i64, _>("weight_percent").map_err(corrupt)?;
    let weight = u8::try_from(weight).map_err(corrupt)?;
    ServingRelease::new(
        release_id,
        weight,
        ServingContractHashes {
            schema: decode_digest(row.try_get("schema_hash").map_err(corrupt)?)?,
            indexes: decode_digest(row.try_get("index_hash").map_err(corrupt)?)?,
            cron_declarations: decode_digest(row.try_get("cron_hash").map_err(corrupt)?)?,
        },
    )
    .map_err(|_| ServingPolicyError::Corruption)
}

async fn load_operation(
    pool: &AnyPool,
    scope: EnvironmentScope,
    operation_id: OperationId,
) -> Result<Option<StoredOperation>, ServingPolicyError> {
    let row = sqlx::query("SELECT command_digest,kind,policy_revision,policy_digest,observed_state,observed_policy_revision,completed_at_micros FROM runku_serving_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3")
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(operation_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;
    row.map(|row| decode_operation(scope, operation_id, &row))
        .transpose()
}

async fn load_operation_tx(
    transaction: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    operation_id: OperationId,
) -> Result<Option<StoredOperation>, ServingPolicyError> {
    let row = sqlx::query("SELECT command_digest,kind,policy_revision,policy_digest,observed_state,observed_policy_revision,completed_at_micros FROM runku_serving_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3")
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(operation_id.to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    row.map(|row| decode_operation(scope, operation_id, &row))
        .transpose()
}

fn decode_operation(
    scope: EnvironmentScope,
    operation_id: OperationId,
    row: &sqlx::any::AnyRow,
) -> Result<StoredOperation, ServingPolicyError> {
    let digest: Vec<u8> = row.try_get("command_digest").map_err(corrupt)?;
    if digest.len() != 32 {
        return Err(ServingPolicyError::Corruption);
    }
    let kind_text: String = row.try_get("kind").map_err(corrupt)?;
    let observed_text: String = row.try_get("observed_state").map_err(corrupt)?;
    let operation = ServingOperation {
        scope,
        operation_id,
        kind: ServingCommandKind::from_persisted(&kind_text)?,
        policy_revision: positive_u64(row.try_get("policy_revision").map_err(corrupt)?)?,
        desired_policy_digest: decode_digest(row.try_get("policy_digest").map_err(corrupt)?)?,
        observed_state: ServingObservedState::from_persisted(&observed_text)?,
        observed_policy_revision: optional_positive_u64(
            row.try_get("observed_policy_revision").map_err(corrupt)?,
        )?,
        completed_at: TimestampMicros::new(row.try_get("completed_at_micros").map_err(corrupt)?),
    };
    operation.validate()?;
    Ok(StoredOperation { digest, operation })
}

async fn load_audit(
    pool: &AnyPool,
    scope: EnvironmentScope,
    request: ServingAuditPageRequest,
) -> Result<ServingAuditPage, ServingPolicyError> {
    let fetch_limit = i64::from(request.limit) + 1;
    let rows = if let Some(after) = request.after {
        sqlx::query("SELECT operation_id,actor_operator_id,kind,previous_policy_revision,policy_revision,policy_digest,observed_state,observed_policy_revision,occurred_at_micros FROM runku_serving_audit WHERE project_id=$1 AND environment_id=$2 AND (occurred_at_micros>$3 OR (occurred_at_micros=$3 AND operation_id>$4)) ORDER BY occurred_at_micros,operation_id LIMIT $5")
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .bind(after.occurred_at.get())
            .bind(after.operation_id.to_string())
            .bind(fetch_limit)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?
    } else {
        sqlx::query("SELECT operation_id,actor_operator_id,kind,previous_policy_revision,policy_revision,policy_digest,observed_state,observed_policy_revision,occurred_at_micros FROM runku_serving_audit WHERE project_id=$1 AND environment_id=$2 ORDER BY occurred_at_micros,operation_id LIMIT $3")
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .bind(fetch_limit)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?
    };
    let mut events = rows
        .iter()
        .map(|row| decode_audit(scope, row))
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = events.len() > usize::from(request.limit);
    if has_more {
        let _ = events.pop();
    }
    let next = if has_more {
        events.last().map(|event| ServingAuditCursor {
            occurred_at: event.occurred_at,
            operation_id: event.operation_id,
        })
    } else {
        None
    };
    Ok(ServingAuditPage {
        scope,
        events,
        next,
    })
}

fn decode_audit(
    scope: EnvironmentScope,
    row: &sqlx::any::AnyRow,
) -> Result<ServingAuditEvent, ServingPolicyError> {
    let operation_id = row
        .try_get::<String, _>("operation_id")
        .map_err(corrupt)?
        .parse()
        .map_err(corrupt)?;
    let kind_text: String = row.try_get("kind").map_err(corrupt)?;
    let observed_text: String = row.try_get("observed_state").map_err(corrupt)?;
    let event = ServingAuditEvent {
        scope,
        operation_id,
        actor_operator_id: row
            .try_get::<Option<String>, _>("actor_operator_id")
            .map_err(corrupt)?
            .map(|value| value.parse::<OperatorId>().map_err(corrupt))
            .transpose()?,
        kind: ServingCommandKind::from_persisted(&kind_text)?,
        previous_policy_revision: optional_positive_u64(
            row.try_get("previous_policy_revision").map_err(corrupt)?,
        )?,
        policy_revision: positive_u64(row.try_get("policy_revision").map_err(corrupt)?)?,
        desired_policy_digest: decode_digest(row.try_get("policy_digest").map_err(corrupt)?)?,
        observed_state: ServingObservedState::from_persisted(&observed_text)?,
        observed_policy_revision: optional_positive_u64(
            row.try_get("observed_policy_revision").map_err(corrupt)?,
        )?,
        occurred_at: TimestampMicros::new(row.try_get("occurred_at_micros").map_err(corrupt)?),
    };
    event.validate()?;
    Ok(event)
}

fn decode_digest(bytes: Vec<u8>) -> Result<Sha256Digest, ServingPolicyError> {
    let bytes: [u8; 32] = bytes.try_into().map_err(corrupt)?;
    Ok(Sha256Digest::from_bytes(bytes))
}

async fn verify_configuration(
    pool: &AnyPool,
    backend: ServingRepositoryBackend,
) -> Result<(), ServingPolicyError> {
    match backend {
        ServingRepositoryBackend::SQLite => {
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
                return Err(ServingPolicyError::Corruption);
            }
        }
        ServingRepositoryBackend::PostgreSQL => {
            let row = sqlx::query("SELECT current_setting('statement_timeout') AS statement_timeout,current_setting('lock_timeout') AS lock_timeout,current_setting('idle_in_transaction_session_timeout') AS idle_timeout")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            let statement: String = row.try_get("statement_timeout").map_err(corrupt)?;
            let lock: String = row.try_get("lock_timeout").map_err(corrupt)?;
            let idle: String = row.try_get("idle_timeout").map_err(corrupt)?;
            if statement != "30s" || lock != "5s" || idle != "30s" {
                return Err(ServingPolicyError::Corruption);
            }
        }
    }
    Ok(())
}

async fn migrate(
    pool: &AnyPool,
    backend: ServingRepositoryBackend,
) -> Result<(), ServingPolicyError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_serving_schema_migrations(version BIGINT PRIMARY KEY,checksum TEXT NOT NULL,applied_at_micros BIGINT NOT NULL)")
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;
    let mut transaction = begin_write(pool, backend).await?;
    if backend == ServingRepositoryBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(7_224_856_026_i64)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
    }
    let stored = sqlx::query(
        "SELECT version,checksum FROM runku_serving_schema_migrations ORDER BY version",
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(map_sqlx_error)?;
    for (index, row) in stored.iter().enumerate() {
        let version: i64 = row.try_get("version").map_err(corrupt)?;
        let checksum: String = row.try_get("checksum").map_err(corrupt)?;
        let Some((expected_version, statements)) = MIGRATIONS.get(index) else {
            return Err(ServingPolicyError::Unsupported);
        };
        if version != *expected_version || checksum != migration_checksum(version, statements) {
            return Err(ServingPolicyError::Corruption);
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
        sqlx::query("INSERT INTO runku_serving_schema_migrations(version,checksum,applied_at_micros) VALUES ($1,$2,$3)")
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
    digest.update(b"RUNKU_SERVING_REPOSITORY_SCHEMA\0");
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
    backend: ServingRepositoryBackend,
) -> Result<Transaction<'_, Any>, ServingPolicyError> {
    let statement = match backend {
        ServingRepositoryBackend::SQLite => "BEGIN IMMEDIATE",
        ServingRepositoryBackend::PostgreSQL => "BEGIN ISOLATION LEVEL SERIALIZABLE",
    };
    pool.begin_with(statement).await.map_err(map_sqlx_error)
}

async fn rollback<T>(
    transaction: Transaction<'_, Any>,
    error: ServingPolicyError,
) -> Result<T, ServingPolicyError> {
    transaction.rollback().await.map_err(map_sqlx_error)?;
    Err(error)
}

fn now_micros() -> Result<i64, ServingPolicyError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServingPolicyError::Internal)?;
    i64::try_from(duration.as_micros()).map_err(|_| ServingPolicyError::Internal)
}

fn to_i64(value: u64) -> Result<i64, ServingPolicyError> {
    i64::try_from(value).map_err(|_| ServingPolicyError::LimitExceeded)
}

fn optional_revision(value: Option<u64>) -> Result<Option<i64>, ServingPolicyError> {
    value.map(to_i64).transpose()
}

fn positive_u64(value: i64) -> Result<u64, ServingPolicyError> {
    if value <= 0 {
        return Err(ServingPolicyError::Corruption);
    }
    u64::try_from(value).map_err(|_| ServingPolicyError::Corruption)
}

fn optional_positive_u64(value: Option<i64>) -> Result<Option<u64>, ServingPolicyError> {
    value.map(positive_u64).transpose()
}

fn map_constraint_error(error: sqlx::Error) -> ServingPolicyError {
    match &error {
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            ServingPolicyError::Conflict
        }
        _ => map_sqlx_error(error),
    }
}

fn map_commit_error(error: sqlx::Error) -> ServingPolicyError {
    match error {
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            ServingPolicyError::Conflict
        }
        sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_) => ServingPolicyError::ResultUncertain,
        other => map_sqlx_error(other),
    }
}

fn map_sqlx_error(error: sqlx::Error) -> ServingPolicyError {
    match error {
        sqlx::Error::PoolTimedOut => ServingPolicyError::Busy,
        sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_) => ServingPolicyError::Unavailable,
        sqlx::Error::Database(database)
            if database
                .code()
                .is_some_and(|code| code == "40001" || code == "40P01" || code == "5") =>
        {
            ServingPolicyError::Busy
        }
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            ServingPolicyError::Conflict
        }
        _ => ServingPolicyError::Corruption,
    }
}

fn corrupt<T>(_error: T) -> ServingPolicyError {
    ServingPolicyError::Corruption
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnected_commit_is_uncertain_and_retryable() {
        let error = map_commit_error(sqlx::Error::PoolClosed);
        assert_eq!(error, ServingPolicyError::ResultUncertain);
        assert!(error.retryable());
    }

    #[test]
    fn migration_checksums_are_version_bound() {
        assert_ne!(
            migration_checksum(1, MIGRATION_1),
            migration_checksum(2, MIGRATION_1)
        );
    }
}
