//! Authoritative `PostgreSQL` adapter implementation.

use std::{
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_core::{DocumentId, IndexId, OutboxEventId, ScheduledInvocationId, TableId, WorkerId};
use runku_data::{
    ClaimedOutboxBatch, ClaimedScheduledInvocation, CommitBatch, CommitResult, DocumentMutation,
    DocumentReadAssertion, DocumentRecord, DocumentRevisionResult, EnvironmentScope,
    ExpectedRevision, IndexEntry, IndexMutation, IndexRange, KeyBound, LogicalStore,
    OutboxConsumerName, OutboxCursor, OutboxEventRecord, PinnedCode, ReadSnapshot,
    ScheduleCancelResult, ScheduleCompletion, ScheduleStatus, ScheduledInvocationRecord,
    StoreBackend, StoreError, StoreTelemetry, StoreTelemetryRecorder, StoreTelemetrySnapshot,
};
use runku_value::{
    CanonicalValue, IndexKey, TimestampMicros, decode_stored_value, encode_stored_value,
};
use sqlx::{
    PgPool, Postgres, QueryBuilder, Row, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions, PgRow},
};

use crate::migration;

const ENVIRONMENT_BINDING_LOCK_ID: i64 = 7_224_856_022;

/// Bounded production connection and statement configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostgresStoreConfig {
    /// Minimum established connections.
    pub min_connections: u32,
    /// Maximum established connections.
    pub max_connections: u32,
    /// Maximum pool acquisition wait.
    pub acquire_timeout: Duration,
    /// Maximum statement execution time.
    pub statement_timeout: Duration,
    /// Maximum lock wait.
    pub lock_timeout: Duration,
    /// Maximum time an open transaction may remain idle.
    pub idle_transaction_timeout: Duration,
}

impl PostgresStoreConfig {
    /// Conservative bounded defaults suitable for one service process.
    pub const PRODUCTION: Self = Self {
        min_connections: 1,
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
        statement_timeout: Duration::from_secs(30),
        lock_timeout: Duration::from_secs(5),
        idle_transaction_timeout: Duration::from_secs(30),
    };

    /// Smaller pool used by deterministic integration tests.
    pub const TEST: Self = Self {
        min_connections: 1,
        max_connections: 8,
        acquire_timeout: Duration::from_secs(5),
        statement_timeout: Duration::from_secs(10),
        lock_timeout: Duration::from_secs(3),
        idle_transaction_timeout: Duration::from_secs(10),
    };

    fn validate(self) -> Result<Self, StoreError> {
        if self.max_connections == 0
            || self.max_connections > 64
            || self.min_connections > self.max_connections
            || self.acquire_timeout.is_zero()
            || self.statement_timeout.is_zero()
            || self.lock_timeout.is_zero()
            || self.idle_transaction_timeout.is_zero()
        {
            return Err(StoreError::LimitExceeded);
        }
        Ok(self)
    }
}

/// Authoritative `PostgreSQL` implementation of the logical persistence contract.
#[derive(Clone, Debug)]
pub struct PostgresStore {
    pool: PgPool,
    telemetry: StoreTelemetry,
    exact_scope: Option<EnvironmentScope>,
}

impl PostgresStore {
    /// Connects, validates `PostgreSQL` 16+, and applies checksum-protected migrations.
    ///
    /// # Errors
    ///
    /// Returns a stable configuration, availability, or migration error.
    pub async fn connect(url: &str, config: PostgresStoreConfig) -> Result<Self, StoreError> {
        let config = config.validate()?;
        let options = PgConnectOptions::from_str(url)
            .map_err(|_| StoreError::Unavailable)?
            .application_name("runku-data")
            .options([
                (
                    "statement_timeout",
                    duration_millis(config.statement_timeout)?,
                ),
                ("lock_timeout", duration_millis(config.lock_timeout)?),
                (
                    "idle_in_transaction_session_timeout",
                    duration_millis(config.idle_transaction_timeout)?,
                ),
            ]);
        let pool = PgPoolOptions::new()
            .min_connections(config.min_connections)
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .connect_with(options)
            .await
            .map_err(map_sqlx_error)?;
        let server_version =
            sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::integer")
                .fetch_one(&pool)
                .await
                .map_err(map_sqlx_error)?;
        if server_version < 160_000 {
            pool.close().await;
            return Err(StoreError::MigrationFailed);
        }
        migration::migrate(&pool).await?;
        Ok(Self {
            pool,
            telemetry: StoreTelemetry::default(),
            exact_scope: None,
        })
    }

    /// Connects an Environment-dedicated database and rejects pre-existing rows from another
    /// Project/Environment scope.
    ///
    /// Every subsequent scoped operation is checked before SQL execution. This is the process-side
    /// guard for deployments that issue one database credential per Environment; database roles
    /// and network policy remain the infrastructure isolation boundary.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Corruption`] when the database already contains another scope, or the
    /// same stable connection/migration errors as [`Self::connect`].
    pub async fn connect_scoped(
        url: &str,
        config: PostgresStoreConfig,
        scope: EnvironmentScope,
    ) -> Result<Self, StoreError> {
        let mut store = Self::connect(url, config).await?;
        let mut transaction = store.pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(ENVIRONMENT_BINDING_LOCK_ID)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
        let different_scope = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM runku_environment_sequences WHERE project_id <> $1 OR environment_id <> $2)",
        )
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        if different_scope {
            transaction.rollback().await.map_err(map_sqlx_error)?;
            store.pool.close().await;
            return Err(StoreError::Corruption);
        }
        sqlx::query(
            "INSERT INTO runku_environment_binding(singleton, project_id, environment_id, bound_at_micros) \
             VALUES (TRUE, $1, $2, $3) ON CONFLICT (singleton) DO NOTHING",
        )
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(now_micros()?)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let binding = sqlx::query(
            "SELECT project_id, environment_id FROM runku_environment_binding WHERE singleton = TRUE",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let bound_project: String = binding
            .try_get("project_id")
            .map_err(|_| StoreError::Corruption)?;
        let bound_environment: String = binding
            .try_get("environment_id")
            .map_err(|_| StoreError::Corruption)?;
        if bound_project != scope.project_id().to_string()
            || bound_environment != scope.environment_id().to_string()
        {
            transaction.rollback().await.map_err(map_sqlx_error)?;
            store.pool.close().await;
            return Err(StoreError::Corruption);
        }
        transaction.commit().await.map_err(map_sqlx_error)?;
        store.exact_scope = Some(scope);
        Ok(store)
    }

    /// Closes the bounded connection pool and waits for checked-out connections.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    fn require_scope(&self, scope: EnvironmentScope) -> Result<(), StoreError> {
        if self.exact_scope.is_some_and(|expected| expected != scope) {
            return Err(StoreError::NotFound);
        }
        Ok(())
    }
}

#[async_trait]
impl LogicalStore for PostgresStore {
    fn backend(&self) -> StoreBackend {
        StoreBackend::PostgreSQL
    }

    async fn begin_read(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Box<dyn ReadSnapshot>, StoreError> {
        self.require_scope(scope)?;
        let mut transaction = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(map_sqlx_error)?;
        let sequence = sqlx::query_scalar::<_, i64>(
            "SELECT commit_sequence FROM runku_environment_sequences WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?
        .unwrap_or(0);
        let recorder = self.telemetry.recorder();
        recorder.snapshot_opened();
        Ok(Box::new(PostgresSnapshot {
            transaction: Some(transaction),
            scope,
            sequence: positive_or_zero_u64(sequence)?,
            recorder,
        }))
    }

    async fn commit(&self, batch: &CommitBatch) -> Result<CommitResult, StoreError> {
        self.require_scope(batch.scope())?;
        let started = Instant::now();
        let digest = batch.digest()?;
        let mut transaction = migration::begin_serializable(&self.pool).await?;
        match apply_commit(&mut transaction, batch, digest).await {
            Ok(result) => {
                if let Err(error) = transaction.commit().await.map_err(map_commit_error) {
                    self.telemetry.recorder().error(error.retryable());
                    return Err(error);
                }
                self.telemetry.recorder().commit(
                    result.replayed,
                    duration_micros_saturated(started.elapsed()),
                );
                Ok(result)
            }
            Err(error) => {
                if error == StoreError::MutationConflict {
                    self.telemetry.recorder().conflict();
                } else {
                    self.telemetry.recorder().error(error.retryable());
                }
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn claim_outbox(
        &self,
        scope: EnvironmentScope,
        consumer: &OutboxConsumerName,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<ClaimedOutboxBatch, StoreError> {
        self.require_scope(scope)?;
        if limit == 0 || limit > 1_000 || lease_until <= now {
            return Err(StoreError::LimitExceeded);
        }
        let project = scope.project_id().to_string();
        let environment = scope.environment_id().to_string();
        let mut transaction = migration::begin_serializable(&self.pool).await?;
        sqlx::query(
            "INSERT INTO runku_environment_sequences(project_id, environment_id, commit_sequence) VALUES ($1, $2, 0) ON CONFLICT DO NOTHING",
        )
        .bind(&project)
        .bind(&environment)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        sqlx::query(
            "INSERT INTO runku_outbox_consumers(project_id, environment_id, consumer_name, updated_at_micros) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        )
        .bind(&project)
        .bind(&environment)
        .bind(consumer.as_str())
        .bind(now.get())
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let lease = sqlx::query(
            "UPDATE runku_outbox_consumers SET lease_owner = $1, lease_until_micros = $2, lease_generation = lease_generation + 1, updated_at_micros = $3 \
             WHERE project_id = $4 AND environment_id = $5 AND consumer_name = $6 AND (lease_owner IS NULL OR lease_owner = $1 OR lease_until_micros <= $3) \
             RETURNING cursor_sequence, cursor_event_id, lease_generation",
        )
        .bind(worker_id.to_string())
        .bind(lease_until.get())
        .bind(now.get())
        .bind(&project)
        .bind(&environment)
        .bind(consumer.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?
        .ok_or(StoreError::Busy)?;
        let cursor_sequence = positive_or_zero_u64(
            lease
                .try_get::<i64, _>("cursor_sequence")
                .map_err(|_| StoreError::Corruption)?,
        )?;
        let cursor_event_text = lease
            .try_get::<Option<String>, _>("cursor_event_id")
            .map_err(|_| StoreError::Corruption)?;
        let acknowledged_through = decode_optional_cursor(cursor_sequence, cursor_event_text)?;
        let rows = sqlx::query(
            "SELECT event_id, commit_sequence, payload_bytes FROM runku_outbox \
             WHERE project_id = $1 AND environment_id = $2 AND (commit_sequence > $3 OR (commit_sequence = $3 AND event_id > COALESCE($4, ''))) \
             ORDER BY commit_sequence, event_id LIMIT $5",
        )
        .bind(&project)
        .bind(&environment)
        .bind(nonnegative_i64(cursor_sequence)?)
        .bind(acknowledged_through.map(|cursor| cursor.event_id.to_string()))
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let events = rows
            .iter()
            .map(decode_outbox_event)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(last) = events.last() {
            sqlx::query(
                "UPDATE runku_outbox_consumers SET claimed_sequence = $1, claimed_event_id = $2, updated_at_micros = $3 \
                 WHERE project_id = $4 AND environment_id = $5 AND consumer_name = $6",
            )
            .bind(positive_i64(last.commit_sequence)?)
            .bind(last.event_id.to_string())
            .bind(now.get())
            .bind(&project)
            .bind(&environment)
            .bind(consumer.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
        } else {
            sqlx::query(
                "UPDATE runku_outbox_consumers SET lease_owner = NULL, lease_until_micros = NULL, updated_at_micros = $1 \
                 WHERE project_id = $2 AND environment_id = $3 AND consumer_name = $4",
            )
            .bind(now.get())
            .bind(&project)
            .bind(&environment)
            .bind(consumer.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
        }
        let lease_generation = positive_u64(
            lease
                .try_get::<i64, _>("lease_generation")
                .map_err(|_| StoreError::Corruption)?,
        )?;
        transaction.commit().await.map_err(map_commit_error)?;
        self.telemetry.recorder().outbox_claimed(events.len());
        Ok(ClaimedOutboxBatch {
            lease_generation,
            acknowledged_through,
            events,
        })
    }

    async fn ack_outbox(
        &self,
        scope: EnvironmentScope,
        consumer: &OutboxConsumerName,
        worker_id: WorkerId,
        lease_generation: u64,
        through: OutboxCursor,
    ) -> Result<(), StoreError> {
        self.require_scope(scope)?;
        if lease_generation == 0 || through.commit_sequence == 0 {
            return Err(StoreError::OutboxLeaseLost);
        }
        let mut transaction = migration::begin_serializable(&self.pool).await?;
        let result = sqlx::query(
            "UPDATE runku_outbox_consumers SET cursor_sequence = $1, cursor_event_id = $2, claimed_sequence = NULL, claimed_event_id = NULL, \
             lease_owner = NULL, lease_until_micros = NULL, updated_at_micros = $3 \
             WHERE project_id = $4 AND environment_id = $5 AND consumer_name = $6 AND lease_owner = $7 AND lease_generation = $8 \
             AND claimed_sequence = $1 AND claimed_event_id = $2",
        )
        .bind(positive_i64(through.commit_sequence)?)
        .bind(through.event_id.to_string())
        .bind(now_micros()?)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(consumer.as_str())
        .bind(worker_id.to_string())
        .bind(positive_i64(lease_generation)?)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        if result.rows_affected() != 1 {
            transaction.rollback().await.map_err(map_sqlx_error)?;
            return Err(StoreError::OutboxLeaseLost);
        }
        transaction.commit().await.map_err(map_commit_error)?;
        self.telemetry.recorder().outbox_ack();
        Ok(())
    }

    async fn claim_due_scheduled(
        &self,
        scope: EnvironmentScope,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<Vec<ClaimedScheduledInvocation>, StoreError> {
        self.require_scope(scope)?;
        if limit == 0 || limit > 100 || lease_until <= now {
            return Err(StoreError::LimitExceeded);
        }
        let mut transaction = migration::begin_serializable(&self.pool).await?;
        let rows = sqlx::query(
            "WITH candidates AS (\
               SELECT scheduled_id FROM runku_scheduled_invocations \
               WHERE project_id = $1 AND environment_id = $2 AND execute_at_micros <= $3 \
               AND (status = 'pending' OR (status = 'running' AND lease_until_micros <= $3)) \
               ORDER BY execute_at_micros, scheduled_id FOR UPDATE SKIP LOCKED LIMIT $4\
             ) UPDATE runku_scheduled_invocations AS scheduled SET status = 'running', attempts = scheduled.attempts + 1, \
               lease_generation = scheduled.lease_generation + 1, lease_owner = $5, lease_until_micros = $6, updated_at_micros = $3 \
             FROM candidates WHERE scheduled.project_id = $1 AND scheduled.environment_id = $2 \
               AND scheduled.scheduled_id = candidates.scheduled_id \
             RETURNING scheduled.scheduled_id, scheduled.pinned_code, scheduled.function_name, scheduled.args_bytes, \
               scheduled.execute_at_micros, scheduled.status, scheduled.attempts, scheduled.lease_generation, \
               scheduled.lease_owner, scheduled.lease_until_micros, scheduled.idempotency_key, \
               scheduled.last_error_code, scheduled.commit_sequence",
        )
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
        .bind(now.get()).bind(i64::from(limit)).bind(worker_id.to_string()).bind(lease_until.get())
        .fetch_all(&mut *transaction).await.map_err(map_sqlx_error)?;
        let mut claimed = rows
            .iter()
            .map(decode_schedule_row)
            .map(|result| result.map(|record| ClaimedScheduledInvocation { record }))
            .collect::<Result<Vec<_>, _>>()?;
        claimed.sort_by_key(|value| (value.record.execute_at, value.record.id));
        transaction.commit().await.map_err(map_commit_error)?;
        self.telemetry.recorder().schedules_claimed(claimed.len());
        Ok(claimed)
    }

    async fn complete_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
        worker_id: WorkerId,
        lease_generation: u64,
        completion: &ScheduleCompletion,
    ) -> Result<(), StoreError> {
        self.require_scope(scope)?;
        validate_completion(completion)?;
        let generation = positive_i64(lease_generation)?;
        let now = now_micros()?;
        let mut transaction = migration::begin_serializable(&self.pool).await?;
        let affected = complete_schedule_update(
            &mut transaction,
            scope,
            id,
            worker_id,
            generation,
            completion,
            now,
        )
        .await?;
        if affected != 1 {
            let _ = transaction.rollback().await;
            return Err(StoreError::LeaseLost);
        }
        transaction.commit().await.map_err(map_commit_error)
    }

    async fn cancel_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
    ) -> Result<ScheduleCancelResult, StoreError> {
        self.require_scope(scope)?;
        let mut transaction = migration::begin_serializable(&self.pool).await?;
        let project = scope.project_id().to_string();
        let environment = scope.environment_id().to_string();
        let id = id.to_string();
        let result = sqlx::query(
            "UPDATE runku_scheduled_invocations SET status = 'cancelled', updated_at_micros = $1 \
             WHERE project_id = $2 AND environment_id = $3 AND scheduled_id = $4 AND status = 'pending'",
        )
        .bind(now_micros()?)
        .bind(&project)
        .bind(&environment)
        .bind(&id)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let outcome = if result.rows_affected() == 1 {
            ScheduleCancelResult::Cancelled
        } else {
            let status = sqlx::query_scalar::<_, String>(
                "SELECT status FROM runku_scheduled_invocations WHERE project_id = $1 AND environment_id = $2 AND scheduled_id = $3",
            )
            .bind(&project)
            .bind(&environment)
            .bind(&id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?
            .ok_or(StoreError::NotFound)?;
            cancel_result(&status)?
        };
        transaction.commit().await.map_err(map_commit_error)?;
        if outcome == ScheduleCancelResult::Cancelled {
            self.telemetry.recorder().schedule_cancelled();
        }
        Ok(outcome)
    }

    async fn health(&self) -> Result<(), StoreError> {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_error)
    }

    fn telemetry(&self) -> StoreTelemetrySnapshot {
        self.telemetry.snapshot(
            self.pool.size(),
            u32::try_from(self.pool.num_idle()).unwrap_or(u32::MAX),
        )
    }
}

struct PostgresSnapshot {
    transaction: Option<Transaction<'static, Postgres>>,
    scope: EnvironmentScope,
    sequence: u64,
    recorder: StoreTelemetryRecorder,
}

impl PostgresSnapshot {
    fn transaction(&mut self) -> Result<&mut Transaction<'static, Postgres>, StoreError> {
        self.transaction.as_mut().ok_or(StoreError::Internal)
    }
}

#[async_trait]
impl ReadSnapshot for PostgresSnapshot {
    fn commit_sequence(&self) -> u64 {
        self.sequence
    }

    async fn get_document(
        &mut self,
        table_id: TableId,
        document_id: DocumentId,
    ) -> Result<Option<DocumentRecord>, StoreError> {
        self.recorder.read();
        let scope = self.scope;
        let row = sqlx::query(
            "SELECT table_id, document_id, revision, commit_sequence, created_at_micros, updated_at_micros, value_bytes \
             FROM runku_documents WHERE project_id = $1 AND environment_id = $2 AND table_id = $3 AND document_id = $4",
        ).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
          .bind(table_id.to_string()).bind(document_id.to_string())
          .fetch_optional(&mut **self.transaction()?).await.map_err(map_sqlx_error)?;
        row.as_ref().map(decode_document_row).transpose()
    }

    async fn scan_index(
        &mut self,
        index_id: IndexId,
        range: &IndexRange,
        limit: u32,
    ) -> Result<Vec<IndexEntry>, StoreError> {
        range.validate(limit)?;
        self.recorder.read();
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT index_id, key_bytes, table_id, document_id, document_revision, commit_sequence \
             FROM runku_index_entries WHERE project_id = ",
        );
        query
            .push_bind(self.scope.project_id().to_string())
            .push(" AND environment_id = ")
            .push_bind(self.scope.environment_id().to_string())
            .push(" AND index_id = ")
            .push_bind(index_id.to_string());
        push_range(&mut query, range);
        query
            .push(" ORDER BY key_bytes, document_id LIMIT ")
            .push_bind(i64::from(limit));
        let rows = query
            .build()
            .fetch_all(&mut **self.transaction()?)
            .await
            .map_err(map_sqlx_error)?;
        rows.iter().map(decode_index_row).collect()
    }

    async fn get_outbox(
        &mut self,
        event_id: OutboxEventId,
    ) -> Result<Option<CanonicalValue>, StoreError> {
        self.recorder.read();
        let scope = self.scope;
        let value = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT payload_bytes FROM runku_outbox WHERE project_id = $1 AND environment_id = $2 AND event_id = $3",
        ).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(event_id.to_string())
          .fetch_optional(&mut **self.transaction()?).await.map_err(map_sqlx_error)?;
        value
            .map(|bytes| decode_stored_value(&bytes).map_err(|_| StoreError::Corruption))
            .transpose()
    }

    async fn get_scheduled(
        &mut self,
        id: ScheduledInvocationId,
    ) -> Result<Option<ScheduledInvocationRecord>, StoreError> {
        self.recorder.read();
        let scope = self.scope;
        let id = id.to_string();
        let row = fetch_schedule_row(self.transaction()?, scope, &id).await?;
        row.as_ref().map(decode_schedule_row).transpose()
    }

    async fn close(mut self: Box<Self>) -> Result<(), StoreError> {
        self.transaction
            .take()
            .ok_or(StoreError::Internal)?
            .rollback()
            .await
            .map_err(map_sqlx_error)
    }
}

#[allow(clippy::too_many_lines)]
async fn apply_commit(
    transaction: &mut Transaction<'static, Postgres>,
    batch: &CommitBatch,
    digest: [u8; 32],
) -> Result<CommitResult, StoreError> {
    if let Some(result) = load_operation(transaction, batch, digest).await? {
        return Ok(result);
    }
    let scope = batch.scope();
    let project = scope.project_id().to_string();
    let environment = scope.environment_id().to_string();
    validate_document_reads(transaction, &project, &environment, batch.reads()).await?;
    let sequence = sqlx::query_scalar::<_, i64>(
        "INSERT INTO runku_environment_sequences(project_id, environment_id, commit_sequence) VALUES ($1, $2, 1) \
         ON CONFLICT(project_id, environment_id) DO UPDATE SET commit_sequence = runku_environment_sequences.commit_sequence + 1 \
         RETURNING commit_sequence",
    ).bind(&project).bind(&environment).fetch_one(&mut **transaction).await.map_err(map_sqlx_error)?;
    let sequence_u64 = positive_u64(sequence)?;
    let now = now_micros()?;
    sqlx::query(
        "INSERT INTO runku_commit_operations(project_id, environment_id, operation_id, batch_digest, commit_sequence, created_at_micros) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    ).bind(&project).bind(&environment).bind(batch.operation_id().to_string())
      .bind(digest.as_slice()).bind(sequence).bind(now)
      .execute(&mut **transaction).await.map_err(map_sqlx_error)?;

    let mut documents = Vec::with_capacity(batch.documents().len());
    for (ordinal, mutation) in batch.documents().iter().enumerate() {
        let result =
            apply_document(transaction, &project, &environment, sequence, now, mutation).await?;
        sqlx::query(
            "INSERT INTO runku_commit_document_results(project_id, environment_id, operation_id, ordinal, table_id, document_id, revision) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        ).bind(&project).bind(&environment).bind(batch.operation_id().to_string())
          .bind(i32::try_from(ordinal).map_err(|_| StoreError::LimitExceeded)?)
          .bind(result.table_id.to_string()).bind(result.document_id.to_string())
          .bind(result.revision.map(positive_i64).transpose()?)
          .execute(&mut **transaction).await.map_err(map_sqlx_error)?;
        documents.push(result);
    }
    for mutation in batch.indexes() {
        apply_index(transaction, &project, &environment, sequence, mutation).await?;
    }
    for event in batch.outbox() {
        let payload = encode_stored_value(&event.payload).map_err(|_| StoreError::LimitExceeded)?;
        let result = sqlx::query(
            "INSERT INTO runku_outbox(project_id, environment_id, event_id, commit_sequence, payload_bytes, created_at_micros) \
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        ).bind(&project).bind(&environment).bind(event.event_id.to_string()).bind(sequence).bind(payload).bind(now)
          .execute(&mut **transaction).await.map_err(map_sqlx_error)?;
        if result.rows_affected() != 1 {
            return Err(StoreError::MutationConflict);
        }
    }
    for schedule in batch.schedules() {
        let args = encode_stored_value(&schedule.args).map_err(|_| StoreError::LimitExceeded)?;
        let result = sqlx::query(
            "INSERT INTO runku_scheduled_invocations(\
               project_id, environment_id, scheduled_id, pinned_code, function_name, args_bytes, execute_at_micros, status, attempts, lease_generation, \
               lease_owner, lease_until_micros, idempotency_key, last_error_code, commit_sequence, created_at_micros, updated_at_micros) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', 0, 0, NULL, NULL, $8, NULL, $9, $10, $10) ON CONFLICT DO NOTHING",
        ).bind(&project).bind(&environment).bind(schedule.id.to_string()).bind(schedule.pinned_code.to_string())
          .bind(schedule.function.as_str()).bind(args).bind(schedule.execute_at.get()).bind(&schedule.idempotency_key)
          .bind(sequence).bind(now).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
        if result.rows_affected() != 1 {
            return Err(StoreError::MutationConflict);
        }
    }
    Ok(CommitResult {
        commit_sequence: sequence_u64,
        documents,
        replayed: false,
    })
}

async fn validate_document_reads(
    transaction: &mut Transaction<'static, Postgres>,
    project: &str,
    environment: &str,
    assertions: &[DocumentReadAssertion],
) -> Result<(), StoreError> {
    for assertion in assertions {
        let revision = sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM runku_documents WHERE project_id = $1 AND environment_id = $2 AND table_id = $3 AND document_id = $4 FOR SHARE",
        )
        .bind(project)
        .bind(environment)
        .bind(assertion.table_id.to_string())
        .bind(assertion.document_id.to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?
        .map(positive_u64)
        .transpose()?;
        if revision != assertion.observed_revision {
            return Err(StoreError::MutationConflict);
        }
    }
    Ok(())
}

async fn load_operation(
    transaction: &mut Transaction<'static, Postgres>,
    batch: &CommitBatch,
    digest: [u8; 32],
) -> Result<Option<CommitResult>, StoreError> {
    let scope = batch.scope();
    let row = sqlx::query(
        "SELECT batch_digest, commit_sequence FROM runku_commit_operations \
         WHERE project_id = $1 AND environment_id = $2 AND operation_id = $3",
    )
    .bind(scope.project_id().to_string())
    .bind(scope.environment_id().to_string())
    .bind(batch.operation_id().to_string())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(map_sqlx_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let stored_digest: Vec<u8> = row
        .try_get("batch_digest")
        .map_err(|_| StoreError::Corruption)?;
    if stored_digest != digest {
        return Err(StoreError::OperationIdReused);
    }
    let sequence = positive_u64(
        row.try_get::<i64, _>("commit_sequence")
            .map_err(|_| StoreError::Corruption)?,
    )?;
    let rows = sqlx::query(
        "SELECT table_id, document_id, revision FROM runku_commit_document_results \
         WHERE project_id = $1 AND environment_id = $2 AND operation_id = $3 ORDER BY ordinal",
    )
    .bind(scope.project_id().to_string())
    .bind(scope.environment_id().to_string())
    .bind(batch.operation_id().to_string())
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_sqlx_error)?;
    let documents = rows
        .iter()
        .map(|row| {
            Ok(DocumentRevisionResult {
                table_id: parse_id(row, "table_id")?,
                document_id: parse_id(row, "document_id")?,
                revision: row
                    .try_get::<Option<i64>, _>("revision")
                    .map_err(|_| StoreError::Corruption)?
                    .map(positive_u64)
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    if documents.len() != batch.documents().len() {
        return Err(StoreError::Corruption);
    }
    Ok(Some(CommitResult {
        commit_sequence: sequence,
        documents,
        replayed: true,
    }))
}

async fn apply_document(
    transaction: &mut Transaction<'static, Postgres>,
    project: &str,
    environment: &str,
    sequence: i64,
    now: i64,
    mutation: &DocumentMutation,
) -> Result<DocumentRevisionResult, StoreError> {
    match mutation {
        DocumentMutation::Upsert {
            table_id,
            document_id,
            expected,
            value,
        } => {
            let encoded = encode_stored_value(value).map_err(|_| StoreError::LimitExceeded)?;
            let revision = match expected {
                ExpectedRevision::Absent => {
                    let result = sqlx::query(
                        "INSERT INTO runku_documents(project_id, environment_id, table_id, document_id, revision, commit_sequence, created_at_micros, updated_at_micros, value_bytes) \
                         VALUES ($1, $2, $3, $4, 1, $5, $6, $6, $7) ON CONFLICT DO NOTHING",
                    ).bind(project).bind(environment).bind(table_id.to_string()).bind(document_id.to_string())
                      .bind(sequence).bind(now).bind(encoded).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
                    if result.rows_affected() != 1 {
                        return Err(StoreError::MutationConflict);
                    }
                    1
                }
                ExpectedRevision::Exact(expected) => {
                    let expected_db = positive_i64(*expected)?;
                    let next = expected.checked_add(1).ok_or(StoreError::LimitExceeded)?;
                    let result = sqlx::query(
                        "UPDATE runku_documents SET revision = $1, commit_sequence = $2, updated_at_micros = $3, value_bytes = $4 \
                         WHERE project_id = $5 AND environment_id = $6 AND table_id = $7 AND document_id = $8 AND revision = $9",
                    ).bind(positive_i64(next)?).bind(sequence).bind(now).bind(encoded).bind(project).bind(environment)
                      .bind(table_id.to_string()).bind(document_id.to_string()).bind(expected_db)
                      .execute(&mut **transaction).await.map_err(map_sqlx_error)?;
                    if result.rows_affected() != 1 {
                        return Err(StoreError::MutationConflict);
                    }
                    next
                }
            };
            Ok(DocumentRevisionResult {
                table_id: *table_id,
                document_id: *document_id,
                revision: Some(revision),
            })
        }
        DocumentMutation::Delete {
            table_id,
            document_id,
            expected_revision,
        } => {
            let result = sqlx::query(
                "DELETE FROM runku_documents WHERE project_id = $1 AND environment_id = $2 AND table_id = $3 AND document_id = $4 AND revision = $5",
            ).bind(project).bind(environment).bind(table_id.to_string()).bind(document_id.to_string())
              .bind(positive_i64(*expected_revision)?).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
            if result.rows_affected() != 1 {
                return Err(StoreError::MutationConflict);
            }
            Ok(DocumentRevisionResult {
                table_id: *table_id,
                document_id: *document_id,
                revision: None,
            })
        }
    }
}

async fn apply_index(
    transaction: &mut Transaction<'static, Postgres>,
    project: &str,
    environment: &str,
    sequence: i64,
    mutation: &IndexMutation,
) -> Result<(), StoreError> {
    match mutation {
        IndexMutation::Put {
            index_id,
            key,
            table_id,
            document_id,
            document_revision,
        } => {
            let revision = positive_i64(*document_revision)?;
            let exists = sqlx::query_scalar::<_, i32>(
                "SELECT 1 FROM runku_documents WHERE project_id = $1 AND environment_id = $2 AND table_id = $3 AND document_id = $4 AND revision = $5",
            ).bind(project).bind(environment).bind(table_id.to_string()).bind(document_id.to_string()).bind(revision)
              .fetch_optional(&mut **transaction).await.map_err(map_sqlx_error)?;
            if exists.is_none() {
                return Err(StoreError::MutationConflict);
            }
            sqlx::query(
                "INSERT INTO runku_index_entries(project_id, environment_id, index_id, key_bytes, table_id, document_id, document_revision, commit_sequence) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT(project_id, environment_id, index_id, key_bytes, document_id) \
                 DO UPDATE SET table_id = excluded.table_id, document_revision = excluded.document_revision, commit_sequence = excluded.commit_sequence",
            ).bind(project).bind(environment).bind(index_id.to_string()).bind(key.as_bytes()).bind(table_id.to_string())
              .bind(document_id.to_string()).bind(revision).bind(sequence).execute(&mut **transaction).await.map_err(map_sqlx_error)?;
        }
        IndexMutation::Delete {
            index_id,
            key,
            document_id,
        } => {
            sqlx::query(
                "DELETE FROM runku_index_entries WHERE project_id = $1 AND environment_id = $2 AND index_id = $3 AND key_bytes = $4 AND document_id = $5",
            ).bind(project).bind(environment).bind(index_id.to_string()).bind(key.as_bytes()).bind(document_id.to_string())
              .execute(&mut **transaction).await.map_err(map_sqlx_error)?;
        }
    }
    Ok(())
}

async fn complete_schedule_update(
    transaction: &mut Transaction<'static, Postgres>,
    scope: EnvironmentScope,
    id: ScheduledInvocationId,
    worker_id: WorkerId,
    generation: i64,
    completion: &ScheduleCompletion,
    now: i64,
) -> Result<u64, StoreError> {
    let result = match completion {
        ScheduleCompletion::Succeeded => sqlx::query(
            "UPDATE runku_scheduled_invocations SET status = 'succeeded', lease_owner = NULL, lease_until_micros = NULL, last_error_code = NULL, updated_at_micros = $1 \
             WHERE project_id = $2 AND environment_id = $3 AND scheduled_id = $4 AND status = 'running' AND lease_owner = $5 AND lease_generation = $6",
        ).bind(now).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
          .bind(id.to_string()).bind(worker_id.to_string()).bind(generation).execute(&mut **transaction).await,
        ScheduleCompletion::Retry { execute_at, error_code } => sqlx::query(
            "UPDATE runku_scheduled_invocations SET status = 'pending', execute_at_micros = $1, lease_owner = NULL, lease_until_micros = NULL, last_error_code = $2, updated_at_micros = $3 \
             WHERE project_id = $4 AND environment_id = $5 AND scheduled_id = $6 AND status = 'running' AND lease_owner = $7 AND lease_generation = $8",
        ).bind(execute_at.get()).bind(error_code).bind(now).bind(scope.project_id().to_string())
          .bind(scope.environment_id().to_string()).bind(id.to_string()).bind(worker_id.to_string()).bind(generation)
          .execute(&mut **transaction).await,
        ScheduleCompletion::Failed { error_code } => sqlx::query(
            "UPDATE runku_scheduled_invocations SET status = 'failed', lease_owner = NULL, lease_until_micros = NULL, last_error_code = $1, updated_at_micros = $2 \
             WHERE project_id = $3 AND environment_id = $4 AND scheduled_id = $5 AND status = 'running' AND lease_owner = $6 AND lease_generation = $7",
        ).bind(error_code).bind(now).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
          .bind(id.to_string()).bind(worker_id.to_string()).bind(generation).execute(&mut **transaction).await,
    }.map_err(map_sqlx_error)?;
    Ok(result.rows_affected())
}

fn push_range(query: &mut QueryBuilder<Postgres>, range: &IndexRange) {
    match range.lower() {
        KeyBound::Unbounded => {}
        KeyBound::Inclusive(value) => {
            query.push(" AND key_bytes >= ").push_bind(value.clone());
        }
        KeyBound::Exclusive(value) => {
            query.push(" AND key_bytes > ").push_bind(value.clone());
        }
    }
    match range.upper() {
        KeyBound::Unbounded => {}
        KeyBound::Inclusive(value) => {
            query.push(" AND key_bytes <= ").push_bind(value.clone());
        }
        KeyBound::Exclusive(value) => {
            query.push(" AND key_bytes < ").push_bind(value.clone());
        }
    }
}

fn decode_document_row(row: &PgRow) -> Result<DocumentRecord, StoreError> {
    let value: Vec<u8> = row
        .try_get("value_bytes")
        .map_err(|_| StoreError::Corruption)?;
    Ok(DocumentRecord {
        table_id: parse_id(row, "table_id")?,
        document_id: parse_id(row, "document_id")?,
        revision: positive_u64(row_i64(row, "revision")?)?,
        commit_sequence: positive_u64(row_i64(row, "commit_sequence")?)?,
        created_at: TimestampMicros::new(row_i64(row, "created_at_micros")?),
        updated_at: TimestampMicros::new(row_i64(row, "updated_at_micros")?),
        value: decode_stored_value(&value).map_err(|_| StoreError::Corruption)?,
    })
}

fn decode_index_row(row: &PgRow) -> Result<IndexEntry, StoreError> {
    let key: Vec<u8> = row
        .try_get("key_bytes")
        .map_err(|_| StoreError::Corruption)?;
    Ok(IndexEntry {
        index_id: parse_id(row, "index_id")?,
        key: IndexKey::decode(&key).map_err(|_| StoreError::Corruption)?,
        table_id: parse_id(row, "table_id")?,
        document_id: parse_id(row, "document_id")?,
        document_revision: positive_u64(row_i64(row, "document_revision")?)?,
        commit_sequence: positive_u64(row_i64(row, "commit_sequence")?)?,
    })
}

async fn fetch_schedule_row(
    transaction: &mut Transaction<'static, Postgres>,
    scope: EnvironmentScope,
    id: &str,
) -> Result<Option<PgRow>, StoreError> {
    sqlx::query(
        "SELECT scheduled_id, pinned_code, function_name, args_bytes, execute_at_micros, status, attempts, lease_generation, lease_owner, lease_until_micros, idempotency_key, last_error_code, commit_sequence \
         FROM runku_scheduled_invocations WHERE project_id = $1 AND environment_id = $2 AND scheduled_id = $3",
    ).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id)
      .fetch_optional(&mut **transaction).await.map_err(map_sqlx_error)
}

fn decode_schedule_row(row: &PgRow) -> Result<ScheduledInvocationRecord, StoreError> {
    let args: Vec<u8> = row
        .try_get("args_bytes")
        .map_err(|_| StoreError::Corruption)?;
    let lease_owner = row
        .try_get::<Option<String>, _>("lease_owner")
        .map_err(|_| StoreError::Corruption)?
        .map(|value| value.parse().map_err(|_| StoreError::Corruption))
        .transpose()?;
    Ok(ScheduledInvocationRecord {
        id: parse_id(row, "scheduled_id")?,
        pinned_code: row
            .try_get::<String, _>("pinned_code")
            .map_err(|_| StoreError::Corruption)?
            .parse::<PinnedCode>()
            .map_err(|_| StoreError::Corruption)?,
        function: row
            .try_get::<String, _>("function_name")
            .map_err(|_| StoreError::Corruption)?
            .parse()
            .map_err(|_| StoreError::Corruption)?,
        args: decode_stored_value(&args).map_err(|_| StoreError::Corruption)?,
        execute_at: TimestampMicros::new(row_i64(row, "execute_at_micros")?),
        status: parse_status(
            &row.try_get::<String, _>("status")
                .map_err(|_| StoreError::Corruption)?,
        )?,
        attempts: positive_or_zero_u32(row_i32(row, "attempts")?)?,
        lease_generation: positive_or_zero_u64(row_i64(row, "lease_generation")?)?,
        lease_owner,
        lease_until: row
            .try_get::<Option<i64>, _>("lease_until_micros")
            .map_err(|_| StoreError::Corruption)?
            .map(TimestampMicros::new),
        idempotency_key: row
            .try_get("idempotency_key")
            .map_err(|_| StoreError::Corruption)?,
        last_error_code: row
            .try_get("last_error_code")
            .map_err(|_| StoreError::Corruption)?,
        commit_sequence: positive_u64(row_i64(row, "commit_sequence")?)?,
    })
}

fn decode_outbox_event(row: &PgRow) -> Result<OutboxEventRecord, StoreError> {
    let payload: Vec<u8> = row
        .try_get("payload_bytes")
        .map_err(|_| StoreError::Corruption)?;
    Ok(OutboxEventRecord {
        event_id: parse_id(row, "event_id")?,
        commit_sequence: positive_u64(row_i64(row, "commit_sequence")?)?,
        payload: decode_stored_value(&payload).map_err(|_| StoreError::Corruption)?,
    })
}

fn decode_optional_cursor(
    sequence: u64,
    event_id: Option<String>,
) -> Result<Option<OutboxCursor>, StoreError> {
    match (sequence, event_id) {
        (0, None) => Ok(None),
        (0, Some(_)) | (_, None) => Err(StoreError::Corruption),
        (commit_sequence, Some(event_id)) => Ok(Some(OutboxCursor {
            commit_sequence,
            event_id: event_id.parse().map_err(|_| StoreError::Corruption)?,
        })),
    }
}

fn parse_status(value: &str) -> Result<ScheduleStatus, StoreError> {
    match value {
        "pending" => Ok(ScheduleStatus::Pending),
        "running" => Ok(ScheduleStatus::Running),
        "succeeded" => Ok(ScheduleStatus::Succeeded),
        "failed" => Ok(ScheduleStatus::Failed),
        "cancelled" => Ok(ScheduleStatus::Cancelled),
        _ => Err(StoreError::Corruption),
    }
}

fn cancel_result(value: &str) -> Result<ScheduleCancelResult, StoreError> {
    match parse_status(value)? {
        ScheduleStatus::Pending => Err(StoreError::Corruption),
        ScheduleStatus::Running => Ok(ScheduleCancelResult::Running),
        ScheduleStatus::Cancelled => Ok(ScheduleCancelResult::AlreadyCancelled),
        ScheduleStatus::Succeeded | ScheduleStatus::Failed => Ok(ScheduleCancelResult::Terminal),
    }
}

fn parse_id<T>(row: &PgRow, column: &str) -> Result<T, StoreError>
where
    T: FromStr,
{
    row.try_get::<String, _>(column)
        .map_err(|_| StoreError::Corruption)?
        .parse()
        .map_err(|_| StoreError::Corruption)
}

fn row_i64(row: &PgRow, column: &str) -> Result<i64, StoreError> {
    row.try_get(column).map_err(|_| StoreError::Corruption)
}

fn row_i32(row: &PgRow, column: &str) -> Result<i32, StoreError> {
    row.try_get(column).map_err(|_| StoreError::Corruption)
}

fn validate_completion(completion: &ScheduleCompletion) -> Result<(), StoreError> {
    let error = match completion {
        ScheduleCompletion::Succeeded => return Ok(()),
        ScheduleCompletion::Retry { error_code, .. }
        | ScheduleCompletion::Failed { error_code } => error_code,
    };
    if error.is_empty()
        || error.len() > 64
        || !error
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(StoreError::LimitExceeded);
    }
    Ok(())
}

pub(crate) fn now_micros() -> Result<i64, StoreError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::Internal)?;
    i64::try_from(duration.as_micros()).map_err(|_| StoreError::Internal)
}

fn duration_millis(duration: Duration) -> Result<String, StoreError> {
    let millis = u64::try_from(duration.as_millis()).map_err(|_| StoreError::LimitExceeded)?;
    Ok(format!("{millis}ms"))
}

fn duration_micros_saturated(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn positive_i64(value: u64) -> Result<i64, StoreError> {
    if value == 0 {
        return Err(StoreError::MutationConflict);
    }
    i64::try_from(value).map_err(|_| StoreError::LimitExceeded)
}

fn nonnegative_i64(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::LimitExceeded)
}

fn positive_u64(value: i64) -> Result<u64, StoreError> {
    if value <= 0 {
        return Err(StoreError::Corruption);
    }
    u64::try_from(value).map_err(|_| StoreError::Corruption)
}

fn positive_or_zero_u64(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Corruption)
}
fn positive_or_zero_u32(value: i32) -> Result<u32, StoreError> {
    u32::try_from(value).map_err(|_| StoreError::Corruption)
}

pub(crate) fn map_sqlx_error(error: sqlx::Error) -> StoreError {
    match error {
        sqlx::Error::PoolTimedOut => StoreError::Busy,
        sqlx::Error::PoolClosed | sqlx::Error::Io(_) | sqlx::Error::Tls(_) => {
            StoreError::Unavailable
        }
        sqlx::Error::Database(database) => match database.code().as_deref() {
            Some("40001" | "40P01") => StoreError::SerializationFailure,
            Some("55P03" | "57014") => StoreError::Busy,
            Some("23505" | "23503" | "23514") => StoreError::MutationConflict,
            _ => StoreError::Internal,
        },
        _ => StoreError::Internal,
    }
}

fn map_commit_error(error: sqlx::Error) -> StoreError {
    match map_sqlx_error(error) {
        StoreError::Busy => StoreError::Busy,
        StoreError::SerializationFailure => StoreError::SerializationFailure,
        StoreError::Unavailable | StoreError::Internal => StoreError::ResultUncertain,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresStore, map_commit_error, map_sqlx_error};
    use runku_core::{EnvironmentId, ProjectId};
    use runku_data::{EnvironmentScope, StoreError, StoreTelemetry};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    #[test]
    fn unavailable_commit_outcome_is_classified_as_uncertain() {
        assert_eq!(
            map_commit_error(sqlx::Error::PoolClosed),
            StoreError::ResultUncertain
        );
        assert!(StoreError::ResultUncertain.retryable());
    }

    #[test]
    fn bounded_pool_exhaustion_remains_a_known_busy_failure() {
        assert_eq!(map_sqlx_error(sqlx::Error::PoolTimedOut), StoreError::Busy);
        assert_eq!(
            map_commit_error(sqlx::Error::PoolTimedOut),
            StoreError::Busy
        );
    }

    #[tokio::test]
    async fn scoped_store_rejects_another_environment_before_sql() {
        let exact = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let other = EnvironmentScope::new(exact.project_id(), EnvironmentId::generate());
        let store = PostgresStore {
            pool: PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new()),
            telemetry: StoreTelemetry::default(),
            exact_scope: Some(exact),
        };
        assert_eq!(store.require_scope(exact), Ok(()));
        assert_eq!(store.require_scope(other), Err(StoreError::NotFound));
    }
}
