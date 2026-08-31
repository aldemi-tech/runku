//! `SQLite` adapter implementation.

use std::{
    collections::BTreeSet,
    path::Path,
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
    QueryBuilder, Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

use crate::migration;

/// Operational role requested from the embedded adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteRole {
    /// Persistent developer database.
    LocalDevelopment,
    /// Ephemeral automated test database.
    Test,
    /// Forbidden role; prevents composition from silently using `SQLite` in production.
    Production,
}

/// Bounded connection and durability configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqliteStoreConfig {
    /// Declared operational role.
    pub role: SqliteRole,
    /// Time to wait for `SQLite`'s single writer.
    pub busy_timeout: Duration,
    /// Time to wait for the one pooled connection.
    pub acquire_timeout: Duration,
}

impl SqliteStoreConfig {
    /// Production-like durability for local development.
    pub const LOCAL: Self = Self {
        role: SqliteRole::LocalDevelopment,
        busy_timeout: Duration::from_secs(5),
        acquire_timeout: Duration::from_secs(5),
    };

    /// Ephemeral test configuration with bounded waits.
    pub const TEST: Self = Self {
        role: SqliteRole::Test,
        busy_timeout: Duration::from_secs(2),
        acquire_timeout: Duration::from_secs(2),
    };
}

/// Embedded `SQLite` implementation restricted to local/test use.
#[derive(Clone, Debug)]
pub struct SqliteStore {
    pool: SqlitePool,
    telemetry: StoreTelemetry,
    role: SqliteRole,
}

/// One outbox row in a portable local Environment export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportedOutboxRecord {
    /// Stable event ID.
    pub event_id: OutboxEventId,
    /// Commit sequence that created the event.
    pub commit_sequence: u64,
    /// Original creation time.
    pub created_at: TimestampMicros,
    /// Canonical payload.
    pub payload: CanonicalValue,
}

/// Validated logical Environment snapshot for local seed/export/import workflows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentExportV1 {
    /// Export format version.
    pub format_version: u8,
    /// Exact source/destination scope.
    pub scope: EnvironmentScope,
    /// Highest Environment commit sequence.
    pub commit_sequence: u64,
    /// Current documents.
    pub documents: Vec<DocumentRecord>,
    /// Current logical index entries.
    pub indexes: Vec<IndexEntry>,
    /// Durable outbox rows.
    pub outbox: Vec<ExportedOutboxRecord>,
    /// Scheduled Invocation records including lease state.
    pub schedules: Vec<ScheduledInvocationRecord>,
}

impl SqliteStore {
    /// Opens or creates a persistent local database and applies migrations.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ProductionBackendUnsupported`] for the Production role and a stable
    /// storage/migration error when the database cannot be opened or validated.
    pub async fn open(
        path: impl AsRef<Path>,
        config: SqliteStoreConfig,
    ) -> Result<Self, StoreError> {
        if config.role == SqliteRole::Production {
            return Err(StoreError::ProductionBackendUnsupported);
        }
        if config.busy_timeout.is_zero() || config.acquire_timeout.is_zero() {
            return Err(StoreError::LimitExceeded);
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(config.busy_timeout);
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .acquire_timeout(config.acquire_timeout)
            .connect_with(options)
            .await
            .map_err(map_sqlx_error)?;
        migration::migrate(&pool).await?;
        verify_pragmas(&pool, config.busy_timeout).await?;
        Ok(Self {
            pool,
            telemetry: StoreTelemetry::default(),
            role: config.role,
        })
    }

    /// Returns the role accepted at construction.
    #[must_use]
    pub const fn role(&self) -> SqliteRole {
        self.role
    }

    /// Exports one Environment as decoded, versioned logical records.
    ///
    /// # Errors
    ///
    /// Returns a stable backend/corruption error. The export is captured in one read snapshot and
    /// never includes another Environment.
    pub async fn export_environment(
        &self,
        scope: EnvironmentScope,
    ) -> Result<EnvironmentExportV1, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(map_sqlx_error)?;
        let project = scope.project_id().to_string();
        let environment = scope.environment_id().to_string();
        let sequence = sqlx::query_scalar::<_, i64>(
            "SELECT commit_sequence FROM runku_environment_sequences WHERE project_id = ? AND environment_id = ?",
        )
        .bind(&project)
        .bind(&environment)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?
        .unwrap_or(0);

        let document_rows = sqlx::query(
            "SELECT table_id, document_id, revision, commit_sequence, created_at_micros, updated_at_micros, value_bytes \
             FROM runku_documents WHERE project_id = ? AND environment_id = ? ORDER BY table_id, document_id",
        )
        .bind(&project)
        .bind(&environment)
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let documents = document_rows
            .iter()
            .map(decode_document_row)
            .collect::<Result<Vec<_>, _>>()?;

        let index_rows = sqlx::query(
            "SELECT index_id, key_bytes, table_id, document_id, document_revision, commit_sequence \
             FROM runku_index_entries WHERE project_id = ? AND environment_id = ? ORDER BY index_id, key_bytes, document_id",
        )
        .bind(&project)
        .bind(&environment)
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let indexes = index_rows
            .iter()
            .map(decode_index_row)
            .collect::<Result<Vec<_>, _>>()?;

        let outbox_rows = sqlx::query(
            "SELECT event_id, commit_sequence, created_at_micros, payload_bytes FROM runku_outbox \
             WHERE project_id = ? AND environment_id = ? ORDER BY commit_sequence, event_id",
        )
        .bind(&project)
        .bind(&environment)
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let outbox = outbox_rows
            .iter()
            .map(decode_exported_outbox)
            .collect::<Result<Vec<_>, _>>()?;

        let schedule_rows = sqlx::query(
            "SELECT scheduled_id, pinned_code, function_name, args_bytes, execute_at_micros, status, attempts, lease_generation, lease_owner, lease_until_micros, idempotency_key, last_error_code, commit_sequence \
             FROM runku_scheduled_invocations WHERE project_id = ? AND environment_id = ? ORDER BY scheduled_id",
        )
        .bind(&project)
        .bind(&environment)
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let schedules = schedule_rows
            .iter()
            .map(decode_schedule_row)
            .collect::<Result<Vec<_>, _>>()?;
        transaction.rollback().await.map_err(map_sqlx_error)?;
        let export = EnvironmentExportV1 {
            format_version: 1,
            scope,
            commit_sequence: positive_or_zero_u64(sequence)?,
            documents,
            indexes,
            outbox,
            schedules,
        };
        validate_export(&export)?;
        Ok(export)
    }

    /// Seeds an Environment only if it has no existing state row.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::MutationConflict`] when the Environment already exists, or a stable
    /// validation/backend error. Import is atomic.
    pub async fn seed_environment(&self, export: &EnvironmentExportV1) -> Result<(), StoreError> {
        self.import_environment_inner(export, false).await
    }

    /// Atomically replaces one Environment after exact ID confirmation.
    ///
    /// Operation journals are intentionally reset: imported data starts a new local retry domain.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRange`] for mismatched confirmation or a stable
    /// validation/backend error.
    pub async fn import_environment(
        &self,
        export: &EnvironmentExportV1,
        confirmation: runku_core::EnvironmentId,
    ) -> Result<(), StoreError> {
        if export.scope.environment_id() != confirmation {
            return Err(StoreError::InvalidRange);
        }
        self.import_environment_inner(export, true).await
    }

    async fn import_environment_inner(
        &self,
        export: &EnvironmentExportV1,
        replace: bool,
    ) -> Result<(), StoreError> {
        validate_export(export)?;
        let scope = export.scope;
        let project = scope.project_id().to_string();
        let environment = scope.environment_id().to_string();
        let mut transaction = migration::begin_immediate(&self.pool).await?;
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM runku_environment_sequences WHERE project_id = ? AND environment_id = ?",
        )
        .bind(&project)
        .bind(&environment)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?
        .is_some();
        if exists && !replace {
            return Err(StoreError::MutationConflict);
        }
        if exists {
            sqlx::query(
                "DELETE FROM runku_environment_sequences WHERE project_id = ? AND environment_id = ?",
            )
            .bind(&project)
            .bind(&environment)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
        }
        sqlx::query(
            "INSERT INTO runku_environment_sequences(project_id, environment_id, commit_sequence) VALUES (?, ?, ?)",
        )
        .bind(&project)
        .bind(&environment)
        .bind(nonnegative_i64(export.commit_sequence)?)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        insert_export_records(&mut transaction, export).await?;
        transaction.commit().await.map_err(map_commit_error)
    }

    /// Removes all state for one Environment after exact ID confirmation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRange`] when confirmation differs, or a backend error. The
    /// operation is one local transaction and never accepts a wildcard.
    pub async fn reset_environment(
        &self,
        scope: EnvironmentScope,
        confirmation: runku_core::EnvironmentId,
    ) -> Result<(), StoreError> {
        if scope.environment_id() != confirmation {
            return Err(StoreError::InvalidRange);
        }
        let mut transaction = migration::begin_immediate(&self.pool).await?;
        sqlx::query(
            "DELETE FROM runku_environment_sequences WHERE project_id = ? AND environment_id = ?",
        )
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        transaction.commit().await.map_err(map_sqlx_error)
    }
}

#[async_trait]
impl LogicalStore for SqliteStore {
    fn backend(&self) -> StoreBackend {
        StoreBackend::SQLite
    }

    async fn begin_read(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Box<dyn ReadSnapshot>, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(map_sqlx_error)?;
        let sequence = sqlx::query_scalar::<_, i64>(
            "SELECT commit_sequence FROM runku_environment_sequences WHERE project_id = ? AND environment_id = ?",
        )
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?
        .unwrap_or(0);
        let recorder = self.telemetry.recorder();
        recorder.snapshot_opened();
        Ok(Box::new(SqliteSnapshot {
            transaction: Some(transaction),
            scope,
            sequence: positive_or_zero_u64(sequence)?,
            recorder,
        }))
    }

    async fn commit(&self, batch: &CommitBatch) -> Result<CommitResult, StoreError> {
        let started = Instant::now();
        let digest = batch.digest()?;
        let mut transaction = migration::begin_immediate(&self.pool).await?;
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
                let _rollback_result = transaction.rollback().await;
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
        if limit == 0 || limit > 1_000 || lease_until <= now {
            return Err(StoreError::LimitExceeded);
        }
        let project = scope.project_id().to_string();
        let environment = scope.environment_id().to_string();
        let mut transaction = migration::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT INTO runku_environment_sequences(project_id, environment_id, commit_sequence) VALUES (?, ?, 0) ON CONFLICT DO NOTHING",
        )
        .bind(&project)
        .bind(&environment)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        sqlx::query(
            "INSERT INTO runku_outbox_consumers(project_id, environment_id, consumer_name, updated_at_micros) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(&project)
        .bind(&environment)
        .bind(consumer.as_str())
        .bind(now.get())
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;
        let lease = sqlx::query(
            "UPDATE runku_outbox_consumers SET lease_owner = ?, lease_until_micros = ?, lease_generation = lease_generation + 1, updated_at_micros = ? \
             WHERE project_id = ? AND environment_id = ? AND consumer_name = ? AND (lease_owner IS NULL OR lease_owner = ? OR lease_until_micros <= ?) \
             RETURNING cursor_sequence, cursor_event_id, lease_generation",
        )
        .bind(worker_id.to_string())
        .bind(lease_until.get())
        .bind(now.get())
        .bind(&project)
        .bind(&environment)
        .bind(consumer.as_str())
        .bind(worker_id.to_string())
        .bind(now.get())
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
             WHERE project_id = ? AND environment_id = ? AND (commit_sequence > ? OR (commit_sequence = ? AND event_id > COALESCE(?, ''))) \
             ORDER BY commit_sequence, event_id LIMIT ?",
        )
        .bind(&project)
        .bind(&environment)
        .bind(nonnegative_i64(cursor_sequence)?)
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
                "UPDATE runku_outbox_consumers SET claimed_sequence = ?, claimed_event_id = ?, updated_at_micros = ? \
                 WHERE project_id = ? AND environment_id = ? AND consumer_name = ?",
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
                "UPDATE runku_outbox_consumers SET lease_owner = NULL, lease_until_micros = NULL, updated_at_micros = ? \
                 WHERE project_id = ? AND environment_id = ? AND consumer_name = ?",
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
        if lease_generation == 0 || through.commit_sequence == 0 {
            return Err(StoreError::OutboxLeaseLost);
        }
        let mut transaction = migration::begin_immediate(&self.pool).await?;
        let result = sqlx::query(
            "UPDATE runku_outbox_consumers SET cursor_sequence = ?, cursor_event_id = ?, claimed_sequence = NULL, claimed_event_id = NULL, \
             lease_owner = NULL, lease_until_micros = NULL, updated_at_micros = ? \
             WHERE project_id = ? AND environment_id = ? AND consumer_name = ? AND lease_owner = ? AND lease_generation = ? \
             AND claimed_sequence = ? AND claimed_event_id = ?",
        )
        .bind(positive_i64(through.commit_sequence)?)
        .bind(through.event_id.to_string())
        .bind(now_micros()?)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(consumer.as_str())
        .bind(worker_id.to_string())
        .bind(positive_i64(lease_generation)?)
        .bind(positive_i64(through.commit_sequence)?)
        .bind(through.event_id.to_string())
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
        if limit == 0 || limit > 100 || lease_until <= now {
            return Err(StoreError::LimitExceeded);
        }
        let mut transaction = migration::begin_immediate(&self.pool).await?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT scheduled_id FROM runku_scheduled_invocations \
             WHERE project_id = ? AND environment_id = ? AND execute_at_micros <= ? \
             AND (status = 'pending' OR (status = 'running' AND lease_until_micros <= ?)) \
             ORDER BY execute_at_micros, scheduled_id LIMIT ?",
        )
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(now.get())
        .bind(now.get())
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx_error)?;

        let mut claimed = Vec::with_capacity(ids.len());
        for id in ids {
            sqlx::query(
                "UPDATE runku_scheduled_invocations SET status = 'running', attempts = attempts + 1, \
                 lease_generation = lease_generation + 1, lease_owner = ?, lease_until_micros = ?, updated_at_micros = ? \
                 WHERE project_id = ? AND environment_id = ? AND scheduled_id = ?",
            )
            .bind(worker_id.to_string())
            .bind(lease_until.get())
            .bind(now.get())
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .bind(&id)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
            let row = fetch_schedule_row(&mut transaction, scope, &id)
                .await?
                .ok_or(StoreError::Corruption)?;
            claimed.push(ClaimedScheduledInvocation {
                record: decode_schedule_row(&row)?,
            });
        }
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
        validate_completion(completion)?;
        let generation = positive_i64(lease_generation)?;
        let now = now_micros()?;
        let mut transaction = migration::begin_immediate(&self.pool).await?;
        let result = complete_schedule_update(
            &mut transaction,
            scope,
            id,
            worker_id,
            generation,
            completion,
            now,
        )
        .await?;
        if result != 1 {
            let _rollback_result = transaction.rollback().await;
            return Err(StoreError::LeaseLost);
        }
        transaction.commit().await.map_err(map_commit_error)
    }

    async fn cancel_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
    ) -> Result<ScheduleCancelResult, StoreError> {
        let mut transaction = migration::begin_immediate(&self.pool).await?;
        let project = scope.project_id().to_string();
        let environment = scope.environment_id().to_string();
        let id = id.to_string();
        let result = sqlx::query(
            "UPDATE runku_scheduled_invocations SET status = 'cancelled', updated_at_micros = ? \
             WHERE project_id = ? AND environment_id = ? AND scheduled_id = ? AND status = 'pending'",
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
                "SELECT status FROM runku_scheduled_invocations WHERE project_id = ? AND environment_id = ? AND scheduled_id = ?",
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
        sqlx::query_scalar::<_, i64>("SELECT 1")
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

struct SqliteSnapshot {
    transaction: Option<Transaction<'static, Sqlite>>,
    scope: EnvironmentScope,
    sequence: u64,
    recorder: StoreTelemetryRecorder,
}

impl SqliteSnapshot {
    fn transaction(&mut self) -> Result<&mut Transaction<'static, Sqlite>, StoreError> {
        self.transaction.as_mut().ok_or(StoreError::Internal)
    }
}

#[async_trait]
impl ReadSnapshot for SqliteSnapshot {
    fn commit_sequence(&self) -> u64 {
        self.sequence
    }

    async fn get_document(
        &mut self,
        table_id: TableId,
        document_id: DocumentId,
    ) -> Result<Option<DocumentRecord>, StoreError> {
        self.recorder.read();
        let project = self.scope.project_id().to_string();
        let environment = self.scope.environment_id().to_string();
        let row = sqlx::query(
            "SELECT table_id, document_id, revision, commit_sequence, created_at_micros, updated_at_micros, value_bytes \
             FROM runku_documents WHERE project_id = ? AND environment_id = ? AND table_id = ? AND document_id = ?",
        )
        .bind(project)
        .bind(environment)
        .bind(table_id.to_string())
        .bind(document_id.to_string())
        .fetch_optional(&mut **self.transaction()?)
        .await
        .map_err(map_sqlx_error)?;
        row.map(|value| decode_document_row(&value)).transpose()
    }

    async fn scan_index(
        &mut self,
        index_id: IndexId,
        range: &IndexRange,
        limit: u32,
    ) -> Result<Vec<IndexEntry>, StoreError> {
        range.validate(limit)?;
        self.recorder.read();
        let mut query = QueryBuilder::<Sqlite>::new(
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
        let project = self.scope.project_id().to_string();
        let environment = self.scope.environment_id().to_string();
        let value = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT payload_bytes FROM runku_outbox WHERE project_id = ? AND environment_id = ? AND event_id = ?",
        )
        .bind(project)
        .bind(environment)
        .bind(event_id.to_string())
        .fetch_optional(&mut **self.transaction()?)
        .await
        .map_err(map_sqlx_error)?;
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
        let transaction = self.transaction.take().ok_or(StoreError::Internal)?;
        transaction.rollback().await.map_err(map_sqlx_error)?;
        // SQLx returns the connection to the pool on a queued cooperative wake. Yielding makes
        // explicit snapshot closure observable before a max-size-one caller opens its next unit.
        tokio::task::yield_now().await;
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
async fn apply_commit(
    transaction: &mut Transaction<'static, Sqlite>,
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
        "INSERT INTO runku_environment_sequences(project_id, environment_id, commit_sequence) VALUES (?, ?, 1) \
         ON CONFLICT(project_id, environment_id) DO UPDATE SET commit_sequence = commit_sequence + 1 \
         RETURNING commit_sequence",
    )
    .bind(&project)
    .bind(&environment)
    .fetch_one(&mut **transaction)
    .await
    .map_err(map_sqlx_error)?;
    let sequence_u64 = positive_u64(sequence)?;
    let now = now_micros()?;
    sqlx::query(
        "INSERT INTO runku_commit_operations(project_id, environment_id, operation_id, batch_digest, commit_sequence, created_at_micros) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(&project)
    .bind(&environment)
    .bind(batch.operation_id().to_string())
    .bind(digest.as_slice())
    .bind(sequence)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(map_sqlx_error)?;

    let mut documents = Vec::with_capacity(batch.documents().len());
    for (ordinal, mutation) in batch.documents().iter().enumerate() {
        let result =
            apply_document(transaction, &project, &environment, sequence, now, mutation).await?;
        sqlx::query(
            "INSERT INTO runku_commit_document_results(project_id, environment_id, operation_id, ordinal, table_id, document_id, revision) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&project)
        .bind(&environment)
        .bind(batch.operation_id().to_string())
        .bind(i64::try_from(ordinal).map_err(|_| StoreError::LimitExceeded)?)
        .bind(result.table_id.to_string())
        .bind(result.document_id.to_string())
        .bind(result.revision.map(positive_i64).transpose()?)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
        documents.push(result);
    }

    for mutation in batch.indexes() {
        apply_index(transaction, &project, &environment, sequence, mutation).await?;
    }
    for event in batch.outbox() {
        let payload = encode_stored_value(&event.payload).map_err(|_| StoreError::LimitExceeded)?;
        let result = sqlx::query(
            "INSERT INTO runku_outbox(project_id, environment_id, event_id, commit_sequence, payload_bytes, created_at_micros) \
             VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(&project)
        .bind(&environment)
        .bind(event.event_id.to_string())
        .bind(sequence)
        .bind(payload)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
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
             VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, NULL, NULL, ?, NULL, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(&project)
        .bind(&environment)
        .bind(schedule.id.to_string())
        .bind(schedule.pinned_code.to_string())
        .bind(schedule.function.as_str())
        .bind(args)
        .bind(schedule.execute_at.get())
        .bind(&schedule.idempotency_key)
        .bind(sequence)
        .bind(now)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
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
    transaction: &mut Transaction<'static, Sqlite>,
    project: &str,
    environment: &str,
    assertions: &[DocumentReadAssertion],
) -> Result<(), StoreError> {
    for assertion in assertions {
        let revision = sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM runku_documents WHERE project_id = ? AND environment_id = ? AND table_id = ? AND document_id = ?",
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
    transaction: &mut Transaction<'static, Sqlite>,
    batch: &CommitBatch,
    digest: [u8; 32],
) -> Result<Option<CommitResult>, StoreError> {
    let scope = batch.scope();
    let row = sqlx::query(
        "SELECT batch_digest, commit_sequence FROM runku_commit_operations \
         WHERE project_id = ? AND environment_id = ? AND operation_id = ?",
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
         WHERE project_id = ? AND environment_id = ? AND operation_id = ? ORDER BY ordinal",
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
    transaction: &mut Transaction<'static, Sqlite>,
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
                         VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    )
                    .bind(project)
                    .bind(environment)
                    .bind(table_id.to_string())
                    .bind(document_id.to_string())
                    .bind(sequence)
                    .bind(now)
                    .bind(now)
                    .bind(encoded)
                    .execute(&mut **transaction)
                    .await
                    .map_err(map_sqlx_error)?;
                    if result.rows_affected() != 1 {
                        return Err(StoreError::MutationConflict);
                    }
                    1
                }
                ExpectedRevision::Exact(expected) => {
                    let expected_db = positive_i64(*expected)?;
                    let next = expected.checked_add(1).ok_or(StoreError::LimitExceeded)?;
                    let result = sqlx::query(
                        "UPDATE runku_documents SET revision = ?, commit_sequence = ?, updated_at_micros = ?, value_bytes = ? \
                         WHERE project_id = ? AND environment_id = ? AND table_id = ? AND document_id = ? AND revision = ?",
                    )
                    .bind(positive_i64(next)?)
                    .bind(sequence)
                    .bind(now)
                    .bind(encoded)
                    .bind(project)
                    .bind(environment)
                    .bind(table_id.to_string())
                    .bind(document_id.to_string())
                    .bind(expected_db)
                    .execute(&mut **transaction)
                    .await
                    .map_err(map_sqlx_error)?;
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
                "DELETE FROM runku_documents WHERE project_id = ? AND environment_id = ? AND table_id = ? AND document_id = ? AND revision = ?",
            )
            .bind(project)
            .bind(environment)
            .bind(table_id.to_string())
            .bind(document_id.to_string())
            .bind(positive_i64(*expected_revision)?)
            .execute(&mut **transaction)
            .await
            .map_err(map_sqlx_error)?;
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
    transaction: &mut Transaction<'static, Sqlite>,
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
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM runku_documents WHERE project_id = ? AND environment_id = ? AND table_id = ? AND document_id = ? AND revision = ?",
            )
            .bind(project)
            .bind(environment)
            .bind(table_id.to_string())
            .bind(document_id.to_string())
            .bind(revision)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(map_sqlx_error)?;
            if exists.is_none() {
                return Err(StoreError::MutationConflict);
            }
            sqlx::query(
                "INSERT INTO runku_index_entries(project_id, environment_id, index_id, key_bytes, table_id, document_id, document_revision, commit_sequence) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(project_id, environment_id, index_id, key_bytes, document_id) \
                 DO UPDATE SET table_id = excluded.table_id, document_revision = excluded.document_revision, commit_sequence = excluded.commit_sequence",
            )
            .bind(project)
            .bind(environment)
            .bind(index_id.to_string())
            .bind(key.as_bytes())
            .bind(table_id.to_string())
            .bind(document_id.to_string())
            .bind(revision)
            .bind(sequence)
            .execute(&mut **transaction)
            .await
            .map_err(map_sqlx_error)?;
        }
        IndexMutation::Delete {
            index_id,
            key,
            document_id,
        } => {
            sqlx::query(
                "DELETE FROM runku_index_entries WHERE project_id = ? AND environment_id = ? AND index_id = ? AND key_bytes = ? AND document_id = ?",
            )
            .bind(project)
            .bind(environment)
            .bind(index_id.to_string())
            .bind(key.as_bytes())
            .bind(document_id.to_string())
            .execute(&mut **transaction)
            .await
            .map_err(map_sqlx_error)?;
        }
    }
    Ok(())
}

fn validate_export(export: &EnvironmentExportV1) -> Result<(), StoreError> {
    const MAX_EXPORT_ROWS_PER_KIND: usize = 1_000_000;
    if export.format_version != 1
        || export.documents.len() > MAX_EXPORT_ROWS_PER_KIND
        || export.indexes.len() > MAX_EXPORT_ROWS_PER_KIND
        || export.outbox.len() > MAX_EXPORT_ROWS_PER_KIND
        || export.schedules.len() > MAX_EXPORT_ROWS_PER_KIND
    {
        return Err(StoreError::LimitExceeded);
    }
    let mut documents = BTreeSet::new();
    for document in &export.documents {
        if document.revision == 0
            || document.commit_sequence == 0
            || document.commit_sequence > export.commit_sequence
            || !documents.insert((document.table_id, document.document_id, document.revision))
        {
            return Err(StoreError::Corruption);
        }
        encode_stored_value(&document.value).map_err(|_| StoreError::LimitExceeded)?;
    }
    let mut indexes = BTreeSet::new();
    for entry in &export.indexes {
        if entry.document_revision == 0
            || entry.commit_sequence == 0
            || entry.commit_sequence > export.commit_sequence
            || !documents.contains(&(entry.table_id, entry.document_id, entry.document_revision))
            || !indexes.insert((entry.index_id, entry.key.as_bytes(), entry.document_id))
        {
            return Err(StoreError::Corruption);
        }
    }
    let mut events = BTreeSet::new();
    for event in &export.outbox {
        if event.commit_sequence == 0
            || event.commit_sequence > export.commit_sequence
            || !events.insert(event.event_id)
        {
            return Err(StoreError::Corruption);
        }
        encode_stored_value(&event.payload).map_err(|_| StoreError::LimitExceeded)?;
    }
    let mut schedules = BTreeSet::new();
    let mut idempotency_keys = BTreeSet::new();
    for schedule in &export.schedules {
        if schedule.commit_sequence == 0
            || schedule.commit_sequence > export.commit_sequence
            || !schedules.insert(schedule.id)
            || schedule.idempotency_key.as_ref().is_some_and(|key| {
                key.is_empty() || key.len() > 128 || !idempotency_keys.insert(key)
            })
            || (schedule.status == ScheduleStatus::Running
                && (schedule.lease_owner.is_none() || schedule.lease_until.is_none()))
            || (schedule.status != ScheduleStatus::Running
                && (schedule.lease_owner.is_some() || schedule.lease_until.is_some()))
        {
            return Err(StoreError::Corruption);
        }
        if let Some(error) = &schedule.last_error_code {
            validate_error_code(error)?;
        }
        encode_stored_value(&schedule.args).map_err(|_| StoreError::LimitExceeded)?;
    }
    Ok(())
}

async fn insert_export_records(
    transaction: &mut Transaction<'static, Sqlite>,
    export: &EnvironmentExportV1,
) -> Result<(), StoreError> {
    let project = export.scope.project_id().to_string();
    let environment = export.scope.environment_id().to_string();
    for document in &export.documents {
        sqlx::query(
            "INSERT INTO runku_documents(project_id, environment_id, table_id, document_id, revision, commit_sequence, created_at_micros, updated_at_micros, value_bytes) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&project)
        .bind(&environment)
        .bind(document.table_id.to_string())
        .bind(document.document_id.to_string())
        .bind(positive_i64(document.revision)?)
        .bind(positive_i64(document.commit_sequence)?)
        .bind(document.created_at.get())
        .bind(document.updated_at.get())
        .bind(encode_stored_value(&document.value).map_err(|_| StoreError::LimitExceeded)?)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    }
    for entry in &export.indexes {
        sqlx::query(
            "INSERT INTO runku_index_entries(project_id, environment_id, index_id, key_bytes, table_id, document_id, document_revision, commit_sequence) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&project)
        .bind(&environment)
        .bind(entry.index_id.to_string())
        .bind(entry.key.as_bytes())
        .bind(entry.table_id.to_string())
        .bind(entry.document_id.to_string())
        .bind(positive_i64(entry.document_revision)?)
        .bind(positive_i64(entry.commit_sequence)?)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    }
    for event in &export.outbox {
        sqlx::query(
            "INSERT INTO runku_outbox(project_id, environment_id, event_id, commit_sequence, payload_bytes, created_at_micros) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&project)
        .bind(&environment)
        .bind(event.event_id.to_string())
        .bind(positive_i64(event.commit_sequence)?)
        .bind(encode_stored_value(&event.payload).map_err(|_| StoreError::LimitExceeded)?)
        .bind(event.created_at.get())
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    }
    let now = now_micros()?;
    for schedule in &export.schedules {
        sqlx::query(
            "INSERT INTO runku_scheduled_invocations(\
                project_id, environment_id, scheduled_id, pinned_code, function_name, args_bytes, execute_at_micros, status, attempts, lease_generation, \
                lease_owner, lease_until_micros, idempotency_key, last_error_code, commit_sequence, created_at_micros, updated_at_micros) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&project)
        .bind(&environment)
        .bind(schedule.id.to_string())
        .bind(schedule.pinned_code.to_string())
        .bind(schedule.function.as_str())
        .bind(encode_stored_value(&schedule.args).map_err(|_| StoreError::LimitExceeded)?)
        .bind(schedule.execute_at.get())
        .bind(status_text(schedule.status))
        .bind(i64::from(schedule.attempts))
        .bind(nonnegative_i64(schedule.lease_generation)?)
        .bind(schedule.lease_owner.map(|owner| owner.to_string()))
        .bind(schedule.lease_until.map(TimestampMicros::get))
        .bind(&schedule.idempotency_key)
        .bind(&schedule.last_error_code)
        .bind(positive_i64(schedule.commit_sequence)?)
        .bind(now)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx_error)?;
    }
    Ok(())
}

fn decode_exported_outbox(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ExportedOutboxRecord, StoreError> {
    let payload: Vec<u8> = row
        .try_get("payload_bytes")
        .map_err(|_| StoreError::Corruption)?;
    Ok(ExportedOutboxRecord {
        event_id: parse_id(row, "event_id")?,
        commit_sequence: positive_u64(row_i64(row, "commit_sequence")?)?,
        created_at: TimestampMicros::new(row_i64(row, "created_at_micros")?),
        payload: decode_stored_value(&payload).map_err(|_| StoreError::Corruption)?,
    })
}

fn decode_outbox_event(row: &sqlx::sqlite::SqliteRow) -> Result<OutboxEventRecord, StoreError> {
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

async fn complete_schedule_update(
    transaction: &mut Transaction<'static, Sqlite>,
    scope: EnvironmentScope,
    id: ScheduledInvocationId,
    worker_id: WorkerId,
    generation: i64,
    completion: &ScheduleCompletion,
    now: i64,
) -> Result<u64, StoreError> {
    let result = match completion {
        ScheduleCompletion::Succeeded => sqlx::query(
            "UPDATE runku_scheduled_invocations SET status = 'succeeded', lease_owner = NULL, lease_until_micros = NULL, last_error_code = NULL, updated_at_micros = ? \
             WHERE project_id = ? AND environment_id = ? AND scheduled_id = ? AND status = 'running' AND lease_owner = ? AND lease_generation = ?",
        )
        .bind(now)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(id.to_string())
        .bind(worker_id.to_string())
        .bind(generation)
        .execute(&mut **transaction)
        .await,
        ScheduleCompletion::Retry {
            execute_at,
            error_code,
        } => sqlx::query(
            "UPDATE runku_scheduled_invocations SET status = 'pending', execute_at_micros = ?, lease_owner = NULL, lease_until_micros = NULL, last_error_code = ?, updated_at_micros = ? \
             WHERE project_id = ? AND environment_id = ? AND scheduled_id = ? AND status = 'running' AND lease_owner = ? AND lease_generation = ?",
        )
        .bind(execute_at.get())
        .bind(error_code)
        .bind(now)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(id.to_string())
        .bind(worker_id.to_string())
        .bind(generation)
        .execute(&mut **transaction)
        .await,
        ScheduleCompletion::Failed { error_code } => sqlx::query(
            "UPDATE runku_scheduled_invocations SET status = 'failed', lease_owner = NULL, lease_until_micros = NULL, last_error_code = ?, updated_at_micros = ? \
             WHERE project_id = ? AND environment_id = ? AND scheduled_id = ? AND status = 'running' AND lease_owner = ? AND lease_generation = ?",
        )
        .bind(error_code)
        .bind(now)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(id.to_string())
        .bind(worker_id.to_string())
        .bind(generation)
        .execute(&mut **transaction)
        .await,
    }
    .map_err(map_sqlx_error)?;
    Ok(result.rows_affected())
}

fn push_range(query: &mut QueryBuilder<Sqlite>, range: &IndexRange) {
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

fn decode_document_row(row: &sqlx::sqlite::SqliteRow) -> Result<DocumentRecord, StoreError> {
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

fn decode_index_row(row: &sqlx::sqlite::SqliteRow) -> Result<IndexEntry, StoreError> {
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
    transaction: &mut Transaction<'static, Sqlite>,
    scope: EnvironmentScope,
    id: &str,
) -> Result<Option<sqlx::sqlite::SqliteRow>, StoreError> {
    sqlx::query(
        "SELECT scheduled_id, pinned_code, function_name, args_bytes, execute_at_micros, status, attempts, lease_generation, lease_owner, lease_until_micros, idempotency_key, last_error_code, commit_sequence \
         FROM runku_scheduled_invocations WHERE project_id = ? AND environment_id = ? AND scheduled_id = ?",
    )
    .bind(scope.project_id().to_string())
    .bind(scope.environment_id().to_string())
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(map_sqlx_error)
}

fn decode_schedule_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ScheduledInvocationRecord, StoreError> {
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
        attempts: positive_or_zero_u32(row_i64(row, "attempts")?)?,
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

const fn status_text(value: ScheduleStatus) -> &'static str {
    match value {
        ScheduleStatus::Pending => "pending",
        ScheduleStatus::Running => "running",
        ScheduleStatus::Succeeded => "succeeded",
        ScheduleStatus::Failed => "failed",
        ScheduleStatus::Cancelled => "cancelled",
    }
}

fn parse_id<T>(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<T, StoreError>
where
    T: FromStr,
{
    row.try_get::<String, _>(column)
        .map_err(|_| StoreError::Corruption)?
        .parse()
        .map_err(|_| StoreError::Corruption)
}

fn row_i64(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<i64, StoreError> {
    row.try_get(column).map_err(|_| StoreError::Corruption)
}

async fn verify_pragmas(pool: &SqlitePool, expected_busy: Duration) -> Result<(), StoreError> {
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
    let expected_busy =
        i64::try_from(expected_busy.as_millis()).map_err(|_| StoreError::LimitExceeded)?;
    if !journal.eq_ignore_ascii_case("wal")
        || foreign_keys != 1
        || synchronous != 2
        || busy_timeout != expected_busy
    {
        return Err(StoreError::Corruption);
    }
    Ok(())
}

fn validate_completion(completion: &ScheduleCompletion) -> Result<(), StoreError> {
    let error = match completion {
        ScheduleCompletion::Succeeded => return Ok(()),
        ScheduleCompletion::Retry { error_code, .. }
        | ScheduleCompletion::Failed { error_code } => error_code,
    };
    validate_error_code(error)
}

fn validate_error_code(error: &str) -> Result<(), StoreError> {
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

fn positive_or_zero_u32(value: i64) -> Result<u32, StoreError> {
    u32::try_from(value).map_err(|_| StoreError::Corruption)
}

pub(crate) fn map_sqlx_error(error: sqlx::Error) -> StoreError {
    match error {
        sqlx::Error::PoolTimedOut => StoreError::Busy,
        sqlx::Error::PoolClosed | sqlx::Error::Io(_) | sqlx::Error::Tls(_) => {
            StoreError::Unavailable
        }
        sqlx::Error::Database(database)
            if database
                .code()
                .is_some_and(|code| matches!(code.as_ref(), "5" | "6")) =>
        {
            StoreError::Busy
        }
        _ => StoreError::Internal,
    }
}

fn map_commit_error(error: sqlx::Error) -> StoreError {
    match map_sqlx_error(error) {
        StoreError::Busy => StoreError::Busy,
        StoreError::Unavailable | StoreError::Internal => StoreError::ResultUncertain,
        other => other,
    }
}
