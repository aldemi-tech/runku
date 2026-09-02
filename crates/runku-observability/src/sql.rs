use std::{
    collections::BTreeSet,
    fmt::Write as _,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationalEventId};
use runku_releases::FunctionType;
use runku_value::{TimestampMicros, decode_stored_value, encode_stored_value};
use sha2::{Digest as _, Sha256};
use sqlx::{
    Any, AnyPool, AssertSqlSafe, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions, AnyRow},
};

use crate::{
    LOG_APPEND_MAX_RECORDS, LOG_PRUNE_MAX_RECORDS, LogCursor, LogEventKind, LogLevel, LogMessage,
    LogPage, LogPrincipalKind, LogQuery, LogRepository, LogRepositoryBackend, LogRepositoryError,
    LogStream, OperationalEventV1, OutcomeCode, PruneResult, SequencedOperationalEvent,
};

const SCHEMA_VERSION: i64 = 1;
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_log_sequences (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, next_sequence BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id))",
    "CREATE TABLE IF NOT EXISTS runku_operational_logs (event_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, environment_id TEXT NOT NULL, sequence BIGINT NOT NULL, occurred_at_micros BIGINT NOT NULL, request_id TEXT NOT NULL, invocation_id TEXT NOT NULL, parent_invocation_id TEXT NULL, release_id TEXT NOT NULL, dev_revision_id TEXT NULL, function_id TEXT NOT NULL, function_name TEXT NOT NULL, function_type TEXT NOT NULL, client_id TEXT NULL, credential_id TEXT NULL, principal_kind TEXT NOT NULL, stream TEXT NOT NULL, level TEXT NOT NULL, level_rank BIGINT NOT NULL, event_kind TEXT NOT NULL, message TEXT NULL, fields BYTEA NULL, duration_micros BIGINT NULL, outcome_code TEXT NULL, UNIQUE(project_id, environment_id, sequence))",
    "CREATE INDEX IF NOT EXISTS runku_logs_by_time ON runku_operational_logs(project_id, environment_id, occurred_at_micros, sequence)",
    "CREATE INDEX IF NOT EXISTS runku_logs_by_function ON runku_operational_logs(project_id, environment_id, function_id, sequence)",
    "CREATE INDEX IF NOT EXISTS runku_logs_by_request ON runku_operational_logs(project_id, environment_id, request_id, sequence)",
    "CREATE INDEX IF NOT EXISTS runku_logs_by_invocation ON runku_operational_logs(project_id, environment_id, invocation_id, sequence)",
    "CREATE INDEX IF NOT EXISTS runku_logs_by_client ON runku_operational_logs(project_id, environment_id, client_id, credential_id, sequence)",
    "CREATE INDEX IF NOT EXISTS runku_logs_by_release ON runku_operational_logs(project_id, environment_id, release_id, sequence)",
];

/// Operational role selected for SQL repository composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogRepositoryRole {
    /// Local/test `SQLite` repository.
    Local,
    /// Authoritative `PostgreSQL` repository.
    Production,
}

/// Bounded SQL pool and acquisition policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogRepositoryConfig {
    /// Required backend role.
    pub role: LogRepositoryRole,
    /// Maximum pooled connections.
    pub max_connections: u32,
    /// Maximum wait for pool admission.
    pub acquire_timeout: Duration,
}

impl LogRepositoryConfig {
    /// Deterministic local `SQLite` configuration.
    pub const LOCAL: Self = Self {
        role: LogRepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };
    /// Bounded authoritative `PostgreSQL` configuration.
    pub const PRODUCTION: Self = Self {
        role: LogRepositoryRole::Production,
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
    };
}

/// Durable SQL Operational Logs repository with SQLite/PostgreSQL parity.
#[derive(Clone, Debug)]
pub struct SqlLogRepository {
    pool: AnyPool,
    backend: LogRepositoryBackend,
    counters: Arc<RepositoryCounters>,
}

#[derive(Debug, Default)]
struct RepositoryCounters {
    pruned: AtomicU64,
}

/// Bounded aggregate SQL repository telemetry without tenant-controlled labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogRepositoryTelemetrySnapshot {
    /// Records removed by successful retention transactions in this repository instance.
    pub pruned: u64,
}

impl SqlLogRepository {
    /// Opens local `SQLite`, verifies its safety settings, and applies migrations.
    ///
    /// # Errors
    ///
    /// Rejects production role, a non-SQLite URL, unsafe pool settings, or migration corruption.
    pub async fn connect_sqlite(
        url: &str,
        config: LogRepositoryConfig,
    ) -> Result<Self, LogRepositoryError> {
        if config.role != LogRepositoryRole::Local || !url.starts_with("sqlite:") {
            return Err(LogRepositoryError::Unsupported);
        }
        Self::connect(url, config, LogRepositoryBackend::SQLite).await
    }

    /// Opens authoritative `PostgreSQL` 16+, verifies session limits, and applies migrations.
    ///
    /// # Errors
    ///
    /// Rejects local role, a non-PostgreSQL URL, server versions below 16, or unsafe settings.
    pub async fn connect_postgres(
        url: &str,
        config: LogRepositoryConfig,
    ) -> Result<Self, LogRepositoryError> {
        if config.role != LogRepositoryRole::Production
            || !(url.starts_with("postgres://") || url.starts_with("postgresql://"))
        {
            return Err(LogRepositoryError::Unsupported);
        }
        Self::connect(url, config, LogRepositoryBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: LogRepositoryConfig,
        backend: LogRepositoryBackend,
    ) -> Result<Self, LogRepositoryError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || backend == LogRepositoryBackend::SQLite && config.max_connections != 1
        {
            return Err(LogRepositoryError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options =
            AnyConnectOptions::from_str(url).map_err(|_| LogRepositoryError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    match backend {
                        LogRepositoryBackend::SQLite => {
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        LogRepositoryBackend::PostgreSQL => {
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
            .map_err(map_sqlx)?;
        if backend == LogRepositoryBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx)?;
            if version < 160_000 {
                pool.close().await;
                return Err(LogRepositoryError::Unsupported);
            }
        }
        migrate(&pool, backend).await?;
        Ok(Self {
            pool,
            backend,
            counters: Arc::new(RepositoryCounters::default()),
        })
    }

    /// Returns aggregate repository telemetry without opening a database transaction.
    #[must_use]
    pub fn telemetry(&self) -> LogRepositoryTelemetrySnapshot {
        LogRepositoryTelemetrySnapshot {
            pruned: self.counters.pruned.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl LogRepository for SqlLogRepository {
    fn backend(&self) -> LogRepositoryBackend {
        self.backend
    }

    async fn append(&self, events: &[OperationalEventV1]) -> Result<LogCursor, LogRepositoryError> {
        if events.is_empty() || events.len() > LOG_APPEND_MAX_RECORDS {
            return Err(LogRepositoryError::LimitExceeded);
        }
        let mut ids = BTreeSet::new();
        for event in events {
            event
                .validate()
                .map_err(|_| LogRepositoryError::InvalidRequest)?;
            if !ids.insert(event.id) {
                return Err(LogRepositoryError::InvalidRequest);
            }
        }
        let scope = events[0].scope;
        if events.iter().any(|event| event.scope != scope) {
            return Err(LogRepositoryError::InvalidRequest);
        }

        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        sqlx::query("INSERT INTO runku_log_sequences (project_id, environment_id, next_sequence) VALUES ($1, $2, 1) ON CONFLICT(project_id, environment_id) DO NOTHING")
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        let sequence_sql = if self.backend == LogRepositoryBackend::PostgreSQL {
            "SELECT next_sequence FROM runku_log_sequences WHERE project_id = $1 AND environment_id = $2 FOR UPDATE"
        } else {
            "SELECT next_sequence FROM runku_log_sequences WHERE project_id = $1 AND environment_id = $2"
        };
        let stored_next = sqlx::query_scalar::<_, i64>(sequence_sql)
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        let mut next = u64::try_from(stored_next).map_err(|_| LogRepositoryError::Corruption)?;
        if next == 0 {
            return Err(LogRepositoryError::Corruption);
        }
        let mut last = LogCursor::START;
        for event in events {
            if let Some(existing) = event_by_id(&mut transaction, event.id).await? {
                if existing.event != *event {
                    return Err(LogRepositoryError::Corruption);
                }
                last = last.max(existing.cursor);
                continue;
            }
            let sequence = i64::try_from(next).map_err(|_| LogRepositoryError::LimitExceeded)?;
            insert_event(&mut transaction, sequence, event).await?;
            last = last.max(LogCursor::new(next));
            next = next
                .checked_add(1)
                .ok_or(LogRepositoryError::LimitExceeded)?;
        }
        sqlx::query("UPDATE runku_log_sequences SET next_sequence = $1 WHERE project_id = $2 AND environment_id = $3")
            .bind(i64::try_from(next).map_err(|_| LogRepositoryError::LimitExceeded)?)
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        transaction.commit().await.map_err(map_commit)?;
        Ok(last)
    }

    async fn query(&self, query: &LogQuery) -> Result<LogPage, LogRepositoryError> {
        query.validate()?;
        let function_id = query.function_id.map(|value| value.to_string());
        let request_id = query.request_id.map(|value| value.to_string());
        let invocation_id = query.invocation_id.map(|value| value.to_string());
        let client_id = query.client_id.map(|value| value.to_string());
        let credential_id = query.credential_id.map(|value| value.to_string());
        let release_id = query.release_id.map(|value| value.to_string());
        let mut sql = format!(
            "{SELECT_COLUMNS} WHERE project_id = $1 AND environment_id = $2 AND sequence > $3"
        );
        let mut position = 4_usize;
        for (column, present) in [
            ("stream", query.stream.is_some()),
            ("level_rank", query.minimum_level.is_some()),
            ("function_id", function_id.is_some()),
            ("request_id", request_id.is_some()),
            ("invocation_id", invocation_id.is_some()),
            ("client_id", client_id.is_some()),
            ("credential_id", credential_id.is_some()),
            ("release_id", release_id.is_some()),
        ] {
            if present {
                let operator = if column == "level_rank" { ">=" } else { "=" };
                write!(&mut sql, " AND {column} {operator} ${position}")
                    .map_err(|_| LogRepositoryError::Unavailable)?;
                position += 1;
            }
        }
        write!(&mut sql, " ORDER BY sequence ASC LIMIT ${position}")
            .map_err(|_| LogRepositoryError::Unavailable)?;
        // Only fixed column names/operators above enter this string; every request value is bound.
        let mut statement = sqlx::query(AssertSqlSafe(sql))
            .bind(query.scope.project_id().to_string())
            .bind(query.scope.environment_id().to_string())
            .bind(
                i64::try_from(query.after.get()).map_err(|_| LogRepositoryError::InvalidRequest)?,
            );
        if let Some(stream) = query.stream {
            statement = statement.bind(stream.as_str());
        }
        if let Some(level) = query.minimum_level {
            statement = statement.bind(level_rank(level));
        }
        if let Some(value) = function_id {
            statement = statement.bind(value);
        }
        if let Some(value) = request_id {
            statement = statement.bind(value);
        }
        if let Some(value) = invocation_id {
            statement = statement.bind(value);
        }
        if let Some(value) = client_id {
            statement = statement.bind(value);
        }
        if let Some(value) = credential_id {
            statement = statement.bind(value);
        }
        if let Some(value) = release_id {
            statement = statement.bind(value);
        }
        let rows = statement
            .bind(i64::from(query.limit))
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        let records = rows.iter().map(decode_row).collect::<Result<Vec<_>, _>>()?;
        let next = records.last().map_or(query.after, |record| record.cursor);
        Ok(LogPage { records, next })
    }

    async fn prune_before(
        &self,
        scope: EnvironmentScope,
        cutoff: TimestampMicros,
        maximum: u32,
        dry_run: bool,
    ) -> Result<PruneResult, LogRepositoryError> {
        if cutoff.get() < 0 || !(1..=LOG_PRUNE_MAX_RECORDS).contains(&maximum) {
            return Err(LogRepositoryError::InvalidRequest);
        }
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        if self.backend == LogRepositoryBackend::PostgreSQL {
            // Serialize retention with append/retention in the same scope without a global lock.
            sqlx::query("SELECT next_sequence FROM runku_log_sequences WHERE project_id = $1 AND environment_id = $2 FOR UPDATE")
                .bind(scope.project_id().to_string())
                .bind(scope.environment_id().to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(map_sqlx)?;
        }
        let fetch = maximum
            .checked_add(1)
            .ok_or(LogRepositoryError::LimitExceeded)?;
        let sequences = sqlx::query_scalar::<_, i64>(
            "SELECT sequence FROM runku_operational_logs WHERE project_id = $1 AND environment_id = $2 AND occurred_at_micros < $3 ORDER BY sequence ASC LIMIT $4",
        )
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(cutoff.get())
        .bind(i64::from(fetch))
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
        let more = sequences.len()
            > usize::try_from(maximum).map_err(|_| LogRepositoryError::LimitExceeded)?;
        let selected = sequences
            .len()
            .min(usize::try_from(maximum).map_err(|_| LogRepositoryError::LimitExceeded)?);
        let matched = u32::try_from(selected).map_err(|_| LogRepositoryError::LimitExceeded)?;
        let deleted = if dry_run || selected == 0 {
            0
        } else {
            let last = sequences[selected - 1];
            let result = sqlx::query("DELETE FROM runku_operational_logs WHERE project_id = $1 AND environment_id = $2 AND occurred_at_micros < $3 AND sequence <= $4")
                .bind(scope.project_id().to_string())
                .bind(scope.environment_id().to_string())
                .bind(cutoff.get())
                .bind(last)
                .execute(&mut *transaction)
                .await
                .map_err(map_sqlx)?;
            u32::try_from(result.rows_affected()).map_err(|_| LogRepositoryError::Corruption)?
        };
        transaction.commit().await.map_err(map_commit)?;
        self.counters
            .pruned
            .fetch_add(u64::from(deleted), Ordering::Relaxed);
        Ok(PruneResult {
            matched,
            deleted,
            more,
        })
    }

    async fn prune_archived_before(
        &self,
        scope: EnvironmentScope,
        cutoff: TimestampMicros,
        archived_through: LogCursor,
        maximum: u32,
        dry_run: bool,
    ) -> Result<PruneResult, LogRepositoryError> {
        if cutoff.get() < 0 || !(1..=LOG_PRUNE_MAX_RECORDS).contains(&maximum) {
            return Err(LogRepositoryError::InvalidRequest);
        }
        let archived_through = i64::try_from(archived_through.get())
            .map_err(|_| LogRepositoryError::InvalidRequest)?;
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        if self.backend == LogRepositoryBackend::PostgreSQL {
            sqlx::query("SELECT next_sequence FROM runku_log_sequences WHERE project_id = $1 AND environment_id = $2 FOR UPDATE")
                .bind(scope.project_id().to_string())
                .bind(scope.environment_id().to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(map_sqlx)?;
        }
        let fetch = maximum
            .checked_add(1)
            .ok_or(LogRepositoryError::LimitExceeded)?;
        let sequences = sqlx::query_scalar::<_, i64>(
            "SELECT sequence FROM runku_operational_logs WHERE project_id = $1 AND environment_id = $2 AND occurred_at_micros < $3 AND sequence <= $4 ORDER BY sequence ASC LIMIT $5",
        )
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(cutoff.get())
        .bind(archived_through)
        .bind(i64::from(fetch))
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
        let maximum = usize::try_from(maximum).map_err(|_| LogRepositoryError::LimitExceeded)?;
        let more = sequences.len() > maximum;
        let selected = sequences.len().min(maximum);
        let matched = u32::try_from(selected).map_err(|_| LogRepositoryError::LimitExceeded)?;
        let deleted = if dry_run || selected == 0 {
            0
        } else {
            let last = sequences[selected - 1];
            let result = sqlx::query("DELETE FROM runku_operational_logs WHERE project_id = $1 AND environment_id = $2 AND occurred_at_micros < $3 AND sequence <= $4")
                .bind(scope.project_id().to_string())
                .bind(scope.environment_id().to_string())
                .bind(cutoff.get())
                .bind(last)
                .execute(&mut *transaction)
                .await
                .map_err(map_sqlx)?;
            u32::try_from(result.rows_affected()).map_err(|_| LogRepositoryError::Corruption)?
        };
        transaction.commit().await.map_err(map_commit)?;
        self.counters
            .pruned
            .fetch_add(u64::from(deleted), Ordering::Relaxed);
        Ok(PruneResult {
            matched,
            deleted,
            more,
        })
    }

    async fn close(&self) {
        self.pool.close().await;
    }
}

const SELECT_COLUMNS: &str = "SELECT event_id, project_id, environment_id, sequence, occurred_at_micros, request_id, invocation_id, parent_invocation_id, release_id, dev_revision_id, function_id, function_name, function_type, client_id, credential_id, principal_kind, stream, level, event_kind, message, fields, duration_micros, outcome_code FROM runku_operational_logs";

async fn event_by_id(
    transaction: &mut Transaction<'_, Any>,
    id: OperationalEventId,
) -> Result<Option<SequencedOperationalEvent>, LogRepositoryError> {
    let sql = format!("{SELECT_COLUMNS} WHERE event_id = $1");
    // `SELECT_COLUMNS` and the predicate are compile-time constants; the ID remains bound.
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(id.to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(map_sqlx)?;
    row.as_ref().map(decode_row).transpose()
}

#[allow(clippy::too_many_lines)]
async fn insert_event(
    transaction: &mut Transaction<'_, Any>,
    sequence: i64,
    event: &OperationalEventV1,
) -> Result<(), LogRepositoryError> {
    let fields = event
        .fields
        .as_ref()
        .map(encode_stored_value)
        .transpose()
        .map_err(|_| LogRepositoryError::InvalidRequest)?;
    sqlx::query("INSERT INTO runku_operational_logs (event_id, project_id, environment_id, sequence, occurred_at_micros, request_id, invocation_id, parent_invocation_id, release_id, dev_revision_id, function_id, function_name, function_type, client_id, credential_id, principal_kind, stream, level, level_rank, event_kind, message, fields, duration_micros, outcome_code) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)")
        .bind(event.id.to_string())
        .bind(event.scope.project_id().to_string())
        .bind(event.scope.environment_id().to_string())
        .bind(sequence)
        .bind(event.occurred_at.get())
        .bind(event.request_id.to_string())
        .bind(event.invocation_id.to_string())
        .bind(event.parent_invocation_id.map(|value| value.to_string()))
        .bind(event.release_id.to_string())
        .bind(event.dev_revision_id.map(|value| value.to_string()))
        .bind(event.function_id.to_string())
        .bind(event.function_name.as_str())
        .bind(function_type_text(event.function_type))
        .bind(event.client_id.map(|value| value.to_string()))
        .bind(event.credential_id.map(|value| value.to_string()))
        .bind(event.principal_kind.as_str())
        .bind(event.stream.as_str())
        .bind(event.level.as_str())
        .bind(level_rank(event.level))
        .bind(event.kind.as_str())
        .bind(event.message.as_ref().map(LogMessage::as_str))
        .bind(fields)
        .bind(event.duration_micros.map(|value| i64::try_from(value).map_err(|_| LogRepositoryError::LimitExceeded)).transpose()?)
        .bind(event.outcome_code.as_ref().map(OutcomeCode::as_str))
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn decode_row(row: &AnyRow) -> Result<SequencedOperationalEvent, LogRepositoryError> {
    let sequence: i64 = row
        .try_get("sequence")
        .map_err(|_| LogRepositoryError::Corruption)?;
    let fields: Option<Vec<u8>> = row
        .try_get("fields")
        .map_err(|_| LogRepositoryError::Corruption)?;
    let message: Option<String> = row
        .try_get("message")
        .map_err(|_| LogRepositoryError::Corruption)?;
    let outcome: Option<String> = row
        .try_get("outcome_code")
        .map_err(|_| LogRepositoryError::Corruption)?;
    let duration: Option<i64> = row
        .try_get("duration_micros")
        .map_err(|_| LogRepositoryError::Corruption)?;
    let event = OperationalEventV1 {
        id: parse(row, "event_id")?,
        occurred_at: TimestampMicros::new(
            row.try_get("occurred_at_micros")
                .map_err(|_| LogRepositoryError::Corruption)?,
        ),
        scope: EnvironmentScope::new(parse(row, "project_id")?, parse(row, "environment_id")?),
        request_id: parse(row, "request_id")?,
        invocation_id: parse(row, "invocation_id")?,
        parent_invocation_id: parse_optional(row, "parent_invocation_id")?,
        release_id: parse(row, "release_id")?,
        dev_revision_id: parse_optional(row, "dev_revision_id")?,
        function_id: parse(row, "function_id")?,
        function_name: parse(row, "function_name")?,
        function_type: parse_function_type(text(row, "function_type")?)?,
        client_id: parse_optional(row, "client_id")?,
        credential_id: parse_optional(row, "credential_id")?,
        principal_kind: parse_principal(text(row, "principal_kind")?)?,
        stream: parse_stream(text(row, "stream")?)?,
        level: parse_level(text(row, "level")?)?,
        kind: parse_kind(text(row, "event_kind")?)?,
        message: message
            .map(LogMessage::new)
            .transpose()
            .map_err(|_| LogRepositoryError::Corruption)?,
        fields: fields
            .map(|bytes| decode_stored_value(&bytes))
            .transpose()
            .map_err(|_| LogRepositoryError::Corruption)?,
        duration_micros: duration
            .map(|value| u64::try_from(value).map_err(|_| LogRepositoryError::Corruption))
            .transpose()?,
        outcome_code: outcome
            .map(OutcomeCode::new)
            .transpose()
            .map_err(|_| LogRepositoryError::Corruption)?,
    };
    event
        .validate()
        .map_err(|_| LogRepositoryError::Corruption)?;
    Ok(SequencedOperationalEvent {
        cursor: LogCursor::new(
            u64::try_from(sequence).map_err(|_| LogRepositoryError::Corruption)?,
        ),
        event,
    })
}

fn parse<T: FromStr>(row: &AnyRow, column: &str) -> Result<T, LogRepositoryError> {
    text(row, column)?
        .parse()
        .map_err(|_| LogRepositoryError::Corruption)
}

fn parse_optional<T: FromStr>(row: &AnyRow, column: &str) -> Result<Option<T>, LogRepositoryError> {
    let value: Option<String> = row
        .try_get(column)
        .map_err(|_| LogRepositoryError::Corruption)?;
    value
        .map(|value| value.parse().map_err(|_| LogRepositoryError::Corruption))
        .transpose()
}

fn text<'a>(row: &'a AnyRow, column: &str) -> Result<&'a str, LogRepositoryError> {
    row.try_get(column)
        .map_err(|_| LogRepositoryError::Corruption)
}

const fn function_type_text(value: FunctionType) -> &'static str {
    match value {
        FunctionType::Query => "query",
        FunctionType::Mutation => "mutation",
        FunctionType::Action => "action",
    }
}

fn parse_function_type(value: &str) -> Result<FunctionType, LogRepositoryError> {
    match value {
        "query" => Ok(FunctionType::Query),
        "mutation" => Ok(FunctionType::Mutation),
        "action" => Ok(FunctionType::Action),
        _ => Err(LogRepositoryError::Corruption),
    }
}

const fn level_rank(value: LogLevel) -> i64 {
    match value {
        LogLevel::Debug => 10,
        LogLevel::Info => 20,
        LogLevel::Warn => 30,
        LogLevel::Error => 40,
    }
}

fn parse_level(value: &str) -> Result<LogLevel, LogRepositoryError> {
    match value {
        "debug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" => Ok(LogLevel::Warn),
        "error" => Ok(LogLevel::Error),
        _ => Err(LogRepositoryError::Corruption),
    }
}

fn parse_stream(value: &str) -> Result<LogStream, LogRepositoryError> {
    match value {
        "platform" => Ok(LogStream::Platform),
        "function" => Ok(LogStream::Function),
        _ => Err(LogRepositoryError::Corruption),
    }
}

fn parse_kind(value: &str) -> Result<LogEventKind, LogRepositoryError> {
    match value {
        "invocation_started" => Ok(LogEventKind::InvocationStarted),
        "invocation_completed" => Ok(LogEventKind::InvocationCompleted),
        "function_message" => Ok(LogEventKind::FunctionMessage),
        _ => Err(LogRepositoryError::Corruption),
    }
}

fn parse_principal(value: &str) -> Result<LogPrincipalKind, LogRepositoryError> {
    match value {
        "none" => Ok(LogPrincipalKind::None),
        "guest" => Ok(LogPrincipalKind::Guest),
        "user" => Ok(LogPrincipalKind::User),
        "service" => Ok(LogPrincipalKind::Service),
        "system" => Ok(LogPrincipalKind::System),
        _ => Err(LogRepositoryError::Corruption),
    }
}

async fn migrate(pool: &AnyPool, backend: LogRepositoryBackend) -> Result<(), LogRepositoryError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_log_migrations (version BIGINT PRIMARY KEY, checksum TEXT NOT NULL)")
        .execute(pool)
        .await
        .map_err(map_sqlx)?;
    let mut transaction = pool.begin().await.map_err(map_sqlx)?;
    if backend == LogRepositoryBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(7_224_856_020_i64)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
    }
    let checksum = migration_checksum();
    let existing = sqlx::query_scalar::<_, String>(
        "SELECT checksum FROM runku_log_migrations WHERE version = $1",
    )
    .bind(SCHEMA_VERSION)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(map_sqlx)?;
    if let Some(existing) = existing {
        if existing != checksum {
            return Err(LogRepositoryError::Corruption);
        }
        return transaction.commit().await.map_err(map_commit);
    }
    for statement in SCHEMA {
        transaction.execute(*statement).await.map_err(map_sqlx)?;
    }
    sqlx::query("INSERT INTO runku_log_migrations (version, checksum) VALUES ($1, $2)")
        .bind(SCHEMA_VERSION)
        .bind(checksum)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
    transaction.commit().await.map_err(map_commit)
}

fn migration_checksum() -> String {
    use std::fmt::Write as _;
    let mut hasher = Sha256::new();
    hasher.update(b"runku-operational-logs-schema-v1\0");
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

fn map_sqlx(_: sqlx::Error) -> LogRepositoryError {
    LogRepositoryError::Unavailable
}

fn map_commit(_: sqlx::Error) -> LogRepositoryError {
    LogRepositoryError::Unavailable
}
