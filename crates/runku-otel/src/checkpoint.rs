use std::{fmt, str::FromStr, time::Duration};

use async_trait::async_trait;
use runku_core::EnvironmentScope;
use runku_observability::LogCursor;
use sha2::{Digest as _, Sha256};
use sqlx::{
    AnyPool, Executor as _, Row as _,
    any::{AnyConnectOptions, AnyPoolOptions},
};
use thiserror::Error;

use crate::{OtlpEndpoint, OtlpHeaders};

const SCHEMA_VERSION: i64 = 1;
const HEX: &[u8; 16] = b"0123456789abcdef";
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_otel_checkpoints (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, exporter_name TEXT NOT NULL, destination_digest TEXT NOT NULL, cursor BIGINT NOT NULL, revision BIGINT NOT NULL, PRIMARY KEY(project_id, environment_id, exporter_name))",
];

/// Stable operator-selected exporter identity within one Environment.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OtlpExporterName(String);

impl OtlpExporterName {
    /// Returns the canonical lowercase name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for OtlpExporterName {
    type Err = CheckpointError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_lowercase())
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            })
        {
            return Err(CheckpointError::InvalidRequest);
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for OtlpExporterName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// SHA-256 binding of endpoint, safe header names, and mapping version; header values excluded.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct OtlpDestinationDigest([u8; 32]);

impl OtlpDestinationDigest {
    /// Computes the destination identity without incorporating sensitive header values.
    #[must_use]
    pub fn new(endpoint: &OtlpEndpoint, headers: &OtlpHeaders) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"runku-otlp-destination-v1\0");
        hasher.update(endpoint.as_url().as_str().as_bytes());
        let mut names = headers.names().collect::<Vec<_>>();
        names.sort_unstable();
        for name in names {
            hasher.update(b"\0");
            hasher.update(name.as_bytes());
        }
        Self(hasher.finalize().into())
    }

    fn parse(value: &str) -> Result<Self, CheckpointError> {
        let hex = value
            .strip_prefix("sha256:")
            .ok_or(CheckpointError::Corruption)?;
        if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(CheckpointError::Corruption);
        }
        let mut output = [0_u8; 32];
        for (index, chunk) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let text = std::str::from_utf8(chunk).map_err(|_| CheckpointError::Corruption)?;
            output[index] =
                u8::from_str_radix(text, 16).map_err(|_| CheckpointError::Corruption)?;
        }
        let digest = Self(output);
        if digest.to_string() != value {
            return Err(CheckpointError::Corruption);
        }
        Ok(digest)
    }
}

impl fmt::Debug for OtlpDestinationDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for OtlpDestinationDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("sha256:")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One durable export position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportCheckpoint {
    /// Exact tenant/environment boundary.
    pub scope: EnvironmentScope,
    /// Independent destination/exporter name.
    pub exporter: OtlpExporterName,
    /// Configuration binding that prevents accidental destination changes.
    pub destination: OtlpDestinationDigest,
    /// Last fully acknowledged Operational Logs cursor.
    pub cursor: LogCursor,
    /// Monotonic checkpoint mutation revision.
    pub revision: u64,
}

/// Result of one checkpoint compare-and-swap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointAdvance {
    /// This caller advanced the checkpoint.
    Advanced,
    /// Another caller already advanced to the exact same cursor.
    Replayed,
}

/// Sanitized durable checkpoint failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CheckpointError {
    /// Name, cursor, scope, or transition is invalid.
    #[error("OTLP checkpoint request is invalid")]
    InvalidRequest,
    /// Existing exporter name is bound to a different destination.
    #[error("OTLP exporter destination conflicts with its checkpoint")]
    ConfigurationDrift,
    /// Compare-and-swap observed a different monotonic cursor.
    #[error("OTLP checkpoint changed concurrently")]
    Conflict,
    /// Database/pool is temporarily unavailable.
    #[error("OTLP checkpoint repository is unavailable")]
    Unavailable,
    /// Durable schema or contents are corrupt.
    #[error("OTLP checkpoint repository is corrupt")]
    Corruption,
    /// Backend/role combination is unsupported.
    #[error("OTLP checkpoint repository backend is unsupported")]
    Unsupported,
}

impl CheckpointError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "OTLP_CHECKPOINT_INVALID",
            Self::ConfigurationDrift => "OTLP_DESTINATION_DRIFT",
            Self::Conflict => "OTLP_CHECKPOINT_CONFLICT",
            Self::Unavailable => "OTLP_CHECKPOINT_UNAVAILABLE",
            Self::Corruption => "OTLP_CHECKPOINT_CORRUPT",
            Self::Unsupported => "OTLP_CHECKPOINT_UNSUPPORTED",
        }
    }
}

/// Async checkpoint boundary independent from Operational Logs storage.
#[async_trait]
pub trait ExportCheckpointRepository: fmt::Debug + Send + Sync {
    /// Loads or atomically creates cursor zero and validates destination binding.
    async fn load_or_create(
        &self,
        scope: EnvironmentScope,
        exporter: &OtlpExporterName,
        destination: OtlpDestinationDigest,
    ) -> Result<ExportCheckpoint, CheckpointError>;

    /// Advances from `expected` to a strictly greater fully acknowledged cursor.
    async fn advance(
        &self,
        checkpoint: &ExportCheckpoint,
        next: LogCursor,
    ) -> Result<CheckpointAdvance, CheckpointError>;

    /// Closes database resources.
    async fn close(&self);
}

/// Required SQL deployment role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpRepositoryRole {
    /// Local/test `SQLite`.
    Local,
    /// Authoritative `PostgreSQL` 16+.
    Production,
}

/// Bounded checkpoint SQL pool policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OtlpRepositoryConfig {
    /// Required backend role.
    pub role: OtlpRepositoryRole,
    /// Maximum SQL connections.
    pub max_connections: u32,
    /// Maximum pool admission wait.
    pub acquire_timeout: Duration,
}

impl OtlpRepositoryConfig {
    /// Local deterministic `SQLite` policy.
    pub const LOCAL: Self = Self {
        role: OtlpRepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };
    /// Shared `PostgreSQL` policy.
    pub const PRODUCTION: Self = Self {
        role: OtlpRepositoryRole::Production,
        max_connections: 8,
        acquire_timeout: Duration::from_secs(5),
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Backend {
    SQLite,
    PostgreSql,
}

/// SQLite/PostgreSQL checkpoint repository with identical CAS semantics.
#[derive(Clone, Debug)]
pub struct SqlExportCheckpointRepository {
    pool: AnyPool,
}

impl SqlExportCheckpointRepository {
    /// Opens/migrates local `SQLite`.
    ///
    /// # Errors
    ///
    /// Rejects role/URL/pool mismatch, migration drift, or unavailable storage.
    pub async fn connect_sqlite(
        url: &str,
        config: OtlpRepositoryConfig,
    ) -> Result<Self, CheckpointError> {
        if config.role != OtlpRepositoryRole::Local || !url.starts_with("sqlite:") {
            return Err(CheckpointError::Unsupported);
        }
        Self::connect(url, config, Backend::SQLite).await
    }

    /// Opens/migrates authoritative `PostgreSQL` 16+.
    ///
    /// # Errors
    ///
    /// Rejects role/URL/pool mismatch, old server, migration drift, or unavailable storage.
    pub async fn connect_postgres(
        url: &str,
        config: OtlpRepositoryConfig,
    ) -> Result<Self, CheckpointError> {
        if config.role != OtlpRepositoryRole::Production
            || !(url.starts_with("postgres://") || url.starts_with("postgresql://"))
        {
            return Err(CheckpointError::Unsupported);
        }
        Self::connect(url, config, Backend::PostgreSql).await
    }

    async fn connect(
        url: &str,
        config: OtlpRepositoryConfig,
        backend: Backend,
    ) -> Result<Self, CheckpointError> {
        if config.max_connections == 0
            || config.max_connections > 32
            || config.acquire_timeout.is_zero()
            || backend == Backend::SQLite && config.max_connections != 1
        {
            return Err(CheckpointError::InvalidRequest);
        }
        sqlx::any::install_default_drivers();
        let options = AnyConnectOptions::from_str(url).map_err(|_| CheckpointError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    match backend {
                        Backend::SQLite => {
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        Backend::PostgreSql => {
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
            .map_err(|_| CheckpointError::Unavailable)?;
        if backend == Backend::PostgreSql {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(|_| CheckpointError::Unavailable)?;
            if version < 160_000 {
                pool.close().await;
                return Err(CheckpointError::Unsupported);
            }
        }
        migrate(&pool).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl ExportCheckpointRepository for SqlExportCheckpointRepository {
    async fn load_or_create(
        &self,
        scope: EnvironmentScope,
        exporter: &OtlpExporterName,
        destination: OtlpDestinationDigest,
    ) -> Result<ExportCheckpoint, CheckpointError> {
        let mut transaction = self.pool.begin().await.map_err(map_sql)?;
        sqlx::query("INSERT INTO runku_otel_checkpoints (project_id, environment_id, exporter_name, destination_digest, cursor, revision) VALUES ($1, $2, $3, $4, 0, 0) ON CONFLICT(project_id, environment_id, exporter_name) DO NOTHING")
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .bind(exporter.as_str())
            .bind(destination.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(map_sql)?;
        let row = sqlx::query("SELECT destination_digest, cursor, revision FROM runku_otel_checkpoints WHERE project_id = $1 AND environment_id = $2 AND exporter_name = $3")
            .bind(scope.project_id().to_string())
            .bind(scope.environment_id().to_string())
            .bind(exporter.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_sql)?;
        let stored = OtlpDestinationDigest::parse(
            row.try_get::<&str, _>("destination_digest")
                .map_err(|_| CheckpointError::Corruption)?,
        )?;
        if stored != destination {
            return Err(CheckpointError::ConfigurationDrift);
        }
        let cursor = decode_u64(&row, "cursor")?;
        let revision = decode_u64(&row, "revision")?;
        transaction.commit().await.map_err(map_sql)?;
        Ok(ExportCheckpoint {
            scope,
            exporter: exporter.clone(),
            destination,
            cursor: LogCursor::new(cursor),
            revision,
        })
    }

    async fn advance(
        &self,
        checkpoint: &ExportCheckpoint,
        next: LogCursor,
    ) -> Result<CheckpointAdvance, CheckpointError> {
        if next <= checkpoint.cursor || i64::try_from(next.get()).is_err() {
            return Err(CheckpointError::InvalidRequest);
        }
        let result = sqlx::query("UPDATE runku_otel_checkpoints SET cursor = $1, revision = revision + 1 WHERE project_id = $2 AND environment_id = $3 AND exporter_name = $4 AND destination_digest = $5 AND cursor = $6 AND revision = $7")
            .bind(i64::try_from(next.get()).map_err(|_| CheckpointError::InvalidRequest)?)
            .bind(checkpoint.scope.project_id().to_string())
            .bind(checkpoint.scope.environment_id().to_string())
            .bind(checkpoint.exporter.as_str())
            .bind(checkpoint.destination.to_string())
            .bind(i64::try_from(checkpoint.cursor.get()).map_err(|_| CheckpointError::InvalidRequest)?)
            .bind(i64::try_from(checkpoint.revision).map_err(|_| CheckpointError::InvalidRequest)?)
            .execute(&self.pool)
            .await
            .map_err(map_sql)?;
        if result.rows_affected() == 1 {
            return Ok(CheckpointAdvance::Advanced);
        }
        let current = self
            .load_or_create(
                checkpoint.scope,
                &checkpoint.exporter,
                checkpoint.destination,
            )
            .await?;
        if current.cursor == next {
            Ok(CheckpointAdvance::Replayed)
        } else {
            Err(CheckpointError::Conflict)
        }
    }

    async fn close(&self) {
        self.pool.close().await;
    }
}

async fn migrate(pool: &AnyPool) -> Result<(), CheckpointError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_otel_migrations (version BIGINT PRIMARY KEY, checksum TEXT NOT NULL)")
        .execute(pool)
        .await
        .map_err(map_sql)?;
    let checksum = schema_checksum();
    if let Some(stored) = sqlx::query_scalar::<_, String>(
        "SELECT checksum FROM runku_otel_migrations WHERE version = $1",
    )
    .bind(SCHEMA_VERSION)
    .fetch_optional(pool)
    .await
    .map_err(map_sql)?
    {
        return if stored == checksum {
            Ok(())
        } else {
            Err(CheckpointError::Corruption)
        };
    }
    let mut transaction = pool.begin().await.map_err(map_sql)?;
    for statement in SCHEMA {
        transaction.execute(*statement).await.map_err(map_sql)?;
    }
    sqlx::query("INSERT INTO runku_otel_migrations (version, checksum) VALUES ($1, $2) ON CONFLICT(version) DO NOTHING")
        .bind(SCHEMA_VERSION)
        .bind(&checksum)
        .execute(&mut *transaction)
        .await
        .map_err(map_sql)?;
    transaction.commit().await.map_err(map_sql)?;
    let stored = sqlx::query_scalar::<_, String>(
        "SELECT checksum FROM runku_otel_migrations WHERE version = $1",
    )
    .bind(SCHEMA_VERSION)
    .fetch_one(pool)
    .await
    .map_err(map_sql)?;
    if stored != checksum {
        return Err(CheckpointError::Corruption);
    }
    Ok(())
}

fn schema_checksum() -> String {
    let mut hasher = Sha256::new();
    for statement in SCHEMA {
        hasher.update(statement.as_bytes());
        hasher.update(b"\0");
    }
    let bytes: [u8; 32] = hasher.finalize().into();
    let mut output = String::from("sha256:");
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn decode_u64(row: &sqlx::any::AnyRow, column: &str) -> Result<u64, CheckpointError> {
    let value = row
        .try_get::<i64, _>(column)
        .map_err(|_| CheckpointError::Corruption)?;
    u64::try_from(value).map_err(|_| CheckpointError::Corruption)
}

fn map_sql(_error: sqlx::Error) -> CheckpointError {
    CheckpointError::Unavailable
}
