//! Durable Environment-scoped variables and encrypted secret references.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{fmt, str::FromStr, sync::Arc, time::Duration};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use runku_core::{EnvironmentScope, OperationId, OperatorId};
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};
use sqlx::{
    Any, AnyPool, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

const MAX_NAME_BYTES: usize = 64;
const MAX_VARIABLE_BYTES: usize = 16 * 1024;
const MAX_SECRET_BYTES: usize = 64 * 1024;
const MAX_ENTRIES: i64 = 512;
const MIGRATION_1: &[&str] = &[
    "CREATE TABLE runku_environment_configuration_state (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, revision BIGINT NOT NULL CHECK(revision >= 0), PRIMARY KEY(project_id,environment_id))",
    "CREATE TABLE runku_environment_configuration_entries (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('variable','secret')), variable_value TEXT NULL, secret_nonce BYTEA NULL, secret_ciphertext BYTEA NULL, revision BIGINT NOT NULL CHECK(revision > 0), created_at_micros BIGINT NOT NULL CHECK(created_at_micros >= 0), updated_at_micros BIGINT NOT NULL CHECK(updated_at_micros >= created_at_micros), PRIMARY KEY(project_id,environment_id,name), CHECK((kind='variable' AND variable_value IS NOT NULL AND secret_nonce IS NULL AND secret_ciphertext IS NULL) OR (kind='secret' AND variable_value IS NULL AND secret_nonce IS NOT NULL AND secret_ciphertext IS NOT NULL)))",
    "CREATE TABLE runku_environment_configuration_operations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, command_digest BYTEA NOT NULL CHECK(length(command_digest)=32), name TEXT NOT NULL, action TEXT NOT NULL CHECK(action IN ('set','delete')), configuration_revision BIGINT NOT NULL CHECK(configuration_revision > 0), result_kind TEXT NULL CHECK(result_kind IS NULL OR result_kind IN ('variable','secret')), result_variable_value TEXT NULL, result_entry_revision BIGINT NULL CHECK(result_entry_revision IS NULL OR result_entry_revision > 0), result_created_at_micros BIGINT NULL CHECK(result_created_at_micros IS NULL OR result_created_at_micros >= 0), result_updated_at_micros BIGINT NULL CHECK(result_updated_at_micros IS NULL OR result_updated_at_micros >= result_created_at_micros), completed_at_micros BIGINT NOT NULL CHECK(completed_at_micros >= 0), PRIMARY KEY(project_id,environment_id,operation_id), CHECK((action='delete' AND result_kind IS NULL AND result_variable_value IS NULL AND result_entry_revision IS NULL AND result_created_at_micros IS NULL AND result_updated_at_micros IS NULL) OR (action='set' AND result_kind IS NOT NULL AND result_entry_revision IS NOT NULL AND result_created_at_micros IS NOT NULL AND result_updated_at_micros IS NOT NULL AND ((result_kind='variable' AND result_variable_value IS NOT NULL) OR (result_kind='secret' AND result_variable_value IS NULL)))))",
    "CREATE TABLE runku_environment_configuration_audit (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, sequence BIGINT NOT NULL CHECK(sequence > 0), operation_id TEXT NOT NULL, actor TEXT NOT NULL, name TEXT NOT NULL, action TEXT NOT NULL CHECK(action IN ('set','delete')), kind TEXT NOT NULL CHECK(kind IN ('variable','secret')), configuration_revision BIGINT NOT NULL CHECK(configuration_revision > 0), occurred_at_micros BIGINT NOT NULL CHECK(occurred_at_micros >= 0), PRIMARY KEY(project_id,environment_id,sequence), UNIQUE(project_id,environment_id,operation_id))",
];

/// Stable configuration registry failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ConfigurationError {
    /// Input is malformed or violates a bound.
    #[error("environment configuration input is invalid")]
    InvalidInput,
    /// The configured entry limit was reached.
    #[error("environment configuration limit exceeded")]
    LimitExceeded,
    /// The exact entry does not exist.
    #[error("environment configuration entry not found")]
    NotFound,
    /// The expected Environment configuration revision does not match.
    #[error("environment configuration conflict")]
    Conflict,
    /// An operation identity was reused for different intent.
    #[error("environment configuration operation identity reused")]
    OperationIdReused,
    /// The SQLite authority is temporarily busy.
    #[error("environment configuration authority busy")]
    Busy,
    /// Storage is unavailable before a confirmed commit.
    #[error("environment configuration authority unavailable")]
    Unavailable,
    /// Persisted state or encrypted material is corrupt.
    #[error("environment configuration authority corrupt")]
    Corruption,
    /// Secret encryption or another internal invariant failed.
    #[error("environment configuration internal failure")]
    Internal,
}

impl ConfigurationError {
    /// Stable public error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "ENVIRONMENT_CONFIGURATION_INVALID_INPUT",
            Self::LimitExceeded => "ENVIRONMENT_CONFIGURATION_LIMIT_EXCEEDED",
            Self::NotFound => "ENVIRONMENT_CONFIGURATION_NOT_FOUND",
            Self::Conflict => "ENVIRONMENT_CONFIGURATION_CONFLICT",
            Self::OperationIdReused => "ENVIRONMENT_CONFIGURATION_OPERATION_ID_REUSED",
            Self::Busy => "ENVIRONMENT_CONFIGURATION_BUSY",
            Self::Unavailable => "ENVIRONMENT_CONFIGURATION_UNAVAILABLE",
            Self::Corruption => "ENVIRONMENT_CONFIGURATION_CORRUPTION",
            Self::Internal => "ENVIRONMENT_CONFIGURATION_INTERNAL",
        }
    }
}

/// Validated Environment configuration entry name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConfigurationName(String);

impl ConfigurationName {
    /// Returns the canonical name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConfigurationName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ConfigurationName {
    type Err = ConfigurationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > MAX_NAME_BYTES
            || value.starts_with('_')
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ConfigurationError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Customer-visible entry kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationKind {
    /// Non-secret UTF-8 value visible in authorized administration responses.
    Variable,
    /// Encrypted value represented only by its name and revision to administrators.
    Secret,
}

impl ConfigurationKind {
    /// Canonical protocol spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Variable => "variable",
            Self::Secret => "secret",
        }
    }
}

impl FromStr for ConfigurationKind {
    type Err = ConfigurationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "variable" => Ok(Self::Variable),
            "secret" => Ok(Self::Secret),
            _ => Err(ConfigurationError::Corruption),
        }
    }
}

/// Secret encryption key supplied outside persisted Product state.
#[derive(Clone)]
pub struct ConfigurationEncryptionKey(Arc<Zeroizing<[u8; 32]>>);

impl ConfigurationEncryptionKey {
    /// Wraps an exact 256-bit encryption key.
    #[must_use]
    pub fn new(value: [u8; 32]) -> Self {
        Self(Arc::new(Zeroizing::new(value)))
    }
}

impl fmt::Debug for ConfigurationEncryptionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConfigurationEncryptionKey([REDACTED])")
    }
}

/// Safe administrative projection of one entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationEntry {
    /// Exact Environment scope.
    pub scope: EnvironmentScope,
    /// Canonical entry name.
    pub name: ConfigurationName,
    /// Variable or secret.
    pub kind: ConfigurationKind,
    /// Plain variable value; always absent for a secret.
    pub variable_value: Option<String>,
    /// Global Environment configuration revision that last changed this entry.
    pub revision: u64,
    /// Creation timestamp.
    pub created_at: TimestampMicros,
    /// Last update/rotation timestamp.
    pub updated_at: TimestampMicros,
}

/// Result of one durable configuration mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationMutationResult {
    /// New global configuration revision.
    pub configuration_revision: u64,
    /// Current entry after set, or absent after delete.
    pub entry: Option<ConfigurationEntry>,
    /// Whether an identical operation was replayed.
    pub replayed: bool,
}

/// One pageless bounded configuration snapshot for a single Environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationSnapshot {
    /// Global configuration revision, zero before the first mutation.
    pub configuration_revision: u64,
    /// Name-ordered safe administrative entries.
    pub entries: Vec<ConfigurationEntry>,
}

/// One immutable configuration audit event without variable or secret values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationAuditEntry {
    /// Monotonic sequence within the exact Environment.
    pub sequence: u64,
    /// Idempotent mutation identity.
    pub operation_id: OperationId,
    /// Authorized Product operator that made the change.
    pub actor: OperatorId,
    /// Entry name affected by the change.
    pub name: ConfigurationName,
    /// `set` or `delete`.
    pub action: String,
    /// Entry kind at the time of the change.
    pub kind: ConfigurationKind,
    /// Global configuration revision produced by the change.
    pub configuration_revision: u64,
    /// Caller-supplied event time.
    pub occurred_at: TimestampMicros,
}

/// Bounded newest-first configuration history page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationHistory {
    /// Audit events ordered by descending sequence.
    pub entries: Vec<ConfigurationAuditEntry>,
    /// Sequence to pass as `before_sequence` for the next page.
    pub next_before_sequence: Option<u64>,
}

/// Durable SQLite-backed Environment configuration authority.
#[derive(Clone, Debug)]
pub struct EnvironmentConfigurationRegistry {
    pool: AnyPool,
    encryption_key: ConfigurationEncryptionKey,
}

impl EnvironmentConfigurationRegistry {
    /// Opens a local/compact SQLite authority and applies checksum-protected migrations.
    ///
    /// # Errors
    ///
    /// Rejects non-SQLite URLs, unsafe pool settings, or corrupt/unavailable storage.
    pub async fn connect_sqlite(
        url: &str,
        encryption_key: ConfigurationEncryptionKey,
    ) -> Result<Self, ConfigurationError> {
        if !url.starts_with("sqlite:") {
            return Err(ConfigurationError::Unavailable);
        }
        sqlx::any::install_default_drivers();
        let options =
            AnyConnectOptions::from_str(url).map_err(|_| ConfigurationError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    connection.execute("PRAGMA foreign_keys = ON").await?;
                    connection.execute("PRAGMA journal_mode = WAL").await?;
                    connection.execute("PRAGMA synchronous = FULL").await?;
                    connection.execute("PRAGMA busy_timeout = 5000").await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(map_sqlx)?;
        migrate(&pool).await?;
        Ok(Self {
            pool,
            encryption_key,
        })
    }

    /// Returns the complete bounded safe snapshot for one Environment.
    ///
    /// # Errors
    ///
    /// Fails closed on malformed persisted state or unavailable storage.
    pub async fn snapshot(
        &self,
        scope: EnvironmentScope,
    ) -> Result<ConfigurationSnapshot, ConfigurationError> {
        let revision = load_revision(&self.pool, scope).await?;
        let rows = sqlx::query("SELECT name,kind,variable_value,revision,created_at_micros,updated_at_micros FROM runku_environment_configuration_entries WHERE project_id=$1 AND environment_id=$2 ORDER BY name")
            .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
            .fetch_all(&self.pool).await.map_err(map_sqlx)?;
        if rows.len() > usize::try_from(MAX_ENTRIES).map_err(|_| ConfigurationError::Internal)? {
            return Err(ConfigurationError::Corruption);
        }
        let entries = rows
            .into_iter()
            .map(|row| decode_entry(scope, &row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ConfigurationSnapshot {
            configuration_revision: revision,
            entries,
        })
    }

    /// Returns immutable value-free audit events, newest first.
    ///
    /// `before_sequence` is exclusive. A returned continuation is stable because audit rows are
    /// append-only.
    ///
    /// # Errors
    ///
    /// Rejects limits outside `1..=100` and fails closed on malformed persisted history.
    pub async fn history(
        &self,
        scope: EnvironmentScope,
        before_sequence: Option<u64>,
        limit: u16,
    ) -> Result<ConfigurationHistory, ConfigurationError> {
        if !(1..=100).contains(&limit) {
            return Err(ConfigurationError::InvalidInput);
        }
        let fetch_limit = i64::from(limit) + 1;
        let rows = match before_sequence {
            Some(before) => sqlx::query("SELECT sequence,operation_id,actor,name,action,kind,configuration_revision,occurred_at_micros FROM runku_environment_configuration_audit WHERE project_id=$1 AND environment_id=$2 AND sequence<$3 ORDER BY sequence DESC LIMIT $4")
                .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(to_i64(before)?).bind(fetch_limit)
                .fetch_all(&self.pool).await.map_err(map_sqlx)?,
            None => sqlx::query("SELECT sequence,operation_id,actor,name,action,kind,configuration_revision,occurred_at_micros FROM runku_environment_configuration_audit WHERE project_id=$1 AND environment_id=$2 ORDER BY sequence DESC LIMIT $3")
                .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(fetch_limit)
                .fetch_all(&self.pool).await.map_err(map_sqlx)?,
        };
        let has_more = rows.len() > usize::from(limit);
        let mut entries = rows
            .into_iter()
            .take(usize::from(limit))
            .map(|row| decode_audit(&row))
            .collect::<Result<Vec<_>, _>>()?;
        let next_before_sequence = if has_more {
            entries.last().map(|entry| entry.sequence)
        } else {
            None
        };
        entries.shrink_to_fit();
        Ok(ConfigurationHistory {
            entries,
            next_before_sequence,
        })
    }

    /// Creates, updates, or rotates one exact entry under global CAS and idempotency.
    ///
    /// Secret plaintext is consumed only for encryption and is absent from the result.
    ///
    /// # Errors
    ///
    /// Rejects invalid values, revision conflicts, reused operation IDs, or storage failures.
    #[allow(clippy::too_many_arguments)]
    pub async fn set(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        actor: OperatorId,
        expected_revision: u64,
        name: ConfigurationName,
        kind: ConfigurationKind,
        mut value: Zeroizing<String>,
        occurred_at: TimestampMicros,
    ) -> Result<ConfigurationMutationResult, ConfigurationError> {
        validate_value(kind, &value, occurred_at)?;
        let digest = command_digest(
            "set",
            scope,
            expected_revision,
            &name,
            Some(kind),
            Some(&value),
        );
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        if let Some(result) = replay(&mut transaction, scope, operation_id, &digest).await? {
            transaction.commit().await.map_err(map_sqlx)?;
            value.zeroize();
            return Ok(result);
        }
        let current_revision = load_revision_tx(&mut transaction, scope).await?;
        if current_revision != expected_revision {
            return rollback(transaction, ConfigurationError::Conflict).await;
        }
        let count = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runku_environment_configuration_entries WHERE project_id=$1 AND environment_id=$2")
            .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string())
            .fetch_one(&mut *transaction).await.map_err(map_sqlx)?;
        let exists = load_entry_tx(&mut transaction, scope, &name)
            .await?
            .is_some();
        if !exists && count >= MAX_ENTRIES {
            return rollback(transaction, ConfigurationError::LimitExceeded).await;
        }
        let revision = current_revision
            .checked_add(1)
            .ok_or(ConfigurationError::LimitExceeded)?;
        let existing_created = load_entry_tx(&mut transaction, scope, &name)
            .await?
            .map_or(occurred_at, |entry| entry.created_at);
        let (variable_value, nonce, ciphertext) = match kind {
            ConfigurationKind::Variable => (Some(value.as_str()), None, None),
            ConfigurationKind::Secret => {
                let sealed = seal(
                    &self.encryption_key,
                    scope,
                    &name,
                    revision,
                    value.as_bytes(),
                )?;
                (None, Some(sealed.0), Some(sealed.1))
            }
        };
        sqlx::query("INSERT INTO runku_environment_configuration_entries(project_id,environment_id,name,kind,variable_value,secret_nonce,secret_ciphertext,revision,created_at_micros,updated_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(project_id,environment_id,name) DO UPDATE SET kind=excluded.kind,variable_value=excluded.variable_value,secret_nonce=excluded.secret_nonce,secret_ciphertext=excluded.secret_ciphertext,revision=excluded.revision,updated_at_micros=excluded.updated_at_micros")
            .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(name.as_str()).bind(kind.as_str())
            .bind(variable_value).bind(nonce).bind(ciphertext).bind(to_i64(revision)?).bind(existing_created.get()).bind(occurred_at.get())
            .execute(&mut *transaction).await.map_err(map_sqlx)?;
        persist_revision(&mut transaction, scope, revision).await?;
        let entry = load_entry_tx(&mut transaction, scope, &name)
            .await?
            .ok_or(ConfigurationError::Corruption)?;
        persist_operation_and_audit(
            &mut transaction,
            scope,
            operation_id,
            actor,
            &name,
            "set",
            kind,
            revision,
            occurred_at,
            &digest,
            Some(&entry),
        )
        .await?;
        transaction.commit().await.map_err(map_sqlx)?;
        value.zeroize();
        Ok(ConfigurationMutationResult {
            configuration_revision: revision,
            entry: Some(entry),
            replayed: false,
        })
    }

    /// Deletes one exact entry under global CAS and idempotency.
    ///
    /// # Errors
    ///
    /// Rejects missing entries, revision conflicts, reused operation IDs, or storage failures.
    pub async fn delete(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        actor: OperatorId,
        expected_revision: u64,
        name: ConfigurationName,
        occurred_at: TimestampMicros,
    ) -> Result<ConfigurationMutationResult, ConfigurationError> {
        if occurred_at.get() < 0 {
            return Err(ConfigurationError::InvalidInput);
        }
        let digest = command_digest("delete", scope, expected_revision, &name, None, None);
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        if let Some(result) = replay(&mut transaction, scope, operation_id, &digest).await? {
            transaction.commit().await.map_err(map_sqlx)?;
            return Ok(result);
        }
        let current_revision = load_revision_tx(&mut transaction, scope).await?;
        if current_revision != expected_revision {
            return rollback(transaction, ConfigurationError::Conflict).await;
        }
        let current = load_entry_tx(&mut transaction, scope, &name)
            .await?
            .ok_or(ConfigurationError::NotFound)?;
        let revision = current_revision
            .checked_add(1)
            .ok_or(ConfigurationError::LimitExceeded)?;
        let result = sqlx::query("DELETE FROM runku_environment_configuration_entries WHERE project_id=$1 AND environment_id=$2 AND name=$3")
            .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(name.as_str())
            .execute(&mut *transaction).await.map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return rollback(transaction, ConfigurationError::Conflict).await;
        }
        persist_revision(&mut transaction, scope, revision).await?;
        persist_operation_and_audit(
            &mut transaction,
            scope,
            operation_id,
            actor,
            &name,
            "delete",
            current.kind,
            revision,
            occurred_at,
            &digest,
            None,
        )
        .await?;
        transaction.commit().await.map_err(map_sqlx)?;
        Ok(ConfigurationMutationResult {
            configuration_revision: revision,
            entry: None,
            replayed: false,
        })
    }

    /// Resolves one value for the trusted Function broker.
    ///
    /// The returned secret zeroizes on drop. This method is intentionally separate from the safe
    /// administrative projection and must be called only after manifest capability authorization.
    ///
    /// # Errors
    ///
    /// Rejects absent entries and fails closed on malformed ciphertext.
    pub async fn resolve(
        &self,
        scope: EnvironmentScope,
        name: &ConfigurationName,
    ) -> Result<Zeroizing<String>, ConfigurationError> {
        self.resolve_kind(scope, name, None).await
    }

    /// Resolves one value only when its persisted kind matches the manifest-authorized kind.
    ///
    /// # Errors
    ///
    /// Returns not-found for a missing name or kind mismatch and fails closed on malformed
    /// ciphertext.
    pub async fn resolve_exact(
        &self,
        scope: EnvironmentScope,
        name: &ConfigurationName,
        expected_kind: ConfigurationKind,
    ) -> Result<Zeroizing<String>, ConfigurationError> {
        self.resolve_kind(scope, name, Some(expected_kind)).await
    }

    async fn resolve_kind(
        &self,
        scope: EnvironmentScope,
        name: &ConfigurationName,
        expected_kind: Option<ConfigurationKind>,
    ) -> Result<Zeroizing<String>, ConfigurationError> {
        let row = sqlx::query("SELECT kind,variable_value,secret_nonce,secret_ciphertext,revision FROM runku_environment_configuration_entries WHERE project_id=$1 AND environment_id=$2 AND name=$3")
            .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(name.as_str())
            .fetch_optional(&self.pool).await.map_err(map_sqlx)?.ok_or(ConfigurationError::NotFound)?;
        let kind = row.try_get::<String, _>("kind").map_err(corrupt)?.parse()?;
        if expected_kind.is_some_and(|expected| expected != kind) {
            return Err(ConfigurationError::NotFound);
        }
        match kind {
            ConfigurationKind::Variable => {
                let value = row
                    .try_get::<String, _>("variable_value")
                    .map_err(corrupt)?;
                validate_value(kind, &value, TimestampMicros::new(0))?;
                Ok(Zeroizing::new(value))
            }
            ConfigurationKind::Secret => {
                let nonce = row.try_get::<Vec<u8>, _>("secret_nonce").map_err(corrupt)?;
                let ciphertext = row
                    .try_get::<Vec<u8>, _>("secret_ciphertext")
                    .map_err(corrupt)?;
                let revision = from_i64(row.try_get("revision").map_err(corrupt)?)?;
                open(
                    &self.encryption_key,
                    scope,
                    name,
                    revision,
                    &nonce,
                    &ciphertext,
                )
            }
        }
    }

    /// Closes the SQLite pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

fn validate_value(
    kind: ConfigurationKind,
    value: &str,
    occurred_at: TimestampMicros,
) -> Result<(), ConfigurationError> {
    let maximum = match kind {
        ConfigurationKind::Variable => MAX_VARIABLE_BYTES,
        ConfigurationKind::Secret => MAX_SECRET_BYTES,
    };
    if value.is_empty() || value.len() > maximum || value.contains('\0') || occurred_at.get() < 0 {
        Err(ConfigurationError::InvalidInput)
    } else {
        Ok(())
    }
}

fn command_digest(
    action: &str,
    scope: EnvironmentScope,
    expected: u64,
    name: &ConfigurationName,
    kind: Option<ConfigurationKind>,
    value: Option<&str>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    for field in [
        action,
        &scope.project_id().to_string(),
        &scope.environment_id().to_string(),
        name.as_str(),
    ] {
        digest.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(field.as_bytes());
    }
    digest.update(expected.to_be_bytes());
    digest.update(kind.map_or("", ConfigurationKind::as_str).as_bytes());
    if let Some(value) = value {
        digest.update(Sha256::digest(value.as_bytes()));
    }
    digest.finalize().into()
}

fn aad(scope: EnvironmentScope, name: &ConfigurationName, revision: u64) -> Vec<u8> {
    format!(
        "runku-environment-configuration-v1\0{}\0{}\0{}\0{revision}",
        scope.project_id(),
        scope.environment_id(),
        name.as_str()
    )
    .into_bytes()
}

fn seal(
    key: &ConfigurationEncryptionKey,
    scope: EnvironmentScope,
    name: &ConfigurationName,
    revision: u64,
    plaintext: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), ConfigurationError> {
    let mut nonce = [0_u8; 12];
    getrandom::fill(&mut nonce).map_err(|_| ConfigurationError::Internal)?;
    let cipher = Aes256Gcm::new_from_slice(key.0.as_ref().as_ref())
        .map_err(|_| ConfigurationError::Internal)?;
    let result = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad(scope, name, revision),
            },
        )
        .map_err(|_| ConfigurationError::Internal)?;
    Ok((nonce.to_vec(), result))
}

fn open(
    key: &ConfigurationEncryptionKey,
    scope: EnvironmentScope,
    name: &ConfigurationName,
    revision: u64,
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Zeroizing<String>, ConfigurationError> {
    if nonce.len() != 12 || ciphertext.len() < 16 || ciphertext.len() > MAX_SECRET_BYTES + 16 {
        return Err(ConfigurationError::Corruption);
    }
    let cipher = Aes256Gcm::new_from_slice(key.0.as_ref().as_ref())
        .map_err(|_| ConfigurationError::Internal)?;
    let mut plaintext = cipher
        .decrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: &aad(scope, name, revision),
            },
        )
        .map_err(|_| ConfigurationError::Corruption)?;
    let value = String::from_utf8(std::mem::take(&mut plaintext)).map_err(|error| {
        let mut bytes = error.into_bytes();
        bytes.zeroize();
        ConfigurationError::Corruption
    })?;
    validate_value(ConfigurationKind::Secret, &value, TimestampMicros::new(0))
        .map_err(|_| ConfigurationError::Corruption)?;
    Ok(Zeroizing::new(value))
}

async fn load_revision(pool: &AnyPool, scope: EnvironmentScope) -> Result<u64, ConfigurationError> {
    let value = sqlx::query_scalar::<_, i64>("SELECT revision FROM runku_environment_configuration_state WHERE project_id=$1 AND environment_id=$2")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).fetch_optional(pool).await.map_err(map_sqlx)?;
    value.map_or(Ok(0), from_i64)
}

async fn load_revision_tx(
    transaction: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
) -> Result<u64, ConfigurationError> {
    let value = sqlx::query_scalar::<_, i64>("SELECT revision FROM runku_environment_configuration_state WHERE project_id=$1 AND environment_id=$2")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).fetch_optional(&mut **transaction).await.map_err(map_sqlx)?;
    value.map_or(Ok(0), from_i64)
}

async fn persist_revision(
    transaction: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    revision: u64,
) -> Result<(), ConfigurationError> {
    sqlx::query("INSERT INTO runku_environment_configuration_state(project_id,environment_id,revision) VALUES($1,$2,$3) ON CONFLICT(project_id,environment_id) DO UPDATE SET revision=excluded.revision")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(to_i64(revision)?)
        .execute(&mut **transaction).await.map_err(map_sqlx)?;
    Ok(())
}

async fn load_entry_tx(
    transaction: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    name: &ConfigurationName,
) -> Result<Option<ConfigurationEntry>, ConfigurationError> {
    sqlx::query("SELECT name,kind,variable_value,revision,created_at_micros,updated_at_micros FROM runku_environment_configuration_entries WHERE project_id=$1 AND environment_id=$2 AND name=$3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(name.as_str())
        .fetch_optional(&mut **transaction).await.map_err(map_sqlx)?.map(|row| decode_entry(scope, &row)).transpose()
}

fn decode_entry(
    scope: EnvironmentScope,
    row: &sqlx::any::AnyRow,
) -> Result<ConfigurationEntry, ConfigurationError> {
    let name = row.try_get::<String, _>("name").map_err(corrupt)?.parse()?;
    let kind: ConfigurationKind = row.try_get::<String, _>("kind").map_err(corrupt)?.parse()?;
    let variable_value = row
        .try_get::<Option<String>, _>("variable_value")
        .map_err(corrupt)?;
    if (kind == ConfigurationKind::Variable) != variable_value.is_some() {
        return Err(ConfigurationError::Corruption);
    }
    if let Some(value) = variable_value.as_deref() {
        validate_value(kind, value, TimestampMicros::new(0))
            .map_err(|_| ConfigurationError::Corruption)?;
    }
    let revision = from_i64(row.try_get("revision").map_err(corrupt)?)?;
    let created_at = TimestampMicros::new(row.try_get("created_at_micros").map_err(corrupt)?);
    let updated_at = TimestampMicros::new(row.try_get("updated_at_micros").map_err(corrupt)?);
    if created_at.get() < 0 || updated_at < created_at {
        return Err(ConfigurationError::Corruption);
    }
    Ok(ConfigurationEntry {
        scope,
        name,
        kind,
        variable_value,
        revision,
        created_at,
        updated_at,
    })
}

fn decode_audit(row: &sqlx::any::AnyRow) -> Result<ConfigurationAuditEntry, ConfigurationError> {
    let action = row.try_get::<String, _>("action").map_err(corrupt)?;
    if !matches!(action.as_str(), "set" | "delete") {
        return Err(ConfigurationError::Corruption);
    }
    let occurred_at = TimestampMicros::new(row.try_get("occurred_at_micros").map_err(corrupt)?);
    if occurred_at.get() < 0 {
        return Err(ConfigurationError::Corruption);
    }
    Ok(ConfigurationAuditEntry {
        sequence: from_i64(row.try_get("sequence").map_err(corrupt)?)?,
        operation_id: row
            .try_get::<String, _>("operation_id")
            .map_err(corrupt)?
            .parse()
            .map_err(|_| ConfigurationError::Corruption)?,
        actor: row
            .try_get::<String, _>("actor")
            .map_err(corrupt)?
            .parse()
            .map_err(|_| ConfigurationError::Corruption)?,
        name: row.try_get::<String, _>("name").map_err(corrupt)?.parse()?,
        action,
        kind: row.try_get::<String, _>("kind").map_err(corrupt)?.parse()?,
        configuration_revision: from_i64(row.try_get("configuration_revision").map_err(corrupt)?)?,
        occurred_at,
    })
}

async fn replay(
    transaction: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    operation_id: OperationId,
    digest: &[u8; 32],
) -> Result<Option<ConfigurationMutationResult>, ConfigurationError> {
    let row = sqlx::query("SELECT command_digest,name,action,configuration_revision,result_kind,result_variable_value,result_entry_revision,result_created_at_micros,result_updated_at_micros FROM runku_environment_configuration_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(operation_id.to_string())
        .fetch_optional(&mut **transaction).await.map_err(map_sqlx)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let stored = row
        .try_get::<Vec<u8>, _>("command_digest")
        .map_err(corrupt)?;
    if stored.as_slice() != digest {
        return Err(ConfigurationError::OperationIdReused);
    }
    let revision = from_i64(row.try_get("configuration_revision").map_err(corrupt)?)?;
    let action = row.try_get::<String, _>("action").map_err(corrupt)?;
    let name = row.try_get::<String, _>("name").map_err(corrupt)?.parse()?;
    let entry = if action == "set" {
        let kind = row
            .try_get::<Option<String>, _>("result_kind")
            .map_err(corrupt)?
            .ok_or(ConfigurationError::Corruption)?
            .parse()?;
        let variable_value = row
            .try_get::<Option<String>, _>("result_variable_value")
            .map_err(corrupt)?;
        if (kind == ConfigurationKind::Variable) != variable_value.is_some() {
            return Err(ConfigurationError::Corruption);
        }
        let entry = ConfigurationEntry {
            scope,
            name,
            kind,
            variable_value,
            revision: from_i64(
                row.try_get::<Option<i64>, _>("result_entry_revision")
                    .map_err(corrupt)?
                    .ok_or(ConfigurationError::Corruption)?,
            )?,
            created_at: TimestampMicros::new(
                row.try_get::<Option<i64>, _>("result_created_at_micros")
                    .map_err(corrupt)?
                    .ok_or(ConfigurationError::Corruption)?,
            ),
            updated_at: TimestampMicros::new(
                row.try_get::<Option<i64>, _>("result_updated_at_micros")
                    .map_err(corrupt)?
                    .ok_or(ConfigurationError::Corruption)?,
            ),
        };
        if entry.created_at.get() < 0
            || entry.updated_at < entry.created_at
            || entry.revision != revision
            || entry.variable_value.as_deref().is_some_and(|value| {
                validate_value(entry.kind, value, TimestampMicros::new(0)).is_err()
            })
        {
            return Err(ConfigurationError::Corruption);
        }
        Some(entry)
    } else if action == "delete" {
        None
    } else {
        return Err(ConfigurationError::Corruption);
    };
    Ok(Some(ConfigurationMutationResult {
        configuration_revision: revision,
        entry,
        replayed: true,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn persist_operation_and_audit(
    transaction: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    operation_id: OperationId,
    actor: OperatorId,
    name: &ConfigurationName,
    action: &str,
    kind: ConfigurationKind,
    revision: u64,
    occurred_at: TimestampMicros,
    digest: &[u8; 32],
    result_entry: Option<&ConfigurationEntry>,
) -> Result<(), ConfigurationError> {
    sqlx::query("INSERT INTO runku_environment_configuration_operations(project_id,environment_id,operation_id,command_digest,name,action,configuration_revision,result_kind,result_variable_value,result_entry_revision,result_created_at_micros,result_updated_at_micros,completed_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(operation_id.to_string()).bind(digest.as_slice()).bind(name.as_str()).bind(action).bind(to_i64(revision)?)
        .bind(result_entry.map(|entry| entry.kind.as_str())).bind(result_entry.and_then(|entry| entry.variable_value.as_deref()))
        .bind(result_entry.map(|entry| to_i64(entry.revision)).transpose()?).bind(result_entry.map(|entry| entry.created_at.get())).bind(result_entry.map(|entry| entry.updated_at.get())).bind(occurred_at.get())
        .execute(&mut **transaction).await.map_err(map_sqlx)?;
    let sequence = sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(sequence),0)+1 FROM runku_environment_configuration_audit WHERE project_id=$1 AND environment_id=$2")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).fetch_one(&mut **transaction).await.map_err(map_sqlx)?;
    sqlx::query("INSERT INTO runku_environment_configuration_audit(project_id,environment_id,sequence,operation_id,actor,name,action,kind,configuration_revision,occurred_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(sequence).bind(operation_id.to_string()).bind(actor.to_string()).bind(name.as_str()).bind(action).bind(kind.as_str()).bind(to_i64(revision)?).bind(occurred_at.get())
        .execute(&mut **transaction).await.map_err(map_sqlx)?;
    Ok(())
}

async fn rollback<T>(
    transaction: Transaction<'_, Any>,
    error: ConfigurationError,
) -> Result<T, ConfigurationError> {
    transaction.rollback().await.map_err(map_sqlx)?;
    Err(error)
}

async fn migrate(pool: &AnyPool) -> Result<(), ConfigurationError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_environment_configuration_schema_migrations(version BIGINT PRIMARY KEY,checksum TEXT NOT NULL)").execute(pool).await.map_err(map_sqlx)?;
    let rows = sqlx::query("SELECT version,checksum FROM runku_environment_configuration_schema_migrations ORDER BY version").fetch_all(pool).await.map_err(map_sqlx)?;
    for row in &rows {
        let version = row.try_get::<i64, _>("version").map_err(corrupt)?;
        let checksum = row.try_get::<String, _>("checksum").map_err(corrupt)?;
        if version != 1 || checksum != migration_checksum(1, MIGRATION_1) {
            return Err(ConfigurationError::Corruption);
        }
    }
    if rows.is_empty() {
        let mut transaction = pool.begin().await.map_err(map_sqlx)?;
        for statement in MIGRATION_1 {
            sqlx::query(*statement)
                .execute(&mut *transaction)
                .await
                .map_err(map_sqlx)?;
        }
        sqlx::query("INSERT INTO runku_environment_configuration_schema_migrations(version,checksum) VALUES(1,$1)")
            .bind(migration_checksum(1, MIGRATION_1)).execute(&mut *transaction).await.map_err(map_sqlx)?;
        transaction.commit().await.map_err(map_sqlx)?;
    }
    Ok(())
}

fn migration_checksum(version: i64, statements: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(version.to_be_bytes());
    for statement in statements {
        digest.update(statement.as_bytes());
        digest.update([0]);
    }
    URL_SAFE_NO_PAD.encode(digest.finalize())
}

fn to_i64(value: u64) -> Result<i64, ConfigurationError> {
    i64::try_from(value).map_err(|_| ConfigurationError::LimitExceeded)
}
fn from_i64(value: i64) -> Result<u64, ConfigurationError> {
    u64::try_from(value).map_err(|_| ConfigurationError::Corruption)
}
fn corrupt<T>(_error: T) -> ConfigurationError {
    ConfigurationError::Corruption
}
#[allow(clippy::needless_pass_by_value)]
fn map_sqlx(error: sqlx::Error) -> ConfigurationError {
    if matches!(error, sqlx::Error::Database(ref value) if value.message().contains("locked") || value.message().contains("busy"))
    {
        ConfigurationError::Busy
    } else {
        ConfigurationError::Unavailable
    }
}
