//! Versioned `PostgreSQL` schema migrations.

use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};

use runku_data::StoreError;

use crate::adapter::{map_sqlx_error, now_micros};

const MIGRATION_LOCK_ID: i64 = 7_224_856_021;

const V1_STATEMENTS: &[&str] = &[
    "CREATE TABLE runku_environment_sequences (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, \
        commit_sequence BIGINT NOT NULL CHECK (commit_sequence >= 0), \
        PRIMARY KEY (project_id, environment_id))",
    "CREATE TABLE runku_commit_operations (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, \
        batch_digest BYTEA NOT NULL CHECK (octet_length(batch_digest) = 32), \
        commit_sequence BIGINT NOT NULL CHECK (commit_sequence > 0), created_at_micros BIGINT NOT NULL, \
        PRIMARY KEY (project_id, environment_id, operation_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE TABLE runku_commit_document_results (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK (ordinal >= 0), \
        table_id TEXT NOT NULL, document_id TEXT NOT NULL, revision BIGINT NULL CHECK (revision IS NULL OR revision > 0), \
        PRIMARY KEY (project_id, environment_id, operation_id, ordinal), \
        FOREIGN KEY (project_id, environment_id, operation_id) REFERENCES runku_commit_operations(project_id, environment_id, operation_id) ON DELETE CASCADE)",
    "CREATE TABLE runku_documents (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, table_id TEXT NOT NULL, document_id TEXT NOT NULL, \
        revision BIGINT NOT NULL CHECK (revision > 0), commit_sequence BIGINT NOT NULL CHECK (commit_sequence > 0), \
        created_at_micros BIGINT NOT NULL, updated_at_micros BIGINT NOT NULL, value_bytes BYTEA NOT NULL, \
        PRIMARY KEY (project_id, environment_id, table_id, document_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE TABLE runku_index_entries (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, index_id TEXT NOT NULL, key_bytes BYTEA NOT NULL, \
        table_id TEXT NOT NULL, document_id TEXT NOT NULL, document_revision BIGINT NOT NULL CHECK (document_revision > 0), \
        commit_sequence BIGINT NOT NULL CHECK (commit_sequence > 0), \
        PRIMARY KEY (project_id, environment_id, index_id, key_bytes, document_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE INDEX runku_index_scan ON runku_index_entries(project_id, environment_id, index_id, key_bytes, document_id)",
    "CREATE TABLE runku_outbox (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, event_id TEXT NOT NULL, \
        commit_sequence BIGINT NOT NULL CHECK (commit_sequence > 0), payload_bytes BYTEA NOT NULL, created_at_micros BIGINT NOT NULL, \
        PRIMARY KEY (project_id, environment_id, event_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE INDEX runku_outbox_sequence ON runku_outbox(project_id, environment_id, commit_sequence, event_id)",
    "CREATE TABLE runku_scheduled_invocations (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, scheduled_id TEXT NOT NULL, pinned_code TEXT NOT NULL, function_name TEXT NOT NULL, \
        args_bytes BYTEA NOT NULL, execute_at_micros BIGINT NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','failed','cancelled')), \
        attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0), lease_generation BIGINT NOT NULL DEFAULT 0 CHECK (lease_generation >= 0), \
        lease_owner TEXT NULL, lease_until_micros BIGINT NULL, idempotency_key TEXT NULL, last_error_code TEXT NULL, \
        commit_sequence BIGINT NOT NULL CHECK (commit_sequence > 0), created_at_micros BIGINT NOT NULL, updated_at_micros BIGINT NOT NULL, \
        PRIMARY KEY (project_id, environment_id, scheduled_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE)",
    "CREATE UNIQUE INDEX runku_schedule_idempotency ON runku_scheduled_invocations(project_id, environment_id, idempotency_key) WHERE idempotency_key IS NOT NULL",
    "CREATE INDEX runku_schedule_due ON runku_scheduled_invocations(project_id, environment_id, status, execute_at_micros, lease_until_micros, scheduled_id)",
];

const V2_STATEMENTS: &[&str] = &["CREATE TABLE runku_outbox_consumers (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, consumer_name TEXT NOT NULL, \
        cursor_sequence BIGINT NOT NULL DEFAULT 0 CHECK (cursor_sequence >= 0), cursor_event_id TEXT NULL, \
        lease_owner TEXT NULL, lease_until_micros BIGINT NULL, lease_generation BIGINT NOT NULL DEFAULT 0 CHECK (lease_generation >= 0), \
        claimed_sequence BIGINT NULL CHECK (claimed_sequence IS NULL OR claimed_sequence > 0), claimed_event_id TEXT NULL, updated_at_micros BIGINT NOT NULL, \
        PRIMARY KEY (project_id, environment_id, consumer_name), \
        CHECK ((cursor_sequence = 0 AND cursor_event_id IS NULL) OR (cursor_sequence > 0 AND cursor_event_id IS NOT NULL)), \
        CHECK ((lease_owner IS NULL AND lease_until_micros IS NULL) OR (lease_owner IS NOT NULL AND lease_until_micros IS NOT NULL)), \
        CHECK ((claimed_sequence IS NULL AND claimed_event_id IS NULL) OR (claimed_sequence IS NOT NULL AND claimed_event_id IS NOT NULL)), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE)"];

const V3_STATEMENTS: &[&str] = &["CREATE TABLE runku_environment_binding (\
        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton), \
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, bound_at_micros BIGINT NOT NULL)"];

pub(crate) async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS runku_schema_migrations (\
            version BIGINT PRIMARY KEY, checksum BYTEA NOT NULL CHECK (octet_length(checksum) = 32), applied_at_micros BIGINT NOT NULL)",
    )
    .execute(pool)
    .await
    .map_err(|_| StoreError::MigrationFailed)?;
    let mut transaction = pool
        .begin()
        .await
        .map_err(|_| StoreError::MigrationFailed)?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::MigrationFailed)?;
    apply_migration(&mut transaction, 1, V1_STATEMENTS, v1_checksum()).await?;
    apply_migration(&mut transaction, 2, V2_STATEMENTS, v2_checksum()).await?;
    apply_migration(&mut transaction, 3, V3_STATEMENTS, v3_checksum()).await?;
    transaction
        .commit()
        .await
        .map_err(|_| StoreError::MigrationFailed)
}

async fn apply_migration(
    transaction: &mut Transaction<'static, Postgres>,
    version: i64,
    statements: &'static [&'static str],
    checksum: [u8; 32],
) -> Result<(), StoreError> {
    let existing = sqlx::query("SELECT checksum FROM runku_schema_migrations WHERE version = $1")
        .bind(version)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| StoreError::MigrationFailed)?;
    if let Some(row) = existing {
        let stored: Vec<u8> = row
            .try_get("checksum")
            .map_err(|_| StoreError::MigrationFailed)?;
        return if stored == checksum {
            Ok(())
        } else {
            Err(StoreError::MigrationFailed)
        };
    }
    for statement in statements {
        sqlx::query(*statement)
            .execute(&mut **transaction)
            .await
            .map_err(|_| StoreError::MigrationFailed)?;
    }
    sqlx::query(
        "INSERT INTO runku_schema_migrations(version, checksum, applied_at_micros) VALUES ($1, $2, $3)",
    )
    .bind(version)
    .bind(checksum.as_slice())
    .bind(now_micros()?)
    .execute(&mut **transaction)
    .await
    .map_err(|_| StoreError::MigrationFailed)?;
    Ok(())
}

fn checksum(magic: &[u8], statements: &[&str]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(magic);
    for statement in statements {
        digest.update(statement.as_bytes());
        digest.update([0]);
    }
    digest.finalize().into()
}

fn v1_checksum() -> [u8; 32] {
    checksum(b"RUNKU_POSTGRES_SCHEMA_V1", V1_STATEMENTS)
}

fn v2_checksum() -> [u8; 32] {
    checksum(b"RUNKU_POSTGRES_SCHEMA_V2", V2_STATEMENTS)
}

fn v3_checksum() -> [u8; 32] {
    checksum(b"RUNKU_POSTGRES_SCHEMA_V3", V3_STATEMENTS)
}

pub(crate) async fn begin_serializable(
    pool: &PgPool,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    pool.begin_with("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .map_err(map_sqlx_error)
}
