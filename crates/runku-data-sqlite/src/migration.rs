//! Versioned `SQLite` schema migrations.

use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use runku_data::StoreError;

use crate::adapter::{map_sqlx_error, now_micros};

const V1_STATEMENTS: &[&str] = &[
    "CREATE TABLE runku_environment_sequences (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, \
        commit_sequence INTEGER NOT NULL CHECK (commit_sequence >= 0), \
        PRIMARY KEY (project_id, environment_id)) STRICT",
    "CREATE TABLE runku_commit_operations (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, \
        batch_digest BLOB NOT NULL CHECK (length(batch_digest) = 32), \
        commit_sequence INTEGER NOT NULL CHECK (commit_sequence > 0), created_at_micros INTEGER NOT NULL, \
        PRIMARY KEY (project_id, environment_id, operation_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE) STRICT",
    "CREATE TABLE runku_commit_document_results (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK (ordinal >= 0), \
        table_id TEXT NOT NULL, document_id TEXT NOT NULL, revision INTEGER NULL CHECK (revision IS NULL OR revision > 0), \
        PRIMARY KEY (project_id, environment_id, operation_id, ordinal), \
        FOREIGN KEY (project_id, environment_id, operation_id) REFERENCES runku_commit_operations(project_id, environment_id, operation_id) ON DELETE CASCADE) STRICT",
    "CREATE TABLE runku_documents (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, table_id TEXT NOT NULL, document_id TEXT NOT NULL, \
        revision INTEGER NOT NULL CHECK (revision > 0), commit_sequence INTEGER NOT NULL CHECK (commit_sequence > 0), \
        created_at_micros INTEGER NOT NULL, updated_at_micros INTEGER NOT NULL, value_bytes BLOB NOT NULL, \
        PRIMARY KEY (project_id, environment_id, table_id, document_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE) STRICT",
    "CREATE TABLE runku_index_entries (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, index_id TEXT NOT NULL, key_bytes BLOB NOT NULL, \
        table_id TEXT NOT NULL, document_id TEXT NOT NULL, document_revision INTEGER NOT NULL CHECK (document_revision > 0), \
        commit_sequence INTEGER NOT NULL CHECK (commit_sequence > 0), \
        PRIMARY KEY (project_id, environment_id, index_id, key_bytes, document_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE) STRICT",
    "CREATE INDEX runku_index_scan ON runku_index_entries(project_id, environment_id, index_id, key_bytes, document_id)",
    "CREATE TABLE runku_outbox (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, event_id TEXT NOT NULL, \
        commit_sequence INTEGER NOT NULL CHECK (commit_sequence > 0), payload_bytes BLOB NOT NULL, created_at_micros INTEGER NOT NULL, \
        PRIMARY KEY (project_id, environment_id, event_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE) STRICT",
    "CREATE INDEX runku_outbox_sequence ON runku_outbox(project_id, environment_id, commit_sequence, event_id)",
    "CREATE TABLE runku_scheduled_invocations (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, scheduled_id TEXT NOT NULL, pinned_code TEXT NOT NULL, function_name TEXT NOT NULL, \
        args_bytes BLOB NOT NULL, execute_at_micros INTEGER NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','failed','cancelled')), \
        attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0), lease_generation INTEGER NOT NULL DEFAULT 0 CHECK (lease_generation >= 0), \
        lease_owner TEXT NULL, lease_until_micros INTEGER NULL, idempotency_key TEXT NULL, last_error_code TEXT NULL, \
        commit_sequence INTEGER NOT NULL CHECK (commit_sequence > 0), created_at_micros INTEGER NOT NULL, updated_at_micros INTEGER NOT NULL, \
        PRIMARY KEY (project_id, environment_id, scheduled_id), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE) STRICT",
    "CREATE UNIQUE INDEX runku_schedule_idempotency ON runku_scheduled_invocations(project_id, environment_id, idempotency_key) WHERE idempotency_key IS NOT NULL",
    "CREATE INDEX runku_schedule_due ON runku_scheduled_invocations(project_id, environment_id, status, execute_at_micros, lease_until_micros, scheduled_id)",
];

const V2_STATEMENTS: &[&str] = &["CREATE TABLE runku_outbox_consumers (\
        project_id TEXT NOT NULL, environment_id TEXT NOT NULL, consumer_name TEXT NOT NULL, \
        cursor_sequence INTEGER NOT NULL DEFAULT 0 CHECK (cursor_sequence >= 0), cursor_event_id TEXT NULL, \
        lease_owner TEXT NULL, lease_until_micros INTEGER NULL, lease_generation INTEGER NOT NULL DEFAULT 0 CHECK (lease_generation >= 0), \
        claimed_sequence INTEGER NULL CHECK (claimed_sequence IS NULL OR claimed_sequence > 0), claimed_event_id TEXT NULL, updated_at_micros INTEGER NOT NULL, \
        PRIMARY KEY (project_id, environment_id, consumer_name), \
        CHECK ((cursor_sequence = 0 AND cursor_event_id IS NULL) OR (cursor_sequence > 0 AND cursor_event_id IS NOT NULL)), \
        CHECK ((lease_owner IS NULL AND lease_until_micros IS NULL) OR (lease_owner IS NOT NULL AND lease_until_micros IS NOT NULL)), \
        CHECK ((claimed_sequence IS NULL AND claimed_event_id IS NULL) OR (claimed_sequence IS NOT NULL AND claimed_event_id IS NOT NULL)), \
        FOREIGN KEY (project_id, environment_id) REFERENCES runku_environment_sequences(project_id, environment_id) ON DELETE CASCADE) STRICT"];

pub(crate) async fn migrate(pool: &SqlitePool) -> Result<(), StoreError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS runku_schema_migrations (\
            version INTEGER PRIMARY KEY, checksum BLOB NOT NULL CHECK (length(checksum) = 32), applied_at_micros INTEGER NOT NULL) STRICT",
    )
    .execute(pool)
    .await
    .map_err(|_| StoreError::MigrationFailed)?;

    let mut transaction = pool
        .begin_with("BEGIN EXCLUSIVE")
        .await
        .map_err(|_| StoreError::MigrationFailed)?;
    apply_migration(&mut transaction, 1, V1_STATEMENTS, v1_checksum()).await?;
    apply_migration(&mut transaction, 2, V2_STATEMENTS, v2_checksum()).await?;
    transaction
        .commit()
        .await
        .map_err(|_| StoreError::MigrationFailed)
}

async fn apply_migration(
    transaction: &mut Transaction<'static, Sqlite>,
    version: i64,
    statements: &'static [&'static str],
    checksum: [u8; 32],
) -> Result<(), StoreError> {
    let existing = sqlx::query("SELECT checksum FROM runku_schema_migrations WHERE version = ?")
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
        "INSERT INTO runku_schema_migrations(version, checksum, applied_at_micros) VALUES (?, ?, ?)",
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
    checksum(b"RUNKU_SQLITE_SCHEMA_V1", V1_STATEMENTS)
}

fn v2_checksum() -> [u8; 32] {
    checksum(b"RUNKU_SQLITE_SCHEMA_V2", V2_STATEMENTS)
}

pub(crate) async fn begin_immediate(
    pool: &SqlitePool,
) -> Result<Transaction<'static, Sqlite>, StoreError> {
    pool.begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(map_sqlx_error)
}

#[cfg(test)]
mod tests {
    use sqlx::{Row, sqlite::SqlitePoolOptions};

    use super::*;

    #[tokio::test]
    async fn v1_database_upgrades_additively_to_v2() -> Result<(), StoreError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .map_err(|_| StoreError::Internal)?;
        sqlx::query(
            "CREATE TABLE runku_schema_migrations (version INTEGER PRIMARY KEY, checksum BLOB NOT NULL CHECK (length(checksum) = 32), applied_at_micros INTEGER NOT NULL) STRICT",
        )
        .execute(&pool)
        .await
        .map_err(|_| StoreError::Internal)?;
        let mut transaction = pool
            .begin_with("BEGIN EXCLUSIVE")
            .await
            .map_err(|_| StoreError::Internal)?;
        apply_migration(&mut transaction, 1, V1_STATEMENTS, v1_checksum()).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Internal)?;

        migrate(&pool).await?;
        let versions = sqlx::query("SELECT version FROM runku_schema_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .map_err(|_| StoreError::Internal)?;
        assert_eq!(
            versions
                .iter()
                .map(|row| row.get::<i64, _>("version"))
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let table: String = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'runku_outbox_consumers'",
        )
        .fetch_one(&pool)
        .await
        .map_err(|_| StoreError::Internal)?;
        assert_eq!(table, "runku_outbox_consumers");
        pool.close().await;
        Ok(())
    }
}
