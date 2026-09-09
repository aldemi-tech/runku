//! `SQLite` conformance, recovery, migration, and production-role rejection.

use std::{collections::BTreeMap, error::Error};

use runku_core::{DocumentId, EnvironmentId, OperationId, ProjectId, TableId};
use runku_data::{
    CommitBatch, DocumentMutation, EnvironmentScope, ExpectedRevision, IndexRange, LogicalStore,
    StoreBackend, StoreError,
};
use runku_data_conformance::run_conformance;
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_schema::{FieldPath, IndexDefinition, SchemaCatalog};
use runku_value::CanonicalValue;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::tempdir;

#[tokio::test]
async fn sqlite_passes_common_conformance_and_reopens() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("runku.sqlite3");
    let store = SqliteStore::open(&path, SqliteStoreConfig::TEST).await?;
    run_conformance(&store, StoreBackend::SQLite).await?;
    drop(store);

    let reopened = SqliteStore::open(&path, SqliteStoreConfig::TEST).await?;
    reopened.health().await?;
    assert_eq!(reopened.role(), SqliteRole::Test);
    Ok(())
}

#[tokio::test]
async fn production_role_is_rejected_before_file_creation() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("must-not-exist.sqlite3");
    let config = SqliteStoreConfig {
        role: SqliteRole::Production,
        ..SqliteStoreConfig::LOCAL
    };
    assert!(matches!(
        SqliteStore::open(&path, config).await,
        Err(StoreError::ProductionBackendUnsupported)
    ));
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn reset_requires_exact_environment_confirmation() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("reset.sqlite3");
    let store = SqliteStore::open(&path, SqliteStoreConfig::TEST).await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    assert_eq!(
        store
            .reset_environment(scope, EnvironmentId::generate())
            .await,
        Err(StoreError::InvalidRange)
    );
    store
        .reset_environment(scope, scope.environment_id())
        .await?;
    Ok(())
}

#[tokio::test]
async fn export_seed_and_confirmed_import_are_atomic() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let source = SqliteStore::open(
        directory.path().join("source.sqlite3"),
        SqliteStoreConfig::TEST,
    )
    .await?;
    let target = SqliteStore::open(
        directory.path().join("target.sqlite3"),
        SqliteStoreConfig::TEST,
    )
    .await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let mut batch = CommitBatch::new(scope, OperationId::generate());
    batch.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected: ExpectedRevision::Absent,
        value: CanonicalValue::String("seed".to_owned()),
    });
    source.commit(&batch).await?;

    let exported = source.export_environment(scope).await?;
    target.seed_environment(&exported).await?;
    assert_eq!(target.export_environment(scope).await?, exported);
    assert_eq!(
        target.seed_environment(&exported).await,
        Err(StoreError::MutationConflict)
    );
    assert_eq!(
        target
            .import_environment(&exported, EnvironmentId::generate())
            .await,
        Err(StoreError::InvalidRange)
    );
    target
        .import_environment(&exported, scope.environment_id())
        .await?;
    assert_eq!(target.export_environment(scope).await?, exported);
    Ok(())
}

#[tokio::test]
async fn interrupted_uncommitted_transaction_is_absent_after_reopen() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let path = directory.path().join("interrupted.sqlite3");
    let initialized = SqliteStore::open(&path, SqliteStoreConfig::TEST).await?;
    drop(initialized);

    let options = SqliteConnectOptions::new().filename(&path);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "INSERT INTO runku_environment_sequences(project_id, environment_id, commit_sequence) VALUES (?, ?, 99)",
    )
    .bind(scope.project_id().to_string())
    .bind(scope.environment_id().to_string())
    .execute(&mut *transaction)
    .await?;
    drop(transaction);
    pool.close().await;

    let reopened = SqliteStore::open(&path, SqliteStoreConfig::TEST).await?;
    let snapshot = reopened.begin_read(scope).await?;
    assert_eq!(snapshot.commit_sequence(), 0);
    snapshot.close().await?;
    Ok(())
}

#[tokio::test]
async fn index_backfill_is_bounded_resumable_and_exposed_for_dual_writes()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let store = SqliteStore::open(
        directory.path().join("indexes.sqlite3"),
        SqliteStoreConfig::TEST,
    )
    .await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    for (document, name) in [
        (DocumentId::generate(), "Ada"),
        (DocumentId::generate(), "Grace"),
    ] {
        let mut batch = CommitBatch::new(scope, OperationId::generate());
        batch.push_document(DocumentMutation::Upsert {
            table_id: table,
            document_id: document,
            expected: ExpectedRevision::Absent,
            value: CanonicalValue::Object(BTreeMap::from([(
                "name".to_owned(),
                CanonicalValue::String(name.to_owned()),
            )])),
        });
        store.commit(&batch).await?;
    }
    let index = runku_core::IndexId::generate();
    let catalog = SchemaCatalog::new(
        scope.project_id(),
        vec![IndexDefinition::new(
            index,
            table,
            "by_name".to_owned(),
            vec![FieldPath::new(vec!["name".to_owned()])?],
        )?],
    )?;
    let first = store.prepare_index_catalog(scope, &catalog, 1).await?;
    assert!(!first.ready);
    assert_eq!(first.processed_documents, 1);
    assert_eq!(
        store
            .effective_write_index_catalog(scope, &SchemaCatalog::new(scope.project_id(), vec![])?)
            .await?
            .indexes(),
        catalog.indexes()
    );
    let second = store.prepare_index_catalog(scope, &catalog, 1).await?;
    assert!(second.ready);
    let mut snapshot = store.begin_read(scope).await?;
    assert_eq!(
        snapshot
            .scan_index(index, &IndexRange::all(), 10)
            .await?
            .len(),
        2
    );
    snapshot.close().await?;
    Ok(())
}
