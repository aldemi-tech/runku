//! `PostgreSQL` adapter integration and concurrency tests.

use std::sync::Arc;

use runku_core::{
    DocumentId, EnvironmentId, OperationId, ProjectId, ReleaseId, ScheduledInvocationId, TableId,
    WorkerId,
};
use runku_data::{
    CommitBatch, DocumentMutation, EnvironmentScope, ExpectedRevision, LogicalStore, PinnedCode,
    ScheduledInvocationInsert, StoreBackend, StoreError,
};
use runku_data_postgres::{PostgresStore, PostgresStoreConfig};
use runku_value::{CanonicalValue, TimestampMicros};
use tokio::sync::Barrier;

fn test_url() -> Option<String> {
    std::env::var("RUNKU_TEST_POSTGRES_URL").ok()
}

#[tokio::test]
async fn common_logical_store_conformance() -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = test_url() else {
        return Ok(());
    };
    let store = PostgresStore::connect(&url, PostgresStoreConfig::TEST).await?;
    runku_data_conformance::run_conformance(&store, StoreBackend::PostgreSQL).await?;
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn health_and_bounded_pool_are_observable() -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = test_url() else {
        return Ok(());
    };
    let config = PostgresStoreConfig {
        min_connections: 1,
        max_connections: 2,
        ..PostgresStoreConfig::TEST
    };
    let store = PostgresStore::connect(&url, config).await?;
    store.health().await?;
    let telemetry = store.telemetry();
    assert!((1..=2).contains(&telemetry.pool_size));
    assert!(telemetry.pool_idle <= telemetry.pool_size);
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn invalid_pool_configuration_is_rejected_before_connect() {
    let config = PostgresStoreConfig {
        min_connections: 2,
        max_connections: 1,
        ..PostgresStoreConfig::TEST
    };
    assert!(
        PostgresStore::connect("postgres://invalid", config)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn repeatable_read_snapshot_does_not_tear() -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = test_url() else {
        return Ok(());
    };
    let store = PostgresStore::connect(&url, PostgresStoreConfig::TEST).await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    store
        .commit(&upsert(
            scope,
            table,
            document,
            ExpectedRevision::Absent,
            "v1",
        ))
        .await?;

    let mut snapshot = store.begin_read(scope).await?;
    assert_eq!(snapshot.commit_sequence(), 1);
    assert_eq!(
        snapshot
            .get_document(table, document)
            .await?
            .ok_or(StoreError::NotFound)?
            .value,
        CanonicalValue::String("v1".into())
    );

    store
        .commit(&upsert(
            scope,
            table,
            document,
            ExpectedRevision::Exact(1),
            "v2",
        ))
        .await?;
    let stable = snapshot
        .get_document(table, document)
        .await?
        .ok_or(StoreError::NotFound)?;
    assert_eq!(stable.revision, 1);
    assert_eq!(stable.value, CanonicalValue::String("v1".into()));
    snapshot.close().await?;

    let mut current = store.begin_read(scope).await?;
    assert_eq!(
        current
            .get_document(table, document)
            .await?
            .ok_or(StoreError::NotFound)?
            .revision,
        2
    );
    current.close().await?;
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn concurrent_occ_updates_allow_exactly_one_winner() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(url) = test_url() else {
        return Ok(());
    };
    let store = PostgresStore::connect(&url, PostgresStoreConfig::TEST).await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    store
        .commit(&upsert(
            scope,
            table,
            document,
            ExpectedRevision::Absent,
            "initial",
        ))
        .await?;
    let barrier = Arc::new(Barrier::new(3));
    let first_store = store.clone();
    let first_barrier = Arc::clone(&barrier);
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_store
            .commit(&upsert(
                scope,
                table,
                document,
                ExpectedRevision::Exact(1),
                "first",
            ))
            .await
    });
    let second_store = store.clone();
    let second_barrier = Arc::clone(&barrier);
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_store
            .commit(&upsert(
                scope,
                table,
                document,
                ExpectedRevision::Exact(1),
                "second",
            ))
            .await
    });
    barrier.wait().await;
    let results = [first.await?, second.await?];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(results.iter().any(|result| matches!(
        result,
        Err(StoreError::MutationConflict | StoreError::SerializationFailure)
    )));
    let mut snapshot = store.begin_read(scope).await?;
    assert_eq!(
        snapshot
            .get_document(table, document)
            .await?
            .ok_or(StoreError::NotFound)?
            .revision,
        2
    );
    snapshot.close().await?;
    store.close().await;
    Ok(())
}

#[tokio::test]
async fn skip_locked_prevents_duplicate_multiworker_claims()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = test_url() else {
        return Ok(());
    };
    let store = PostgresStore::connect(&url, PostgresStoreConfig::TEST).await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let schedule = ScheduledInvocationId::generate();
    let mut batch = CommitBatch::new(scope, OperationId::generate());
    batch.push_schedule(ScheduledInvocationInsert {
        id: schedule,
        pinned_code: PinnedCode::Release(ReleaseId::generate()),
        function: "test.concurrent".parse()?,
        args: CanonicalValue::Null,
        execute_at: TimestampMicros::new(10),
        idempotency_key: None,
    });
    store.commit(&batch).await?;
    let barrier = Arc::new(Barrier::new(3));
    let mut tasks = Vec::new();
    for worker in [WorkerId::generate(), WorkerId::generate()] {
        let worker_store = store.clone();
        let worker_barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            worker_barrier.wait().await;
            worker_store
                .claim_due_scheduled(
                    scope,
                    worker,
                    TimestampMicros::new(10),
                    TimestampMicros::new(100),
                    1,
                )
                .await
        }));
    }
    barrier.wait().await;
    let mut claimed = Vec::new();
    for task in tasks {
        match task.await? {
            Ok(records) => claimed.extend(records),
            Err(StoreError::SerializationFailure) => {}
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].record.id, schedule);
    let follow_up = store
        .claim_due_scheduled(
            scope,
            WorkerId::generate(),
            TimestampMicros::new(10),
            TimestampMicros::new(100),
            1,
        )
        .await?;
    assert!(follow_up.is_empty());
    store.close().await;
    Ok(())
}

fn upsert(
    scope: EnvironmentScope,
    table: TableId,
    document: DocumentId,
    expected: ExpectedRevision,
    value: &str,
) -> CommitBatch {
    let mut batch = CommitBatch::new(scope, OperationId::generate());
    batch.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected,
        value: CanonicalValue::String(value.to_owned()),
    });
    batch
}
