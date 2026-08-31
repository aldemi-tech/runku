//! Shared behavioral conformance suite for every Runku `LogicalStore` adapter.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use runku_core::{
    DocumentId, EnvironmentId, IndexId, OperationId, OutboxEventId, ProjectId, ReleaseId,
    ScheduledInvocationId, TableId, WorkerId,
};
use runku_data::{
    CommitBatch, DocumentMutation, DocumentReadAssertion, EnvironmentScope, ExpectedRevision,
    IndexMutation, IndexRange, LogicalStore, OutboxAppend, OutboxConsumerName, PinnedCode,
    ScheduleCancelResult, ScheduleCompletion, ScheduleStatus, ScheduledInvocationInsert,
    StoreBackend, StoreError,
};
use runku_value::{CanonicalValue, IndexKey, IndexValue, TimestampMicros};

/// Runs the common behavioral contract against one isolated adapter instance.
///
/// The supplied store must not already contain the generated scope. Assertions cover atomic
/// document/index/outbox/schedule writes, OCC, idempotent replay, ordered scans, tenant scoping,
/// scheduler leases, deletes, and bounded telemetry.
///
/// # Errors
///
/// Returns the first stable [`StoreError`] produced by the adapter.
///
/// # Panics
///
/// Panics when an adapter returns a successful value that violates the normative contract. This
/// function is a test oracle and must make such drift fail loudly.
#[allow(clippy::too_many_lines)]
pub async fn run_conformance(
    store: &dyn LogicalStore,
    expected_backend: StoreBackend,
) -> Result<(), StoreError> {
    assert_eq!(store.backend(), expected_backend);
    store.health().await?;

    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let other_scope = EnvironmentScope::new(scope.project_id(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let index = IndexId::generate();
    let key_a = IndexKey::encode(&[IndexValue::String("team".to_owned()), IndexValue::Int64(1)])
        .map_err(|_| StoreError::Internal)?;
    let key_b = IndexKey::encode(&[IndexValue::String("team".to_owned()), IndexValue::Int64(2)])
        .map_err(|_| StoreError::Internal)?;
    let outbox = OutboxEventId::generate();
    let schedule = ScheduledInvocationId::generate();
    let cancelled_schedule = ScheduledInvocationId::generate();

    let mut initial = CommitBatch::new(scope, OperationId::generate());
    initial.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected: ExpectedRevision::Absent,
        value: CanonicalValue::String("v1".to_owned()),
    });
    initial.push_index(IndexMutation::Put {
        index_id: index,
        key: key_a.clone(),
        table_id: table,
        document_id: document,
        document_revision: 1,
    });
    initial.push_outbox(OutboxAppend {
        event_id: outbox,
        payload: CanonicalValue::String("changed:v1".to_owned()),
    });
    initial.push_schedule(ScheduledInvocationInsert {
        id: schedule,
        pinned_code: PinnedCode::Release(ReleaseId::generate()),
        function: "messages.deliver"
            .parse()
            .map_err(|_| StoreError::Internal)?,
        args: CanonicalValue::Int64(7),
        execute_at: TimestampMicros::new(100),
        idempotency_key: Some("delivery-7".to_owned()),
    });
    initial.push_schedule(ScheduledInvocationInsert {
        id: cancelled_schedule,
        pinned_code: PinnedCode::Release(ReleaseId::generate()),
        function: "messages.cancelled"
            .parse()
            .map_err(|_| StoreError::Internal)?,
        args: CanonicalValue::Null,
        execute_at: TimestampMicros::new(100),
        idempotency_key: Some("cancel-me".to_owned()),
    });

    let committed = store.commit(&initial).await?;
    assert_eq!(committed.commit_sequence, 1);
    assert_eq!(committed.documents[0].revision, Some(1));
    assert!(!committed.replayed);
    assert_eq!(
        store.cancel_scheduled(scope, cancelled_schedule).await?,
        ScheduleCancelResult::Cancelled
    );
    assert_eq!(
        store.cancel_scheduled(scope, cancelled_schedule).await?,
        ScheduleCancelResult::AlreadyCancelled
    );

    let replay = store.commit(&initial).await?;
    assert_eq!(replay.commit_sequence, committed.commit_sequence);
    assert_eq!(replay.documents, committed.documents);
    assert!(replay.replayed);

    let mut reused = initial.clone();
    reused.push_outbox(OutboxAppend {
        event_id: OutboxEventId::generate(),
        payload: CanonicalValue::Null,
    });
    assert_eq!(
        store.commit(&reused).await,
        Err(StoreError::OperationIdReused)
    );

    let mut snapshot = store.begin_read(scope).await?;
    assert_eq!(snapshot.commit_sequence(), 1);
    let stored = snapshot
        .get_document(table, document)
        .await?
        .ok_or(StoreError::NotFound)?;
    assert_eq!(stored.revision, 1);
    assert_eq!(stored.value, CanonicalValue::String("v1".to_owned()));
    assert_eq!(
        snapshot.get_outbox(outbox).await?,
        Some(CanonicalValue::String("changed:v1".to_owned()))
    );
    let scheduled = snapshot
        .get_scheduled(schedule)
        .await?
        .ok_or(StoreError::NotFound)?;
    assert_eq!(scheduled.status, ScheduleStatus::Pending);
    assert_eq!(scheduled.commit_sequence, 1);
    let prefix = key_a.prefix(1).map_err(|_| StoreError::Internal)?;
    let entries = snapshot
        .scan_index(index, &IndexRange::prefix(&prefix)?, 100)
        .await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, key_a);
    snapshot.close().await?;

    let mut other = store.begin_read(other_scope).await?;
    assert!(other.get_document(table, document).await?.is_none());
    assert!(other.get_outbox(outbox).await?.is_none());
    other.close().await?;

    assert_atomic_rollback(store, scope, table, document).await?;

    let update_outbox = OutboxEventId::generate();
    let mut update = CommitBatch::new(scope, OperationId::generate());
    update.push_read(DocumentReadAssertion {
        table_id: table,
        document_id: document,
        observed_revision: Some(1),
    });
    update.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected: ExpectedRevision::Exact(1),
        value: CanonicalValue::String("v2".to_owned()),
    });
    update.push_index(IndexMutation::Delete {
        index_id: index,
        key: key_a.clone(),
        document_id: document,
    });
    update.push_index(IndexMutation::Put {
        index_id: index,
        key: key_b.clone(),
        table_id: table,
        document_id: document,
        document_revision: 2,
    });
    update.push_outbox(OutboxAppend {
        event_id: update_outbox,
        payload: CanonicalValue::String("changed:v2".to_owned()),
    });
    let updated = store.commit(&update).await?;
    assert_eq!(updated.commit_sequence, 2);
    assert_eq!(updated.documents[0].revision, Some(2));

    assert_outbox_consumer_leases(store, scope, other_scope, outbox, update_outbox).await?;

    let stale_event = OutboxEventId::generate();
    let mut stale_read = CommitBatch::new(scope, OperationId::generate());
    stale_read.push_read(DocumentReadAssertion {
        table_id: table,
        document_id: document,
        observed_revision: Some(1),
    });
    stale_read.push_outbox(OutboxAppend {
        event_id: stale_event,
        payload: CanonicalValue::String("must-not-exist".to_owned()),
    });
    assert_eq!(
        store.commit(&stale_read).await,
        Err(StoreError::MutationConflict)
    );
    let mut stale_snapshot = store.begin_read(scope).await?;
    assert!(stale_snapshot.get_outbox(stale_event).await?.is_none());
    stale_snapshot.close().await?;

    let mut after_update = store.begin_read(scope).await?;
    let entries = after_update
        .scan_index(index, &IndexRange::all(), 100)
        .await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, key_b);
    assert_eq!(entries[0].document_revision, 2);
    after_update.close().await?;

    assert_scheduler_leases(store, scope, schedule).await?;

    let mut delete = CommitBatch::new(scope, OperationId::generate());
    delete.push_index(IndexMutation::Delete {
        index_id: index,
        key: key_b,
        document_id: document,
    });
    delete.push_document(DocumentMutation::Delete {
        table_id: table,
        document_id: document,
        expected_revision: 2,
    });
    let deleted = store.commit(&delete).await?;
    assert_eq!(deleted.commit_sequence, 3);
    assert_eq!(deleted.documents[0].revision, None);
    let mut after_delete = store.begin_read(scope).await?;
    assert!(after_delete.get_document(table, document).await?.is_none());
    assert!(
        after_delete
            .scan_index(index, &IndexRange::all(), 100)
            .await?
            .is_empty()
    );
    after_delete.close().await?;

    let telemetry = store.telemetry();
    assert!(telemetry.snapshots_opened >= 5);
    assert!(telemetry.reads >= 10);
    assert_eq!(telemetry.commits, 3);
    assert!(telemetry.commit_replays >= 1);
    assert!(telemetry.conflicts >= 1);
    assert!(telemetry.schedules_claimed >= 2);
    assert_eq!(telemetry.schedules_cancelled, 1);
    assert!(telemetry.outbox_events_claimed >= 5);
    assert_eq!(telemetry.outbox_acks, 2);
    assert!(telemetry.pool_size >= 1);
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn assert_outbox_consumer_leases(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    other_scope: EnvironmentScope,
    first_event: OutboxEventId,
    second_event: OutboxEventId,
) -> Result<(), StoreError> {
    let consumer: OutboxConsumerName = "realtime-v1".parse()?;
    let independent: OutboxConsumerName = "analytics-v1".parse()?;
    let first_worker = WorkerId::generate();
    let second_worker = WorkerId::generate();

    assert_eq!(
        store
            .claim_outbox(
                scope,
                &consumer,
                first_worker,
                TimestampMicros::new(100),
                TimestampMicros::new(200),
                0,
            )
            .await,
        Err(StoreError::LimitExceeded)
    );
    let first = store
        .claim_outbox(
            scope,
            &consumer,
            first_worker,
            TimestampMicros::new(100),
            TimestampMicros::new(200),
            1,
        )
        .await?;
    assert_eq!(first.acknowledged_through, None);
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0].event_id, first_event);
    assert_eq!(first.events[0].commit_sequence, 1);
    let first_cursor = first.events[0].cursor();
    store
        .ack_outbox(
            scope,
            &consumer,
            first_worker,
            first.lease_generation,
            first_cursor,
        )
        .await?;

    let second = store
        .claim_outbox(
            scope,
            &consumer,
            first_worker,
            TimestampMicros::new(201),
            TimestampMicros::new(300),
            10,
        )
        .await?;
    assert_eq!(second.acknowledged_through, Some(first_cursor));
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].event_id, second_event);
    assert_eq!(second.events[0].commit_sequence, 2);
    let second_cursor = second.events[0].cursor();
    assert_eq!(
        store
            .claim_outbox(
                scope,
                &consumer,
                second_worker,
                TimestampMicros::new(250),
                TimestampMicros::new(350),
                10,
            )
            .await,
        Err(StoreError::Busy)
    );
    assert_eq!(
        store
            .ack_outbox(
                scope,
                &consumer,
                second_worker,
                second.lease_generation,
                second_cursor,
            )
            .await,
        Err(StoreError::OutboxLeaseLost)
    );

    let redelivered = store
        .claim_outbox(
            scope,
            &consumer,
            second_worker,
            TimestampMicros::new(301),
            TimestampMicros::new(400),
            10,
        )
        .await?;
    assert!(redelivered.lease_generation > second.lease_generation);
    assert_eq!(redelivered.acknowledged_through, Some(first_cursor));
    assert_eq!(redelivered.events, second.events);
    store
        .ack_outbox(
            scope,
            &consumer,
            second_worker,
            redelivered.lease_generation,
            second_cursor,
        )
        .await?;

    let caught_up = store
        .claim_outbox(
            scope,
            &consumer,
            first_worker,
            TimestampMicros::new(302),
            TimestampMicros::new(500),
            10,
        )
        .await?;
    assert_eq!(caught_up.acknowledged_through, Some(second_cursor));
    assert!(caught_up.events.is_empty());

    let independent_batch = store
        .claim_outbox(
            scope,
            &independent,
            first_worker,
            TimestampMicros::new(100),
            TimestampMicros::new(200),
            10,
        )
        .await?;
    assert_eq!(independent_batch.events.len(), 2);
    assert_eq!(independent_batch.events[0].event_id, first_event);
    assert_eq!(independent_batch.events[1].event_id, second_event);

    let isolated = store
        .claim_outbox(
            other_scope,
            &consumer,
            first_worker,
            TimestampMicros::new(100),
            TimestampMicros::new(200),
            10,
        )
        .await?;
    assert!(isolated.events.is_empty());
    Ok(())
}

async fn assert_atomic_rollback(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    table: TableId,
    document: DocumentId,
) -> Result<(), StoreError> {
    let event = OutboxEventId::generate();
    let scheduled_id = ScheduledInvocationId::generate();
    let mut conflict = CommitBatch::new(scope, OperationId::generate());
    conflict.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected: ExpectedRevision::Absent,
        value: CanonicalValue::String("must-not-commit".to_owned()),
    });
    conflict.push_outbox(OutboxAppend {
        event_id: event,
        payload: CanonicalValue::String("must-not-exist".to_owned()),
    });
    conflict.push_schedule(ScheduledInvocationInsert {
        id: scheduled_id,
        pinned_code: PinnedCode::Release(ReleaseId::generate()),
        function: "must.not.run".parse().map_err(|_| StoreError::Internal)?,
        args: CanonicalValue::Null,
        execute_at: TimestampMicros::new(0),
        idempotency_key: None,
    });
    assert_eq!(
        store.commit(&conflict).await,
        Err(StoreError::MutationConflict)
    );
    let mut snapshot = store.begin_read(scope).await?;
    assert!(snapshot.get_outbox(event).await?.is_none());
    assert!(snapshot.get_scheduled(scheduled_id).await?.is_none());
    let current = snapshot
        .get_document(table, document)
        .await?
        .ok_or(StoreError::NotFound)?;
    assert_eq!(current.value, CanonicalValue::String("v1".to_owned()));
    snapshot.close().await
}

async fn assert_scheduler_leases(
    store: &dyn LogicalStore,
    scope: EnvironmentScope,
    schedule: ScheduledInvocationId,
) -> Result<(), StoreError> {
    let first_worker = WorkerId::generate();
    let second_worker = WorkerId::generate();
    let first = store
        .claim_due_scheduled(
            scope,
            first_worker,
            TimestampMicros::new(100),
            TimestampMicros::new(200),
            10,
        )
        .await?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].record.id, schedule);
    assert_eq!(first[0].record.lease_generation, 1);
    assert_eq!(
        store.cancel_scheduled(scope, schedule).await?,
        ScheduleCancelResult::Running
    );
    let second = store
        .claim_due_scheduled(
            scope,
            second_worker,
            TimestampMicros::new(100),
            TimestampMicros::new(200),
            10,
        )
        .await?;
    assert!(second.is_empty());
    assert_eq!(
        store
            .complete_scheduled(
                scope,
                schedule,
                second_worker,
                1,
                &ScheduleCompletion::Succeeded,
            )
            .await,
        Err(StoreError::LeaseLost)
    );
    store
        .complete_scheduled(
            scope,
            schedule,
            first_worker,
            1,
            &ScheduleCompletion::Retry {
                execute_at: TimestampMicros::new(300),
                error_code: "TEMPORARY_UNAVAILABLE".to_owned(),
            },
        )
        .await?;
    assert!(
        store
            .claim_due_scheduled(
                scope,
                second_worker,
                TimestampMicros::new(299),
                TimestampMicros::new(400),
                10,
            )
            .await?
            .is_empty()
    );
    let retried = store
        .claim_due_scheduled(
            scope,
            second_worker,
            TimestampMicros::new(300),
            TimestampMicros::new(400),
            10,
        )
        .await?;
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].record.lease_generation, 2);
    store
        .complete_scheduled(
            scope,
            schedule,
            second_worker,
            2,
            &ScheduleCompletion::Succeeded,
        )
        .await?;
    let mut snapshot = store.begin_read(scope).await?;
    let record = snapshot
        .get_scheduled(schedule)
        .await?
        .ok_or(StoreError::NotFound)?;
    assert_eq!(record.status, ScheduleStatus::Succeeded);
    assert_eq!(record.attempts, 2);
    snapshot.close().await?;
    assert_eq!(
        store.cancel_scheduled(scope, schedule).await?,
        ScheduleCancelResult::Terminal
    );
    Ok(())
}
