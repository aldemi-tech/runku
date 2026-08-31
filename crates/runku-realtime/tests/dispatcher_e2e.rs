//! Mutation-to-delivery E2E including crash-before-ACK recovery.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use runku_core::{
    DocumentId, EnvironmentId, EnvironmentScope, FunctionName, IndexId, OperationId, OutboxEventId,
    ProjectId, ReleaseId, ScheduledInvocationId, SubscriptionId, TableId, WorkerId,
};
use runku_data::{
    ClaimedOutboxBatch, ClaimedScheduledInvocation, CommitBatch, CommitResult, DocumentMutation,
    ExpectedRevision, IndexMutation, LogicalStore, OutboxAppend, OutboxConsumerName, OutboxCursor,
    PinnedCode, ReadSnapshot, ScheduleCancelResult, ScheduleCompletion, StoreBackend, StoreError,
    StoreTelemetrySnapshot,
};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::{DependencyBound, ExecutionError, QueryOutcome, ReadDependency};
use runku_realtime::{
    ChangeDispatcher, DeliveryEvent, DispatcherConfig, DispatcherError, RegistryConfig,
    SubscriptionRegistry, SubscriptionRunFailure, SubscriptionRunner, SubscriptionSpec,
};
use runku_value::{CanonicalValue, IndexKey, IndexValue, TimestampMicros};
use tempfile::TempDir;

mod support;

#[derive(Debug)]
struct FailFirstAckStore {
    inner: Arc<SqliteStore>,
    fail_ack: AtomicBool,
}

#[async_trait]
impl LogicalStore for FailFirstAckStore {
    fn backend(&self) -> StoreBackend {
        self.inner.backend()
    }

    async fn begin_read(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Box<dyn ReadSnapshot>, StoreError> {
        self.inner.begin_read(scope).await
    }

    async fn commit(&self, batch: &CommitBatch) -> Result<CommitResult, StoreError> {
        self.inner.commit(batch).await
    }

    async fn claim_outbox(
        &self,
        scope: EnvironmentScope,
        consumer: &OutboxConsumerName,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<ClaimedOutboxBatch, StoreError> {
        self.inner
            .claim_outbox(scope, consumer, worker_id, now, lease_until, limit)
            .await
    }

    async fn ack_outbox(
        &self,
        scope: EnvironmentScope,
        consumer: &OutboxConsumerName,
        worker_id: WorkerId,
        lease_generation: u64,
        through: OutboxCursor,
    ) -> Result<(), StoreError> {
        if self.fail_ack.swap(false, Ordering::AcqRel) {
            return Err(StoreError::OutboxLeaseLost);
        }
        self.inner
            .ack_outbox(scope, consumer, worker_id, lease_generation, through)
            .await
    }

    async fn claim_due_scheduled(
        &self,
        scope: EnvironmentScope,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<Vec<ClaimedScheduledInvocation>, StoreError> {
        self.inner
            .claim_due_scheduled(scope, worker_id, now, lease_until, limit)
            .await
    }

    async fn complete_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
        worker_id: WorkerId,
        lease_generation: u64,
        completion: &ScheduleCompletion,
    ) -> Result<(), StoreError> {
        self.inner
            .complete_scheduled(scope, id, worker_id, lease_generation, completion)
            .await
    }

    async fn cancel_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
    ) -> Result<ScheduleCancelResult, StoreError> {
        self.inner.cancel_scheduled(scope, id).await
    }

    async fn health(&self) -> Result<(), StoreError> {
        self.inner.health().await
    }

    fn telemetry(&self) -> StoreTelemetrySnapshot {
        self.inner.telemetry()
    }
}

#[derive(Debug)]
struct DeterministicRunner {
    table: TableId,
    document: DocumentId,
    index: IndexId,
    old_key: IndexKey,
    new_key: IndexKey,
    calls: AtomicU64,
}

#[async_trait]
impl SubscriptionRunner for DeterministicRunner {
    async fn rerun(&self, spec: &SubscriptionSpec) -> Result<QueryOutcome, SubscriptionRunFailure> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        let dependency = match spec.function.as_str() {
            "point.read" => ReadDependency::Point {
                table_id: self.table,
                document_id: self.document,
                observed_revision: Some(2),
                snapshot_sequence: 2,
            },
            "range.old" => ReadDependency::Range {
                index_id: self.index,
                lower: DependencyBound::Inclusive(self.old_key.as_bytes().to_vec()),
                upper: DependencyBound::Inclusive(self.old_key.as_bytes().to_vec()),
                snapshot_sequence: 2,
            },
            "range.new" => ReadDependency::Range {
                index_id: self.index,
                lower: DependencyBound::Inclusive(self.new_key.as_bytes().to_vec()),
                upper: DependencyBound::Inclusive(self.new_key.as_bytes().to_vec()),
                snapshot_sequence: 2,
            },
            _ => {
                return Err(SubscriptionRunFailure::from(ExecutionError::Storage(
                    StoreError::Internal,
                )));
            }
        };
        Ok(QueryOutcome {
            value: CanonicalValue::String(format!("{}:{call}", spec.function)),
            snapshot_sequence: Some(2),
            dependencies: vec![dependency],
        })
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn crash_before_ack_redelivers_without_duplicate_rerun_and_reconnects()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let sqlite = Arc::new(
        SqliteStore::open(
            directory.path().join("realtime.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let store = Arc::new(FailFirstAckStore {
        inner: sqlite,
        fail_ack: AtomicBool::new(true),
    });
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let index = IndexId::generate();
    let old_key = IndexKey::encode(&[IndexValue::String("old".to_owned())])?;
    let new_key = IndexKey::encode(&[IndexValue::String("new".to_owned())])?;

    let mut seed = CommitBatch::new(scope, OperationId::generate());
    seed.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected: ExpectedRevision::Absent,
        value: CanonicalValue::String("v1".to_owned()),
    });
    seed.push_index(IndexMutation::Put {
        index_id: index,
        key: old_key.clone(),
        table_id: table,
        document_id: document,
        document_revision: 1,
    });
    store.commit(&seed).await?;

    let registry = SubscriptionRegistry::new(RegistryConfig {
        max_subscriptions: 10,
        max_dependencies: 10,
        max_result_bytes: 4_096,
        delivery_buffer: 8,
        retry_base_micros: 10,
        retry_max_micros: 100,
        max_consecutive_failures: 3,
    })?;
    let mut handles = Vec::new();
    for (name, dependency) in [
        (
            "point.read",
            ReadDependency::Point {
                table_id: table,
                document_id: document,
                observed_revision: Some(1),
                snapshot_sequence: 1,
            },
        ),
        (
            "range.old",
            ReadDependency::Range {
                index_id: index,
                lower: DependencyBound::Inclusive(old_key.as_bytes().to_vec()),
                upper: DependencyBound::Inclusive(old_key.as_bytes().to_vec()),
                snapshot_sequence: 1,
            },
        ),
        (
            "range.new",
            ReadDependency::Range {
                index_id: index,
                lower: DependencyBound::Inclusive(new_key.as_bytes().to_vec()),
                upper: DependencyBound::Inclusive(new_key.as_bytes().to_vec()),
                snapshot_sequence: 1,
            },
        ),
    ] {
        let release_id = ReleaseId::generate();
        let spec = SubscriptionSpec {
            id: SubscriptionId::generate(),
            scope,
            release_id,
            pinned_code: PinnedCode::Release(release_id),
            function: name.parse::<FunctionName>()?,
            arguments: CanonicalValue::Null,
            identity: support::anonymous_identity(scope).await?,
            authorized_until: TimestampMicros::new(1_000_000),
        };
        handles.push(registry.register(
            spec,
            QueryOutcome {
                value: CanonicalValue::String("initial".to_owned()),
                snapshot_sequence: Some(1),
                dependencies: vec![dependency],
            },
        )?);
    }

    let event_id = OutboxEventId::generate();
    let mut update = CommitBatch::new(scope, OperationId::generate());
    update.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected: ExpectedRevision::Exact(1),
        value: CanonicalValue::String("v2".to_owned()),
    });
    update.push_index(IndexMutation::Delete {
        index_id: index,
        key: old_key.clone(),
        document_id: document,
    });
    update.push_index(IndexMutation::Put {
        index_id: index,
        key: new_key.clone(),
        table_id: table,
        document_id: document,
        document_revision: 2,
    });
    update.push_outbox(OutboxAppend {
        event_id,
        payload: impact_payload(table, document, index, &old_key, &new_key),
    });
    assert_eq!(store.commit(&update).await?.commit_sequence, 2);

    let runner = Arc::new(DeterministicRunner {
        table,
        document,
        index,
        old_key,
        new_key,
        calls: AtomicU64::new(0),
    });
    let consumer: OutboxConsumerName = "realtime-e2e".parse()?;
    let first = ChangeDispatcher::new(
        store.clone(),
        registry.clone(),
        runner.clone(),
        consumer.clone(),
        WorkerId::generate(),
        DispatcherConfig {
            batch_limit: 10,
            lease_micros: 50,
        },
    )?;
    assert_eq!(
        first.poll_once(scope, TimestampMicros::new(100)).await,
        Err(DispatcherError::Storage(StoreError::OutboxLeaseLost))
    );
    assert_eq!(runner.calls.load(Ordering::Relaxed), 3);

    let recovered = ChangeDispatcher::new(
        store.clone(),
        registry.clone(),
        runner.clone(),
        consumer,
        WorkerId::generate(),
        DispatcherConfig {
            batch_limit: 10,
            lease_micros: 50,
        },
    )?;
    let recovery = recovered
        .poll_once(scope, TimestampMicros::new(151))
        .await?;
    assert_eq!(recovery.events, 1);
    assert_eq!(recovery.reruns, 0);
    assert_eq!(runner.calls.load(Ordering::Relaxed), 3);
    assert_eq!(
        recovery.acknowledged_through.map(|value| value.event_id),
        Some(event_id)
    );

    for handle in &mut handles {
        assert!(matches!(
            handle.receiver.recv().await?,
            DeliveryEvent::State {
                delivery_revision: 1,
                ..
            }
        ));
        assert!(matches!(
            handle.receiver.recv().await?,
            DeliveryEvent::State {
                delivery_revision: 2,
                ..
            }
        ));
        let reconnected = registry.subscribe(handle.snapshot.spec.id)?;
        assert_eq!(reconnected.snapshot.delivery_revision, 2);
        assert_eq!(
            reconnected
                .snapshot
                .processed_through
                .map(|value| value.event_id),
            Some(event_id)
        );
    }

    let aborted_event = OutboxEventId::generate();
    let mut aborted = CommitBatch::new(scope, OperationId::generate());
    aborted.push_document(DocumentMutation::Upsert {
        table_id: table,
        document_id: document,
        expected: ExpectedRevision::Exact(1),
        value: CanonicalValue::String("never".to_owned()),
    });
    aborted.push_outbox(OutboxAppend {
        event_id: aborted_event,
        payload: impact_payload(table, document, index, &runner.old_key, &runner.new_key),
    });
    assert_eq!(
        store.commit(&aborted).await,
        Err(StoreError::MutationConflict)
    );
    let caught_up = recovered
        .poll_once(scope, TimestampMicros::new(152))
        .await?;
    assert_eq!(caught_up.events, 0);
    let mut snapshot = store.begin_read(scope).await?;
    assert!(snapshot.get_outbox(aborted_event).await?.is_none());
    snapshot.close().await?;
    assert_eq!(recovered.telemetry().acknowledgements, 1);
    Ok(())
}

fn impact_payload(
    table: TableId,
    document: DocumentId,
    index: IndexId,
    old_key: &IndexKey,
    new_key: &IndexKey,
) -> CanonicalValue {
    CanonicalValue::Object(BTreeMap::from([
        (
            "indexes".to_owned(),
            CanonicalValue::Array(vec![
                index_impact("delete", index, document, old_key),
                index_impact("put", index, document, new_key),
            ]),
        ),
        (
            "type".to_owned(),
            CanonicalValue::String("document_write_set_v2".to_owned()),
        ),
        (
            "writes".to_owned(),
            CanonicalValue::Array(vec![CanonicalValue::Object(BTreeMap::from([
                (
                    "documentId".to_owned(),
                    CanonicalValue::String(document.to_string()),
                ),
                (
                    "kind".to_owned(),
                    CanonicalValue::String("replace".to_owned()),
                ),
                (
                    "tableId".to_owned(),
                    CanonicalValue::String(table.to_string()),
                ),
            ]))]),
        ),
    ]))
}

#[derive(Debug, Default)]
struct NoCallRunner(AtomicU64);

#[async_trait]
impl SubscriptionRunner for NoCallRunner {
    async fn rerun(
        &self,
        _spec: &SubscriptionSpec,
    ) -> Result<QueryOutcome, SubscriptionRunFailure> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(QueryOutcome {
            value: CanonicalValue::Null,
            snapshot_sequence: None,
            dependencies: Vec::new(),
        })
    }
}

#[tokio::test]
async fn expired_authorization_suspends_without_runner_or_state_loss() -> Result<(), Box<dyn Error>>
{
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("expired.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let release_id = ReleaseId::generate();
    let registry = SubscriptionRegistry::new(RegistryConfig {
        max_subscriptions: 2,
        max_dependencies: 2,
        max_result_bytes: 1_024,
        delivery_buffer: 4,
        retry_base_micros: 10,
        retry_max_micros: 100,
        max_consecutive_failures: 2,
    })?;
    let spec = SubscriptionSpec {
        id: SubscriptionId::generate(),
        scope,
        release_id,
        pinned_code: PinnedCode::Release(release_id),
        function: "point.read".parse()?,
        arguments: CanonicalValue::Null,
        identity: support::anonymous_identity(scope).await?,
        authorized_until: TimestampMicros::new(5),
    };
    let id = spec.id;
    let mut handle = registry.register(
        spec,
        QueryOutcome {
            value: CanonicalValue::String("last-valid".to_owned()),
            snapshot_sequence: Some(1),
            dependencies: vec![ReadDependency::Point {
                table_id: table,
                document_id: document,
                observed_revision: None,
                snapshot_sequence: 1,
            }],
        },
    )?;
    let mut batch = CommitBatch::new(scope, OperationId::generate());
    batch.push_outbox(OutboxAppend {
        event_id: OutboxEventId::generate(),
        payload: impact_payload(
            table,
            document,
            IndexId::generate(),
            &IndexKey::encode(&[IndexValue::String("a".to_owned())])?,
            &IndexKey::encode(&[IndexValue::String("b".to_owned())])?,
        ),
    });
    store.commit(&batch).await?;
    let runner = Arc::new(NoCallRunner::default());
    let dispatcher = ChangeDispatcher::new(
        store,
        registry.clone(),
        runner.clone(),
        "expired-auth".parse()?,
        WorkerId::generate(),
        DispatcherConfig::PRODUCTION,
    )?;
    assert!(matches!(
        dispatcher.poll_once(scope, TimestampMicros::new(5)).await,
        Err(DispatcherError::Rerun(error)) if error.code() == "AUTHORIZATION_EXPIRED"
    ));
    assert_eq!(runner.0.load(Ordering::Relaxed), 0);
    let snapshot = registry.subscribe(id)?.snapshot;
    assert!(snapshot.suspended);
    assert_eq!(
        snapshot.value,
        CanonicalValue::String("last-valid".to_owned())
    );
    assert!(matches!(
        handle.receiver.recv().await?,
        DeliveryEvent::State {
            delivery_revision: 1,
            ..
        }
    ));
    assert!(matches!(
        handle.receiver.recv().await?,
        DeliveryEvent::Error {
            delivery_revision: 2,
            code: "AUTHORIZATION_EXPIRED",
            suspended: true,
            ..
        }
    ));
    Ok(())
}

fn index_impact(
    kind: &str,
    index: IndexId,
    document: DocumentId,
    key: &IndexKey,
) -> CanonicalValue {
    CanonicalValue::Object(BTreeMap::from([
        (
            "documentId".to_owned(),
            CanonicalValue::String(document.to_string()),
        ),
        (
            "indexId".to_owned(),
            CanonicalValue::String(index.to_string()),
        ),
        (
            "key".to_owned(),
            CanonicalValue::Bytes(key.as_bytes().to_vec()),
        ),
        ("kind".to_owned(), CanonicalValue::String(kind.to_owned())),
    ]))
}
