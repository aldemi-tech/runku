//! Subscription registry state-machine conformance.

use std::{collections::BTreeMap, error::Error};

use runku_core::{
    DocumentId, EnvironmentId, EnvironmentScope, FunctionName, OutboxEventId, ProjectId, ReleaseId,
    SubscriptionId, TableId,
};
use runku_data::{OutboxCursor, PinnedCode};
use runku_execution::{QueryOutcome, ReadDependency};
use runku_realtime::{
    ChangeImpact, DeliveryEvent, RealtimeError, RegistryConfig, SubscriptionRegistry,
    SubscriptionSpec,
};
use runku_value::{CanonicalValue, TimestampMicros};

mod support;

fn config() -> RegistryConfig {
    RegistryConfig {
        max_subscriptions: 4,
        max_dependencies: 4,
        max_result_bytes: 1_024,
        delivery_buffer: 4,
        retry_base_micros: 10,
        retry_max_micros: 40,
        max_consecutive_failures: 2,
    }
}

async fn spec(scope: EnvironmentScope) -> Result<SubscriptionSpec, Box<dyn Error>> {
    let release_id = ReleaseId::generate();
    Ok(SubscriptionSpec {
        id: SubscriptionId::generate(),
        scope,
        release_id,
        pinned_code: PinnedCode::Release(release_id),
        function: "messages.list".parse::<FunctionName>()?,
        arguments: CanonicalValue::Null,
        identity: support::anonymous_identity(scope).await?,
        authorized_until: TimestampMicros::new(1_000_000),
    })
}

fn outcome(table: TableId, document: DocumentId, value: &str) -> QueryOutcome {
    QueryOutcome {
        value: CanonicalValue::String(value.to_owned()),
        snapshot_sequence: Some(1),
        dependencies: vec![ReadDependency::Point {
            table_id: table,
            document_id: document,
            observed_revision: None,
            snapshot_sequence: 1,
        }],
    }
}

fn impact(table: TableId, document: DocumentId) -> Result<ChangeImpact, RealtimeError> {
    ChangeImpact::decode(&CanonicalValue::Object(BTreeMap::from([
        ("indexes".to_owned(), CanonicalValue::Array(Vec::new())),
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
                    CanonicalValue::String("insert".to_owned()),
                ),
                (
                    "tableId".to_owned(),
                    CanonicalValue::String(table.to_string()),
                ),
            ]))]),
        ),
    ])))
}

#[tokio::test]
async fn initial_delivery_coalescing_fencing_and_cursor_dedup_are_exact()
-> Result<(), Box<dyn Error>> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let registry = SubscriptionRegistry::new(config())?;
    let spec = spec(scope).await?;
    let id = spec.id;
    let mut handle = registry.register(spec, outcome(table, document, "initial"))?;
    assert!(matches!(
        handle.receiver.recv().await?,
        DeliveryEvent::State {
            delivery_revision: 1,
            ..
        }
    ));

    let first_cursor = OutboxCursor {
        commit_sequence: 1,
        event_id: OutboxEventId::generate(),
    };
    let second_cursor = OutboxCursor {
        commit_sequence: 2,
        event_id: OutboxEventId::generate(),
    };
    let change = impact(table, document)?;
    let first = registry.mark_impacted(scope, first_cursor, &change, TimestampMicros::new(0))?;
    assert_eq!(first.len(), 1);
    assert!(registry.has_pending_through(scope, first_cursor)?);
    assert!(
        registry
            .mark_impacted(scope, second_cursor, &change, TimestampMicros::new(1))?
            .is_empty()
    );
    let follow_up = registry
        .complete_success(&first[0], outcome(table, document, "after-first"))?
        .ok_or("coalesced rerun was not issued")?;
    assert!(follow_up.generation > first[0].generation);
    assert_eq!(
        registry.complete_success(&first[0], outcome(table, document, "stale")),
        Err(RealtimeError::StaleTicket)
    );
    assert!(
        registry
            .complete_success(&follow_up, outcome(table, document, "final"))?
            .is_none()
    );
    assert!(!registry.has_pending_through(scope, second_cursor)?);
    assert!(
        registry
            .mark_impacted(scope, second_cursor, &change, TimestampMicros::new(2))?
            .is_empty()
    );

    let reconnected = registry.subscribe(id)?;
    assert_eq!(reconnected.snapshot.delivery_revision, 3);
    assert_eq!(reconnected.snapshot.processed_through, Some(second_cursor));
    assert_eq!(
        reconnected.snapshot.value,
        CanonicalValue::String("final".to_owned())
    );
    let telemetry = registry.telemetry();
    assert_eq!(telemetry.matches, 2);
    assert_eq!(telemetry.coalesced, 1);
    assert_eq!(telemetry.reruns_started, 2);
    assert_eq!(telemetry.reruns_succeeded, 2);
    Ok(())
}

#[tokio::test]
async fn failure_backoff_budget_and_explicit_resume_preserve_valid_state()
-> Result<(), Box<dyn Error>> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let registry = SubscriptionRegistry::new(config())?;
    let descriptor = spec(scope).await?;
    let id = descriptor.id;
    let mut handle = registry.register(descriptor, outcome(table, document, "stable"))?;
    let _initial = handle.receiver.recv().await?;
    let cursor = OutboxCursor {
        commit_sequence: 1,
        event_id: OutboxEventId::generate(),
    };
    let ticket = registry
        .mark_impacted(
            scope,
            cursor,
            &impact(table, document)?,
            TimestampMicros::new(100),
        )?
        .remove(0);
    registry.complete_failure(
        &ticket,
        "STORAGE_UNAVAILABLE",
        true,
        TimestampMicros::new(100),
    )?;
    assert!(
        registry
            .ready_retries(TimestampMicros::new(109))?
            .is_empty()
    );
    let retry = registry.ready_retries(TimestampMicros::new(110))?.remove(0);
    registry.complete_failure(
        &retry,
        "STORAGE_UNAVAILABLE",
        true,
        TimestampMicros::new(110),
    )?;
    let suspended = registry.subscribe(id)?.snapshot;
    assert!(suspended.suspended);
    assert!(!registry.has_pending_through(scope, cursor)?);
    assert_eq!(suspended.value, CanonicalValue::String("stable".to_owned()));
    assert!(
        registry
            .ready_retries(TimestampMicros::new(1_000))?
            .is_empty()
    );
    registry.resume(id)?;
    let resumed = registry
        .ready_retries(TimestampMicros::new(1_000))?
        .remove(0);
    registry.complete_success(&resumed, outcome(table, document, "recovered"))?;
    assert_eq!(
        registry.subscribe(id)?.snapshot.value,
        CanonicalValue::String("recovered".to_owned())
    );
    assert_eq!(registry.telemetry().suspensions, 1);
    Ok(())
}

#[tokio::test]
async fn invalid_limits_duplicates_capacity_and_oversized_outcomes_fail_closed()
-> Result<(), Box<dyn Error>> {
    let mut invalid = config();
    invalid.delivery_buffer = 0;
    assert!(matches!(
        SubscriptionRegistry::new(invalid),
        Err(RealtimeError::InvalidConfiguration)
    ));
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let mut bounded = config();
    bounded.max_subscriptions = 1;
    let registry = SubscriptionRegistry::new(bounded)?;
    let first = spec(scope).await?;
    registry.register(first.clone(), outcome(table, document, "ok"))?;
    assert!(matches!(
        registry.register(first, outcome(table, document, "ok")),
        Err(RealtimeError::AlreadyExists)
    ));
    assert!(matches!(
        registry.register(spec(scope).await?, outcome(table, document, "ok")),
        Err(RealtimeError::LimitExceeded)
    ));
    let mut tiny = config();
    tiny.max_result_bytes = 1;
    let tiny_registry = SubscriptionRegistry::new(tiny)?;
    assert!(matches!(
        tiny_registry.register(spec(scope).await?, outcome(table, document, "too-large")),
        Err(RealtimeError::LimitExceeded)
    ));
    Ok(())
}
