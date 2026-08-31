//! Durable Operational Logs repository conformance across `SQLite` and `PostgreSQL`.

use std::{collections::BTreeMap, error::Error, str::FromStr, sync::Arc};

use runku_core::{
    ApplicationClientId, CredentialId, EnvironmentId, EnvironmentScope, FunctionId, FunctionName,
    InvocationId, OperationalEventId, ProjectId, ReleaseId, RequestId,
};
use runku_observability::{
    LogCursor, LogEventKind, LogLevel, LogMessage, LogPrincipalKind, LogQuery, LogRepository,
    LogRepositoryConfig, LogRepositoryError, LogStream, OperationalEventV1, OutcomeCode,
    SqlLogRepository, sanitize_function_fields,
};
use runku_releases::FunctionType;
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::tempdir;
use ulid::Ulid;

type TestResult = Result<(), Box<dyn Error>>;

fn scope(seed: u128) -> EnvironmentScope {
    EnvironmentScope::new(
        ProjectId::from_ulid(Ulid::from(seed)),
        EnvironmentId::from_ulid(Ulid::from(seed + 1)),
    )
}

fn event(
    scope: EnvironmentScope,
    sequence: u128,
    kind: LogEventKind,
) -> Result<OperationalEventV1, Box<dyn Error>> {
    let function_message = kind == LogEventKind::FunctionMessage;
    let completed = kind == LogEventKind::InvocationCompleted;
    Ok(OperationalEventV1 {
        id: OperationalEventId::from_ulid(Ulid::from(sequence)),
        occurred_at: TimestampMicros::new(1_800_000_000_000_000 + i64::try_from(sequence)?),
        scope,
        request_id: RequestId::from_ulid(Ulid::from(10)),
        invocation_id: InvocationId::from_ulid(Ulid::from(11)),
        parent_invocation_id: None,
        release_id: ReleaseId::from_ulid(Ulid::from(12)),
        dev_revision_id: None,
        function_id: FunctionId::from_ulid(Ulid::from(13)),
        function_name: FunctionName::from_str("orders.process")?,
        function_type: FunctionType::Action,
        client_id: Some(ApplicationClientId::from_ulid(Ulid::from(14))),
        credential_id: Some(CredentialId::from_ulid(Ulid::from(15))),
        principal_kind: LogPrincipalKind::Service,
        stream: if function_message {
            LogStream::Function
        } else {
            LogStream::Platform
        },
        level: LogLevel::Info,
        kind,
        message: function_message
            .then(|| LogMessage::new("order accepted".to_owned()))
            .transpose()?,
        fields: function_message
            .then(|| {
                sanitize_function_fields(CanonicalValue::Object(BTreeMap::from([
                    (
                        "orderId".to_owned(),
                        CanonicalValue::String("ord_123".to_owned()),
                    ),
                    (
                        "accessToken".to_owned(),
                        CanonicalValue::String("must-not-survive".to_owned()),
                    ),
                ])))
            })
            .transpose()?,
        duration_micros: completed.then_some(73),
        outcome_code: completed
            .then(|| OutcomeCode::new("OK".to_owned()))
            .transpose()?,
    })
}

fn query(scope: EnvironmentScope) -> LogQuery {
    LogQuery {
        scope,
        after: LogCursor::START,
        limit: 100,
        stream: None,
        minimum_level: None,
        function_id: None,
        request_id: None,
        invocation_id: None,
        client_id: None,
        credential_id: None,
        release_id: None,
    }
}

async fn conformance(repository: Arc<SqlLogRepository>) -> TestResult {
    let first_scope = scope(100);
    let other_scope = scope(200);
    let events = vec![
        event(first_scope, 1_001, LogEventKind::InvocationStarted)?,
        event(first_scope, 1_002, LogEventKind::FunctionMessage)?,
        event(first_scope, 1_003, LogEventKind::InvocationCompleted)?,
    ];
    assert_eq!(repository.append(&events).await?, LogCursor::new(3));
    assert_eq!(repository.append(&events).await?, LogCursor::new(3));

    let page = repository.query(&query(first_scope)).await?;
    assert_eq!(page.records.len(), 3);
    assert_eq!(page.next, LogCursor::new(3));
    assert!(
        page.records
            .windows(2)
            .all(|pair| pair[0].cursor < pair[1].cursor)
    );
    let CanonicalValue::Object(fields) = page.records[1]
        .event
        .fields
        .as_ref()
        .ok_or("missing fields")?
    else {
        return Err("fields are not an object".into());
    };
    assert_eq!(
        fields["accessToken"],
        CanonicalValue::String("[REDACTED]".to_owned())
    );

    let function_page = repository
        .query(&LogQuery {
            stream: Some(LogStream::Function),
            credential_id: events[1].credential_id,
            ..query(first_scope)
        })
        .await?;
    assert_eq!(function_page.records.len(), 1);
    assert_eq!(function_page.records[0].event.id, events[1].id);
    let empty = repository
        .query(&LogQuery {
            after: function_page.next,
            stream: Some(LogStream::Function),
            ..query(first_scope)
        })
        .await?;
    assert!(empty.records.is_empty());
    assert_eq!(empty.next, function_page.next);

    let other = event(other_scope, 2_001, LogEventKind::InvocationStarted)?;
    assert_eq!(repository.append(&[other]).await?, LogCursor::new(1));
    assert_eq!(
        repository.query(&query(other_scope)).await?.records.len(),
        1
    );
    assert_eq!(
        repository.query(&query(first_scope)).await?.records.len(),
        3
    );

    let mut conflicting = events[0].clone();
    conflicting.level = LogLevel::Error;
    assert_eq!(
        repository.append(&[conflicting]).await,
        Err(LogRepositoryError::InvalidRequest)
    );
    assert_eq!(
        repository.query(&query(first_scope)).await?.records.len(),
        3
    );

    let cutoff = TimestampMicros::new(events[2].occurred_at.get());
    let dry = repository
        .prune_before(first_scope, cutoff, 1, true)
        .await?;
    assert_eq!((dry.matched, dry.deleted, dry.more), (1, 0, true));
    let first = repository
        .prune_before(first_scope, cutoff, 1, false)
        .await?;
    assert_eq!((first.matched, first.deleted, first.more), (1, 1, true));
    let second = repository
        .prune_before(first_scope, cutoff, 10, false)
        .await?;
    assert_eq!((second.matched, second.deleted, second.more), (1, 1, false));
    assert_eq!(repository.telemetry().pruned, 2);
    assert_eq!(
        repository.query(&query(first_scope)).await?.records.len(),
        1
    );
    assert_eq!(
        repository.query(&query(other_scope)).await?.records.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_replay_filters_retention_isolation_and_reopen() -> TestResult {
    let directory = tempdir()?;
    let path = directory.path().join("observability.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let repository =
        Arc::new(SqlLogRepository::connect_sqlite(&url, LogRepositoryConfig::LOCAL).await?);
    conformance(Arc::clone(&repository)).await?;
    repository.close().await;

    let reopened = SqlLogRepository::connect_sqlite(&url, LogRepositoryConfig::LOCAL).await?;
    assert_eq!(reopened.query(&query(scope(100))).await?.records.len(), 1);
    assert_eq!(reopened.query(&query(scope(200))).await?.records.len(), 1);
    reopened.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_concurrent_append_assigns_one_cursor_per_event() -> TestResult {
    let directory = tempdir()?;
    let path = directory.path().join("concurrent.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let repository =
        Arc::new(SqlLogRepository::connect_sqlite(&url, LogRepositoryConfig::LOCAL).await?);
    let environment = scope(300);
    let mut tasks = Vec::new();
    for index in 0..32_u128 {
        let repository = Arc::clone(&repository);
        let record = event(environment, 3_000 + index, LogEventKind::InvocationStarted)?;
        tasks.push(tokio::spawn(
            async move { repository.append(&[record]).await },
        ));
    }
    for task in tasks {
        task.await??;
    }
    let page = repository.query(&query(environment)).await?;
    assert_eq!(page.records.len(), 32);
    assert_eq!(page.next, LogCursor::new(32));
    concurrent_retention(Arc::clone(&repository), environment, 22).await?;
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn sqlite_committed_records_survive_ungraceful_pool_drop() -> TestResult {
    let directory = tempdir()?;
    let path = directory.path().join("crash-reopen.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let environment = scope(400);
    {
        let repository = SqlLogRepository::connect_sqlite(&url, LogRepositoryConfig::LOCAL).await?;
        repository
            .append(&[event(environment, 4_001, LogEventKind::InvocationStarted)?])
            .await?;
    }
    let reopened = SqlLogRepository::connect_sqlite(&url, LogRepositoryConfig::LOCAL).await?;
    let page = reopened.query(&query(environment)).await?;
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.next, LogCursor::new(1));
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn postgres_replay_filters_retention_isolation() -> TestResult {
    let Ok(url) = std::env::var("RUNKU_TEST_POSTGRES_URL") else {
        return Ok(());
    };
    let repository =
        Arc::new(SqlLogRepository::connect_postgres(&url, LogRepositoryConfig::PRODUCTION).await?);
    sqlx_cleanup(&url).await?;
    conformance(Arc::clone(&repository)).await?;
    concurrent_append(Arc::clone(&repository), scope(500), 5_000).await?;
    concurrent_retention(Arc::clone(&repository), scope(500), 22).await?;
    repository.close().await;
    Ok(())
}

async fn concurrent_append(
    repository: Arc<SqlLogRepository>,
    environment: EnvironmentScope,
    seed: u128,
) -> TestResult {
    let mut tasks = Vec::new();
    for index in 0..32_u128 {
        let repository = Arc::clone(&repository);
        let record = event(environment, seed + index, LogEventKind::InvocationStarted)?;
        tasks.push(tokio::spawn(
            async move { repository.append(&[record]).await },
        ));
    }
    for task in tasks {
        task.await??;
    }
    let page = repository.query(&query(environment)).await?;
    assert_eq!(page.records.len(), 32);
    assert_eq!(page.next, LogCursor::new(32));
    Ok(())
}

async fn concurrent_retention(
    repository: Arc<SqlLogRepository>,
    environment: EnvironmentScope,
    expected_remaining: usize,
) -> TestResult {
    let cutoff = TimestampMicros::new(i64::MAX);
    let first = {
        let repository = Arc::clone(&repository);
        tokio::spawn(async move { repository.prune_before(environment, cutoff, 5, false).await })
    };
    let second = {
        let repository = Arc::clone(&repository);
        tokio::spawn(async move { repository.prune_before(environment, cutoff, 5, false).await })
    };
    let first = first.await??;
    let second = second.await??;
    assert_eq!(first.deleted + second.deleted, 10);
    assert_eq!(
        repository.query(&query(environment)).await?.records.len(),
        expected_remaining
    );
    Ok(())
}

async fn sqlx_cleanup(url: &str) -> TestResult {
    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(url).await?;
    sqlx::query("DELETE FROM runku_operational_logs")
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM runku_log_sequences")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}
