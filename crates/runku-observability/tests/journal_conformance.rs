//! Optional NATS `JetStream` to S3-compatible Parquet acceptance.

use std::{error::Error, str::FromStr, sync::Arc, time::Duration};

use runku_core::{
    EnvironmentId, EnvironmentScope, FunctionId, FunctionName, InvocationId, OperationalEventId,
    ProjectId, ReleaseId, RequestId,
};
use runku_observability::{
    JournalArchiveOutcome, JournalForwardOutcome, LogArchive, LogArchiveCredentials, LogCursor,
    LogEventKind, LogJournalArchiver, LogJournalForwarder, LogLevel, LogPrincipalKind, LogQuery,
    LogRepository, LogRepositoryConfig, LogStream, NatsLogJournal, NatsLogJournalConfig,
    OperationalEventV1, S3LogArchiveConfig, SqlLogRepository, TieredLogRepository,
};
use runku_releases::FunctionType;
use runku_value::TimestampMicros;
use tempfile::tempdir;
use ulid::Ulid;

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
async fn puback_source_replay_batch_archive_and_tiered_query() -> TestResult {
    let Ok(nats_url) = std::env::var("RUNKU_TEST_NATS_URL") else {
        return Ok(());
    };
    let endpoint = std::env::var("RUNKU_TEST_S3_ENDPOINT")?;
    let bucket = std::env::var("RUNKU_TEST_S3_BUCKET")?;
    let access_key = std::env::var("RUNKU_TEST_S3_ACCESS_KEY")?;
    let secret_key = std::env::var("RUNKU_TEST_S3_SECRET_KEY")?;

    let client = async_nats::connect(&nats_url).await?;
    let config = NatsLogJournalConfig {
        replicas: 1,
        ..NatsLogJournalConfig::default()
    };
    let journal = NatsLogJournal::open(client, config).await?;
    let mut archive_config = S3LogArchiveConfig::new(bucket, "us-east-1");
    archive_config.endpoint = Some(endpoint);
    archive_config.prefix = "operational-log-conformance".to_owned();
    archive_config.allow_http = true;
    archive_config.credentials = LogArchiveCredentials::Static(
        runku_observability::LogArchiveStaticCredentials::new(access_key, secret_key),
    );
    let archive = LogArchive::open_s3(&archive_config)?;

    let directory = tempdir()?;
    let database = directory.path().join("journal-hot.sqlite3");
    let hot: Arc<dyn LogRepository> = Arc::new(
        SqlLogRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            LogRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let environment = EnvironmentScope::new(
        ProjectId::from_ulid(Ulid::from(9_000_u128)),
        EnvironmentId::from_ulid(Ulid::from(9_001_u128)),
    );
    hot.append(&[event(environment, 9_100)?, event(environment, 9_101)?])
        .await?;
    let mut forwarder =
        LogJournalForwarder::new(Arc::clone(&hot), journal.clone(), environment, 100)?;
    assert_eq!(
        forwarder.run_once().await?,
        JournalForwardOutcome::Forwarded {
            records: 2,
            through: LogCursor::new(2),
        }
    );
    let mut restarted_forwarder =
        LogJournalForwarder::new(Arc::clone(&hot), journal.clone(), environment, 100)?;
    assert!(matches!(
        restarted_forwarder.run_once().await?,
        JournalForwardOutcome::Forwarded { records: 2, .. }
    ));

    if std::env::var("RUNKU_TEST_EXTERNAL_LOG_WORKER").as_deref() == Ok("true") {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if archive.status(environment).await?.records == 2 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("external log worker did not commit the expected archive".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    } else {
        let worker = LogJournalArchiver::new(journal.clone(), archive.clone());
        assert!(matches!(
            worker.run_once(Duration::from_secs(5)).await?,
            JournalArchiveOutcome::Processed {
                archived_records: 2,
                replayed_records: 0,
                segments: 1,
            }
        ));
    }
    let page = hot.query(&query(environment)).await?;
    let tiered = TieredLogRepository::new(Arc::clone(&hot), archive.clone());
    assert_eq!(
        tiered.query(&query(environment)).await?.records,
        page.records
    );
    assert_eq!(archive.status(environment).await?.records, 2);
    let mut archive_resumed = LogJournalForwarder::resume_after(
        Arc::clone(&hot),
        journal,
        environment,
        100,
        LogCursor::new(2),
    )?;
    assert_eq!(
        archive_resumed.run_once().await?,
        JournalForwardOutcome::Idle {
            through: LogCursor::new(2),
        }
    );
    hot.close().await;
    Ok(())
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

fn event(scope: EnvironmentScope, seed: u128) -> Result<OperationalEventV1, Box<dyn Error>> {
    Ok(OperationalEventV1 {
        id: OperationalEventId::from_ulid(Ulid::from(seed)),
        occurred_at: TimestampMicros::new(1_800_000_000_000_000 + i64::try_from(seed)?),
        scope,
        request_id: RequestId::from_ulid(Ulid::from(seed + 1)),
        invocation_id: InvocationId::from_ulid(Ulid::from(seed + 2)),
        parent_invocation_id: None,
        release_id: ReleaseId::from_ulid(Ulid::from(seed + 3)),
        dev_revision_id: None,
        function_id: FunctionId::from_ulid(Ulid::from(seed + 4)),
        function_name: FunctionName::from_str("orders.query")?,
        function_type: FunctionType::Query,
        client_id: None,
        credential_id: None,
        principal_kind: LogPrincipalKind::None,
        stream: LogStream::Platform,
        level: LogLevel::Info,
        kind: LogEventKind::InvocationStarted,
        message: None,
        fields: None,
        duration_micros: None,
        outcome_code: None,
    })
}
