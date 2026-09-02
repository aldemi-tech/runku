//! Filesystem Parquet archive, `DuckDB` query, and tier-boundary conformance.

use std::{collections::BTreeMap, error::Error, str::FromStr, sync::Arc};

use runku_core::{
    ApplicationClientId, CredentialId, EnvironmentId, EnvironmentScope, FunctionId, FunctionName,
    InvocationId, OperationalEventId, ProjectId, ReleaseId, RequestId,
};
use runku_observability::{
    LogArchive, LogArchiveRunOutcome, LogArchiver, LogCursor, LogEventKind, LogLevel, LogMessage,
    LogPrincipalKind, LogQuery, LogRepository, LogRepositoryConfig, LogRepositoryError, LogStream,
    OperationalEventV1, SqlLogRepository, TieredLogRepository, sanitize_function_fields,
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

fn event(environment: EnvironmentScope, seed: u128) -> Result<OperationalEventV1, Box<dyn Error>> {
    Ok(OperationalEventV1 {
        id: OperationalEventId::from_ulid(Ulid::from(seed)),
        occurred_at: TimestampMicros::new(1_800_000_000_000_000 + i64::try_from(seed)?),
        scope: environment,
        request_id: RequestId::from_ulid(Ulid::from(seed + 10)),
        invocation_id: InvocationId::from_ulid(Ulid::from(seed + 11)),
        parent_invocation_id: None,
        release_id: ReleaseId::from_ulid(Ulid::from(seed + 12)),
        dev_revision_id: None,
        function_id: FunctionId::from_ulid(Ulid::from(seed + 13)),
        function_name: FunctionName::from_str("orders.process")?,
        function_type: FunctionType::Action,
        client_id: Some(ApplicationClientId::from_ulid(Ulid::from(seed + 14))),
        credential_id: Some(CredentialId::from_ulid(Ulid::from(seed + 15))),
        principal_kind: LogPrincipalKind::Service,
        stream: LogStream::Function,
        level: LogLevel::Info,
        kind: LogEventKind::FunctionMessage,
        message: Some(LogMessage::new(format!("event {seed}"))?),
        fields: Some(sanitize_function_fields(CanonicalValue::Object(
            BTreeMap::from([(
                "eventNumber".to_owned(),
                CanonicalValue::Int64(i64::try_from(seed)?),
            )]),
        ))?),
        duration_micros: None,
        outcome_code: None,
    })
}

fn query(environment: EnvironmentScope) -> LogQuery {
    LogQuery {
        scope: environment,
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

#[tokio::test]
async fn archives_replays_and_queries_across_hot_and_parquet_tiers() -> TestResult {
    let directory = tempdir()?;
    let database = directory.path().join("hot.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", database.display());
    let hot: Arc<dyn LogRepository> =
        Arc::new(SqlLogRepository::connect_sqlite(&url, LogRepositoryConfig::LOCAL).await?);
    let archive = LogArchive::open_filesystem(directory.path().join("archive")).await?;
    let environment = scope(100);
    let events = vec![
        event(environment, 1_001)?,
        event(environment, 1_002)?,
        event(environment, 1_003)?,
    ];
    hot.append(&events).await?;

    let archiver = LogArchiver::new(Arc::clone(&hot), archive.clone(), environment, 2)?;
    assert_eq!(
        archiver.run_once().await?,
        LogArchiveRunOutcome::Archived {
            records: 2,
            through: LogCursor::new(2),
        }
    );
    let hot_page = hot.query(&query(environment)).await?;
    archive.commit(&hot_page.records[..2]).await?;
    let tiered = TieredLogRepository::new(Arc::clone(&hot), archive.clone());
    let removed = tiered
        .prune_before(environment, TimestampMicros::new(i64::MAX), 100, false)
        .await?;
    assert_eq!(removed.deleted, 2);
    let hot_after_safe_prune = hot.query(&query(environment)).await?;
    assert_eq!(hot_after_safe_prune.records.len(), 1);
    assert_eq!(hot_after_safe_prune.records[0].cursor, LogCursor::new(3));
    assert_eq!(tiered.query(&query(environment)).await?.records.len(), 3);

    assert_eq!(
        archiver.run_once().await?,
        LogArchiveRunOutcome::Archived {
            records: 1,
            through: LogCursor::new(3),
        }
    );
    assert_eq!(archive.status(environment).await?.records, 3);

    let page = tiered.query(&query(environment)).await?;
    assert_eq!(page.records.len(), 3);
    assert_eq!(page.next, LogCursor::new(3));

    let removed = tiered
        .prune_before(environment, TimestampMicros::new(i64::MAX), 100, false)
        .await?;
    assert_eq!(removed.deleted, 1);
    let archived_only = tiered.query(&query(environment)).await?;
    assert_eq!(archived_only.records, page.records);

    let other = scope(200);
    assert!(tiered.query(&query(other)).await?.records.is_empty());
    assert_eq!(archive.status(other).await?.records, 0);
    hot.close().await;
    Ok(())
}

#[tokio::test]
async fn fails_closed_when_committed_parquet_bytes_are_modified() -> TestResult {
    let directory = tempdir()?;
    let database = directory.path().join("hot.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", database.display());
    let hot: Arc<dyn LogRepository> =
        Arc::new(SqlLogRepository::connect_sqlite(&url, LogRepositoryConfig::LOCAL).await?);
    let archive_root = directory.path().join("archive");
    let archive = LogArchive::open_filesystem(archive_root.clone()).await?;
    let environment = scope(400);
    hot.append(&[event(environment, 4_001)?]).await?;
    LogArchiver::new(Arc::clone(&hot), archive.clone(), environment, 10)?
        .run_once()
        .await?;

    let parquet = walk_files(&archive_root)?
        .into_iter()
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .ok_or("parquet not found")?;
    std::fs::write(parquet, b"modified")?;
    assert_eq!(
        TieredLogRepository::new(Arc::clone(&hot), archive)
            .query(&query(environment))
            .await,
        Err(LogRepositoryError::Corruption)
    );
    hot.close().await;
    Ok(())
}

#[tokio::test]
async fn fails_closed_when_a_committed_manifest_is_modified() -> TestResult {
    let directory = tempdir()?;
    let database = directory.path().join("hot.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", database.display());
    let hot: Arc<dyn LogRepository> =
        Arc::new(SqlLogRepository::connect_sqlite(&url, LogRepositoryConfig::LOCAL).await?);
    let archive_root = directory.path().join("archive");
    let archive = LogArchive::open_filesystem(archive_root.clone()).await?;
    let environment = scope(300);
    hot.append(&[event(environment, 3_001)?]).await?;
    LogArchiver::new(Arc::clone(&hot), archive.clone(), environment, 10)?
        .run_once()
        .await?;

    let manifest = walk_files(&archive_root)?
        .into_iter()
        .find(|path| path.to_string_lossy().ends_with(".manifest.json"))
        .ok_or("manifest not found")?;
    std::fs::write(manifest, b"{}")?;
    assert_eq!(
        archive.status(environment).await,
        Err(LogRepositoryError::Corruption)
    );
    let tiered = TieredLogRepository::new(Arc::clone(&hot), archive);
    assert_eq!(
        tiered.query(&query(environment)).await,
        Err(LogRepositoryError::Corruption)
    );
    hot.close().await;
    Ok(())
}

fn walk_files(root: &std::path::Path) -> Result<Vec<std::path::PathBuf>, std::io::Error> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else {
                files.push(entry.path());
            }
        }
    }
    Ok(files)
}
