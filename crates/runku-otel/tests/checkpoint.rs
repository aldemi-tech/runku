//! Durable OTLP checkpoint conformance across `SQLite` and `PostgreSQL`.

use std::{collections::BTreeMap, error::Error, str::FromStr as _, sync::Arc};

use runku_core::{EnvironmentId, EnvironmentScope, ProjectId};
use runku_observability::LogCursor;
use runku_otel::{
    CheckpointAdvance, CheckpointError, ExportCheckpointRepository, OtlpDestinationDigest,
    OtlpEndpoint, OtlpExporterName, OtlpHeaders, OtlpRepositoryConfig,
    SqlExportCheckpointRepository,
};
use tempfile::tempdir;
use ulid::Ulid;

type TestResult = Result<(), Box<dyn Error>>;

fn scope(seed: u128) -> EnvironmentScope {
    EnvironmentScope::new(
        ProjectId::from_ulid(Ulid::from(seed)),
        EnvironmentId::from_ulid(Ulid::from(seed + 1)),
    )
}

fn destination(port: u16, header_name: &str) -> Result<OtlpDestinationDigest, Box<dyn Error>> {
    let endpoint = OtlpEndpoint::from_str(&format!("http://127.0.0.1:{port}/v1/logs"))?;
    let headers = OtlpHeaders::new(BTreeMap::from([(
        header_name.to_owned(),
        "secret-never-persisted".to_owned(),
    )]))?;
    Ok(OtlpDestinationDigest::new(&endpoint, &headers))
}

async fn conformance(repository: Arc<SqlExportCheckpointRepository>) -> TestResult {
    let environment = scope(21_000);
    let other_environment = scope(22_000);
    let name = OtlpExporterName::from_str("primary")?;
    let digest = destination(4_318, "authorization")?;

    let initial = repository
        .load_or_create(environment, &name, digest)
        .await?;
    assert_eq!(initial.cursor, LogCursor::START);
    assert_eq!(initial.revision, 0);
    assert_eq!(
        repository.advance(&initial, LogCursor::new(10)).await?,
        CheckpointAdvance::Advanced
    );
    assert_eq!(
        repository.advance(&initial, LogCursor::new(10)).await?,
        CheckpointAdvance::Replayed
    );
    assert_eq!(
        repository.advance(&initial, LogCursor::new(11)).await,
        Err(CheckpointError::Conflict)
    );

    let current = repository
        .load_or_create(environment, &name, digest)
        .await?;
    assert_eq!(current.cursor, LogCursor::new(10));
    assert_eq!(current.revision, 1);
    assert_eq!(
        repository.advance(&current, LogCursor::new(9)).await,
        Err(CheckpointError::InvalidRequest)
    );
    assert_eq!(
        repository
            .load_or_create(environment, &name, destination(4_319, "authorization")?)
            .await,
        Err(CheckpointError::ConfigurationDrift)
    );

    let isolated = repository
        .load_or_create(other_environment, &name, digest)
        .await?;
    assert_eq!(isolated.cursor, LogCursor::START);
    Ok(())
}

#[test]
fn names_and_destination_digest_are_canonical_and_secret_free() -> TestResult {
    assert!(OtlpExporterName::from_str("primary_2-prod").is_ok());
    for invalid in ["", "Primary", "2primary", "has.dot", "has space"] {
        assert_eq!(
            OtlpExporterName::from_str(invalid),
            Err(CheckpointError::InvalidRequest)
        );
    }
    let first = destination(4_318, "x-api-key")?;
    let second = destination(4_318, "x-api-key")?;
    let different = destination(4_318, "authorization")?;
    assert_eq!(first, second);
    assert_ne!(first, different);
    assert!(!first.to_string().contains("secret"));
    Ok(())
}

#[tokio::test]
async fn sqlite_checkpoint_is_monotonic_isolated_and_survives_reopen() -> TestResult {
    let directory = tempdir()?;
    let path = directory.path().join("otel.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let repository = Arc::new(
        SqlExportCheckpointRepository::connect_sqlite(&url, OtlpRepositoryConfig::LOCAL).await?,
    );
    conformance(Arc::clone(&repository)).await?;
    repository.close().await;

    let reopened =
        SqlExportCheckpointRepository::connect_sqlite(&url, OtlpRepositoryConfig::LOCAL).await?;
    let checkpoint = reopened
        .load_or_create(
            scope(21_000),
            &OtlpExporterName::from_str("primary")?,
            destination(4_318, "authorization")?,
        )
        .await?;
    assert_eq!(checkpoint.cursor, LogCursor::new(10));
    reopened.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_concurrent_exact_cas_is_idempotent() -> TestResult {
    let directory = tempdir()?;
    let path = directory.path().join("concurrent.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let repository = Arc::new(
        SqlExportCheckpointRepository::connect_sqlite(&url, OtlpRepositoryConfig::LOCAL).await?,
    );
    let initial = repository
        .load_or_create(
            scope(23_000),
            &OtlpExporterName::from_str("concurrent")?,
            destination(4_318, "authorization")?,
        )
        .await?;
    let first = {
        let repository = Arc::clone(&repository);
        let initial = initial.clone();
        tokio::spawn(async move { repository.advance(&initial, LogCursor::new(20)).await })
    };
    let second = {
        let repository = Arc::clone(&repository);
        tokio::spawn(async move { repository.advance(&initial, LogCursor::new(20)).await })
    };
    let mut outcomes = [first.await??, second.await??];
    outcomes.sort_by_key(|outcome| match outcome {
        CheckpointAdvance::Advanced => 0,
        CheckpointAdvance::Replayed => 1,
    });
    assert_eq!(
        outcomes,
        [CheckpointAdvance::Advanced, CheckpointAdvance::Replayed]
    );
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn postgres_checkpoint_conformance() -> TestResult {
    let Ok(url) = std::env::var("RUNKU_TEST_POSTGRES_URL") else {
        return Ok(());
    };
    let repository = Arc::new(
        SqlExportCheckpointRepository::connect_postgres(&url, OtlpRepositoryConfig::PRODUCTION)
            .await?,
    );
    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(&url).await?;
    sqlx::query("DELETE FROM runku_otel_checkpoints")
        .execute(&pool)
        .await?;
    pool.close().await;
    conformance(Arc::clone(&repository)).await?;
    repository.close().await;
    Ok(())
}
