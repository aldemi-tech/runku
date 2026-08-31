//! Shared Development Access persistence and lifecycle conformance.

use std::error::Error;

use runku_core::{DevelopmentCredentialId, EnvironmentId, EnvironmentScope, ProjectId};
use runku_development::DevelopmentActor;
use runku_development_access::{
    DevelopmentAccessBackend, DevelopmentAccessError, DevelopmentAccessRepository,
    DevelopmentAccessRepositoryConfig, DevelopmentAccessResolver, DevelopmentCredential,
    DevelopmentCredentialStatus, DevelopmentKeyCrypto, DevelopmentLifecycleResult,
    GeneratedDevelopmentKey, ParsedDevelopmentKey, SqlDevelopmentAccessRepository,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;

#[tokio::test]
async fn sqlite_conformance_reopens_and_detects_migration_drift() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("development-access.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    assert!(matches!(
        SqlDevelopmentAccessRepository::connect_sqlite(
            &url,
            DevelopmentAccessRepositoryConfig::AUTHORITATIVE,
        )
        .await,
        Err(DevelopmentAccessError::Unsupported)
    ));
    let repository = SqlDevelopmentAccessRepository::connect_sqlite(
        &url,
        DevelopmentAccessRepositoryConfig::LOCAL,
    )
    .await?;
    let scope = run_conformance(&repository, DevelopmentAccessBackend::SQLite).await?;
    repository.close().await;

    let reopened = SqlDevelopmentAccessRepository::connect_sqlite(
        &url,
        DevelopmentAccessRepositoryConfig::LOCAL,
    )
    .await?;
    assert_eq!(reopened.configuration_revision(scope).await?, 6);
    assert_eq!(reopened.list_credentials(scope).await?.len(), 3);
    reopened.close().await;

    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(&url).await?;
    sqlx::query(
        "UPDATE runku_development_access_migrations SET checksum = 'tampered' WHERE version = 1",
    )
    .execute(&pool)
    .await?;
    pool.close().await;
    assert!(matches!(
        SqlDevelopmentAccessRepository::connect_sqlite(
            &url,
            DevelopmentAccessRepositoryConfig::LOCAL,
        )
        .await,
        Err(DevelopmentAccessError::Corruption)
    ));
    Ok(())
}

#[tokio::test]
async fn postgres_conformance() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let repository = SqlDevelopmentAccessRepository::connect_postgres(
        &url,
        DevelopmentAccessRepositoryConfig::AUTHORITATIVE,
    )
    .await?;
    run_conformance(&repository, DevelopmentAccessBackend::PostgreSQL).await?;
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn sqlite_concurrent_exact_create_is_one_mutation() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("concurrent.sqlite3").display()
    );
    let repository = SqlDevelopmentAccessRepository::connect_sqlite(
        &url,
        DevelopmentAccessRepositoryConfig::LOCAL,
    )
    .await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let crypto = DevelopmentKeyCrypto::new([23; 32]);
    let generated = crypto.generate(DevelopmentCredentialId::generate())?;
    let credential = active_credential(scope, "parallel", "parallel", &generated, 10, None)?;
    let first = repository.create_credential(&credential);
    let second = repository.create_credential(&credential);
    let (first, second) = tokio::join!(first, second);
    let mut results = [first?, second?];
    results.sort_unstable();
    assert_eq!(results, [false, true]);
    assert_eq!(repository.configuration_revision(scope).await?, 1);
    assert_eq!(repository.list_credentials(scope).await?.len(), 1);
    repository.close().await;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_conformance(
    repository: &SqlDevelopmentAccessRepository,
    backend: DevelopmentAccessBackend,
) -> Result<EnvironmentScope, Box<dyn Error>> {
    assert_eq!(repository.backend(), backend);
    repository.health().await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let other_scope = EnvironmentScope::new(scope.project_id(), EnvironmentId::generate());
    assert_eq!(repository.configuration_revision(scope).await?, 0);
    let crypto = DevelopmentKeyCrypto::new([19; 32]);

    let first_key = crypto.generate(DevelopmentCredentialId::generate())?;
    let first = active_credential(scope, "manuel", "cli-manuel", &first_key, 10, None)?;
    assert!(repository.create_credential(&first).await?);
    assert!(!repository.create_credential(&first).await?);
    let mut divergent = first.clone();
    divergent.actor = "attacker".parse()?;
    assert_eq!(
        repository.create_credential(&divergent).await,
        Err(DevelopmentAccessError::Conflict)
    );

    let second_key = crypto.generate(DevelopmentCredentialId::generate())?;
    let second = active_credential(scope, "ci.release", "ci-release", &second_key, 11, None)?;
    assert!(repository.create_credential(&second).await?);
    let expired_key = crypto.generate(DevelopmentCredentialId::generate())?;
    let expired = active_credential(scope, "old.agent", "expired", &expired_key, 12, Some(20))?;
    assert!(repository.create_credential(&expired).await?);
    assert_eq!(repository.configuration_revision(scope).await?, 3);

    let listed = repository.list_credentials(scope).await?;
    assert_eq!(listed.len(), 3);
    assert!(listed.windows(2).all(|pair| pair[0].id < pair[1].id));
    assert_eq!(
        repository.get_credential(scope, first.id).await?,
        Some(first.clone())
    );

    let parsed_first: ParsedDevelopmentKey = first_key.key.expose().parse()?;
    let identity = repository
        .resolve_key(scope, &parsed_first, &crypto, TimestampMicros::new(19))
        .await?;
    assert_eq!(identity.scope, scope);
    assert_eq!(identity.actor.as_str(), "manuel");
    assert_eq!(identity.credential_id, first.id);
    assert_eq!(identity.configuration_revision, 3);
    assert_eq!(
        repository
            .resolve_key(
                other_scope,
                &parsed_first,
                &crypto,
                TimestampMicros::new(19)
            )
            .await,
        Err(DevelopmentAccessError::InvalidCredential)
    );
    assert_eq!(
        repository
            .resolve_key(
                scope,
                &parsed_first,
                &DevelopmentKeyCrypto::new([20; 32]),
                TimestampMicros::new(19)
            )
            .await,
        Err(DevelopmentAccessError::InvalidCredential)
    );
    let parsed_expired: ParsedDevelopmentKey = expired_key.key.expose().parse()?;
    assert_eq!(
        repository
            .resolve_key(scope, &parsed_expired, &crypto, TimestampMicros::new(20))
            .await,
        Err(DevelopmentAccessError::InvalidCredential)
    );
    assert_eq!(
        repository
            .resolve_key(scope, &parsed_first, &crypto, TimestampMicros::new(-1))
            .await,
        Err(DevelopmentAccessError::InvalidInput)
    );

    assert_eq!(
        repository
            .delete_credential(scope, first.id, TimestampMicros::new(29))
            .await,
        Err(DevelopmentAccessError::Conflict)
    );
    assert_eq!(
        repository
            .revoke_credential(scope, first.id, TimestampMicros::new(30))
            .await?,
        DevelopmentLifecycleResult::Applied
    );
    assert_eq!(
        repository
            .revoke_credential(scope, first.id, TimestampMicros::new(31))
            .await?,
        DevelopmentLifecycleResult::Replayed
    );
    assert_eq!(
        repository
            .resolve_key(scope, &parsed_first, &crypto, TimestampMicros::new(31))
            .await,
        Err(DevelopmentAccessError::InvalidCredential)
    );
    assert_eq!(
        repository
            .delete_credential(scope, first.id, TimestampMicros::new(32))
            .await?,
        DevelopmentLifecycleResult::Applied
    );
    assert_eq!(
        repository
            .delete_credential(scope, first.id, TimestampMicros::new(33))
            .await?,
        DevelopmentLifecycleResult::Replayed
    );
    assert_eq!(repository.configuration_revision(scope).await?, 5);
    assert_eq!(repository.list_credentials(scope).await?.len(), 2);
    let Some(deleted) = repository.get_credential(scope, first.id).await? else {
        return Err(std::io::Error::other("deleted credential tombstone missing").into());
    };
    assert_eq!(deleted.status, DevelopmentCredentialStatus::Deleted);

    let third_key = crypto.generate(DevelopmentCredentialId::generate())?;
    let third = active_credential(
        scope,
        "mobile.preview",
        "mobile-preview",
        &third_key,
        40,
        None,
    )?;
    assert!(repository.create_credential(&third).await?);
    assert_eq!(repository.configuration_revision(scope).await?, 6);
    let parsed_second: ParsedDevelopmentKey = second_key.key.expose().parse()?;
    assert_eq!(
        repository
            .resolve_key(scope, &parsed_second, &crypto, TimestampMicros::new(41))
            .await?
            .actor
            .as_str(),
        "ci.release"
    );

    assert_eq!(
        repository
            .revoke_credential(
                scope,
                DevelopmentCredentialId::generate(),
                TimestampMicros::new(50)
            )
            .await,
        Err(DevelopmentAccessError::NotFound)
    );
    let telemetry = repository.telemetry();
    assert_eq!(telemetry.credentials_created, 4);
    assert_eq!(telemetry.create_replays, 1);
    assert_eq!(telemetry.credentials_revoked, 1);
    assert_eq!(telemetry.credentials_deleted, 1);
    assert_eq!(telemetry.resolutions, 2);
    assert!(telemetry.resolution_failures >= 4);
    Ok(scope)
}

fn active_credential(
    scope: EnvironmentScope,
    actor: &str,
    label: &str,
    generated: &GeneratedDevelopmentKey,
    created_at: i64,
    expires_at: Option<i64>,
) -> Result<DevelopmentCredential, Box<dyn Error>> {
    Ok(DevelopmentCredential {
        id: generated
            .key
            .expose()
            .parse::<ParsedDevelopmentKey>()?
            .credential_id(),
        scope,
        actor: actor.parse::<DevelopmentActor>()?,
        label: label.parse()?,
        digest: generated.digest,
        status: DevelopmentCredentialStatus::Active,
        created_at: TimestampMicros::new(created_at),
        expires_at: expires_at.map(TimestampMicros::new),
        revoked_at: None,
        deleted_at: None,
    })
}
