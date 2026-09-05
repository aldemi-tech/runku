//! Opt-in PostgreSQL conformance for append-only managed grant reconciliation.

use std::{collections::BTreeSet, str::FromStr as _, sync::Arc};

use runku_core::ProjectId;
use runku_platform_identity::{
    AccessScope, DeviceName, ExternalOperatorIdentity, ManagedSourceAuthority, OperatorGrant,
    OperatorName, PlatformCapability, PlatformIdentityCrypto, PlatformIdentityError,
    PlatformIdentityRepository, PlatformIdentityRepositoryConfig, PlatformIdentityService,
    SessionTokenPolicy, SqlPlatformIdentityRepository,
};
use runku_value::TimestampMicros;

fn test_url() -> Option<String> {
    std::env::var("RUNKU_TEST_POSTGRES_URL").ok()
}

#[tokio::test]
async fn postgres_managed_grants_are_monotonic_atomic_and_source_owned()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_url) = test_url() else {
        return Ok(());
    };
    let database_name = format!(
        "runkuidentity{}",
        runku_core::OperatorId::generate()
            .to_string()
            .replace('_', "")
            .to_ascii_lowercase()
    );
    let admin = sqlx::PgPool::connect(&admin_url).await?;
    let create_database = format!("CREATE DATABASE {database_name}");
    sqlx::query(sqlx::AssertSqlSafe(create_database))
        .execute(&admin)
        .await?;
    let mut database_url = url::Url::parse(&admin_url)?;
    database_url.set_path(&format!("/{database_name}"));

    let result = exercise_postgres(database_url.as_str()).await;
    let drop_database = format!("DROP DATABASE {database_name} WITH (FORCE)");
    sqlx::query(sqlx::AssertSqlSafe(drop_database))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

#[allow(clippy::too_many_lines)]
async fn exercise_postgres(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_postgres(
            url,
            PlatformIdentityRepositoryConfig::AUTHORITATIVE,
        )
        .await?,
    );
    let service = Arc::new(PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([29; 32])),
        SessionTokenPolicy::DEFAULT,
    )?);
    let authority = ManagedSourceAuthority::from_str("https://cloud.runku.example")?;
    let project = ProjectId::generate();
    let initial = service
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "cloud".to_owned(),
                subject_id: "postgres-managed".to_owned(),
            },
            OperatorName::from_str("Postgres managed")?,
            authority.clone(),
            1,
            vec![grant(project, PlatformCapability::ReleasesRead)],
            DeviceName::from_str("postgres")?,
            TimestampMicros::new(1_920_000_000_000_000),
        )
        .await?;
    let operator_id = initial.login.context.operator.id;

    let first_service = service.clone();
    let first_authority = authority.clone();
    let first = tokio::spawn(async move {
        first_service
            .reconcile_managed_grants(
                operator_id,
                first_authority,
                2,
                vec![grant(project, PlatformCapability::DataRead)],
                TimestampMicros::new(1_920_000_000_000_001),
            )
            .await
    });
    let second_service = service.clone();
    let second_authority = authority.clone();
    let second = tokio::spawn(async move {
        second_service
            .reconcile_managed_grants(
                operator_id,
                second_authority,
                2,
                vec![grant(project, PlatformCapability::DataWrite)],
                TimestampMicros::new(1_920_000_000_000_002),
            )
            .await
    });
    let first = first.await?;
    let second = second.await?;
    assert!(matches!(
        (&first, &second),
        (Ok(_), Err(PlatformIdentityError::ManagedSourceConflict))
            | (Err(PlatformIdentityError::ManagedSourceConflict), Ok(_))
    ));
    let winning_capability = if first.is_ok() {
        PlatformCapability::DataRead
    } else {
        PlatformCapability::DataWrite
    };
    assert!(matches!(
        service
            .reconcile_managed_grants(
                operator_id,
                authority.clone(),
                1,
                vec![grant(project, PlatformCapability::ReleasesRead)],
                TimestampMicros::new(1_920_000_000_000_003),
            )
            .await,
        Err(PlatformIdentityError::ManagedSourceStale)
    ));
    let maximum = service
        .reconcile_managed_grants(
            operator_id,
            ManagedSourceAuthority::from_str("https://maximum.runku.example")?,
            u64::MAX,
            vec![grant(project, PlatformCapability::CredentialsRead)],
            TimestampMicros::new(1_920_000_000_000_004),
        )
        .await?;
    assert!(maximum.applied);
    let maximum_replay = service
        .reconcile_managed_grants(
            operator_id,
            ManagedSourceAuthority::from_str("https://maximum.runku.example")?,
            u64::MAX,
            vec![grant(project, PlatformCapability::CredentialsRead)],
            TimestampMicros::new(1_920_000_000_000_005),
        )
        .await?;
    assert!(maximum_replay.replayed);
    let with_both_sources = service
        .authenticate(
            &initial.login.access_token,
            TimestampMicros::new(1_920_000_000_000_006),
        )
        .await?;
    with_both_sources.authorize(AccessScope::Project(project), winning_capability)?;
    with_both_sources.authorize(
        AccessScope::Project(project),
        PlatformCapability::CredentialsRead,
    )?;

    service
        .reconcile_managed_grants(
            operator_id,
            authority,
            3,
            Vec::new(),
            TimestampMicros::new(1_920_000_000_000_007),
        )
        .await?;
    let source_isolated = service
        .authenticate(
            &initial.login.access_token,
            TimestampMicros::new(1_920_000_000_000_008),
        )
        .await?;
    assert_eq!(
        source_isolated.authorize(AccessScope::Project(project), winning_capability),
        Err(PlatformIdentityError::Forbidden)
    );
    source_isolated.authorize(
        AccessScope::Project(project),
        PlatformCapability::CredentialsRead,
    )?;
    repository.close().await;
    Ok(())
}

fn grant(project: ProjectId, capability: PlatformCapability) -> OperatorGrant {
    OperatorGrant {
        scope: AccessScope::Project(project),
        capabilities: BTreeSet::from([capability]),
    }
}
