//! Shared keyring lifecycle and isolation conformance for SQLite/PostgreSQL.

use std::{collections::BTreeSet, error::Error};

use runku_core::{ApplicationClientId, CredentialId, EnvironmentId, EnvironmentScope, ProjectId};
use runku_identity::{
    ApplicationAssurance, ApplicationClient, ApplicationClientStatus, ApplicationCredential,
    ApplicationCredentialResolver, ApplicationIdentityRepository, ApplicationScope, ClientKind,
    CredentialKind, CredentialStatus, IdentityError, IdentityRepositoryBackend, KeyringCrypto,
    ParsedApplicationKey,
};
use runku_identity_repository::{IdentityRepositoryConfig, SqlApplicationIdentityRepository};
use runku_value::TimestampMicros;
use tempfile::tempdir;

#[tokio::test]
async fn sqlite_keyring_conformance_reopens() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("identity.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    assert!(matches!(
        SqlApplicationIdentityRepository::connect_sqlite(
            &url,
            IdentityRepositoryConfig::PRODUCTION
        )
        .await,
        Err(IdentityError::ProductionBackendUnsupported)
    ));
    let repository =
        SqlApplicationIdentityRepository::connect_sqlite(&url, IdentityRepositoryConfig::LOCAL)
            .await?;
    let scope = run_conformance(&repository, IdentityRepositoryBackend::SQLite).await?;
    repository.close().await;

    let reopened =
        SqlApplicationIdentityRepository::connect_sqlite(&url, IdentityRepositoryConfig::LOCAL)
            .await?;
    assert_eq!(reopened.configuration_revision(scope).await?, 8);
    assert_eq!(reopened.list_clients(scope).await?.len(), 2);
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn postgres_keyring_conformance() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let repository = SqlApplicationIdentityRepository::connect_postgres(
        &url,
        IdentityRepositoryConfig::PRODUCTION,
    )
    .await?;
    run_conformance(&repository, IdentityRepositoryBackend::PostgreSQL).await?;
    repository.close().await;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_conformance(
    repository: &SqlApplicationIdentityRepository,
    backend: IdentityRepositoryBackend,
) -> Result<EnvironmentScope, Box<dyn Error>> {
    assert_eq!(repository.backend(), backend);
    repository.health().await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let other_scope = EnvironmentScope::new(scope.project_id(), EnvironmentId::generate());
    let crypto = KeyringCrypto::new([17; 32]);
    let invoke = scope_set(&["function:invoke"])?;
    let subscribe = scope_set(&["function:invoke", "realtime:subscribe"])?;
    let public_id = ApplicationClientId::generate();
    let public = ApplicationClient {
        scope,
        id: public_id,
        name: "web-storefront".parse()?,
        kind: ClientKind::Public,
        status: ApplicationClientStatus::Active,
        scope_ceiling: subscribe.clone(),
        created_at: TimestampMicros::new(10),
    };
    assert!(repository.create_client(&public).await?);
    assert!(!repository.create_client(&public).await?);
    let mut conflict = public.clone();
    conflict.name = "renamed".parse()?;
    assert_eq!(
        repository.create_client(&conflict).await,
        Err(IdentityError::Conflict)
    );

    let public_key_one = crypto.generate_publishable(CredentialId::generate())?;
    let public_one = active_credential(
        scope,
        public_id,
        &public_key_one,
        "browser-rollout-a",
        invoke.clone(),
        20,
        None,
    )?;
    assert!(repository.create_credential(&public_one).await?);
    assert!(!repository.create_credential(&public_one).await?);

    let public_key_two = crypto.generate_publishable(CredentialId::generate())?;
    let public_two = active_credential(
        scope,
        public_id,
        &public_key_two,
        "browser-rollout-b",
        subscribe,
        21,
        None,
    )?;
    assert!(repository.create_credential(&public_two).await?);

    let confidential_id = ApplicationClientId::generate();
    let confidential = ApplicationClient {
        scope,
        id: confidential_id,
        name: "billing-worker".parse()?,
        kind: ClientKind::Confidential,
        status: ApplicationClientStatus::Active,
        scope_ceiling: invoke.clone(),
        created_at: TimestampMicros::new(30),
    };
    assert!(repository.create_client(&confidential).await?);
    let secret_key = crypto.generate_secret(CredentialId::generate())?;
    let secret = active_credential(
        scope,
        confidential_id,
        &secret_key,
        "worker-2026-08",
        invoke.clone(),
        31,
        None,
    )?;
    assert!(repository.create_credential(&secret).await?);

    let expired_key = crypto.generate_secret(CredentialId::generate())?;
    let expired = active_credential(
        scope,
        confidential_id,
        &expired_key,
        "expired",
        invoke.clone(),
        32,
        Some(40),
    )?;
    assert!(repository.create_credential(&expired).await?);
    assert_eq!(repository.configuration_revision(scope).await?, 6);

    let parsed_public: ParsedApplicationKey = public_key_one.key.expose().parse()?;
    let context = repository
        .resolve_key(scope, &parsed_public, &crypto, TimestampMicros::new(35))
        .await?;
    assert_eq!(context.client_id, public_id);
    assert_eq!(context.assurance, ApplicationAssurance::Declared);
    assert_eq!(context.configuration_revision, 6);
    assert_eq!(
        repository
            .resolve_key(
                other_scope,
                &parsed_public,
                &crypto,
                TimestampMicros::new(35)
            )
            .await,
        Err(IdentityError::InvalidCredential)
    );

    let parsed_secret: ParsedApplicationKey = secret_key.key.expose().parse()?;
    let service = repository
        .resolve_key(scope, &parsed_secret, &crypto, TimestampMicros::new(35))
        .await?;
    assert_eq!(service.assurance, ApplicationAssurance::Verified);
    assert_eq!(service.credential_kind, CredentialKind::Secret);
    assert_eq!(
        repository
            .resolve_key(
                scope,
                &parsed_secret,
                &KeyringCrypto::new([18; 32]),
                TimestampMicros::new(35)
            )
            .await,
        Err(IdentityError::InvalidCredential)
    );
    let parsed_expired: ParsedApplicationKey = expired_key.key.expose().parse()?;
    assert_eq!(
        repository
            .resolve_key(scope, &parsed_expired, &crypto, TimestampMicros::new(40))
            .await,
        Err(IdentityError::CredentialInactive)
    );

    let wrong_kind = active_credential(
        scope,
        public_id,
        &crypto.generate_secret(CredentialId::generate())?,
        "wrong-kind",
        invoke.clone(),
        41,
        None,
    )?;
    assert_eq!(
        repository.create_credential(&wrong_kind).await,
        Err(IdentityError::CredentialTypeMismatch)
    );
    let escalated_key = crypto.generate_publishable(CredentialId::generate())?;
    let escalated = active_credential(
        scope,
        public_id,
        &escalated_key,
        "escalated",
        scope_set(&["admin:all"])?,
        42,
        None,
    )?;
    assert_eq!(
        repository.create_credential(&escalated).await,
        Err(IdentityError::ScopeEscalation)
    );

    assert_eq!(
        repository
            .delete_credential(scope, public_one.id, TimestampMicros::new(49))
            .await,
        Err(IdentityError::InvalidTransition)
    );
    assert_eq!(
        repository
            .revoke_credential(scope, public_one.id, TimestampMicros::new(50))
            .await?,
        runku_identity::CredentialLifecycleResult::Changed
    );
    assert_eq!(
        repository
            .revoke_credential(scope, public_one.id, TimestampMicros::new(51))
            .await?,
        runku_identity::CredentialLifecycleResult::Replayed
    );
    assert_eq!(
        repository
            .resolve_key(scope, &parsed_public, &crypto, TimestampMicros::new(51))
            .await,
        Err(IdentityError::CredentialInactive)
    );
    let parsed_public_two: ParsedApplicationKey = public_key_two.key.expose().parse()?;
    assert!(
        repository
            .resolve_key(scope, &parsed_public_two, &crypto, TimestampMicros::new(51))
            .await
            .is_ok()
    );
    assert_eq!(
        repository
            .delete_credential(scope, public_one.id, TimestampMicros::new(52))
            .await?,
        runku_identity::CredentialLifecycleResult::Changed
    );
    assert_eq!(repository.configuration_revision(scope).await?, 8);
    assert_eq!(
        repository.list_credentials(scope, public_id).await?.len(),
        1
    );
    assert_eq!(repository.list_clients(other_scope).await?.len(), 0);

    let telemetry = repository.telemetry();
    assert_eq!(telemetry.clients_created, 2);
    assert_eq!(telemetry.credentials_created, 4);
    assert_eq!(telemetry.create_replays, 2);
    assert_eq!(telemetry.credentials_revoked, 1);
    assert_eq!(telemetry.credentials_deleted, 1);
    assert!(telemetry.resolutions >= 3);
    assert!(telemetry.resolution_failures >= 4);
    Ok(scope)
}

fn active_credential(
    scope: EnvironmentScope,
    client_id: ApplicationClientId,
    generated: &runku_identity::GeneratedCredentialKey,
    label: &str,
    scopes: BTreeSet<ApplicationScope>,
    created_at: i64,
    expires_at: Option<i64>,
) -> Result<ApplicationCredential, IdentityError> {
    let parsed: ParsedApplicationKey = generated.key.expose().parse()?;
    Ok(ApplicationCredential {
        scope,
        id: parsed.credential_id(),
        client_id,
        kind: generated.kind,
        label: label.parse()?,
        status: CredentialStatus::Active,
        digest: generated.digest,
        scopes,
        created_at: TimestampMicros::new(created_at),
        expires_at: expires_at.map(TimestampMicros::new),
        revoked_at: None,
        deleted_at: None,
    })
}

fn scope_set(values: &[&str]) -> Result<BTreeSet<ApplicationScope>, IdentityError> {
    values.iter().map(|value| value.parse()).collect()
}
