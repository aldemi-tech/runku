//! Reproducible local baseline for key creation and hot-path resolution.

use std::{collections::BTreeSet, error::Error, time::Instant};

use runku_core::{ApplicationClientId, CredentialId, EnvironmentId, EnvironmentScope, ProjectId};
use runku_identity::{
    ApplicationClient, ApplicationClientStatus, ApplicationCredential,
    ApplicationCredentialResolver, ApplicationIdentityRepository, ApplicationScope, ClientKind,
    CredentialKind, CredentialStatus, KeyringCrypto, ParsedApplicationKey,
};
use runku_identity_repository::{IdentityRepositoryConfig, SqlApplicationIdentityRepository};
use runku_value::TimestampMicros;
use tempfile::tempdir;

const RECORDS: u32 = 10_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("keyring-baseline.sqlite3");
    let repository = SqlApplicationIdentityRepository::connect_sqlite(
        &format!("sqlite://{}?mode=rwc", path.display()),
        IdentityRepositoryConfig::LOCAL,
    )
    .await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let client_id = ApplicationClientId::generate();
    let scopes = BTreeSet::from(["function:invoke".parse::<ApplicationScope>()?]);
    repository
        .create_client(&ApplicationClient {
            scope,
            id: client_id,
            name: "baseline-browser".parse()?,
            kind: ClientKind::Public,
            status: ApplicationClientStatus::Active,
            scope_ceiling: scopes.clone(),
            created_at: TimestampMicros::new(1),
        })
        .await?;
    let crypto = KeyringCrypto::new([33; 32]);
    let mut keys = Vec::with_capacity(usize::try_from(RECORDS)?);

    let create_started = Instant::now();
    for ordinal in 0..RECORDS {
        let generated = crypto.generate_publishable(CredentialId::generate())?;
        let parsed: ParsedApplicationKey = generated.key.expose().parse()?;
        repository
            .create_credential(&ApplicationCredential {
                scope,
                id: parsed.credential_id(),
                client_id,
                kind: CredentialKind::Publishable,
                label: format!("browser-{ordinal:05}").parse()?,
                status: CredentialStatus::Active,
                digest: generated.digest,
                scopes: scopes.clone(),
                created_at: TimestampMicros::new(i64::from(ordinal) + 2),
                expires_at: None,
                revoked_at: None,
                deleted_at: None,
            })
            .await?;
        keys.push(generated.key);
    }
    let create_elapsed = create_started.elapsed();

    let resolve_started = Instant::now();
    for key in &keys {
        let parsed: ParsedApplicationKey = key.expose().parse()?;
        repository
            .resolve_key(scope, &parsed, &crypto, TimestampMicros::new(20_000))
            .await?;
    }
    let resolve_elapsed = resolve_started.elapsed();
    println!(
        "keyring_baseline records={RECORDS} create_us={} create_per_sec={:.0} resolve_us={} resolve_per_sec={:.0}",
        create_elapsed.as_micros(),
        f64::from(RECORDS) / create_elapsed.as_secs_f64(),
        resolve_elapsed.as_micros(),
        f64::from(RECORDS) / resolve_elapsed.as_secs_f64(),
    );
    repository.close().await;
    Ok(())
}
