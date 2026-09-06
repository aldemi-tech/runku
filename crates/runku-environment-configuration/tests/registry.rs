//! Environment configuration persistence and secret-redaction conformance.

use std::{error::Error, str::FromStr};

use runku_core::{EnvironmentId, EnvironmentScope, OperationId, OperatorId, ProjectId};
use runku_environment_configuration::{
    ConfigurationEncryptionKey, ConfigurationError, ConfigurationKind, ConfigurationName,
    EnvironmentConfigurationRegistry,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;
use zeroize::Zeroizing;

fn scope() -> Result<EnvironmentScope, Box<dyn Error>> {
    Ok(EnvironmentScope::new(
        ProjectId::from_str("prj_00000000000000000000000001")?,
        EnvironmentId::from_str("env_00000000000000000000000002")?,
    ))
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn variables_and_secrets_are_revisioned_encrypted_and_idempotent()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("configuration.sqlite3").display()
    );
    let registry = EnvironmentConfigurationRegistry::connect_sqlite(
        &url,
        ConfigurationEncryptionKey::new([7; 32]),
    )
    .await?;
    let scope = scope()?;
    let actor = OperatorId::from_str("opr_00000000000000000000000003")?;
    let variable_name = ConfigurationName::from_str("FEATURE_CHECKOUT_V3")?;
    let first_operation = OperationId::from_str("opn_00000000000000000000000004")?;
    let variable = registry
        .set(
            scope,
            first_operation,
            actor,
            0,
            variable_name.clone(),
            ConfigurationKind::Variable,
            Zeroizing::new("true".to_owned()),
            TimestampMicros::new(10),
        )
        .await?;
    assert_eq!(variable.configuration_revision, 1);
    assert_eq!(
        variable
            .entry
            .as_ref()
            .and_then(|entry| entry.variable_value.as_deref()),
        Some("true")
    );
    let replay = registry
        .set(
            scope,
            first_operation,
            actor,
            0,
            variable_name,
            ConfigurationKind::Variable,
            Zeroizing::new("true".to_owned()),
            TimestampMicros::new(10),
        )
        .await?;
    assert!(replay.replayed);

    let secret_name = ConfigurationName::from_str("PAYMENTS_API_KEY")?;
    let secret = registry
        .set(
            scope,
            OperationId::from_str("opn_00000000000000000000000005")?,
            actor,
            1,
            secret_name.clone(),
            ConfigurationKind::Secret,
            Zeroizing::new("private-api-key".to_owned()),
            TimestampMicros::new(20),
        )
        .await?;
    assert_eq!(secret.configuration_revision, 2);
    assert_eq!(
        secret
            .entry
            .as_ref()
            .and_then(|entry| entry.variable_value.as_deref()),
        None
    );
    let snapshot = registry.snapshot(scope).await?;
    assert_eq!(snapshot.configuration_revision, 2);
    assert_eq!(snapshot.entries.len(), 2);
    assert!(format!("{snapshot:?}").find("private-api-key").is_none());
    assert_eq!(
        registry.resolve(scope, &secret_name).await?.as_str(),
        "private-api-key"
    );

    registry.close().await;
    let bytes = std::fs::read(directory.path().join("configuration.sqlite3"))?;
    assert!(
        !bytes
            .windows(b"private-api-key".len())
            .any(|window| window == b"private-api-key")
    );

    let reopened = EnvironmentConfigurationRegistry::connect_sqlite(
        &url,
        ConfigurationEncryptionKey::new([7; 32]),
    )
    .await?;
    assert_eq!(
        reopened.resolve(scope, &secret_name).await?.as_str(),
        "private-api-key"
    );
    let rotated = reopened
        .set(
            scope,
            OperationId::from_str("opn_00000000000000000000000006")?,
            actor,
            2,
            secret_name.clone(),
            ConfigurationKind::Secret,
            Zeroizing::new("rotated-api-key".to_owned()),
            TimestampMicros::new(30),
        )
        .await?;
    assert_eq!(rotated.configuration_revision, 3);
    assert_eq!(
        reopened.resolve(scope, &secret_name).await?.as_str(),
        "rotated-api-key"
    );
    let historical_replay = reopened
        .set(
            scope,
            first_operation,
            actor,
            0,
            ConfigurationName::from_str("FEATURE_CHECKOUT_V3")?,
            ConfigurationKind::Variable,
            Zeroizing::new("true".to_owned()),
            TimestampMicros::new(10),
        )
        .await?;
    assert!(historical_replay.replayed);
    assert_eq!(historical_replay.configuration_revision, 1);
    assert_eq!(
        historical_replay.entry.as_ref().map(|entry| entry.revision),
        Some(1)
    );
    assert_eq!(
        reopened
            .set(
                scope,
                OperationId::from_str("opn_00000000000000000000000007")?,
                actor,
                2,
                ConfigurationName::from_str("OTHER")?,
                ConfigurationKind::Variable,
                Zeroizing::new("x".to_owned()),
                TimestampMicros::new(31)
            )
            .await,
        Err(ConfigurationError::Conflict)
    );
    let deleted = reopened
        .delete(
            scope,
            OperationId::from_str("opn_00000000000000000000000008")?,
            actor,
            3,
            secret_name.clone(),
            TimestampMicros::new(40),
        )
        .await?;
    assert_eq!(deleted.configuration_revision, 4);
    let first_history = reopened.history(scope, None, 2).await?;
    assert_eq!(first_history.entries.len(), 2);
    assert_eq!(first_history.entries[0].action, "delete");
    assert_eq!(first_history.entries[0].configuration_revision, 4);
    assert_eq!(first_history.entries[1].configuration_revision, 3);
    let second_history = reopened
        .history(scope, first_history.next_before_sequence, 2)
        .await?;
    assert_eq!(second_history.entries.len(), 2);
    assert!(second_history.next_before_sequence.is_none());
    assert_eq!(second_history.entries[1].configuration_revision, 1);
    assert_eq!(
        reopened.resolve(scope, &secret_name).await,
        Err(ConfigurationError::NotFound)
    );
    Ok(())
}
