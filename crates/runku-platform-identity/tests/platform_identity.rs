//! Platform Identity bootstrap, session rotation, OIDC linking, and scope isolation.

use std::{str::FromStr as _, sync::Arc};

use runku_core::{EnvironmentId, EnvironmentScope, ProjectId};
use runku_platform_identity::{
    AccessScope, BootstrapResult, DeviceName, ExternalOperatorIdentity, OperatorName, OperatorRole,
    PlatformCapability, PlatformIdentityCrypto, PlatformIdentityError, PlatformIdentityRepository,
    PlatformIdentityRepositoryConfig, PlatformIdentityService, SessionTokenPolicy,
    SqlPlatformIdentityRepository,
};
use runku_value::TimestampMicros;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn bootstrap_invite_refresh_revoke_and_oidc_are_scope_safe()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("platform.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let service = PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([7; 32])),
        SessionTokenPolicy::DEFAULT,
    )?;
    let start = TimestampMicros::new(1_800_000_000_000_000);
    let bootstrap = match service
        .initialize_bootstrap(OperatorName::from_str("Initial owner")?, start)
        .await?
    {
        BootstrapResult::Created(code) => code,
        BootstrapResult::Replayed => return Err("fresh storage replayed bootstrap".into()),
        BootstrapResult::Complete => return Err("fresh storage completed bootstrap".into()),
    };
    assert!(matches!(
        service
            .initialize_bootstrap(OperatorName::from_str("Other")?, start)
            .await?,
        BootstrapResult::Replayed
    ));

    let owner = service
        .login_with_invitation(
            &bootstrap.code,
            DeviceName::from_str("owner-laptop")?,
            None,
            TimestampMicros::new(start.get() + 1),
        )
        .await?;
    owner.context.authorize(
        AccessScope::Installation,
        PlatformCapability::InstallationManage,
    )?;

    let project = ProjectId::generate();
    let environment = EnvironmentScope::new(project, EnvironmentId::generate());
    let other = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let invitation = service
        .create_invitation(
            &owner.context,
            OperatorName::from_str("Observer")?,
            AccessScope::Environment(environment),
            OperatorRole::Observer,
            TimestampMicros::new(start.get() + 2),
        )
        .await?;
    let external = ExternalOperatorIdentity {
        provider_id: "corporate".to_owned(),
        subject_id: "pri_v1_opaque".to_owned(),
    };
    let observer = service
        .login_with_invitation(
            &invitation.code,
            DeviceName::from_str("observer-laptop")?,
            Some(external.clone()),
            TimestampMicros::new(start.get() + 3),
        )
        .await?;
    observer.context.authorize(
        AccessScope::Environment(environment),
        PlatformCapability::LogsFollow,
    )?;
    assert_eq!(
        observer.context.authorize(
            AccessScope::Environment(other),
            PlatformCapability::LogsFollow,
        ),
        Err(PlatformIdentityError::Forbidden)
    );
    assert_eq!(
        observer.context.authorize(
            AccessScope::Environment(environment),
            PlatformCapability::LogsPrune,
        ),
        Err(PlatformIdentityError::Forbidden)
    );

    let oidc = service
        .login_with_external_identity(
            &external,
            DeviceName::from_str("observer-second-device")?,
            TimestampMicros::new(start.get() + 4),
        )
        .await?;
    assert_eq!(oidc.context.operator.id, observer.context.operator.id);

    let refreshed = service
        .refresh(
            &observer.refresh_token,
            TimestampMicros::new(start.get() + 5),
        )
        .await?;
    assert_eq!(refreshed.context.session.id, observer.context.session.id);
    assert!(matches!(
        service
            .refresh(
                &observer.refresh_token,
                TimestampMicros::new(start.get() + 6)
            )
            .await
            .map(|_| ()),
        Err(PlatformIdentityError::Unauthenticated)
    ));
    let authenticated = service
        .authenticate(
            &refreshed.access_token,
            TimestampMicros::new(start.get() + 7),
        )
        .await?;
    assert_eq!(authenticated.operator.id, observer.context.operator.id);

    assert!(
        service
            .revoke_session(
                &authenticated,
                authenticated.session.id,
                TimestampMicros::new(start.get() + 8),
            )
            .await?
    );
    assert!(matches!(
        service
            .authenticate(
                &refreshed.access_token,
                TimestampMicros::new(start.get() + 9),
            )
            .await,
        Err(PlatformIdentityError::Unauthenticated)
    ));

    let sessions = repository.list_sessions(&owner.context).await?;
    assert_eq!(sessions.len(), 1);
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn expired_bootstrap_is_replaced_without_creating_an_operator()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("expired.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let service = PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([9; 32])),
        SessionTokenPolicy::DEFAULT,
    )?;
    let first = TimestampMicros::new(1_800_000_000_000_000);
    assert!(matches!(
        service
            .initialize_bootstrap(OperatorName::from_str("Owner")?, first)
            .await?,
        BootstrapResult::Created(_)
    ));
    let after_expiry = TimestampMicros::new(first.get() + 24 * 60 * 60 * 1_000_000 + 1);
    assert!(matches!(
        service
            .initialize_bootstrap(OperatorName::from_str("Owner")?, after_expiry)
            .await?,
        BootstrapResult::Created(_)
    ));
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn lost_bootstrap_recovery_revokes_the_old_code_and_closes_after_enrollment()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("recovered.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let service = PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([11; 32])),
        SessionTokenPolicy::DEFAULT,
    )?;
    let start = TimestampMicros::new(1_800_000_000_000_000);
    let original = match service
        .initialize_bootstrap(OperatorName::from_str("Owner")?, start)
        .await?
    {
        BootstrapResult::Created(generated) => generated,
        BootstrapResult::Replayed | BootstrapResult::Complete => {
            return Err("fresh storage did not create a bootstrap".into());
        }
    };
    let replacement = service
        .recover_bootstrap(
            OperatorName::from_str("Owner")?,
            TimestampMicros::new(start.get() + 1),
        )
        .await?;

    assert!(matches!(
        service
            .login_with_invitation(
                &original.code,
                DeviceName::from_str("old-code")?,
                None,
                TimestampMicros::new(start.get() + 2),
            )
            .await,
        Err(PlatformIdentityError::Unauthenticated)
    ));
    service
        .login_with_invitation(
            &replacement.code,
            DeviceName::from_str("recovered-code")?,
            None,
            TimestampMicros::new(start.get() + 3),
        )
        .await?;
    assert!(matches!(
        service
            .recover_bootstrap(
                OperatorName::from_str("Owner")?,
                TimestampMicros::new(start.get() + 4),
            )
            .await,
        Err(PlatformIdentityError::AlreadyInitialized)
    ));
    repository.close().await;
    Ok(())
}
