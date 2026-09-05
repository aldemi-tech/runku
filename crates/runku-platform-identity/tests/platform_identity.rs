//! Platform Identity bootstrap, session rotation, OIDC linking, and scope isolation.

use std::{collections::BTreeSet, str::FromStr as _, sync::Arc};

use runku_core::{EnvironmentId, EnvironmentScope, OperationId, ProjectId};
use runku_platform_identity::{
    AccessScope, BootstrapResult, DeviceName, ExternalOperatorIdentity, IdempotentInvitationResult,
    InvitationStatus, ManagedSourceAuthority, OperatorGrant, OperatorName, OperatorRole,
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
#[allow(clippy::too_many_lines)]
async fn managed_oidc_enrollment_creates_and_reconciles_authoritative_project_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("managed.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let service = PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([19; 32])),
        SessionTokenPolicy::DEFAULT,
    )?;
    let identity = ExternalOperatorIdentity {
        provider_id: "cloud".to_owned(),
        subject_id: "better-user-1".to_owned(),
    };
    let first_project = ProjectId::generate();
    let second_project = ProjectId::generate();
    let authority = ManagedSourceAuthority::from_str("https://cloud.runku.example")?;
    let first = service
        .login_with_managed_external_identity(
            identity.clone(),
            OperatorName::from_str("Cloud user")?,
            authority.clone(),
            1,
            vec![OperatorGrant {
                scope: AccessScope::Project(first_project),
                capabilities: OperatorRole::Developer.capabilities(),
            }],
            DeviceName::from_str("first device")?,
            TimestampMicros::new(1_900_000_000_000_000),
        )
        .await?;
    assert!(first.reconciliation.applied);
    assert!(!first.reconciliation.replayed);
    first.login.context.authorize(
        AccessScope::Project(first_project),
        PlatformCapability::ReleasesPublish,
    )?;

    let second = service
        .login_with_managed_external_identity(
            identity,
            OperatorName::from_str("Ignored replacement name")?,
            authority.clone(),
            2,
            vec![OperatorGrant {
                scope: AccessScope::Project(second_project),
                capabilities: OperatorRole::Observer.capabilities(),
            }],
            DeviceName::from_str("second device")?,
            TimestampMicros::new(1_900_000_000_000_001),
        )
        .await?;
    assert_eq!(
        second.login.context.operator.id,
        first.login.context.operator.id
    );
    assert!(
        second.login.context.operator.authorization_revision
            > first.login.context.operator.authorization_revision
    );
    assert_eq!(
        second.login.context.authorize(
            AccessScope::Project(first_project),
            PlatformCapability::ReleasesRead
        ),
        Err(PlatformIdentityError::Forbidden),
    );
    second.login.context.authorize(
        AccessScope::Project(second_project),
        PlatformCapability::ReleasesRead,
    )?;
    assert_eq!(
        second.login.context.authorize(
            AccessScope::Project(second_project),
            PlatformCapability::ReleasesPublish
        ),
        Err(PlatformIdentityError::Forbidden),
    );
    let refreshed_first = service
        .authenticate(
            &first.login.access_token,
            TimestampMicros::new(1_900_000_000_000_002),
        )
        .await?;
    assert_eq!(refreshed_first.grants, second.login.context.grants);

    let replay = service
        .reconcile_managed_grants(
            first.login.context.operator.id,
            authority.clone(),
            2,
            second.login.context.grants.clone(),
            TimestampMicros::new(1_900_000_000_000_003),
        )
        .await?;
    assert!(!replay.applied);
    assert!(replay.replayed);
    assert_eq!(
        replay.authorization_revision,
        second.login.context.operator.authorization_revision
    );
    assert!(matches!(
        service
            .reconcile_managed_grants(
                first.login.context.operator.id,
                authority.clone(),
                2,
                Vec::new(),
                TimestampMicros::new(1_900_000_000_000_004),
            )
            .await,
        Err(PlatformIdentityError::ManagedSourceConflict)
    ));
    assert!(matches!(
        service
            .reconcile_managed_grants(
                first.login.context.operator.id,
                authority.clone(),
                1,
                second.login.context.grants.clone(),
                TimestampMicros::new(1_900_000_000_000_005),
            )
            .await,
        Err(PlatformIdentityError::ManagedSourceStale)
    ));

    let second_authority = ManagedSourceAuthority::from_str("https://access.runku.example")?;
    service
        .reconcile_managed_grants(
            first.login.context.operator.id,
            second_authority.clone(),
            1,
            vec![OperatorGrant {
                scope: AccessScope::Project(second_project),
                capabilities: BTreeSet::from([PlatformCapability::CredentialsRead]),
            }],
            TimestampMicros::new(1_900_000_000_000_006),
        )
        .await?;

    let revoked = service
        .reconcile_managed_grants(
            first.login.context.operator.id,
            authority,
            3,
            Vec::new(),
            TimestampMicros::new(1_900_000_000_000_007),
        )
        .await?;
    assert!(revoked.applied);
    let active_session = service
        .authenticate(
            &first.login.access_token,
            TimestampMicros::new(1_900_000_000_000_008),
        )
        .await?;
    assert_eq!(
        active_session.authorize(
            AccessScope::Project(second_project),
            PlatformCapability::ReleasesRead,
        ),
        Err(PlatformIdentityError::Forbidden)
    );
    active_session.authorize(
        AccessScope::Project(second_project),
        PlatformCapability::CredentialsRead,
    )?;
    service
        .reconcile_managed_grants(
            first.login.context.operator.id,
            second_authority,
            2,
            Vec::new(),
            TimestampMicros::new(1_900_000_000_000_009),
        )
        .await?;
    let fully_revoked = service
        .authenticate(
            &first.login.access_token,
            TimestampMicros::new(1_900_000_000_000_010),
        )
        .await?;
    assert!(fully_revoked.grants.is_empty());

    let canonical_authority = ManagedSourceAuthority::from_str("https://canonical.runku.example")?;
    let canonical_first = OperatorGrant {
        scope: AccessScope::Project(first_project),
        capabilities: BTreeSet::from([PlatformCapability::ReleasesRead]),
    };
    let canonical_second = OperatorGrant {
        scope: AccessScope::Project(second_project),
        capabilities: BTreeSet::from([PlatformCapability::DataRead]),
    };
    service
        .reconcile_managed_grants(
            first.login.context.operator.id,
            canonical_authority.clone(),
            1,
            vec![canonical_second.clone(), canonical_first.clone()],
            TimestampMicros::new(1_900_000_000_000_011),
        )
        .await?;
    let canonical_replay = service
        .reconcile_managed_grants(
            first.login.context.operator.id,
            canonical_authority,
            1,
            vec![canonical_first, canonical_second],
            TimestampMicros::new(1_900_000_000_000_012),
        )
        .await?;
    assert!(canonical_replay.replayed);
    repository.close().await;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn first_managed_reconciliation_adopts_only_the_explicit_operator_legacy_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("managed-legacy.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let service = PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([23; 32])),
        SessionTokenPolicy::DEFAULT,
    )?;
    let start = TimestampMicros::new(1_910_000_000_000_000);
    let bootstrap = match service
        .initialize_bootstrap(OperatorName::from_str("Owner")?, start)
        .await?
    {
        BootstrapResult::Created(generated) => generated,
        BootstrapResult::Replayed | BootstrapResult::Complete => {
            return Err("fresh bootstrap was not created".into());
        }
    };
    let owner = service
        .login_with_invitation(
            &bootstrap.code,
            DeviceName::from_str("owner")?,
            None,
            TimestampMicros::new(start.get() + 1),
        )
        .await?;
    let linked_project = ProjectId::generate();
    let linked_invitation = service
        .create_invitation(
            &owner.context,
            OperatorName::from_str("Linked operator")?,
            AccessScope::Project(linked_project),
            OperatorRole::Observer,
            TimestampMicros::new(start.get() + 2),
        )
        .await?;
    let external = ExternalOperatorIdentity {
        provider_id: "cloud".to_owned(),
        subject_id: "legacy-linked".to_owned(),
    };
    let linked = service
        .login_with_invitation(
            &linked_invitation.code,
            DeviceName::from_str("linked")?,
            Some(external.clone()),
            TimestampMicros::new(start.get() + 3),
        )
        .await?;

    let invitation_only_project = ProjectId::generate();
    let invitation_only_invitation = service
        .create_invitation(
            &owner.context,
            OperatorName::from_str("Invitation only")?,
            AccessScope::Project(invitation_only_project),
            OperatorRole::Observer,
            TimestampMicros::new(start.get() + 4),
        )
        .await?;
    let invitation_only = service
        .login_with_invitation(
            &invitation_only_invitation.code,
            DeviceName::from_str("invitation-only")?,
            None,
            TimestampMicros::new(start.get() + 5),
        )
        .await?;

    let authority = ManagedSourceAuthority::from_str("https://cloud.runku.example")?;
    service
        .login_with_managed_external_identity(
            external,
            OperatorName::from_str("Ignored")?,
            authority.clone(),
            1,
            Vec::new(),
            DeviceName::from_str("managed")?,
            TimestampMicros::new(start.get() + 6),
        )
        .await?;
    let linked_after = service
        .authenticate(&linked.access_token, TimestampMicros::new(start.get() + 7))
        .await?;
    assert!(linked_after.grants.is_empty());
    let invitation_only_after = service
        .authenticate(
            &invitation_only.access_token,
            TimestampMicros::new(start.get() + 8),
        )
        .await?;
    invitation_only_after.authorize(
        AccessScope::Project(invitation_only_project),
        PlatformCapability::ReleasesRead,
    )?;

    service
        .reconcile_managed_grants(
            invitation_only.context.operator.id,
            authority,
            1,
            Vec::new(),
            TimestampMicros::new(start.get() + 9),
        )
        .await?;
    let explicit_after = service
        .authenticate(
            &invitation_only.access_token,
            TimestampMicros::new(start.get() + 10),
        )
        .await?;
    assert!(explicit_after.grants.is_empty());
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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn invitation_operations_reconcile_conflict_revoke_and_replace_without_secret_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("invitation-operations.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let service = PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([13; 32])),
        SessionTokenPolicy::DEFAULT,
    )?;
    let start = TimestampMicros::new(1_800_000_000_000_000);
    let bootstrap = match service
        .initialize_bootstrap(OperatorName::from_str("Owner")?, start)
        .await?
    {
        BootstrapResult::Created(generated) => generated,
        BootstrapResult::Replayed | BootstrapResult::Complete => {
            return Err("fresh storage did not create bootstrap".into());
        }
    };
    let owner = service
        .login_with_invitation(
            &bootstrap.code,
            DeviceName::from_str("owner")?,
            None,
            TimestampMicros::new(start.get() + 1),
        )
        .await?;
    let scope = AccessScope::Environment(EnvironmentScope::new(
        ProjectId::generate(),
        EnvironmentId::generate(),
    ));
    let missing_operation = OperationId::generate();
    assert!(matches!(
        service
            .invitation_by_operation(&owner.context, missing_operation)
            .await,
        Err(PlatformIdentityError::NotFound)
    ));

    let operation = OperationId::generate();
    let created_at = TimestampMicros::new(start.get() + 2);
    let (invitation_id, code) = match service
        .create_invitation_idempotent(
            &owner.context,
            operation,
            OperatorName::from_str("Cloud operator")?,
            scope,
            OperatorRole::Observer,
            created_at,
        )
        .await?
    {
        IdempotentInvitationResult::Created {
            invitation,
            generated,
        } => {
            assert_eq!(invitation.operation_id, Some(operation));
            assert_eq!(invitation.status_at(created_at), InvitationStatus::Pending);
            (invitation.id, generated.code)
        }
        IdempotentInvitationResult::Replayed(_) => {
            return Err("first operation unexpectedly replayed".into());
        }
    };
    match service
        .create_invitation_idempotent(
            &owner.context,
            operation,
            OperatorName::from_str("Cloud operator")?,
            scope,
            OperatorRole::Observer,
            TimestampMicros::new(start.get() + 3),
        )
        .await?
    {
        IdempotentInvitationResult::Replayed(invitation) => {
            assert_eq!(invitation.id, invitation_id);
            assert_eq!(invitation.operation_id, Some(operation));
        }
        IdempotentInvitationResult::Created { .. } => {
            return Err("exact operation created a second invitation".into());
        }
    }
    assert!(matches!(
        service
            .create_invitation_idempotent(
                &owner.context,
                operation,
                OperatorName::from_str("Different request")?,
                scope,
                OperatorRole::Observer,
                TimestampMicros::new(start.get() + 4),
            )
            .await,
        Err(PlatformIdentityError::InvitationOperationReused)
    ));
    let reconciled = service
        .invitation_by_operation(&owner.context, operation)
        .await?;
    assert_eq!(reconciled.id, invitation_id);

    let limited_code = service
        .create_invitation(
            &owner.context,
            OperatorName::from_str("Limited observer")?,
            scope,
            OperatorRole::Observer,
            TimestampMicros::new(start.get() + 5),
        )
        .await?;
    let limited = service
        .login_with_invitation(
            &limited_code.code,
            DeviceName::from_str("limited")?,
            None,
            TimestampMicros::new(start.get() + 6),
        )
        .await?;
    assert!(matches!(
        service
            .invitation_by_operation(&limited.context, operation)
            .await,
        Err(PlatformIdentityError::NotFound)
    ));
    assert!(matches!(
        service
            .revoke_invitation(
                &limited.context,
                invitation_id,
                TimestampMicros::new(start.get() + 7),
            )
            .await,
        Err(PlatformIdentityError::NotFound)
    ));

    assert!(
        service
            .revoke_invitation(
                &owner.context,
                invitation_id,
                TimestampMicros::new(start.get() + 8),
            )
            .await?
    );
    assert!(
        !service
            .revoke_invitation(
                &owner.context,
                invitation_id,
                TimestampMicros::new(start.get() + 9),
            )
            .await?
    );
    assert!(matches!(
        service
            .login_with_invitation(
                &code,
                DeviceName::from_str("revoked")?,
                None,
                TimestampMicros::new(start.get() + 10),
            )
            .await,
        Err(PlatformIdentityError::Unauthenticated)
    ));
    let revoked = service
        .invitation_by_operation(&owner.context, operation)
        .await?;
    assert_eq!(
        revoked.status_at(TimestampMicros::new(start.get() + 11)),
        InvitationStatus::Revoked
    );

    let replacement = service
        .create_invitation_idempotent(
            &owner.context,
            OperationId::generate(),
            OperatorName::from_str("Cloud operator")?,
            scope,
            OperatorRole::Observer,
            TimestampMicros::new(start.get() + 12),
        )
        .await?;
    assert!(matches!(
        replacement,
        IdempotentInvitationResult::Created { .. }
    ));
    let concurrent_operation = OperationId::generate();
    let first = service.create_invitation_idempotent(
        &owner.context,
        concurrent_operation,
        OperatorName::from_str("Concurrent operator")?,
        scope,
        OperatorRole::Observer,
        TimestampMicros::new(start.get() + 13),
    );
    let second = service.create_invitation_idempotent(
        &owner.context,
        concurrent_operation,
        OperatorName::from_str("Concurrent operator")?,
        scope,
        OperatorRole::Observer,
        TimestampMicros::new(start.get() + 14),
    );
    let (first, second) = tokio::join!(first, second);
    let (first, second) = (first?, second?);
    let created_id = match &first {
        IdempotentInvitationResult::Created { invitation, .. } => Some(invitation.id),
        IdempotentInvitationResult::Replayed(_) => None,
    }
    .or(match &second {
        IdempotentInvitationResult::Created { invitation, .. } => Some(invitation.id),
        IdempotentInvitationResult::Replayed(_) => None,
    })
    .ok_or("concurrent operation never created an invitation")?;
    let replayed_id = match (&first, &second) {
        (
            IdempotentInvitationResult::Created { .. },
            IdempotentInvitationResult::Replayed(invitation),
        )
        | (
            IdempotentInvitationResult::Replayed(invitation),
            IdempotentInvitationResult::Created { .. },
        ) => invitation.id,
        _ => return Err("concurrent operation did not produce one create and one replay".into()),
    };
    assert_eq!(created_id, replayed_id);
    let telemetry = repository.telemetry();
    assert_eq!(telemetry.invitations_created, 4);
    assert_eq!(telemetry.invitation_replays, 2);
    assert_eq!(telemetry.invitations_revoked, 1);
    repository.close().await;
    Ok(())
}
