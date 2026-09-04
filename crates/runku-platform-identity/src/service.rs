//! Framework-independent Platform Identity orchestration.

use std::{sync::Arc, time::Duration};

use runku_core::{OperationId, OperatorId, OperatorInvitationId, OperatorSessionId};
use runku_value::TimestampMicros;
use sha2::{Digest as _, Sha256};

use crate::{
    AccessScope, AccessToken, ConsumedInvitation, DeviceName, ExternalOperatorIdentity,
    GeneratedInvitationCode, IdempotentInvitationCreate, InvitationCode, InvitationKind,
    InvitationStatus, NewInvitation, NewOperatorSession, OperatorContext, OperatorGrant,
    OperatorInvitation, OperatorName, OperatorRole, PlatformCapability, PlatformIdentityCrypto,
    PlatformIdentityError, PlatformIdentityRepository, RefreshToken, RefreshedSession,
};

/// Bounded lifetime policy for operator credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionTokenPolicy {
    /// Short-lived access token lifetime.
    pub access_ttl: Duration,
    /// Maximum rotating refresh-token lifetime from each successful rotation.
    pub refresh_ttl: Duration,
    /// Single-use invitation lifetime.
    pub invitation_ttl: Duration,
    /// Initial owner setup-code lifetime.
    pub bootstrap_ttl: Duration,
}

impl SessionTokenPolicy {
    /// Conservative production defaults.
    pub const DEFAULT: Self = Self {
        access_ttl: Duration::from_mins(10),
        refresh_ttl: Duration::from_hours(720),
        invitation_ttl: Duration::from_mins(30),
        bootstrap_ttl: Duration::from_hours(24),
    };

    fn validate(self) -> Result<(), PlatformIdentityError> {
        if !(Duration::from_mins(1)..=Duration::from_hours(1)).contains(&self.access_ttl)
            || !(Duration::from_hours(1)..=Duration::from_hours(2_160)).contains(&self.refresh_ttl)
            || !(Duration::from_mins(5)..=Duration::from_hours(168)).contains(&self.invitation_ttl)
            || !(Duration::from_mins(5)..=Duration::from_hours(168)).contains(&self.bootstrap_ttl)
            || self.refresh_ttl <= self.access_ttl
        {
            return Err(PlatformIdentityError::InvalidInput);
        }
        Ok(())
    }
}

/// Result of first-start initialization.
#[derive(Debug)]
pub enum BootstrapResult {
    /// A new code must be written once to protected local storage.
    Created(GeneratedInvitationCode),
    /// The exact pending bootstrap already exists; retain the previously written protected file.
    Replayed,
    /// At least one operator already exists and bootstrap remains permanently closed.
    Complete,
}

/// Tokens and token-free identity returned after login or refresh.
#[derive(Debug)]
pub struct LoginResult {
    /// Short-lived bearer sent to Management API requests.
    pub access_token: AccessToken,
    /// Rotating bearer stored only in the OS credential store or protected fallback file.
    pub refresh_token: RefreshToken,
    /// Current server-authoritative operator context.
    pub context: OperatorContext,
}

/// Result of a durable invitation issuance operation.
#[derive(Debug)]
pub enum IdempotentInvitationResult {
    /// A new bearer was committed and is available exactly in this response.
    Created {
        /// Non-secret durable invitation metadata.
        invitation: OperatorInvitation,
        /// One-time bearer that must never be persisted by Platform Identity.
        generated: GeneratedInvitationCode,
    },
    /// The exact operation was already committed; only non-secret metadata is recoverable.
    Replayed(OperatorInvitation),
}

/// High-level bootstrap, invitation, login, refresh, and authorization service.
pub struct PlatformIdentityService {
    repository: Arc<dyn PlatformIdentityRepository>,
    crypto: Arc<PlatformIdentityCrypto>,
    policy: SessionTokenPolicy,
}

impl std::fmt::Debug for PlatformIdentityService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlatformIdentityService")
            .field("backend", &self.repository.backend())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl PlatformIdentityService {
    /// Composes the service from mandatory authoritative storage and a dedicated pepper.
    ///
    /// # Errors
    ///
    /// Rejects unsafe credential lifetime policy.
    pub fn new(
        repository: Arc<dyn PlatformIdentityRepository>,
        crypto: Arc<PlatformIdentityCrypto>,
        policy: SessionTokenPolicy,
    ) -> Result<Self, PlatformIdentityError> {
        policy.validate()?;
        Ok(Self {
            repository,
            crypto,
            policy,
        })
    }

    /// Creates the first-owner invitation exactly once while the installation has no operator.
    ///
    /// # Errors
    ///
    /// Returns storage, entropy, clock, conflict, or already-initialized failures.
    pub async fn initialize_bootstrap(
        &self,
        operator_name: OperatorName,
        now: TimestampMicros,
    ) -> Result<BootstrapResult, PlatformIdentityError> {
        let id = OperatorInvitationId::generate();
        let generated = self.crypto.generate_invitation(id)?;
        let invitation = NewInvitation {
            id,
            kind: InvitationKind::Bootstrap,
            operator_name,
            grants: vec![OperatorGrant {
                scope: AccessScope::Installation,
                capabilities: PlatformCapability::owner_set(),
            }],
            created_by: None,
            created_at: now,
            expires_at: add(now, self.policy.bootstrap_ttl)?,
            digest: generated.digest,
        };
        match self.repository.create_bootstrap(&invitation).await {
            Err(PlatformIdentityError::AlreadyInitialized) => Ok(BootstrapResult::Complete),
            Err(error) => Err(error),
            Ok(crate::BootstrapCreate::Created) => Ok(BootstrapResult::Created(generated)),
            Ok(crate::BootstrapCreate::Replayed) => Ok(BootstrapResult::Replayed),
        }
    }

    /// Replaces a pending first-owner invitation when its protected local file was lost.
    ///
    /// # Errors
    ///
    /// Rejects recovery after any operator exists and propagates entropy, storage, clock, or
    /// uncertain-commit failures. The previous pending credential is revoked in the same
    /// transaction that persists the replacement.
    pub async fn recover_bootstrap(
        &self,
        operator_name: OperatorName,
        now: TimestampMicros,
    ) -> Result<GeneratedInvitationCode, PlatformIdentityError> {
        let id = OperatorInvitationId::generate();
        let generated = self.crypto.generate_invitation(id)?;
        let invitation = NewInvitation {
            id,
            kind: InvitationKind::Bootstrap,
            operator_name,
            grants: vec![OperatorGrant {
                scope: AccessScope::Installation,
                capabilities: PlatformCapability::owner_set(),
            }],
            created_by: None,
            created_at: now,
            expires_at: add(now, self.policy.bootstrap_ttl)?,
            digest: generated.digest,
        };
        self.repository.replace_bootstrap(&invitation).await?;
        Ok(generated)
    }

    /// Creates a single-use invitation after least-privilege delegation checks.
    ///
    /// # Errors
    ///
    /// Rejects privilege escalation, invalid scope/name, entropy failure, or repository failure.
    pub async fn create_invitation(
        &self,
        actor: &OperatorContext,
        operator_name: OperatorName,
        scope: AccessScope,
        role: OperatorRole,
        now: TimestampMicros,
    ) -> Result<GeneratedInvitationCode, PlatformIdentityError> {
        actor.authorize(scope, PlatformCapability::OperatorsManage)?;
        let capabilities = role.capabilities();
        if capabilities
            .iter()
            .any(|capability| actor.authorize(scope, *capability).is_err())
        {
            return Err(PlatformIdentityError::Forbidden);
        }
        let id = OperatorInvitationId::generate();
        let generated = self.crypto.generate_invitation(id)?;
        let invitation = NewInvitation {
            id,
            kind: InvitationKind::Operator,
            operator_name,
            grants: vec![OperatorGrant {
                scope,
                capabilities,
            }],
            created_by: Some(actor.operator.id),
            created_at: now,
            expires_at: add(now, self.policy.invitation_ttl)?,
            digest: generated.digest,
        };
        self.repository
            .create_invitation(actor, &invitation)
            .await?;
        Ok(generated)
    }

    /// Creates or reconciles a delegated invitation under one canonical operation identity.
    ///
    /// # Errors
    ///
    /// Rejects privilege escalation, operation reuse with different content, invalid input, or
    /// repository failures. A replay never reconstructs or reveals the invitation bearer.
    pub async fn create_invitation_idempotent(
        &self,
        actor: &OperatorContext,
        operation_id: OperationId,
        operator_name: OperatorName,
        scope: AccessScope,
        role: OperatorRole,
        now: TimestampMicros,
    ) -> Result<IdempotentInvitationResult, PlatformIdentityError> {
        let capabilities = authorize_invitation(actor, scope, role)?;
        let request_digest = invitation_request_digest(&operator_name, scope, &capabilities);
        if let Some(invitation) = self
            .repository
            .replay_invitation_operation(actor, operation_id, request_digest)
            .await?
        {
            return Ok(IdempotentInvitationResult::Replayed(invitation));
        }
        let id = OperatorInvitationId::generate();
        let generated = self.crypto.generate_invitation(id)?;
        let invitation = NewInvitation {
            id,
            kind: InvitationKind::Operator,
            operator_name,
            grants: vec![OperatorGrant {
                scope,
                capabilities,
            }],
            created_by: Some(actor.operator.id),
            created_at: now,
            expires_at: add(now, self.policy.invitation_ttl)?,
            digest: generated.digest,
        };
        match self
            .repository
            .create_invitation_idempotent(actor, operation_id, request_digest, &invitation)
            .await?
        {
            IdempotentInvitationCreate::Created => Ok(IdempotentInvitationResult::Created {
                invitation: invitation_metadata(&invitation, Some(operation_id))?,
                generated,
            }),
            IdempotentInvitationCreate::Replayed(invitation) => {
                Ok(IdempotentInvitationResult::Replayed(invitation))
            }
        }
    }

    /// Reconciles one prior invitation issuance operation without returning bearer material.
    ///
    /// # Errors
    ///
    /// Returns not found, forbidden, availability, or corruption failures.
    pub async fn invitation_by_operation(
        &self,
        actor: &OperatorContext,
        operation_id: OperationId,
    ) -> Result<OperatorInvitation, PlatformIdentityError> {
        self.repository
            .invitation_by_operation(actor, operation_id)
            .await
    }

    /// Irreversibly revokes one pending delegated invitation.
    ///
    /// # Errors
    ///
    /// Returns forbidden, not found, conflict after consumption, invalid time, or storage failure.
    pub async fn revoke_invitation(
        &self,
        actor: &OperatorContext,
        invitation_id: OperatorInvitationId,
        now: TimestampMicros,
    ) -> Result<bool, PlatformIdentityError> {
        self.repository
            .revoke_invitation(actor, invitation_id, now)
            .await
    }

    /// Exchanges a setup/invitation code for one operator and device session.
    ///
    /// # Errors
    ///
    /// Rejects malformed, wrong, consumed, revoked, or expired codes and unsafe device names.
    pub async fn login_with_invitation(
        &self,
        code: &InvitationCode,
        device_name: DeviceName,
        external_identity: Option<ExternalOperatorIdentity>,
        now: TimestampMicros,
    ) -> Result<LoginResult, PlatformIdentityError> {
        if let Some(identity) = &external_identity {
            identity.validate()?;
        }
        let operator_id = OperatorId::generate();
        let (session, access, refresh) = self.new_session(device_name, now)?;
        let candidate = ConsumedInvitation {
            operator_id,
            external_identity,
            session,
        };
        let digest = self.crypto.invitation_digest(code)?;
        let context = self
            .repository
            .consume_invitation(code.id(), digest, &candidate, now)
            .await?;
        Ok(LoginResult {
            access_token: access.token,
            refresh_token: refresh.token,
            context,
        })
    }

    /// Starts a device session for an identity already linked through a configured external `IdP`.
    ///
    /// # Errors
    ///
    /// Rejects unlinked identities, disabled operators, or repository/entropy failures.
    pub async fn login_with_external_identity(
        &self,
        identity: &ExternalOperatorIdentity,
        device_name: DeviceName,
        now: TimestampMicros,
    ) -> Result<LoginResult, PlatformIdentityError> {
        identity.validate()?;
        let (session, access, refresh) = self.new_session(device_name, now)?;
        let context = self
            .repository
            .login_external(identity, &session, now)
            .await?;
        Ok(LoginResult {
            access_token: access.token,
            refresh_token: refresh.token,
            context,
        })
    }

    /// Authenticates one Management API bearer and reloads current grants.
    ///
    /// # Errors
    ///
    /// Rejects malformed, wrong, expired, or revoked access tokens.
    pub async fn authenticate(
        &self,
        token: &AccessToken,
        now: TimestampMicros,
    ) -> Result<OperatorContext, PlatformIdentityError> {
        self.repository
            .authenticate_access(token.id(), self.crypto.access_digest(token)?, now)
            .await
    }

    /// Rotates both session bearers after verifying the current refresh token.
    ///
    /// # Errors
    ///
    /// Rejects replayed, wrong, expired, or revoked refresh tokens.
    pub async fn refresh(
        &self,
        token: &RefreshToken,
        now: TimestampMicros,
    ) -> Result<LoginResult, PlatformIdentityError> {
        let access = self.crypto.generate_access(token.id())?;
        let refresh = self.crypto.generate_refresh(token.id())?;
        let replacement = RefreshedSession {
            refreshed_at: now,
            access_expires_at: add(now, self.policy.access_ttl)?,
            refresh_expires_at: add(now, self.policy.refresh_ttl)?,
            access_digest: access.digest,
            refresh_digest: refresh.digest,
        };
        let presented = self.crypto.refresh_digest(token)?;
        let context = self
            .repository
            .refresh_session(token.id(), presented, &replacement)
            .await?;
        Ok(LoginResult {
            access_token: access.token,
            refresh_token: refresh.token,
            context,
        })
    }

    /// Revokes one device session. Operators may revoke their own sessions; managing another
    /// operator requires installation-level `operators:manage`.
    ///
    /// # Errors
    ///
    /// Returns forbidden, not found, invalid clock, or repository failures.
    pub async fn revoke_session(
        &self,
        actor: &OperatorContext,
        session_id: OperatorSessionId,
        now: TimestampMicros,
    ) -> Result<bool, PlatformIdentityError> {
        self.repository.revoke_session(actor, session_id, now).await
    }

    /// Lists non-secret metadata for every device session owned by the authenticated operator.
    ///
    /// # Errors
    ///
    /// Returns repository availability or corruption failures.
    pub async fn list_sessions(
        &self,
        actor: &OperatorContext,
    ) -> Result<Vec<crate::OperatorSession>, PlatformIdentityError> {
        self.repository.list_sessions(actor).await
    }

    /// Performs one bounded authoritative storage health query.
    ///
    /// # Errors
    ///
    /// Returns repository availability, compatibility, or corruption failures.
    pub async fn health(&self) -> Result<(), PlatformIdentityError> {
        self.repository.health().await
    }

    fn new_session(
        &self,
        device_name: DeviceName,
        now: TimestampMicros,
    ) -> Result<
        (
            NewOperatorSession,
            crate::GeneratedAccessToken,
            crate::GeneratedRefreshToken,
        ),
        PlatformIdentityError,
    > {
        let id = OperatorSessionId::generate();
        let access = self.crypto.generate_access(id)?;
        let refresh = self.crypto.generate_refresh(id)?;
        let session = NewOperatorSession {
            id,
            device_name,
            created_at: now,
            access_expires_at: add(now, self.policy.access_ttl)?,
            refresh_expires_at: add(now, self.policy.refresh_ttl)?,
            access_digest: access.digest,
            refresh_digest: refresh.digest,
        };
        Ok((session, access, refresh))
    }
}

fn add(
    timestamp: TimestampMicros,
    duration: Duration,
) -> Result<TimestampMicros, PlatformIdentityError> {
    if timestamp.get() < 0 {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let micros =
        i64::try_from(duration.as_micros()).map_err(|_| PlatformIdentityError::InvalidInput)?;
    let value = timestamp
        .get()
        .checked_add(micros)
        .ok_or(PlatformIdentityError::InvalidInput)?;
    Ok(TimestampMicros::new(value))
}

fn authorize_invitation(
    actor: &OperatorContext,
    scope: AccessScope,
    role: OperatorRole,
) -> Result<std::collections::BTreeSet<PlatformCapability>, PlatformIdentityError> {
    actor.authorize(scope, PlatformCapability::OperatorsManage)?;
    let capabilities = role.capabilities();
    if capabilities
        .iter()
        .any(|capability| actor.authorize(scope, *capability).is_err())
    {
        return Err(PlatformIdentityError::Forbidden);
    }
    Ok(capabilities)
}

fn invitation_request_digest(
    operator_name: &OperatorName,
    scope: AccessScope,
    capabilities: &std::collections::BTreeSet<PlatformCapability>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"runku-platform-invitation-request-v1\0");
    digest_field(&mut digest, operator_name.as_str());
    match scope {
        AccessScope::Installation => digest.update(b"installation\0"),
        AccessScope::Project(project) => {
            digest.update(b"project\0");
            digest_field(&mut digest, &project.to_string());
        }
        AccessScope::Environment(environment) => {
            digest.update(b"environment\0");
            digest_field(&mut digest, &environment.project_id().to_string());
            digest_field(&mut digest, &environment.environment_id().to_string());
        }
    }
    for capability in capabilities {
        digest_field(&mut digest, capability.as_str());
    }
    digest.finalize().into()
}

fn digest_field(digest: &mut Sha256, value: &str) {
    digest.update(value.as_bytes());
    digest.update([0]);
}

fn invitation_metadata(
    invitation: &NewInvitation,
    operation_id: Option<OperationId>,
) -> Result<OperatorInvitation, PlatformIdentityError> {
    Ok(OperatorInvitation {
        id: invitation.id,
        operation_id,
        operator_name: invitation.operator_name.clone(),
        grants: invitation.grants.clone(),
        status: InvitationStatus::Pending,
        created_by: invitation
            .created_by
            .ok_or(PlatformIdentityError::InvalidInput)?,
        created_at: invitation.created_at,
        expires_at: invitation.expires_at,
        consumed_at: None,
        revoked_at: None,
    })
}
