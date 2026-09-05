//! Durable, transaction-oriented Platform Identity repository contract.

use std::fmt;

use async_trait::async_trait;
use runku_core::{OperationId, OperatorId, OperatorInvitationId, OperatorSessionId};
use runku_value::TimestampMicros;

use crate::{
    DeviceName, ExternalOperatorIdentity, InvitationKind, OperatorContext, OperatorGrant,
    OperatorInvitation, OperatorName, OperatorSession, PlatformIdentityError, key::PlatformDigest,
};

/// Physical storage backend selected by composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlatformIdentityBackend {
    /// Embedded deterministic backend for tests and local composition.
    SQLite,
    /// Authoritative self-hosted backend.
    PostgreSQL,
}

/// Newly generated invitation persisted with its non-recoverable digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewInvitation {
    /// Stable invitation identity.
    pub id: OperatorInvitationId,
    /// Bootstrap or delegated operator invitation.
    pub kind: InvitationKind,
    /// Name assigned to the operator created by exchange.
    pub operator_name: OperatorName,
    /// Exact grants assigned atomically on exchange.
    pub grants: Vec<OperatorGrant>,
    /// Authenticated creator; absent only for server bootstrap.
    pub created_by: Option<OperatorId>,
    /// Server-owned creation time.
    pub created_at: TimestampMicros,
    /// Absolute single-use expiry.
    pub expires_at: TimestampMicros,
    pub(crate) digest: PlatformDigest,
}

/// Candidate session created during invitation/OIDC exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewOperatorSession {
    /// Stable session identity.
    pub id: OperatorSessionId,
    /// Device label.
    pub device_name: DeviceName,
    /// Creation and initial last-use timestamp.
    pub created_at: TimestampMicros,
    /// Short-lived access-token expiry.
    pub access_expires_at: TimestampMicros,
    /// Rotating refresh-token expiry.
    pub refresh_expires_at: TimestampMicros,
    pub(crate) access_digest: PlatformDigest,
    pub(crate) refresh_digest: PlatformDigest,
}

/// Complete candidate identity created by consuming one invitation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumedInvitation {
    /// Stable new operator identity.
    pub operator_id: OperatorId,
    /// Optional already-verified external identity to link atomically.
    pub external_identity: Option<ExternalOperatorIdentity>,
    /// First device session.
    pub session: NewOperatorSession,
}

/// Candidate identity, authoritative grants, and session supplied by a trusted OIDC gateway.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedExternalLogin {
    /// Stable identity allocated if this external subject is new.
    pub operator_id: OperatorId,
    /// Name used only when the external subject is first enrolled.
    pub operator_name: OperatorName,
    /// Already verified external identity.
    pub external_identity: ExternalOperatorIdentity,
    /// Complete authoritative grant set to reconcile for this subject.
    pub grants: Vec<OperatorGrant>,
    /// New device session.
    pub session: NewOperatorSession,
}

/// Atomic refresh replacement material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshedSession {
    /// Rotation timestamp and new last-use time.
    pub refreshed_at: TimestampMicros,
    /// New access-token expiry.
    pub access_expires_at: TimestampMicros,
    /// New refresh-token expiry.
    pub refresh_expires_at: TimestampMicros,
    pub(crate) access_digest: PlatformDigest,
    pub(crate) refresh_digest: PlatformDigest,
}

/// Outcome of idempotent startup bootstrap initialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapCreate {
    /// This call persisted the pending first-owner invitation.
    Created,
    /// The exact pending bootstrap invitation already exists.
    Replayed,
}

/// Result of atomically applying one idempotent invitation issuance operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdempotentInvitationCreate {
    /// The operation committed a newly generated invitation.
    Created,
    /// The exact operation already committed; no bearer can be revealed again.
    Replayed(OperatorInvitation),
}

/// Aggregate non-sensitive repository counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlatformIdentityTelemetrySnapshot {
    /// Initial bootstrap invitations created.
    pub bootstraps_created: u64,
    /// Delegated invitations created.
    pub invitations_created: u64,
    /// Exact delegated invitation operations replayed without revealing bearer material.
    pub invitation_replays: u64,
    /// Invitations successfully consumed.
    pub invitations_consumed: u64,
    /// Pending delegated invitations irreversibly revoked.
    pub invitations_revoked: u64,
    /// Access tokens successfully authenticated.
    pub authentications: u64,
    /// Failed authentication attempts.
    pub authentication_failures: u64,
    /// Refresh tokens successfully rotated.
    pub refreshes: u64,
    /// Sessions revoked.
    pub sessions_revoked: u64,
    /// Retryable repository failures.
    pub retryable_errors: u64,
}

/// Authoritative transactional storage boundary for human operator identity.
#[async_trait]
pub trait PlatformIdentityRepository: fmt::Debug + Send + Sync {
    /// Physical backend.
    fn backend(&self) -> PlatformIdentityBackend;

    /// Creates the only pending bootstrap invitation while no operator exists.
    async fn create_bootstrap(
        &self,
        invitation: &NewInvitation,
    ) -> Result<BootstrapCreate, PlatformIdentityError>;

    /// Replaces every pending bootstrap while no operator exists.
    ///
    /// This is an explicit local recovery boundary for a lost protected bootstrap file. The
    /// replacement and revocation of the previous credential commit atomically.
    async fn replace_bootstrap(
        &self,
        invitation: &NewInvitation,
    ) -> Result<(), PlatformIdentityError>;

    /// Creates an invitation after checking that the actor can delegate every requested grant.
    async fn create_invitation(
        &self,
        actor: &OperatorContext,
        invitation: &NewInvitation,
    ) -> Result<(), PlatformIdentityError>;

    /// Creates or reconciles one invitation under a durable operation identity.
    async fn create_invitation_idempotent(
        &self,
        actor: &OperatorContext,
        operation_id: OperationId,
        request_digest: [u8; 32],
        invitation: &NewInvitation,
    ) -> Result<IdempotentInvitationCreate, PlatformIdentityError>;

    /// Checks for an exact committed operation before new bearer generation.
    async fn replay_invitation_operation(
        &self,
        actor: &OperatorContext,
        operation_id: OperationId,
        request_digest: [u8; 32],
    ) -> Result<Option<OperatorInvitation>, PlatformIdentityError>;

    /// Loads non-secret invitation metadata for one committed issuance operation.
    async fn invitation_by_operation(
        &self,
        actor: &OperatorContext,
        operation_id: OperationId,
    ) -> Result<OperatorInvitation, PlatformIdentityError>;

    /// Irreversibly revokes one pending delegated invitation.
    async fn revoke_invitation(
        &self,
        actor: &OperatorContext,
        invitation_id: OperatorInvitationId,
        now: TimestampMicros,
    ) -> Result<bool, PlatformIdentityError>;

    /// Verifies and consumes an invitation, creating operator, grants, identity, and session in one
    /// transaction.
    async fn consume_invitation(
        &self,
        invitation_id: OperatorInvitationId,
        presented_digest: PlatformDigest,
        candidate: &ConsumedInvitation,
        now: TimestampMicros,
    ) -> Result<OperatorContext, PlatformIdentityError>;

    /// Creates a session for an already-linked verified external identity.
    async fn login_external(
        &self,
        identity: &ExternalOperatorIdentity,
        session: &NewOperatorSession,
        now: TimestampMicros,
    ) -> Result<OperatorContext, PlatformIdentityError>;

    /// Creates or reconciles a trusted managed external identity and starts one session atomically.
    async fn login_external_managed(
        &self,
        candidate: &ManagedExternalLogin,
        now: TimestampMicros,
    ) -> Result<OperatorContext, PlatformIdentityError>;

    /// Resolves one current access token and loads current grants.
    async fn authenticate_access(
        &self,
        session_id: OperatorSessionId,
        presented_digest: PlatformDigest,
        now: TimestampMicros,
    ) -> Result<OperatorContext, PlatformIdentityError>;

    /// Atomically rotates access and refresh credentials after verifying the current refresh token.
    async fn refresh_session(
        &self,
        session_id: OperatorSessionId,
        presented_digest: PlatformDigest,
        replacement: &RefreshedSession,
    ) -> Result<OperatorContext, PlatformIdentityError>;

    /// Irreversibly revokes one session after current authorization is checked.
    async fn revoke_session(
        &self,
        actor: &OperatorContext,
        session_id: OperatorSessionId,
        now: TimestampMicros,
    ) -> Result<bool, PlatformIdentityError>;

    /// Returns non-secret session metadata visible to the owning operator.
    async fn list_sessions(
        &self,
        actor: &OperatorContext,
    ) -> Result<Vec<OperatorSession>, PlatformIdentityError>;

    /// Performs one bounded backend health query.
    async fn health(&self) -> Result<(), PlatformIdentityError>;

    /// Returns race-tolerant process-local counters.
    fn telemetry(&self) -> PlatformIdentityTelemetrySnapshot;

    /// Closes pooled resources.
    async fn close(&self);
}
