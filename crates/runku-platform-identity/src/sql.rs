//! PostgreSQL-authoritative Platform Identity repository with SQLite conformance support.

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    str::FromStr as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{
    EnvironmentId, EnvironmentScope, OperationId, OperatorId, OperatorInvitationId,
    OperatorSessionId, ProjectId,
};
use runku_value::TimestampMicros;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sqlx::{
    Any, AnyPool, Executor as _, Row as _, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

use crate::{
    AccessScope, BootstrapCreate, ConsumedInvitation, ExternalOperatorIdentity,
    IdempotentInvitationCreate, InvitationKind, InvitationStatus, NewInvitation,
    NewOperatorSession, Operator, OperatorContext, OperatorGrant, OperatorInvitation, OperatorName,
    OperatorSession, OperatorStatus, PlatformCapability, PlatformIdentityBackend,
    PlatformIdentityError, PlatformIdentityRepository, PlatformIdentityTelemetrySnapshot,
    RefreshedSession, SessionStatus, key::PlatformDigest,
};

const SCHEMA_VERSION: i64 = 2;
const MIGRATION_LOCK: i64 = 7_251_204_431;
const SCHEMA_V1: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS runku_platform_meta (singleton_id BIGINT PRIMARY KEY, initialized BOOLEAN NOT NULL, authorization_revision BIGINT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS runku_operators (operator_id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL, created_at_micros BIGINT NOT NULL, authorization_revision BIGINT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS runku_operator_identities (provider_id TEXT NOT NULL, subject_id TEXT NOT NULL, operator_id TEXT NOT NULL, created_at_micros BIGINT NOT NULL, PRIMARY KEY(provider_id, subject_id), FOREIGN KEY(operator_id) REFERENCES runku_operators(operator_id) ON DELETE CASCADE)",
    "CREATE TABLE IF NOT EXISTS runku_operator_grants (operator_id TEXT NOT NULL, scope_key TEXT NOT NULL, scope_kind TEXT NOT NULL, project_id TEXT NULL, environment_id TEXT NULL, capability TEXT NOT NULL, created_at_micros BIGINT NOT NULL, created_by TEXT NULL, PRIMARY KEY(operator_id, scope_key, capability), FOREIGN KEY(operator_id) REFERENCES runku_operators(operator_id) ON DELETE CASCADE)",
    "CREATE TABLE IF NOT EXISTS runku_operator_invitations (invitation_id TEXT PRIMARY KEY, kind TEXT NOT NULL, operator_name TEXT NOT NULL, grants_json BYTEA NOT NULL, digest BYTEA NOT NULL, status TEXT NOT NULL, created_by TEXT NULL, created_at_micros BIGINT NOT NULL, expires_at_micros BIGINT NOT NULL, consumed_at_micros BIGINT NULL)",
    "CREATE INDEX IF NOT EXISTS runku_invitations_by_status ON runku_operator_invitations(kind, status, expires_at_micros)",
    "CREATE TABLE IF NOT EXISTS runku_operator_sessions (session_id TEXT PRIMARY KEY, operator_id TEXT NOT NULL, device_name TEXT NOT NULL, access_digest BYTEA NOT NULL, refresh_digest BYTEA NOT NULL, status TEXT NOT NULL, created_at_micros BIGINT NOT NULL, last_used_at_micros BIGINT NOT NULL, access_expires_at_micros BIGINT NOT NULL, refresh_expires_at_micros BIGINT NOT NULL, revoked_at_micros BIGINT NULL, FOREIGN KEY(operator_id) REFERENCES runku_operators(operator_id) ON DELETE CASCADE)",
    "CREATE INDEX IF NOT EXISTS runku_sessions_by_operator ON runku_operator_sessions(operator_id, status, session_id)",
    "CREATE TABLE IF NOT EXISTS runku_platform_audit (event_id TEXT PRIMARY KEY, actor_operator_id TEXT NULL, subject_operator_id TEXT NULL, operation TEXT NOT NULL, outcome TEXT NOT NULL, occurred_at_micros BIGINT NOT NULL)",
];
const SCHEMA_V2: &[&str] = &[
    "ALTER TABLE runku_operator_invitations ADD COLUMN revoked_at_micros BIGINT NULL",
    "CREATE TABLE runku_operator_invitation_operations (operation_id TEXT PRIMARY KEY, request_digest BYTEA NOT NULL, invitation_id TEXT NOT NULL UNIQUE, scope_kind TEXT NOT NULL, project_id TEXT NULL, environment_id TEXT NULL, created_by TEXT NOT NULL, created_at_micros BIGINT NOT NULL, CHECK (length(request_digest) = 32), CHECK (created_at_micros >= 0), CHECK ((scope_kind = 'installation' AND project_id IS NULL AND environment_id IS NULL) OR (scope_kind = 'project' AND project_id IS NOT NULL AND environment_id IS NULL) OR (scope_kind = 'environment' AND project_id IS NOT NULL AND environment_id IS NOT NULL)), FOREIGN KEY(invitation_id) REFERENCES runku_operator_invitations(invitation_id), FOREIGN KEY(created_by) REFERENCES runku_operators(operator_id))",
    "CREATE INDEX runku_invitation_operations_by_creator ON runku_operator_invitation_operations(created_by, operation_id)",
    "ALTER TABLE runku_platform_audit ADD COLUMN request_operation_id TEXT NULL",
    "ALTER TABLE runku_platform_audit ADD COLUMN subject_invitation_id TEXT NULL",
    "CREATE INDEX runku_platform_audit_by_invitation ON runku_platform_audit(subject_invitation_id, occurred_at_micros, event_id)",
];

/// Operational role selected for repository composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlatformIdentityRepositoryRole {
    /// Local/test SQLite semantics.
    Local,
    /// Authoritative PostgreSQL 16+ storage.
    Authoritative,
}

/// Bounded connection-pool policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlatformIdentityRepositoryConfig {
    /// Declared operational role.
    pub role: PlatformIdentityRepositoryRole,
    /// Maximum connections.
    pub max_connections: u32,
    /// Maximum pool acquisition wait.
    pub acquire_timeout: Duration,
}

impl PlatformIdentityRepositoryConfig {
    /// Deterministic SQLite test/local policy.
    pub const LOCAL: Self = Self {
        role: PlatformIdentityRepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };
    /// Bounded authoritative PostgreSQL policy.
    pub const AUTHORITATIVE: Self = Self {
        role: PlatformIdentityRepositoryRole::Authoritative,
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
    };
}

#[derive(Debug, Default)]
struct Counters {
    bootstraps_created: AtomicU64,
    invitations_created: AtomicU64,
    invitation_replays: AtomicU64,
    invitations_consumed: AtomicU64,
    invitations_revoked: AtomicU64,
    authentications: AtomicU64,
    authentication_failures: AtomicU64,
    refreshes: AtomicU64,
    sessions_revoked: AtomicU64,
    retryable_errors: AtomicU64,
}

/// Durable SQL Platform Identity repository.
#[derive(Clone, Debug)]
pub struct SqlPlatformIdentityRepository {
    pool: AnyPool,
    backend: PlatformIdentityBackend,
    counters: Arc<Counters>,
}

impl SqlPlatformIdentityRepository {
    /// Opens SQLite and applies checksum-protected schema initialization.
    ///
    /// # Errors
    ///
    /// Rejects an incompatible role, URL, pool policy, configuration, or schema.
    pub async fn connect_sqlite(
        url: &str,
        config: PlatformIdentityRepositoryConfig,
    ) -> Result<Self, PlatformIdentityError> {
        if config.role != PlatformIdentityRepositoryRole::Local || !url.starts_with("sqlite:") {
            return Err(PlatformIdentityError::Unsupported);
        }
        Self::connect(url, config, PlatformIdentityBackend::SQLite).await
    }

    /// Opens authoritative PostgreSQL 16+ and applies checksum-protected schema initialization.
    ///
    /// # Errors
    ///
    /// Rejects an incompatible role, URL, pool policy, database version, or schema.
    pub async fn connect_postgres(
        url: &str,
        config: PlatformIdentityRepositoryConfig,
    ) -> Result<Self, PlatformIdentityError> {
        if config.role != PlatformIdentityRepositoryRole::Authoritative
            || !(url.starts_with("postgres://") || url.starts_with("postgresql://"))
        {
            return Err(PlatformIdentityError::Unsupported);
        }
        Self::connect(url, config, PlatformIdentityBackend::PostgreSQL).await
    }

    async fn connect(
        url: &str,
        config: PlatformIdentityRepositoryConfig,
        backend: PlatformIdentityBackend,
    ) -> Result<Self, PlatformIdentityError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || backend == PlatformIdentityBackend::SQLite && config.max_connections != 1
        {
            return Err(PlatformIdentityError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options =
            AnyConnectOptions::from_str(url).map_err(|_| PlatformIdentityError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    match backend {
                        PlatformIdentityBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        PlatformIdentityBackend::PostgreSQL => {
                            connection.execute("SET statement_timeout = '30s'").await?;
                            connection.execute("SET lock_timeout = '5s'").await?;
                            connection
                                .execute("SET idle_in_transaction_session_timeout = '30s'")
                                .await?;
                        }
                    }
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(map_sqlx_error)?;
        if backend == PlatformIdentityBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(PlatformIdentityError::Unsupported);
            }
        }
        migrate(&pool, backend).await?;
        Ok(Self {
            pool,
            backend,
            counters: Arc::new(Counters::default()),
        })
    }

    fn track<T>(
        &self,
        result: Result<T, PlatformIdentityError>,
    ) -> Result<T, PlatformIdentityError> {
        if result.as_ref().is_err_and(|error| error.retryable()) {
            self.counters
                .retryable_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

#[async_trait]
impl PlatformIdentityRepository for SqlPlatformIdentityRepository {
    fn backend(&self) -> PlatformIdentityBackend {
        self.backend
    }

    async fn create_bootstrap(
        &self,
        invitation: &NewInvitation,
    ) -> Result<BootstrapCreate, PlatformIdentityError> {
        let result = create_bootstrap(&self.pool, self.backend, invitation).await;
        if matches!(result, Ok(BootstrapCreate::Created)) {
            self.counters
                .bootstraps_created
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn replace_bootstrap(
        &self,
        invitation: &NewInvitation,
    ) -> Result<(), PlatformIdentityError> {
        let result = replace_bootstrap(&self.pool, self.backend, invitation).await;
        if result.is_ok() {
            self.counters
                .bootstraps_created
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn create_invitation(
        &self,
        actor: &OperatorContext,
        invitation: &NewInvitation,
    ) -> Result<(), PlatformIdentityError> {
        let result = create_invitation(&self.pool, self.backend, actor, invitation).await;
        if result.is_ok() {
            self.counters
                .invitations_created
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn create_invitation_idempotent(
        &self,
        actor: &OperatorContext,
        operation_id: OperationId,
        request_digest: [u8; 32],
        invitation: &NewInvitation,
    ) -> Result<IdempotentInvitationCreate, PlatformIdentityError> {
        let result = create_invitation_idempotent(
            &self.pool,
            self.backend,
            actor,
            operation_id,
            request_digest,
            invitation,
        )
        .await;
        match result {
            Ok(IdempotentInvitationCreate::Created) => {
                self.counters
                    .invitations_created
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(IdempotentInvitationCreate::Replayed(_)) => {
                self.counters
                    .invitation_replays
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {}
        }
        self.track(result)
    }

    async fn replay_invitation_operation(
        &self,
        actor: &OperatorContext,
        operation_id: OperationId,
        request_digest: [u8; 32],
    ) -> Result<Option<OperatorInvitation>, PlatformIdentityError> {
        let result = replay_invitation_operation(
            &self.pool,
            self.backend,
            actor,
            operation_id,
            request_digest,
        )
        .await;
        if matches!(result, Ok(Some(_))) {
            self.counters
                .invitation_replays
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn invitation_by_operation(
        &self,
        actor: &OperatorContext,
        operation_id: OperationId,
    ) -> Result<OperatorInvitation, PlatformIdentityError> {
        self.track(invitation_by_operation(&self.pool, self.backend, actor, operation_id).await)
    }

    async fn revoke_invitation(
        &self,
        actor: &OperatorContext,
        invitation_id: OperatorInvitationId,
        now: TimestampMicros,
    ) -> Result<bool, PlatformIdentityError> {
        let result = revoke_invitation(&self.pool, self.backend, actor, invitation_id, now).await;
        if matches!(result, Ok(true)) {
            self.counters
                .invitations_revoked
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn consume_invitation(
        &self,
        invitation_id: OperatorInvitationId,
        presented_digest: PlatformDigest,
        candidate: &ConsumedInvitation,
        now: TimestampMicros,
    ) -> Result<OperatorContext, PlatformIdentityError> {
        let result = consume_invitation(
            &self.pool,
            self.backend,
            invitation_id,
            presented_digest,
            candidate,
            now,
        )
        .await;
        if result.is_ok() {
            self.counters
                .invitations_consumed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters
                .authentication_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn login_external(
        &self,
        identity: &ExternalOperatorIdentity,
        session: &NewOperatorSession,
        now: TimestampMicros,
    ) -> Result<OperatorContext, PlatformIdentityError> {
        self.track(login_external(&self.pool, self.backend, identity, session, now).await)
    }

    async fn authenticate_access(
        &self,
        session_id: OperatorSessionId,
        presented_digest: PlatformDigest,
        now: TimestampMicros,
    ) -> Result<OperatorContext, PlatformIdentityError> {
        let result = authenticate_access(&self.pool, session_id, presented_digest, now).await;
        if result.is_ok() {
            self.counters
                .authentications
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters
                .authentication_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn refresh_session(
        &self,
        session_id: OperatorSessionId,
        presented_digest: PlatformDigest,
        replacement: &RefreshedSession,
    ) -> Result<OperatorContext, PlatformIdentityError> {
        let result = refresh_session(
            &self.pool,
            self.backend,
            session_id,
            presented_digest,
            replacement,
        )
        .await;
        if result.is_ok() {
            self.counters.refreshes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters
                .authentication_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn revoke_session(
        &self,
        actor: &OperatorContext,
        session_id: OperatorSessionId,
        now: TimestampMicros,
    ) -> Result<bool, PlatformIdentityError> {
        let result = revoke_session(&self.pool, self.backend, actor, session_id, now).await;
        if matches!(result, Ok(true)) {
            self.counters
                .sessions_revoked
                .fetch_add(1, Ordering::Relaxed);
        }
        self.track(result)
    }

    async fn list_sessions(
        &self,
        actor: &OperatorContext,
    ) -> Result<Vec<OperatorSession>, PlatformIdentityError> {
        self.track(list_sessions(&self.pool, actor).await)
    }

    async fn health(&self) -> Result<(), PlatformIdentityError> {
        self.track(
            sqlx::query_scalar::<_, i64>("SELECT 1")
                .fetch_one(&self.pool)
                .await
                .map(|_| ())
                .map_err(map_sqlx_error),
        )
    }

    fn telemetry(&self) -> PlatformIdentityTelemetrySnapshot {
        PlatformIdentityTelemetrySnapshot {
            bootstraps_created: self.counters.bootstraps_created.load(Ordering::Relaxed),
            invitations_created: self.counters.invitations_created.load(Ordering::Relaxed),
            invitation_replays: self.counters.invitation_replays.load(Ordering::Relaxed),
            invitations_consumed: self.counters.invitations_consumed.load(Ordering::Relaxed),
            invitations_revoked: self.counters.invitations_revoked.load(Ordering::Relaxed),
            authentications: self.counters.authentications.load(Ordering::Relaxed),
            authentication_failures: self
                .counters
                .authentication_failures
                .load(Ordering::Relaxed),
            refreshes: self.counters.refreshes.load(Ordering::Relaxed),
            sessions_revoked: self.counters.sessions_revoked.load(Ordering::Relaxed),
            retryable_errors: self.counters.retryable_errors.load(Ordering::Relaxed),
        }
    }

    async fn close(&self) {
        self.pool.close().await;
    }
}

async fn create_bootstrap(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    invitation: &NewInvitation,
) -> Result<BootstrapCreate, PlatformIdentityError> {
    validate_invitation(invitation)?;
    if invitation.kind != InvitationKind::Bootstrap
        || invitation.created_by.is_some()
        || invitation.grants.len() != 1
        || invitation.grants[0].scope != AccessScope::Installation
        || invitation.grants[0].capabilities != PlatformCapability::owner_set()
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let operators = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runku_operators")
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    if operators != 0 {
        return Err(PlatformIdentityError::AlreadyInitialized);
    }
    sqlx::query("UPDATE runku_operator_invitations SET status = 'revoked' WHERE kind = 'bootstrap' AND status = 'pending' AND expires_at_micros <= $1")
        .bind(invitation.created_at.get()).execute(&mut *tx).await.map_err(map_sqlx_error)?;
    let pending = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runku_operator_invitations WHERE kind = 'bootstrap' AND status = 'pending'")
        .fetch_one(&mut *tx).await.map_err(map_sqlx_error)?;
    if pending != 0 {
        tx.commit()
            .await
            .map_err(|error| map_commit_error(&error))?;
        return Ok(BootstrapCreate::Replayed);
    }
    insert_invitation(&mut tx, invitation).await?;
    audit(
        &mut tx,
        None,
        None,
        "bootstrap.create",
        invitation.created_at,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(BootstrapCreate::Created)
}

async fn replace_bootstrap(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    invitation: &NewInvitation,
) -> Result<(), PlatformIdentityError> {
    validate_invitation(invitation)?;
    if invitation.kind != InvitationKind::Bootstrap
        || invitation.created_by.is_some()
        || invitation.grants.len() != 1
        || invitation.grants[0].scope != AccessScope::Installation
        || invitation.grants[0].capabilities != PlatformCapability::owner_set()
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let operators = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runku_operators")
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    if operators != 0 {
        return Err(PlatformIdentityError::AlreadyInitialized);
    }
    sqlx::query("UPDATE runku_operator_invitations SET status = 'revoked' WHERE kind = 'bootstrap' AND status = 'pending'")
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    insert_invitation(&mut tx, invitation).await?;
    audit(
        &mut tx,
        None,
        None,
        "bootstrap.recover",
        invitation.created_at,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(())
}

async fn create_invitation(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    actor: &OperatorContext,
    invitation: &NewInvitation,
) -> Result<(), PlatformIdentityError> {
    validate_invitation(invitation)?;
    if invitation.kind != InvitationKind::Operator
        || invitation.created_by != Some(actor.operator.id)
        || invitation.grants.len() != 1
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let fresh = load_context_tx(&mut tx, actor.session.id).await?;
    if fresh.operator.authorization_revision != actor.operator.authorization_revision {
        return Err(PlatformIdentityError::Forbidden);
    }
    for grant in &invitation.grants {
        fresh.authorize(grant.scope, PlatformCapability::OperatorsManage)?;
        if grant
            .capabilities
            .iter()
            .any(|capability| fresh.authorize(grant.scope, *capability).is_err())
        {
            return Err(PlatformIdentityError::Forbidden);
        }
    }
    insert_invitation(&mut tx, invitation).await?;
    audit(
        &mut tx,
        Some(actor.operator.id),
        None,
        "invitation.create",
        invitation.created_at,
    )
    .await?;
    tx.commit().await.map_err(|error| map_commit_error(&error))
}

async fn create_invitation_idempotent(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    actor: &OperatorContext,
    operation_id: OperationId,
    request_digest: [u8; 32],
    invitation: &NewInvitation,
) -> Result<IdempotentInvitationCreate, PlatformIdentityError> {
    validate_invitation(invitation)?;
    if invitation.kind != InvitationKind::Operator
        || invitation.created_by != Some(actor.operator.id)
        || invitation.grants.len() != 1
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let fresh = load_context_tx(&mut tx, actor.session.id).await?;
    if fresh.operator.authorization_revision != actor.operator.authorization_revision {
        return Err(PlatformIdentityError::Forbidden);
    }
    authorize_invitation_grants(&fresh, &invitation.grants)?;
    let prior = sqlx::query(
        "SELECT request_digest, invitation_id FROM runku_operator_invitation_operations WHERE operation_id = $1",
    )
    .bind(operation_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    if let Some(prior) = prior {
        let stored: Vec<u8> = prior
            .try_get("request_digest")
            .map_err(|_| PlatformIdentityError::Corruption)?;
        if decode_sha256(stored)? != request_digest {
            return Err(PlatformIdentityError::InvitationOperationReused);
        }
        let invitation_id = text(&prior, "invitation_id")?
            .parse::<OperatorInvitationId>()
            .map_err(|_| PlatformIdentityError::Corruption)?;
        let invitation = load_invitation_tx(&mut tx, invitation_id).await?;
        tx.commit()
            .await
            .map_err(|error| map_commit_error(&error))?;
        return Ok(IdempotentInvitationCreate::Replayed(invitation));
    }
    insert_invitation(&mut tx, invitation).await?;
    let grant = invitation
        .grants
        .first()
        .ok_or(PlatformIdentityError::InvalidInput)?;
    let (_, scope_kind, project_id, environment_id) = encode_scope(grant.scope);
    sqlx::query("INSERT INTO runku_operator_invitation_operations (operation_id, request_digest, invitation_id, scope_kind, project_id, environment_id, created_by, created_at_micros) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
        .bind(operation_id.to_string())
        .bind(request_digest.to_vec())
        .bind(invitation.id.to_string())
        .bind(scope_kind)
        .bind(project_id)
        .bind(environment_id)
        .bind(actor.operator.id.to_string())
        .bind(invitation.created_at.get())
        .execute(&mut *tx)
        .await
        .map_err(map_constraint_error)?;
    audit_invitation(
        &mut tx,
        actor.operator.id,
        "invitation.create",
        Some(operation_id),
        invitation.id,
        invitation.created_at,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(IdempotentInvitationCreate::Created)
}

async fn invitation_by_operation(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    actor: &OperatorContext,
    operation_id: OperationId,
) -> Result<OperatorInvitation, PlatformIdentityError> {
    let mut tx = begin_write(pool, backend).await?;
    let current = load_context_tx(&mut tx, actor.session.id).await?;
    if current.operator.authorization_revision != actor.operator.authorization_revision {
        return Err(PlatformIdentityError::Forbidden);
    }
    let invitation_id = sqlx::query_scalar::<_, String>(
        "SELECT invitation_id FROM runku_operator_invitation_operations WHERE operation_id = $1",
    )
    .bind(operation_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?
    .ok_or(PlatformIdentityError::NotFound)?
    .parse::<OperatorInvitationId>()
    .map_err(|_| PlatformIdentityError::Corruption)?;
    let invitation = load_invitation_tx(&mut tx, invitation_id).await?;
    authorize_invitation_management(&current, &invitation)?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(invitation)
}

async fn replay_invitation_operation(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    actor: &OperatorContext,
    operation_id: OperationId,
    request_digest: [u8; 32],
) -> Result<Option<OperatorInvitation>, PlatformIdentityError> {
    let mut tx = begin_write(pool, backend).await?;
    let current = load_context_tx(&mut tx, actor.session.id).await?;
    if current.operator.authorization_revision != actor.operator.authorization_revision {
        return Err(PlatformIdentityError::Forbidden);
    }
    let prior = sqlx::query(
        "SELECT request_digest, invitation_id FROM runku_operator_invitation_operations WHERE operation_id = $1",
    )
    .bind(operation_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    let Some(prior) = prior else {
        tx.commit()
            .await
            .map_err(|error| map_commit_error(&error))?;
        return Ok(None);
    };
    let stored: Vec<u8> = prior
        .try_get("request_digest")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    if decode_sha256(stored)? != request_digest {
        return Err(PlatformIdentityError::InvitationOperationReused);
    }
    let invitation_id = text(&prior, "invitation_id")?
        .parse::<OperatorInvitationId>()
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let invitation = load_invitation_tx(&mut tx, invitation_id).await?;
    authorize_invitation_grants(&current, &invitation.grants)?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(Some(invitation))
}

async fn revoke_invitation(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    actor: &OperatorContext,
    invitation_id: OperatorInvitationId,
    now: TimestampMicros,
) -> Result<bool, PlatformIdentityError> {
    if now.get() < 0 {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let current = load_context_tx(&mut tx, actor.session.id).await?;
    if current.operator.authorization_revision != actor.operator.authorization_revision {
        return Err(PlatformIdentityError::Forbidden);
    }
    let invitation = load_invitation_tx(&mut tx, invitation_id).await?;
    authorize_invitation_management(&current, &invitation)?;
    match invitation.status {
        InvitationStatus::Consumed => return Err(PlatformIdentityError::Conflict),
        InvitationStatus::Revoked => {
            tx.commit()
                .await
                .map_err(|error| map_commit_error(&error))?;
            return Ok(false);
        }
        InvitationStatus::Pending | InvitationStatus::Expired => {}
    }
    let changed = sqlx::query("UPDATE runku_operator_invitations SET status = 'revoked', revoked_at_micros = $1 WHERE invitation_id = $2 AND status = 'pending'")
        .bind(now.get())
        .bind(invitation_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    if changed.rows_affected() != 1 {
        return Err(PlatformIdentityError::Conflict);
    }
    audit_invitation(
        &mut tx,
        actor.operator.id,
        "invitation.revoke",
        invitation.operation_id,
        invitation_id,
        now,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(true)
}

fn authorize_invitation_grants(
    actor: &OperatorContext,
    grants: &[OperatorGrant],
) -> Result<(), PlatformIdentityError> {
    for grant in grants {
        actor.authorize(grant.scope, PlatformCapability::OperatorsManage)?;
        if grant
            .capabilities
            .iter()
            .any(|capability| actor.authorize(grant.scope, *capability).is_err())
        {
            return Err(PlatformIdentityError::Forbidden);
        }
    }
    Ok(())
}

fn authorize_invitation_management(
    actor: &OperatorContext,
    invitation: &OperatorInvitation,
) -> Result<(), PlatformIdentityError> {
    for grant in &invitation.grants {
        if actor
            .authorize(grant.scope, PlatformCapability::OperatorsManage)
            .is_err()
        {
            return Err(PlatformIdentityError::NotFound);
        }
    }
    Ok(())
}

async fn consume_invitation(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    invitation_id: OperatorInvitationId,
    presented_digest: PlatformDigest,
    candidate: &ConsumedInvitation,
    now: TimestampMicros,
) -> Result<OperatorContext, PlatformIdentityError> {
    validate_new_session(&candidate.session)?;
    if now.get() < 0 || candidate.session.created_at != now {
        return Err(PlatformIdentityError::InvalidInput);
    }
    if let Some(identity) = &candidate.external_identity {
        identity.validate()?;
    }
    let mut tx = begin_write(pool, backend).await?;
    let row = sqlx::query("SELECT kind, operator_name, grants_json, digest, status, expires_at_micros FROM runku_operator_invitations WHERE invitation_id = $1")
        .bind(invitation_id.to_string()).fetch_optional(&mut *tx).await.map_err(map_sqlx_error)?
        .ok_or(PlatformIdentityError::Unauthenticated)?;
    let status: String = row
        .try_get("status")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let expires_at: i64 = row
        .try_get("expires_at_micros")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let stored = decode_digest(
        row.try_get("digest")
            .map_err(|_| PlatformIdentityError::Corruption)?,
    )?;
    if status != "pending" || expires_at <= now.get() || !stored.matches(presented_digest) {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    let kind: String = row
        .try_get("kind")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    if kind == "bootstrap" {
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runku_operators")
            .fetch_one(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        if count != 0 {
            return Err(PlatformIdentityError::AlreadyInitialized);
        }
    } else if kind != "operator" {
        return Err(PlatformIdentityError::Corruption);
    }
    let name: String = row
        .try_get("operator_name")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let name = name
        .parse::<OperatorName>()
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let grants_json: Vec<u8> = row
        .try_get("grants_json")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let grants = decode_grants(&grants_json)?;
    sqlx::query("INSERT INTO runku_operators (operator_id, name, status, created_at_micros, authorization_revision) VALUES ($1, $2, 'active', $3, 1)")
        .bind(candidate.operator_id.to_string()).bind(name.as_str()).bind(now.get())
        .execute(&mut *tx).await.map_err(map_constraint_error)?;
    for grant in &grants {
        insert_grant(&mut tx, candidate.operator_id, grant, now, None).await?;
    }
    if let Some(identity) = &candidate.external_identity {
        sqlx::query("INSERT INTO runku_operator_identities (provider_id, subject_id, operator_id, created_at_micros) VALUES ($1, $2, $3, $4)")
            .bind(&identity.provider_id).bind(&identity.subject_id).bind(candidate.operator_id.to_string())
            .bind(now.get()).execute(&mut *tx).await.map_err(map_constraint_error)?;
    }
    insert_session(&mut tx, candidate.operator_id, &candidate.session).await?;
    let changed = sqlx::query("UPDATE runku_operator_invitations SET status = 'consumed', consumed_at_micros = $1 WHERE invitation_id = $2 AND status = 'pending'")
        .bind(now.get()).bind(invitation_id.to_string()).execute(&mut *tx).await.map_err(map_sqlx_error)?;
    if changed.rows_affected() != 1 {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    sqlx::query("UPDATE runku_platform_meta SET initialized = TRUE, authorization_revision = authorization_revision + 1 WHERE singleton_id = 1")
        .execute(&mut *tx).await.map_err(map_sqlx_error)?;
    audit(
        &mut tx,
        Some(candidate.operator_id),
        Some(candidate.operator_id),
        "invitation.consume",
        now,
    )
    .await?;
    let context = load_context_tx(&mut tx, candidate.session.id).await?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(context)
}

async fn login_external(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    identity: &ExternalOperatorIdentity,
    session: &NewOperatorSession,
    now: TimestampMicros,
) -> Result<OperatorContext, PlatformIdentityError> {
    identity.validate()?;
    validate_new_session(session)?;
    if session.created_at != now {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let operator_id = sqlx::query_scalar::<_, String>("SELECT operator_id FROM runku_operator_identities WHERE provider_id = $1 AND subject_id = $2")
        .bind(&identity.provider_id).bind(&identity.subject_id).fetch_optional(&mut *tx).await
        .map_err(map_sqlx_error)?.ok_or(PlatformIdentityError::Unauthenticated)?
        .parse::<OperatorId>().map_err(|_| PlatformIdentityError::Corruption)?;
    insert_session(&mut tx, operator_id, session).await?;
    audit(
        &mut tx,
        Some(operator_id),
        Some(operator_id),
        "oidc.login",
        now,
    )
    .await?;
    let context = load_context_tx(&mut tx, session.id).await?;
    if context.operator.status != OperatorStatus::Active {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(context)
}

async fn authenticate_access(
    pool: &AnyPool,
    session_id: OperatorSessionId,
    presented_digest: PlatformDigest,
    now: TimestampMicros,
) -> Result<OperatorContext, PlatformIdentityError> {
    if now.get() < 0 {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let row = sqlx::query("SELECT access_digest, access_expires_at_micros, status FROM runku_operator_sessions WHERE session_id = $1")
        .bind(session_id.to_string()).fetch_optional(pool).await.map_err(map_sqlx_error)?
        .ok_or(PlatformIdentityError::Unauthenticated)?;
    let digest = decode_digest(
        row.try_get("access_digest")
            .map_err(|_| PlatformIdentityError::Corruption)?,
    )?;
    let expires: i64 = row
        .try_get("access_expires_at_micros")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let status: String = row
        .try_get("status")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    if status != "active" || expires <= now.get() || !digest.matches(presented_digest) {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    let context = load_context(pool, session_id).await?;
    if context.operator.status != OperatorStatus::Active {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    Ok(context)
}

async fn refresh_session(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    session_id: OperatorSessionId,
    presented_digest: PlatformDigest,
    replacement: &RefreshedSession,
) -> Result<OperatorContext, PlatformIdentityError> {
    if replacement.refreshed_at.get() < 0
        || replacement.access_expires_at <= replacement.refreshed_at
        || replacement.refresh_expires_at <= replacement.access_expires_at
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let row = sqlx::query("SELECT refresh_digest, refresh_expires_at_micros, status FROM runku_operator_sessions WHERE session_id = $1")
        .bind(session_id.to_string()).fetch_optional(&mut *tx).await.map_err(map_sqlx_error)?
        .ok_or(PlatformIdentityError::Unauthenticated)?;
    let digest = decode_digest(
        row.try_get("refresh_digest")
            .map_err(|_| PlatformIdentityError::Corruption)?,
    )?;
    let expires: i64 = row
        .try_get("refresh_expires_at_micros")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let status: String = row
        .try_get("status")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    if status != "active"
        || expires <= replacement.refreshed_at.get()
        || !digest.matches(presented_digest)
    {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    sqlx::query("UPDATE runku_operator_sessions SET access_digest = $1, refresh_digest = $2, last_used_at_micros = $3, access_expires_at_micros = $4, refresh_expires_at_micros = $5 WHERE session_id = $6 AND status = 'active'")
        .bind(replacement.access_digest.as_bytes().to_vec()).bind(replacement.refresh_digest.as_bytes().to_vec())
        .bind(replacement.refreshed_at.get()).bind(replacement.access_expires_at.get())
        .bind(replacement.refresh_expires_at.get()).bind(session_id.to_string())
        .execute(&mut *tx).await.map_err(map_sqlx_error)?;
    let context = load_context_tx(&mut tx, session_id).await?;
    if context.operator.status != OperatorStatus::Active {
        return Err(PlatformIdentityError::Unauthenticated);
    }
    audit(
        &mut tx,
        Some(context.operator.id),
        Some(context.operator.id),
        "session.refresh",
        replacement.refreshed_at,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(context)
}

async fn revoke_session(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
    actor: &OperatorContext,
    session_id: OperatorSessionId,
    now: TimestampMicros,
) -> Result<bool, PlatformIdentityError> {
    if now.get() < 0 {
        return Err(PlatformIdentityError::InvalidInput);
    }
    let mut tx = begin_write(pool, backend).await?;
    let fresh = load_context_tx(&mut tx, actor.session.id).await?;
    if fresh.operator.authorization_revision != actor.operator.authorization_revision {
        return Err(PlatformIdentityError::Forbidden);
    }
    let target_operator = sqlx::query_scalar::<_, String>(
        "SELECT operator_id FROM runku_operator_sessions WHERE session_id = $1",
    )
    .bind(session_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?
    .ok_or(PlatformIdentityError::NotFound)?
    .parse::<OperatorId>()
    .map_err(|_| PlatformIdentityError::Corruption)?;
    if target_operator != actor.operator.id {
        fresh.authorize(
            AccessScope::Installation,
            PlatformCapability::OperatorsManage,
        )?;
    }
    let result = sqlx::query("UPDATE runku_operator_sessions SET status = 'revoked', revoked_at_micros = $1 WHERE session_id = $2 AND status = 'active'")
        .bind(now.get()).bind(session_id.to_string()).execute(&mut *tx).await.map_err(map_sqlx_error)?;
    audit(
        &mut tx,
        Some(actor.operator.id),
        Some(target_operator),
        "session.revoke",
        now,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| map_commit_error(&error))?;
    Ok(result.rows_affected() == 1)
}

async fn list_sessions(
    pool: &AnyPool,
    actor: &OperatorContext,
) -> Result<Vec<OperatorSession>, PlatformIdentityError> {
    let current = load_context(pool, actor.session.id).await?;
    if current.operator.authorization_revision != actor.operator.authorization_revision {
        return Err(PlatformIdentityError::Forbidden);
    }
    let rows = sqlx::query("SELECT session_id, operator_id, device_name, status, created_at_micros, last_used_at_micros, access_expires_at_micros, refresh_expires_at_micros FROM runku_operator_sessions WHERE operator_id = $1 ORDER BY session_id")
        .bind(actor.operator.id.to_string()).fetch_all(pool).await.map_err(map_sqlx_error)?;
    rows.iter().map(decode_session).collect()
}

async fn load_invitation_tx(
    tx: &mut Transaction<'_, Any>,
    invitation_id: OperatorInvitationId,
) -> Result<OperatorInvitation, PlatformIdentityError> {
    let row = sqlx::query("SELECT i.invitation_id, i.kind, i.operator_name, i.grants_json, i.status, i.created_by, i.created_at_micros, i.expires_at_micros, i.consumed_at_micros, i.revoked_at_micros, o.operation_id, o.scope_kind AS operation_scope_kind, o.project_id AS operation_project_id, o.environment_id AS operation_environment_id FROM runku_operator_invitations i LEFT JOIN runku_operator_invitation_operations o ON o.invitation_id = i.invitation_id WHERE i.invitation_id = $1")
        .bind(invitation_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or(PlatformIdentityError::NotFound)?;
    decode_invitation(&row)
}

fn decode_invitation(row: &sqlx::any::AnyRow) -> Result<OperatorInvitation, PlatformIdentityError> {
    let kind = text(row, "kind")?;
    if kind != "operator" {
        return Err(PlatformIdentityError::NotFound);
    }
    let grants_json: Vec<u8> = row
        .try_get("grants_json")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let created_by = row
        .try_get::<Option<String>, _>("created_by")
        .map_err(|_| PlatformIdentityError::Corruption)?
        .ok_or(PlatformIdentityError::Corruption)?
        .parse::<OperatorId>()
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let operation_id = row
        .try_get::<Option<String>, _>("operation_id")
        .map_err(|_| PlatformIdentityError::Corruption)?
        .map(|value| {
            value
                .parse::<OperationId>()
                .map_err(|_| PlatformIdentityError::Corruption)
        })
        .transpose()?;
    let grants = decode_grants(&grants_json)?;
    if operation_id.is_some() {
        let scope_kind = row
            .try_get::<Option<String>, _>("operation_scope_kind")
            .map_err(|_| PlatformIdentityError::Corruption)?
            .ok_or(PlatformIdentityError::Corruption)?;
        let project_id = row
            .try_get::<Option<String>, _>("operation_project_id")
            .map_err(|_| PlatformIdentityError::Corruption)?;
        let environment_id = row
            .try_get::<Option<String>, _>("operation_environment_id")
            .map_err(|_| PlatformIdentityError::Corruption)?;
        let operation_scope = decode_scope_values(
            &scope_kind,
            project_id.as_deref(),
            environment_id.as_deref(),
        )?;
        if grants.as_slice().first().map(|grant| grant.scope) != Some(operation_scope)
            || grants.len() != 1
        {
            return Err(PlatformIdentityError::Corruption);
        }
    }
    Ok(OperatorInvitation {
        id: text(row, "invitation_id")?
            .parse()
            .map_err(|_| PlatformIdentityError::Corruption)?,
        operation_id,
        operator_name: text(row, "operator_name")?
            .parse()
            .map_err(|_| PlatformIdentityError::Corruption)?,
        grants,
        status: parse_invitation_status(&text(row, "status")?)?,
        created_by,
        created_at: timestamp(row, "created_at_micros")?,
        expires_at: timestamp(row, "expires_at_micros")?,
        consumed_at: optional_timestamp(row, "consumed_at_micros")?,
        revoked_at: optional_timestamp(row, "revoked_at_micros")?,
    })
}

async fn insert_invitation(
    tx: &mut Transaction<'_, Any>,
    invitation: &NewInvitation,
) -> Result<(), PlatformIdentityError> {
    sqlx::query("INSERT INTO runku_operator_invitations (invitation_id, kind, operator_name, grants_json, digest, status, created_by, created_at_micros, expires_at_micros, consumed_at_micros) VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7, $8, NULL)")
        .bind(invitation.id.to_string()).bind(encode_invitation_kind(invitation.kind))
        .bind(invitation.operator_name.as_str()).bind(encode_grants(&invitation.grants)?)
        .bind(invitation.digest.as_bytes().to_vec()).bind(invitation.created_by.map(|id| id.to_string()))
        .bind(invitation.created_at.get()).bind(invitation.expires_at.get())
        .execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn insert_grant(
    tx: &mut Transaction<'_, Any>,
    operator_id: OperatorId,
    grant: &OperatorGrant,
    created_at: TimestampMicros,
    created_by: Option<OperatorId>,
) -> Result<(), PlatformIdentityError> {
    grant.validate()?;
    let (scope_key, kind, project, environment) = encode_scope(grant.scope);
    for capability in &grant.capabilities {
        sqlx::query("INSERT INTO runku_operator_grants (operator_id, scope_key, scope_kind, project_id, environment_id, capability, created_at_micros, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
            .bind(operator_id.to_string()).bind(&scope_key).bind(kind).bind(project.clone())
            .bind(environment.clone()).bind(capability.as_str()).bind(created_at.get())
            .bind(created_by.map(|id| id.to_string())).execute(&mut **tx).await
            .map_err(map_constraint_error)?;
    }
    Ok(())
}

async fn insert_session(
    tx: &mut Transaction<'_, Any>,
    operator_id: OperatorId,
    session: &NewOperatorSession,
) -> Result<(), PlatformIdentityError> {
    validate_new_session(session)?;
    sqlx::query("INSERT INTO runku_operator_sessions (session_id, operator_id, device_name, access_digest, refresh_digest, status, created_at_micros, last_used_at_micros, access_expires_at_micros, refresh_expires_at_micros, revoked_at_micros) VALUES ($1, $2, $3, $4, $5, 'active', $6, $6, $7, $8, NULL)")
        .bind(session.id.to_string()).bind(operator_id.to_string()).bind(session.device_name.as_str())
        .bind(session.access_digest.as_bytes().to_vec()).bind(session.refresh_digest.as_bytes().to_vec())
        .bind(session.created_at.get()).bind(session.access_expires_at.get()).bind(session.refresh_expires_at.get())
        .execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn load_context(
    pool: &AnyPool,
    session_id: OperatorSessionId,
) -> Result<OperatorContext, PlatformIdentityError> {
    let row = sqlx::query("SELECT s.session_id, s.operator_id, s.device_name, s.status AS session_status, s.created_at_micros AS session_created_at, s.last_used_at_micros, s.access_expires_at_micros, s.refresh_expires_at_micros, o.name, o.status AS operator_status, o.created_at_micros AS operator_created_at, o.authorization_revision FROM runku_operator_sessions s JOIN runku_operators o ON o.operator_id = s.operator_id WHERE s.session_id = $1")
        .bind(session_id.to_string()).fetch_optional(pool).await.map_err(map_sqlx_error)?
        .ok_or(PlatformIdentityError::Unauthenticated)?;
    decode_context(pool, &row).await
}

async fn load_context_tx(
    tx: &mut Transaction<'_, Any>,
    session_id: OperatorSessionId,
) -> Result<OperatorContext, PlatformIdentityError> {
    let row = sqlx::query("SELECT s.session_id, s.operator_id, s.device_name, s.status AS session_status, s.created_at_micros AS session_created_at, s.last_used_at_micros, s.access_expires_at_micros, s.refresh_expires_at_micros, o.name, o.status AS operator_status, o.created_at_micros AS operator_created_at, o.authorization_revision FROM runku_operator_sessions s JOIN runku_operators o ON o.operator_id = s.operator_id WHERE s.session_id = $1")
        .bind(session_id.to_string()).fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?
        .ok_or(PlatformIdentityError::Unauthenticated)?;
    let operator_id = parse_operator_id(&row, "operator_id")?;
    let grants = load_grants_tx(tx, operator_id).await?;
    decode_context_row(&row, grants)
}

async fn decode_context(
    pool: &AnyPool,
    row: &sqlx::any::AnyRow,
) -> Result<OperatorContext, PlatformIdentityError> {
    let operator_id = parse_operator_id(row, "operator_id")?;
    let grants = load_grants(pool, operator_id).await?;
    decode_context_row(row, grants)
}

fn decode_context_row(
    row: &sqlx::any::AnyRow,
    grants: Vec<OperatorGrant>,
) -> Result<OperatorContext, PlatformIdentityError> {
    let operator_id = parse_operator_id(row, "operator_id")?;
    let operator = Operator {
        id: operator_id,
        name: text(row, "name")?
            .parse()
            .map_err(|_| PlatformIdentityError::Corruption)?,
        status: parse_operator_status(&text(row, "operator_status")?)?,
        created_at: timestamp(row, "operator_created_at")?,
        authorization_revision: nonnegative_u64(row, "authorization_revision")?,
    };
    let session = OperatorSession {
        id: text(row, "session_id")?
            .parse()
            .map_err(|_| PlatformIdentityError::Corruption)?,
        operator_id,
        device_name: text(row, "device_name")?
            .parse()
            .map_err(|_| PlatformIdentityError::Corruption)?,
        status: parse_session_status(&text(row, "session_status")?)?,
        created_at: timestamp(row, "session_created_at")?,
        last_used_at: timestamp(row, "last_used_at_micros")?,
        access_expires_at: timestamp(row, "access_expires_at_micros")?,
        refresh_expires_at: timestamp(row, "refresh_expires_at_micros")?,
    };
    Ok(OperatorContext {
        operator,
        session,
        grants,
    })
}

async fn load_grants(
    pool: &AnyPool,
    operator_id: OperatorId,
) -> Result<Vec<OperatorGrant>, PlatformIdentityError> {
    let rows = sqlx::query("SELECT scope_kind, project_id, environment_id, capability FROM runku_operator_grants WHERE operator_id = $1 ORDER BY scope_key, capability")
        .bind(operator_id.to_string()).fetch_all(pool).await.map_err(map_sqlx_error)?;
    fold_grants(&rows)
}

async fn load_grants_tx(
    tx: &mut Transaction<'_, Any>,
    operator_id: OperatorId,
) -> Result<Vec<OperatorGrant>, PlatformIdentityError> {
    let rows = sqlx::query("SELECT scope_kind, project_id, environment_id, capability FROM runku_operator_grants WHERE operator_id = $1 ORDER BY scope_key, capability")
        .bind(operator_id.to_string()).fetch_all(&mut **tx).await.map_err(map_sqlx_error)?;
    fold_grants(&rows)
}

fn fold_grants(rows: &[sqlx::any::AnyRow]) -> Result<Vec<OperatorGrant>, PlatformIdentityError> {
    let mut grants: Vec<OperatorGrant> = Vec::new();
    for row in rows {
        let scope = decode_scope(row)?;
        let capability = PlatformCapability::parse(&text(row, "capability")?)?;
        if let Some(grant) = grants.iter_mut().find(|grant| grant.scope == scope) {
            if !grant.capabilities.insert(capability) {
                return Err(PlatformIdentityError::Corruption);
            }
        } else {
            grants.push(OperatorGrant {
                scope,
                capabilities: BTreeSet::from([capability]),
            });
        }
    }
    if grants.iter().any(|grant| grant.validate().is_err()) {
        return Err(PlatformIdentityError::Corruption);
    }
    Ok(grants)
}

fn decode_session(row: &sqlx::any::AnyRow) -> Result<OperatorSession, PlatformIdentityError> {
    Ok(OperatorSession {
        id: text(row, "session_id")?
            .parse()
            .map_err(|_| PlatformIdentityError::Corruption)?,
        operator_id: parse_operator_id(row, "operator_id")?,
        device_name: text(row, "device_name")?
            .parse()
            .map_err(|_| PlatformIdentityError::Corruption)?,
        status: parse_session_status(&text(row, "status")?)?,
        created_at: timestamp(row, "created_at_micros")?,
        last_used_at: timestamp(row, "last_used_at_micros")?,
        access_expires_at: timestamp(row, "access_expires_at_micros")?,
        refresh_expires_at: timestamp(row, "refresh_expires_at_micros")?,
    })
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantWire {
    scope_kind: String,
    project_id: Option<String>,
    environment_id: Option<String>,
    capabilities: Vec<String>,
}

fn encode_grants(grants: &[OperatorGrant]) -> Result<Vec<u8>, PlatformIdentityError> {
    let wire = grants
        .iter()
        .map(|grant| {
            let (_, kind, project_id, environment_id) = encode_scope(grant.scope);
            GrantWire {
                scope_kind: kind.to_owned(),
                project_id,
                environment_id,
                capabilities: grant
                    .capabilities
                    .iter()
                    .map(|value| value.as_str().to_owned())
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&wire).map_err(|_| PlatformIdentityError::Corruption)
}

fn decode_grants(bytes: &[u8]) -> Result<Vec<OperatorGrant>, PlatformIdentityError> {
    if bytes.is_empty() || bytes.len() > 64 * 1024 {
        return Err(PlatformIdentityError::Corruption);
    }
    let wire: Vec<GrantWire> =
        serde_json::from_slice(bytes).map_err(|_| PlatformIdentityError::Corruption)?;
    if wire.is_empty() || wire.len() > 32 {
        return Err(PlatformIdentityError::Corruption);
    }
    wire.into_iter()
        .map(|grant| {
            let scope = decode_scope_values(
                &grant.scope_kind,
                grant.project_id.as_deref(),
                grant.environment_id.as_deref(),
            )?;
            let capabilities = grant
                .capabilities
                .iter()
                .map(|value| PlatformCapability::parse(value))
                .collect::<Result<BTreeSet<_>, _>>()?;
            let model = OperatorGrant {
                scope,
                capabilities,
            };
            model
                .validate()
                .map_err(|_| PlatformIdentityError::Corruption)?;
            Ok(model)
        })
        .collect()
}

fn encode_scope(scope: AccessScope) -> (String, &'static str, Option<String>, Option<String>) {
    match scope {
        AccessScope::Installation => ("installation".to_owned(), "installation", None, None),
        AccessScope::Project(project) => (
            project.to_string(),
            "project",
            Some(project.to_string()),
            None,
        ),
        AccessScope::Environment(scope) => (
            format!("{}/{}", scope.project_id(), scope.environment_id()),
            "environment",
            Some(scope.project_id().to_string()),
            Some(scope.environment_id().to_string()),
        ),
    }
}

fn decode_scope(row: &sqlx::any::AnyRow) -> Result<AccessScope, PlatformIdentityError> {
    let kind = text(row, "scope_kind")?;
    let project: Option<String> = row
        .try_get("project_id")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    let environment: Option<String> = row
        .try_get("environment_id")
        .map_err(|_| PlatformIdentityError::Corruption)?;
    decode_scope_values(&kind, project.as_deref(), environment.as_deref())
}

fn decode_scope_values(
    kind: &str,
    project: Option<&str>,
    environment: Option<&str>,
) -> Result<AccessScope, PlatformIdentityError> {
    match (kind, project, environment) {
        ("installation", None, None) => Ok(AccessScope::Installation),
        ("project", Some(project), None) => Ok(AccessScope::Project(
            project
                .parse()
                .map_err(|_| PlatformIdentityError::Corruption)?,
        )),
        ("environment", Some(project), Some(environment)) => {
            Ok(AccessScope::Environment(EnvironmentScope::new(
                project
                    .parse::<ProjectId>()
                    .map_err(|_| PlatformIdentityError::Corruption)?,
                environment
                    .parse::<EnvironmentId>()
                    .map_err(|_| PlatformIdentityError::Corruption)?,
            )))
        }
        _ => Err(PlatformIdentityError::Corruption),
    }
}

fn validate_invitation(invitation: &NewInvitation) -> Result<(), PlatformIdentityError> {
    if invitation.grants.is_empty()
        || invitation.grants.len() > 32
        || invitation.created_at.get() < 0
        || invitation.expires_at <= invitation.created_at
        || invitation
            .grants
            .iter()
            .any(|grant| grant.validate().is_err())
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    Ok(())
}

fn validate_new_session(session: &NewOperatorSession) -> Result<(), PlatformIdentityError> {
    if session.created_at.get() < 0
        || session.access_expires_at <= session.created_at
        || session.refresh_expires_at <= session.access_expires_at
    {
        return Err(PlatformIdentityError::InvalidInput);
    }
    Ok(())
}

async fn audit(
    tx: &mut Transaction<'_, Any>,
    actor: Option<OperatorId>,
    subject: Option<OperatorId>,
    operation: &str,
    at: TimestampMicros,
) -> Result<(), PlatformIdentityError> {
    sqlx::query("INSERT INTO runku_platform_audit (event_id, actor_operator_id, subject_operator_id, operation, outcome, occurred_at_micros) VALUES ($1, $2, $3, $4, 'succeeded', $5)")
        .bind(OperationId::generate().to_string()).bind(actor.map(|id| id.to_string()))
        .bind(subject.map(|id| id.to_string())).bind(operation).bind(at.get())
        .execute(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn audit_invitation(
    tx: &mut Transaction<'_, Any>,
    actor: OperatorId,
    operation: &str,
    request_operation_id: Option<OperationId>,
    invitation_id: OperatorInvitationId,
    at: TimestampMicros,
) -> Result<(), PlatformIdentityError> {
    sqlx::query("INSERT INTO runku_platform_audit (event_id, actor_operator_id, subject_operator_id, operation, outcome, occurred_at_micros, request_operation_id, subject_invitation_id) VALUES ($1, $2, NULL, $3, 'succeeded', $4, $5, $6)")
        .bind(OperationId::generate().to_string())
        .bind(actor.to_string())
        .bind(operation)
        .bind(at.get())
        .bind(request_operation_id.map(|id| id.to_string()))
        .bind(invitation_id.to_string())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

async fn begin_write(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
) -> Result<Transaction<'_, Any>, PlatformIdentityError> {
    let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
    if backend == PlatformIdentityBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(MIGRATION_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
    }
    Ok(tx)
}

async fn migrate(
    pool: &AnyPool,
    backend: PlatformIdentityBackend,
) -> Result<(), PlatformIdentityError> {
    let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
    if backend == PlatformIdentityBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(MIGRATION_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
    }
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_platform_migrations (version BIGINT PRIMARY KEY, checksum TEXT NOT NULL)")
        .execute(&mut *tx).await.map_err(map_sqlx_error)?;
    let recorded =
        sqlx::query("SELECT version, checksum FROM runku_platform_migrations ORDER BY version")
            .fetch_all(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
    if recorded.len() > usize::try_from(SCHEMA_VERSION).unwrap_or(usize::MAX)
        || recorded.iter().any(|row| {
            !row.try_get::<i64, _>("version")
                .is_ok_and(|version| (1..=SCHEMA_VERSION).contains(&version))
        })
    {
        return Err(PlatformIdentityError::Corruption);
    }
    for (version, statements) in [(1_i64, SCHEMA_V1), (2_i64, SCHEMA_V2)] {
        let checksum = schema_checksum(version, statements);
        if let Some(row) = recorded
            .iter()
            .find(|row| row.try_get::<i64, _>("version").ok() == Some(version))
        {
            if text(row, "checksum")? != checksum {
                return Err(PlatformIdentityError::Corruption);
            }
            continue;
        }
        if recorded.iter().any(|row| {
            row.try_get::<i64, _>("version")
                .is_ok_and(|recorded_version| recorded_version > version)
        }) {
            return Err(PlatformIdentityError::Corruption);
        }
        for statement in statements {
            sqlx::query(*statement)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        }
        sqlx::query("INSERT INTO runku_platform_migrations (version, checksum) VALUES ($1, $2)")
            .bind(version)
            .bind(&checksum)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
    }
    sqlx::query("INSERT INTO runku_platform_meta (singleton_id, initialized, authorization_revision) VALUES (1, FALSE, 0) ON CONFLICT(singleton_id) DO NOTHING")
        .execute(&mut *tx).await.map_err(map_sqlx_error)?;
    tx.commit().await.map_err(|error| map_commit_error(&error))
}

fn schema_checksum(version: i64, statements: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(format!("runku-platform-identity-schema-v{version}\0").as_bytes());
    for statement in statements {
        digest.update(statement.as_bytes());
        digest.update([0]);
    }
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn encode_invitation_kind(kind: InvitationKind) -> &'static str {
    match kind {
        InvitationKind::Bootstrap => "bootstrap",
        InvitationKind::Operator => "operator",
    }
}

fn parse_operator_status(value: &str) -> Result<OperatorStatus, PlatformIdentityError> {
    match value {
        "active" => Ok(OperatorStatus::Active),
        "disabled" => Ok(OperatorStatus::Disabled),
        _ => Err(PlatformIdentityError::Corruption),
    }
}

fn parse_session_status(value: &str) -> Result<SessionStatus, PlatformIdentityError> {
    match value {
        "active" => Ok(SessionStatus::Active),
        "revoked" => Ok(SessionStatus::Revoked),
        _ => Err(PlatformIdentityError::Corruption),
    }
}

fn parse_invitation_status(value: &str) -> Result<InvitationStatus, PlatformIdentityError> {
    match value {
        "pending" => Ok(InvitationStatus::Pending),
        "consumed" => Ok(InvitationStatus::Consumed),
        "revoked" => Ok(InvitationStatus::Revoked),
        _ => Err(PlatformIdentityError::Corruption),
    }
}

fn parse_operator_id(
    row: &sqlx::any::AnyRow,
    column: &str,
) -> Result<OperatorId, PlatformIdentityError> {
    text(row, column)?
        .parse()
        .map_err(|_| PlatformIdentityError::Corruption)
}

fn text(row: &sqlx::any::AnyRow, column: &str) -> Result<String, PlatformIdentityError> {
    row.try_get(column)
        .map_err(|_| PlatformIdentityError::Corruption)
}

fn timestamp(
    row: &sqlx::any::AnyRow,
    column: &str,
) -> Result<TimestampMicros, PlatformIdentityError> {
    let value: i64 = row
        .try_get(column)
        .map_err(|_| PlatformIdentityError::Corruption)?;
    if value < 0 {
        return Err(PlatformIdentityError::Corruption);
    }
    Ok(TimestampMicros::new(value))
}

fn optional_timestamp(
    row: &sqlx::any::AnyRow,
    column: &str,
) -> Result<Option<TimestampMicros>, PlatformIdentityError> {
    row.try_get::<Option<i64>, _>(column)
        .map_err(|_| PlatformIdentityError::Corruption)?
        .map(|value| {
            if value < 0 {
                Err(PlatformIdentityError::Corruption)
            } else {
                Ok(TimestampMicros::new(value))
            }
        })
        .transpose()
}

fn nonnegative_u64(row: &sqlx::any::AnyRow, column: &str) -> Result<u64, PlatformIdentityError> {
    let value: i64 = row
        .try_get(column)
        .map_err(|_| PlatformIdentityError::Corruption)?;
    u64::try_from(value).map_err(|_| PlatformIdentityError::Corruption)
}

fn decode_digest(bytes: Vec<u8>) -> Result<PlatformDigest, PlatformIdentityError> {
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| PlatformIdentityError::Corruption)?;
    Ok(PlatformDigest::from_bytes(array))
}

fn decode_sha256(bytes: Vec<u8>) -> Result<[u8; 32], PlatformIdentityError> {
    bytes
        .try_into()
        .map_err(|_| PlatformIdentityError::Corruption)
}

fn map_constraint_error(error: sqlx::Error) -> PlatformIdentityError {
    if error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
    {
        PlatformIdentityError::Conflict
    } else {
        map_sqlx_error(error)
    }
}

fn map_commit_error(error: &sqlx::Error) -> PlatformIdentityError {
    if error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "40001" || code == "40P01")
    {
        PlatformIdentityError::Unavailable
    } else {
        PlatformIdentityError::ResultUncertain
    }
}

fn map_sqlx_error(error: sqlx::Error) -> PlatformIdentityError {
    match error {
        sqlx::Error::RowNotFound => PlatformIdentityError::NotFound,
        sqlx::Error::Database(database) if database.is_unique_violation() => {
            PlatformIdentityError::Conflict
        }
        sqlx::Error::Database(database)
            if database
                .code()
                .is_some_and(|code| code == "40001" || code == "40P01") =>
        {
            PlatformIdentityError::Unavailable
        }
        sqlx::Error::Decode(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::TypeNotFound { .. } => PlatformIdentityError::Corruption,
        _ => PlatformIdentityError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn schema_v1_upgrades_append_only_to_invitation_operations()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            schema_checksum(1, SCHEMA_V1),
            "cd094b1d5bb6b8c6995b7d3d09fcb0dc09fa230afa734e4d5dbb6f71ed15be35"
        );
        sqlx::any::install_default_drivers();
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("platform-v1.sqlite3");
        let options =
            AnyConnectOptions::from_str(&format!("sqlite://{}?mode=rwc", database.display()))?;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE runku_platform_migrations (version BIGINT PRIMARY KEY, checksum TEXT NOT NULL)")
            .execute(&pool)
            .await?;
        for statement in SCHEMA_V1 {
            sqlx::query(*statement).execute(&pool).await?;
        }
        sqlx::query("INSERT INTO runku_platform_migrations (version, checksum) VALUES (1, $1)")
            .bind(schema_checksum(1, SCHEMA_V1))
            .execute(&pool)
            .await?;

        migrate(&pool, PlatformIdentityBackend::SQLite).await?;

        let versions = sqlx::query_scalar::<_, i64>(
            "SELECT version FROM runku_platform_migrations ORDER BY version",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(versions, vec![1, 2]);
        let operation_tables = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'runku_operator_invitation_operations'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(operation_tables, 1);
        let revoked_columns = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pragma_table_info('runku_operator_invitations') WHERE name = 'revoked_at_micros'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(revoked_columns, 1);
        pool.close().await;
        Ok(())
    }
}
