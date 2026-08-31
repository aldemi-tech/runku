//! Authenticated semantic orchestration over existing Product Base repositories.

use std::{
    fmt,
    str::FromStr as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use runku_compatibility::{CompatibilityEngine, CompatibilityError, ReleasePackage};
use runku_core::{EnvironmentDescriptor, EnvironmentScope, OperationId, RequestId};
use runku_development::{
    DevelopmentCommand, DevelopmentContext, DevelopmentError, DevelopmentRepository,
    DevelopmentRevisionEntry,
};
use runku_development_access::{
    DevelopmentAccessError, DevelopmentAccessResolver, DevelopmentIdentity, DevelopmentKeyCrypto,
    ParsedDevelopmentKey,
};
use runku_gateway::{DevelopmentCatalog, ServingCatalog, ServingRefresh};
use runku_protocol::{
    DevelopmentCreateWorkspaceRequestV1, DevelopmentCreateWorkspaceResponseV1,
    DevelopmentFreezeDiagnosticV1, DevelopmentFreezeOutcomeV1, DevelopmentFreezeRequestV1,
    DevelopmentFreezeResponseV1, DevelopmentFreezeStageV1, DevelopmentPublishRequestV1,
    DevelopmentPublishResponseV1, DevelopmentStateRequestV1, DevelopmentStateResponseV1,
    DevelopmentWorkspaceStateV1, derive_development_freeze_operation_id_v1,
    derive_development_revision_id_v1,
};
use runku_releases::{
    ArtifactStore, ReleaseCommand, ReleaseError, ReleaseRepository, ReleaseStatus, Sha256Digest,
    decode_safe_esm_bundle, encode_release_manifest,
};

use crate::{
    DevelopmentAuditEvent, DevelopmentAuditOperation, DevelopmentAuditOutcome,
    DevelopmentAuditSink, DevelopmentServiceClock, DevelopmentServiceError,
    DevelopmentServiceTelemetrySnapshot,
};

/// Trusted fixed configuration for one Environment-serving service instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteWorkspaceServiceConfig {
    /// Exact tenant/Environment storage boundary.
    pub scope: EnvironmentScope,
    /// Server-authoritative policy descriptor; requests never supply it.
    pub environment: EnvironmentDescriptor,
}

impl RemoteWorkspaceServiceConfig {
    fn context(self) -> DevelopmentContext {
        DevelopmentContext {
            scope: self.scope,
            environment: self.environment,
        }
    }

    fn validate_identity(self) -> Result<(), DevelopmentServiceError> {
        if self.scope.environment_id() != self.environment.id() {
            return Err(DevelopmentServiceError::Internal);
        }
        Ok(())
    }

    fn validate_policy(self) -> Result<(), DevelopmentServiceError> {
        self.validate_identity()?;
        self.environment
            .validate_development_sync()
            .map_err(|_| DevelopmentServiceError::PolicyDenied)
    }
}

/// Serving refresh boundary required before mutation success is observable.
#[async_trait]
pub trait DevelopmentServingRefresher: fmt::Debug + Send + Sync {
    /// Fixed Environment scope.
    fn scope(&self) -> EnvironmentScope;

    /// Publishes or confirms the latest coherent Development snapshot and returns its revision.
    async fn refresh(&self) -> Result<u64, DevelopmentError>;
}

/// Release serving refresh boundary required before freeze success is observable.
#[async_trait]
pub trait ReleaseServingRefresher: fmt::Debug + Send + Sync {
    /// Fixed Environment scope.
    fn scope(&self) -> EnvironmentScope;

    /// Publishes or confirms the latest coherent Release snapshot.
    async fn refresh(&self) -> Result<u64, ReleaseError>;
}

#[async_trait]
impl ReleaseServingRefresher for ServingCatalog {
    fn scope(&self) -> EnvironmentScope {
        self.scope()
    }

    async fn refresh(&self) -> Result<u64, ReleaseError> {
        match self.refresh().await? {
            ServingRefresh::Published { revision } | ServingRefresh::Unchanged { revision } => {
                Ok(revision)
            }
        }
    }
}

#[async_trait]
impl DevelopmentServingRefresher for DevelopmentCatalog {
    fn scope(&self) -> EnvironmentScope {
        self.scope()
    }

    async fn refresh(&self) -> Result<u64, DevelopmentError> {
        match self.refresh().await? {
            ServingRefresh::Published { revision } | ServingRefresh::Unchanged { revision } => {
                Ok(revision)
            }
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    state_successes: AtomicU64,
    create_successes: AtomicU64,
    publish_successes: AtomicU64,
    freeze_successes: AtomicU64,
    authentication_failures: AtomicU64,
    policy_rejections: AtomicU64,
    conflicts: AtomicU64,
    retryable_failures: AtomicU64,
    admission_rejections: AtomicU64,
    deadline_responses: AtomicU64,
}

/// Complete semantic Remote Workspace composition for one trusted Environment.
pub struct RemoteWorkspaceService {
    config: RemoteWorkspaceServiceConfig,
    access: Arc<dyn DevelopmentAccessResolver>,
    crypto: Arc<DevelopmentKeyCrypto>,
    development: Arc<dyn DevelopmentRepository>,
    releases: Arc<dyn ReleaseRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    refresher: Arc<dyn DevelopmentServingRefresher>,
    release_refresher: Arc<dyn ReleaseServingRefresher>,
    clock: Arc<dyn DevelopmentServiceClock>,
    audit: Arc<dyn DevelopmentAuditSink>,
    counters: Arc<Counters>,
}

impl fmt::Debug for RemoteWorkspaceService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteWorkspaceService")
            .field("scope", &self.config.scope)
            .field("development_backend", &self.development.backend())
            .field("release_backend", &self.releases.backend())
            .finish_non_exhaustive()
    }
}

impl RemoteWorkspaceService {
    /// Composes all mandatory Product Base dependencies. No dependency has an implicit in-memory
    /// or `SaaS` fallback.
    ///
    /// # Errors
    ///
    /// Rejects contradictory Environment identity or a serving refresher for another scope.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: RemoteWorkspaceServiceConfig,
        access: Arc<dyn DevelopmentAccessResolver>,
        crypto: Arc<DevelopmentKeyCrypto>,
        development: Arc<dyn DevelopmentRepository>,
        releases: Arc<dyn ReleaseRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        refresher: Arc<dyn DevelopmentServingRefresher>,
        release_refresher: Arc<dyn ReleaseServingRefresher>,
        clock: Arc<dyn DevelopmentServiceClock>,
        audit: Arc<dyn DevelopmentAuditSink>,
    ) -> Result<Self, DevelopmentServiceError> {
        config.validate_identity()?;
        if refresher.scope() != config.scope || release_refresher.scope() != config.scope {
            return Err(DevelopmentServiceError::Internal);
        }
        Ok(Self {
            config,
            access,
            crypto,
            development,
            releases,
            artifacts,
            refresher,
            release_refresher,
            clock,
            audit,
            counters: Arc::new(Counters::default()),
        })
    }

    /// Returns authoritative Environment and optional Workspace state after Development auth.
    ///
    /// # Errors
    ///
    /// Returns sanitized auth, policy, repository, or invariant failures.
    pub async fn state(
        &self,
        request_id: RequestId,
        bearer: &str,
        request: DevelopmentStateRequestV1,
    ) -> Result<DevelopmentStateResponseV1, DevelopmentServiceError> {
        let now = self.clock.now()?;
        let mut credential = None;
        let result = async {
            let identity = self.authenticate(bearer, now).await?;
            credential = Some(identity.credential_id);
            self.config.validate_policy()?;
            let snapshot = self
                .development
                .snapshot(self.config.context())
                .await
                .map_err(map_development)?;
            if snapshot.scope() != self.config.scope {
                return Err(DevelopmentServiceError::Corruption);
            }
            Ok(DevelopmentStateResponseV1 {
                request_id,
                scope: self.config.scope,
                environment: self.config.environment,
                development_revision: snapshot.revision(),
                workspace: snapshot
                    .workspace_binding(&request.workspace_ref)
                    .map(workspace_state),
            })
        }
        .await;
        self.finish(
            DevelopmentAuditOperation::State,
            request_id,
            None,
            credential,
            now,
            &result,
        );
        result
    }

    /// Creates one empty Workspace through the durable Development operation journal.
    ///
    /// # Errors
    ///
    /// Returns sanitized auth, policy, idempotency, repository, refresh, or invariant failures.
    pub async fn create_workspace(
        &self,
        request_id: RequestId,
        bearer: &str,
        request: DevelopmentCreateWorkspaceRequestV1,
    ) -> Result<DevelopmentCreateWorkspaceResponseV1, DevelopmentServiceError> {
        let now = self.clock.now()?;
        let mut credential = None;
        let result = async {
            let identity = self.authenticate(bearer, now).await?;
            credential = Some(identity.credential_id);
            self.config.validate_policy()?;
            let snapshot = self
                .development
                .snapshot(self.config.context())
                .await
                .map_err(map_development)?;
            let created_at = snapshot
                .workspace_binding(&request.workspace_ref)
                .filter(|binding| {
                    binding.workspace_id == request.workspace_id
                        && binding.updated_by == identity.actor
                        && binding.head_revision.is_none()
                })
                .map_or(now, |binding| binding.updated_at);
            let command = DevelopmentCommand::CreateWorkspace {
                workspace_id: request.workspace_id,
                workspace_ref: request.workspace_ref.clone(),
                actor: identity.actor,
                created_at,
            };
            let applied = self
                .development
                .apply(self.config.context(), request.operation_id, &command)
                .await
                .map_err(map_development)?;
            if applied.head_revision.is_some() {
                return Err(DevelopmentServiceError::Corruption);
            }
            let refreshed = self.refresh_after_commit(applied.serving_revision).await?;
            let snapshot = self
                .development
                .snapshot(self.config.context())
                .await
                .map_err(map_after_commit)?;
            let binding = snapshot
                .workspace_binding(&request.workspace_ref)
                .ok_or(DevelopmentServiceError::Corruption)?;
            if binding.workspace_id != request.workspace_id || binding.head_revision.is_some() {
                return Err(DevelopmentServiceError::Corruption);
            }
            Ok(DevelopmentCreateWorkspaceResponseV1 {
                request_id,
                workspace: workspace_state(binding),
                development_revision: refreshed,
                replayed: applied.replayed,
            })
        }
        .await;
        self.finish(
            DevelopmentAuditOperation::Create,
            request_id,
            Some(request.operation_id),
            credential,
            now,
            &result,
        );
        result
    }

    /// Persists one validated package artifact-first, registers its candidate Release, moves one
    /// Workspace HEAD by CAS, and refreshes serving. It deliberately leaves shared Cron activation
    /// unchanged; durable `runAfter`/`runAt` pins remain embedded in the Dev Revision.
    ///
    /// # Errors
    ///
    /// Returns sanitized auth, policy, package, CAS, dependency, uncertain, or corruption errors.
    pub async fn publish(
        &self,
        request_id: RequestId,
        bearer: &str,
        request: DevelopmentPublishRequestV1,
    ) -> Result<DevelopmentPublishResponseV1, DevelopmentServiceError> {
        let now = self.clock.now()?;
        let mut credential = None;
        let result = async {
            let identity = self.authenticate(bearer, now).await?;
            credential = Some(identity.credential_id);
            self.publish_authenticated(request_id, now, &identity, &request)
                .await
        }
        .await;
        self.finish(
            DevelopmentAuditOperation::Publish,
            request_id,
            Some(request.operation_id),
            credential,
            now,
            &result,
        );
        result
    }

    async fn publish_authenticated(
        &self,
        request_id: RequestId,
        now: runku_value::TimestampMicros,
        identity: &DevelopmentIdentity,
        request: &DevelopmentPublishRequestV1,
    ) -> Result<DevelopmentPublishResponseV1, DevelopmentServiceError> {
        self.config.validate_policy()?;
        if request.project_id != self.config.scope.project_id() {
            return Err(DevelopmentServiceError::Forbidden);
        }
        if request.manifest.project_id != request.project_id
            || encode_release_manifest(&request.manifest).map_err(map_release)?
                != request.manifest_bytes
        {
            return Err(DevelopmentServiceError::InvalidRequest);
        }
        decode_safe_esm_bundle(&request.artifact_bytes)
            .map_err(map_release)?
            .verify_manifest(&request.manifest, &request.artifact_bytes)
            .map_err(map_release)?;
        let manifest_digest = Sha256Digest::of(&request.manifest_bytes);
        let revision_id = derive_development_revision_id_v1(
            self.config.scope,
            request.operation_id,
            &request.workspace_ref,
            manifest_digest,
        );
        let before = self
            .development
            .snapshot(self.config.context())
            .await
            .map_err(map_development)?;
        let created_at = match before.resolve_revision(revision_id).ok() {
            Some(existing)
                if existing.revision.release_id == request.manifest.release_id
                    && existing.revision.manifest_digest == manifest_digest
                    && existing.revision.manifest_bytes == request.manifest_bytes
                    && existing.revision.actor == identity.actor =>
            {
                existing.revision.created_at
            }
            Some(_) => return Err(DevelopmentServiceError::Conflict),
            None => now,
        };
        let revision = DevelopmentRevisionEntry {
            revision_id,
            release_id: request.manifest.release_id,
            manifest_digest,
            manifest_bytes: request.manifest_bytes.clone(),
            actor: identity.actor.clone(),
            created_at,
        };
        self.artifacts
            .put(&request.manifest.artifact, &request.artifact_bytes)
            .await
            .map_err(map_release)?;
        self.releases
            .apply(
                self.config.scope,
                request.operation_id,
                &ReleaseCommand::Register {
                    manifest_bytes: request.manifest_bytes.clone(),
                },
            )
            .await
            .map_err(map_release)?;
        let applied = self
            .development
            .apply(
                self.config.context(),
                request.operation_id,
                &DevelopmentCommand::PublishRevision {
                    workspace_ref: request.workspace_ref.clone(),
                    expected_head: request.expected_head,
                    revision,
                },
            )
            .await
            .map_err(map_development)?;
        if applied.head_revision != Some(revision_id) {
            return Err(DevelopmentServiceError::Corruption);
        }
        let refreshed = self.refresh_after_commit(applied.serving_revision).await?;
        Ok(DevelopmentPublishResponseV1 {
            request_id,
            revision_id,
            release_id: request.manifest.release_id,
            manifest_digest,
            development_revision: refreshed,
            replayed: applied.replayed,
        })
    }

    /// Revalidates and explicitly advances one Development candidate Release to `SERVABLE`, or
    /// durably records a compatibility-blocked result. Workspace HEAD, Channels, and Cron are not
    /// mutated.
    ///
    /// # Errors
    ///
    /// Returns sanitized auth, policy, candidate/baseline, lifecycle, dependency, uncertain, or
    /// corruption failures.
    pub async fn freeze(
        &self,
        request_id: RequestId,
        bearer: &str,
        request: DevelopmentFreezeRequestV1,
    ) -> Result<DevelopmentFreezeResponseV1, DevelopmentServiceError> {
        let now = self.clock.now()?;
        let mut credential = None;
        let result = async {
            let identity = self.authenticate(bearer, now).await?;
            credential = Some(identity.credential_id);
            self.config.validate_policy()?;
            self.freeze_authenticated(request_id, &request).await
        }
        .await;
        self.finish(
            DevelopmentAuditOperation::Freeze,
            request_id,
            Some(request.operation_id),
            credential,
            now,
            &result,
        );
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn freeze_authenticated(
        &self,
        request_id: RequestId,
        request: &DevelopmentFreezeRequestV1,
    ) -> Result<DevelopmentFreezeResponseV1, DevelopmentServiceError> {
        let development = self
            .development
            .snapshot(self.config.context())
            .await
            .map_err(map_development)?;
        let candidate_revision =
            development
                .resolve_release(request.release_id)
                .map_err(|error| match error {
                    DevelopmentError::RevisionNotFound => DevelopmentServiceError::NotFound,
                    other => map_development(other),
                })?;
        let candidate = self.release_package(request.release_id).await?;
        if encode_release_manifest(candidate.manifest()).map_err(map_release)?
            != candidate_revision.revision.manifest_bytes
        {
            return Err(DevelopmentServiceError::Corruption);
        }
        let baseline = match request.against_release_id {
            Some(release_id) => Some(self.release_package(release_id).await?),
            None => None,
        };

        let mut snapshot = self
            .releases
            .snapshot(self.config.scope)
            .await
            .map_err(map_release)?;
        let mut status = snapshot
            .release(request.release_id)
            .ok_or(DevelopmentServiceError::NotFound)?
            .status;
        if request.against_release_id.is_some_and(|release_id| {
            snapshot.release(release_id).is_none_or(|entry| {
                !matches!(
                    entry.status,
                    ReleaseStatus::Servable | ReleaseStatus::Active | ReleaseStatus::Deprecated
                )
            })
        }) {
            return Err(DevelopmentServiceError::InvalidRequest);
        }
        if matches!(status, ReleaseStatus::Servable | ReleaseStatus::Active) {
            return Ok(DevelopmentFreezeResponseV1 {
                request_id,
                release_id: request.release_id,
                outcome: DevelopmentFreezeOutcomeV1::Servable,
                diagnostics: vec![],
                serving_revision: snapshot.revision(),
                replayed: true,
            });
        }
        if status == ReleaseStatus::CompatibilityBlocked {
            let diagnostics = freeze_diagnostics(compatibility(&candidate, baseline.as_ref())?);
            if !diagnostics.is_empty() {
                return Ok(DevelopmentFreezeResponseV1 {
                    request_id,
                    release_id: request.release_id,
                    outcome: DevelopmentFreezeOutcomeV1::CompatibilityBlocked,
                    diagnostics,
                    serving_revision: snapshot.revision(),
                    replayed: true,
                });
            }
            self.freeze_transition(
                request,
                DevelopmentFreezeStageV1::Validating,
                ReleaseStatus::CompatibilityBlocked,
                ReleaseStatus::Validating,
            )
            .await?;
            status = ReleaseStatus::Validating;
        } else {
            let ownership = self
                .freeze_transition(
                    request,
                    DevelopmentFreezeStageV1::Building,
                    ReleaseStatus::Created,
                    ReleaseStatus::Building,
                )
                .await?;
            snapshot = self
                .releases
                .snapshot(self.config.scope)
                .await
                .map_err(map_release)?;
            status = snapshot
                .release(request.release_id)
                .ok_or(DevelopmentServiceError::Corruption)?
                .status;
            if !ownership.replayed && status != ReleaseStatus::Building {
                return Err(DevelopmentServiceError::Corruption);
            }
            if status == ReleaseStatus::Building {
                self.freeze_transition(
                    request,
                    DevelopmentFreezeStageV1::Validating,
                    ReleaseStatus::Building,
                    ReleaseStatus::Validating,
                )
                .await?;
                status = ReleaseStatus::Validating;
            }
        }

        if status == ReleaseStatus::Validating {
            let diagnostics = freeze_diagnostics(compatibility(&candidate, baseline.as_ref())?);
            if !diagnostics.is_empty() {
                let applied = self
                    .freeze_transition(
                        request,
                        DevelopmentFreezeStageV1::CompatibilityBlocked,
                        ReleaseStatus::Validating,
                        ReleaseStatus::CompatibilityBlocked,
                    )
                    .await?;
                let revision = self
                    .refresh_release_after_commit(applied.serving_revision)
                    .await?;
                return Ok(DevelopmentFreezeResponseV1 {
                    request_id,
                    release_id: request.release_id,
                    outcome: DevelopmentFreezeOutcomeV1::CompatibilityBlocked,
                    diagnostics,
                    serving_revision: revision,
                    replayed: applied.replayed,
                });
            }
            self.freeze_transition(
                request,
                DevelopmentFreezeStageV1::Ready,
                ReleaseStatus::Validating,
                ReleaseStatus::Ready,
            )
            .await?;
            status = ReleaseStatus::Ready;
        }
        if status == ReleaseStatus::Ready {
            let applied = self
                .freeze_transition(
                    request,
                    DevelopmentFreezeStageV1::Servable,
                    ReleaseStatus::Ready,
                    ReleaseStatus::Servable,
                )
                .await?;
            let revision = self
                .refresh_release_after_commit(applied.serving_revision)
                .await?;
            return Ok(DevelopmentFreezeResponseV1 {
                request_id,
                release_id: request.release_id,
                outcome: DevelopmentFreezeOutcomeV1::Servable,
                diagnostics: vec![],
                serving_revision: revision,
                replayed: applied.replayed,
            });
        }
        Err(DevelopmentServiceError::Conflict)
    }

    async fn freeze_transition(
        &self,
        request: &DevelopmentFreezeRequestV1,
        stage: DevelopmentFreezeStageV1,
        expected: ReleaseStatus,
        next: ReleaseStatus,
    ) -> Result<runku_releases::ReleaseCommandResult, DevelopmentServiceError> {
        self.releases
            .apply(
                self.config.scope,
                derive_development_freeze_operation_id_v1(
                    request.operation_id,
                    request.release_id,
                    request.against_release_id,
                    stage,
                ),
                &ReleaseCommand::Transition {
                    release_id: request.release_id,
                    expected,
                    next,
                },
            )
            .await
            .map_err(map_release)
    }

    async fn release_package(
        &self,
        release_id: runku_core::ReleaseId,
    ) -> Result<ReleasePackage, DevelopmentServiceError> {
        let manifest = self
            .releases
            .manifest(self.config.scope, release_id)
            .await
            .map_err(map_release)?;
        let artifact = self
            .artifacts
            .get(&manifest.artifact)
            .await
            .map_err(map_release)?;
        ReleasePackage::load(manifest, &artifact).map_err(map_compatibility)
    }

    /// Returns aggregate non-cardinal service counters.
    #[must_use]
    pub fn telemetry(&self) -> DevelopmentServiceTelemetrySnapshot {
        DevelopmentServiceTelemetrySnapshot {
            state_successes: self.counters.state_successes.load(Ordering::Relaxed),
            create_successes: self.counters.create_successes.load(Ordering::Relaxed),
            publish_successes: self.counters.publish_successes.load(Ordering::Relaxed),
            freeze_successes: self.counters.freeze_successes.load(Ordering::Relaxed),
            authentication_failures: self
                .counters
                .authentication_failures
                .load(Ordering::Relaxed),
            policy_rejections: self.counters.policy_rejections.load(Ordering::Relaxed),
            conflicts: self.counters.conflicts.load(Ordering::Relaxed),
            retryable_failures: self.counters.retryable_failures.load(Ordering::Relaxed),
            admission_rejections: self.counters.admission_rejections.load(Ordering::Relaxed),
            deadline_responses: self.counters.deadline_responses.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_admission_rejection(&self) {
        self.counters
            .admission_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_deadline_response(&self) {
        self.counters
            .deadline_responses
            .fetch_add(1, Ordering::Relaxed);
    }

    async fn authenticate(
        &self,
        bearer: &str,
        now: runku_value::TimestampMicros,
    ) -> Result<DevelopmentIdentity, DevelopmentServiceError> {
        let key = ParsedDevelopmentKey::from_str(bearer)
            .map_err(|_| DevelopmentServiceError::Unauthenticated)?;
        self.access
            .resolve_key(self.config.scope, &key, &self.crypto, now)
            .await
            .map_err(map_access)
    }

    async fn refresh_after_commit(
        &self,
        committed_revision: u64,
    ) -> Result<u64, DevelopmentServiceError> {
        let refreshed = self.refresher.refresh().await.map_err(map_after_commit)?;
        if refreshed < committed_revision {
            return Err(DevelopmentServiceError::ResultUncertain);
        }
        Ok(refreshed)
    }

    async fn refresh_release_after_commit(
        &self,
        committed_revision: u64,
    ) -> Result<u64, DevelopmentServiceError> {
        let refreshed = self
            .release_refresher
            .refresh()
            .await
            .map_err(map_release_after_commit)?;
        if refreshed < committed_revision {
            return Err(DevelopmentServiceError::ResultUncertain);
        }
        Ok(refreshed)
    }

    #[allow(clippy::too_many_arguments)]
    fn finish<T>(
        &self,
        operation: DevelopmentAuditOperation,
        request_id: RequestId,
        operation_id: Option<OperationId>,
        credential_id: Option<runku_core::DevelopmentCredentialId>,
        occurred_at: runku_value::TimestampMicros,
        result: &Result<T, DevelopmentServiceError>,
    ) {
        let error = result.as_ref().err().copied();
        let outcome = match error {
            None => DevelopmentAuditOutcome::Succeeded,
            Some(error) if error.retryable() => DevelopmentAuditOutcome::Retryable,
            Some(
                DevelopmentServiceError::InvalidRequest
                | DevelopmentServiceError::Unauthenticated
                | DevelopmentServiceError::Forbidden
                | DevelopmentServiceError::NotFound
                | DevelopmentServiceError::Conflict
                | DevelopmentServiceError::PolicyDenied
                | DevelopmentServiceError::LimitExceeded,
            ) => DevelopmentAuditOutcome::Rejected,
            Some(_) => DevelopmentAuditOutcome::Failed,
        };
        match (operation, error) {
            (DevelopmentAuditOperation::State, None) => {
                self.counters
                    .state_successes
                    .fetch_add(1, Ordering::Relaxed);
            }
            (DevelopmentAuditOperation::Create, None) => {
                self.counters
                    .create_successes
                    .fetch_add(1, Ordering::Relaxed);
            }
            (DevelopmentAuditOperation::Publish, None) => {
                self.counters
                    .publish_successes
                    .fetch_add(1, Ordering::Relaxed);
            }
            (DevelopmentAuditOperation::Freeze, None) => {
                self.counters
                    .freeze_successes
                    .fetch_add(1, Ordering::Relaxed);
            }
            (_, Some(DevelopmentServiceError::Unauthenticated)) => {
                self.counters
                    .authentication_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            (_, Some(DevelopmentServiceError::PolicyDenied)) => {
                self.counters
                    .policy_rejections
                    .fetch_add(1, Ordering::Relaxed);
            }
            (_, Some(DevelopmentServiceError::Conflict)) => {
                self.counters.conflicts.fetch_add(1, Ordering::Relaxed);
            }
            (_, Some(error)) if error.retryable() => {
                self.counters
                    .retryable_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        self.audit.try_emit(DevelopmentAuditEvent {
            request_id,
            scope: self.config.scope,
            operation,
            operation_id,
            credential_id,
            error: error.map(DevelopmentServiceError::wire),
            outcome,
            occurred_at,
        });
    }
}

fn workspace_state(binding: &runku_development::WorkspaceBinding) -> DevelopmentWorkspaceStateV1 {
    DevelopmentWorkspaceStateV1 {
        workspace_id: binding.workspace_id,
        workspace_ref: binding.workspace_ref.clone(),
        head_revision: binding.head_revision,
    }
}

fn map_access(error: DevelopmentAccessError) -> DevelopmentServiceError {
    match error {
        DevelopmentAccessError::InvalidCredential => DevelopmentServiceError::Unauthenticated,
        DevelopmentAccessError::NotFound => DevelopmentServiceError::NotFound,
        DevelopmentAccessError::Conflict => DevelopmentServiceError::Conflict,
        DevelopmentAccessError::LimitExceeded => DevelopmentServiceError::LimitExceeded,
        DevelopmentAccessError::Unavailable => DevelopmentServiceError::Unavailable,
        DevelopmentAccessError::ResultUncertain => DevelopmentServiceError::ResultUncertain,
        DevelopmentAccessError::Corruption => DevelopmentServiceError::Corruption,
        DevelopmentAccessError::InvalidInput
        | DevelopmentAccessError::Unsupported
        | DevelopmentAccessError::EntropyUnavailable => DevelopmentServiceError::Internal,
    }
}

fn map_development(error: DevelopmentError) -> DevelopmentServiceError {
    match error {
        DevelopmentError::InvalidInput | DevelopmentError::InvalidRevision => {
            DevelopmentServiceError::InvalidRequest
        }
        DevelopmentError::PolicyDenied => DevelopmentServiceError::PolicyDenied,
        DevelopmentError::WorkspaceNotFound
        | DevelopmentError::WorkspaceEmpty
        | DevelopmentError::RevisionNotFound => DevelopmentServiceError::NotFound,
        DevelopmentError::Conflict => DevelopmentServiceError::Conflict,
        DevelopmentError::LimitExceeded => DevelopmentServiceError::LimitExceeded,
        DevelopmentError::Unavailable => DevelopmentServiceError::Unavailable,
        DevelopmentError::ResultUncertain => DevelopmentServiceError::ResultUncertain,
        DevelopmentError::Corruption | DevelopmentError::InvalidSnapshot => {
            DevelopmentServiceError::Corruption
        }
        DevelopmentError::Unsupported => DevelopmentServiceError::Internal,
    }
}

fn map_after_commit(error: DevelopmentError) -> DevelopmentServiceError {
    match map_development(error) {
        DevelopmentServiceError::Corruption => DevelopmentServiceError::Corruption,
        _ => DevelopmentServiceError::ResultUncertain,
    }
}

fn compatibility(
    candidate: &ReleasePackage,
    baseline: Option<&ReleasePackage>,
) -> Result<Vec<runku_compatibility::CompatibilityDiagnostic>, DevelopmentServiceError> {
    match baseline {
        Some(baseline) => CompatibilityEngine::compare(baseline, candidate)
            .map(|report| report.diagnostics)
            .map_err(map_compatibility),
        None => Ok(vec![]),
    }
}

fn freeze_diagnostics(
    diagnostics: Vec<runku_compatibility::CompatibilityDiagnostic>,
) -> Vec<DevelopmentFreezeDiagnosticV1> {
    const MAX_EXPOSED: usize = 128;
    let truncated = diagnostics.len() > MAX_EXPOSED;
    let retain = if truncated {
        MAX_EXPOSED - 1
    } else {
        MAX_EXPOSED
    };
    let mut projected = diagnostics
        .into_iter()
        .take(retain)
        .map(|diagnostic| DevelopmentFreezeDiagnosticV1 {
            code: diagnostic.code.to_owned(),
            subject: diagnostic.subject,
        })
        .collect::<Vec<_>>();
    if truncated {
        projected.push(DevelopmentFreezeDiagnosticV1 {
            code: "DIAGNOSTICS_TRUNCATED".to_owned(),
            subject: "release".to_owned(),
        });
    }
    projected
}

const fn map_compatibility(error: CompatibilityError) -> DevelopmentServiceError {
    match error {
        CompatibilityError::LimitExceeded => DevelopmentServiceError::LimitExceeded,
        CompatibilityError::InvalidRelease
        | CompatibilityError::InvalidArtifact
        | CompatibilityError::InvalidContract => DevelopmentServiceError::Corruption,
    }
}

fn map_release_after_commit(error: ReleaseError) -> DevelopmentServiceError {
    match map_release(error) {
        DevelopmentServiceError::Corruption => DevelopmentServiceError::Corruption,
        _ => DevelopmentServiceError::ResultUncertain,
    }
}

fn map_release(error: ReleaseError) -> DevelopmentServiceError {
    match error {
        ReleaseError::InvalidManifest
        | ReleaseError::InvalidArtifact
        | ReleaseError::Unsupported
        | ReleaseError::DigestMismatch
        | ReleaseError::DescriptorMismatch
        | ReleaseError::InvalidTransition => DevelopmentServiceError::InvalidRequest,
        ReleaseError::LimitExceeded => DevelopmentServiceError::LimitExceeded,
        ReleaseError::NotFound
        | ReleaseError::ReleaseNotFound
        | ReleaseError::ChannelNotFound
        | ReleaseError::DefaultChannelMissing => DevelopmentServiceError::NotFound,
        ReleaseError::OperationIdReused | ReleaseError::RepositoryConflict => {
            DevelopmentServiceError::Conflict
        }
        ReleaseError::Busy | ReleaseError::Unavailable => DevelopmentServiceError::Unavailable,
        ReleaseError::ResultUncertain => DevelopmentServiceError::ResultUncertain,
        ReleaseError::Corruption | ReleaseError::InvalidSnapshot => {
            DevelopmentServiceError::Corruption
        }
        ReleaseError::Internal
        | ReleaseError::ProductionBackendUnsupported
        | ReleaseError::ReleaseNotServable
        | ReleaseError::ReleaseRetired
        | ReleaseError::WorkspaceUnsupported => DevelopmentServiceError::Internal,
    }
}
