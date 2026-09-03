//! Product Base composition from a decoded public call to the existing execution engines.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_core::{CodeTarget, EnvironmentScope, InvocationId, OperationId, PinnedCode};
use runku_data::{ScheduledInvocationRecord, StoreError};
use runku_development::{
    DevelopmentContext, DevelopmentError, DevelopmentRepository, DevelopmentResolution,
    DevelopmentRevisionResolution, DevelopmentSnapshot,
};
use runku_execution::{
    ActionExecutionError, ActionExecutor, ExecutionError, MutationExecutionError, MutationExecutor,
    NodeActionExecutor, QueryExecutor, QueryOutcome, ScheduledInvocationRunner,
    ScheduledRunFailure,
};
use runku_identity::{
    ApplicationCredentialResolver, ApplicationScope, AuthBoundary, AuthGateway, AuthInput,
    AuthenticatedPrincipal, GuestKeyring, IdentityError, KeyringCrypto, PrincipalContext,
    PrincipalEvidence, PrincipalId, PrincipalKind, RequestIdentity,
};
use runku_identity_provider::{JwtProviderManager, ProviderError};
use runku_node_runtime::FullNodeActionRuntime;
use runku_observability::OperationalLogSink;
use runku_protocol::{ErrorClassV1, PublicErrorV1, SuccessMetadataV1};
use runku_realtime::{SubscriptionRunFailure, SubscriptionRunner, SubscriptionSpec};
use runku_releases::{
    ArtifactDescriptor, ArtifactStore, Capability, EffectiveRelease, FunctionManifest,
    FunctionType, ReleaseError, ReleaseManifestV1, ReleaseRepository, ReleaseRouter, RuntimeClass,
    ServingSnapshot, Sha256Digest,
};
use runku_runtime::{
    DataReadError, FileStorage, HttpsEgress, InvocationRequest, RuntimeError, ScheduleError,
};
use runku_schema::SchemaError;
use runku_value::TimestampMicros;

use crate::{
    GatewayFailure, GatewaySuccess, InvocationContext, InvocationService, InvokeCallV1,
    PresentedCredentials, RealtimePreparedSubscription, RealtimeQueryService,
    RealtimeSubscribeContext,
};

struct ProductNodeActionExecutor(Arc<dyn FullNodeActionRuntime>);

impl fmt::Debug for ProductNodeActionExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductNodeActionExecutor")
    }
}

#[async_trait]
impl NodeActionExecutor for ProductNodeActionExecutor {
    async fn execute_node(
        &self,
        request: InvocationRequest,
    ) -> Result<runku_value::CanonicalValue, RuntimeError> {
        self.0.execute(request).await.map(|outcome| outcome.value)
    }
}

/// Result of attempting to publish a newer serving snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServingRefresh {
    /// A strictly newer coherent revision replaced the current snapshot.
    Published {
        /// Newly published serving revision.
        revision: u64,
    },
    /// Repository returned the exact snapshot already published.
    Unchanged {
        /// Revision that was already current.
        revision: u64,
    },
}

/// Process-local atomically replaced serving view for one trusted Environment scope.
pub struct ServingCatalog {
    scope: EnvironmentScope,
    repository: Arc<dyn ReleaseRepository>,
    snapshot: RwLock<Option<Arc<ServingSnapshot>>>,
}

impl fmt::Debug for ServingCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServingCatalog")
            .field("scope", &self.scope)
            .field("repository_backend", &self.repository.backend())
            .finish_non_exhaustive()
    }
}

impl ServingCatalog {
    /// Loads and validates the initial serving snapshot before accepting traffic.
    ///
    /// # Errors
    ///
    /// Returns the repository/snapshot failure; no empty fallback catalog is created.
    pub async fn load(
        scope: EnvironmentScope,
        repository: Arc<dyn ReleaseRepository>,
    ) -> Result<Self, ReleaseError> {
        let snapshot = repository.snapshot(scope).await?;
        if snapshot.scope() != scope {
            return Err(ReleaseError::InvalidSnapshot);
        }
        Ok(Self {
            scope,
            repository,
            snapshot: RwLock::new(Some(Arc::new(snapshot))),
        })
    }

    /// Creates a catalog for an Environment that may not have registered its first Release yet.
    /// The first successful [`Self::refresh`] publishes the authoritative snapshot; resolution
    /// returns Release-not-found until then.
    ///
    /// # Errors
    ///
    /// Propagates repository errors other than the expected absent-scope state.
    pub async fn load_allow_empty(
        scope: EnvironmentScope,
        repository: Arc<dyn ReleaseRepository>,
    ) -> Result<Self, ReleaseError> {
        let snapshot = match repository.snapshot(scope).await {
            Ok(snapshot) => {
                if snapshot.scope() != scope {
                    return Err(ReleaseError::InvalidSnapshot);
                }
                Some(Arc::new(snapshot))
            }
            Err(ReleaseError::NotFound | ReleaseError::ReleaseNotFound) => None,
            Err(error) => return Err(error),
        };
        Ok(Self {
            scope,
            repository,
            snapshot: RwLock::new(snapshot),
        })
    }

    /// Resolves against one immutable snapshot captured under a short read lock.
    ///
    /// # Errors
    ///
    /// Returns stable routing failures or unavailable if local synchronization is poisoned.
    pub fn resolve(
        &self,
        target: &runku_core::CodeTarget,
    ) -> Result<EffectiveRelease, ReleaseError> {
        let snapshot = self
            .snapshot
            .read()
            .map_err(|_| ReleaseError::Unavailable)?
            .clone()
            .ok_or(ReleaseError::ReleaseNotFound)?;
        ReleaseRouter::new((*snapshot).clone()).resolve(target)
    }

    /// Fetches a coherent repository snapshot and publishes only a monotonic revision.
    ///
    /// # Errors
    ///
    /// Fails closed on scope drift, revision rollback/equivocation, repository, or lock failure.
    pub async fn refresh(&self) -> Result<ServingRefresh, ReleaseError> {
        let next = self.repository.snapshot(self.scope).await?;
        if next.scope() != self.scope {
            return Err(ReleaseError::InvalidSnapshot);
        }
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| ReleaseError::Unavailable)?;
        if let Some(existing) = current.as_ref() {
            if next.revision() < existing.revision()
                || (next.revision() == existing.revision() && next != **existing)
            {
                return Err(ReleaseError::Corruption);
            }
            if next.revision() == existing.revision() {
                return Ok(ServingRefresh::Unchanged {
                    revision: next.revision(),
                });
            }
        }
        let revision = next.revision();
        *current = Some(Arc::new(next));
        Ok(ServingRefresh::Published { revision })
    }

    /// Trusted Environment scope fixed by this catalog.
    #[must_use]
    pub const fn scope(&self) -> EnvironmentScope {
        self.scope
    }
}

/// Process-local coherent Development Workspace view for one trusted Environment.
pub struct DevelopmentCatalog {
    context: DevelopmentContext,
    repository: Arc<dyn DevelopmentRepository>,
    snapshot: RwLock<Arc<DevelopmentSnapshot>>,
}

impl fmt::Debug for DevelopmentCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DevelopmentCatalog")
            .field("scope", &self.context.scope)
            .field("backend", &self.repository.backend())
            .finish_non_exhaustive()
    }
}

impl DevelopmentCatalog {
    /// Loads and validates the initial snapshot before Workspace traffic is accepted.
    ///
    /// # Errors
    ///
    /// Returns repository, policy, or snapshot failures without an empty fallback.
    pub async fn load(
        context: DevelopmentContext,
        repository: Arc<dyn DevelopmentRepository>,
    ) -> Result<Self, DevelopmentError> {
        context.validate()?;
        let snapshot = repository.snapshot(context).await?;
        if snapshot.scope() != context.scope {
            return Err(DevelopmentError::InvalidSnapshot);
        }
        Ok(Self {
            context,
            repository,
            snapshot: RwLock::new(Arc::new(snapshot)),
        })
    }

    /// Resolves one Workspace against a single immutable local snapshot.
    ///
    /// # Errors
    ///
    /// Fails closed on policy, unknown/empty Workspace, or poisoned synchronization.
    pub fn resolve(
        &self,
        workspace: &runku_core::WorkspaceRef,
    ) -> Result<DevelopmentResolution, DevelopmentError> {
        self.context.validate()?;
        self.snapshot
            .read()
            .map_err(|_| DevelopmentError::Unavailable)?
            .resolve(workspace)
    }

    /// Resolves one immutable Development Revision without re-reading Workspace HEAD.
    ///
    /// # Errors
    ///
    /// Fails closed on policy, unknown revision, corruption, or poisoned synchronization.
    pub fn resolve_revision(
        &self,
        revision_id: runku_core::DevRevisionId,
    ) -> Result<DevelopmentRevisionResolution, DevelopmentError> {
        self.context.validate()?;
        self.snapshot
            .read()
            .map_err(|_| DevelopmentError::Unavailable)?
            .resolve_revision(revision_id)
    }

    /// Loads a coherent snapshot and publishes only a monotonic revision.
    ///
    /// # Errors
    ///
    /// Rejects scope drift, rollback, equivocation, or repository failure.
    pub async fn refresh(&self) -> Result<ServingRefresh, DevelopmentError> {
        let next = self.repository.snapshot(self.context).await?;
        if next.scope() != self.context.scope {
            return Err(DevelopmentError::InvalidSnapshot);
        }
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| DevelopmentError::Unavailable)?;
        if next.revision() < current.revision()
            || (next.revision() == current.revision() && next != **current)
        {
            return Err(DevelopmentError::Corruption);
        }
        if next.revision() == current.revision() {
            return Ok(ServingRefresh::Unchanged {
                revision: next.revision(),
            });
        }
        let revision = next.revision();
        *current = Arc::new(next);
        Ok(ServingRefresh::Published { revision })
    }

    /// Returns the fixed trusted Environment scope.
    #[must_use]
    pub const fn scope(&self) -> EnvironmentScope {
        self.context.scope
    }
}

/// Sanitized result of a bearer verification adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalVerificationError {
    /// Supplied evidence is malformed, invalid, expired, or not accepted by this provider.
    Invalid,
    /// Provider discovery/JWKS/network/cache is temporarily unavailable.
    Unavailable,
    /// Provider verification exceeded its bounded deadline.
    Timeout,
    /// Trusted verifier configuration or invariant failed.
    Internal,
}

/// Pluggable functional bearer verifier independent from application-key resolution.
#[async_trait]
pub trait PrincipalVerifier: fmt::Debug + Send + Sync {
    /// Verifies supplied evidence into a token-free principal.
    async fn verify(
        &self,
        scope: EnvironmentScope,
        token: &str,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<PrincipalEvidence, PrincipalVerificationError>;
}

#[async_trait]
impl PrincipalVerifier for JwtProviderManager {
    async fn verify(
        &self,
        _scope: EnvironmentScope,
        token: &str,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<PrincipalEvidence, PrincipalVerificationError> {
        self.verify(token, crypto, now)
            .await
            .map_err(map_provider_verification)
    }
}

#[async_trait]
impl PrincipalVerifier for GuestKeyring {
    async fn verify(
        &self,
        scope: EnvironmentScope,
        token: &str,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<PrincipalEvidence, PrincipalVerificationError> {
        self.verify(scope, token, crypto, now)
            .map_err(|_| PrincipalVerificationError::Invalid)
    }
}

/// Clock injected into auth decisions and tests.
pub trait GatewayClock: fmt::Debug + Send + Sync {
    /// Current UTC Unix time in microseconds.
    ///
    /// # Errors
    ///
    /// Returns an internal clock failure when UTC cannot be represented safely.
    fn now(&self) -> Result<TimestampMicros, PrincipalVerificationError>;
}

/// Operating-system wall clock implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemGatewayClock;

impl GatewayClock for SystemGatewayClock {
    fn now(&self) -> Result<TimestampMicros, PrincipalVerificationError> {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PrincipalVerificationError::Internal)?
            .as_micros();
        let micros = i64::try_from(micros).map_err(|_| PrincipalVerificationError::Internal)?;
        Ok(TimestampMicros::new(micros))
    }
}

/// Validated semantic invocation limits for one Environment-serving process.
#[derive(Clone, Copy, Debug)]
pub struct ProductInvocationConfig {
    /// Trusted Product/Environment owner of every request reaching this Router instance.
    pub scope: EnvironmentScope,
    /// Runtime wall budget, required to fit inside the outer HTTP deadline.
    pub execution_timeout: Duration,
    /// Maximum process memory retained for verified immutable artifact bytes.
    pub max_cached_artifact_bytes: usize,
}

impl ProductInvocationConfig {
    /// Validates the 1ms–5min Runtime envelope limit.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive execution timeouts.
    pub fn validate(self) -> Result<Self, GatewayFailure> {
        if self.execution_timeout < Duration::from_millis(1)
            || self.execution_timeout > Duration::from_mins(5)
            || !(1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_cached_artifact_bytes)
        {
            return Err(failure(
                ErrorClassV1::Internal,
                "GATEWAY_CONFIGURATION_INVALID",
                false,
            ));
        }
        Ok(self)
    }
}

/// Aggregate content-addressed artifact cache counters without tenant labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArtifactCacheTelemetrySnapshot {
    /// Requests served from verified process memory.
    pub hits: u64,
    /// Requests requiring a cold artifact-store read.
    pub misses: u64,
    /// Entries evicted to remain within byte/entry limits.
    pub evictions: u64,
    /// Current retained verified bytes.
    pub retained_bytes: u64,
    /// Current retained entry count.
    pub entries: u64,
}

#[derive(Debug, Default)]
struct ArtifactCacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct ArtifactCacheState {
    entries: BTreeMap<Sha256Digest, Arc<[u8]>>,
    order: VecDeque<Sha256Digest>,
    retained_bytes: usize,
}

struct ArtifactCache {
    store: Arc<dyn ArtifactStore>,
    maximum_bytes: usize,
    state: Mutex<ArtifactCacheState>,
    cold_gate: tokio::sync::Mutex<()>,
    counters: ArtifactCacheCounters,
}

struct ResolvedCode {
    effective: EffectiveRelease,
    manifest: Arc<ReleaseManifestV1>,
    pinned_code: PinnedCode,
}

impl fmt::Debug for ArtifactCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactCache")
            .field("maximum_bytes", &self.maximum_bytes)
            .field("telemetry", &self.telemetry())
            .finish_non_exhaustive()
    }
}

impl ArtifactCache {
    fn new(store: Arc<dyn ArtifactStore>, maximum_bytes: usize) -> Self {
        Self {
            store,
            maximum_bytes,
            state: Mutex::new(ArtifactCacheState::default()),
            cold_gate: tokio::sync::Mutex::new(()),
            counters: ArtifactCacheCounters::default(),
        }
    }

    async fn load(&self, descriptor: &ArtifactDescriptor) -> Result<Arc<[u8]>, ReleaseError> {
        if let Some(bytes) = self.get(descriptor.digest)? {
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        let _cold = self.cold_gate.lock().await;
        if let Some(bytes) = self.get(descriptor.digest)? {
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        self.counters.misses.fetch_add(1, Ordering::Relaxed);
        let bytes: Arc<[u8]> = self.store.get(descriptor).await?.into();
        if bytes.len() > self.maximum_bytes {
            return Ok(bytes);
        }
        let mut state = self.state.lock().map_err(|_| ReleaseError::Unavailable)?;
        while state.entries.len() >= 1024
            || state.retained_bytes.saturating_add(bytes.len()) > self.maximum_bytes
        {
            let Some(oldest) = state.order.pop_front() else {
                return Err(ReleaseError::Internal);
            };
            if let Some(removed) = state.entries.remove(&oldest) {
                state.retained_bytes = state.retained_bytes.saturating_sub(removed.len());
                self.counters.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        state.retained_bytes = state.retained_bytes.saturating_add(bytes.len());
        state.order.push_back(descriptor.digest);
        state.entries.insert(descriptor.digest, Arc::clone(&bytes));
        Ok(bytes)
    }

    fn get(&self, digest: Sha256Digest) -> Result<Option<Arc<[u8]>>, ReleaseError> {
        self.state
            .lock()
            .map_err(|_| ReleaseError::Unavailable)
            .map(|state| state.entries.get(&digest).cloned())
    }

    fn telemetry(&self) -> ArtifactCacheTelemetrySnapshot {
        let (retained_bytes, entries) = self.state.lock().map_or((0, 0), |state| {
            (
                u64::try_from(state.retained_bytes).unwrap_or(u64::MAX),
                u64::try_from(state.entries.len()).unwrap_or(u64::MAX),
            )
        });
        ArtifactCacheTelemetrySnapshot {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            evictions: self.counters.evictions.load(Ordering::Relaxed),
            retained_bytes,
            entries,
        }
    }
}

/// Concrete Product Base service behind the framework-specific HTTP adapter.
pub struct ProductInvocationService {
    config: ProductInvocationConfig,
    catalog: Arc<ServingCatalog>,
    development: Option<Arc<DevelopmentCatalog>>,
    releases: Arc<dyn ReleaseRepository>,
    artifacts: ArtifactCache,
    credentials: Arc<dyn ApplicationCredentialResolver>,
    crypto: Arc<KeyringCrypto>,
    principals: Arc<dyn PrincipalVerifier>,
    clock: Arc<dyn GatewayClock>,
    query: QueryExecutor,
    mutation: MutationExecutor,
    action: ActionExecutor,
    full_node: Option<Arc<dyn FullNodeActionRuntime>>,
    https: Option<Arc<dyn HttpsEgress>>,
    operational_logs: Option<Arc<dyn OperationalLogSink>>,
}

impl fmt::Debug for ProductInvocationService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductInvocationService")
            .field("scope", &self.config.scope)
            .field("catalog", &self.catalog)
            .field("development_configured", &self.development.is_some())
            .field("release_backend", &self.releases.backend())
            .field("query", &self.query)
            .field("mutation", &self.mutation)
            .field("action", &self.action)
            .field("full_node_configured", &self.full_node.is_some())
            .field("https_configured", &self.https.is_some())
            .finish_non_exhaustive()
    }
}

impl ProductInvocationService {
    /// Composes already-initialized Product Base dependencies.
    ///
    /// # Errors
    ///
    /// Rejects invalid timeout or a serving catalog bound to another Environment.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: ProductInvocationConfig,
        catalog: Arc<ServingCatalog>,
        releases: Arc<dyn ReleaseRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        credentials: Arc<dyn ApplicationCredentialResolver>,
        crypto: Arc<KeyringCrypto>,
        principals: Arc<dyn PrincipalVerifier>,
        clock: Arc<dyn GatewayClock>,
        query: QueryExecutor,
        mutation: MutationExecutor,
        action: ActionExecutor,
        https: Option<Arc<dyn HttpsEgress>>,
    ) -> Result<Self, GatewayFailure> {
        let config = config.validate()?;
        if catalog.scope() != config.scope {
            return Err(failure(
                ErrorClassV1::Internal,
                "GATEWAY_SCOPE_MISMATCH",
                false,
            ));
        }
        let action = action.with_nested_executors(query.clone(), mutation.clone());
        let action = match &https {
            Some(https) => action.with_https_egress(Arc::clone(https)),
            None => action,
        };
        Ok(Self {
            config,
            catalog,
            development: None,
            releases,
            artifacts: ArtifactCache::new(artifacts, config.max_cached_artifact_bytes),
            credentials,
            crypto,
            principals,
            clock,
            query,
            mutation,
            action,
            full_node: None,
            https,
            operational_logs: None,
        })
    }

    /// Attaches the optional Product Base Development Workspace serving catalog.
    ///
    /// # Errors
    ///
    /// Rejects a catalog bound to another Environment. Absence keeps Workspace targets
    /// unsupported without affecting Release/Channel serving.
    pub fn with_development_catalog(
        mut self,
        catalog: Arc<DevelopmentCatalog>,
    ) -> Result<Self, GatewayFailure> {
        if catalog.scope() != self.config.scope {
            return Err(failure(
                ErrorClassV1::Internal,
                "GATEWAY_SCOPE_MISMATCH",
                false,
            ));
        }
        self.development = Some(catalog);
        Ok(self)
    }

    /// Attaches Product Base operational logs to HTTP, Realtime, scheduled, and nested
    /// invocation envelopes created by this service.
    #[must_use]
    pub fn with_operational_logs(mut self, sink: Arc<dyn OperationalLogSink>) -> Self {
        self.operational_logs = Some(sink);
        self
    }

    /// Attaches capability-scoped application file storage to root and nested Actions.
    #[must_use]
    pub fn with_file_storage(mut self, storage: Arc<dyn FileStorage>) -> Self {
        self.action = self.action.clone().with_file_storage(storage);
        self
    }

    /// Attaches the opt-in out-of-process Full Node Action runtime.
    ///
    /// Without this adapter the gateway continues to reject every `full-node` Release. Attaching
    /// it enables only the narrow contract validated by
    /// [`ReleaseManifestV1::ensure_full_node_v1_supported`].
    #[must_use]
    pub fn with_full_node_runtime(mut self, runtime: Arc<dyn FullNodeActionRuntime>) -> Self {
        let dispatcher: Arc<dyn NodeActionExecutor> =
            Arc::new(ProductNodeActionExecutor(Arc::clone(&runtime)));
        self.action = self.action.clone().with_node_runtime(dispatcher);
        self.full_node = Some(runtime);
        self
    }

    /// Returns bounded aggregate asset-cache counters.
    #[must_use]
    pub fn artifact_cache_telemetry(&self) -> ArtifactCacheTelemetrySnapshot {
        self.artifacts.telemetry()
    }

    async fn resolve_code(&self, target: &CodeTarget) -> Result<ResolvedCode, GatewayFailure> {
        let (effective, manifest, pinned_code) = match target {
            CodeTarget::Workspace(workspace) => {
                let catalog = self.development.as_ref().ok_or_else(|| {
                    failure(ErrorClassV1::NotFound, "WORKSPACE_NOT_CONFIGURED", false)
                })?;
                let resolved = catalog.resolve(workspace).map_err(map_development)?;
                let pinned = resolved.pinned_code();
                let manifest = resolved.manifest;
                let effective = EffectiveRelease {
                    scope: resolved.scope,
                    serving_revision: resolved.serving_revision,
                    release_id: manifest.release_id,
                    manifest_digest: resolved.revision.manifest_digest,
                    artifact: manifest.artifact,
                    runtime_version: manifest.runtime_version.clone(),
                };
                validate_effective_manifest(&effective, &manifest, self.full_node.as_deref())?;
                (effective, manifest, pinned)
            }
            CodeTarget::Release(_) | CodeTarget::Channel(_) => {
                let effective = self.catalog.resolve(target).map_err(map_release)?;
                let manifest = self
                    .releases
                    .manifest(self.config.scope, effective.release_id)
                    .await
                    .map_err(map_release)?;
                validate_effective_manifest(&effective, &manifest, self.full_node.as_deref())?;
                let pinned = PinnedCode::Release(effective.release_id);
                (effective, manifest, pinned)
            }
        };
        Ok(ResolvedCode {
            effective,
            manifest: Arc::new(manifest),
            pinned_code,
        })
    }

    async fn authorize_function(
        &self,
        credentials: &PresentedCredentials,
        function: &FunctionManifest,
        now: TimestampMicros,
    ) -> Result<Arc<RequestIdentity>, GatewayFailure> {
        if credentials.application_key().is_none() {
            return Err(failure(
                ErrorClassV1::Unauthenticated,
                "APPLICATION_CREDENTIAL_REQUIRED",
                false,
            ));
        }
        let principal = match credentials.bearer() {
            None => PrincipalEvidence::Absent,
            Some(token) => match self
                .principals
                .verify(self.config.scope, token, &self.crypto, now)
                .await
            {
                Ok(PrincipalEvidence::Valid(principal)) => PrincipalEvidence::Valid(principal),
                Ok(PrincipalEvidence::Absent | PrincipalEvidence::Invalid) => {
                    return Err(map_principal_verification(
                        PrincipalVerificationError::Invalid,
                    ));
                }
                Err(error) => return Err(map_principal_verification(error)),
            },
        };
        let auth_input = AuthInput::parse(
            AuthBoundary::External,
            credentials.application_key(),
            principal,
        )
        .map_err(map_identity)?;
        let identity = AuthGateway::new(self.credentials.as_ref(), &self.crypto)
            .authorize(
                self.config.scope,
                function.visibility,
                function.auth_policy,
                auth_input,
                now,
            )
            .await
            .map_err(map_identity)?;
        let can_invoke = identity.application.as_ref().is_some_and(|application| {
            application
                .scopes
                .iter()
                .any(|scope| scope.as_str() == "functions:invoke")
        });
        if !can_invoke {
            return Err(failure(
                ErrorClassV1::Forbidden,
                "APPLICATION_SCOPE_DENIED",
                false,
            ));
        }
        Ok(Arc::new(identity))
    }

    async fn invocation_request(
        &self,
        resolved: &ResolvedCode,
        function: &FunctionManifest,
        request_id: runku_core::RequestId,
        arguments: runku_value::CanonicalValue,
        cancellation: runku_runtime::CancellationToken,
        identity: Arc<RequestIdentity>,
    ) -> Result<InvocationRequest, GatewayFailure> {
        let artifact = self
            .artifacts
            .load(&resolved.manifest.artifact)
            .await
            .map_err(map_release)?;
        let mut request = InvocationRequest::new(
            self.config.scope,
            resolved.effective.release_id,
            request_id,
            InvocationId::generate(),
            function.id,
            Arc::clone(&resolved.manifest),
            artifact,
            arguments,
            self.config.execution_timeout,
            cancellation,
        )
        .map_err(map_runtime)?
        .with_pinned_code(resolved.pinned_code)
        .map_err(map_runtime)?
        .with_identity(identity);
        if let Some(logs) = &self.operational_logs {
            request = request.with_operational_logs(Arc::clone(logs));
        }
        if function.capabilities.contains(&Capability::NetworkHttps) {
            let https = self.https.as_ref().ok_or_else(|| {
                failure(ErrorClassV1::Unavailable, "ACTION_HTTPS_UNAVAILABLE", true)
            })?;
            request = request.with_https(Arc::clone(https)).map_err(map_runtime)?;
        }
        Ok(request)
    }

    async fn resolve_pinned(
        &self,
        spec: &SubscriptionSpec,
    ) -> Result<ResolvedCode, GatewayFailure> {
        let resolved = self.resolve_pinned_code(spec.pinned_code).await?;
        if resolved.effective.release_id != spec.release_id {
            return Err(failure(
                ErrorClassV1::Internal,
                "REALTIME_PIN_MISMATCH",
                false,
            ));
        }
        Ok(resolved)
    }

    async fn resolve_pinned_code(
        &self,
        pinned_code: PinnedCode,
    ) -> Result<ResolvedCode, GatewayFailure> {
        let (manifest, serving_revision) = match pinned_code {
            PinnedCode::Release(release_id) => {
                let manifest = self
                    .releases
                    .manifest(self.config.scope, release_id)
                    .await
                    .map_err(map_release)?;
                (manifest, 1)
            }
            PinnedCode::DevRevision(revision_id) => {
                let catalog = self.development.as_ref().ok_or_else(|| {
                    failure(ErrorClassV1::NotFound, "WORKSPACE_NOT_CONFIGURED", false)
                })?;
                let resolved = catalog
                    .resolve_revision(revision_id)
                    .map_err(map_development)?;
                (resolved.manifest, resolved.serving_revision)
            }
        };
        let effective = EffectiveRelease {
            scope: self.config.scope,
            serving_revision,
            release_id: manifest.release_id,
            manifest_digest: manifest.digest().map_err(map_release)?,
            artifact: manifest.artifact,
            runtime_version: manifest.runtime_version.clone(),
        };
        validate_effective_manifest(&effective, &manifest, self.full_node.as_deref())?;
        Ok(ResolvedCode {
            effective,
            manifest: Arc::new(manifest),
            pinned_code,
        })
    }

    async fn scheduled_identity(
        &self,
        function: &FunctionManifest,
        now: TimestampMicros,
    ) -> Result<Arc<RequestIdentity>, GatewayFailure> {
        let mut principal_seed = Vec::with_capacity(128);
        principal_seed.extend_from_slice(self.config.scope.project_id().to_string().as_bytes());
        principal_seed.extend_from_slice(self.config.scope.environment_id().to_string().as_bytes());
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::from_bytes(*Sha256Digest::of(&principal_seed).as_bytes()),
            PrincipalKind::System,
            "runku-system",
            ["function:invoke"
                .parse::<ApplicationScope>()
                .map_err(map_identity)?]
            .into_iter()
            .collect(),
            None,
            Some(now),
            None,
            1,
        )
        .map_err(map_identity)?;
        let input = AuthInput::parse(
            AuthBoundary::TrustedInternal,
            None,
            PrincipalEvidence::Valid(principal),
        )
        .map_err(map_identity)?;
        AuthGateway::new(self.credentials.as_ref(), &self.crypto)
            .authorize(
                self.config.scope,
                function.visibility,
                function.auth_policy,
                input,
                now,
            )
            .await
            .map(Arc::new)
            .map_err(map_identity)
    }

    #[allow(clippy::too_many_lines)]
    async fn invoke_inner(
        &self,
        context: InvocationContext,
        call: InvokeCallV1,
    ) -> Result<GatewaySuccess, GatewayFailure> {
        let (target, name, arguments, operation, requested_type) = match call {
            InvokeCallV1::Query(call) => (
                call.target,
                call.function,
                call.arguments,
                None,
                FunctionType::Query,
            ),
            InvokeCallV1::Mutation(call) => (
                call.target,
                call.function,
                call.arguments,
                Some(call.operation_id),
                FunctionType::Mutation,
            ),
            InvokeCallV1::Action(call) => (
                call.target,
                call.function,
                call.arguments,
                None,
                FunctionType::Action,
            ),
        };
        let resolved = self.resolve_code(&target).await?;
        let function = resolved
            .manifest
            .functions
            .iter()
            .find(|function| function.name == name)
            .cloned()
            .ok_or_else(|| failure(ErrorClassV1::NotFound, "FUNCTION_NOT_FOUND", false))?;
        if function.function_type != requested_type {
            return Err(failure(
                ErrorClassV1::InvalidRequest,
                "FUNCTION_TYPE_MISMATCH",
                false,
            ));
        }
        let now = self.clock.now().map_err(map_principal_verification)?;
        let identity = self
            .authorize_function(&context.credentials, &function, now)
            .await?;
        let request = self
            .invocation_request(
                &resolved,
                &function,
                context.request_id,
                arguments,
                context.cancellation,
                identity,
            )
            .await?;
        match requested_type {
            FunctionType::Query => {
                let outcome = self.query.execute(request).await.map_err(map_query)?;
                Ok(GatewaySuccess {
                    release_id: resolved.effective.release_id,
                    value: outcome.value,
                    metadata: SuccessMetadataV1::Query {
                        snapshot_sequence: outcome.snapshot_sequence,
                    },
                })
            }
            FunctionType::Mutation => {
                let operation = operation
                    .ok_or_else(|| failure(ErrorClassV1::Internal, "GATEWAY_INTERNAL", false))?;
                let outcome = self
                    .mutation
                    .execute(request, operation)
                    .await
                    .map_err(map_mutation)?;
                Ok(GatewaySuccess {
                    release_id: resolved.effective.release_id,
                    value: outcome.value,
                    metadata: SuccessMetadataV1::Mutation {
                        commit_sequence: outcome.commit_sequence,
                        replayed: outcome.replayed,
                        attempts: outcome.attempts,
                    },
                })
            }
            FunctionType::Action => {
                let outcome = self.action.execute(request).await.map_err(map_action)?;
                Ok(GatewaySuccess {
                    release_id: resolved.effective.release_id,
                    value: outcome.value,
                    metadata: SuccessMetadataV1::Action {
                        schedules_created: outcome.schedules_created,
                    },
                })
            }
        }
    }
}

#[async_trait]
impl InvocationService for ProductInvocationService {
    async fn invoke(
        &self,
        context: InvocationContext,
        call: InvokeCallV1,
    ) -> Result<GatewaySuccess, GatewayFailure> {
        self.invoke_inner(context, call).await
    }
}

#[async_trait]
impl RealtimeQueryService for ProductInvocationService {
    async fn prepare_subscription(
        &self,
        context: RealtimeSubscribeContext,
        target: CodeTarget,
        function_name: runku_core::FunctionName,
        arguments: runku_value::CanonicalValue,
    ) -> Result<RealtimePreparedSubscription, GatewayFailure> {
        let resolved = self.resolve_code(&target).await?;
        let function = resolved
            .manifest
            .functions
            .iter()
            .find(|function| function.name == function_name)
            .cloned()
            .ok_or_else(|| failure(ErrorClassV1::NotFound, "FUNCTION_NOT_FOUND", false))?;
        if function.function_type != FunctionType::Query {
            return Err(failure(
                ErrorClassV1::InvalidRequest,
                "FUNCTION_TYPE_MISMATCH",
                false,
            ));
        }
        let now = self.clock.now().map_err(map_principal_verification)?;
        let authorization_delta = i64::try_from(context.maximum_authorization_duration.as_micros())
            .map_err(|_| {
                failure(
                    ErrorClassV1::Internal,
                    "GATEWAY_CONFIGURATION_INVALID",
                    false,
                )
            })?;
        let maximum_authorized_until = now
            .get()
            .checked_add(authorization_delta)
            .map(TimestampMicros::new)
            .ok_or_else(|| {
                failure(
                    ErrorClassV1::Internal,
                    "GATEWAY_CONFIGURATION_INVALID",
                    false,
                )
            })?;
        let identity = self
            .authorize_function(&context.credentials, &function, now)
            .await?;
        let authorized_until = match &identity.principal {
            PrincipalContext::Authenticated(principal) => principal
                .expires_at()
                .map_or(maximum_authorized_until, |expires| {
                    expires.min(maximum_authorized_until)
                }),
            PrincipalContext::None => maximum_authorized_until,
        };
        if authorized_until <= now {
            return Err(failure(
                ErrorClassV1::Unauthenticated,
                "AUTHORIZATION_EXPIRED",
                false,
            ));
        }
        let request = self
            .invocation_request(
                &resolved,
                &function,
                context.request_id,
                arguments.clone(),
                context.cancellation,
                Arc::clone(&identity),
            )
            .await?;
        let outcome = self.query.execute(request).await.map_err(map_query)?;
        Ok(RealtimePreparedSubscription {
            spec: SubscriptionSpec {
                id: context.subscription_id,
                scope: self.config.scope,
                release_id: resolved.effective.release_id,
                pinned_code: resolved.pinned_code,
                function: function.name,
                arguments,
                identity,
                authorized_until,
            },
            outcome,
        })
    }
}

#[async_trait]
impl SubscriptionRunner for ProductInvocationService {
    async fn rerun(&self, spec: &SubscriptionSpec) -> Result<QueryOutcome, SubscriptionRunFailure> {
        let now = self
            .clock
            .now()
            .map_err(|error| subscription_failure(map_principal_verification(error)))?;
        if spec.authorized_until <= now {
            return Err(SubscriptionRunFailure::authorization_expired());
        }
        let resolved = self
            .resolve_pinned(spec)
            .await
            .map_err(subscription_failure)?;
        let function = resolved
            .manifest
            .functions
            .iter()
            .find(|function| function.name == spec.function)
            .cloned()
            .ok_or_else(|| {
                subscription_failure(failure(ErrorClassV1::NotFound, "FUNCTION_NOT_FOUND", false))
            })?;
        if function.function_type != FunctionType::Query {
            return Err(subscription_failure(failure(
                ErrorClassV1::Internal,
                "FUNCTION_TYPE_MISMATCH",
                false,
            )));
        }
        let request = self
            .invocation_request(
                &resolved,
                &function,
                runku_core::RequestId::generate(),
                spec.arguments.clone(),
                runku_runtime::CancellationToken::new(),
                Arc::clone(&spec.identity),
            )
            .await
            .map_err(subscription_failure)?;
        self.query
            .execute(request)
            .await
            .map_err(map_query)
            .map_err(subscription_failure)
    }
}

#[async_trait]
impl ScheduledInvocationRunner for ProductInvocationService {
    async fn execute(
        &self,
        scope: EnvironmentScope,
        record: &ScheduledInvocationRecord,
    ) -> Result<(), ScheduledRunFailure> {
        if scope != self.config.scope {
            return Err(scheduled_failure(failure(
                ErrorClassV1::Internal,
                "SCHEDULE_SCOPE_MISMATCH",
                false,
            )));
        }
        let resolved = self
            .resolve_pinned_code(record.pinned_code)
            .await
            .map_err(scheduled_failure)?;
        let function = resolved
            .manifest
            .functions
            .iter()
            .find(|function| function.name == record.function)
            .cloned()
            .ok_or_else(|| {
                scheduled_failure(failure(ErrorClassV1::NotFound, "FUNCTION_NOT_FOUND", false))
            })?;
        if !matches!(
            function.function_type,
            FunctionType::Mutation | FunctionType::Action
        ) {
            return Err(scheduled_failure(failure(
                ErrorClassV1::InvalidRequest,
                "SCHEDULE_FUNCTION_TYPE_INVALID",
                false,
            )));
        }
        let now = self
            .clock
            .now()
            .map_err(map_principal_verification)
            .map_err(scheduled_failure)?;
        let identity = self
            .scheduled_identity(&function, now)
            .await
            .map_err(scheduled_failure)?;
        let request = self
            .invocation_request(
                &resolved,
                &function,
                runku_core::RequestId::generate(),
                record.args.clone(),
                runku_runtime::CancellationToken::new(),
                identity,
            )
            .await
            .map_err(scheduled_failure)?;
        match function.function_type {
            FunctionType::Mutation => self
                .mutation
                .execute(request, scheduled_mutation_operation_id(record))
                .await
                .map(|_| ())
                .map_err(map_mutation)
                .map_err(scheduled_failure),
            FunctionType::Action => self
                .action
                .execute(request)
                .await
                .map(|_| ())
                .map_err(map_action)
                .map_err(scheduled_failure),
            FunctionType::Query => Err(ScheduledRunFailure::internal()),
        }
    }
}

fn scheduled_mutation_operation_id(record: &ScheduledInvocationRecord) -> OperationId {
    let mut material = Vec::with_capacity(48);
    material.extend_from_slice(b"RUNKU_SCHEDULED_MUTATION_OPERATION_V1");
    material.extend_from_slice(&record.id.as_ulid().to_bytes());
    let digest = Sha256Digest::of(&material);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    OperationId::from_ulid(ulid::Ulid::from_bytes(bytes))
}

fn subscription_failure(error: GatewayFailure) -> SubscriptionRunFailure {
    match SubscriptionRunFailure::new(error.error.code(), error.error.retryable()) {
        Ok(error) => error,
        Err(_) => SubscriptionRunFailure::internal(),
    }
}

fn scheduled_failure(error: GatewayFailure) -> ScheduledRunFailure {
    ScheduledRunFailure::new(error.error.code(), error.error.retryable())
        .unwrap_or_else(|_| ScheduledRunFailure::internal())
}

fn validate_effective_manifest(
    effective: &EffectiveRelease,
    manifest: &runku_releases::ReleaseManifestV1,
    full_node: Option<&dyn FullNodeActionRuntime>,
) -> Result<(), GatewayFailure> {
    if manifest.project_id != effective.scope.project_id()
        || manifest.release_id != effective.release_id
        || manifest.digest().map_err(map_release)? != effective.manifest_digest
        || manifest.artifact != effective.artifact
        || manifest.runtime_version != effective.runtime_version
    {
        return Err(failure(
            ErrorClassV1::Internal,
            "RELEASE_SNAPSHOT_DRIFT",
            false,
        ));
    }
    if manifest
        .functions
        .iter()
        .any(|function| function.runtime_class == RuntimeClass::FullNode)
    {
        full_node
            .ok_or_else(|| map_release(ReleaseError::Unsupported))?
            .validate_manifest(manifest)
            .map_err(map_runtime)
    } else {
        manifest.ensure_mvp_runtime_supported().map_err(map_release)
    }
}

fn map_provider_verification(error: ProviderError) -> PrincipalVerificationError {
    match error {
        ProviderError::DnsUnavailable
        | ProviderError::TransportUnavailable
        | ProviderError::Unavailable
        | ProviderError::Identity(
            IdentityError::JwksRefreshRequired | IdentityError::JwksSnapshotExpired,
        ) => PrincipalVerificationError::Unavailable,
        ProviderError::Timeout => PrincipalVerificationError::Timeout,
        ProviderError::Identity(IdentityError::InvalidPrincipal) => {
            PrincipalVerificationError::Invalid
        }
        ProviderError::InvalidConfig
        | ProviderError::UrlDenied
        | ProviderError::AddressDenied
        | ProviderError::InvalidResponse
        | ProviderError::LimitExceeded
        | ProviderError::Identity(_) => PrincipalVerificationError::Internal,
    }
}

fn map_principal_verification(error: PrincipalVerificationError) -> GatewayFailure {
    match error {
        PrincipalVerificationError::Invalid => {
            failure(ErrorClassV1::Unauthenticated, "PRINCIPAL_INVALID", false)
        }
        PrincipalVerificationError::Unavailable => failure(
            ErrorClassV1::Unavailable,
            "IDENTITY_PROVIDER_UNAVAILABLE",
            true,
        ),
        PrincipalVerificationError::Timeout => {
            failure(ErrorClassV1::Unavailable, "IDENTITY_PROVIDER_TIMEOUT", true)
        }
        PrincipalVerificationError::Internal => {
            failure(ErrorClassV1::Internal, "IDENTITY_PROVIDER_INTERNAL", false)
        }
    }
}

fn map_release(error: ReleaseError) -> GatewayFailure {
    let class = match error {
        ReleaseError::ReleaseNotFound | ReleaseError::ChannelNotFound | ReleaseError::NotFound => {
            ErrorClassV1::NotFound
        }
        ReleaseError::ReleaseRetired => ErrorClassV1::Gone,
        ReleaseError::LimitExceeded => ErrorClassV1::LimitExceeded,
        ReleaseError::Busy => ErrorClassV1::Busy,
        ReleaseError::Unavailable | ReleaseError::ResultUncertain => ErrorClassV1::Unavailable,
        ReleaseError::RepositoryConflict | ReleaseError::OperationIdReused => {
            ErrorClassV1::Conflict
        }
        ReleaseError::WorkspaceUnsupported
        | ReleaseError::DefaultChannelMissing
        | ReleaseError::ReleaseNotServable => ErrorClassV1::NotFound,
        ReleaseError::InvalidManifest
        | ReleaseError::InvalidArtifact
        | ReleaseError::Unsupported
        | ReleaseError::DigestMismatch
        | ReleaseError::DescriptorMismatch
        | ReleaseError::Corruption
        | ReleaseError::ProductionBackendUnsupported
        | ReleaseError::Internal
        | ReleaseError::InvalidTransition
        | ReleaseError::InvalidSnapshot => ErrorClassV1::Internal,
    };
    failure(class, error.code(), error.retryable())
}

fn map_development(error: DevelopmentError) -> GatewayFailure {
    let class = match error {
        DevelopmentError::PolicyDenied => ErrorClassV1::Forbidden,
        DevelopmentError::WorkspaceNotFound
        | DevelopmentError::WorkspaceEmpty
        | DevelopmentError::RevisionNotFound => ErrorClassV1::NotFound,
        DevelopmentError::Conflict => ErrorClassV1::Conflict,
        DevelopmentError::LimitExceeded => ErrorClassV1::LimitExceeded,
        DevelopmentError::Unavailable | DevelopmentError::ResultUncertain => {
            ErrorClassV1::Unavailable
        }
        DevelopmentError::InvalidInput => ErrorClassV1::InvalidRequest,
        DevelopmentError::InvalidRevision
        | DevelopmentError::InvalidSnapshot
        | DevelopmentError::Corruption
        | DevelopmentError::Unsupported => ErrorClassV1::Internal,
    };
    failure(class, error.code(), error.retryable())
}

fn map_identity(error: IdentityError) -> GatewayFailure {
    let class = match error {
        IdentityError::InvalidCredential
        | IdentityError::ClientNotFound
        | IdentityError::CredentialNotFound
        | IdentityError::ClientInactive
        | IdentityError::CredentialInactive
        | IdentityError::InvalidPrincipal => ErrorClassV1::Unauthenticated,
        IdentityError::ApplicationMismatch
        | IdentityError::InternalFunctionDenied
        | IdentityError::PolicyDenied
        | IdentityError::ScopeEscalation
        | IdentityError::CredentialTypeMismatch => ErrorClassV1::Forbidden,
        IdentityError::Conflict | IdentityError::InvalidTransition => ErrorClassV1::Conflict,
        IdentityError::LimitExceeded => ErrorClassV1::LimitExceeded,
        IdentityError::Unavailable
        | IdentityError::ResultUncertain
        | IdentityError::EntropyUnavailable
        | IdentityError::JwksRefreshRequired
        | IdentityError::JwksSnapshotExpired => ErrorClassV1::Unavailable,
        IdentityError::InvalidInput => ErrorClassV1::InvalidRequest,
        IdentityError::Corruption
        | IdentityError::ProductionBackendUnsupported
        | IdentityError::Unsupported => ErrorClassV1::Internal,
    };
    failure(class, error.code(), error.retryable())
}

fn map_runtime(error: RuntimeError) -> GatewayFailure {
    let class = match error {
        RuntimeError::Busy => ErrorClassV1::Busy,
        RuntimeError::Unavailable => ErrorClassV1::Unavailable,
        RuntimeError::DeadlineExceeded | RuntimeError::Cancelled => ErrorClassV1::Timeout,
        RuntimeError::HeapLimitExceeded => ErrorClassV1::LimitExceeded,
        RuntimeError::FunctionNotFound => ErrorClassV1::NotFound,
        RuntimeError::InvalidArguments => ErrorClassV1::InvalidRequest,
        RuntimeError::InvalidConfiguration
        | RuntimeError::InvalidInvocation
        | RuntimeError::UnsupportedRuntime
        | RuntimeError::InvalidArtifact
        | RuntimeError::JavaScript
        | RuntimeError::InvalidResult
        | RuntimeError::Internal => ErrorClassV1::Internal,
    };
    failure(class, error.code(), error.retryable())
}

fn map_store(error: StoreError) -> GatewayFailure {
    let class = match error {
        StoreError::LimitExceeded => ErrorClassV1::LimitExceeded,
        StoreError::OperationIdReused | StoreError::MutationConflict => ErrorClassV1::Conflict,
        StoreError::NotFound => ErrorClassV1::NotFound,
        StoreError::Busy | StoreError::SerializationFailure => ErrorClassV1::Busy,
        StoreError::ResultUncertain | StoreError::Unavailable => ErrorClassV1::Unavailable,
        StoreError::EmptyBatch
        | StoreError::DuplicateMutation
        | StoreError::InvalidRange
        | StoreError::LeaseLost
        | StoreError::OutboxLeaseLost
        | StoreError::ProductionBackendUnsupported
        | StoreError::Corruption
        | StoreError::MigrationFailed
        | StoreError::Internal => ErrorClassV1::Internal,
    };
    failure(class, error.code(), error.retryable())
}

fn map_data(error: DataReadError) -> GatewayFailure {
    let class = match error {
        DataReadError::InvalidRequest => ErrorClassV1::InvalidRequest,
        DataReadError::Unavailable | DataReadError::Storage => ErrorClassV1::Unavailable,
        DataReadError::Timeout | DataReadError::Cancelled => ErrorClassV1::Timeout,
        DataReadError::LimitExceeded => ErrorClassV1::LimitExceeded,
    };
    failure(
        class,
        error.code(),
        matches!(error, DataReadError::Unavailable | DataReadError::Timeout),
    )
}

fn map_query(error: ExecutionError) -> GatewayFailure {
    match error {
        ExecutionError::Runtime(error) => map_runtime(error),
        ExecutionError::Storage(error) => map_store(error),
        ExecutionError::Data(error) => map_data(error),
    }
}

fn map_mutation(error: MutationExecutionError) -> GatewayFailure {
    match error {
        MutationExecutionError::Runtime(error) => map_runtime(error),
        MutationExecutionError::Storage(error) => map_store(error),
        MutationExecutionError::Data(error) => map_data(error),
        MutationExecutionError::Schema(error) => map_schema(error),
        MutationExecutionError::Schedule(error) => map_schedule(error),
    }
}

fn map_action(error: ActionExecutionError) -> GatewayFailure {
    match error {
        ActionExecutionError::Runtime(error) => map_runtime(error),
        ActionExecutionError::Schedule(error) => map_schedule(error),
    }
}

fn map_schema(error: SchemaError) -> GatewayFailure {
    failure(ErrorClassV1::Internal, error.code(), error.retryable())
}

fn map_schedule(error: ScheduleError) -> GatewayFailure {
    let class = match error {
        ScheduleError::InvalidRequest => ErrorClassV1::InvalidRequest,
        ScheduleError::LimitExceeded => ErrorClassV1::LimitExceeded,
        ScheduleError::Storage | ScheduleError::Unavailable | ScheduleError::ResultUncertain => {
            ErrorClassV1::Unavailable
        }
        ScheduleError::Timeout | ScheduleError::Cancelled => ErrorClassV1::Timeout,
    };
    failure(
        class,
        error.code(),
        matches!(
            error,
            ScheduleError::Storage
                | ScheduleError::Unavailable
                | ScheduleError::Timeout
                | ScheduleError::ResultUncertain
        ),
    )
}

fn failure(class: ErrorClassV1, code: &'static str, retryable: bool) -> GatewayFailure {
    let error = match PublicErrorV1::new(class, code, retryable) {
        Ok(error) => error,
        Err(_) => runku_protocol::ProtocolError::InvalidResponse.public_error(),
    };
    GatewayFailure { error }
}
