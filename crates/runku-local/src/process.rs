//! One-process local Product Base composition and bounded lifecycle.

use std::{
    collections::BTreeSet,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use runku_core::WorkerId;
use runku_cron::{
    CronContext, CronMaterializer, CronMaterializerConfig, CronRepository, CronRepositoryConfig,
    SqlCronRepository,
};
use runku_data::{LogicalStore, OutboxConsumerName};
use runku_data_sqlite::{SqliteStore, SqliteStoreConfig};
use runku_development::{
    DevelopmentContext, DevelopmentRepository, DevelopmentRepositoryConfig,
    SqlDevelopmentRepository,
};
use runku_execution::{
    ActionExecutor, MutationExecutor, QueryExecutor, ScheduledInvocationRunner, ScheduledWorker,
    ScheduledWorkerConfig,
};
use runku_file_storage::{FileObjectStore, FileStorageLimits, FileStorageService, FileUsageSink};
use runku_gateway::{
    CorsOrigin, DevelopmentCatalog, GatewayClock, GatewayHttpConfig, PrincipalVerificationError,
    PrincipalVerifier, ProductInvocationConfig, ProductInvocationService, RealtimeGateway,
    RealtimeGatewayConfig, ServingCatalog, SystemGatewayClock,
    build_router_with_realtime_and_files,
};
use runku_identity::{ApplicationCredentialResolver, KeyringCrypto, PrincipalEvidence};
use runku_identity_provider::JwtProviderManager;
use runku_identity_repository::{IdentityRepositoryConfig, SqlApplicationIdentityRepository};
use runku_node_runtime::{LocalNodeRuntime, LocalNodeRuntimeConfig};
use runku_observability::{
    BufferedLogSink, JournalForwardOutcome, LogArchive, LogArchiveRunOutcome, LogArchiver,
    LogJournalForwarder, LogRepository, LogRepositoryConfig, LogSpoolConfig, NatsLogJournal,
    OperationalLogSink, SqlLogRepository,
};
use runku_realtime::{
    ChangeDispatcher, DispatcherConfig, RegistryConfig, SubscriptionRegistry, SubscriptionRunner,
};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{
    ArtifactStore, FilesystemArtifactStore, FilesystemStoreRole, ReleaseRepository,
};
use runku_runtime::{RuntimeLimits, RuntimeSupervisor};
use runku_value::TimestampMicros;
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

use crate::{
    LocalProjectState, LocalStateError, load_local, load_local_auth_config,
    publish::reconcile_cron_head,
    state::{LocalLock, acquire_process_lock, load_file_storage_pepper, load_identity_pepper},
};

/// Validated bounded local daemon policy.
#[derive(Clone, Debug)]
pub struct LocalProcessConfig {
    /// Exact browser origins allowed to use HTTP/Realtime; non-browser requests need no Origin.
    pub allowed_origins: BTreeSet<CorsOrigin>,
    /// Optional strict local JWT provider descriptor relative to the project root.
    pub auth_config: Option<PathBuf>,
    /// Poll cadence for Realtime outbox, Scheduled Invocation, and Cron workers.
    pub worker_interval: Duration,
    /// Refresh cadence for immutable Release and Development serving snapshots.
    pub catalog_refresh_interval: Duration,
    /// Cadence for committing bounded hot-log batches to immutable local Parquet.
    pub log_archive_interval: Duration,
    /// Optional external archive; absent selects `.runku/observability-archive`.
    pub log_archive: Option<LogArchive>,
    /// Optional replicated journal; absent archives directly in the same process.
    pub log_journal: Option<NatsLogJournal>,
    /// Optional externally configured object backend; absent uses the local dedicated directory.
    pub file_object_store: Option<FileObjectStore>,
    /// Environment, per-file, Action-memory, concurrency, and grant limits.
    pub file_storage_limits: FileStorageLimits,
    /// Optional at-least-once sink for authoritative application-file usage events.
    pub file_usage_sink: Option<Arc<dyn FileUsageSink>>,
    /// Cadence for bounded usage outbox delivery.
    pub file_usage_interval: Duration,
    /// Maximum time graceful shutdown waits before aborting a stuck task.
    pub shutdown_grace: Duration,
}

impl Default for LocalProcessConfig {
    fn default() -> Self {
        Self {
            allowed_origins: BTreeSet::new(),
            auth_config: None,
            worker_interval: Duration::from_millis(100),
            catalog_refresh_interval: Duration::from_millis(250),
            log_archive_interval: Duration::from_secs(60),
            log_archive: None,
            log_journal: None,
            file_object_store: None,
            file_storage_limits: FileStorageLimits::DEFAULT,
            file_usage_sink: None,
            file_usage_interval: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

impl LocalProcessConfig {
    fn validate(&self) -> Result<(), LocalProcessError> {
        if !(Duration::from_millis(10)..=Duration::from_secs(30)).contains(&self.worker_interval)
            || !(Duration::from_millis(25)..=Duration::from_secs(30))
                .contains(&self.catalog_refresh_interval)
            || !(Duration::from_millis(100)..=Duration::from_secs(3600))
                .contains(&self.log_archive_interval)
            || !(Duration::from_millis(100)..=Duration::from_secs(30))
                .contains(&self.shutdown_grace)
            || !(Duration::from_millis(100)..=Duration::from_secs(300))
                .contains(&self.file_usage_interval)
        {
            return Err(LocalProcessError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Stable local composition/lifecycle failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalProcessError {
    /// A duration, limit, or listener address violates the local process contract.
    #[error("local process configuration is invalid")]
    InvalidConfiguration,
    /// Persistent local state is absent, invalid, or corrupt.
    #[error("local process state is invalid")]
    InvalidState,
    /// A repository, runtime, or router could not be composed coherently.
    #[error("local product base composition failed")]
    Composition,
    /// The explicit listener could not be bound or served.
    #[error("local listener is unavailable")]
    ListenerUnavailable,
    /// Another local daemon already owns this project's process lock.
    #[error("local process is already running")]
    AlreadyRunning,
}

impl LocalProcessError {
    /// Stable machine-readable category.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "LOCAL_PROCESS_CONFIGURATION_INVALID",
            Self::InvalidState => "LOCAL_PROCESS_STATE_INVALID",
            Self::Composition => "LOCAL_PROCESS_COMPOSITION_FAILED",
            Self::ListenerUnavailable => "LOCAL_PROCESS_LISTENER_UNAVAILABLE",
            Self::AlreadyRunning => "LOCAL_PROCESS_ALREADY_RUNNING",
        }
    }

    /// Whether external recovery followed by a retry may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Composition | Self::ListenerUnavailable)
    }
}

/// Aggregate process-loop counters without user-controlled labels or credentials.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalProcessTelemetrySnapshot {
    /// Successful Realtime dispatcher polls.
    pub realtime_polls: u64,
    /// Successful Scheduled worker polls.
    pub scheduled_polls: u64,
    /// Successful Cron materializer polls.
    pub cron_polls: u64,
    /// Successful serving/development catalog refresh cycles.
    pub catalog_refreshes: u64,
    /// Successful Operational Log archive polls, including idle polls.
    pub log_archive_polls: u64,
    /// Operational Log records committed to immutable Parquet by this process.
    pub log_archive_records: u64,
    /// Successful replicated-journal forward polls, including idle polls.
    pub log_journal_polls: u64,
    /// Operational Log records confirmed by replicated-journal `PubAck`.
    pub log_journal_records: u64,
    /// Successful authoritative file-usage outbox deliveries, including idle polls.
    pub file_usage_polls: u64,
    /// Authoritative file-usage facts durably accepted by the configured sink.
    pub file_usage_events: u64,
    /// Sanitized background failures across all loops.
    pub background_failures: u64,
    /// Tasks forcibly aborted after the shutdown grace period.
    pub forced_shutdowns: u64,
}

#[derive(Debug, Default)]
struct LocalProcessTelemetry {
    realtime_polls: AtomicU64,
    scheduled_polls: AtomicU64,
    cron_polls: AtomicU64,
    catalog_refreshes: AtomicU64,
    log_archive_polls: AtomicU64,
    log_archive_records: AtomicU64,
    log_journal_polls: AtomicU64,
    log_journal_records: AtomicU64,
    file_usage_polls: AtomicU64,
    file_usage_events: AtomicU64,
    background_failures: AtomicU64,
    forced_shutdowns: AtomicU64,
}

impl LocalProcessTelemetry {
    fn snapshot(&self) -> LocalProcessTelemetrySnapshot {
        LocalProcessTelemetrySnapshot {
            realtime_polls: self.realtime_polls.load(Ordering::Relaxed),
            scheduled_polls: self.scheduled_polls.load(Ordering::Relaxed),
            cron_polls: self.cron_polls.load(Ordering::Relaxed),
            catalog_refreshes: self.catalog_refreshes.load(Ordering::Relaxed),
            log_archive_polls: self.log_archive_polls.load(Ordering::Relaxed),
            log_archive_records: self.log_archive_records.load(Ordering::Relaxed),
            log_journal_polls: self.log_journal_polls.load(Ordering::Relaxed),
            log_journal_records: self.log_journal_records.load(Ordering::Relaxed),
            file_usage_polls: self.file_usage_polls.load(Ordering::Relaxed),
            file_usage_events: self.file_usage_events.load(Ordering::Relaxed),
            background_failures: self.background_failures.load(Ordering::Relaxed),
            forced_shutdowns: self.forced_shutdowns.load(Ordering::Relaxed),
        }
    }
}

/// Running local Product Base listener and all cancellable background components.
pub struct LocalProcess {
    state: LocalProjectState,
    address: SocketAddr,
    service: Arc<ProductInvocationService>,
    registry: SubscriptionRegistry,
    runtime: RuntimeSupervisor,
    ready: Arc<AtomicBool>,
    telemetry: Arc<LocalProcessTelemetry>,
    shutdown: watch::Sender<bool>,
    log_shutdown: watch::Sender<bool>,
    log_maintenance_shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    log_task: Option<JoinHandle<()>>,
    log_maintenance_tasks: Vec<JoinHandle<()>>,
    log_repository: Arc<dyn LogRepository>,
    shutdown_grace: Duration,
    _process_lock: LocalLock,
}

/// Exclusive local daemon lease acquired before source sync mutates Workspace HEAD.
pub struct LocalProcessLease {
    root: PathBuf,
    lock: LocalLock,
}

impl fmt::Debug for LocalProcessLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalProcessLease")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// Acquires the same exclusive lease held for the lifetime of [`LocalProcess`].
///
/// Source-driven development uses this before its initial build/publication so a losing daemon
/// attempt cannot move Workspace HEAD and only later discover that another process is active.
///
/// # Errors
///
/// Returns `LOCAL_PROCESS_ALREADY_RUNNING` when another lease/process owns the project.
pub async fn acquire_local_process_lease(
    root: &Path,
) -> Result<LocalProcessLease, LocalProcessError> {
    let (_, paths) = load_local(root).await.map_err(map_state)?;
    let lock = acquire_process_lock(&paths)
        .await
        .map_err(|error| match error {
            LocalStateError::Conflict => LocalProcessError::AlreadyRunning,
            error => map_state(error),
        })?;
    Ok(LocalProcessLease {
        root: paths.root,
        lock,
    })
}

impl fmt::Debug for LocalProcess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalProcess")
            .field("scope", &self.state.scope())
            .field("address", &self.address)
            .field("ready", &self.is_ready())
            .field("telemetry", &self.telemetry())
            .finish_non_exhaustive()
    }
}

impl LocalProcess {
    /// Opens every local Product Base dependency, binds the explicit loopback listener, and starts
    /// bounded refresh/Realtime/Scheduled/Cron loops.
    ///
    /// # Errors
    ///
    /// Fails before returning on invalid state/config, missing publication, repository drift,
    /// runtime setup, router setup, or listener bind failures.
    #[allow(clippy::too_many_lines)]
    pub async fn start(root: &Path, config: LocalProcessConfig) -> Result<Self, LocalProcessError> {
        let lease = acquire_local_process_lease(root).await?;
        Self::start_with_lease(root, config, lease).await
    }

    /// Starts the complete process while consuming a lease acquired before preparation.
    ///
    /// # Errors
    ///
    /// Rejects a lease acquired for a different canonical project root and every error documented
    /// by [`Self::start`].
    #[allow(clippy::too_many_lines)]
    pub async fn start_with_lease(
        root: &Path,
        config: LocalProcessConfig,
        lease: LocalProcessLease,
    ) -> Result<Self, LocalProcessError> {
        config.validate()?;
        let (state, paths) = load_local(root).await.map_err(map_state)?;
        if lease.root != paths.root {
            return Err(LocalProcessError::InvalidState);
        }
        let process_lock = lease.lock;
        let principals: Arc<dyn PrincipalVerifier> = match config.auth_config.as_ref() {
            Some(relative) => Arc::new(
                JwtProviderManager::local(
                    load_local_auth_config(&paths.root, relative)
                        .map_err(|_| LocalProcessError::InvalidConfiguration)?,
                )
                .map_err(|_| LocalProcessError::InvalidConfiguration)?,
            ),
            None => Arc::new(RejectingPrincipalVerifier),
        };
        if !state.listen_address.ip().is_loopback() {
            return Err(LocalProcessError::InvalidConfiguration);
        }
        let listener = TcpListener::bind(state.listen_address)
            .await
            .map_err(|_| LocalProcessError::ListenerUnavailable)?;
        let address = listener
            .local_addr()
            .map_err(|_| LocalProcessError::ListenerUnavailable)?;
        if !address.ip().is_loopback() {
            return Err(LocalProcessError::InvalidConfiguration);
        }

        let releases = Arc::new(
            SqlReleaseRepository::connect_sqlite(
                &sqlite_url(&paths.release_database),
                RepositoryConfig::LOCAL,
            )
            .await
            .map_err(|_| LocalProcessError::Composition)?,
        );
        let identity = Arc::new(
            SqlApplicationIdentityRepository::connect_sqlite(
                &sqlite_url(&paths.identity_database),
                IdentityRepositoryConfig::LOCAL,
            )
            .await
            .map_err(|_| LocalProcessError::Composition)?,
        );
        let development_context = DevelopmentContext {
            scope: state.scope(),
            environment: state.environment(),
        };
        let development = Arc::new(
            SqlDevelopmentRepository::connect_sqlite(
                &sqlite_url(&paths.development_database),
                DevelopmentRepositoryConfig::LOCAL,
                development_context,
            )
            .await
            .map_err(|_| LocalProcessError::Composition)?,
        );
        let cron_context = CronContext {
            scope: state.scope(),
            environment: state.environment(),
        };
        let cron = Arc::new(
            SqlCronRepository::connect_sqlite(
                &sqlite_url(&paths.cron_database),
                CronRepositoryConfig::LOCAL,
                cron_context,
            )
            .await
            .map_err(|_| LocalProcessError::Composition)?,
        );
        let artifacts = Arc::new(
            FilesystemArtifactStore::open(&paths.artifacts, FilesystemStoreRole::LocalDevelopment)
                .await
                .map_err(|_| LocalProcessError::Composition)?,
        );
        let store = Arc::new(
            SqliteStore::open(&paths.data_database, SqliteStoreConfig::LOCAL)
                .await
                .map_err(|_| LocalProcessError::Composition)?,
        );
        let file_objects = match config.file_object_store.clone() {
            Some(objects) => objects,
            None => FileObjectStore::filesystem(&paths.file_storage_objects)
                .await
                .map_err(|_| LocalProcessError::Composition)?,
        };
        let files = Arc::new(
            FileStorageService::open_sqlite(
                state.scope(),
                &paths.file_storage_database,
                file_objects,
                load_file_storage_pepper(&paths).await.map_err(map_state)?,
                config.file_storage_limits,
            )
            .await
            .map_err(|_| LocalProcessError::Composition)?,
        );
        let log_repository: Arc<dyn LogRepository> = Arc::new(
            SqlLogRepository::connect_sqlite(
                &sqlite_url(&paths.observability_database),
                LogRepositoryConfig::LOCAL,
            )
            .await
            .map_err(|_| LocalProcessError::Composition)?,
        );
        let log_archive = match config.log_archive.clone() {
            Some(archive) => archive,
            None => LogArchive::open_filesystem(paths.observability_archive.clone())
                .await
                .map_err(|_| LocalProcessError::Composition)?,
        };
        let log_archive_frontier = if config.log_journal.is_some() {
            log_archive
                .status(state.scope())
                .await
                .map_err(|_| LocalProcessError::Composition)?
                .through
        } else {
            runku_observability::LogCursor::START
        };
        let log_archiver = config
            .log_journal
            .is_none()
            .then(|| {
                LogArchiver::new(
                    Arc::clone(&log_repository),
                    log_archive.clone(),
                    state.scope(),
                    1_000,
                )
            })
            .transpose()
            .map_err(|_| LocalProcessError::Composition)?;
        let log_forwarder = config
            .log_journal
            .clone()
            .map(|journal| {
                LogJournalForwarder::resume_after(
                    Arc::clone(&log_repository),
                    journal,
                    state.scope(),
                    256,
                    log_archive_frontier,
                )
            })
            .transpose()
            .map_err(|_| LocalProcessError::Composition)?;
        let (log_sink, log_writer) =
            BufferedLogSink::new(LogSpoolConfig::LOCAL, Arc::clone(&log_repository))
                .map_err(|_| LocalProcessError::Composition)?;
        let log_boundary: Arc<dyn OperationalLogSink> = Arc::new(log_sink);
        let release_boundary: Arc<dyn ReleaseRepository> = releases;
        let serving = Arc::new(
            ServingCatalog::load(state.scope(), Arc::clone(&release_boundary))
                .await
                .map_err(|_| LocalProcessError::Composition)?,
        );
        let development_boundary: Arc<dyn DevelopmentRepository> = development;
        let development_catalog = Arc::new(
            DevelopmentCatalog::load(development_context, development_boundary)
                .await
                .map_err(|_| LocalProcessError::Composition)?,
        );
        development_catalog
            .resolve(&state.workspace_ref)
            .map_err(|_| LocalProcessError::Composition)?;
        reconcile_cron_head(&state, &paths)
            .await
            .map_err(|_| LocalProcessError::Composition)?;

        let logical_store: Arc<dyn LogicalStore> = store;
        let runtime = RuntimeSupervisor::start(
            RuntimeLimits::builder(2, 128)
                .build()
                .map_err(|_| LocalProcessError::InvalidConfiguration)?,
        )
        .map_err(|_| LocalProcessError::Composition)?;
        let query = QueryExecutor::new(runtime.clone(), Arc::clone(&logical_store));
        let mutation = MutationExecutor::new(runtime.clone(), Arc::clone(&logical_store));
        let action = ActionExecutor::new(runtime.clone(), Arc::clone(&logical_store));
        let artifact_boundary: Arc<dyn ArtifactStore> = artifacts;
        let identity_boundary: Arc<dyn ApplicationCredentialResolver> = identity;
        let crypto = Arc::new(KeyringCrypto::new(
            load_identity_pepper(&paths).await.map_err(map_state)?,
        ));
        let clock: Arc<dyn GatewayClock> = Arc::new(SystemGatewayClock);
        let local_node = Arc::new(
            LocalNodeRuntime::new(
                LocalNodeRuntimeConfig::new(&paths.root, 16)
                    .map_err(|_| LocalProcessError::InvalidConfiguration)?,
            )
            .map_err(|_| LocalProcessError::InvalidConfiguration)?,
        );
        let service = Arc::new(
            ProductInvocationService::new(
                ProductInvocationConfig {
                    scope: state.scope(),
                    execution_timeout: Duration::from_secs(30),
                    max_cached_artifact_bytes: 256 * 1024 * 1024,
                },
                Arc::clone(&serving),
                Arc::clone(&release_boundary),
                artifact_boundary,
                identity_boundary,
                crypto,
                principals,
                clock,
                query,
                mutation,
                action,
                None,
            )
            .map_err(|_| LocalProcessError::Composition)?
            .with_full_node_runtime(local_node)
            .with_file_storage(files.clone())
            .with_development_catalog(Arc::clone(&development_catalog))
            .map_err(|_| LocalProcessError::Composition)?
            .with_operational_logs(log_boundary),
        );
        let registry = SubscriptionRegistry::new(RegistryConfig::PRODUCTION)
            .map_err(|_| LocalProcessError::Composition)?;
        let realtime = RealtimeGateway::new(
            RealtimeGatewayConfig::PRODUCTION,
            service.clone(),
            registry.clone(),
        )
        .map_err(|_| LocalProcessError::Composition)?;
        let gateway = build_router_with_realtime_and_files(
            GatewayHttpConfig {
                allowed_origins: config.allowed_origins,
                max_concurrent_requests: 1_024,
                request_timeout: Duration::from_secs(35),
            },
            service.clone(),
            realtime,
            files.clone(),
        )
        .map_err(|_| LocalProcessError::Composition)?;

        let ready = Arc::new(AtomicBool::new(true));
        let router = health_routes(gateway, &ready);
        let telemetry = Arc::new(LocalProcessTelemetry::default());
        let (shutdown, _) = watch::channel(false);
        let (log_shutdown, log_shutdown_receiver) = watch::channel(false);
        let (log_maintenance_shutdown, _) = watch::channel(false);
        let log_task = tokio::spawn(async move {
            let _ = log_writer
                .run_preserving_repository(log_shutdown_receiver)
                .await;
        });
        let mut tasks = Vec::with_capacity(6);
        let mut log_maintenance_tasks = Vec::with_capacity(1);
        tasks.push(spawn_server(
            listener,
            router,
            shutdown.subscribe(),
            Arc::clone(&ready),
        ));

        let subscription_runner: Arc<dyn SubscriptionRunner> = service.clone();
        let dispatcher = ChangeDispatcher::new(
            Arc::clone(&logical_store),
            registry.clone(),
            subscription_runner,
            "local-realtime-v1"
                .parse::<OutboxConsumerName>()
                .map_err(|_| LocalProcessError::InvalidConfiguration)?,
            WorkerId::generate(),
            DispatcherConfig::PRODUCTION,
        )
        .map_err(|_| LocalProcessError::Composition)?;
        tasks.push(spawn_realtime_loop(
            dispatcher,
            state.scope(),
            config.worker_interval,
            shutdown.subscribe(),
            Arc::clone(&telemetry),
        ));

        let scheduled_runner: Arc<dyn ScheduledInvocationRunner> = service.clone();
        let scheduled = ScheduledWorker::new(
            Arc::clone(&logical_store),
            scheduled_runner,
            WorkerId::generate(),
            ScheduledWorkerConfig {
                batch_limit: 8,
                lease_micros: 361_000_000,
                invocation_timeout: Duration::from_secs(45),
                max_attempts: 10,
                retry_base_micros: 1_000_000,
                retry_max_micros: 300_000_000,
            },
        )
        .map_err(|_| LocalProcessError::Composition)?;
        tasks.push(spawn_scheduled_loop(
            scheduled,
            state.scope(),
            config.worker_interval,
            shutdown.subscribe(),
            Arc::clone(&telemetry),
        ));

        let cron_boundary: Arc<dyn CronRepository> = cron;
        let materializer = CronMaterializer::new(
            cron_boundary,
            logical_store,
            cron_context,
            WorkerId::generate(),
            CronMaterializerConfig::DEFAULT,
        )
        .map_err(|_| LocalProcessError::Composition)?;
        tasks.push(spawn_cron_loop(
            materializer,
            config.worker_interval,
            shutdown.subscribe(),
            Arc::clone(&telemetry),
        ));
        tasks.push(spawn_catalog_loop(
            serving,
            development_catalog,
            config.catalog_refresh_interval,
            shutdown.subscribe(),
            Arc::clone(&telemetry),
        ));
        if let Some(log_archiver) = log_archiver {
            log_maintenance_tasks.push(spawn_log_archive_loop(
                log_archiver,
                config.log_archive_interval,
                log_maintenance_shutdown.subscribe(),
                Arc::clone(&telemetry),
            ));
        }
        if let Some(log_forwarder) = log_forwarder {
            log_maintenance_tasks.push(spawn_log_forward_loop(
                log_forwarder,
                config.log_archive_interval,
                log_maintenance_shutdown.subscribe(),
                Arc::clone(&telemetry),
            ));
        }
        if let Some(sink) = config.file_usage_sink.clone() {
            tasks.push(spawn_file_usage_loop(
                files,
                sink,
                config.file_usage_interval,
                shutdown.subscribe(),
                Arc::clone(&telemetry),
            ));
        }

        Ok(Self {
            state,
            address,
            service,
            registry,
            runtime,
            ready,
            telemetry,
            shutdown,
            log_shutdown,
            log_maintenance_shutdown,
            tasks,
            log_task: Some(log_task),
            log_maintenance_tasks,
            log_repository,
            shutdown_grace: config.shutdown_grace,
            _process_lock: process_lock,
        })
    }

    /// Actual bound address; differs from configured address only when port zero was explicit.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Stable local Project/Environment/Workspace identity.
    #[must_use]
    pub const fn state(&self) -> &LocalProjectState {
        &self.state
    }

    /// True while initial composition succeeded and the listener has not begun shutdown.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Shared semantic service, exposed for local diagnostics and conformance tests.
    #[must_use]
    pub fn service(&self) -> &Arc<ProductInvocationService> {
        &self.service
    }

    /// Process-local Realtime registry, exposed as bounded operational telemetry/state.
    #[must_use]
    pub const fn registry(&self) -> &SubscriptionRegistry {
        &self.registry
    }

    /// Runtime worker-pool telemetry.
    #[must_use]
    pub fn runtime(&self) -> &RuntimeSupervisor {
        &self.runtime
    }

    /// Aggregate background-loop counters.
    #[must_use]
    pub fn telemetry(&self) -> LocalProcessTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    /// Stops admission, cancels loops, and awaits every task within the configured grace.
    pub async fn shutdown(mut self) {
        self.ready.store(false, Ordering::Release);
        self.shutdown.send_replace(true);
        let deadline = Instant::now() + self.shutdown_grace;
        for mut task in self.tasks.drain(..) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut task).await.is_err() {
                task.abort();
                drop(task.await);
                self.telemetry
                    .forced_shutdowns
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        self.log_shutdown.send_replace(true);
        if let Some(mut task) = self.log_task.take() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut task).await.is_err() {
                task.abort();
                drop(task.await);
                self.telemetry
                    .forced_shutdowns
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        self.log_maintenance_shutdown.send_replace(true);
        for mut task in self.log_maintenance_tasks.drain(..) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut task).await.is_err() {
                task.abort();
                drop(task.await);
                self.telemetry
                    .forced_shutdowns
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        self.log_repository.close().await;
    }
}

impl Drop for LocalProcess {
    fn drop(&mut self) {
        self.ready.store(false, Ordering::Release);
        self.shutdown.send_replace(true);
        self.log_shutdown.send_replace(true);
        self.log_maintenance_shutdown.send_replace(true);
        for task in &self.tasks {
            task.abort();
        }
        if let Some(task) = &self.log_task {
            task.abort();
        }
        for task in &self.log_maintenance_tasks {
            task.abort();
        }
    }
}

#[derive(Debug)]
struct RejectingPrincipalVerifier;

#[async_trait]
impl PrincipalVerifier for RejectingPrincipalVerifier {
    async fn verify(
        &self,
        _scope: runku_core::EnvironmentScope,
        _token: &str,
        _crypto: &KeyringCrypto,
        _now: TimestampMicros,
    ) -> Result<PrincipalEvidence, PrincipalVerificationError> {
        Err(PrincipalVerificationError::Invalid)
    }
}

fn health_routes(router: Router, ready: &Arc<AtomicBool>) -> Router {
    let readiness = Arc::clone(ready);
    router
        .route(
            "/healthz",
            get(|| async { (StatusCode::OK, "{\"status\":\"ok\"}") }),
        )
        .route(
            "/readyz",
            get(move || {
                let ready = Arc::clone(&readiness);
                async move {
                    if ready.load(Ordering::Acquire) {
                        (StatusCode::OK, "{\"status\":\"ready\"}").into_response()
                    } else {
                        (StatusCode::SERVICE_UNAVAILABLE, "{\"status\":\"stopping\"}")
                            .into_response()
                    }
                }
            }),
        )
}

fn spawn_server(
    listener: TcpListener,
    router: Router,
    mut shutdown: watch::Receiver<bool>,
    ready: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let graceful =
            async move { while !*shutdown.borrow() && shutdown.changed().await.is_ok() {} };
        if axum::serve(listener, router)
            .with_graceful_shutdown(graceful)
            .await
            .is_err()
        {
            ready.store(false, Ordering::Release);
        }
    })
}

fn spawn_realtime_loop(
    dispatcher: ChangeDispatcher,
    scope: runku_core::EnvironmentScope,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
    telemetry: Arc<LocalProcessTelemetry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_loop(interval, shutdown, || async {
            match system_now() {
                Ok(now) => match dispatcher.poll_once(scope, now).await {
                    Ok(_) => telemetry.realtime_polls.fetch_add(1, Ordering::Relaxed),
                    Err(_) => telemetry
                        .background_failures
                        .fetch_add(1, Ordering::Relaxed),
                },
                Err(()) => telemetry
                    .background_failures
                    .fetch_add(1, Ordering::Relaxed),
            };
        })
        .await;
    })
}

fn spawn_scheduled_loop(
    worker: ScheduledWorker,
    scope: runku_core::EnvironmentScope,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
    telemetry: Arc<LocalProcessTelemetry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_loop(interval, shutdown, || async {
            if worker.poll_once(scope).await.is_ok() {
                telemetry.scheduled_polls.fetch_add(1, Ordering::Relaxed);
            } else {
                telemetry
                    .background_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        })
        .await;
    })
}

fn spawn_cron_loop(
    materializer: CronMaterializer,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
    telemetry: Arc<LocalProcessTelemetry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_loop(interval, shutdown, || async {
            if materializer.poll().await.is_ok() {
                telemetry.cron_polls.fetch_add(1, Ordering::Relaxed);
            } else {
                telemetry
                    .background_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        })
        .await;
    })
}

fn spawn_catalog_loop(
    serving: Arc<ServingCatalog>,
    development: Arc<DevelopmentCatalog>,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
    telemetry: Arc<LocalProcessTelemetry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_loop(interval, shutdown, || async {
            if serving.refresh().await.is_ok() && development.refresh().await.is_ok() {
                telemetry.catalog_refreshes.fetch_add(1, Ordering::Relaxed);
            } else {
                telemetry
                    .background_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        })
        .await;
    })
}

fn spawn_log_archive_loop(
    archiver: LogArchiver,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
    telemetry: Arc<LocalProcessTelemetry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let archive_once = || async {
            match archiver.run_once().await {
                Ok(LogArchiveRunOutcome::Archived { records, .. }) => {
                    telemetry
                        .log_archive_records
                        .fetch_add(u64::from(records), Ordering::Relaxed);
                    telemetry.log_archive_polls.fetch_add(1, Ordering::Relaxed);
                    true
                }
                Ok(LogArchiveRunOutcome::Idle { .. }) => {
                    telemetry.log_archive_polls.fetch_add(1, Ordering::Relaxed);
                    false
                }
                Err(_) => {
                    telemetry
                        .background_failures
                        .fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
        };
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => { archive_once().await; }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        while archive_once().await {}
                        return;
                    }
                }
            }
        }
    })
}

fn spawn_log_forward_loop(
    mut forwarder: LogJournalForwarder,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
    telemetry: Arc<LocalProcessTelemetry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => match forwarder.run_once().await {
                    Ok(JournalForwardOutcome::Forwarded { records, .. }) => {
                        telemetry.log_journal_records.fetch_add(u64::from(records), Ordering::Relaxed);
                        telemetry.log_journal_polls.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(JournalForwardOutcome::Idle { .. }) => {
                        telemetry.log_journal_polls.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        telemetry.background_failures.fetch_add(1, Ordering::Relaxed);
                    }
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        loop {
                            match forwarder.run_once().await {
                                Ok(JournalForwardOutcome::Forwarded { records, .. }) => {
                                    telemetry.log_journal_records.fetch_add(u64::from(records), Ordering::Relaxed);
                                    telemetry.log_journal_polls.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(JournalForwardOutcome::Idle { .. }) => {
                                    telemetry.log_journal_polls.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Err(_) => {
                                    telemetry.background_failures.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                            }
                        }
                        return;
                    }
                }
            }
        }
    })
}

fn spawn_file_usage_loop(
    files: Arc<FileStorageService>,
    sink: Arc<dyn FileUsageSink>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
    telemetry: Arc<LocalProcessTelemetry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let deliver_once = || async {
            let events = files.pending_usage_events(100).await?;
            if events.is_empty() {
                return Ok::<usize, runku_runtime::FileStorageError>(0);
            }
            sink.deliver(&events).await?;
            let through = events
                .last()
                .map(|event| event.sequence)
                .ok_or(runku_runtime::FileStorageError::Corruption)?;
            files.acknowledge_usage_events(through).await?;
            Ok(events.len())
        };
        let record = |result: Result<usize, runku_runtime::FileStorageError>| {
            if let Ok(events) = result {
                telemetry.file_usage_polls.fetch_add(1, Ordering::Relaxed);
                telemetry
                    .file_usage_events
                    .fetch_add(u64::try_from(events).unwrap_or(u64::MAX), Ordering::Relaxed);
                events > 0
            } else {
                telemetry
                    .background_failures
                    .fetch_add(1, Ordering::Relaxed);
                false
            }
        };
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => { record(deliver_once().await); }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        while record(deliver_once().await) {}
                        return;
                    }
                }
            }
        }
    })
}

async fn run_loop<F, Fut>(interval: Duration, mut shutdown: watch::Receiver<bool>, mut poll: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => poll().await,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

fn system_now() -> Result<TimestampMicros, ()> {
    SystemGatewayClock.now().map_err(|_| ())
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display())
}

fn map_state(error: LocalStateError) -> LocalProcessError {
    match error {
        LocalStateError::Unavailable => LocalProcessError::Composition,
        LocalStateError::InvalidPath
        | LocalStateError::InvalidState
        | LocalStateError::Conflict
        | LocalStateError::Corruption => LocalProcessError::InvalidState,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, net::SocketAddr, path::Path, str::FromStr, time::Duration};

    use runku_build::{BuildMetadata, build_project};
    use runku_core::{
        ApplicationClientId, BuildId, CodeTarget, CredentialId, FunctionId, ProjectId, ReleaseId,
        WorkspaceRef,
    };
    use runku_development::DevelopmentActor;
    use runku_identity::{ApplicationScope, ClientKind};
    use runku_protocol::{
        ActionCallV1, QueryCallV1, decode_error_v1, decode_success_v1, encode_action_call_v1,
        encode_query_call_v1,
    };
    use runku_releases::{
        AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility,
        ReleaseManifestV1, RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_release_manifest,
        encode_safe_esm_bundle,
    };
    use runku_value::{CanonicalValue, TimestampMicros};
    use tempfile::tempdir;

    use super::{LocalProcess, LocalProcessConfig, LocalProcessError, acquire_local_process_lease};
    use crate::{LocalIdentityManager, initialize_local, publish_local};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    struct Package {
        manifest: Vec<u8>,
        artifact: Vec<u8>,
        release_id: ReleaseId,
    }

    fn package(project_id: ProjectId) -> Result<Package, Box<dyn std::error::Error>> {
        let source = "export default async (_ctx, value) => value;";
        let bundle = SafeEsmBundleV1::from_sources([source])?;
        let artifact = encode_safe_esm_bundle(&bundle)?;
        let contract = Sha256Digest::of(b"local-process-test-contract");
        let release_id = ReleaseId::generate();
        let manifest = ReleaseManifestV1 {
            release_id,
            project_id,
            build_id: BuildId::generate(),
            created_at: TimestampMicros::new(1_800_000_000_000_000),
            runtime_version: "platform-js-1".parse()?,
            artifact: bundle.descriptor()?,
            function_contract_hash: contract,
            schema_contract_hash: contract,
            index_contract_hash: contract,
            functions: vec![FunctionManifest {
                id: FunctionId::generate(),
                name: "queries.echo".parse()?,
                function_type: FunctionType::Query,
                visibility: FunctionVisibility::Public,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(source.as_bytes()),
                arguments_contract_hash: contract,
                result_contract_hash: contract,
                capabilities: vec![Capability::DbRead],
            }],
            cron_definitions: vec![],
        };
        Ok(Package {
            manifest: encode_release_manifest(&manifest)?,
            artifact,
            release_id,
        })
    }

    fn test_config() -> LocalProcessConfig {
        LocalProcessConfig {
            worker_interval: Duration::from_millis(20),
            catalog_refresh_interval: Duration::from_millis(25),
            shutdown_grace: Duration::from_secs(2),
            ..LocalProcessConfig::default()
        }
    }

    #[tokio::test]
    async fn preparation_lease_is_exclusive_and_recoverable_before_publication() -> TestResult {
        let directory = tempdir()?;
        initialize_local(
            directory.path(),
            WorkspaceRef::from_str("default")?,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?;
        let lease = acquire_local_process_lease(directory.path()).await?;
        assert!(matches!(
            acquire_local_process_lease(directory.path()).await,
            Err(LocalProcessError::AlreadyRunning)
        ));
        drop(lease);
        let recovered = acquire_local_process_lease(directory.path()).await?;
        drop(recovered);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn process_serves_health_and_v8_then_restarts_cleanly() -> TestResult {
        let directory = tempdir()?;
        let workspace = WorkspaceRef::from_str("default")?;
        let (state, _) = initialize_local(
            directory.path(),
            workspace.clone(),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?;
        let package = package(state.project_id)?;
        publish_local(
            directory.path(),
            &workspace,
            &DevelopmentActor::from_str("local-test")?,
            &package.manifest,
            &package.artifact,
        )
        .await?;
        let identities = LocalIdentityManager::open(directory.path()).await?;
        let application_scopes = BTreeSet::from(["functions:invoke".parse::<ApplicationScope>()?]);
        let application_client_id = ApplicationClientId::generate();
        identities
            .create_client(
                application_client_id,
                "process-test-browser".parse()?,
                ClientKind::Public,
                application_scopes.clone(),
                TimestampMicros::new(1_800_000_000_000_001),
            )
            .await?;
        let application_key = identities
            .create_credential(
                CredentialId::generate(),
                application_client_id,
                "process-test-key".parse()?,
                application_scopes,
                TimestampMicros::new(1_800_000_000_000_002),
                None,
            )
            .await?;

        let process = LocalProcess::start(directory.path(), test_config()).await?;
        assert!(process.is_ready());
        assert_eq!(process.state(), &state);
        assert!(matches!(
            LocalProcess::start(directory.path(), test_config()).await,
            Err(LocalProcessError::AlreadyRunning)
        ));
        let base = format!("http://{}", process.address());
        let client = reqwest::Client::new();
        let health = client.get(format!("{base}/healthz")).send().await?;
        assert_eq!(health.status(), reqwest::StatusCode::OK);
        assert_eq!(health.text().await?, "{\"status\":\"ok\"}");
        let call = QueryCallV1 {
            target: CodeTarget::Workspace(workspace),
            function: "queries.echo".parse()?,
            arguments: CanonicalValue::String("hello-local".to_owned()),
        };
        let response = client
            .post(format!("{base}/v1/query"))
            .header("content-type", "application/json")
            .header("x-runku-key", application_key.key.expose())
            .body(encode_query_call_v1(&call)?)
            .send()
            .await?;
        let status = response.status();
        let response_bytes = response.bytes().await?;
        if status != reqwest::StatusCode::OK {
            return Err(format!("query failed: {:?}", decode_error_v1(&response_bytes)?).into());
        }
        let success = decode_success_v1(&response_bytes)?;
        assert_eq!(success.release_id, package.release_id);
        assert_eq!(
            success.result,
            CanonicalValue::String("hello-local".to_owned())
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        let telemetry = process.telemetry();
        assert!(telemetry.realtime_polls > 0);
        assert!(telemetry.scheduled_polls > 0);
        assert!(telemetry.cron_polls > 0);
        assert!(telemetry.catalog_refreshes > 0);
        process.shutdown().await;

        let restarted = LocalProcess::start(directory.path(), test_config()).await?;
        assert_eq!(restarted.state(), &state);
        assert!(restarted.is_ready());
        restarted.shutdown().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::too_many_lines)]
    async fn process_action_grants_drive_authenticated_http_file_transfer() -> TestResult {
        let directory = tempdir()?;
        let workspace = WorkspaceRef::from_str("default")?;
        let (state, _) = initialize_local(
            directory.path(),
            workspace.clone(),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?;
        let source = directory.path().join("runku");
        std::fs::create_dir(&source)?;
        std::fs::write(
            source.join("schema.ts"),
            "import { defineSchema } from '@runku/server'; export default defineSchema({});",
        )?;
        std::fs::write(
            source.join("files.ts"),
            r#"
import { action, v } from "@runku/server"
export const beginUpload = action({
  auth: "none", visibility: "public", capabilities: ["storage:write"],
  args: v.null(), returns: v.any(),
  handler(ctx) { return ctx.storage.createUpload({ maxBytes: 6, contentType: "text/plain" }); },
})
export const beginDownload = action({
  auth: "none", visibility: "public", capabilities: ["storage:read"],
  args: v.string(), returns: v.any(),
  handler(ctx, fileId) { return ctx.storage.createDownload(fileId, { expiresInMicros: 1000000n }); },
})
"#,
        )?;
        let output = build_project(
            directory.path(),
            Path::new("runku"),
            state.project_id,
            BuildMetadata {
                release_id: ReleaseId::generate(),
                build_id: BuildId::generate(),
                created_at: TimestampMicros::new(1_800_000_000_000_001),
            },
        )?;
        publish_local(
            directory.path(),
            &workspace,
            &DevelopmentActor::from_str("file-transfer-test")?,
            &std::fs::read(output.manifest_path)?,
            &std::fs::read(output.artifact_path)?,
        )
        .await?;
        let identities = LocalIdentityManager::open(directory.path()).await?;
        let scopes = BTreeSet::from(["functions:invoke".parse::<ApplicationScope>()?]);
        let client_id = ApplicationClientId::generate();
        identities
            .create_client(
                client_id,
                "file-transfer-browser".parse()?,
                ClientKind::Public,
                scopes.clone(),
                TimestampMicros::new(1_800_000_000_000_002),
            )
            .await?;
        let application_key = identities
            .create_credential(
                CredentialId::generate(),
                client_id,
                "file-transfer-key".parse()?,
                scopes,
                TimestampMicros::new(1_800_000_000_000_003),
                None,
            )
            .await?;
        let process = LocalProcess::start(directory.path(), test_config()).await?;
        let base = format!("http://{}", process.address());
        let client = reqwest::Client::new();
        let invoke = |function: &str,
                      arguments: CanonicalValue|
         -> Result<Vec<u8>, Box<dyn std::error::Error>> {
            Ok(encode_action_call_v1(&ActionCallV1 {
                target: CodeTarget::Workspace(workspace.clone()),
                function: function.parse()?,
                arguments,
            })?)
        };
        let upload_response = client
            .post(format!("{base}/v1/action"))
            .header("content-type", "application/json")
            .header("x-runku-key", application_key.key.expose())
            .body(invoke("files.beginUpload", CanonicalValue::Null)?)
            .send()
            .await?;
        let status = upload_response.status();
        let bytes = upload_response.bytes().await?;
        if status != reqwest::StatusCode::OK {
            return Err(
                format!("upload grant Action failed: {:?}", decode_error_v1(&bytes)?).into(),
            );
        }
        let CanonicalValue::Object(upload) = decode_success_v1(&bytes)?.result else {
            return Err("upload grant was not an object".into());
        };
        let Some(CanonicalValue::String(upload_path)) = upload.get("path") else {
            return Err("upload grant path missing".into());
        };
        let Some(CanonicalValue::String(upload_token)) = upload.get("token") else {
            return Err("upload grant token missing".into());
        };
        let stored = client
            .put(format!("{base}{upload_path}"))
            .bearer_auth(upload_token)
            .header("content-type", "text/plain")
            .body("abcdef")
            .send()
            .await?;
        assert_eq!(stored.status(), reqwest::StatusCode::CREATED);
        let stored: serde_json::Value = serde_json::from_slice(&stored.bytes().await?)?;
        let file_id = stored["file"]["fileId"]
            .as_str()
            .ok_or("stored file ID missing")?
            .to_owned();
        let download_response = client
            .post(format!("{base}/v1/action"))
            .header("content-type", "application/json")
            .header("x-runku-key", application_key.key.expose())
            .body(invoke(
                "files.beginDownload",
                CanonicalValue::String(file_id),
            )?)
            .send()
            .await?;
        let status = download_response.status();
        let bytes = download_response.bytes().await?;
        if status != reqwest::StatusCode::OK {
            return Err(format!(
                "download grant Action failed: {:?}",
                decode_error_v1(&bytes)?
            )
            .into());
        }
        let CanonicalValue::Object(download) = decode_success_v1(&bytes)?.result else {
            return Err("download grant was not an object".into());
        };
        let Some(CanonicalValue::String(download_path)) = download.get("path") else {
            return Err("download grant path missing".into());
        };
        let Some(CanonicalValue::String(download_token)) = download.get("token") else {
            return Err("download grant token missing".into());
        };
        let downloaded = client
            .get(format!("{base}{download_path}"))
            .bearer_auth(download_token)
            .send()
            .await?;
        assert_eq!(downloaded.status(), reqwest::StatusCode::OK);
        assert_eq!(downloaded.bytes().await?.as_ref(), b"abcdef");
        process.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn process_rejects_missing_publication_and_occupied_port() -> TestResult {
        let unpublished = tempdir()?;
        let workspace = WorkspaceRef::from_str("default")?;
        initialize_local(
            unpublished.path(),
            workspace.clone(),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            TimestampMicros::new(1),
        )
        .await?;
        assert!(matches!(
            LocalProcess::start(unpublished.path(), test_config()).await,
            Err(LocalProcessError::Composition)
        ));

        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = occupied.local_addr()?;
        let directory = tempdir()?;
        let (state, _) = initialize_local(
            directory.path(),
            workspace.clone(),
            address,
            TimestampMicros::new(2),
        )
        .await?;
        let package = package(state.project_id)?;
        publish_local(
            directory.path(),
            &workspace,
            &DevelopmentActor::from_str("local-test")?,
            &package.manifest,
            &package.artifact,
        )
        .await?;
        assert!(matches!(
            LocalProcess::start(directory.path(), test_config()).await,
            Err(LocalProcessError::ListenerUnavailable)
        ));
        Ok(())
    }
}
