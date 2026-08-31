//! Real repository and real socket conformance for Remote Workspace service v1.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use runku_core::{
    BuildId, CodeTarget, DevelopmentCredentialId, EnvironmentDescriptor, EnvironmentId,
    EnvironmentLocation, EnvironmentProtection, EnvironmentPurpose, EnvironmentScope, FunctionId,
    OperationId, ProjectId, ReleaseId, RequestId, WorkspaceId,
};
use runku_development::{
    DevelopmentActor, DevelopmentBackend, DevelopmentCommand, DevelopmentCommandResult,
    DevelopmentContext, DevelopmentError, DevelopmentRepository, DevelopmentRepositoryConfig,
    DevelopmentSnapshot, DevelopmentTelemetrySnapshot, SqlDevelopmentRepository,
};
use runku_development_access::{
    DevelopmentAccessRepository, DevelopmentAccessRepositoryConfig,
    DevelopmentAccessRepositoryRole, DevelopmentCredential, DevelopmentCredentialStatus,
    DevelopmentKeyCrypto, ParsedDevelopmentKey, SqlDevelopmentAccessRepository,
};
use runku_development_service::{
    DevelopmentAuditEvent, DevelopmentAuditSink, DevelopmentHttpConfig, DevelopmentHttpExposure,
    DevelopmentServiceClock, DevelopmentServiceError, DevelopmentServingRefresher,
    ReleaseServingRefresher, RemoteWorkspaceService, RemoteWorkspaceServiceConfig,
    build_development_router, serve_development,
};
use runku_gateway::{DevelopmentCatalog, ServingCatalog, ServingRefresh};
use runku_protocol::{
    DevelopmentCreateWorkspaceRequestV1, DevelopmentFreezeOutcomeV1, DevelopmentFreezeRequestV1,
    DevelopmentPublishRequestV1, DevelopmentStateRequestV1, decode_development_error_v1,
    decode_development_state_response_v1, encode_development_freeze_request_v1,
    encode_development_publish_request_v1, encode_development_state_request_v1,
};
use runku_release_repository::{RepositoryConfig, RepositoryRole, SqlReleaseRepository};
use runku_releases::{
    ArtifactDescriptor, ArtifactStore, AuthPolicy, FilesystemArtifactStore, FilesystemStoreRole,
    FunctionManifest, FunctionType, FunctionVisibility, ReleaseCommand, ReleaseCommandResult,
    ReleaseError, ReleaseManifestV1, ReleaseRepository, ReleaseRepositoryBackend,
    ReleaseRepositoryTelemetrySnapshot, ReleaseStatus, RuntimeClass, SafeEsmBundleV1,
    ServingSnapshot, Sha256Digest, encode_release_manifest, encode_safe_esm_bundle,
};
use runku_value::TimestampMicros;
use tempfile::TempDir;
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Default)]
struct Audit(Mutex<Vec<DevelopmentAuditEvent>>);

impl DevelopmentAuditSink for Audit {
    fn try_emit(&self, event: DevelopmentAuditEvent) {
        if let Ok(mut events) = self.0.lock() {
            events.push(event);
        }
    }
}

#[derive(Debug)]
struct Clock(AtomicI64);

impl Clock {
    const fn new(start: i64) -> Self {
        Self(AtomicI64::new(start))
    }
}

impl DevelopmentServiceClock for Clock {
    fn now(&self) -> Result<TimestampMicros, DevelopmentServiceError> {
        Ok(TimestampMicros::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

struct Setup {
    directory: TempDir,
    scope: EnvironmentScope,
    environment: EnvironmentDescriptor,
    bearer: String,
    service: Arc<RemoteWorkspaceService>,
    access: Arc<SqlDevelopmentAccessRepository>,
    crypto: Arc<DevelopmentKeyCrypto>,
    development: Arc<SqlDevelopmentRepository>,
    releases: Arc<SqlReleaseRepository>,
    artifacts: Arc<FilesystemArtifactStore>,
    catalog: Arc<DevelopmentCatalog>,
    serving: Arc<ServingCatalog>,
    audit: Arc<Audit>,
}

#[derive(Debug)]
struct SlowRefresher {
    catalog: Arc<DevelopmentCatalog>,
    delay: Duration,
}

#[derive(Debug)]
struct SlowReleaseRefresher {
    catalog: Arc<ServingCatalog>,
    delay: Duration,
}

#[derive(Debug)]
struct FailOnceRefresher {
    catalog: Arc<DevelopmentCatalog>,
    fail: AtomicBool,
}

#[async_trait]
impl DevelopmentServingRefresher for FailOnceRefresher {
    fn scope(&self) -> EnvironmentScope {
        self.catalog.scope()
    }

    async fn refresh(&self) -> Result<u64, DevelopmentError> {
        if self.fail.swap(false, Ordering::Relaxed) {
            return Err(DevelopmentError::Unavailable);
        }
        match self.catalog.refresh().await? {
            ServingRefresh::Published { revision } | ServingRefresh::Unchanged { revision } => {
                Ok(revision)
            }
        }
    }
}

#[derive(Debug)]
struct FailOnceArtifact {
    inner: Arc<FilesystemArtifactStore>,
    fail: AtomicBool,
}

#[async_trait]
impl ArtifactStore for FailOnceArtifact {
    async fn put(&self, descriptor: &ArtifactDescriptor, bytes: &[u8]) -> Result<(), ReleaseError> {
        if self.fail.swap(false, Ordering::Relaxed) {
            return Err(ReleaseError::Unavailable);
        }
        self.inner.put(descriptor, bytes).await
    }

    async fn get(&self, descriptor: &ArtifactDescriptor) -> Result<Vec<u8>, ReleaseError> {
        self.inner.get(descriptor).await
    }
}

#[derive(Debug)]
struct FailOnceReleases {
    inner: Arc<SqlReleaseRepository>,
    fail: AtomicBool,
}

#[async_trait]
impl ReleaseRepository for FailOnceReleases {
    fn backend(&self) -> ReleaseRepositoryBackend {
        self.inner.backend()
    }

    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &ReleaseCommand,
    ) -> Result<ReleaseCommandResult, ReleaseError> {
        if self.fail.swap(false, Ordering::Relaxed) {
            return Err(ReleaseError::Unavailable);
        }
        self.inner.apply(scope, operation_id, command).await
    }

    async fn snapshot(&self, scope: EnvironmentScope) -> Result<ServingSnapshot, ReleaseError> {
        self.inner.snapshot(scope).await
    }

    async fn manifest(
        &self,
        scope: EnvironmentScope,
        release_id: ReleaseId,
    ) -> Result<ReleaseManifestV1, ReleaseError> {
        self.inner.manifest(scope, release_id).await
    }

    async fn health(&self) -> Result<(), ReleaseError> {
        self.inner.health().await
    }

    fn telemetry(&self) -> ReleaseRepositoryTelemetrySnapshot {
        self.inner.telemetry()
    }
}

#[derive(Debug)]
struct FailOnceDevelopment {
    inner: Arc<SqlDevelopmentRepository>,
    fail: AtomicBool,
}

#[async_trait]
impl DevelopmentRepository for FailOnceDevelopment {
    fn backend(&self) -> DevelopmentBackend {
        self.inner.backend()
    }

    async fn apply(
        &self,
        context: DevelopmentContext,
        operation_id: OperationId,
        command: &DevelopmentCommand,
    ) -> Result<DevelopmentCommandResult, DevelopmentError> {
        if matches!(command, DevelopmentCommand::PublishRevision { .. })
            && self.fail.swap(false, Ordering::Relaxed)
        {
            return Err(DevelopmentError::Unavailable);
        }
        self.inner.apply(context, operation_id, command).await
    }

    async fn snapshot(
        &self,
        context: DevelopmentContext,
    ) -> Result<DevelopmentSnapshot, DevelopmentError> {
        self.inner.snapshot(context).await
    }

    async fn health(&self) -> Result<(), DevelopmentError> {
        self.inner.health().await
    }

    fn telemetry(&self) -> DevelopmentTelemetrySnapshot {
        self.inner.telemetry()
    }
}

#[async_trait]
impl DevelopmentServingRefresher for SlowRefresher {
    fn scope(&self) -> EnvironmentScope {
        self.catalog.scope()
    }

    async fn refresh(&self) -> Result<u64, runku_development::DevelopmentError> {
        tokio::time::sleep(self.delay).await;
        match self.catalog.refresh().await? {
            ServingRefresh::Published { revision } | ServingRefresh::Unchanged { revision } => {
                Ok(revision)
            }
        }
    }
}

#[async_trait]
impl ReleaseServingRefresher for SlowReleaseRefresher {
    fn scope(&self) -> EnvironmentScope {
        self.catalog.scope()
    }

    async fn refresh(&self) -> Result<u64, ReleaseError> {
        tokio::time::sleep(self.delay).await;
        match self.catalog.refresh().await? {
            ServingRefresh::Published { revision } | ServingRefresh::Unchanged { revision } => {
                Ok(revision)
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn setup() -> TestResult<Setup> {
    let directory = tempfile::tempdir()?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let environment = EnvironmentDescriptor::new(
        scope.environment_id(),
        EnvironmentPurpose::Development,
        EnvironmentProtection::Open,
        EnvironmentLocation::Local,
        true,
    )?;
    let context = DevelopmentContext { scope, environment };
    let access = Arc::new(
        SqlDevelopmentAccessRepository::connect_sqlite(
            &format!(
                "sqlite://{}?mode=rwc",
                directory.path().join("access.sqlite3").display()
            ),
            DevelopmentAccessRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let crypto = Arc::new(DevelopmentKeyCrypto::new([41; 32]));
    let generated = crypto.generate(DevelopmentCredentialId::generate())?;
    let parsed: ParsedDevelopmentKey = generated.key.expose().parse()?;
    access
        .create_credential(&DevelopmentCredential {
            id: parsed.credential_id(),
            scope,
            actor: "manuel.remote".parse::<DevelopmentActor>()?,
            label: "laptop".parse()?,
            digest: generated.digest,
            status: DevelopmentCredentialStatus::Active,
            created_at: TimestampMicros::new(1),
            expires_at: None,
            revoked_at: None,
            deleted_at: None,
        })
        .await?;
    let bearer = generated.key.expose().to_owned();
    let development = Arc::new(
        SqlDevelopmentRepository::connect_sqlite(
            &format!(
                "sqlite://{}?mode=rwc",
                directory.path().join("development.sqlite3").display()
            ),
            DevelopmentRepositoryConfig::LOCAL,
            context,
        )
        .await?,
    );
    development
        .apply(
            context,
            OperationId::generate(),
            &DevelopmentCommand::CreateWorkspace {
                workspace_id: WorkspaceId::generate(),
                workspace_ref: "bootstrap".parse()?,
                actor: "system".parse()?,
                created_at: TimestampMicros::new(1),
            },
        )
        .await?;
    let releases = Arc::new(
        SqlReleaseRepository::connect_sqlite(
            &format!(
                "sqlite://{}?mode=rwc",
                directory.path().join("releases.sqlite3").display()
            ),
            RepositoryConfig::LOCAL,
        )
        .await?,
    );
    let artifacts = Arc::new(
        FilesystemArtifactStore::open(
            directory.path().join("artifacts"),
            FilesystemStoreRole::LocalDevelopment,
        )
        .await?,
    );
    let catalog = Arc::new(DevelopmentCatalog::load(context, development.clone()).await?);
    let serving = Arc::new(ServingCatalog::load_allow_empty(scope, releases.clone()).await?);
    let audit = Arc::new(Audit::default());
    let service = Arc::new(RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig { scope, environment },
        access.clone(),
        crypto.clone(),
        development.clone(),
        releases.clone(),
        artifacts.clone(),
        catalog.clone(),
        serving.clone(),
        Arc::new(Clock::new(100)),
        audit.clone(),
    )?);
    Ok(Setup {
        directory,
        scope,
        environment,
        bearer,
        service,
        access,
        crypto,
        development,
        releases,
        artifacts,
        catalog,
        serving,
        audit,
    })
}

struct Package {
    manifest: ReleaseManifestV1,
    manifest_bytes: Vec<u8>,
    artifact_bytes: Vec<u8>,
}

fn package(project_id: ProjectId, sequence: u128) -> TestResult<Package> {
    let source = format!("export default () => ({sequence});");
    let implementation_hash = Sha256Digest::of(source.as_bytes());
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact_bytes = encode_safe_esm_bundle(&bundle)?;
    let contract = Sha256Digest::of(&sequence.to_be_bytes());
    let manifest = ReleaseManifestV1 {
        release_id: ReleaseId::from_ulid(ulid::Ulid::from(sequence + 1000)),
        project_id,
        build_id: BuildId::from_ulid(ulid::Ulid::from(sequence + 2000)),
        created_at: TimestampMicros::new(i64::try_from(sequence)?),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: contract,
        schema_contract_hash: contract,
        index_contract_hash: contract,
        functions: vec![FunctionManifest {
            id: FunctionId::from_ulid(ulid::Ulid::from(sequence + 3000)),
            name: "queries.version".parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash,
            arguments_contract_hash: contract,
            result_contract_hash: contract,
            capabilities: vec![],
        }],
        cron_definitions: vec![],
    };
    Ok(Package {
        manifest_bytes: encode_release_manifest(&manifest)?,
        manifest,
        artifact_bytes,
    })
}

fn publish_request(
    package: Package,
    operation_id: OperationId,
    workspace: &str,
    expected_head: Option<runku_core::DevRevisionId>,
) -> TestResult<DevelopmentPublishRequestV1> {
    Ok(DevelopmentPublishRequestV1 {
        operation_id,
        project_id: package.manifest.project_id,
        workspace_ref: workspace.parse()?,
        expected_head,
        manifest: package.manifest,
        manifest_bytes: package.manifest_bytes,
        artifact_bytes: package.artifact_bytes,
    })
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn authenticated_create_publish_replay_two_workspaces_and_revoke_are_exact() -> TestResult {
    let setup = setup().await?;
    let create = DevelopmentCreateWorkspaceRequestV1 {
        operation_id: OperationId::generate(),
        workspace_id: WorkspaceId::generate(),
        workspace_ref: "dev/manuel".parse()?,
    };
    let first = setup
        .service
        .create_workspace(RequestId::generate(), &setup.bearer, create.clone())
        .await?;
    assert!(!first.replayed);
    let replay = setup
        .service
        .create_workspace(RequestId::generate(), &setup.bearer, create)
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.workspace, first.workspace);

    let operation = OperationId::generate();
    let request = publish_request(
        package(setup.scope.project_id(), 10)?,
        operation,
        "dev/manuel",
        None,
    )?;
    let first_publish = setup
        .service
        .publish(RequestId::generate(), &setup.bearer, request.clone())
        .await?;
    assert!(!first_publish.replayed);
    let publish_replay = setup
        .service
        .publish(RequestId::generate(), &setup.bearer, request)
        .await?;
    assert!(publish_replay.replayed);
    assert_eq!(publish_replay.revision_id, first_publish.revision_id);
    let restarted_catalog = Arc::new(
        DevelopmentCatalog::load(
            DevelopmentContext {
                scope: setup.scope,
                environment: setup.environment,
            },
            setup.development.clone(),
        )
        .await?,
    );
    let restarted = RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig {
            scope: setup.scope,
            environment: setup.environment,
        },
        setup.access.clone(),
        setup.crypto.clone(),
        setup.development.clone(),
        setup.releases.clone(),
        setup.artifacts.clone(),
        restarted_catalog,
        setup.serving.clone(),
        Arc::new(Clock::new(400)),
        setup.audit.clone(),
    )?;
    assert!(
        restarted
            .publish(
                RequestId::generate(),
                &setup.bearer,
                publish_request(
                    package(setup.scope.project_id(), 10)?,
                    operation,
                    "dev/manuel",
                    None,
                )?,
            )
            .await?
            .replayed
    );
    assert_eq!(
        setup
            .releases
            .manifest(setup.scope, first_publish.release_id)
            .await?
            .release_id,
        first_publish.release_id
    );

    let second_create = DevelopmentCreateWorkspaceRequestV1 {
        operation_id: OperationId::generate(),
        workspace_id: WorkspaceId::generate(),
        workspace_ref: "agent/fix".parse()?,
    };
    setup
        .service
        .create_workspace(RequestId::generate(), &setup.bearer, second_create)
        .await?;
    let second = setup
        .service
        .publish(
            RequestId::generate(),
            &setup.bearer,
            publish_request(
                package(setup.scope.project_id(), 11)?,
                OperationId::generate(),
                "agent/fix",
                None,
            )?,
        )
        .await?;
    assert_ne!(second.revision_id, first_publish.revision_id);
    let snapshot = setup
        .development
        .snapshot(DevelopmentContext {
            scope: setup.scope,
            environment: setup.environment,
        })
        .await?;
    assert_eq!(
        snapshot
            .workspace_binding(&"dev/manuel".parse()?)
            .ok_or("missing first workspace")?
            .head_revision,
        Some(first_publish.revision_id)
    );
    assert_eq!(
        snapshot
            .workspace_binding(&"agent/fix".parse()?)
            .ok_or("missing second workspace")?
            .head_revision,
        Some(second.revision_id)
    );

    for invalid in [
        "rk_pub_v1_not-a-development-key",
        "rk_sec_v1_not-a-development-key",
        "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature",
        "malformed",
    ] {
        assert_eq!(
            setup
                .service
                .state(
                    RequestId::generate(),
                    invalid,
                    DevelopmentStateRequestV1 {
                        workspace_ref: "dev/manuel".parse()?,
                    },
                )
                .await,
            Err(DevelopmentServiceError::Unauthenticated)
        );
    }
    let other_scope = EnvironmentScope::new(setup.scope.project_id(), EnvironmentId::generate());
    let other_generated = setup.crypto.generate(DevelopmentCredentialId::generate())?;
    let other_parsed: ParsedDevelopmentKey = other_generated.key.expose().parse()?;
    setup
        .access
        .create_credential(&DevelopmentCredential {
            id: other_parsed.credential_id(),
            scope: other_scope,
            actor: "cross.scope".parse()?,
            label: "cross-scope".parse()?,
            digest: other_generated.digest,
            status: DevelopmentCredentialStatus::Active,
            created_at: TimestampMicros::new(1),
            expires_at: None,
            revoked_at: None,
            deleted_at: None,
        })
        .await?;
    assert_eq!(
        setup
            .service
            .state(
                RequestId::generate(),
                other_generated.key.expose(),
                DevelopmentStateRequestV1 {
                    workspace_ref: "dev/manuel".parse()?,
                },
            )
            .await,
        Err(DevelopmentServiceError::Unauthenticated)
    );

    let parsed: ParsedDevelopmentKey = setup.bearer.parse()?;
    setup
        .access
        .revoke_credential(
            setup.scope,
            parsed.credential_id(),
            TimestampMicros::new(500),
        )
        .await?;
    assert_eq!(
        setup
            .service
            .state(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentStateRequestV1 {
                    workspace_ref: "dev/manuel".parse()?,
                },
            )
            .await,
        Err(DevelopmentServiceError::Unauthenticated)
    );
    assert_eq!(setup.service.telemetry().create_successes, 3);
    assert_eq!(setup.service.telemetry().publish_successes, 3);
    let audit = setup.audit.0.lock().map_err(|_| "audit lock poisoned")?;
    assert_eq!(audit.len(), 13);
    assert!(audit.iter().all(|event| event.scope == setup.scope));
    assert!(!format!("{audit:?}").contains(&setup.bearer));
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn freeze_is_explicit_replayable_servable_and_compatibility_blocked() -> TestResult {
    let setup = setup().await?;
    setup
        .service
        .create_workspace(
            RequestId::generate(),
            &setup.bearer,
            DevelopmentCreateWorkspaceRequestV1 {
                operation_id: OperationId::generate(),
                workspace_id: WorkspaceId::generate(),
                workspace_ref: "freeze/test".parse()?,
            },
        )
        .await?;
    let baseline = setup
        .service
        .publish(
            RequestId::generate(),
            &setup.bearer,
            publish_request(
                package(setup.scope.project_id(), 80)?,
                OperationId::generate(),
                "freeze/test",
                None,
            )?,
        )
        .await?;
    assert_eq!(
        setup
            .releases
            .snapshot(setup.scope)
            .await?
            .release(baseline.release_id)
            .ok_or("candidate missing")?
            .status,
        ReleaseStatus::Created
    );
    assert!(
        setup
            .serving
            .resolve(&CodeTarget::Release(baseline.release_id))
            .is_err()
    );
    let freeze = DevelopmentFreezeRequestV1 {
        operation_id: OperationId::generate(),
        release_id: baseline.release_id,
        against_release_id: None,
    };
    let frozen = setup
        .service
        .freeze(RequestId::generate(), &setup.bearer, freeze.clone())
        .await
        .map_err(|error| format!("first freeze failed: {error:?}"))?;
    assert_eq!(frozen.outcome, DevelopmentFreezeOutcomeV1::Servable);
    assert!(!frozen.replayed);
    let resolved = setup
        .serving
        .resolve(&CodeTarget::Release(baseline.release_id));
    assert!(
        resolved.is_ok(),
        "stable release did not refresh: {resolved:?}"
    );
    assert_eq!(resolved?.release_id, baseline.release_id);
    assert!(
        setup
            .service
            .freeze(RequestId::generate(), &setup.bearer, freeze)
            .await?
            .replayed
    );

    let candidate = setup
        .service
        .publish(
            RequestId::generate(),
            &setup.bearer,
            publish_request(
                package(setup.scope.project_id(), 81)?,
                OperationId::generate(),
                "freeze/test",
                Some(baseline.revision_id),
            )?,
        )
        .await
        .map_err(|error| format!("second publish failed: {error:?}"))?;
    let blocked_request = DevelopmentFreezeRequestV1 {
        operation_id: OperationId::generate(),
        release_id: candidate.release_id,
        against_release_id: Some(baseline.release_id),
    };
    let blocked = setup
        .service
        .freeze(
            RequestId::generate(),
            &setup.bearer,
            blocked_request.clone(),
        )
        .await
        .map_err(|error| format!("blocked freeze failed: {error:?}"))?;
    assert_eq!(
        blocked.outcome,
        DevelopmentFreezeOutcomeV1::CompatibilityBlocked
    );
    assert!(!blocked.diagnostics.is_empty());
    assert!(
        setup
            .serving
            .resolve(&CodeTarget::Release(candidate.release_id))
            .is_err()
    );
    assert!(
        setup
            .service
            .freeze(RequestId::generate(), &setup.bearer, blocked_request,)
            .await?
            .replayed
    );
    assert_eq!(setup.service.telemetry().freeze_successes, 4);
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn freeze_concurrent_retry_cross_scope_and_artifact_tamper_fail_closed() -> TestResult {
    let setup = setup().await?;
    for workspace in ["freeze/race", "freeze/tamper"] {
        setup
            .service
            .create_workspace(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentCreateWorkspaceRequestV1 {
                    operation_id: OperationId::generate(),
                    workspace_id: WorkspaceId::generate(),
                    workspace_ref: workspace.parse()?,
                },
            )
            .await?;
    }

    let candidate = setup
        .service
        .publish(
            RequestId::generate(),
            &setup.bearer,
            publish_request(
                package(setup.scope.project_id(), 82)?,
                OperationId::generate(),
                "freeze/race",
                None,
            )?,
        )
        .await?;
    let request = DevelopmentFreezeRequestV1 {
        operation_id: OperationId::generate(),
        release_id: candidate.release_id,
        against_release_id: None,
    };
    let (left, right) = tokio::join!(
        setup
            .service
            .freeze(RequestId::generate(), &setup.bearer, request.clone()),
        setup
            .service
            .freeze(RequestId::generate(), &setup.bearer, request)
    );
    let left = left?;
    let right = right?;
    assert_eq!(left.outcome, DevelopmentFreezeOutcomeV1::Servable);
    assert_eq!(right.outcome, DevelopmentFreezeOutcomeV1::Servable);
    assert_ne!(left.replayed, right.replayed);

    let other = crate::setup().await?;
    assert_eq!(
        other
            .service
            .freeze(
                RequestId::generate(),
                &other.bearer,
                DevelopmentFreezeRequestV1 {
                    operation_id: OperationId::generate(),
                    release_id: candidate.release_id,
                    against_release_id: None,
                },
            )
            .await,
        Err(DevelopmentServiceError::NotFound)
    );

    let tamper_package = package(setup.scope.project_id(), 83)?;
    let descriptor = tamper_package.manifest.artifact;
    let tampered_size = tamper_package.artifact_bytes.len();
    let tamper_candidate = setup
        .service
        .publish(
            RequestId::generate(),
            &setup.bearer,
            publish_request(
                tamper_package,
                OperationId::generate(),
                "freeze/tamper",
                None,
            )?,
        )
        .await?;
    let digest = descriptor.digest.to_string();
    let artifact_path = setup
        .directory
        .path()
        .join("artifacts")
        .join(&digest[..2])
        .join(&digest[2..4])
        .join(format!("{}.artifact", &digest[4..]));
    tokio::fs::write(artifact_path, vec![0x5a; tampered_size]).await?;
    assert_eq!(
        setup
            .service
            .freeze(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentFreezeRequestV1 {
                    operation_id: OperationId::generate(),
                    release_id: tamper_candidate.release_id,
                    against_release_id: None,
                },
            )
            .await,
        Err(DevelopmentServiceError::Corruption)
    );
    assert!(
        setup
            .serving
            .resolve(&CodeTarget::Release(tamper_candidate.release_id))
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn freeze_timeout_detaches_then_exact_retry_observes_the_committed_release() -> TestResult {
    let setup = setup().await?;
    setup
        .service
        .create_workspace(
            RequestId::generate(),
            &setup.bearer,
            DevelopmentCreateWorkspaceRequestV1 {
                operation_id: OperationId::generate(),
                workspace_id: WorkspaceId::generate(),
                workspace_ref: "freeze/slow".parse()?,
            },
        )
        .await?;
    let candidate = setup
        .service
        .publish(
            RequestId::generate(),
            &setup.bearer,
            publish_request(
                package(setup.scope.project_id(), 84)?,
                OperationId::generate(),
                "freeze/slow",
                None,
            )?,
        )
        .await?;
    let freeze = DevelopmentFreezeRequestV1 {
        operation_id: OperationId::generate(),
        release_id: candidate.release_id,
        against_release_id: None,
    };
    let slow_service = Arc::new(RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig {
            scope: setup.scope,
            environment: setup.environment,
        },
        setup.access,
        setup.crypto,
        setup.development,
        setup.releases,
        setup.artifacts,
        setup.catalog,
        Arc::new(SlowReleaseRefresher {
            catalog: setup.serving.clone(),
            delay: Duration::from_millis(150),
        }),
        Arc::new(Clock::new(850)),
        setup.audit,
    )?);
    let config = DevelopmentHttpConfig {
        max_concurrent_requests: 1,
        request_timeout: Duration::from_millis(10),
        exposure: DevelopmentHttpExposure::LoopbackPlaintext,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_development(
        listener,
        build_development_router(config, slow_service.clone())?,
        config.exposure,
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let response = reqwest::Client::new()
        .post(format!("http://{address}/v1/development/freeze"))
        .header("content-type", "application/json")
        .bearer_auth(&setup.bearer)
        .body(encode_development_freeze_request_v1(&freeze)?)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        decode_development_error_v1(&response.bytes().await?)?.error,
        runku_protocol::DevelopmentAdminErrorCodeV1::ResultUncertain
    );
    let mut resolved = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if setup
            .serving
            .resolve(&CodeTarget::Release(candidate.release_id))
            .is_ok()
        {
            resolved = true;
            break;
        }
    }
    assert!(
        resolved,
        "detached freeze did not refresh the serving catalog"
    );
    assert!(
        slow_service
            .freeze(RequestId::generate(), &setup.bearer, freeze)
            .await?
            .replayed
    );
    assert_eq!(slow_service.telemetry().deadline_responses, 1);
    shutdown_tx
        .send(())
        .map_err(|()| "shutdown receiver gone")?;
    server.await??;
    Ok(())
}

#[tokio::test]
async fn stale_concurrent_cas_has_one_winner_and_invalid_package_changes_no_head() -> TestResult {
    let setup = setup().await?;
    setup
        .service
        .create_workspace(
            RequestId::generate(),
            &setup.bearer,
            DevelopmentCreateWorkspaceRequestV1 {
                operation_id: OperationId::generate(),
                workspace_id: WorkspaceId::generate(),
                workspace_ref: "race".parse()?,
            },
        )
        .await?;
    let first = setup.service.publish(
        RequestId::generate(),
        &setup.bearer,
        publish_request(
            package(setup.scope.project_id(), 20)?,
            OperationId::generate(),
            "race",
            None,
        )?,
    );
    let second = setup.service.publish(
        RequestId::generate(),
        &setup.bearer,
        publish_request(
            package(setup.scope.project_id(), 21)?,
            OperationId::generate(),
            "race",
            None,
        )?,
    );
    let (first, second) = tokio::join!(first, second);
    assert!(matches!(
        (&first, &second),
        (Ok(_), Err(DevelopmentServiceError::Conflict))
            | (Err(DevelopmentServiceError::Conflict), Ok(_))
    ));
    let winner = first.ok().or_else(|| second.ok()).ok_or("winner missing")?;
    let mut invalid = publish_request(
        package(setup.scope.project_id(), 22)?,
        OperationId::generate(),
        "race",
        Some(winner.revision_id),
    )?;
    invalid.artifact_bytes.push(0);
    assert_eq!(
        setup
            .service
            .publish(RequestId::generate(), &setup.bearer, invalid)
            .await,
        Err(DevelopmentServiceError::InvalidRequest)
    );
    let state = setup
        .service
        .state(
            RequestId::generate(),
            &setup.bearer,
            DevelopmentStateRequestV1 {
                workspace_ref: "race".parse()?,
            },
        )
        .await?;
    assert_eq!(
        state.workspace.ok_or("workspace absent")?.head_revision,
        Some(winner.revision_id)
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn real_socket_is_strict_hardened_and_plaintext_loopback_only() -> TestResult {
    let setup = setup().await?;
    let config = DevelopmentHttpConfig {
        max_concurrent_requests: 4,
        request_timeout: Duration::from_secs(2),
        exposure: DevelopmentHttpExposure::LoopbackPlaintext,
    };
    let router = build_development_router(config, setup.service.clone())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_development(
        listener,
        router,
        config.exposure,
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let body = encode_development_state_request_v1(&DevelopmentStateRequestV1 {
        workspace_ref: "bootstrap".parse()?,
    })?;
    let response = client
        .post(format!("http://{address}/v1/development/state"))
        .header("content-type", "application/json")
        .bearer_auth(&setup.bearer)
        .body(body.clone())
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(response.headers().contains_key("x-runku-request-id"));
    assert_eq!(
        decode_development_state_response_v1(&response.bytes().await?)?
            .workspace
            .ok_or("bootstrap absent")?
            .workspace_ref
            .as_str(),
        "bootstrap"
    );

    for response in [
        client
            .post(format!("http://{address}/v1/development/state?x=1"))
            .header("content-type", "application/json")
            .bearer_auth(&setup.bearer)
            .body(body.clone())
            .send()
            .await?,
        client
            .post(format!("http://{address}/v1/development/state"))
            .header("content-type", "application/json; charset=utf-8")
            .bearer_auth(&setup.bearer)
            .body(body.clone())
            .send()
            .await?,
        client
            .post(format!("http://{address}/v1/development/state"))
            .header("content-type", "application/json")
            .header("origin", "https://attacker.invalid")
            .bearer_auth(&setup.bearer)
            .body(body.clone())
            .send()
            .await?,
        client
            .post(format!("http://{address}/v1/development/state"))
            .header("content-type", "application/json")
            .header("x-runku-key", "rk_pub_v1_invalid")
            .bearer_auth(&setup.bearer)
            .body(body.clone())
            .send()
            .await?,
    ] {
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let decoded = decode_development_error_v1(&response.bytes().await?)?;
        assert_eq!(
            decoded.error,
            runku_protocol::DevelopmentAdminErrorCodeV1::InvalidRequest
        );
    }
    let unauthorized = client
        .post(format!("http://{address}/v1/development/state"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await?;
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        unauthorized.headers()["www-authenticate"],
        "Bearer realm=\"runku-development\""
    );
    shutdown_tx
        .send(())
        .map_err(|()| "shutdown receiver gone")?;
    server.await??;

    let public = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let Err(error) = serve_development(
        public,
        build_development_router(config, setup.service)?,
        DevelopmentHttpExposure::LoopbackPlaintext,
        std::future::pending(),
    )
    .await
    else {
        return Err("public plaintext listener was accepted".into());
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    Ok(())
}

#[tokio::test]
async fn production_policy_fails_closed_after_auth_without_repository_mutation() -> TestResult {
    let setup = setup().await?;
    let production = EnvironmentDescriptor::new(
        setup.scope.environment_id(),
        EnvironmentPurpose::Production,
        EnvironmentProtection::Production,
        EnvironmentLocation::Managed,
        false,
    )?;
    let service = RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig {
            scope: setup.scope,
            environment: production,
        },
        setup.access,
        setup.crypto,
        setup.development.clone(),
        setup.releases,
        setup.artifacts,
        setup.catalog,
        setup.serving,
        Arc::new(Clock::new(600)),
        setup.audit,
    )?;
    assert_eq!(
        service
            .state(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentStateRequestV1 {
                    workspace_ref: "bootstrap".parse()?,
                },
            )
            .await,
        Err(DevelopmentServiceError::PolicyDenied)
    );
    assert_eq!(service.telemetry().policy_rejections, 1);
    let snapshot = setup
        .development
        .snapshot(DevelopmentContext {
            scope: setup.scope,
            environment: setup.environment,
        })
        .await?;
    assert_eq!(snapshot.revision(), 1);
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn mutation_timeout_detaches_completion_and_retains_admission_until_refresh() -> TestResult {
    let setup = setup().await?;
    setup
        .service
        .create_workspace(
            RequestId::generate(),
            &setup.bearer,
            DevelopmentCreateWorkspaceRequestV1 {
                operation_id: OperationId::generate(),
                workspace_id: WorkspaceId::generate(),
                workspace_ref: "slow".parse()?,
            },
        )
        .await?;
    let slow_service = Arc::new(RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig {
            scope: setup.scope,
            environment: setup.environment,
        },
        setup.access,
        setup.crypto,
        setup.development,
        setup.releases,
        setup.artifacts,
        Arc::new(SlowRefresher {
            catalog: setup.catalog,
            delay: Duration::from_millis(150),
        }),
        setup.serving,
        Arc::new(Clock::new(700)),
        setup.audit,
    )?);
    let config = DevelopmentHttpConfig {
        max_concurrent_requests: 1,
        request_timeout: Duration::from_millis(10),
        exposure: DevelopmentHttpExposure::LoopbackPlaintext,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_development(
        listener,
        build_development_router(config, slow_service.clone())?,
        config.exposure,
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let client = reqwest::Client::new();
    let publish = publish_request(
        package(setup.scope.project_id(), 30)?,
        OperationId::generate(),
        "slow",
        None,
    )?;
    let response = client
        .post(format!("http://{address}/v1/development/publish"))
        .header(
            "content-type",
            "application/vnd.runku.development-publish-v1",
        )
        .bearer_auth(&setup.bearer)
        .body(encode_development_publish_request_v1(&publish)?)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        decode_development_error_v1(&response.bytes().await?)?.error,
        runku_protocol::DevelopmentAdminErrorCodeV1::ResultUncertain
    );

    let state_body = encode_development_state_request_v1(&DevelopmentStateRequestV1 {
        workspace_ref: "slow".parse()?,
    })?;
    let busy = client
        .post(format!("http://{address}/v1/development/state"))
        .header("content-type", "application/json")
        .bearer_auth(&setup.bearer)
        .body(state_body.clone())
        .send()
        .await?;
    assert_eq!(busy.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    let mut complete = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let response = client
            .post(format!("http://{address}/v1/development/state"))
            .header("content-type", "application/json")
            .bearer_auth(&setup.bearer)
            .body(state_body.clone())
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::OK {
            complete = Some(response);
            break;
        }
        assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    }
    let complete = complete.ok_or("detached mutation did not release admission")?;
    let complete = decode_development_state_response_v1(&complete.bytes().await?)?;
    let completed_head = complete
        .workspace
        .ok_or("slow Workspace missing")?
        .head_revision
        .ok_or("slow Workspace remained empty")?;
    assert_eq!(
        setup
            .service
            .state(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentStateRequestV1 {
                    workspace_ref: "slow".parse()?,
                },
            )
            .await?
            .workspace
            .ok_or("slow workspace absent")?
            .head_revision,
        Some(completed_head)
    );
    assert_eq!(slow_service.telemetry().deadline_responses, 1);
    assert!(slow_service.telemetry().admission_rejections >= 1);
    shutdown_tx
        .send(())
        .map_err(|()| "shutdown receiver gone")?;
    server.await??;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn every_publish_boundary_is_recoverable_and_never_moves_head_early() -> TestResult {
    let setup = setup().await?;
    for workspace in [
        "fail/artifact",
        "fail/release",
        "fail/development",
        "fail/refresh",
    ] {
        setup
            .service
            .create_workspace(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentCreateWorkspaceRequestV1 {
                    operation_id: OperationId::generate(),
                    workspace_id: WorkspaceId::generate(),
                    workspace_ref: workspace.parse()?,
                },
            )
            .await?;
    }

    let artifact_service = RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig {
            scope: setup.scope,
            environment: setup.environment,
        },
        setup.access.clone(),
        setup.crypto.clone(),
        setup.development.clone(),
        setup.releases.clone(),
        Arc::new(FailOnceArtifact {
            inner: setup.artifacts.clone(),
            fail: AtomicBool::new(true),
        }),
        setup.catalog.clone(),
        setup.serving.clone(),
        Arc::new(Clock::new(900)),
        setup.audit.clone(),
    )?;
    let artifact_request = publish_request(
        package(setup.scope.project_id(), 50)?,
        OperationId::generate(),
        "fail/artifact",
        None,
    )?;
    assert_eq!(
        artifact_service
            .publish(
                RequestId::generate(),
                &setup.bearer,
                artifact_request.clone(),
            )
            .await,
        Err(DevelopmentServiceError::Unavailable)
    );
    assert!(
        setup
            .service
            .state(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentStateRequestV1 {
                    workspace_ref: "fail/artifact".parse()?,
                },
            )
            .await?
            .workspace
            .ok_or("artifact workspace missing")?
            .head_revision
            .is_none()
    );
    assert!(
        !artifact_service
            .publish(RequestId::generate(), &setup.bearer, artifact_request)
            .await?
            .replayed
    );

    let release_service = RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig {
            scope: setup.scope,
            environment: setup.environment,
        },
        setup.access.clone(),
        setup.crypto.clone(),
        setup.development.clone(),
        Arc::new(FailOnceReleases {
            inner: setup.releases.clone(),
            fail: AtomicBool::new(true),
        }),
        setup.artifacts.clone(),
        setup.catalog.clone(),
        setup.serving.clone(),
        Arc::new(Clock::new(920)),
        setup.audit.clone(),
    )?;
    let release_request = publish_request(
        package(setup.scope.project_id(), 51)?,
        OperationId::generate(),
        "fail/release",
        None,
    )?;
    assert_eq!(
        release_service
            .publish(
                RequestId::generate(),
                &setup.bearer,
                release_request.clone(),
            )
            .await,
        Err(DevelopmentServiceError::Unavailable)
    );
    assert_eq!(
        setup
            .artifacts
            .get(&release_request.manifest.artifact)
            .await?,
        release_request.artifact_bytes
    );
    assert!(
        setup
            .service
            .state(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentStateRequestV1 {
                    workspace_ref: "fail/release".parse()?,
                },
            )
            .await?
            .workspace
            .ok_or("release workspace missing")?
            .head_revision
            .is_none()
    );
    release_service
        .publish(RequestId::generate(), &setup.bearer, release_request)
        .await?;

    let development_service = RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig {
            scope: setup.scope,
            environment: setup.environment,
        },
        setup.access.clone(),
        setup.crypto.clone(),
        Arc::new(FailOnceDevelopment {
            inner: setup.development.clone(),
            fail: AtomicBool::new(true),
        }),
        setup.releases.clone(),
        setup.artifacts.clone(),
        setup.catalog.clone(),
        setup.serving.clone(),
        Arc::new(Clock::new(940)),
        setup.audit.clone(),
    )?;
    let development_request = publish_request(
        package(setup.scope.project_id(), 52)?,
        OperationId::generate(),
        "fail/development",
        None,
    )?;
    assert_eq!(
        development_service
            .publish(
                RequestId::generate(),
                &setup.bearer,
                development_request.clone(),
            )
            .await,
        Err(DevelopmentServiceError::Unavailable)
    );
    assert_eq!(
        setup
            .releases
            .manifest(setup.scope, development_request.manifest.release_id)
            .await?
            .release_id,
        development_request.manifest.release_id
    );
    assert!(
        setup
            .service
            .state(
                RequestId::generate(),
                &setup.bearer,
                DevelopmentStateRequestV1 {
                    workspace_ref: "fail/development".parse()?,
                },
            )
            .await?
            .workspace
            .ok_or("development workspace missing")?
            .head_revision
            .is_none()
    );
    development_service
        .publish(RequestId::generate(), &setup.bearer, development_request)
        .await?;

    let refresh_service = RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig {
            scope: setup.scope,
            environment: setup.environment,
        },
        setup.access,
        setup.crypto,
        setup.development,
        setup.releases,
        setup.artifacts,
        Arc::new(FailOnceRefresher {
            catalog: setup.catalog,
            fail: AtomicBool::new(true),
        }),
        setup.serving,
        Arc::new(Clock::new(960)),
        setup.audit,
    )?;
    let refresh_request = publish_request(
        package(setup.scope.project_id(), 53)?,
        OperationId::generate(),
        "fail/refresh",
        None,
    )?;
    assert_eq!(
        refresh_service
            .publish(
                RequestId::generate(),
                &setup.bearer,
                refresh_request.clone(),
            )
            .await,
        Err(DevelopmentServiceError::ResultUncertain)
    );
    let replay = refresh_service
        .publish(RequestId::generate(), &setup.bearer, refresh_request)
        .await?;
    assert!(replay.replayed);
    Ok(())
}

#[test]
fn configuration_bounds_are_closed() {
    for config in [
        DevelopmentHttpConfig {
            max_concurrent_requests: 0,
            request_timeout: Duration::from_secs(1),
            exposure: DevelopmentHttpExposure::LoopbackPlaintext,
        },
        DevelopmentHttpConfig {
            max_concurrent_requests: 1,
            request_timeout: Duration::ZERO,
            exposure: DevelopmentHttpExposure::TrustedTlsTermination,
        },
        DevelopmentHttpConfig {
            max_concurrent_requests: 100_001,
            request_timeout: Duration::from_secs(1),
            exposure: DevelopmentHttpExposure::TrustedTlsTermination,
        },
    ] {
        assert_eq!(
            config.validate(),
            Err(DevelopmentServiceError::InvalidRequest)
        );
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn postgres16_real_service_matches_create_publish_freeze_replay_and_cas() -> TestResult {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let directory = tempfile::tempdir()?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let environment = EnvironmentDescriptor::new(
        scope.environment_id(),
        EnvironmentPurpose::Preview,
        EnvironmentProtection::Protected,
        EnvironmentLocation::SelfHosted,
        true,
    )?;
    let context = DevelopmentContext { scope, environment };
    let access = Arc::new(
        SqlDevelopmentAccessRepository::connect_postgres(
            &url,
            DevelopmentAccessRepositoryConfig {
                role: DevelopmentAccessRepositoryRole::Authoritative,
                max_connections: 4,
                acquire_timeout: Duration::from_secs(5),
            },
        )
        .await?,
    );
    let crypto = Arc::new(DevelopmentKeyCrypto::new([51; 32]));
    let generated = crypto.generate(DevelopmentCredentialId::generate())?;
    let parsed: ParsedDevelopmentKey = generated.key.expose().parse()?;
    access
        .create_credential(&DevelopmentCredential {
            id: parsed.credential_id(),
            scope,
            actor: "postgres.agent".parse()?,
            label: "postgres-agent".parse()?,
            digest: generated.digest,
            status: DevelopmentCredentialStatus::Active,
            created_at: TimestampMicros::new(1),
            expires_at: None,
            revoked_at: None,
            deleted_at: None,
        })
        .await?;
    let development = Arc::new(
        SqlDevelopmentRepository::connect_postgres(
            &url,
            DevelopmentRepositoryConfig {
                role: runku_development::DevelopmentRepositoryRole::Authoritative,
                max_connections: 4,
                acquire_timeout: Duration::from_secs(5),
            },
            context,
        )
        .await?,
    );
    development
        .apply(
            context,
            OperationId::generate(),
            &DevelopmentCommand::CreateWorkspace {
                workspace_id: WorkspaceId::generate(),
                workspace_ref: "bootstrap".parse()?,
                actor: "system".parse()?,
                created_at: TimestampMicros::new(1),
            },
        )
        .await?;
    let releases = Arc::new(
        SqlReleaseRepository::connect_postgres(
            &url,
            RepositoryConfig {
                role: RepositoryRole::Production,
                max_connections: 4,
                acquire_timeout: Duration::from_secs(5),
            },
        )
        .await?,
    );
    let artifacts = Arc::new(
        FilesystemArtifactStore::open(
            directory.path().join("artifacts"),
            FilesystemStoreRole::LocalDevelopment,
        )
        .await?,
    );
    let catalog = Arc::new(DevelopmentCatalog::load(context, development.clone()).await?);
    let serving = Arc::new(ServingCatalog::load_allow_empty(scope, releases.clone()).await?);
    let service = RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig { scope, environment },
        access.clone(),
        crypto,
        development.clone(),
        releases,
        artifacts,
        catalog,
        serving,
        Arc::new(Clock::new(800)),
        Arc::new(Audit::default()),
    )?;
    let bearer = generated.key.expose();
    let create = DevelopmentCreateWorkspaceRequestV1 {
        operation_id: OperationId::generate(),
        workspace_id: WorkspaceId::generate(),
        workspace_ref: "shared/postgres".parse()?,
    };
    assert!(
        !service
            .create_workspace(RequestId::generate(), bearer, create.clone())
            .await?
            .replayed
    );
    assert!(
        service
            .create_workspace(RequestId::generate(), bearer, create)
            .await?
            .replayed
    );
    let request = publish_request(
        package(scope.project_id(), 40)?,
        OperationId::generate(),
        "shared/postgres",
        None,
    )?;
    let published = service
        .publish(RequestId::generate(), bearer, request.clone())
        .await?;
    assert!(
        service
            .publish(RequestId::generate(), bearer, request)
            .await?
            .replayed
    );
    assert_eq!(
        development
            .snapshot(context)
            .await?
            .workspace_binding(&"shared/postgres".parse()?)
            .ok_or("postgres Workspace missing")?
            .head_revision,
        Some(published.revision_id)
    );
    let freeze = DevelopmentFreezeRequestV1 {
        operation_id: OperationId::generate(),
        release_id: published.release_id,
        against_release_id: None,
    };
    let frozen = service
        .freeze(RequestId::generate(), bearer, freeze.clone())
        .await?;
    assert_eq!(frozen.outcome, DevelopmentFreezeOutcomeV1::Servable);
    assert!(!frozen.replayed);
    assert!(
        service
            .freeze(RequestId::generate(), bearer, freeze)
            .await?
            .replayed
    );
    access.close().await;
    Ok(())
}
