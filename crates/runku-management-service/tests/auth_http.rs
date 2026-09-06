//! Management HTTP contract coverage for bootstrap exchange and authenticated identity.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr as _,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use runku_core::{EnvironmentId, EnvironmentScope, OperationId, ProjectId};
use runku_management_service::{
    ExternalIdentityAuthenticator, ManagedEnrollmentKey, ManagementApplicationClientList,
    ManagementBucketPage, ManagementCronActivationResult, ManagementCronActivationSet,
    ManagementCronCatalog, ManagementCronQuery, ManagementDataDocument,
    ManagementDataInsertRequest, ManagementDataWriteResult, ManagementEnvironment,
    ManagementEnvironmentConfiguration, ManagementEnvironmentLifecycleChange,
    ManagementEnvironmentResult, ManagementHealthComponent, ManagementHttpConfig,
    ManagementHttpExposure, ManagementInstanceHealth, ManagementLogArchiveStatus,
    ManagementLogPage, ManagementLogPruneRequest, ManagementLogPruneResult, ManagementLogQuery,
    ManagementMetric, ManagementMetrics, ManagementObject, ManagementObjectDownload,
    ManagementObjectPage, ManagementObjectPut, ManagementObjectResult, ManagementProduct,
    ManagementProductError, ManagementReleaseOutcome, ManagementReleaseStatus,
    ManagementResolvedTarget, ManagementScheduledPage, ManagementServingCompatibility,
    ManagementServingRelease, ManagementWorkspacePublish, OidcClientConfiguration,
    build_management_router, build_management_router_with_product,
};
use runku_platform_identity::{
    AccessScope, BootstrapResult, DeviceName, ExternalOperatorIdentity, ManagedSourceAuthority,
    OperatorGrant, OperatorName, PlatformCapability, PlatformIdentityCrypto, PlatformIdentityError,
    PlatformIdentityRepository, PlatformIdentityRepositoryConfig, PlatformIdentityService,
    SessionTokenPolicy, SqlPlatformIdentityRepository,
};
use runku_value::TimestampMicros;
use serde_json::{Value, json};
use tower::ServiceExt as _;

#[derive(Debug)]
struct RejectingExternalIdentity;

#[derive(Debug)]
struct AcceptingExternalIdentity;

#[derive(Debug)]
struct ArchiveStatusProduct {
    scope: EnvironmentScope,
    calls: AtomicUsize,
    healthy: AtomicBool,
}

#[derive(Debug)]
struct DataProbeProduct {
    scope: EnvironmentScope,
    reads: AtomicUsize,
    writes: AtomicUsize,
    credential_reads: AtomicUsize,
    storage_reads: AtomicUsize,
    storage_writes: AtomicUsize,
    environment_reads: AtomicUsize,
    cron_reads: AtomicUsize,
    cron_writes: AtomicUsize,
    scheduled_reads: AtomicUsize,
    metrics_reads: AtomicUsize,
    instance_health_reads: AtomicUsize,
    compatibility_reads: AtomicUsize,
    environment_archives: AtomicUsize,
    environment_restores: AtomicUsize,
}

fn lifecycle_environment(
    scope: EnvironmentScope,
    revision: u64,
    desired_state: &str,
) -> ManagementEnvironment {
    ManagementEnvironment {
        version: 1,
        project_id: scope.project_id().to_string(),
        environment_id: scope.environment_id().to_string(),
        configuration: ManagementEnvironmentConfiguration {
            name: "Development".to_owned(),
            slug: "development".to_owned(),
            region: "local".to_owned(),
            purpose: "development".to_owned(),
            protection: "open".to_owned(),
            location: "local".to_owned(),
            workspace_targets_enabled: true,
        },
        configuration_revision: revision,
        desired_state: desired_state.to_owned(),
        observed_state: "ready".to_owned(),
        observed_configuration_revision: Some(revision),
        converged: true,
        created_at_micros: "1".to_owned(),
        updated_at_micros: "2".to_owned(),
        observed_at_micros: Some("2".to_owned()),
    }
}

#[async_trait]
impl ManagementProduct for DataProbeProduct {
    fn scope(&self) -> EnvironmentScope {
        self.scope
    }

    async fn health(&self) -> Result<(), ManagementProductError> {
        Ok(())
    }

    async fn metrics(&self) -> Result<ManagementMetrics, ManagementProductError> {
        self.metrics_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementMetrics {
            version: 1,
            metrics: vec![ManagementMetric {
                name: "runtime.admitted".to_owned(),
                value: "7".to_owned(),
                unit: "count".to_owned(),
            }],
        })
    }

    async fn instance_health(&self) -> Result<ManagementInstanceHealth, ManagementProductError> {
        self.instance_health_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementInstanceHealth {
            version: 1,
            instance_id: "product".to_owned(),
            status: "ready".to_owned(),
            components: vec![ManagementHealthComponent {
                name: "runtime".to_owned(),
                status: "ready".to_owned(),
            }],
        })
    }

    async fn serving_compatibility(
        &self,
    ) -> Result<ManagementServingCompatibility, ManagementProductError> {
        self.compatibility_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementServingCompatibility {
            version: 1,
            policy_revision: 2,
            compatible: true,
            converged: true,
            schema_contract_hash:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            index_contract_hash:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            cron_declarations_hash:
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            releases: vec![ManagementServingRelease {
                release_id: "rel_00000000000000000000000001".to_owned(),
                weight_percent: 100,
            }],
            diagnostics: Vec::new(),
        })
    }

    async fn environment(&self) -> Result<ManagementEnvironment, ManagementProductError> {
        self.environment_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementEnvironment {
            version: 1,
            project_id: self.scope.project_id().to_string(),
            environment_id: self.scope.environment_id().to_string(),
            configuration: ManagementEnvironmentConfiguration {
                name: "Development".to_owned(),
                slug: "development".to_owned(),
                region: "local".to_owned(),
                purpose: "development".to_owned(),
                protection: "open".to_owned(),
                location: "local".to_owned(),
                workspace_targets_enabled: true,
            },
            configuration_revision: 1,
            desired_state: "active".to_owned(),
            observed_state: "pending".to_owned(),
            observed_configuration_revision: None,
            converged: false,
            created_at_micros: "1".to_owned(),
            updated_at_micros: "1".to_owned(),
            observed_at_micros: None,
        })
    }

    async fn environment_archive(
        &self,
        operation_id: OperationId,
        request: &ManagementEnvironmentLifecycleChange,
    ) -> Result<ManagementEnvironmentResult, ManagementProductError> {
        self.environment_archives.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementEnvironmentResult {
            environment: lifecycle_environment(
                self.scope,
                request.expected_revision + 1,
                "archived",
            ),
            operation_id: operation_id.to_string(),
            replayed: false,
        })
    }

    async fn environment_restore(
        &self,
        operation_id: OperationId,
        request: &ManagementEnvironmentLifecycleChange,
    ) -> Result<ManagementEnvironmentResult, ManagementProductError> {
        self.environment_restores.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementEnvironmentResult {
            environment: lifecycle_environment(self.scope, request.expected_revision + 1, "active"),
            operation_id: operation_id.to_string(),
            replayed: false,
        })
    }

    async fn crons(
        &self,
        query: &ManagementCronQuery,
    ) -> Result<ManagementCronCatalog, ManagementProductError> {
        self.cron_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementCronCatalog {
            version: 1,
            target: ManagementResolvedTarget {
                requested: query.target.clone(),
                resolved: "release:rel_00000000000000000000000001".to_owned(),
                release_id: "rel_00000000000000000000000001".to_owned(),
                serving_revision: 1,
                schema_contract_hash:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
            },
            activation_revision: 0,
            crons: Vec::new(),
        })
    }

    async fn scheduled(
        &self,
        _after: Option<&str>,
        _limit: u16,
    ) -> Result<ManagementScheduledPage, ManagementProductError> {
        self.scheduled_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementScheduledPage {
            version: 1,
            scheduled: Vec::new(),
            next: None,
        })
    }

    async fn cron_activation_set(
        &self,
        _name: &str,
        operation_id: OperationId,
        request: &ManagementCronActivationSet,
    ) -> Result<ManagementCronActivationResult, ManagementProductError> {
        self.cron_writes.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementCronActivationResult {
            operation_id: operation_id.to_string(),
            repository_revision: request.expected_revision + 1,
            active_definitions: u32::from(request.enabled),
            replayed: false,
        })
    }

    async fn application_clients(
        &self,
    ) -> Result<ManagementApplicationClientList, ManagementProductError> {
        self.credential_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementApplicationClientList {
            version: 1,
            configuration_revision: 1,
            clients: Vec::new(),
        })
    }

    async fn buckets(
        &self,
        _after: Option<&str>,
        _limit: u16,
    ) -> Result<ManagementBucketPage, ManagementProductError> {
        self.storage_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementBucketPage {
            version: 1,
            buckets: Vec::new(),
            next: None,
        })
    }

    async fn storage_objects(
        &self,
        _bucket_id: &str,
        prefix: &str,
        _delimiter: Option<char>,
        _after: Option<&str>,
        _limit: u16,
    ) -> Result<ManagementObjectPage, ManagementProductError> {
        self.storage_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementObjectPage {
            version: 1,
            prefix: prefix.to_owned(),
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            next: None,
        })
    }

    async fn storage_object(
        &self,
        _bucket_id: &str,
        key: &str,
    ) -> Result<ManagementObjectDownload, ManagementProductError> {
        self.storage_reads.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementObjectDownload {
            object: probe_object(key),
            bytes: b"object bytes".to_vec(),
        })
    }

    async fn storage_object_put(
        &self,
        _bucket_id: &str,
        key: &str,
        operation_id: OperationId,
        _actor: runku_core::OperatorId,
        request: &ManagementObjectPut,
        bytes: Vec<u8>,
    ) -> Result<ManagementObjectResult, ManagementProductError> {
        assert_eq!(request.content_type, "text/plain");
        assert_eq!(request.at_micros, "1900000000000010");
        assert_eq!(
            request.metadata.get("cache-control").map(String::as_str),
            Some("private")
        );
        assert_eq!(bytes, b"object bytes");
        self.storage_writes.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementObjectResult {
            object: Some(probe_object(key)),
            operation_id: operation_id.to_string(),
            kind: "put".to_owned(),
            version_id: "ovr_00000000000000000000000001".to_owned(),
            replayed: false,
        })
    }

    async fn publish(
        &self,
        _actor: &str,
        _request: &[u8],
    ) -> Result<ManagementWorkspacePublish, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn release(
        &self,
        _release_id: &str,
        _against: Option<&str>,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn promote(
        &self,
        _channel: &str,
        _release_id: &str,
        _expected: Option<Option<&str>>,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn rollback(
        &self,
        _channel: &str,
        _expected: &str,
        _target: &str,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn status(&self) -> Result<ManagementReleaseStatus, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn data_get(
        &self,
        _target: &str,
        _table: &str,
        _document_id: &str,
    ) -> Result<ManagementDataDocument, ManagementProductError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Err(ManagementProductError::Invalid)
    }

    async fn data_insert(
        &self,
        _operation_id: OperationId,
        _table: &str,
        _request: &ManagementDataInsertRequest,
    ) -> Result<ManagementDataWriteResult, ManagementProductError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(ManagementProductError::Invalid)
    }

    async fn logs(
        &self,
        _query: &ManagementLogQuery,
    ) -> Result<ManagementLogPage, ManagementProductError> {
        Ok(ManagementLogPage {
            records: Vec::new(),
            next: "logc_0".to_owned(),
        })
    }

    async fn log_archive_status(
        &self,
    ) -> Result<ManagementLogArchiveStatus, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn log_prune(
        &self,
        _request: &ManagementLogPruneRequest,
    ) -> Result<ManagementLogPruneResult, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }
}

fn probe_object(key: &str) -> ManagementObject {
    ManagementObject {
        key: key.to_owned(),
        version_id: "ovr_00000000000000000000000001".to_owned(),
        size_bytes: "12".to_owned(),
        etag: format!("\"{}\"", "a".repeat(64)),
        sha256: "a".repeat(64),
        content_type: "text/plain".to_owned(),
        metadata: BTreeMap::from([("cache-control".to_owned(), "private".to_owned())]),
        created_at_micros: "1900000000000010".to_owned(),
    }
}

#[async_trait]
impl ManagementProduct for ArchiveStatusProduct {
    fn scope(&self) -> EnvironmentScope {
        self.scope
    }

    async fn health(&self) -> Result<(), ManagementProductError> {
        if self.healthy.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(ManagementProductError::Unavailable)
        }
    }

    async fn publish(
        &self,
        _actor: &str,
        _request: &[u8],
    ) -> Result<ManagementWorkspacePublish, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn release(
        &self,
        _release_id: &str,
        _against: Option<&str>,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn promote(
        &self,
        _channel: &str,
        _release_id: &str,
        _expected: Option<Option<&str>>,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn rollback(
        &self,
        _channel: &str,
        _expected: &str,
        _target: &str,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn status(&self) -> Result<ManagementReleaseStatus, ManagementProductError> {
        Err(ManagementProductError::Invalid)
    }

    async fn logs(
        &self,
        _query: &ManagementLogQuery,
    ) -> Result<ManagementLogPage, ManagementProductError> {
        Ok(ManagementLogPage {
            records: Vec::new(),
            next: "logc_0".to_owned(),
        })
    }

    async fn log_archive_status(
        &self,
    ) -> Result<ManagementLogArchiveStatus, ManagementProductError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementLogArchiveStatus {
            parquet_bytes: 4096,
            records: 12,
            segments: 2,
            through: "logc_12".to_owned(),
        })
    }

    async fn log_prune(
        &self,
        request: &ManagementLogPruneRequest,
    ) -> Result<ManagementLogPruneResult, ManagementProductError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ManagementLogPruneResult {
            applied: request.apply,
            deleted: u32::from(request.apply),
            environment_id: self.scope.environment_id().to_string(),
            matched: 1,
            more: false,
        })
    }
}

#[async_trait]
impl ExternalIdentityAuthenticator for RejectingExternalIdentity {
    async fn authenticate(
        &self,
        _bearer: &str,
        _now: TimestampMicros,
    ) -> Result<ExternalOperatorIdentity, PlatformIdentityError> {
        Err(PlatformIdentityError::Unauthenticated)
    }
}

#[async_trait]
impl ExternalIdentityAuthenticator for AcceptingExternalIdentity {
    async fn authenticate(
        &self,
        bearer: &str,
        _now: TimestampMicros,
    ) -> Result<ExternalOperatorIdentity, PlatformIdentityError> {
        if bearer != "verified-external-token" {
            return Err(PlatformIdentityError::Unauthenticated);
        }
        Ok(ExternalOperatorIdentity {
            provider_id: "cloud".to_owned(),
            subject_id: "better-user-1".to_owned(),
        })
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn managed_oidc_requires_gateway_secret_and_exposes_linkable_resources()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("managed-http.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let identity = Arc::new(PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([47; 32])),
        SessionTokenPolicy::DEFAULT,
    )?);
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let product = Arc::new(ArchiveStatusProduct {
        scope,
        calls: AtomicUsize::new(0),
        healthy: AtomicBool::new(true),
    });
    let router = build_management_router_with_product(
        ManagementHttpConfig {
            max_concurrent_requests: 8,
            exposure: ManagementHttpExposure::LoopbackPlaintext,
            public_management_endpoint: None,
            managed_enrollment_key: Some(ManagedEnrollmentKey::new(&"m".repeat(32))?),
            managed_source_authority: Some(ManagedSourceAuthority::from_str(
                "https://cloud.runku.example",
            )?),
        },
        identity,
        Some(Arc::new(AcceptingExternalIdentity)),
        Some(product),
        None,
    )?;
    let request_body = json!({
        "deviceName": "managed-device",
        "managedEnrollment": {
            "operatorName": "Cloud user",
            "sourceRevision": 1,
            "grants": [{
                "role": "developer",
                "scope": {"kind": "project", "projectId": scope.project_id(), "environmentId": null}
            }]
        }
    })
    .to_string();
    let denied = router
        .clone()
        .oneshot(
            Request::post("/v1/auth/oidc")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer verified-external-token")
                .body(Body::from(request_body.clone()))?,
        )
        .await?;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let accepted = router
        .clone()
        .oneshot(
            Request::post("/v1/auth/oidc")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer verified-external-token")
                .header(
                    "runku-managed-enrollment",
                    format!("Bearer {}", "m".repeat(32)),
                )
                .body(Body::from(request_body))?,
        )
        .await?;
    assert_eq!(accepted.status(), StatusCode::OK);
    let login: Value = serde_json::from_slice(&to_bytes(accepted.into_body(), 16 * 1024).await?)?;
    assert_eq!(login["applied"], true);
    assert_eq!(login["replayed"], false);
    assert_eq!(login["sourceRevision"], 1);
    let access = login["accessToken"]
        .as_str()
        .ok_or("missing access token")?;
    let operator_id = login["operatorId"].as_str().ok_or("missing operator id")?;
    let resources = router
        .clone()
        .oneshot(
            Request::get("/v1/auth/resources")
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(resources.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(resources.into_body(), 16 * 1024).await?)?;
    assert_eq!(body["version"], 1);
    assert_eq!(
        body["resources"][0]["projectId"],
        scope.project_id().to_string()
    );

    let follow_path = format!(
        "/v1/projects/{}/environments/{}/logs/follow?after=logc_0&limit=1",
        scope.project_id(),
        scope.environment_id()
    );
    let follow = router
        .clone()
        .oneshot(
            Request::get(&follow_path)
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(follow.status(), StatusCode::OK);

    let reconcile_path = format!("/v1/auth/managed/operators/{operator_id}/grants");
    let revoke_body = json!({"sourceRevision": 2, "grants": []}).to_string();
    let denied = router
        .clone()
        .oneshot(
            Request::put(&reconcile_path)
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(revoke_body.clone()))?,
        )
        .await?;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let mut duplicate = Request::put(&reconcile_path)
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            "runku-managed-enrollment",
            format!("Bearer {}", "m".repeat(32)),
        )
        .body(Body::from(revoke_body.clone()))?;
    duplicate.headers_mut().append(
        axum::http::HeaderName::from_static("runku-managed-enrollment"),
        axum::http::HeaderValue::from_str(&format!("Bearer {}", "m".repeat(32)))?,
    );
    let duplicate = router.clone().oneshot(duplicate).await?;
    assert_eq!(duplicate.status(), StatusCode::UNAUTHORIZED);

    let reconcile = |body: String| {
        Request::put(&reconcile_path)
            .header(header::CONTENT_TYPE, "application/json")
            .header(
                "runku-managed-enrollment",
                format!("Bearer {}", "m".repeat(32)),
            )
            .body(Body::from(body))
    };
    let revoked = router
        .clone()
        .oneshot(reconcile(revoke_body.clone())?)
        .await?;
    assert_eq!(revoked.status(), StatusCode::OK);
    let revoked: Value = serde_json::from_slice(&to_bytes(revoked.into_body(), 16 * 1024).await?)?;
    assert_eq!(revoked["applied"], true);
    assert_eq!(revoked["replayed"], false);
    assert_eq!(revoked["sourceRevision"], 2);
    assert!(revoked.get("accessToken").is_none());

    let replay = router.clone().oneshot(reconcile(revoke_body)?).await?;
    assert_eq!(replay.status(), StatusCode::OK);
    let replay: Value = serde_json::from_slice(&to_bytes(replay.into_body(), 16 * 1024).await?)?;
    assert_eq!(replay["applied"], false);
    assert_eq!(replay["replayed"], true);

    let divergent = router
        .clone()
        .oneshot(reconcile(
            json!({
                "sourceRevision": 2,
                "grants": [{
                    "role": "observer",
                    "scope": {"kind": "project", "projectId": scope.project_id(), "environmentId": null}
                }]
            })
            .to_string(),
        )?)
        .await?;
    assert_eq!(divergent.status(), StatusCode::CONFLICT);
    let divergent: Value =
        serde_json::from_slice(&to_bytes(divergent.into_body(), 16 * 1024).await?)?;
    assert_eq!(divergent["code"], "PLATFORM_MANAGED_SOURCE_CONFLICT");

    let stale = router
        .clone()
        .oneshot(reconcile(
            json!({"sourceRevision": 1, "grants": []}).to_string(),
        )?)
        .await?;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let stale: Value = serde_json::from_slice(&to_bytes(stale.into_body(), 16 * 1024).await?)?;
    assert_eq!(stale["code"], "PLATFORM_MANAGED_SOURCE_STALE");

    let resources = router
        .clone()
        .oneshot(
            Request::get("/v1/auth/resources")
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    let resources: Value =
        serde_json::from_slice(&to_bytes(resources.into_body(), 16 * 1024).await?)?;
    assert_eq!(resources["resources"], json!([]));
    let followed = to_bytes(follow.into_body(), 16 * 1024).await?;
    assert_eq!(
        followed.as_ref(),
        b"{\"error\":{\"code\":\"PLATFORM_UNAUTHENTICATED\"}}\n"
    );
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn readiness_requires_the_attached_product_store() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("readiness.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let identity = Arc::new(PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([41; 32])),
        SessionTokenPolicy::DEFAULT,
    )?);
    let product = Arc::new(ArchiveStatusProduct {
        scope: EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate()),
        calls: AtomicUsize::new(0),
        healthy: AtomicBool::new(false),
    });
    let router = build_management_router_with_product(
        ManagementHttpConfig {
            max_concurrent_requests: 8,
            exposure: ManagementHttpExposure::LoopbackPlaintext,
            public_management_endpoint: None,
            managed_enrollment_key: None,
            managed_source_authority: None,
        },
        identity,
        None,
        Some(product.clone()),
        None,
    )?;
    let response = router
        .clone()
        .oneshot(Request::get("/health/ready").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    product.healthy.store(true, Ordering::SeqCst);
    let response = router
        .oneshot(Request::get("/health/ready").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    repository.close().await;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn console_product_http_enforces_independent_least_privilege_capabilities()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("data-admin-auth.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let identity = Arc::new(PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([53; 32])),
        SessionTokenPolicy::DEFAULT,
    )?);
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let environment_manager = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "environment-manager".to_owned(),
            },
            OperatorName::from_str("Environment manager")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([PlatformCapability::EnvironmentsManage]),
            }],
            DeviceName::from_str("manager device")?,
            TimestampMicros::new(1_900_000_000_000_000),
        )
        .await?;
    let data_reader = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "data-reader".to_owned(),
            },
            OperatorName::from_str("Data reader")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([PlatformCapability::DataRead]),
            }],
            DeviceName::from_str("reader device")?,
            TimestampMicros::new(1_900_000_000_000_001),
        )
        .await?;
    let environment_reader = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "environment-reader".to_owned(),
            },
            OperatorName::from_str("Environment reader")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([PlatformCapability::EnvironmentsRead]),
            }],
            DeviceName::from_str("environment reader device")?,
            TimestampMicros::new(1_900_000_000_000_004),
        )
        .await?;
    let credential_reader = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "credential-reader".to_owned(),
            },
            OperatorName::from_str("Credential reader")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([PlatformCapability::CredentialsRead]),
            }],
            DeviceName::from_str("credential reader device")?,
            TimestampMicros::new(1_900_000_000_000_002),
        )
        .await?;
    let storage_reader = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "storage-reader".to_owned(),
            },
            OperatorName::from_str("Storage reader")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([PlatformCapability::StorageRead]),
            }],
            DeviceName::from_str("storage reader device")?,
            TimestampMicros::new(1_900_000_000_000_003),
        )
        .await?;
    let storage_manager = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "storage-manager".to_owned(),
            },
            OperatorName::from_str("Storage manager")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([
                    PlatformCapability::StorageRead,
                    PlatformCapability::StorageManage,
                ]),
            }],
            DeviceName::from_str("storage manager device")?,
            TimestampMicros::new(1_900_000_000_000_009),
        )
        .await?;
    let automation_reader = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "automation-reader".to_owned(),
            },
            OperatorName::from_str("Automation reader")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([
                    PlatformCapability::CronRead,
                    PlatformCapability::SchedulesRead,
                ]),
            }],
            DeviceName::from_str("automation reader device")?,
            TimestampMicros::new(1_900_000_000_000_005),
        )
        .await?;
    let automation_manager = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "automation-manager".to_owned(),
            },
            OperatorName::from_str("Automation manager")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([
                    PlatformCapability::CronRead,
                    PlatformCapability::CronActivate,
                ]),
            }],
            DeviceName::from_str("automation manager device")?,
            TimestampMicros::new(1_900_000_000_000_006),
        )
        .await?;
    let usage_reader = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "usage-reader".to_owned(),
            },
            OperatorName::from_str("Usage reader")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([PlatformCapability::UsageRead]),
            }],
            DeviceName::from_str("usage reader device")?,
            TimestampMicros::new(1_900_000_000_000_007),
        )
        .await?;
    let release_reader = identity
        .login_with_managed_external_identity(
            ExternalOperatorIdentity {
                provider_id: "test".to_owned(),
                subject_id: "release-reader".to_owned(),
            },
            OperatorName::from_str("Release reader")?,
            ManagedSourceAuthority::from_str("https://test.runku.example")?,
            1,
            vec![OperatorGrant {
                scope: AccessScope::Environment(scope),
                capabilities: BTreeSet::from([PlatformCapability::ReleasesRead]),
            }],
            DeviceName::from_str("release reader device")?,
            TimestampMicros::new(1_900_000_000_000_008),
        )
        .await?;
    let product = Arc::new(DataProbeProduct {
        scope,
        reads: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        credential_reads: AtomicUsize::new(0),
        storage_reads: AtomicUsize::new(0),
        storage_writes: AtomicUsize::new(0),
        environment_reads: AtomicUsize::new(0),
        cron_reads: AtomicUsize::new(0),
        cron_writes: AtomicUsize::new(0),
        scheduled_reads: AtomicUsize::new(0),
        metrics_reads: AtomicUsize::new(0),
        instance_health_reads: AtomicUsize::new(0),
        compatibility_reads: AtomicUsize::new(0),
        environment_archives: AtomicUsize::new(0),
        environment_restores: AtomicUsize::new(0),
    });
    let router = build_management_router_with_product(
        ManagementHttpConfig {
            max_concurrent_requests: 8,
            exposure: ManagementHttpExposure::LoopbackPlaintext,
            public_management_endpoint: None,
            managed_enrollment_key: None,
            managed_source_authority: None,
        },
        identity,
        None,
        Some(product.clone()),
        None,
    )?;
    let document_path = format!(
        "/v1/projects/{}/environments/{}/data/documents/notes/doc_test?target=workspace:local",
        scope.project_id(),
        scope.environment_id()
    );
    let insert_path = format!(
        "/v1/projects/{}/environments/{}/data/documents/notes",
        scope.project_id(),
        scope.environment_id()
    );
    let credential_path = format!(
        "/v1/projects/{}/environments/{}/application-clients",
        scope.project_id(),
        scope.environment_id()
    );
    let storage_path = format!(
        "/v1/projects/{}/environments/{}/buckets?limit=10",
        scope.project_id(),
        scope.environment_id()
    );
    let object_list_path = format!(
        "/v1/projects/{}/environments/{}/buckets/bkt_00000000000000000000000000/objects?prefix=docs%2F&delimiter=%2F&limit=10",
        scope.project_id(),
        scope.environment_id()
    );
    let object_path = format!(
        "/v1/projects/{}/environments/{}/buckets/bkt_00000000000000000000000000/objects/docs/readme.txt",
        scope.project_id(),
        scope.environment_id()
    );
    let environment_path = format!(
        "/v1/projects/{}/environments/{}",
        scope.project_id(),
        scope.environment_id()
    );
    let environment_archive_path = format!("{environment_path}/archive");
    let environment_restore_path = format!("{environment_path}/restore");
    let cron_path = format!(
        "/v1/projects/{}/environments/{}/crons?target=workspace%3Alocal",
        scope.project_id(),
        scope.environment_id()
    );
    let scheduled_path = format!(
        "/v1/projects/{}/environments/{}/scheduled?limit=10",
        scope.project_id(),
        scope.environment_id()
    );
    let cron_activation_path = format!(
        "/v1/projects/{}/environments/{}/crons/crons.hourly/activation",
        scope.project_id(),
        scope.environment_id()
    );
    let metrics_path = format!(
        "/v1/projects/{}/environments/{}/metrics",
        scope.project_id(),
        scope.environment_id()
    );
    let instance_health_path = format!(
        "/v1/projects/{}/environments/{}/instances/healthz",
        scope.project_id(),
        scope.environment_id()
    );
    let compatibility_path = format!(
        "/v1/projects/{}/environments/{}/schemas/compatibility",
        scope.project_id(),
        scope.environment_id()
    );
    let insert = |access: &str| {
        Request::post(&insert_path)
            .header(header::AUTHORIZATION, format!("Bearer {access}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", OperationId::generate().to_string())
            .body(Body::from(
                json!({"target":"workspace:local", "value":{"type":"null"}}).to_string(),
            ))
    };
    let lifecycle = |path: &str, access: &str, revision: u64| {
        Request::post(path)
            .header(header::AUTHORIZATION, format!("Bearer {access}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", OperationId::generate().to_string())
            .body(Body::from(
                json!({
                    "expectedRevision": revision,
                    "changedAtMicros": "1900000000000009"
                })
                .to_string(),
            ))
    };

    let manager_access = environment_manager.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::get(&document_path)
                .header(header::AUTHORIZATION, format!("Bearer {manager_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = router.clone().oneshot(insert(manager_access)?).await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.reads.load(Ordering::SeqCst), 0);
    assert_eq!(product.writes.load(Ordering::SeqCst), 0);
    let response = router
        .clone()
        .oneshot(
            Request::get(&credential_path)
                .header(header::AUTHORIZATION, format!("Bearer {manager_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.credential_reads.load(Ordering::SeqCst), 0);
    let response = router
        .clone()
        .oneshot(
            Request::get(&storage_path)
                .header(header::AUTHORIZATION, format!("Bearer {manager_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.storage_reads.load(Ordering::SeqCst), 0);
    let response = router
        .clone()
        .oneshot(
            Request::get(&environment_path)
                .header(header::AUTHORIZATION, format!("Bearer {manager_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.environment_reads.load(Ordering::SeqCst), 0);
    let response = router
        .clone()
        .oneshot(lifecycle(&environment_archive_path, manager_access, 1)?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.environment_archives.load(Ordering::SeqCst), 1);
    let response = router
        .clone()
        .oneshot(lifecycle(&environment_restore_path, manager_access, 2)?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.environment_restores.load(Ordering::SeqCst), 1);
    let response = router
        .clone()
        .oneshot(
            Request::get(&metrics_path)
                .header(header::AUTHORIZATION, format!("Bearer {manager_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.metrics_reads.load(Ordering::SeqCst), 0);

    let reader_access = data_reader.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::get(&document_path)
                .header(header::AUTHORIZATION, format!("Bearer {reader_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(product.reads.load(Ordering::SeqCst), 1);
    let response = router.clone().oneshot(insert(reader_access)?).await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.writes.load(Ordering::SeqCst), 0);

    let credential_access = credential_reader.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::get(&credential_path)
                .header(header::AUTHORIZATION, format!("Bearer {credential_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.credential_reads.load(Ordering::SeqCst), 1);

    let storage_access = storage_reader.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::get(&storage_path)
                .header(header::AUTHORIZATION, format!("Bearer {storage_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.storage_reads.load(Ordering::SeqCst), 1);
    let response = router
        .clone()
        .oneshot(
            Request::get(&object_list_path)
                .header(header::AUTHORIZATION, format!("Bearer {storage_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.storage_reads.load(Ordering::SeqCst), 2);
    let response = router
        .clone()
        .oneshot(
            Request::put(&object_path)
                .header(header::AUTHORIZATION, format!("Bearer {storage_access}"))
                .header(header::CONTENT_TYPE, "text/plain")
                .header("idempotency-key", OperationId::generate().to_string())
                .header("x-runku-at-micros", "1900000000000010")
                .body(Body::from("denied"))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    assert_eq!(product.storage_writes.load(Ordering::SeqCst), 0);
    let storage_manager_access = storage_manager.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::put(&object_path)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {storage_manager_access}"),
                )
                .header(header::CONTENT_TYPE, "text/plain")
                .header("idempotency-key", OperationId::generate().to_string())
                .header("x-runku-at-micros", "1900000000000010")
                .header("x-runku-meta-cache-control", "private")
                .body(Body::from("object bytes"))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.storage_writes.load(Ordering::SeqCst), 1);
    let response = router
        .clone()
        .oneshot(
            Request::get(&object_path)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {storage_manager_access}"),
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(format!("\"{}\"", "a".repeat(64)).as_str())
    );
    assert_eq!(
        response
            .headers()
            .get("x-runku-object-version")
            .and_then(|value| value.to_str().ok()),
        Some("ovr_00000000000000000000000001")
    );
    assert_eq!(to_bytes(response.into_body(), 1024).await?, "object bytes");

    let environment_access = environment_reader.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::get(&environment_path)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {environment_access}"),
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.environment_reads.load(Ordering::SeqCst), 1);
    let response = router
        .clone()
        .oneshot(lifecycle(&environment_archive_path, environment_access, 1)?)
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.environment_archives.load(Ordering::SeqCst), 1);
    let response = router
        .clone()
        .oneshot(
            Request::get(&instance_health_path)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {environment_access}"),
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.instance_health_reads.load(Ordering::SeqCst), 1);

    let release_access = release_reader.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::get(&compatibility_path)
                .header(header::AUTHORIZATION, format!("Bearer {release_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.compatibility_reads.load(Ordering::SeqCst), 1);
    let response = router
        .clone()
        .oneshot(
            Request::get(&compatibility_path)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {environment_access}"),
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.compatibility_reads.load(Ordering::SeqCst), 1);

    let usage_access = usage_reader.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::get(&metrics_path)
                .header(header::AUTHORIZATION, format!("Bearer {usage_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.metrics_reads.load(Ordering::SeqCst), 1);
    let response = router
        .clone()
        .oneshot(
            Request::get(&instance_health_path)
                .header(header::AUTHORIZATION, format!("Bearer {usage_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.instance_health_reads.load(Ordering::SeqCst), 1);

    let automation_access = automation_reader.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(
            Request::get(&cron_path)
                .header(header::AUTHORIZATION, format!("Bearer {automation_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let activation = |access: &str| {
        Request::put(&cron_activation_path)
            .header(header::AUTHORIZATION, format!("Bearer {access}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", OperationId::generate().to_string())
            .body(Body::from(
                json!({
                    "target":"workspace:local",
                    "expectedRevision":0,
                    "enabled":false,
                    "changedAtMicros":"1900000000000007"
                })
                .to_string(),
            ))
    };
    let response = router
        .clone()
        .oneshot(activation(automation_access)?)
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(product.cron_writes.load(Ordering::SeqCst), 0);
    let automation_manager_access = automation_manager.login.access_token.expose();
    let response = router
        .clone()
        .oneshot(activation(automation_manager_access)?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.cron_writes.load(Ordering::SeqCst), 1);
    let response = router
        .oneshot(
            Request::get(&scheduled_path)
                .header(header::AUTHORIZATION, format!("Bearer {automation_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(product.cron_reads.load(Ordering::SeqCst), 1);
    assert_eq!(product.scheduled_reads.load(Ordering::SeqCst), 1);

    repository.close().await;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn bootstrap_exchange_returns_no_store_session_usable_for_me()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("management.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let identity = Arc::new(PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([31; 32])),
        SessionTokenPolicy::DEFAULT,
    )?);
    let bootstrap = match identity
        .initialize_bootstrap(
            OperatorName::from_str("Initial owner")?,
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?
    {
        BootstrapResult::Created(generated) => generated,
        BootstrapResult::Replayed | BootstrapResult::Complete => {
            return Err("fresh database did not create bootstrap".into());
        }
    };
    let router = build_management_router(
        ManagementHttpConfig {
            max_concurrent_requests: 8,
            exposure: ManagementHttpExposure::LoopbackPlaintext,
            public_management_endpoint: None,
            managed_enrollment_key: None,
            managed_source_authority: None,
        },
        identity.clone(),
        None,
    )?;
    let response = router
        .clone()
        .oneshot(Request::get("/v1/auth/config").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(body, json!({"version": 1, "methods": ["invitationCode"]}));

    let oidc_router = build_management_router_with_product(
        ManagementHttpConfig {
            max_concurrent_requests: 8,
            exposure: ManagementHttpExposure::LoopbackPlaintext,
            public_management_endpoint: Some("https://api.runku.example".to_owned()),
            managed_enrollment_key: None,
            managed_source_authority: None,
        },
        identity.clone(),
        Some(Arc::new(RejectingExternalIdentity)),
        None,
        Some(OidcClientConfiguration {
            issuer: "https://identity.runku.example".to_owned(),
            authorization_endpoint: "https://identity.runku.example/authorize".to_owned(),
            token_endpoint: "https://identity.runku.example/token".to_owned(),
            client_id: "runku-cli".to_owned(),
            scopes: vec!["openid".to_owned(), "profile".to_owned()],
            resource: None,
        }),
    )?;
    let response = oidc_router
        .clone()
        .oneshot(Request::get("/v1/auth/config").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(
        body,
        json!({
            "version": 1,
            "methods": ["oidcBrowser", "invitationCode", "oidcToken"],
            "managementEndpoint": "https://api.runku.example"
        })
    );
    let response = router
        .clone()
        .oneshot(
            Request::post("/v1/auth/exchange")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "code": bootstrap.code.expose(),
                        "deviceName": "test-device"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store, max-age=0")
    );
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    let access = body["accessToken"]
        .as_str()
        .ok_or("missing access token")?
        .to_owned();
    let session_id = body["sessionId"]
        .as_str()
        .ok_or("missing session id")?
        .to_owned();
    assert!(access.starts_with("rk_at_v1_"));
    assert!(
        body["refreshToken"]
            .as_str()
            .is_some_and(|value| value.starts_with("rk_rt_v1_"))
    );

    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let product = Arc::new(ArchiveStatusProduct {
        scope,
        calls: AtomicUsize::new(0),
        healthy: AtomicBool::new(true),
    });
    let product_router = build_management_router_with_product(
        ManagementHttpConfig {
            max_concurrent_requests: 8,
            exposure: ManagementHttpExposure::LoopbackPlaintext,
            public_management_endpoint: None,
            managed_enrollment_key: None,
            managed_source_authority: None,
        },
        identity.clone(),
        None,
        Some(product.clone()),
        None,
    )?;
    let archive_path = format!(
        "/v1/projects/{}/environments/{}/logs/archive-status",
        scope.project_id(),
        scope.environment_id()
    );
    let response = product_router
        .clone()
        .oneshot(Request::get(&archive_path).body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(product.calls.load(Ordering::SeqCst), 0);

    let response = product_router
        .clone()
        .oneshot(
            Request::get(format!(
                "/v1/projects/{}/environments/{}/logs/archive-status",
                scope.project_id(),
                EnvironmentId::generate()
            ))
            .header(header::AUTHORIZATION, format!("Bearer {access}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(product.calls.load(Ordering::SeqCst), 0);

    let response = product_router
        .clone()
        .oneshot(
            Request::get(&archive_path)
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(
        body,
        json!({
            "parquetBytes": 4096,
            "records": 12,
            "segments": 2,
            "through": "logc_12"
        })
    );
    assert_eq!(product.calls.load(Ordering::SeqCst), 1);

    let prune_path = format!(
        "/v1/projects/{}/environments/{}/logs/prune",
        scope.project_id(),
        scope.environment_id()
    );
    let response = product_router
        .oneshot(
            Request::post(prune_path)
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "beforeMicros": 1_800_000_000_000_000_i64,
                        "maximum": 100,
                        "apply": false,
                        "environmentId": null
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(body["applied"], false);
    assert_eq!(body["environmentId"], scope.environment_id().to_string());
    assert_eq!(product.calls.load(Ordering::SeqCst), 2);

    let response = router
        .clone()
        .oneshot(
            Request::get("/v1/auth/me")
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(body["name"], "Initial owner");
    assert_eq!(body["deviceName"], "test-device");

    for request in [
        Request::get("/v1/auth/me")
            .header(header::AUTHORIZATION, format!("Bearer {access}"))
            .header(header::AUTHORIZATION, "Bearer injected")
            .body(Body::empty())?,
        Request::get("/v1/auth/me")
            .header(header::AUTHORIZATION, format!("Bearer {access} injected"))
            .body(Body::empty())?,
        Request::get("/v1/auth/me")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {access}, Bearer injected"),
            )
            .body(Body::empty())?,
        Request::get("/v1/auth/me")
            .header(header::AUTHORIZATION, format!("bearer {access}"))
            .body(Body::empty())?,
    ] {
        let response = router.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let response = router
        .clone()
        .oneshot(
            Request::post("/v1/auth/exchange")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "code": format!("{}\nInjected", bootstrap.code.expose()),
                        "deviceName": "injected-device"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = router
        .clone()
        .oneshot(
            Request::get("/v1/auth/sessions")
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(body["sessions"][0]["sessionId"], session_id);
    assert_eq!(body["sessions"][0]["status"], "active");

    let response = router
        .clone()
        .oneshot(
            Request::delete(format!("/v1/auth/sessions/{session_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = router
        .oneshot(
            Request::get("/v1/auth/me")
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    repository.close().await;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn invitation_operation_is_reconcilable_conflict_safe_and_revocable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("invitation-http.sqlite3");
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", database.display()),
            PlatformIdentityRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let identity = Arc::new(PlatformIdentityService::new(
        repository.clone(),
        Arc::new(PlatformIdentityCrypto::new([47; 32])),
        SessionTokenPolicy::DEFAULT,
    )?);
    let bootstrap = match identity
        .initialize_bootstrap(
            OperatorName::from_str("Initial owner")?,
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?
    {
        BootstrapResult::Created(generated) => generated,
        BootstrapResult::Replayed | BootstrapResult::Complete => {
            return Err("fresh database did not create bootstrap".into());
        }
    };
    let router = build_management_router(
        ManagementHttpConfig {
            max_concurrent_requests: 8,
            exposure: ManagementHttpExposure::LoopbackPlaintext,
            public_management_endpoint: None,
            managed_enrollment_key: None,
            managed_source_authority: None,
        },
        identity,
        None,
    )?;
    let response = router
        .clone()
        .oneshot(
            Request::post("/v1/auth/exchange")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "code": bootstrap.code.expose(),
                        "deviceName": "owner-device"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    let access = body["accessToken"]
        .as_str()
        .ok_or("missing access token")?
        .to_owned();
    let operation = OperationId::generate();
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let request = json!({
        "operatorName": "Cloud operator",
        "role": "observer",
        "scope": {
            "kind": "environment",
            "projectId": scope.project_id(),
            "environmentId": scope.environment_id()
        }
    });
    let issue = || {
        Request::post("/v1/access/invitations")
            .header(header::AUTHORIZATION, format!("Bearer {access}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", operation.to_string())
            .body(Body::from(request.to_string()))
    };

    let response = router.clone().oneshot(issue()?).await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store, max-age=0")
    );
    let created: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    let invitation_id = created["invitationId"]
        .as_str()
        .ok_or("missing invitation id")?
        .to_owned();
    let code = created["code"]
        .as_str()
        .ok_or("missing one-time code")?
        .to_owned();
    assert_eq!(created["operationId"], operation.to_string());
    assert_eq!(created["secretShownOnce"], true);
    assert_eq!(created["replayed"], false);

    let response = router.clone().oneshot(issue()?).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let replayed: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(replayed["invitationId"], invitation_id);
    assert_eq!(replayed["secretShownOnce"], false);
    assert_eq!(replayed["replayed"], true);
    assert!(replayed.get("code").is_none());

    let response = router
        .clone()
        .oneshot(
            Request::get(format!("/v1/access/invitation-operations/{operation}"))
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let reconciled: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(reconciled["invitationId"], invitation_id);
    assert!(reconciled.get("code").is_none());

    let changed = json!({
        "operatorName": "Different request",
        "role": "observer",
        "scope": {
            "kind": "environment",
            "projectId": scope.project_id(),
            "environmentId": scope.environment_id()
        }
    });
    let response = router
        .clone()
        .oneshot(
            Request::post("/v1/access/invitations")
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", operation.to_string())
                .body(Body::from(changed.to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let conflict: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(conflict["code"], "PLATFORM_INVITATION_OPERATION_REUSED");

    for _ in 0..2 {
        let response = router
            .clone()
            .oneshot(
                Request::delete(format!("/v1/access/invitations/{invitation_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {access}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let response = router
        .clone()
        .oneshot(
            Request::post("/v1/auth/exchange")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"code": code, "deviceName": "revoked"}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = router
        .oneshot(
            Request::post("/v1/access/invitations")
                .header(header::AUTHORIZATION, format!("Bearer {access}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", OperationId::generate().to_string())
                .header("idempotency-key", OperationId::generate().to_string())
                .body(Body::from(request.to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    repository.close().await;
    Ok(())
}
