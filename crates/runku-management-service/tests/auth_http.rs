//! Management HTTP contract coverage for bootstrap exchange and authenticated identity.

use std::{
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
    ExternalIdentityAuthenticator, ManagedEnrollmentKey, ManagementHttpConfig,
    ManagementHttpExposure, ManagementLogArchiveStatus, ManagementLogPage,
    ManagementLogPruneRequest, ManagementLogPruneResult, ManagementLogQuery, ManagementProduct,
    ManagementProductError, ManagementReleaseOutcome, ManagementReleaseStatus,
    ManagementWorkspacePublish, OidcClientConfiguration, build_management_router,
    build_management_router_with_product,
};
use runku_platform_identity::{
    BootstrapResult, ExternalOperatorIdentity, OperatorName, PlatformIdentityCrypto,
    PlatformIdentityError, PlatformIdentityRepository, PlatformIdentityRepositoryConfig,
    PlatformIdentityService, SessionTokenPolicy, SqlPlatformIdentityRepository,
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
        Err(ManagementProductError::Invalid)
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
    let access = login["accessToken"]
        .as_str()
        .ok_or("missing access token")?;
    let resources = router
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
