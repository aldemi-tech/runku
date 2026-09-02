//! Management HTTP contract coverage for bootstrap exchange and authenticated identity.

use std::{str::FromStr as _, sync::Arc};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use runku_management_service::{
    ExternalIdentityAuthenticator, ManagementHttpConfig, ManagementHttpExposure,
    OidcClientConfiguration, build_management_router, build_management_router_with_product,
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
