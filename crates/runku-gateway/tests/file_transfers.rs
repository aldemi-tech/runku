//! Black-box HTTP transfer tests over the filesystem adapter.

use std::{
    collections::BTreeSet,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use runku_core::{EnvironmentId, EnvironmentScope, ProjectId, ReleaseId};
use runku_file_storage::{FileObjectStore, FileStorageLimits, FileStorageService};
use runku_gateway::{
    CorsOrigin, GatewayFailure, GatewayHttpConfig, GatewaySuccess, InvocationContext,
    InvocationService, InvokeCallV1, build_router_with_files,
};
use runku_protocol::SuccessMetadataV1;
use runku_runtime::{
    CancellationToken, FileDownloadGrantRequest, FileStorage, FileUploadGrantRequest,
};
use runku_value::CanonicalValue;
use tempfile::TempDir;
use tower::ServiceExt;

#[derive(Debug)]
struct UnusedInvocationService;

#[async_trait]
impl InvocationService for UnusedInvocationService {
    async fn invoke(
        &self,
        _context: InvocationContext,
        call: InvokeCallV1,
    ) -> Result<GatewaySuccess, GatewayFailure> {
        let metadata = match call {
            InvokeCallV1::Query(_) => SuccessMetadataV1::Query {
                snapshot_sequence: None,
            },
            InvokeCallV1::Mutation(_) => SuccessMetadataV1::Mutation {
                commit_sequence: None,
                replayed: false,
                attempts: 1,
            },
            InvokeCallV1::Action(_) => SuccessMetadataV1::Action {
                schedules_created: 0,
            },
        };
        Ok(GatewaySuccess {
            release_id: ReleaseId::generate(),
            value: CanonicalValue::Null,
            metadata,
        })
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn grants_stream_upload_download_range_and_fail_closed() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let objects = FileObjectStore::filesystem(&temporary.path().join("objects")).await?;
    let files = Arc::new(
        FileStorageService::open_sqlite(
            scope,
            &temporary.path().join("files.sqlite3"),
            objects,
            [19; 32],
            FileStorageLimits {
                environment_bytes: 32,
                file_bytes: 16,
                action_bytes: 16,
                filesystem_minimum_free_bytes: 0,
                ..FileStorageLimits::DEFAULT
            },
        )
        .await?,
    );
    let router = build_router_with_files(
        GatewayHttpConfig {
            allowed_origins: BTreeSet::from(["https://app.example".parse::<CorsOrigin>()?]),
            max_concurrent_requests: 8,
            request_timeout: Duration::from_secs(5),
        },
        Arc::new(UnusedInvocationService),
        Arc::clone(&files),
    )?;
    let grant = files
        .create_upload_grant(
            FileUploadGrantRequest {
                max_bytes: 6,
                content_type: Some("text/plain".to_owned()),
                sha256: None,
            },
            deadline(),
            CancellationToken::new(),
        )
        .await?;
    let upload = Request::builder()
        .method("PUT")
        .uri(&grant.path)
        .header(header::AUTHORIZATION, format!("Bearer {}", grant.token))
        .header(header::CONTENT_TYPE, "text/plain")
        .header(header::CONTENT_LENGTH, "6")
        .header(header::ORIGIN, "https://app.example")
        .body(Body::from("abcdef"))?;
    let response = router.clone().oneshot(upload).await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    let file_id = body["file"]["fileId"]
        .as_str()
        .ok_or("missing file ID")?
        .to_owned();

    let replay = Request::builder()
        .method("PUT")
        .uri(&grant.path)
        .header(header::AUTHORIZATION, format!("Bearer {}", grant.token))
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("abcdef"))?;
    assert_eq!(
        router.clone().oneshot(replay).await?.status(),
        StatusCode::CONFLICT
    );

    let download = files
        .create_download_grant(
            FileDownloadGrantRequest {
                file_id: file_id.clone(),
                expires_in_micros: "1000000".to_owned(),
            },
            deadline(),
            CancellationToken::new(),
        )
        .await?;
    let range = Request::builder()
        .method("GET")
        .uri(&download.path)
        .header(header::AUTHORIZATION, format!("Bearer {}", download.token))
        .header(header::RANGE, "bytes=1-3")
        .header(header::ORIGIN, "https://app.example")
        .body(Body::empty())?;
    let response = router.clone().oneshot(range).await?;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("content-range"))
    );
    assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 1-3/6");
    assert_eq!(to_bytes(response.into_body(), 16).await?.as_ref(), b"bcd");

    let denied = Request::builder()
        .method("GET")
        .uri(&download.path)
        .header(header::AUTHORIZATION, "Bearer changed")
        .body(Body::empty())?;
    assert_eq!(
        router.clone().oneshot(denied).await?.status(),
        StatusCode::FORBIDDEN
    );

    let wrong_origin = Request::builder()
        .method("GET")
        .uri(&download.path)
        .header(header::AUTHORIZATION, format!("Bearer {}", download.token))
        .header(header::ORIGIN, "https://evil.example")
        .body(Body::empty())?;
    assert_eq!(
        router.oneshot(wrong_origin).await?.status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}
