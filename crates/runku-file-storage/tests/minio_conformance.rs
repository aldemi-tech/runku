//! Opt-in S3-compatible conformance suite executed by the file-storage evidence script.

use std::{
    error::Error,
    fmt::Write as _,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::StreamExt;
use runku_core::{EnvironmentId, EnvironmentScope, ProjectId};
use runku_file_storage::{
    FileObjectStore, FileStorageLimits, FileStorageService, S3FileCredentials,
    S3FileStaticCredentials, S3FileStoreConfig,
};
use runku_runtime::{
    CancellationToken, FileDownloadGrantRequest, FileStorage, FileUploadGrantRequest,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

fn s3_store() -> Result<Option<FileObjectStore>, Box<dyn Error>> {
    let Ok(endpoint) = std::env::var("RUNKU_TEST_S3_ENDPOINT") else {
        return Ok(None);
    };
    let bucket = std::env::var("RUNKU_TEST_FILE_S3_BUCKET")?;
    let access = std::env::var("RUNKU_TEST_S3_ACCESS_KEY")?;
    let secret = std::env::var("RUNKU_TEST_S3_SECRET_KEY")?;
    let mut config = S3FileStoreConfig::new(bucket, "us-east-1");
    config.endpoint = Some(endpoint);
    config.prefix = format!("conformance-{}", std::process::id());
    config.allow_loopback_http = true;
    config.credentials = S3FileCredentials::Static(S3FileStaticCredentials::new(access, secret));
    Ok(Some(FileObjectStore::s3(&config)?))
}

#[tokio::test]
async fn minio_streams_multipart_range_integrity_and_delete() -> Result<(), Box<dyn Error>> {
    let Some(objects) = s3_store()? else {
        eprintln!("skipped: RUNKU_TEST_S3_ENDPOINT is not configured");
        return Ok(());
    };
    let temporary = TempDir::new()?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let service = FileStorageService::open_sqlite(
        scope,
        &temporary.path().join("files.sqlite3"),
        objects,
        [41; 32],
        FileStorageLimits {
            environment_bytes: 16 * 1024 * 1024,
            file_bytes: 8 * 1024 * 1024,
            action_bytes: 1024,
            filesystem_minimum_free_bytes: 0,
            ..FileStorageLimits::DEFAULT
        },
    )
    .await?;
    let bytes = vec![0x5a; 6 * 1024 * 1024 + 17];
    let digest = Sha256::digest(&bytes);
    let mut checksum = String::with_capacity(64);
    for byte in digest {
        write!(&mut checksum, "{byte:02x}")?;
    }
    let grant = service
        .create_upload_grant(
            FileUploadGrantRequest {
                max_bytes: u64::try_from(bytes.len())?,
                content_type: Some("application/octet-stream".to_owned()),
                sha256: Some(checksum.clone()),
            },
            deadline(),
            CancellationToken::new(),
        )
        .await?;
    let chunks = bytes
        .chunks(256 * 1024)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect::<Vec<_>>();
    let metadata = service
        .upload_http(
            &grant.upload_id,
            &grant.token,
            Some(u64::try_from(bytes.len())?),
            Some("application/octet-stream"),
            Box::pin(futures_util::stream::iter(chunks)),
            deadline(),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(metadata.sha256, checksum);
    let download = service
        .create_download_grant(
            FileDownloadGrantRequest {
                file_id: metadata.file_id.clone(),
                expires_in_micros: "10000000".to_owned(),
            },
            deadline(),
            CancellationToken::new(),
        )
        .await?;
    let opened = service
        .download_http(
            &metadata.file_id,
            &download.token,
            Some(5_000_000..5_000_123),
            deadline(),
            CancellationToken::new(),
        )
        .await?;
    let chunks = opened.stream.collect::<Vec<_>>().await;
    assert_eq!(
        chunks.into_iter().collect::<Result<Vec<_>, _>>()?.concat(),
        bytes[5_000_000..5_000_123]
    );
    service
        .delete(
            metadata.file_id.clone(),
            deadline(),
            CancellationToken::new(),
        )
        .await?;
    assert!(
        service
            .download_http(
                &metadata.file_id,
                &download.token,
                None,
                deadline(),
                CancellationToken::new(),
            )
            .await
            .is_err()
    );
    Ok(())
}
