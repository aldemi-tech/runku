//! Opt-in S3 conformance suite executed by the infrastructure evidence script.

use runku_artifact_s3::{
    S3ArtifactStore, S3ArtifactStoreConfig, S3Credentials, S3StaticCredentials,
};
use runku_releases::{
    ArtifactDescriptor, ArtifactFormat, ArtifactStore, ReleaseError, Sha256Digest,
};

fn store() -> Result<Option<S3ArtifactStore>, ReleaseError> {
    let Ok(endpoint) = std::env::var("RUNKU_TEST_S3_ENDPOINT") else {
        return Ok(None);
    };
    let bucket = std::env::var("RUNKU_TEST_S3_BUCKET").map_err(|_| ReleaseError::Unavailable)?;
    let access_key =
        std::env::var("RUNKU_TEST_S3_ACCESS_KEY").map_err(|_| ReleaseError::Unavailable)?;
    let secret_key =
        std::env::var("RUNKU_TEST_S3_SECRET_KEY").map_err(|_| ReleaseError::Unavailable)?;
    let mut config = S3ArtifactStoreConfig::new(bucket, "us-east-1");
    config.endpoint = Some(endpoint);
    config.prefix = format!("conformance-{}", std::process::id());
    config.allow_http = true;
    config.credentials = S3Credentials::Static(S3StaticCredentials::new(access_key, secret_key));
    S3ArtifactStore::open(&config).map(Some)
}

fn descriptor(bytes: &[u8]) -> ArtifactDescriptor {
    ArtifactDescriptor {
        format: ArtifactFormat::NodeEsmBundleV1,
        digest: Sha256Digest::of(bytes),
        size_bytes: u64::try_from(bytes.len()).unwrap_or(0),
    }
}

#[tokio::test]
async fn minio_is_content_addressed_idempotent_and_verified()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(store) = store()? else {
        eprintln!("skipped: RUNKU_TEST_S3_ENDPOINT is not configured");
        return Ok(());
    };
    let bytes = b"immutable-runku-artifact";
    let expected = descriptor(bytes);

    store.put(&expected, bytes).await?;
    store.put(&expected, bytes).await?;
    assert_eq!(store.get(&expected).await?, bytes);

    let absent_bytes = b"absent-artifact";
    let absent = descriptor(absent_bytes);
    assert_eq!(store.get(&absent).await, Err(ReleaseError::NotFound));

    let mismatch = ArtifactDescriptor {
        size_bytes: expected.size_bytes + 1,
        ..expected
    };
    assert_eq!(
        store.put(&mismatch, bytes).await,
        Err(ReleaseError::DescriptorMismatch)
    );
    Ok(())
}
