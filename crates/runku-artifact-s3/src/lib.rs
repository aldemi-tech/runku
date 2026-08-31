//! S3-compatible immutable artifact storage for distributed Runku deployments.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{fmt, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, aws::AmazonS3Builder, path::Path,
};
use runku_releases::{
    ARTIFACT_MAX_BYTES, ArtifactDescriptor, ArtifactStore, FilesystemArtifactStore,
    FilesystemStoreRole, ReleaseError, Sha256Digest,
};
use url::Url;

const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_PREFIX_BYTES: usize = 256;

/// Startup-only artifact backend selection for a server composition.
#[derive(Debug)]
pub enum ServerArtifactStoreConfig {
    /// Persistent filesystem backend for exactly one self-hosted server node.
    FilesystemSingleNode {
        /// Private persistent artifact root.
        root: PathBuf,
    },
    /// Shared S3-compatible backend for distributed self-hosted or `SaaS` deployments.
    S3Compatible(S3ArtifactStoreConfig),
}

impl ServerArtifactStoreConfig {
    /// Opens the selected backend behind the common immutable storage boundary.
    ///
    /// # Errors
    ///
    /// Returns a sanitized validation or backend availability error.
    pub async fn open(self) -> Result<Arc<dyn ArtifactStore>, ReleaseError> {
        match self {
            Self::FilesystemSingleNode { root } => {
                FilesystemArtifactStore::open(root, FilesystemStoreRole::SingleNodeServer)
                    .await
                    .map(|store| Arc::new(store) as Arc<dyn ArtifactStore>)
            }
            Self::S3Compatible(config) => S3ArtifactStore::open(&config)
                .map(|store| Arc::new(store) as Arc<dyn ArtifactStore>),
        }
    }
}

/// Credentials used by the S3-compatible artifact adapter.
pub enum S3Credentials {
    /// Let the AWS credential chain read environment, workload identity, or instance metadata.
    Environment,
    /// Use explicit credentials, primarily for `MinIO` and other self-hosted endpoints.
    Static(S3StaticCredentials),
}

impl fmt::Debug for S3Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Environment => formatter.write_str("Environment"),
            Self::Static(_) => formatter.write_str("Static([REDACTED])"),
        }
    }
}

/// Explicit S3 credentials whose debug representation never reveals secret values.
pub struct S3StaticCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl fmt::Debug for S3StaticCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("S3StaticCredentials([REDACTED])")
    }
}

impl S3StaticCredentials {
    /// Creates explicit credentials. Empty identifiers or secrets are rejected when opening the store.
    #[must_use]
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
        }
    }

    /// Adds an optional short-lived session token.
    #[must_use]
    pub fn with_session_token(mut self, session_token: impl Into<String>) -> Self {
        self.session_token = Some(session_token.into());
        self
    }
}

/// Validated configuration for an S3-compatible immutable artifact bucket.
#[derive(Debug)]
pub struct S3ArtifactStoreConfig {
    /// Bucket containing immutable artifacts.
    pub bucket: String,
    /// Signing region. `MinIO` commonly uses `us-east-1`.
    pub region: String,
    /// Optional S3-compatible endpoint, such as R2 or `MinIO`.
    pub endpoint: Option<String>,
    /// Optional namespace below the bucket.
    pub prefix: String,
    /// Whether bucket names are placed in the URL host instead of the path.
    pub virtual_hosted_style: bool,
    /// Explicit opt-in for clear-text HTTP, intended only for trusted local networks and tests.
    pub allow_http: bool,
    /// Maximum duration of one backend operation.
    pub operation_timeout: Duration,
    /// Credential source.
    pub credentials: S3Credentials,
}

impl S3ArtifactStoreConfig {
    /// Creates conservative production defaults for the given bucket and region.
    #[must_use]
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            prefix: "runku-artifacts".to_owned(),
            virtual_hosted_style: false,
            allow_http: false,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            credentials: S3Credentials::Environment,
        }
    }
}

/// Content-addressed S3-compatible artifact store.
#[derive(Clone)]
pub struct S3ArtifactStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    operation_timeout: Duration,
}

impl fmt::Debug for S3ArtifactStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3ArtifactStore")
            .field("prefix", &self.prefix)
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl S3ArtifactStore {
    /// Opens an S3-compatible store after validating all security-sensitive configuration.
    ///
    /// # Errors
    ///
    /// Returns a sanitized configuration or availability error.
    pub fn open(config: &S3ArtifactStoreConfig) -> Result<Self, ReleaseError> {
        validate_config(config)?;
        let mut builder = match &config.credentials {
            S3Credentials::Environment => AmazonS3Builder::from_env(),
            S3Credentials::Static(credentials) => {
                let mut builder = AmazonS3Builder::new()
                    .with_access_key_id(credentials.access_key_id.clone())
                    .with_secret_access_key(credentials.secret_access_key.clone());
                if let Some(token) = &credentials.session_token {
                    builder = builder.with_token(token.clone());
                }
                builder
            }
        }
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_virtual_hosted_style_request(config.virtual_hosted_style)
        .with_allow_http(config.allow_http);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        let store = builder.build().map_err(|_| ReleaseError::Unavailable)?;
        Ok(Self {
            store: Arc::new(store),
            prefix: config.prefix.trim_matches('/').to_owned(),
            operation_timeout: config.operation_timeout,
        })
    }

    fn path_for(&self, digest: Sha256Digest) -> Path {
        let digest = digest.to_string();
        let relative = format!(
            "v1/sha256/{}/{}/{}.artifact",
            &digest[..2],
            &digest[2..4],
            &digest[4..]
        );
        let key = if self.prefix.is_empty() {
            relative
        } else {
            format!("{}/{relative}", self.prefix)
        };
        Path::from(key)
    }

    async fn get_verified(&self, descriptor: &ArtifactDescriptor) -> Result<Vec<u8>, ReleaseError> {
        validate_descriptor(descriptor)?;
        let path = self.path_for(descriptor.digest);
        let result = tokio::time::timeout(self.operation_timeout, self.store.get(&path))
            .await
            .map_err(|_| ReleaseError::Busy)?
            .map_err(map_get_error)?;
        if result.meta.size != descriptor.size_bytes {
            return Err(ReleaseError::DescriptorMismatch);
        }
        let bytes = tokio::time::timeout(self.operation_timeout, result.bytes())
            .await
            .map_err(|_| ReleaseError::Busy)?
            .map_err(map_get_error)?;
        if bytes.is_empty() || bytes.len() > ARTIFACT_MAX_BYTES {
            return Err(ReleaseError::Corruption);
        }
        if u64::try_from(bytes.len()).map_err(|_| ReleaseError::Internal)? != descriptor.size_bytes
        {
            return Err(ReleaseError::DescriptorMismatch);
        }
        if Sha256Digest::of(&bytes) != descriptor.digest {
            return Err(ReleaseError::Corruption);
        }
        Ok(bytes.to_vec())
    }
}

#[async_trait]
impl ArtifactStore for S3ArtifactStore {
    async fn put(&self, descriptor: &ArtifactDescriptor, bytes: &[u8]) -> Result<(), ReleaseError> {
        validate_descriptor(descriptor)?;
        if bytes.is_empty() || bytes.len() > ARTIFACT_MAX_BYTES {
            return Err(ReleaseError::LimitExceeded);
        }
        if u64::try_from(bytes.len()).map_err(|_| ReleaseError::Internal)? != descriptor.size_bytes
        {
            return Err(ReleaseError::DescriptorMismatch);
        }
        if Sha256Digest::of(bytes) != descriptor.digest {
            return Err(ReleaseError::DigestMismatch);
        }
        let result = tokio::time::timeout(
            self.operation_timeout,
            self.store.put_opts(
                &self.path_for(descriptor.digest),
                bytes.to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            ),
        )
        .await
        .map_err(|_| ReleaseError::Busy)?;
        match result {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.get_verified(descriptor).await.map(|_| ())
            }
            Err(error) => Err(map_backend_error(error)),
        }
    }

    async fn get(&self, descriptor: &ArtifactDescriptor) -> Result<Vec<u8>, ReleaseError> {
        self.get_verified(descriptor).await
    }
}

fn validate_config(config: &S3ArtifactStoreConfig) -> Result<(), ReleaseError> {
    if config.bucket.is_empty()
        || config.region.is_empty()
        || config.operation_timeout.is_zero()
        || config.prefix.len() > MAX_PREFIX_BYTES
        || config
            .prefix
            .split('/')
            .any(|part| part == "." || part == "..")
        || config.prefix.contains(['\\', '\0'])
    {
        return Err(ReleaseError::InvalidArtifact);
    }
    if let Some(endpoint) = &config.endpoint {
        let url = Url::parse(endpoint).map_err(|_| ReleaseError::InvalidArtifact)?;
        let accepted_scheme =
            url.scheme() == "https" || (url.scheme() == "http" && config.allow_http);
        if !accepted_scheme
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ReleaseError::InvalidArtifact);
        }
    }
    if config.endpoint.is_none() && config.allow_http {
        return Err(ReleaseError::InvalidArtifact);
    }
    if let S3Credentials::Static(credentials) = &config.credentials
        && (credentials.access_key_id.is_empty()
            || credentials.secret_access_key.is_empty()
            || credentials
                .session_token
                .as_ref()
                .is_some_and(String::is_empty))
    {
        return Err(ReleaseError::InvalidArtifact);
    }
    Ok(())
}

fn validate_descriptor(descriptor: &ArtifactDescriptor) -> Result<(), ReleaseError> {
    let maximum = u64::try_from(ARTIFACT_MAX_BYTES).map_err(|_| ReleaseError::Internal)?;
    if descriptor.size_bytes == 0 || descriptor.size_bytes > maximum {
        return Err(ReleaseError::LimitExceeded);
    }
    Ok(())
}

fn map_get_error(error: object_store::Error) -> ReleaseError {
    match error {
        object_store::Error::NotFound { .. } => ReleaseError::NotFound,
        error => map_backend_error(error),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_backend_error(error: object_store::Error) -> ReleaseError {
    match error {
        object_store::Error::NotFound { .. } => ReleaseError::NotFound,
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => {
            ReleaseError::Busy
        }
        _ => ReleaseError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_cleartext_without_explicit_opt_in() {
        let mut config = S3ArtifactStoreConfig::new("artifacts", "us-east-1");
        config.endpoint = Some("http://127.0.0.1:9000".to_owned());
        assert_eq!(
            S3ArtifactStore::open(&config).err(),
            Some(ReleaseError::InvalidArtifact)
        );
    }

    #[test]
    fn debug_output_redacts_static_credentials() {
        let credentials = S3Credentials::Static(S3StaticCredentials::new("key", "secret"));
        let output = format!("{credentials:?}");
        assert!(!output.contains("secret"));
        assert!(!output.contains("key"));
    }
}
