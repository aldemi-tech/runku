//! Scoped application file storage with durable quotas and signed HTTP transfer grants.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{
    fmt,
    path::{Path as FsPath, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use hmac::{Hmac, KeyInit, Mac};
use object_store::aws::AmazonS3ConfigKey;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, WriteMultipart, aws::AmazonS3Builder,
    local::LocalFileSystem, path::Path,
};
use runku_core::EnvironmentScope;
use runku_runtime::{
    CancellationToken, FileBytes, FileDownloadGrant, FileDownloadGrantRequest, FileMetadata,
    FileStorage, FileStoreRequest, FileUploadGrant, FileUploadGrantRequest,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tokio::sync::OwnedSemaphorePermit;
use ulid::Ulid;
use url::Url;
use zeroize::Zeroizing;

pub use runku_runtime::FileStorageError;

const SCHEMA_VERSION: i64 = 1;
const TOKEN_VERSION: &str = "rfs1";
const TOKEN_DOMAIN: &[u8] = b"RUNKU_FILE_TRANSFER_GRANT_V1\0";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const MULTIPART_CHUNK_BYTES: usize = 5 * 1024 * 1024;
const MAX_S3_PREFIX_BYTES: usize = 256;

/// Byte stream accepted by the upload service without collecting the request body.
pub type FileUploadStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, FileStorageError>> + Send + 'static>>;

/// Byte stream returned by the download service without collecting the object.
pub type FileDownloadStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, FileStorageError>> + Send + 'static>>;

/// Validated limits for one Environment's application file storage.
#[derive(Clone, Copy, Debug)]
pub struct FileStorageLimits {
    /// Total committed plus reserved bytes admitted for the Environment.
    pub environment_bytes: u64,
    /// Maximum bytes admitted for one file.
    pub file_bytes: u64,
    /// Maximum bytes copied into a Safe V8 or Full Node Action.
    pub action_bytes: u64,
    /// Maximum simultaneously active HTTP uploads.
    pub concurrent_uploads: usize,
    /// Maximum simultaneously active HTTP downloads, held for the complete response stream.
    pub concurrent_downloads: usize,
    /// Maximum unexpired upload grants retained for replay protection and admission control.
    pub maximum_live_upload_grants: usize,
    /// Maximum ready or deleting file metadata rows admitted for the Environment.
    pub maximum_files: usize,
    /// Maximum durable usage events waiting for collector acknowledgement.
    pub maximum_pending_usage_events: usize,
    /// Filesystem bytes that must remain free after admitting an upload reservation.
    pub filesystem_minimum_free_bytes: u64,
    /// Lifetime of one upload grant.
    pub upload_grant_ttl: Duration,
    /// Maximum lifetime a Function may request for one download grant.
    pub maximum_download_grant_ttl: Duration,
}

impl FileStorageLimits {
    /// Conservative local and compact-profile defaults.
    pub const DEFAULT: Self = Self {
        environment_bytes: 10 * 1024 * 1024 * 1024,
        file_bytes: 256 * 1024 * 1024,
        action_bytes: 2 * 1024 * 1024,
        concurrent_uploads: 16,
        concurrent_downloads: 64,
        maximum_live_upload_grants: 4096,
        maximum_files: 100_000,
        maximum_pending_usage_events: 1_000_000,
        filesystem_minimum_free_bytes: 512 * 1024 * 1024,
        upload_grant_ttl: Duration::from_mins(15),
        maximum_download_grant_ttl: Duration::from_mins(15),
    };

    /// Validates all related quota, concurrency, and grant-lifetime bounds.
    ///
    /// # Errors
    ///
    /// Rejects zero, inverted, or unreasonably large policy values.
    pub fn validated(self) -> Result<Self, FileStorageError> {
        if self.environment_bytes == 0
            || self.file_bytes == 0
            || self.file_bytes > self.environment_bytes
            || self.action_bytes == 0
            || self.action_bytes > self.file_bytes
            || !(1..=10_000).contains(&self.concurrent_uploads)
            || !(1..=10_000).contains(&self.concurrent_downloads)
            || !(1..=1_000_000).contains(&self.maximum_live_upload_grants)
            || !(1..=10_000_000).contains(&self.maximum_files)
            || !(1..=10_000_000).contains(&self.maximum_pending_usage_events)
            || self.filesystem_minimum_free_bytes > u64::MAX - self.file_bytes
            || !(Duration::from_secs(1)..=Duration::from_hours(24)).contains(&self.upload_grant_ttl)
            || !(Duration::from_secs(1)..=Duration::from_hours(24))
                .contains(&self.maximum_download_grant_ttl)
        {
            return Err(FileStorageError::InvalidRequest);
        }
        Ok(self)
    }
}

/// Durable authoritative application-file usage fact waiting for external acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileUsageEvent {
    /// Monotonic Environment-local outbox sequence.
    pub sequence: u64,
    /// Stable replay-safe event identifier.
    pub event_id: String,
    /// Exact owning Project identifier.
    pub project_id: String,
    /// Exact owning Environment identifier.
    pub environment_id: String,
    /// `application_file.committed` or `application_file.deleted`.
    pub kind: String,
    /// Positive decimal byte quantity. The kind determines whether capacity was added or removed.
    pub quantity: String,
    /// Always `byte` for version 1.
    pub unit: String,
    /// Event time as Unix microseconds encoded as a signed decimal string.
    pub occurred_at_micros: String,
}

/// At-least-once external sink for ordered authoritative application-file usage facts.
#[async_trait]
pub trait FileUsageSink: fmt::Debug + Send + Sync {
    /// Durably accepts one non-empty ordered batch or returns without acknowledging any event.
    async fn deliver(&self, events: &[FileUsageEvent]) -> Result<(), FileStorageError>;
}

/// Credentials used by an S3-compatible application-file backend.
pub enum S3FileCredentials {
    /// Use the AWS environment, workload-identity, and instance-role chain.
    Environment,
    /// Use explicit credentials, primarily for a separately operated `MinIO` installation.
    Static(S3FileStaticCredentials),
}

impl fmt::Debug for S3FileCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Environment => formatter.write_str("Environment"),
            Self::Static(_) => formatter.write_str("Static([REDACTED])"),
        }
    }
}

/// Explicit S3 credentials with permanently redacted debug output.
pub struct S3FileStaticCredentials {
    access_key_id: Zeroizing<String>,
    secret_access_key: Zeroizing<String>,
    session_token: Option<Zeroizing<String>>,
}

impl fmt::Debug for S3FileStaticCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("S3FileStaticCredentials([REDACTED])")
    }
}

impl S3FileStaticCredentials {
    /// Creates explicit access-key credentials.
    #[must_use]
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: Zeroizing::new(access_key_id.into()),
            secret_access_key: Zeroizing::new(secret_access_key.into()),
            session_token: None,
        }
    }

    /// Adds an optional temporary session token.
    #[must_use]
    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(Zeroizing::new(token.into()));
        self
    }
}

/// Strict configuration for an S3-compatible application-file backend.
#[derive(Debug)]
pub struct S3FileStoreConfig {
    /// Bucket containing file objects.
    pub bucket: String,
    /// Signing region.
    pub region: String,
    /// Optional compatible-service endpoint.
    pub endpoint: Option<String>,
    /// Namespace dedicated to application files.
    pub prefix: String,
    /// Whether the bucket is placed in the request hostname.
    pub virtual_hosted_style: bool,
    /// Explicit development-only cleartext opt-in for a literal loopback endpoint.
    pub allow_loopback_http: bool,
    /// Maximum duration of a backend operation.
    pub operation_timeout: Duration,
    /// Credential source.
    pub credentials: S3FileCredentials,
}

impl S3FileStoreConfig {
    /// Creates secure production defaults for one bucket and region.
    #[must_use]
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            prefix: "runku-files".to_owned(),
            virtual_hosted_style: false,
            allow_loopback_http: false,
            operation_timeout: Duration::from_secs(30),
            credentials: S3FileCredentials::Environment,
        }
    }
}

/// Cloneable filesystem or S3 object boundary used only for application file bytes.
#[derive(Clone)]
pub struct FileObjectStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    operation_timeout: Duration,
    backend: &'static str,
    filesystem_root: Option<PathBuf>,
}

struct ObjectReadExpectation<'a> {
    size: u64,
    e_tag: &'a str,
    version: Option<&'a str>,
}

impl fmt::Debug for FileObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileObjectStore")
            .field("backend", &self.backend)
            .field("prefix", &self.prefix)
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl FileObjectStore {
    /// Opens a private persistent filesystem object root.
    ///
    /// # Errors
    ///
    /// Rejects absent/symlink/non-directory roots and backend initialization failures.
    pub async fn filesystem(root: &FsPath) -> Result<Self, FileStorageError> {
        ensure_private_directory(root).await?;
        let metadata = tokio::fs::symlink_metadata(root)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(FileStorageError::InvalidRequest);
        }
        let root = tokio::fs::canonicalize(root)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        let store =
            LocalFileSystem::new_with_prefix(&root).map_err(|_| FileStorageError::Unavailable)?;
        Ok(Self {
            store: Arc::new(store),
            prefix: String::new(),
            operation_timeout: Duration::from_secs(30),
            backend: "filesystem",
            filesystem_root: Some(root),
        })
    }

    /// Opens an S3-compatible backend after validating transport and credential configuration.
    ///
    /// # Errors
    ///
    /// Rejects unsafe endpoint/prefix/credential settings or an unavailable builder.
    pub fn s3(config: &S3FileStoreConfig) -> Result<Self, FileStorageError> {
        validate_s3(config)?;
        let mut builder = match &config.credentials {
            S3FileCredentials::Environment => environment_s3_builder()?,
            S3FileCredentials::Static(credentials) => {
                let mut value = AmazonS3Builder::new()
                    .with_access_key_id(credentials.access_key_id.to_string())
                    .with_secret_access_key(credentials.secret_access_key.to_string());
                if let Some(token) = &credentials.session_token {
                    value = value.with_token(token.to_string());
                }
                value
            }
        }
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_virtual_hosted_style_request(config.virtual_hosted_style)
        .with_allow_http(config.allow_loopback_http);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        Ok(Self {
            store: Arc::new(builder.build().map_err(|_| FileStorageError::Unavailable)?),
            prefix: config.prefix.trim_matches('/').to_owned(),
            operation_timeout: config.operation_timeout,
            backend: "s3",
            filesystem_root: None,
        })
    }

    /// Returns the stable backend kind without credentials or endpoint details.
    #[must_use]
    pub const fn backend(&self) -> &'static str {
        self.backend
    }

    fn ensure_filesystem_capacity(
        &self,
        reservation: u64,
        minimum_free: u64,
    ) -> Result<(), FileStorageError> {
        let Some(root) = &self.filesystem_root else {
            return Ok(());
        };
        let available = fs2::available_space(root).map_err(|_| FileStorageError::Unavailable)?;
        if available < reservation.saturating_add(minimum_free) {
            return Err(FileStorageError::LimitExceeded);
        }
        Ok(())
    }

    fn path(&self, scope: EnvironmentScope, file_id: &str) -> Path {
        let relative = format!(
            "v1/projects/{}/environments/{}/files/{file_id}",
            scope.project_id(),
            scope.environment_id()
        );
        Path::from(if self.prefix.is_empty() {
            relative
        } else {
            format!("{}/{relative}", self.prefix)
        })
    }

    async fn put(
        &self,
        scope: EnvironmentScope,
        file_id: &str,
        mut stream: FileUploadStream,
        maximum: u64,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(u64, [u8; 32], object_store::PutResult), FileStorageError> {
        let path = self.path(scope, file_id);
        let upload = wait(
            deadline,
            &cancellation,
            tokio::time::timeout(self.operation_timeout, self.store.put_multipart(&path)),
        )
        .await?
        .map_err(|_| FileStorageError::Unavailable)?
        .map_err(map_object_error)?;
        let mut writer = Some(WriteMultipart::new_with_chunk_size(
            upload,
            MULTIPART_CHUNK_BYTES,
        ));
        let mut size = 0_u64;
        let mut hash = Sha256::new();
        loop {
            let next = wait(deadline, &cancellation, stream.next()).await;
            let next = match next {
                Ok(value) => value,
                Err(error) => {
                    abort_writer(writer.take(), self.operation_timeout).await;
                    return Err(error);
                }
            };
            let Some(chunk) = next else { break };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    abort_writer(writer.take(), self.operation_timeout).await;
                    return Err(error);
                }
            };
            size = size
                .checked_add(
                    u64::try_from(chunk.len()).map_err(|_| FileStorageError::LimitExceeded)?,
                )
                .ok_or(FileStorageError::LimitExceeded)?;
            if size > maximum {
                abort_writer(writer.take(), self.operation_timeout).await;
                return Err(FileStorageError::LimitExceeded);
            }
            hash.update(&chunk);
            let active = writer.as_mut().ok_or(FileStorageError::Unavailable)?;
            wait(deadline, &cancellation, active.wait_for_capacity(4))
                .await?
                .map_err(map_object_error)?;
            active.put(chunk);
        }
        if size == 0 {
            abort_writer(writer.take(), self.operation_timeout).await;
            return Err(FileStorageError::InvalidRequest);
        }
        let active = writer.take().ok_or(FileStorageError::Unavailable)?;
        let committed = wait(
            deadline,
            &cancellation,
            tokio::time::timeout(self.operation_timeout, active.finish()),
        )
        .await?
        .map_err(|_| FileStorageError::Unavailable)?
        .map_err(map_object_error)?;
        Ok((size, hash.finalize().into(), committed))
    }

    async fn get(
        &self,
        scope: EnvironmentScope,
        file_id: &str,
        range: Option<std::ops::Range<u64>>,
        expected: ObjectReadExpectation<'_>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(std::ops::Range<u64>, FileDownloadStream), FileStorageError> {
        let options = GetOptions {
            range: range.map(GetRange::Bounded),
            if_match: Some(expected.e_tag.to_owned()),
            version: expected.version.map(str::to_owned),
            ..GetOptions::default()
        };
        let result = wait(
            deadline,
            &cancellation,
            tokio::time::timeout(
                self.operation_timeout,
                self.store.get_opts(&self.path(scope, file_id), options),
            ),
        )
        .await?
        .map_err(|_| FileStorageError::Unavailable)?
        .map_err(map_object_read_error)?;
        if result.meta.size != expected.size
            || result.meta.e_tag.as_deref() != Some(expected.e_tag)
            || expected.version.is_some() && result.meta.version.as_deref() != expected.version
        {
            return Err(FileStorageError::Corruption);
        }
        let result_range = result.range.clone();
        let stream = Box::pin(result.into_stream());
        let operation_timeout = self.operation_timeout;
        let guarded = futures_util::stream::unfold(
            (stream, cancellation),
            move |(mut stream, cancellation)| async move {
                let next = wait(
                    deadline,
                    &cancellation,
                    tokio::time::timeout(operation_timeout, stream.next()),
                )
                .await;
                let item = match next {
                    Err(error) => Err(error),
                    Ok(Err(_)) => Err(FileStorageError::Unavailable),
                    Ok(Ok(None)) => return None,
                    Ok(Ok(Some(item))) => item.map_err(map_object_error),
                };
                Some((item, (stream, cancellation)))
            },
        );
        Ok((result_range, Box::pin(guarded)))
    }

    async fn delete(
        &self,
        scope: EnvironmentScope,
        file_id: &str,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), FileStorageError> {
        let result = wait(
            deadline,
            &cancellation,
            tokio::time::timeout(
                self.operation_timeout,
                self.store.delete(&self.path(scope, file_id)),
            ),
        )
        .await?
        .map_err(|_| FileStorageError::Unavailable)?;
        match result {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(map_object_error(error)),
        }
    }
}

/// Download metadata and stream after a transfer grant has been verified.
pub struct AuthorizedFileDownload {
    /// Immutable file metadata.
    pub metadata: FileMetadata,
    /// Exact returned inclusive-exclusive byte range.
    pub range: std::ops::Range<u64>,
    /// Backend byte stream.
    pub stream: FileDownloadStream,
}

struct VerifiedDownloadStream {
    inner: FileDownloadStream,
    remaining: u64,
    hash: Option<Sha256>,
    expected_hash: Option<[u8; 32]>,
    permit: Option<OwnedSemaphorePermit>,
    finished: bool,
}

impl VerifiedDownloadStream {
    fn new(
        inner: FileDownloadStream,
        expected_bytes: u64,
        expected_hash: Option<[u8; 32]>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            inner,
            remaining: expected_bytes,
            hash: expected_hash.map(|_| Sha256::new()),
            expected_hash,
            permit: Some(permit),
            finished: false,
        }
    }

    fn finish(&mut self) {
        self.finished = true;
        self.permit.take();
    }
}

impl Stream for VerifiedDownloadStream {
    type Item = Result<Bytes, FileStorageError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(error))) => {
                self.finish();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(chunk))) => {
                let Ok(length) = u64::try_from(chunk.len()) else {
                    self.finish();
                    return Poll::Ready(Some(Err(FileStorageError::Corruption)));
                };
                let Some(remaining) = self.remaining.checked_sub(length) else {
                    self.finish();
                    return Poll::Ready(Some(Err(FileStorageError::Corruption)));
                };
                self.remaining = remaining;
                if let Some(hash) = &mut self.hash {
                    hash.update(&chunk);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(None) => {
                let valid_length = self.remaining == 0;
                let valid_hash = match (self.hash.take(), self.expected_hash) {
                    (Some(hash), Some(expected)) => <[u8; 32]>::from(hash.finalize()) == expected,
                    (None, None) => true,
                    _ => false,
                };
                self.finish();
                if valid_length && valid_hash {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(FileStorageError::Corruption)))
                }
            }
        }
    }
}

impl fmt::Debug for AuthorizedFileDownload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizedFileDownload")
            .field("metadata", &self.metadata)
            .field("range", &self.range)
            .finish_non_exhaustive()
    }
}

/// Durable one-Environment application file service.
pub struct FileStorageService {
    scope: EnvironmentScope,
    metadata: SqlitePool,
    objects: FileObjectStore,
    token_key: Zeroizing<[u8; 32]>,
    limits: FileStorageLimits,
    uploads: Arc<tokio::sync::Semaphore>,
    downloads: Arc<tokio::sync::Semaphore>,
}

impl fmt::Debug for FileStorageService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileStorageService")
            .field("scope", &self.scope)
            .field("objects", &self.objects)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl FileStorageService {
    /// Opens and migrates the scoped SQLite metadata repository.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits/key material or unavailable/corrupt metadata state.
    pub async fn open_sqlite(
        scope: EnvironmentScope,
        database: &FsPath,
        objects: FileObjectStore,
        token_key: [u8; 32],
        limits: FileStorageLimits,
    ) -> Result<Self, FileStorageError> {
        let limits = limits.validated()?;
        if token_key.iter().all(|byte| *byte == 0) {
            return Err(FileStorageError::InvalidRequest);
        }
        let options = SqliteConnectOptions::new()
            .filename(database)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let metadata = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        migrate(&metadata).await?;
        let service = Self {
            scope,
            metadata,
            objects,
            token_key: Zeroizing::new(token_key),
            limits,
            uploads: Arc::new(tokio::sync::Semaphore::new(limits.concurrent_uploads)),
            downloads: Arc::new(tokio::sync::Semaphore::new(limits.concurrent_downloads)),
        };
        service.reconcile_interrupted_operations().await?;
        Ok(service)
    }

    /// Returns the stable configured object backend name.
    #[must_use]
    pub const fn backend(&self) -> &'static str {
        self.objects.backend()
    }

    /// Returns the oldest unacknowledged authoritative usage facts for this Environment.
    ///
    /// The caller must deliver the complete ordered batch idempotently before acknowledging its
    /// final sequence. A failed or ambiguous delivery is replayed with the same event IDs.
    ///
    /// # Errors
    ///
    /// Rejects a zero or greater-than-1000 limit and unavailable/corrupt metadata.
    pub async fn pending_usage_events(
        &self,
        limit: usize,
    ) -> Result<Vec<FileUsageEvent>, FileStorageError> {
        if !(1..=1000).contains(&limit) {
            return Err(FileStorageError::InvalidRequest);
        }
        let rows = sqlx::query("SELECT sequence, event_id, kind, quantity, occurred_at_micros FROM runku_file_usage_outbox WHERE project_id = ? AND environment_id = ? ORDER BY sequence LIMIT ?")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(i64::try_from(limit).map_err(|_| FileStorageError::InvalidRequest)?)
            .fetch_all(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        rows.into_iter()
            .map(|row| {
                let sequence = u64::try_from(
                    row.try_get::<i64, _>("sequence")
                        .map_err(|_| FileStorageError::Corruption)?,
                )
                .map_err(|_| FileStorageError::Corruption)?;
                let quantity = u64::try_from(
                    row.try_get::<i64, _>("quantity")
                        .map_err(|_| FileStorageError::Corruption)?,
                )
                .map_err(|_| FileStorageError::Corruption)?;
                let kind: String = row
                    .try_get("kind")
                    .map_err(|_| FileStorageError::Corruption)?;
                if !matches!(
                    kind.as_str(),
                    "application_file.committed" | "application_file.deleted"
                ) {
                    return Err(FileStorageError::Corruption);
                }
                Ok(FileUsageEvent {
                    sequence,
                    event_id: row
                        .try_get("event_id")
                        .map_err(|_| FileStorageError::Corruption)?,
                    project_id: self.scope.project_id().to_string(),
                    environment_id: self.scope.environment_id().to_string(),
                    kind,
                    quantity: quantity.to_string(),
                    unit: "byte".to_owned(),
                    occurred_at_micros: row
                        .try_get::<i64, _>("occurred_at_micros")
                        .map_err(|_| FileStorageError::Corruption)?
                        .to_string(),
                })
            })
            .collect()
    }

    /// Removes an ordered usage prefix after the sink durably accepted every event through it.
    ///
    /// # Errors
    ///
    /// Rejects zero or a sequence beyond the currently pending frontier.
    pub async fn acknowledge_usage_events(
        &self,
        through_sequence: u64,
    ) -> Result<(), FileStorageError> {
        if through_sequence == 0 {
            return Err(FileStorageError::InvalidRequest);
        }
        let through =
            i64::try_from(through_sequence).map_err(|_| FileStorageError::InvalidRequest)?;
        let maximum: Option<i64> = sqlx::query_scalar("SELECT MAX(sequence) FROM runku_file_usage_outbox WHERE project_id = ? AND environment_id = ?")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .fetch_one(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if maximum.is_none_or(|value| through > value) {
            return Err(FileStorageError::Conflict);
        }
        sqlx::query("DELETE FROM runku_file_usage_outbox WHERE project_id = ? AND environment_id = ? AND sequence <= ?")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(through)
            .execute(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        Ok(())
    }

    /// Verifies a one-shot upload token and streams the request into the selected backend.
    ///
    /// # Errors
    ///
    /// Rejects invalid/expired/replayed grants, header mismatch, quota overflow, corrupt checksum,
    /// cancellation, or backend failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_http(
        &self,
        upload_id: &str,
        token: &str,
        content_length: Option<u64>,
        content_type: Option<&str>,
        stream: FileUploadStream,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileMetadata, FileStorageError> {
        validate_resource_id(upload_id, "upl")?;
        verify_token(
            &self.token_key,
            self.scope,
            "upload",
            upload_id,
            token,
            now_micros()?,
        )?;
        let permit = self
            .uploads
            .clone()
            .try_acquire_owned()
            .map_err(|_| FileStorageError::LimitExceeded)?;
        let row = self
            .begin_upload(upload_id, content_length, content_type)
            .await?;
        let result = self
            .objects
            .put(
                self.scope,
                &row.file_id,
                stream,
                row.max_bytes,
                deadline,
                cancellation.clone(),
            )
            .await;
        drop(permit);
        let (size, digest, committed) = match result {
            Ok(result) => result,
            Err(error) => {
                self.cleanup_upload(&row).await;
                return Err(error);
            }
        };
        if row
            .expected_sha256
            .is_some_and(|expected| expected != digest)
        {
            self.cleanup_upload(&row).await;
            return Err(FileStorageError::Corruption);
        }
        let Some(backend_e_tag) = committed.e_tag else {
            self.cleanup_upload(&row).await;
            return Err(FileStorageError::Unavailable);
        };
        match self
            .complete_upload(
                &row,
                size,
                digest,
                &backend_e_tag,
                committed.version.as_deref(),
            )
            .await
        {
            Ok(metadata) => Ok(metadata),
            Err(error) => {
                self.cleanup_upload(&row).await;
                Err(error)
            }
        }
    }

    /// Verifies a download grant and opens an optional byte range.
    ///
    /// # Errors
    ///
    /// Rejects invalid/expired tokens, unknown files, invalid ranges, and backend inconsistencies.
    pub async fn download_http(
        &self,
        file_id: &str,
        token: &str,
        range: Option<std::ops::Range<u64>>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<AuthorizedFileDownload, FileStorageError> {
        validate_resource_id(file_id, "fil")?;
        verify_token(
            &self.token_key,
            self.scope,
            "download",
            file_id,
            token,
            now_micros()?,
        )?;
        let stored = self.load_stored_file(file_id).await?;
        let size = parse_size(&stored.metadata.size_bytes)?;
        if let Some(value) = &range
            && (value.start >= value.end || value.end > size)
        {
            return Err(FileStorageError::InvalidRequest);
        }
        let permit = self
            .downloads
            .clone()
            .try_acquire_owned()
            .map_err(|_| FileStorageError::LimitExceeded)?;
        let (returned, stream) = self
            .objects
            .get(
                self.scope,
                file_id,
                range,
                ObjectReadExpectation {
                    size,
                    e_tag: &stored.backend_e_tag,
                    version: stored.backend_version.as_deref(),
                },
                deadline,
                cancellation,
            )
            .await?;
        let expected_bytes = returned
            .end
            .checked_sub(returned.start)
            .ok_or(FileStorageError::Corruption)?;
        let expected_hash = (returned == (0..size)).then_some(stored.digest);
        let stream = Box::pin(VerifiedDownloadStream::new(
            stream,
            expected_bytes,
            expected_hash,
            permit,
        ));
        Ok(AuthorizedFileDownload {
            metadata: stored.metadata,
            range: returned,
            stream,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn reserve_upload(
        &self,
        request: FileUploadGrantRequest,
    ) -> Result<(UploadRow, i64), FileStorageError> {
        if request.max_bytes == 0 || request.max_bytes > self.limits.file_bytes {
            return Err(FileStorageError::LimitExceeded);
        }
        let content_type = validate_content_type(request.content_type.as_deref())?;
        let expected_sha256 = request.sha256.as_deref().map(parse_sha256).transpose()?;
        let now = now_micros()?;
        let ttl = i64::try_from(self.limits.upload_grant_ttl.as_micros())
            .map_err(|_| FileStorageError::InvalidRequest)?;
        let expires = now
            .checked_add(ttl)
            .ok_or(FileStorageError::InvalidRequest)?;
        let row = UploadRow {
            upload_id: resource_id("upl"),
            file_id: resource_id("fil"),
            max_bytes: request.max_bytes,
            content_type,
            content_type_required: request.content_type.is_some(),
            expected_sha256,
        };
        let mut transaction = self
            .metadata
            .begin()
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        sqlx::query("UPDATE runku_file_uploads SET state = 'expired' WHERE project_id = ? AND environment_id = ? AND expires_at_micros < ? AND state = 'reserved'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        sqlx::query("DELETE FROM runku_file_uploads WHERE project_id = ? AND environment_id = ? AND expires_at_micros < ? AND state IN ('completed', 'failed', 'expired')")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        sqlx::query("DELETE FROM runku_files WHERE project_id = ? AND environment_id = ? AND state = 'deleted'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        let live_grants: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runku_file_uploads WHERE project_id = ? AND environment_id = ? AND expires_at_micros >= ?")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(now)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if usize::try_from(live_grants).map_err(|_| FileStorageError::Corruption)?
            >= self.limits.maximum_live_upload_grants
        {
            return Err(FileStorageError::LimitExceeded);
        }
        let committed: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(size_bytes), 0) FROM runku_files WHERE project_id = ? AND environment_id = ? AND state IN ('ready', 'deleting')")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        let file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runku_files WHERE project_id = ? AND environment_id = ? AND state IN ('ready', 'deleting')")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if usize::try_from(file_count).map_err(|_| FileStorageError::Corruption)?
            >= self.limits.maximum_files
        {
            return Err(FileStorageError::LimitExceeded);
        }
        let reserved: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(max_bytes), 0) FROM runku_file_uploads WHERE project_id = ? AND environment_id = ? AND state IN ('reserved', 'uploading')")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        let reserved = u64::try_from(reserved).map_err(|_| FileStorageError::Corruption)?;
        self.objects.ensure_filesystem_capacity(
            reserved
                .checked_add(row.max_bytes)
                .ok_or(FileStorageError::Corruption)?,
            self.limits.filesystem_minimum_free_bytes,
        )?;
        let admitted = u64::try_from(committed)
            .ok()
            .and_then(|value| value.checked_add(reserved))
            .and_then(|value| value.checked_add(row.max_bytes))
            .ok_or(FileStorageError::Corruption)?;
        if admitted > self.limits.environment_bytes {
            return Err(FileStorageError::LimitExceeded);
        }
        sqlx::query("INSERT INTO runku_file_uploads(project_id, environment_id, upload_id, file_id, state, max_bytes, content_type, content_type_required, expected_sha256, created_at_micros, expires_at_micros) VALUES (?, ?, ?, ?, 'reserved', ?, ?, ?, ?, ?, ?)")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(&row.upload_id)
            .bind(&row.file_id)
            .bind(i64::try_from(row.max_bytes).map_err(|_| FileStorageError::LimitExceeded)?)
            .bind(&row.content_type)
            .bind(row.content_type_required)
            .bind(row.expected_sha256.map(|value| value.to_vec()))
            .bind(now)
            .bind(expires)
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        Ok((row, expires))
    }

    async fn begin_upload(
        &self,
        upload_id: &str,
        content_length: Option<u64>,
        content_type: Option<&str>,
    ) -> Result<UploadRow, FileStorageError> {
        let now = now_micros()?;
        let row = sqlx::query("SELECT file_id, max_bytes, content_type, content_type_required, expected_sha256, expires_at_micros, state FROM runku_file_uploads WHERE project_id = ? AND environment_id = ? AND upload_id = ?")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(upload_id)
            .fetch_optional(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?
            .ok_or(FileStorageError::NotFound)?;
        let state: String = row
            .try_get("state")
            .map_err(|_| FileStorageError::Corruption)?;
        let expires: i64 = row
            .try_get("expires_at_micros")
            .map_err(|_| FileStorageError::Corruption)?;
        if state != "reserved" || expires < now {
            return Err(FileStorageError::Conflict);
        }
        let maximum = u64::try_from(
            row.try_get::<i64, _>("max_bytes")
                .map_err(|_| FileStorageError::Corruption)?,
        )
        .map_err(|_| FileStorageError::Corruption)?;
        if content_length.is_some_and(|length| length == 0 || length > maximum) {
            return Err(FileStorageError::LimitExceeded);
        }
        let expected_type: String = row
            .try_get("content_type")
            .map_err(|_| FileStorageError::Corruption)?;
        let content_type_required: bool = row
            .try_get("content_type_required")
            .map_err(|_| FileStorageError::Corruption)?;
        let actual_type = validate_content_type(content_type)?;
        if content_type_required && content_type.is_none()
            || content_type.is_some() && actual_type != expected_type
        {
            return Err(FileStorageError::InvalidRequest);
        }
        let changed = sqlx::query("UPDATE runku_file_uploads SET state = 'uploading' WHERE project_id = ? AND environment_id = ? AND upload_id = ? AND state = 'reserved'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(upload_id)
            .execute(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if changed.rows_affected() != 1 {
            return Err(FileStorageError::Conflict);
        }
        let expected = row
            .try_get::<Option<Vec<u8>>, _>("expected_sha256")
            .map_err(|_| FileStorageError::Corruption)?
            .map(|bytes| bytes.try_into().map_err(|_| FileStorageError::Corruption))
            .transpose()?;
        Ok(UploadRow {
            upload_id: upload_id.to_owned(),
            file_id: row
                .try_get("file_id")
                .map_err(|_| FileStorageError::Corruption)?,
            max_bytes: maximum,
            content_type: expected_type,
            content_type_required,
            expected_sha256: expected,
        })
    }

    async fn complete_upload(
        &self,
        row: &UploadRow,
        size: u64,
        digest: [u8; 32],
        backend_e_tag: &str,
        backend_version: Option<&str>,
    ) -> Result<FileMetadata, FileStorageError> {
        let now = now_micros()?;
        let mut transaction = self
            .metadata
            .begin()
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        let pending_usage: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runku_file_usage_outbox WHERE project_id = ? AND environment_id = ?")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if usize::try_from(pending_usage).map_err(|_| FileStorageError::Corruption)?
            >= self.limits.maximum_pending_usage_events
        {
            return Err(FileStorageError::LimitExceeded);
        }
        sqlx::query("INSERT INTO runku_files(project_id, environment_id, file_id, state, size_bytes, sha256, content_type, created_at_micros, backend_e_tag, backend_version) VALUES (?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?)")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(&row.file_id)
            .bind(i64::try_from(size).map_err(|_| FileStorageError::LimitExceeded)?)
            .bind(digest.to_vec())
            .bind(&row.content_type)
            .bind(now)
            .bind(backend_e_tag)
            .bind(backend_version)
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        let changed = sqlx::query("UPDATE runku_file_uploads SET state = 'completed' WHERE project_id = ? AND environment_id = ? AND upload_id = ? AND state = 'uploading'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(&row.upload_id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if changed.rows_affected() != 1 {
            return Err(FileStorageError::Conflict);
        }
        sqlx::query("INSERT INTO runku_file_usage_outbox(project_id, environment_id, event_id, kind, quantity, occurred_at_micros) VALUES (?, ?, ?, 'application_file.committed', ?, ?)")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(resource_id("use"))
            .bind(i64::try_from(size).map_err(|_| FileStorageError::LimitExceeded)?)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        Ok(metadata(&row.file_id, size, digest, &row.content_type, now))
    }

    async fn fail_upload(&self, upload_id: &str) {
        let _ = sqlx::query("UPDATE runku_file_uploads SET state = 'failed' WHERE project_id = ? AND environment_id = ? AND upload_id = ? AND state IN ('reserved', 'uploading')")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(upload_id)
            .execute(&self.metadata)
            .await;
    }

    async fn cleanup_upload(&self, row: &UploadRow) {
        let deleted = self
            .objects
            .delete(
                self.scope,
                &row.file_id,
                Instant::now() + self.objects.operation_timeout,
                CancellationToken::new(),
            )
            .await
            .is_ok();
        if deleted {
            self.fail_upload(&row.upload_id).await;
        }
    }

    async fn load_stored_file(&self, file_id: &str) -> Result<StoredFile, FileStorageError> {
        validate_resource_id(file_id, "fil")?;
        let row = sqlx::query("SELECT size_bytes, sha256, content_type, created_at_micros, backend_e_tag, backend_version FROM runku_files WHERE project_id = ? AND environment_id = ? AND file_id = ? AND state = 'ready'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(file_id)
            .fetch_optional(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?
            .ok_or(FileStorageError::NotFound)?;
        let size = u64::try_from(
            row.try_get::<i64, _>("size_bytes")
                .map_err(|_| FileStorageError::Corruption)?,
        )
        .map_err(|_| FileStorageError::Corruption)?;
        let digest: [u8; 32] = row
            .try_get::<Vec<u8>, _>("sha256")
            .map_err(|_| FileStorageError::Corruption)?
            .try_into()
            .map_err(|_| FileStorageError::Corruption)?;
        Ok(StoredFile {
            metadata: metadata(
                file_id,
                size,
                digest,
                row.try_get("content_type")
                    .map_err(|_| FileStorageError::Corruption)?,
                row.try_get("created_at_micros")
                    .map_err(|_| FileStorageError::Corruption)?,
            ),
            digest,
            backend_e_tag: row
                .try_get("backend_e_tag")
                .map_err(|_| FileStorageError::Corruption)?,
            backend_version: row
                .try_get("backend_version")
                .map_err(|_| FileStorageError::Corruption)?,
        })
    }

    async fn load_metadata(&self, file_id: &str) -> Result<FileMetadata, FileStorageError> {
        self.load_stored_file(file_id)
            .await
            .map(|stored| stored.metadata)
    }

    async fn reconcile_interrupted_operations(&self) -> Result<(), FileStorageError> {
        let now = now_micros()?;
        sqlx::query("UPDATE runku_file_uploads SET state = 'expired' WHERE project_id = ? AND environment_id = ? AND expires_at_micros < ? AND state = 'reserved'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(now)
            .execute(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        sqlx::query("DELETE FROM runku_file_uploads WHERE project_id = ? AND environment_id = ? AND expires_at_micros < ? AND state IN ('completed', 'failed', 'expired')")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(now)
            .execute(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        sqlx::query("DELETE FROM runku_files WHERE project_id = ? AND environment_id = ? AND state = 'deleted'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .execute(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        let cancellation = CancellationToken::new();
        loop {
            let interrupted = sqlx::query_scalar::<_, String>("SELECT file_id FROM runku_file_uploads WHERE project_id = ? AND environment_id = ? AND state = 'uploading' LIMIT 1000")
                .bind(self.scope.project_id().to_string())
                .bind(self.scope.environment_id().to_string())
                .fetch_all(&self.metadata)
                .await
                .map_err(|_| FileStorageError::Unavailable)?;
            if interrupted.is_empty() {
                break;
            }
            for file_id in interrupted {
                self.objects
                    .delete(
                        self.scope,
                        &file_id,
                        Instant::now() + self.objects.operation_timeout,
                        cancellation.clone(),
                    )
                    .await?;
                sqlx::query("UPDATE runku_file_uploads SET state = 'failed' WHERE project_id = ? AND environment_id = ? AND file_id = ? AND state = 'uploading'")
                    .bind(self.scope.project_id().to_string())
                    .bind(self.scope.environment_id().to_string())
                    .bind(&file_id)
                    .execute(&self.metadata)
                    .await
                    .map_err(|_| FileStorageError::Unavailable)?;
            }
        }
        loop {
            let deleting = sqlx::query_scalar::<_, String>("SELECT file_id FROM runku_files WHERE project_id = ? AND environment_id = ? AND state = 'deleting' LIMIT 1000")
                .bind(self.scope.project_id().to_string())
                .bind(self.scope.environment_id().to_string())
                .fetch_all(&self.metadata)
                .await
                .map_err(|_| FileStorageError::Unavailable)?;
            if deleting.is_empty() {
                break;
            }
            for file_id in deleting {
                self.finish_delete(
                    &file_id,
                    Instant::now() + self.objects.operation_timeout,
                    cancellation.clone(),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn finish_delete(
        &self,
        file_id: &str,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), FileStorageError> {
        let size: i64 = sqlx::query_scalar("SELECT size_bytes FROM runku_files WHERE project_id = ? AND environment_id = ? AND file_id = ? AND state = 'deleting'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(file_id)
            .fetch_optional(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?
            .ok_or(FileStorageError::NotFound)?;
        self.objects
            .delete(self.scope, file_id, deadline, cancellation)
            .await?;
        let now = now_micros()?;
        let mut transaction = self
            .metadata
            .begin()
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        sqlx::query("INSERT INTO runku_file_usage_outbox(project_id, environment_id, event_id, kind, quantity, occurred_at_micros) VALUES (?, ?, ?, 'application_file.deleted', ?, ?)")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(resource_id("use"))
            .bind(size)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        let deleted = sqlx::query("DELETE FROM runku_files WHERE project_id = ? AND environment_id = ? AND file_id = ? AND state = 'deleting'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(file_id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if deleted.rows_affected() != 1 {
            return Err(FileStorageError::Conflict);
        }
        transaction
            .commit()
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        Ok(())
    }
}

#[async_trait]
impl FileStorage for FileStorageService {
    async fn create_upload_grant(
        &self,
        request: FileUploadGrantRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileUploadGrant, FileStorageError> {
        check_lifecycle(deadline, &cancellation)?;
        let (row, expires) = self.reserve_upload(request).await?;
        let token = sign_token(
            &self.token_key,
            self.scope,
            "upload",
            &row.upload_id,
            expires,
        )?;
        Ok(FileUploadGrant {
            path: format!("/v1/files/uploads/{}", row.upload_id),
            token,
            upload_id: row.upload_id,
            expires_at_micros: expires.to_string(),
            max_bytes: row.max_bytes.to_string(),
        })
    }

    async fn store(
        &self,
        request: FileStoreRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileMetadata, FileStorageError> {
        check_lifecycle(deadline, &cancellation)?;
        let length =
            u64::try_from(request.bytes.len()).map_err(|_| FileStorageError::LimitExceeded)?;
        if length == 0 || length > self.limits.action_bytes {
            return Err(FileStorageError::LimitExceeded);
        }
        let (row, expires) = self
            .reserve_upload(FileUploadGrantRequest {
                max_bytes: length,
                content_type: request.content_type,
                sha256: request.sha256,
            })
            .await?;
        let token = sign_token(
            &self.token_key,
            self.scope,
            "upload",
            &row.upload_id,
            expires,
        )?;
        self.upload_http(
            &row.upload_id,
            &token,
            Some(length),
            Some(&row.content_type),
            Box::pin(futures_util::stream::once(async move {
                Ok(Bytes::from(request.bytes))
            })),
            deadline,
            cancellation,
        )
        .await
    }

    async fn metadata(
        &self,
        file_id: String,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileMetadata, FileStorageError> {
        check_lifecycle(deadline, &cancellation)?;
        self.load_metadata(&file_id).await
    }

    async fn create_download_grant(
        &self,
        request: FileDownloadGrantRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileDownloadGrant, FileStorageError> {
        check_lifecycle(deadline, &cancellation)?;
        let metadata = self.load_metadata(&request.file_id).await?;
        let requested = request
            .expires_in_micros
            .parse::<u64>()
            .map_err(|_| FileStorageError::InvalidRequest)?;
        let maximum = u64::try_from(self.limits.maximum_download_grant_ttl.as_micros())
            .map_err(|_| FileStorageError::InvalidRequest)?;
        if requested == 0 || requested > maximum {
            return Err(FileStorageError::LimitExceeded);
        }
        let now = now_micros()?;
        let expires = now
            .checked_add(i64::try_from(requested).map_err(|_| FileStorageError::InvalidRequest)?)
            .ok_or(FileStorageError::InvalidRequest)?;
        let token = sign_token(
            &self.token_key,
            self.scope,
            "download",
            &request.file_id,
            expires,
        )?;
        Ok(FileDownloadGrant {
            path: format!("/v1/files/downloads/{}", request.file_id),
            token,
            expires_at_micros: expires.to_string(),
            metadata,
        })
    }

    async fn get(
        &self,
        file_id: String,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileBytes, FileStorageError> {
        check_lifecycle(deadline, &cancellation)?;
        let stored = self.load_stored_file(&file_id).await?;
        let size = parse_size(&stored.metadata.size_bytes)?;
        if size > self.limits.action_bytes {
            return Err(FileStorageError::LimitExceeded);
        }
        let (_, mut stream) = self
            .objects
            .get(
                self.scope,
                &file_id,
                None,
                ObjectReadExpectation {
                    size,
                    e_tag: &stored.backend_e_tag,
                    version: stored.backend_version.as_deref(),
                },
                deadline,
                cancellation.clone(),
            )
            .await?;
        let mut bytes =
            Vec::with_capacity(usize::try_from(size).map_err(|_| FileStorageError::LimitExceeded)?);
        let mut hash = Sha256::new();
        while let Some(chunk) = wait(deadline, &cancellation, stream.next()).await? {
            let chunk = chunk?;
            if bytes.len().saturating_add(chunk.len())
                > usize::try_from(self.limits.action_bytes).unwrap_or(usize::MAX)
            {
                return Err(FileStorageError::LimitExceeded);
            }
            hash.update(&chunk);
            bytes.extend_from_slice(&chunk);
        }
        if u64::try_from(bytes.len()).map_err(|_| FileStorageError::Corruption)? != size
            || <[u8; 32]>::from(hash.finalize()) != stored.digest
        {
            return Err(FileStorageError::Corruption);
        }
        Ok(FileBytes {
            metadata: stored.metadata,
            bytes,
        })
    }

    async fn delete(
        &self,
        file_id: String,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), FileStorageError> {
        check_lifecycle(deadline, &cancellation)?;
        validate_resource_id(&file_id, "fil")?;
        let changed = sqlx::query("UPDATE runku_files SET state = 'deleting' WHERE project_id = ? AND environment_id = ? AND file_id = ? AND state = 'ready'")
            .bind(self.scope.project_id().to_string())
            .bind(self.scope.environment_id().to_string())
            .bind(&file_id)
            .execute(&self.metadata)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if changed.rows_affected() == 0 {
            let state = sqlx::query_scalar::<_, String>("SELECT state FROM runku_files WHERE project_id = ? AND environment_id = ? AND file_id = ?")
                .bind(self.scope.project_id().to_string())
                .bind(self.scope.environment_id().to_string())
                .bind(&file_id)
                .fetch_optional(&self.metadata)
                .await
                .map_err(|_| FileStorageError::Unavailable)?;
            match state.as_deref() {
                Some("deleted") | None => return Ok(()),
                Some("deleting") => (),
                Some(_) => return Err(FileStorageError::Conflict),
            }
        }
        self.finish_delete(&file_id, deadline, cancellation).await
    }
}

#[derive(Debug)]
struct UploadRow {
    upload_id: String,
    file_id: String,
    max_bytes: u64,
    content_type: String,
    content_type_required: bool,
    expected_sha256: Option<[u8; 32]>,
}

#[derive(Debug)]
struct StoredFile {
    metadata: FileMetadata,
    digest: [u8; 32],
    backend_e_tag: String,
    backend_version: Option<String>,
}

async fn migrate(pool: &SqlitePool) -> Result<(), FileStorageError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_file_schema(version INTEGER PRIMARY KEY, applied_at_micros INTEGER NOT NULL)")
        .execute(pool).await.map_err(|_| FileStorageError::Unavailable)?;
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_file_uploads(project_id TEXT NOT NULL, environment_id TEXT NOT NULL, upload_id TEXT NOT NULL, file_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('reserved','uploading','completed','failed','expired')), max_bytes INTEGER NOT NULL CHECK(max_bytes > 0), content_type TEXT NOT NULL, content_type_required INTEGER NOT NULL CHECK(content_type_required IN (0,1)), expected_sha256 BLOB NULL, created_at_micros INTEGER NOT NULL, expires_at_micros INTEGER NOT NULL, PRIMARY KEY(project_id, environment_id, upload_id), UNIQUE(project_id, environment_id, file_id))")
        .execute(pool).await.map_err(|_| FileStorageError::Unavailable)?;
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_files(project_id TEXT NOT NULL, environment_id TEXT NOT NULL, file_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('ready','deleting','deleted')), size_bytes INTEGER NOT NULL CHECK(size_bytes > 0), sha256 BLOB NOT NULL, content_type TEXT NOT NULL, created_at_micros INTEGER NOT NULL, backend_e_tag TEXT NOT NULL CHECK(length(backend_e_tag) BETWEEN 1 AND 1024), backend_version TEXT NULL CHECK(backend_version IS NULL OR length(backend_version) BETWEEN 1 AND 1024), PRIMARY KEY(project_id, environment_id, file_id))")
        .execute(pool).await.map_err(|_| FileStorageError::Unavailable)?;
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_file_usage_outbox(sequence INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT NOT NULL, environment_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL CHECK(kind IN ('application_file.committed','application_file.deleted')), quantity INTEGER NOT NULL CHECK(quantity > 0), occurred_at_micros INTEGER NOT NULL)")
        .execute(pool).await.map_err(|_| FileStorageError::Unavailable)?;
    sqlx::query("CREATE INDEX IF NOT EXISTS runku_file_upload_state ON runku_file_uploads(project_id, environment_id, state, expires_at_micros)")
        .execute(pool).await.map_err(|_| FileStorageError::Unavailable)?;
    sqlx::query("CREATE INDEX IF NOT EXISTS runku_file_state ON runku_files(project_id, environment_id, state)")
        .execute(pool).await.map_err(|_| FileStorageError::Unavailable)?;
    sqlx::query("CREATE INDEX IF NOT EXISTS runku_file_usage_scope_sequence ON runku_file_usage_outbox(project_id, environment_id, sequence)")
        .execute(pool).await.map_err(|_| FileStorageError::Unavailable)?;
    sqlx::query(
        "INSERT OR IGNORE INTO runku_file_schema(version, applied_at_micros) VALUES (?, ?)",
    )
    .bind(SCHEMA_VERSION)
    .bind(now_micros()?)
    .execute(pool)
    .await
    .map_err(|_| FileStorageError::Unavailable)?;
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM runku_file_schema ORDER BY version")
            .fetch_all(pool)
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
    if versions != [SCHEMA_VERSION] {
        return Err(FileStorageError::Corruption);
    }
    Ok(())
}

fn metadata(
    file_id: &str,
    size: u64,
    digest: [u8; 32],
    content_type: &str,
    created_at: i64,
) -> FileMetadata {
    FileMetadata {
        file_id: file_id.to_owned(),
        size_bytes: size.to_string(),
        sha256: hex(&digest),
        content_type: content_type.to_owned(),
        created_at_micros: created_at.to_string(),
    }
}

fn validate_s3(config: &S3FileStoreConfig) -> Result<(), FileStorageError> {
    if config.bucket.is_empty()
        || config.bucket.len() > 255
        || !config
            .bucket
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        || config.region.is_empty()
        || config.region.len() > 128
        || !config
            .region
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        || config.operation_timeout.is_zero()
        || config.operation_timeout > Duration::from_mins(5)
        || config.prefix.is_empty()
        || config.prefix.len() > MAX_S3_PREFIX_BYTES
        || config.prefix.starts_with('/')
        || config.prefix.ends_with('/')
        || config.prefix.contains("//")
        || config
            .prefix
            .split('/')
            .any(|part| matches!(part, "." | ".."))
        || config.prefix.contains(['\\', '\0'])
    {
        return Err(FileStorageError::InvalidRequest);
    }
    if let Some(endpoint) = &config.endpoint {
        let url = Url::parse(endpoint).map_err(|_| FileStorageError::InvalidRequest)?;
        let loopback = url.host_str().is_some_and(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
        });
        if (url.scheme() != "https"
            && !(url.scheme() == "http" && config.allow_loopback_http && loopback))
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || !matches!(url.path(), "" | "/")
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(FileStorageError::InvalidRequest);
        }
    } else if config.allow_loopback_http {
        return Err(FileStorageError::InvalidRequest);
    }
    if let S3FileCredentials::Static(credentials) = &config.credentials
        && (credentials.access_key_id.is_empty()
            || credentials.secret_access_key.is_empty()
            || credentials.access_key_id.len() > 1024
            || credentials.secret_access_key.len() > 4096
            || !credentials
                .access_key_id
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
            || !credentials
                .secret_access_key
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
            || credentials.session_token.as_ref().is_some_and(|value| {
                value.is_empty()
                    || value.len() > 16 * 1024
                    || !value.bytes().all(|byte| byte.is_ascii_graphic())
            }))
    {
        return Err(FileStorageError::InvalidRequest);
    }
    Ok(())
}

fn environment_s3_builder() -> Result<AmazonS3Builder, FileStorageError> {
    const ALLOWED: [(&str, AmazonS3ConfigKey); 7] = [
        ("AWS_ACCESS_KEY_ID", AmazonS3ConfigKey::AccessKeyId),
        ("AWS_SECRET_ACCESS_KEY", AmazonS3ConfigKey::SecretAccessKey),
        ("AWS_SESSION_TOKEN", AmazonS3ConfigKey::Token),
        (
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            AmazonS3ConfigKey::WebIdentityTokenFile,
        ),
        ("AWS_ROLE_ARN", AmazonS3ConfigKey::RoleArn),
        ("AWS_ROLE_SESSION_NAME", AmazonS3ConfigKey::RoleSessionName),
        (
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
        ),
    ];
    let mut builder = AmazonS3Builder::new();
    for (name, key) in ALLOWED {
        match std::env::var(name) {
            Ok(value) => {
                if value.is_empty()
                    || value.len() > 16 * 1024
                    || value.bytes().any(|byte| !byte.is_ascii_graphic())
                {
                    return Err(FileStorageError::InvalidRequest);
                }
                if name == "AWS_WEB_IDENTITY_TOKEN_FILE" && !FsPath::new(&value).is_absolute() {
                    return Err(FileStorageError::InvalidRequest);
                }
                if name == "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"
                    && (!value.starts_with('/') || value.contains("://"))
                {
                    return Err(FileStorageError::InvalidRequest);
                }
                builder = builder.with_config(key, value);
            }
            Err(std::env::VarError::NotPresent) => (),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(FileStorageError::InvalidRequest);
            }
        }
    }
    Ok(builder)
}

async fn ensure_private_directory(path: &FsPath) -> Result<(), FileStorageError> {
    if !path.is_absolute() || path.parent().is_none() {
        return Err(FileStorageError::InvalidRequest);
    }
    let created = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(FileStorageError::InvalidRequest);
        }
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir(path)
                .await
                .map_err(|_| FileStorageError::Unavailable)?;
            true
        }
        Err(_) => return Err(FileStorageError::Unavailable),
    };
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| FileStorageError::Unavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FileStorageError::InvalidRequest);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if created {
            tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                .await
                .map_err(|_| FileStorageError::Unavailable)?;
        } else if metadata.permissions().mode() & 0o077 != 0 {
            return Err(FileStorageError::InvalidRequest);
        }
    }
    Ok(())
}

fn resource_id(prefix: &str) -> String {
    format!("{prefix}_{}", Ulid::generate())
}

fn validate_resource_id(value: &str, prefix: &str) -> Result<(), FileStorageError> {
    let Some(rest) = value.strip_prefix(&format!("{prefix}_")) else {
        return Err(FileStorageError::InvalidRequest);
    };
    let parsed = Ulid::from_str(rest).map_err(|_| FileStorageError::InvalidRequest)?;
    if parsed.to_string() != rest {
        return Err(FileStorageError::InvalidRequest);
    }
    Ok(())
}

fn validate_content_type(value: Option<&str>) -> Result<String, FileStorageError> {
    let value = value.unwrap_or(DEFAULT_CONTENT_TYPE);
    if value.is_empty()
        || value.len() > 255
        || value.bytes().any(|byte| !(0x20..=0x7e).contains(&byte))
        || value.matches('/').count() != 1
        || value.contains(['\\', '"', '\''])
    {
        return Err(FileStorageError::InvalidRequest);
    }
    Ok(value.to_ascii_lowercase())
}

fn parse_sha256(value: &str) -> Result<[u8; 32], FileStorageError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(FileStorageError::InvalidRequest);
    }
    let mut output = [0_u8; 32];
    for (index, target) in output.iter_mut().enumerate() {
        *target = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| FileStorageError::InvalidRequest)?;
    }
    Ok(output)
}

fn parse_size(value: &str) -> Result<u64, FileStorageError> {
    value.parse().map_err(|_| FileStorageError::Corruption)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn token_payload(scope: EnvironmentScope, operation: &str, resource: &str, expires: i64) -> String {
    format!(
        "{TOKEN_VERSION}\n{operation}\n{}\n{}\n{resource}\n{expires}",
        scope.project_id(),
        scope.environment_id()
    )
}

fn sign_token(
    key: &[u8; 32],
    scope: EnvironmentScope,
    operation: &str,
    resource: &str,
    expires: i64,
) -> Result<String, FileStorageError> {
    let payload = token_payload(scope, operation, resource, expires);
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| FileStorageError::Unavailable)?;
    mac.update(TOKEN_DOMAIN);
    mac.update(payload.as_bytes());
    let signature = mac.finalize().into_bytes();
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload.as_bytes()),
        URL_SAFE_NO_PAD.encode(signature)
    ))
}

fn verify_token(
    key: &[u8; 32],
    scope: EnvironmentScope,
    operation: &str,
    resource: &str,
    token: &str,
    now: i64,
) -> Result<(), FileStorageError> {
    if token.len() > 1024 || token.bytes().any(|byte| !byte.is_ascii_graphic()) {
        return Err(FileStorageError::Forbidden);
    }
    let (encoded_payload, encoded_signature) =
        token.split_once('.').ok_or(FileStorageError::Forbidden)?;
    if encoded_signature.contains('.') {
        return Err(FileStorageError::Forbidden);
    }
    let payload = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .map_err(|_| FileStorageError::Forbidden)?;
    let signature = URL_SAFE_NO_PAD
        .decode(encoded_signature)
        .map_err(|_| FileStorageError::Forbidden)?;
    let text = std::str::from_utf8(&payload).map_err(|_| FileStorageError::Forbidden)?;
    let fields = text.split('\n').collect::<Vec<_>>();
    if fields.len() != 6
        || fields[0] != TOKEN_VERSION
        || fields[1] != operation
        || fields[2] != scope.project_id().to_string()
        || fields[3] != scope.environment_id().to_string()
        || fields[4] != resource
    {
        return Err(FileStorageError::Forbidden);
    }
    let expires = fields[5]
        .parse::<i64>()
        .map_err(|_| FileStorageError::Forbidden)?;
    if expires < now {
        return Err(FileStorageError::Forbidden);
    }
    let expected = token_payload(scope, operation, resource, expires);
    if expected.as_bytes() != payload {
        return Err(FileStorageError::Forbidden);
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| FileStorageError::Unavailable)?;
    mac.update(TOKEN_DOMAIN);
    mac.update(&payload);
    mac.verify_slice(&signature)
        .map_err(|_| FileStorageError::Forbidden)
}

fn now_micros() -> Result<i64, FileStorageError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| FileStorageError::Unavailable)?
            .as_micros(),
    )
    .map_err(|_| FileStorageError::Unavailable)
}

fn check_lifecycle(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), FileStorageError> {
    if cancellation.is_cancelled() {
        Err(FileStorageError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(FileStorageError::Timeout)
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use runku_core::{EnvironmentId, ProjectId};
    use tempfile::TempDir;

    async fn service(
        temporary: &TempDir,
        scope: EnvironmentScope,
        limits: FileStorageLimits,
    ) -> Result<FileStorageService, FileStorageError> {
        let root = temporary.path().join("objects");
        let objects = FileObjectStore::filesystem(&root).await?;
        FileStorageService::open_sqlite(
            scope,
            &temporary
                .path()
                .join(format!("{}.sqlite3", scope.environment_id())),
            objects,
            [7; 32],
            limits,
        )
        .await
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(10)
    }

    #[test]
    fn s3_configuration_rejects_cleartext_nonliteral_and_secret_disclosure() {
        let credentials = S3FileStaticCredentials::new("visible-access", "visible-secret")
            .with_session_token("visible-session");
        assert_eq!(
            format!("{credentials:?}"),
            "S3FileStaticCredentials([REDACTED])"
        );
        let mut config = S3FileStoreConfig::new("file-bucket", "us-east-1");
        config.credentials = S3FileCredentials::Static(credentials);
        config.endpoint = Some("http://storage.example:9000".to_owned());
        config.allow_loopback_http = true;
        assert!(matches!(
            FileObjectStore::s3(&config),
            Err(FileStorageError::InvalidRequest)
        ));
        config.endpoint = Some("http://localhost:9000".to_owned());
        assert!(matches!(
            FileObjectStore::s3(&config),
            Err(FileStorageError::InvalidRequest)
        ));
        config.endpoint = Some("http://127.0.0.1:9000".to_owned());
        assert!(FileObjectStore::s3(&config).is_ok());
        config.endpoint = Some("https://user@storage.example/path".to_owned());
        config.allow_loopback_http = false;
        assert!(matches!(
            FileObjectStore::s3(&config),
            Err(FileStorageError::InvalidRequest)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn filesystem_root_rejects_a_symlink_without_changing_target_permissions()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _, symlink};

        let temporary = TempDir::new()?;
        let target = temporary.path().join("target");
        std::fs::create_dir(&target)?;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))?;
        let link = temporary.path().join("objects");
        symlink(&target, &link)?;
        assert!(matches!(
            FileObjectStore::filesystem(&link).await,
            Err(FileStorageError::InvalidRequest)
        ));
        assert_eq!(std::fs::metadata(target)?.mode() & 0o777, 0o755);
        assert!(matches!(
            FileObjectStore::filesystem(FsPath::new(".")).await,
            Err(FileStorageError::InvalidRequest)
        ));
        assert!(matches!(
            FileObjectStore::filesystem(FsPath::new("/")).await,
            Err(FileStorageError::InvalidRequest)
        ));
        let public = temporary.path().join("public");
        std::fs::create_dir(&public)?;
        std::fs::set_permissions(&public, std::fs::Permissions::from_mode(0o755))?;
        assert!(matches!(
            FileObjectStore::filesystem(&public).await,
            Err(FileStorageError::InvalidRequest)
        ));
        assert_eq!(std::fs::metadata(public)?.mode() & 0o777, 0o755);
        Ok(())
    }

    #[tokio::test]
    async fn filesystem_round_trip_range_delete_and_integrity()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let service = service(
            &temporary,
            scope,
            FileStorageLimits {
                filesystem_minimum_free_bytes: 0,
                ..FileStorageLimits::DEFAULT
            },
        )
        .await?;
        let stored = service
            .store(
                FileStoreRequest {
                    bytes: b"abcdef".to_vec(),
                    content_type: Some("text/plain".to_owned()),
                    sha256: Some(hex(&Sha256::digest(b"abcdef"))),
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(stored.size_bytes, "6");
        assert_eq!(stored.content_type, "text/plain");
        assert_eq!(
            service
                .get(stored.file_id.clone(), deadline(), CancellationToken::new())
                .await?
                .bytes,
            b"abcdef"
        );
        let grant = service
            .create_download_grant(
                FileDownloadGrantRequest {
                    file_id: stored.file_id.clone(),
                    expires_in_micros: "1000000".to_owned(),
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        let download = service
            .download_http(
                &stored.file_id,
                &grant.token,
                Some(1..4),
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(download.range, 1..4);
        let chunks = download.stream.collect::<Vec<_>>().await;
        assert_eq!(
            chunks.into_iter().collect::<Result<Vec<_>, _>>()?.concat(),
            b"bcd"
        );
        service
            .delete(stored.file_id.clone(), deadline(), CancellationToken::new())
            .await?;
        service
            .delete(stored.file_id.clone(), deadline(), CancellationToken::new())
            .await?;
        assert_eq!(
            service
                .metadata(stored.file_id, deadline(), CancellationToken::new())
                .await,
            Err(FileStorageError::NotFound)
        );
        Ok(())
    }

    #[tokio::test]
    async fn downloads_hold_admission_and_reject_replaced_objects()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let service = service(
            &temporary,
            scope,
            FileStorageLimits {
                concurrent_downloads: 1,
                filesystem_minimum_free_bytes: 0,
                ..FileStorageLimits::DEFAULT
            },
        )
        .await?;
        let stored = service
            .store(
                FileStoreRequest {
                    bytes: b"abcdef".to_vec(),
                    content_type: Some("text/plain".to_owned()),
                    sha256: None,
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        let grant = service
            .create_download_grant(
                FileDownloadGrantRequest {
                    file_id: stored.file_id.clone(),
                    expires_in_micros: "1000000".to_owned(),
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        let first = service
            .download_http(
                &stored.file_id,
                &grant.token,
                None,
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        assert!(matches!(
            service
                .download_http(
                    &stored.file_id,
                    &grant.token,
                    None,
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::LimitExceeded)
        ));
        let bytes = first.stream.collect::<Vec<_>>().await;
        assert_eq!(
            bytes.into_iter().collect::<Result<Vec<_>, _>>()?.concat(),
            b"abcdef"
        );
        let released = service
            .download_http(
                &stored.file_id,
                &grant.token,
                Some(0..1),
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        drop(released);

        let relative = service.objects.path(scope, &stored.file_id).to_string();
        let replacement = temporary.path().join("replacement");
        tokio::fs::write(&replacement, b"ghijkl").await?;
        tokio::fs::rename(replacement, temporary.path().join("objects").join(relative)).await?;
        assert!(matches!(
            service
                .download_http(
                    &stored.file_id,
                    &grant.token,
                    None,
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::Corruption)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn usage_outbox_is_transactional_replayable_bounded_and_acknowledged()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let service = service(
            &temporary,
            scope,
            FileStorageLimits {
                maximum_pending_usage_events: 1,
                filesystem_minimum_free_bytes: 0,
                ..FileStorageLimits::DEFAULT
            },
        )
        .await?;
        let stored = service
            .store(
                FileStoreRequest {
                    bytes: b"abc".to_vec(),
                    content_type: None,
                    sha256: None,
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        let first = service.pending_usage_events(10).await?;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].kind, "application_file.committed");
        assert_eq!(first[0].quantity, "3");
        assert_eq!(first[0].unit, "byte");
        assert_eq!(first, service.pending_usage_events(10).await?);
        assert_eq!(
            service
                .acknowledge_usage_events(first[0].sequence + 1)
                .await,
            Err(FileStorageError::Conflict)
        );
        assert!(matches!(
            service
                .store(
                    FileStoreRequest {
                        bytes: b"blocked".to_vec(),
                        content_type: None,
                        sha256: None,
                    },
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::LimitExceeded)
        ));
        service
            .delete(stored.file_id, deadline(), CancellationToken::new())
            .await?;
        let with_delete = service.pending_usage_events(10).await?;
        assert_eq!(with_delete.len(), 2);
        assert_eq!(with_delete[1].kind, "application_file.deleted");
        assert_eq!(with_delete[1].quantity, "3");
        service
            .acknowledge_usage_events(with_delete[1].sequence)
            .await?;
        assert!(service.pending_usage_events(10).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn grants_reject_tampering_replay_header_drift_checksum_and_quota()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let limits = FileStorageLimits {
            environment_bytes: 8,
            file_bytes: 8,
            action_bytes: 8,
            filesystem_minimum_free_bytes: 0,
            ..FileStorageLimits::DEFAULT
        };
        let service = service(&temporary, scope, limits).await?;
        let digest = hex(&Sha256::digest(b"abc"));
        let grant = service
            .create_upload_grant(
                FileUploadGrantRequest {
                    max_bytes: 3,
                    content_type: Some("image/png".to_owned()),
                    sha256: Some(digest),
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            service
                .upload_http(
                    &grant.upload_id,
                    &grant.token,
                    Some(3),
                    None,
                    Box::pin(futures_util::stream::once(async {
                        Ok(Bytes::from_static(b"abc"))
                    })),
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::InvalidRequest)
        );
        let metadata = service
            .upload_http(
                &grant.upload_id,
                &grant.token,
                Some(3),
                Some("image/png"),
                Box::pin(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"abc"))
                })),
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            service
                .upload_http(
                    &grant.upload_id,
                    &grant.token,
                    Some(3),
                    Some("image/png"),
                    Box::pin(futures_util::stream::empty()),
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::Conflict)
        );
        let mut tampered = grant.token;
        tampered.push('A');
        assert_eq!(
            service
                .download_http(
                    &metadata.file_id,
                    &tampered,
                    None,
                    deadline(),
                    CancellationToken::new(),
                )
                .await
                .err(),
            Some(FileStorageError::Forbidden)
        );
        assert_eq!(
            service
                .create_upload_grant(
                    FileUploadGrantRequest {
                        max_bytes: 6,
                        content_type: None,
                        sha256: None,
                    },
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::LimitExceeded)
        );
        let checksum_grant = service
            .create_upload_grant(
                FileUploadGrantRequest {
                    max_bytes: 5,
                    content_type: None,
                    sha256: Some(hex(&Sha256::digest(b"right"))),
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            service
                .upload_http(
                    &checksum_grant.upload_id,
                    &checksum_grant.token,
                    Some(5),
                    None,
                    Box::pin(futures_util::stream::once(async {
                        Ok(Bytes::from_static(b"wrong"))
                    })),
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::Corruption)
        );
        Ok(())
    }

    #[tokio::test]
    async fn live_grant_admission_bounds_metadata_amplification()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let grant_service = service(
            &temporary,
            scope,
            FileStorageLimits {
                maximum_live_upload_grants: 1,
                filesystem_minimum_free_bytes: 0,
                ..FileStorageLimits::DEFAULT
            },
        )
        .await?;
        grant_service
            .create_upload_grant(
                FileUploadGrantRequest {
                    max_bytes: 1,
                    content_type: None,
                    sha256: None,
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            grant_service
                .create_upload_grant(
                    FileUploadGrantRequest {
                        max_bytes: 1,
                        content_type: None,
                        sha256: None,
                    },
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::LimitExceeded)
        );

        let file_scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let file_service = service(
            &temporary,
            file_scope,
            FileStorageLimits {
                maximum_files: 1,
                filesystem_minimum_free_bytes: 0,
                ..FileStorageLimits::DEFAULT
            },
        )
        .await?;
        file_service
            .store(
                FileStoreRequest {
                    bytes: vec![1],
                    content_type: None,
                    sha256: None,
                },
                deadline(),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            file_service
                .create_upload_grant(
                    FileUploadGrantRequest {
                        max_bytes: 1,
                        content_type: None,
                        sha256: None,
                    },
                    deadline(),
                    CancellationToken::new(),
                )
                .await,
            Err(FileStorageError::LimitExceeded)
        );
        Ok(())
    }
}

async fn wait<T>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: impl std::future::Future<Output = T>,
) -> Result<T, FileStorageError> {
    check_lifecycle(deadline, cancellation)?;
    tokio::select! {
        () = cancellation.cancelled() => Err(FileStorageError::Cancelled),
        result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future) => {
            result.map_err(|_| FileStorageError::Timeout)
        }
    }
}

async fn abort_writer(writer: Option<WriteMultipart>, timeout: Duration) {
    if let Some(writer) = writer {
        let _ = tokio::time::timeout(timeout, writer.abort()).await;
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_object_error(error: object_store::Error) -> FileStorageError {
    match error {
        object_store::Error::NotFound { .. } => FileStorageError::NotFound,
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => {
            FileStorageError::Conflict
        }
        _ => FileStorageError::Unavailable,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_object_read_error(error: object_store::Error) -> FileStorageError {
    match error {
        object_store::Error::Precondition { .. } | object_store::Error::NotModified { .. } => {
            FileStorageError::Corruption
        }
        other => map_object_error(other),
    }
}
