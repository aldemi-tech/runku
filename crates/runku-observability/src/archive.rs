use std::{collections::BTreeSet, fmt, path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use duckdb::{Connection, params};
use futures_util::TryStreamExt as _;
use object_store::{
    ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, aws::AmazonS3Builder,
    local::LocalFileSystem, path::Path as ObjectPath,
};
use runku_core::{EnvironmentScope, FunctionName};
use runku_releases::FunctionType;
use runku_value::{TimestampMicros, decode_stored_value, encode_stored_value};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{
    LOG_QUERY_MAX_RECORDS, LogCursor, LogEventKind, LogLevel, LogMessage, LogPage,
    LogPrincipalKind, LogQuery, LogRepository, LogRepositoryBackend, LogRepositoryError, LogStream,
    OperationalEventV1, OutcomeCode, PruneResult, SequencedOperationalEvent,
};

const ARCHIVE_FORMAT_VERSION: u8 = 1;
const ARCHIVE_MAX_SEGMENT_BYTES: usize = 64 * 1024 * 1024;
const ARCHIVE_MAX_MANIFEST_BYTES: usize = 64 * 1024;
const ARCHIVE_MAX_MANIFESTS_PER_SCOPE: usize = 10_000;
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_QUERY_MEMORY_LIMIT: &str = "256MB";

/// Credentials used by an S3-compatible Operational Log archive.
pub enum LogArchiveCredentials {
    /// Use the standard environment, workload-identity, or instance-role chain.
    Environment,
    /// Use explicit credentials for a private S3-compatible endpoint.
    Static(LogArchiveStaticCredentials),
}

impl fmt::Debug for LogArchiveCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Environment => formatter.write_str("Environment"),
            Self::Static(_) => formatter.write_str("Static([REDACTED])"),
        }
    }
}

/// Explicit S3-compatible credentials whose debug output is always redacted.
pub struct LogArchiveStaticCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl fmt::Debug for LogArchiveStaticCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LogArchiveStaticCredentials([REDACTED])")
    }
}

impl LogArchiveStaticCredentials {
    /// Creates a static credential pair.
    #[must_use]
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
        }
    }

    /// Adds a short-lived session token.
    #[must_use]
    pub fn with_session_token(mut self, session_token: impl Into<String>) -> Self {
        self.session_token = Some(session_token.into());
        self
    }
}

/// Validated S3-compatible archive configuration.
#[derive(Debug)]
pub struct S3LogArchiveConfig {
    /// Bucket containing immutable Parquet segments and commit manifests.
    pub bucket: String,
    /// Signing region.
    pub region: String,
    /// Optional S3-compatible endpoint.
    pub endpoint: Option<String>,
    /// Namespace below the bucket.
    pub prefix: String,
    /// Whether bucket names are encoded in the hostname.
    pub virtual_hosted_style: bool,
    /// Explicit local/test opt-in for clear-text endpoints.
    pub allow_http: bool,
    /// Maximum duration of one object-store operation.
    pub operation_timeout: Duration,
    /// Credential source.
    pub credentials: LogArchiveCredentials,
}

impl S3LogArchiveConfig {
    /// Creates conservative defaults for one bucket and region.
    #[must_use]
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            prefix: "runku-logs".to_owned(),
            virtual_hosted_style: false,
            allow_http: false,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            credentials: LogArchiveCredentials::Environment,
        }
    }
}

/// Immutable committed archive segment metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogArchiveManifestV1 {
    /// Persisted manifest/Parquet contract version.
    pub format_version: u8,
    /// Exact Project boundary.
    pub project_id: String,
    /// Exact Environment boundary.
    pub environment_id: String,
    /// First repository cursor in the segment.
    pub first_cursor: u64,
    /// Last repository cursor in the segment.
    pub last_cursor: u64,
    /// Earliest event timestamp in the segment.
    pub first_occurred_at_micros: i64,
    /// Latest event timestamp in the segment.
    pub last_occurred_at_micros: i64,
    /// Exact number of rows in the segment.
    pub record_count: u32,
    /// Exact Parquet object byte count.
    pub parquet_bytes: u64,
    /// Lowercase SHA-256 of the Parquet bytes.
    pub parquet_sha256: String,
    /// Archive-relative immutable Parquet object key.
    pub object_key: String,
}

impl LogArchiveManifestV1 {
    fn scope(&self) -> Result<EnvironmentScope, LogRepositoryError> {
        Ok(EnvironmentScope::new(
            self.project_id
                .parse()
                .map_err(|_| LogRepositoryError::Corruption)?,
            self.environment_id
                .parse()
                .map_err(|_| LogRepositoryError::Corruption)?,
        ))
    }

    fn validate(&self) -> Result<(), LogRepositoryError> {
        let count = u64::from(self.record_count);
        if self.format_version != ARCHIVE_FORMAT_VERSION
            || self.first_cursor == 0
            || self.last_cursor < self.first_cursor
            || self.last_cursor - self.first_cursor + 1 != count
            || self.first_occurred_at_micros < 0
            || self.last_occurred_at_micros < self.first_occurred_at_micros
            || self.parquet_bytes == 0
            || self.parquet_bytes
                > u64::try_from(ARCHIVE_MAX_SEGMENT_BYTES)
                    .map_err(|_| LogRepositoryError::LimitExceeded)?
            || self.parquet_sha256.len() != 64
            || !self
                .parquet_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(LogRepositoryError::Corruption);
        }
        self.scope()?;
        let expected = parquet_object_key(
            self.scope()?,
            self.first_cursor,
            self.last_cursor,
            &self.parquet_sha256,
        );
        if self.object_key != expected {
            return Err(LogRepositoryError::Corruption);
        }
        Ok(())
    }
}

/// Aggregate archive state for one exact Environment.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogArchiveStatus {
    /// Number of committed immutable segments.
    pub segments: u32,
    /// Total committed rows.
    pub records: u64,
    /// Highest contiguous archived cursor, or zero for an empty archive.
    pub through: LogCursor,
    /// Total committed Parquet bytes.
    pub parquet_bytes: u64,
}

/// Result of one bounded hot-to-archive cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogArchiveRunOutcome {
    /// No hot records exist after the committed archive frontier.
    Idle {
        /// Current committed archive frontier.
        through: LogCursor,
    },
    /// One immutable segment was committed.
    Archived {
        /// Number of newly committed records.
        records: u32,
        /// New committed archive frontier.
        through: LogCursor,
    },
}

/// Filesystem or S3-compatible immutable Parquet Operational Log archive.
#[derive(Clone)]
pub struct LogArchive {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    operation_timeout: Duration,
}

impl fmt::Debug for LogArchive {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogArchive")
            .field("prefix", &self.prefix)
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl LogArchive {
    /// Opens a persistent local archive root.
    ///
    /// # Errors
    ///
    /// Rejects a relative/root/symlink path or an unavailable directory.
    pub async fn open_filesystem(root: PathBuf) -> Result<Self, LogRepositoryError> {
        if !root.is_absolute() || root.parent().is_none() {
            return Err(LogRepositoryError::InvalidRequest);
        }
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|_| LogRepositoryError::Unavailable)?;
        let metadata = tokio::fs::symlink_metadata(&root)
            .await
            .map_err(|_| LogRepositoryError::Unavailable)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(LogRepositoryError::InvalidRequest);
        }
        let store =
            LocalFileSystem::new_with_prefix(&root).map_err(|_| LogRepositoryError::Unavailable)?;
        Ok(Self {
            store: Arc::new(store),
            prefix: String::new(),
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        })
    }

    /// Opens an S3-compatible archive using the validated credential policy.
    ///
    /// # Errors
    ///
    /// Rejects unsafe endpoints, namespaces, credentials, or backend configuration.
    pub fn open_s3(config: &S3LogArchiveConfig) -> Result<Self, LogRepositoryError> {
        validate_s3_config(config)?;
        let mut builder = match &config.credentials {
            LogArchiveCredentials::Environment => AmazonS3Builder::from_env(),
            LogArchiveCredentials::Static(credentials) => {
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
        let store = builder
            .build()
            .map_err(|_| LogRepositoryError::Unavailable)?;
        Ok(Self {
            store: Arc::new(store),
            prefix: config.prefix.trim_matches('/').to_owned(),
            operation_timeout: config.operation_timeout,
        })
    }

    /// Commits one contiguous immutable segment, replaying an identical existing segment safely.
    ///
    /// # Errors
    ///
    /// Rejects empty, mixed-scope, non-contiguous, oversized, conflicting, or unavailable writes.
    pub async fn commit(
        &self,
        records: &[SequencedOperationalEvent],
    ) -> Result<LogArchiveManifestV1, LogRepositoryError> {
        validate_segment(records)?;
        let owned = records.to_vec();
        let parquet = tokio::task::spawn_blocking(move || encode_parquet(&owned))
            .await
            .map_err(|_| LogRepositoryError::Unavailable)??;
        if parquet.is_empty() || parquet.len() > ARCHIVE_MAX_SEGMENT_BYTES {
            return Err(LogRepositoryError::LimitExceeded);
        }
        let scope = records[0].event.scope;
        let first = records[0].cursor;
        let last = records[records.len() - 1].cursor;
        let digest = sha256_hex(&parquet);
        let object_key = parquet_object_key(scope, first.get(), last.get(), &digest);
        let first_time = records
            .iter()
            .map(|record| record.event.occurred_at.get())
            .min()
            .ok_or(LogRepositoryError::InvalidRequest)?;
        let last_time = records
            .iter()
            .map(|record| record.event.occurred_at.get())
            .max()
            .ok_or(LogRepositoryError::InvalidRequest)?;
        let manifest = LogArchiveManifestV1 {
            format_version: ARCHIVE_FORMAT_VERSION,
            project_id: scope.project_id().to_string(),
            environment_id: scope.environment_id().to_string(),
            first_cursor: first.get(),
            last_cursor: last.get(),
            first_occurred_at_micros: first_time,
            last_occurred_at_micros: last_time,
            record_count: u32::try_from(records.len())
                .map_err(|_| LogRepositoryError::LimitExceeded)?,
            parquet_bytes: u64::try_from(parquet.len())
                .map_err(|_| LogRepositoryError::LimitExceeded)?,
            parquet_sha256: digest,
            object_key,
        };
        manifest.validate()?;
        self.put_create_or_verify(&manifest.object_key, Bytes::from(parquet))
            .await?;
        let manifest_bytes =
            serde_json::to_vec(&manifest).map_err(|_| LogRepositoryError::InvalidRequest)?;
        if manifest_bytes.len() > ARCHIVE_MAX_MANIFEST_BYTES {
            return Err(LogRepositoryError::LimitExceeded);
        }
        self.put_create_or_verify(&manifest_object_key(&manifest), Bytes::from(manifest_bytes))
            .await?;
        Ok(manifest)
    }

    /// Returns and verifies the contiguous committed archive summary for one Environment.
    ///
    /// # Errors
    ///
    /// Fails closed on gaps, overlaps, malformed manifests, or unavailable object storage.
    pub async fn status(
        &self,
        scope: EnvironmentScope,
    ) -> Result<LogArchiveStatus, LogRepositoryError> {
        let manifests = self.manifests(scope).await?;
        let mut records = 0_u64;
        let mut bytes = 0_u64;
        for manifest in &manifests {
            records = records
                .checked_add(u64::from(manifest.record_count))
                .ok_or(LogRepositoryError::Corruption)?;
            bytes = bytes
                .checked_add(manifest.parquet_bytes)
                .ok_or(LogRepositoryError::Corruption)?;
        }
        Ok(LogArchiveStatus {
            segments: u32::try_from(manifests.len())
                .map_err(|_| LogRepositoryError::LimitExceeded)?,
            records,
            through: manifests.last().map_or(LogCursor::START, |manifest| {
                LogCursor::new(manifest.last_cursor)
            }),
            parquet_bytes: bytes,
        })
    }

    pub(crate) async fn query(
        &self,
        query: &LogQuery,
    ) -> Result<(Vec<SequencedOperationalEvent>, LogCursor), LogRepositoryError> {
        query.validate()?;
        let manifests = self.manifests(query.scope).await?;
        let through = manifests.last().map_or(LogCursor::START, |manifest| {
            LogCursor::new(manifest.last_cursor)
        });
        let mut records = Vec::with_capacity(usize::from(query.limit));
        for manifest in manifests
            .iter()
            .filter(|manifest| manifest.last_cursor > query.after.get())
        {
            let bytes = self.get_verified_segment(manifest).await?;
            let path_query = query.clone();
            let decoded = tokio::task::spawn_blocking(move || decode_parquet(&bytes, &path_query))
                .await
                .map_err(|_| LogRepositoryError::Unavailable)??;
            for record in decoded {
                if matches_query(&record, query) {
                    records.push(record);
                    if records.len() == usize::from(query.limit) {
                        return Ok((records, through));
                    }
                }
            }
        }
        Ok((records, through))
    }

    async fn manifests(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Vec<LogArchiveManifestV1>, LogRepositoryError> {
        let prefix = self.path(&scope_prefix(scope));
        let mut listed = self.store.list(Some(&prefix));
        let mut manifests = Vec::new();
        while let Some(meta) = timeout(self.operation_timeout, listed.try_next())
            .await?
            .map_err(|_| LogRepositoryError::Unavailable)?
        {
            let location = meta.location.to_string();
            if !location.ends_with(".manifest.json") {
                continue;
            }
            if manifests.len() >= ARCHIVE_MAX_MANIFESTS_PER_SCOPE {
                return Err(LogRepositoryError::LimitExceeded);
            }
            let result = timeout(self.operation_timeout, self.store.get(&meta.location))
                .await?
                .map_err(|_| LogRepositoryError::Unavailable)?;
            if result.meta.size > ARCHIVE_MAX_MANIFEST_BYTES as u64 {
                return Err(LogRepositoryError::Corruption);
            }
            let bytes = timeout(self.operation_timeout, result.bytes())
                .await?
                .map_err(|_| LogRepositoryError::Unavailable)?;
            let manifest: LogArchiveManifestV1 =
                serde_json::from_slice(&bytes).map_err(|_| LogRepositoryError::Corruption)?;
            manifest.validate()?;
            if manifest.scope()? != scope
                || self.path(&manifest_object_key(&manifest)) != meta.location
            {
                return Err(LogRepositoryError::Corruption);
            }
            manifests.push(manifest);
        }
        manifests.sort_by_key(|manifest| manifest.first_cursor);
        let mut expected = 1_u64;
        for manifest in &manifests {
            if manifest.first_cursor != expected {
                return Err(LogRepositoryError::Corruption);
            }
            expected = manifest
                .last_cursor
                .checked_add(1)
                .ok_or(LogRepositoryError::Corruption)?;
        }
        Ok(manifests)
    }

    async fn get_verified_segment(
        &self,
        manifest: &LogArchiveManifestV1,
    ) -> Result<Vec<u8>, LogRepositoryError> {
        let result = timeout(
            self.operation_timeout,
            self.store.get(&self.path(&manifest.object_key)),
        )
        .await?
        .map_err(|_| LogRepositoryError::Unavailable)?;
        if result.meta.size != manifest.parquet_bytes {
            return Err(LogRepositoryError::Corruption);
        }
        let bytes = timeout(self.operation_timeout, result.bytes())
            .await?
            .map_err(|_| LogRepositoryError::Unavailable)?;
        if bytes.len() > ARCHIVE_MAX_SEGMENT_BYTES || sha256_hex(&bytes) != manifest.parquet_sha256
        {
            return Err(LogRepositoryError::Corruption);
        }
        Ok(bytes.to_vec())
    }

    async fn put_create_or_verify(
        &self,
        relative: &str,
        bytes: Bytes,
    ) -> Result<(), LogRepositoryError> {
        let location = self.path(relative);
        let result = timeout(
            self.operation_timeout,
            self.store.put_opts(
                &location,
                bytes.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            ),
        )
        .await?;
        match result {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = timeout(self.operation_timeout, self.store.get(&location))
                    .await?
                    .map_err(|_| LogRepositoryError::Unavailable)?;
                let existing = timeout(self.operation_timeout, existing.bytes())
                    .await?
                    .map_err(|_| LogRepositoryError::Unavailable)?;
                if existing == bytes {
                    Ok(())
                } else {
                    Err(LogRepositoryError::Corruption)
                }
            }
            Err(_) => Err(LogRepositoryError::Unavailable),
        }
    }

    fn path(&self, relative: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(relative)
        } else {
            ObjectPath::from(format!("{}/{relative}", self.prefix))
        }
    }
}

/// Repository composition that writes to a hot store and reads transparently across Parquet and hot tiers.
#[derive(Clone, Debug)]
pub struct TieredLogRepository {
    hot: Arc<dyn LogRepository>,
    archive: LogArchive,
}

impl TieredLogRepository {
    /// Creates a tiered repository over one exact hot repository and archive namespace.
    #[must_use]
    pub fn new(hot: Arc<dyn LogRepository>, archive: LogArchive) -> Self {
        Self { hot, archive }
    }
}

#[async_trait]
impl LogRepository for TieredLogRepository {
    fn backend(&self) -> LogRepositoryBackend {
        self.hot.backend()
    }

    async fn append(&self, events: &[OperationalEventV1]) -> Result<LogCursor, LogRepositoryError> {
        self.hot.append(events).await
    }

    async fn query(&self, query: &LogQuery) -> Result<LogPage, LogRepositoryError> {
        let (mut archived, through) = self.archive.query(query).await?;
        if archived.len() == usize::from(query.limit) {
            let next = archived.last().map_or(query.after, |record| record.cursor);
            return Ok(LogPage {
                records: archived,
                next,
            });
        }
        let remaining = usize::from(query.limit).saturating_sub(archived.len());
        let mut hot_query = query.clone();
        hot_query.after = query.after.max(through);
        hot_query.limit =
            u16::try_from(remaining).map_err(|_| LogRepositoryError::LimitExceeded)?;
        let hot = self.hot.query(&hot_query).await?;
        let archived_ids = archived
            .iter()
            .map(|record| record.event.id)
            .collect::<BTreeSet<_>>();
        archived.extend(
            hot.records
                .into_iter()
                .filter(|record| !archived_ids.contains(&record.event.id)),
        );
        let next = archived.last().map_or(query.after, |record| record.cursor);
        Ok(LogPage {
            records: archived,
            next,
        })
    }

    async fn prune_before(
        &self,
        scope: EnvironmentScope,
        cutoff: TimestampMicros,
        maximum: u32,
        dry_run: bool,
    ) -> Result<PruneResult, LogRepositoryError> {
        let status = self.archive.status(scope).await?;
        self.hot
            .prune_archived_before(scope, cutoff, status.through, maximum, dry_run)
            .await
    }

    async fn prune_archived_before(
        &self,
        scope: EnvironmentScope,
        cutoff: TimestampMicros,
        archived_through: LogCursor,
        maximum: u32,
        dry_run: bool,
    ) -> Result<PruneResult, LogRepositoryError> {
        let status = self.archive.status(scope).await?;
        self.hot
            .prune_archived_before(
                scope,
                cutoff,
                archived_through.min(status.through),
                maximum,
                dry_run,
            )
            .await
    }

    async fn close(&self) {
        self.hot.close().await;
    }
}

/// Bounded idempotent hot-store to Parquet archive coordinator.
#[derive(Clone, Debug)]
pub struct LogArchiver {
    source: Arc<dyn LogRepository>,
    archive: LogArchive,
    scope: EnvironmentScope,
    maximum_batch_records: u16,
}

impl LogArchiver {
    /// Creates an archiver for one exact Environment.
    ///
    /// # Errors
    ///
    /// Rejects a zero or oversized segment batch.
    pub fn new(
        source: Arc<dyn LogRepository>,
        archive: LogArchive,
        scope: EnvironmentScope,
        maximum_batch_records: u16,
    ) -> Result<Self, LogRepositoryError> {
        if !(1..=LOG_QUERY_MAX_RECORDS).contains(&maximum_batch_records) {
            return Err(LogRepositoryError::LimitExceeded);
        }
        Ok(Self {
            source,
            archive,
            scope,
            maximum_batch_records,
        })
    }

    /// Archives at most one segment and advances only through a committed manifest.
    ///
    /// # Errors
    ///
    /// Fails closed when the hot source no longer contains the next required cursor.
    pub async fn run_once(&self) -> Result<LogArchiveRunOutcome, LogRepositoryError> {
        let status = self.archive.status(self.scope).await?;
        let page = self
            .source
            .query(&LogQuery {
                scope: self.scope,
                after: status.through,
                limit: self.maximum_batch_records,
                stream: None,
                minimum_level: None,
                function_id: None,
                request_id: None,
                invocation_id: None,
                client_id: None,
                credential_id: None,
                release_id: None,
            })
            .await?;
        if page.records.is_empty() {
            return Ok(LogArchiveRunOutcome::Idle {
                through: status.through,
            });
        }
        let expected = status
            .through
            .get()
            .checked_add(1)
            .ok_or(LogRepositoryError::Corruption)?;
        if page.records[0].cursor.get() != expected {
            return Err(LogRepositoryError::Corruption);
        }
        let manifest = self.archive.commit(&page.records).await?;
        Ok(LogArchiveRunOutcome::Archived {
            records: manifest.record_count,
            through: LogCursor::new(manifest.last_cursor),
        })
    }
}

async fn timeout<T>(
    duration: Duration,
    future: impl std::future::Future<Output = Result<T, object_store::Error>>,
) -> Result<Result<T, object_store::Error>, LogRepositoryError> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| LogRepositoryError::Unavailable)
}

fn validate_s3_config(config: &S3LogArchiveConfig) -> Result<(), LogRepositoryError> {
    if config.bucket.is_empty()
        || config.region.is_empty()
        || config.operation_timeout.is_zero()
        || config.prefix.len() > 256
        || config
            .prefix
            .split('/')
            .any(|part| matches!(part, "." | ".."))
        || config.prefix.contains(['\\', '\0'])
    {
        return Err(LogRepositoryError::InvalidRequest);
    }
    if let Some(endpoint) = &config.endpoint {
        let endpoint = url::Url::parse(endpoint).map_err(|_| LogRepositoryError::InvalidRequest)?;
        let loopback = endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
        if endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !(endpoint.scheme() == "https"
                || endpoint.scheme() == "http" && config.allow_http && loopback)
        {
            return Err(LogRepositoryError::InvalidRequest);
        }
    } else if config.allow_http {
        return Err(LogRepositoryError::InvalidRequest);
    }
    if let LogArchiveCredentials::Static(credentials) = &config.credentials
        && (credentials.access_key_id.is_empty()
            || credentials.secret_access_key.is_empty()
            || credentials
                .session_token
                .as_ref()
                .is_some_and(String::is_empty))
    {
        return Err(LogRepositoryError::InvalidRequest);
    }
    Ok(())
}

fn validate_segment(records: &[SequencedOperationalEvent]) -> Result<(), LogRepositoryError> {
    if records.is_empty() || records.len() > usize::from(LOG_QUERY_MAX_RECORDS) {
        return Err(LogRepositoryError::LimitExceeded);
    }
    let scope = records[0].event.scope;
    let mut expected = records[0].cursor.get();
    let mut ids = BTreeSet::new();
    for record in records {
        record
            .event
            .validate()
            .map_err(|_| LogRepositoryError::InvalidRequest)?;
        if record.cursor.get() == 0
            || record.cursor.get() != expected
            || record.event.scope != scope
            || !ids.insert(record.event.id)
        {
            return Err(LogRepositoryError::InvalidRequest);
        }
        expected = expected
            .checked_add(1)
            .ok_or(LogRepositoryError::LimitExceeded)?;
    }
    Ok(())
}

fn scope_prefix(scope: EnvironmentScope) -> String {
    format!(
        "v1/projects/{}/environments/{}/segments",
        scope.project_id(),
        scope.environment_id()
    )
}

fn parquet_object_key(scope: EnvironmentScope, first: u64, last: u64, digest: &str) -> String {
    format!(
        "{}/{first:020}-{last:020}-{}.parquet",
        scope_prefix(scope),
        &digest[..16]
    )
}

fn manifest_object_key(manifest: &LogArchiveManifestV1) -> String {
    format!("{}.manifest.json", manifest.object_key)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[allow(clippy::too_many_lines)]
fn encode_parquet(records: &[SequencedOperationalEvent]) -> Result<Vec<u8>, LogRepositoryError> {
    let directory = tempfile::tempdir().map_err(|_| LogRepositoryError::Unavailable)?;
    let path = directory.path().join("segment.parquet");
    let connection = Connection::open_in_memory().map_err(map_duckdb)?;
    configure_duckdb(&connection)?;
    connection
        .execute_batch(
            "CREATE TABLE logs (format_version UTINYINT NOT NULL, sequence BIGINT NOT NULL, event_id VARCHAR NOT NULL, project_id VARCHAR NOT NULL, environment_id VARCHAR NOT NULL, occurred_at_micros BIGINT NOT NULL, request_id VARCHAR NOT NULL, invocation_id VARCHAR NOT NULL, parent_invocation_id VARCHAR, release_id VARCHAR NOT NULL, dev_revision_id VARCHAR, function_id VARCHAR NOT NULL, function_name VARCHAR NOT NULL, function_type VARCHAR NOT NULL, client_id VARCHAR, credential_id VARCHAR, principal_kind VARCHAR NOT NULL, stream VARCHAR NOT NULL, level VARCHAR NOT NULL, event_kind VARCHAR NOT NULL, message VARCHAR, fields BLOB, duration_micros BIGINT, outcome_code VARCHAR)",
        )
        .map_err(map_duckdb)?;
    let mut statement = connection
        .prepare("INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .map_err(map_duckdb)?;
    for record in records {
        let event = &record.event;
        let fields = event
            .fields
            .as_ref()
            .map(encode_stored_value)
            .transpose()
            .map_err(|_| LogRepositoryError::InvalidRequest)?;
        statement
            .execute(params![
                    ARCHIVE_FORMAT_VERSION,
                    i64::try_from(record.cursor.get())
                        .map_err(|_| LogRepositoryError::LimitExceeded)?,
                    event.id.to_string(),
                    event.scope.project_id().to_string(),
                    event.scope.environment_id().to_string(),
                    event.occurred_at.get(),
                    event.request_id.to_string(),
                    event.invocation_id.to_string(),
                    event.parent_invocation_id.map(|value| value.to_string()),
                    event.release_id.to_string(),
                    event.dev_revision_id.map(|value| value.to_string()),
                    event.function_id.to_string(),
                    event.function_name.as_str(),
                    function_type_text(event.function_type),
                    event.client_id.map(|value| value.to_string()),
                    event.credential_id.map(|value| value.to_string()),
                    event.principal_kind.as_str(),
                    event.stream.as_str(),
                    event.level.as_str(),
                    event.kind.as_str(),
                    event.message.as_ref().map(LogMessage::as_str),
                    fields,
                    event
                        .duration_micros
                        .map(|value| i64::try_from(value)
                            .map_err(|_| LogRepositoryError::LimitExceeded))
                        .transpose()?,
                    event.outcome_code.as_ref().map(OutcomeCode::as_str),
                ])
            .map_err(map_duckdb)?;
    }
    drop(statement);
    let path = path
        .to_str()
        .ok_or(LogRepositoryError::Unavailable)?
        .replace('\'', "''");
    connection
        .execute_batch(&format!(
            "COPY logs TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        ))
        .map_err(map_duckdb)?;
    std::fs::read(directory.path().join("segment.parquet"))
        .map_err(|_| LogRepositoryError::Unavailable)
}

#[allow(clippy::too_many_lines)]
fn decode_parquet(
    bytes: &[u8],
    query: &LogQuery,
) -> Result<Vec<SequencedOperationalEvent>, LogRepositoryError> {
    if bytes.is_empty() || bytes.len() > ARCHIVE_MAX_SEGMENT_BYTES {
        return Err(LogRepositoryError::Corruption);
    }
    let directory = tempfile::tempdir().map_err(|_| LogRepositoryError::Unavailable)?;
    let path = directory.path().join("segment.parquet");
    std::fs::write(&path, bytes).map_err(|_| LogRepositoryError::Unavailable)?;
    let connection = Connection::open_in_memory().map_err(map_duckdb)?;
    configure_duckdb(&connection)?;
    let path = path.to_str().ok_or(LogRepositoryError::Unavailable)?;
    let mut statement = connection
        .prepare("SELECT format_version, sequence, event_id, project_id, environment_id, occurred_at_micros, request_id, invocation_id, parent_invocation_id, release_id, dev_revision_id, function_id, function_name, function_type, client_id, credential_id, principal_kind, stream, level, event_kind, message, fields, duration_micros, outcome_code FROM read_parquet(?) WHERE project_id = ? AND environment_id = ? AND sequence > ? ORDER BY sequence ASC")
        .map_err(map_duckdb)?;
    let rows = statement
        .query_map(
            params![
                path,
                query.scope.project_id().to_string(),
                query.scope.environment_id().to_string(),
                i64::try_from(query.after.get()).map_err(|_| LogRepositoryError::InvalidRequest)?,
            ],
            |row| {
                Ok(ArchivedRow {
                    format_version: row.get(0)?,
                    sequence: row.get(1)?,
                    event_id: row.get(2)?,
                    project_id: row.get(3)?,
                    environment_id: row.get(4)?,
                    occurred_at_micros: row.get(5)?,
                    request_id: row.get(6)?,
                    invocation_id: row.get(7)?,
                    parent_invocation_id: row.get(8)?,
                    release_id: row.get(9)?,
                    dev_revision_id: row.get(10)?,
                    function_id: row.get(11)?,
                    function_name: row.get(12)?,
                    function_type: row.get(13)?,
                    client_id: row.get(14)?,
                    credential_id: row.get(15)?,
                    principal_kind: row.get(16)?,
                    stream: row.get(17)?,
                    level: row.get(18)?,
                    event_kind: row.get(19)?,
                    message: row.get(20)?,
                    fields: row.get(21)?,
                    duration_micros: row.get(22)?,
                    outcome_code: row.get(23)?,
                })
            },
        )
        .map_err(map_duckdb)?;
    rows.map(|row| row.map_err(map_duckdb).and_then(ArchivedRow::decode))
        .collect()
}

struct ArchivedRow {
    format_version: u8,
    sequence: i64,
    event_id: String,
    project_id: String,
    environment_id: String,
    occurred_at_micros: i64,
    request_id: String,
    invocation_id: String,
    parent_invocation_id: Option<String>,
    release_id: String,
    dev_revision_id: Option<String>,
    function_id: String,
    function_name: String,
    function_type: String,
    client_id: Option<String>,
    credential_id: Option<String>,
    principal_kind: String,
    stream: String,
    level: String,
    event_kind: String,
    message: Option<String>,
    fields: Option<Vec<u8>>,
    duration_micros: Option<i64>,
    outcome_code: Option<String>,
}

impl ArchivedRow {
    fn decode(self) -> Result<SequencedOperationalEvent, LogRepositoryError> {
        if self.format_version != ARCHIVE_FORMAT_VERSION {
            return Err(LogRepositoryError::Corruption);
        }
        let event = OperationalEventV1 {
            id: parse(&self.event_id)?,
            occurred_at: TimestampMicros::new(self.occurred_at_micros),
            scope: EnvironmentScope::new(parse(&self.project_id)?, parse(&self.environment_id)?),
            request_id: parse(&self.request_id)?,
            invocation_id: parse(&self.invocation_id)?,
            parent_invocation_id: parse_optional(self.parent_invocation_id.as_deref())?,
            release_id: parse(&self.release_id)?,
            dev_revision_id: parse_optional(self.dev_revision_id.as_deref())?,
            function_id: parse(&self.function_id)?,
            function_name: FunctionName::from_str(&self.function_name)
                .map_err(|_| LogRepositoryError::Corruption)?,
            function_type: parse_function_type(&self.function_type)?,
            client_id: parse_optional(self.client_id.as_deref())?,
            credential_id: parse_optional(self.credential_id.as_deref())?,
            principal_kind: parse_principal(&self.principal_kind)?,
            stream: parse_stream(&self.stream)?,
            level: parse_level(&self.level)?,
            kind: parse_kind(&self.event_kind)?,
            message: self
                .message
                .map(LogMessage::new)
                .transpose()
                .map_err(|_| LogRepositoryError::Corruption)?,
            fields: self
                .fields
                .map(|bytes| decode_stored_value(&bytes))
                .transpose()
                .map_err(|_| LogRepositoryError::Corruption)?,
            duration_micros: self
                .duration_micros
                .map(|value| u64::try_from(value).map_err(|_| LogRepositoryError::Corruption))
                .transpose()?,
            outcome_code: self
                .outcome_code
                .map(OutcomeCode::new)
                .transpose()
                .map_err(|_| LogRepositoryError::Corruption)?,
        };
        event
            .validate()
            .map_err(|_| LogRepositoryError::Corruption)?;
        Ok(SequencedOperationalEvent {
            cursor: LogCursor::new(
                u64::try_from(self.sequence).map_err(|_| LogRepositoryError::Corruption)?,
            ),
            event,
        })
    }
}

fn configure_duckdb(connection: &Connection) -> Result<(), LogRepositoryError> {
    connection
        .execute_batch(&format!(
            "SET threads = 1; SET memory_limit = '{DEFAULT_QUERY_MEMORY_LIMIT}'; SET autoinstall_known_extensions = false; SET autoload_known_extensions = false"
        ))
        .map_err(map_duckdb)
}

fn matches_query(record: &SequencedOperationalEvent, query: &LogQuery) -> bool {
    let event = &record.event;
    record.cursor > query.after
        && event.scope == query.scope
        && query.stream.is_none_or(|value| event.stream == value)
        && query
            .minimum_level
            .is_none_or(|value| level_rank(event.level) >= level_rank(value))
        && query
            .function_id
            .is_none_or(|value| event.function_id == value)
        && query
            .request_id
            .is_none_or(|value| event.request_id == value)
        && query
            .invocation_id
            .is_none_or(|value| event.invocation_id == value)
        && query
            .client_id
            .is_none_or(|value| event.client_id == Some(value))
        && query
            .credential_id
            .is_none_or(|value| event.credential_id == Some(value))
        && query
            .release_id
            .is_none_or(|value| event.release_id == value)
}

fn parse<T: FromStr>(value: &str) -> Result<T, LogRepositoryError> {
    value.parse().map_err(|_| LogRepositoryError::Corruption)
}

fn parse_optional<T: FromStr>(value: Option<&str>) -> Result<Option<T>, LogRepositoryError> {
    value.map(parse).transpose()
}

const fn function_type_text(value: FunctionType) -> &'static str {
    match value {
        FunctionType::Query => "query",
        FunctionType::Mutation => "mutation",
        FunctionType::Action => "action",
    }
}

fn parse_function_type(value: &str) -> Result<FunctionType, LogRepositoryError> {
    match value {
        "query" => Ok(FunctionType::Query),
        "mutation" => Ok(FunctionType::Mutation),
        "action" => Ok(FunctionType::Action),
        _ => Err(LogRepositoryError::Corruption),
    }
}

const fn level_rank(value: LogLevel) -> u8 {
    match value {
        LogLevel::Debug => 10,
        LogLevel::Info => 20,
        LogLevel::Warn => 30,
        LogLevel::Error => 40,
    }
}

fn parse_level(value: &str) -> Result<LogLevel, LogRepositoryError> {
    match value {
        "debug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" => Ok(LogLevel::Warn),
        "error" => Ok(LogLevel::Error),
        _ => Err(LogRepositoryError::Corruption),
    }
}

fn parse_stream(value: &str) -> Result<LogStream, LogRepositoryError> {
    match value {
        "platform" => Ok(LogStream::Platform),
        "function" => Ok(LogStream::Function),
        _ => Err(LogRepositoryError::Corruption),
    }
}

fn parse_kind(value: &str) -> Result<LogEventKind, LogRepositoryError> {
    match value {
        "invocation_started" => Ok(LogEventKind::InvocationStarted),
        "invocation_completed" => Ok(LogEventKind::InvocationCompleted),
        "function_message" => Ok(LogEventKind::FunctionMessage),
        _ => Err(LogRepositoryError::Corruption),
    }
}

fn parse_principal(value: &str) -> Result<LogPrincipalKind, LogRepositoryError> {
    match value {
        "none" => Ok(LogPrincipalKind::None),
        "guest" => Ok(LogPrincipalKind::Guest),
        "user" => Ok(LogPrincipalKind::User),
        "service" => Ok(LogPrincipalKind::Service),
        "system" => Ok(LogPrincipalKind::System),
        _ => Err(LogRepositoryError::Corruption),
    }
}

fn map_duckdb(_error: duckdb::Error) -> LogRepositoryError {
    LogRepositoryError::Corruption
}
