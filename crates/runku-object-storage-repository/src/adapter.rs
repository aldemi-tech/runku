//! SQL-backed Object Storage repository with equivalent SQLite/PostgreSQL semantics.

#![allow(clippy::missing_errors_doc)]

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId};
use runku_object_storage::{
    AccessKeyConfiguration, AccessKeyId, AccessKeyMetadata, AccessKeyPage, AccessKeyPageRequest,
    AccessKeyState, AuditEvent, AuditPage, AuditPageRequest, Bucket, BucketId, BucketPage,
    BucketPageRequest, BucketState, DeleteObjectCommand, EncryptedAccessKeyGeneration,
    EncryptedAccessKeySecret, ObjectMetadata, ObjectOperation, ObjectOperationResult, ObjectPage,
    ObjectPageRequest, ObjectStorageActor, ObjectStorageCommand, ObjectStorageError,
    ObjectStorageOperation, ObjectStorageOperationResult, ObjectStorageRepository,
    ObjectStorageRepositoryBackend, ObjectStorageTelemetrySnapshot, ObjectVersionId,
    PutObjectCommand, object_etag,
};
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};
use sqlx::{
    Any, AnyPool, Executor, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};

const MIGRATION_1: &[&str] = &[
    "CREATE TABLE runku_storage_buckets (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, bucket_id TEXT NOT NULL, name TEXT NOT NULL, configuration_json TEXT NOT NULL, revision BIGINT NOT NULL CHECK(revision > 0), state TEXT NOT NULL CHECK(state IN ('active','archived')), created_at_micros BIGINT NOT NULL CHECK(created_at_micros >= 0), updated_at_micros BIGINT NOT NULL CHECK(updated_at_micros >= created_at_micros), PRIMARY KEY(project_id,environment_id,bucket_id), UNIQUE(project_id,environment_id,name))",
    "CREATE INDEX runku_storage_buckets_by_environment ON runku_storage_buckets(project_id,environment_id,bucket_id)",
    "CREATE TABLE runku_storage_access_keys (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, bucket_id TEXT NOT NULL, access_key_id TEXT NOT NULL, configuration_json TEXT NOT NULL, revision BIGINT NOT NULL CHECK(revision > 0), state TEXT NOT NULL CHECK(state IN ('active','revoked')), created_at_micros BIGINT NOT NULL CHECK(created_at_micros >= 0), updated_at_micros BIGINT NOT NULL CHECK(updated_at_micros >= created_at_micros), previous_generation_valid_until_micros BIGINT NULL, PRIMARY KEY(project_id,environment_id,bucket_id,access_key_id), FOREIGN KEY(project_id,environment_id,bucket_id) REFERENCES runku_storage_buckets(project_id,environment_id,bucket_id) ON DELETE RESTRICT)",
    "CREATE INDEX runku_storage_access_keys_by_bucket ON runku_storage_access_keys(project_id,environment_id,bucket_id,access_key_id)",
    "CREATE TABLE runku_storage_access_key_generations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, bucket_id TEXT NOT NULL, access_key_id TEXT NOT NULL, generation BIGINT NOT NULL CHECK(generation > 0), secret_digest BYTEA NOT NULL CHECK(length(secret_digest)=32), valid_from_micros BIGINT NOT NULL CHECK(valid_from_micros >= 0), valid_until_micros BIGINT NULL, revoked_at_micros BIGINT NULL, PRIMARY KEY(project_id,environment_id,bucket_id,access_key_id,generation), FOREIGN KEY(project_id,environment_id,bucket_id,access_key_id) REFERENCES runku_storage_access_keys(project_id,environment_id,bucket_id,access_key_id) ON DELETE RESTRICT)",
    "CREATE TABLE runku_storage_operations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, command_digest BYTEA NOT NULL CHECK(length(command_digest)=32), kind TEXT NOT NULL CHECK(kind IN ('create_bucket','update_bucket','archive_bucket','issue_access_key','rotate_access_key','revoke_access_key')), bucket_id TEXT NOT NULL, access_key_id TEXT NULL, revision BIGINT NOT NULL CHECK(revision > 0), completed_at_micros BIGINT NOT NULL CHECK(completed_at_micros >= 0), PRIMARY KEY(project_id,environment_id,operation_id))",
    "CREATE TABLE runku_storage_audit (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, sequence BIGINT NOT NULL CHECK(sequence > 0), operation_id TEXT NOT NULL, kind TEXT NOT NULL, actor TEXT NOT NULL, bucket_id TEXT NOT NULL, access_key_id TEXT NULL, revision BIGINT NOT NULL CHECK(revision > 0), occurred_at_micros BIGINT NOT NULL CHECK(occurred_at_micros >= 0), PRIMARY KEY(project_id,environment_id,sequence), UNIQUE(project_id,environment_id,operation_id))",
];
const MIGRATION_2: &[&str] = &[
    "CREATE TABLE runku_storage_objects (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, bucket_id TEXT NOT NULL, object_key TEXT NOT NULL, version_id TEXT NOT NULL, size_bytes BIGINT NOT NULL CHECK(size_bytes >= 0), sha256 BYTEA NOT NULL CHECK(length(sha256)=32), etag TEXT NOT NULL, content_type TEXT NOT NULL, metadata_json TEXT NOT NULL, created_at_micros BIGINT NOT NULL CHECK(created_at_micros >= 0), PRIMARY KEY(project_id,environment_id,bucket_id,object_key), FOREIGN KEY(project_id,environment_id,bucket_id) REFERENCES runku_storage_buckets(project_id,environment_id,bucket_id) ON DELETE RESTRICT)",
    "CREATE INDEX runku_storage_objects_by_prefix ON runku_storage_objects(project_id,environment_id,bucket_id,object_key)",
    "CREATE TABLE runku_storage_object_versions (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, bucket_id TEXT NOT NULL, object_key TEXT NOT NULL, version_id TEXT NOT NULL, size_bytes BIGINT NOT NULL CHECK(size_bytes >= 0), sha256 BYTEA NOT NULL CHECK(length(sha256)=32), etag TEXT NOT NULL, content_type TEXT NOT NULL, metadata_json TEXT NOT NULL, created_at_micros BIGINT NOT NULL CHECK(created_at_micros >= 0), PRIMARY KEY(project_id,environment_id,bucket_id,object_key,version_id), FOREIGN KEY(project_id,environment_id,bucket_id) REFERENCES runku_storage_buckets(project_id,environment_id,bucket_id) ON DELETE RESTRICT)",
    "CREATE INDEX runku_storage_object_versions_by_key ON runku_storage_object_versions(project_id,environment_id,bucket_id,object_key,created_at_micros,version_id)",
    "CREATE TABLE runku_storage_object_operations (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, operation_id TEXT NOT NULL, command_digest BYTEA NOT NULL CHECK(length(command_digest)=32), kind TEXT NOT NULL CHECK(kind IN ('put','delete')), bucket_id TEXT NOT NULL, object_key TEXT NOT NULL, version_id TEXT NOT NULL, completed_at_micros BIGINT NOT NULL CHECK(completed_at_micros >= 0), PRIMARY KEY(project_id,environment_id,operation_id))",
    "CREATE TABLE runku_storage_object_audit (project_id TEXT NOT NULL, environment_id TEXT NOT NULL, sequence BIGINT NOT NULL CHECK(sequence > 0), operation_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('put','delete')), actor TEXT NOT NULL, bucket_id TEXT NOT NULL, object_key TEXT NOT NULL, version_id TEXT NOT NULL, size_bytes BIGINT NULL CHECK(size_bytes IS NULL OR size_bytes >= 0), occurred_at_micros BIGINT NOT NULL CHECK(occurred_at_micros >= 0), PRIMARY KEY(project_id,environment_id,sequence), UNIQUE(project_id,environment_id,operation_id))",
];
const MIGRATION_3: &[&str] = &[
    "ALTER TABLE runku_storage_access_key_generations ADD COLUMN secret_nonce BYTEA NULL",
    "ALTER TABLE runku_storage_access_key_generations ADD COLUMN secret_ciphertext BYTEA NULL",
    "CREATE INDEX runku_storage_access_key_generations_by_key ON runku_storage_access_key_generations(project_id,environment_id,access_key_id,generation)",
];
const MIGRATIONS: &[(i64, &[&str])] = &[(1, MIGRATION_1), (2, MIGRATION_2), (3, MIGRATION_3)];

/// Operational role selected for repository composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryRole {
    /// Local/test SQLite role.
    Local,
    /// Authoritative production PostgreSQL role.
    Production,
}

/// Bounded pool and acquisition policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectStorageRepositoryConfig {
    /// Declared operational role.
    pub role: RepositoryRole,
    /// Maximum connections.
    pub max_connections: u32,
    /// Maximum acquisition wait.
    pub acquire_timeout: Duration,
}

impl ObjectStorageRepositoryConfig {
    /// Deterministic local/test SQLite configuration.
    pub const LOCAL: Self = Self {
        role: RepositoryRole::Local,
        max_connections: 1,
        acquire_timeout: Duration::from_secs(5),
    };
    /// Bounded authoritative PostgreSQL configuration.
    pub const PRODUCTION: Self = Self {
        role: RepositoryRole::Production,
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
    };
}

#[derive(Debug, Default)]
struct Counters {
    commands: AtomicU64,
    replays: AtomicU64,
    conflicts: AtomicU64,
    reads: AtomicU64,
    retryable_errors: AtomicU64,
}

/// Durable SQL Object Storage repository.
#[derive(Clone, Debug)]
pub struct SqlObjectStorageRepository {
    pool: AnyPool,
    backend: ObjectStorageRepositoryBackend,
    counters: Arc<Counters>,
}

impl SqlObjectStorageRepository {
    /// Connects local SQLite and applies checksum-protected append-only migrations.
    pub async fn connect_sqlite(
        url: &str,
        config: ObjectStorageRepositoryConfig,
    ) -> Result<Self, ObjectStorageError> {
        if config.role == RepositoryRole::Production {
            return Err(ObjectStorageError::ProductionBackendUnsupported);
        }
        if !url.starts_with("sqlite:") {
            return Err(ObjectStorageError::Unavailable);
        }
        Self::connect(url, config, ObjectStorageRepositoryBackend::SQLite).await
    }
    /// Connects PostgreSQL 16+ and applies checksum-protected append-only migrations.
    pub async fn connect_postgres(
        url: &str,
        config: ObjectStorageRepositoryConfig,
    ) -> Result<Self, ObjectStorageError> {
        if config.role != RepositoryRole::Production {
            return Err(ObjectStorageError::ProductionBackendUnsupported);
        }
        if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            return Err(ObjectStorageError::Unavailable);
        }
        Self::connect(url, config, ObjectStorageRepositoryBackend::PostgreSQL).await
    }
    async fn connect(
        url: &str,
        config: ObjectStorageRepositoryConfig,
        backend: ObjectStorageRepositoryBackend,
    ) -> Result<Self, ObjectStorageError> {
        if config.max_connections == 0
            || config.max_connections > 64
            || config.acquire_timeout.is_zero()
            || (backend == ObjectStorageRepositoryBackend::SQLite && config.max_connections != 1)
        {
            return Err(ObjectStorageError::LimitExceeded);
        }
        sqlx::any::install_default_drivers();
        let options =
            AnyConnectOptions::from_str(url).map_err(|_| ObjectStorageError::Unavailable)?;
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    match backend {
                        ObjectStorageRepositoryBackend::SQLite => {
                            connection.execute("PRAGMA foreign_keys = ON").await?;
                            connection.execute("PRAGMA journal_mode = WAL").await?;
                            connection.execute("PRAGMA synchronous = FULL").await?;
                            connection.execute("PRAGMA busy_timeout = 5000").await?;
                        }
                        ObjectStorageRepositoryBackend::PostgreSQL => {
                            connection.execute("SET statement_timeout = '30s'").await?;
                            connection.execute("SET lock_timeout = '5s'").await?;
                            connection
                                .execute("SET idle_in_transaction_session_timeout = '30s'")
                                .await?;
                        }
                    }
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(map_sqlx_error)?;
        if backend == ObjectStorageRepositoryBackend::PostgreSQL {
            let version = sqlx::query_scalar::<_, i64>(
                "SELECT current_setting('server_version_num')::bigint",
            )
            .fetch_one(&pool)
            .await
            .map_err(map_sqlx_error)?;
            if version < 160_000 {
                pool.close().await;
                return Err(ObjectStorageError::Unsupported);
            }
        }
        verify_configuration(&pool, backend).await?;
        migrate(&pool, backend).await?;
        Ok(Self {
            pool,
            backend,
            counters: Arc::new(Counters::default()),
        })
    }
    /// Closes the bounded pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

#[async_trait]
impl ObjectStorageRepository for SqlObjectStorageRepository {
    fn backend(&self) -> ObjectStorageRepositoryBackend {
        self.backend
    }
    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &ObjectStorageCommand,
    ) -> Result<ObjectStorageOperationResult, ObjectStorageError> {
        let result = apply(&self.pool, self.backend, scope, operation_id, command).await;
        match &result {
            Ok(value) if value.replayed => {
                self.counters.replays.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => {
                self.counters.commands.fetch_add(1, Ordering::Relaxed);
            }
            Err(ObjectStorageError::Conflict | ObjectStorageError::OperationIdReused) => {
                self.counters.conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.retryable() => {
                self.counters
                    .retryable_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {}
        }
        result
    }
    async fn get_bucket(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
    ) -> Result<Option<Bucket>, ObjectStorageError> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        load_bucket(&self.pool, scope, bucket_id).await
    }
    async fn list_buckets(
        &self,
        scope: EnvironmentScope,
        request: BucketPageRequest,
    ) -> Result<BucketPage, ObjectStorageError> {
        request.validate()?;
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        list_buckets(&self.pool, scope, request).await
    }
    async fn get_access_key(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        access_key_id: AccessKeyId,
    ) -> Result<Option<AccessKeyMetadata>, ObjectStorageError> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        load_key(&self.pool, scope, bucket_id, access_key_id).await
    }
    async fn list_access_keys(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: AccessKeyPageRequest,
    ) -> Result<AccessKeyPage, ObjectStorageError> {
        request.validate()?;
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        list_keys(&self.pool, scope, bucket_id, request).await
    }
    async fn authenticate_access_key(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        access_key_id: AccessKeyId,
        secret_digest: &runku_object_storage::SecretDigest,
        at: TimestampMicros,
    ) -> Result<Option<AccessKeyMetadata>, ObjectStorageError> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        authenticate_key(
            &self.pool,
            scope,
            bucket_id,
            access_key_id,
            secret_digest.as_bytes(),
            at,
        )
        .await
    }
    async fn encrypted_access_key_generations(
        &self,
        scope: EnvironmentScope,
        access_key_id: AccessKeyId,
        at: TimestampMicros,
    ) -> Result<Vec<EncryptedAccessKeyGeneration>, ObjectStorageError> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        load_encrypted_key_generations(&self.pool, scope, access_key_id, at).await
    }
    async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ObjectStorageOperation>, ObjectStorageError> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        load_operation(&self.pool, scope, operation_id)
            .await
            .map(|value| value.map(|stored| stored.operation))
    }
    async fn audit(
        &self,
        scope: EnvironmentScope,
        request: AuditPageRequest,
    ) -> Result<AuditPage, ObjectStorageError> {
        request.validate()?;
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        list_audit(&self.pool, scope, request).await
    }
    async fn put_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        command: &PutObjectCommand,
    ) -> Result<ObjectOperationResult, ObjectStorageError> {
        let result = put_object(
            &self.pool,
            self.backend,
            scope,
            bucket_id,
            operation_id,
            command,
        )
        .await;
        record_object_mutation(&self.counters, &result);
        result
    }
    async fn get_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStorageError> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        load_object(&self.pool, scope, bucket_id, key).await
    }
    async fn list_objects(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: &ObjectPageRequest,
    ) -> Result<ObjectPage, ObjectStorageError> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        list_objects(&self.pool, scope, bucket_id, request).await
    }
    async fn delete_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        command: &DeleteObjectCommand,
    ) -> Result<ObjectOperationResult, ObjectStorageError> {
        let result = delete_object(
            &self.pool,
            self.backend,
            scope,
            bucket_id,
            operation_id,
            command,
        )
        .await;
        record_object_mutation(&self.counters, &result);
        result
    }
    async fn object_operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ObjectOperation>, ObjectStorageError> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        load_object_operation(&self.pool, scope, operation_id)
            .await
            .map(|value| value.map(|stored| stored.operation))
    }
    async fn health(&self) -> Result<(), ObjectStorageError> {
        sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_error)
    }
    fn telemetry(&self) -> ObjectStorageTelemetrySnapshot {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        ObjectStorageTelemetrySnapshot {
            commands: get(&self.counters.commands),
            replays: get(&self.counters.replays),
            conflicts: get(&self.counters.conflicts),
            reads: get(&self.counters.reads),
            retryable_errors: get(&self.counters.retryable_errors),
            pool_size: self.pool.size(),
            pool_idle: u32::try_from(self.pool.num_idle()).unwrap_or(u32::MAX),
        }
    }
}

fn record_object_mutation(
    counters: &Counters,
    result: &Result<ObjectOperationResult, ObjectStorageError>,
) {
    match result {
        Ok(value) if value.replayed => {
            counters.replays.fetch_add(1, Ordering::Relaxed);
        }
        Ok(_) => {
            counters.commands.fetch_add(1, Ordering::Relaxed);
        }
        Err(ObjectStorageError::Conflict | ObjectStorageError::OperationIdReused) => {
            counters.conflicts.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) if error.retryable() => {
            counters.retryable_errors.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {}
    }
}

#[derive(Debug)]
struct StoredObjectOperation {
    digest: Vec<u8>,
    operation: ObjectOperation,
}

#[derive(Debug)]
struct StoredOperation {
    digest: Vec<u8>,
    operation: ObjectStorageOperation,
}

#[allow(clippy::too_many_lines)]
async fn apply(
    pool: &AnyPool,
    backend: ObjectStorageRepositoryBackend,
    scope: EnvironmentScope,
    operation_id: OperationId,
    command: &ObjectStorageCommand,
) -> Result<ObjectStorageOperationResult, ObjectStorageError> {
    let digest = command.digest(scope);
    let mut tx = begin_write(pool, backend).await?;
    if let Some(stored) = load_operation_tx(&mut tx, scope, operation_id).await? {
        if stored.digest.as_slice() != digest {
            return rollback(tx, ObjectStorageError::OperationIdReused).await;
        }
        tx.commit().await.map_err(map_commit_error)?;
        return Ok(ObjectStorageOperationResult {
            operation: stored.operation,
            replayed: true,
        });
    }
    command.validate_new()?;
    let (bucket_id, access_key_id, revision) = match command {
        ObjectStorageCommand::CreateBucket {
            bucket_id,
            configuration,
            at,
            ..
        } => {
            let bucket = Bucket {
                scope,
                id: *bucket_id,
                configuration: configuration.clone(),
                revision: 1,
                state: BucketState::Active,
                created_at: *at,
                updated_at: *at,
            };
            bucket.validate().map_err(input_error)?;
            insert_bucket(&mut tx, &bucket).await?;
            (*bucket_id, None, 1)
        }
        ObjectStorageCommand::UpdateBucket {
            bucket_id,
            expected_revision,
            configuration,
            at,
            ..
        } => {
            let current = load_bucket_tx(&mut tx, backend, scope, *bucket_id)
                .await?
                .ok_or(ObjectStorageError::NotFound)?;
            require_active(&current, *expected_revision)?;
            require_not_before(*at, current.updated_at)?;
            let revision = expected_revision
                .checked_add(1)
                .ok_or(ObjectStorageError::LimitExceeded)?;
            let next = Bucket {
                configuration: configuration.clone(),
                revision,
                updated_at: *at,
                ..current
            };
            next.validate().map_err(input_error)?;
            update_bucket(&mut tx, &next, *expected_revision).await?;
            (*bucket_id, None, revision)
        }
        ObjectStorageCommand::ArchiveBucket {
            bucket_id,
            expected_revision,
            at,
            ..
        } => {
            let current = load_bucket_tx(&mut tx, backend, scope, *bucket_id)
                .await?
                .ok_or(ObjectStorageError::NotFound)?;
            require_active(&current, *expected_revision)?;
            require_not_before(*at, current.updated_at)?;
            if latest_key_update(&mut tx, scope, *bucket_id)
                .await?
                .is_some_and(|updated_at| *at < updated_at)
            {
                return rollback(tx, ObjectStorageError::Conflict).await;
            }
            let object_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runku_storage_objects WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3")
                .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket_id.to_string())
                .fetch_one(&mut *tx).await.map_err(map_sqlx_error)?;
            if object_count != 0 {
                return rollback(tx, ObjectStorageError::Conflict).await;
            }
            let revision = expected_revision
                .checked_add(1)
                .ok_or(ObjectStorageError::LimitExceeded)?;
            archive_bucket(
                &mut tx,
                scope,
                *bucket_id,
                *expected_revision,
                revision,
                *at,
            )
            .await?;
            revoke_bucket_keys(&mut tx, scope, *bucket_id, *at).await?;
            (*bucket_id, None, revision)
        }
        ObjectStorageCommand::IssueAccessKey {
            bucket_id,
            access_key_id,
            configuration,
            secret_digest,
            encrypted_secret,
            at,
            ..
        } => {
            let bucket = load_bucket_tx(&mut tx, backend, scope, *bucket_id)
                .await?
                .ok_or(ObjectStorageError::NotFound)?;
            if bucket.state != BucketState::Active {
                return rollback(tx, ObjectStorageError::Conflict).await;
            }
            require_not_before(*at, bucket.updated_at)?;
            let metadata = AccessKeyMetadata {
                scope,
                bucket_id: *bucket_id,
                id: *access_key_id,
                configuration: configuration.clone(),
                revision: 1,
                state: AccessKeyState::Active,
                created_at: *at,
                updated_at: *at,
                previous_generation_valid_until: None,
            };
            metadata.validate().map_err(input_error)?;
            insert_key(
                &mut tx,
                &metadata,
                secret_digest.as_bytes(),
                encrypted_secret,
            )
            .await?;
            (*bucket_id, Some(*access_key_id), 1)
        }
        ObjectStorageCommand::RotateAccessKey {
            bucket_id,
            access_key_id,
            expected_revision,
            secret_digest,
            encrypted_secret,
            overlap_until,
            at,
            ..
        } => {
            let bucket = load_bucket_tx(&mut tx, backend, scope, *bucket_id)
                .await?
                .ok_or(ObjectStorageError::NotFound)?;
            if bucket.state != BucketState::Active {
                return rollback(tx, ObjectStorageError::Conflict).await;
            }
            let current = load_key_tx(&mut tx, backend, scope, *bucket_id, *access_key_id)
                .await?
                .ok_or(ObjectStorageError::NotFound)?;
            if current.state != AccessKeyState::Active || current.revision != *expected_revision {
                return rollback(tx, ObjectStorageError::Conflict).await;
            }
            require_not_before(*at, current.updated_at)?;
            let revision = expected_revision
                .checked_add(1)
                .ok_or(ObjectStorageError::LimitExceeded)?;
            rotate_key(
                &mut tx,
                scope,
                *bucket_id,
                *access_key_id,
                *expected_revision,
                revision,
                secret_digest.as_bytes(),
                encrypted_secret,
                *overlap_until,
                *at,
            )
            .await?;
            (*bucket_id, Some(*access_key_id), revision)
        }
        ObjectStorageCommand::RevokeAccessKey {
            bucket_id,
            access_key_id,
            expected_revision,
            at,
            ..
        } => {
            let current = load_key_tx(&mut tx, backend, scope, *bucket_id, *access_key_id)
                .await?
                .ok_or(ObjectStorageError::NotFound)?;
            if current.state != AccessKeyState::Active || current.revision != *expected_revision {
                return rollback(tx, ObjectStorageError::Conflict).await;
            }
            require_not_before(*at, current.updated_at)?;
            let revision = expected_revision
                .checked_add(1)
                .ok_or(ObjectStorageError::LimitExceeded)?;
            revoke_key(
                &mut tx,
                scope,
                *bucket_id,
                *access_key_id,
                *expected_revision,
                revision,
                *at,
            )
            .await?;
            (*bucket_id, Some(*access_key_id), revision)
        }
    };
    let operation = ObjectStorageOperation {
        scope,
        operation_id,
        kind: command.kind(),
        bucket_id,
        access_key_id,
        revision,
        completed_at: command.at(),
    };
    insert_operation(&mut tx, &operation, &digest).await?;
    insert_audit(&mut tx, &operation, command.actor()).await?;
    tx.commit().await.map_err(map_commit_error)?;
    Ok(ObjectStorageOperationResult {
        operation,
        replayed: false,
    })
}

fn input_error(error: ObjectStorageError) -> ObjectStorageError {
    match error {
        ObjectStorageError::Corruption => ObjectStorageError::InvalidInput,
        other => other,
    }
}
fn require_active(bucket: &Bucket, expected: u64) -> Result<(), ObjectStorageError> {
    if bucket.state != BucketState::Active || bucket.revision != expected {
        Err(ObjectStorageError::Conflict)
    } else {
        Ok(())
    }
}

fn require_not_before(
    at: TimestampMicros,
    current: TimestampMicros,
) -> Result<(), ObjectStorageError> {
    if at < current {
        Err(ObjectStorageError::Conflict)
    } else {
        Ok(())
    }
}

async fn latest_key_update(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    bucket: BucketId,
) -> Result<Option<TimestampMicros>, ObjectStorageError> {
    let value = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(updated_at_micros) FROM runku_storage_access_keys WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).fetch_one(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(value.map(TimestampMicros::new))
}

async fn insert_bucket(
    tx: &mut Transaction<'_, Any>,
    value: &Bucket,
) -> Result<(), ObjectStorageError> {
    let json =
        serde_json::to_string(&value.configuration).map_err(|_| ObjectStorageError::Internal)?;
    sqlx::query("INSERT INTO runku_storage_buckets(project_id,environment_id,bucket_id,name,configuration_json,revision,state,created_at_micros,updated_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(value.scope.project_id().to_string()).bind(value.scope.environment_id().to_string()).bind(value.id.to_string()).bind(value.configuration.name.as_str()).bind(json).bind(to_i64(value.revision)?).bind(value.state.as_str()).bind(value.created_at.get()).bind(value.updated_at.get()).execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn update_bucket(
    tx: &mut Transaction<'_, Any>,
    value: &Bucket,
    expected: u64,
) -> Result<(), ObjectStorageError> {
    let json =
        serde_json::to_string(&value.configuration).map_err(|_| ObjectStorageError::Internal)?;
    let result = sqlx::query("UPDATE runku_storage_buckets SET name=$1,configuration_json=$2,revision=$3,updated_at_micros=$4 WHERE project_id=$5 AND environment_id=$6 AND bucket_id=$7 AND revision=$8 AND state='active'")
        .bind(value.configuration.name.as_str()).bind(json).bind(to_i64(value.revision)?).bind(value.updated_at.get()).bind(value.scope.project_id().to_string()).bind(value.scope.environment_id().to_string()).bind(value.id.to_string()).bind(to_i64(expected)?).execute(&mut **tx).await.map_err(map_constraint_error)?;
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ObjectStorageError::Conflict)
    }
}

async fn archive_bucket(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    id: BucketId,
    expected: u64,
    revision: u64,
    at: TimestampMicros,
) -> Result<(), ObjectStorageError> {
    let result = sqlx::query("UPDATE runku_storage_buckets SET state='archived',revision=$1,updated_at_micros=$2 WHERE project_id=$3 AND environment_id=$4 AND bucket_id=$5 AND revision=$6 AND state='active'").bind(to_i64(revision)?).bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string()).bind(to_i64(expected)?).execute(&mut **tx).await.map_err(map_sqlx_error)?;
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ObjectStorageError::Conflict)
    }
}

async fn insert_key(
    tx: &mut Transaction<'_, Any>,
    value: &AccessKeyMetadata,
    digest: &[u8; 32],
    encrypted_secret: &EncryptedAccessKeySecret,
) -> Result<(), ObjectStorageError> {
    let json =
        serde_json::to_string(&value.configuration).map_err(|_| ObjectStorageError::Internal)?;
    sqlx::query("INSERT INTO runku_storage_access_keys(project_id,environment_id,bucket_id,access_key_id,configuration_json,revision,state,created_at_micros,updated_at_micros,previous_generation_valid_until_micros) VALUES($1,$2,$3,$4,$5,1,'active',$6,$6,NULL)").bind(value.scope.project_id().to_string()).bind(value.scope.environment_id().to_string()).bind(value.bucket_id.to_string()).bind(value.id.to_string()).bind(json).bind(value.created_at.get()).execute(&mut **tx).await.map_err(map_constraint_error)?;
    sqlx::query("INSERT INTO runku_storage_access_key_generations(project_id,environment_id,bucket_id,access_key_id,generation,secret_digest,valid_from_micros,valid_until_micros,revoked_at_micros,secret_nonce,secret_ciphertext) VALUES($1,$2,$3,$4,1,$5,$6,NULL,NULL,$7,$8)").bind(value.scope.project_id().to_string()).bind(value.scope.environment_id().to_string()).bind(value.bucket_id.to_string()).bind(value.id.to_string()).bind(digest.as_slice()).bind(value.created_at.get()).bind(encrypted_secret.nonce().as_slice()).bind(encrypted_secret.ciphertext()).execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn rotate_key(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    bucket: BucketId,
    key: AccessKeyId,
    expected: u64,
    revision: u64,
    digest: &[u8; 32],
    encrypted_secret: &EncryptedAccessKeySecret,
    overlap: TimestampMicros,
    at: TimestampMicros,
) -> Result<(), ObjectStorageError> {
    let result = sqlx::query("UPDATE runku_storage_access_keys SET revision=$1,updated_at_micros=$2,previous_generation_valid_until_micros=$3 WHERE project_id=$4 AND environment_id=$5 AND bucket_id=$6 AND access_key_id=$7 AND revision=$8 AND state='active'").bind(to_i64(revision)?).bind(at.get()).bind(overlap.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(key.to_string()).bind(to_i64(expected)?).execute(&mut **tx).await.map_err(map_sqlx_error)?;
    if result.rows_affected() != 1 {
        return Err(ObjectStorageError::Conflict);
    }
    let retired = sqlx::query("UPDATE runku_storage_access_key_generations SET valid_until_micros=$1 WHERE project_id=$2 AND environment_id=$3 AND bucket_id=$4 AND access_key_id=$5 AND generation=$6 AND revoked_at_micros IS NULL AND valid_until_micros IS NULL").bind(overlap.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(key.to_string()).bind(to_i64(expected)?).execute(&mut **tx).await.map_err(map_sqlx_error)?;
    if retired.rows_affected() != 1 {
        return Err(ObjectStorageError::Corruption);
    }
    sqlx::query("INSERT INTO runku_storage_access_key_generations(project_id,environment_id,bucket_id,access_key_id,generation,secret_digest,valid_from_micros,valid_until_micros,revoked_at_micros,secret_nonce,secret_ciphertext) VALUES($1,$2,$3,$4,$5,$6,$7,NULL,NULL,$8,$9)").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(key.to_string()).bind(to_i64(revision)?).bind(digest.as_slice()).bind(at.get()).bind(encrypted_secret.nonce().as_slice()).bind(encrypted_secret.ciphertext()).execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn revoke_key(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    bucket: BucketId,
    key: AccessKeyId,
    expected: u64,
    revision: u64,
    at: TimestampMicros,
) -> Result<(), ObjectStorageError> {
    let result = sqlx::query("UPDATE runku_storage_access_keys SET revision=$1,state='revoked',updated_at_micros=$2,previous_generation_valid_until_micros=NULL WHERE project_id=$3 AND environment_id=$4 AND bucket_id=$5 AND access_key_id=$6 AND revision=$7 AND state='active'").bind(to_i64(revision)?).bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(key.to_string()).bind(to_i64(expected)?).execute(&mut **tx).await.map_err(map_sqlx_error)?;
    if result.rows_affected() != 1 {
        return Err(ObjectStorageError::Conflict);
    }
    sqlx::query("UPDATE runku_storage_access_key_generations SET revoked_at_micros=$1 WHERE project_id=$2 AND environment_id=$3 AND bucket_id=$4 AND access_key_id=$5 AND revoked_at_micros IS NULL").bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(key.to_string()).execute(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn revoke_bucket_keys(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    bucket: BucketId,
    at: TimestampMicros,
) -> Result<(), ObjectStorageError> {
    sqlx::query("UPDATE runku_storage_access_keys SET revision=revision+1,state='revoked',updated_at_micros=$1,previous_generation_valid_until_micros=NULL WHERE project_id=$2 AND environment_id=$3 AND bucket_id=$4 AND state='active'").bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).execute(&mut **tx).await.map_err(map_sqlx_error)?;
    sqlx::query("UPDATE runku_storage_access_key_generations SET revoked_at_micros=$1 WHERE project_id=$2 AND environment_id=$3 AND bucket_id=$4 AND revoked_at_micros IS NULL").bind(at.get()).bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).execute(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn insert_operation(
    tx: &mut Transaction<'_, Any>,
    value: &ObjectStorageOperation,
    digest: &[u8; 32],
) -> Result<(), ObjectStorageError> {
    sqlx::query("INSERT INTO runku_storage_operations(project_id,environment_id,operation_id,command_digest,kind,bucket_id,access_key_id,revision,completed_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)").bind(value.scope.project_id().to_string()).bind(value.scope.environment_id().to_string()).bind(value.operation_id.to_string()).bind(digest.as_slice()).bind(value.kind.as_str()).bind(value.bucket_id.to_string()).bind(value.access_key_id.map(|id| id.to_string())).bind(to_i64(value.revision)?).bind(value.completed_at.get()).execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn insert_audit(
    tx: &mut Transaction<'_, Any>,
    operation: &ObjectStorageOperation,
    actor: &ObjectStorageActor,
) -> Result<(), ObjectStorageError> {
    let current = sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(sequence),0) FROM runku_storage_audit WHERE project_id=$1 AND environment_id=$2").bind(operation.scope.project_id().to_string()).bind(operation.scope.environment_id().to_string()).fetch_one(&mut **tx).await.map_err(map_sqlx_error)?;
    let sequence = current
        .checked_add(1)
        .ok_or(ObjectStorageError::LimitExceeded)?;
    sqlx::query("INSERT INTO runku_storage_audit(project_id,environment_id,sequence,operation_id,kind,actor,bucket_id,access_key_id,revision,occurred_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)").bind(operation.scope.project_id().to_string()).bind(operation.scope.environment_id().to_string()).bind(sequence).bind(operation.operation_id.to_string()).bind(operation.kind.as_str()).bind(actor.as_str()).bind(operation.bucket_id.to_string()).bind(operation.access_key_id.map(|id| id.to_string())).bind(to_i64(operation.revision)?).bind(operation.completed_at.get()).execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn load_bucket(
    pool: &AnyPool,
    scope: EnvironmentScope,
    id: BucketId,
) -> Result<Option<Bucket>, ObjectStorageError> {
    let row = sqlx::query("SELECT configuration_json,revision,state,created_at_micros,updated_at_micros FROM runku_storage_buckets WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(id.to_string()).fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_bucket(scope, id, &row)).transpose()
}

async fn load_bucket_tx(
    tx: &mut Transaction<'_, Any>,
    backend: ObjectStorageRepositoryBackend,
    scope: EnvironmentScope,
    id: BucketId,
) -> Result<Option<Bucket>, ObjectStorageError> {
    let statement = if backend == ObjectStorageRepositoryBackend::PostgreSQL {
        "SELECT configuration_json,revision,state,created_at_micros,updated_at_micros FROM runku_storage_buckets WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 FOR UPDATE"
    } else {
        "SELECT configuration_json,revision,state,created_at_micros,updated_at_micros FROM runku_storage_buckets WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3"
    };
    let row = sqlx::query(statement)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    row.map(|row| decode_bucket(scope, id, &row)).transpose()
}

fn decode_bucket(
    scope: EnvironmentScope,
    id: BucketId,
    row: &sqlx::any::AnyRow,
) -> Result<Bucket, ObjectStorageError> {
    let json: String = row.try_get("configuration_json").map_err(corrupt)?;
    let value = Bucket {
        scope,
        id,
        configuration: serde_json::from_str(&json).map_err(corrupt)?,
        revision: positive_u64(row.try_get("revision").map_err(corrupt)?)?,
        state: parse_domain(row, "state")?,
        created_at: TimestampMicros::new(row.try_get("created_at_micros").map_err(corrupt)?),
        updated_at: TimestampMicros::new(row.try_get("updated_at_micros").map_err(corrupt)?),
    };
    value.validate()?;
    Ok(value)
}

async fn list_buckets(
    pool: &AnyPool,
    scope: EnvironmentScope,
    request: BucketPageRequest,
) -> Result<BucketPage, ObjectStorageError> {
    let limit = i64::from(request.limit) + 1;
    let rows = if let Some(after) = request.after {
        sqlx::query("SELECT bucket_id,configuration_json,revision,state,created_at_micros,updated_at_micros FROM runku_storage_buckets WHERE project_id=$1 AND environment_id=$2 AND bucket_id>$3 ORDER BY bucket_id LIMIT $4").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(after.to_string()).bind(limit).fetch_all(pool).await.map_err(map_sqlx_error)?
    } else {
        sqlx::query("SELECT bucket_id,configuration_json,revision,state,created_at_micros,updated_at_micros FROM runku_storage_buckets WHERE project_id=$1 AND environment_id=$2 ORDER BY bucket_id LIMIT $3").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(limit).fetch_all(pool).await.map_err(map_sqlx_error)?
    };
    let mut buckets = rows
        .iter()
        .map(|row| {
            let id: BucketId = parse_domain(row, "bucket_id")?;
            decode_bucket(scope, id, row)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let more = buckets.len() > usize::from(request.limit);
    if more {
        let _ = buckets.pop();
    }
    let next = more.then(|| buckets.last().map(|value| value.id)).flatten();
    Ok(BucketPage {
        scope,
        buckets,
        next,
    })
}

async fn load_key(
    pool: &AnyPool,
    scope: EnvironmentScope,
    bucket: BucketId,
    key: AccessKeyId,
) -> Result<Option<AccessKeyMetadata>, ObjectStorageError> {
    let row = sqlx::query("SELECT configuration_json,revision,state,created_at_micros,updated_at_micros,previous_generation_valid_until_micros FROM runku_storage_access_keys WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND access_key_id=$4").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(key.to_string()).fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_key(scope, bucket, key, &row))
        .transpose()
}

async fn authenticate_key(
    pool: &AnyPool,
    scope: EnvironmentScope,
    bucket: BucketId,
    key: AccessKeyId,
    digest: &[u8; 32],
    at: TimestampMicros,
) -> Result<Option<AccessKeyMetadata>, ObjectStorageError> {
    let row = sqlx::query("SELECT k.configuration_json,k.revision,k.state,k.created_at_micros,k.updated_at_micros,k.previous_generation_valid_until_micros FROM runku_storage_access_keys k JOIN runku_storage_access_key_generations g ON g.project_id=k.project_id AND g.environment_id=k.environment_id AND g.bucket_id=k.bucket_id AND g.access_key_id=k.access_key_id WHERE k.project_id=$1 AND k.environment_id=$2 AND k.bucket_id=$3 AND k.access_key_id=$4 AND k.state='active' AND g.secret_digest=$5 AND g.valid_from_micros<=$6 AND g.revoked_at_micros IS NULL AND (g.valid_until_micros IS NULL OR g.valid_until_micros>$6) LIMIT 1")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(key.to_string()).bind(digest.as_slice()).bind(at.get()).fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_key(scope, bucket, key, &row))
        .transpose()
}

async fn load_encrypted_key_generations(
    pool: &AnyPool,
    scope: EnvironmentScope,
    key: AccessKeyId,
    at: TimestampMicros,
) -> Result<Vec<EncryptedAccessKeyGeneration>, ObjectStorageError> {
    let rows = sqlx::query("SELECT k.bucket_id,k.configuration_json,k.revision,k.state,k.created_at_micros,k.updated_at_micros,k.previous_generation_valid_until_micros,g.generation,g.secret_nonce,g.secret_ciphertext FROM runku_storage_access_keys k JOIN runku_storage_access_key_generations g ON g.project_id=k.project_id AND g.environment_id=k.environment_id AND g.bucket_id=k.bucket_id AND g.access_key_id=k.access_key_id WHERE k.project_id=$1 AND k.environment_id=$2 AND k.access_key_id=$3 AND k.state='active' AND g.valid_from_micros<=$4 AND g.revoked_at_micros IS NULL AND (g.valid_until_micros IS NULL OR g.valid_until_micros>$4) AND g.secret_nonce IS NOT NULL AND g.secret_ciphertext IS NOT NULL ORDER BY g.generation DESC LIMIT 3")
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(key.to_string())
        .bind(at.get())
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
    rows.iter()
        .map(|row| {
            let bucket: BucketId = parse_domain(row, "bucket_id")?;
            let metadata = decode_key(scope, bucket, key, row)?;
            let generation = positive_u64(row.try_get("generation").map_err(corrupt)?)?;
            let nonce: Vec<u8> = row.try_get("secret_nonce").map_err(corrupt)?;
            let ciphertext: Vec<u8> = row.try_get("secret_ciphertext").map_err(corrupt)?;
            Ok(EncryptedAccessKeyGeneration {
                metadata,
                generation,
                secret: EncryptedAccessKeySecret::from_parts(&nonce, ciphertext)?,
            })
        })
        .collect()
}

async fn load_key_tx(
    tx: &mut Transaction<'_, Any>,
    backend: ObjectStorageRepositoryBackend,
    scope: EnvironmentScope,
    bucket: BucketId,
    key: AccessKeyId,
) -> Result<Option<AccessKeyMetadata>, ObjectStorageError> {
    let statement = if backend == ObjectStorageRepositoryBackend::PostgreSQL {
        "SELECT configuration_json,revision,state,created_at_micros,updated_at_micros,previous_generation_valid_until_micros FROM runku_storage_access_keys WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND access_key_id=$4 FOR UPDATE"
    } else {
        "SELECT configuration_json,revision,state,created_at_micros,updated_at_micros,previous_generation_valid_until_micros FROM runku_storage_access_keys WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND access_key_id=$4"
    };
    let row = sqlx::query(statement)
        .bind(scope.project_id().to_string())
        .bind(scope.environment_id().to_string())
        .bind(bucket.to_string())
        .bind(key.to_string())
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    row.map(|row| decode_key(scope, bucket, key, &row))
        .transpose()
}

fn decode_key(
    scope: EnvironmentScope,
    bucket: BucketId,
    key: AccessKeyId,
    row: &sqlx::any::AnyRow,
) -> Result<AccessKeyMetadata, ObjectStorageError> {
    let json: String = row.try_get("configuration_json").map_err(corrupt)?;
    let value = AccessKeyMetadata {
        scope,
        bucket_id: bucket,
        id: key,
        configuration: serde_json::from_str::<AccessKeyConfiguration>(&json).map_err(corrupt)?,
        revision: positive_u64(row.try_get("revision").map_err(corrupt)?)?,
        state: parse_domain(row, "state")?,
        created_at: TimestampMicros::new(row.try_get("created_at_micros").map_err(corrupt)?),
        updated_at: TimestampMicros::new(row.try_get("updated_at_micros").map_err(corrupt)?),
        previous_generation_valid_until: row
            .try_get::<Option<i64>, _>("previous_generation_valid_until_micros")
            .map_err(corrupt)?
            .map(TimestampMicros::new),
    };
    value.validate()?;
    Ok(value)
}

async fn list_keys(
    pool: &AnyPool,
    scope: EnvironmentScope,
    bucket: BucketId,
    request: AccessKeyPageRequest,
) -> Result<AccessKeyPage, ObjectStorageError> {
    if load_bucket(pool, scope, bucket).await?.is_none() {
        return Err(ObjectStorageError::NotFound);
    }
    let limit = i64::from(request.limit) + 1;
    let rows = if let Some(after) = request.after {
        sqlx::query("SELECT access_key_id,configuration_json,revision,state,created_at_micros,updated_at_micros,previous_generation_valid_until_micros FROM runku_storage_access_keys WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND access_key_id>$4 ORDER BY access_key_id LIMIT $5").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(after.to_string()).bind(limit).fetch_all(pool).await.map_err(map_sqlx_error)?
    } else {
        sqlx::query("SELECT access_key_id,configuration_json,revision,state,created_at_micros,updated_at_micros,previous_generation_valid_until_micros FROM runku_storage_access_keys WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 ORDER BY access_key_id LIMIT $4").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket.to_string()).bind(limit).fetch_all(pool).await.map_err(map_sqlx_error)?
    };
    let mut keys = rows
        .iter()
        .map(|row| {
            let id: AccessKeyId = parse_domain(row, "access_key_id")?;
            decode_key(scope, bucket, id, row)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let more = keys.len() > usize::from(request.limit);
    if more {
        let _ = keys.pop();
    }
    let next = more.then(|| keys.last().map(|value| value.id)).flatten();
    Ok(AccessKeyPage {
        scope,
        bucket_id: bucket,
        keys,
        next,
    })
}

async fn load_operation(
    pool: &AnyPool,
    scope: EnvironmentScope,
    operation_id: OperationId,
) -> Result<Option<StoredOperation>, ObjectStorageError> {
    let row = sqlx::query("SELECT command_digest,kind,bucket_id,access_key_id,revision,completed_at_micros FROM runku_storage_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(operation_id.to_string()).fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_operation(scope, operation_id, &row))
        .transpose()
}

async fn load_operation_tx(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    operation_id: OperationId,
) -> Result<Option<StoredOperation>, ObjectStorageError> {
    let row = sqlx::query("SELECT command_digest,kind,bucket_id,access_key_id,revision,completed_at_micros FROM runku_storage_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(operation_id.to_string()).fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_operation(scope, operation_id, &row))
        .transpose()
}

fn decode_operation(
    scope: EnvironmentScope,
    operation_id: OperationId,
    row: &sqlx::any::AnyRow,
) -> Result<StoredOperation, ObjectStorageError> {
    let digest: Vec<u8> = row.try_get("command_digest").map_err(corrupt)?;
    if digest.len() != 32 {
        return Err(ObjectStorageError::Corruption);
    }
    Ok(StoredOperation {
        digest,
        operation: ObjectStorageOperation {
            scope,
            operation_id,
            kind: parse_domain(row, "kind")?,
            bucket_id: parse_domain(row, "bucket_id")?,
            access_key_id: row
                .try_get::<Option<String>, _>("access_key_id")
                .map_err(corrupt)?
                .map(|value| value.parse().map_err(corrupt))
                .transpose()?,
            revision: positive_u64(row.try_get("revision").map_err(corrupt)?)?,
            completed_at: TimestampMicros::new(
                row.try_get("completed_at_micros").map_err(corrupt)?,
            ),
        },
    })
}

async fn list_audit(
    pool: &AnyPool,
    scope: EnvironmentScope,
    request: AuditPageRequest,
) -> Result<AuditPage, ObjectStorageError> {
    let after = to_i64(request.after.unwrap_or(0))?;
    let limit = i64::from(request.limit) + 1;
    let rows = sqlx::query("SELECT sequence,operation_id,kind,actor,bucket_id,access_key_id,revision,occurred_at_micros FROM runku_storage_audit WHERE project_id=$1 AND environment_id=$2 AND sequence>$3 ORDER BY sequence LIMIT $4").bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(after).bind(limit).fetch_all(pool).await.map_err(map_sqlx_error)?;
    let mut events = rows
        .iter()
        .map(|row| {
            Ok(AuditEvent {
                scope,
                sequence: positive_u64(row.try_get("sequence").map_err(corrupt)?)?,
                operation_id: parse_domain(row, "operation_id")?,
                kind: parse_domain(row, "kind")?,
                actor: parse_domain(row, "actor")?,
                bucket_id: parse_domain(row, "bucket_id")?,
                access_key_id: row
                    .try_get::<Option<String>, _>("access_key_id")
                    .map_err(corrupt)?
                    .map(|value| value.parse().map_err(corrupt))
                    .transpose()?,
                revision: positive_u64(row.try_get("revision").map_err(corrupt)?)?,
                occurred_at: TimestampMicros::new(
                    row.try_get("occurred_at_micros").map_err(corrupt)?,
                ),
            })
        })
        .collect::<Result<Vec<_>, ObjectStorageError>>()?;
    let more = events.len() > usize::from(request.limit);
    if more {
        let _ = events.pop();
    }
    let next = more
        .then(|| events.last().map(|value| value.sequence))
        .flatten();
    Ok(AuditPage {
        scope,
        events,
        next,
    })
}

#[allow(clippy::too_many_lines)]
async fn put_object(
    pool: &AnyPool,
    backend: ObjectStorageRepositoryBackend,
    scope: EnvironmentScope,
    bucket_id: BucketId,
    operation_id: OperationId,
    command: &PutObjectCommand,
) -> Result<ObjectOperationResult, ObjectStorageError> {
    let digest = command.digest(scope, bucket_id);
    let mut tx = begin_write(pool, backend).await?;
    if let Some(stored) = load_object_operation_tx(&mut tx, scope, operation_id).await? {
        if stored.digest.as_slice() != digest {
            return rollback(tx, ObjectStorageError::OperationIdReused).await;
        }
        let object = load_object_version_tx(
            &mut tx,
            scope,
            stored.operation.bucket_id,
            &stored.operation.key,
            stored.operation.version_id,
        )
        .await?
        .ok_or(ObjectStorageError::Corruption)?;
        tx.commit().await.map_err(map_commit_error)?;
        return Ok(ObjectOperationResult {
            operation: stored.operation,
            object: Some(object),
            replayed: true,
        });
    }
    command.validate()?;
    let bucket = load_bucket_tx(&mut tx, backend, scope, bucket_id)
        .await?
        .ok_or(ObjectStorageError::NotFound)?;
    if bucket.state != BucketState::Active {
        return rollback(tx, ObjectStorageError::Conflict).await;
    }
    if command.size > bucket.configuration.quota.max_object_bytes {
        return rollback(tx, ObjectStorageError::LimitExceeded).await;
    }
    let current = load_object_tx(&mut tx, scope, bucket_id, &command.key).await?;
    let row = sqlx::query("SELECT COALESCE(SUM(size_bytes),0) AS total_bytes,COUNT(*) AS object_count FROM runku_storage_objects WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket_id.to_string())
        .fetch_one(&mut *tx).await.map_err(map_sqlx_error)?;
    let total: i64 = row.try_get("total_bytes").map_err(corrupt)?;
    let count: i64 = row.try_get("object_count").map_err(corrupt)?;
    let prior_size = current.as_ref().map_or(0, |value| value.size);
    let next_total = u64::try_from(total)
        .map_err(corrupt)?
        .checked_sub(prior_size)
        .and_then(|value| value.checked_add(command.size))
        .ok_or(ObjectStorageError::LimitExceeded)?;
    let next_count = u64::try_from(count)
        .map_err(corrupt)?
        .checked_add(u64::from(current.is_none()))
        .ok_or(ObjectStorageError::LimitExceeded)?;
    if next_total > bucket.configuration.quota.max_total_bytes
        || next_count > bucket.configuration.quota.max_objects
    {
        return rollback(tx, ObjectStorageError::LimitExceeded).await;
    }
    let object = ObjectMetadata {
        scope,
        bucket_id,
        key: command.key.clone(),
        version_id: command.version_id,
        size: command.size,
        sha256: command.sha256,
        etag: object_etag(&command.sha256),
        content_type: command.content_type.clone(),
        metadata: command.metadata.clone(),
        created_at: command.at,
    };
    object.validate().map_err(input_error)?;
    insert_object_version(&mut tx, &object).await?;
    upsert_current_object(&mut tx, &object).await?;
    let operation = ObjectOperation {
        scope,
        operation_id,
        kind: "put",
        bucket_id,
        key: command.key.clone(),
        version_id: command.version_id,
        completed_at: command.at,
    };
    insert_object_operation(&mut tx, &operation, &digest).await?;
    insert_object_audit(
        &mut tx,
        &operation,
        command.actor.as_str(),
        Some(command.size),
    )
    .await?;
    tx.commit().await.map_err(map_commit_error)?;
    Ok(ObjectOperationResult {
        operation,
        object: Some(object),
        replayed: false,
    })
}

async fn delete_object(
    pool: &AnyPool,
    backend: ObjectStorageRepositoryBackend,
    scope: EnvironmentScope,
    bucket_id: BucketId,
    operation_id: OperationId,
    command: &DeleteObjectCommand,
) -> Result<ObjectOperationResult, ObjectStorageError> {
    let digest = command.digest(scope, bucket_id);
    let mut tx = begin_write(pool, backend).await?;
    if let Some(stored) = load_object_operation_tx(&mut tx, scope, operation_id).await? {
        if stored.digest.as_slice() != digest {
            return rollback(tx, ObjectStorageError::OperationIdReused).await;
        }
        tx.commit().await.map_err(map_commit_error)?;
        return Ok(ObjectOperationResult {
            operation: stored.operation,
            object: None,
            replayed: true,
        });
    }
    command.validate()?;
    let bucket = load_bucket_tx(&mut tx, backend, scope, bucket_id)
        .await?
        .ok_or(ObjectStorageError::NotFound)?;
    if bucket.state != BucketState::Active {
        return rollback(tx, ObjectStorageError::Conflict).await;
    }
    let current = load_object_tx(&mut tx, scope, bucket_id, &command.key)
        .await?
        .ok_or(ObjectStorageError::NotFound)?;
    if current.version_id != command.expected_version_id {
        return rollback(tx, ObjectStorageError::Conflict).await;
    }
    let deleted = sqlx::query("DELETE FROM runku_storage_objects WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND object_key=$4 AND version_id=$5")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket_id.to_string())
        .bind(&command.key).bind(command.expected_version_id.to_string()).execute(&mut *tx).await.map_err(map_sqlx_error)?;
    if deleted.rows_affected() != 1 {
        return rollback(tx, ObjectStorageError::Conflict).await;
    }
    let operation = ObjectOperation {
        scope,
        operation_id,
        kind: "delete",
        bucket_id,
        key: command.key.clone(),
        version_id: command.expected_version_id,
        completed_at: command.at,
    };
    insert_object_operation(&mut tx, &operation, &digest).await?;
    insert_object_audit(
        &mut tx,
        &operation,
        command.actor.as_str(),
        Some(current.size),
    )
    .await?;
    tx.commit().await.map_err(map_commit_error)?;
    Ok(ObjectOperationResult {
        operation,
        object: None,
        replayed: false,
    })
}

async fn insert_object_version(
    tx: &mut Transaction<'_, Any>,
    value: &ObjectMetadata,
) -> Result<(), ObjectStorageError> {
    let metadata =
        serde_json::to_string(&value.metadata).map_err(|_| ObjectStorageError::Internal)?;
    sqlx::query("INSERT INTO runku_storage_object_versions(project_id,environment_id,bucket_id,object_key,version_id,size_bytes,sha256,etag,content_type,metadata_json,created_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
        .bind(value.scope.project_id().to_string()).bind(value.scope.environment_id().to_string())
        .bind(value.bucket_id.to_string()).bind(&value.key).bind(value.version_id.to_string())
        .bind(to_i64(value.size)?).bind(value.sha256.as_slice()).bind(&value.etag)
        .bind(&value.content_type).bind(metadata).bind(value.created_at.get())
        .execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn upsert_current_object(
    tx: &mut Transaction<'_, Any>,
    value: &ObjectMetadata,
) -> Result<(), ObjectStorageError> {
    let metadata =
        serde_json::to_string(&value.metadata).map_err(|_| ObjectStorageError::Internal)?;
    sqlx::query("INSERT INTO runku_storage_objects(project_id,environment_id,bucket_id,object_key,version_id,size_bytes,sha256,etag,content_type,metadata_json,created_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(project_id,environment_id,bucket_id,object_key) DO UPDATE SET version_id=excluded.version_id,size_bytes=excluded.size_bytes,sha256=excluded.sha256,etag=excluded.etag,content_type=excluded.content_type,metadata_json=excluded.metadata_json,created_at_micros=excluded.created_at_micros")
        .bind(value.scope.project_id().to_string()).bind(value.scope.environment_id().to_string())
        .bind(value.bucket_id.to_string()).bind(&value.key).bind(value.version_id.to_string())
        .bind(to_i64(value.size)?).bind(value.sha256.as_slice()).bind(&value.etag)
        .bind(&value.content_type).bind(metadata).bind(value.created_at.get())
        .execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn load_object(
    pool: &AnyPool,
    scope: EnvironmentScope,
    bucket_id: BucketId,
    key: &str,
) -> Result<Option<ObjectMetadata>, ObjectStorageError> {
    let row = sqlx::query("SELECT object_key,version_id,size_bytes,sha256,etag,content_type,metadata_json,created_at_micros FROM runku_storage_objects WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND object_key=$4")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket_id.to_string()).bind(key)
        .fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_object(scope, bucket_id, &row))
        .transpose()
}

async fn load_object_tx(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    bucket_id: BucketId,
    key: &str,
) -> Result<Option<ObjectMetadata>, ObjectStorageError> {
    let row = sqlx::query("SELECT object_key,version_id,size_bytes,sha256,etag,content_type,metadata_json,created_at_micros FROM runku_storage_objects WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND object_key=$4")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket_id.to_string()).bind(key)
        .fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_object(scope, bucket_id, &row))
        .transpose()
}

async fn load_object_version_tx(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    bucket_id: BucketId,
    key: &str,
    version_id: ObjectVersionId,
) -> Result<Option<ObjectMetadata>, ObjectStorageError> {
    let row = sqlx::query("SELECT object_key,version_id,size_bytes,sha256,etag,content_type,metadata_json,created_at_micros FROM runku_storage_object_versions WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND object_key=$4 AND version_id=$5")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket_id.to_string()).bind(key).bind(version_id.to_string())
        .fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_object(scope, bucket_id, &row))
        .transpose()
}

fn decode_object(
    scope: EnvironmentScope,
    bucket_id: BucketId,
    row: &sqlx::any::AnyRow,
) -> Result<ObjectMetadata, ObjectStorageError> {
    let digest: Vec<u8> = row.try_get("sha256").map_err(corrupt)?;
    let sha256: [u8; 32] = digest.try_into().map_err(corrupt)?;
    let metadata_json: String = row.try_get("metadata_json").map_err(corrupt)?;
    let value = ObjectMetadata {
        scope,
        bucket_id,
        key: row.try_get("object_key").map_err(corrupt)?,
        version_id: parse_domain(row, "version_id")?,
        size: u64::try_from(row.try_get::<i64, _>("size_bytes").map_err(corrupt)?)
            .map_err(corrupt)?,
        sha256,
        etag: row.try_get("etag").map_err(corrupt)?,
        content_type: row.try_get("content_type").map_err(corrupt)?,
        metadata: serde_json::from_str(&metadata_json).map_err(corrupt)?,
        created_at: TimestampMicros::new(row.try_get("created_at_micros").map_err(corrupt)?),
    };
    value.validate()?;
    Ok(value)
}

async fn list_objects(
    pool: &AnyPool,
    scope: EnvironmentScope,
    bucket_id: BucketId,
    request: &ObjectPageRequest,
) -> Result<ObjectPage, ObjectStorageError> {
    request.validate()?;
    if load_bucket(pool, scope, bucket_id).await?.is_none() {
        return Err(ObjectStorageError::NotFound);
    }
    let pattern = format!("{}%", escape_like(&request.prefix));
    let limit = i64::from(request.limit) + 1;
    let rows = if let Some(after) = &request.after {
        sqlx::query("SELECT object_key,version_id,size_bytes,sha256,etag,content_type,metadata_json,created_at_micros FROM runku_storage_objects WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND object_key LIKE $4 ESCAPE '\\' AND object_key>$5 ORDER BY object_key LIMIT $6")
            .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket_id.to_string())
            .bind(&pattern).bind(after).bind(limit).fetch_all(pool).await.map_err(map_sqlx_error)?
    } else {
        sqlx::query("SELECT object_key,version_id,size_bytes,sha256,etag,content_type,metadata_json,created_at_micros FROM runku_storage_objects WHERE project_id=$1 AND environment_id=$2 AND bucket_id=$3 AND object_key LIKE $4 ESCAPE '\\' ORDER BY object_key LIMIT $5")
            .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(bucket_id.to_string())
            .bind(&pattern).bind(limit).fetch_all(pool).await.map_err(map_sqlx_error)?
    };
    let more = rows.len() > usize::from(request.limit);
    let visible = &rows[..rows.len().min(usize::from(request.limit))];
    let next = more
        .then(|| {
            visible
                .last()
                .and_then(|row| row.try_get::<String, _>("object_key").ok())
        })
        .flatten();
    let mut objects = Vec::new();
    let mut prefixes = BTreeSet::new();
    for row in visible {
        let object = decode_object(scope, bucket_id, row)?;
        let suffix = object
            .key
            .strip_prefix(&request.prefix)
            .ok_or(ObjectStorageError::Corruption)?;
        if request.delimiter == Some('/') {
            if let Some(position) = suffix.find('/') {
                prefixes.insert(format!("{}{}/", request.prefix, &suffix[..position]));
                continue;
            }
        }
        objects.push(object);
    }
    Ok(ObjectPage {
        scope,
        bucket_id,
        prefix: request.prefix.clone(),
        objects,
        common_prefixes: prefixes.into_iter().collect(),
        next,
    })
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

async fn insert_object_operation(
    tx: &mut Transaction<'_, Any>,
    operation: &ObjectOperation,
    digest: &[u8; 32],
) -> Result<(), ObjectStorageError> {
    sqlx::query("INSERT INTO runku_storage_object_operations(project_id,environment_id,operation_id,command_digest,kind,bucket_id,object_key,version_id,completed_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(operation.scope.project_id().to_string()).bind(operation.scope.environment_id().to_string())
        .bind(operation.operation_id.to_string()).bind(digest.as_slice()).bind(operation.kind)
        .bind(operation.bucket_id.to_string()).bind(&operation.key).bind(operation.version_id.to_string())
        .bind(operation.completed_at.get()).execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn insert_object_audit(
    tx: &mut Transaction<'_, Any>,
    operation: &ObjectOperation,
    actor: &str,
    size: Option<u64>,
) -> Result<(), ObjectStorageError> {
    let current = sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(sequence),0) FROM runku_storage_object_audit WHERE project_id=$1 AND environment_id=$2")
        .bind(operation.scope.project_id().to_string()).bind(operation.scope.environment_id().to_string())
        .fetch_one(&mut **tx).await.map_err(map_sqlx_error)?;
    let sequence = current
        .checked_add(1)
        .ok_or(ObjectStorageError::LimitExceeded)?;
    sqlx::query("INSERT INTO runku_storage_object_audit(project_id,environment_id,sequence,operation_id,kind,actor,bucket_id,object_key,version_id,size_bytes,occurred_at_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
        .bind(operation.scope.project_id().to_string()).bind(operation.scope.environment_id().to_string())
        .bind(sequence).bind(operation.operation_id.to_string()).bind(operation.kind).bind(actor)
        .bind(operation.bucket_id.to_string()).bind(&operation.key).bind(operation.version_id.to_string())
        .bind(size.map(to_i64).transpose()?).bind(operation.completed_at.get())
        .execute(&mut **tx).await.map_err(map_constraint_error)?;
    Ok(())
}

async fn load_object_operation(
    pool: &AnyPool,
    scope: EnvironmentScope,
    operation_id: OperationId,
) -> Result<Option<StoredObjectOperation>, ObjectStorageError> {
    let row = sqlx::query("SELECT command_digest,kind,bucket_id,object_key,version_id,completed_at_micros FROM runku_storage_object_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(operation_id.to_string())
        .fetch_optional(pool).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_object_operation(scope, operation_id, &row))
        .transpose()
}

async fn load_object_operation_tx(
    tx: &mut Transaction<'_, Any>,
    scope: EnvironmentScope,
    operation_id: OperationId,
) -> Result<Option<StoredObjectOperation>, ObjectStorageError> {
    let row = sqlx::query("SELECT command_digest,kind,bucket_id,object_key,version_id,completed_at_micros FROM runku_storage_object_operations WHERE project_id=$1 AND environment_id=$2 AND operation_id=$3")
        .bind(scope.project_id().to_string()).bind(scope.environment_id().to_string()).bind(operation_id.to_string())
        .fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.map(|row| decode_object_operation(scope, operation_id, &row))
        .transpose()
}

fn decode_object_operation(
    scope: EnvironmentScope,
    operation_id: OperationId,
    row: &sqlx::any::AnyRow,
) -> Result<StoredObjectOperation, ObjectStorageError> {
    let digest: Vec<u8> = row.try_get("command_digest").map_err(corrupt)?;
    if digest.len() != 32 {
        return Err(ObjectStorageError::Corruption);
    }
    let kind: String = row.try_get("kind").map_err(corrupt)?;
    let kind = match kind.as_str() {
        "put" => "put",
        "delete" => "delete",
        _ => return Err(ObjectStorageError::Corruption),
    };
    Ok(StoredObjectOperation {
        digest,
        operation: ObjectOperation {
            scope,
            operation_id,
            kind,
            bucket_id: parse_domain(row, "bucket_id")?,
            key: row.try_get("object_key").map_err(corrupt)?,
            version_id: parse_domain(row, "version_id")?,
            completed_at: TimestampMicros::new(
                row.try_get("completed_at_micros").map_err(corrupt)?,
            ),
        },
    })
}

fn parse_domain<T: FromStr>(row: &sqlx::any::AnyRow, field: &str) -> Result<T, ObjectStorageError> {
    row.try_get::<String, _>(field)
        .map_err(corrupt)?
        .parse()
        .map_err(corrupt)
}

async fn verify_configuration(
    pool: &AnyPool,
    backend: ObjectStorageRepositoryBackend,
) -> Result<(), ObjectStorageError> {
    match backend {
        ObjectStorageRepositoryBackend::SQLite => {
            let journal = sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            let foreign = sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            let synchronous = sqlx::query_scalar::<_, i64>("PRAGMA synchronous")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            let busy = sqlx::query_scalar::<_, i64>("PRAGMA busy_timeout")
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
            if !journal.eq_ignore_ascii_case("wal")
                || foreign != 1
                || synchronous != 2
                || busy != 5_000
            {
                return Err(ObjectStorageError::Corruption);
            }
        }
        ObjectStorageRepositoryBackend::PostgreSQL => {
            let row = sqlx::query("SELECT current_setting('statement_timeout') AS statement_timeout,current_setting('lock_timeout') AS lock_timeout,current_setting('idle_in_transaction_session_timeout') AS idle_timeout").fetch_one(pool).await.map_err(map_sqlx_error)?;
            let statement: String = row.try_get("statement_timeout").map_err(corrupt)?;
            let lock: String = row.try_get("lock_timeout").map_err(corrupt)?;
            let idle: String = row.try_get("idle_timeout").map_err(corrupt)?;
            if statement != "30s" || lock != "5s" || idle != "30s" {
                return Err(ObjectStorageError::Corruption);
            }
        }
    }
    Ok(())
}

async fn migrate(
    pool: &AnyPool,
    backend: ObjectStorageRepositoryBackend,
) -> Result<(), ObjectStorageError> {
    sqlx::query("CREATE TABLE IF NOT EXISTS runku_storage_schema_migrations(version BIGINT PRIMARY KEY,checksum TEXT NOT NULL,applied_at_micros BIGINT NOT NULL)").execute(pool).await.map_err(map_sqlx_error)?;
    let mut tx = begin_write(pool, backend).await?;
    if backend == ObjectStorageRepositoryBackend::PostgreSQL {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(7_224_856_118_i64)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
    }
    let stored = sqlx::query(
        "SELECT version,checksum FROM runku_storage_schema_migrations ORDER BY version",
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    for (index, row) in stored.iter().enumerate() {
        let version: i64 = row.try_get("version").map_err(corrupt)?;
        let checksum: String = row.try_get("checksum").map_err(corrupt)?;
        let Some((expected, statements)) = MIGRATIONS.get(index) else {
            return Err(ObjectStorageError::Unsupported);
        };
        if version != *expected || checksum != migration_checksum(version, statements) {
            return Err(ObjectStorageError::Corruption);
        }
    }
    for (version, statements) in MIGRATIONS {
        if stored
            .iter()
            .any(|row| row.try_get::<i64, _>("version").ok() == Some(*version))
        {
            continue;
        }
        for statement in *statements {
            tx.execute(*statement).await.map_err(map_sqlx_error)?;
        }
        sqlx::query("INSERT INTO runku_storage_schema_migrations(version,checksum,applied_at_micros) VALUES($1,$2,$3)").bind(*version).bind(migration_checksum(*version, statements)).bind(now_micros()?).execute(&mut *tx).await.map_err(map_sqlx_error)?;
    }
    tx.commit().await.map_err(map_commit_error)
}

fn migration_checksum(version: i64, statements: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"RUNKU_OBJECT_STORAGE_REPOSITORY_SCHEMA\0");
    digest.update(version.to_be_bytes());
    for statement in statements {
        digest.update(statement.as_bytes());
        digest.update([0]);
    }
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        let _ = write!(output, "{byte:02x}");
    }
    output
}
async fn begin_write(
    pool: &AnyPool,
    backend: ObjectStorageRepositoryBackend,
) -> Result<Transaction<'_, Any>, ObjectStorageError> {
    pool.begin_with(match backend {
        ObjectStorageRepositoryBackend::SQLite => "BEGIN IMMEDIATE",
        ObjectStorageRepositoryBackend::PostgreSQL => "BEGIN ISOLATION LEVEL SERIALIZABLE",
    })
    .await
    .map_err(map_sqlx_error)
}
async fn rollback<T>(
    tx: Transaction<'_, Any>,
    error: ObjectStorageError,
) -> Result<T, ObjectStorageError> {
    tx.rollback().await.map_err(map_sqlx_error)?;
    Err(error)
}
fn now_micros() -> Result<i64, ObjectStorageError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ObjectStorageError::Internal)?;
    i64::try_from(duration.as_micros()).map_err(|_| ObjectStorageError::Internal)
}
fn to_i64(value: u64) -> Result<i64, ObjectStorageError> {
    i64::try_from(value).map_err(|_| ObjectStorageError::LimitExceeded)
}
fn positive_u64(value: i64) -> Result<u64, ObjectStorageError> {
    if value <= 0 {
        return Err(ObjectStorageError::Corruption);
    }
    u64::try_from(value).map_err(|_| ObjectStorageError::Corruption)
}
fn map_constraint_error(error: sqlx::Error) -> ObjectStorageError {
    match &error {
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            ObjectStorageError::Conflict
        }
        _ => map_sqlx_error(error),
    }
}
fn map_commit_error(error: sqlx::Error) -> ObjectStorageError {
    match error {
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            ObjectStorageError::Conflict
        }
        sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_) => ObjectStorageError::ResultUncertain,
        other => map_sqlx_error(other),
    }
}
fn map_sqlx_error(error: sqlx::Error) -> ObjectStorageError {
    match error {
        sqlx::Error::PoolTimedOut => ObjectStorageError::Busy,
        sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_) => ObjectStorageError::Unavailable,
        sqlx::Error::Database(database)
            if database
                .code()
                .is_some_and(|code| code == "40001" || code == "40P01" || code == "5") =>
        {
            ObjectStorageError::Busy
        }
        sqlx::Error::Database(database)
            if database.is_unique_violation() || database.is_foreign_key_violation() =>
        {
            ObjectStorageError::Conflict
        }
        _ => ObjectStorageError::Corruption,
    }
}
fn corrupt<T>(_error: T) -> ObjectStorageError {
    ObjectStorageError::Corruption
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disconnected_commit_is_uncertain() {
        let error = map_commit_error(sqlx::Error::PoolClosed);
        assert_eq!(error, ObjectStorageError::ResultUncertain);
        assert!(error.retryable());
    }
    #[test]
    fn checksums_bind_versions() {
        assert_ne!(
            migration_checksum(1, MIGRATION_1),
            migration_checksum(2, MIGRATION_1)
        );
    }
}
