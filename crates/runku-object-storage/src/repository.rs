//! Durable repository boundary for logical Object Storage state.

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId};

use crate::{
    AccessKeyId, AccessKeyMetadata, AccessKeyPage, AccessKeyPageRequest, AuditPage,
    AuditPageRequest, Bucket, BucketId, BucketPage, BucketPageRequest, DeleteObjectCommand,
    EncryptedAccessKeyGeneration, ObjectMetadata, ObjectOperation, ObjectOperationResult,
    ObjectPage, ObjectPageRequest, ObjectStorageCommand, ObjectStorageError,
    ObjectStorageOperation, ObjectStorageOperationResult, PutObjectCommand,
};

/// Physical backend selected by composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStorageRepositoryBackend {
    /// Embedded local/test SQLite.
    SQLite,
    /// Authoritative PostgreSQL.
    PostgreSQL,
}

/// Bounded process-local repository telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectStorageTelemetrySnapshot {
    /// Newly committed commands.
    pub commands: u64,
    /// Exact operation-journal replays.
    pub replays: u64,
    /// CAS, name, or operation identity conflicts.
    pub conflicts: u64,
    /// Read queries.
    pub reads: u64,
    /// Retryable failures.
    pub retryable_errors: u64,
    /// Current pool size.
    pub pool_size: u32,
    /// Current idle connections.
    pub pool_idle: u32,
}

/// Durable provider-independent Object Storage registry.
#[async_trait]
pub trait ObjectStorageRepository: Send + Sync {
    /// Returns the selected backend.
    fn backend(&self) -> ObjectStorageRepositoryBackend;

    /// Applies one exact-scope idempotent command atomically with audit.
    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &ObjectStorageCommand,
    ) -> Result<ObjectStorageOperationResult, ObjectStorageError>;

    /// Gets one bucket without side effects.
    async fn get_bucket(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
    ) -> Result<Option<Bucket>, ObjectStorageError>;

    /// Lists one stable bucket page for an exact Environment.
    async fn list_buckets(
        &self,
        scope: EnvironmentScope,
        request: BucketPageRequest,
    ) -> Result<BucketPage, ObjectStorageError>;

    /// Gets non-secret key metadata from an exact bucket.
    async fn get_access_key(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        access_key_id: AccessKeyId,
    ) -> Result<Option<AccessKeyMetadata>, ObjectStorageError>;

    /// Lists non-secret access-key metadata.
    async fn list_access_keys(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: AccessKeyPageRequest,
    ) -> Result<AccessKeyPage, ObjectStorageError>;

    /// Resolves one active credential generation by exact scope and HMAC digest.
    async fn authenticate_access_key(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        access_key_id: AccessKeyId,
        secret_digest: &crate::SecretDigest,
        at: runku_value::TimestampMicros,
    ) -> Result<Option<AccessKeyMetadata>, ObjectStorageError>;

    /// Loads bounded currently valid encrypted generations by exact Environment and key ID.
    async fn encrypted_access_key_generations(
        &self,
        scope: EnvironmentScope,
        access_key_id: AccessKeyId,
        at: runku_value::TimestampMicros,
    ) -> Result<Vec<EncryptedAccessKeyGeneration>, ObjectStorageError>;

    /// Looks up an operation after an uncertain result.
    async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ObjectStorageOperation>, ObjectStorageError>;

    /// Lists append-only audit events for an exact Environment.
    async fn audit(
        &self,
        scope: EnvironmentScope,
        request: AuditPageRequest,
    ) -> Result<AuditPage, ObjectStorageError>;

    /// Atomically commits current/version metadata after immutable bytes are durable.
    async fn put_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        command: &PutObjectCommand,
    ) -> Result<ObjectOperationResult, ObjectStorageError>;

    /// Gets one current object without consulting the physical provider.
    async fn get_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStorageError>;

    /// Lists one stable current-object prefix/delimiter page.
    async fn list_objects(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: &ObjectPageRequest,
    ) -> Result<ObjectPage, ObjectStorageError>;

    /// Removes one exact current version while retaining immutable bytes for reconciliation/GC.
    async fn delete_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        command: &DeleteObjectCommand,
    ) -> Result<ObjectOperationResult, ObjectStorageError>;

    /// Looks up one object mutation after an uncertain commit acknowledgement.
    async fn object_operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ObjectOperation>, ObjectStorageError>;

    /// Performs a lightweight health query.
    async fn health(&self) -> Result<(), ObjectStorageError>;

    /// Returns bounded telemetry.
    fn telemetry(&self) -> ObjectStorageTelemetrySnapshot;
}
