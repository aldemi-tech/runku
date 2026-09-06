//! Durable repository boundary for logical Object Storage state.

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId};

use crate::{
    AccessKeyId, AccessKeyMetadata, AccessKeyPage, AccessKeyPageRequest, AuditPage,
    AuditPageRequest, Bucket, BucketId, BucketName, BucketPage, BucketPageRequest,
    DeleteObjectCommand, EncryptedAccessKeyGeneration, LifecycleResult, MultipartPart,
    MultipartUpload, MultipartUploadId, MultipartUploadPage, ObjectMetadata, ObjectOperation,
    ObjectOperationResult, ObjectPage, ObjectPageRequest, ObjectStorageCommand, ObjectStorageError,
    ObjectStorageOperation, ObjectStorageOperationResult, ObjectVersionPage,
    ObjectVersionPageRequest, PutObjectCommand,
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

    /// Gets one bucket by its exact Environment-unique logical name.
    async fn get_bucket_by_name(
        &self,
        scope: EnvironmentScope,
        name: &BucketName,
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

    /// Gets one immutable object version without consulting the physical provider.
    async fn get_object_version(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        key: &str,
        version_id: crate::ObjectVersionId,
    ) -> Result<Option<ObjectMetadata>, ObjectStorageError>;

    /// Lists one stable current-object prefix/delimiter page.
    async fn list_objects(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: &ObjectPageRequest,
    ) -> Result<ObjectPage, ObjectStorageError>;

    /// Lists immutable object versions, including versions that are no longer current.
    async fn list_object_versions(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: &ObjectVersionPageRequest,
    ) -> Result<ObjectVersionPage, ObjectStorageError>;

    /// Deletes one exact immutable version and promotes the newest remaining version when needed.
    async fn delete_object_version(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        key: &str,
        version_id: crate::ObjectVersionId,
    ) -> Result<bool, ObjectStorageError>;

    /// Creates one durable multipart upload.
    async fn create_multipart_upload(
        &self,
        upload: &MultipartUpload,
    ) -> Result<(), ObjectStorageError>;

    /// Gets multipart upload metadata by exact scope and ID.
    async fn get_multipart_upload(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: MultipartUploadId,
    ) -> Result<Option<MultipartUpload>, ObjectStorageError>;

    /// Upserts one immutable content-addressed part while the upload is active.
    async fn put_multipart_part(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: MultipartUploadId,
        part: &MultipartPart,
    ) -> Result<(), ObjectStorageError>;

    /// Lists all parts of one upload in ascending part-number order.
    async fn list_multipart_parts(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: MultipartUploadId,
    ) -> Result<Vec<MultipartPart>, ObjectStorageError>;

    /// Lists active uploads by key/upload cursor.
    async fn list_multipart_uploads(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        prefix: &str,
        after: Option<(&str, MultipartUploadId)>,
        limit: u16,
    ) -> Result<MultipartUploadPage, ObjectStorageError>;

    /// Claims completion with the digest of the exact ordered part request.
    async fn claim_multipart_completion(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: MultipartUploadId,
        completion_digest: [u8; 32],
    ) -> Result<(), ObjectStorageError>;

    /// Marks one upload completed with its exact object version, idempotently.
    async fn complete_multipart_upload(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: MultipartUploadId,
        version_id: crate::ObjectVersionId,
        at: runku_value::TimestampMicros,
    ) -> Result<(), ObjectStorageError>;

    /// Aborts one incomplete upload. Repeating an abort is safe.
    async fn abort_multipart_upload(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: MultipartUploadId,
    ) -> Result<(), ObjectStorageError>;

    /// Applies one bounded bucket lifecycle pass at a trusted time.
    async fn apply_lifecycle(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        at: runku_value::TimestampMicros,
        limit: u16,
    ) -> Result<LifecycleResult, ObjectStorageError>;

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
