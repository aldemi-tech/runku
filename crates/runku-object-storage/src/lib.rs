//! Provider-independent logical Object Storage registry.
//!
//! This crate owns Product bucket configuration, object metadata and scoped Product access-key
//! contracts. Physical bytes remain behind a provider boundary and the crate deliberately has no
//! dependency on S3, HTTP, SQL, or a runtime.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod model;
mod repository;
mod service;

pub use error::ObjectStorageError;
pub use model::{
    AccessKeyConfiguration, AccessKeyId, AccessKeyMetadata, AccessKeyOperation, AccessKeyPage,
    AccessKeyPageRequest, AccessKeySecret, AccessKeyState, AuditEvent, AuditPage, AuditPageRequest,
    Bucket, BucketConfiguration, BucketId, BucketLifecycle, BucketName, BucketPage,
    BucketPageRequest, BucketPolicy, BucketQuota, BucketState, CorsMethod, CorsRule,
    DeleteObjectCommand, IssuedAccessKey, ObjectMetadata, ObjectOperation, ObjectOperationResult,
    ObjectPage, ObjectPageRequest, ObjectStorageActor, ObjectStorageCommand,
    ObjectStorageOperation, ObjectStorageOperationKind, ObjectStorageOperationResult,
    ObjectVersionId, PutObjectCommand, SecretDigest, Versioning, object_etag, validate_object_key,
};
pub use repository::{
    ObjectStorageRepository, ObjectStorageRepositoryBackend, ObjectStorageTelemetrySnapshot,
};
pub use service::{ObjectStorageService, SecretDigestKey};
