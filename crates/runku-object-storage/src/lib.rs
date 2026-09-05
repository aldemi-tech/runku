//! Provider-independent logical Object Storage registry.
//!
//! This crate owns Product bucket configuration and scoped Product access-key contracts. It does
//! not store object bytes and deliberately has no dependency on S3, HTTP, SQL, or a runtime.

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
    IssuedAccessKey, ObjectStorageActor, ObjectStorageCommand, ObjectStorageOperation,
    ObjectStorageOperationKind, ObjectStorageOperationResult, SecretDigest, Versioning,
};
pub use repository::{
    ObjectStorageRepository, ObjectStorageRepositoryBackend, ObjectStorageTelemetrySnapshot,
};
pub use service::{ObjectStorageService, SecretDigestKey};
