//! Capability-scoped application file storage broker contracts.

use std::{fmt, time::Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::CancellationToken;

/// Stable mediated file-storage failure exposed to Function runtimes.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FileStorageError {
    /// A caller supplied an invalid identifier, media type, checksum, size, or expiry.
    #[error("file storage request is invalid")]
    InvalidRequest,
    /// The requested file or transfer grant does not exist.
    #[error("file storage resource was not found")]
    NotFound,
    /// A one-shot grant was already consumed or the lifecycle state conflicts.
    #[error("file storage operation conflicts")]
    Conflict,
    /// The Environment quota, per-file bound, or action-memory bound was exceeded.
    #[error("file storage limit was exceeded")]
    LimitExceeded,
    /// The selected capability does not authorize the operation.
    #[error("file storage capability was denied")]
    Forbidden,
    /// The backing metadata or object store is temporarily unavailable.
    #[error("file storage is unavailable")]
    Unavailable,
    /// The caller deadline elapsed.
    #[error("file storage operation timed out")]
    Timeout,
    /// The invocation was cancelled.
    #[error("file storage operation was cancelled")]
    Cancelled,
    /// Persisted metadata and stored bytes disagree.
    #[error("file storage state is corrupt")]
    Corruption,
}

impl FileStorageError {
    /// Stable machine-readable code safe to expose to Function code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "FILE_STORAGE_REQUEST_INVALID",
            Self::NotFound => "FILE_STORAGE_NOT_FOUND",
            Self::Conflict => "FILE_STORAGE_CONFLICT",
            Self::LimitExceeded => "FILE_STORAGE_LIMIT_EXCEEDED",
            Self::Forbidden => "FILE_STORAGE_FORBIDDEN",
            Self::Unavailable => "FILE_STORAGE_UNAVAILABLE",
            Self::Timeout => "FILE_STORAGE_TIMEOUT",
            Self::Cancelled => "FILE_STORAGE_CANCELLED",
            Self::Corruption => "FILE_STORAGE_CORRUPT",
        }
    }
}

/// Request for a one-shot, short-lived HTTP upload grant.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileUploadGrantRequest {
    /// Maximum accepted bytes for this upload.
    pub max_bytes: u64,
    /// Optional exact media type required on the HTTP request.
    pub content_type: Option<String>,
    /// Optional lowercase hexadecimal SHA-256 required at commit.
    pub sha256: Option<String>,
}

/// Request for storing bounded bytes directly from an Action.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileStoreRequest {
    /// File bytes. Runtime-specific bridges must bound this before allocation.
    pub bytes: Vec<u8>,
    /// Optional trusted response media type after strict validation.
    pub content_type: Option<String>,
    /// Optional lowercase hexadecimal SHA-256 required at commit.
    pub sha256: Option<String>,
}

/// Request for a short-lived HTTP download grant.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileDownloadGrantRequest {
    /// Canonical opaque file identifier.
    pub file_id: String,
    /// Requested validity in microseconds, encoded as an unsigned decimal string.
    pub expires_in_micros: String,
}

/// Immutable public file metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    /// Canonical opaque file identifier.
    pub file_id: String,
    /// Exact committed byte length encoded as an unsigned decimal string.
    pub size_bytes: String,
    /// Lowercase hexadecimal SHA-256 of committed bytes.
    pub sha256: String,
    /// Validated media type, or `application/octet-stream` when unspecified.
    pub content_type: String,
    /// Creation time in Unix microseconds encoded as a signed decimal string.
    pub created_at_micros: String,
}

/// One-shot authorization for the raw HTTP upload endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileUploadGrant {
    /// Canonical upload identifier used only in the route path.
    pub upload_id: String,
    /// Same-origin API path; callers resolve it against their configured Runku origin.
    pub path: String,
    /// Secret bearer credential. It must never be logged or placed in a URL.
    pub token: String,
    /// Expiration time in Unix microseconds encoded as a signed decimal string.
    pub expires_at_micros: String,
    /// Maximum bytes reserved for the upload encoded as an unsigned decimal string.
    pub max_bytes: String,
}

/// Short-lived authorization for the raw HTTP download endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileDownloadGrant {
    /// Same-origin API path; callers resolve it against their configured Runku origin.
    pub path: String,
    /// Secret bearer credential. It must never be logged or placed in a URL.
    pub token: String,
    /// Expiration time in Unix microseconds encoded as a signed decimal string.
    pub expires_at_micros: String,
    /// Metadata pinned when the grant was issued.
    pub metadata: FileMetadata,
}

/// Bounded bytes read directly by an Action.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileBytes {
    /// Metadata verified before returning the bytes.
    pub metadata: FileMetadata,
    /// Exact file bytes.
    pub bytes: Vec<u8>,
}

/// Server-owned application file storage authority exposed through Platform Ops.
#[async_trait]
pub trait FileStorage: fmt::Debug + Send + Sync {
    /// Reserves quota and creates a one-shot upload authorization.
    async fn create_upload_grant(
        &self,
        request: FileUploadGrantRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileUploadGrant, FileStorageError>;

    /// Stores a bounded in-memory object from an Action.
    async fn store(
        &self,
        request: FileStoreRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileMetadata, FileStorageError>;

    /// Reads immutable metadata for one ready file.
    async fn metadata(
        &self,
        file_id: String,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileMetadata, FileStorageError>;

    /// Creates a short-lived download authorization.
    async fn create_download_grant(
        &self,
        request: FileDownloadGrantRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileDownloadGrant, FileStorageError>;

    /// Reads a bounded object directly into an Action.
    async fn get(
        &self,
        file_id: String,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FileBytes, FileStorageError>;

    /// Revokes access and removes the backing object idempotently.
    async fn delete(
        &self,
        file_id: String,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), FileStorageError>;
}
