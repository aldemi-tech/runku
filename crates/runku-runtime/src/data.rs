//! Typed read-only data broker contract exposed to capability-authorized Queries.

use std::{fmt, time::Instant};

use async_trait::async_trait;
use runku_core::{DocumentId, IndexId, TableId};
use runku_value::{CanonicalValue, TimestampMicros};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::CancellationToken;

/// Explicit lower/upper bound kind for one Index Key v1 byte string.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataBoundKind {
    /// Include the exact key.
    Inclusive,
    /// Exclude the exact key.
    Exclusive,
}

/// Optional encoded Index Key v1 range endpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataKeyBound {
    /// Inclusive/exclusive semantics.
    pub kind: DataBoundKind,
    /// Canonical Index Key v1 bytes.
    pub key: Vec<u8>,
}

/// Point-read request from a trusted Platform Op.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataGetRequest {
    /// Logical table.
    pub table_id: TableId,
    /// Opaque document.
    pub document_id: DocumentId,
}

/// Logical-index scan request from a trusted Platform Op.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataScanRequest {
    /// Logical index.
    pub index_id: IndexId,
    /// Optional lower endpoint.
    pub lower: Option<DataKeyBound>,
    /// Optional upper endpoint.
    pub upper: Option<DataKeyBound>,
    /// Maximum rows in `1..=1000`.
    pub limit: u32,
}

/// Comparison supported by the bounded logical table-query planner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataQueryOperator {
    /// Equal canonical values.
    Equal,
    /// Unequal canonical values.
    NotEqual,
    /// Strictly greater comparable scalar.
    GreaterThan,
    /// Greater-or-equal comparable scalar.
    GreaterThanOrEqual,
    /// Strictly less comparable scalar.
    LessThan,
    /// Less-or-equal comparable scalar.
    LessThanOrEqual,
    /// Unicode substring or exact array-element membership.
    Contains,
    /// Normalized Unicode whole-word match backed by a declared search index.
    Search,
}

/// One validated user-field predicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataQueryFilter {
    /// Dotted object-property path.
    pub field: String,
    /// Comparison operation.
    pub operator: DataQueryOperator,
    /// Canonical comparison value.
    pub value: CanonicalValue,
}

/// Stable ordering direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataQueryDirection {
    /// Smallest values first.
    Ascending,
    /// Largest values first.
    Descending,
}

/// One requested user or system-field ordering component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataQueryOrder {
    /// Dotted field path or `$createdAt`, `$updatedAt`, `$id`.
    pub field: String,
    /// Ordering direction.
    pub direction: DataQueryDirection,
}

/// One bounded logical table query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataQueryRequest {
    /// Logical table.
    pub table_id: TableId,
    /// Conjunctive predicates.
    pub filters: Vec<DataQueryFilter>,
    /// Ordering components. Empty selects the platform default.
    pub order: Vec<DataQueryOrder>,
    /// Maximum returned documents in `1..=200`.
    pub limit: u32,
    /// Opaque continuation token from an identical query.
    pub cursor: Option<String>,
}

/// One bounded logical table-query page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataQueryPage {
    /// Matching documents in stable requested order.
    pub documents: Vec<DataDocument>,
    /// Opaque continuation token, when more matches exist.
    pub next_cursor: Option<String>,
}

/// One application document observed through the Query snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataDocument {
    /// Logical table.
    pub table_id: TableId,
    /// Opaque document.
    pub document_id: DocumentId,
    /// Positive OCC revision.
    pub revision: u64,
    /// Commit sequence that wrote this revision.
    pub commit_sequence: u64,
    /// Creation time.
    pub created_at: TimestampMicros,
    /// Last update time.
    pub updated_at: TimestampMicros,
    /// Canonical application value.
    pub value: CanonicalValue,
}

/// One logical-index entry observed through the Query snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataIndexEntry {
    /// Logical index.
    pub index_id: IndexId,
    /// Canonical Index Key v1 bytes.
    pub key: Vec<u8>,
    /// Referenced logical table.
    pub table_id: TableId,
    /// Referenced document.
    pub document_id: DocumentId,
    /// Revision represented by this entry.
    pub document_revision: u64,
    /// Commit sequence that wrote this entry.
    pub commit_sequence: u64,
}

/// Sanitized failure returned by a Data Read broker.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DataReadError {
    /// Request IDs, range, key bytes, or limits are invalid.
    #[error("data read request is invalid")]
    InvalidRequest,
    /// The trusted data broker is unavailable.
    #[error("data read broker is unavailable")]
    Unavailable,
    /// Storage rejected or failed the operation.
    #[error("data read failed")]
    Storage,
    /// Invocation deadline elapsed.
    #[error("data read timed out")]
    Timeout,
    /// Invocation was explicitly cancelled.
    #[error("data read was cancelled")]
    Cancelled,
    /// Aggregate invocation data limits were exceeded.
    #[error("data read exceeds a limit")]
    LimitExceeded,
    /// An unindexed query exceeded the bounded table-scan threshold.
    #[error("data query requires an index")]
    QueryRequiresIndex,
}

impl DataReadError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "DATA_READ_INVALID",
            Self::Unavailable => "DATA_READ_UNAVAILABLE",
            Self::Storage => "DATA_READ_STORAGE_FAILED",
            Self::Timeout => "DATA_READ_TIMEOUT",
            Self::Cancelled => "DATA_READ_CANCELLED",
            Self::LimitExceeded => "DATA_READ_LIMIT_EXCEEDED",
            Self::QueryRequiresIndex => "DATA_QUERY_REQUIRES_INDEX",
        }
    }
}

/// Read-only data authority injected into one trusted Query invocation.
#[async_trait]
pub trait DataRead: fmt::Debug + Send + Sync {
    /// Reads one document from the invocation snapshot.
    async fn get(
        &self,
        request: DataGetRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<DataDocument>, DataReadError>;

    /// Scans one logical index from the same invocation snapshot.
    async fn scan(
        &self,
        request: DataScanRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Vec<DataIndexEntry>, DataReadError>;

    /// Executes one bounded table query from the same invocation snapshot.
    async fn query(
        &self,
        _request: DataQueryRequest,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<DataQueryPage, DataReadError> {
        Err(DataReadError::InvalidRequest)
    }
}

/// Buffered document write authority injected into one trusted Mutation invocation.
#[async_trait]
pub trait DataWrite: fmt::Debug + Send + Sync {
    /// Buffers an insert that requires the document to be absent.
    async fn insert(
        &self,
        table_id: TableId,
        document_id: DocumentId,
        value: CanonicalValue,
    ) -> Result<(), DataReadError>;

    /// Buffers a replacement at one exact positive revision.
    async fn replace(
        &self,
        table_id: TableId,
        document_id: DocumentId,
        expected_revision: u64,
        value: CanonicalValue,
    ) -> Result<(), DataReadError>;

    /// Buffers a deletion at one exact positive revision.
    async fn delete(
        &self,
        table_id: TableId,
        document_id: DocumentId,
        expected_revision: u64,
    ) -> Result<(), DataReadError>;
}
