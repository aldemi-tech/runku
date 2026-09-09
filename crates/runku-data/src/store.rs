//! Async object-safe `LogicalStore` interfaces.

use async_trait::async_trait;
use runku_core::{DocumentId, IndexId, OutboxEventId, ScheduledInvocationId, TableId, WorkerId};
use runku_schema::SchemaCatalog;
use runku_value::{CanonicalValue, TimestampMicros};

use crate::{
    ClaimedOutboxBatch, ClaimedScheduledInvocation, CommitBatch, CommitResult, DocumentRecord,
    EnvironmentScope, IndexEntry, IndexRange, IndexScanCursor, IndexScanDirection,
    OutboxConsumerName, OutboxCursor, ScheduleCancelResult, ScheduleCompletion,
    ScheduledInvocationRecord, StoreError, StoreTelemetrySnapshot, TableScanCursor,
};

/// Physical backend selected by composition.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StoreBackend {
    /// Embedded local `SQLite`.
    SQLite,
    /// Authoritative `PostgreSQL`.
    PostgreSQL,
    /// Distributed PostgreSQL-compatible `YugabyteDB` YSQL.
    YugabyteDB,
}

/// Durable progress from one bounded logical-index preparation step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexPreparation {
    /// True only when every definition in the requested catalog is fully backfilled.
    pub ready: bool,
    /// Documents reconciled by this step.
    pub processed_documents: u32,
}

/// A real consistent read transaction.
#[async_trait]
pub trait ReadSnapshot: Send {
    /// Commit sequence visible when the snapshot was established.
    fn commit_sequence(&self) -> u64;

    /// Reads one current document inside this snapshot.
    async fn get_document(
        &mut self,
        table_id: TableId,
        document_id: DocumentId,
    ) -> Result<Option<DocumentRecord>, StoreError>;

    /// Scans one logical table in stable `(created_at DESC, document_id DESC)` order.
    ///
    /// This primitive exists for bounded query planning; callers must enforce their own scan cap.
    async fn scan_documents(
        &mut self,
        table_id: TableId,
        after: Option<TableScanCursor>,
        limit: u32,
    ) -> Result<Vec<DocumentRecord>, StoreError>;

    /// Scans one logical index in `(key, document_id)` order.
    async fn scan_index(
        &mut self,
        index_id: IndexId,
        range: &IndexRange,
        limit: u32,
    ) -> Result<Vec<IndexEntry>, StoreError>;

    /// Scans one logical index with stable keyset pagination in either direction.
    async fn scan_index_page(
        &mut self,
        index_id: IndexId,
        range: &IndexRange,
        after: Option<IndexScanCursor>,
        direction: IndexScanDirection,
        limit: u32,
    ) -> Result<Vec<IndexEntry>, StoreError>;

    /// Reads a durable outbox payload for conformance/dispatcher use.
    async fn get_outbox(
        &mut self,
        event_id: OutboxEventId,
    ) -> Result<Option<CanonicalValue>, StoreError>;

    /// Reads one Scheduled Invocation.
    async fn get_scheduled(
        &mut self,
        id: ScheduledInvocationId,
    ) -> Result<Option<ScheduledInvocationRecord>, StoreError>;

    /// Lists Scheduled Invocations in stable ID order after an optional exclusive cursor.
    async fn list_scheduled(
        &mut self,
        after: Option<ScheduledInvocationId>,
        limit: u32,
    ) -> Result<Vec<ScheduledInvocationRecord>, StoreError>;

    /// Closes the read transaction explicitly.
    async fn close(self: Box<Self>) -> Result<(), StoreError>;
}

/// Logical persistence contract implemented identically by `SQLite` and `PostgreSQL` adapters.
#[async_trait]
pub trait LogicalStore: Send + Sync {
    /// Returns the selected physical backend.
    fn backend(&self) -> StoreBackend;

    /// Opens a consistent read transaction bound to one Environment.
    async fn begin_read(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Box<dyn ReadSnapshot>, StoreError>;

    /// Validates and commits documents/index/outbox/scheduling atomically.
    async fn commit(&self, batch: &CommitBatch) -> Result<CommitResult, StoreError>;

    /// Registers and advances a bounded, resumable index backfill for one catalog.
    async fn prepare_index_catalog(
        &self,
        _scope: EnvironmentScope,
        _catalog: &SchemaCatalog,
        _document_limit: u32,
    ) -> Result<IndexPreparation, StoreError> {
        Err(StoreError::Internal)
    }

    /// Merges release indexes with every durable building/ready projection for dual writes.
    async fn effective_write_index_catalog(
        &self,
        _scope: EnvironmentScope,
        release_catalog: &SchemaCatalog,
    ) -> Result<SchemaCatalog, StoreError> {
        Ok(release_catalog.clone())
    }

    /// Claims the next ordered outbox batch under a fenced consumer lease.
    async fn claim_outbox(
        &self,
        scope: EnvironmentScope,
        consumer: &OutboxConsumerName,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<ClaimedOutboxBatch, StoreError>;

    /// Advances a consumer cursor iff worker/generation own the exact persisted claim.
    async fn ack_outbox(
        &self,
        scope: EnvironmentScope,
        consumer: &OutboxConsumerName,
        worker_id: WorkerId,
        lease_generation: u64,
        through: OutboxCursor,
    ) -> Result<(), StoreError>;

    /// Atomically claims due or lease-expired Scheduled Invocations.
    async fn claim_due_scheduled(
        &self,
        scope: EnvironmentScope,
        worker_id: WorkerId,
        now: TimestampMicros,
        lease_until: TimestampMicros,
        limit: u32,
    ) -> Result<Vec<ClaimedScheduledInvocation>, StoreError>;

    /// Completes or reschedules an invocation iff worker/generation still own its lease.
    async fn complete_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
        worker_id: WorkerId,
        lease_generation: u64,
        completion: &ScheduleCompletion,
    ) -> Result<(), StoreError>;

    /// Cancels pending work or reports its exact already-running/terminal state.
    async fn cancel_scheduled(
        &self,
        scope: EnvironmentScope,
        id: ScheduledInvocationId,
    ) -> Result<ScheduleCancelResult, StoreError>;

    /// Performs a lightweight backend health query.
    async fn health(&self) -> Result<(), StoreError>;

    /// Returns bounded process-local counters and current pool state.
    fn telemetry(&self) -> StoreTelemetrySnapshot;
}
