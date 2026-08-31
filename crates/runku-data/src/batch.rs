//! Declarative atomic commit batches.

use std::collections::BTreeSet;

use runku_core::{DocumentId, IndexId, OperationId, OutboxEventId, ScheduledInvocationId, TableId};
use runku_value::{CanonicalValue, IndexKey, TimestampMicros, encode_stored_value};
use sha2::{Digest, Sha256};

use crate::{EnvironmentScope, FunctionName, PinnedCode, StoreError};

/// Hard v1 limits applied before opening a write transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitLimits {
    /// Maximum document read assertions.
    pub reads: usize,
    /// Maximum document mutations.
    pub documents: usize,
    /// Maximum index mutations.
    pub indexes: usize,
    /// Maximum outbox appends.
    pub outbox: usize,
    /// Maximum scheduled inserts.
    pub schedules: usize,
    /// Maximum aggregate encoded value/key bytes.
    pub payload_bytes: usize,
}

impl CommitLimits {
    /// Product contract for `CommitBatch` v1.
    pub const V1: Self = Self {
        reads: 10_000,
        documents: 1_000,
        indexes: 10_000,
        outbox: 100,
        schedules: 100,
        payload_bytes: 8 * 1024 * 1024,
    };
}

/// One document state observed while a Mutation Function evaluated.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DocumentReadAssertion {
    /// Logical table.
    pub table_id: TableId,
    /// Document ID.
    pub document_id: DocumentId,
    /// Revision observed, or `None` when the document was absent.
    pub observed_revision: Option<u64>,
}

/// OCC expectation for a document mutation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExpectedRevision {
    /// Document must not exist.
    Absent,
    /// Document must exist at this positive revision.
    Exact(u64),
}

/// Upsert or delete applied to one document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DocumentMutation {
    /// Insert or replace a document after checking its revision.
    Upsert {
        /// Logical table.
        table_id: TableId,
        /// Document ID.
        document_id: DocumentId,
        /// Required prior state.
        expected: ExpectedRevision,
        /// New canonical value.
        value: CanonicalValue,
    },
    /// Delete a document after checking its revision.
    Delete {
        /// Logical table.
        table_id: TableId,
        /// Document ID.
        document_id: DocumentId,
        /// Required current positive revision.
        expected_revision: u64,
    },
}

impl DocumentMutation {
    /// Returns the logical table.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        match self {
            Self::Upsert { table_id, .. } | Self::Delete { table_id, .. } => *table_id,
        }
    }

    /// Returns the document ID.
    #[must_use]
    pub const fn document_id(&self) -> DocumentId {
        match self {
            Self::Upsert { document_id, .. } | Self::Delete { document_id, .. } => *document_id,
        }
    }
}

/// Atomic logical-index entry change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexMutation {
    /// Insert or replace one entry.
    Put {
        /// Logical index.
        index_id: IndexId,
        /// Ordered compound key.
        key: IndexKey,
        /// Referenced table.
        table_id: TableId,
        /// Referenced document.
        document_id: DocumentId,
        /// Revision represented after the batch.
        document_revision: u64,
    },
    /// Remove one entry.
    Delete {
        /// Logical index.
        index_id: IndexId,
        /// Ordered compound key.
        key: IndexKey,
        /// Referenced document.
        document_id: DocumentId,
    },
}

impl IndexMutation {
    fn identity(&self) -> (IndexId, &[u8], DocumentId) {
        match self {
            Self::Put {
                index_id,
                key,
                document_id,
                ..
            }
            | Self::Delete {
                index_id,
                key,
                document_id,
            } => (*index_id, key.as_bytes(), *document_id),
        }
    }
}

/// Durable outbox payload appended in the same commit as application writes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxAppend {
    /// Caller-generated event ID, unique in the Environment.
    pub event_id: OutboxEventId,
    /// Versioned canonical payload.
    pub payload: CanonicalValue,
}

/// Scheduled Invocation inserted by a Mutation, Action, or Cron materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledInvocationInsert {
    /// Stable record ID.
    pub id: ScheduledInvocationId,
    /// Exact Release or Dev Revision.
    pub pinned_code: PinnedCode,
    /// Destination Mutation/Action name.
    pub function: FunctionName,
    /// Canonical arguments.
    pub args: CanonicalValue,
    /// First eligible execution time.
    pub execute_at: TimestampMicros,
    /// Optional application-level deduplication key, maximum 128 bytes.
    pub idempotency_key: Option<String>,
}

/// Validated declarative unit committed atomically by any adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitBatch {
    scope: EnvironmentScope,
    operation_id: OperationId,
    reads: Vec<DocumentReadAssertion>,
    documents: Vec<DocumentMutation>,
    indexes: Vec<IndexMutation>,
    outbox: Vec<OutboxAppend>,
    schedules: Vec<ScheduledInvocationInsert>,
}

impl CommitBatch {
    /// Creates an empty builder bound to one Environment and operation ID.
    #[must_use]
    pub const fn new(scope: EnvironmentScope, operation_id: OperationId) -> Self {
        Self {
            scope,
            operation_id,
            reads: Vec::new(),
            documents: Vec::new(),
            indexes: Vec::new(),
            outbox: Vec::new(),
            schedules: Vec::new(),
        }
    }

    /// Appends one document read assertion used by Mutation OCC.
    pub fn push_read(&mut self, assertion: DocumentReadAssertion) {
        self.reads.push(assertion);
    }

    /// Appends one document mutation.
    pub fn push_document(&mut self, mutation: DocumentMutation) {
        self.documents.push(mutation);
    }

    /// Appends one logical-index mutation.
    pub fn push_index(&mut self, mutation: IndexMutation) {
        self.indexes.push(mutation);
    }

    /// Appends one durable outbox record.
    pub fn push_outbox(&mut self, append: OutboxAppend) {
        self.outbox.push(append);
    }

    /// Appends one Scheduled Invocation insert.
    pub fn push_schedule(&mut self, schedule: ScheduledInvocationInsert) {
        self.schedules.push(schedule);
    }

    /// Returns the explicit tenant scope.
    #[must_use]
    pub const fn scope(&self) -> EnvironmentScope {
        self.scope
    }

    /// Returns the durable idempotency identity.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Returns document read assertions in caller order.
    #[must_use]
    pub fn reads(&self) -> &[DocumentReadAssertion] {
        &self.reads
    }

    /// Returns document mutations in caller order.
    #[must_use]
    pub fn documents(&self) -> &[DocumentMutation] {
        &self.documents
    }

    /// Returns index mutations in caller order.
    #[must_use]
    pub fn indexes(&self) -> &[IndexMutation] {
        &self.indexes
    }

    /// Returns outbox appends in caller order.
    #[must_use]
    pub fn outbox(&self) -> &[OutboxAppend] {
        &self.outbox
    }

    /// Returns schedule inserts in caller order.
    #[must_use]
    pub fn schedules(&self) -> &[ScheduledInvocationInsert] {
        &self.schedules
    }

    /// Validates v1 counts, encoded bytes, positive revisions, and duplicate identities.
    ///
    /// # Errors
    ///
    /// Returns a stable [`StoreError`] before any adapter opens a transaction.
    pub fn validate(&self) -> Result<(), StoreError> {
        if self.documents.is_empty()
            && self.indexes.is_empty()
            && self.outbox.is_empty()
            && self.schedules.is_empty()
        {
            return Err(StoreError::EmptyBatch);
        }
        if self.reads.len() > CommitLimits::V1.reads
            || self.documents.len() > CommitLimits::V1.documents
            || self.indexes.len() > CommitLimits::V1.indexes
            || self.outbox.len() > CommitLimits::V1.outbox
            || self.schedules.len() > CommitLimits::V1.schedules
        {
            return Err(StoreError::LimitExceeded);
        }

        let mut read_keys = BTreeSet::new();
        for assertion in &self.reads {
            if assertion.observed_revision == Some(0) {
                return Err(StoreError::MutationConflict);
            }
            if !read_keys.insert((assertion.table_id, assertion.document_id)) {
                return Err(StoreError::DuplicateMutation);
            }
        }

        let mut document_keys = BTreeSet::new();
        for mutation in &self.documents {
            if !document_keys.insert((mutation.table_id(), mutation.document_id())) {
                return Err(StoreError::DuplicateMutation);
            }
            match mutation {
                DocumentMutation::Upsert {
                    expected: ExpectedRevision::Exact(0),
                    ..
                }
                | DocumentMutation::Delete {
                    expected_revision: 0,
                    ..
                } => return Err(StoreError::MutationConflict),
                DocumentMutation::Upsert { .. } | DocumentMutation::Delete { .. } => {}
            }
        }

        let mut index_keys = BTreeSet::new();
        for mutation in &self.indexes {
            let (index, key, document) = mutation.identity();
            if !index_keys.insert((index, key.to_vec(), document)) {
                return Err(StoreError::DuplicateMutation);
            }
            if matches!(
                mutation,
                IndexMutation::Put {
                    document_revision: 0,
                    ..
                }
            ) {
                return Err(StoreError::MutationConflict);
            }
        }

        let mut event_ids = BTreeSet::new();
        for event in &self.outbox {
            if !event_ids.insert(event.event_id) {
                return Err(StoreError::DuplicateMutation);
            }
        }

        let mut schedule_ids = BTreeSet::new();
        let mut schedule_keys = BTreeSet::new();
        for schedule in &self.schedules {
            if !schedule_ids.insert(schedule.id) {
                return Err(StoreError::DuplicateMutation);
            }
            if let Some(key) = &schedule.idempotency_key
                && (key.is_empty() || key.len() > 128 || !schedule_keys.insert(key))
            {
                return Err(StoreError::DuplicateMutation);
            }
        }

        let payload_bytes = self.payload_bytes()?;
        if payload_bytes > CommitLimits::V1.payload_bytes {
            return Err(StoreError::LimitExceeded);
        }
        Ok(())
    }

    /// Returns the canonical SHA-256 digest persisted with the operation journal.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] if a value cannot be encoded or the batch is invalid.
    pub fn digest(&self) -> Result<[u8; 32], StoreError> {
        self.validate()?;
        let mut hash = Sha256::new();
        hash.update(b"RUNKU_COMMIT_BATCH_V1");
        hash_text(&mut hash, &self.scope.project_id().to_string())?;
        hash_text(&mut hash, &self.scope.environment_id().to_string())?;

        hash_count(&mut hash, self.reads.len())?;
        for assertion in &self.reads {
            hash_text(&mut hash, &assertion.table_id.to_string())?;
            hash_text(&mut hash, &assertion.document_id.to_string())?;
            match assertion.observed_revision {
                Some(revision) => {
                    hash.update([1]);
                    hash.update(revision.to_be_bytes());
                }
                None => hash.update([0]),
            }
        }

        hash_count(&mut hash, self.documents.len())?;
        for mutation in &self.documents {
            match mutation {
                DocumentMutation::Upsert {
                    table_id,
                    document_id,
                    expected,
                    value,
                } => {
                    hash.update([0]);
                    hash_text(&mut hash, &table_id.to_string())?;
                    hash_text(&mut hash, &document_id.to_string())?;
                    hash_expected(&mut hash, *expected);
                    hash_bytes(&mut hash, &encode_value(value)?)?;
                }
                DocumentMutation::Delete {
                    table_id,
                    document_id,
                    expected_revision,
                } => {
                    hash.update([1]);
                    hash_text(&mut hash, &table_id.to_string())?;
                    hash_text(&mut hash, &document_id.to_string())?;
                    hash.update(expected_revision.to_be_bytes());
                }
            }
        }

        hash_count(&mut hash, self.indexes.len())?;
        for mutation in &self.indexes {
            match mutation {
                IndexMutation::Put {
                    index_id,
                    key,
                    table_id,
                    document_id,
                    document_revision,
                } => {
                    hash.update([0]);
                    hash_text(&mut hash, &index_id.to_string())?;
                    hash_bytes(&mut hash, key.as_bytes())?;
                    hash_text(&mut hash, &table_id.to_string())?;
                    hash_text(&mut hash, &document_id.to_string())?;
                    hash.update(document_revision.to_be_bytes());
                }
                IndexMutation::Delete {
                    index_id,
                    key,
                    document_id,
                } => {
                    hash.update([1]);
                    hash_text(&mut hash, &index_id.to_string())?;
                    hash_bytes(&mut hash, key.as_bytes())?;
                    hash_text(&mut hash, &document_id.to_string())?;
                }
            }
        }

        hash_count(&mut hash, self.outbox.len())?;
        for event in &self.outbox {
            hash_text(&mut hash, &event.event_id.to_string())?;
            hash_bytes(&mut hash, &encode_value(&event.payload)?)?;
        }

        hash_count(&mut hash, self.schedules.len())?;
        for schedule in &self.schedules {
            hash_text(&mut hash, &schedule.id.to_string())?;
            hash_text(&mut hash, &schedule.pinned_code.to_string())?;
            hash_text(&mut hash, schedule.function.as_str())?;
            hash_bytes(&mut hash, &encode_value(&schedule.args)?)?;
            hash.update(schedule.execute_at.get().to_be_bytes());
            match &schedule.idempotency_key {
                Some(value) => {
                    hash.update([1]);
                    hash_text(&mut hash, value)?;
                }
                None => hash.update([0]),
            }
        }

        Ok(hash.finalize().into())
    }

    fn payload_bytes(&self) -> Result<usize, StoreError> {
        let mut total = 0_usize;
        for mutation in &self.documents {
            if let DocumentMutation::Upsert { value, .. } = mutation {
                total = add_payload(total, encode_value(value)?.len())?;
            }
        }
        for mutation in &self.indexes {
            let (_, key, _) = mutation.identity();
            total = add_payload(total, key.len())?;
        }
        for event in &self.outbox {
            total = add_payload(total, encode_value(&event.payload)?.len())?;
        }
        for schedule in &self.schedules {
            total = add_payload(total, encode_value(&schedule.args)?.len())?;
        }
        Ok(total)
    }
}

fn encode_value(value: &CanonicalValue) -> Result<Vec<u8>, StoreError> {
    encode_stored_value(value).map_err(|_| StoreError::LimitExceeded)
}

fn add_payload(total: usize, amount: usize) -> Result<usize, StoreError> {
    total
        .checked_add(amount)
        .filter(|value| *value <= CommitLimits::V1.payload_bytes)
        .ok_or(StoreError::LimitExceeded)
}

fn hash_expected(hash: &mut Sha256, expected: ExpectedRevision) {
    match expected {
        ExpectedRevision::Absent => hash.update([0]),
        ExpectedRevision::Exact(value) => {
            hash.update([1]);
            hash.update(value.to_be_bytes());
        }
    }
}

fn hash_count(hash: &mut Sha256, count: usize) -> Result<(), StoreError> {
    let count = u32::try_from(count).map_err(|_| StoreError::LimitExceeded)?;
    hash.update(count.to_be_bytes());
    Ok(())
}

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), StoreError> {
    hash_bytes(hash, value.as_bytes())
}

fn hash_bytes(hash: &mut Sha256, value: &[u8]) -> Result<(), StoreError> {
    let length = u32::try_from(value.len()).map_err(|_| StoreError::LimitExceeded)?;
    hash.update(length.to_be_bytes());
    hash.update(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use runku_core::{EnvironmentId, OperationId, ProjectId};
    use ulid::Ulid;

    use super::*;

    fn scope() -> EnvironmentScope {
        EnvironmentScope::new(
            ProjectId::from_ulid(Ulid::from(1_u128)),
            EnvironmentId::from_ulid(Ulid::from(2_u128)),
        )
    }

    #[test]
    fn empty_and_duplicate_batches_fail_before_storage() {
        let mut batch = CommitBatch::new(scope(), OperationId::from_ulid(Ulid::from(3_u128)));
        assert_eq!(batch.validate(), Err(StoreError::EmptyBatch));
        let mutation = DocumentMutation::Delete {
            table_id: TableId::from_ulid(Ulid::from(4_u128)),
            document_id: DocumentId::from_ulid(Ulid::from(5_u128)),
            expected_revision: 1,
        };
        batch.push_document(mutation.clone());
        batch.push_document(mutation);
        assert_eq!(batch.validate(), Err(StoreError::DuplicateMutation));

        let mut reads = CommitBatch::new(scope(), OperationId::from_ulid(Ulid::from(6_u128)));
        let assertion = DocumentReadAssertion {
            table_id: TableId::from_ulid(Ulid::from(4_u128)),
            document_id: DocumentId::from_ulid(Ulid::from(5_u128)),
            observed_revision: Some(1),
        };
        reads.push_read(assertion);
        reads.push_read(assertion);
        reads.push_outbox(OutboxAppend {
            event_id: OutboxEventId::from_ulid(Ulid::from(7_u128)),
            payload: CanonicalValue::Null,
        });
        assert_eq!(reads.validate(), Err(StoreError::DuplicateMutation));
    }

    #[test]
    fn digest_is_stable_and_sensitive_to_content() -> Result<(), Box<dyn Error>> {
        let mut first = CommitBatch::new(scope(), OperationId::from_ulid(Ulid::from(3_u128)));
        first.push_outbox(OutboxAppend {
            event_id: OutboxEventId::from_ulid(Ulid::from(4_u128)),
            payload: CanonicalValue::Int64(1),
        });
        let mut second = first.clone();
        assert_eq!(first.digest()?, second.digest()?);
        second.outbox[0].payload = CanonicalValue::Int64(2);
        assert_ne!(first.digest()?, second.digest()?);
        let mut third = first.clone();
        third.push_read(DocumentReadAssertion {
            table_id: TableId::from_ulid(Ulid::from(8_u128)),
            document_id: DocumentId::from_ulid(Ulid::from(9_u128)),
            observed_revision: None,
        });
        assert_ne!(first.digest()?, third.digest()?);
        Ok(())
    }
}
