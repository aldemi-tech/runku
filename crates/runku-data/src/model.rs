//! Logical records shared by storage adapters.

use std::{fmt, str::FromStr};

use runku_core::{
    DocumentId, FunctionName, IndexId, OutboxEventId, PinnedCode, ScheduledInvocationId, TableId,
    WorkerId,
};
use runku_value::{CanonicalValue, IndexKey, IndexKeyPrefix, TimestampMicros};

use crate::StoreError;

/// Stable bounded name of one independent durable outbox consumer.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxConsumerName(String);

impl OutboxConsumerName {
    /// Returns the canonical consumer name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OutboxConsumerName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for OutboxConsumerName {
    type Err = StoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = value.bytes();
        let Some(first) = bytes.next() else {
            return Err(StoreError::InvalidRange);
        };
        if value.len() > 64
            || !first.is_ascii_alphanumeric()
            || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(StoreError::InvalidRange);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Total durable outbox ordering position.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxCursor {
    /// Environment commit sequence.
    pub commit_sequence: u64,
    /// Tie-breaker for multiple events in one commit.
    pub event_id: OutboxEventId,
}

/// One durable outbox row returned to a leased consumer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxEventRecord {
    /// Durable event identity.
    pub event_id: OutboxEventId,
    /// Commit sequence assigned with the originating Mutation.
    pub commit_sequence: u64,
    /// Versioned canonical payload.
    pub payload: CanonicalValue,
}

impl OutboxEventRecord {
    /// Returns this event's total ordering position.
    #[must_use]
    pub const fn cursor(&self) -> OutboxCursor {
        OutboxCursor {
            commit_sequence: self.commit_sequence,
            event_id: self.event_id,
        }
    }
}

/// Batch atomically claimed by one durable outbox consumer worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedOutboxBatch {
    /// Monotonic lease fencing token.
    pub lease_generation: u64,
    /// Cursor already acknowledged before this claim.
    pub acknowledged_through: Option<OutboxCursor>,
    /// Ordered unacknowledged events. Empty means the consumer is caught up.
    pub events: Vec<OutboxEventRecord>,
}

/// One current logical document observed from a snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentRecord {
    /// Logical table.
    pub table_id: TableId,
    /// Opaque document ID.
    pub document_id: DocumentId,
    /// Positive OCC revision.
    pub revision: u64,
    /// Commit sequence that produced this version.
    pub commit_sequence: u64,
    /// Creation timestamp preserved across updates.
    pub created_at: TimestampMicros,
    /// Last update timestamp.
    pub updated_at: TimestampMicros,
    /// Decoded Stored Value.
    pub value: CanonicalValue,
}

/// One logical index entry returned in bytewise key order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexEntry {
    /// Logical index.
    pub index_id: IndexId,
    /// Canonical ordered key.
    pub key: IndexKey,
    /// Table containing the referenced document.
    pub table_id: TableId,
    /// Referenced document.
    pub document_id: DocumentId,
    /// Document revision represented by this entry.
    pub document_revision: u64,
    /// Commit sequence that last inserted the entry.
    pub commit_sequence: u64,
}

/// Lower or upper byte bound used by an index scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyBound {
    /// No bound in this direction.
    Unbounded,
    /// Includes the supplied encoded bytes.
    Inclusive(Vec<u8>),
    /// Excludes the supplied encoded bytes.
    Exclusive(Vec<u8>),
}

impl KeyBound {
    /// Returns encoded bytes when bounded.
    #[must_use]
    pub fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Unbounded => None,
            Self::Inclusive(value) | Self::Exclusive(value) => Some(value),
        }
    }
}

/// Bytewise bounds for an Index Key v1 scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRange {
    lower: KeyBound,
    upper: KeyBound,
}

impl IndexRange {
    /// Creates an unbounded range.
    #[must_use]
    pub const fn all() -> Self {
        Self {
            lower: KeyBound::Unbounded,
            upper: KeyBound::Unbounded,
        }
    }

    /// Creates inclusive/exclusive bounds from complete keys.
    #[must_use]
    pub fn between(lower: KeyBound, upper: KeyBound) -> Self {
        Self { lower, upper }
    }

    /// Creates `[prefix, successor(prefix))` bounds.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRange`] if the prefix has no finite successor.
    pub fn prefix(prefix: &IndexKeyPrefix) -> Result<Self, StoreError> {
        let upper = prefix
            .exclusive_end()
            .map_err(|_| StoreError::InvalidRange)?;
        Ok(Self {
            lower: KeyBound::Inclusive(prefix.inclusive_start().to_vec()),
            upper: KeyBound::Exclusive(upper),
        })
    }

    /// Returns the lower bound.
    #[must_use]
    pub const fn lower(&self) -> &KeyBound {
        &self.lower
    }

    /// Returns the upper bound.
    #[must_use]
    pub const fn upper(&self) -> &KeyBound {
        &self.upper
    }

    /// Validates ordering and the v1 scan limit.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRange`] for zero/over-limit scans or inverted bounds.
    pub fn validate(&self, limit: u32) -> Result<(), StoreError> {
        if limit == 0 || limit > 1_000 {
            return Err(StoreError::InvalidRange);
        }
        if let (Some(lower), Some(upper)) = (self.lower.bytes(), self.upper.bytes()) {
            let empty_or_inverted = lower > upper
                || (lower == upper
                    && (!matches!(self.lower, KeyBound::Inclusive(_))
                        || !matches!(self.upper, KeyBound::Inclusive(_))));
            if empty_or_inverted {
                return Err(StoreError::InvalidRange);
            }
        }
        Ok(())
    }
}

/// Durable scheduler state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScheduleStatus {
    /// Waiting for execute time or retry.
    Pending,
    /// Owned by a worker until lease expiration.
    Running,
    /// Finished successfully.
    Succeeded,
    /// Permanently failed.
    Failed,
    /// Cancelled before terminal execution.
    Cancelled,
}

/// Persisted Scheduled Invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledInvocationRecord {
    /// Stable record ID.
    pub id: ScheduledInvocationId,
    /// Exact code identity.
    pub pinned_code: PinnedCode,
    /// Destination function.
    pub function: FunctionName,
    /// Canonical arguments.
    pub args: CanonicalValue,
    /// Next eligible execution time.
    pub execute_at: TimestampMicros,
    /// Current durable state.
    pub status: ScheduleStatus,
    /// Number of claims made so far.
    pub attempts: u32,
    /// Monotonic lease generation.
    pub lease_generation: u64,
    /// Optional current lease owner.
    pub lease_owner: Option<WorkerId>,
    /// Optional lease deadline.
    pub lease_until: Option<TimestampMicros>,
    /// Optional application-level deduplication key.
    pub idempotency_key: Option<String>,
    /// Last bounded execution error category, if any.
    pub last_error_code: Option<String>,
    /// Commit sequence that created the record.
    pub commit_sequence: u64,
}

/// Scheduled Invocation returned from an atomic claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedScheduledInvocation {
    /// Claimed record including updated lease fields.
    pub record: ScheduledInvocationRecord,
}

/// Terminal or retry transition requested by a lease owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleCompletion {
    /// Mark successful.
    Succeeded,
    /// Return to pending at a specific time after a retryable failure.
    Retry {
        /// Next eligible execution time.
        execute_at: TimestampMicros,
        /// Stable bounded failure category.
        error_code: String,
    },
    /// Mark terminal failure.
    Failed {
        /// Stable bounded failure category.
        error_code: String,
    },
}

/// Result of a cancellation request without overstating running-work guarantees.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleCancelResult {
    /// Pending work transitioned durably to cancelled.
    Cancelled,
    /// The record was already cancelled.
    AlreadyCancelled,
    /// A worker already owns the invocation; v1 does not promise interruption.
    Running,
    /// The invocation already succeeded or failed terminally.
    Terminal,
}

/// Result revision for one document mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentRevisionResult {
    /// Logical table.
    pub table_id: TableId,
    /// Document.
    pub document_id: DocumentId,
    /// New revision, or `None` after delete.
    pub revision: Option<u64>,
}

/// Durable result of an idempotent commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitResult {
    /// Per-Environment sequence assigned once.
    pub commit_sequence: u64,
    /// Result revisions in batch document order.
    pub documents: Vec<DocumentRevisionResult>,
    /// True when returned from the operation journal rather than newly committed.
    pub replayed: bool,
}
