//! Infrastructure-independent contracts for Runku's logical data store.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod batch;
mod error;
mod model;
mod store;
mod telemetry;

pub use batch::{
    CommitBatch, CommitLimits, DocumentMutation, DocumentReadAssertion, ExpectedRevision,
    IndexMutation, OutboxAppend, ScheduledInvocationInsert,
};
pub use error::StoreError;
pub use model::{
    ClaimedOutboxBatch, ClaimedScheduledInvocation, CommitResult, DocumentRecord,
    DocumentRevisionResult, IndexEntry, IndexRange, KeyBound, OutboxConsumerName, OutboxCursor,
    OutboxEventRecord, ScheduleCancelResult, ScheduleCompletion, ScheduleStatus,
    ScheduledInvocationRecord,
};
pub use runku_core::{EnvironmentScope, FunctionName, ParseFunctionNameError, PinnedCode};
pub use store::{LogicalStore, ReadSnapshot, StoreBackend};
pub use telemetry::{StoreTelemetry, StoreTelemetryRecorder, StoreTelemetrySnapshot};
