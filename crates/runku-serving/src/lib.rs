//! Provider-independent weighted Release serving policy contracts.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod model;
mod repository;
mod service;

pub use error::ServingPolicyError;
pub use model::{
    ServingAuditCursor, ServingAuditEvent, ServingAuditPage, ServingAuditPageRequest,
    ServingCommand, ServingCommandKind, ServingContractHashes, ServingLifecycle,
    ServingMaterializationOutcome, ServingMode, ServingObservedState, ServingOperation,
    ServingOperationResult, ServingPolicy, ServingPolicyRecord, ServingRelease,
    canonical_cron_declarations_hash,
};
pub use repository::{
    ServingPolicyRepository, ServingRepositoryBackend, ServingRepositoryTelemetrySnapshot,
};
pub use service::ServingPolicyService;
