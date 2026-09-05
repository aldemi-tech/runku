//! Infrastructure-independent Environment lifecycle and repository contracts.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod model;
mod repository;
mod service;

pub use error::EnvironmentError;
pub use model::{
    Environment, EnvironmentCommand, EnvironmentConfiguration, EnvironmentDesiredState,
    EnvironmentLifecycle, EnvironmentMaterializationOutcome, EnvironmentName,
    EnvironmentObservedState, EnvironmentOperation, EnvironmentOperationKind,
    EnvironmentOperationResult, EnvironmentPage, EnvironmentPageRequest, EnvironmentRegion,
    EnvironmentSlug,
};
pub use repository::{
    EnvironmentRepository, EnvironmentRepositoryBackend, EnvironmentRepositoryTelemetrySnapshot,
};
pub use service::EnvironmentService;
