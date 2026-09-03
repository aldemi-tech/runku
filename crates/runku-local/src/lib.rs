//! Reproducible local Product Base state, package publication, and process composition.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod auth;
mod development_access;
mod doctor;
mod identity;
mod lifecycle;
mod logs;
mod otel;
mod process;
mod publish;
mod state;

pub use auth::{LocalAuthConfigError, load_local_auth_config};
pub use development_access::{
    LocalCreatedDevelopmentCredential, LocalDevelopmentAccessError, LocalDevelopmentAccessManager,
    LocalDevelopmentCredentialMetadata,
};
pub use doctor::{LocalDoctorError, LocalDoctorReport, doctor_local};
pub use identity::{
    LocalCreatedCredential, LocalCredentialMetadata, LocalIdentityError, LocalIdentityManager,
};
pub use lifecycle::{
    LocalChannelExpectation, LocalChannelStatus, LocalCompatibilityDiagnostic, LocalReleaseError,
    LocalReleaseManager, LocalReleaseOutcome, LocalReleaseStatus, LocalReleaseStatusReport,
};
pub use logs::{LocalLogError, LocalLogManager};
pub use otel::{LocalOtlpError, LocalOtlpExporter, LocalOtlpReport};
pub use process::{
    LocalProcess, LocalProcessConfig, LocalProcessError, LocalProcessLease,
    LocalProcessTelemetrySnapshot, acquire_local_process_lease,
};
pub use publish::{LocalPublishError, LocalPublishResult, publish_local, publish_local_if_head};
pub use state::{
    LOCAL_STATE_DIRECTORY, LocalPaths, LocalProjectState, LocalStateError, initialize_local,
    initialize_local_with_scope, load_local,
};
