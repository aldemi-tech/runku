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
mod s3;
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
    LocalChannelExpectation, LocalChannelStatus, LocalCodeResolution, LocalCompatibilityDiagnostic,
    LocalCompatibilityReport, LocalReleaseDiff, LocalReleaseError, LocalReleaseInspection,
    LocalReleaseManager, LocalReleaseOutcome, LocalReleaseStatus, LocalReleaseStatusReport,
};
pub use logs::{LocalLogError, LocalLogManager};
pub use otel::{LocalOtlpError, LocalOtlpExporter, LocalOtlpReport};
pub use process::{
    LocalProcess, LocalProcessConfig, LocalProcessError, LocalProcessLease, LocalProcessListener,
    LocalProcessTelemetrySnapshot, acquire_local_process_lease,
};
pub use publish::{LocalPublishError, LocalPublishResult, publish_local, publish_local_if_head};
pub use s3::{S3ProductConfig, build_s3_router};
pub use state::{
    LOCAL_STATE_DIRECTORY, LocalPaths, LocalProjectState, LocalStateError,
    derive_local_object_storage_digest_key, initialize_local, initialize_local_with_scope,
    load_local,
};
