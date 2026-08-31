//! Authenticated Product Base service and HTTP boundary for Remote Development Workspaces.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod model;
mod router;
mod service;

pub use model::{
    DevelopmentAuditEvent, DevelopmentAuditOperation, DevelopmentAuditOutcome,
    DevelopmentAuditSink, DevelopmentServiceClock, DevelopmentServiceError,
    DevelopmentServiceTelemetrySnapshot, SystemDevelopmentServiceClock,
};
pub use router::{
    DevelopmentHttpConfig, DevelopmentHttpExposure, build_development_router, serve_development,
};
pub use service::{
    DevelopmentServingRefresher, ReleaseServingRefresher, RemoteWorkspaceService,
    RemoteWorkspaceServiceConfig,
};
