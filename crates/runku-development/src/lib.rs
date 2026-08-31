//! Durable Development Workspace pointers over immutable Dev Revisions.
//!
//! A revision embeds canonical candidate Release manifest bytes. Serving does not consult a `SaaS`
//! control plane, and freezing registers those exact bytes in the Release lifecycle without a
//! rebuild.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;
mod model;

pub use adapter::{
    DevelopmentRepositoryConfig, DevelopmentRepositoryRole, SqlDevelopmentRepository,
};
pub use model::{
    DevelopmentActor, DevelopmentBackend, DevelopmentCommand, DevelopmentCommandResult,
    DevelopmentContext, DevelopmentError, DevelopmentRepository, DevelopmentResolution,
    DevelopmentRevisionEntry, DevelopmentRevisionResolution, DevelopmentSnapshot,
    DevelopmentTelemetrySnapshot, WorkspaceBinding,
};
