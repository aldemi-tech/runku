//! Durable Cron definitions, activations, leases, and Scheduled Invocation materialization.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;
mod materializer;
mod model;

pub use adapter::{CronRepositoryConfig, CronRepositoryRole, SqlCronRepository};
pub use materializer::{
    CronMaterializer, CronMaterializerConfig, CronMaterializerTelemetrySnapshot, CronPollOutcome,
};
pub use model::{
    ClaimedCronActivation, CronActivation, CronBackend, CronCommand, CronCommandResult,
    CronContext, CronError, CronRepository, CronSnapshot, CronTelemetrySnapshot,
};
