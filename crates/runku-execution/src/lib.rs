//! Product Base coordination of Safe Runtime invocations with logical platform services.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod action;
mod mutation;
mod nested;
mod query;
mod scheduler;

pub use action::{
    ActionExecutionError, ActionExecutor, ActionOutcome, ActionTelemetrySnapshot,
    NodeActionExecutor,
};
pub use mutation::{
    MutationExecutionError, MutationExecutor, MutationOutcome, MutationTelemetrySnapshot,
};
pub use query::{
    DependencyBound, ExecutionError, QueryExecutor, QueryOutcome, QueryTelemetrySnapshot,
    ReadDependency,
};
pub use scheduler::{
    ScheduledInvocationRunner, ScheduledPollOutcome, ScheduledRunFailure, ScheduledWorker,
    ScheduledWorkerConfig, ScheduledWorkerError, ScheduledWorkerTelemetrySnapshot, SchedulerClock,
    SystemSchedulerClock,
};
