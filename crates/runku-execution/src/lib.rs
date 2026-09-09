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
    document_write_set_payload, plan_document_index_mutations,
};
pub use query::{
    DependencyBound, ExecutionError, LogicalQueryOutcome, QueryExecutor, QueryOutcome,
    QueryTelemetrySnapshot, ReadDependency, execute_logical_query,
};
pub use scheduler::{
    ScheduledInvocationRunner, ScheduledPollOutcome, ScheduledRunFailure, ScheduledWorker,
    ScheduledWorkerConfig, ScheduledWorkerError, ScheduledWorkerTelemetrySnapshot, SchedulerClock,
    SystemSchedulerClock,
};
