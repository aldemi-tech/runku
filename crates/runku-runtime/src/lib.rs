//! Bounded Safe V8 execution for immutable Runku Function artifacts.
//!
//! The crate owns no storage, network, identity provider, or `SaaS` dependency. It accepts
//! pre-authorized immutable invocation inputs and exposes only explicitly registered Platform Ops.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod data;
mod error;
mod function;
mod https;
mod invocation;
mod logging;
mod scheduling;
mod supervisor;
mod value_bridge;
mod worker;

pub use data::{
    DataBoundKind, DataDocument, DataGetRequest, DataIndexEntry, DataKeyBound, DataRead,
    DataReadError, DataScanRequest, DataWrite,
};
pub use error::RuntimeError;
pub use function::{FunctionCallError, FunctionCallKind, FunctionCallRequest, FunctionInvoke};
pub use https::{
    DnsResolver, HttpsEgress, HttpsError, HttpsMethod, HttpsOrigin, HttpsPolicy,
    HttpsPolicyBuilder, HttpsRequest, HttpsResponse, HttpsTelemetrySnapshot, MediatedHttpsClient,
    SystemDnsResolver,
};
pub use invocation::{
    CancellationToken, InvocationRequest, RuntimeLimits, RuntimeLimitsBuilder,
    RuntimeTelemetrySnapshot,
};
pub use scheduling::{ScheduleCreate, ScheduleError, ScheduleRequest, ScheduleTime};
pub use supervisor::RuntimeSupervisor;
