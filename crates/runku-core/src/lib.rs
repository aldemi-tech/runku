//! Stable, infrastructure-independent domain contracts for Runku.
//!
//! This crate deliberately has no dependencies on storage, HTTP, JavaScript runtimes, or `SaaS`
//! services. Types defined here can cross component boundaries without leaking an infrastructure
//! implementation into the domain.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod environment;
mod function;
mod id;
mod scope;
mod target;

pub use environment::{
    EnvironmentDescriptor, EnvironmentLocation, EnvironmentPolicyError, EnvironmentProtection,
    EnvironmentPurpose, TargetPolicyError,
};
pub use function::{FunctionName, ParseFunctionNameError};
pub use id::{
    ApplicationClientId, BuildId, CredentialId, DevRevisionId, DevelopmentCredentialId, DocumentId,
    EnvironmentId, FunctionId, IndexId, InvocationId, OperationId, OperationalEventId,
    OutboxEventId, ParseResourceIdError, ProjectId, ReleaseId, RequestId, ScheduledInvocationId,
    SubscriptionId, TableId, WorkerId, WorkspaceId,
};
pub use scope::EnvironmentScope;
pub use target::{
    ChannelName, CodeTarget, ParseChannelNameError, ParseCodeTargetError, ParsePinnedCodeError,
    ParseWorkspaceRefError, PinnedCode, WorkspaceRef,
};
