//! Out-of-process Full Node execution behind an immutable runner contract.
//!
//! The production shared executor uses supervised Firecracker microVMs without changing the
//! gateway contract. Docker remains a local conformance adapter and is not a shared production
//! isolation boundary.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod docker;
mod firecracker;
mod host;
mod local;
mod mailbox;
mod performance;
mod protocol;
mod remote;
mod server;

use async_trait::async_trait;
use runku_releases::ReleaseManifestV1;
use runku_runtime::{InvocationRequest, RuntimeError};
use runku_value::CanonicalValue;

pub use docker::{DockerNodeRuntime, DockerNodeRuntimeConfig, DockerRestrictedNetwork};
pub use firecracker::{
    FirecrackerNodeRuntime, FirecrackerNodeRuntimeConfig, FirecrackerNodeRuntimeTelemetrySnapshot,
};
pub use host::{
    DedicatedHostPolicy, HostNodeArtifactCache, HostNodeRuntime, HostNodeRuntimeConfig,
};
pub use local::{LocalNodeRuntime, LocalNodeRuntimeConfig};
pub use remote::{
    FullNodeExecutionHandler, QueuedNodeRuntime, QueuedNodeRuntimeConfig,
    REMOTE_NODE_INVOCATION_FORMAT_VERSION,
};
pub use server::{ServerNodeRuntime, ServerNodeRuntimeConfig};

/// Successful Full Node Action result.
#[derive(Clone, Debug, PartialEq)]
pub struct FullNodeActionOutcome {
    /// Canonical handler result.
    pub value: CanonicalValue,
    /// Process/sandbox resource sample when detailed diagnostics were enabled.
    pub resource_usage: Option<runku_observability::PerformanceResourceUsage>,
}

/// Runtime-agnostic Full Node Action execution boundary.
#[async_trait]
pub trait FullNodeActionRuntime: Send + Sync {
    /// Validates whether this concrete executor supports an immutable Full Node manifest.
    ///
    /// # Errors
    ///
    /// Returns a stable unsupported/configuration error before artifact loading or execution.
    fn validate_manifest(&self, manifest: &ReleaseManifestV1) -> Result<(), RuntimeError>;

    /// Materializes immutable dependencies before a queue ACK permits user code to start.
    ///
    /// Direct/local adapters use the default validation-only implementation. Distributed OCI
    /// adapters override it to pull and verify the digest-addressed image while redelivery is
    /// still safe.
    async fn prepare(
        &self,
        manifest: &ReleaseManifestV1,
        _artifact_bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        self.validate_manifest(manifest)
    }

    /// Executes one already-authorized immutable invocation.
    ///
    /// # Errors
    ///
    /// Returns a stable sanitized runtime error.
    async fn execute(
        &self,
        request: InvocationRequest,
    ) -> Result<FullNodeActionOutcome, RuntimeError>;
}
