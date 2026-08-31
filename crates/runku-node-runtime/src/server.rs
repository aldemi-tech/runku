//! Explicit server-side selection between dedicated-host and Firecracker execution.

use async_trait::async_trait;
use runku_releases::ReleaseManifestV1;
use runku_runtime::{InvocationRequest, RuntimeError};

use crate::{
    DockerNodeRuntime, DockerNodeRuntimeConfig, FirecrackerNodeRuntime,
    FirecrackerNodeRuntimeConfig, FullNodeActionOutcome, FullNodeActionRuntime, HostNodeRuntime,
    HostNodeRuntimeConfig,
};

/// Startup-only Full Node backend selection for a Runku server composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerNodeRuntimeConfig {
    /// Native Node inside one externally isolated tenant VM/instance.
    DedicatedHost(HostNodeRuntimeConfig),
    /// Ephemeral OCI container sandbox adapter.
    DockerSandbox(DockerNodeRuntimeConfig),
    /// Production shared sandbox backed by supervised Firecracker microVMs.
    Firecracker(FirecrackerNodeRuntimeConfig),
}

impl ServerNodeRuntimeConfig {
    /// Builds the selected server executor.
    ///
    /// # Errors
    ///
    /// Rejects invalid backend-specific resource or isolation settings.
    pub fn build(self) -> Result<ServerNodeRuntime, RuntimeError> {
        match self {
            Self::DedicatedHost(config) => {
                HostNodeRuntime::new(config).map(ServerNodeRuntime::Host)
            }
            Self::DockerSandbox(config) => {
                DockerNodeRuntime::new(config).map(ServerNodeRuntime::Docker)
            }
            Self::Firecracker(config) => FirecrackerNodeRuntime::new(config)
                .map(Box::new)
                .map(ServerNodeRuntime::Firecracker),
        }
    }
}

/// Concrete startup-selected Full Node server executor.
pub enum ServerNodeRuntime {
    /// Dedicated native host process backend.
    Host(HostNodeRuntime),
    /// Docker/OCI sandbox backend.
    Docker(DockerNodeRuntime),
    /// Supervised Firecracker production backend.
    Firecracker(Box<FirecrackerNodeRuntime>),
}

impl std::fmt::Debug for ServerNodeRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Host(runtime) => formatter.debug_tuple("Host").field(runtime).finish(),
            Self::Docker(runtime) => formatter.debug_tuple("Docker").field(runtime).finish(),
            Self::Firecracker(runtime) => {
                formatter.debug_tuple("Firecracker").field(runtime).finish()
            }
        }
    }
}

#[async_trait]
impl FullNodeActionRuntime for ServerNodeRuntime {
    fn validate_manifest(&self, manifest: &ReleaseManifestV1) -> Result<(), RuntimeError> {
        match self {
            Self::Host(runtime) => runtime.validate_manifest(manifest),
            Self::Docker(runtime) => runtime.validate_manifest(manifest),
            Self::Firecracker(runtime) => runtime.validate_manifest(manifest),
        }
    }

    async fn prepare(
        &self,
        manifest: &ReleaseManifestV1,
        artifact_bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Host(runtime) => runtime.prepare(manifest, artifact_bytes).await,
            Self::Docker(runtime) => runtime.prepare(manifest, artifact_bytes).await,
            Self::Firecracker(runtime) => runtime.prepare(manifest, artifact_bytes).await,
        }
    }

    async fn execute(
        &self,
        request: InvocationRequest,
    ) -> Result<FullNodeActionOutcome, RuntimeError> {
        match self {
            Self::Host(runtime) => runtime.execute(request).await,
            Self::Docker(runtime) => runtime.execute(request).await,
            Self::Firecracker(runtime) => runtime.execute(request).await,
        }
    }
}
