//! Stable Release/artifact error taxonomy.

use thiserror::Error;

/// Error returned by immutable Release and artifact boundaries.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReleaseError {
    /// Manifest or configuration is structurally invalid.
    #[error("release manifest is invalid")]
    InvalidManifest,
    /// Runtime artifact container or implementation source is structurally invalid.
    #[error("runtime artifact is invalid")]
    InvalidArtifact,
    /// A version/tag is well-formed but unsupported.
    #[error("release format or runtime is unsupported")]
    Unsupported,
    /// A v1 byte/count limit was exceeded.
    #[error("release operation exceeds a v1 limit")]
    LimitExceeded,
    /// Supplied bytes do not match the declared digest.
    #[error("artifact digest does not match content")]
    DigestMismatch,
    /// Supplied or persisted length does not match the immutable descriptor.
    #[error("artifact size does not match descriptor")]
    DescriptorMismatch,
    /// Requested artifact does not exist.
    #[error("artifact was not found")]
    NotFound,
    /// Persisted artifact bytes fail integrity validation.
    #[error("artifact integrity validation failed")]
    Corruption,
    /// Local-only storage was requested for production composition.
    #[error("artifact backend is not allowed for production")]
    ProductionBackendUnsupported,
    /// Backend is temporarily busy.
    #[error("artifact backend is temporarily busy")]
    Busy,
    /// Backend is unavailable.
    #[error("artifact backend is unavailable")]
    Unavailable,
    /// Unexpected implementation failure without private detail.
    #[error("release operation failed internally")]
    Internal,
    /// Requested lifecycle transition is not permitted.
    #[error("release state transition is invalid")]
    InvalidTransition,
    /// Serving snapshot violates scope, uniqueness, or channel invariants.
    #[error("serving snapshot is invalid")]
    InvalidSnapshot,
    /// Release is absent from the exact serving scope.
    #[error("release was not found")]
    ReleaseNotFound,
    /// Release exists but is not invocable in its current state.
    #[error("release is not servable")]
    ReleaseNotServable,
    /// Release has been retired and cannot accept new invocations.
    #[error("release is retired")]
    ReleaseRetired,
    /// Channel is absent from the exact serving scope.
    #[error("channel was not found")]
    ChannelNotFound,
    /// No default Channel is configured for an explicitly defaulted route.
    #[error("default channel is not configured")]
    DefaultChannelMissing,
    /// Stable Release routing cannot resolve a Development Workspace.
    #[error("workspace target requires the development resolver")]
    WorkspaceUnsupported,
    /// Same operation ID was reused for different command content.
    #[error("release operation ID was reused with different content")]
    OperationIdReused,
    /// Expected repository state/revision/binding does not match current state.
    #[error("release repository command conflicted with current state")]
    RepositoryConflict,
    /// Commit may have succeeded but its result could not be observed.
    #[error("release repository commit result is uncertain")]
    ResultUncertain,
}

impl ReleaseError {
    /// Stable machine-readable public code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidManifest => "RELEASE_MANIFEST_INVALID",
            Self::InvalidArtifact => "ARTIFACT_FORMAT_INVALID",
            Self::Unsupported => "RELEASE_UNSUPPORTED",
            Self::LimitExceeded => "RELEASE_LIMIT_EXCEEDED",
            Self::DigestMismatch => "ARTIFACT_DIGEST_MISMATCH",
            Self::DescriptorMismatch => "ARTIFACT_DESCRIPTOR_MISMATCH",
            Self::NotFound => "ARTIFACT_NOT_FOUND",
            Self::Corruption => "ARTIFACT_CORRUPTION",
            Self::ProductionBackendUnsupported => "ARTIFACT_BACKEND_PRODUCTION_UNSUPPORTED",
            Self::Busy => "ARTIFACT_BUSY",
            Self::Unavailable => "ARTIFACT_UNAVAILABLE",
            Self::Internal => "RELEASE_INTERNAL_ERROR",
            Self::InvalidTransition => "RELEASE_TRANSITION_INVALID",
            Self::InvalidSnapshot => "SERVING_SNAPSHOT_INVALID",
            Self::ReleaseNotFound => "RELEASE_NOT_FOUND",
            Self::ReleaseNotServable => "RELEASE_NOT_SERVABLE",
            Self::ReleaseRetired => "RELEASE_RETIRED",
            Self::ChannelNotFound => "CHANNEL_NOT_FOUND",
            Self::DefaultChannelMissing => "DEFAULT_CHANNEL_MISSING",
            Self::WorkspaceUnsupported => "WORKSPACE_ROUTER_UNSUPPORTED",
            Self::OperationIdReused => "RELEASE_OPERATION_ID_REUSED",
            Self::RepositoryConflict => "RELEASE_REPOSITORY_CONFLICT",
            Self::ResultUncertain => "RELEASE_RESULT_UNCERTAIN",
        }
    }

    /// Whether retrying may succeed without changing logical input.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::Busy | Self::Unavailable | Self::RepositoryConflict | Self::ResultUncertain
        )
    }
}
