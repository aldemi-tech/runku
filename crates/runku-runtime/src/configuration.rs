//! Mediated Environment variable and secret reads for Function runtimes.

use std::{fmt::Debug, time::Instant};

use async_trait::async_trait;
use thiserror::Error;
use zeroize::Zeroizing;

use crate::CancellationToken;

/// Exact kind authorized by a Function manifest capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationValueKind {
    /// Non-secret Environment variable.
    Variable,
    /// Encrypted Environment secret reference.
    Secret,
}

/// Stable failures from the configuration broker.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ConfigurationReadError {
    /// The name or kind is malformed.
    #[error("configuration request is invalid")]
    InvalidRequest,
    /// The exact name does not exist with the required kind.
    #[error("configuration value was not found")]
    NotFound,
    /// The broker is unavailable.
    #[error("configuration broker is unavailable")]
    Unavailable,
    /// The operation exceeded its deadline.
    #[error("configuration request timed out")]
    Timeout,
    /// The invocation was cancelled.
    #[error("configuration request was cancelled")]
    Cancelled,
    /// Persisted encrypted material failed validation.
    #[error("configuration state is corrupt")]
    Corruption,
}

impl ConfigurationReadError {
    /// Stable error code safe to expose to Function code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "CONFIGURATION_REQUEST_INVALID",
            Self::NotFound => "CONFIGURATION_NOT_FOUND",
            Self::Unavailable => "CONFIGURATION_UNAVAILABLE",
            Self::Timeout => "CONFIGURATION_TIMEOUT",
            Self::Cancelled => "CONFIGURATION_CANCELLED",
            Self::Corruption => "CONFIGURATION_CORRUPT",
        }
    }
}

/// Trusted, exact-Environment source for capability-authorized configuration reads.
#[async_trait]
pub trait ConfigurationRead: Debug + Send + Sync {
    /// Resolves one exact name and kind without logging or retaining its value.
    async fn read(
        &self,
        kind: ConfigurationValueKind,
        name: &str,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Zeroizing<String>, ConfigurationReadError>;
}
