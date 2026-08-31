use std::{fmt, str::FromStr};

use async_trait::async_trait;
use runku_core::{
    ApplicationClientId, CredentialId, EnvironmentScope, FunctionId, InvocationId, ReleaseId,
    RequestId,
};
use runku_value::TimestampMicros;
use thiserror::Error;

use crate::{LogLevel, LogStream, OperationalEventV1};

/// Maximum records returned by one log query.
pub const LOG_QUERY_MAX_RECORDS: u16 = 1_000;
/// Maximum records accepted by one durable append batch.
pub const LOG_APPEND_MAX_RECORDS: usize = 256;
/// Maximum rows removed by one retention transaction.
pub const LOG_PRUNE_MAX_RECORDS: u32 = 10_000;

/// Monotonic, repository-local cursor within one Environment scope.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogCursor(u64);

impl LogCursor {
    /// Beginning of the durable stream.
    pub const START: Self = Self(0);

    /// Creates a cursor from a persisted nonnegative sequence.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying monotonic sequence.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for LogCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "logc_{}", self.0)
    }
}

impl FromStr for LogCursor {
    type Err = LogRepositoryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let digits = value
            .strip_prefix("logc_")
            .ok_or(LogRepositoryError::InvalidRequest)?;
        if digits.is_empty()
            || digits.len() > 20
            || !digits.bytes().all(|byte| byte.is_ascii_digit())
            || digits.starts_with('0') && digits != "0"
        {
            return Err(LogRepositoryError::InvalidRequest);
        }
        digits
            .parse::<u64>()
            .map(Self)
            .map_err(|_| LogRepositoryError::InvalidRequest)
    }
}

/// Exact bounded filters for one Environment log stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogQuery {
    /// Environment boundary; queries never cross it.
    pub scope: EnvironmentScope,
    /// Return records strictly after this cursor.
    pub after: LogCursor,
    /// Maximum result count in `1..=1000`.
    pub limit: u16,
    /// Optional stream filter.
    pub stream: Option<LogStream>,
    /// Optional minimum severity filter.
    pub minimum_level: Option<LogLevel>,
    /// Optional exact Function identity.
    pub function_id: Option<FunctionId>,
    /// Optional exact Request correlation.
    pub request_id: Option<RequestId>,
    /// Optional exact Invocation correlation.
    pub invocation_id: Option<InvocationId>,
    /// Optional exact Application Client attribution.
    pub client_id: Option<ApplicationClientId>,
    /// Optional exact credential attribution.
    pub credential_id: Option<CredentialId>,
    /// Optional exact Release pin.
    pub release_id: Option<ReleaseId>,
}

impl LogQuery {
    /// Validates limits and identity relationships.
    ///
    /// # Errors
    ///
    /// Rejects zero or oversized limits.
    pub fn validate(&self) -> Result<(), LogRepositoryError> {
        if !(1..=LOG_QUERY_MAX_RECORDS).contains(&self.limit) {
            return Err(LogRepositoryError::InvalidRequest);
        }
        Ok(())
    }
}

/// One event paired with its durable ordering cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SequencedOperationalEvent {
    /// Cursor assigned atomically by the repository.
    pub cursor: LogCursor,
    /// Validated event payload.
    pub event: OperationalEventV1,
}

/// Stable ordered page and continuation cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogPage {
    /// Strictly ascending records.
    pub records: Vec<SequencedOperationalEvent>,
    /// Last returned cursor, or the input cursor for an empty page.
    pub next: LogCursor,
}

/// Bounded retention result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PruneResult {
    /// Rows matching cutoff at the start of the transaction, capped for dry-run.
    pub matched: u32,
    /// Rows actually removed; zero for dry-run.
    pub deleted: u32,
    /// Whether more matching rows may remain.
    pub more: bool,
}

/// Durable repository engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogRepositoryBackend {
    /// Local/test `SQLite`.
    SQLite,
    /// Authoritative `PostgreSQL`.
    PostgreSQL,
}

/// Sanitized durable log repository failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LogRepositoryError {
    /// Query/event/configuration is invalid.
    #[error("operational log request is invalid")]
    InvalidRequest,
    /// A supported bounded limit was exceeded.
    #[error("operational log request exceeds a limit")]
    LimitExceeded,
    /// Repository is temporarily unavailable.
    #[error("operational log repository is unavailable")]
    Unavailable,
    /// Durable contents violate the v1 contract.
    #[error("operational log repository is corrupt")]
    Corruption,
    /// Backend/configuration role is unsupported.
    #[error("operational log repository backend is unsupported")]
    Unsupported,
}

impl LogRepositoryError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "LOG_REPOSITORY_INVALID",
            Self::LimitExceeded => "LOG_REPOSITORY_LIMIT",
            Self::Unavailable => "LOG_REPOSITORY_UNAVAILABLE",
            Self::Corruption => "LOG_REPOSITORY_CORRUPT",
            Self::Unsupported => "LOG_REPOSITORY_UNSUPPORTED",
        }
    }
}

/// Nonblocking emission failure; execution must never wait or roll back because of it.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LogSinkError {
    /// Event failed validation before admission.
    #[error("operational log event is invalid")]
    InvalidEvent,
    /// Bounded spool is full.
    #[error("operational log spool is full")]
    Full,
    /// Writer has shut down or is unavailable.
    #[error("operational log sink is unavailable")]
    Unavailable,
}

/// Synchronous nonblocking boundary used by execution/runtime code.
pub trait OperationalLogSink: fmt::Debug + Send + Sync {
    /// Attempts to admit exactly one already-sanitized event without blocking.
    ///
    /// # Errors
    ///
    /// Returns invalid/full/unavailable; callers record the drop and preserve functional results.
    fn try_emit(&self, event: OperationalEventV1) -> Result<(), LogSinkError>;
}

/// Async durable storage/query/retention boundary implemented identically by SQLite/PostgreSQL.
#[async_trait]
pub trait LogRepository: fmt::Debug + Send + Sync {
    /// Engine used by this instance.
    fn backend(&self) -> LogRepositoryBackend;

    /// Atomically appends one non-empty bounded batch in supplied order.
    async fn append(&self, events: &[OperationalEventV1]) -> Result<LogCursor, LogRepositoryError>;

    /// Returns one ascending page for exact bounded filters.
    async fn query(&self, query: &LogQuery) -> Result<LogPage, LogRepositoryError>;

    /// Counts or removes a bounded prefix strictly older than `cutoff` in one scope.
    async fn prune_before(
        &self,
        scope: EnvironmentScope,
        cutoff: TimestampMicros,
        maximum: u32,
        dry_run: bool,
    ) -> Result<PruneResult, LogRepositoryError>;

    /// Closes pooled resources after writers have drained.
    async fn close(&self);
}
