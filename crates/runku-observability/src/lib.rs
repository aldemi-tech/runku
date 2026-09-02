//! Product Base operational event contracts, bounded emission, and durable query repositories.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod archive;
mod contract;
mod journal;
mod performance;
mod repository;
mod spool;
mod sql;

pub use archive::{
    LogArchive, LogArchiveCredentials, LogArchiveManifestV1, LogArchiveRunOutcome,
    LogArchiveStaticCredentials, LogArchiveStatus, LogArchiver, S3LogArchiveConfig,
    TieredLogRepository,
};
pub use contract::{
    FUNCTION_FIELDS_MAX_BYTES, FUNCTION_LOGS_MAX_BYTES, FUNCTION_LOGS_MAX_RECORDS,
    FUNCTION_MESSAGE_MAX_BYTES, LogEventKind, LogLevel, LogMessage, LogPrincipalKind, LogStream,
    OperationalEventError, OperationalEventV1, OutcomeCode, sanitize_function_fields,
};
pub use journal::{
    JournalArchiveOutcome, JournalForwardOutcome, LogJournalArchiver, LogJournalDelivery,
    LogJournalError, LogJournalForwarder, NatsLogJournal, NatsLogJournalConfig,
};
pub use performance::{
    AggregateInvocationPerformanceSink, INVOCATION_PERFORMANCE_DURATION_BUCKETS_MICROS,
    INVOCATION_PERFORMANCE_FORMAT_VERSION, INVOCATION_PERFORMANCE_MAX_SPANS,
    InvocationPerformanceMetricKey, InvocationPerformanceMetricSeries,
    InvocationPerformanceMetricValue, InvocationPerformanceMetricsSnapshot,
    InvocationPerformanceRecorder, InvocationPerformanceSink, InvocationPerformanceSpanV1,
    InvocationPerformanceTimer, MemoryInvocationPerformanceSink, PerformanceComponent,
    PerformanceOperation, PerformanceOutcome, PerformanceResourceUsage, PerformanceRuntime,
    PerformanceSinkError,
};
pub use repository::{
    LOG_APPEND_MAX_RECORDS, LOG_PRUNE_MAX_RECORDS, LOG_QUERY_MAX_RECORDS, LogCursor, LogPage,
    LogQuery, LogRepository, LogRepositoryBackend, LogRepositoryError, LogSinkError,
    OperationalLogSink, PruneResult, SequencedOperationalEvent,
};
pub use spool::{BufferedLogSink, LogSpoolConfig, LogSpoolTelemetrySnapshot, LogSpoolWriter};
pub use sql::{
    LogRepositoryConfig, LogRepositoryRole, LogRepositoryTelemetrySnapshot, SqlLogRepository,
};
