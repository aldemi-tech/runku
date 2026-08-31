//! Durable, scope-safe OTLP/HTTP Logs export outside the Function execution hot path.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod checkpoint;
mod exporter;
mod mapping;
mod transport;

pub use checkpoint::{
    CheckpointAdvance, CheckpointError, ExportCheckpoint, ExportCheckpointRepository,
    OtlpDestinationDigest, OtlpExporterName, OtlpRepositoryConfig, OtlpRepositoryRole,
    SqlExportCheckpointRepository,
};
pub use exporter::{
    OTLP_EXPORT_MAX_REQUEST_BYTES, OtlpExportError, OtlpExportOutcome, OtlpExporterConfig,
    OtlpExporterMode, OtlpExporterTelemetrySnapshot, OtlpLogExporter,
};
pub use mapping::{OTLP_INSTRUMENTATION_SCOPE, encode_otlp_logs};
pub use transport::{
    OtlpEndpoint, OtlpHeaders, OtlpHttpTransport, OtlpTransport, OtlpTransportConfig,
    OtlpTransportError, OtlpTransportOutcome,
};
