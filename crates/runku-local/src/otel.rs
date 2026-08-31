//! Strict local configuration and lifecycle for the optional Product Base OTLP Logs exporter.

use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use runku_observability::{LogRepository, LogRepositoryConfig, SqlLogRepository};
use runku_otel::{
    ExportCheckpointRepository, OtlpDestinationDigest, OtlpEndpoint, OtlpExportError,
    OtlpExporterConfig, OtlpExporterMode, OtlpExporterName, OtlpExporterTelemetrySnapshot,
    OtlpHeaders, OtlpHttpTransport, OtlpLogExporter, OtlpRepositoryConfig, OtlpTransport,
    OtlpTransportConfig, SqlExportCheckpointRepository,
};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::watch;
use zeroize::Zeroizing;

use crate::{
    LocalStateError, load_local,
    state::{LocalLock, acquire_otel_exporter_lock, sqlite_url},
};

const CONFIG_MAX_BYTES: u64 = 64 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigWire {
    version: u8,
    name: String,
    endpoint: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    maximum_batch_records: u16,
    maximum_request_bytes: usize,
    request_timeout_millis: u64,
    maximum_response_bytes: usize,
    poll_interval_millis: u64,
    maximum_attempts: u8,
    retry_initial_millis: u64,
    retry_maximum_millis: u64,
}

/// Sanitized local OTLP configuration, lifecycle, or exporter failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalOtlpError {
    /// Root/config path, file permissions, JSON, endpoint, or bounds are invalid.
    #[error("local OTLP exporter configuration is invalid")]
    InvalidConfiguration,
    /// Initialized source/checkpoint state is missing or unsafe.
    #[error("local OTLP exporter state is invalid")]
    InvalidState,
    /// The same named exporter already holds its local process lock.
    #[error("local OTLP exporter is already running")]
    AlreadyRunning,
    /// Source, checkpoint, collector, or filesystem is temporarily unavailable.
    #[error("local OTLP exporter is unavailable")]
    Unavailable,
    /// Durable checkpoint state is corrupt.
    #[error("local OTLP exporter checkpoint is corrupt")]
    Corruption,
    /// Existing exporter name is bound to a different destination configuration.
    #[error("local OTLP exporter destination changed")]
    ConfigurationDrift,
    /// Collector permanently or partially rejected the current batch.
    #[error("local OTLP collector rejected the batch")]
    Rejected,
}

impl LocalOtlpError {
    /// Stable machine-readable category.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "LOCAL_OTLP_CONFIGURATION_INVALID",
            Self::InvalidState => "LOCAL_OTLP_STATE_INVALID",
            Self::AlreadyRunning => "LOCAL_OTLP_ALREADY_RUNNING",
            Self::Unavailable => "LOCAL_OTLP_UNAVAILABLE",
            Self::Corruption => "LOCAL_OTLP_CORRUPT",
            Self::ConfigurationDrift => "LOCAL_OTLP_CONFIGURATION_DRIFT",
            Self::Rejected => "LOCAL_OTLP_REJECTED",
        }
    }
}

/// Non-sensitive completion state for one local exporter process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalOtlpReport {
    /// Named checkpoint/destination identity.
    pub exporter: OtlpExporterName,
    /// Requested process behavior.
    pub mode: OtlpExporterMode,
    /// Aggregate bounded counters.
    pub telemetry: OtlpExporterTelemetrySnapshot,
}

/// Exclusive local exporter composition over dedicated source/checkpoint repositories.
pub struct LocalOtlpExporter {
    name: OtlpExporterName,
    exporter: OtlpLogExporter,
    logs: Arc<SqlLogRepository>,
    checkpoints: Arc<SqlExportCheckpointRepository>,
    _lock: LocalLock,
}

impl std::fmt::Debug for LocalOtlpExporter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalOtlpExporter")
            .field("name", &self.name)
            .field("exporter", &self.exporter)
            .finish_non_exhaustive()
    }
}

impl LocalOtlpExporter {
    /// Opens a named exporter from a strict config path relative to an initialized root.
    ///
    /// # Errors
    ///
    /// Rejects unsafe paths/files/permissions, unknown JSON fields, invalid bounds, missing local
    /// databases, lock contention, or repository migration failure before sending any request.
    pub async fn open(root: &Path, config_path: &Path) -> Result<Self, LocalOtlpError> {
        let (state, paths) = load_local(root).await.map_err(map_state)?;
        validate_database(&paths.observability_database).await?;
        validate_database(&paths.otlp_database).await?;
        let (wire, sensitive) = read_config(&paths.root, config_path).await?;
        if wire.version != 1 {
            return Err(LocalOtlpError::InvalidConfiguration);
        }
        if sensitive {
            validate_private_config(&paths.root, config_path).await?;
        }
        let name = wire
            .name
            .parse::<OtlpExporterName>()
            .map_err(|_| LocalOtlpError::InvalidConfiguration)?;
        let endpoint = wire
            .endpoint
            .parse::<OtlpEndpoint>()
            .map_err(|_| LocalOtlpError::InvalidConfiguration)?;
        let headers =
            OtlpHeaders::new(wire.headers).map_err(|_| LocalOtlpError::InvalidConfiguration)?;
        let destination = OtlpDestinationDigest::new(&endpoint, &headers);
        let transport = OtlpHttpTransport::new(OtlpTransportConfig {
            endpoint,
            headers,
            request_timeout: Duration::from_millis(wire.request_timeout_millis),
            maximum_response_bytes: wire.maximum_response_bytes,
        })
        .map_err(|_| LocalOtlpError::InvalidConfiguration)?;
        let config = OtlpExporterConfig {
            scope: state.scope(),
            name: name.clone(),
            destination,
            maximum_batch_records: wire.maximum_batch_records,
            maximum_request_bytes: wire.maximum_request_bytes,
            poll_interval: Duration::from_millis(wire.poll_interval_millis),
            maximum_attempts: wire.maximum_attempts,
            retry_initial: Duration::from_millis(wire.retry_initial_millis),
            retry_maximum: Duration::from_millis(wire.retry_maximum_millis),
        };
        let lock = acquire_otel_exporter_lock(&paths, name.as_str())
            .await
            .map_err(map_lock)?;
        let logs = Arc::new(
            SqlLogRepository::connect_sqlite(
                &sqlite_url(&paths.observability_database),
                LogRepositoryConfig::LOCAL,
            )
            .await
            .map_err(|_| LocalOtlpError::Unavailable)?,
        );
        let checkpoints = Arc::new(
            SqlExportCheckpointRepository::connect_sqlite(
                &sqlite_url(&paths.otlp_database),
                OtlpRepositoryConfig::LOCAL,
            )
            .await
            .map_err(|_| LocalOtlpError::Unavailable)?,
        );
        let source: Arc<dyn LogRepository> = logs.clone();
        let durable: Arc<dyn ExportCheckpointRepository> = checkpoints.clone();
        let boundary: Arc<dyn OtlpTransport> = Arc::new(transport);
        let exporter =
            OtlpLogExporter::new(config, source, durable, boundary).map_err(map_exporter)?;
        Ok(Self {
            name,
            exporter,
            logs,
            checkpoints,
            _lock: lock,
        })
    }

    /// Stable non-sensitive exporter name for process status output.
    #[must_use]
    pub fn name(&self) -> &OtlpExporterName {
        &self.name
    }

    /// Runs until one cycle completes or follow mode receives shutdown, then closes both pools.
    ///
    /// # Errors
    ///
    /// Preserves the current checkpoint on every failed request and returns a sanitized category.
    pub async fn run(
        self,
        mode: OtlpExporterMode,
        shutdown: watch::Receiver<bool>,
    ) -> Result<LocalOtlpReport, LocalOtlpError> {
        let result = self.exporter.run(mode, shutdown).await;
        self.logs.close().await;
        self.checkpoints.close().await;
        Ok(LocalOtlpReport {
            exporter: self.name,
            mode,
            telemetry: result.map_err(map_exporter)?,
        })
    }
}

async fn read_config(root: &Path, relative: &Path) -> Result<(ConfigWire, bool), LocalOtlpError> {
    let path = safe_relative_file(root, relative).await?;
    let bytes = Zeroizing::new(
        tokio::fs::read(path)
            .await
            .map_err(|_| LocalOtlpError::Unavailable)?,
    );
    let wire: ConfigWire =
        serde_json::from_slice(&bytes).map_err(|_| LocalOtlpError::InvalidConfiguration)?;
    let sensitive = !wire.headers.is_empty();
    Ok((wire, sensitive))
}

async fn safe_relative_file(root: &Path, relative: &Path) -> Result<PathBuf, LocalOtlpError> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(LocalOtlpError::InvalidConfiguration);
    }
    let mut current = root.to_path_buf();
    let components = relative.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(segment) = component else {
            return Err(LocalOtlpError::InvalidConfiguration);
        };
        current.push(segment);
        let metadata = tokio::fs::symlink_metadata(&current)
            .await
            .map_err(|_| LocalOtlpError::InvalidConfiguration)?;
        if metadata.file_type().is_symlink()
            || index + 1 < components.len() && !metadata.is_dir()
            || index + 1 == components.len()
                && (!metadata.is_file() || metadata.len() == 0 || metadata.len() > CONFIG_MAX_BYTES)
        {
            return Err(LocalOtlpError::InvalidConfiguration);
        }
    }
    Ok(current)
}

async fn validate_private_config(root: &Path, relative: &Path) -> Result<(), LocalOtlpError> {
    let metadata = tokio::fs::metadata(safe_relative_file(root, relative).await?)
        .await
        .map_err(|_| LocalOtlpError::InvalidConfiguration)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(LocalOtlpError::InvalidConfiguration);
        }
    }
    Ok(())
}

async fn validate_database(path: &Path) -> Result<(), LocalOtlpError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| LocalOtlpError::InvalidState)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
        return Err(LocalOtlpError::InvalidState);
    }
    Ok(())
}

fn map_state(error: LocalStateError) -> LocalOtlpError {
    match error {
        LocalStateError::Unavailable => LocalOtlpError::Unavailable,
        LocalStateError::Conflict
        | LocalStateError::InvalidPath
        | LocalStateError::InvalidState
        | LocalStateError::Corruption => LocalOtlpError::InvalidState,
    }
}

fn map_lock(error: LocalStateError) -> LocalOtlpError {
    match error {
        LocalStateError::Conflict => LocalOtlpError::AlreadyRunning,
        LocalStateError::Unavailable => LocalOtlpError::Unavailable,
        LocalStateError::InvalidPath
        | LocalStateError::InvalidState
        | LocalStateError::Corruption => LocalOtlpError::InvalidState,
    }
}

fn map_exporter(error: OtlpExportError) -> LocalOtlpError {
    match error {
        OtlpExportError::InvalidConfiguration | OtlpExportError::Payload => {
            LocalOtlpError::InvalidConfiguration
        }
        OtlpExportError::AlreadyRunning | OtlpExportError::CheckpointConflict => {
            LocalOtlpError::AlreadyRunning
        }
        OtlpExportError::ConfigurationDrift => LocalOtlpError::ConfigurationDrift,
        OtlpExportError::CheckpointCorrupt => LocalOtlpError::Corruption,
        OtlpExportError::Rejected | OtlpExportError::InvalidResponse => LocalOtlpError::Rejected,
        OtlpExportError::Cancelled
        | OtlpExportError::Source
        | OtlpExportError::CheckpointUnavailable
        | OtlpExportError::RetryExhausted => LocalOtlpError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, net::SocketAddr, path::Path, str::FromStr as _};

    use runku_core::WorkspaceRef;
    use runku_value::TimestampMicros;
    use tempfile::tempdir;

    use super::{LocalOtlpError, LocalOtlpExporter};
    use crate::initialize_local;

    type TestResult = Result<(), Box<dyn Error>>;

    fn config(secret: &str, extra: &str) -> String {
        format!(
            r#"{{"version":1,"name":"primary","endpoint":"http://127.0.0.1:4318/v1/logs","headers":{{"authorization":"Bearer {secret}"}},"maximumBatchRecords":100,"maximumRequestBytes":1048576,"requestTimeoutMillis":1000,"maximumResponseBytes":1024,"pollIntervalMillis":50,"maximumAttempts":3,"retryInitialMillis":10,"retryMaximumMillis":20{extra}}}"#
        )
    }

    #[tokio::test]
    async fn config_path_permissions_secrets_and_exporter_lock_fail_closed() -> TestResult {
        let directory = tempdir()?;
        initialize_local(
            directory.path(),
            WorkspaceRef::from_str("default")?,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            TimestampMicros::new(100),
        )
        .await?;
        let path = directory.path().join("otel.json");
        tokio::fs::write(&path, config("must-never-debug", "")).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).await?;
            assert!(matches!(
                LocalOtlpExporter::open(directory.path(), Path::new("otel.json")).await,
                Err(LocalOtlpError::InvalidConfiguration)
            ));
            tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).await?;
        }
        let first = LocalOtlpExporter::open(directory.path(), Path::new("otel.json")).await?;
        assert!(!format!("{first:?}").contains("must-never-debug"));
        assert!(matches!(
            LocalOtlpExporter::open(directory.path(), Path::new("otel.json")).await,
            Err(LocalOtlpError::AlreadyRunning)
        ));
        drop(first);
        assert!(
            LocalOtlpExporter::open(directory.path(), Path::new("otel.json"))
                .await
                .is_ok()
        );

        tokio::fs::write(&path, config("must-never-debug", ",\"unknown\":true")).await?;
        assert!(matches!(
            LocalOtlpExporter::open(directory.path(), Path::new("otel.json")).await,
            Err(LocalOtlpError::InvalidConfiguration)
        ));
        assert!(matches!(
            LocalOtlpExporter::open(directory.path(), Path::new("../otel.json")).await,
            Err(LocalOtlpError::InvalidConfiguration)
        ));
        Ok(())
    }
}
