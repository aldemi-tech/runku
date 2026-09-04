//! `runku-server` self-hosted process composition.

mod product;

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::OpenOptions,
    io::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr as _,
    sync::Arc,
    time::{Duration, SystemTime},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use runku_file_storage::{
    FileObjectStore, FileStorageError, FileStorageLimits, FileUsageEvent, FileUsageSink,
    S3FileCredentials, S3FileStaticCredentials, S3FileStoreConfig,
};
use runku_gateway::CorsOrigin;
use runku_identity::{
    ApplicationScope, JwtAlgorithm, JwtPrincipalProfile, JwtProviderConfig, KeyringCrypto,
};
use runku_identity_provider::{
    AllowedHttpsOrigin, AllowedLoopbackOrigin, JwtProviderManager, LocalProviderNetworkConfig,
    ProviderNetworkConfig,
};
use runku_management_service::{
    ExternalIdentityAuthenticator, JwtExternalIdentityAuthenticator, ManagementHttpConfig,
    ManagementHttpExposure, ManagementProduct, OidcClientConfiguration,
    build_management_router_with_product, serve_management,
};
use runku_observability::{
    JournalArchiveOutcome, LogArchive, LogJournalArchiver, NatsLogJournal, NatsLogJournalConfig,
    S3LogArchiveConfig,
};
use runku_platform_identity::{
    BootstrapResult, OperatorName, PlatformIdentityCrypto, PlatformIdentityRepository,
    PlatformIdentityRepositoryConfig, PlatformIdentityService, SessionTokenPolicy,
    SqlPlatformIdentityRepository,
};
use runku_value::TimestampMicros;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use zeroize::Zeroizing;

use crate::product::{ProductAdapter, ProductAdapterConfig, migrate_platform_database};

const DEFAULT_LISTEN: &str = "127.0.0.1:3220";
const BOOTSTRAP_RECOVERY_CONFIRMATION: &str = "replace-lost-initial-owner-code";
const MAX_SECRET_FILE_BYTES: u64 = 64 * 1024;

#[tokio::main]
async fn main() -> ExitCode {
    match Box::pin(run()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => {
            eprintln!("error: {code}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run() -> Result<(), &'static str> {
    let command = env::args().nth(1).unwrap_or_else(|| "serve".to_owned());
    if !matches!(
        command.as_str(),
        "serve"
            | "check"
            | "migrate"
            | "recover-bootstrap"
            | "logs-worker"
            | "probe-live"
            | "probe-ready"
            | "version"
    ) || env::args().len() > 2
    {
        return Err("SERVER_USAGE_INVALID");
    }
    if command == "version" {
        println!("runku-server {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if command == "probe-live" {
        return probe_management("/health/live").await;
    }
    if command == "probe-ready" {
        return probe_management("/health/ready").await;
    }
    if command == "logs-worker" {
        return run_logs_worker_command().await;
    }
    let config = ServerConfig::load()?;
    let file_object_store = config.file_storage.open().await?;
    if command == "check" {
        let _ = external_authenticator(config.oidc.as_ref())?;
        println!("configuration valid");
        return Ok(());
    }
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_postgres(
            &config.identity_database_url,
            PlatformIdentityRepositoryConfig::AUTHORITATIVE,
        )
        .await
        .map_err(|_| "SERVER_PLATFORM_DATABASE_UNAVAILABLE")?,
    );
    if command == "migrate" {
        if let (Some(root), Some(url)) = (
            config.product_root.as_deref(),
            config.platform_database_url.as_ref(),
        ) {
            migrate_platform_database(root, url.as_str()).await?;
        }
        repository.close().await;
        println!("migrations applied");
        return Ok(());
    }
    let identity = Arc::new(
        PlatformIdentityService::new(
            repository.clone(),
            Arc::new(PlatformIdentityCrypto::new(config.pepper)),
            SessionTokenPolicy::DEFAULT,
        )
        .map_err(|_| "SERVER_PLATFORM_IDENTITY_INVALID")?,
    );
    if command == "recover-bootstrap" {
        if required("RUNKU_BOOTSTRAP_RECOVERY_CONFIRM")? != BOOTSTRAP_RECOVERY_CONFIRMATION {
            repository.close().await;
            return Err("SERVER_BOOTSTRAP_RECOVERY_CONFIRMATION_INVALID");
        }
        let result = recover_bootstrap(&identity, &config.state_directory).await;
        repository.close().await;
        return result;
    }
    let log_journal = open_log_journal(config.log_journal.as_ref()).await?;
    initialize_bootstrap(&identity, &config.state_directory).await?;
    let http = ManagementHttpConfig {
        max_concurrent_requests: 1_024,
        exposure: config.exposure,
        public_management_endpoint: config.public_management_endpoint.clone(),
    };
    let external = external_authenticator(config.oidc.as_ref())?;
    let product_adapter = match config.product_root.as_ref() {
        Some(root) => Some(Arc::new(
            Box::pin(ProductAdapter::open(
                root.clone(),
                ProductAdapterConfig {
                    platform_database_url: config.platform_database_url.clone(),
                    log_archive: config.log_archive.clone(),
                    log_journal: log_journal.clone(),
                    allowed_origins: config.product_allowed_origins.clone(),
                    auth_config: config.product_auth_config.clone(),
                    file_object_store,
                    file_storage_limits: config.file_storage_limits,
                    file_usage_sink: config.file_usage_sink.clone(),
                    file_usage_interval: config.file_usage_interval,
                },
            ))
            .await?,
        )),
        None => None,
    };
    let product = product_adapter
        .as_ref()
        .map(|adapter| Arc::clone(adapter) as Arc<dyn ManagementProduct>);
    let oidc_client = config.oidc.as_ref().and_then(|oidc| {
        oidc.native_client
            .as_ref()
            .map(|native| OidcClientConfiguration {
                issuer: oidc.issuer.clone(),
                authorization_endpoint: native.authorization_endpoint.clone(),
                token_endpoint: native.token_endpoint.clone(),
                client_id: native.client_id.clone(),
                scopes: native.scopes.clone(),
                resource: native.resource.clone(),
            })
    });
    let router =
        build_management_router_with_product(http, identity, external, product, oidc_client)
            .map_err(|_| "SERVER_MANAGEMENT_CONFIGURATION_INVALID")?;
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|_| "SERVER_MANAGEMENT_LISTENER_UNAVAILABLE")?;
    println!("runku-server management listening on {}", config.listen);
    let result = serve_management(listener, router, config.exposure, shutdown()).await;
    if let Some(product) = product_adapter {
        product.shutdown().await;
    }
    repository.close().await;
    result.map_err(|_| "SERVER_MANAGEMENT_STOPPED")
}

async fn run_logs_worker_command() -> Result<(), &'static str> {
    let archive = load_log_archive()?.ok_or("SERVER_LOG_ARCHIVE_S3_REQUIRED")?;
    let journal_config = load_log_journal()?.ok_or("SERVER_LOG_JOURNAL_CONFIGURATION_MISSING")?;
    let journal = open_log_journal(Some(&journal_config))
        .await?
        .ok_or("SERVER_LOG_JOURNAL_CONFIGURATION_MISSING")?;
    run_log_worker(journal, archive, log_archive_batch_wait()?).await
}

struct ServerConfig {
    identity_database_url: String,
    pepper: [u8; 32],
    state_directory: PathBuf,
    listen: SocketAddr,
    exposure: ManagementHttpExposure,
    public_management_endpoint: Option<String>,
    oidc: Option<OidcConfig>,
    product_root: Option<PathBuf>,
    platform_database_url: Option<Zeroizing<String>>,
    product_allowed_origins: BTreeSet<CorsOrigin>,
    product_auth_config: Option<PathBuf>,
    log_archive: Option<LogArchive>,
    log_journal: Option<ServerLogJournalConfig>,
    file_storage: ServerFileStorage,
    file_storage_limits: FileStorageLimits,
    file_usage_sink: Option<Arc<dyn FileUsageSink>>,
    file_usage_interval: Duration,
}

enum ServerFileStorage {
    ProductFilesystem,
    Filesystem(PathBuf),
    S3(FileObjectStore),
}

impl ServerFileStorage {
    async fn open(&self) -> Result<Option<FileObjectStore>, &'static str> {
        match self {
            Self::ProductFilesystem => Ok(None),
            Self::Filesystem(root) => FileObjectStore::filesystem(root)
                .await
                .map(Some)
                .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID"),
            Self::S3(store) => Ok(Some(store.clone())),
        }
    }
}

impl ServerConfig {
    #[allow(clippy::too_many_lines)]
    fn load() -> Result<Self, &'static str> {
        let identity_database_url =
            required_secret_alias("RUNKU_IDENTITY_DATABASE_URL", "RUNKU_DATABASE_URL")?;
        let identity_database_target = postgres_database_target(&identity_database_url)
            .map_err(|()| "SERVER_DATABASE_URL_INVALID")?;
        let encoded = required_secret("RUNKU_PLATFORM_IDENTITY_PEPPER")?;
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| "SERVER_PLATFORM_PEPPER_INVALID")?;
        let pepper: [u8; 32] = decoded
            .try_into()
            .map_err(|_| "SERVER_PLATFORM_PEPPER_INVALID")?;
        let state_directory = PathBuf::from(required("RUNKU_STATE_DIRECTORY")?);
        if !state_directory.is_absolute() || state_directory == Path::new("/") {
            return Err("SERVER_STATE_DIRECTORY_INVALID");
        }
        let listen = env::var("RUNKU_MANAGEMENT_LISTEN")
            .unwrap_or_else(|_| DEFAULT_LISTEN.to_owned())
            .parse::<SocketAddr>()
            .map_err(|_| "SERVER_MANAGEMENT_LISTEN_INVALID")?;
        let tls_terminated = match env::var("RUNKU_MANAGEMENT_TLS_TERMINATED") {
            Ok(value) if value == "true" => true,
            Ok(value) if value == "false" => false,
            Err(env::VarError::NotPresent) => false,
            Ok(_) | Err(env::VarError::NotUnicode(_)) => {
                return Err("SERVER_MANAGEMENT_TLS_CONFIGURATION_INVALID");
            }
        };
        let exposure = if tls_terminated {
            ManagementHttpExposure::TrustedTlsTermination
        } else if listen.ip().is_loopback() {
            ManagementHttpExposure::LoopbackPlaintext
        } else {
            return Err("SERVER_MANAGEMENT_TLS_REQUIRED");
        };
        let public_management_endpoint = env::var("RUNKU_PUBLIC_MANAGEMENT_URL")
            .ok()
            .map(|value| {
                validate_public_management_endpoint(&value)?;
                Ok::<_, &'static str>(value)
            })
            .transpose()?;
        let oidc = env::var("RUNKU_PLATFORM_OIDC_CONFIG")
            .ok()
            .map(|path| load_oidc(Path::new(&path)))
            .transpose()?;
        let product_root = env::var_os("RUNKU_PRODUCT_ROOT")
            .map(PathBuf::from)
            .map(|path| {
                if !path.is_absolute() || path == Path::new("/") {
                    Err("SERVER_PRODUCT_ROOT_INVALID")
                } else {
                    Ok(path)
                }
            })
            .transpose()?;
        let platform_database_url =
            optional_secret_alias("RUNKU_PLATFORM_DATABASE_URL", "RUNKU_PRODUCT_DATABASE_URL")?
                .map(|value| {
                    let target = postgres_database_target(&value)
                        .map_err(|()| "SERVER_PRODUCT_DATABASE_URL_INVALID")?;
                    if target == identity_database_target {
                        return Err("SERVER_PRODUCT_DATABASE_NOT_ISOLATED");
                    }
                    Ok(Zeroizing::new(value))
                })
                .transpose()?;
        if platform_database_url.is_some() && product_root.is_none() {
            return Err("SERVER_PRODUCT_DATABASE_WITHOUT_PRODUCT_ROOT");
        }
        let product_allowed_origins = load_product_allowed_origins()?;
        let product_auth_config = env::var_os("RUNKU_PRODUCT_AUTH_CONFIG")
            .map(PathBuf::from)
            .map(|path| {
                if path.is_absolute()
                    || path.as_os_str().is_empty()
                    || path.components().any(|component| {
                        matches!(
                            component,
                            std::path::Component::ParentDir
                                | std::path::Component::RootDir
                                | std::path::Component::Prefix(_)
                        )
                    })
                {
                    Err("SERVER_PRODUCT_AUTH_CONFIG_INVALID")
                } else {
                    Ok(path)
                }
            })
            .transpose()?;
        if product_root.is_none()
            && (!product_allowed_origins.is_empty() || product_auth_config.is_some())
        {
            return Err("SERVER_PRODUCT_CONFIGURATION_WITHOUT_ROOT");
        }
        let log_archive = load_log_archive()?;
        let log_journal = load_log_journal()?;
        if log_journal.is_some() && log_archive.is_none() {
            return Err("SERVER_LOG_ARCHIVE_S3_REQUIRED");
        }
        let (file_storage, file_storage_limits) = load_file_storage()?;
        let (file_usage_sink, file_usage_interval) = load_file_usage_sink()?;
        if product_root.is_none() && !matches!(file_storage, ServerFileStorage::ProductFilesystem) {
            return Err("SERVER_FILE_STORAGE_WITHOUT_PRODUCT_ROOT");
        }
        if product_root.is_none() && file_usage_sink.is_some() {
            return Err("SERVER_FILE_USAGE_WITHOUT_PRODUCT_ROOT");
        }
        Ok(Self {
            identity_database_url,
            pepper,
            state_directory,
            listen,
            exposure,
            public_management_endpoint,
            oidc,
            product_root,
            platform_database_url,
            product_allowed_origins,
            product_auth_config,
            log_archive,
            log_journal,
            file_storage,
            file_storage_limits,
            file_usage_sink,
            file_usage_interval,
        })
    }
}

fn load_file_storage() -> Result<(ServerFileStorage, FileStorageLimits), &'static str> {
    let limits = load_file_storage_limits()?;
    let backend =
        env::var("RUNKU_FILE_STORAGE_BACKEND").unwrap_or_else(|_| "filesystem".to_owned());
    let storage = match backend.as_str() {
        "filesystem" => match env::var_os("RUNKU_FILE_STORAGE_FILESYSTEM_ROOT") {
            None => ServerFileStorage::ProductFilesystem,
            Some(value) => {
                let root = PathBuf::from(value);
                if !root.is_absolute() || root == Path::new("/") {
                    return Err("SERVER_FILE_STORAGE_CONFIGURATION_INVALID");
                }
                ServerFileStorage::Filesystem(root)
            }
        },
        "s3" => load_s3_file_storage()?,
        _ => return Err("SERVER_FILE_STORAGE_CONFIGURATION_INVALID"),
    };
    Ok((storage, limits))
}

fn load_file_storage_limits() -> Result<FileStorageLimits, &'static str> {
    let limits = FileStorageLimits {
        environment_bytes: optional_u64(
            "RUNKU_FILE_STORAGE_ENVIRONMENT_BYTES",
            FileStorageLimits::DEFAULT.environment_bytes,
        )?,
        file_bytes: optional_u64(
            "RUNKU_FILE_STORAGE_FILE_BYTES",
            FileStorageLimits::DEFAULT.file_bytes,
        )?,
        action_bytes: optional_u64(
            "RUNKU_FILE_STORAGE_ACTION_BYTES",
            FileStorageLimits::DEFAULT.action_bytes,
        )?,
        concurrent_uploads: usize::try_from(optional_u64(
            "RUNKU_FILE_STORAGE_CONCURRENT_UPLOADS",
            u64::try_from(FileStorageLimits::DEFAULT.concurrent_uploads)
                .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        )?)
        .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        concurrent_downloads: usize::try_from(optional_u64(
            "RUNKU_FILE_STORAGE_CONCURRENT_DOWNLOADS",
            u64::try_from(FileStorageLimits::DEFAULT.concurrent_downloads)
                .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        )?)
        .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        maximum_live_upload_grants: usize::try_from(optional_u64(
            "RUNKU_FILE_STORAGE_MAXIMUM_LIVE_UPLOAD_GRANTS",
            u64::try_from(FileStorageLimits::DEFAULT.maximum_live_upload_grants)
                .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        )?)
        .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        maximum_files: usize::try_from(optional_u64(
            "RUNKU_FILE_STORAGE_MAXIMUM_FILES",
            u64::try_from(FileStorageLimits::DEFAULT.maximum_files)
                .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        )?)
        .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        maximum_pending_usage_events: usize::try_from(optional_u64(
            "RUNKU_FILE_STORAGE_MAXIMUM_PENDING_USAGE_EVENTS",
            u64::try_from(FileStorageLimits::DEFAULT.maximum_pending_usage_events)
                .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        )?)
        .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?,
        filesystem_minimum_free_bytes: optional_u64(
            "RUNKU_FILE_STORAGE_FILESYSTEM_MINIMUM_FREE_BYTES",
            FileStorageLimits::DEFAULT.filesystem_minimum_free_bytes,
        )?,
        upload_grant_ttl: Duration::from_secs(optional_u64(
            "RUNKU_FILE_STORAGE_UPLOAD_GRANT_TTL_SECONDS",
            FileStorageLimits::DEFAULT.upload_grant_ttl.as_secs(),
        )?),
        maximum_download_grant_ttl: Duration::from_secs(optional_u64(
            "RUNKU_FILE_STORAGE_DOWNLOAD_GRANT_MAX_TTL_SECONDS",
            FileStorageLimits::DEFAULT
                .maximum_download_grant_ttl
                .as_secs(),
        )?),
    }
    .validated()
    .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")?;
    Ok(limits)
}

fn load_s3_file_storage() -> Result<ServerFileStorage, &'static str> {
    let mut config = S3FileStoreConfig::new(
        required("RUNKU_FILE_STORAGE_S3_BUCKET")?,
        required("RUNKU_FILE_STORAGE_S3_REGION")?,
    );
    config.endpoint = env::var("RUNKU_FILE_STORAGE_S3_ENDPOINT")
        .ok()
        .filter(|value| !value.is_empty());
    if let Ok(prefix) = env::var("RUNKU_FILE_STORAGE_S3_PREFIX") {
        config.prefix = prefix;
    }
    config.virtual_hosted_style =
        storage_bool("RUNKU_FILE_STORAGE_S3_VIRTUAL_HOSTED_STYLE", false)?;
    config.allow_loopback_http = storage_bool("RUNKU_FILE_STORAGE_S3_ALLOW_LOOPBACK_HTTP", false)?;
    let access = optional_secret("RUNKU_FILE_STORAGE_S3_ACCESS_KEY_ID")?;
    let secret = optional_secret("RUNKU_FILE_STORAGE_S3_SECRET_ACCESS_KEY")?;
    let session = optional_secret("RUNKU_FILE_STORAGE_S3_SESSION_TOKEN")?;
    config.credentials = match (access, secret) {
        (None, None) if session.is_none() => S3FileCredentials::Environment,
        (Some(access), Some(secret)) => {
            let credentials = S3FileStaticCredentials::new(access, secret);
            S3FileCredentials::Static(match session {
                Some(token) => credentials.with_session_token(token),
                None => credentials,
            })
        }
        _ => return Err("SERVER_FILE_STORAGE_CONFIGURATION_INVALID"),
    };
    FileObjectStore::s3(&config)
        .map(ServerFileStorage::S3)
        .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")
}

#[derive(Clone)]
struct HttpFileUsageSink {
    client: reqwest::Client,
    endpoint: url::Url,
    cell_id: String,
    token: Arc<Zeroizing<String>>,
}

impl std::fmt::Debug for HttpFileUsageSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpFileUsageSink")
            .field("endpoint", &self.endpoint)
            .field("cell_id", &self.cell_id)
            .field("token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageSinkEnvelope<'a> {
    events: Vec<UsageSinkEvent<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageSinkEvent<'a> {
    event_id: &'a str,
    cell_id: &'a str,
    project_id: &'a str,
    environment_id: &'a str,
    kind: &'a str,
    quantity: &'a str,
    unit: &'a str,
    occurred_at_micros: i64,
    dimensions: BTreeMap<&'static str, &'static str>,
}

#[async_trait::async_trait]
impl FileUsageSink for HttpFileUsageSink {
    async fn deliver(&self, events: &[FileUsageEvent]) -> Result<(), FileStorageError> {
        if events.is_empty() || events.len() > 100 {
            return Err(FileStorageError::InvalidRequest);
        }
        let events = events
            .iter()
            .map(|event| {
                Ok(UsageSinkEvent {
                    event_id: &event.event_id,
                    cell_id: &self.cell_id,
                    project_id: &event.project_id,
                    environment_id: &event.environment_id,
                    kind: &event.kind,
                    quantity: &event.quantity,
                    unit: &event.unit,
                    occurred_at_micros: event
                        .occurred_at_micros
                        .parse()
                        .map_err(|_| FileStorageError::Corruption)?,
                    dimensions: BTreeMap::new(),
                })
            })
            .collect::<Result<Vec<_>, FileStorageError>>()?;
        let body = serde_json::to_vec(&UsageSinkEnvelope { events })
            .map_err(|_| FileStorageError::Corruption)?;
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(self.token.as_str())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| FileStorageError::Unavailable)?;
        if response.status() != reqwest::StatusCode::ACCEPTED {
            return Err(FileStorageError::Unavailable);
        }
        Ok(())
    }
}

type FileUsageSinkConfig = (Option<Arc<dyn FileUsageSink>>, Duration);

fn load_file_usage_sink() -> Result<FileUsageSinkConfig, &'static str> {
    let endpoint = env::var("RUNKU_FILE_USAGE_SINK_URL").ok();
    let cell_id = env::var("RUNKU_FILE_USAGE_CELL_ID").ok();
    let token = optional_secret("RUNKU_FILE_USAGE_SINK_TOKEN")?;
    let allow_loopback = storage_bool("RUNKU_FILE_USAGE_SINK_ALLOW_LOOPBACK_HTTP", false)?;
    let interval = Duration::from_secs(optional_u64("RUNKU_FILE_USAGE_INTERVAL_SECONDS", 5)?);
    if !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&interval) {
        return Err("SERVER_FILE_USAGE_CONFIGURATION_INVALID");
    }
    let (endpoint, cell_id, token) = match (endpoint, cell_id, token) {
        (None, None, None)
            if !allow_loopback && env::var_os("RUNKU_FILE_USAGE_INTERVAL_SECONDS").is_none() =>
        {
            return Ok((None, interval));
        }
        (Some(endpoint), Some(cell_id), Some(token)) => (endpoint, cell_id, token),
        _ => return Err("SERVER_FILE_USAGE_CONFIGURATION_INVALID"),
    };
    let endpoint =
        url::Url::parse(&endpoint).map_err(|_| "SERVER_FILE_USAGE_CONFIGURATION_INVALID")?;
    let loopback = endpoint.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
    });
    if (endpoint.scheme() != "https"
        && !(endpoint.scheme() == "http" && allow_loopback && loopback))
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || endpoint.path() == "/"
        || !cell_id.starts_with("cell_")
        || cell_id.len() > 69
        || !cell_id[5..]
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || cell_id.len() < 7
        || token.len() > 16 * 1024
        || token.bytes().any(|byte| !byte.is_ascii_graphic())
    {
        return Err("SERVER_FILE_USAGE_CONFIGURATION_INVALID");
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| "SERVER_FILE_USAGE_CONFIGURATION_INVALID")?;
    Ok((
        Some(Arc::new(HttpFileUsageSink {
            client,
            endpoint,
            cell_id,
            token: Arc::new(Zeroizing::new(token)),
        })),
        interval,
    ))
}

fn optional_u64(name: &str, default: u64) -> Result<u64, &'static str> {
    env::var(name)
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| "SERVER_FILE_STORAGE_CONFIGURATION_INVALID")
        .map(|value| value.unwrap_or(default))
}

fn storage_bool(name: &str, default: bool) -> Result<bool, &'static str> {
    match env::var(name) {
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Err(env::VarError::NotPresent) => Ok(default),
        Ok(_) | Err(env::VarError::NotUnicode(_)) => {
            Err("SERVER_FILE_STORAGE_CONFIGURATION_INVALID")
        }
    }
}

fn load_product_allowed_origins() -> Result<BTreeSet<CorsOrigin>, &'static str> {
    let Some(value) = env::var("RUNKU_PRODUCT_ALLOWED_ORIGINS").ok() else {
        return Ok(BTreeSet::new());
    };
    if value.is_empty() || value.len() > 16 * 1024 {
        return Err("SERVER_PRODUCT_ALLOWED_ORIGINS_INVALID");
    }
    let values = value.split(',').collect::<Vec<_>>();
    if values.len() > 64 || values.iter().any(|origin| origin.is_empty()) {
        return Err("SERVER_PRODUCT_ALLOWED_ORIGINS_INVALID");
    }
    let origins = values
        .into_iter()
        .map(|origin| {
            origin
                .parse::<CorsOrigin>()
                .map_err(|_| "SERVER_PRODUCT_ALLOWED_ORIGINS_INVALID")
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if origins.len() != value.split(',').count() {
        return Err("SERVER_PRODUCT_ALLOWED_ORIGINS_INVALID");
    }
    Ok(origins)
}

fn load_log_archive() -> Result<Option<LogArchive>, &'static str> {
    let backend = env::var("RUNKU_LOG_ARCHIVE_BACKEND").unwrap_or_else(|_| "filesystem".to_owned());
    match backend.as_str() {
        "filesystem" => Ok(None),
        "s3" => {
            let mut config = S3LogArchiveConfig::new(
                required("RUNKU_LOG_ARCHIVE_S3_BUCKET")?,
                required("RUNKU_LOG_ARCHIVE_S3_REGION")?,
            );
            if let Ok(value) = env::var("RUNKU_LOG_ARCHIVE_S3_PREFIX") {
                config.prefix = value;
            }
            config.endpoint = env::var("RUNKU_LOG_ARCHIVE_S3_ENDPOINT").ok();
            config.virtual_hosted_style =
                optional_bool("RUNKU_LOG_ARCHIVE_S3_VIRTUAL_HOSTED_STYLE", false)?;
            config.allow_http = optional_bool("RUNKU_LOG_ARCHIVE_S3_ALLOW_HTTP", false)?;
            LogArchive::open_s3(&config)
                .map(Some)
                .map_err(|_| "SERVER_LOG_ARCHIVE_CONFIGURATION_INVALID")
        }
        _ => Err("SERVER_LOG_ARCHIVE_CONFIGURATION_INVALID"),
    }
}

#[derive(Clone)]
struct ServerLogJournalConfig {
    url: String,
    credentials_file: Option<PathBuf>,
    require_tls: bool,
    journal: NatsLogJournalConfig,
}

fn load_log_journal() -> Result<Option<ServerLogJournalConfig>, &'static str> {
    let Some(url) = env::var("RUNKU_LOG_JOURNAL_URL").ok() else {
        return Ok(None);
    };
    let parsed = url::Url::parse(&url).map_err(|_| "SERVER_LOG_JOURNAL_CONFIGURATION_INVALID")?;
    let loopback = parsed.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if !matches!(parsed.scheme(), "nats" | "tls")
        || parsed.host_str().is_none()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.scheme() == "nats" && !loopback
    {
        return Err("SERVER_LOG_JOURNAL_CONFIGURATION_INVALID");
    }
    let journal = NatsLogJournalConfig {
        replicas: env::var("RUNKU_LOG_JOURNAL_REPLICAS")
            .ok()
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| "SERVER_LOG_JOURNAL_CONFIGURATION_INVALID")?
            .unwrap_or(3),
        ..NatsLogJournalConfig::default()
    };
    let credentials_file = env::var_os("RUNKU_LOG_JOURNAL_CREDENTIALS_FILE")
        .map(PathBuf::from)
        .map(|path| {
            if !path.is_absolute() {
                return Err("SERVER_LOG_JOURNAL_CONFIGURATION_INVALID");
            }
            let metadata = std::fs::symlink_metadata(&path)
                .map_err(|_| "SERVER_LOG_JOURNAL_CONFIGURATION_INVALID")?;
            if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
                return Err("SERVER_LOG_JOURNAL_CONFIGURATION_INVALID");
            }
            Ok(path)
        })
        .transpose()?;
    Ok(Some(ServerLogJournalConfig {
        url,
        credentials_file,
        require_tls: parsed.scheme() == "tls",
        journal,
    }))
}

async fn open_log_journal(
    config: Option<&ServerLogJournalConfig>,
) -> Result<Option<NatsLogJournal>, &'static str> {
    let Some(config) = config else {
        return Ok(None);
    };
    let mut options = async_nats::ConnectOptions::new().require_tls(config.require_tls);
    if let Some(path) = &config.credentials_file {
        options = options
            .credentials_file(path)
            .await
            .map_err(|_| "SERVER_LOG_JOURNAL_CREDENTIALS_INVALID")?;
    }
    let client = options
        .connect(&config.url)
        .await
        .map_err(|_| "SERVER_LOG_JOURNAL_UNAVAILABLE")?;
    NatsLogJournal::open(client, config.journal.clone())
        .await
        .map(Some)
        .map_err(|_| "SERVER_LOG_JOURNAL_UNAVAILABLE")
}

async fn run_log_worker(
    journal: NatsLogJournal,
    archive: LogArchive,
    batch_wait: Duration,
) -> Result<(), &'static str> {
    let worker = LogJournalArchiver::new(journal, archive);
    loop {
        tokio::select! {
            result = worker.run_once(batch_wait) => {
                match result {
                    Ok(JournalArchiveOutcome::Idle | JournalArchiveOutcome::Processed { .. }) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
                }
            }
            () = shutdown() => return Ok(()),
        }
    }
}

fn log_archive_batch_wait() -> Result<Duration, &'static str> {
    let seconds = env::var("RUNKU_LOG_ARCHIVE_BATCH_WAIT_SECONDS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| "SERVER_LOG_ARCHIVE_CONFIGURATION_INVALID")?
        .unwrap_or(30);
    if !(1..=60).contains(&seconds) {
        return Err("SERVER_LOG_ARCHIVE_CONFIGURATION_INVALID");
    }
    Ok(Duration::from_secs(seconds))
}

fn optional_bool(name: &str, default: bool) -> Result<bool, &'static str> {
    match env::var(name) {
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Err(env::VarError::NotPresent) => Ok(default),
        Ok(_) | Err(env::VarError::NotUnicode(_)) => {
            Err("SERVER_LOG_ARCHIVE_CONFIGURATION_INVALID")
        }
    }
}

fn validate_public_management_endpoint(value: &str) -> Result<(), &'static str> {
    let endpoint = url::Url::parse(value).map_err(|_| "SERVER_PUBLIC_MANAGEMENT_URL_INVALID")?;
    let loopback = endpoint
        .host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback());
    if endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
        || endpoint.origin().ascii_serialization() != value
    {
        return Err("SERVER_PUBLIC_MANAGEMENT_URL_INVALID");
    }
    Ok(())
}

fn postgres_database_target(value: &str) -> Result<String, ()> {
    let url = url::Url::parse(value).map_err(|_| ())?;
    let host = url.host_str().map(str::to_ascii_lowercase);
    let host = host.as_deref().map(|value| value.trim_end_matches('.'));
    let database = url.path().strip_prefix('/');
    if !matches!(url.scheme(), "postgres" | "postgresql")
        || host.is_none_or(str::is_empty)
        || database.is_none_or(|value| value.is_empty() || value.contains('/'))
        || url.fragment().is_some()
    {
        return Err(());
    }
    Ok(format!(
        "{}:{}{}",
        host.unwrap_or_default(),
        url.port().unwrap_or(5432),
        url.path()
    ))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OidcConfig {
    provider_id: String,
    issuer: String,
    discovery_url: String,
    audience: String,
    allowed_origins: BTreeSet<String>,
    discriminator_claim: String,
    discriminator_value: String,
    algorithm: String,
    required_type: Option<String>,
    subject_pepper: String,
    #[serde(default)]
    allow_loopback_http: bool,
    native_client: Option<OidcNativeClientConfig>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OidcNativeClientConfig {
    authorization_endpoint: String,
    token_endpoint: String,
    client_id: String,
    scopes: Vec<String>,
    resource: Option<String>,
}

fn load_oidc(path: &Path) -> Result<OidcConfig, &'static str> {
    if !path.is_absolute() {
        return Err("SERVER_OIDC_CONFIG_INVALID");
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
    if !metadata.file_type().is_file() || metadata.len() == 0 || metadata.len() > 64 * 1024 {
        return Err("SERVER_OIDC_CONFIG_INVALID");
    }
    let bytes = std::fs::read(path).map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
    serde_json::from_slice(&bytes).map_err(|_| "SERVER_OIDC_CONFIG_INVALID")
}

fn external_authenticator(
    config: Option<&OidcConfig>,
) -> Result<Option<Arc<dyn ExternalIdentityAuthenticator>>, &'static str> {
    let Some(config) = config else {
        return Ok(None);
    };
    if let Some(native) = &config.native_client {
        validate_native_oidc(native, config.allow_loopback_http)?;
    }
    let algorithm = match config.algorithm.as_str() {
        "RS256" => JwtAlgorithm::Rs256,
        "PS256" => JwtAlgorithm::Ps256,
        "ES256" => JwtAlgorithm::Es256,
        "EdDSA" => JwtAlgorithm::EdDsa,
        _ => return Err("SERVER_OIDC_CONFIG_INVALID"),
    };
    let base_scope = "platform:login"
        .parse::<ApplicationScope>()
        .map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
    let provider = JwtProviderConfig {
        provider_id: config.provider_id.clone(),
        issuer: config.issuer.clone(),
        audiences: BTreeSet::from([config.audience.clone()]),
        profile: JwtPrincipalProfile::User,
        required_type: config.required_type.clone(),
        discriminator_claim: config.discriminator_claim.clone(),
        discriminator_value: config.discriminator_value.clone(),
        algorithms: BTreeSet::from([algorithm]),
        base_scopes: BTreeSet::from([base_scope]),
        scope_claim: None,
        scope_mapping: BTreeMap::default(),
        application_claim: None,
        application_mapping: BTreeMap::default(),
        max_token_ttl: Duration::from_hours(24),
        future_clock_skew: Duration::from_mins(2),
        mapping_revision: 1,
    };
    let manager = if config.allow_loopback_http {
        if config.allowed_origins.len() != 1 {
            return Err("SERVER_OIDC_CONFIG_INVALID");
        }
        let allowed_origin = config
            .allowed_origins
            .iter()
            .next()
            .ok_or("SERVER_OIDC_CONFIG_INVALID")?
            .parse::<AllowedLoopbackOrigin>()
            .map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
        JwtProviderManager::local(LocalProviderNetworkConfig {
            provider,
            discovery_url: config.discovery_url.clone(),
            allowed_origin,
            default_cache_ttl: Duration::from_mins(5),
            max_cache_ttl: Duration::from_hours(1),
            request_timeout: Duration::from_secs(10),
            unknown_kid_cooldown: Duration::from_secs(10),
        })
    } else {
        let allowed_origins = config
            .allowed_origins
            .iter()
            .map(|origin| {
                origin
                    .parse::<AllowedHttpsOrigin>()
                    .map_err(|_| "SERVER_OIDC_CONFIG_INVALID")
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        JwtProviderManager::production(ProviderNetworkConfig {
            provider,
            discovery_url: config.discovery_url.clone(),
            allowed_origins,
            default_cache_ttl: Duration::from_mins(5),
            max_cache_ttl: Duration::from_hours(1),
            request_timeout: Duration::from_secs(10),
            unknown_kid_cooldown: Duration::from_secs(10),
        })
    }
    .map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
    let bytes = URL_SAFE_NO_PAD
        .decode(&config.subject_pepper)
        .map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
    let pepper: [u8; 32] = bytes.try_into().map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
    Ok(Some(Arc::new(JwtExternalIdentityAuthenticator::new(
        Arc::new(manager),
        Arc::new(KeyringCrypto::new(pepper)),
    ))))
}

fn validate_native_oidc(
    config: &OidcNativeClientConfig,
    allow_loopback_http: bool,
) -> Result<(), &'static str> {
    if config.client_id.is_empty()
        || config.client_id.len() > 256
        || config.scopes.is_empty()
        || config.scopes.len() > 16
        || config.scopes.iter().any(|scope| {
            scope.is_empty()
                || scope.len() > 128
                || scope.chars().any(char::is_whitespace)
                || scope.chars().any(char::is_control)
        })
    {
        return Err("SERVER_OIDC_CONFIG_INVALID");
    }
    for endpoint in [&config.authorization_endpoint, &config.token_endpoint] {
        let url = url::Url::parse(endpoint).map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if url.username() != ""
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_some()
            || !(url.scheme() == "https"
                || allow_loopback_http && url.scheme() == "http" && loopback)
        {
            return Err("SERVER_OIDC_CONFIG_INVALID");
        }
    }
    if let Some(resource) = &config.resource {
        validate_oidc_resource(resource, allow_loopback_http)?;
    }
    Ok(())
}

fn validate_oidc_resource(value: &str, allow_loopback_http: bool) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > 2_048 {
        return Err("SERVER_OIDC_CONFIG_INVALID");
    }
    let resource = url::Url::parse(value).map_err(|_| "SERVER_OIDC_CONFIG_INVALID")?;
    let loopback = resource
        .host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback());
    if resource.host_str().is_none()
        || !resource.username().is_empty()
        || resource.password().is_some()
        || resource.fragment().is_some()
        || !(resource.scheme() == "https"
            || allow_loopback_http && resource.scheme() == "http" && loopback)
    {
        return Err("SERVER_OIDC_CONFIG_INVALID");
    }
    Ok(())
}

async fn initialize_bootstrap(
    identity: &PlatformIdentityService,
    state_directory: &Path,
) -> Result<(), &'static str> {
    let owner = OperatorName::from_str("initial-owner")
        .map_err(|_| "SERVER_BOOTSTRAP_CONFIGURATION_INVALID")?;
    let result = identity
        .initialize_bootstrap(owner, now()?)
        .await
        .map_err(|_| "SERVER_BOOTSTRAP_FAILED")?;
    let directory = state_directory.join("bootstrap");
    let path = directory.join("initial-owner.code");
    match result {
        BootstrapResult::Created(generated) => {
            std::fs::create_dir_all(&directory).map_err(|_| "SERVER_BOOTSTRAP_WRITE_FAILED")?;
            write_secret(&path, generated.code.expose())?;
            println!("initial owner code written to {}", path.display());
        }
        BootstrapResult::Replayed => {
            if !path.is_file() {
                return Err("SERVER_BOOTSTRAP_FILE_MISSING");
            }
            println!(
                "pending initial owner code is available at {}",
                path.display()
            );
        }
        BootstrapResult::Complete => {
            if path.is_file() {
                std::fs::remove_file(&path).map_err(|_| "SERVER_BOOTSTRAP_WRITE_FAILED")?;
            }
            println!("platform identity bootstrap complete");
        }
    }
    Ok(())
}

async fn recover_bootstrap(
    identity: &PlatformIdentityService,
    state_directory: &Path,
) -> Result<(), &'static str> {
    let owner = OperatorName::from_str("initial-owner")
        .map_err(|_| "SERVER_BOOTSTRAP_CONFIGURATION_INVALID")?;
    let generated =
        identity
            .recover_bootstrap(owner, now()?)
            .await
            .map_err(|error| match error {
                runku_platform_identity::PlatformIdentityError::AlreadyInitialized => {
                    "SERVER_BOOTSTRAP_ALREADY_COMPLETE"
                }
                runku_platform_identity::PlatformIdentityError::ResultUncertain => {
                    "SERVER_BOOTSTRAP_RECOVERY_RESULT_UNCERTAIN"
                }
                _ => "SERVER_BOOTSTRAP_RECOVERY_FAILED",
            })?;
    let directory = state_directory.join("bootstrap");
    std::fs::create_dir_all(&directory).map_err(|_| "SERVER_BOOTSTRAP_WRITE_FAILED")?;
    let path = directory.join("initial-owner.code");
    write_secret(&path, generated.code.expose())?;
    println!(
        "replacement initial owner code written to {}",
        path.display()
    );
    Ok(())
}

fn write_secret(path: &Path, value: &str) -> Result<(), &'static str> {
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| !metadata.file_type().is_file()) {
        return Err("SERVER_BOOTSTRAP_WRITE_FAILED");
    }
    let temporary = path.with_extension("code.new");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|_| "SERVER_BOOTSTRAP_WRITE_FAILED")?;
    file.write_all(value.as_bytes())
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all())
        .map_err(|_| "SERVER_BOOTSTRAP_WRITE_FAILED")?;
    if path.exists() {
        std::fs::remove_file(path).map_err(|_| "SERVER_BOOTSTRAP_WRITE_FAILED")?;
    }
    std::fs::rename(temporary, path).map_err(|_| "SERVER_BOOTSTRAP_WRITE_FAILED")
}

fn required(name: &str) -> Result<String, &'static str> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.trim() == value)
        .ok_or("SERVER_CONFIGURATION_MISSING")
}

fn required_secret(name: &str) -> Result<String, &'static str> {
    let direct = match env::var(name) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => return Err("SERVER_SECRET_VALUE_INVALID"),
    };
    let file_name = format!("{name}_FILE");
    let file = env::var_os(&file_name);
    match (direct, file) {
        (Some(_), Some(_)) => Err("SERVER_SECRET_CONFIGURATION_CONFLICT"),
        (Some(value), None) => validate_secret_value(value),
        (None, Some(path)) => read_secret_file(Path::new(&path)),
        (None, None) => Err("SERVER_CONFIGURATION_MISSING"),
    }
}

fn required_secret_alias(canonical: &str, legacy: &str) -> Result<String, &'static str> {
    optional_secret_alias(canonical, legacy)?.ok_or("SERVER_CONFIGURATION_MISSING")
}

fn optional_secret_alias(canonical: &str, legacy: &str) -> Result<Option<String>, &'static str> {
    select_secret_alias(optional_secret(canonical)?, optional_secret(legacy)?)
}

fn select_secret_alias(
    canonical: Option<String>,
    legacy: Option<String>,
) -> Result<Option<String>, &'static str> {
    match (canonical, legacy) {
        (Some(_), Some(_)) => Err("SERVER_SECRET_CONFIGURATION_CONFLICT"),
        (Some(value), None) | (None, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn optional_secret(name: &str) -> Result<Option<String>, &'static str> {
    let direct = match env::var(name) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => return Err("SERVER_SECRET_VALUE_INVALID"),
    };
    let file_name = format!("{name}_FILE");
    let file = env::var_os(&file_name);
    match (direct, file) {
        (Some(_), Some(_)) => Err("SERVER_SECRET_CONFIGURATION_CONFLICT"),
        (Some(value), None) => validate_secret_value(value).map(Some),
        (None, Some(path)) => read_secret_file(Path::new(&path)).map(Some),
        (None, None) => Ok(None),
    }
}

fn validate_secret_value(value: String) -> Result<String, &'static str> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err("SERVER_SECRET_VALUE_INVALID");
    }
    Ok(value)
}

fn read_secret_file(path: &Path) -> Result<String, &'static str> {
    if !path.is_absolute() || path == Path::new("/") {
        return Err("SERVER_SECRET_FILE_INVALID");
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| "SERVER_SECRET_FILE_INVALID")?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > MAX_SECRET_FILE_BYTES
    {
        return Err("SERVER_SECRET_FILE_INVALID");
    }
    let mut value = std::fs::read_to_string(path).map_err(|_| "SERVER_SECRET_FILE_INVALID")?;
    if value.ends_with('\n') {
        value.pop();
        if value.ends_with('\r') {
            value.pop();
        }
    }
    validate_secret_value(value).map_err(|_| "SERVER_SECRET_FILE_INVALID")
}

async fn probe_management(path: &str) -> Result<(), &'static str> {
    let listen = env::var("RUNKU_MANAGEMENT_LISTEN")
        .unwrap_or_else(|_| DEFAULT_LISTEN.to_owned())
        .parse::<SocketAddr>()
        .map_err(|_| "SERVER_MANAGEMENT_LISTEN_INVALID")?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|_| "SERVER_HEALTH_PROBE_UNAVAILABLE")?;
    let response = client
        .get(format!("http://{listen}{path}"))
        .send()
        .await
        .map_err(|_| "SERVER_HEALTH_PROBE_UNAVAILABLE")?;
    if response.status().as_u16() != 204 {
        return Err("SERVER_HEALTH_PROBE_FAILED");
    }
    Ok(())
}

fn now() -> Result<TimestampMicros, &'static str> {
    let micros = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| "SERVER_CLOCK_INVALID")?
        .as_micros();
    Ok(TimestampMicros::new(
        i64::try_from(micros).map_err(|_| "SERVER_CLOCK_INVALID")?,
    ))
}

async fn shutdown() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
            return;
        }
    }
    let _result = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::NamedTempFile;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[test]
    fn secret_file_accepts_one_line_and_rejects_ambiguous_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut valid = NamedTempFile::new()?;
        writeln!(valid, "secret-value")?;
        assert_eq!(read_secret_file(valid.path())?, "secret-value");

        let mut multiline = NamedTempFile::new()?;
        writeln!(multiline, "first")?;
        writeln!(multiline, "second")?;
        assert_eq!(
            read_secret_file(multiline.path()),
            Err("SERVER_SECRET_FILE_INVALID")
        );

        Ok(())
    }

    #[test]
    fn postgres_database_target_ignores_credentials_but_preserves_database() {
        assert_eq!(
            postgres_database_target("postgres://platform:a@db.example/platform"),
            postgres_database_target("postgresql://other:b@DB.EXAMPLE:5432/platform")
        );
        assert_ne!(
            postgres_database_target("postgres://platform:a@db.example/platform"),
            postgres_database_target("postgres://product:b@db.example/product")
        );
        assert!(postgres_database_target("https://db.example/product").is_err());
        assert!(postgres_database_target("postgres:///product").is_err());
        assert!(postgres_database_target("postgres://db.example").is_err());
        assert!(postgres_database_target("postgres://db.example/").is_err());
        assert!(postgres_database_target("postgres://db.example/path/extra").is_err());
        assert_eq!(
            postgres_database_target("postgres://a:x@db.example./product"),
            postgres_database_target("postgres://b:y@db.example/product")
        );
    }

    #[test]
    fn canonical_database_secret_aliases_are_exclusive_and_legacy_compatible() {
        assert_eq!(
            select_secret_alias(Some("canonical".to_owned()), None),
            Ok(Some("canonical".to_owned()))
        );
        assert_eq!(
            select_secret_alias(None, Some("legacy".to_owned())),
            Ok(Some("legacy".to_owned()))
        );
        assert_eq!(select_secret_alias(None, None), Ok(None));
        assert_eq!(
            select_secret_alias(Some("canonical".to_owned()), Some("legacy".to_owned())),
            Err("SERVER_SECRET_CONFIGURATION_CONFLICT")
        );
    }

    #[cfg(unix)]
    #[test]
    fn secret_file_rejects_symlinks() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir()?;
        let target = directory.path().join("target");
        std::fs::write(&target, "secret-value\n")?;
        let link = directory.path().join("link");
        symlink(&target, &link)?;
        assert_eq!(read_secret_file(&link), Err("SERVER_SECRET_FILE_INVALID"));
        Ok(())
    }

    #[tokio::test]
    async fn file_usage_sink_sends_exact_authenticated_batch_without_redirects()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 2048];
            loop {
                let read = stream.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let Some(headers_end) = request.windows(4).position(|part| part == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = std::str::from_utf8(&request[..headers_end])?;
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .ok_or("missing content length")?;
                if request.len() >= headers_end + 4 + length {
                    break;
                }
            }
            stream
                .write_all(
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await?;
            Ok::<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>(request)
        });
        let sink = HttpFileUsageSink {
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint: url::Url::parse(&format!("http://{address}/v1/internal/usage-events"))?,
            cell_id: "cell_local-test".to_owned(),
            token: Arc::new(Zeroizing::new("workload-secret".to_owned())),
        };
        sink.deliver(&[FileUsageEvent {
            sequence: 1,
            event_id: "use_01J00000000000000000000000".to_owned(),
            project_id: "prj_01J00000000000000000000000".to_owned(),
            environment_id: "env_01J00000000000000000000000".to_owned(),
            kind: "application_file.committed".to_owned(),
            quantity: "6".to_owned(),
            unit: "byte".to_owned(),
            occurred_at_micros: "1767225600000000".to_owned(),
        }])
        .await?;
        let request = server.await??;
        let text = std::str::from_utf8(&request)?;
        let headers = text
            .split("\r\n\r\n")
            .next()
            .ok_or("missing headers")?
            .to_ascii_lowercase();
        assert!(headers.contains("authorization: bearer workload-secret\r\n"));
        assert!(text.contains("\"cellId\":\"cell_local-test\""));
        let body = text.split("\r\n\r\n").nth(1).ok_or("missing body")?;
        assert!(!body.contains("workload-secret"));
        assert!(body.contains("\"occurredAtMicros\":1767225600000000"));
        Ok(())
    }
}
