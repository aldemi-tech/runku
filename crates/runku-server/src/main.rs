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
use serde::Deserialize;
use tokio::net::TcpListener;

use crate::product::ProductAdapter;

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
    if command == "check" {
        let _ = external_authenticator(config.oidc.as_ref())?;
        println!("configuration valid");
        return Ok(());
    }
    let repository = Arc::new(
        SqlPlatformIdentityRepository::connect_postgres(
            &config.database_url,
            PlatformIdentityRepositoryConfig::AUTHORITATIVE,
        )
        .await
        .map_err(|_| "SERVER_PLATFORM_DATABASE_UNAVAILABLE")?,
    );
    if command == "migrate" {
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
                config.log_archive.clone(),
                log_journal.clone(),
                config.product_allowed_origins.clone(),
                config.product_auth_config.clone(),
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
    database_url: String,
    pepper: [u8; 32],
    state_directory: PathBuf,
    listen: SocketAddr,
    exposure: ManagementHttpExposure,
    public_management_endpoint: Option<String>,
    oidc: Option<OidcConfig>,
    product_root: Option<PathBuf>,
    product_allowed_origins: BTreeSet<CorsOrigin>,
    product_auth_config: Option<PathBuf>,
    log_archive: Option<LogArchive>,
    log_journal: Option<ServerLogJournalConfig>,
}

impl ServerConfig {
    fn load() -> Result<Self, &'static str> {
        let database_url = required_secret("RUNKU_DATABASE_URL")?;
        if !(database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")) {
            return Err("SERVER_DATABASE_URL_INVALID");
        }
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
        Ok(Self {
            database_url,
            pepper,
            state_directory,
            listen,
            exposure,
            public_management_endpoint,
            oidc,
            product_root,
            product_allowed_origins,
            product_auth_config,
            log_archive,
            log_journal,
        })
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
    use tempfile::NamedTempFile;

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
}
