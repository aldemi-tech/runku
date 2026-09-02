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

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => {
            eprintln!("error: {code}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), &'static str> {
    let command = env::args().nth(1).unwrap_or_else(|| "serve".to_owned());
    if !matches!(
        command.as_str(),
        "serve" | "check" | "migrate" | "recover-bootstrap" | "version"
    ) || env::args().len() > 2
    {
        return Err("SERVER_USAGE_INVALID");
    }
    if command == "version" {
        println!("runku-server {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
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
    initialize_bootstrap(&identity, &config.state_directory).await?;
    let http = ManagementHttpConfig {
        max_concurrent_requests: 1_024,
        exposure: config.exposure,
        public_management_endpoint: config.public_management_endpoint.clone(),
    };
    let external = external_authenticator(config.oidc.as_ref())?;
    let product = match config.product_root.as_ref() {
        Some(root) => {
            Some(Arc::new(ProductAdapter::open(root.clone()).await?) as Arc<dyn ManagementProduct>)
        }
        None => None,
    };
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
    repository.close().await;
    result.map_err(|_| "SERVER_MANAGEMENT_STOPPED")
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
}

impl ServerConfig {
    fn load() -> Result<Self, &'static str> {
        let database_url = required("RUNKU_DATABASE_URL")?;
        if !(database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")) {
            return Err("SERVER_DATABASE_URL_INVALID");
        }
        let encoded = required("RUNKU_PLATFORM_IDENTITY_PEPPER")?;
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
        Ok(Self {
            database_url,
            pepper,
            state_directory,
            listen,
            exposure,
            public_management_endpoint,
            oidc,
            product_root,
        })
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
    let _ = tokio::signal::ctrl_c().await;
}
