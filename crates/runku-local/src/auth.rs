//! Strict local functional-identity provider configuration.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
    str::FromStr as _,
    time::Duration,
};

use runku_identity::{ApplicationScope, JwtAlgorithm, JwtPrincipalProfile, JwtProviderConfig};
use runku_identity_provider::{AllowedLoopbackOrigin, LocalProviderNetworkConfig};
use serde::Deserialize;
use thiserror::Error;

const AUTH_CONFIG_MAX_BYTES: u64 = 64 * 1024;
const AUTH_CONFIG_PATH_MAX_BYTES: usize = 512;

/// Stable failure loading one local functional-identity provider.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalAuthConfigError {
    /// Relative path, file shape, size, or symlink policy failed.
    #[error("local auth configuration path is invalid")]
    InvalidPath,
    /// JSON shape, claim mapping, URL, duration, or cryptographic policy failed.
    #[error("local auth configuration is invalid")]
    InvalidConfiguration,
}

impl LocalAuthConfigError {
    /// Stable machine-readable category.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidPath => "LOCAL_AUTH_CONFIG_PATH_INVALID",
            Self::InvalidConfiguration => "LOCAL_AUTH_CONFIG_INVALID",
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LocalAuthWire {
    version: u8,
    provider_id: String,
    discovery_url: String,
    allowed_origin: String,
    issuer: String,
    audiences: Vec<String>,
    profile: ProfileWire,
    required_type: serde_json::Value,
    discriminator_claim: String,
    discriminator_value: String,
    algorithms: Vec<AlgorithmWire>,
    base_scopes: Vec<String>,
    max_token_ttl_seconds: u64,
    future_clock_skew_seconds: u64,
    mapping_revision: u64,
    default_cache_ttl_seconds: u64,
    max_cache_ttl_seconds: u64,
    request_timeout_millis: u64,
    unknown_kid_cooldown_seconds: u64,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProfileWire {
    User,
}

#[derive(Clone, Copy, Deserialize)]
enum AlgorithmWire {
    #[serde(rename = "RS256")]
    Rs256,
    #[serde(rename = "PS256")]
    Ps256,
    #[serde(rename = "ES256")]
    Es256,
    #[serde(rename = "EdDSA")]
    EdDsa,
}

impl From<AlgorithmWire> for JwtAlgorithm {
    fn from(value: AlgorithmWire) -> Self {
        match value {
            AlgorithmWire::Rs256 => Self::Rs256,
            AlgorithmWire::Ps256 => Self::Ps256,
            AlgorithmWire::Es256 => Self::Es256,
            AlgorithmWire::EdDsa => Self::EdDsa,
        }
    }
}

/// Loads one non-secret local auth descriptor under an explicit project root.
///
/// # Errors
///
/// Rejects absolute/traversing/symlink paths, non-files, empty/oversized input, unknown JSON
/// fields, duplicates, invalid scopes, non-loopback discovery, or unsafe JWT/cache policy.
pub fn load_local_auth_config(
    root: &Path,
    relative: &Path,
) -> Result<LocalProviderNetworkConfig, LocalAuthConfigError> {
    let file = safe_file(root, relative)?;
    let bytes = std::fs::read(file).map_err(|_| LocalAuthConfigError::InvalidPath)?;
    let wire: LocalAuthWire =
        serde_json::from_slice(&bytes).map_err(|_| LocalAuthConfigError::InvalidConfiguration)?;
    if wire.version != 1 {
        return Err(LocalAuthConfigError::InvalidConfiguration);
    }
    let audiences = unique_set(wire.audiences)?;
    let algorithms = unique_set(
        wire.algorithms
            .into_iter()
            .map(JwtAlgorithm::from)
            .collect(),
    )?;
    let base_scopes = unique_set(
        wire.base_scopes
            .into_iter()
            .map(|scope| ApplicationScope::from_str(&scope))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| LocalAuthConfigError::InvalidConfiguration)?,
    )?;
    let required_type = match wire.required_type {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => Some(value),
        _ => return Err(LocalAuthConfigError::InvalidConfiguration),
    };
    let config = LocalProviderNetworkConfig {
        provider: JwtProviderConfig {
            provider_id: wire.provider_id,
            issuer: wire.issuer,
            audiences,
            profile: match wire.profile {
                ProfileWire::User => JwtPrincipalProfile::User,
            },
            required_type,
            discriminator_claim: wire.discriminator_claim,
            discriminator_value: wire.discriminator_value,
            algorithms,
            base_scopes,
            scope_claim: None,
            scope_mapping: BTreeMap::new(),
            application_claim: None,
            application_mapping: BTreeMap::new(),
            max_token_ttl: Duration::from_secs(wire.max_token_ttl_seconds),
            future_clock_skew: Duration::from_secs(wire.future_clock_skew_seconds),
            mapping_revision: wire.mapping_revision,
        },
        discovery_url: wire.discovery_url,
        allowed_origin: wire
            .allowed_origin
            .parse::<AllowedLoopbackOrigin>()
            .map_err(|_| LocalAuthConfigError::InvalidConfiguration)?,
        default_cache_ttl: Duration::from_secs(wire.default_cache_ttl_seconds),
        max_cache_ttl: Duration::from_secs(wire.max_cache_ttl_seconds),
        request_timeout: Duration::from_millis(wire.request_timeout_millis),
        unknown_kid_cooldown: Duration::from_secs(wire.unknown_kid_cooldown_seconds),
    };
    config
        .validate()
        .map_err(|_| LocalAuthConfigError::InvalidConfiguration)?;
    Ok(config)
}

fn unique_set<T: Ord>(values: Vec<T>) -> Result<BTreeSet<T>, LocalAuthConfigError> {
    let length = values.len();
    let set = values.into_iter().collect::<BTreeSet<_>>();
    if set.len() == length {
        Ok(set)
    } else {
        Err(LocalAuthConfigError::InvalidConfiguration)
    }
}

fn safe_file(root: &Path, relative: &Path) -> Result<PathBuf, LocalAuthConfigError> {
    if relative.as_os_str().is_empty()
        || relative.as_os_str().as_encoded_bytes().len() > AUTH_CONFIG_PATH_MAX_BYTES
        || relative.is_absolute()
        || !relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(LocalAuthConfigError::InvalidPath);
    }
    let root_metadata =
        std::fs::symlink_metadata(root).map_err(|_| LocalAuthConfigError::InvalidPath)?;
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        return Err(LocalAuthConfigError::InvalidPath);
    }
    let canonical_root =
        std::fs::canonicalize(root).map_err(|_| LocalAuthConfigError::InvalidPath)?;
    let mut candidate = canonical_root.clone();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(LocalAuthConfigError::InvalidPath);
        };
        candidate.push(component);
        let metadata =
            std::fs::symlink_metadata(&candidate).map_err(|_| LocalAuthConfigError::InvalidPath)?;
        if metadata.file_type().is_symlink() {
            return Err(LocalAuthConfigError::InvalidPath);
        }
    }
    let metadata = std::fs::metadata(&candidate).map_err(|_| LocalAuthConfigError::InvalidPath)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > AUTH_CONFIG_MAX_BYTES {
        return Err(LocalAuthConfigError::InvalidPath);
    }
    let canonical =
        std::fs::canonicalize(candidate).map_err(|_| LocalAuthConfigError::InvalidPath)?;
    if !canonical.starts_with(canonical_root) {
        return Err(LocalAuthConfigError::InvalidPath);
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use tempfile::tempdir;

    use super::{LocalAuthConfigError, load_local_auth_config};

    type TestResult = Result<(), Box<dyn Error>>;

    fn valid() -> &'static str {
        r#"{
  "version": 1,
  "providerId": "better-auth-local",
  "discoveryUrl": "http://127.0.0.1:3000/api/auth/.well-known/openid-configuration",
  "allowedOrigin": "http://127.0.0.1:3000",
  "issuer": "https://chat.local.runku",
  "audiences": ["runku-chat-local"],
  "profile": "user",
  "requiredType": null,
  "discriminatorClaim": "token_use",
  "discriminatorValue": "user",
  "algorithms": ["EdDSA"],
  "baseScopes": ["function:invoke"],
  "maxTokenTtlSeconds": 900,
  "futureClockSkewSeconds": 30,
  "mappingRevision": 1,
  "defaultCacheTtlSeconds": 30,
  "maxCacheTtlSeconds": 300,
  "requestTimeoutMillis": 2000,
  "unknownKidCooldownSeconds": 5
}"#
    }

    #[test]
    fn loads_exact_local_provider_policy() -> TestResult {
        let directory = tempdir()?;
        std::fs::write(directory.path().join("runku.auth.json"), valid())?;
        let config = load_local_auth_config(directory.path(), Path::new("runku.auth.json"))?;
        assert_eq!(config.provider.provider_id, "better-auth-local");
        assert_eq!(config.provider.issuer, "https://chat.local.runku");
        assert_eq!(config.allowed_origin.to_string(), "http://127.0.0.1:3000");
        Ok(())
    }

    #[test]
    fn rejects_unknown_duplicate_non_loopback_and_traversal() -> TestResult {
        let directory = tempdir()?;
        let path = directory.path().join("runku.auth.json");
        std::fs::write(&path, valid().replace("\n}", ",\n  \"extra\": true\n}"))?;
        assert_eq!(
            load_local_auth_config(directory.path(), Path::new("runku.auth.json")),
            Err(LocalAuthConfigError::InvalidConfiguration)
        );
        std::fs::write(
            &path,
            valid().replace(
                "\"audiences\": [\"runku-chat-local\"]",
                "\"audiences\": [\"runku-chat-local\", \"runku-chat-local\"]",
            ),
        )?;
        assert_eq!(
            load_local_auth_config(directory.path(), Path::new("runku.auth.json")),
            Err(LocalAuthConfigError::InvalidConfiguration)
        );
        std::fs::write(&path, valid().replace("127.0.0.1", "192.168.1.5"))?;
        assert_eq!(
            load_local_auth_config(directory.path(), Path::new("runku.auth.json")),
            Err(LocalAuthConfigError::InvalidConfiguration)
        );
        std::fs::write(&path, valid().replace("  \"requiredType\": null,\n", ""))?;
        assert_eq!(
            load_local_auth_config(directory.path(), Path::new("runku.auth.json")),
            Err(LocalAuthConfigError::InvalidConfiguration)
        );
        std::fs::write(&path, valid().replace("null", "[]"))?;
        assert_eq!(
            load_local_auth_config(directory.path(), Path::new("runku.auth.json")),
            Err(LocalAuthConfigError::InvalidConfiguration)
        );
        assert_eq!(
            load_local_auth_config(directory.path(), Path::new("../runku.auth.json")),
            Err(LocalAuthConfigError::InvalidPath)
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_config() -> TestResult {
        use std::os::unix::fs::symlink;

        let directory = tempdir()?;
        std::fs::write(directory.path().join("real.json"), valid())?;
        symlink(
            directory.path().join("real.json"),
            directory.path().join("runku.auth.json"),
        )?;
        assert_eq!(
            load_local_auth_config(directory.path(), Path::new("runku.auth.json")),
            Err(LocalAuthConfigError::InvalidPath)
        );
        Ok(())
    }
}
