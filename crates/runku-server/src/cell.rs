//! Multi-Environment cell manifest and Host-dispatched application ingress.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    str::FromStr as _,
    sync::Arc,
};

use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse as _, Response},
};
use serde::Deserialize;
use tower::ServiceExt as _;

use crate::product::ProductAdapter;

const MAX_CELL_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_ENVIRONMENTS_PER_MEMBER: usize = 64;
const MAX_HOSTS_PER_ENVIRONMENT: usize = 8;

/// Operator-selected resource isolation profile for one cell member.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum CellMode {
    /// Several independently scoped Environments may share this process.
    Shared,
    /// One Environment owns this process, using the identical Product implementation.
    Dedicated,
}

/// Strict versioned configuration for one cell member.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CellManifest {
    pub(crate) version: u8,
    pub(crate) mode: CellMode,
    pub(crate) member_id: String,
    pub(crate) environments: Vec<CellEnvironment>,
}

/// One exact Environment assigned to a cell member.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CellEnvironment {
    pub(crate) root: PathBuf,
    pub(crate) hosts: Vec<String>,
    pub(crate) platform_database_url_file: Option<PathBuf>,
    #[serde(default)]
    pub(crate) allowed_origins: Vec<String>,
    pub(crate) auth_config: Option<PathBuf>,
}

impl CellManifest {
    /// Loads a bounded, regular, non-symlinked JSON manifest and validates all routing keys.
    pub(crate) fn load(path: &Path) -> Result<Self, &'static str> {
        if !path.is_absolute() || path == Path::new("/") {
            return Err("SERVER_CELL_CONFIG_INVALID");
        }
        let metadata = std::fs::symlink_metadata(path).map_err(|_| "SERVER_CELL_CONFIG_INVALID")?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() == 0
            || metadata.len() > MAX_CELL_MANIFEST_BYTES
        {
            return Err("SERVER_CELL_CONFIG_INVALID");
        }
        let bytes = std::fs::read(path).map_err(|_| "SERVER_CELL_CONFIG_INVALID")?;
        let manifest: Self =
            serde_json::from_slice(&bytes).map_err(|_| "SERVER_CELL_CONFIG_INVALID")?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1
            || !valid_member_id(&self.member_id)
            || self.environments.is_empty()
            || self.environments.len() > MAX_ENVIRONMENTS_PER_MEMBER
            || self.mode == CellMode::Dedicated && self.environments.len() != 1
        {
            return Err("SERVER_CELL_CONFIG_INVALID");
        }
        let mut roots = BTreeSet::new();
        let mut hosts = BTreeSet::new();
        for environment in &self.environments {
            if !environment.root.is_absolute()
                || environment.root == Path::new("/")
                || !roots.insert(environment.root.clone())
                || environment.hosts.is_empty()
                || environment.hosts.len() > MAX_HOSTS_PER_ENVIRONMENT
                || environment.allowed_origins.len() > 64
                || environment
                    .platform_database_url_file
                    .as_ref()
                    .is_some_and(|path| !path.is_absolute() || path == Path::new("/"))
                || environment.auth_config.as_ref().is_some_and(|path| {
                    path.is_absolute()
                        || path.as_os_str().is_empty()
                        || path.components().any(|component| {
                            matches!(
                                component,
                                std::path::Component::ParentDir
                                    | std::path::Component::RootDir
                                    | std::path::Component::Prefix(_)
                            )
                        })
                })
            {
                return Err("SERVER_CELL_CONFIG_INVALID");
            }
            let distinct_origins = environment.allowed_origins.iter().collect::<BTreeSet<_>>();
            if distinct_origins.len() != environment.allowed_origins.len() {
                return Err("SERVER_CELL_CONFIG_INVALID");
            }
            for host in &environment.hosts {
                if !valid_host(host) || !hosts.insert(host.clone()) {
                    return Err("SERVER_CELL_CONFIG_INVALID");
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RoutedProduct {
    adapter: Arc<ProductAdapter>,
    s3: Router,
}

#[derive(Clone)]
struct CellRouterState {
    routes: Arc<BTreeMap<String, RoutedProduct>>,
}

/// Creates the single application listener service for every warm Environment in a member.
pub(crate) fn build_cell_router(
    products: Vec<(Vec<String>, Arc<ProductAdapter>)>,
) -> Result<Router, &'static str> {
    let mut routes = BTreeMap::new();
    for (hosts, adapter) in products {
        let product = RoutedProduct {
            s3: adapter
                .s3_router()
                .map_err(|_| "SERVER_PRODUCT_CONFIGURATION_INVALID")?,
            adapter,
        };
        for host in hosts {
            if routes.insert(host, product.clone()).is_some() {
                return Err("SERVER_CELL_CONFIG_INVALID");
            }
        }
    }
    Ok(Router::new()
        .fallback(dispatch)
        .with_state(CellRouterState {
            routes: Arc::new(routes),
        }))
}

async fn dispatch(State(state): State<CellRouterState>, request: Request) -> Response {
    let Some(host) = request_host(&request) else {
        return cell_failure(StatusCode::BAD_REQUEST, "CELL_HOST_INVALID");
    };
    let Some(product) = state.routes.get(&host) else {
        return cell_failure(StatusCode::NOT_FOUND, "CELL_ROUTE_NOT_FOUND");
    };
    let is_s3 = request.uri().path().starts_with("/s3/");
    let mut router = product.s3.clone();
    if let Some(application) = product.adapter.application_router().await {
        router = router.merge(application);
    } else if !is_s3 {
        return cell_failure(StatusCode::SERVICE_UNAVAILABLE, "CELL_ENVIRONMENT_COLD");
    }
    match router.oneshot(request).await {
        Ok(response) => response,
        Err(error) => match error {},
    }
}

fn request_host(request: &Request) -> Option<String> {
    let values = request
        .headers()
        .get_all(header::HOST)
        .iter()
        .collect::<Vec<_>>();
    let [value] = values.as_slice() else {
        return None;
    };
    let authority = axum::http::uri::Authority::from_str(value.to_str().ok()?).ok()?;
    let host = authority.host();
    valid_host(host).then(|| host.to_owned())
}

fn cell_failure(status: StatusCode, code: &'static str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        axum::Json(serde_json::json!({"error":{"code":code}})),
    )
        .into_response()
}

fn valid_member_id(value: &str) -> bool {
    value.strip_prefix("member_").is_some_and(|suffix| {
        (2..=64).contains(&suffix.len())
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    })
}

fn valid_host(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value == value.to_ascii_lowercase()
        && !value.ends_with('.')
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, net::SocketAddr};

    use super::*;
    use crate::product::{ProductAdapter, ProductAdapterConfig};
    use axum::body::to_bytes;
    use runku_core::{
        ApplicationClientId, BuildId, CodeTarget, CredentialId, FunctionId, ReleaseId, WorkspaceRef,
    };
    use runku_development::DevelopmentActor;
    use runku_identity::{ApplicationScope, ClientKind};
    use runku_local::{LocalIdentityManager, initialize_local, publish_local};
    use runku_management_service::ManagementProduct;
    use runku_protocol::{QueryCallV1, decode_success_v1, encode_query_call_v1};
    use runku_releases::{
        AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility,
        ReleaseManifestV1, RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_release_manifest,
        encode_safe_esm_bundle,
    };
    use runku_value::{CanonicalValue, TimestampMicros};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn manifest_rejects_duplicate_hosts_and_dedicated_fanout() {
        let environment = |root: &str| CellEnvironment {
            root: PathBuf::from(root),
            hosts: vec!["tenant.example.test".to_owned()],
            platform_database_url_file: None,
            allowed_origins: Vec::new(),
            auth_config: None,
        };
        let shared = CellManifest {
            version: 1,
            mode: CellMode::Shared,
            member_id: "member_test-01".to_owned(),
            environments: vec![environment("/tmp/a"), environment("/tmp/b")],
        };
        assert_eq!(shared.validate(), Err("SERVER_CELL_CONFIG_INVALID"));

        let dedicated = CellManifest {
            version: 1,
            mode: CellMode::Dedicated,
            member_id: "member_test-01".to_owned(),
            environments: vec![
                environment("/tmp/a"),
                CellEnvironment {
                    hosts: vec!["other.example.test".to_owned()],
                    ..environment("/tmp/b")
                },
            ],
        };
        assert_eq!(dedicated.validate(), Err("SERVER_CELL_CONFIG_INVALID"));
    }

    #[test]
    fn host_and_member_keys_are_canonical_and_bounded() {
        assert!(valid_host("environment-a.runku.example"));
        assert!(!valid_host("Environment-A.runku.example"));
        assert!(!valid_host("environment-a.runku.example."));
        assert!(!valid_host("-environment.runku.example"));
        assert!(valid_member_id("member_use1-a"));
        assert!(!valid_member_id("member_USE1-A"));
    }

    #[test]
    fn request_host_ignores_forwarded_host_and_accepts_an_explicit_port() -> TestResult {
        let request = Request::builder()
            .uri("/v1/query")
            .header(header::HOST, "environment-a.runku.example:443")
            .header("x-forwarded-host", "attacker.example")
            .body(axum::body::Body::empty())?;
        assert_eq!(
            request_host(&request),
            Some("environment-a.runku.example".to_owned())
        );
        Ok(())
    }

    #[test]
    fn duplicate_host_header_fails_closed() -> TestResult {
        let request = Request::builder()
            .uri("/v1/query")
            .header(header::HOST, "environment-a.runku.example")
            .header(header::HOST, "environment-b.runku.example")
            .body(axum::body::Body::empty())?;
        assert_eq!(request_host(&request), None);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_cell_router_serves_two_warm_environments_without_key_confusion() -> TestResult {
        let first_root = tempfile::tempdir()?;
        let second_root = tempfile::tempdir()?;
        let (first, first_key) = Box::pin(warm_product(first_root.path(), "first")).await?;
        let (second, second_key) = Box::pin(warm_product(second_root.path(), "second")).await?;
        let router = build_cell_router(vec![
            (vec!["first.runku.test".to_owned()], Arc::clone(&first)),
            (vec!["second.runku.test".to_owned()], Arc::clone(&second)),
        ])?;

        let call = encode_query_call_v1(&QueryCallV1 {
            target: CodeTarget::Workspace("default".parse()?),
            function: "queries.echo".parse()?,
            arguments: CanonicalValue::String("from-cell".to_owned()),
        })?;
        let invoke = |host: &'static str, key: String| {
            Request::post("/v1/query")
                .header(header::HOST, host)
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-runku-key", key)
                .body(axum::body::Body::from(call.clone()))
        };
        let first_response = router
            .clone()
            .oneshot(invoke("first.runku.test", first_key.clone())?)
            .await?;
        assert_eq!(first_response.status(), StatusCode::OK);
        assert_eq!(
            decode_success_v1(&to_bytes(first_response.into_body(), 1024 * 1024).await?)?.result,
            CanonicalValue::String("from-cell".to_owned())
        );
        let second_response = router
            .clone()
            .oneshot(invoke("second.runku.test", second_key)?)
            .await?;
        assert_eq!(second_response.status(), StatusCode::OK);

        let confused = router
            .oneshot(invoke("second.runku.test", first_key)?)
            .await?;
        assert_eq!(confused.status(), StatusCode::UNAUTHORIZED);
        first.shutdown().await;
        second.shutdown().await;
        Ok(())
    }

    async fn warm_product(
        root: &Path,
        label: &str,
    ) -> Result<(Arc<ProductAdapter>, String), Box<dyn std::error::Error>> {
        let workspace: WorkspaceRef = "default".parse()?;
        let (state, _) = initialize_local(
            root,
            workspace.clone(),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?;
        let source = "export default async (_ctx, value) => value;";
        let bundle = SafeEsmBundleV1::from_sources([source])?;
        let artifact = encode_safe_esm_bundle(&bundle)?;
        let contract = Sha256Digest::of(format!("cell-{label}-contract").as_bytes());
        let release_id = ReleaseId::generate();
        let manifest = encode_release_manifest(&ReleaseManifestV1 {
            release_id,
            project_id: state.project_id,
            build_id: BuildId::generate(),
            created_at: TimestampMicros::new(1_800_000_000_000_001),
            runtime_version: "platform-js-1".parse()?,
            artifact: bundle.descriptor()?,
            function_contract_hash: contract,
            schema_contract_hash: contract,
            index_contract_hash: contract,
            functions: vec![FunctionManifest {
                id: FunctionId::generate(),
                name: "queries.echo".parse()?,
                function_type: FunctionType::Query,
                visibility: FunctionVisibility::Public,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(source.as_bytes()),
                arguments_contract_hash: contract,
                result_contract_hash: contract,
                capabilities: vec![Capability::DbRead],
            }],
            cron_definitions: Vec::new(),
        })?;
        publish_local(
            root,
            &workspace,
            &DevelopmentActor::from_str(&format!("cell-{label}"))?,
            &manifest,
            &artifact,
        )
        .await?;
        let identities = LocalIdentityManager::open(root).await?;
        let scopes = BTreeSet::from(["functions:invoke".parse::<ApplicationScope>()?]);
        let client_id = ApplicationClientId::generate();
        identities
            .create_client(
                client_id,
                format!("{label}-browser").parse()?,
                ClientKind::Public,
                scopes.clone(),
                TimestampMicros::new(1_800_000_000_000_002),
            )
            .await?;
        let key = identities
            .create_credential(
                CredentialId::generate(),
                client_id,
                format!("{label}-key").parse()?,
                scopes,
                TimestampMicros::new(1_800_000_000_000_003),
                None,
            )
            .await?
            .key
            .expose()
            .to_owned();
        let product = Arc::new(
            Box::pin(ProductAdapter::open(
                root.to_path_buf(),
                ProductAdapterConfig {
                    trusted_application_listen: None,
                    embedded_application_listener: true,
                    platform_database_url: None,
                    log_archive: None,
                    log_journal: None,
                    allowed_origins: BTreeSet::new(),
                    auth_config: None,
                    file_object_store: None,
                    file_storage_limits: runku_file_storage::FileStorageLimits::DEFAULT,
                    file_usage_sink: None,
                    file_usage_interval: std::time::Duration::from_secs(1),
                    full_node_runtime: None,
                },
            ))
            .await?,
        );
        product.release(&release_id.to_string(), None).await?;
        product
            .promote("stable", &release_id.to_string(), Some(None))
            .await?;
        Ok((product, key))
    }
}
