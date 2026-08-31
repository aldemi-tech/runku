//! Full CLI-to-service Remote Workspace synchronization over real sockets.

use std::{
    error::Error,
    process::{Command, Output},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use runku_core::{
    CodeTarget, DevelopmentCredentialId, EnvironmentDescriptor, EnvironmentId, EnvironmentScope,
    OperationId, ProjectId, ReleaseId, WorkspaceId,
};
use runku_development::{
    DevelopmentActor, DevelopmentCommand, DevelopmentContext, DevelopmentRepository,
    DevelopmentRepositoryConfig, SqlDevelopmentRepository,
};
use runku_development_access::{
    DevelopmentAccessRepository, DevelopmentAccessRepositoryConfig, DevelopmentCredential,
    DevelopmentCredentialStatus, DevelopmentKeyCrypto, ParsedDevelopmentKey,
    SqlDevelopmentAccessRepository,
};
use runku_development_client::{DevelopmentClient, DevelopmentClientConfig, DevelopmentEndpoint};
use runku_development_service::{
    DevelopmentAuditEvent, DevelopmentAuditSink, DevelopmentHttpConfig, DevelopmentHttpExposure,
    DevelopmentServiceClock, DevelopmentServiceError, RemoteWorkspaceService,
    RemoteWorkspaceServiceConfig, build_development_router, serve_development,
};
use runku_gateway::{DevelopmentCatalog, ServingCatalog};
use runku_protocol::{DevelopmentStateRequestV1, DevelopmentStateResponseV1};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{FilesystemArtifactStore, FilesystemStoreRole};
use runku_value::TimestampMicros;
use tempfile::{TempDir, tempdir};
use tokio::{net::TcpListener, task::JoinHandle};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Default)]
struct NoopAudit;

impl DevelopmentAuditSink for NoopAudit {
    fn try_emit(&self, _event: DevelopmentAuditEvent) {}
}

#[derive(Debug)]
struct Clock(AtomicI64);

impl DevelopmentServiceClock for Clock {
    fn now(&self) -> Result<TimestampMicros, DevelopmentServiceError> {
        Ok(TimestampMicros::new(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

struct Server {
    _directory: TempDir,
    endpoint: DevelopmentEndpoint,
    scope: EnvironmentScope,
    access: Arc<SqlDevelopmentAccessRepository>,
    crypto: Arc<DevelopmentKeyCrypto>,
    serving: Arc<ServingCatalog>,
    first_id: DevelopmentCredentialId,
    first_key: String,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn issue(
    access: &SqlDevelopmentAccessRepository,
    crypto: &DevelopmentKeyCrypto,
    scope: EnvironmentScope,
    label: &str,
    created_at: i64,
) -> TestResult<(DevelopmentCredentialId, String)> {
    let generated = crypto.generate(DevelopmentCredentialId::generate())?;
    let parsed: ParsedDevelopmentKey = generated.key.expose().parse()?;
    access
        .create_credential(&DevelopmentCredential {
            id: parsed.credential_id(),
            scope,
            actor: "cli.remote".parse::<DevelopmentActor>()?,
            label: label.parse()?,
            digest: generated.digest,
            status: DevelopmentCredentialStatus::Active,
            created_at: TimestampMicros::new(created_at),
            expires_at: None,
            revoked_at: None,
            deleted_at: None,
        })
        .await?;
    Ok((parsed.credential_id(), generated.key.expose().to_owned()))
}

async fn server() -> TestResult<Server> {
    let directory = tempdir()?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let environment = EnvironmentDescriptor::local_development(scope.environment_id());
    let context = DevelopmentContext { scope, environment };
    let access = Arc::new(
        SqlDevelopmentAccessRepository::connect_sqlite(
            &format!(
                "sqlite://{}?mode=rwc",
                directory.path().join("access.sqlite3").display()
            ),
            DevelopmentAccessRepositoryConfig::LOCAL,
        )
        .await?,
    );
    let crypto = Arc::new(DevelopmentKeyCrypto::new([91; 32]));
    let (first_id, first_key) = issue(&access, &crypto, scope, "first", 1).await?;
    let development = Arc::new(
        SqlDevelopmentRepository::connect_sqlite(
            &format!(
                "sqlite://{}?mode=rwc",
                directory.path().join("development.sqlite3").display()
            ),
            DevelopmentRepositoryConfig::LOCAL,
            context,
        )
        .await?,
    );
    development
        .apply(
            context,
            OperationId::generate(),
            &DevelopmentCommand::CreateWorkspace {
                workspace_id: WorkspaceId::generate(),
                workspace_ref: "bootstrap".parse()?,
                actor: "system".parse()?,
                created_at: TimestampMicros::new(1),
            },
        )
        .await?;
    let releases = Arc::new(
        SqlReleaseRepository::connect_sqlite(
            &format!(
                "sqlite://{}?mode=rwc",
                directory.path().join("releases.sqlite3").display()
            ),
            RepositoryConfig::LOCAL,
        )
        .await?,
    );
    let artifacts = Arc::new(
        FilesystemArtifactStore::open(
            directory.path().join("artifacts"),
            FilesystemStoreRole::LocalDevelopment,
        )
        .await?,
    );
    let catalog = Arc::new(DevelopmentCatalog::load(context, development.clone()).await?);
    let serving = Arc::new(ServingCatalog::load_allow_empty(scope, releases.clone()).await?);
    let service = Arc::new(RemoteWorkspaceService::new(
        RemoteWorkspaceServiceConfig { scope, environment },
        access.clone(),
        crypto.clone(),
        development,
        releases,
        artifacts,
        catalog,
        serving.clone(),
        Arc::new(Clock(AtomicI64::new(100))),
        Arc::new(NoopAudit),
    )?);
    let config = DevelopmentHttpConfig {
        max_concurrent_requests: 8,
        request_timeout: Duration::from_secs(5),
        exposure: DevelopmentHttpExposure::LoopbackPlaintext,
    };
    let router = build_development_router(config, service)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let _result = serve_development(
            listener,
            router,
            DevelopmentHttpExposure::LoopbackPlaintext,
            std::future::pending(),
        )
        .await;
    });
    Ok(Server {
        _directory: directory,
        endpoint: format!("http://{address}").parse()?,
        scope,
        access,
        crypto,
        serving,
        first_id,
        first_key,
        task,
    })
}

fn source_root(value: u8) -> TestResult<TempDir> {
    let root = tempdir()?;
    std::fs::create_dir(root.path().join("runku"))?;
    std::fs::write(
        root.path().join("runku/queries.ts"),
        format!(
            "import {{ query, v }} from '@runku/server';\nexport const version = query({{ auth: 'none', visibility: 'public', capabilities: [], args: v.null(), returns: v.float64(), handler() {{ return {value}; }} }});\n"
        ),
    )?;
    std::fs::write(
        root.path().join("runku/schema.ts"),
        "import { defineSchema } from '@runku/server';\nexport default defineSchema({});\n",
    )?;
    Ok(root)
}

fn sync(
    root: &TempDir,
    endpoint: &DevelopmentEndpoint,
    workspace: &str,
    key: &str,
) -> TestResult<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_runku"))
        .args([
            "workspace",
            "sync",
            "--root",
            root.path().to_str().ok_or("non-Unicode root")?,
            "--url",
            endpoint.as_str(),
            "--workspace",
            workspace,
            "--token-env",
            "RUNKU_TEST_DEV_KEY",
            "--create",
        ])
        .env("RUNKU_TEST_DEV_KEY", key)
        .output()?)
}

fn freeze(endpoint: &DevelopmentEndpoint, release_id: &str, key: &str) -> TestResult<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_runku"))
        .args([
            "workspace",
            "freeze",
            "--url",
            endpoint.as_str(),
            "--release",
            release_id,
            "--token-env",
            "RUNKU_TEST_DEV_KEY",
        ])
        .env("RUNKU_TEST_DEV_KEY", key)
        .output()?)
}

fn lines(output: &Output) -> TestResult<Vec<serde_json::Value>> {
    Ok(String::from_utf8(output.stdout.clone())?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?)
}

async fn state(
    endpoint: DevelopmentEndpoint,
    key: String,
    workspace: &str,
) -> TestResult<DevelopmentStateResponseV1> {
    let client = DevelopmentClient::new(endpoint, key, DevelopmentClientConfig::default())?;
    Ok(client
        .state(&DevelopmentStateRequestV1 {
            workspace_ref: workspace.parse()?,
        })
        .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn two_roots_keys_and_workspaces_sync_without_secret_or_lost_state() -> TestResult {
    let server = server().await?;
    let first = source_root(1)?;
    let second = source_root(2)?;
    let first_output = sync(&first, &server.endpoint, "team/first", &server.first_key)?;
    let second_output = sync(&second, &server.endpoint, "team/second", &server.first_key)?;
    assert!(
        first_output.status.success(),
        "first sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );
    assert!(
        second_output.status.success(),
        "second sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&second_output.stdout),
        String::from_utf8_lossy(&second_output.stderr)
    );
    for output in [&first_output, &second_output] {
        let stdout = String::from_utf8(output.stdout.clone())?;
        let stderr = String::from_utf8(output.stderr.clone())?;
        assert!(!stdout.contains(&server.first_key));
        assert!(!stderr.contains(&server.first_key));
        let events = lines(output)?;
        assert_eq!(events.len(), 4);
        assert_eq!(events[0]["stage"], "state");
        assert_eq!(events[1]["stage"], "create");
        assert_eq!(events[2]["stage"], "build");
        assert_eq!(events[3]["stage"], "publish");
    }
    let first_events = lines(&first_output)?;
    let stable_release = first_events[3]["releaseId"]
        .as_str()
        .ok_or("publish release missing")?;
    let freeze_output = freeze(&server.endpoint, stable_release, &server.first_key)?;
    assert!(
        freeze_output.status.success(),
        "freeze failed: {}",
        String::from_utf8_lossy(&freeze_output.stderr)
    );
    let freeze_events = lines(&freeze_output)?;
    assert_eq!(freeze_events.len(), 1);
    assert_eq!(freeze_events[0]["outcome"], "servable");
    let stable_release_id: ReleaseId = stable_release.parse()?;
    assert_eq!(
        server
            .serving
            .resolve(&CodeTarget::Release(stable_release_id))?
            .release_id,
        stable_release_id
    );
    let first_state = state(
        server.endpoint.clone(),
        server.first_key.clone(),
        "team/first",
    )
    .await?;
    let second_state = state(
        server.endpoint.clone(),
        server.first_key.clone(),
        "team/second",
    )
    .await?;
    let first_head = first_state
        .workspace
        .and_then(|binding| binding.head_revision)
        .ok_or("first HEAD missing")?;
    let second_head = second_state
        .workspace
        .and_then(|binding| binding.head_revision)
        .ok_or("second HEAD missing")?;
    assert_eq!(first_state.scope, server.scope);
    assert_eq!(second_state.scope, server.scope);
    assert_ne!(first_head, second_head);

    std::fs::write(
        first.path().join("runku/queries.ts"),
        "import { query, v } from '@runku/server';\nexport const version = query({ auth: 'none', visibility: 'public', capabilities: [], args: v.null(), returns: v.float64(), handler() { return 4; } });\n",
    )?;
    let advanced = sync(&first, &server.endpoint, "team/first", &server.first_key)?;
    assert!(advanced.status.success());
    let advanced_state = state(
        server.endpoint.clone(),
        server.first_key.clone(),
        "team/first",
    )
    .await?;
    assert_ne!(
        advanced_state
            .workspace
            .and_then(|binding| binding.head_revision),
        Some(first_head)
    );
    assert_eq!(
        server
            .serving
            .resolve(&CodeTarget::Release(stable_release_id))?
            .release_id,
        stable_release_id
    );

    let (_second_id, second_key) =
        issue(&server.access, &server.crypto, server.scope, "second", 2).await?;
    server
        .access
        .revoke_credential(server.scope, server.first_id, TimestampMicros::new(3))
        .await?;
    let denied = sync(&first, &server.endpoint, "team/first", &server.first_key)?;
    assert_eq!(denied.status.code(), Some(7));
    let denied_stderr = String::from_utf8(denied.stderr)?;
    assert!(denied_stderr.starts_with("error: DEVELOPMENT_AUTH_INVALID\n"));
    assert!(
        denied_stderr
            .contains("message: The Remote Workspace service rejected the supplied credential.")
    );
    assert!(denied_stderr.contains("hint: "));
    assert!(!denied_stderr.contains(&server.first_key));
    let accepted = sync(
        &source_root(3)?,
        &server.endpoint,
        "team/third",
        &second_key,
    )?;
    assert!(accepted.status.success());
    assert!(!String::from_utf8(accepted.stdout)?.contains(&second_key));
    Ok(())
}
