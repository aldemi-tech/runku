//! Strict argument model for the `runku` local Product Base CLI.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{collections::BTreeSet, ffi::OsString, net::SocketAddr, path::PathBuf, str::FromStr};

use runku_build::BuildMetadata;
use runku_core::{
    ApplicationClientId, BuildId, ChannelName, CredentialId, DevRevisionId,
    DevelopmentCredentialId, EnvironmentId, EnvironmentScope, FunctionId, InvocationId, ProjectId,
    ReleaseId, RequestId, WorkspaceRef,
};
use runku_development::DevelopmentActor;
use runku_development_access::DevelopmentCredentialLabel;
use runku_development_client::DevelopmentEndpoint;
use runku_gateway::CorsOrigin;
use runku_identity::{ApplicationClientName, ApplicationScope, ClientKind, CredentialLabel};
use runku_observability::{LogCursor, LogLevel, LogStream};
use runku_platform_identity::DeviceName;
use runku_value::TimestampMicros;

/// Stable base command-line help.
pub const HELP: &str = "runku 0.3.0\n\nUSAGE:\n  runku init [--root PATH] [--workspace REF] [--listen LOOPBACK:PORT] [--project-id prj_* --environment-id env_*]\n  runku build [--root PATH] [--release-id rel_* --build-id bld_* --created-at-micros I64]\n  runku publish [--root PATH] --manifest FILE --artifact FILE [--workspace REF] [--actor LABEL] [--expected-head empty|drv_*]\n  runku release [--root PATH] --release rel_* [--against CHANNEL]\n  runku promote [--root PATH] --channel CHANNEL --release rel_* [--expected empty|rel_*]\n  runku rollback [--root PATH] --channel CHANNEL --expected rel_* --to rel_*\n  runku status [--root PATH]\n  runku dev [--root PATH] [--origin http(s)://HOST[:PORT]]... [--prebuilt] [--auth-config RELATIVE] [--application-env RELATIVE] [--public-env-prefix PREFIX] [--prepare] [--replace-remote-credentials]\n  runku doctor [--root PATH]\n  runku logs [--root PATH] [--after logc_N] [--limit 1..1000] [--stream platform|function] [--level debug|info|warn|error] [--function fnc_*] [--request req_*] [--invocation inv_*] [--client app_*] [--credential crd_*] [--release rel_*] [--follow]\n  runku logs prune [--root PATH] --before-micros I64 [--maximum 1..10000] [--apply --environment env_*]\n  runku logs export-otlp [--root PATH] --config RELATIVE [--once]\n  runku client create [--root PATH] --name NAME --kind public|confidential --scope SCOPE... [--client-id app_*]\n  runku client list [--root PATH]\n  runku key create [--root PATH] --client app_* --label LABEL --scope SCOPE... [--key-id crd_*] [--expires-at-micros I64]\n  runku key list [--root PATH] --client app_*\n  runku key reveal [--root PATH] --client app_* --key crd_*\n  runku key rotate [--root PATH] --client app_* --key crd_* --label LABEL [--new-key-id crd_*] [--expires-at-micros I64]\n  runku key revoke [--root PATH] --key crd_*\n  runku key delete [--root PATH] --key crd_*\n  runku workspace key create [--root PATH] --actor ACTOR --label LABEL [--key-id dvk_*] [--expires-at-micros I64]\n  runku workspace key list [--root PATH]\n  runku workspace key rotate [--root PATH] --key dvk_* --label LABEL [--new-key-id dvk_*] [--expires-at-micros I64]\n  runku workspace key revoke [--root PATH] --key dvk_*\n  runku workspace key delete [--root PATH] --key dvk_*\n  runku workspace sync [--root PATH] --url ORIGIN --workspace REF --token-env RUNKU_NAME [--expected-head empty|drv_*] [--create]\n  runku --help\n  runku --version\n\nPROJECT ROOT:\n  --root PATH  Project directory; defaults to the current working directory.\n\nLOCAL DEVELOPMENT:\n  init defaults to workspace local and listener 127.0.0.1:3210.\n  dev initializes missing local state, reconciles local Application Credentials, builds, publishes, and watches runku/.\n  public dotenv aliases are detected for known frontend tools; the SDK itself is framework-agnostic.\n  RUNKU_SECRET_KEY always remains server-only and is never copied to a public alias.\n  --prebuilt serves an already-published package without reading application sources.\n";

/// Immutable archive administration help appended by the executable.
pub const LOG_ARCHIVE_HELP: &str = "\nLOG ARCHIVE:\n  runku logs archive-status [--root PATH] [--remote]\n  runku logs prune [--root PATH] [--remote] --before-micros I64 [--maximum 1..10000] [--apply --environment env_*]\n  Archive status verifies contiguous Parquet manifests. Prune never passes the verified frontier.\n";

/// Default Workspace created by zero-configuration local development.
pub const DEFAULT_LOCAL_WORKSPACE: &str = "local";

/// Default listener created by zero-configuration local development.
pub const DEFAULT_LOCAL_LISTENER: &str = "127.0.0.1:3210";

/// Stable Remote Release freeze help appended by the executable.
pub const WORKSPACE_FREEZE_HELP: &str = "\nREMOTE RELEASE:\n  runku workspace freeze --url ORIGIN --release rel_* --token-env RUNKU_NAME [--against rel_*]\n";

/// Platform login help appended by the executable.
pub const LOGIN_HELP: &str = "\nREMOTE LOGIN:\n  runku login\n  runku login [--url ORIGIN] [--device NAME] [--browser] [--code-env RUNKU_NAME] [--no-open]\n  runku login [--url ORIGIN] [--device NAME] --oidc-token-env RUNKU_NAME [--code-env RUNKU_NAME]\n\n  Without --url, login offers the saved authentication server or uses https://api.runku.app.\n  Without an authentication flag, login discovers and offers the server-supported methods.\n";

/// Authenticated Management API lifecycle help appended by the executable.
pub const MANAGEMENT_HELP: &str = "\nREMOTE MANAGEMENT:\n  Add --remote to publish, release, promote, rollback, status, or logs.\n  These commands use the current runku login session and the Project/Environment in --root.\n  Remote publish requires --expected-head empty|drv_*; logs --remote --follow uses one streaming connection.\n";

/// Strict parsed CLI command; the project root defaults to the current directory and Production is never implicit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CliCommand {
    /// Print [`HELP`].
    Help,
    /// Print package version.
    Version,
    /// Enroll one CLI device through a Runku authentication server.
    Login {
        /// Optional canonical HTTPS or literal-loopback HTTP authentication origin.
        endpoint: Option<DevelopmentEndpoint>,
        /// Optional human-readable device name; absent derives a safe local default.
        device_name: Option<DeviceName>,
        /// Optional allowlisted environment variable containing a one-time invitation code.
        code_environment: Option<TokenEnvironmentName>,
        /// Optional allowlisted environment variable containing a verified external-provider token.
        oidc_token_environment: Option<TokenEnvironmentName>,
        /// Complete an OIDC Authorization Code with PKCE flow in the system browser.
        browser: bool,
        /// Print the authorization URL instead of opening the system browser.
        no_open: bool,
    },
    /// Initialize one local project root.
    Init {
        /// Existing project directory.
        root: PathBuf,
        /// Durable default Workspace.
        workspace: WorkspaceRef,
        /// Explicit loopback listener persisted in local state.
        listen: SocketAddr,
        /// Optional externally allocated exact Product scope; both IDs are supplied together.
        scope: Option<EnvironmentScope>,
    },
    /// Compile strict TypeScript/JavaScript sources into one immutable canonical package.
    Build {
        /// Initialized project root.
        root: PathBuf,
        /// Canonical declarative source directory (`runku/`).
        config: PathBuf,
        /// Optional complete reproducibility tuple; absent allocates fresh metadata.
        metadata: Option<BuildMetadata>,
    },
    /// Publish one already-built canonical package.
    Publish {
        /// Use the Management API and the current `runku login` session.
        remote: bool,
        /// Initialized project root.
        root: PathBuf,
        /// Canonical Release Manifest bytes file.
        manifest: PathBuf,
        /// Canonical Safe ESM bundle bytes file.
        artifact: PathBuf,
        /// Optional Workspace override; absent selects the initialized default.
        workspace: Option<WorkspaceRef>,
        /// Bounded audit actor.
        actor: DevelopmentActor,
        /// Optional exact CAS precondition. `Some(None)` requires an empty Workspace.
        expected_head: Option<Option<DevRevisionId>>,
    },
    /// Validate one published candidate and make it explicitly servable.
    Release {
        /// Use the Management API and the current `runku login` session.
        remote: bool,
        /// Initialized project root.
        root: PathBuf,
        /// Published immutable candidate.
        release_id: ReleaseId,
        /// Optional exact Channel compatibility baseline.
        against: Option<ChannelName>,
    },
    /// Move one Channel to a compatible servable Release.
    Promote {
        /// Use the Management API and the current `runku login` session.
        remote: bool,
        /// Initialized project root.
        root: PathBuf,
        /// Channel to create or move.
        channel: ChannelName,
        /// Target servable Release.
        release_id: ReleaseId,
        /// Optional operator CAS precondition; `Some(None)` requires no binding.
        expected: Option<Option<ReleaseId>>,
    },
    /// Move one Channel back with an exact current-Release precondition.
    Rollback {
        /// Use the Management API and the current `runku login` session.
        remote: bool,
        /// Initialized project root.
        root: PathBuf,
        /// Channel to move.
        channel: ChannelName,
        /// Required current Channel Release.
        expected: ReleaseId,
        /// Older compatible target Release.
        target: ReleaseId,
    },
    /// Print one coherent read-only Release/Channel snapshot.
    Status {
        /// Use the Management API and the current `runku login` session.
        remote: bool,
        /// Initialized project root.
        root: PathBuf,
    },
    /// Run the complete local Product Base until a termination signal.
    Dev {
        /// Initialized and published project root.
        root: PathBuf,
        /// Exact browser origins admitted for CORS/WebSocket handshake.
        origins: BTreeSet<CorsOrigin>,
        /// Optional canonical source directory built and synchronized continuously.
        watch_config: Option<PathBuf>,
        /// Optional local functional-identity descriptor relative to the project root.
        auth_config: Option<PathBuf>,
        /// Relative dotenv file reconciled with local Application Credentials.
        application_env: PathBuf,
        /// Prefix used for public URL, target, and key variables.
        public_env_prefix: Option<String>,
        /// Prepare state/credentials and exit before building or serving.
        prepare: bool,
        /// Replace remote/foreign values without an interactive confirmation.
        replace_remote_credentials: bool,
    },
    /// Diagnose one initialized/published local project without repair.
    Doctor {
        /// Project root to inspect.
        root: PathBuf,
    },
    /// Query or follow one exact local Operational Logs stream.
    Logs {
        /// Use the Management API and the current `runku login` session.
        remote: bool,
        /// Initialized project root.
        root: PathBuf,
        /// Exclusive durable cursor.
        after: LogCursor,
        /// Bounded page size.
        limit: u16,
        /// Optional exact stream.
        stream: Option<LogStream>,
        /// Optional minimum severity.
        minimum_level: Option<LogLevel>,
        /// Optional exact Function.
        function_id: Option<FunctionId>,
        /// Optional exact Request.
        request_id: Option<RequestId>,
        /// Optional exact Invocation.
        invocation_id: Option<InvocationId>,
        /// Optional exact Application Client.
        client_id: Option<ApplicationClientId>,
        /// Optional exact credential.
        credential_id: Option<CredentialId>,
        /// Optional exact Release.
        release_id: Option<ReleaseId>,
        /// Continue polling from each emitted cursor until interrupted.
        follow: bool,
    },
    /// Verify and summarize immutable Operational Log archive coverage.
    LogsArchiveStatus {
        /// Use the Management API and the current `runku login` session.
        remote: bool,
        /// Initialized project root.
        root: PathBuf,
    },
    /// Dry-run or explicitly apply bounded Operational Logs retention.
    LogsPrune {
        /// Use the Management API and the current `runku login` session.
        remote: bool,
        /// Initialized project root.
        root: PathBuf,
        /// Delete records strictly older than this timestamp.
        before: TimestampMicros,
        /// Maximum rows inspected/deleted in one transaction.
        maximum: u32,
        /// False performs a dry-run; true deletes.
        apply: bool,
        /// Exact Environment confirmation required when applying.
        environment: Option<EnvironmentId>,
    },
    /// Export Operational Logs through a strict named OTLP/HTTP configuration.
    LogsExportOtlp {
        /// Initialized project root.
        root: PathBuf,
        /// Strict exporter JSON file relative to the project root.
        config: PathBuf,
        /// Execute one bounded batch instead of following continuously.
        once: bool,
    },
    /// Create one stable public or confidential Application Client.
    ClientCreate {
        /// Initialized project root.
        root: PathBuf,
        /// Optional caller-supplied ID for result reconciliation.
        id: Option<ApplicationClientId>,
        /// Unique operator-facing client name.
        name: ApplicationClientName,
        /// Public or trusted-server execution context.
        kind: ClientKind,
        /// Maximum scopes allowed on keys under this client.
        scopes: BTreeSet<ApplicationScope>,
    },
    /// List Application Clients without credential material.
    ClientList {
        /// Initialized project root.
        root: PathBuf,
    },
    /// Create another independently revocable key.
    KeyCreate {
        /// Initialized project root.
        root: PathBuf,
        /// Optional caller-supplied ID for reconciliation.
        id: Option<CredentialId>,
        /// Owning Application Client.
        client_id: ApplicationClientId,
        /// Operator-facing key label.
        label: CredentialLabel,
        /// Least-privilege scopes for this key.
        scopes: BTreeSet<ApplicationScope>,
        /// Optional absolute expiry.
        expires_at: Option<TimestampMicros>,
    },
    /// List safe credential metadata under one client.
    KeyList {
        /// Initialized project root.
        root: PathBuf,
        /// Owning Application Client.
        client_id: ApplicationClientId,
    },
    /// Re-derive and reveal a publishable key.
    KeyReveal {
        /// Initialized project root.
        root: PathBuf,
        /// Owning Application Client.
        client_id: ApplicationClientId,
        /// Publishable credential identifier.
        credential_id: CredentialId,
    },
    /// Create a replacement with the exact scopes of an existing key.
    KeyRotate {
        /// Initialized project root.
        root: PathBuf,
        /// Owning Application Client.
        client_id: ApplicationClientId,
        /// Existing source credential; it remains unchanged.
        source_id: CredentialId,
        /// Optional replacement ID for reconciliation.
        replacement_id: Option<CredentialId>,
        /// New key label.
        label: CredentialLabel,
        /// Optional absolute expiry for the replacement.
        expires_at: Option<TimestampMicros>,
    },
    /// Irreversibly revoke one key.
    KeyRevoke {
        /// Initialized project root.
        root: PathBuf,
        /// Credential to revoke.
        credential_id: CredentialId,
    },
    /// Tombstone one already-revoked key.
    KeyDelete {
        /// Initialized project root.
        root: PathBuf,
        /// Credential to tombstone.
        credential_id: CredentialId,
    },
    /// Create one actor-bound Development Access key.
    WorkspaceKeyCreate {
        /// Initialized project root.
        root: PathBuf,
        /// Optional caller-selected ID for conflict reconciliation.
        id: Option<DevelopmentCredentialId>,
        /// Trusted audit actor bound by the server to this credential.
        actor: DevelopmentActor,
        /// Operator-facing key usage label.
        label: DevelopmentCredentialLabel,
        /// Optional absolute expiry.
        expires_at: Option<TimestampMicros>,
    },
    /// List safe Development Access metadata.
    WorkspaceKeyList {
        /// Initialized project root.
        root: PathBuf,
    },
    /// Create an overlapping replacement preserving the source actor.
    WorkspaceKeyRotate {
        /// Initialized project root.
        root: PathBuf,
        /// Existing non-deleted source credential.
        source_id: DevelopmentCredentialId,
        /// Optional replacement ID for conflict reconciliation.
        replacement_id: Option<DevelopmentCredentialId>,
        /// Replacement usage label.
        label: DevelopmentCredentialLabel,
        /// Optional absolute expiry for the replacement.
        expires_at: Option<TimestampMicros>,
    },
    /// Irreversibly revoke one Development Access key.
    WorkspaceKeyRevoke {
        /// Initialized project root.
        root: PathBuf,
        /// Credential to revoke.
        credential_id: DevelopmentCredentialId,
    },
    /// Tombstone one already-revoked Development Access key.
    WorkspaceKeyDelete {
        /// Initialized project root.
        root: PathBuf,
        /// Credential to tombstone.
        credential_id: DevelopmentCredentialId,
    },
    /// Build and CAS-publish one source snapshot to a remote Workspace.
    WorkspaceSync {
        /// Existing source project directory; it need not be locally initialized.
        root: PathBuf,
        /// Descriptor path relative to the project root.
        config: PathBuf,
        /// Canonical HTTPS or literal-loopback HTTP administrative origin.
        endpoint: DevelopmentEndpoint,
        /// Exact mutable remote Workspace reference.
        workspace: WorkspaceRef,
        /// Allowlisted environment-variable name containing the Development key.
        token_environment: TokenEnvironmentName,
        /// Optional exact CAS precondition. `Some(None)` requires an empty Workspace.
        expected_head: Option<Option<DevRevisionId>>,
        /// Whether an absent Workspace may be created.
        create: bool,
    },
    /// Validate and explicitly make one remote candidate Release servable.
    WorkspaceFreeze {
        /// Canonical HTTPS or literal-loopback HTTP administrative origin.
        endpoint: DevelopmentEndpoint,
        /// Candidate Release returned by Workspace publish.
        release_id: ReleaseId,
        /// Optional exact already-servable compatibility baseline.
        against_release_id: Option<ReleaseId>,
        /// Allowlisted environment-variable name containing the Development key.
        token_environment: TokenEnvironmentName,
    },
}

/// Allowlisted environment-variable name from which remote Development auth may be read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenEnvironmentName(String);

impl TokenEnvironmentName {
    /// Returns the exact non-secret variable name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for TokenEnvironmentName {
    type Err = CliUsageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let suffix = value.strip_prefix("RUNKU_").ok_or(CliUsageError)?;
        if value.len() > 64
            || suffix.is_empty()
            || !suffix
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_uppercase())
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(CliUsageError);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Deterministic invalid command-line shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CliUsageError;

/// Parses arguments after the executable name using an exact flag grammar.
///
/// # Errors
///
/// Rejects absent commands/values, non-Unicode flag names/typed values, positional arguments,
/// duplicate singleton flags, unknown flags, non-loopback listeners, and invalid IDs/names.
pub fn parse_args<I>(args: I) -> Result<CliCommand, CliUsageError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let command = args.next().ok_or(CliUsageError)?;
    let command = command.to_str().ok_or(CliUsageError)?;
    if matches!(command, "--help" | "-h" | "help") {
        return no_more(args, CliCommand::Help);
    }
    if matches!(command, "--version" | "-V" | "version") {
        return no_more(args, CliCommand::Version);
    }
    let remaining: Vec<OsString> = args.collect();
    if remaining.len() == 1 && matches!(remaining[0].to_str(), Some("--help" | "-h")) {
        return Ok(CliCommand::Help);
    }
    match command {
        "init" => parse_init(remaining),
        "login" => parse_login(remaining),
        "build" => parse_build(remaining),
        "publish" => parse_publish(remaining),
        "release" => parse_release(remaining),
        "promote" => parse_promote(remaining),
        "rollback" => parse_rollback(remaining),
        "status" => parse_status(remaining),
        "dev" => parse_dev(remaining),
        "doctor" => parse_doctor(remaining),
        "logs" => parse_logs(remaining),
        "client" => parse_client(remaining),
        "key" => parse_key(remaining),
        "workspace" => parse_workspace(remaining),
        _ => Err(CliUsageError),
    }
}

fn parse_login(args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let mut args = args;
    let browser = take_switch(&mut args, "--browser")?;
    let no_open = take_switch(&mut args, "--no-open")?;
    if no_open && !browser {
        return Err(CliUsageError);
    }
    let mut flags = Flags::new(args)?;
    let code_environment = parse_optional(&mut flags, "--code-env")?;
    let oidc_token_environment = parse_optional(&mut flags, "--oidc-token-env")?;
    if browser && oidc_token_environment.is_some() {
        return Err(CliUsageError);
    }
    let command = CliCommand::Login {
        endpoint: parse_optional(&mut flags, "--url")?,
        device_name: parse_optional(&mut flags, "--device")?,
        code_environment,
        oidc_token_environment,
        browser,
        no_open,
    };
    flags.finish()?;
    Ok(command)
}

fn parse_release(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let remote = take_switch(&mut args, "--remote")?;
    let mut flags = Flags::new(args)?;
    let command = CliCommand::Release {
        remote,
        root: flags.project_root()?,
        release_id: parse_required(&mut flags, "--release")?,
        against: parse_optional(&mut flags, "--against")?,
    };
    flags.finish()?;
    Ok(command)
}

fn parse_promote(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let remote = take_switch(&mut args, "--remote")?;
    let mut flags = Flags::new(args)?;
    let expected = flags
        .optional_string("--expected")?
        .map(|value| {
            if value == "empty" {
                Ok(None)
            } else {
                value
                    .parse::<ReleaseId>()
                    .map(Some)
                    .map_err(|_| CliUsageError)
            }
        })
        .transpose()?;
    let command = CliCommand::Promote {
        remote,
        root: flags.project_root()?,
        channel: parse_required(&mut flags, "--channel")?,
        release_id: parse_required(&mut flags, "--release")?,
        expected,
    };
    flags.finish()?;
    Ok(command)
}

fn parse_rollback(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let remote = take_switch(&mut args, "--remote")?;
    let mut flags = Flags::new(args)?;
    let command = CliCommand::Rollback {
        remote,
        root: flags.project_root()?,
        channel: parse_required(&mut flags, "--channel")?,
        expected: parse_required(&mut flags, "--expected")?,
        target: parse_required(&mut flags, "--to")?,
    };
    flags.finish()?;
    Ok(command)
}

fn parse_status(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let remote = take_switch(&mut args, "--remote")?;
    let mut flags = Flags::new(args)?;
    let command = CliCommand::Status {
        remote,
        root: flags.project_root()?,
    };
    flags.finish()?;
    Ok(command)
}

fn parse_build(args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let config = PathBuf::from("runku");
    let release_id = flags
        .optional_string("--release-id")?
        .map(|value| value.parse::<ReleaseId>().map_err(|_| CliUsageError))
        .transpose()?;
    let build_id = flags
        .optional_string("--build-id")?
        .map(|value| value.parse::<BuildId>().map_err(|_| CliUsageError))
        .transpose()?;
    let created_at = flags
        .optional_string("--created-at-micros")?
        .map(|value| {
            let parsed = value.parse::<i64>().map_err(|_| CliUsageError)?;
            if parsed < 0 || parsed.to_string() != value {
                return Err(CliUsageError);
            }
            Ok(TimestampMicros::new(parsed))
        })
        .transpose()?;
    let metadata = match (release_id, build_id, created_at) {
        (None, None, None) => None,
        (Some(release_id), Some(build_id), Some(created_at)) => Some(BuildMetadata {
            release_id,
            build_id,
            created_at,
        }),
        _ => return Err(CliUsageError),
    };
    flags.finish()?;
    Ok(CliCommand::Build {
        root,
        config,
        metadata,
    })
}

fn parse_init(args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let workspace = flags
        .optional_string("--workspace")?
        .unwrap_or_else(|| DEFAULT_LOCAL_WORKSPACE.to_owned())
        .parse()
        .map_err(|_| CliUsageError)?;
    let listen = flags
        .optional_string("--listen")?
        .unwrap_or_else(|| DEFAULT_LOCAL_LISTENER.to_owned())
        .parse::<SocketAddr>()
        .map_err(|_| CliUsageError)?;
    if !listen.ip().is_loopback() {
        return Err(CliUsageError);
    }
    let project_id = parse_optional::<ProjectId>(&mut flags, "--project-id")?;
    let environment_id = parse_optional::<EnvironmentId>(&mut flags, "--environment-id")?;
    let scope = match (project_id, environment_id) {
        (None, None) => None,
        (Some(project_id), Some(environment_id)) => {
            Some(EnvironmentScope::new(project_id, environment_id))
        }
        _ => return Err(CliUsageError),
    };
    flags.finish()?;
    Ok(CliCommand::Init {
        root,
        workspace,
        listen,
        scope,
    })
}

fn parse_publish(args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let mut args = args;
    let remote = take_switch(&mut args, "--remote")?;
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let manifest = flags.required_path("--manifest")?;
    let artifact = flags.required_path("--artifact")?;
    let workspace = flags
        .optional_string("--workspace")?
        .map(|value| WorkspaceRef::from_str(&value).map_err(|_| CliUsageError))
        .transpose()?;
    let actor = flags
        .optional_string("--actor")?
        .unwrap_or_else(|| "local-cli".to_owned())
        .parse()
        .map_err(|_| CliUsageError)?;
    let expected_head = flags
        .optional_string("--expected-head")?
        .map(|value| {
            if value == "empty" {
                Ok(None)
            } else {
                value.parse().map(Some).map_err(|_| CliUsageError)
            }
        })
        .transpose()?;
    flags.finish()?;
    Ok(CliCommand::Publish {
        remote,
        root,
        manifest,
        artifact,
        workspace,
        actor,
        expected_head,
    })
}

fn parse_dev(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let prebuilt = take_switch(&mut args, "--prebuilt")?;
    let prepare = take_switch(&mut args, "--prepare")?;
    let replace_remote_credentials = take_switch(&mut args, "--replace-remote-credentials")?;
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let origin_values = flags.repeated_strings("--origin")?;
    let origins = origin_values
        .iter()
        .map(|value| value.parse().map_err(|_| CliUsageError))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if origins.len() != origin_values.len() {
        return Err(CliUsageError);
    }
    let watch_config = (!prebuilt).then(|| PathBuf::from("runku"));
    let auth_config = flags.optional_string("--auth-config")?.map(PathBuf::from);
    let application_env = flags
        .optional_string("--application-env")?
        .map_or_else(|| PathBuf::from(".env.local"), PathBuf::from);
    let public_env_prefix = flags
        .optional_string("--public-env-prefix")?
        .map(|prefix| {
            if prefix.len() > 48
                || prefix.is_empty()
                || !prefix.ends_with('_')
                || !prefix
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            {
                Err(CliUsageError)
            } else {
                Ok(prefix)
            }
        })
        .transpose()?;
    flags.finish()?;
    Ok(CliCommand::Dev {
        root,
        origins,
        watch_config,
        auth_config,
        application_env,
        public_env_prefix,
        prepare,
        replace_remote_credentials,
    })
}

fn parse_doctor(args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    flags.finish()?;
    Ok(CliCommand::Doctor { root })
}

fn parse_logs(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    if matches!(
        args.first().and_then(|value| value.to_str()),
        Some("archive-status")
    ) {
        args.remove(0);
        let remote = take_switch(&mut args, "--remote")?;
        let mut flags = Flags::new(args)?;
        let command = CliCommand::LogsArchiveStatus {
            remote,
            root: flags.project_root()?,
        };
        flags.finish()?;
        return Ok(command);
    }
    if matches!(
        args.first().and_then(|value| value.to_str()),
        Some("export-otlp")
    ) {
        args.remove(0);
        let once = take_switch(&mut args, "--once")?;
        let mut flags = Flags::new(args)?;
        let command = CliCommand::LogsExportOtlp {
            root: flags.project_root()?,
            config: flags.required_path("--config")?,
            once,
        };
        flags.finish()?;
        return Ok(command);
    }
    if matches!(args.first().and_then(|value| value.to_str()), Some("prune")) {
        args.remove(0);
        return parse_logs_prune(args);
    }
    let follow = take_switch(&mut args, "--follow")?;
    let remote = take_switch(&mut args, "--remote")?;
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let after = parse_optional(&mut flags, "--after")?.unwrap_or(LogCursor::START);
    let limit = flags
        .optional_string("--limit")?
        .map_or(Ok(100_u16), |value| canonical_integer(&value))?;
    if !(1..=1_000).contains(&limit) {
        return Err(CliUsageError);
    }
    let stream = flags
        .optional_string("--stream")?
        .map(|value| match value.as_str() {
            "platform" => Ok(LogStream::Platform),
            "function" => Ok(LogStream::Function),
            _ => Err(CliUsageError),
        })
        .transpose()?;
    let minimum_level = flags
        .optional_string("--level")?
        .map(|value| match value.as_str() {
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warn" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            _ => Err(CliUsageError),
        })
        .transpose()?;
    let command = CliCommand::Logs {
        remote,
        root,
        after,
        limit,
        stream,
        minimum_level,
        function_id: parse_optional(&mut flags, "--function")?,
        request_id: parse_optional(&mut flags, "--request")?,
        invocation_id: parse_optional(&mut flags, "--invocation")?,
        client_id: parse_optional(&mut flags, "--client")?,
        credential_id: parse_optional(&mut flags, "--credential")?,
        release_id: parse_optional(&mut flags, "--release")?,
        follow,
    };
    flags.finish()?;
    Ok(command)
}

fn parse_logs_prune(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let apply = take_switch(&mut args, "--apply")?;
    let remote = take_switch(&mut args, "--remote")?;
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let before = parse_required_timestamp(&mut flags, "--before-micros")?;
    let maximum = flags
        .optional_string("--maximum")?
        .map_or(Ok(10_000_u32), |value| canonical_integer(&value))?;
    if !(1..=10_000).contains(&maximum) {
        return Err(CliUsageError);
    }
    let environment = parse_optional(&mut flags, "--environment")?;
    if apply && environment.is_none() || !apply && environment.is_some() {
        return Err(CliUsageError);
    }
    flags.finish()?;
    Ok(CliCommand::LogsPrune {
        remote,
        root,
        before,
        maximum,
        apply,
        environment,
    })
}

fn parse_client(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    if args.is_empty() {
        return Err(CliUsageError);
    }
    let subcommand = args.remove(0).into_string().map_err(|_| CliUsageError)?;
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let command = match subcommand.as_str() {
        "create" => {
            let id = parse_optional(&mut flags, "--client-id")?;
            let name = parse_required(&mut flags, "--name")?;
            let kind = match flags.required_string("--kind")?.as_str() {
                "public" => ClientKind::Public,
                "confidential" => ClientKind::Confidential,
                _ => return Err(CliUsageError),
            };
            let scopes = parse_scopes(&mut flags)?;
            CliCommand::ClientCreate {
                root,
                id,
                name,
                kind,
                scopes,
            }
        }
        "list" => CliCommand::ClientList { root },
        _ => return Err(CliUsageError),
    };
    flags.finish()?;
    Ok(command)
}

fn parse_key(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    if args.is_empty() {
        return Err(CliUsageError);
    }
    let subcommand = args.remove(0).into_string().map_err(|_| CliUsageError)?;
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let command = match subcommand.as_str() {
        "create" => CliCommand::KeyCreate {
            root,
            id: parse_optional(&mut flags, "--key-id")?,
            client_id: parse_required(&mut flags, "--client")?,
            label: parse_required(&mut flags, "--label")?,
            scopes: parse_scopes(&mut flags)?,
            expires_at: parse_optional_timestamp(&mut flags, "--expires-at-micros")?,
        },
        "list" => CliCommand::KeyList {
            root,
            client_id: parse_required(&mut flags, "--client")?,
        },
        "reveal" => CliCommand::KeyReveal {
            root,
            client_id: parse_required(&mut flags, "--client")?,
            credential_id: parse_required(&mut flags, "--key")?,
        },
        "rotate" => CliCommand::KeyRotate {
            root,
            client_id: parse_required(&mut flags, "--client")?,
            source_id: parse_required(&mut flags, "--key")?,
            replacement_id: parse_optional(&mut flags, "--new-key-id")?,
            label: parse_required(&mut flags, "--label")?,
            expires_at: parse_optional_timestamp(&mut flags, "--expires-at-micros")?,
        },
        "revoke" => CliCommand::KeyRevoke {
            root,
            credential_id: parse_required(&mut flags, "--key")?,
        },
        "delete" => CliCommand::KeyDelete {
            root,
            credential_id: parse_required(&mut flags, "--key")?,
        },
        _ => return Err(CliUsageError),
    };
    flags.finish()?;
    Ok(command)
}

fn parse_workspace(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    match args.first().and_then(|value| value.to_str()) {
        Some("key") => {
            args.remove(0);
            parse_workspace_key(args)
        }
        Some("sync") => {
            args.remove(0);
            parse_workspace_sync(args)
        }
        Some("freeze") => {
            args.remove(0);
            parse_workspace_freeze(args)
        }
        _ => Err(CliUsageError),
    }
}

fn parse_workspace_freeze(args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let mut flags = Flags::new(args)?;
    let command = CliCommand::WorkspaceFreeze {
        endpoint: parse_required(&mut flags, "--url")?,
        release_id: parse_required(&mut flags, "--release")?,
        against_release_id: parse_optional(&mut flags, "--against")?,
        token_environment: parse_required(&mut flags, "--token-env")?,
    };
    flags.finish()?;
    Ok(command)
}

fn parse_workspace_sync(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    let create = take_switch(&mut args, "--create")?;
    let mut flags = Flags::new(args)?;
    let expected_head = flags
        .optional_string("--expected-head")?
        .map(|value| {
            if value == "empty" {
                Ok(None)
            } else {
                value
                    .parse::<DevRevisionId>()
                    .map(Some)
                    .map_err(|_| CliUsageError)
            }
        })
        .transpose()?;
    let command = CliCommand::WorkspaceSync {
        root: flags.project_root()?,
        config: PathBuf::from("runku"),
        endpoint: parse_required(&mut flags, "--url")?,
        workspace: parse_required(&mut flags, "--workspace")?,
        token_environment: parse_required(&mut flags, "--token-env")?,
        expected_head,
        create,
    };
    flags.finish()?;
    Ok(command)
}

fn parse_workspace_key(mut args: Vec<OsString>) -> Result<CliCommand, CliUsageError> {
    if args.is_empty() {
        return Err(CliUsageError);
    }
    let subcommand = args.remove(0).into_string().map_err(|_| CliUsageError)?;
    let mut flags = Flags::new(args)?;
    let root = flags.project_root()?;
    let command = match subcommand.as_str() {
        "create" => CliCommand::WorkspaceKeyCreate {
            root,
            id: parse_optional(&mut flags, "--key-id")?,
            actor: parse_required(&mut flags, "--actor")?,
            label: parse_required(&mut flags, "--label")?,
            expires_at: parse_optional_timestamp(&mut flags, "--expires-at-micros")?,
        },
        "list" => CliCommand::WorkspaceKeyList { root },
        "rotate" => CliCommand::WorkspaceKeyRotate {
            root,
            source_id: parse_required(&mut flags, "--key")?,
            replacement_id: parse_optional(&mut flags, "--new-key-id")?,
            label: parse_required(&mut flags, "--label")?,
            expires_at: parse_optional_timestamp(&mut flags, "--expires-at-micros")?,
        },
        "revoke" => CliCommand::WorkspaceKeyRevoke {
            root,
            credential_id: parse_required(&mut flags, "--key")?,
        },
        "delete" => CliCommand::WorkspaceKeyDelete {
            root,
            credential_id: parse_required(&mut flags, "--key")?,
        },
        _ => return Err(CliUsageError),
    };
    flags.finish()?;
    Ok(command)
}

fn parse_required<T>(flags: &mut Flags, name: &str) -> Result<T, CliUsageError>
where
    T: FromStr,
{
    flags
        .required_string(name)?
        .parse()
        .map_err(|_| CliUsageError)
}

fn parse_optional<T>(flags: &mut Flags, name: &str) -> Result<Option<T>, CliUsageError>
where
    T: FromStr,
{
    flags
        .optional_string(name)?
        .map(|value| value.parse().map_err(|_| CliUsageError))
        .transpose()
}

fn parse_optional_timestamp(
    flags: &mut Flags,
    name: &str,
) -> Result<Option<TimestampMicros>, CliUsageError> {
    flags
        .optional_string(name)?
        .map(|value| {
            let parsed = value.parse::<i64>().map_err(|_| CliUsageError)?;
            if parsed < 0 || parsed.to_string() != value {
                return Err(CliUsageError);
            }
            Ok(TimestampMicros::new(parsed))
        })
        .transpose()
}

fn parse_required_timestamp(
    flags: &mut Flags,
    name: &str,
) -> Result<TimestampMicros, CliUsageError> {
    let value = flags.required_string(name)?;
    let parsed = canonical_integer::<i64>(&value)?;
    if parsed < 0 {
        return Err(CliUsageError);
    }
    Ok(TimestampMicros::new(parsed))
}

fn canonical_integer<T>(value: &str) -> Result<T, CliUsageError>
where
    T: FromStr + ToString,
{
    let parsed = value.parse::<T>().map_err(|_| CliUsageError)?;
    if parsed.to_string() != value {
        return Err(CliUsageError);
    }
    Ok(parsed)
}

fn take_switch(args: &mut Vec<OsString>, name: &str) -> Result<bool, CliUsageError> {
    let positions = args
        .iter()
        .enumerate()
        .filter_map(|(index, value)| (value.to_str() == Some(name)).then_some(index))
        .collect::<Vec<_>>();
    if positions.len() > 1 {
        return Err(CliUsageError);
    }
    if let Some(index) = positions.first() {
        args.remove(*index);
        Ok(true)
    } else {
        Ok(false)
    }
}

fn parse_scopes(flags: &mut Flags) -> Result<BTreeSet<ApplicationScope>, CliUsageError> {
    let values = flags.repeated_strings("--scope")?;
    let scopes = values
        .iter()
        .map(|value| value.parse().map_err(|_| CliUsageError))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if scopes.is_empty() || scopes.len() != values.len() {
        return Err(CliUsageError);
    }
    Ok(scopes)
}

fn no_more<I>(mut args: I, command: CliCommand) -> Result<CliCommand, CliUsageError>
where
    I: Iterator<Item = OsString>,
{
    if args.next().is_some() {
        Err(CliUsageError)
    } else {
        Ok(command)
    }
}

struct Flags {
    values: Vec<(String, OsString)>,
}

impl Flags {
    fn new(args: Vec<OsString>) -> Result<Self, CliUsageError> {
        let mut values = Vec::new();
        let mut iterator = args.into_iter();
        while let Some(flag) = iterator.next() {
            let flag = flag.to_str().ok_or(CliUsageError)?;
            if !flag.starts_with("--") {
                return Err(CliUsageError);
            }
            let value = iterator.next().ok_or(CliUsageError)?;
            if value.is_empty() || value.to_str().is_some_and(|value| value.starts_with("--")) {
                return Err(CliUsageError);
            }
            values.push((flag.to_owned(), value));
        }
        Ok(Self { values })
    }

    fn required_path(&mut self, flag: &str) -> Result<PathBuf, CliUsageError> {
        self.take_one(flag)?.map(PathBuf::from).ok_or(CliUsageError)
    }

    fn project_root(&mut self) -> Result<PathBuf, CliUsageError> {
        Ok(self
            .take_one("--root")?
            .map_or_else(|| PathBuf::from("."), PathBuf::from))
    }

    fn required_string(&mut self, flag: &str) -> Result<String, CliUsageError> {
        self.optional_string(flag)?.ok_or(CliUsageError)
    }

    fn optional_string(&mut self, flag: &str) -> Result<Option<String>, CliUsageError> {
        self.take_one(flag)?
            .map(|value| value.into_string().map_err(|_| CliUsageError))
            .transpose()
    }

    fn repeated_strings(&mut self, flag: &str) -> Result<Vec<String>, CliUsageError> {
        let mut found = Vec::new();
        let mut retained = Vec::new();
        for (candidate, value) in self.values.drain(..) {
            if candidate == flag {
                found.push(value.into_string().map_err(|_| CliUsageError)?);
            } else {
                retained.push((candidate, value));
            }
        }
        self.values = retained;
        Ok(found)
    }

    fn take_one(&mut self, flag: &str) -> Result<Option<OsString>, CliUsageError> {
        let matches: Vec<usize> = self
            .values
            .iter()
            .enumerate()
            .filter_map(|(index, (candidate, _))| (candidate == flag).then_some(index))
            .collect();
        if matches.len() > 1 {
            return Err(CliUsageError);
        }
        Ok(matches.first().map(|index| self.values.remove(*index).1))
    }

    fn finish(self) -> Result<(), CliUsageError> {
        if self.values.is_empty() {
            Ok(())
        } else {
            Err(CliUsageError)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        ffi::OsString,
        path::{Path, PathBuf},
    };

    use super::{CliCommand, parse_args};

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn project_commands_default_root_to_current_directory() {
        let current = PathBuf::from(".");
        assert!(matches!(
            parse_args(args(&["init"])),
            Ok(CliCommand::Init { root, workspace, listen, scope })
                if root == current
                    && workspace.as_str() == "local"
                    && listen.to_string() == "127.0.0.1:3210"
                    && scope.is_none()
        ));
        assert!(matches!(
            parse_args(args(&["build"])),
            Ok(CliCommand::Build { root, .. }) if root == current
        ));
        assert!(matches!(
            parse_args(args(&["dev"])),
            Ok(CliCommand::Dev { root, watch_config: Some(_), .. }) if root == current
        ));
        assert!(matches!(
            parse_args(args(&["dev", "--prebuilt"])),
            Ok(CliCommand::Dev {
                watch_config: None,
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "login",
                "--url",
                "https://runku.example.com",
                "--device",
                "manuel-laptop",
                "--browser",
                "--no-open",
            ])),
            Ok(CliCommand::Login {
                browser: true,
                no_open: true,
                code_environment: None,
                oidc_token_environment: None,
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&["status", "--root", "/tmp/project", "--remote"])),
            Ok(CliCommand::Status { remote: true, .. })
        ));
        assert!(matches!(
            parse_args(args(&["doctor"])),
            Ok(CliCommand::Doctor { root }) if root == current
        ));
        assert!(matches!(
            parse_args(args(&["logs"])),
            Ok(CliCommand::Logs { root, .. }) if root == current
        ));
        assert!(matches!(
            parse_args(args(&["client", "list"])),
            Ok(CliCommand::ClientList { root }) if root == current
        ));
        assert!(matches!(
            parse_args(args(&["workspace", "key", "list"])),
            Ok(CliCommand::WorkspaceKeyList { root }) if root == current
        ));
        assert!(matches!(
            parse_args(args(&["status", "--root", "/tmp/project"])),
            Ok(CliCommand::Status { root, remote: false }) if root.as_path() == Path::new("/tmp/project")
        ));
    }

    #[test]
    fn parses_remote_login_without_accepting_code_as_an_argument() {
        assert!(matches!(
            parse_args(args(&[
                "login",
                "--url",
                "https://runku.example.com",
                "--device",
                "manuel-laptop",
                "--code-env",
                "RUNKU_INVITATION_CODE",
            ])),
            Ok(CliCommand::Login { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "login",
                "--url",
                "https://runku.example.com",
                "--device",
                "manuel-laptop",
                "--oidc-token-env",
                "RUNKU_OIDC_TOKEN",
            ])),
            Ok(CliCommand::Login {
                code_environment: None,
                oidc_token_environment: Some(_),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "login",
                "--url",
                "https://runku.example.com",
                "--device",
                "manuel-laptop",
            ])),
            Ok(CliCommand::Login {
                endpoint: Some(_),
                device_name: Some(_),
                browser: false,
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&["login"])),
            Ok(CliCommand::Login {
                endpoint: None,
                device_name: None,
                browser: false,
                ..
            })
        ));
        assert!(
            parse_args(args(&[
                "login",
                "--url",
                "https://runku.example.com",
                "--device",
                "manuel-laptop",
                "--code",
                "secret",
            ]))
            .is_err()
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn parses_every_command_and_repeated_origins() {
        assert!(matches!(
            parse_args(args(&["build", "--root", "/tmp/project"])),
            Ok(CliCommand::Build { metadata: None, .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "build",
                "--root",
                "/tmp/project",
                "--release-id",
                "rel_00000000000000000000000001",
                "--build-id",
                "bld_00000000000000000000000002",
                "--created-at-micros",
                "1800000000000000"
            ])),
            Ok(CliCommand::Build {
                metadata: Some(_),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "init",
                "--root",
                "/tmp/project",
                "--workspace",
                "team/default",
                "--listen",
                "127.0.0.1:3210"
            ])),
            Ok(CliCommand::Init { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "init",
                "--root",
                "/tmp/project",
                "--project-id",
                "prj_00000000000000000000000001",
                "--environment-id",
                "env_00000000000000000000000002"
            ])),
            Ok(CliCommand::Init { scope: Some(_), .. })
        ));
        assert!(
            parse_args(args(&[
                "init",
                "--project-id",
                "prj_00000000000000000000000001"
            ]))
            .is_err()
        );
        assert!(matches!(
            parse_args(args(&[
                "publish",
                "--root",
                "/tmp/project",
                "--manifest",
                "manifest.bin",
                "--artifact",
                "artifact.bin",
                "--expected-head",
                "empty"
            ])),
            Ok(CliCommand::Publish {
                expected_head: Some(None),
                ..
            })
        ));
        let parsed = parse_args(args(&[
            "dev",
            "--root",
            "/tmp/project",
            "--origin",
            "http://localhost:3000",
            "--origin",
            "https://app.example",
        ]));
        assert!(matches!(
            &parsed,
            Ok(CliCommand::Dev { origins, .. }) if origins.len() == 2
        ));
        let origins = match parsed {
            Ok(CliCommand::Dev { origins, .. }) => origins,
            _ => BTreeSet::new(),
        };
        assert_eq!(origins.len(), 2);
        assert!(matches!(
            parse_args(args(&[
                "dev",
                "--root",
                "/tmp/project",
                "--auth-config",
                "runku.auth.json",
            ])),
            Ok(CliCommand::Dev {
                watch_config: Some(_),
                auth_config: Some(_),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&["doctor", "--root", "/tmp/project"])),
            Ok(CliCommand::Doctor { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "logs",
                "archive-status",
                "--root",
                "/tmp/project",
                "--remote",
            ])),
            Ok(CliCommand::LogsArchiveStatus { remote: true, .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "logs",
                "--root",
                "/tmp/project",
                "--after",
                "logc_42",
                "--limit",
                "250",
                "--stream",
                "function",
                "--level",
                "warn",
                "--function",
                "fnc_00000000000000000000000001",
                "--request",
                "req_00000000000000000000000002",
                "--invocation",
                "inv_00000000000000000000000003",
                "--client",
                "app_00000000000000000000000004",
                "--credential",
                "crd_00000000000000000000000005",
                "--release",
                "rel_00000000000000000000000006",
                "--follow",
            ])),
            Ok(CliCommand::Logs {
                limit: 250,
                follow: true,
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "logs",
                "prune",
                "--root",
                "/tmp/project",
                "--before-micros",
                "1800000000000000",
                "--remote",
            ])),
            Ok(CliCommand::LogsPrune {
                remote: true,
                maximum: 10_000,
                apply: false,
                environment: None,
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "logs",
                "export-otlp",
                "--root",
                "/tmp/project",
                "--config",
                "otel.json",
                "--once",
            ])),
            Ok(CliCommand::LogsExportOtlp { once: true, .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "logs",
                "prune",
                "--root",
                "/tmp/project",
                "--before-micros",
                "1800000000000000",
                "--maximum",
                "7",
                "--apply",
                "--environment",
                "env_00000000000000000000000001",
            ])),
            Ok(CliCommand::LogsPrune {
                remote: false,
                maximum: 7,
                apply: true,
                environment: Some(_),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "release",
                "--root",
                "/tmp/project",
                "--release",
                "rel_00000000000000000000000001",
                "--against",
                "stable",
            ])),
            Ok(CliCommand::Release {
                against: Some(_),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "promote",
                "--root",
                "/tmp/project",
                "--channel",
                "stable",
                "--release",
                "rel_00000000000000000000000001",
                "--expected",
                "empty",
            ])),
            Ok(CliCommand::Promote {
                expected: Some(None),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "rollback",
                "--root",
                "/tmp/project",
                "--channel",
                "stable",
                "--expected",
                "rel_00000000000000000000000002",
                "--to",
                "rel_00000000000000000000000001",
            ])),
            Ok(CliCommand::Rollback { .. })
        ));
        assert!(matches!(
            parse_args(args(&["status", "--root", "/tmp/project"])),
            Ok(CliCommand::Status { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "client",
                "create",
                "--root",
                "/tmp/project",
                "--name",
                "web",
                "--kind",
                "public",
                "--scope",
                "documents:read",
                "--scope",
                "documents:write",
            ])),
            Ok(CliCommand::ClientCreate { scopes, .. }) if scopes.len() == 2
        ));
        assert!(matches!(
            parse_args(args(&["client", "list", "--root", "/tmp/project"])),
            Ok(CliCommand::ClientList { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "key",
                "create",
                "--root",
                "/tmp/project",
                "--client",
                "app_00000000000000000000000001",
                "--label",
                "primary",
                "--scope",
                "documents:read",
                "--expires-at-micros",
                "1900000000000000",
            ])),
            Ok(CliCommand::KeyCreate {
                expires_at: Some(_),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "key",
                "list",
                "--root",
                "/tmp/project",
                "--client",
                "app_00000000000000000000000001",
            ])),
            Ok(CliCommand::KeyList { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "key",
                "reveal",
                "--root",
                "/tmp/project",
                "--client",
                "app_00000000000000000000000001",
                "--key",
                "crd_00000000000000000000000002",
            ])),
            Ok(CliCommand::KeyReveal { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "key",
                "rotate",
                "--root",
                "/tmp/project",
                "--client",
                "app_00000000000000000000000001",
                "--key",
                "crd_00000000000000000000000002",
                "--label",
                "replacement",
            ])),
            Ok(CliCommand::KeyRotate { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "key",
                "revoke",
                "--root",
                "/tmp/project",
                "--key",
                "crd_00000000000000000000000002",
            ])),
            Ok(CliCommand::KeyRevoke { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "key",
                "delete",
                "--root",
                "/tmp/project",
                "--key",
                "crd_00000000000000000000000002",
            ])),
            Ok(CliCommand::KeyDelete { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "workspace",
                "key",
                "create",
                "--root",
                "/tmp/project",
                "--actor",
                "manuel",
                "--label",
                "laptop",
                "--expires-at-micros",
                "1900000000000000",
            ])),
            Ok(CliCommand::WorkspaceKeyCreate {
                expires_at: Some(_),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "workspace",
                "key",
                "list",
                "--root",
                "/tmp/project",
            ])),
            Ok(CliCommand::WorkspaceKeyList { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "workspace",
                "key",
                "rotate",
                "--root",
                "/tmp/project",
                "--key",
                "dvk_00000000000000000000000001",
                "--new-key-id",
                "dvk_00000000000000000000000002",
                "--label",
                "replacement",
            ])),
            Ok(CliCommand::WorkspaceKeyRotate {
                replacement_id: Some(_),
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "workspace",
                "key",
                "revoke",
                "--root",
                "/tmp/project",
                "--key",
                "dvk_00000000000000000000000001",
            ])),
            Ok(CliCommand::WorkspaceKeyRevoke { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "workspace",
                "key",
                "delete",
                "--root",
                "/tmp/project",
                "--key",
                "dvk_00000000000000000000000001",
            ])),
            Ok(CliCommand::WorkspaceKeyDelete { .. })
        ));
        assert!(matches!(
            parse_args(args(&[
                "workspace",
                "sync",
                "--root",
                "/tmp/project",
                "--url",
                "https://dev.example.com",
                "--workspace",
                "manuel/fix-42",
                "--token-env",
                "RUNKU_PREVIEW_KEY",
                "--expected-head",
                "empty",
                "--create",
            ])),
            Ok(CliCommand::WorkspaceSync {
                expected_head: Some(None),
                create: true,
                ..
            })
        ));
        assert!(matches!(
            parse_args(args(&[
                "workspace",
                "freeze",
                "--url",
                "https://dev.example.com",
                "--release",
                "rel_00000000000000000000000001",
                "--against",
                "rel_00000000000000000000000002",
                "--token-env",
                "RUNKU_PREVIEW_KEY",
            ])),
            Ok(CliCommand::WorkspaceFreeze {
                against_release_id: Some(_),
                ..
            })
        ));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn rejects_unknown_duplicate_missing_positional_and_non_loopback_values() {
        for invalid in [
            args(&[
                "init",
                "--root",
                "/tmp/x",
                "--root",
                "/tmp/y",
                "--workspace",
                "default",
                "--listen",
                "127.0.0.1:1",
            ]),
            args(&["doctor", "/tmp/x"]),
            args(&["doctor", "--root", "/tmp/x", "--unknown", "x"]),
            args(&[
                "build",
                "--root",
                "/tmp/x",
                "--release-id",
                "rel_00000000000000000000000001",
            ]),
            args(&[
                "build",
                "--root",
                "/tmp/x",
                "--release-id",
                "rel_00000000000000000000000001",
                "--build-id",
                "bld_00000000000000000000000002",
                "--created-at-micros",
                "01",
            ]),
            args(&[
                "init",
                "--root",
                "/tmp/x",
                "--workspace",
                "default",
                "--listen",
                "0.0.0.0:3210",
            ]),
            args(&["client", "--root", "/tmp/x"]),
            args(&[
                "client", "create", "--root", "/tmp/x", "--name", "web", "--kind", "secret",
                "--scope", "read",
            ]),
            args(&[
                "client", "create", "--root", "/tmp/x", "--name", "web", "--kind", "public",
            ]),
            args(&[
                "client", "create", "--root", "/tmp/x", "--name", "web", "--kind", "public",
                "--scope", "read", "--scope", "read",
            ]),
            args(&[
                "key", "create", "--root", "/tmp/x", "--client", "bad", "--label", "primary",
                "--scope", "read",
            ]),
            args(&[
                "key",
                "create",
                "--root",
                "/tmp/x",
                "--client",
                "app_00000000000000000000000001",
                "--label",
                "primary",
                "--scope",
                "read",
                "--expires-at-micros",
                "01",
            ]),
            args(&[
                "key",
                "rotate",
                "--root",
                "/tmp/x",
                "--client",
                "app_00000000000000000000000001",
                "--key",
                "crd_00000000000000000000000002",
            ]),
            args(&["workspace", "--root", "/tmp/x"]),
            args(&[
                "workspace",
                "key",
                "create",
                "--root",
                "/tmp/x",
                "--actor",
                "UPPER",
                "--label",
                "laptop",
            ]),
            args(&[
                "workspace",
                "key",
                "rotate",
                "--root",
                "/tmp/x",
                "--key",
                "dvk_00000000000000000000000001",
            ]),
            args(&[
                "workspace",
                "sync",
                "--root",
                "/tmp/x",
                "--url",
                "http://example.com",
                "--workspace",
                "dev/x",
                "--token-env",
                "RUNKU_DEV_KEY",
            ]),
            args(&[
                "workspace",
                "sync",
                "--root",
                "/tmp/x",
                "--url",
                "https://example.com",
                "--workspace",
                "dev/x",
                "--token-env",
                "AWS_SECRET_ACCESS_KEY",
            ]),
            args(&[
                "workspace",
                "sync",
                "--root",
                "/tmp/x",
                "--url",
                "https://example.com",
                "--workspace",
                "dev/x",
                "--token-env",
                "RUNKU_dev_key",
            ]),
            args(&[
                "workspace",
                "sync",
                "--root",
                "/tmp/x",
                "--url",
                "https://example.com",
                "--workspace",
                "dev/x",
                "--token-env",
                "RUNKU_DEV_KEY",
                "--create",
                "--create",
            ]),
            args(&["dev", "--root", "/tmp/x", "--prebuilt", "--prebuilt"]),
            args(&[
                "dev",
                "--root",
                "/tmp/x",
                "--auth-config",
                "a.json",
                "--auth-config",
                "b.json",
            ]),
            args(&["logs", "--root", "/tmp/x", "--limit", "0"]),
            args(&["logs", "--root", "/tmp/x", "--limit", "01"]),
            args(&["logs", "--root", "/tmp/x", "--after", "logc_01"]),
            args(&["logs", "--root", "/tmp/x", "--follow", "--follow"]),
            args(&["logs", "--root", "/tmp/x", "--stream", "all"]),
            args(&["logs", "--root", "/tmp/x", "--level", "trace"]),
            args(&[
                "logs",
                "prune",
                "--root",
                "/tmp/x",
                "--before-micros",
                "1",
                "--apply",
            ]),
            args(&[
                "logs",
                "prune",
                "--root",
                "/tmp/x",
                "--before-micros",
                "1",
                "--environment",
                "env_00000000000000000000000001",
            ]),
            args(&["logs", "prune", "--root", "/tmp/x", "--before-micros", "-1"]),
            args(&[
                "logs",
                "prune",
                "--root",
                "/tmp/x",
                "--before-micros",
                "1",
                "--maximum",
                "10001",
            ]),
        ] {
            assert!(parse_args(invalid).is_err());
        }
    }
}
