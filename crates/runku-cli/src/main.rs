//! `runku` executable entry point.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::{IsTerminal as _, Write as _},
    path::{Component, Path, PathBuf},
    process::ExitCode,
    time::SystemTime,
};

use runku_build::{BuildError, BuildMetadata, build_project, source_fingerprint};
use runku_cli::{
    CliCommand, DEFAULT_LOCAL_LISTENER, DEFAULT_LOCAL_WORKSPACE, HELP, TokenEnvironmentName,
    WORKSPACE_FREEZE_HELP, parse_args,
};
use runku_core::{ApplicationClientId, CredentialId, OperationId, WorkspaceId, WorkspaceRef};
use runku_development::DevelopmentActor;
use runku_development_access::{DevelopmentCredentialStatus, DevelopmentLifecycleResult};
use runku_development_client::{
    DevelopmentClient, DevelopmentClientConfig, DevelopmentClientError, DevelopmentEndpoint,
};
use runku_identity::{
    ApplicationAssurance, ApplicationClient, ApplicationClientStatus, ApplicationScope, ClientKind,
    CredentialKind, CredentialLifecycleResult, CredentialStatus,
};
use runku_local::{
    LocalChannelExpectation, LocalCredentialMetadata, LocalDevelopmentAccessError,
    LocalDevelopmentAccessManager, LocalDevelopmentCredentialMetadata, LocalDoctorError,
    LocalIdentityError, LocalIdentityManager, LocalLogError, LocalLogManager, LocalOtlpError,
    LocalOtlpExporter, LocalProcess, LocalProcessConfig, LocalProcessError, LocalPublishError,
    LocalReleaseError, LocalReleaseManager, LocalReleaseOutcome, LocalReleaseStatusReport,
    LocalStateError, acquire_local_process_lease, doctor_local, initialize_local, load_local,
    publish_local, publish_local_if_head,
};
use runku_observability::{LogQuery, SequencedOperationalEvent};
use runku_otel::{OtlpExporterMode, OtlpExporterTelemetrySnapshot};
use runku_protocol::{
    DevelopmentCreateWorkspaceRequestV1, DevelopmentFreezeOutcomeV1, DevelopmentFreezeRequestV1,
    DevelopmentPublishRequestV1, DevelopmentStateRequestV1, WireValueV1,
    derive_development_freeze_request_operation_id_v1, derive_development_revision_id_v1,
};
use runku_releases::{
    ARTIFACT_MAX_BYTES, FunctionType, MANIFEST_MAX_BYTES, Sha256Digest, decode_release_manifest,
};
use runku_value::TimestampMicros;
use serde::Serialize;

const EXIT_INTERNAL: u8 = 1;
const EXIT_USAGE: u8 = 2;
const EXIT_INVALID: u8 = 3;
const EXIT_CONFLICT: u8 = 4;
const EXIT_UNAVAILABLE: u8 = 5;
const EXIT_CORRUPT: u8 = 6;
const EXIT_AUTH: u8 = 7;
const EXIT_POLICY: u8 = 8;
const EXIT_UNCERTAIN: u8 = 9;

#[tokio::main]
async fn main() -> ExitCode {
    if let Ok(command) = parse_args(std::env::args_os().skip(1)) {
        match execute(command).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                print_failure(error);
                ExitCode::from(error.exit)
            }
        }
    } else {
        print_failure(CliFailure {
            code: "CLI_USAGE_INVALID",
            exit: EXIT_USAGE,
        });
        eprintln!();
        eprintln!("{HELP}{WORKSPACE_FREEZE_HELP}");
        ExitCode::from(EXIT_USAGE)
    }
}

#[derive(Clone, Copy)]
struct CliFailure {
    code: &'static str,
    exit: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FailureExplanation {
    message: &'static str,
    hint: &'static str,
}

fn print_failure(failure: CliFailure) {
    let explanation = explain_failure(failure);
    eprintln!("error: {}", failure.code);
    eprintln!("message: {}", explanation.message);
    eprintln!("hint: {}", explanation.hint);
}

#[allow(clippy::too_many_lines)]
fn explain_failure(failure: CliFailure) -> FailureExplanation {
    let code = failure.code;
    match code {
        "CLI_USAGE_INVALID" => FailureExplanation {
            message: "The command or its arguments are invalid.",
            hint: "Review the usage shown below. Flags are exact, singleton flags cannot be repeated, and project commands default to the current directory.",
        },
        "LOCAL_PATH_INVALID" => FailureExplanation {
            message: "The selected project directory is missing, unsafe, or not a supported directory.",
            hint: "Run from a regular project directory or pass --root PATH. System root, the home directory, and symlinked project roots are rejected.",
        },
        "BUILD_PATH_INVALID" => FailureExplanation {
            message: "Runku could not find or safely read the declarative source directory.",
            hint: "Run from the application root and add a regular runku/ directory containing schema.ts and declarative Functions; source symlinks and path escapes are rejected.",
        },
        "LOCAL_STATE_INVALID"
        | "LOCAL_PROCESS_STATE_INVALID"
        | "LOCAL_DOCTOR_STATE_INVALID"
        | "LOCAL_LOG_STATE_INVALID"
        | "LOCAL_IDENTITY_STATE_INVALID"
        | "LOCAL_DEVELOPMENT_ACCESS_STATE_INVALID"
        | "LOCAL_OTLP_STATE_INVALID"
        | "LOCAL_PUBLISH_STATE_INVALID" => FailureExplanation {
            message: "Runku could not open a valid initialized project in the selected directory.",
            hint: "Run from the project root or pass --root PATH. Projects containing runku/ are initialized automatically by runku dev; use runku init only for non-default settings or prebuilt packages.",
        },
        "LOCAL_STATE_CONFLICT" => FailureExplanation {
            message: "The project is already initialized with different local settings.",
            hint: "Reuse the original workspace and listener values, or initialize a different directory. Runku will not overwrite existing local identity or data.",
        },
        "LOCAL_PROCESS_ALREADY_RUNNING" => FailureExplanation {
            message: "Another Runku development process already owns this project.",
            hint: "Use the running process or stop it cleanly before starting another runku dev for the same project.",
        },
        "LOCAL_PROCESS_LISTENER_UNAVAILABLE" => FailureExplanation {
            message: "Runku could not bind the configured local listener.",
            hint: "Check whether the configured port is already in use, then stop the conflicting process or initialize a project with another loopback port.",
        },
        "LOCAL_PROCESS_CONFIGURATION_INVALID" => FailureExplanation {
            message: "The local development process configuration is invalid.",
            hint: "Check browser origins, the authentication descriptor, and the initialized loopback listener before retrying.",
        },
        "LOCAL_PROCESS_COMPOSITION_FAILED" | "LOCAL_PROCESS_STOPPED" => FailureExplanation {
            message: "The local development process could not start or stopped unexpectedly.",
            hint: "Run runku doctor, verify the project files and local dependencies, then retry runku dev.",
        },
        "LOCAL_BOOTSTRAP_DEFAULTS_INVALID" => FailureExplanation {
            message: "Runku's built-in local development defaults are invalid.",
            hint: "Report this internal CLI error with the Runku version; no project state was intentionally replaced.",
        },
        "LOCAL_AUTH_CONFIG_PATH_INVALID" | "LOCAL_AUTH_CONFIG_INVALID" => FailureExplanation {
            message: "The local functional-authentication descriptor is missing, unsafe, or invalid.",
            hint: "Use a regular project-relative JSON file and verify its issuer, discovery URL, audience, algorithm, claims, origin, and limits.",
        },
        "LOCAL_APPLICATION_ENV_PATH_INVALID" | "LOCAL_APPLICATION_ENV_INVALID" => {
            FailureExplanation {
                message: "The local application environment file is missing, unsafe, oversized, or malformed.",
                hint: "Use a regular project-relative dotenv file of at most 64 KiB; symlinks and path escapes are rejected.",
            }
        }
        "LOCAL_APPLICATION_ENV_AMBIGUOUS" => FailureExplanation {
            message: "Runku detected conflicting frontend environment conventions.",
            hint: "Pass --public-env-prefix explicitly, for example RUNKU_, NEXT_PUBLIC_RUNKU_, VITE_RUNKU_, VUE_APP_RUNKU_, or PUBLIC_RUNKU_.",
        },
        "LOCAL_APPLICATION_ENV_CONFIRMATION_REQUIRED" => FailureExplanation {
            message: "The application environment contains remote or foreign Runku credentials and cannot be changed non-interactively.",
            hint: "Keep the remote configuration and use workspace sync, or deliberately rerun local development with --replace-remote-credentials.",
        },
        "LOCAL_APPLICATION_ENV_PRESERVED" => FailureExplanation {
            message: "The remote application configuration was preserved and local development did not start.",
            hint: "Use the remote Environment with workspace sync, or rerun runku dev and confirm replacement with local credentials.",
        },
        "LOCAL_APPLICATION_ENV_CONFLICT" => FailureExplanation {
            message: "Reserved local development Application Client state conflicts with durable identity configuration.",
            hint: "Inspect runku client list and key list; do not overwrite or repair identity state implicitly.",
        },
        "LOCAL_APPLICATION_ENV_UNAVAILABLE" => FailureExplanation {
            message: "Runku could not atomically persist the local application environment.",
            hint: "Check directory permissions and concurrent writers, then retry without editing identity state manually.",
        },
        "BUILD_SOURCE_SYNTAX_INVALID" => FailureExplanation {
            message: "A Runku source file contains invalid TypeScript or JavaScript syntax.",
            hint: "Fix the reported source revision and save again. In runku dev, the last valid revision remains active.",
        },
        "BUILD_SOURCE_POLICY_DENIED" => FailureExplanation {
            message: "The source graph uses an import, directive, runtime, or language feature that Runku does not allow.",
            hint: "Keep Functions inside runku/, use supported declarative exports, and remove forbidden dynamic, Node, network, or filesystem capabilities.",
        },
        "BUILD_CONFIG_INVALID" => FailureExplanation {
            message: "The declarative Runku source graph is incomplete or internally inconsistent.",
            hint: "Check schema.ts, Function declarations, validators, runtimes, capabilities, indexes, and cron references.",
        },
        "BUILD_FEATURE_UNSUPPORTED" => FailureExplanation {
            message: "The project requests a feature that the current Runku runtime does not support.",
            hint: "Use a supported runtime/declaration or update the CLI and runtime together before rebuilding.",
        },
        "BUILD_LIMIT_EXCEEDED" => FailureExplanation {
            message: "The source graph or generated artifact exceeds a bounded build limit.",
            hint: "Reduce the affected source, declaration, contract, or artifact size and build again.",
        },
        "BUILD_OUTPUT_CONFLICT" => FailureExplanation {
            message: "An immutable build output already exists with different content.",
            hint: "Do not edit files under .runku/builds-v1. Rebuild with fresh metadata and inspect the existing output for tampering or concurrent writes.",
        },
        "SOURCE_SNAPSHOT_UNSTABLE" => FailureExplanation {
            message: "The source files kept changing while Runku was trying to build one coherent revision.",
            hint: "Wait for editors or generators to finish writing, then retry. runku dev will keep the last valid revision active.",
        },
        "SOURCE_WATCH_CONFIG_INVALID" | "SOURCE_WATCH_ACTOR_INVALID" => FailureExplanation {
            message: "Runku could not initialize the local source watcher safely.",
            hint: "Verify the build configuration and local project state, then retry runku dev. If it persists, report the stable error code.",
        },
        "BUILD_OUTPUT_CORRUPT" | "BUILD_OUTPUT_INVALID" | "BUILD_OUTPUT_PATH_INVALID" => {
            FailureExplanation {
                message: "The generated build output is missing, malformed, or inconsistent with its manifest.",
                hint: "Do not edit immutable build files. Run runku build again and investigate filesystem or concurrent-writer issues if it repeats.",
            }
        }
        "LOCAL_PACKAGE_FILE_INVALID" | "LOCAL_PACKAGE_FILE_UNAVAILABLE" => FailureExplanation {
            message: "A manifest or artifact file cannot be safely read as a regular bounded package file.",
            hint: "Use the exact manifestPath and artifactPath returned by runku build; symlinks, empty files, oversized files, and unreadable paths are rejected.",
        },
        "LOCAL_PUBLISH_PACKAGE_INVALID" | "LOCAL_PUBLISH_PROJECT_MISMATCH" => FailureExplanation {
            message: "The package is invalid or belongs to a different Runku project.",
            hint: "Publish the unmodified manifest and artifact produced for this project by the same runku build output.",
        },
        "LOCAL_PUBLISH_CONFLICT" => FailureExplanation {
            message: "The Workspace head changed before this package could be published.",
            hint: "Read the latest Workspace state, rebuild or confirm the intended revision, and retry with the correct expected head.",
        },
        "LOCAL_RELEASE_NOT_FOUND" => FailureExplanation {
            message: "The requested Release does not exist in this project.",
            hint: "Run runku status and use a Release ID produced and published by this project.",
        },
        "LOCAL_RELEASE_INVALID" => FailureExplanation {
            message: "The Release, Channel, or lifecycle transition is invalid.",
            hint: "Run runku status and verify IDs, channel names, expected values, and the required Release lifecycle state.",
        },
        "LOCAL_RELEASE_CONFLICT" => FailureExplanation {
            message: "The Release or Channel changed since the expected state was observed.",
            hint: "Run runku status, review the current binding, and retry with an updated --expected value.",
        },
        "RELEASE_COMPATIBILITY_BLOCKED" | "DEVELOPMENT_COMPATIBILITY_BLOCKED" => {
            FailureExplanation {
                message: "The requested Release transition is blocked by compatibility rules.",
                hint: "Review the diagnostics emitted on stdout, update the incompatible Function/schema contract, build a new Release, and retry.",
            }
        }
        "LOCAL_DOCTOR_INCONSISTENT" => FailureExplanation {
            message: "Runku found inconsistent durable project state and did not repair it automatically.",
            hint: "Stop local processes, preserve the .runku directory, inspect doctor/log evidence, and restore from a known-good backup or package instead of deleting state.",
        },
        "LOCAL_IDENTITY_INPUT_INVALID" => FailureExplanation {
            message: "The application client or key operation is invalid for the supplied values or current lifecycle state.",
            hint: "Verify client/key IDs, scopes, labels, and expiry. Revoke an active key before deleting it; secret keys cannot be revealed again.",
        },
        "LOCAL_IDENTITY_NOT_FOUND" => FailureExplanation {
            message: "The requested application client or key was not found.",
            hint: "Run runku client list or runku key list --client app_* and retry with an ID from the selected project.",
        },
        "LOCAL_IDENTITY_CONFLICT" => FailureExplanation {
            message: "The application client or key operation conflicts with current durable state.",
            hint: "List the current credentials, choose a distinct ID when creating, and retry from the observed lifecycle state.",
        },
        "LOCAL_DEVELOPMENT_ACCESS_INPUT_INVALID" => FailureExplanation {
            message: "The Development Workspace key operation is invalid for the supplied values or lifecycle state.",
            hint: "Verify key IDs, actor, label, and expiry. Revoke an active Development key before deleting it.",
        },
        "LOCAL_DEVELOPMENT_ACCESS_NOT_FOUND" => FailureExplanation {
            message: "The requested Development Workspace key was not found.",
            hint: "Run runku workspace key list and retry with an ID from the selected project.",
        },
        "LOCAL_DEVELOPMENT_ACCESS_CONFLICT" => FailureExplanation {
            message: "The Development Workspace key operation conflicts with its current lifecycle state.",
            hint: "List the current keys, avoid duplicate IDs, and revoke an active key before deleting it.",
        },
        "DEVELOPMENT_AUTH_ENV_INVALID" => FailureExplanation {
            message: "The required Remote Workspace token environment variable is missing or invalid.",
            hint: "Set the exact RUNKU_* variable named by --token-env without placing the token in command arguments or files.",
        },
        "DEVELOPMENT_AUTH_INVALID" => FailureExplanation {
            message: "The Remote Workspace service rejected the supplied credential.",
            hint: "Verify the selected token variable, rotate or replace the credential if necessary, and retry without printing the token.",
        },
        "DEVELOPMENT_ACCESS_DENIED" | "DEVELOPMENT_POLICY_DENIED" => FailureExplanation {
            message: "The authenticated identity is not allowed to perform this Remote Workspace operation.",
            hint: "Check credential scopes, Environment policy, Workspace access, and whether the target permits live Development revisions.",
        },
        "DEVELOPMENT_WORKSPACE_ABSENT" | "DEVELOPMENT_RESOURCE_NOT_FOUND" => FailureExplanation {
            message: "The requested remote Workspace or Release does not exist.",
            hint: "Verify the target and Workspace reference. Use --create only when intentionally creating a missing Workspace.",
        },
        "DEVELOPMENT_STATE_CONFLICT" => FailureExplanation {
            message: "Remote Workspace state changed before the operation committed.",
            hint: "Fetch the latest remote state and retry with the correct expected head; do not force or overwrite the remote pointer.",
        },
        "DEVELOPMENT_RESULT_UNCERTAIN" => FailureExplanation {
            message: "The remote service may have committed the operation, but the client did not receive a definitive result.",
            hint: "Reconcile remote state before retrying. Reuse the same logical operation rather than assuming it failed.",
        },
        "LOCAL_OTLP_CONFIGURATION_INVALID" | "LOCAL_OTLP_CONFIGURATION_DRIFT" => {
            FailureExplanation {
                message: "The OTLP exporter configuration is invalid or changed for an existing checkpoint identity.",
                hint: "Check the project-relative exporter file, endpoint, headers, limits, and exporter name; use a new exporter identity for a different destination.",
            }
        }
        "LOCAL_OTLP_ALREADY_RUNNING" => FailureExplanation {
            message: "Another OTLP exporter already owns this project/exporter checkpoint.",
            hint: "Use the running exporter or stop it cleanly before starting another process for the same exporter.",
        },
        "LOCAL_OTLP_REJECTED" => FailureExplanation {
            message: "The OTLP destination rejected the exported log batch.",
            hint: "Check collector compatibility, authentication, and destination policy. The checkpoint was not advanced for a rejected batch.",
        },
        "LOCAL_LOG_REQUEST_INVALID" => FailureExplanation {
            message: "The operational log query or retention request is invalid.",
            hint: "Check cursor, limit, filters, timestamps, and the required Environment confirmation for destructive pruning.",
        },
        "LOCAL_LOG_CORRUPT" => FailureExplanation {
            message: "The operational log store contains data that failed canonical validation.",
            hint: "Stop writers, preserve .runku, and investigate or restore the log database; Runku will not silently skip corrupted records.",
        },
        "LOCAL_CLOCK_UNAVAILABLE" => FailureExplanation {
            message: "The system clock cannot provide a valid timestamp for this operation.",
            hint: "Correct the host clock and retry; Runku will not create identities or revisions with an invalid timestamp.",
        },
        "LOCAL_SIGNAL_UNAVAILABLE" => FailureExplanation {
            message: "Runku could not install or receive the local shutdown signal handler.",
            hint: "Stop the process with the host supervisor and retry in a supported terminal or process environment.",
        },
        "CLI_OUTPUT_INVALID" | "CLI_OUTPUT_UNAVAILABLE" => FailureExplanation {
            message: "Runku could not write a complete valid response to stdout.",
            hint: "Check the output pipe, terminal, and available disk space before retrying the command.",
        },
        value if value.starts_with("BUILD_") => FailureExplanation {
            message: "The Runku build could not produce a valid immutable package.",
            hint: "Check the declarative source graph and filesystem, then retry runku build without editing generated artifacts.",
        },
        value if value.starts_with("LOCAL_IDENTITY_") => FailureExplanation {
            message: "The local application identity operation could not be completed safely.",
            hint: "Inspect clients and keys, preserve the identity database, and retry only after the reported availability or state issue is resolved.",
        },
        value if value.starts_with("LOCAL_DEVELOPMENT_ACCESS_") => FailureExplanation {
            message: "The local Development access operation could not be completed safely.",
            hint: "Inspect Workspace keys, preserve the credential database, and retry only after the reported availability or state issue is resolved.",
        },
        value if value.starts_with("LOCAL_RELEASE_") => FailureExplanation {
            message: "The local Release lifecycle operation could not be completed safely.",
            hint: "Run runku status, preserve Release state and artifacts, and retry from the currently observed lifecycle state.",
        },
        value if value.starts_with("LOCAL_PUBLISH_") => FailureExplanation {
            message: "The package could not be published to the local Workspace safely.",
            hint: "Run runku doctor and use an unmodified package from runku build before retrying.",
        },
        value if value.starts_with("LOCAL_OTLP_") => FailureExplanation {
            message: "The local OTLP exporter could not complete its operation.",
            hint: "Check exporter configuration, collector availability, checkpoint state, and runku doctor before retrying.",
        },
        value if value.starts_with("LOCAL_LOG_") => FailureExplanation {
            message: "The local operational log operation could not be completed.",
            hint: "Run runku doctor and retry after resolving storage or availability issues; destructive pruning still requires exact confirmation.",
        },
        value if value.starts_with("DEVELOPMENT_") => FailureExplanation {
            message: "The Remote Workspace operation could not be completed safely.",
            hint: "Check endpoint, token environment, remote state, and policy; reconcile uncertain results before retrying.",
        },
        _ => fallback_explanation(failure.exit),
    }
}

fn fallback_explanation(exit: u8) -> FailureExplanation {
    match exit {
        EXIT_INVALID => FailureExplanation {
            message: "The command input or selected project state is invalid.",
            hint: "Check the command values and project state, then retry without deleting or overwriting durable data.",
        },
        EXIT_CONFLICT => FailureExplanation {
            message: "The operation conflicts with state that changed or already exists.",
            hint: "Read the latest state and retry with updated expectations; do not force an overwrite.",
        },
        EXIT_UNAVAILABLE => FailureExplanation {
            message: "A required local or remote dependency is temporarily unavailable.",
            hint: "Check the listener, filesystem, database, or remote service and retry after it recovers.",
        },
        EXIT_CORRUPT => FailureExplanation {
            message: "Runku detected inconsistent or corrupted durable state.",
            hint: "Stop writers, preserve the project state, and diagnose or restore it instead of deleting files automatically.",
        },
        EXIT_AUTH => FailureExplanation {
            message: "Authentication failed or the required credential configuration is invalid.",
            hint: "Check the configured credential source and scopes without printing or copying secrets into command arguments.",
        },
        EXIT_POLICY => FailureExplanation {
            message: "The operation is not permitted by the target Environment or compatibility policy.",
            hint: "Review target policy and compatibility diagnostics, then change the request or build a compatible Release.",
        },
        EXIT_UNCERTAIN => FailureExplanation {
            message: "The operation may have committed but its final result is unknown.",
            hint: "Reconcile current durable state before retrying the same logical operation.",
        },
        _ => FailureExplanation {
            message: "Runku encountered an internal failure and stopped the command safely.",
            hint: "Retry once; if it persists, preserve the project state and report the stable error code with runku --version.",
        },
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BuildOutputWire<'a> {
    artifact_digest: String,
    artifact_path: &'a str,
    build_id: String,
    generated_types_digest: String,
    generated_types_path: &'a str,
    stable_generated_types_path: &'a str,
    manifest_digest: String,
    manifest_path: &'a str,
    release_id: String,
    replayed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientWire<'a> {
    client_id: String,
    created_at_micros: String,
    kind: &'static str,
    name: &'a str,
    scopes: Vec<&'a str>,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientsWire<'a> {
    clients: Vec<ClientWire<'a>>,
    configuration_revision: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialWire<'a> {
    client_id: String,
    created_at_micros: String,
    credential_id: String,
    expires_at_micros: Option<String>,
    kind: &'static str,
    label: &'a str,
    revoked_at_micros: Option<String>,
    scopes: Vec<&'a str>,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialsWire<'a> {
    configuration_revision: u64,
    credentials: Vec<CredentialWire<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreatedCredentialWire<'a> {
    configuration_revision: u64,
    credential: CredentialWire<'a>,
    key: &'a str,
    recoverable: bool,
    secret_shown_once: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LifecycleWire {
    configuration_revision: u64,
    credential_id: String,
    replayed: bool,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DevelopmentCredentialWire<'a> {
    actor: &'a str,
    created_at_micros: String,
    credential_id: String,
    expires_at_micros: Option<String>,
    label: &'a str,
    revoked_at_micros: Option<String>,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DevelopmentCredentialsWire<'a> {
    configuration_revision: u64,
    credentials: Vec<DevelopmentCredentialWire<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreatedDevelopmentCredentialWire<'a> {
    configuration_revision: u64,
    credential: DevelopmentCredentialWire<'a>,
    key: &'a str,
    recoverable: bool,
    secret_shown_once: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DevelopmentLifecycleWire {
    configuration_revision: u64,
    credential_id: String,
    replayed: bool,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteWorkspaceStateWire {
    event_version: u8,
    stage: &'static str,
    request_id: String,
    project_id: String,
    environment_id: String,
    workspace: String,
    exists: bool,
    head: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteWorkspaceCreateWire {
    event_version: u8,
    stage: &'static str,
    request_id: String,
    workspace: String,
    workspace_id: String,
    replayed: bool,
    reconciled: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteWorkspaceBuildWire {
    event_version: u8,
    stage: &'static str,
    build_id: String,
    release_id: String,
    replayed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteWorkspacePublishWire {
    event_version: u8,
    stage: &'static str,
    request_id: String,
    revision_id: String,
    release_id: String,
    replayed: bool,
    reconciled: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteWorkspaceFreezeWire {
    event_version: u8,
    stage: &'static str,
    request_id: String,
    release_id: String,
    outcome: &'static str,
    diagnostics: Vec<RemoteWorkspaceFreezeDiagnosticWire>,
    serving_revision: u64,
    replayed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteWorkspaceFreezeDiagnosticWire {
    code: String,
    subject: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseDiagnosticWire<'a> {
    code: &'static str,
    subject: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseOutcomeWire<'a> {
    channel: Option<String>,
    compatible: bool,
    diagnostics: Vec<ReleaseDiagnosticWire<'a>>,
    release_id: String,
    replayed: bool,
    serving_revision: u64,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseStatusEntryWire {
    release_id: String,
    runtime_version: String,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ChannelStatusWire {
    channel: String,
    default: bool,
    release_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseStatusWire {
    channels: Vec<ChannelStatusWire>,
    default_channel: Option<String>,
    releases: Vec<ReleaseStatusEntryWire>,
    serving_revision: u64,
}

struct SourceSyncOutput {
    fingerprint: String,
    release_id: String,
    revision_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WatchEventWire<'a> {
    event_version: u8,
    status: &'static str,
    fingerprint: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    release_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision_id: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OperationalLogWire {
    cursor: String,
    event_id: String,
    occurred_at_micros: String,
    project_id: String,
    environment_id: String,
    request_id: String,
    invocation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_invocation_id: Option<String>,
    release_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    dev_revision_id: Option<String>,
    function_id: String,
    function_name: String,
    function_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential_id: Option<String>,
    principal_kind: &'static str,
    stream: &'static str,
    level: &'static str,
    event_kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<WireValueV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_micros: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome_code: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpStatusWire<'a> {
    event_version: u8,
    exporter: &'a str,
    status: &'static str,
    mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    telemetry: Option<OtlpTelemetryWire>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpTelemetryWire {
    cycles: u64,
    requests: u64,
    exported_records: u64,
    retries: u64,
    duplicates_possible: u64,
    checkpoint_replays: u64,
    failures: u64,
}

impl From<OtlpExporterTelemetrySnapshot> for OtlpTelemetryWire {
    fn from(value: OtlpExporterTelemetrySnapshot) -> Self {
        Self {
            cycles: value.cycles,
            requests: value.requests,
            exported_records: value.exported_records,
            retries: value.retries,
            duplicates_possible: value.duplicates_possible,
            checkpoint_replays: value.checkpoint_replays,
            failures: value.failures,
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn execute(command: CliCommand) -> Result<(), CliFailure> {
    match command {
        CliCommand::Help => {
            print!("{HELP}{WORKSPACE_FREEZE_HELP}");
            Ok(())
        }
        CliCommand::Version => {
            println!("runku {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        CliCommand::Init {
            root,
            workspace,
            listen,
        } => {
            let (state, _) = initialize_local(&root, workspace, listen, now()?)
                .await
                .map_err(map_state)?;
            println!(
                "{{\"environmentId\":\"{}\",\"projectId\":\"{}\",\"workspaceId\":\"{}\"}}",
                state.environment_id, state.project_id, state.workspace_id
            );
            Ok(())
        }
        CliCommand::Publish {
            root,
            manifest,
            artifact,
            workspace,
            actor,
            expected_head,
        } => {
            let state = load_local(&root).await.map_err(map_state)?.0;
            let workspace = workspace.unwrap_or(state.workspace_ref);
            let manifest = read_bounded(&manifest, MANIFEST_MAX_BYTES).await?;
            let artifact = read_bounded(&artifact, ARTIFACT_MAX_BYTES).await?;
            let result = match expected_head {
                Some(expected) => {
                    publish_local_if_head(&root, &workspace, &actor, expected, &manifest, &artifact)
                        .await
                }
                None => publish_local(&root, &workspace, &actor, &manifest, &artifact).await,
            }
            .map_err(map_publish)?;
            println!(
                "{{\"releaseId\":\"{}\",\"replayed\":{},\"revisionId\":\"{}\"}}",
                result.release_id, result.replayed, result.revision_id
            );
            Ok(())
        }
        CliCommand::Build {
            root,
            config,
            metadata,
        } => execute_build(root, config, metadata).await,
        CliCommand::Release {
            root,
            release_id,
            against,
        } => {
            let manager = LocalReleaseManager::open(&root)
                .await
                .map_err(map_release)?;
            let result = manager
                .release(release_id, against.as_ref())
                .await
                .map_err(map_release)?;
            emit_release_outcome(&result)
        }
        CliCommand::Promote {
            root,
            channel,
            release_id,
            expected,
        } => {
            let manager = LocalReleaseManager::open(&root)
                .await
                .map_err(map_release)?;
            let result = manager
                .promote(
                    channel,
                    release_id,
                    match expected {
                        None => LocalChannelExpectation::Current,
                        Some(None) => LocalChannelExpectation::Empty,
                        Some(Some(release_id)) => LocalChannelExpectation::Release(release_id),
                    },
                )
                .await
                .map_err(map_release)?;
            emit_release_outcome(&result)
        }
        CliCommand::Rollback {
            root,
            channel,
            expected,
            target,
        } => {
            let manager = LocalReleaseManager::open(&root)
                .await
                .map_err(map_release)?;
            let result = manager
                .rollback(channel, expected, target)
                .await
                .map_err(map_release)?;
            emit_release_outcome(&result)
        }
        CliCommand::Status { root } => {
            let manager = LocalReleaseManager::open(&root)
                .await
                .map_err(map_release)?;
            let report = manager.status().await.map_err(map_release)?;
            emit_release_status(&report)
        }
        CliCommand::Doctor { root } => {
            let report = doctor_local(&root).await.map_err(map_doctor)?;
            println!(
                "{{\"activeCronDefinitions\":{},\"developmentAccessHealthy\":{},\"developmentRevision\":{},\"operationalLogsHealthy\":{},\"otlpCheckpointsHealthy\":{},\"releaseId\":\"{}\",\"revisionId\":\"{}\",\"status\":\"ok\"}}",
                report.active_cron_definitions,
                report.development_access_healthy,
                report.development_revision,
                report.operational_logs_healthy,
                report.otlp_checkpoints_healthy,
                report.release_id,
                report.revision_id
            );
            Ok(())
        }
        CliCommand::Logs {
            root,
            after,
            limit,
            stream,
            minimum_level,
            function_id,
            request_id,
            invocation_id,
            client_id,
            credential_id,
            release_id,
            follow,
        } => {
            let manager = LocalLogManager::open(&root).await.map_err(map_logs)?;
            let scope = manager.scope();
            let result = emit_logs(
                &manager,
                LogQuery {
                    scope,
                    after,
                    limit,
                    stream,
                    minimum_level,
                    function_id,
                    request_id,
                    invocation_id,
                    client_id,
                    credential_id,
                    release_id,
                },
                follow,
            )
            .await;
            manager.close().await;
            result
        }
        CliCommand::LogsPrune {
            root,
            before,
            maximum,
            apply,
            environment,
        } => {
            let manager = LocalLogManager::open(&root).await.map_err(map_logs)?;
            let result = manager
                .prune_before(before, maximum, apply, environment)
                .await;
            let environment_id = manager.environment_id();
            manager.close().await;
            let result = result.map_err(map_logs)?;
            println!(
                "{{\"applied\":{},\"deleted\":{},\"environmentId\":\"{}\",\"matched\":{},\"more\":{}}}",
                apply, result.deleted, environment_id, result.matched, result.more
            );
            Ok(())
        }
        CliCommand::LogsExportOtlp { root, config, once } => {
            let exporter = LocalOtlpExporter::open(&root, &config)
                .await
                .map_err(map_otlp)?;
            let mode = if once {
                OtlpExporterMode::Once
            } else {
                OtlpExporterMode::Follow
            };
            let mode_text = if once { "once" } else { "follow" };
            emit_json(&OtlpStatusWire {
                event_version: 1,
                exporter: exporter.name().as_str(),
                status: "running",
                mode: mode_text,
                telemetry: None,
            })?;
            let (shutdown, receiver) = tokio::sync::watch::channel(false);
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    let _sent = shutdown.send(true);
                }
            });
            let report = exporter.run(mode, receiver).await.map_err(map_otlp)?;
            emit_json(&OtlpStatusWire {
                event_version: 1,
                exporter: report.exporter.as_str(),
                status: if once { "complete" } else { "stopped" },
                mode: mode_text,
                telemetry: Some(report.telemetry.into()),
            })
        }
        CliCommand::Dev {
            root,
            origins,
            watch_config,
            auth_config,
            application_env,
            public_env_prefix,
            prepare,
            replace_remote_credentials,
        } => {
            if watch_config.is_some() || prepare {
                if !prepare {
                    source_fingerprint(
                        &root,
                        watch_config.as_ref().ok_or(CliFailure {
                            code: "SOURCE_WATCH_CONFIG_INVALID",
                            exit: EXIT_INTERNAL,
                        })?,
                    )
                    .map_err(map_build)?;
                }
                ensure_local_development_project(&root).await?;
            }
            let public_env_prefix = match public_env_prefix {
                Some(prefix) => prefix,
                None => detect_public_env_prefix(&root)?,
            };
            reconcile_dev_application_env(
                &root,
                &application_env,
                &public_env_prefix,
                replace_remote_credentials,
            )
            .await?;
            if prepare {
                emit_json(&serde_json::json!({
                    "applicationEnv": application_env.to_string_lossy(),
                    "status": "prepared",
                }))?;
                return Ok(());
            }
            let config = LocalProcessConfig {
                allowed_origins: origins,
                auth_config,
                ..LocalProcessConfig::default()
            };
            let (process, initial_sync) = match watch_config.as_ref() {
                Some(source_config) => {
                    let lease = acquire_local_process_lease(&root)
                        .await
                        .map_err(map_process)?;
                    let synced = sync_source(&root, source_config).await?;
                    let process = LocalProcess::start_with_lease(&root, config, lease)
                        .await
                        .map_err(map_process)?;
                    (process, Some(synced))
                }
                None => (
                    LocalProcess::start(&root, config)
                        .await
                        .map_err(map_process)?,
                    None,
                ),
            };
            if let Some(synced) = initial_sync {
                println!(
                    "{{\"address\":\"{}\",\"environmentId\":\"{}\",\"eventVersion\":1,\"fingerprint\":\"{}\",\"releaseId\":\"{}\",\"revisionId\":\"{}\",\"status\":\"ready\",\"watching\":true,\"workspace\":\"{}\"}}",
                    process.address(),
                    process.state().environment_id,
                    synced.fingerprint,
                    synced.release_id,
                    synced.revision_id,
                    process.state().workspace_ref
                );
                wait_for_watch_shutdown(
                    &process,
                    &root,
                    watch_config.as_ref().ok_or(CliFailure {
                        code: "SOURCE_WATCH_CONFIG_INVALID",
                        exit: EXIT_INTERNAL,
                    })?,
                    synced.fingerprint,
                )
                .await?;
            } else {
                println!(
                    "{{\"address\":\"{}\",\"environmentId\":\"{}\",\"status\":\"ready\",\"workspace\":\"{}\"}}",
                    process.address(),
                    process.state().environment_id,
                    process.state().workspace_ref
                );
                wait_for_shutdown(&process).await?;
            }
            process.shutdown().await;
            Ok(())
        }
        CliCommand::ClientCreate {
            root,
            id,
            name,
            kind,
            scopes,
        } => {
            let manager = LocalIdentityManager::open(&root)
                .await
                .map_err(map_identity)?;
            let client = manager
                .create_client(
                    id.unwrap_or_else(runku_identity::ApplicationClientId::generate),
                    name,
                    kind,
                    scopes,
                    now()?,
                )
                .await
                .map_err(map_identity)?;
            emit_json(&ClientsWire {
                clients: vec![client_wire(&client)],
                configuration_revision: manager
                    .configuration_revision()
                    .await
                    .map_err(map_identity)?,
            })
        }
        CliCommand::ClientList { root } => {
            let manager = LocalIdentityManager::open(&root)
                .await
                .map_err(map_identity)?;
            let clients = manager.list_clients().await.map_err(map_identity)?;
            let wire = ClientsWire {
                clients: clients.iter().map(client_wire).collect(),
                configuration_revision: manager
                    .configuration_revision()
                    .await
                    .map_err(map_identity)?,
            };
            emit_json(&wire)
        }
        CliCommand::KeyCreate {
            root,
            id,
            client_id,
            label,
            scopes,
            expires_at,
        } => {
            let manager = LocalIdentityManager::open(&root)
                .await
                .map_err(map_identity)?;
            let created = manager
                .create_credential(
                    id.unwrap_or_else(runku_identity::CredentialId::generate),
                    client_id,
                    label,
                    scopes,
                    now()?,
                    expires_at,
                )
                .await
                .map_err(map_identity)?;
            emit_created_credential(&manager, &created).await
        }
        CliCommand::KeyList { root, client_id } => {
            let manager = LocalIdentityManager::open(&root)
                .await
                .map_err(map_identity)?;
            let credentials = manager
                .list_credentials(client_id)
                .await
                .map_err(map_identity)?;
            let wire = CredentialsWire {
                configuration_revision: manager
                    .configuration_revision()
                    .await
                    .map_err(map_identity)?,
                credentials: credentials.iter().map(credential_wire).collect(),
            };
            emit_json(&wire)
        }
        CliCommand::KeyReveal {
            root,
            client_id,
            credential_id,
        } => {
            let manager = LocalIdentityManager::open(&root)
                .await
                .map_err(map_identity)?;
            let revealed = manager
                .reveal_publishable(client_id, credential_id)
                .await
                .map_err(map_identity)?;
            emit_created_credential(&manager, &revealed).await
        }
        CliCommand::KeyRotate {
            root,
            client_id,
            source_id,
            replacement_id,
            label,
            expires_at,
        } => {
            let manager = LocalIdentityManager::open(&root)
                .await
                .map_err(map_identity)?;
            let replacement = manager
                .rotate_credential(
                    client_id,
                    source_id,
                    replacement_id.unwrap_or_else(runku_identity::CredentialId::generate),
                    label,
                    now()?,
                    expires_at,
                )
                .await
                .map_err(map_identity)?;
            emit_created_credential(&manager, &replacement).await
        }
        CliCommand::KeyRevoke {
            root,
            credential_id,
        } => execute_lifecycle(root, credential_id, false).await,
        CliCommand::KeyDelete {
            root,
            credential_id,
        } => execute_lifecycle(root, credential_id, true).await,
        CliCommand::WorkspaceSync {
            root,
            config,
            endpoint,
            workspace,
            token_environment,
            expected_head,
            create,
        } => {
            execute_workspace_sync(
                root,
                config,
                endpoint,
                workspace,
                token_environment,
                expected_head,
                create,
            )
            .await
        }
        CliCommand::WorkspaceFreeze {
            endpoint,
            release_id,
            against_release_id,
            token_environment,
        } => {
            execute_workspace_freeze(endpoint, release_id, against_release_id, token_environment)
                .await
        }
        CliCommand::WorkspaceKeyCreate {
            root,
            id,
            actor,
            label,
            expires_at,
        } => {
            let manager = LocalDevelopmentAccessManager::open(&root)
                .await
                .map_err(map_development_access)?;
            let created = manager
                .create_credential(
                    id.unwrap_or_else(runku_core::DevelopmentCredentialId::generate),
                    actor,
                    label,
                    now()?,
                    expires_at,
                )
                .await
                .map_err(map_development_access)?;
            emit_created_development_credential(&manager, &created).await
        }
        CliCommand::WorkspaceKeyList { root } => {
            let manager = LocalDevelopmentAccessManager::open(&root)
                .await
                .map_err(map_development_access)?;
            let credentials = manager
                .list_credentials()
                .await
                .map_err(map_development_access)?;
            emit_json(&DevelopmentCredentialsWire {
                configuration_revision: manager
                    .configuration_revision()
                    .await
                    .map_err(map_development_access)?,
                credentials: credentials
                    .iter()
                    .map(development_credential_wire)
                    .collect(),
            })
        }
        CliCommand::WorkspaceKeyRotate {
            root,
            source_id,
            replacement_id,
            label,
            expires_at,
        } => {
            let manager = LocalDevelopmentAccessManager::open(&root)
                .await
                .map_err(map_development_access)?;
            let created = manager
                .rotate_credential(
                    source_id,
                    replacement_id.unwrap_or_else(runku_core::DevelopmentCredentialId::generate),
                    label,
                    now()?,
                    expires_at,
                )
                .await
                .map_err(map_development_access)?;
            emit_created_development_credential(&manager, &created).await
        }
        CliCommand::WorkspaceKeyRevoke {
            root,
            credential_id,
        } => execute_development_lifecycle(root, credential_id, false).await,
        CliCommand::WorkspaceKeyDelete {
            root,
            credential_id,
        } => execute_development_lifecycle(root, credential_id, true).await,
    }
}

async fn ensure_local_development_project(root: &Path) -> Result<(), CliFailure> {
    let (workspace, listener) = match load_local(root).await {
        Ok((state, _)) => (state.workspace_ref, state.listen_address),
        Err(LocalStateError::InvalidPath | LocalStateError::InvalidState) => (
            DEFAULT_LOCAL_WORKSPACE.parse().map_err(|_| CliFailure {
                code: "LOCAL_BOOTSTRAP_DEFAULTS_INVALID",
                exit: EXIT_INTERNAL,
            })?,
            DEFAULT_LOCAL_LISTENER.parse().map_err(|_| CliFailure {
                code: "LOCAL_BOOTSTRAP_DEFAULTS_INVALID",
                exit: EXIT_INTERNAL,
            })?,
        ),
        Err(error) => return Err(map_state(error)),
    };
    initialize_local(root, workspace, listener, now()?)
        .await
        .map_err(map_state)?;
    Ok(())
}

const DEV_PUBLIC_CLIENT_ID: &str = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const DEV_SECRET_CLIENT_ID: &str = "app_01ARZ3NDEKTSV4RRFFQ69G5FAW";
const DEV_PUBLIC_CREDENTIAL_ID: &str = "crd_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const APPLICATION_ENV_MAX_BYTES: u64 = 64 * 1024;
const APPLICATION_ENV_COMMENT: &str =
    "# Managed by runku dev; remote values are never replaced silently.";

fn detect_public_env_prefix(root: &Path) -> Result<String, CliFailure> {
    const NEXT_CONFIGS: [&str; 4] = [
        "next.config.js",
        "next.config.mjs",
        "next.config.cjs",
        "next.config.ts",
    ];
    const VITE_CONFIGS: [&str; 6] = [
        "vite.config.js",
        "vite.config.mjs",
        "vite.config.cjs",
        "vite.config.ts",
        "vite.config.mts",
        "vite.config.cts",
    ];
    const SVELTE_CONFIGS: [&str; 4] = [
        "svelte.config.js",
        "svelte.config.mjs",
        "svelte.config.cjs",
        "svelte.config.ts",
    ];
    const VUE_CLI_CONFIGS: [&str; 4] = [
        "vue.config.js",
        "vue.config.mjs",
        "vue.config.cjs",
        "vue.config.ts",
    ];
    let next = NEXT_CONFIGS.iter().any(|file| root.join(file).is_file());
    let vite = VITE_CONFIGS.iter().any(|file| root.join(file).is_file());
    let svelte = SVELTE_CONFIGS.iter().any(|file| root.join(file).is_file());
    let vue_cli = VUE_CLI_CONFIGS.iter().any(|file| root.join(file).is_file());
    if (next && (vite || svelte || vue_cli)) || (vue_cli && (vite || svelte)) {
        return Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_AMBIGUOUS",
            exit: EXIT_CONFLICT,
        });
    }
    Ok(if next {
        "NEXT_PUBLIC_RUNKU_"
    } else if svelte {
        "PUBLIC_RUNKU_"
    } else if vue_cli {
        "VUE_APP_RUNKU_"
    } else if vite {
        "VITE_RUNKU_"
    } else {
        "RUNKU_"
    }
    .to_owned())
}

async fn reconcile_dev_application_env(
    root: &Path,
    relative: &Path,
    public_prefix: &str,
    replace_remote: bool,
) -> Result<(), CliFailure> {
    let path = safe_application_env_path(root, relative)?;
    let source = read_application_env(&path)?;
    let public_url_name = format!("{public_prefix}URL");
    let public_target_name = format!("{public_prefix}TARGET");
    let public_key_name = format!("{public_prefix}KEY");
    let managed_names = BTreeSet::from([
        public_url_name.clone(),
        public_target_name.clone(),
        public_key_name.clone(),
        "RUNKU_URL".to_owned(),
        "RUNKU_TARGET".to_owned(),
        "RUNKU_SECRET_KEY".to_owned(),
    ]);
    let assignments =
        effective_application_env_assignments(root, relative, &source, &managed_names)?;
    let state = load_local(root).await.map_err(map_state)?.0;
    let manager = LocalIdentityManager::open(root)
        .await
        .map_err(map_identity)?;
    let created_at = now()?;
    let invoke_scope: ApplicationScope = "functions:invoke".parse().map_err(|_| CliFailure {
        code: "LOCAL_APPLICATION_ENV_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    let expected_url = format!("http://{}", state.listen_address);
    let expected_target = format!("workspace:{}", state.workspace_ref);
    let url_is_foreign = [&public_url_name, "RUNKU_URL"].into_iter().any(|name| {
        assignments
            .get(name)
            .is_some_and(|value| !value.is_empty() && *value != expected_url)
    });
    let public_key = assignments
        .get(&public_key_name)
        .filter(|value| !value.is_empty());
    let secret_key = assignments
        .get("RUNKU_SECRET_KEY")
        .filter(|value| !value.is_empty());
    let public_is_local = application_key_is_local(
        &manager,
        public_key,
        ApplicationAssurance::Declared,
        &invoke_scope,
        created_at,
    )
    .await;
    let secret_is_local = application_key_is_local(
        &manager,
        secret_key,
        ApplicationAssurance::Verified,
        &invoke_scope,
        created_at,
    )
    .await;
    if url_is_foreign || !public_is_local || !secret_is_local {
        confirm_remote_replacement(replace_remote)?;
    }

    let (public_material, secret_material) = dev_application_key_material(
        &manager,
        public_key.filter(|_| public_is_local),
        secret_key.filter(|_| secret_is_local),
        &invoke_scope,
        created_at,
    )
    .await?;
    let managed_values = BTreeMap::from([
        (public_url_name, expected_url.clone()),
        (public_target_name, expected_target.clone()),
        (public_key_name, public_material),
        ("RUNKU_URL".to_owned(), expected_url),
        ("RUNKU_TARGET".to_owned(), expected_target),
        ("RUNKU_SECRET_KEY".to_owned(), secret_material),
    ]);
    write_application_env(&path, &source, &managed_values)
}

async fn application_key_is_local(
    manager: &LocalIdentityManager,
    key: Option<&String>,
    assurance: ApplicationAssurance,
    invoke_scope: &ApplicationScope,
    created_at: TimestampMicros,
) -> bool {
    let Some(key) = key else {
        return true;
    };
    manager
        .resolve_key(key, created_at)
        .await
        .is_ok_and(|context| {
            context.assurance == assurance && context.scopes.contains(invoke_scope)
        })
}

async fn dev_application_key_material(
    manager: &LocalIdentityManager,
    existing_public: Option<&String>,
    existing_secret: Option<&String>,
    invoke_scope: &ApplicationScope,
    created_at: TimestampMicros,
) -> Result<(String, String), CliFailure> {
    let public_client_id = ensure_dev_client(
        manager,
        DEV_PUBLIC_CLIENT_ID,
        "runku-dev-public",
        ClientKind::Public,
        invoke_scope,
        created_at,
    )
    .await?;
    let secret_client_id = ensure_dev_client(
        manager,
        DEV_SECRET_CLIENT_ID,
        "runku-dev-server",
        ClientKind::Confidential,
        invoke_scope,
        created_at,
    )
    .await?;
    let public_material = match existing_public {
        Some(key) => key.clone(),
        None => dev_publishable_key(manager, public_client_id, invoke_scope, created_at).await?,
    };
    let secret_material = match existing_secret {
        Some(key) => key.clone(),
        None => manager
            .create_credential(
                CredentialId::generate(),
                secret_client_id,
                "runku-dev-server".parse().map_err(|_| CliFailure {
                    code: "LOCAL_APPLICATION_ENV_INVALID",
                    exit: EXIT_INTERNAL,
                })?,
                BTreeSet::from([invoke_scope.clone()]),
                created_at,
                None,
            )
            .await
            .map_err(map_identity)?
            .key
            .expose()
            .to_owned(),
    };
    Ok((public_material, secret_material))
}

async fn ensure_dev_client(
    manager: &LocalIdentityManager,
    id: &str,
    name: &str,
    kind: ClientKind,
    invoke_scope: &ApplicationScope,
    created_at: TimestampMicros,
) -> Result<ApplicationClientId, CliFailure> {
    let id: ApplicationClientId = id.parse().map_err(|_| CliFailure {
        code: "LOCAL_APPLICATION_ENV_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    if let Some(existing) = manager
        .list_clients()
        .await
        .map_err(map_identity)?
        .into_iter()
        .find(|client| client.id == id)
    {
        if existing.kind != kind
            || existing.status != ApplicationClientStatus::Active
            || !existing.scope_ceiling.contains(invoke_scope)
        {
            return Err(CliFailure {
                code: "LOCAL_APPLICATION_ENV_CONFLICT",
                exit: EXIT_CONFLICT,
            });
        }
        return Ok(id);
    }
    manager
        .create_client(
            id,
            name.parse().map_err(|_| CliFailure {
                code: "LOCAL_APPLICATION_ENV_INVALID",
                exit: EXIT_INTERNAL,
            })?,
            kind,
            BTreeSet::from([invoke_scope.clone()]),
            created_at,
        )
        .await
        .map_err(map_identity)?;
    Ok(id)
}

async fn dev_publishable_key(
    manager: &LocalIdentityManager,
    client_id: ApplicationClientId,
    invoke_scope: &ApplicationScope,
    created_at: TimestampMicros,
) -> Result<String, CliFailure> {
    let preferred: CredentialId = DEV_PUBLIC_CREDENTIAL_ID.parse().map_err(|_| CliFailure {
        code: "LOCAL_APPLICATION_ENV_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    let credentials = manager
        .list_credentials(client_id)
        .await
        .map_err(map_identity)?;
    if credentials.iter().any(|existing| {
        existing.id == preferred
            && existing.kind == CredentialKind::Publishable
            && existing.status == CredentialStatus::Active
            && existing.scopes.contains(invoke_scope)
    }) {
        return manager
            .reveal_publishable(client_id, preferred)
            .await
            .map_err(map_identity)
            .map(|created| created.key.expose().to_owned());
    }
    let id = if credentials
        .iter()
        .any(|credential| credential.id == preferred)
    {
        CredentialId::generate()
    } else {
        preferred
    };
    manager
        .create_credential(
            id,
            client_id,
            "runku-dev-public".parse().map_err(|_| CliFailure {
                code: "LOCAL_APPLICATION_ENV_INVALID",
                exit: EXIT_INTERNAL,
            })?,
            BTreeSet::from([invoke_scope.clone()]),
            created_at,
            None,
        )
        .await
        .map_err(map_identity)
        .map(|created| created.key.expose().to_owned())
}

fn safe_application_env_path(root: &Path, relative: &Path) -> Result<PathBuf, CliFailure> {
    if relative.as_os_str().is_empty()
        || relative.as_os_str().as_encoded_bytes().len() > 512
        || relative.is_absolute()
        || !relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_PATH_INVALID",
            exit: EXIT_INVALID,
        });
    }
    let root = std::fs::canonicalize(root).map_err(|_| CliFailure {
        code: "LOCAL_APPLICATION_ENV_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    let mut ancestor = root.clone();
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            let Component::Normal(component) = component else {
                return Err(CliFailure {
                    code: "LOCAL_APPLICATION_ENV_PATH_INVALID",
                    exit: EXIT_INVALID,
                });
            };
            ancestor.push(component);
            let metadata = std::fs::symlink_metadata(&ancestor).map_err(|_| CliFailure {
                code: "LOCAL_APPLICATION_ENV_PATH_INVALID",
                exit: EXIT_INVALID,
            })?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(CliFailure {
                    code: "LOCAL_APPLICATION_ENV_PATH_INVALID",
                    exit: EXIT_INVALID,
                });
            }
        }
    }
    if !std::fs::canonicalize(&ancestor).is_ok_and(|parent| parent.starts_with(&root)) {
        return Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_PATH_INVALID",
            exit: EXIT_INVALID,
        });
    }
    let path = root.join(relative);
    let parent = path.parent().ok_or(CliFailure {
        code: "LOCAL_APPLICATION_ENV_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    if std::fs::symlink_metadata(parent)
        .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
        && (!path.exists()
            || std::fs::symlink_metadata(&path)
                .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink()))
    {
        Ok(path)
    } else {
        Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_PATH_INVALID",
            exit: EXIT_INVALID,
        })
    }
}

fn read_application_env(path: &Path) -> Result<String, CliFailure> {
    if !path.exists() {
        return Ok(String::new());
    }
    if std::fs::metadata(path).map_or(true, |metadata| metadata.len() > APPLICATION_ENV_MAX_BYTES) {
        return Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_INVALID",
            exit: EXIT_INVALID,
        });
    }
    std::fs::read_to_string(path).map_err(|_| CliFailure {
        code: "LOCAL_APPLICATION_ENV_INVALID",
        exit: EXIT_INVALID,
    })
}

fn parse_application_env(
    source: &str,
    managed_names: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>, CliFailure> {
    let mut assignments = BTreeMap::new();
    for (name, value) in source.lines().filter_map(|line| line.split_once('=')) {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            continue;
        }
        if assignments
            .insert(name.to_owned(), value.to_owned())
            .is_some()
            && managed_names.contains(name)
        {
            return Err(CliFailure {
                code: "LOCAL_APPLICATION_ENV_INVALID",
                exit: EXIT_INVALID,
            });
        }
    }
    Ok(assignments)
}

fn effective_application_env_assignments(
    root: &Path,
    relative: &Path,
    source: &str,
    managed_names: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>, CliFailure> {
    let mut assignments = BTreeMap::new();
    if relative == Path::new(".env.local") {
        let fallback = safe_application_env_path(root, Path::new(".env"))?;
        let fallback_source = read_application_env(&fallback)?;
        assignments.extend(parse_application_env(&fallback_source, managed_names)?);
    }
    assignments.extend(parse_application_env(source, managed_names)?);
    Ok(assignments)
}

fn confirm_remote_replacement(explicit: bool) -> Result<(), CliFailure> {
    if explicit {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        return Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_CONFIRMATION_REQUIRED",
            exit: EXIT_CONFLICT,
        });
    }
    eprint!(
        "The application configuration targets another Environment. Replace its URL, target, and keys with local credentials? [y/N] "
    );
    std::io::stderr().flush().map_err(|_| CliFailure {
        code: "LOCAL_APPLICATION_ENV_UNAVAILABLE",
        exit: EXIT_UNAVAILABLE,
    })?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .map_err(|_| CliFailure {
            code: "LOCAL_APPLICATION_ENV_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })?;
    if matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        Ok(())
    } else {
        Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_PRESERVED",
            exit: EXIT_CONFLICT,
        })
    }
}

fn write_application_env(
    path: &Path,
    source: &str,
    managed: &BTreeMap<String, String>,
) -> Result<(), CliFailure> {
    let mut output = source
        .lines()
        .filter(|line| {
            *line != APPLICATION_ENV_COMMENT
                && line
                    .split_once('=')
                    .is_none_or(|(name, _)| !managed.contains_key(name))
        })
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    while output.last().is_some_and(String::is_empty) {
        output.pop();
    }
    if !output.is_empty() {
        output.push(String::new());
    }
    output.push(APPLICATION_ENV_COMMENT.to_owned());
    output.extend(
        managed
            .iter()
            .map(|(name, value)| format!("{name}={value}")),
    );
    output.push(String::new());
    let bytes = output.join("\n");
    if bytes.len() as u64 > APPLICATION_ENV_MAX_BYTES {
        return Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_INVALID",
            exit: EXIT_INVALID,
        });
    }
    let temporary = path.with_extension(format!("runku-{}.tmp", std::process::id()));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(|_| CliFailure {
        code: "LOCAL_APPLICATION_ENV_UNAVAILABLE",
        exit: EXIT_UNAVAILABLE,
    })?;
    let result = (|| {
        file.write_all(bytes.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ignored = std::fs::remove_file(&temporary);
        return Err(CliFailure {
            code: "LOCAL_APPLICATION_ENV_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|_| {
            CliFailure {
                code: "LOCAL_APPLICATION_ENV_UNAVAILABLE",
                exit: EXIT_UNAVAILABLE,
            }
        })?;
    }
    Ok(())
}

fn emit_release_outcome(result: &LocalReleaseOutcome) -> Result<(), CliFailure> {
    emit_json(&ReleaseOutcomeWire {
        channel: result.channel.as_ref().map(ToString::to_string),
        compatible: result.diagnostics.is_empty(),
        diagnostics: result
            .diagnostics
            .iter()
            .map(|diagnostic| ReleaseDiagnosticWire {
                code: diagnostic.code,
                subject: &diagnostic.subject,
            })
            .collect(),
        release_id: result.release_id.to_string(),
        replayed: result.replayed,
        serving_revision: result.serving_revision,
        status: result.status.as_str(),
    })?;
    if result.diagnostics.is_empty() {
        Ok(())
    } else {
        Err(CliFailure {
            code: "RELEASE_COMPATIBILITY_BLOCKED",
            exit: EXIT_CONFLICT,
        })
    }
}

fn emit_release_status(report: &LocalReleaseStatusReport) -> Result<(), CliFailure> {
    emit_json(&ReleaseStatusWire {
        channels: report
            .channels
            .iter()
            .map(|channel| ChannelStatusWire {
                channel: channel.channel.to_string(),
                default: channel.default,
                release_id: channel.release_id.to_string(),
            })
            .collect(),
        default_channel: report.default_channel.as_ref().map(ToString::to_string),
        releases: report
            .releases
            .iter()
            .map(|release| ReleaseStatusEntryWire {
                release_id: release.release_id.to_string(),
                runtime_version: release.runtime_version.clone(),
                status: release.status.as_str(),
            })
            .collect(),
        serving_revision: report.serving_revision,
    })
}

async fn emit_created_credential(
    manager: &LocalIdentityManager,
    created: &runku_local::LocalCreatedCredential,
) -> Result<(), CliFailure> {
    let secret = created.credential.kind == CredentialKind::Secret;
    emit_json(&CreatedCredentialWire {
        configuration_revision: manager
            .configuration_revision()
            .await
            .map_err(map_identity)?,
        credential: credential_wire(&created.credential),
        key: created.key.expose(),
        recoverable: !secret,
        secret_shown_once: secret,
    })
}

async fn execute_lifecycle(
    root: PathBuf,
    credential_id: runku_identity::CredentialId,
    delete: bool,
) -> Result<(), CliFailure> {
    let manager = LocalIdentityManager::open(&root)
        .await
        .map_err(map_identity)?;
    let outcome = if delete {
        manager.delete_credential(credential_id, now()?).await
    } else {
        manager.revoke_credential(credential_id, now()?).await
    }
    .map_err(map_identity)?;
    emit_json(&LifecycleWire {
        configuration_revision: manager
            .configuration_revision()
            .await
            .map_err(map_identity)?,
        credential_id: credential_id.to_string(),
        replayed: outcome == CredentialLifecycleResult::Replayed,
        status: if delete { "deleted" } else { "revoked" },
    })
}

async fn emit_created_development_credential(
    manager: &LocalDevelopmentAccessManager,
    created: &runku_local::LocalCreatedDevelopmentCredential,
) -> Result<(), CliFailure> {
    emit_json(&CreatedDevelopmentCredentialWire {
        configuration_revision: manager
            .configuration_revision()
            .await
            .map_err(map_development_access)?,
        credential: development_credential_wire(&created.credential),
        key: created.key.expose(),
        recoverable: false,
        secret_shown_once: true,
    })
}

async fn execute_development_lifecycle(
    root: PathBuf,
    credential_id: runku_core::DevelopmentCredentialId,
    delete: bool,
) -> Result<(), CliFailure> {
    let manager = LocalDevelopmentAccessManager::open(&root)
        .await
        .map_err(map_development_access)?;
    let outcome = if delete {
        manager.delete_credential(credential_id, now()?).await
    } else {
        manager.revoke_credential(credential_id, now()?).await
    }
    .map_err(map_development_access)?;
    emit_json(&DevelopmentLifecycleWire {
        configuration_revision: manager
            .configuration_revision()
            .await
            .map_err(map_development_access)?,
        credential_id: credential_id.to_string(),
        replayed: outcome == DevelopmentLifecycleResult::Replayed,
        status: if delete { "deleted" } else { "revoked" },
    })
}

fn development_credential_wire(
    credential: &LocalDevelopmentCredentialMetadata,
) -> DevelopmentCredentialWire<'_> {
    DevelopmentCredentialWire {
        actor: credential.actor.as_str(),
        created_at_micros: credential.created_at.get().to_string(),
        credential_id: credential.id.to_string(),
        expires_at_micros: credential
            .expires_at
            .map(|timestamp| timestamp.get().to_string()),
        label: credential.label.as_str(),
        revoked_at_micros: credential
            .revoked_at
            .map(|timestamp| timestamp.get().to_string()),
        status: match credential.status {
            DevelopmentCredentialStatus::Active => "active",
            DevelopmentCredentialStatus::Revoked => "revoked",
            DevelopmentCredentialStatus::Deleted => "deleted",
        },
    }
}

fn client_wire(client: &ApplicationClient) -> ClientWire<'_> {
    ClientWire {
        client_id: client.id.to_string(),
        created_at_micros: client.created_at.get().to_string(),
        kind: match client.kind {
            ClientKind::Public => "public",
            ClientKind::Confidential => "confidential",
        },
        name: client.name.as_str(),
        scopes: client
            .scope_ceiling
            .iter()
            .map(runku_identity::ApplicationScope::as_str)
            .collect(),
        status: match client.status {
            ApplicationClientStatus::Active => "active",
            ApplicationClientStatus::Disabled => "disabled",
        },
    }
}

fn credential_wire(credential: &LocalCredentialMetadata) -> CredentialWire<'_> {
    CredentialWire {
        client_id: credential.client_id.to_string(),
        created_at_micros: credential.created_at.get().to_string(),
        credential_id: credential.id.to_string(),
        expires_at_micros: credential
            .expires_at
            .map(|timestamp| timestamp.get().to_string()),
        kind: match credential.kind {
            CredentialKind::Publishable => "publishable",
            CredentialKind::Secret => "secret",
        },
        label: credential.label.as_str(),
        revoked_at_micros: credential
            .revoked_at
            .map(|timestamp| timestamp.get().to_string()),
        scopes: credential
            .scopes
            .iter()
            .map(runku_identity::ApplicationScope::as_str)
            .collect(),
        status: match credential.status {
            CredentialStatus::Active => "active",
            CredentialStatus::Revoked => "revoked",
            CredentialStatus::Deleted => "deleted",
        },
    }
}

fn emit_json<T: Serialize>(value: &T) -> Result<(), CliFailure> {
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, value).map_err(|_| CliFailure {
        code: "CLI_OUTPUT_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    output.write_all(b"\n").map_err(|_| CliFailure {
        code: "CLI_OUTPUT_UNAVAILABLE",
        exit: EXIT_UNAVAILABLE,
    })
}

async fn execute_workspace_freeze(
    endpoint: DevelopmentEndpoint,
    release_id: runku_core::ReleaseId,
    against_release_id: Option<runku_core::ReleaseId>,
    token_environment: TokenEnvironmentName,
) -> Result<(), CliFailure> {
    let token = std::env::var(token_environment.as_str()).map_err(|_| CliFailure {
        code: "DEVELOPMENT_AUTH_ENV_INVALID",
        exit: EXIT_AUTH,
    })?;
    let client = DevelopmentClient::new(endpoint, token, DevelopmentClientConfig::default())
        .map_err(map_development_client)?;
    let response = client
        .freeze(&DevelopmentFreezeRequestV1 {
            operation_id: derive_development_freeze_request_operation_id_v1(
                release_id,
                against_release_id,
            ),
            release_id,
            against_release_id,
        })
        .await
        .map_err(map_development_client)?;
    let blocked = response.outcome == DevelopmentFreezeOutcomeV1::CompatibilityBlocked;
    emit_json(&RemoteWorkspaceFreezeWire {
        event_version: 1,
        stage: "freeze",
        request_id: response.request_id.to_string(),
        release_id: response.release_id.to_string(),
        outcome: if blocked {
            "compatibility_blocked"
        } else {
            "servable"
        },
        diagnostics: response
            .diagnostics
            .into_iter()
            .map(|diagnostic| RemoteWorkspaceFreezeDiagnosticWire {
                code: diagnostic.code,
                subject: diagnostic.subject,
            })
            .collect(),
        serving_revision: response.serving_revision,
        replayed: response.replayed,
    })?;
    if blocked {
        Err(CliFailure {
            code: "DEVELOPMENT_COMPATIBILITY_BLOCKED",
            exit: EXIT_POLICY,
        })
    } else {
        Ok(())
    }
}

#[allow(
    clippy::option_option,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
async fn execute_workspace_sync(
    root: PathBuf,
    config: PathBuf,
    endpoint: DevelopmentEndpoint,
    workspace: WorkspaceRef,
    token_environment: TokenEnvironmentName,
    expected_head: Option<Option<runku_core::DevRevisionId>>,
    create: bool,
) -> Result<(), CliFailure> {
    const STABLE_ATTEMPTS: usize = 4;

    let root = std::fs::canonicalize(root).map_err(|_| CliFailure {
        code: "BUILD_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    if !root.is_dir() {
        return Err(CliFailure {
            code: "BUILD_PATH_INVALID",
            exit: EXIT_INVALID,
        });
    }
    let token = std::env::var(token_environment.as_str()).map_err(|_| CliFailure {
        code: "DEVELOPMENT_AUTH_ENV_INVALID",
        exit: EXIT_AUTH,
    })?;
    let client = DevelopmentClient::new(endpoint, token, DevelopmentClientConfig::default())
        .map_err(map_development_client)?;
    let state_request = DevelopmentStateRequestV1 {
        workspace_ref: workspace.clone(),
    };
    let state = client
        .state(&state_request)
        .await
        .map_err(map_development_client)?;
    let observed_head = state
        .workspace
        .as_ref()
        .and_then(|binding| binding.head_revision);
    emit_json(&RemoteWorkspaceStateWire {
        event_version: 1,
        stage: "state",
        request_id: state.request_id.to_string(),
        project_id: state.scope.project_id().to_string(),
        environment_id: state.scope.environment_id().to_string(),
        workspace: workspace.to_string(),
        exists: state.workspace.is_some(),
        head: observed_head.map(|revision| revision.to_string()),
    })?;

    if state.workspace.is_none() {
        if !create {
            return Err(CliFailure {
                code: "DEVELOPMENT_WORKSPACE_ABSENT",
                exit: EXIT_INVALID,
            });
        }
        let request = DevelopmentCreateWorkspaceRequestV1 {
            operation_id: OperationId::generate(),
            workspace_id: WorkspaceId::generate(),
            workspace_ref: workspace.clone(),
        };
        match client.create_workspace(&request).await {
            Ok(created) => emit_json(&RemoteWorkspaceCreateWire {
                event_version: 1,
                stage: "create",
                request_id: created.request_id.to_string(),
                workspace: workspace.to_string(),
                workspace_id: created.workspace.workspace_id.to_string(),
                replayed: created.replayed,
                reconciled: false,
            })?,
            Err(DevelopmentClientError::ResultUncertain) => {
                let reconciled = client.state(&state_request).await.map_err(|_| CliFailure {
                    code: DevelopmentClientError::ResultUncertain.code(),
                    exit: EXIT_UNCERTAIN,
                })?;
                match reconciled.workspace {
                    Some(binding) if binding.workspace_id == request.workspace_id => {
                        emit_json(&RemoteWorkspaceCreateWire {
                            event_version: 1,
                            stage: "create",
                            request_id: reconciled.request_id.to_string(),
                            workspace: workspace.to_string(),
                            workspace_id: binding.workspace_id.to_string(),
                            replayed: true,
                            reconciled: true,
                        })?;
                    }
                    Some(_) => {
                        return Err(map_development_client(DevelopmentClientError::Conflict));
                    }
                    None => {
                        return Err(map_development_client(
                            DevelopmentClientError::ResultUncertain,
                        ));
                    }
                }
            }
            Err(error) => return Err(map_development_client(error)),
        }
    }

    let mut built = None;
    for _ in 0..STABLE_ATTEMPTS {
        let before = source_fingerprint(&root, &config).map_err(map_build)?;
        let output = build_project(
            &root,
            &config,
            state.scope.project_id(),
            BuildMetadata::generate(now()?),
        )
        .map_err(map_build)?;
        let after = source_fingerprint(&root, &config).map_err(map_build)?;
        if before == output.source_fingerprint && output.source_fingerprint == after {
            built = Some(output);
            break;
        }
    }
    let output = built.ok_or(CliFailure {
        code: "SOURCE_SNAPSHOT_UNSTABLE",
        exit: EXIT_CONFLICT,
    })?;
    emit_json(&RemoteWorkspaceBuildWire {
        event_version: 1,
        stage: "build",
        build_id: output.build_id.to_string(),
        release_id: output.release_id.to_string(),
        replayed: output.replayed,
    })?;
    let manifest_bytes = read_bounded(&output.manifest_path, MANIFEST_MAX_BYTES).await?;
    let artifact_bytes = read_bounded(&output.artifact_path, ARTIFACT_MAX_BYTES).await?;
    let manifest = decode_release_manifest(&manifest_bytes).map_err(|_| CliFailure {
        code: "BUILD_OUTPUT_CORRUPT",
        exit: EXIT_CORRUPT,
    })?;
    let operation_id = OperationId::generate();
    let manifest_digest = Sha256Digest::of(&manifest_bytes);
    let revision_id =
        derive_development_revision_id_v1(state.scope, operation_id, &workspace, manifest_digest);
    let request = DevelopmentPublishRequestV1 {
        operation_id,
        project_id: state.scope.project_id(),
        workspace_ref: workspace.clone(),
        expected_head: expected_head.unwrap_or(observed_head),
        manifest,
        manifest_bytes,
        artifact_bytes,
    };
    match client.publish(&request).await {
        Ok(published) => emit_json(&RemoteWorkspacePublishWire {
            event_version: 1,
            stage: "publish",
            request_id: published.request_id.to_string(),
            revision_id: published.revision_id.to_string(),
            release_id: published.release_id.to_string(),
            replayed: published.replayed,
            reconciled: false,
        }),
        Err(DevelopmentClientError::ResultUncertain) => {
            let reconciled = client.state(&state_request).await.map_err(|_| CliFailure {
                code: DevelopmentClientError::ResultUncertain.code(),
                exit: EXIT_UNCERTAIN,
            })?;
            if reconciled
                .workspace
                .as_ref()
                .and_then(|binding| binding.head_revision)
                != Some(revision_id)
            {
                return Err(map_development_client(
                    DevelopmentClientError::ResultUncertain,
                ));
            }
            emit_json(&RemoteWorkspacePublishWire {
                event_version: 1,
                stage: "publish",
                request_id: reconciled.request_id.to_string(),
                revision_id: revision_id.to_string(),
                release_id: request.manifest.release_id.to_string(),
                replayed: true,
                reconciled: true,
            })
        }
        Err(error) => Err(map_development_client(error)),
    }
}

async fn execute_build(
    root: PathBuf,
    config: PathBuf,
    metadata: Option<BuildMetadata>,
) -> Result<(), CliFailure> {
    let state = load_local(&root).await.map_err(map_state)?.0;
    let metadata = match metadata {
        Some(metadata) => metadata,
        None => BuildMetadata::generate(now()?),
    };
    let output = build_project(&root, &config, state.project_id, metadata).map_err(map_build)?;
    let manifest_path = output.manifest_path.to_str().ok_or(CliFailure {
        code: "BUILD_OUTPUT_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    let artifact_path = output.artifact_path.to_str().ok_or(CliFailure {
        code: "BUILD_OUTPUT_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    let generated_types_path = output.generated_types_path.to_str().ok_or(CliFailure {
        code: "BUILD_OUTPUT_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    let stable_generated_types_path =
        output
            .stable_generated_types_path
            .to_str()
            .ok_or(CliFailure {
                code: "BUILD_OUTPUT_PATH_INVALID",
                exit: EXIT_INVALID,
            })?;
    let json = serde_json::to_string(&BuildOutputWire {
        artifact_digest: output.artifact_digest.to_string(),
        artifact_path,
        build_id: output.build_id.to_string(),
        generated_types_digest: output.generated_types_digest.to_string(),
        generated_types_path,
        stable_generated_types_path,
        manifest_digest: output.manifest_digest.to_string(),
        manifest_path,
        release_id: output.release_id.to_string(),
        replayed: output.replayed,
    })
    .map_err(|_| CliFailure {
        code: "BUILD_OUTPUT_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    println!("{json}");
    Ok(())
}

async fn sync_source(root: &Path, config: &Path) -> Result<SourceSyncOutput, CliFailure> {
    const STABLE_ATTEMPTS: usize = 4;

    let state = load_local(root).await.map_err(map_state)?.0;
    let actor: DevelopmentActor = "local-source-watch".parse().map_err(|_| CliFailure {
        code: "SOURCE_WATCH_ACTOR_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    for _ in 0..STABLE_ATTEMPTS {
        let before = source_fingerprint(root, config).map_err(map_build)?;
        let output = build_project(
            root,
            config,
            state.project_id,
            BuildMetadata::generate(now()?),
        )
        .map_err(map_build)?;
        let after = source_fingerprint(root, config).map_err(map_build)?;
        if before != output.source_fingerprint || output.source_fingerprint != after {
            continue;
        }
        let manifest = read_bounded(&output.manifest_path, MANIFEST_MAX_BYTES).await?;
        let artifact = read_bounded(&output.artifact_path, ARTIFACT_MAX_BYTES).await?;
        let published = publish_local(root, &state.workspace_ref, &actor, &manifest, &artifact)
            .await
            .map_err(map_publish)?;
        return Ok(SourceSyncOutput {
            fingerprint: after.to_string(),
            release_id: published.release_id.to_string(),
            revision_id: published.revision_id.to_string(),
        });
    }
    Err(CliFailure {
        code: "SOURCE_SNAPSHOT_UNSTABLE",
        exit: EXIT_CONFLICT,
    })
}

async fn wait_for_watch_shutdown(
    process: &LocalProcess,
    root: &Path,
    config: &Path,
    mut observed: String,
) -> Result<(), CliFailure> {
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut unavailable_retry_at = std::time::Instant::now();
    let mut last_reported: Option<(String, &'static str)> = None;
    loop {
        tokio::select! {
            result = &mut signal => return result.map_err(|_| CliFailure {
                code: "LOCAL_SIGNAL_UNAVAILABLE",
                exit: EXIT_INTERNAL,
            }),
            _ = interval.tick() => {
                if !process.is_ready() {
                    return Err(CliFailure {
                        code: "LOCAL_PROCESS_STOPPED",
                        exit: EXIT_UNAVAILABLE,
                    });
                }
                let fingerprint = match source_fingerprint(root, config) {
                    Ok(fingerprint) => fingerprint.to_string(),
                    Err(error) => {
                        let failure = map_build(error);
                        report_watch_error("unavailable", failure, &mut last_reported)?;
                        continue;
                    }
                };
                if fingerprint == observed || std::time::Instant::now() < unavailable_retry_at {
                    continue;
                }
                match sync_source(root, config).await {
                    Ok(synced) => {
                        emit_json(&WatchEventWire {
                            event_version: 1,
                            status: "synced",
                            fingerprint: &synced.fingerprint,
                            code: None,
                            release_id: Some(&synced.release_id),
                            revision_id: Some(&synced.revision_id),
                        })?;
                        observed = synced.fingerprint;
                        last_reported = None;
                    }
                    Err(failure) => {
                        report_watch_error(&fingerprint, failure, &mut last_reported)?;
                        if failure.exit == EXIT_UNAVAILABLE {
                            unavailable_retry_at = std::time::Instant::now()
                                + std::time::Duration::from_secs(2);
                        } else if failure.code != "SOURCE_SNAPSHOT_UNSTABLE" {
                            observed = fingerprint;
                        }
                    }
                }
            }
        }
    }
}

fn report_watch_error(
    fingerprint: &str,
    failure: CliFailure,
    last_reported: &mut Option<(String, &'static str)>,
) -> Result<(), CliFailure> {
    if last_reported
        .as_ref()
        .is_some_and(|(previous, code)| previous == fingerprint && *code == failure.code)
    {
        return Ok(());
    }
    emit_json(&WatchEventWire {
        event_version: 1,
        status: "build-error",
        fingerprint,
        code: Some(failure.code),
        release_id: None,
        revision_id: None,
    })?;
    *last_reported = Some((fingerprint.to_owned(), failure.code));
    Ok(())
}

async fn wait_for_shutdown(process: &LocalProcess) -> Result<(), CliFailure> {
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        tokio::select! {
            result = &mut signal => return result.map_err(|_| CliFailure {
                code: "LOCAL_SIGNAL_UNAVAILABLE",
                exit: EXIT_INTERNAL,
            }),
            _ = interval.tick() => {
                if !process.is_ready() {
                    return Err(CliFailure {
                        code: "LOCAL_PROCESS_STOPPED",
                        exit: EXIT_UNAVAILABLE,
                    });
                }
            }
        }
    }
}

async fn emit_logs(
    manager: &LocalLogManager,
    mut query: LogQuery,
    follow: bool,
) -> Result<(), CliFailure> {
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    loop {
        let page = manager.query(&query).await.map_err(map_logs)?;
        let count = page.records.len();
        for record in &page.records {
            emit_operational_log(record)?;
        }
        query.after = page.next;
        if !follow {
            return Ok(());
        }
        if count == usize::from(query.limit) {
            tokio::task::yield_now().await;
            continue;
        }
        tokio::select! {
            result = &mut signal => return result.map_err(|_| CliFailure {
                code: "LOCAL_SIGNAL_UNAVAILABLE",
                exit: EXIT_INTERNAL,
            }),
            () = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
        }
    }
}

fn emit_operational_log(record: &SequencedOperationalEvent) -> Result<(), CliFailure> {
    let event = &record.event;
    let fields = event
        .fields
        .as_ref()
        .map(WireValueV1::from_canonical)
        .transpose()
        .map_err(|_| CliFailure {
            code: "LOCAL_LOG_CORRUPT",
            exit: EXIT_CORRUPT,
        })?;
    emit_json(&OperationalLogWire {
        cursor: record.cursor.to_string(),
        event_id: event.id.to_string(),
        occurred_at_micros: event.occurred_at.get().to_string(),
        project_id: event.scope.project_id().to_string(),
        environment_id: event.scope.environment_id().to_string(),
        request_id: event.request_id.to_string(),
        invocation_id: event.invocation_id.to_string(),
        parent_invocation_id: event.parent_invocation_id.map(|value| value.to_string()),
        release_id: event.release_id.to_string(),
        dev_revision_id: event.dev_revision_id.map(|value| value.to_string()),
        function_id: event.function_id.to_string(),
        function_name: event.function_name.to_string(),
        function_type: match event.function_type {
            FunctionType::Query => "query",
            FunctionType::Mutation => "mutation",
            FunctionType::Action => "action",
        },
        client_id: event.client_id.map(|value| value.to_string()),
        credential_id: event.credential_id.map(|value| value.to_string()),
        principal_kind: event.principal_kind.as_str(),
        stream: event.stream.as_str(),
        level: event.level.as_str(),
        event_kind: event.kind.as_str(),
        message: event.message.as_ref().map(ToString::to_string),
        fields,
        duration_micros: event.duration_micros.map(|value| value.to_string()),
        outcome_code: event
            .outcome_code
            .as_ref()
            .map(|value| value.as_str().to_owned()),
    })
}

async fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, CliFailure> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| CliFailure {
            code: "LOCAL_PACKAGE_FILE_INVALID",
            exit: EXIT_INVALID,
        })?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len()
            > u64::try_from(maximum).map_err(|_| CliFailure {
                code: "LOCAL_PACKAGE_FILE_INVALID",
                exit: EXIT_INVALID,
            })?
    {
        return Err(CliFailure {
            code: "LOCAL_PACKAGE_FILE_INVALID",
            exit: EXIT_INVALID,
        });
    }
    tokio::fs::read(path).await.map_err(|_| CliFailure {
        code: "LOCAL_PACKAGE_FILE_UNAVAILABLE",
        exit: EXIT_UNAVAILABLE,
    })
}

fn now() -> Result<TimestampMicros, CliFailure> {
    let elapsed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| CliFailure {
            code: "LOCAL_CLOCK_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })?;
    i64::try_from(elapsed.as_micros())
        .map(TimestampMicros::new)
        .map_err(|_| CliFailure {
            code: "LOCAL_CLOCK_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })
}

fn map_state(error: LocalStateError) -> CliFailure {
    let exit = match error {
        LocalStateError::Conflict => EXIT_CONFLICT,
        LocalStateError::Unavailable => EXIT_UNAVAILABLE,
        LocalStateError::Corruption => EXIT_CORRUPT,
        LocalStateError::InvalidPath | LocalStateError::InvalidState => EXIT_INVALID,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_publish(error: LocalPublishError) -> CliFailure {
    let exit = match error {
        LocalPublishError::Conflict => EXIT_CONFLICT,
        LocalPublishError::Unavailable => EXIT_UNAVAILABLE,
        LocalPublishError::Corruption => EXIT_CORRUPT,
        LocalPublishError::InvalidState
        | LocalPublishError::InvalidPackage
        | LocalPublishError::ProjectMismatch => EXIT_INVALID,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_doctor(error: LocalDoctorError) -> CliFailure {
    let exit = match error {
        LocalDoctorError::InvalidState => EXIT_INVALID,
        LocalDoctorError::Unavailable => EXIT_UNAVAILABLE,
        LocalDoctorError::Inconsistent => EXIT_CORRUPT,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_logs(error: LocalLogError) -> CliFailure {
    let exit = match error {
        LocalLogError::InvalidState | LocalLogError::InvalidRequest => EXIT_INVALID,
        LocalLogError::Unavailable => EXIT_UNAVAILABLE,
        LocalLogError::Corruption => EXIT_CORRUPT,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_otlp(error: LocalOtlpError) -> CliFailure {
    let exit = match error {
        LocalOtlpError::InvalidConfiguration
        | LocalOtlpError::InvalidState
        | LocalOtlpError::Rejected => EXIT_INVALID,
        LocalOtlpError::AlreadyRunning | LocalOtlpError::ConfigurationDrift => EXIT_CONFLICT,
        LocalOtlpError::Unavailable => EXIT_UNAVAILABLE,
        LocalOtlpError::Corruption => EXIT_CORRUPT,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_process(error: LocalProcessError) -> CliFailure {
    let exit = match error {
        LocalProcessError::InvalidConfiguration | LocalProcessError::InvalidState => EXIT_INVALID,
        LocalProcessError::AlreadyRunning => EXIT_CONFLICT,
        LocalProcessError::Composition | LocalProcessError::ListenerUnavailable => EXIT_UNAVAILABLE,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_identity(error: LocalIdentityError) -> CliFailure {
    let exit = match error {
        LocalIdentityError::InvalidState
        | LocalIdentityError::InvalidInput
        | LocalIdentityError::NotFound => EXIT_INVALID,
        LocalIdentityError::Conflict => EXIT_CONFLICT,
        LocalIdentityError::Unavailable
        | LocalIdentityError::ResultUncertain
        | LocalIdentityError::EntropyUnavailable => EXIT_UNAVAILABLE,
        LocalIdentityError::Corruption => EXIT_CORRUPT,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_development_access(error: LocalDevelopmentAccessError) -> CliFailure {
    let exit = match error {
        LocalDevelopmentAccessError::InvalidState
        | LocalDevelopmentAccessError::InvalidInput
        | LocalDevelopmentAccessError::NotFound => EXIT_INVALID,
        LocalDevelopmentAccessError::Conflict => EXIT_CONFLICT,
        LocalDevelopmentAccessError::Unavailable | LocalDevelopmentAccessError::ResultUncertain => {
            EXIT_UNAVAILABLE
        }
        LocalDevelopmentAccessError::Corruption => EXIT_CORRUPT,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_build(error: BuildError) -> CliFailure {
    let exit = match error {
        BuildError::Conflict => EXIT_CONFLICT,
        BuildError::Unavailable => EXIT_UNAVAILABLE,
        BuildError::Corruption => EXIT_CORRUPT,
        BuildError::InvalidPath
        | BuildError::InvalidConfig
        | BuildError::Unsupported
        | BuildError::SourceSyntax
        | BuildError::SourcePolicy
        | BuildError::LimitExceeded => EXIT_INVALID,
        BuildError::Internal => EXIT_INTERNAL,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_development_client(error: DevelopmentClientError) -> CliFailure {
    let exit = match error {
        DevelopmentClientError::InvalidConfig
        | DevelopmentClientError::Unauthenticated
        | DevelopmentClientError::Forbidden => EXIT_AUTH,
        DevelopmentClientError::PolicyDenied => EXIT_POLICY,
        DevelopmentClientError::Conflict => EXIT_CONFLICT,
        DevelopmentClientError::Busy | DevelopmentClientError::Unavailable => EXIT_UNAVAILABLE,
        DevelopmentClientError::ResultUncertain => EXIT_UNCERTAIN,
        DevelopmentClientError::Corruption | DevelopmentClientError::InvalidResponse => {
            EXIT_CORRUPT
        }
        DevelopmentClientError::InvalidRequest
        | DevelopmentClientError::NotFound
        | DevelopmentClientError::LimitExceeded => EXIT_INVALID,
        DevelopmentClientError::Internal => EXIT_INTERNAL,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

fn map_release(error: LocalReleaseError) -> CliFailure {
    let exit = match error {
        LocalReleaseError::InvalidRequest | LocalReleaseError::NotFound => EXIT_INVALID,
        LocalReleaseError::Conflict => EXIT_CONFLICT,
        LocalReleaseError::Unavailable => EXIT_UNAVAILABLE,
        LocalReleaseError::Corruption => EXIT_CORRUPT,
    };
    CliFailure {
        code: error.code(),
        exit,
    }
}

#[cfg(test)]
mod tests {
    use super::{CliFailure, EXIT_CONFLICT, EXIT_INVALID, explain_failure};

    #[test]
    fn known_failures_have_specific_actionable_explanations() {
        let explanation = explain_failure(CliFailure {
            code: "LOCAL_PROCESS_STATE_INVALID",
            exit: EXIT_INVALID,
        });
        assert!(explanation.message.contains("initialized project"));
        assert!(explanation.hint.contains("--root PATH"));
        assert!(explanation.hint.contains("initialized automatically"));

        let explanation = explain_failure(CliFailure {
            code: "LOCAL_IDENTITY_INPUT_INVALID",
            exit: EXIT_INVALID,
        });
        assert!(explanation.message.contains("key operation"));
        assert!(explanation.hint.contains("Revoke"));
    }

    #[test]
    fn future_failures_receive_a_safe_exit_category_fallback() {
        let explanation = explain_failure(CliFailure {
            code: "FUTURE_CONFLICT_CODE",
            exit: EXIT_CONFLICT,
        });
        assert!(explanation.message.contains("conflicts"));
        assert!(explanation.hint.contains("latest state"));
        assert!(!explanation.message.contains("FUTURE_CONFLICT_CODE"));
        assert!(!explanation.message.contains('\n'));
        assert!(!explanation.hint.contains('\n'));
    }
}
