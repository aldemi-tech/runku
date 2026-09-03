//! `runku` executable entry point.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::{IsTerminal as _, Write as _},
    path::{Component, Path, PathBuf},
    process::ExitCode,
    time::SystemTime,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

use runku_build::{BuildError, BuildMetadata, build_project, source_fingerprint};
use runku_cli::{
    CliCommand, DEFAULT_LOCAL_LISTENER, DEFAULT_LOCAL_WORKSPACE, HELP, LINK_HELP, LOG_ARCHIVE_HELP,
    LOGIN_HELP, MANAGEMENT_HELP, TokenEnvironmentName, WORKSPACE_FREEZE_HELP, parse_args,
};
use runku_core::{
    ApplicationClientId, CredentialId, EnvironmentId, EnvironmentScope, OperationId, OperatorId,
    OperatorSessionId, WorkspaceId, WorkspaceRef,
};
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
    LocalStateError, acquire_local_process_lease, doctor_local, initialize_local,
    initialize_local_with_scope, load_local, publish_local, publish_local_if_head,
};
use runku_observability::{LogQuery, SequencedOperationalEvent};
use runku_otel::{OtlpExporterMode, OtlpExporterTelemetrySnapshot};
use runku_platform_identity::{AccessToken, DeviceName, RefreshToken};
use runku_protocol::{
    DevelopmentCreateWorkspaceRequestV1, DevelopmentFreezeOutcomeV1, DevelopmentFreezeRequestV1,
    DevelopmentPublishRequestV1, DevelopmentStateRequestV1, WireValueV1,
    derive_development_freeze_request_operation_id_v1, derive_development_revision_id_v1,
    encode_development_publish_request_v1,
};
use runku_releases::{
    ARTIFACT_MAX_BYTES, FunctionType, MANIFEST_MAX_BYTES, Sha256Digest, decode_release_manifest,
};
use runku_value::TimestampMicros;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use zeroize::{Zeroize, Zeroizing};

const EXIT_INTERNAL: u8 = 1;
const EXIT_USAGE: u8 = 2;
const EXIT_INVALID: u8 = 3;
const EXIT_CONFLICT: u8 = 4;
const EXIT_UNAVAILABLE: u8 = 5;
const EXIT_CORRUPT: u8 = 6;
const EXIT_AUTH: u8 = 7;
const EXIT_POLICY: u8 = 8;
const EXIT_UNCERTAIN: u8 = 9;
const DEFAULT_AUTHENTICATION_SERVER: &str = "https://api.runku.app";
const MANAGEMENT_LINK_FILE: &str = "management-link-v1.json";
const MANAGEMENT_LINK_MAX_BYTES: u64 = 8 * 1024;

#[tokio::main]
async fn main() -> ExitCode {
    if let Ok(command) = parse_args(std::env::args_os().skip(1)) {
        match Box::pin(execute(command)).await {
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
        eprintln!(
            "{HELP}{LOG_ARCHIVE_HELP}{LOGIN_HELP}{MANAGEMENT_HELP}{LINK_HELP}{WORKSPACE_FREEZE_HELP}"
        );
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
        "PLATFORM_LINK_CONFLICT" => FailureExplanation {
            message: "The project root is already bound to a different Product scope or Management origin.",
            hint: "Use the original server and scope or select a different empty project directory. Runku will not replace an existing remote binding.",
        },
        "PLATFORM_LINK_STATE_INVALID" | "PLATFORM_LINK_WRITE_FAILED" => FailureExplanation {
            message: "The authenticated Product link could not be validated or persisted safely.",
            hint: "Preserve the project directory, verify its private .runku state and permissions, then repeat the same runku link command after resolving the filesystem issue.",
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
        "REMOTE_PUBLISH_EXPECTED_HEAD_REQUIRED" => FailureExplanation {
            message: "Remote publication requires an explicit Workspace HEAD precondition.",
            hint: "Read and reconcile the remote Workspace, then pass --expected-head empty for its first publication or the exact drv_* revision you observed.",
        },
        "PLATFORM_LOGIN_REQUIRED" | "PLATFORM_SESSION_FILE_INVALID" => FailureExplanation {
            message: "No usable Runku operator session is available for this server operation.",
            hint: "Run runku login for the intended server and device. Do not substitute an rk_pub, rk_sec, or rk_dev credential.",
        },
        "PLATFORM_ACCESS_DENIED" => FailureExplanation {
            message: "The authenticated operator lacks the required capability at this exact Project and Environment.",
            hint: "Delegate only the missing capability at the intended scope, or select the correct project root; do not broaden an application key.",
        },
        "PLATFORM_OPERATION_CONFLICT" => FailureExplanation {
            message: "The remote Workspace or Channel changed after the supplied precondition was observed.",
            hint: "Run remote status, reconcile the current binding, and retry only with a deliberate updated expectation.",
        },
        "PLATFORM_LOG_STREAM_REVOKED" => FailureExplanation {
            message: "The remote log stream stopped because its operator authority is no longer valid.",
            hint: "Re-authenticate or restore the intended logs:follow grant; the CLI will not reconnect with a revoked session.",
        },
        "PLATFORM_LOGIN_SELECTION_REQUIRED" => FailureExplanation {
            message: "The authentication server offers more than one login method, but this process cannot ask which one to use.",
            hint: "Choose explicitly with --browser, --code-env RUNKU_NAME, or --oidc-token-env RUNKU_NAME.",
        },
        "PLATFORM_LOGIN_SELECTION_INVALID" => FailureExplanation {
            message: "The selected authentication method is not one of the options advertised by the server.",
            hint: "Run runku login again and select a displayed number, or use an explicit authentication flag.",
        },
        "PLATFORM_AUTH_CONFIGURATION_UNAVAILABLE" | "PLATFORM_AUTH_CONFIGURATION_INVALID" => {
            FailureExplanation {
                message: "Runku could not obtain a safe, supported login configuration from the authentication server.",
                hint: "Verify the exact authentication URL, TLS certificate, /v1/auth/config response, and advertised Management origin; redirects are never followed.",
            }
        }
        "PLATFORM_INVITATION_REQUIRED" => FailureExplanation {
            message: "This login method requires an invitation, but no protected interactive or environment input is available.",
            hint: "Use an interactive terminal or set an uppercase RUNKU_* variable and pass its name with --code-env.",
        },
        value if value.starts_with("PLATFORM_OIDC_") => FailureExplanation {
            message: "The interactive OIDC login could not be completed safely.",
            hint: "Check the configured native client, provider availability, loopback callback policy, and browser result, then retry to generate fresh state and PKCE material.",
        },
        value if value.starts_with("PLATFORM_") => FailureExplanation {
            message: "The authenticated Management API operation could not be completed safely.",
            hint: "Check the stored operator session, exact project root, server health, and current state before retrying.",
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginRequestWire<'a> {
    code: &'a str,
    device_name: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OidcLoginRequestWire<'a> {
    device_name: &'a str,
    invitation_code: Option<&'a str>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LoginResponseWire {
    access_token: String,
    refresh_token: String,
    operator_id: String,
    session_id: String,
    authorization_revision: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredSessionWire<'a> {
    version: u8,
    authentication_server: &'a str,
    server: &'a str,
    access_token: &'a str,
    refresh_token: &'a str,
    operator_id: &'a str,
    session_id: &'a str,
    authorization_revision: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredSession {
    version: u8,
    #[serde(default)]
    authentication_server: Option<String>,
    server: String,
    access_token: String,
    refresh_token: String,
    operator_id: String,
    session_id: String,
    authorization_revision: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManagementLinkWire {
    version: u8,
    management_origin: String,
    project_id: String,
    environment_id: String,
    linked_at_micros: String,
}

impl Drop for StoredSession {
    fn drop(&mut self) {
        self.access_token.zeroize();
        self.refresh_token.zeroize();
    }
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
            print!(
                "{HELP}{LOG_ARCHIVE_HELP}{LOGIN_HELP}{MANAGEMENT_HELP}{LINK_HELP}{WORKSPACE_FREEZE_HELP}"
            );
            Ok(())
        }
        CliCommand::Version => {
            println!("runku {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        CliCommand::Login {
            endpoint,
            device_name,
            code_environment,
            oidc_token_environment,
            browser,
            no_open,
        } => {
            remote_login(
                endpoint.as_ref(),
                device_name.as_ref(),
                code_environment.as_ref(),
                oidc_token_environment.as_ref(),
                browser,
                no_open,
            )
            .await
        }
        CliCommand::Init {
            root,
            workspace,
            listen,
            scope,
        } => {
            let (state, _) = initialize_local_with_scope(&root, workspace, listen, scope, now()?)
                .await
                .map_err(map_state)?;
            println!(
                "{{\"environmentId\":\"{}\",\"projectId\":\"{}\",\"workspaceId\":\"{}\"}}",
                state.environment_id, state.project_id, state.workspace_id
            );
            Ok(())
        }
        CliCommand::Link {
            root,
            workspace,
            listen,
            scope,
        } => remote_link(&root, workspace, listen, scope).await,
        CliCommand::Publish {
            remote,
            root,
            manifest,
            artifact,
            workspace,
            actor,
            expected_head,
        } => {
            if remote {
                return remote_publish(
                    &root,
                    &manifest,
                    &artifact,
                    workspace.as_ref(),
                    expected_head,
                )
                .await;
            }
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
            remote,
            root,
            release_id,
            against,
        } => {
            if remote {
                return remote_release(&root, release_id, against.as_ref()).await;
            }
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
            remote,
            root,
            channel,
            release_id,
            expected,
        } => {
            if remote {
                return remote_promote(&root, &channel, release_id, expected).await;
            }
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
            remote,
            root,
            channel,
            expected,
            target,
        } => {
            if remote {
                return remote_rollback(&root, &channel, expected, target).await;
            }
            let manager = LocalReleaseManager::open(&root)
                .await
                .map_err(map_release)?;
            let result = manager
                .rollback(channel, expected, target)
                .await
                .map_err(map_release)?;
            emit_release_outcome(&result)
        }
        CliCommand::Status { remote, root } => {
            if remote {
                return remote_status(&root).await;
            }
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
            remote,
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
            if remote {
                return remote_logs(
                    &root,
                    &LogQuery {
                        scope: load_local(&root).await.map_err(map_state)?.0.scope(),
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
            }
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
            remote,
            root,
            before,
            maximum,
            apply,
            environment,
        } => {
            if remote {
                return remote_log_prune(&root, before, maximum, apply, environment).await;
            }
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
        CliCommand::LogsArchiveStatus { remote, root } => {
            if remote {
                return remote_log_archive_status(&root).await;
            }
            let manager = LocalLogManager::open(&root).await.map_err(map_logs)?;
            let result = manager.archive_status().await;
            let environment_id = manager.environment_id();
            manager.close().await;
            let status = result.map_err(map_logs)?;
            println!(
                "{{\"environmentId\":\"{}\",\"parquetBytes\":{},\"records\":{},\"segments\":{},\"status\":\"ok\",\"through\":\"{}\"}}",
                environment_id,
                status.parquet_bytes,
                status.records,
                status.segments,
                status.through
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

#[allow(clippy::too_many_lines)]
struct ManagementClient {
    http: reqwest::Client,
    endpoint: DevelopmentEndpoint,
    authentication_endpoint: DevelopmentEndpoint,
    session: StoredSession,
}

impl ManagementClient {
    fn load() -> Result<Self, CliFailure> {
        let path = session_path()?;
        let metadata = std::fs::symlink_metadata(&path).map_err(|_| CliFailure {
            code: "PLATFORM_LOGIN_REQUIRED",
            exit: EXIT_AUTH,
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 32 * 1024 {
            return Err(CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            });
        }
        let bytes = std::fs::read(&path).map_err(|_| CliFailure {
            code: "PLATFORM_SESSION_FILE_INVALID",
            exit: EXIT_AUTH,
        })?;
        let session: StoredSession = serde_json::from_slice(&bytes).map_err(|_| CliFailure {
            code: "PLATFORM_SESSION_FILE_INVALID",
            exit: EXIT_AUTH,
        })?;
        let endpoint = session
            .server
            .parse::<DevelopmentEndpoint>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            })?;
        let authentication_server = session
            .authentication_server
            .as_deref()
            .unwrap_or(session.server.as_str());
        let authentication_endpoint = authentication_server
            .parse::<DevelopmentEndpoint>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            })?;
        let access = session
            .access_token
            .parse::<AccessToken>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            })?;
        let refresh = session
            .refresh_token
            .parse::<RefreshToken>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            })?;
        let session_id = session
            .session_id
            .parse::<OperatorSessionId>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            })?;
        session
            .operator_id
            .parse::<OperatorId>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            })?;
        if !matches!(session.version, 1 | 2)
            || session.version == 1 && session.authentication_server.is_some()
            || session.version == 2 && session.authentication_server.is_none()
            || session.authorization_revision == 0
            || access.id() != session_id
            || refresh.id() != session_id
            || endpoint.as_str() != session.server
        {
            return Err(CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            });
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .https_only(!endpoint.as_str().starts_with("http://"))
            .build()
            .map_err(|_| CliFailure {
                code: "PLATFORM_CLIENT_INVALID",
                exit: EXIT_INTERNAL,
            })?;
        Ok(Self {
            http,
            endpoint,
            authentication_endpoint,
            session,
        })
    }

    fn load_for(root: &Path, scope: EnvironmentScope) -> Result<Self, CliFailure> {
        let client = Self::load()?;
        validate_management_link(root, scope, &client.endpoint)?;
        Ok(client)
    }

    async fn request(
        &mut self,
        method: reqwest::Method,
        url: reqwest::Url,
        body: Option<Vec<u8>>,
        content_type: Option<&'static str>,
    ) -> Result<reqwest::Response, CliFailure> {
        let mut response = self
            .send(method.clone(), url.clone(), body.clone(), content_type)
            .await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            self.refresh().await?;
            response = self.send(method, url, body, content_type).await?;
        }
        if !response.status().is_success() {
            return Err(map_management_status(response.status()));
        }
        Ok(response)
    }

    async fn send(
        &self,
        method: reqwest::Method,
        url: reqwest::Url,
        body: Option<Vec<u8>>,
        content_type: Option<&'static str>,
    ) -> Result<reqwest::Response, CliFailure> {
        let mut request = self
            .http
            .request(method, url)
            .bearer_auth(&self.session.access_token);
        if let Some(body) = body {
            request = request.body(body);
        }
        if let Some(content_type) = content_type {
            request = request.header(reqwest::header::CONTENT_TYPE, content_type);
        }
        request.send().await.map_err(|_| CliFailure {
            code: "PLATFORM_REQUEST_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })
    }

    async fn refresh(&mut self) -> Result<(), CliFailure> {
        let response = self
            .http
            .post(format!(
                "{}/v1/auth/refresh",
                self.authentication_endpoint.as_str()
            ))
            .json(&serde_json::json!({"refreshToken": self.session.refresh_token}))
            .send()
            .await
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_REFRESH_UNAVAILABLE",
                exit: EXIT_UNAVAILABLE,
            })?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(CliFailure {
                code: "PLATFORM_LOGIN_REQUIRED",
                exit: EXIT_AUTH,
            });
        }
        let bytes = bounded_response(response, 16 * 1024).await?;
        let result: LoginResponseWire = serde_json::from_slice(&bytes).map_err(|_| CliFailure {
            code: "PLATFORM_SESSION_REFRESH_INVALID",
            exit: EXIT_CORRUPT,
        })?;
        let access_token = Zeroizing::new(result.access_token);
        let refresh_token = Zeroizing::new(result.refresh_token);
        let access = access_token
            .parse::<AccessToken>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_REFRESH_INVALID",
                exit: EXIT_CORRUPT,
            })?;
        let refresh = refresh_token
            .parse::<RefreshToken>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_REFRESH_INVALID",
                exit: EXIT_CORRUPT,
            })?;
        let parsed_session = result
            .session_id
            .parse::<OperatorSessionId>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_REFRESH_INVALID",
                exit: EXIT_CORRUPT,
            })?;
        result
            .operator_id
            .parse::<OperatorId>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_REFRESH_INVALID",
                exit: EXIT_CORRUPT,
            })?;
        if result.operator_id != self.session.operator_id
            || result.session_id != self.session.session_id
            || access.id() != parsed_session
            || refresh.id() != parsed_session
            || result.authorization_revision < self.session.authorization_revision
        {
            return Err(CliFailure {
                code: "PLATFORM_SESSION_REFRESH_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
        self.session.access_token.zeroize();
        self.session.refresh_token.zeroize();
        self.session.access_token = access_token.to_string();
        self.session.refresh_token = refresh_token.to_string();
        self.session.authorization_revision = result.authorization_revision;
        self.session.version = 2;
        self.session.authentication_server = Some(self.authentication_endpoint.to_string());
        persist_owned_session(&self.session)
    }

    fn url(&self, path: &str) -> Result<reqwest::Url, CliFailure> {
        reqwest::Url::parse(&format!("{}{}", self.endpoint.as_str(), path)).map_err(|_| {
            CliFailure {
                code: "PLATFORM_REQUEST_INVALID",
                exit: EXIT_INTERNAL,
            }
        })
    }
}

#[allow(clippy::option_option)]
async fn remote_publish(
    root: &Path,
    manifest_path: &Path,
    artifact_path: &Path,
    workspace: Option<&WorkspaceRef>,
    expected_head: Option<Option<runku_core::DevRevisionId>>,
) -> Result<(), CliFailure> {
    let state = load_local(root).await.map_err(map_state)?.0;
    let expected_head = expected_head.ok_or(CliFailure {
        code: "REMOTE_PUBLISH_EXPECTED_HEAD_REQUIRED",
        exit: EXIT_USAGE,
    })?;
    let manifest_bytes = read_bounded(manifest_path, MANIFEST_MAX_BYTES).await?;
    let artifact_bytes = read_bounded(artifact_path, ARTIFACT_MAX_BYTES).await?;
    let manifest = decode_release_manifest(&manifest_bytes).map_err(|_| CliFailure {
        code: "LOCAL_PACKAGE_FILE_INVALID",
        exit: EXIT_INVALID,
    })?;
    let request = DevelopmentPublishRequestV1 {
        operation_id: OperationId::generate(),
        project_id: state.project_id,
        workspace_ref: workspace
            .cloned()
            .unwrap_or_else(|| state.workspace_ref.clone()),
        expected_head,
        manifest,
        manifest_bytes,
        artifact_bytes,
    };
    let body = encode_development_publish_request_v1(&request).map_err(|_| CliFailure {
        code: "LOCAL_PACKAGE_FILE_INVALID",
        exit: EXIT_INVALID,
    })?;
    let mut client = ManagementClient::load_for(root, state.scope())?;
    let path = product_path(state.scope(), "/workspace/publish");
    let url = client.url(&path)?;
    let response = client
        .request(
            reqwest::Method::POST,
            url,
            Some(body),
            Some("application/vnd.runku.management-publish-v1"),
        )
        .await?;
    emit_management_response(response).await
}

async fn remote_release(
    root: &Path,
    release: runku_core::ReleaseId,
    against: Option<&runku_core::ChannelName>,
) -> Result<(), CliFailure> {
    let state = load_local(root).await.map_err(map_state)?.0;
    let mut client = ManagementClient::load_for(root, state.scope())?;
    let path = product_path(state.scope(), &format!("/releases/{release}"));
    let body = serde_json::to_vec(&serde_json::json!({
        "against": against.map(ToString::to_string)
    }))
    .map_err(|_| CliFailure {
        code: "PLATFORM_REQUEST_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    let url = client.url(&path)?;
    let response = client
        .request(
            reqwest::Method::POST,
            url,
            Some(body),
            Some("application/json"),
        )
        .await?;
    emit_management_response(response).await
}

#[allow(clippy::option_option)]
async fn remote_promote(
    root: &Path,
    channel: &runku_core::ChannelName,
    release: runku_core::ReleaseId,
    expected: Option<Option<runku_core::ReleaseId>>,
) -> Result<(), CliFailure> {
    let state = load_local(root).await.map_err(map_state)?.0;
    let mut client = ManagementClient::load_for(root, state.scope())?;
    let path = product_path(state.scope(), &format!("/channels/{channel}"));
    let expected =
        expected.map(|value| value.map_or_else(|| "empty".to_owned(), |id| id.to_string()));
    let body = serde_json::to_vec(&serde_json::json!({
        "releaseId": release.to_string(), "expected": expected
    }))
    .map_err(|_| CliFailure {
        code: "PLATFORM_REQUEST_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    let url = client.url(&path)?;
    let response = client
        .request(
            reqwest::Method::PUT,
            url,
            Some(body),
            Some("application/json"),
        )
        .await?;
    emit_management_response(response).await
}

async fn remote_rollback(
    root: &Path,
    channel: &runku_core::ChannelName,
    expected: runku_core::ReleaseId,
    target: runku_core::ReleaseId,
) -> Result<(), CliFailure> {
    let state = load_local(root).await.map_err(map_state)?.0;
    let mut client = ManagementClient::load_for(root, state.scope())?;
    let path = product_path(state.scope(), &format!("/channels/{channel}/rollback"));
    let body = serde_json::to_vec(&serde_json::json!({
        "expected": expected.to_string(), "target": target.to_string()
    }))
    .map_err(|_| CliFailure {
        code: "PLATFORM_REQUEST_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    let url = client.url(&path)?;
    let response = client
        .request(
            reqwest::Method::POST,
            url,
            Some(body),
            Some("application/json"),
        )
        .await?;
    emit_management_response(response).await
}

async fn remote_status(root: &Path) -> Result<(), CliFailure> {
    let state = load_local(root).await.map_err(map_state)?.0;
    let mut client = ManagementClient::load_for(root, state.scope())?;
    let url = client.url(&product_path(state.scope(), "/status"))?;
    let response = client
        .request(reqwest::Method::GET, url, None, None)
        .await?;
    emit_management_response(response).await
}

async fn remote_link(
    root: &Path,
    workspace: WorkspaceRef,
    listen: std::net::SocketAddr,
    scope: EnvironmentScope,
) -> Result<(), CliFailure> {
    preflight_link_state(root, &workspace, listen, scope).await?;
    let mut client = ManagementClient::load()?;
    validate_management_link(root, scope, &client.endpoint)?;
    let url = client.url(&product_path(scope, "/status"))?;
    let response = client
        .request(reqwest::Method::GET, url, None, None)
        .await?;
    let bytes = bounded_response(response, 2 * 1024 * 1024).await?;
    validate_link_status(&bytes)?;

    let (state, paths) = initialize_local_with_scope(root, workspace, listen, Some(scope), now()?)
        .await
        .map_err(map_state)?;
    let replayed =
        persist_management_link(&paths.state, scope, &client.endpoint, state.created_at)?;
    println!(
        "{{\"environmentId\":\"{}\",\"managementOrigin\":\"{}\",\"projectId\":\"{}\",\"replayed\":{},\"status\":\"linked\",\"workspaceId\":\"{}\"}}",
        state.environment_id,
        client.endpoint.as_str(),
        state.project_id,
        replayed,
        state.workspace_id,
    );
    Ok(())
}

async fn preflight_link_state(
    root: &Path,
    workspace: &WorkspaceRef,
    listen: std::net::SocketAddr,
    scope: EnvironmentScope,
) -> Result<(), CliFailure> {
    let state_path = root
        .join(runku_local::LOCAL_STATE_DIRECTORY)
        .join("local-state-v1.json");
    match std::fs::symlink_metadata(state_path) {
        Ok(_) => {
            let state = load_local(root).await.map_err(map_state)?.0;
            if state.scope() != scope
                || state.workspace_ref != *workspace
                || state.listen_address != listen
            {
                return Err(CliFailure {
                    code: "PLATFORM_LINK_CONFLICT",
                    exit: EXIT_CONFLICT,
                });
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(CliFailure {
                code: "PLATFORM_LINK_STATE_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
    }
    Ok(())
}

fn validate_link_status(bytes: &[u8]) -> Result<(), CliFailure> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| CliFailure {
        code: "PLATFORM_RESPONSE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    let valid = value
        .get("servingRevision")
        .and_then(serde_json::Value::as_u64)
        .is_some()
        && value
            .get("releases")
            .and_then(serde_json::Value::as_array)
            .is_some()
        && value
            .get("channels")
            .and_then(serde_json::Value::as_array)
            .is_some();
    if !valid {
        return Err(CliFailure {
            code: "PLATFORM_RESPONSE_INVALID",
            exit: EXIT_CORRUPT,
        });
    }
    Ok(())
}

fn validate_management_link(
    root: &Path,
    scope: EnvironmentScope,
    endpoint: &DevelopmentEndpoint,
) -> Result<bool, CliFailure> {
    let path = root
        .join(runku_local::LOCAL_STATE_DIRECTORY)
        .join(MANAGEMENT_LINK_FILE);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => {
            return Err(CliFailure {
                code: "PLATFORM_LINK_STATE_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MANAGEMENT_LINK_MAX_BYTES
    {
        return Err(CliFailure {
            code: "PLATFORM_LINK_STATE_INVALID",
            exit: EXIT_CORRUPT,
        });
    }
    let bytes = std::fs::read(path).map_err(|_| CliFailure {
        code: "PLATFORM_LINK_STATE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    let link: ManagementLinkWire = serde_json::from_slice(&bytes).map_err(|_| CliFailure {
        code: "PLATFORM_LINK_STATE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    let project = link.project_id.parse().map_err(|_| CliFailure {
        code: "PLATFORM_LINK_STATE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    let environment = link.environment_id.parse().map_err(|_| CliFailure {
        code: "PLATFORM_LINK_STATE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    let origin = link
        .management_origin
        .parse::<DevelopmentEndpoint>()
        .map_err(|_| CliFailure {
            code: "PLATFORM_LINK_STATE_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    let linked_at = link
        .linked_at_micros
        .parse::<i64>()
        .map_err(|_| CliFailure {
            code: "PLATFORM_LINK_STATE_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    if link.version != 1
        || linked_at < 0
        || linked_at.to_string() != link.linked_at_micros
        || EnvironmentScope::new(project, environment) != scope
        || origin.as_str() != endpoint.as_str()
    {
        return Err(CliFailure {
            code: "PLATFORM_LINK_CONFLICT",
            exit: EXIT_CONFLICT,
        });
    }
    Ok(true)
}

fn persist_management_link(
    state_directory: &Path,
    scope: EnvironmentScope,
    endpoint: &DevelopmentEndpoint,
    linked_at: TimestampMicros,
) -> Result<bool, CliFailure> {
    let root = state_directory.parent().ok_or(CliFailure {
        code: "PLATFORM_LINK_STATE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    if validate_management_link(root, scope, endpoint)? {
        return Ok(true);
    }
    let link = ManagementLinkWire {
        version: 1,
        management_origin: endpoint.to_string(),
        project_id: scope.project_id().to_string(),
        environment_id: scope.environment_id().to_string(),
        linked_at_micros: linked_at.get().to_string(),
    };
    let bytes = serde_json::to_vec(&link).map_err(|_| CliFailure {
        code: "PLATFORM_LINK_WRITE_FAILED",
        exit: EXIT_INTERNAL,
    })?;
    let target = state_directory.join(MANAGEMENT_LINK_FILE);
    let temporary =
        state_directory.join(format!(".management-link-{}.tmp", OperationId::generate()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(|_| CliFailure {
        code: "PLATFORM_LINK_WRITE_FAILED",
        exit: EXIT_UNAVAILABLE,
    })?;
    let write = file
        .write_all(&bytes)
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all());
    drop(file);
    if write.is_err() {
        let _ = std::fs::remove_file(&temporary);
        return Err(CliFailure {
            code: "PLATFORM_LINK_WRITE_FAILED",
            exit: EXIT_UNAVAILABLE,
        });
    }
    let linked = std::fs::hard_link(&temporary, &target);
    let _ = std::fs::remove_file(&temporary);
    match linked {
        Ok(()) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_management_link(root, scope, endpoint).map(|_| true)
        }
        Err(_) => Err(CliFailure {
            code: "PLATFORM_LINK_WRITE_FAILED",
            exit: EXIT_UNAVAILABLE,
        }),
    }
}

#[allow(clippy::too_many_lines)]
async fn remote_logs(root: &Path, query: &LogQuery, follow: bool) -> Result<(), CliFailure> {
    let state = load_local(root).await.map_err(map_state)?.0;
    if query.scope != state.scope() {
        return Err(CliFailure {
            code: "PLATFORM_REQUEST_INVALID",
            exit: EXIT_INVALID,
        });
    }
    let mut client = ManagementClient::load_for(root, state.scope())?;
    let suffix = if follow { "/logs/follow" } else { "/logs" };
    let mut url = client.url(&product_path(state.scope(), suffix))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("after", &query.after.to_string());
        pairs.append_pair("limit", &query.limit.to_string());
        if let Some(value) = query.stream {
            pairs.append_pair("stream", value.as_str());
        }
        if let Some(value) = query.minimum_level {
            pairs.append_pair("level", value.as_str());
        }
        if let Some(value) = query.function_id {
            pairs.append_pair("functionId", &value.to_string());
        }
        if let Some(value) = query.request_id {
            pairs.append_pair("requestId", &value.to_string());
        }
        if let Some(value) = query.invocation_id {
            pairs.append_pair("invocationId", &value.to_string());
        }
        if let Some(value) = query.client_id {
            pairs.append_pair("clientId", &value.to_string());
        }
        if let Some(value) = query.credential_id {
            pairs.append_pair("credentialId", &value.to_string());
        }
        if let Some(value) = query.release_id {
            pairs.append_pair("releaseId", &value.to_string());
        }
    }
    let mut response = client
        .request(reqwest::Method::GET, url, None, None)
        .await?;
    if !follow {
        let bytes = bounded_response(response, 2 * 1024 * 1024).await?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| CliFailure {
            code: "PLATFORM_RESPONSE_INVALID",
            exit: EXIT_CORRUPT,
        })?;
        let records = value
            .get("records")
            .and_then(serde_json::Value::as_array)
            .ok_or(CliFailure {
                code: "PLATFORM_RESPONSE_INVALID",
                exit: EXIT_CORRUPT,
            })?;
        for record in records {
            emit_json(record)?;
        }
        return Ok(());
    }
    let mut pending = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| CliFailure {
        code: "PLATFORM_LOG_STREAM_UNAVAILABLE",
        exit: EXIT_UNAVAILABLE,
    })? {
        if pending.len().saturating_add(chunk.len()) > 2 * 1024 * 1024 {
            return Err(CliFailure {
                code: "PLATFORM_RESPONSE_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
        pending.extend_from_slice(&chunk);
        while let Some(index) = pending.iter().position(|byte| *byte == b'\n') {
            let line = pending.drain(..=index).collect::<Vec<_>>();
            if line.len() <= 1 {
                continue;
            }
            let value: serde_json::Value = serde_json::from_slice(&line[..line.len() - 1])
                .map_err(|_| CliFailure {
                    code: "PLATFORM_RESPONSE_INVALID",
                    exit: EXIT_CORRUPT,
                })?;
            if let Some(code) = value
                .get("error")
                .and_then(|error| error.get("code"))
                .and_then(serde_json::Value::as_str)
            {
                return Err(if code == "PLATFORM_UNAUTHENTICATED" {
                    CliFailure {
                        code: "PLATFORM_LOG_STREAM_REVOKED",
                        exit: EXIT_AUTH,
                    }
                } else {
                    CliFailure {
                        code: "PLATFORM_LOG_STREAM_UNAVAILABLE",
                        exit: EXIT_UNAVAILABLE,
                    }
                });
            }
            emit_json(&value)?;
        }
    }
    Ok(())
}

fn product_path(scope: runku_core::EnvironmentScope, suffix: &str) -> String {
    format!(
        "/v1/projects/{}/environments/{}{}",
        scope.project_id(),
        scope.environment_id(),
        suffix
    )
}

async fn remote_log_archive_status(root: &Path) -> Result<(), CliFailure> {
    let state = load_local(root).await.map_err(map_state)?.0;
    let mut client = ManagementClient::load_for(root, state.scope())?;
    let url = client.url(&product_path(state.scope(), "/logs/archive-status"))?;
    let response = client
        .request(reqwest::Method::GET, url, None, None)
        .await?;
    emit_management_response(response).await
}

async fn remote_log_prune(
    root: &Path,
    before: TimestampMicros,
    maximum: u32,
    apply: bool,
    environment: Option<EnvironmentId>,
) -> Result<(), CliFailure> {
    let state = load_local(root).await.map_err(map_state)?.0;
    let mut client = ManagementClient::load_for(root, state.scope())?;
    let url = client.url(&product_path(state.scope(), "/logs/prune"))?;
    let body = serde_json::to_vec(&serde_json::json!({
        "beforeMicros": before.get(),
        "maximum": maximum,
        "apply": apply,
        "environmentId": environment.map(|value| value.to_string()),
    }))
    .map_err(|_| CliFailure {
        code: "PLATFORM_REQUEST_INVALID",
        exit: EXIT_INTERNAL,
    })?;
    let response = client
        .request(
            reqwest::Method::POST,
            url,
            Some(body),
            Some("application/json"),
        )
        .await?;
    emit_management_response(response).await
}

async fn emit_management_response(response: reqwest::Response) -> Result<(), CliFailure> {
    let bytes = bounded_response(response, 2 * 1024 * 1024).await?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| CliFailure {
        code: "PLATFORM_RESPONSE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    emit_json(&value)
}

async fn bounded_response(
    mut response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, CliFailure> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| CliFailure {
        code: "PLATFORM_RESPONSE_INVALID",
        exit: EXIT_UNAVAILABLE,
    })? {
        if bytes.len().saturating_add(chunk.len()) > maximum {
            return Err(CliFailure {
                code: "PLATFORM_RESPONSE_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn persist_owned_session(session: &StoredSession) -> Result<(), CliFailure> {
    let path = session_path()?;
    let stored = StoredSessionWire {
        version: 2,
        authentication_server: session
            .authentication_server
            .as_deref()
            .unwrap_or(session.server.as_str()),
        server: &session.server,
        access_token: &session.access_token,
        refresh_token: &session.refresh_token,
        operator_id: &session.operator_id,
        session_id: &session.session_id,
        authorization_revision: session.authorization_revision,
    };
    let encoded = serde_json::to_vec(&stored).map_err(|_| CliFailure {
        code: "PLATFORM_SESSION_WRITE_FAILED",
        exit: EXIT_INTERNAL,
    })?;
    write_session_file(&path, &encoded)
}

fn map_management_status(status: reqwest::StatusCode) -> CliFailure {
    match status {
        reqwest::StatusCode::UNAUTHORIZED => CliFailure {
            code: "PLATFORM_LOGIN_REQUIRED",
            exit: EXIT_AUTH,
        },
        reqwest::StatusCode::FORBIDDEN => CliFailure {
            code: "PLATFORM_ACCESS_DENIED",
            exit: EXIT_POLICY,
        },
        reqwest::StatusCode::CONFLICT => CliFailure {
            code: "PLATFORM_OPERATION_CONFLICT",
            exit: EXIT_CONFLICT,
        },
        reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::NOT_FOUND => CliFailure {
            code: "PLATFORM_REQUEST_INVALID",
            exit: EXIT_INVALID,
        },
        _ if status.is_server_error() => CliFailure {
            code: "PLATFORM_REQUEST_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        },
        _ => CliFailure {
            code: "PLATFORM_RESPONSE_INVALID",
            exit: EXIT_CORRUPT,
        },
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OidcClientConfigurationWire {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    client_id: String,
    scopes: Vec<String>,
    resource: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthenticationConfigurationWire {
    version: u8,
    methods: Vec<String>,
    management_endpoint: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InteractiveLoginMethod {
    Browser,
    Invitation,
    OidcToken,
}

struct BrowserAuthorization {
    token: Zeroizing<String>,
    callback: tokio::net::TcpStream,
}

#[derive(Deserialize)]
struct OidcTokenResponseWire {
    access_token: String,
}

#[allow(clippy::too_many_lines)]
async fn browser_oidc_token(
    client: &reqwest::Client,
    endpoint: &DevelopmentEndpoint,
    no_open: bool,
) -> Result<BrowserAuthorization, CliFailure> {
    let response = client
        .get(format!("{}/v1/auth/oidc/config", endpoint.as_str()))
        .send()
        .await
        .map_err(|_| CliFailure {
            code: "PLATFORM_OIDC_CONFIGURATION_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_NOT_CONFIGURED",
            exit: EXIT_AUTH,
        });
    }
    let bytes = bounded_response(response, 16 * 1024).await?;
    let config: OidcClientConfigurationWire =
        serde_json::from_slice(&bytes).map_err(|_| CliFailure {
            code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    validate_oidc_client_configuration(&config)?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })?;
    let callback = format!(
        "http://127.0.0.1:{}/callback",
        listener
            .local_addr()
            .map_err(|_| CliFailure {
                code: "PLATFORM_OIDC_CALLBACK_UNAVAILABLE",
                exit: EXIT_UNAVAILABLE,
            })?
            .port()
    );
    let state = random_url_secret()?;
    let verifier = Zeroizing::new(random_url_secret()?);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut authorization =
        reqwest::Url::parse(&config.authorization_endpoint).map_err(|_| CliFailure {
            code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    authorization
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &config.client_id)
        .append_pair("redirect_uri", &callback)
        .append_pair("scope", &config.scopes.join(" "))
        .append_pair("state", &state)
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256");
    if let Some(resource) = &config.resource {
        authorization
            .query_pairs_mut()
            .append_pair("resource", resource);
    }
    eprintln!("authorization URL: {authorization}");
    if !no_open {
        open_system_browser(authorization.as_str()).await?;
    }

    let (mut socket, peer) =
        tokio::time::timeout(std::time::Duration::from_secs(300), listener.accept())
            .await
            .map_err(|_| CliFailure {
                code: "PLATFORM_OIDC_CALLBACK_TIMEOUT",
                exit: EXIT_AUTH,
            })?
            .map_err(|_| CliFailure {
                code: "PLATFORM_OIDC_CALLBACK_UNAVAILABLE",
                exit: EXIT_UNAVAILABLE,
            })?;
    if !peer.ip().is_loopback() {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        });
    }
    let mut request = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let count = socket.read(&mut chunk).await.map_err(|_| CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        })?;
        if count == 0 || request.len().saturating_add(count) > 16 * 1024 {
            return Err(CliFailure {
                code: "PLATFORM_OIDC_CALLBACK_INVALID",
                exit: EXIT_AUTH,
            });
        }
        request.extend_from_slice(&chunk[..count]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let expected_host = callback
        .strip_prefix("http://")
        .and_then(|value| value.strip_suffix("/callback"))
        .ok_or(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        })?;
    let code = match parse_oidc_callback_request(&request, expected_host, &state, &config.issuer) {
        Ok(code) => code,
        Err(error) => {
            let _ = write_browser_result(&mut socket, false).await;
            return Err(error);
        }
    };
    let mut token_form = vec![
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", callback.as_str()),
        ("client_id", config.client_id.as_str()),
        ("code_verifier", verifier.as_str()),
    ];
    if let Some(resource) = &config.resource {
        token_form.push(("resource", resource.as_str()));
    }
    let Ok(response) = client
        .post(&config.token_endpoint)
        .form(&token_form)
        .send()
        .await
    else {
        let _ = write_browser_result(&mut socket, false).await;
        return Err(CliFailure {
            code: "PLATFORM_OIDC_TOKEN_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        });
    };
    if response.status() != reqwest::StatusCode::OK {
        let _ = write_browser_result(&mut socket, false).await;
        return Err(CliFailure {
            code: "PLATFORM_OIDC_TOKEN_REJECTED",
            exit: EXIT_AUTH,
        });
    }
    let bytes = match bounded_response(response, 64 * 1024).await {
        Ok(bytes) => bytes,
        Err(error) => {
            let _ = write_browser_result(&mut socket, false).await;
            return Err(error);
        }
    };
    let Ok(token) = serde_json::from_slice::<OidcTokenResponseWire>(&bytes) else {
        let _ = write_browser_result(&mut socket, false).await;
        return Err(CliFailure {
            code: "PLATFORM_OIDC_TOKEN_INVALID",
            exit: EXIT_AUTH,
        });
    };
    if token.access_token.is_empty() || token.access_token.len() > 16 * 1024 {
        let _ = write_browser_result(&mut socket, false).await;
        return Err(CliFailure {
            code: "PLATFORM_OIDC_TOKEN_INVALID",
            exit: EXIT_AUTH,
        });
    }
    Ok(BrowserAuthorization {
        token: Zeroizing::new(token.access_token),
        callback: socket,
    })
}

fn parse_oidc_callback_request(
    request: &[u8],
    expected_host: &str,
    expected_state: &str,
    expected_issuer: &str,
) -> Result<String, CliFailure> {
    let end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
        .filter(|end| *end == request.len())
        .ok_or(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        })?;
    let request = std::str::from_utf8(&request[..end]).map_err(|_| CliFailure {
        code: "PLATFORM_OIDC_CALLBACK_INVALID",
        exit: EXIT_AUTH,
    })?;
    let mut lines = request.split("\r\n");
    let target = lines
        .next()
        .and_then(|line| line.strip_prefix("GET "))
        .and_then(|line| line.strip_suffix(" HTTP/1.1"))
        .ok_or(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        })?;
    let mut host = None;
    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').ok_or(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        })?;
        if name.eq_ignore_ascii_case("host") && host.replace(value.trim()).is_some() {
            return Err(CliFailure {
                code: "PLATFORM_OIDC_CALLBACK_INVALID",
                exit: EXIT_AUTH,
            });
        }
    }
    if host != Some(expected_host) {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        });
    }
    let callback_url =
        reqwest::Url::parse(&format!("http://{expected_host}{target}")).map_err(|_| {
            CliFailure {
                code: "PLATFORM_OIDC_CALLBACK_INVALID",
                exit: EXIT_AUTH,
            }
        })?;
    if callback_url.path() != "/callback" || callback_url.fragment().is_some() {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        });
    }
    let mut received_state = None;
    let mut code = None;
    let mut issuer = None;
    let mut provider_error = None;
    for (key, value) in callback_url.query_pairs() {
        let slot = match key.as_ref() {
            "state" => &mut received_state,
            "code" => &mut code,
            "iss" => &mut issuer,
            "error" => &mut provider_error,
            _ => continue,
        };
        if slot.replace(value.into_owned()).is_some() {
            return Err(CliFailure {
                code: "PLATFORM_OIDC_CALLBACK_INVALID",
                exit: EXIT_AUTH,
            });
        }
    }
    if received_state.as_deref() != Some(expected_state)
        || provider_error.is_some()
        || code.is_none()
        || issuer
            .as_deref()
            .is_some_and(|value| value != expected_issuer)
    {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_REJECTED",
            exit: EXIT_AUTH,
        });
    }
    let code = code.unwrap_or_default();
    if code.is_empty()
        || code.len() > 4_096
        || code.trim() != code
        || code.chars().any(char::is_control)
    {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_INVALID",
            exit: EXIT_AUTH,
        });
    }
    Ok(code)
}

async fn write_browser_result(
    socket: &mut tokio::net::TcpStream,
    success: bool,
) -> Result<(), CliFailure> {
    let (status, title, message) = if success {
        (
            "200 OK",
            "Runku login complete",
            "Runku login complete. Runku verified the identity and created the session. You may close this window.",
        )
    } else {
        (
            "400 Bad Request",
            "Runku login rejected",
            "Runku did not create a session. Return to the terminal for a safe error message.",
        )
    };
    let body = format!(
        "<!doctype html><meta charset=utf-8><meta name=referrer content=no-referrer><title>{title}</title><p>{message}</p>"
    );
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store, max-age=0\r\nContent-Security-Policy: default-src 'none'; frame-ancestors 'none'\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    socket
        .write_all(response.as_bytes())
        .await
        .map_err(|_| CliFailure {
            code: "PLATFORM_OIDC_CALLBACK_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })
}

fn validate_oidc_client_configuration(
    config: &OidcClientConfigurationWire,
) -> Result<(), CliFailure> {
    if config.client_id.is_empty()
        || config.client_id.len() > 256
        || config.scopes.is_empty()
        || config.scopes.len() > 16
        || !config.scopes.iter().any(|scope| scope == "openid")
        || config.scopes.iter().any(|scope| {
            scope.is_empty()
                || scope.len() > 128
                || scope.chars().any(char::is_whitespace)
                || scope.chars().any(char::is_control)
        })
        || config.scopes.iter().collect::<BTreeSet<_>>().len() != config.scopes.len()
    {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        });
    }
    let issuer = reqwest::Url::parse(&config.issuer).map_err(|_| CliFailure {
        code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    if issuer.scheme() != "https"
        || issuer.host_str().is_none()
        || !issuer.username().is_empty()
        || issuer.password().is_some()
        || issuer.query().is_some()
        || issuer.fragment().is_some()
    {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        });
    }
    for raw in [&config.authorization_endpoint, &config.token_endpoint] {
        let url = reqwest::Url::parse(raw).map_err(|_| CliFailure {
            code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        })?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
        if url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https" || url.scheme() == "http" && loopback)
        {
            return Err(CliFailure {
                code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
    }
    if let Some(raw) = &config.resource {
        if raw.is_empty() || raw.len() > 2_048 {
            return Err(CliFailure {
                code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
        let resource = reqwest::Url::parse(raw).map_err(|_| CliFailure {
            code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        })?;
        let loopback = resource.host_str().is_some_and(|host| {
            host.parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
        });
        if resource.host_str().is_none()
            || !resource.username().is_empty()
            || resource.password().is_some()
            || resource.fragment().is_some()
            || !(resource.scheme() == "https" || resource.scheme() == "http" && loopback)
        {
            return Err(CliFailure {
                code: "PLATFORM_OIDC_CONFIGURATION_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
    }
    Ok(())
}

fn random_url_secret() -> Result<String, CliFailure> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| CliFailure {
        code: "PLATFORM_OIDC_ENTROPY_UNAVAILABLE",
        exit: EXIT_INTERNAL,
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

async fn open_system_browser(url: &str) -> Result<(), CliFailure> {
    let status = if cfg!(target_os = "macos") {
        tokio::process::Command::new("open").arg(url).status().await
    } else if cfg!(target_os = "windows") {
        tokio::process::Command::new("rundll32.exe")
            .arg("url.dll,FileProtocolHandler")
            .arg(url)
            .status()
            .await
    } else {
        tokio::process::Command::new("xdg-open")
            .arg(url)
            .status()
            .await
    }
    .map_err(|_| CliFailure {
        code: "PLATFORM_OIDC_BROWSER_UNAVAILABLE",
        exit: EXIT_UNAVAILABLE,
    })?;
    if !status.success() {
        return Err(CliFailure {
            code: "PLATFORM_OIDC_BROWSER_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        });
    }
    Ok(())
}

async fn remote_login(
    explicit_endpoint: Option<&DevelopmentEndpoint>,
    explicit_device_name: Option<&DeviceName>,
    code_environment: Option<&TokenEnvironmentName>,
    oidc_token_environment: Option<&TokenEnvironmentName>,
    force_browser: bool,
    no_open: bool,
) -> Result<(), CliFailure> {
    let authentication_endpoint = resolve_authentication_endpoint(explicit_endpoint)?;
    let device_name = resolve_device_name(explicit_device_name)?;
    let client = login_http_client(&authentication_endpoint)?;
    let (configuration, management_endpoint) =
        discover_authentication(&client, &authentication_endpoint).await?;
    eprintln!("authentication server: {authentication_endpoint}");
    eprintln!("management server: {management_endpoint}");
    eprintln!("device: {device_name}");

    let method = select_login_method(
        &configuration,
        code_environment.is_some(),
        oidc_token_environment.is_some(),
        force_browser,
    )?;
    let mut code = invitation_from_environment(code_environment)?;
    if method == InteractiveLoginMethod::Invitation && code.is_none() {
        code = Some(prompt_invitation_code()?);
    }
    let explicit_oidc_token = oidc_token_from_environment(oidc_token_environment)?;
    let mut browser_authorization = if method == InteractiveLoginMethod::Browser {
        Some(browser_oidc_token(&client, &authentication_endpoint, no_open).await?)
    } else {
        None
    };
    let oidc_token = browser_authorization
        .as_ref()
        .map(|authorization| authorization.token.as_str())
        .or(explicit_oidc_token.as_deref().map(String::as_str));
    let result = complete_remote_login(
        &client,
        &authentication_endpoint,
        &management_endpoint,
        &device_name,
        code.as_deref().map(String::as_str),
        oidc_token,
    )
    .await;
    if let Some(authorization) = &mut browser_authorization {
        let _ = write_browser_result(&mut authorization.callback, result.is_ok()).await;
    }
    result
}

#[allow(clippy::too_many_lines)]
async fn complete_remote_login(
    client: &reqwest::Client,
    authentication_endpoint: &DevelopmentEndpoint,
    management_endpoint: &DevelopmentEndpoint,
    device_name: &DeviceName,
    code: Option<&str>,
    oidc_token: Option<&str>,
) -> Result<(), CliFailure> {
    let mut response = if let Some(oidc_token) = oidc_token {
        client
            .post(format!("{}/v1/auth/oidc", authentication_endpoint.as_str()))
            .bearer_auth(oidc_token)
            .json(&OidcLoginRequestWire {
                device_name: device_name.as_str(),
                invitation_code: code,
            })
            .send()
            .await
    } else {
        let code = code.ok_or(CliFailure {
            code: "PLATFORM_INVITATION_REQUIRED",
            exit: EXIT_AUTH,
        })?;
        client
            .post(format!(
                "{}/v1/auth/exchange",
                authentication_endpoint.as_str()
            ))
            .json(&LoginRequestWire {
                code,
                device_name: device_name.as_str(),
            })
            .send()
            .await
    }
    .map_err(|_| CliFailure {
        code: "PLATFORM_LOGIN_UNAVAILABLE",
        exit: EXIT_UNAVAILABLE,
    })?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(CliFailure {
            code: if matches!(
                response.status(),
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
            ) {
                "PLATFORM_LOGIN_REJECTED"
            } else {
                "PLATFORM_LOGIN_UNAVAILABLE"
            },
            exit: if response.status().is_server_error() {
                EXIT_UNAVAILABLE
            } else {
                EXIT_AUTH
            },
        });
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| CliFailure {
        code: "PLATFORM_LOGIN_RESPONSE_INVALID",
        exit: EXIT_UNAVAILABLE,
    })? {
        if bytes.len().saturating_add(chunk.len()) > 16 * 1024 {
            return Err(CliFailure {
                code: "PLATFORM_LOGIN_RESPONSE_INVALID",
                exit: EXIT_CORRUPT,
            });
        }
        bytes.extend_from_slice(&chunk);
    }
    let result: LoginResponseWire = serde_json::from_slice(&bytes).map_err(|_| CliFailure {
        code: "PLATFORM_LOGIN_RESPONSE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    let LoginResponseWire {
        access_token,
        refresh_token,
        operator_id,
        session_id,
        authorization_revision,
    } = result;
    let access_token = Zeroizing::new(access_token);
    let refresh_token = Zeroizing::new(refresh_token);
    let access = access_token
        .parse::<AccessToken>()
        .map_err(|_| CliFailure {
            code: "PLATFORM_LOGIN_RESPONSE_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    let refresh = refresh_token
        .parse::<RefreshToken>()
        .map_err(|_| CliFailure {
            code: "PLATFORM_LOGIN_RESPONSE_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    let parsed_operator = operator_id.parse::<OperatorId>().map_err(|_| CliFailure {
        code: "PLATFORM_LOGIN_RESPONSE_INVALID",
        exit: EXIT_CORRUPT,
    })?;
    let parsed_session = session_id
        .parse::<OperatorSessionId>()
        .map_err(|_| CliFailure {
            code: "PLATFORM_LOGIN_RESPONSE_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    if access.id() != parsed_session
        || refresh.id() != parsed_session
        || authorization_revision == 0
    {
        return Err(CliFailure {
            code: "PLATFORM_LOGIN_RESPONSE_INVALID",
            exit: EXIT_CORRUPT,
        });
    }
    let path = session_path()?;
    let stored = StoredSessionWire {
        version: 2,
        authentication_server: authentication_endpoint.as_str(),
        server: management_endpoint.as_str(),
        access_token: &access_token,
        refresh_token: &refresh_token,
        operator_id: &operator_id,
        session_id: &session_id,
        authorization_revision,
    };
    let encoded = serde_json::to_vec(&stored).map_err(|_| CliFailure {
        code: "PLATFORM_SESSION_WRITE_FAILED",
        exit: EXIT_INTERNAL,
    })?;
    write_session_file(&path, &encoded)?;
    println!(
        "{{\"authenticationServer\":\"{authentication_endpoint}\",\"managementServer\":\"{management_endpoint}\",\"operatorId\":\"{parsed_operator}\",\"sessionId\":\"{parsed_session}\"}}"
    );
    Ok(())
}

fn login_http_client(endpoint: &DevelopmentEndpoint) -> Result<reqwest::Client, CliFailure> {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(30))
        .https_only(!endpoint.as_str().starts_with("http://"))
        .build()
        .map_err(|_| CliFailure {
            code: "PLATFORM_LOGIN_CLIENT_INVALID",
            exit: EXIT_INTERNAL,
        })
}

async fn discover_authentication(
    client: &reqwest::Client,
    authentication_endpoint: &DevelopmentEndpoint,
) -> Result<(AuthenticationConfigurationWire, DevelopmentEndpoint), CliFailure> {
    let response = client
        .get(format!(
            "{}/v1/auth/config",
            authentication_endpoint.as_str()
        ))
        .send()
        .await
        .map_err(|_| CliFailure {
            code: "PLATFORM_AUTH_CONFIGURATION_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(CliFailure {
            code: "PLATFORM_AUTH_CONFIGURATION_INVALID",
            exit: if response.status().is_server_error() {
                EXIT_UNAVAILABLE
            } else {
                EXIT_AUTH
            },
        });
    }
    let bytes = bounded_response(response, 16 * 1024).await?;
    let configuration: AuthenticationConfigurationWire =
        serde_json::from_slice(&bytes).map_err(|_| CliFailure {
            code: "PLATFORM_AUTH_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    let unique = configuration.methods.iter().collect::<BTreeSet<_>>();
    if configuration.version != 1
        || configuration.methods.is_empty()
        || configuration.methods.len() > 4
        || unique.len() != configuration.methods.len()
        || configuration.methods.iter().any(|method| {
            !matches!(
                method.as_str(),
                "oidcBrowser" | "invitationCode" | "oidcToken"
            )
        })
    {
        return Err(CliFailure {
            code: "PLATFORM_AUTH_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        });
    }
    let management_endpoint = configuration
        .management_endpoint
        .as_deref()
        .unwrap_or(authentication_endpoint.as_str())
        .parse::<DevelopmentEndpoint>()
        .map_err(|_| CliFailure {
            code: "PLATFORM_AUTH_CONFIGURATION_INVALID",
            exit: EXIT_CORRUPT,
        })?;
    Ok((configuration, management_endpoint))
}

fn select_login_method(
    configuration: &AuthenticationConfigurationWire,
    has_invitation_environment: bool,
    has_oidc_token_environment: bool,
    force_browser: bool,
) -> Result<InteractiveLoginMethod, CliFailure> {
    let browser_available = configuration
        .methods
        .iter()
        .any(|method| method == "oidcBrowser");
    let invitation_available = configuration
        .methods
        .iter()
        .any(|method| method == "invitationCode");
    let oidc_token_available = configuration
        .methods
        .iter()
        .any(|method| method == "oidcToken");
    if has_oidc_token_environment {
        if !oidc_token_available {
            return Err(CliFailure {
                code: "PLATFORM_OIDC_NOT_CONFIGURED",
                exit: EXIT_AUTH,
            });
        }
        return Ok(InteractiveLoginMethod::OidcToken);
    }
    if force_browser {
        if !browser_available {
            return Err(CliFailure {
                code: "PLATFORM_OIDC_NOT_CONFIGURED",
                exit: EXIT_AUTH,
            });
        }
        return Ok(InteractiveLoginMethod::Browser);
    }
    if has_invitation_environment {
        if !invitation_available {
            return Err(CliFailure {
                code: "PLATFORM_INVITATION_NOT_CONFIGURED",
                exit: EXIT_AUTH,
            });
        }
        return Ok(InteractiveLoginMethod::Invitation);
    }
    match (browser_available, invitation_available) {
        (true, true) => prompt_login_method(),
        (true, false) => Ok(InteractiveLoginMethod::Browser),
        (false, true) => Ok(InteractiveLoginMethod::Invitation),
        (false, false) => Err(CliFailure {
            code: "PLATFORM_LOGIN_METHOD_UNAVAILABLE",
            exit: EXIT_AUTH,
        }),
    }
}

fn prompt_login_method() -> Result<InteractiveLoginMethod, CliFailure> {
    if !std::io::stdin().is_terminal() {
        return Err(CliFailure {
            code: "PLATFORM_LOGIN_SELECTION_REQUIRED",
            exit: EXIT_AUTH,
        });
    }
    eprintln!("Authentication methods:");
    eprintln!("  1. Sign in with the configured identity provider (recommended)");
    eprintln!("  2. Use a Runku invitation code");
    let answer = prompt_line("Select a method [1]: ")?;
    match answer.trim() {
        "" | "1" => Ok(InteractiveLoginMethod::Browser),
        "2" => Ok(InteractiveLoginMethod::Invitation),
        _ => Err(CliFailure {
            code: "PLATFORM_LOGIN_SELECTION_INVALID",
            exit: EXIT_INVALID,
        }),
    }
}

fn invitation_from_environment(
    environment: Option<&TokenEnvironmentName>,
) -> Result<Option<Zeroizing<String>>, CliFailure> {
    environment
        .map(|environment| {
            std::env::var(environment.as_str())
                .ok()
                .filter(|value| {
                    value.starts_with("rk_inv_v1_")
                        && value.len() <= 256
                        && value.trim() == value.as_str()
                        && !value.chars().any(char::is_control)
                })
                .map(Zeroizing::new)
                .ok_or(CliFailure {
                    code: "PLATFORM_INVITATION_ENV_INVALID",
                    exit: EXIT_AUTH,
                })
        })
        .transpose()
}

fn prompt_invitation_code() -> Result<Zeroizing<String>, CliFailure> {
    if !std::io::stdin().is_terminal() {
        return Err(CliFailure {
            code: "PLATFORM_INVITATION_REQUIRED",
            exit: EXIT_AUTH,
        });
    }
    let code = Zeroizing::new(
        rpassword::prompt_password("Invitation code: ").map_err(|_| CliFailure {
            code: "PLATFORM_INVITATION_READ_FAILED",
            exit: EXIT_UNAVAILABLE,
        })?,
    );
    if !code.starts_with("rk_inv_v1_")
        || code.len() > 256
        || code.trim() != code.as_str()
        || code.chars().any(char::is_control)
    {
        return Err(CliFailure {
            code: "PLATFORM_INVITATION_INVALID",
            exit: EXIT_AUTH,
        });
    }
    Ok(code)
}

fn oidc_token_from_environment(
    environment: Option<&TokenEnvironmentName>,
) -> Result<Option<Zeroizing<String>>, CliFailure> {
    environment
        .map(|environment| {
            std::env::var(environment.as_str())
                .ok()
                .filter(|value| {
                    !value.is_empty() && value.len() <= 16 * 1024 && value.trim() == value.as_str()
                })
                .map(Zeroizing::new)
                .ok_or(CliFailure {
                    code: "PLATFORM_OIDC_TOKEN_ENV_INVALID",
                    exit: EXIT_AUTH,
                })
        })
        .transpose()
}

fn resolve_device_name(explicit: Option<&DeviceName>) -> Result<DeviceName, CliFailure> {
    if let Some(device) = explicit {
        return Ok(device.clone());
    }
    for variable in ["COMPUTERNAME", "HOSTNAME"] {
        if let Ok(value) = std::env::var(variable)
            && let Ok(device) = value.parse::<DeviceName>()
        {
            return Ok(device);
        }
    }
    "runku-cli".parse::<DeviceName>().map_err(|_| CliFailure {
        code: "PLATFORM_DEVICE_INVALID",
        exit: EXIT_INTERNAL,
    })
}

fn resolve_authentication_endpoint(
    explicit: Option<&DevelopmentEndpoint>,
) -> Result<DevelopmentEndpoint, CliFailure> {
    if let Some(endpoint) = explicit {
        return Ok(endpoint.clone());
    }
    if let Some(saved) = saved_authentication_endpoint()? {
        if !std::io::stdin().is_terminal() {
            return Ok(saved);
        }
        let answer = prompt_line(&format!(
            "Connect to the saved authentication server {saved}? [Y/n] "
        ))?;
        if matches!(answer.trim(), "" | "y" | "Y" | "yes" | "YES") {
            return Ok(saved);
        }
        let value = prompt_line(&format!(
            "Authentication server [{DEFAULT_AUTHENTICATION_SERVER}]: "
        ))?;
        let selected = if value.trim().is_empty() {
            DEFAULT_AUTHENTICATION_SERVER
        } else {
            value.trim()
        };
        return selected
            .parse::<DevelopmentEndpoint>()
            .map_err(|_| CliFailure {
                code: "PLATFORM_AUTH_SERVER_INVALID",
                exit: EXIT_INVALID,
            });
    }
    DEFAULT_AUTHENTICATION_SERVER
        .parse::<DevelopmentEndpoint>()
        .map_err(|_| CliFailure {
            code: "PLATFORM_AUTH_SERVER_INVALID",
            exit: EXIT_INTERNAL,
        })
}

fn saved_authentication_endpoint() -> Result<Option<DevelopmentEndpoint>, CliFailure> {
    let path = session_path()?;
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(CliFailure {
                code: "PLATFORM_SESSION_FILE_INVALID",
                exit: EXIT_AUTH,
            });
        }
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 32 * 1024 {
        return Err(CliFailure {
            code: "PLATFORM_SESSION_FILE_INVALID",
            exit: EXIT_AUTH,
        });
    }
    let bytes = std::fs::read(path).map_err(|_| CliFailure {
        code: "PLATFORM_SESSION_FILE_INVALID",
        exit: EXIT_AUTH,
    })?;
    let session: StoredSession = serde_json::from_slice(&bytes).map_err(|_| CliFailure {
        code: "PLATFORM_SESSION_FILE_INVALID",
        exit: EXIT_AUTH,
    })?;
    let endpoint = session
        .authentication_server
        .as_deref()
        .unwrap_or(session.server.as_str())
        .parse::<DevelopmentEndpoint>()
        .map_err(|_| CliFailure {
            code: "PLATFORM_SESSION_FILE_INVALID",
            exit: EXIT_AUTH,
        })?;
    Ok(Some(endpoint))
}

fn prompt_line(prompt: &str) -> Result<String, CliFailure> {
    eprint!("{prompt}");
    std::io::stderr().flush().map_err(|_| CliFailure {
        code: "PLATFORM_LOGIN_PROMPT_UNAVAILABLE",
        exit: EXIT_UNAVAILABLE,
    })?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .map_err(|_| CliFailure {
            code: "PLATFORM_LOGIN_PROMPT_UNAVAILABLE",
            exit: EXIT_UNAVAILABLE,
        })?;
    Ok(answer)
}

fn session_path() -> Result<PathBuf, CliFailure> {
    if let Ok(directory) = std::env::var("RUNKU_CONFIG_HOME") {
        let directory = PathBuf::from(directory);
        if directory.is_absolute() && directory != Path::new("/") {
            return Ok(directory.join("credentials-v1.json"));
        }
        return Err(CliFailure {
            code: "PLATFORM_SESSION_PATH_INVALID",
            exit: EXIT_INVALID,
        });
    }
    let base = if cfg!(target_os = "windows") {
        std::env::var_os("APPDATA").map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|path| path.join("Library/Application Support"))
    } else {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .map(|path| path.join(".config"))
            })
    }
    .ok_or(CliFailure {
        code: "PLATFORM_SESSION_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    Ok(base.join("runku/credentials-v1.json"))
}

fn write_session_file(path: &Path, bytes: &[u8]) -> Result<(), CliFailure> {
    let parent = path.parent().ok_or(CliFailure {
        code: "PLATFORM_SESSION_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    #[cfg(unix)]
    let parent_existed = parent.exists();
    std::fs::create_dir_all(parent).map_err(|_| CliFailure {
        code: "PLATFORM_SESSION_WRITE_FAILED",
        exit: EXIT_UNAVAILABLE,
    })?;
    let parent_metadata = std::fs::symlink_metadata(parent).map_err(|_| CliFailure {
        code: "PLATFORM_SESSION_PATH_INVALID",
        exit: EXIT_INVALID,
    })?;
    if !parent_metadata.is_dir() || parent_metadata.file_type().is_symlink() {
        return Err(CliFailure {
            code: "PLATFORM_SESSION_PATH_INVALID",
            exit: EXIT_INVALID,
        });
    }
    #[cfg(unix)]
    if !parent_existed {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(|_| {
            CliFailure {
                code: "PLATFORM_SESSION_WRITE_FAILED",
                exit: EXIT_UNAVAILABLE,
            }
        })?;
    }
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| !metadata.file_type().is_file()) {
        return Err(CliFailure {
            code: "PLATFORM_SESSION_PATH_INVALID",
            exit: EXIT_INVALID,
        });
    }
    let temporary = path.with_extension("json.new");
    match std::fs::symlink_metadata(&temporary) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            std::fs::remove_file(&temporary).map_err(|_| CliFailure {
                code: "PLATFORM_SESSION_WRITE_FAILED",
                exit: EXIT_UNAVAILABLE,
            })?;
        }
        Ok(_) => {
            return Err(CliFailure {
                code: "PLATFORM_SESSION_PATH_INVALID",
                exit: EXIT_INVALID,
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(CliFailure {
                code: "PLATFORM_SESSION_WRITE_FAILED",
                exit: EXIT_UNAVAILABLE,
            });
        }
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(|_| CliFailure {
        code: "PLATFORM_SESSION_WRITE_FAILED",
        exit: EXIT_UNAVAILABLE,
    })?;
    file.write_all(bytes)
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all())
        .map_err(|_| CliFailure {
            code: "PLATFORM_SESSION_WRITE_FAILED",
            exit: EXIT_UNAVAILABLE,
        })?;
    if std::fs::rename(&temporary, path).is_ok() {
        return Ok(());
    }
    if path.is_file() {
        std::fs::remove_file(path).map_err(|_| CliFailure {
            code: "PLATFORM_SESSION_WRITE_FAILED",
            exit: EXIT_UNAVAILABLE,
        })?;
    }
    std::fs::rename(temporary, path).map_err(|_| CliFailure {
        code: "PLATFORM_SESSION_WRITE_FAILED",
        exit: EXIT_UNAVAILABLE,
    })
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
    use super::{
        AuthenticationConfigurationWire, CliFailure, EXIT_CONFLICT, EXIT_INVALID,
        InteractiveLoginMethod, OidcClientConfigurationWire, explain_failure,
        parse_oidc_callback_request, select_login_method, validate_oidc_client_configuration,
    };

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

    #[test]
    fn oidc_callback_binds_host_state_issuer_and_single_parameters() {
        let valid = b"GET /callback?code=abc%26client_id%3Devil&state=state-1&iss=https%3A%2F%2Fidentity.example.com HTTP/1.1\r\nHost: 127.0.0.1:4321\r\nConnection: close\r\n\r\n";
        let code = parse_oidc_callback_request(
            valid,
            "127.0.0.1:4321",
            "state-1",
            "https://identity.example.com",
        );
        assert!(matches!(code.as_deref(), Ok("abc&client_id=evil")));

        for request in [
            b"GET /callback?code=abc&state=wrong HTTP/1.1\r\nHost: 127.0.0.1:4321\r\n\r\n".as_slice(),
            b"GET /callback?code=abc&state=state-1&iss=https%3A%2F%2Fevil.example HTTP/1.1\r\nHost: 127.0.0.1:4321\r\n\r\n".as_slice(),
            b"GET /callback?code=abc&code=other&state=state-1 HTTP/1.1\r\nHost: 127.0.0.1:4321\r\n\r\n".as_slice(),
            b"GET /callback?code=abc&state=state-1&state=other HTTP/1.1\r\nHost: 127.0.0.1:4321\r\n\r\n".as_slice(),
            b"GET /callback?code=abc&state=state-1&error=access_denied HTTP/1.1\r\nHost: 127.0.0.1:4321\r\n\r\n".as_slice(),
            b"GET /callback?code=abc&state=state-1 HTTP/1.1\r\nHost: 127.0.0.1:9999\r\n\r\n".as_slice(),
            b"GET /callback?code=abc&state=state-1 HTTP/1.1\r\nHost: 127.0.0.1:4321\r\nHost: 127.0.0.1:4321\r\n\r\n".as_slice(),
            b"POST /callback?code=abc&state=state-1 HTTP/1.1\r\nHost: 127.0.0.1:4321\r\n\r\n".as_slice(),
            b"GET /callback?code=abc%0d%0aInjected%3Ayes&state=state-1 HTTP/1.1\r\nHost: 127.0.0.1:4321\r\n\r\n".as_slice(),
            b"GET /callback?code=abc&state=state-1 HTTP/1.1\r\nHost: 127.0.0.1:4321\r\n\r\nbody".as_slice(),
        ] {
            assert!(
                parse_oidc_callback_request(
                    request,
                    "127.0.0.1:4321",
                    "state-1",
                    "https://identity.example.com",
                )
                .is_err()
            );
        }
    }

    #[test]
    fn login_selection_requires_an_advertised_explicit_method() {
        let configuration = AuthenticationConfigurationWire {
            version: 1,
            methods: vec![
                "oidcBrowser".to_owned(),
                "invitationCode".to_owned(),
                "oidcToken".to_owned(),
            ],
            management_endpoint: None,
        };
        assert_eq!(
            select_login_method(&configuration, false, false, true).ok(),
            Some(InteractiveLoginMethod::Browser)
        );
        assert_eq!(
            select_login_method(&configuration, true, false, false).ok(),
            Some(InteractiveLoginMethod::Invitation)
        );
        assert_eq!(
            select_login_method(&configuration, false, true, false).ok(),
            Some(InteractiveLoginMethod::OidcToken)
        );
        let invitation_only = AuthenticationConfigurationWire {
            version: 1,
            methods: vec!["invitationCode".to_owned()],
            management_endpoint: None,
        };
        assert!(select_login_method(&invitation_only, false, false, true).is_err());
        assert!(select_login_method(&invitation_only, false, true, false).is_err());
    }

    #[test]
    fn native_oidc_configuration_rejects_injected_or_ambiguous_values() {
        let valid = || OidcClientConfigurationWire {
            issuer: "https://identity.example.com/tenant".to_owned(),
            authorization_endpoint: "https://identity.example.com/authorize".to_owned(),
            token_endpoint: "https://identity.example.com/token".to_owned(),
            client_id: "runku-cli".to_owned(),
            scopes: vec!["openid".to_owned(), "profile".to_owned()],
            resource: None,
        };
        assert!(validate_oidc_client_configuration(&valid()).is_ok());

        let mut injected_endpoint = valid();
        injected_endpoint.authorization_endpoint =
            "https://identity.example.com/authorize?redirect=https://evil.example".to_owned();
        assert!(validate_oidc_client_configuration(&injected_endpoint).is_err());

        let mut embedded_credentials = valid();
        embedded_credentials.token_endpoint =
            "https://attacker:secret@identity.example.com/token".to_owned();
        assert!(validate_oidc_client_configuration(&embedded_credentials).is_err());

        let mut insecure_issuer = valid();
        insecure_issuer.issuer = "http://identity.example.com/tenant".to_owned();
        assert!(validate_oidc_client_configuration(&insecure_issuer).is_err());

        let mut duplicate_scope = valid();
        duplicate_scope.scopes.push("openid".to_owned());
        assert!(validate_oidc_client_configuration(&duplicate_scope).is_err());

        let mut unsafe_resource = valid();
        unsafe_resource.resource = Some("http://identity.example.com/api".to_owned());
        assert!(validate_oidc_client_configuration(&unsafe_resource).is_err());

        let mut safe_resource = valid();
        safe_resource.resource = Some("https://api.example.com/runku".to_owned());
        assert!(validate_oidc_client_configuration(&safe_resource).is_ok());
    }
}
