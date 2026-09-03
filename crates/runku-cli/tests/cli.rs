//! Black-box executable conformance for local CLI lifecycle and exit codes.

use std::{
    error::Error,
    io::{BufRead, BufReader, Read as _, Write as _},
    net::TcpListener,
    path::Path,
    process::{Command, Stdio},
};

use runku_core::{BuildId, DocumentId, FunctionId, ReleaseId, TableId};
use runku_local::load_local;
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_release_manifest, encode_safe_esm_bundle,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;

fn run(args: &[&str]) -> Result<std::process::Output, Box<dyn Error>> {
    Ok(Command::new(env!("CARGO_BIN_EXE_runku"))
        .args(args)
        .output()?)
}

fn run_in(directory: &Path, args: &[&str]) -> Result<std::process::Output, Box<dyn Error>> {
    Ok(Command::new(env!("CARGO_BIN_EXE_runku"))
        .current_dir(directory)
        .args(args)
        .output()?)
}

fn failure_stderr<'a>(
    output: &'a std::process::Output,
    code: &str,
) -> Result<&'a str, std::str::Utf8Error> {
    let stderr = std::str::from_utf8(&output.stderr)?;
    assert!(stderr.starts_with(&format!("error: {code}\n")));
    assert!(stderr.contains("message: "));
    assert!(stderr.contains("hint: "));
    Ok(stderr)
}

#[test]
fn executable_defaults_project_root_to_current_directory() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let missing = run_in(directory.path(), &["dev"])?;
    assert_eq!(missing.status.code(), Some(3));
    let missing_stderr = failure_stderr(&missing, "BUILD_PATH_INVALID")?;
    assert!(missing_stderr.contains("runku/"));
    assert!(!missing_stderr.contains(&directory.path().to_string_lossy().to_string()));
    assert!(!directory.path().join(".runku").exists());

    let initialized = run_in(
        directory.path(),
        &["init", "--workspace", "local", "--listen", "127.0.0.1:0"],
    )?;
    assert!(initialized.status.success());
    assert!(
        directory
            .path()
            .join(".runku/local-state-v1.json")
            .is_file()
    );

    let clients = run_in(directory.path(), &["client", "list"])?;
    assert!(clients.status.success());
    let clients: serde_json::Value = serde_json::from_slice(&clients.stdout)?;
    assert_eq!(clients["clients"], serde_json::json!([]));
    Ok(())
}

#[test]
fn dev_owns_local_credentials_detects_next_and_preserves_remote_configuration()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    std::fs::write(
        directory.path().join("next.config.ts"),
        "export default {}\n",
    )?;
    let remote = concat!(
        "UNCHANGED=value\n",
        "NEXT_PUBLIC_RUNKU_URL=https://remote.example\n",
        "NEXT_PUBLIC_RUNKU_TARGET=workspace:team/dev\n",
        "NEXT_PUBLIC_RUNKU_KEY=rk_pub_v1_7ZZZZZZZZZZZZZZZZZZZZZZZZZ_AAAAAAAAAAAAAAAAAAAAAA\n",
        "RUNKU_SECRET_KEY=rk_sec_v1_7ZZZZZZZZZZZZZZZZZZZZZZZZZ.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n",
    );
    let env = directory.path().join(".env.local");
    std::fs::write(&env, remote)?;

    let preserved = run(&["dev", "--root", root, "--prepare"])?;
    assert_eq!(preserved.status.code(), Some(4));
    failure_stderr(&preserved, "LOCAL_APPLICATION_ENV_CONFIRMATION_REQUIRED")?;
    assert_eq!(std::fs::read_to_string(&env)?, remote);
    assert!(!String::from_utf8_lossy(&preserved.stderr).contains("rk_pub_v1_"));
    assert!(!String::from_utf8_lossy(&preserved.stderr).contains("rk_sec_v1_"));

    let replaced = run(&[
        "dev",
        "--root",
        root,
        "--prepare",
        "--replace-remote-credentials",
    ])?;
    assert!(replaced.status.success());
    let prepared: serde_json::Value = serde_json::from_slice(&replaced.stdout)?;
    assert_eq!(prepared["status"], "prepared");
    let local = std::fs::read_to_string(&env)?;
    assert!(local.contains("UNCHANGED=value"));
    assert!(local.contains("NEXT_PUBLIC_RUNKU_URL=http://127.0.0.1:3210"));
    assert!(local.contains("NEXT_PUBLIC_RUNKU_TARGET=workspace:local"));
    assert!(local.contains("NEXT_PUBLIC_RUNKU_KEY=rk_pub_v1_"));
    assert!(local.contains("RUNKU_SECRET_KEY=rk_sec_v1_"));
    assert!(local.contains("RUNKU_URL=http://127.0.0.1:3210"));
    assert!(!local.contains("remote.example"));
    assert!(!local.contains("7ZZZZZZZZZZZZZZZZZZZZZZZZZ"));
    assert!(!String::from_utf8_lossy(&replaced.stdout).contains("rk_pub_v1_"));
    assert!(!String::from_utf8_lossy(&replaced.stdout).contains("rk_sec_v1_"));

    let before = local;
    let replay = run(&["dev", "--root", root, "--prepare"])?;
    assert!(replay.status.success());
    assert_eq!(std::fs::read_to_string(&env)?, before);
    let metadata = std::fs::metadata(&env)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }
    Ok(())
}

#[test]
fn dev_detects_remote_values_in_the_effective_dotenv_fallback() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    std::fs::write(
        directory.path().join("vite.config.ts"),
        "export default {}\n",
    )?;
    let remote = concat!(
        "UNCHANGED=value\n",
        "VITE_RUNKU_URL=https://remote.example\n",
        "VITE_RUNKU_TARGET=workspace:team/dev\n",
        "VITE_RUNKU_KEY=rk_pub_v1_7ZZZZZZZZZZZZZZZZZZZZZZZZZ_AAAAAAAAAAAAAAAAAAAAAA\n",
        "RUNKU_SECRET_KEY=rk_sec_v1_7ZZZZZZZZZZZZZZZZZZZZZZZZZ.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n",
    );
    let fallback = directory.path().join(".env");
    std::fs::write(&fallback, remote)?;

    let preserved = run(&["dev", "--root", root, "--prepare"])?;
    assert_eq!(preserved.status.code(), Some(4));
    failure_stderr(&preserved, "LOCAL_APPLICATION_ENV_CONFIRMATION_REQUIRED")?;
    assert_eq!(std::fs::read_to_string(&fallback)?, remote);
    assert!(!directory.path().join(".env.local").exists());

    let replaced = run(&[
        "dev",
        "--root",
        root,
        "--prepare",
        "--replace-remote-credentials",
    ])?;
    assert!(replaced.status.success());
    assert_eq!(std::fs::read_to_string(&fallback)?, remote);
    let local = std::fs::read_to_string(directory.path().join(".env.local"))?;
    assert!(local.contains("VITE_RUNKU_URL=http://127.0.0.1:3210"));
    assert!(local.contains("VITE_RUNKU_KEY=rk_pub_v1_"));
    assert!(local.contains("RUNKU_SECRET_KEY=rk_sec_v1_"));
    assert!(!local.contains("remote.example"));
    assert!(!local.contains("7ZZZZZZZZZZZZZZZZZZZZZZZZZ"));
    Ok(())
}

#[test]
fn dev_detects_frontend_env_conventions_and_rejects_ambiguity() -> Result<(), Box<dyn Error>> {
    for (config, expected, rejected) in [
        (
            Some("vite.config.ts"),
            "VITE_RUNKU_KEY=rk_pub_v1_",
            "NEXT_PUBLIC_RUNKU_KEY=",
        ),
        (
            Some("svelte.config.js"),
            "PUBLIC_RUNKU_KEY=rk_pub_v1_",
            "VITE_RUNKU_KEY=",
        ),
        (
            Some("vue.config.js"),
            "VUE_APP_RUNKU_KEY=rk_pub_v1_",
            "VITE_RUNKU_KEY=",
        ),
        (
            Some("angular.json"),
            "RUNKU_KEY=rk_pub_v1_",
            "NEXT_PUBLIC_RUNKU_KEY=",
        ),
        (None, "RUNKU_KEY=rk_pub_v1_", "VITE_RUNKU_KEY="),
    ] {
        let directory = tempdir()?;
        let root = directory.path().to_str().ok_or("non-Unicode test path")?;
        if let Some(config) = config {
            std::fs::write(directory.path().join(config), "{}\n")?;
        }
        if config.is_some_and(|config| config.starts_with("svelte")) {
            std::fs::write(
                directory.path().join("vite.config.ts"),
                "export default {}\n",
            )?;
        }
        let prepared = run(&["dev", "--root", root, "--prepare"])?;
        assert!(prepared.status.success(), "{config:?}");
        let env = std::fs::read_to_string(directory.path().join(".env.local"))?;
        assert!(env.contains(expected), "{config:?}: {env}");
        assert!(!env.contains(rejected), "{config:?}: {env}");
        assert!(env.contains("RUNKU_SECRET_KEY=rk_sec_v1_"));
    }

    let ambiguous = tempdir()?;
    let root = ambiguous.path().to_str().ok_or("non-Unicode test path")?;
    std::fs::write(
        ambiguous.path().join("next.config.ts"),
        "export default {}\n",
    )?;
    std::fs::write(
        ambiguous.path().join("vite.config.ts"),
        "export default {}\n",
    )?;
    let output = run(&["dev", "--root", root, "--prepare"])?;
    assert_eq!(output.status.code(), Some(4));
    failure_stderr(&output, "LOCAL_APPLICATION_ENV_AMBIGUOUS")?;
    assert!(!ambiguous.path().join(".env.local").exists());

    let explicit = run(&[
        "dev",
        "--root",
        root,
        "--prepare",
        "--public-env-prefix",
        "WEB_PUBLIC_RUNKU_",
    ])?;
    assert!(explicit.status.success());
    assert!(
        std::fs::read_to_string(ambiguous.path().join(".env.local"))?
            .contains("WEB_PUBLIC_RUNKU_KEY=rk_pub_v1_")
    );
    Ok(())
}

#[test]
fn dev_rejects_ambiguous_managed_assignments_without_replacing_the_file()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    let env = directory.path().join(".env.local");
    let duplicate = "RUNKU_URL=http://127.0.0.1:3210\nRUNKU_URL=http://127.0.0.1:3211\n";
    std::fs::write(&env, duplicate)?;

    let output = run(&["dev", "--root", root, "--prepare"])?;
    assert_eq!(output.status.code(), Some(3));
    failure_stderr(&output, "LOCAL_APPLICATION_ENV_INVALID")?;
    assert_eq!(std::fs::read_to_string(env)?, duplicate);
    Ok(())
}

#[cfg(unix)]
#[test]
fn dev_refuses_a_symlinked_application_environment() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    let target = directory.path().join("actual.env");
    std::fs::write(&target, "PRESERVED=value\n")?;
    symlink(&target, directory.path().join(".env.local"))?;

    let output = run(&["dev", "--root", root, "--prepare"])?;
    assert_eq!(output.status.code(), Some(3));
    failure_stderr(&output, "LOCAL_APPLICATION_ENV_PATH_INVALID")?;
    assert_eq!(std::fs::read_to_string(target)?, "PRESERVED=value\n");
    Ok(())
}

#[tokio::test]
async fn explicit_product_scope_is_exact_idempotent_and_conflict_safe() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    let project_id = "prj_00000000000000000000000001";
    let environment_id = "env_00000000000000000000000002";
    let init = |environment_id: &str| {
        run(&[
            "init",
            "--root",
            root,
            "--listen",
            "127.0.0.1:0",
            "--project-id",
            project_id,
            "--environment-id",
            environment_id,
        ])
    };

    let first = init(environment_id)?;
    assert!(first.status.success());
    let replay = init(environment_id)?;
    assert!(replay.status.success());
    assert_eq!(replay.stdout, first.stdout);
    let initialized: serde_json::Value = serde_json::from_slice(&first.stdout)?;
    assert_eq!(initialized["projectId"], project_id);
    assert_eq!(initialized["environmentId"], environment_id);

    let conflict = init("env_00000000000000000000000003")?;
    assert_eq!(conflict.status.code(), Some(4));
    failure_stderr(&conflict, "LOCAL_STATE_CONFLICT")?;
    let durable = load_local(directory.path()).await?.0;
    assert_eq!(durable.project_id.to_string(), project_id);
    assert_eq!(durable.environment_id.to_string(), environment_id);
    Ok(())
}

#[tokio::test]
async fn executable_lifecycle_is_strict_idempotent_and_graceful() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    let init_args = [
        "init",
        "--root",
        root,
        "--workspace",
        "default",
        "--listen",
        "127.0.0.1:0",
    ];
    let first = run(&init_args)?;
    assert!(first.status.success());
    let second = run(&init_args)?;
    assert!(second.status.success());
    assert_eq!(first.stdout, second.stdout);

    let divergent = run(&[
        "init",
        "--root",
        root,
        "--workspace",
        "other",
        "--listen",
        "127.0.0.1:0",
    ])?;
    assert_eq!(divergent.status.code(), Some(4));
    failure_stderr(&divergent, "LOCAL_STATE_CONFLICT")?;

    let state = load_local(directory.path()).await?.0;
    let source = "export default async (_ctx, value) => value;";
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact = encode_safe_esm_bundle(&bundle)?;
    let contract = Sha256Digest::of(b"cli-test-contract");
    let manifest = encode_release_manifest(&ReleaseManifestV1 {
        release_id: ReleaseId::generate(),
        project_id: state.project_id,
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_800_000_000_000_000),
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
        cron_definitions: vec![],
    })?;
    let manifest_path = directory.path().join("manifest.bin");
    let artifact_path = directory.path().join("artifact.bin");
    std::fs::write(&manifest_path, manifest)?;
    std::fs::write(&artifact_path, artifact)?;
    let manifest_path = manifest_path.to_str().ok_or("non-Unicode manifest path")?;
    let artifact_path = artifact_path.to_str().ok_or("non-Unicode artifact path")?;
    let publish_args = [
        "publish",
        "--root",
        root,
        "--manifest",
        manifest_path,
        "--artifact",
        artifact_path,
    ];
    assert!(run(&publish_args)?.status.success());
    let replay = run(&publish_args)?;
    assert!(replay.status.success());
    assert!(String::from_utf8(replay.stdout)?.contains("\"replayed\":true"));
    assert!(run(&["doctor", "--root", root])?.status.success());

    let invalid = run(&["doctor", "--root", root, "--unknown", "value"])?;
    assert_eq!(invalid.status.code(), Some(2));
    let invalid_stderr = failure_stderr(&invalid, "CLI_USAGE_INVALID")?;
    assert!(invalid_stderr.contains("message: The command or its arguments are invalid."));
    assert!(invalid_stderr.contains("USAGE:"));

    #[cfg(unix)]
    {
        let mut child = Command::new(env!("CARGO_BIN_EXE_runku"))
            .args(["dev", "--root", root, "--prebuilt"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdout = child.stdout.take().ok_or("dev stdout unavailable")?;
        let mut lines = BufReader::new(stdout).lines();
        let ready = lines.next().ok_or("dev exited before readiness")??;
        assert!(ready.contains("\"status\":\"ready\""));
        let signal = Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()?;
        assert!(signal.success());
        assert!(child.wait()?.success());
    }
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn release_channel_cli_promotes_rolls_back_and_blocks_incompatible_code()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    assert!(
        run(&[
            "init",
            "--root",
            root,
            "--workspace",
            "default",
            "--listen",
            "127.0.0.1:0",
        ])?
        .status
        .success()
    );
    let functions = directory.path().join("runku");
    std::fs::create_dir(&functions)?;
    let source = functions.join("queries.ts");
    write_version_source(&source, "v1")?;
    std::fs::write(
        functions.join("schema.ts"),
        "import { defineSchema } from '@runku/server';\nexport default defineSchema({});\n",
    )?;

    let build_publish = |root: &str| -> Result<serde_json::Value, Box<dyn Error>> {
        let built = run(&["build", "--root", root])?;
        if !built.status.success() {
            return Err(String::from_utf8_lossy(&built.stderr).into_owned().into());
        }
        let built: serde_json::Value = serde_json::from_slice(&built.stdout)?;
        let published = run(&[
            "publish",
            "--root",
            root,
            "--manifest",
            built["manifestPath"]
                .as_str()
                .ok_or("manifest path missing")?,
            "--artifact",
            built["artifactPath"]
                .as_str()
                .ok_or("artifact path missing")?,
        ])?;
        if !published.status.success() {
            return Err(String::from_utf8_lossy(&published.stderr)
                .into_owned()
                .into());
        }
        Ok(built)
    };

    let first = build_publish(root)?;
    let first_id = first["releaseId"].as_str().ok_or("first release missing")?;
    let released = run(&["release", "--root", root, "--release", first_id])?;
    assert!(released.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&released.stdout)?["status"],
        "servable"
    );
    let promoted = run(&[
        "promote",
        "--root",
        root,
        "--channel",
        "stable",
        "--release",
        first_id,
        "--expected",
        "empty",
    ])?;
    assert!(promoted.status.success());

    write_version_source(&source, "v2")?;
    let second = build_publish(root)?;
    let second_id = second["releaseId"]
        .as_str()
        .ok_or("second release missing")?;
    assert!(
        run(&["release", "--root", root, "--release", second_id,])?
            .status
            .success()
    );
    assert!(
        run(&[
            "promote",
            "--root",
            root,
            "--channel",
            "stable",
            "--release",
            second_id,
            "--expected",
            first_id,
        ])?
        .status
        .success()
    );
    assert!(
        run(&[
            "rollback",
            "--root",
            root,
            "--channel",
            "stable",
            "--expected",
            second_id,
            "--to",
            first_id,
        ])?
        .status
        .success()
    );
    let status = run(&["status", "--root", root])?;
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout)?;
    assert_eq!(status["defaultChannel"], "stable");
    assert_eq!(status["channels"][0]["releaseId"], first_id);

    std::fs::write(
        &source,
        "import { query, v } from '@runku/server';\nexport const version = query({ auth: 'user', visibility: 'public', capabilities: [], args: v.any(), returns: v.string(), handler() { return 'v3'; } });\n",
    )?;
    let incompatible = build_publish(root)?;
    let incompatible_id = incompatible["releaseId"]
        .as_str()
        .ok_or("incompatible release missing")?;
    let blocked = run(&["release", "--root", root, "--release", incompatible_id])?;
    assert_eq!(blocked.status.code(), Some(4));
    let blocked_stderr = failure_stderr(&blocked, "RELEASE_COMPATIBILITY_BLOCKED")?;
    assert!(
        blocked_stderr.contains(
            "message: The requested Release transition is blocked by compatibility rules."
        )
    );
    assert!(blocked_stderr.contains("hint: Review the diagnostics emitted on stdout"));
    let blocked: serde_json::Value = serde_json::from_slice(&blocked.stdout)?;
    assert_eq!(blocked["compatible"], false);
    assert_eq!(blocked["status"], "compatibility_blocked");
    assert_eq!(
        blocked["diagnostics"][0]["code"],
        "PUBLIC_FUNCTION_METADATA_CHANGED"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn sdk_http_realtime_and_restart_preserve_local_data() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    assert!(
        run(&[
            "init",
            "--root",
            root,
            "--workspace",
            "default",
            "--listen",
            "127.0.0.1:0",
        ])?
        .status
        .success()
    );
    let application_key = create_test_application_key(root)?;
    let state = load_local(directory.path()).await?.0;
    let mutation_source = r"
        export default async (ctx, input) => {
          await ctx.db.insert(input.tableId, input.documentId, input.value);
          return input.value;
        };
    ";
    let query_source = r"
        export default async (ctx, input) => {
          const document = await ctx.db.get(input.tableId, input.documentId);
          return document === null ? null : document.value;
        };
    ";
    let bundle = SafeEsmBundleV1::from_sources([mutation_source, query_source])?;
    let artifact = encode_safe_esm_bundle(&bundle)?;
    let contract = Sha256Digest::of(b"sdk-local-e2e-contract");
    let manifest = encode_release_manifest(&ReleaseManifestV1 {
        release_id: ReleaseId::generate(),
        project_id: state.project_id,
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_800_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: contract,
        schema_contract_hash: contract,
        index_contract_hash: contract,
        functions: vec![
            FunctionManifest {
                id: FunctionId::generate(),
                name: "mutations.insert".parse()?,
                function_type: FunctionType::Mutation,
                visibility: FunctionVisibility::Public,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(mutation_source.as_bytes()),
                arguments_contract_hash: contract,
                result_contract_hash: contract,
                capabilities: vec![Capability::DbWrite],
            },
            FunctionManifest {
                id: FunctionId::generate(),
                name: "queries.document".parse()?,
                function_type: FunctionType::Query,
                visibility: FunctionVisibility::Public,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(query_source.as_bytes()),
                arguments_contract_hash: contract,
                result_contract_hash: contract,
                capabilities: vec![Capability::DbRead],
            },
        ],
        cron_definitions: vec![],
    })?;
    let manifest_path = directory.path().join("e2e-manifest.bin");
    let artifact_path = directory.path().join("e2e-artifact.bin");
    std::fs::write(&manifest_path, manifest)?;
    std::fs::write(&artifact_path, artifact)?;
    assert!(
        run(&[
            "publish",
            "--root",
            root,
            "--manifest",
            manifest_path.to_str().ok_or("non-Unicode manifest path")?,
            "--artifact",
            artifact_path.to_str().ok_or("non-Unicode artifact path")?,
        ])?
        .status
        .success()
    );

    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/client/dist/index.js")
        .canonicalize()?;
    let table_id = TableId::generate();
    let document_id = DocumentId::generate();
    let first_script = directory.path().join("sdk-first.mjs");
    std::fs::write(
        &first_script,
        format!(
            r#"
import {{ RunkuClient }} from {sdk:?};
const client = new RunkuClient({{ baseUrl: process.env.RUNKU_URL, target: "workspace:default", applicationKey: process.env.RUNKU_KEY }});
const args = {{ tableId: {table:?}, documentId: {document:?} }};
const realtime = client.realtime();
let resolveUpdate;
const updated = new Promise((resolve) => {{ resolveUpdate = resolve; }});
const subscription = realtime.subscribe("queries.document", args, {{
  onValue: (state) => {{ if (state.value === 77n) resolveUpdate(state); }},
  onError: (error) => {{ throw error; }},
}});
const initial = await subscription.ready;
if (initial.value !== null) throw new Error("expected empty initial state");
const mutation = await client.mutation("mutations.insert", {{ ...args, value: 77n }});
if (mutation.value !== 77n) throw new Error("unexpected mutation value");
const delivered = await Promise.race([
  updated,
  new Promise((_, reject) => setTimeout(() => reject(new Error("realtime timeout")), 5000)),
]);
if (delivered.value !== 77n || delivered.deliveryRevision < 2n) throw new Error("invalid delivery");
await subscription.unsubscribe();
realtime.close();
const query = await client.query("queries.document", args);
if (query.value !== 77n) throw new Error("query did not observe mutation");
"#,
            sdk = sdk.to_string_lossy(),
            table = table_id.to_string(),
            document = document_id.to_string(),
        ),
    )?;
    let (mut dev, address) = start_dev(root)?;
    let first_node = Command::new("node")
        .arg("--experimental-websocket")
        .arg(&first_script)
        .env("RUNKU_URL", format!("http://{address}"))
        .env("RUNKU_KEY", &application_key)
        .output()?;
    if !first_node.status.success() {
        return Err(format!(
            "SDK first process failed: {}",
            String::from_utf8_lossy(&first_node.stderr)
        )
        .into());
    }
    stop_dev(&mut dev)?;

    let second_script = directory.path().join("sdk-second.mjs");
    std::fs::write(
        &second_script,
        format!(
            r#"
import {{ RunkuClient }} from {sdk:?};
const client = new RunkuClient({{ baseUrl: process.env.RUNKU_URL, target: "workspace:default", applicationKey: process.env.RUNKU_KEY }});
const result = await client.query("queries.document", {{ tableId: {table:?}, documentId: {document:?} }});
if (result.value !== 77n) throw new Error("data was not durable across restart");
"#,
            sdk = sdk.to_string_lossy(),
            table = table_id.to_string(),
            document = document_id.to_string(),
        ),
    )?;
    let (mut restarted, restarted_address) = start_dev(root)?;
    let second_node = Command::new("node")
        .arg(&second_script)
        .env("RUNKU_URL", format!("http://{restarted_address}"))
        .env("RUNKU_KEY", &application_key)
        .output()?;
    if !second_node.status.success() {
        return Err(format!(
            "SDK restart process failed: {}",
            String::from_utf8_lossy(&second_node.stderr)
        )
        .into());
    }
    stop_dev(&mut restarted)?;
    assert_eq!(load_local(directory.path()).await?.0, state);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn source_build_publish_and_scheduled_functions_run_end_to_end() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    assert!(
        run(&[
            "init",
            "--root",
            root,
            "--workspace",
            "default",
            "--listen",
            "127.0.0.1:0",
        ])?
        .status
        .success()
    );
    let application_key = create_test_application_key(root)?;
    let functions = directory.path().join("runku");
    std::fs::create_dir(&functions)?;
    std::fs::write(
        functions.join("schema.ts"),
        "import { defineSchema, defineTable, v } from '@runku/server';\nexport default defineSchema({ records: defineTable(v.string()) });\n",
    )?;
    std::fs::write(
        functions.join("queries.ts"),
        r#"
import { query, v } from "@runku/server";
import schema from "./schema";
export const document = query({
  auth: "none", visibility: "public", capabilities: ["db:read"],
  args: v.object({ documentId: v.id("doc") }), returns: v.union(v.null(), v.string()),
  async handler(ctx, input) {
    const document = await ctx.db.get(schema.tables.records, input.documentId.toString());
    return document === null ? null : document.value;
  },
});
"#,
    )?;
    std::fs::write(
        functions.join("mutations.ts"),
        r#"
import { mutation, v } from "@runku/server";
const input = v.object({ documentId: v.id("doc"), value: v.string() });
export const schedule = mutation({
  auth: "none", visibility: "public", capabilities: ["scheduler:create"],
  args: input, returns: v.string(),
  handler: (ctx, value) => ctx.scheduler.runAfter(0n, "internal.mark", value, { idempotencyKey: "mutation-mark" }),
});
"#,
    )?;
    std::fs::write(
        functions.join("actions.ts"),
        r#"
import { action, v } from "@runku/server";
const input = v.object({ documentId: v.id("doc"), value: v.string() });
export const schedule = action({
  auth: "none", visibility: "public", capabilities: ["scheduler:create"],
  args: input, returns: v.string(),
  handler: (ctx, value) => ctx.scheduler.runAfter(0n, "internal.mark", value, { idempotencyKey: "action-mark" }),
});
"#,
    )?;
    std::fs::write(
        functions.join("internal.ts"),
        r#"
import { mutation, v } from "@runku/server";
import schema from "./schema";
export const mark = mutation({
  auth: "none", visibility: "internal", capabilities: ["db:write"],
  args: v.object({ documentId: v.id("doc"), value: v.string() }), returns: v.string(),
  async handler(ctx, input) {
    await ctx.db.insert(schema.tables.records, input.documentId.toString(), input.value);
    return input.value;
  },
});
"#,
    )?;
    let build_args = [
        "build",
        "--root",
        root,
        "--release-id",
        "rel_00000000000000000000000100",
        "--build-id",
        "bld_00000000000000000000000101",
        "--created-at-micros",
        "1800000000000000",
    ];
    let built = run(&build_args)?;
    if !built.status.success() {
        return Err(format!(
            "source build failed: {}",
            String::from_utf8_lossy(&built.stderr)
        )
        .into());
    }
    let built_json: serde_json::Value = serde_json::from_slice(&built.stdout)?;
    assert_eq!(built_json["replayed"], false);
    let replay = run(&build_args)?;
    assert!(replay.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&replay.stdout)?["replayed"],
        true
    );
    let manifest = built_json["manifestPath"]
        .as_str()
        .ok_or("build manifest path missing")?;
    let artifact = built_json["artifactPath"]
        .as_str()
        .ok_or("build artifact path missing")?;
    let published = run(&[
        "publish",
        "--root",
        root,
        "--manifest",
        manifest,
        "--artifact",
        artifact,
    ])?;
    assert!(published.status.success());

    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/client/dist/index.js")
        .canonicalize()?;
    let mutation_document = DocumentId::generate();
    let action_document = DocumentId::generate();
    let script = directory.path().join("source-build-e2e.mjs");
    std::fs::write(
        &script,
        format!(
            r#"
import {{ RunkuClient, RunkuId }} from {sdk:?};
const client = new RunkuClient({{ baseUrl: process.env.RUNKU_URL, target: "workspace:default", applicationKey: process.env.RUNKU_KEY }});
const mutationDocument = new RunkuId({mutation_document:?});
const actionDocument = new RunkuId({action_document:?});
const mutation = await client.mutation("mutations.schedule", {{ documentId: mutationDocument, value: "mutation" }});
if (typeof mutation.value !== "string" || !mutation.value.startsWith("sch_")) throw new Error("mutation did not schedule");
const action = await client.action("actions.schedule", {{ documentId: actionDocument, value: "action" }});
if (typeof action.value !== "string" || !action.value.startsWith("sch_")) throw new Error("action did not schedule");
const deadline = Date.now() + 5000;
for (;;) {{
  const first = await client.query("queries.document", {{ documentId: mutationDocument }});
  const second = await client.query("queries.document", {{ documentId: actionDocument }});
  if (first.value === "mutation" && second.value === "action") break;
  if (Date.now() >= deadline) throw new Error("scheduled functions did not commit");
  await new Promise((resolve) => setTimeout(resolve, 25));
}}
"#,
            sdk = sdk.to_string_lossy(),
            mutation_document = mutation_document.to_string(),
            action_document = action_document.to_string(),
        ),
    )?;
    let (mut dev, address) = start_dev(root)?;
    let node = Command::new("node")
        .arg(&script)
        .env("RUNKU_URL", format!("http://{address}"))
        .env("RUNKU_KEY", &application_key)
        .output()?;
    if !node.status.success() {
        return Err(format!(
            "source build SDK process failed: {}",
            String::from_utf8_lossy(&node.stderr)
        )
        .into());
    }
    stop_dev(&mut dev)?;
    assert!(run(&["doctor", "--root", root])?.status.success());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn full_node_source_build_publish_and_dev_use_the_machine_node() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    assert!(
        run(&[
            "init",
            "--root",
            root,
            "--workspace",
            "default",
            "--listen",
            "127.0.0.1:0",
        ])?
        .status
        .success()
    );
    let application_key = create_test_application_key(root)?;
    let functions = directory.path().join("runku");
    std::fs::create_dir(&functions)?;
    std::fs::write(
        functions.join("schema.ts"),
        "import { defineSchema } from '@runku/server';\nexport default defineSchema({});\n",
    )?;
    std::fs::write(
        functions.join("actions.ts"),
        r#"
"use runku node"
import { action, v } from "@runku/server"
import path from "node:path"
export const basename = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.string(), returns: v.string(),
  handler(_ctx, input) { return path.basename(input) },
})
"#,
    )?;
    let built = run(&["build", "--root", root])?;
    if !built.status.success() {
        return Err(format!(
            "Full Node source build failed: {}",
            String::from_utf8_lossy(&built.stderr)
        )
        .into());
    }
    let built: serde_json::Value = serde_json::from_slice(&built.stdout)?;
    let published = run(&[
        "publish",
        "--root",
        root,
        "--manifest",
        built["manifestPath"]
            .as_str()
            .ok_or("Full Node manifest path missing")?,
        "--artifact",
        built["artifactPath"]
            .as_str()
            .ok_or("Full Node artifact path missing")?,
    ])?;
    if !published.status.success() {
        return Err(format!(
            "Full Node local publish failed: {}",
            String::from_utf8_lossy(&published.stderr)
        )
        .into());
    }

    let script = r#"
const response = await fetch(`${process.env.RUNKU_URL}/v1/action`, {
  method: "POST",
  headers: { "content-type": "application/json", "x-runku-key": process.env.RUNKU_KEY },
  body: JSON.stringify({
    version: 1,
    target: "workspace:default",
    function: "actions.basename",
    arguments: { type: "string", value: "/tmp/runku/result.txt" },
  }),
});
const result = await response.json();
if (!response.ok) throw new Error(JSON.stringify(result));
if (result.result?.type !== "string" || result.result.value !== "result.txt") {
  throw new Error(`unexpected local Node result: ${JSON.stringify(result)}`);
}
"#;
    let (mut dev, address) = start_dev(root)?;
    let node = Command::new("node")
        .args(["--input-type=module", "--eval", script])
        .env("RUNKU_URL", format!("http://{address}"))
        .env("RUNKU_KEY", &application_key)
        .output()?;
    if !node.status.success() {
        return Err(format!(
            "Full Node local SDK invocation failed: {}",
            String::from_utf8_lossy(&node.stderr)
        )
        .into());
    }
    stop_dev(&mut dev)?;
    let runtime_directory = directory.path().join(".runku/node-runtime-v1");
    assert!(
        !runtime_directory.exists() || std::fs::read_dir(runtime_directory)?.next().is_none(),
        "local invocation source was not cleaned up"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn structured_contracts_enforce_public_calls_results_and_documents_e2e()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    assert!(
        run(&[
            "init",
            "--root",
            root,
            "--workspace",
            "default",
            "--listen",
            "127.0.0.1:0",
        ])?
        .status
        .success()
    );
    let application_key = create_test_application_key(root)?;
    let functions = directory.path().join("runku");
    std::fs::create_dir(&functions)?;
    std::fs::write(
        functions.join("schema.ts"),
        "import { defineSchema, defineTable, v } from '@runku/server';\nexport default defineSchema({ messages: defineTable(v.string({ minBytes: 1, maxBytes: 64 })) });\n",
    )?;
    std::fs::write(
        functions.join("queries.ts"),
        r#"
import { query, v } from "@runku/server";
export const echo = query({
  auth: "none", visibility: "public", capabilities: [],
  args: v.object({ value: v.string({ minBytes: 1, maxBytes: 32 }) }),
  returns: v.string({ minBytes: 1, maxBytes: 32 }), handler: (_ctx, input) => input.value,
});
export const badResult = query({
  auth: "none", visibility: "public", capabilities: [], args: v.null(), returns: v.string(),
  handler: () => 1n as unknown as string,
});
"#,
    )?;
    std::fs::write(
        functions.join("mutations.ts"),
        r#"
import { mutation, v } from "@runku/server";
import schema from "./schema";
export const insert = mutation({
  auth: "none", visibility: "public", capabilities: ["db:write"],
  args: v.object({ documentId: v.id("doc"), value: v.any() }), returns: v.any(),
  async handler(ctx, input) {
    await ctx.db.insert(schema.tables.messages, input.documentId.toString(), input.value);
    return input.value;
  },
});
"#,
    )?;
    let built = run(&["build", "--root", root])?;
    if !built.status.success() {
        return Err(format!(
            "structured build failed: {}",
            String::from_utf8_lossy(&built.stderr)
        )
        .into());
    }
    let built: serde_json::Value = serde_json::from_slice(&built.stdout)?;
    let generated = built["generatedTypesPath"]
        .as_str()
        .ok_or("generated types path missing")?;
    let generated_text = std::fs::read_to_string(generated)?;
    let stable_generated = built["stableGeneratedTypesPath"]
        .as_str()
        .ok_or("stable generated types path missing")?;
    assert_eq!(generated_text, std::fs::read_to_string(stable_generated)?);
    assert!(generated_text.contains("readonly \"queries.echo\""));
    assert!(generated_text.contains("readonly \"messages\""));
    assert!(
        run(&[
            "publish",
            "--root",
            root,
            "--manifest",
            built["manifestPath"]
                .as_str()
                .ok_or("manifest path missing")?,
            "--artifact",
            built["artifactPath"]
                .as_str()
                .ok_or("artifact path missing")?,
        ])?
        .status
        .success()
    );

    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/client/dist/index.js")
        .canonicalize()?;
    let document_id = DocumentId::generate();
    let script = directory.path().join("contracts-e2e.mjs");
    std::fs::write(
        &script,
        format!(
            r#"
import {{ RunkuClient, RunkuId }} from {sdk:?};
const client = new RunkuClient({{ baseUrl: process.env.RUNKU_URL, target: "workspace:default", applicationKey: process.env.RUNKU_KEY, maxAttempts: 1 }});
const valid = await client.query("queries.echo", {{ value: "hello" }});
if (valid.value !== "hello") throw new Error("valid contract result mismatch");
async function expectCode(code, call) {{
  try {{ await call(); }} catch (error) {{ if (error.code === code) return; throw error; }}
  throw new Error(`expected ${{code}}`);
}}
await expectCode("RUNTIME_ARGUMENTS_INVALID", () => client.query("queries.echo", {{}}));
await expectCode("RUNTIME_RESULT_INVALID", () => client.query("queries.badResult", null));
await expectCode("RUNTIME_JAVASCRIPT_ERROR", () => client.mutation("mutations.insert", {{
  documentId: new RunkuId({document:?}), value: 7n
}}));
"#,
            sdk = sdk.to_string_lossy(),
            document = document_id.to_string(),
        ),
    )?;
    let (mut dev, address) = start_dev(root)?;
    let node = Command::new("node")
        .arg(&script)
        .env("RUNKU_URL", format!("http://{address}"))
        .env("RUNKU_KEY", &application_key)
        .output()?;
    if !node.status.success() {
        return Err(format!(
            "structured contract SDK process failed: {}",
            String::from_utf8_lossy(&node.stderr)
        )
        .into());
    }
    stop_dev(&mut dev)?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn application_keys_cli_rotate_and_revoke_one_live_key_only() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    assert!(
        run(&[
            "init",
            "--root",
            root,
            "--workspace",
            "default",
            "--listen",
            "127.0.0.1:0",
        ])?
        .status
        .success()
    );
    let functions = directory.path().join("runku");
    std::fs::create_dir(&functions)?;
    std::fs::write(
        functions.join("queries.ts"),
        "import { query, v } from '@runku/server';\nexport const echo = query({ auth: 'none', visibility: 'public', capabilities: [], args: v.any(), returns: v.any(), handler(_ctx, input) { return input; } });\n",
    )?;
    std::fs::write(
        functions.join("schema.ts"),
        "import { defineSchema } from '@runku/server';\nexport default defineSchema({});\n",
    )?;
    let built = run(&["build", "--root", root])?;
    if !built.status.success() {
        return Err(format!(
            "identity E2E build failed: {}",
            String::from_utf8_lossy(&built.stderr)
        )
        .into());
    }
    let built: serde_json::Value = serde_json::from_slice(&built.stdout)?;
    let published = run(&[
        "publish",
        "--root",
        root,
        "--manifest",
        built["manifestPath"]
            .as_str()
            .ok_or("manifest path missing")?,
        "--artifact",
        built["artifactPath"]
            .as_str()
            .ok_or("artifact path missing")?,
    ])?;
    assert!(published.status.success());

    let public = run(&[
        "client",
        "create",
        "--root",
        root,
        "--name",
        "web-storefront",
        "--kind",
        "public",
        "--scope",
        "functions:invoke",
    ])?;
    assert!(public.status.success());
    let public: serde_json::Value = serde_json::from_slice(&public.stdout)?;
    let public_id = public["clients"][0]["clientId"]
        .as_str()
        .ok_or("public client id missing")?;
    let first = create_key(root, public_id, "web-blue")?;
    let first_key = first["key"].as_str().ok_or("first key missing")?;
    let first_id = first["credential"]["credentialId"]
        .as_str()
        .ok_or("first key id missing")?;
    assert!(first_key.starts_with("rk_pub_v1_"));
    assert_eq!(first["recoverable"], true);
    assert_eq!(first["secretShownOnce"], false);
    let second = create_key(root, public_id, "web-green")?;
    let second_key = second["key"].as_str().ok_or("second key missing")?;
    let rotated = run(&[
        "key",
        "rotate",
        "--root",
        root,
        "--client",
        public_id,
        "--key",
        first_id,
        "--label",
        "web-rotated",
    ])?;
    assert!(rotated.status.success());
    let rotated: serde_json::Value = serde_json::from_slice(&rotated.stdout)?;
    let rotated_key = rotated["key"].as_str().ok_or("rotated key missing")?;
    assert_ne!(rotated_key, first_key);
    assert_eq!(
        rotated["credential"]["scopes"],
        first["credential"]["scopes"]
    );
    let revealed = run(&[
        "key", "reveal", "--root", root, "--client", public_id, "--key", first_id,
    ])?;
    assert!(revealed.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&revealed.stdout)?["key"],
        first_key
    );
    let listed = run(&["key", "list", "--root", root, "--client", public_id])?;
    assert!(listed.status.success());
    let listed_text = String::from_utf8(listed.stdout)?;
    assert!(!listed_text.contains("rk_pub_v1_"));
    assert!(!listed_text.contains("digest"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&listed_text)?["credentials"]
            .as_array()
            .ok_or("credential list missing")?
            .len(),
        3
    );

    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/client/dist/index.js")
        .canonicalize()?;
    let (mut dev, address) = start_dev(root)?;
    for key in [first_key, second_key, rotated_key] {
        let output = sdk_query(&sdk, &address, key)?;
        if !output.status.success() {
            return Err(format!(
                "valid application key query failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
    }
    let revoked = run(&["key", "revoke", "--root", root, "--key", first_id])?;
    assert!(revoked.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&revoked.stdout)?["replayed"],
        false
    );
    let replay = run(&["key", "revoke", "--root", root, "--key", first_id])?;
    assert!(replay.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&replay.stdout)?["replayed"],
        true
    );
    let denied = sdk_query(&sdk, &address, first_key)?;
    assert!(!denied.status.success());
    assert!(!String::from_utf8(denied.stderr)?.contains(first_key));
    assert!(sdk_query(&sdk, &address, second_key)?.status.success());
    assert!(sdk_query(&sdk, &address, rotated_key)?.status.success());
    stop_dev(&mut dev)?;

    let deleted = run(&["key", "delete", "--root", root, "--key", first_id])?;
    assert!(deleted.status.success());
    let listed = run(&["key", "list", "--root", root, "--client", public_id])?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&listed.stdout)?["credentials"]
            .as_array()
            .ok_or("credential list missing after delete")?
            .len(),
        2
    );

    let confidential = run(&[
        "client",
        "create",
        "--root",
        root,
        "--name",
        "billing-worker",
        "--kind",
        "confidential",
        "--scope",
        "functions:invoke",
    ])?;
    assert!(confidential.status.success());
    let confidential: serde_json::Value = serde_json::from_slice(&confidential.stdout)?;
    let confidential_id = confidential["clients"][0]["clientId"]
        .as_str()
        .ok_or("confidential client id missing")?;
    let secret = create_key(root, confidential_id, "billing-primary")?;
    let secret_material = secret["key"].as_str().ok_or("secret missing")?;
    let secret_id = secret["credential"]["credentialId"]
        .as_str()
        .ok_or("secret id missing")?;
    assert!(secret_material.starts_with("rk_sec_v1_"));
    assert_eq!(secret["recoverable"], false);
    assert_eq!(secret["secretShownOnce"], true);
    let reveal_secret = run(&[
        "key",
        "reveal",
        "--root",
        root,
        "--client",
        confidential_id,
        "--key",
        secret_id,
    ])?;
    assert_eq!(reveal_secret.status.code(), Some(3));
    assert!(reveal_secret.stdout.is_empty());
    assert!(!String::from_utf8(reveal_secret.stderr)?.contains(secret_material));
    let secret_list = run(&["key", "list", "--root", root, "--client", confidential_id])?;
    let secret_list = String::from_utf8(secret_list.stdout)?;
    assert!(!secret_list.contains(secret_material));
    assert!(!secret_list.contains("digest"));
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn development_access_cli_rotates_revokes_and_never_lists_secrets()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    assert!(
        run(&[
            "init",
            "--root",
            root,
            "--workspace",
            "default",
            "--listen",
            "127.0.0.1:0",
        ])?
        .status
        .success()
    );
    let first = run(&[
        "workspace",
        "key",
        "create",
        "--root",
        root,
        "--actor",
        "manuel",
        "--label",
        "laptop",
    ])?;
    assert!(first.status.success());
    let first_text = String::from_utf8(first.stdout)?;
    let first: serde_json::Value = serde_json::from_str(&first_text)?;
    let first_key = first["key"]
        .as_str()
        .ok_or("Development Access key missing")?;
    let first_id = first["credential"]["credentialId"]
        .as_str()
        .ok_or("Development Access credential ID missing")?;
    assert!(first_key.starts_with("rk_dev_v1_"));
    assert_eq!(first["recoverable"], false);
    assert_eq!(first["secretShownOnce"], true);
    assert_eq!(first["credential"]["actor"], "manuel");

    let second = run(&[
        "workspace",
        "key",
        "create",
        "--root",
        root,
        "--actor",
        "ci.release",
        "--label",
        "ci-release",
    ])?;
    assert!(second.status.success());
    let second: serde_json::Value = serde_json::from_slice(&second.stdout)?;
    let second_key = second["key"].as_str().ok_or("second key missing")?;

    let rotated = run(&[
        "workspace",
        "key",
        "rotate",
        "--root",
        root,
        "--key",
        first_id,
        "--label",
        "laptop-rotated",
    ])?;
    assert!(rotated.status.success());
    let rotated: serde_json::Value = serde_json::from_slice(&rotated.stdout)?;
    let rotated_key = rotated["key"].as_str().ok_or("rotated key missing")?;
    assert_ne!(rotated_key, first_key);
    assert_eq!(rotated["credential"]["actor"], "manuel");

    let listed = run(&["workspace", "key", "list", "--root", root])?;
    assert!(listed.status.success());
    let listed_text = String::from_utf8(listed.stdout)?;
    for secret in [first_key, second_key, rotated_key] {
        assert!(!listed_text.contains(secret));
    }
    assert!(!listed_text.contains("digest"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&listed_text)?["credentials"]
            .as_array()
            .ok_or("Development Access list missing")?
            .len(),
        3
    );

    let revoked = run(&[
        "workspace",
        "key",
        "revoke",
        "--root",
        root,
        "--key",
        first_id,
    ])?;
    assert!(revoked.status.success());
    assert!(!String::from_utf8_lossy(&revoked.stdout).contains(first_key));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&revoked.stdout)?["replayed"],
        false
    );
    let replay = run(&[
        "workspace",
        "key",
        "revoke",
        "--root",
        root,
        "--key",
        first_id,
    ])?;
    assert!(replay.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&replay.stdout)?["replayed"],
        true
    );
    let deleted = run(&[
        "workspace",
        "key",
        "delete",
        "--root",
        root,
        "--key",
        first_id,
    ])?;
    assert!(deleted.status.success());
    let listed = run(&["workspace", "key", "list", "--root", root])?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&listed.stdout)?["credentials"]
            .as_array()
            .ok_or("post-delete list missing")?
            .len(),
        2
    );

    let invalid = run(&[
        "workspace",
        "key",
        "create",
        "--root",
        root,
        "--actor",
        "UPPER",
        "--label",
        "bad",
    ])?;
    assert_eq!(invalid.status.code(), Some(2));
    let invalid_stderr = String::from_utf8(invalid.stderr)?;
    for secret in [first_key, second_key, rotated_key] {
        assert!(!invalid_stderr.contains(secret));
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn operational_logs_snapshot_follow_attribution_redaction_and_retention_e2e()
-> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    assert!(
        run(&[
            "init",
            "--root",
            root,
            "--workspace",
            "default",
            "--listen",
            "127.0.0.1:0",
        ])?
        .status
        .success()
    );
    let functions = directory.path().join("runku");
    std::fs::create_dir(&functions)?;
    std::fs::write(
        functions.join("actions.ts"),
        r#"
import { action, v } from "@runku/server";
export const record = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.object({ orderId: v.string() }), returns: v.string(),
  async handler(ctx, input) {
    await ctx.log.info("order accepted", {
      orderId: input.orderId,
      accessToken: "function-secret-token",
      nested: { password: "function-secret-password" },
    });
    return input.orderId;
  },
});
"#,
    )?;
    std::fs::write(
        functions.join("schema.ts"),
        "import { defineSchema } from '@runku/server';\nexport default defineSchema({});\n",
    )?;
    let built = run(&["build", "--root", root])?;
    if !built.status.success() {
        return Err(format!(
            "operational logs build failed: {}",
            String::from_utf8_lossy(&built.stderr)
        )
        .into());
    }
    let built: serde_json::Value = serde_json::from_slice(&built.stdout)?;
    assert!(
        run(&[
            "publish",
            "--root",
            root,
            "--manifest",
            built["manifestPath"]
                .as_str()
                .ok_or("log manifest path missing")?,
            "--artifact",
            built["artifactPath"]
                .as_str()
                .ok_or("log artifact path missing")?,
        ])?
        .status
        .success()
    );
    let release_id = built["releaseId"]
        .as_str()
        .ok_or("log release id missing")?;
    let client = run(&[
        "client",
        "create",
        "--root",
        root,
        "--name",
        "logs-e2e",
        "--kind",
        "public",
        "--scope",
        "functions:invoke",
    ])?;
    assert!(client.status.success());
    let client: serde_json::Value = serde_json::from_slice(&client.stdout)?;
    let client_id = client["clients"][0]["clientId"]
        .as_str()
        .ok_or("log client id missing")?;
    let credential = create_key(root, client_id, "logs-key")?;
    let credential_id = credential["credential"]["credentialId"]
        .as_str()
        .ok_or("log credential id missing")?;
    let key = credential["key"].as_str().ok_or("log key missing")?;
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/client/dist/index.js")
        .canonicalize()?;

    let (mut dev, address) = start_dev(root)?;
    invoke_log_action(&sdk, &address, key, "ord_snapshot")?;
    stop_dev(&mut dev)?;

    let snapshot = run(&["logs", "--root", root, "--limit", "100"])?;
    assert!(snapshot.status.success());
    let snapshot_text = String::from_utf8(snapshot.stdout)?;
    assert!(!snapshot_text.contains("function-secret-token"));
    assert!(!snapshot_text.contains("function-secret-password"));
    assert!(snapshot_text.contains("[REDACTED]"));
    let records = parse_json_lines(&snapshot_text)?;
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["eventKind"], "invocation_started");
    assert_eq!(records[1]["eventKind"], "function_message");
    assert_eq!(records[2]["eventKind"], "invocation_completed");
    assert_eq!(records[1]["message"], "order accepted");
    assert_eq!(records[0]["requestId"], records[1]["requestId"]);
    assert_eq!(records[1]["invocationId"], records[2]["invocationId"]);
    assert!(records.iter().all(|record| record["clientId"] == client_id));
    assert!(
        records
            .iter()
            .all(|record| record["credentialId"] == credential_id)
    );
    assert!(
        records
            .iter()
            .all(|record| record["releaseId"] == release_id)
    );
    let function_only = run(&[
        "logs",
        "--root",
        root,
        "--stream",
        "function",
        "--client",
        client_id,
        "--credential",
        credential_id,
        "--release",
        release_id,
    ])?;
    assert!(function_only.status.success());
    assert_eq!(
        parse_json_lines(&String::from_utf8(function_only.stdout)?)?.len(),
        1
    );

    let (collector, collector_requests, collector_task) = start_otlp_collector()?;
    let collector_secret = "otel-e2e-secret-never-output";
    let otlp_config = directory.path().join("otel.json");
    std::fs::write(
        &otlp_config,
        format!(
            r#"{{"version":1,"name":"e2e","endpoint":"http://{collector}/v1/logs","headers":{{"authorization":"Bearer {collector_secret}"}},"maximumBatchRecords":100,"maximumRequestBytes":1048576,"requestTimeoutMillis":2000,"maximumResponseBytes":1024,"pollIntervalMillis":50,"maximumAttempts":3,"retryInitialMillis":10,"retryMaximumMillis":20}}"#
        ),
    )?;
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&otlp_config, std::fs::Permissions::from_mode(0o600))?;
    }
    let exported = run(&[
        "logs",
        "export-otlp",
        "--root",
        root,
        "--config",
        "otel.json",
        "--once",
    ])?;
    assert!(
        exported.status.success(),
        "OTLP export failed: {}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let exported_stdout = String::from_utf8(exported.stdout)?;
    let exported_stderr = String::from_utf8(exported.stderr)?;
    assert!(!exported_stdout.contains(collector_secret));
    assert!(!exported_stderr.contains(collector_secret));
    let statuses = parse_json_lines(&exported_stdout)?;
    assert_eq!(statuses.len(), 2);
    assert_eq!(statuses[0]["status"], "running");
    assert_eq!(statuses[1]["status"], "complete");
    assert_eq!(statuses[1]["telemetry"]["exportedRecords"], 3);
    assert_eq!(statuses[1]["telemetry"]["requests"], 1);
    let request = collector_requests.recv_timeout(std::time::Duration::from_secs(5))?;
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("collector request headers missing")?;
    let headers = String::from_utf8_lossy(&request[..header_end]);
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: application/x-protobuf")
    );
    assert!(headers.contains(collector_secret));
    assert!(!request[header_end + 4..].is_empty());
    collector_task
        .join()
        .map_err(|_| "OTLP collector panicked")?
        .map_err(|error| -> Box<dyn Error> { error.into() })?;

    let replay = run(&[
        "logs",
        "export-otlp",
        "--root",
        root,
        "--config",
        "otel.json",
        "--once",
    ])?;
    assert!(replay.status.success());
    let replay = parse_json_lines(&String::from_utf8(replay.stdout)?)?;
    assert_eq!(replay[1]["telemetry"]["exportedRecords"], 0);
    assert_eq!(replay[1]["telemetry"]["requests"], 0);

    let state = load_local(directory.path()).await?.0;
    let before = i64::MAX.to_string();
    let dry = run(&[
        "logs",
        "prune",
        "--root",
        root,
        "--before-micros",
        &before,
        "--maximum",
        "100",
    ])?;
    assert!(dry.status.success());
    let dry: serde_json::Value = serde_json::from_slice(&dry.stdout)?;
    assert_eq!(dry["applied"], false);
    assert_eq!(dry["matched"], 3);
    let applied = run(&[
        "logs",
        "prune",
        "--root",
        root,
        "--before-micros",
        &before,
        "--maximum",
        "100",
        "--apply",
        "--environment",
        &state.environment_id.to_string(),
    ])?;
    assert!(applied.status.success());
    let applied: serde_json::Value = serde_json::from_slice(&applied.stdout)?;
    assert_eq!(applied["deleted"], 3);

    let mut follower = Command::new(env!("CARGO_BIN_EXE_runku"))
        .args(["logs", "--root", root, "--follow"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let follower_stdout = follower
        .stdout
        .take()
        .ok_or("logs follow stdout unavailable")?;
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(follower_stdout).lines() {
            if sender.send(line).is_err() {
                return;
            }
        }
    });
    let (mut dev, address) = start_dev(root)?;
    invoke_log_action(&sdk, &address, key, "ord_follow")?;
    let mut live_records = Vec::new();
    while live_records.len() < 3 {
        let line = receiver.recv_timeout(std::time::Duration::from_secs(10))??;
        live_records.push(serde_json::from_str::<serde_json::Value>(&line)?);
    }
    assert_eq!(live_records[1]["eventKind"], "function_message");
    assert_eq!(live_records[1]["message"], "order accepted");
    stop_dev(&mut dev)?;
    stop_dev(&mut follower)?;
    drop(receiver);
    reader.join().map_err(|_| "logs follow reader panicked")?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn source_watch_hot_reloads_keeps_last_good_and_recovers() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().to_str().ok_or("non-Unicode test path")?;
    let functions = directory.path().join("runku");
    std::fs::create_dir(&functions)?;
    let source = functions.join("queries.ts");
    write_version_source(&source, "v1")?;
    std::fs::write(
        functions.join("schema.ts"),
        "import { defineSchema } from '@runku/server';\nexport default defineSchema({});\n",
    )?;

    let mut child = Command::new(env!("CARGO_BIN_EXE_runku"))
        .args(["dev", "--root", root])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().ok_or("watch stdout unavailable")?;
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if sender.send(line).is_err() {
                return;
            }
        }
    });
    let ready = match receiver.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(line) => line?,
        Err(error) => {
            let _status = child.wait()?;
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .ok_or("watch stderr unavailable")?
                .read_to_string(&mut stderr)?;
            return Err(format!("watch exited before readiness ({error}): {stderr}").into());
        }
    };
    let ready: serde_json::Value = serde_json::from_str(&ready)?;
    assert_eq!(ready["status"], "ready");
    assert_eq!(ready["watching"], true);
    assert_eq!(ready["eventVersion"], 1);
    assert_eq!(ready["workspace"], "local");
    assert_eq!(ready["address"], "127.0.0.1:3210");
    assert!(
        directory
            .path()
            .join(".runku/local-state-v1.json")
            .is_file()
    );
    let address = ready["address"].as_str().ok_or("watch address missing")?;
    let initial_revision = ready["revisionId"]
        .as_str()
        .ok_or("initial revision missing")?
        .to_owned();
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/client/dist/index.js")
        .canonicalize()?;
    let application_key = create_test_application_key(root)?;
    await_version(&sdk, address, &application_key, "v1")
        .map_err(|error| format!("initial v1: {error}"))?;
    assert!(
        receiver
            .recv_timeout(std::time::Duration::from_millis(650))
            .is_err(),
        "unchanged valid input triggered a rebuild"
    );

    write_version_source(&source, "v2")?;
    let synced =
        receive_watch_status(&receiver, "synced").map_err(|error| format!("v2 sync: {error}"))?;
    assert_ne!(synced["revisionId"], initial_revision);
    await_version(&sdk, address, &application_key, "v2")
        .map_err(|error| format!("serve v2: {error}"))?;

    std::fs::write(&source, "export default async function broken( {")?;
    let error = receive_watch_status(&receiver, "build-error")
        .map_err(|error| format!("invalid source event: {error}"))?;
    assert_eq!(error["code"], "BUILD_SOURCE_SYNTAX_INVALID");
    await_version(&sdk, address, &application_key, "v2")
        .map_err(|error| format!("last-known-good v2: {error}"))?;
    assert!(
        receiver
            .recv_timeout(std::time::Duration::from_millis(800))
            .is_err(),
        "unchanged invalid input emitted duplicate events"
    );

    write_version_source(&source, "v3")?;
    let recovered = receive_watch_status(&receiver, "synced")
        .map_err(|error| format!("v3 recovery: {error}"))?;
    assert_ne!(recovered["revisionId"], synced["revisionId"]);
    await_version(&sdk, address, &application_key, "v3")
        .map_err(|error| format!("serve v3: {error}"))?;
    stop_dev(&mut child)?;
    drop(receiver);
    reader.join().map_err(|_| "watch reader panicked")?;
    Ok(())
}

#[cfg(unix)]
fn write_version_source(path: &std::path::Path, version: &str) -> Result<(), Box<dyn Error>> {
    std::fs::write(
        path,
        format!(
            "import {{ query, v }} from '@runku/server';\nexport const version = query({{ auth: 'none', visibility: 'public', capabilities: [], args: v.any(), returns: v.string(), handler() {{ return {version:?}; }} }});\n"
        ),
    )?;
    Ok(())
}

#[cfg(unix)]
fn receive_watch_status(
    receiver: &std::sync::mpsc::Receiver<Result<String, std::io::Error>>,
    expected: &str,
) -> Result<serde_json::Value, Box<dyn Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let line = receiver.recv_timeout(remaining)??;
        let event: serde_json::Value = serde_json::from_str(&line)?;
        if event["status"] == expected {
            return Ok(event);
        }
    }
    Err(format!("watch event {expected} was not observed").into())
}

#[cfg(unix)]
fn await_version(
    sdk: &std::path::Path,
    address: &str,
    application_key: &str,
    expected: &str,
) -> Result<(), Box<dyn Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut last_failure = String::new();
    while std::time::Instant::now() < deadline {
        let script = format!(
            r#"
import {{ RunkuClient }} from {sdk:?};
const client = new RunkuClient({{ baseUrl: process.env.RUNKU_URL, target: "workspace:local", applicationKey: process.env.RUNKU_KEY }});
const result = await client.query("queries.version", null);
if (result.value !== process.env.EXPECTED) throw new Error(`expected ${{process.env.EXPECTED}}, got ${{result.value}}`);
"#,
            sdk = sdk.to_string_lossy(),
        );
        let output = Command::new("node")
            .args(["--input-type=module", "--eval", &script])
            .env("RUNKU_URL", format!("http://{address}"))
            .env("RUNKU_KEY", application_key)
            .env("EXPECTED", expected)
            .output()?;
        if output.status.success() {
            return Ok(());
        }
        last_failure = format!(
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    Err(format!("version {expected} was not served before deadline: {last_failure}").into())
}

#[cfg(unix)]
fn create_key(
    root: &str,
    client_id: &str,
    label: &str,
) -> Result<serde_json::Value, Box<dyn Error>> {
    let output = run(&[
        "key",
        "create",
        "--root",
        root,
        "--client",
        client_id,
        "--label",
        label,
        "--scope",
        "functions:invoke",
    ])?;
    if !output.status.success() {
        return Err(format!(
            "key create failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[cfg(unix)]
fn create_test_application_key(root: &str) -> Result<String, Box<dyn Error>> {
    let client = run(&[
        "client",
        "create",
        "--root",
        root,
        "--name",
        "black-box-browser",
        "--kind",
        "public",
        "--scope",
        "functions:invoke",
    ])?;
    if !client.status.success() {
        return Err(format!(
            "test Application Client create failed: {}",
            String::from_utf8_lossy(&client.stderr)
        )
        .into());
    }
    let client: serde_json::Value = serde_json::from_slice(&client.stdout)?;
    let client_id = client["clients"][0]["clientId"]
        .as_str()
        .ok_or("created Application Client ID missing")?;
    let created = create_key(root, client_id, "black-box-publishable")?;
    created["key"]
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| "created Application Key missing".into())
}

#[cfg(unix)]
fn invoke_log_action(
    sdk: &std::path::Path,
    address: &str,
    key: &str,
    order_id: &str,
) -> Result<(), Box<dyn Error>> {
    let script = format!(
        r#"
import {{ RunkuClient }} from {sdk:?};
const client = new RunkuClient({{
  baseUrl: process.env.RUNKU_URL,
  target: "workspace:default",
  applicationKey: process.env.RUNKU_KEY,
}});
const result = await client.action("actions.record", {{ orderId: process.env.ORDER_ID }});
if (result.value !== process.env.ORDER_ID) throw new Error("unexpected action result");
"#,
        sdk = sdk.to_string_lossy(),
    );
    let output = Command::new("node")
        .args(["--input-type=module", "--eval", &script])
        .env("RUNKU_URL", format!("http://{address}"))
        .env("RUNKU_KEY", key)
        .env("ORDER_ID", order_id)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "log action failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

#[cfg(unix)]
fn parse_json_lines(input: &str) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
    input
        .lines()
        .map(|line| serde_json::from_str(line).map_err(Into::into))
        .collect()
}

#[cfg(unix)]
type CollectorTask = std::thread::JoinHandle<Result<(), String>>;

#[cfg(unix)]
type CollectorHarness = (String, std::sync::mpsc::Receiver<Vec<u8>>, CollectorTask);

#[cfg(unix)]
fn start_otlp_collector() -> Result<CollectorHarness, Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?.to_string();
    let (sender, receiver) = std::sync::mpsc::channel();
    let task = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .map_err(|error| error.to_string())?;
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let mut expected = None;
        loop {
            let read = stream
                .read(&mut buffer)
                .map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if expected.is_none()
                && let Some(header_end) =
                    request.windows(4).position(|window| window == b"\r\n\r\n")
            {
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .ok_or_else(|| "content-length missing".to_owned())?;
                expected = Some(header_end + 4 + length);
            }
            if expected.is_some_and(|length| request.len() >= length) {
                break;
            }
        }
        sender.send(request).map_err(|error| error.to_string())?;
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
            .map_err(|error| error.to_string())?;
        stream.flush().map_err(|error| error.to_string())
    });
    Ok((address, receiver, task))
}

#[cfg(unix)]
fn sdk_query(
    sdk: &std::path::Path,
    address: &str,
    key: &str,
) -> Result<std::process::Output, Box<dyn Error>> {
    let script = format!(
        r#"
import {{ RunkuClient }} from {sdk:?};
const client = new RunkuClient({{
  baseUrl: process.env.RUNKU_URL,
  target: "workspace:default",
  applicationKey: process.env.RUNKU_KEY,
}});
const result = await client.query("queries.echo", {{ value: 41n }});
if (result.value.value !== 41n) throw new Error("unexpected query result");
"#,
        sdk = sdk.to_string_lossy(),
    );
    Ok(Command::new("node")
        .args(["--input-type=module", "--eval", &script])
        .env("RUNKU_URL", format!("http://{address}"))
        .env("RUNKU_KEY", key)
        .output()?)
}

#[cfg(unix)]
fn start_dev(root: &str) -> Result<(std::process::Child, String), Box<dyn Error>> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_runku"))
        .args(["dev", "--root", root, "--prebuilt"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().ok_or("dev stdout unavailable")?;
    let mut lines = BufReader::new(stdout).lines();
    let ready = lines.next().ok_or("dev exited before readiness")??;
    let address = ready
        .split("\"address\":\"")
        .nth(1)
        .and_then(|value| value.split('"').next())
        .ok_or("dev readiness address missing")?
        .to_owned();
    Ok((child, address))
}

#[cfg(unix)]
fn stop_dev(child: &mut std::process::Child) -> Result<(), Box<dyn Error>> {
    let signal = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()?;
    if !signal.success() || !child.wait()?.success() {
        return Err("dev did not shut down gracefully".into());
    }
    Ok(())
}
