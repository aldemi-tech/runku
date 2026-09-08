//! Process-level configuration tests for the two PostgreSQL roles.

use std::{
    io::Write as _,
    process::{Command, Output},
};

use tempfile::{NamedTempFile, TempDir};

const PEPPER: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const IDENTITY_URL: &str = "postgres://identity:identity-secret@db.example/runku_identity";
const PLATFORM_URL: &str = "postgres://functions:function-secret@db.example/runku_functions";

fn check(environment: &[(&str, &str)]) -> Result<Output, Box<dyn std::error::Error>> {
    let state = TempDir::new()?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_runku-server"));
    command
        .arg("check")
        .env_clear()
        .env("RUNKU_PLATFORM_IDENTITY_PEPPER", PEPPER)
        .env("RUNKU_STATE_DIRECTORY", state.path());
    for (name, value) in environment {
        command.env(name, value);
    }
    Ok(command.output()?)
}

fn assert_error(output: &Output, expected: &str) {
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(stderr, format!("error: {expected}\n"));
    assert!(!stderr.contains("identity-secret"));
    assert!(!stderr.contains("function-secret"));
}

#[test]
fn canonical_database_names_pass_real_configuration_check() -> Result<(), Box<dyn std::error::Error>>
{
    let product = TempDir::new()?;
    let output = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PLATFORM_DATABASE_URL", PLATFORM_URL),
        (
            "RUNKU_PRODUCT_ROOT",
            product.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
    ])?;
    assert!(output.status.success());
    assert_eq!(output.stdout, b"configuration valid\n");
    assert!(output.stderr.is_empty());
    Ok(())
}

#[test]
fn legacy_database_names_remain_accepted() -> Result<(), Box<dyn std::error::Error>> {
    let product = TempDir::new()?;
    let output = check(&[
        ("RUNKU_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PRODUCT_DATABASE_URL", PLATFORM_URL),
        (
            "RUNKU_PRODUCT_ROOT",
            product.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
    ])?;
    assert!(output.status.success());
    assert_eq!(output.stdout, b"configuration valid\n");
    assert!(output.stderr.is_empty());
    Ok(())
}

#[test]
fn canonical_file_names_read_the_same_urls() -> Result<(), Box<dyn std::error::Error>> {
    let product = TempDir::new()?;
    let mut identity = NamedTempFile::new()?;
    let mut platform = NamedTempFile::new()?;
    writeln!(identity, "{IDENTITY_URL}")?;
    writeln!(platform, "{PLATFORM_URL}")?;
    let output = check(&[
        (
            "RUNKU_IDENTITY_DATABASE_URL_FILE",
            identity.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
        (
            "RUNKU_PLATFORM_DATABASE_URL_FILE",
            platform.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
        (
            "RUNKU_PRODUCT_ROOT",
            product.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
    ])?;
    assert!(output.status.success());
    assert_eq!(output.stdout, b"configuration valid\n");
    assert!(output.stderr.is_empty());
    Ok(())
}

#[test]
fn canonical_and_legacy_identity_names_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let output = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_DATABASE_URL", IDENTITY_URL),
    ])?;
    assert_error(&output, "SERVER_SECRET_CONFIGURATION_CONFLICT");
    Ok(())
}

#[test]
fn canonical_and_legacy_platform_names_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let product = TempDir::new()?;
    let output = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PLATFORM_DATABASE_URL", PLATFORM_URL),
        ("RUNKU_PRODUCT_DATABASE_URL", PLATFORM_URL),
        (
            "RUNKU_PRODUCT_ROOT",
            product.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
    ])?;
    assert_error(&output, "SERVER_SECRET_CONFIGURATION_CONFLICT");
    Ok(())
}

#[test]
fn database_urls_require_host_and_database_name() -> Result<(), Box<dyn std::error::Error>> {
    for invalid in [
        "postgres:///runku_identity",
        "postgres://db.example/",
        "postgres://db.example/runku/identity",
    ] {
        let output = check(&[("RUNKU_IDENTITY_DATABASE_URL", invalid)])?;
        assert_error(&output, "SERVER_DATABASE_URL_INVALID");
    }
    Ok(())
}

#[test]
fn identity_and_function_platform_must_target_different_databases()
-> Result<(), Box<dyn std::error::Error>> {
    let product = TempDir::new()?;
    let output = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        (
            "RUNKU_PLATFORM_DATABASE_URL",
            "postgresql://other:other-secret@DB.EXAMPLE:5432/runku_identity",
        ),
        (
            "RUNKU_PRODUCT_ROOT",
            product.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
    ])?;
    assert_error(&output, "SERVER_PRODUCT_DATABASE_NOT_ISOLATED");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("other-secret"));
    Ok(())
}

#[test]
fn managed_token_and_canonical_source_authority_are_an_exact_pair()
-> Result<(), Box<dyn std::error::Error>> {
    let token = "m".repeat(32);
    let token_only = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PLATFORM_MANAGED_ENROLLMENT_TOKEN", &token),
    ])?;
    assert_error(
        &token_only,
        "SERVER_MANAGED_SOURCE_CONFIGURATION_INCOMPLETE",
    );

    let authority_only = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        (
            "RUNKU_PLATFORM_MANAGED_SOURCE_AUTHORITY",
            "https://cloud.runku.example",
        ),
    ])?;
    assert_error(
        &authority_only,
        "SERVER_MANAGED_SOURCE_CONFIGURATION_INCOMPLETE",
    );

    let noncanonical = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PLATFORM_MANAGED_ENROLLMENT_TOKEN", &token),
        (
            "RUNKU_PLATFORM_MANAGED_SOURCE_AUTHORITY",
            "https://cloud.runku.example/",
        ),
    ])?;
    assert_error(&noncanonical, "SERVER_MANAGED_SOURCE_AUTHORITY_INVALID");

    let paired = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PLATFORM_MANAGED_ENROLLMENT_TOKEN", &token),
        (
            "RUNKU_PLATFORM_MANAGED_SOURCE_AUTHORITY",
            "https://cloud.runku.example",
        ),
    ])?;
    assert!(paired.status.success());
    assert_eq!(paired.stdout, b"configuration valid\n");
    Ok(())
}

#[test]
fn application_listener_requires_product_root_and_explicit_tls_termination()
-> Result<(), Box<dyn std::error::Error>> {
    let product = TempDir::new()?;
    let product_root = product.path().to_str().ok_or("non-UTF-8 temp path")?;
    let without_tls = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PRODUCT_ROOT", product_root),
        ("RUNKU_APPLICATION_LISTEN", "0.0.0.0:3210"),
    ])?;
    assert_error(&without_tls, "SERVER_APPLICATION_TLS_REQUIRED");

    let without_listener = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PRODUCT_ROOT", product_root),
        ("RUNKU_APPLICATION_TLS_TERMINATED", "true"),
    ])?;
    assert_error(
        &without_listener,
        "SERVER_APPLICATION_LISTENER_CONFIGURATION_INCOMPLETE",
    );

    let without_product = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_APPLICATION_LISTEN", "0.0.0.0:3210"),
        ("RUNKU_APPLICATION_TLS_TERMINATED", "true"),
    ])?;
    assert_error(
        &without_product,
        "SERVER_PRODUCT_CONFIGURATION_WITHOUT_ROOT",
    );

    let configured = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_PRODUCT_ROOT", product_root),
        ("RUNKU_APPLICATION_LISTEN", "0.0.0.0:3210"),
        ("RUNKU_APPLICATION_TLS_TERMINATED", "true"),
    ])?;
    assert!(configured.status.success());
    assert_eq!(configured.stdout, b"configuration valid\n");
    Ok(())
}

#[test]
fn shared_cell_manifest_accepts_two_environments_and_conflicts_with_compact_root()
-> Result<(), Box<dyn std::error::Error>> {
    let first = TempDir::new()?;
    let second = TempDir::new()?;
    let mut manifest = NamedTempFile::new()?;
    write!(
        manifest,
        "{}",
        serde_json::json!({
            "version": 1,
            "mode": "shared",
            "memberId": "member_test-01",
            "environments": [
                {
                    "root": first.path(),
                    "hosts": ["first.runku.test"],
                    "platformDatabaseUrlFile": null,
                    "allowedOrigins": [],
                    "authConfig": null
                },
                {
                    "root": second.path(),
                    "hosts": ["second.runku.test"],
                    "platformDatabaseUrlFile": null,
                    "allowedOrigins": [],
                    "authConfig": null
                }
            ]
        })
    )?;
    let manifest_path = manifest.path().to_str().ok_or("non-UTF-8 temp path")?;
    let configured = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_CELL_CONFIG", manifest_path),
        ("RUNKU_APPLICATION_LISTEN", "0.0.0.0:3210"),
        ("RUNKU_APPLICATION_TLS_TERMINATED", "true"),
    ])?;
    assert!(configured.status.success());
    assert_eq!(configured.stdout, b"configuration valid\n");

    let without_listener = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_CELL_CONFIG", manifest_path),
    ])?;
    assert_error(
        &without_listener,
        "SERVER_CELL_APPLICATION_LISTENER_REQUIRED",
    );

    let conflict = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        ("RUNKU_CELL_CONFIG", manifest_path),
        (
            "RUNKU_PRODUCT_ROOT",
            first.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
        ("RUNKU_APPLICATION_LISTEN", "0.0.0.0:3210"),
        ("RUNKU_APPLICATION_TLS_TERMINATED", "true"),
    ])?;
    assert_error(&conflict, "SERVER_PRODUCT_CONFIGURATION_CONFLICT");
    Ok(())
}

#[test]
fn full_node_profile_is_explicit_and_rejects_shared_cells() -> Result<(), Box<dyn std::error::Error>>
{
    let product = TempDir::new()?;
    let invalid = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        (
            "RUNKU_PRODUCT_ROOT",
            product.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
        ("RUNKU_FULL_NODE_PROFILE", "automatic"),
    ])?;
    assert_error(&invalid, "SERVER_FULL_NODE_CONFIGURATION_INVALID");

    let incomplete = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        (
            "RUNKU_PRODUCT_ROOT",
            product.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
        ("RUNKU_FULL_NODE_PROFILE", "dedicated-host"),
    ])?;
    assert_error(&incomplete, "SERVER_FULL_NODE_CONFIGURATION_INVALID");

    let first = TempDir::new()?;
    let second = TempDir::new()?;
    let mut manifest = NamedTempFile::new()?;
    write!(
        manifest,
        "{}",
        serde_json::json!({
            "version": 1,
            "mode": "shared",
            "memberId": "member_full-node-test",
            "environments": [
                {
                    "root": first.path(),
                    "hosts": ["first-node.runku.test"],
                    "platformDatabaseUrlFile": null,
                    "allowedOrigins": [],
                    "authConfig": null
                },
                {
                    "root": second.path(),
                    "hosts": ["second-node.runku.test"],
                    "platformDatabaseUrlFile": null,
                    "allowedOrigins": [],
                    "authConfig": null
                }
            ]
        })
    )?;
    let shared = check(&[
        ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
        (
            "RUNKU_CELL_CONFIG",
            manifest.path().to_str().ok_or("non-UTF-8 temp path")?,
        ),
        ("RUNKU_APPLICATION_LISTEN", "0.0.0.0:3210"),
        ("RUNKU_APPLICATION_TLS_TERMINATED", "true"),
        ("RUNKU_FULL_NODE_PROFILE", "dedicated-host"),
        ("RUNKU_FULL_NODE_INSTANCE_CPU_MILLIS", "1000"),
        ("RUNKU_FULL_NODE_INSTANCE_MEMORY_BYTES", "536870912"),
        ("RUNKU_FULL_NODE_INSTANCE_PIDS", "64"),
    ])?;
    assert_error(&shared, "SERVER_FULL_NODE_REQUIRES_DEDICATED_CELL");

    let node = Command::new("node")
        .args(["--print", "process.execPath"])
        .output()?;
    if node.status.success() {
        let binary = String::from_utf8(node.stdout)?.trim().to_owned();
        let dedicated = check(&[
            ("RUNKU_IDENTITY_DATABASE_URL", IDENTITY_URL),
            (
                "RUNKU_PRODUCT_ROOT",
                product.path().to_str().ok_or("non-UTF-8 temp path")?,
            ),
            ("RUNKU_FULL_NODE_PROFILE", "dedicated-host"),
            ("RUNKU_FULL_NODE_BINARY", &binary),
            ("RUNKU_FULL_NODE_MAX_CONCURRENCY", "1"),
            ("RUNKU_FULL_NODE_INSTANCE_CPU_MILLIS", "1000"),
            ("RUNKU_FULL_NODE_INSTANCE_MEMORY_BYTES", "536870912"),
            ("RUNKU_FULL_NODE_INSTANCE_PIDS", "64"),
        ])?;
        assert!(dedicated.status.success());
        assert_eq!(dedicated.stdout, b"configuration valid\n");
    }
    Ok(())
}
