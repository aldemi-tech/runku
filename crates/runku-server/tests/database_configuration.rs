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
