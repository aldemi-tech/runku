//! Release Manifest and local Artifact Store conformance tests.

use std::{error::Error, sync::Arc};

use proptest::prelude::*;
use runku_core::{
    BuildId, ChannelName, EnvironmentId, EnvironmentScope, FunctionId, ProjectId, ReleaseId,
};
use runku_releases::{
    ARTIFACT_MAX_BYTES, ArtifactDescriptor, ArtifactFormat, ArtifactStore, AuthPolicy, Capability,
    CronDefinition, FilesystemArtifactStore, FilesystemStoreRole, FunctionManifest, FunctionType,
    FunctionVisibility, NodeEsmBundleV1, NodeOciDescriptorV1, ReleaseCommand, ReleaseError,
    ReleaseManifestV1, ReleaseStatus, RuntimeClass, SafeEsmBundleV1, Sha256Digest,
    decode_hybrid_oci_artifact, decode_release_manifest, decode_safe_esm_bundle,
    encode_hybrid_oci_artifact, encode_node_esm_bundle, encode_node_oci_descriptor,
    encode_release_manifest, encode_safe_esm_bundle, hybrid_oci_descriptor,
};
use runku_value::{CanonicalValue, TimestampMicros};
use serde::Deserialize;
use tempfile::tempdir;
use ulid::Ulid;

#[test]
fn manifest_round_trips_and_digest_is_stable() -> Result<(), Box<dyn Error>> {
    let manifest = sample_manifest()?;
    manifest.validate()?;
    manifest.ensure_mvp_runtime_supported()?;
    let encoded = encode_release_manifest(&manifest)?;
    assert_eq!(decode_release_manifest(&encoded)?, manifest);
    assert_eq!(manifest.digest()?, Sha256Digest::of(&encoded));
    assert_eq!(
        encode_release_manifest(&decode_release_manifest(&encoded)?)?,
        encoded
    );
    Ok(())
}

#[test]
fn decoder_rejects_every_truncation_trailing_bytes_and_unknown_version()
-> Result<(), Box<dyn Error>> {
    let encoded = encode_release_manifest(&sample_manifest()?)?;
    for length in 0..encoded.len() {
        assert!(
            decode_release_manifest(&encoded[..length]).is_err(),
            "accepted truncation {length}"
        );
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        decode_release_manifest(&trailing),
        Err(ReleaseError::InvalidManifest)
    );
    let mut unknown = encoded;
    unknown[2] = 2;
    assert_eq!(
        decode_release_manifest(&unknown),
        Err(ReleaseError::Unsupported)
    );
    Ok(())
}

#[test]
fn capability_matrix_ordering_and_runtime_support_fail_closed() -> Result<(), Box<dyn Error>> {
    let mut manifest = sample_manifest()?;
    manifest.functions[0].capabilities.push(Capability::DbWrite);
    assert_eq!(manifest.validate(), Err(ReleaseError::InvalidManifest));

    let mut manifest = sample_manifest()?;
    manifest.functions.reverse();
    assert_eq!(manifest.validate(), Err(ReleaseError::InvalidManifest));

    let mut manifest = sample_manifest()?;
    manifest.functions[0].runtime_class = RuntimeClass::FullNode;
    assert_eq!(
        manifest.ensure_mvp_runtime_supported(),
        Err(ReleaseError::Unsupported)
    );
    assert!(encode_release_manifest(&manifest).is_ok());
    Ok(())
}

#[test]
fn duplicate_function_ids_and_capabilities_are_rejected() -> Result<(), Box<dyn Error>> {
    let mut manifest = sample_manifest()?;
    manifest.functions[1].id = manifest.functions[0].id;
    assert_eq!(manifest.validate(), Err(ReleaseError::InvalidManifest));

    let mut manifest = sample_manifest()?;
    manifest.functions[0].capabilities = vec![Capability::DbRead, Capability::DbRead];
    assert_eq!(manifest.validate(), Err(ReleaseError::InvalidManifest));
    Ok(())
}

#[test]
fn cron_definitions_are_canonical_ordered_and_internal_mutation_or_action()
-> Result<(), Box<dyn Error>> {
    let mut manifest = sample_manifest()?;
    manifest.cron_definitions.push(CronDefinition {
        name: "aaa".parse()?,
        schedule: "0 0 * * *".parse()?,
        function: "webhooks.send".parse()?,
        args: CanonicalValue::Null,
    });
    assert_eq!(manifest.validate(), Err(ReleaseError::InvalidManifest));

    let mut manifest = sample_manifest()?;
    manifest.cron_definitions[0].function = "messages.list".parse()?;
    assert_eq!(manifest.validate(), Err(ReleaseError::InvalidManifest));

    let manifest = sample_manifest()?;
    let encoded = encode_release_manifest(&manifest)?;
    let decoded = decode_release_manifest(&encoded)?;
    assert_eq!(decoded.cron_definitions, manifest.cron_definitions);
    assert_eq!(
        decoded.cron_definitions[0].schedule.as_str(),
        "0,15,30,45 * * * *"
    );
    Ok(())
}

#[test]
fn repository_commands_bind_scope_content_and_non_noop_transitions() -> Result<(), Box<dyn Error>> {
    let manifest = sample_manifest()?;
    let bytes = encode_release_manifest(&manifest)?;
    let scope = EnvironmentScope::new(
        manifest.project_id,
        EnvironmentId::from_ulid(Ulid::from(100_u128)),
    );
    let register = ReleaseCommand::Register {
        manifest_bytes: bytes.clone(),
    };
    register.validate(scope)?;
    let other_environment = EnvironmentScope::new(
        manifest.project_id,
        EnvironmentId::from_ulid(Ulid::from(101_u128)),
    );
    assert_ne!(register.digest(scope)?, register.digest(other_environment)?);
    let foreign_scope = EnvironmentScope::new(
        ProjectId::from_ulid(Ulid::from(999_u128)),
        scope.environment_id(),
    );
    assert_eq!(
        register.validate(foreign_scope),
        Err(ReleaseError::InvalidManifest)
    );
    assert_eq!(
        ReleaseCommand::Register {
            manifest_bytes: bytes[..bytes.len() - 1].to_vec(),
        }
        .validate(scope),
        Err(ReleaseError::InvalidManifest)
    );
    assert_eq!(
        ReleaseCommand::Transition {
            release_id: manifest.release_id,
            expected: ReleaseStatus::Active,
            next: ReleaseStatus::Deprecated,
        }
        .validate(scope),
        Err(ReleaseError::InvalidTransition)
    );
    let stable = "stable".parse::<ChannelName>()?;
    assert_eq!(
        ReleaseCommand::SetDefaultChannel {
            expected_channel: Some(stable.clone()),
            target_channel: Some(stable),
        }
        .validate(scope),
        Err(ReleaseError::InvalidTransition)
    );
    Ok(())
}

#[test]
fn safe_esm_bundle_is_canonical_bounded_and_manifest_bound() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        SafeEsmBundleV1::from_sources([""]),
        Err(ReleaseError::InvalidArtifact)
    );
    assert_eq!(
        SafeEsmBundleV1::from_sources(["x".repeat(8 * 1024 * 1024 + 1)]),
        Err(ReleaseError::LimitExceeded)
    );
    let source = "export default (_ctx, args) => args;\n";
    let bundle = SafeEsmBundleV1::from_sources([source, source])?;
    assert_eq!(bundle.len(), 1);
    let encoded = encode_safe_esm_bundle(&bundle)?;
    assert_eq!(decode_safe_esm_bundle(&encoded)?, bundle);
    let implementation_hash = Sha256Digest::of(source.as_bytes());
    assert_eq!(bundle.source(implementation_hash), Some(source));

    let mut manifest = sample_manifest()?;
    manifest.artifact = bundle.descriptor()?;
    for function in &mut manifest.functions {
        function.implementation_hash = implementation_hash;
    }
    bundle.verify_manifest(&manifest, &encoded)?;

    let mut missing = manifest.clone();
    missing.functions[0].implementation_hash = Sha256Digest::of(b"missing");
    assert_eq!(
        bundle.verify_manifest(&missing, &encoded),
        Err(ReleaseError::InvalidArtifact)
    );
    let mut wrong_descriptor = manifest;
    wrong_descriptor.artifact.size_bytes += 1;
    assert_eq!(
        bundle.verify_manifest(&wrong_descriptor, &encoded),
        Err(ReleaseError::DescriptorMismatch)
    );
    Ok(())
}

#[test]
fn safe_esm_bundle_decoder_rejects_malformed_and_noncanonical_bytes() -> Result<(), Box<dyn Error>>
{
    let bundle =
        SafeEsmBundleV1::from_sources(["export default () => 1;\n", "export default () => 2;\n"])?;
    let encoded = encode_safe_esm_bundle(&bundle)?;
    for length in 0..encoded.len() {
        assert!(
            decode_safe_esm_bundle(&encoded[..length]).is_err(),
            "accepted truncation {length}"
        );
    }

    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        decode_safe_esm_bundle(&trailing),
        Err(ReleaseError::InvalidArtifact)
    );
    let mut unsupported = encoded.clone();
    unsupported[2] = 2;
    assert_eq!(
        decode_safe_esm_bundle(&unsupported),
        Err(ReleaseError::Unsupported)
    );
    let mut tampered = encoded.clone();
    let last = tampered
        .last_mut()
        .ok_or("encoded bundle unexpectedly empty")?;
    *last ^= 1;
    assert_eq!(
        decode_safe_esm_bundle(&tampered),
        Err(ReleaseError::DigestMismatch)
    );

    let mut invalid_utf8 = encoded.clone();
    let source_start = first_source_start(&invalid_utf8)?;
    invalid_utf8[source_start] = 0xff;
    let second_record_start = next_record_start(&invalid_utf8, 5)?;
    let invalid_source = &invalid_utf8[source_start..second_record_start];
    let invalid_digest = Sha256Digest::of(invalid_source);
    invalid_utf8[5..37].copy_from_slice(invalid_digest.as_bytes());
    assert_eq!(
        decode_safe_esm_bundle(&invalid_utf8),
        Err(ReleaseError::InvalidArtifact)
    );

    let first_length_offset = 5 + 32;
    let first_length =
        u32::from_be_bytes(encoded[first_length_offset..first_length_offset + 4].try_into()?)
            as usize;
    let first_record_end = first_length_offset + 4 + first_length;
    let mut reversed = encoded[..5].to_vec();
    reversed.extend_from_slice(&encoded[first_record_end..]);
    reversed.extend_from_slice(&encoded[5..first_record_end]);
    assert_eq!(
        decode_safe_esm_bundle(&reversed),
        Err(ReleaseError::InvalidArtifact)
    );

    let mut duplicate = encoded[..5].to_vec();
    duplicate.extend_from_slice(&encoded[5..first_record_end]);
    duplicate.extend_from_slice(&encoded[5..first_record_end]);
    assert_eq!(
        decode_safe_esm_bundle(&duplicate),
        Err(ReleaseError::InvalidArtifact)
    );
    Ok(())
}

#[test]
fn safe_esm_bundle_golden_vector_is_normative() -> Result<(), Box<dyn Error>> {
    #[derive(Deserialize)]
    struct GoldenVector {
        source: String,
        implementation_sha256: String,
        artifact_sha256: String,
        encoded_hex: String,
    }
    let vector: GoldenVector = serde_json::from_str(include_str!(
        "../../../protocol/v1/safe-esm-bundle-vectors.json"
    ))?;
    let bundle = SafeEsmBundleV1::from_sources([vector.source.as_str()])?;
    let encoded = encode_safe_esm_bundle(&bundle)?;
    assert_eq!(
        Sha256Digest::of(vector.source.as_bytes()).to_string(),
        vector.implementation_sha256
    );
    assert_eq!(
        Sha256Digest::of(&encoded).to_string(),
        vector.artifact_sha256
    );
    assert_eq!(lower_hex(&encoded), vector.encoded_hex);
    Ok(())
}

#[test]
fn hybrid_oci_artifact_is_canonical_bounded_and_tamper_evident() -> Result<(), Box<dyn Error>> {
    let resources = encode_node_esm_bundle(&NodeEsmBundleV1::from_sources([
        "export const safe = () => 'safe';\n",
        "export const node = () => 'node';\n",
    ])?)?;
    let descriptor = encode_node_oci_descriptor(&NodeOciDescriptorV1::new(format!(
        "registry.example/runku/app@sha256:{}",
        "a".repeat(64)
    ))?)?;
    let encoded = encode_hybrid_oci_artifact(&resources, &descriptor)?;
    let (decoded_resources, decoded_descriptor) = decode_hybrid_oci_artifact(&encoded)?;
    assert_eq!(decoded_resources, resources);
    assert_eq!(decoded_descriptor, descriptor);
    assert_eq!(
        hybrid_oci_descriptor(&encoded)?.format,
        ArtifactFormat::HybridOciArtifactV1
    );
    assert_eq!(
        encode_hybrid_oci_artifact(decoded_resources, decoded_descriptor)?,
        encoded
    );

    for length in 0..encoded.len() {
        assert!(
            decode_hybrid_oci_artifact(&encoded[..length]).is_err(),
            "accepted hybrid truncation {length}"
        );
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        decode_hybrid_oci_artifact(&trailing),
        Err(ReleaseError::InvalidArtifact)
    );
    let mut tampered = encoded;
    let last = tampered.last_mut().ok_or("empty hybrid artifact")?;
    *last ^= 1;
    assert!(decode_hybrid_oci_artifact(&tampered).is_err());
    Ok(())
}

#[test]
fn release_manifest_golden_vector_is_normative() -> Result<(), Box<dyn Error>> {
    #[derive(Deserialize)]
    struct GoldenVector {
        encoded_hex: String,
        manifest_sha256: String,
    }
    let vector: GoldenVector = serde_json::from_str(include_str!(
        "../../../protocol/v1/release-manifest-vectors.json"
    ))?;
    let encoded = encode_release_manifest(&sample_manifest()?)?;
    assert_eq!(lower_hex(&encoded), vector.encoded_hex);
    assert_eq!(
        Sha256Digest::of(&encoded).to_string(),
        vector.manifest_sha256
    );
    Ok(())
}

proptest! {
    #[test]
    fn bounded_manifest_variants_round_trip(
        created_at in any::<i64>(),
        artifact_size in 1_u64..=1_000_000,
        digest_byte in any::<u8>(),
    ) {
        let mut manifest = sample_manifest().map_err(|error| TestCaseError::fail(error.to_string()))?;
        manifest.created_at = TimestampMicros::new(created_at);
        manifest.artifact.size_bytes = artifact_size;
        manifest.artifact.digest = Sha256Digest::from_bytes([digest_byte; 32]);
        let encoded = encode_release_manifest(&manifest)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let decoded = decode_release_manifest(&encoded)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(decoded, manifest);
    }

    #[test]
    fn arbitrary_bounded_safe_esm_sources_round_trip(
        sources in proptest::collection::vec(".{1,256}", 1..8),
    ) {
        let bundle = SafeEsmBundleV1::from_sources(sources)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let encoded = encode_safe_esm_bundle(&bundle)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let decoded = decode_safe_esm_bundle(&encoded)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(decoded, bundle);
    }
}

#[tokio::test]
async fn local_artifact_put_get_reopen_and_idempotency() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let bytes = b"export default { functions: ['query', 'action'] };";
    let descriptor = artifact_descriptor(bytes)?;
    let store = FilesystemArtifactStore::open(directory.path(), FilesystemStoreRole::Test).await?;
    store.put(&descriptor, bytes).await?;
    store.put(&descriptor, bytes).await?;
    assert_eq!(store.get(&descriptor).await?, bytes);
    let wrong_size = ArtifactDescriptor {
        size_bytes: descriptor.size_bytes + 1,
        ..descriptor
    };
    assert_eq!(
        store.get(&wrong_size).await,
        Err(ReleaseError::DescriptorMismatch)
    );
    drop(store);

    let stale = directory
        .path()
        .join(".tmp")
        .join("opn_00000000000000000000000000.tmp");
    tokio::fs::write(&stale, b"interrupted write").await?;

    let reopened =
        FilesystemArtifactStore::open(directory.path(), FilesystemStoreRole::Test).await?;
    assert_eq!(reopened.get(&descriptor).await?, bytes);
    assert!(!stale.exists());
    Ok(())
}

#[tokio::test]
async fn concurrent_identical_puts_create_one_valid_object() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let store = FilesystemArtifactStore::open(directory.path(), FilesystemStoreRole::Test).await?;
    let bytes = Arc::new(vec![7_u8; 128 * 1024]);
    let descriptor = artifact_descriptor(&bytes)?;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let task_store = store.clone();
        let task_bytes = Arc::clone(&bytes);
        tasks.push(tokio::spawn(async move {
            task_store.put(&descriptor, &task_bytes).await
        }));
    }
    for task in tasks {
        task.await??;
    }
    assert_eq!(store.get(&descriptor).await?, *bytes);
    Ok(())
}

#[tokio::test]
async fn mismatch_limits_missing_and_production_role_are_explicit() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let root = directory.path().join("artifacts");
    assert!(matches!(
        FilesystemArtifactStore::open(&root, FilesystemStoreRole::Production).await,
        Err(ReleaseError::ProductionBackendUnsupported)
    ));
    assert!(!root.exists());
    let store = FilesystemArtifactStore::open(&root, FilesystemStoreRole::Test).await?;
    let mismatched = ArtifactDescriptor {
        format: ArtifactFormat::SafeEsmBundleV1,
        digest: Sha256Digest::of(b"different"),
        size_bytes: 7,
    };
    assert_eq!(
        store.put(&mismatched, b"content").await,
        Err(ReleaseError::DigestMismatch)
    );
    assert_eq!(
        store.put(&artifact_descriptor(b"")?, b"").await,
        Err(ReleaseError::LimitExceeded)
    );
    let missing = artifact_descriptor(b"missing")?;
    assert_eq!(store.get(&missing).await, Err(ReleaseError::NotFound));
    let oversized = vec![0_u8; ARTIFACT_MAX_BYTES + 1];
    let oversized_descriptor = artifact_descriptor(&oversized)?;
    assert_eq!(
        store.put(&oversized_descriptor, &oversized).await,
        Err(ReleaseError::LimitExceeded)
    );
    Ok(())
}

#[tokio::test]
async fn tampering_is_detected_and_never_overwritten() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let bytes = b"immutable artifact";
    let digest = Sha256Digest::of(bytes);
    let descriptor = artifact_descriptor(bytes)?;
    let store = FilesystemArtifactStore::open(directory.path(), FilesystemStoreRole::Test).await?;
    store.put(&descriptor, bytes).await?;
    let hex = digest.to_string();
    let path = directory
        .path()
        .join(&hex[..2])
        .join(&hex[2..4])
        .join(format!("{}.artifact", &hex[4..]));
    tokio::fs::write(&path, b"immutable artifacU").await?;
    assert_eq!(store.get(&descriptor).await, Err(ReleaseError::Corruption));
    assert_eq!(
        store.put(&descriptor, bytes).await,
        Err(ReleaseError::Corruption)
    );
    assert_eq!(tokio::fs::read(path).await?, b"immutable artifacU");
    Ok(())
}

fn sample_manifest() -> Result<ReleaseManifestV1, Box<dyn Error>> {
    let query = FunctionManifest {
        id: FunctionId::from_ulid(Ulid::from(4_u128)),
        name: "messages.list".parse()?,
        function_type: FunctionType::Query,
        visibility: FunctionVisibility::Public,
        auth_policy: AuthPolicy::Optional,
        runtime_class: RuntimeClass::SafeV8,
        implementation_hash: Sha256Digest::from_bytes([4; 32]),
        arguments_contract_hash: Sha256Digest::from_bytes([5; 32]),
        result_contract_hash: Sha256Digest::from_bytes([6; 32]),
        capabilities: vec![
            Capability::DbRead,
            Capability::AuthRead,
            Capability::FunctionQuery,
        ],
    };
    let action = FunctionManifest {
        id: FunctionId::from_ulid(Ulid::from(5_u128)),
        name: "webhooks.send".parse()?,
        function_type: FunctionType::Action,
        visibility: FunctionVisibility::Internal,
        auth_policy: AuthPolicy::Service,
        runtime_class: RuntimeClass::SafeV8,
        implementation_hash: Sha256Digest::from_bytes([7; 32]),
        arguments_contract_hash: Sha256Digest::from_bytes([8; 32]),
        result_contract_hash: Sha256Digest::from_bytes([9; 32]),
        capabilities: vec![
            Capability::AuthRead,
            Capability::FunctionMutation,
            Capability::NetworkHttps,
            Capability::SchedulerCreate,
            Capability::Secret("webhook-signing".to_owned()),
        ],
    };
    Ok(ReleaseManifestV1 {
        release_id: ReleaseId::from_ulid(Ulid::from(1_u128)),
        project_id: ProjectId::from_ulid(Ulid::from(2_u128)),
        build_id: BuildId::from_ulid(Ulid::from(3_u128)),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: ArtifactDescriptor {
            format: ArtifactFormat::SafeEsmBundleV1,
            digest: Sha256Digest::from_bytes([1; 32]),
            size_bytes: 4096,
        },
        function_contract_hash: Sha256Digest::from_bytes([2; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([3; 32]),
        index_contract_hash: Sha256Digest::from_bytes([4; 32]),
        functions: vec![query, action],
        cron_definitions: vec![CronDefinition {
            name: "webhook-quarter-hour".parse()?,
            schedule: "*/15 * * * *".parse()?,
            function: "webhooks.send".parse()?,
            args: CanonicalValue::Null,
        }],
    })
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn first_source_start(bytes: &[u8]) -> Result<usize, Box<dyn Error>> {
    if bytes.len() < 41 {
        return Err("bundle record header is truncated".into());
    }
    Ok(41)
}

fn next_record_start(bytes: &[u8], record_start: usize) -> Result<usize, Box<dyn Error>> {
    let length_start = record_start + 32;
    let length_end = length_start + 4;
    let length = usize::try_from(u32::from_be_bytes(
        bytes
            .get(length_start..length_end)
            .ok_or("bundle record length is truncated")?
            .try_into()?,
    ))?;
    length_end
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| "bundle record source is truncated".into())
}

fn artifact_descriptor(bytes: &[u8]) -> Result<ArtifactDescriptor, std::num::TryFromIntError> {
    Ok(ArtifactDescriptor {
        format: ArtifactFormat::SafeEsmBundleV1,
        digest: Sha256Digest::of(bytes),
        size_bytes: u64::try_from(bytes.len())?,
    })
}
