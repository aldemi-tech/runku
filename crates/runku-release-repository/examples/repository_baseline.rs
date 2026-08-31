//! Reproducible local Release Repository promotion/snapshot baseline.

use std::{error::Error, hint::black_box, time::Instant};

use runku_core::{
    BuildId, ChannelName, EnvironmentId, EnvironmentScope, FunctionId, OperationId, ProjectId,
    ReleaseId,
};
use runku_release_repository::{RepositoryConfig, SqlReleaseRepository};
use runku_releases::{
    ArtifactDescriptor, ArtifactFormat, AuthPolicy, Capability, FunctionManifest, FunctionType,
    FunctionVisibility, ReleaseCommand, ReleaseManifestV1, ReleaseRepository, ReleaseStatus,
    RuntimeClass, Sha256Digest, encode_release_manifest,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;

const PROMOTIONS: u32 = 200;
const SNAPSHOTS: u32 = 200;

fn main() -> Result<(), Box<dyn Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("repository.sqlite3").display()
    );
    let repository = SqlReleaseRepository::connect_sqlite(&url, RepositoryConfig::LOCAL).await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let releases = [ReleaseId::generate(), ReleaseId::generate()];
    for (index, release) in releases.into_iter().enumerate() {
        repository
            .apply(
                scope,
                OperationId::generate(),
                &register(scope, release, u8::try_from(index + 1)?)?,
            )
            .await?;
        for (expected, next) in [
            (ReleaseStatus::Created, ReleaseStatus::Building),
            (ReleaseStatus::Building, ReleaseStatus::Validating),
            (ReleaseStatus::Validating, ReleaseStatus::Ready),
            (ReleaseStatus::Ready, ReleaseStatus::Servable),
        ] {
            repository
                .apply(
                    scope,
                    OperationId::generate(),
                    &ReleaseCommand::Transition {
                        release_id: release,
                        expected,
                        next,
                    },
                )
                .await?;
        }
    }
    let channel: ChannelName = "stable".parse()?;
    repository
        .apply(
            scope,
            OperationId::generate(),
            &ReleaseCommand::SetChannel {
                channel: channel.clone(),
                expected_release: None,
                target_release: Some(releases[0]),
            },
        )
        .await?;

    let promotion_started = Instant::now();
    let mut current = releases[0];
    for index in 0..PROMOTIONS {
        let target = releases[usize::try_from((index + 1) % 2)?];
        repository
            .apply(
                scope,
                OperationId::generate(),
                &ReleaseCommand::SetChannel {
                    channel: channel.clone(),
                    expected_release: Some(current),
                    target_release: Some(target),
                },
            )
            .await?;
        current = target;
    }
    let promotion_micros = promotion_started.elapsed().as_micros();

    let snapshot_started = Instant::now();
    for _ in 0..SNAPSHOTS {
        black_box(repository.snapshot(scope).await?);
    }
    let snapshot_micros = snapshot_started.elapsed().as_micros();
    println!("promotions={PROMOTIONS}");
    println!("promotion_total_micros={promotion_micros}");
    println!("snapshots={SNAPSHOTS}");
    println!("snapshot_total_micros={snapshot_micros}");
    repository.close().await;
    Ok(())
}

fn register(
    scope: EnvironmentScope,
    release_id: ReleaseId,
    seed: u8,
) -> Result<ReleaseCommand, Box<dyn Error>> {
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(i64::from(seed)),
        runtime_version: "platform-js-1".parse()?,
        artifact: ArtifactDescriptor {
            format: ArtifactFormat::SafeEsmBundleV1,
            digest: Sha256Digest::from_bytes([seed; 32]),
            size_bytes: 1024,
        },
        function_contract_hash: Sha256Digest::from_bytes([seed + 1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([seed + 2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([seed + 3; 32]),
        functions: vec![FunctionManifest {
            id: FunctionId::generate(),
            name: format!("function{seed}").parse()?,
            function_type: FunctionType::Query,
            visibility: FunctionVisibility::Public,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::from_bytes([seed + 4; 32]),
            arguments_contract_hash: Sha256Digest::from_bytes([seed + 5; 32]),
            result_contract_hash: Sha256Digest::from_bytes([seed + 6; 32]),
            capabilities: vec![Capability::DbRead],
        }],
        cron_definitions: Vec::new(),
    };
    Ok(ReleaseCommand::Register {
        manifest_bytes: encode_release_manifest(&manifest)?,
    })
}
