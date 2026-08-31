//! Exhaustive lifecycle and two-Release serving routing conformance.

use std::error::Error;

use runku_core::{ChannelName, CodeTarget, EnvironmentId, EnvironmentScope, ProjectId, ReleaseId};
use runku_releases::{
    ArtifactDescriptor, ArtifactFormat, ChannelBinding, ReleaseError, ReleaseLifecycle,
    ReleaseRouter, ReleaseStatus, ServingReleaseEntry, ServingSnapshot, Sha256Digest,
};
use serde::Deserialize;
use ulid::Ulid;

const STATUSES: [ReleaseStatus; 13] = [
    ReleaseStatus::Created,
    ReleaseStatus::Building,
    ReleaseStatus::BuildFailed,
    ReleaseStatus::Validating,
    ReleaseStatus::ValidationFailed,
    ReleaseStatus::CompatibilityBlocked,
    ReleaseStatus::MigrationRequired,
    ReleaseStatus::Ready,
    ReleaseStatus::Servable,
    ReleaseStatus::Active,
    ReleaseStatus::Deprecated,
    ReleaseStatus::Retired,
    ReleaseStatus::GarbageCollected,
];

#[test]
fn lifecycle_transition_matrix_is_exhaustive() {
    for current in STATUSES {
        for next in STATUSES {
            let expected = allowed_transition(current, next);
            assert_eq!(ReleaseLifecycle::advance(current, next).is_ok(), expected);
        }
    }
    assert_eq!(
        ReleaseLifecycle::with_channel_reference(ReleaseStatus::Servable, true),
        Ok(ReleaseStatus::Active)
    );
    assert_eq!(
        ReleaseLifecycle::with_channel_reference(ReleaseStatus::Active, false),
        Ok(ReleaseStatus::Servable)
    );
    assert_eq!(
        ReleaseLifecycle::with_channel_reference(ReleaseStatus::Deprecated, true),
        Err(ReleaseError::InvalidTransition)
    );
}

#[test]
fn invocable_statuses_are_exact() {
    for status in STATUSES {
        assert_eq!(
            status.explicitly_invocable(),
            matches!(
                status,
                ReleaseStatus::Servable | ReleaseStatus::Active | ReleaseStatus::Deprecated
            )
        );
    }
}

#[test]
fn snapshot_rejects_scope_channel_default_and_active_drift() -> Result<(), Box<dyn Error>> {
    let scope = scope();
    let release = release_id(1);
    let entry = entry(release, ReleaseStatus::Servable)?;
    assert!(matches!(
        ServingSnapshot::new(scope, 0, vec![entry.clone()], vec![], None),
        Err(ReleaseError::InvalidSnapshot)
    ));
    let foreign = ServingReleaseEntry {
        project_id: ProjectId::from_ulid(Ulid::from(99_u128)),
        ..entry.clone()
    };
    assert!(matches!(
        ServingSnapshot::new(scope, 1, vec![foreign], vec![], None),
        Err(ReleaseError::InvalidSnapshot)
    ));
    let stable: ChannelName = "stable".parse()?;
    assert!(matches!(
        ServingSnapshot::new(
            scope,
            1,
            vec![entry.clone()],
            vec![ChannelBinding {
                channel: stable.clone(),
                release_id: release
            }],
            Some(stable.clone()),
        ),
        Err(ReleaseError::InvalidSnapshot)
    ));
    assert!(matches!(
        ServingSnapshot::new(scope, 1, vec![entry], vec![], Some(stable)),
        Err(ReleaseError::InvalidSnapshot)
    ));
    Ok(())
}

#[test]
fn promotion_and_rollback_preserve_explicit_r1_r2_bindings() -> Result<(), Box<dyn Error>> {
    let stable: ChannelName = "stable".parse()?;
    let r1 = release_id(1);
    let r2 = release_id(2);
    let promoted = router(
        10,
        vec![
            entry(r1, ReleaseStatus::Servable)?,
            entry(r2, ReleaseStatus::Active)?,
        ],
        vec![ChannelBinding {
            channel: stable.clone(),
            release_id: r2,
        }],
        Some(stable.clone()),
    )?;
    assert_eq!(promoted.resolve(&CodeTarget::Release(r1))?.release_id, r1);
    assert_eq!(promoted.resolve(&CodeTarget::Release(r2))?.release_id, r2);
    assert_eq!(
        promoted
            .resolve(&CodeTarget::Channel(stable.clone()))?
            .release_id,
        r2
    );
    assert_eq!(promoted.resolve_default()?.release_id, r2);

    let rolled_back = router(
        11,
        vec![
            entry(r1, ReleaseStatus::Active)?,
            entry(r2, ReleaseStatus::Servable)?,
        ],
        vec![ChannelBinding {
            channel: stable.clone(),
            release_id: r1,
        }],
        Some(stable.clone()),
    )?;
    assert_eq!(
        rolled_back
            .resolve(&CodeTarget::Channel(stable))?
            .release_id,
        r1
    );
    assert_eq!(
        rolled_back.resolve(&CodeTarget::Release(r2))?.release_id,
        r2
    );
    assert_eq!(rolled_back.resolve_default()?.serving_revision, 11);
    Ok(())
}

#[test]
fn routing_golden_cases_are_normative() -> Result<(), Box<dyn Error>> {
    #[derive(Deserialize)]
    struct Vector {
        cases: Vec<Case>,
    }
    #[derive(Deserialize)]
    struct Case {
        target: String,
        expected_release: Option<String>,
        expected_error: Option<String>,
    }
    let vector: Vector = serde_json::from_str(include_str!(
        "../../../protocol/v1/release-routing-vectors.json"
    ))?;
    let stable: ChannelName = "stable".parse()?;
    let r1 = release_id(1);
    let r2 = release_id(2);
    let ready = release_id(3);
    let retired = release_id(4);
    let router = router(
        7,
        vec![
            entry(r1, ReleaseStatus::Active)?,
            entry(r2, ReleaseStatus::Deprecated)?,
            entry(ready, ReleaseStatus::Ready)?,
            entry(retired, ReleaseStatus::Retired)?,
        ],
        vec![ChannelBinding {
            channel: stable,
            release_id: r1,
        }],
        None,
    )?;
    for case in vector.cases {
        let target: CodeTarget = case.target.parse()?;
        match router.resolve(&target) {
            Ok(effective) => {
                assert_eq!(
                    Some(effective.release_id.to_string()),
                    case.expected_release
                );
                assert!(case.expected_error.is_none());
            }
            Err(error) => {
                assert_eq!(Some(error.code().to_owned()), case.expected_error);
                assert!(case.expected_release.is_none());
            }
        }
    }
    assert_eq!(
        router.resolve_default(),
        Err(ReleaseError::DefaultChannelMissing)
    );
    Ok(())
}

fn allowed_transition(current: ReleaseStatus, next: ReleaseStatus) -> bool {
    matches!(
        (current, next),
        (ReleaseStatus::Created, ReleaseStatus::Building)
            | (
                ReleaseStatus::Building,
                ReleaseStatus::BuildFailed | ReleaseStatus::Validating
            )
            | (
                ReleaseStatus::Validating,
                ReleaseStatus::ValidationFailed
                    | ReleaseStatus::CompatibilityBlocked
                    | ReleaseStatus::MigrationRequired
                    | ReleaseStatus::Ready
            )
            | (
                ReleaseStatus::CompatibilityBlocked | ReleaseStatus::MigrationRequired,
                ReleaseStatus::Validating
            )
            | (ReleaseStatus::Ready, ReleaseStatus::Servable)
            | (ReleaseStatus::Servable, ReleaseStatus::Deprecated)
            | (ReleaseStatus::Deprecated, ReleaseStatus::Retired)
            | (ReleaseStatus::Retired, ReleaseStatus::GarbageCollected)
    )
}

fn scope() -> EnvironmentScope {
    EnvironmentScope::new(
        ProjectId::from_ulid(Ulid::from(10_u128)),
        EnvironmentId::from_ulid(Ulid::from(20_u128)),
    )
}

fn release_id(value: u128) -> ReleaseId {
    ReleaseId::from_ulid(Ulid::from(value))
}

fn entry(
    release_id: ReleaseId,
    status: ReleaseStatus,
) -> Result<ServingReleaseEntry, ReleaseError> {
    Ok(ServingReleaseEntry {
        release_id,
        project_id: scope().project_id(),
        manifest_digest: Sha256Digest::from_bytes(
            [u8::try_from(release_id.as_ulid().0 % 251).map_err(|_| ReleaseError::Internal)?; 32],
        ),
        artifact: ArtifactDescriptor {
            format: ArtifactFormat::SafeEsmBundleV1,
            digest: Sha256Digest::from_bytes([42; 32]),
            size_bytes: 1024,
        },
        runtime_version: "platform-js-1".parse()?,
        status,
    })
}

fn router(
    revision: u64,
    releases: Vec<ServingReleaseEntry>,
    channels: Vec<ChannelBinding>,
    default_channel: Option<ChannelName>,
) -> Result<ReleaseRouter, ReleaseError> {
    Ok(ReleaseRouter::new(ServingSnapshot::new(
        scope(),
        revision,
        releases,
        channels,
        default_channel,
    )?))
}
