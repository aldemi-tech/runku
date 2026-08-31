//! Development Access bearer, digest, and model invariants.

use std::error::Error;

use runku_core::{DevelopmentCredentialId, EnvironmentId, EnvironmentScope, ProjectId};
use runku_development::DevelopmentActor;
use runku_development_access::{
    DevelopmentAccessError, DevelopmentCredential, DevelopmentCredentialLabel,
    DevelopmentCredentialStatus, DevelopmentKeyCrypto, ParsedDevelopmentKey,
};
use runku_value::TimestampMicros;

#[test]
fn keys_are_strict_canonical_and_secrets_are_redacted() -> Result<(), Box<dyn Error>> {
    let id = DevelopmentCredentialId::generate();
    let crypto = DevelopmentKeyCrypto::new([7; 32]);
    let generated = crypto.generate(id)?;
    assert!(generated.key.expose().starts_with("rk_dev_v1_"));
    assert_eq!(generated.key.expose().len(), 80);
    assert_eq!(format!("{:?}", generated.key), "DevelopmentKey([REDACTED])");
    assert_eq!(
        format!("{:?}", generated.digest),
        "DevelopmentKeyDigest([REDACTED])"
    );

    let parsed: ParsedDevelopmentKey = generated.key.expose().parse()?;
    assert_eq!(parsed.credential_id(), id);
    assert!(crypto.verify(parsed.key(), generated.digest));
    assert!(!DevelopmentKeyCrypto::new([8; 32]).verify(parsed.key(), generated.digest));
    let debug = format!("{parsed:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(generated.key.expose()));

    for malformed in [
        "",
        "rk_dev_v1_",
        "rk_dev_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV.bad+token",
        "rk_dev_v1_01arz3ndektsv4rrffq69g5fav.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "rk_sec_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ] {
        assert!(matches!(
            malformed.parse::<ParsedDevelopmentKey>(),
            Err(DevelopmentAccessError::InvalidCredential)
        ));
    }
    Ok(())
}

#[test]
fn labels_and_lifecycle_relationships_are_closed() -> Result<(), Box<dyn Error>> {
    let valid: DevelopmentCredentialLabel = "agent.release_1".parse()?;
    assert_eq!(valid.as_str(), "agent.release_1");
    for invalid in ["", "UPPER", "-leading", "contains space", &"a".repeat(65)] {
        assert_eq!(
            invalid.parse::<DevelopmentCredentialLabel>(),
            Err(DevelopmentAccessError::InvalidInput)
        );
    }

    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let crypto = DevelopmentKeyCrypto::new([9; 32]);
    let generated = crypto.generate(DevelopmentCredentialId::generate())?;
    let mut credential = DevelopmentCredential {
        id: generated
            .key
            .expose()
            .parse::<ParsedDevelopmentKey>()?
            .credential_id(),
        scope,
        actor: "manuel.local".parse::<DevelopmentActor>()?,
        label: valid,
        digest: generated.digest,
        status: DevelopmentCredentialStatus::Active,
        created_at: TimestampMicros::new(10),
        expires_at: Some(TimestampMicros::new(20)),
        revoked_at: None,
        deleted_at: None,
    };
    credential.validate()?;
    credential.expires_at = Some(TimestampMicros::new(10));
    assert_eq!(
        credential.validate(),
        Err(DevelopmentAccessError::InvalidInput)
    );
    credential.expires_at = None;
    credential.status = DevelopmentCredentialStatus::Revoked;
    assert_eq!(
        credential.validate(),
        Err(DevelopmentAccessError::InvalidInput)
    );
    credential.revoked_at = Some(TimestampMicros::new(11));
    credential.validate()?;
    credential.status = DevelopmentCredentialStatus::Deleted;
    credential.deleted_at = Some(TimestampMicros::new(10));
    assert_eq!(
        credential.validate(),
        Err(DevelopmentAccessError::InvalidInput)
    );
    credential.deleted_at = Some(TimestampMicros::new(12));
    credential.validate()?;
    Ok(())
}
