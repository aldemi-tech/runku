//! Shared SQLite/PostgreSQL registry conformance and adversarial coverage.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    sync::Arc,
};

use runku_core::{EnvironmentId, EnvironmentScope, OperationId, ProjectId};
use runku_object_storage::{
    AccessKeyConfiguration, AccessKeyOperation, AccessKeyPageRequest, AccessKeyState,
    AuditPageRequest, BucketConfiguration, BucketLifecycle, BucketPageRequest, BucketPolicy,
    BucketQuota, BucketState, DeleteObjectCommand, ObjectPageRequest, ObjectStorageActor,
    ObjectStorageError, ObjectStorageRepository, ObjectStorageRepositoryBackend,
    ObjectStorageService, ObjectVersionId, PutObjectCommand, SecretDigestKey, Versioning,
};
use runku_object_storage_repository::{
    ObjectStorageRepositoryConfig, RepositoryRole, SqlObjectStorageRepository,
};
use runku_value::TimestampMicros;
use tempfile::tempdir;
use tokio::sync::Barrier;

#[tokio::test]
async fn sqlite_conformance_reopen_and_checksum() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("object-storage.sqlite3");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    assert!(matches!(
        SqlObjectStorageRepository::connect_sqlite(&url, ObjectStorageRepositoryConfig::PRODUCTION)
            .await,
        Err(ObjectStorageError::ProductionBackendUnsupported)
    ));
    let repository =
        SqlObjectStorageRepository::connect_sqlite(&url, ObjectStorageRepositoryConfig::LOCAL)
            .await?;
    let (scope, bucket_id, create_operation) =
        run_conformance(&repository, ObjectStorageRepositoryBackend::SQLite).await?;
    assert_concurrent_cas(&repository).await?;
    repository.close().await;

    let reopened =
        SqlObjectStorageRepository::connect_sqlite(&url, ObjectStorageRepositoryConfig::LOCAL)
            .await?;
    let bucket = reopened
        .get_bucket(scope, bucket_id)
        .await?
        .ok_or("bucket missing")?;
    assert_eq!(bucket.state, BucketState::Archived);
    assert!(reopened.operation(scope, create_operation).await?.is_some());
    reopened.close().await;

    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(&url).await?;
    sqlx::query("UPDATE runku_storage_schema_migrations SET checksum='tampered' WHERE version=1")
        .execute(&pool)
        .await?;
    pool.close().await;
    assert!(matches!(
        SqlObjectStorageRepository::connect_sqlite(&url, ObjectStorageRepositoryConfig::LOCAL)
            .await,
        Err(ObjectStorageError::Corruption)
    ));
    Ok(())
}

#[tokio::test]
async fn postgres_conformance() -> Result<(), Box<dyn Error>> {
    let Some(url) = std::env::var("RUNKU_TEST_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let repository = SqlObjectStorageRepository::connect_postgres(
        &url,
        ObjectStorageRepositoryConfig::PRODUCTION,
    )
    .await?;
    run_conformance(&repository, ObjectStorageRepositoryBackend::PostgreSQL).await?;
    assert_concurrent_cas(&repository).await?;
    repository.close().await;
    Ok(())
}

#[tokio::test]
async fn delimiter_pagination_never_repeats_a_folded_prefix() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("pagination.sqlite3").display()
    );
    let repository =
        SqlObjectStorageRepository::connect_sqlite(&url, ObjectStorageRepositoryConfig::LOCAL)
            .await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let service =
        ObjectStorageService::new(Arc::new(repository.clone()), SecretDigestKey::new([19; 32]));
    let actor: ObjectStorageActor = "operator:pagination".parse()?;
    let bucket = service
        .create_bucket(
            scope,
            OperationId::generate(),
            configuration("paging", BucketPolicy::Private)?,
            actor.clone(),
            TimestampMicros::new(1),
        )
        .await?
        .operation
        .bucket_id;
    for (index, key) in ["folder/a", "folder/b", "z"].into_iter().enumerate() {
        let index = u8::try_from(index)?;
        service
            .put_object(
                scope,
                bucket,
                OperationId::generate(),
                &PutObjectCommand {
                    version_id: ObjectVersionId::generate(),
                    key: key.to_owned(),
                    size: 1,
                    sha256: [index; 32],
                    content_type: "text/plain".to_owned(),
                    metadata: BTreeMap::new(),
                    actor: actor.clone(),
                    at: TimestampMicros::new(2 + i64::from(index)),
                },
            )
            .await?;
    }
    let first = service
        .list_objects(
            scope,
            bucket,
            &ObjectPageRequest {
                prefix: String::new(),
                delimiter: Some('/'),
                after: None,
                limit: 1,
            },
        )
        .await?;
    assert_eq!(first.common_prefixes, ["folder/"]);
    assert!(first.objects.is_empty());
    let second = service
        .list_objects(
            scope,
            bucket,
            &ObjectPageRequest {
                prefix: String::new(),
                delimiter: Some('/'),
                after: first.next,
                limit: 1,
            },
        )
        .await?;
    assert!(second.common_prefixes.is_empty());
    assert_eq!(second.objects[0].key, "z");
    assert!(second.next.is_none());
    repository.close().await;
    Ok(())
}

async fn assert_concurrent_cas(
    repository: &SqlObjectStorageRepository,
) -> Result<(), Box<dyn Error>> {
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let service =
        ObjectStorageService::new(Arc::new(repository.clone()), SecretDigestKey::new([8; 32]));
    let actor: ObjectStorageActor = "operator:concurrency".parse()?;
    let created = service
        .create_bucket(
            scope,
            OperationId::generate(),
            configuration("concurrent", BucketPolicy::Private)?,
            actor.clone(),
            TimestampMicros::new(100),
        )
        .await?;
    let bucket = created.operation.bucket_id;
    let barrier = Arc::new(Barrier::new(3));
    let mut tasks = Vec::new();
    for name in ["concurrent-a", "concurrent-b"] {
        let service = service.clone();
        let barrier = Arc::clone(&barrier);
        let actor = actor.clone();
        let configuration = configuration(name, BucketPolicy::Private)?;
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            service
                .update_bucket(
                    scope,
                    bucket,
                    OperationId::generate(),
                    1,
                    configuration,
                    actor,
                    TimestampMicros::new(101),
                )
                .await
        }));
    }
    barrier.wait().await;
    let mut successes = 0;
    let mut rejected = 0;
    for task in tasks {
        match task.await? {
            Ok(_) => successes += 1,
            Err(ObjectStorageError::Conflict | ObjectStorageError::Busy) => rejected += 1,
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!((successes, rejected), (1, 1));
    assert_eq!(
        service
            .get_bucket(scope, bucket)
            .await?
            .ok_or("bucket missing")?
            .revision,
        2
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_conformance(
    repository: &SqlObjectStorageRepository,
    backend: ObjectStorageRepositoryBackend,
) -> Result<
    (
        EnvironmentScope,
        runku_object_storage::BucketId,
        OperationId,
    ),
    Box<dyn Error>,
> {
    assert_eq!(repository.backend(), backend);
    repository.health().await?;
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let other_scope = EnvironmentScope::new(scope.project_id(), EnvironmentId::generate());
    let service =
        ObjectStorageService::new(Arc::new(repository.clone()), SecretDigestKey::new([7; 32]));
    let actor: ObjectStorageActor = "operator:01".parse()?;
    let create_operation = OperationId::generate();
    let created = service
        .create_bucket(
            scope,
            create_operation,
            configuration("media", BucketPolicy::Private)?,
            actor.clone(),
            TimestampMicros::new(10),
        )
        .await?;
    assert!(!created.replayed);
    let bucket_id = created.operation.bucket_id;
    assert_eq!(
        service
            .get_bucket_by_name(scope, &"media".parse()?)
            .await?
            .ok_or("named bucket missing")?
            .id,
        bucket_id
    );
    assert!(
        service
            .get_bucket_by_name(other_scope, &"media".parse()?)
            .await?
            .is_none()
    );
    let replay = service
        .create_bucket(
            scope,
            create_operation,
            configuration("media", BucketPolicy::Private)?,
            actor.clone(),
            TimestampMicros::new(10),
        )
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.operation.bucket_id, bucket_id);
    assert!(matches!(
        service
            .create_bucket(
                scope,
                create_operation,
                configuration("assets", BucketPolicy::Private)?,
                actor.clone(),
                TimestampMicros::new(10)
            )
            .await,
        Err(ObjectStorageError::OperationIdReused)
    ));

    assert_eq!(
        service
            .create_bucket(
                scope,
                OperationId::generate(),
                configuration("media", BucketPolicy::Private)?,
                actor.clone(),
                TimestampMicros::new(11)
            )
            .await,
        Err(ObjectStorageError::Conflict)
    );
    service
        .create_bucket(
            other_scope,
            OperationId::generate(),
            configuration("media", BucketPolicy::Private)?,
            actor.clone(),
            TimestampMicros::new(11),
        )
        .await?;
    assert!(service.get_bucket(other_scope, bucket_id).await?.is_none());

    let updated = service
        .update_bucket(
            scope,
            bucket_id,
            OperationId::generate(),
            1,
            configuration("assets", BucketPolicy::PublicRead)?,
            actor.clone(),
            TimestampMicros::new(12),
        )
        .await?;
    assert_eq!(updated.operation.revision, 2);
    assert_eq!(
        service
            .update_bucket(
                scope,
                bucket_id,
                OperationId::generate(),
                1,
                configuration("stale", BucketPolicy::Private)?,
                actor.clone(),
                TimestampMicros::new(13)
            )
            .await,
        Err(ObjectStorageError::Conflict)
    );

    let issue_operation = OperationId::generate();
    let issued = service
        .issue_access_key(
            scope,
            bucket_id,
            issue_operation,
            key_configuration(),
            actor.clone(),
            TimestampMicros::new(14),
        )
        .await?;
    assert!(issued.secret.is_some());
    assert!(
        issued
            .secret
            .as_ref()
            .is_some_and(|secret| secret.expose().starts_with("rk_st_v1_sak_"))
    );
    let first_secret = issued
        .secret
        .as_ref()
        .ok_or("secret missing")?
        .expose()
        .to_owned();
    let first_s3_secret = first_secret
        .rsplit_once('.')
        .ok_or("malformed issued secret")?
        .1;
    let key_id = issued.metadata.id;
    let s3_material = service
        .s3_access_key_material(scope, key_id, TimestampMicros::new(14))
        .await?
        .ok_or("S3 material missing")?;
    assert_eq!(s3_material.metadata, issued.metadata);
    assert_eq!(s3_material.secrets.len(), 1);
    assert_eq!(s3_material.secrets[0].expose(), first_s3_secret);
    assert_eq!(
        format!("{:?}", s3_material.secrets[0]),
        "S3AccessKeySecret([REDACTED])"
    );
    assert!(!format!("{:?}", s3_material.secrets[0]).contains(first_s3_secret));
    assert!(
        service
            .authorize_access_key(
                &first_secret,
                scope,
                bucket_id,
                "uploads/image.png",
                AccessKeyOperation::Write,
                TimestampMicros::new(14)
            )
            .await?
            .is_some()
    );
    assert!(
        service
            .authorize_access_key(
                &first_secret,
                scope,
                bucket_id,
                "private/image.png",
                AccessKeyOperation::Write,
                TimestampMicros::new(14)
            )
            .await?
            .is_none()
    );
    assert!(
        service
            .authorize_access_key(
                &first_secret,
                other_scope,
                bucket_id,
                "uploads/image.png",
                AccessKeyOperation::Write,
                TimestampMicros::new(14)
            )
            .await?
            .is_none()
    );
    let issue_replay = service
        .issue_access_key(
            scope,
            bucket_id,
            issue_operation,
            key_configuration(),
            actor.clone(),
            TimestampMicros::new(14),
        )
        .await?;
    assert!(issue_replay.replayed);
    assert!(issue_replay.secret.is_none());
    assert_eq!(issue_replay.metadata.id, key_id);

    let rotated = service
        .rotate_access_key(
            scope,
            bucket_id,
            key_id,
            OperationId::generate(),
            1,
            TimestampMicros::new(20),
            actor.clone(),
            TimestampMicros::new(15),
        )
        .await?;
    assert!(rotated.secret.is_some());
    let rotated_secret = rotated
        .secret
        .as_ref()
        .ok_or("rotated secret missing")?
        .expose()
        .to_owned();
    let rotated_s3_secret = rotated_secret
        .rsplit_once('.')
        .ok_or("malformed rotated secret")?
        .1;
    assert_eq!(rotated.metadata.revision, 2);
    assert_eq!(
        rotated.metadata.previous_generation_valid_until,
        Some(TimestampMicros::new(20))
    );
    let overlap_material = service
        .s3_access_key_material(scope, key_id, TimestampMicros::new(19))
        .await?
        .ok_or("overlap S3 material missing")?;
    assert_eq!(overlap_material.secrets.len(), 2);
    assert_eq!(overlap_material.secrets[0].expose(), rotated_s3_secret);
    assert_eq!(overlap_material.secrets[1].expose(), first_s3_secret);
    let current_material = service
        .s3_access_key_material(scope, key_id, TimestampMicros::new(20))
        .await?
        .ok_or("current S3 material missing")?;
    assert_eq!(current_material.secrets.len(), 1);
    assert_eq!(current_material.secrets[0].expose(), rotated_s3_secret);
    assert!(
        service
            .authorize_access_key(
                &first_secret,
                scope,
                bucket_id,
                "uploads/image.png",
                AccessKeyOperation::Read,
                TimestampMicros::new(19)
            )
            .await?
            .is_some()
    );
    assert!(
        service
            .authorize_access_key(
                &first_secret,
                scope,
                bucket_id,
                "uploads/image.png",
                AccessKeyOperation::Read,
                TimestampMicros::new(20)
            )
            .await?
            .is_none()
    );
    assert!(
        service
            .authorize_access_key(
                &rotated_secret,
                scope,
                bucket_id,
                "uploads/image.png",
                AccessKeyOperation::Read,
                TimestampMicros::new(20)
            )
            .await?
            .is_some()
    );
    let revoked = service
        .revoke_access_key(
            scope,
            bucket_id,
            key_id,
            OperationId::generate(),
            2,
            actor.clone(),
            TimestampMicros::new(21),
        )
        .await?;
    assert_eq!(revoked.operation.revision, 3);
    assert_eq!(
        service
            .get_access_key(scope, bucket_id, key_id)
            .await?
            .ok_or("key missing")?
            .state,
        AccessKeyState::Revoked
    );
    assert!(
        service
            .authorize_access_key(
                &rotated_secret,
                scope,
                bucket_id,
                "uploads/image.png",
                AccessKeyOperation::Read,
                TimestampMicros::new(22)
            )
            .await?
            .is_none()
    );
    assert!(
        service
            .s3_access_key_material(scope, key_id, TimestampMicros::new(22))
            .await?
            .is_none()
    );

    let second = service
        .issue_access_key(
            scope,
            bucket_id,
            OperationId::generate(),
            key_configuration(),
            actor.clone(),
            TimestampMicros::new(22),
        )
        .await?;
    let second_id = second.metadata.id;
    let put_operation = OperationId::generate();
    let put = service
        .put_object(
            scope,
            bucket_id,
            put_operation,
            &PutObjectCommand {
                version_id: ObjectVersionId::generate(),
                key: "uploads/folder/image.png".to_owned(),
                size: 4,
                sha256: [5; 32],
                content_type: "image/png".to_owned(),
                metadata: BTreeMap::from([("cache-control".to_owned(), "private".to_owned())]),
                actor: actor.clone(),
                at: TimestampMicros::new(22),
            },
        )
        .await?;
    assert!(!put.replayed);
    let object = put.object.as_ref().ok_or("put metadata missing")?;
    let object_version = object.version_id;
    assert_eq!(object.size, 4);
    assert_eq!(
        service.get_object(scope, bucket_id, &object.key).await?,
        Some(object.clone())
    );
    let replay = service
        .put_object(
            scope,
            bucket_id,
            put_operation,
            &PutObjectCommand {
                version_id: ObjectVersionId::generate(),
                key: object.key.clone(),
                size: 4,
                sha256: [5; 32],
                content_type: "image/png".to_owned(),
                metadata: BTreeMap::from([("cache-control".to_owned(), "private".to_owned())]),
                actor: actor.clone(),
                at: TimestampMicros::new(999),
            },
        )
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.operation.version_id, object_version);
    assert!(matches!(
        service
            .put_object(
                scope,
                bucket_id,
                put_operation,
                &PutObjectCommand {
                    version_id: ObjectVersionId::generate(),
                    key: object.key.clone(),
                    size: 5,
                    sha256: [6; 32],
                    content_type: "image/png".to_owned(),
                    metadata: BTreeMap::new(),
                    actor: actor.clone(),
                    at: TimestampMicros::new(23),
                },
            )
            .await,
        Err(ObjectStorageError::OperationIdReused)
    ));
    let page = service
        .list_objects(
            scope,
            bucket_id,
            &ObjectPageRequest {
                prefix: "uploads/".to_owned(),
                delimiter: Some('/'),
                after: None,
                limit: 10,
            },
        )
        .await?;
    assert!(page.objects.is_empty());
    assert_eq!(page.common_prefixes, ["uploads/folder/"]);
    assert!(
        service
            .get_object(other_scope, bucket_id, &object.key)
            .await?
            .is_none()
    );
    assert_eq!(
        service
            .delete_object(
                scope,
                bucket_id,
                OperationId::generate(),
                &DeleteObjectCommand {
                    key: object.key.clone(),
                    expected_version_id: ObjectVersionId::generate(),
                    actor: actor.clone(),
                    at: TimestampMicros::new(23),
                },
            )
            .await,
        Err(ObjectStorageError::Conflict)
    );
    let delete_operation = OperationId::generate();
    let deleted = service
        .delete_object(
            scope,
            bucket_id,
            delete_operation,
            &DeleteObjectCommand {
                key: object.key.clone(),
                expected_version_id: object_version,
                actor: actor.clone(),
                at: TimestampMicros::new(23),
            },
        )
        .await?;
    assert!(!deleted.replayed);
    assert!(
        service
            .get_object(scope, bucket_id, &object.key)
            .await?
            .is_none()
    );
    assert!(
        service
            .object_operation(scope, delete_operation)
            .await?
            .is_some()
    );
    assert!(
        service
            .delete_object(
                scope,
                bucket_id,
                delete_operation,
                &DeleteObjectCommand {
                    key: object.key.clone(),
                    expected_version_id: object_version,
                    actor: actor.clone(),
                    at: TimestampMicros::new(23),
                },
            )
            .await?
            .replayed
    );
    let archived = service
        .archive_bucket(
            scope,
            bucket_id,
            OperationId::generate(),
            2,
            actor,
            TimestampMicros::new(23),
        )
        .await?;
    assert_eq!(archived.operation.revision, 3);
    assert_eq!(
        service
            .get_access_key(scope, bucket_id, second_id)
            .await?
            .ok_or("key missing")?
            .state,
        AccessKeyState::Revoked
    );
    assert!(matches!(
        service
            .issue_access_key(
                scope,
                bucket_id,
                OperationId::generate(),
                key_configuration(),
                "operator:02".parse()?,
                TimestampMicros::new(24)
            )
            .await,
        Err(ObjectStorageError::Conflict)
    ));

    let page = service
        .list_buckets(scope, BucketPageRequest::new(None, 1)?)
        .await?;
    assert_eq!(page.buckets.len(), 1);
    let keys = service
        .list_access_keys(scope, bucket_id, AccessKeyPageRequest::new(None, 1)?)
        .await?;
    assert_eq!(keys.keys.len(), 1);
    assert!(keys.next.is_some());
    let audit = service
        .audit(scope, AuditPageRequest::new(None, 100)?)
        .await?;
    assert_eq!(audit.events.len(), 7);
    assert_eq!(audit.events[0].operation_id, create_operation);
    assert!(repository.telemetry().commands >= 6);
    assert!(repository.telemetry().replays >= 2);
    Ok((scope, bucket_id, create_operation))
}

fn configuration(
    name: &str,
    policy: BucketPolicy,
) -> Result<BucketConfiguration, ObjectStorageError> {
    Ok(BucketConfiguration {
        name: name.parse()?,
        policy,
        cors: Vec::new(),
        versioning: Versioning::Enabled,
        lifecycle: BucketLifecycle {
            expire_current_after_days: Some(365),
            expire_noncurrent_after_days: Some(30),
            abort_incomplete_after_days: Some(7),
        },
        quota: BucketQuota {
            max_object_bytes: 10_000_000,
            max_total_bytes: 1_000_000_000,
            max_objects: 10_000,
        },
    })
}

fn key_configuration() -> AccessKeyConfiguration {
    AccessKeyConfiguration {
        label: "media-uploader".to_owned(),
        prefix: "uploads/".to_owned(),
        operations: BTreeSet::from([AccessKeyOperation::Read, AccessKeyOperation::Write]),
    }
}

#[test]
fn invalid_domains_fail_closed() {
    assert!(
        "Bad_Name"
            .parse::<runku_object_storage::BucketName>()
            .is_err()
    );
    assert!(
        AccessKeyConfiguration {
            label: "x".to_owned(),
            prefix: "../escape".to_owned(),
            operations: BTreeSet::from([AccessKeyOperation::Read])
        }
        .validate()
        .is_err()
    );
}

#[test]
fn local_role_cannot_open_postgres() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let Ok(runtime) = runtime else {
        return;
    };
    assert!(matches!(
        runtime.block_on(SqlObjectStorageRepository::connect_postgres(
            "postgres://localhost/runku",
            ObjectStorageRepositoryConfig {
                role: RepositoryRole::Local,
                ..ObjectStorageRepositoryConfig::LOCAL
            }
        )),
        Err(ObjectStorageError::ProductionBackendUnsupported)
    ));
}
