//! Storage-independent Object Storage service and Product secret issuance.

#![allow(clippy::missing_errors_doc, clippy::too_many_arguments)]

use std::{fmt, sync::Arc};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead as _, KeyInit as _, Payload},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use getrandom::fill;
use hmac::{Hmac, KeyInit, Mac};
use runku_core::{EnvironmentScope, OperationId};
use runku_value::TimestampMicros;
use sha2::{Digest as _, Sha256};
use zeroize::{Zeroize, Zeroizing};

use crate::{
    AccessKeyConfiguration, AccessKeyId, AccessKeyMetadata, AccessKeyPage, AccessKeyPageRequest,
    AccessKeySecret, AuditPage, AuditPageRequest, Bucket, BucketConfiguration, BucketId,
    BucketName, BucketPage, BucketPageRequest, DeleteObjectCommand, EncryptedAccessKeySecret,
    IssuedAccessKey, ObjectMetadata, ObjectOperation, ObjectOperationResult, ObjectPage,
    ObjectPageRequest, ObjectStorageActor, ObjectStorageCommand, ObjectStorageError,
    ObjectStorageOperation, ObjectStorageOperationResult, ObjectStorageRepository,
    ObjectStorageRepositoryBackend, ObjectStorageTelemetrySnapshot, PutObjectCommand, SecretDigest,
};

/// Deployment-owned HMAC key used only to digest Product access-key secrets.
pub struct SecretDigestKey([u8; 32]);

impl SecretDigestKey {
    /// Creates a key from exactly 32 bytes of high-entropy secret configuration.
    #[must_use]
    pub const fn new(value: [u8; 32]) -> Self {
        Self(value)
    }

    fn digest(&self, secret: &[u8; 32]) -> Result<SecretDigest, ObjectStorageError> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.0).map_err(|_| ObjectStorageError::Internal)?;
        mac.update(b"RUNKU_OBJECT_STORAGE_ACCESS_KEY_V1\0");
        mac.update(secret);
        Ok(SecretDigest::new(mac.finalize().into_bytes().into()))
    }
}

impl fmt::Debug for SecretDigestKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretDigestKey([REDACTED])")
    }
}

impl Drop for SecretDigestKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// One decrypted AWS-compatible secret-access-key candidate with redacted diagnostics.
pub struct S3AccessKeySecret(Zeroizing<String>);

impl S3AccessKeySecret {
    /// Borrows the base64url secret for immediate signature verification.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for S3AccessKeySecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("S3AccessKeySecret([REDACTED])")
    }
}

/// Currently valid S3 verification material and its exact Product authorization scope.
#[derive(Debug)]
pub struct S3AccessKeyMaterial {
    /// Non-secret Product authorization metadata.
    pub metadata: AccessKeyMetadata,
    /// Current and optional overlap generation, never more than two.
    pub secrets: Vec<S3AccessKeySecret>,
}

/// Shared provider-independent service.
#[derive(Clone)]
pub struct ObjectStorageService {
    repository: Arc<dyn ObjectStorageRepository>,
    digest_key: Arc<SecretDigestKey>,
}

impl fmt::Debug for ObjectStorageService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectStorageService")
            .field("backend", &self.repository.backend())
            .finish_non_exhaustive()
    }
}

impl ObjectStorageService {
    /// Composes a service over one authoritative repository and secret digest key.
    #[must_use]
    pub fn new(repository: Arc<dyn ObjectStorageRepository>, digest_key: SecretDigestKey) -> Self {
        Self {
            repository,
            digest_key: Arc::new(digest_key),
        }
    }

    /// Returns the selected backend.
    #[must_use]
    pub fn backend(&self) -> ObjectStorageRepositoryBackend {
        self.repository.backend()
    }

    /// Checks repository availability.
    pub async fn health(&self) -> Result<(), ObjectStorageError> {
        self.repository.health().await
    }

    /// Returns bounded telemetry.
    #[must_use]
    pub fn telemetry(&self) -> ObjectStorageTelemetrySnapshot {
        self.repository.telemetry()
    }

    /// Creates one Environment-scoped bucket idempotently.
    pub async fn create_bucket(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        configuration: BucketConfiguration,
        actor: ObjectStorageActor,
        at: TimestampMicros,
    ) -> Result<ObjectStorageOperationResult, ObjectStorageError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &ObjectStorageCommand::CreateBucket {
                    bucket_id: BucketId::generate(),
                    configuration,
                    actor,
                    at,
                },
            )
            .await
    }

    /// Replaces complete bucket configuration with CAS.
    pub async fn update_bucket(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        expected_revision: u64,
        configuration: BucketConfiguration,
        actor: ObjectStorageActor,
        at: TimestampMicros,
    ) -> Result<ObjectStorageOperationResult, ObjectStorageError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &ObjectStorageCommand::UpdateBucket {
                    bucket_id,
                    expected_revision,
                    configuration,
                    actor,
                    at,
                },
            )
            .await
    }

    /// Archives a bucket with CAS. Archive is intentionally irreversible in v1.
    pub async fn archive_bucket(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        expected_revision: u64,
        actor: ObjectStorageActor,
        at: TimestampMicros,
    ) -> Result<ObjectStorageOperationResult, ObjectStorageError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &ObjectStorageCommand::ArchiveBucket {
                    bucket_id,
                    expected_revision,
                    actor,
                    at,
                },
            )
            .await
    }

    /// Gets one exact bucket.
    pub async fn get_bucket(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
    ) -> Result<Option<Bucket>, ObjectStorageError> {
        self.repository.get_bucket(scope, bucket_id).await
    }

    /// Gets one exact bucket by its Environment-unique logical name.
    pub async fn get_bucket_by_name(
        &self,
        scope: EnvironmentScope,
        name: &BucketName,
    ) -> Result<Option<Bucket>, ObjectStorageError> {
        self.repository.get_bucket_by_name(scope, name).await
    }

    /// Lists a bounded stable bucket page.
    pub async fn list_buckets(
        &self,
        scope: EnvironmentScope,
        request: BucketPageRequest,
    ) -> Result<BucketPage, ObjectStorageError> {
        self.repository.list_buckets(scope, request).await
    }

    /// Issues one Product access key. The returned secret exists only for the successful first response.
    pub async fn issue_access_key(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        configuration: AccessKeyConfiguration,
        actor: ObjectStorageActor,
        at: TimestampMicros,
    ) -> Result<IssuedAccessKey, ObjectStorageError> {
        let access_key_id = AccessKeyId::generate();
        let (secret, digest, encrypted_secret) =
            self.issue_material(scope, bucket_id, access_key_id, 1)?;
        let result = self
            .repository
            .apply(
                scope,
                operation_id,
                &ObjectStorageCommand::IssueAccessKey {
                    bucket_id,
                    access_key_id,
                    configuration,
                    secret_digest: digest,
                    encrypted_secret,
                    actor,
                    at,
                },
            )
            .await;
        let result = result?;
        let metadata = self
            .repository
            .get_access_key(
                scope,
                bucket_id,
                result
                    .operation
                    .access_key_id
                    .ok_or(ObjectStorageError::Corruption)?,
            )
            .await?
            .ok_or(ObjectStorageError::Corruption)?;
        let reveal = !result.replayed && metadata.id == access_key_id;
        Ok(IssuedAccessKey {
            metadata,
            secret: reveal.then_some(secret),
            replayed: result.replayed,
        })
    }

    /// Rotates a Product access key and retains its prior generation through `overlap_until`.
    pub async fn rotate_access_key(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        access_key_id: AccessKeyId,
        operation_id: OperationId,
        expected_revision: u64,
        overlap_until: TimestampMicros,
        actor: ObjectStorageActor,
        at: TimestampMicros,
    ) -> Result<IssuedAccessKey, ObjectStorageError> {
        let generation = expected_revision
            .checked_add(1)
            .ok_or(ObjectStorageError::LimitExceeded)?;
        let (secret, digest, encrypted_secret) =
            self.issue_material(scope, bucket_id, access_key_id, generation)?;
        let result = self
            .repository
            .apply(
                scope,
                operation_id,
                &ObjectStorageCommand::RotateAccessKey {
                    bucket_id,
                    access_key_id,
                    expected_revision,
                    secret_digest: digest,
                    encrypted_secret,
                    overlap_until,
                    actor,
                    at,
                },
            )
            .await;
        let result = result?;
        let metadata = self
            .repository
            .get_access_key(scope, bucket_id, access_key_id)
            .await?
            .ok_or(ObjectStorageError::Corruption)?;
        Ok(IssuedAccessKey {
            metadata,
            secret: (!result.replayed).then_some(secret),
            replayed: result.replayed,
        })
    }

    /// Revokes every credential generation for a key with CAS.
    pub async fn revoke_access_key(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        access_key_id: AccessKeyId,
        operation_id: OperationId,
        expected_revision: u64,
        actor: ObjectStorageActor,
        at: TimestampMicros,
    ) -> Result<ObjectStorageOperationResult, ObjectStorageError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &ObjectStorageCommand::RevokeAccessKey {
                    bucket_id,
                    access_key_id,
                    expected_revision,
                    actor,
                    at,
                },
            )
            .await
    }

    /// Gets non-secret key metadata.
    pub async fn get_access_key(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        access_key_id: AccessKeyId,
    ) -> Result<Option<AccessKeyMetadata>, ObjectStorageError> {
        self.repository
            .get_access_key(scope, bucket_id, access_key_id)
            .await
    }

    /// Lists non-secret key metadata.
    pub async fn list_access_keys(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: AccessKeyPageRequest,
    ) -> Result<AccessKeyPage, ObjectStorageError> {
        self.repository
            .list_access_keys(scope, bucket_id, request)
            .await
    }

    /// Authorizes a Product credential for one exact bucket, object key, and operation.
    ///
    /// Malformed, revoked, expired, wrong-scope, and under-scoped credentials all return `None`
    /// so callers do not gain an enumeration oracle.
    pub async fn authorize_access_key(
        &self,
        secret: &str,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        object_key: &str,
        operation: crate::AccessKeyOperation,
        at: TimestampMicros,
    ) -> Result<Option<AccessKeyMetadata>, ObjectStorageError> {
        if object_key.len() > 1_024
            || object_key.starts_with('/')
            || object_key.chars().any(char::is_control)
        {
            return Ok(None);
        }
        let Some((encoded_id, encoded_secret)) = secret
            .strip_prefix("rk_st_v1_")
            .and_then(|value| value.rsplit_once('.'))
        else {
            return Ok(None);
        };
        let Ok(access_key_id) = encoded_id.parse::<AccessKeyId>() else {
            return Ok(None);
        };
        let Ok(raw) = URL_SAFE_NO_PAD.decode(encoded_secret) else {
            return Ok(None);
        };
        let Ok(mut secret_bytes) = <[u8; 32]>::try_from(raw) else {
            return Ok(None);
        };
        let digest = self.digest_key.digest(&secret_bytes)?;
        secret_bytes.zeroize();
        let metadata = self
            .repository
            .authenticate_access_key(scope, bucket_id, access_key_id, &digest, at)
            .await?;
        Ok(metadata.filter(|value| {
            value.configuration.operations.contains(&operation)
                && object_key.starts_with(&value.configuration.prefix)
        }))
    }

    /// Looks up an exact operation after an uncertain result. Secrets are never recoverable here.
    pub async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ObjectStorageOperation>, ObjectStorageError> {
        self.repository.operation(scope, operation_id).await
    }

    /// Lists append-only audit events.
    pub async fn audit(
        &self,
        scope: EnvironmentScope,
        request: AuditPageRequest,
    ) -> Result<AuditPage, ObjectStorageError> {
        self.repository.audit(scope, request).await
    }

    /// Commits current/version metadata after the caller durably stores content-addressed bytes.
    pub async fn put_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        command: &PutObjectCommand,
    ) -> Result<ObjectOperationResult, ObjectStorageError> {
        self.repository
            .put_object(scope, bucket_id, operation_id, command)
            .await
    }

    /// Gets current object metadata.
    pub async fn get_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStorageError> {
        crate::validate_object_key(key)?;
        self.repository.get_object(scope, bucket_id, key).await
    }

    /// Gets one immutable object version.
    pub async fn get_object_version(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        key: &str,
        version_id: crate::ObjectVersionId,
    ) -> Result<Option<ObjectMetadata>, ObjectStorageError> {
        crate::validate_object_key(key)?;
        self.repository
            .get_object_version(scope, bucket_id, key, version_id)
            .await
    }

    /// Lists one bounded object browser page.
    pub async fn list_objects(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: &ObjectPageRequest,
    ) -> Result<ObjectPage, ObjectStorageError> {
        request.validate()?;
        self.repository
            .list_objects(scope, bucket_id, request)
            .await
    }

    /// Lists immutable object versions for one exact bucket.
    pub async fn list_object_versions(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        request: &crate::ObjectVersionPageRequest,
    ) -> Result<crate::ObjectVersionPage, ObjectStorageError> {
        request.validate()?;
        self.repository
            .list_object_versions(scope, bucket_id, request)
            .await
    }

    /// Deletes one exact immutable version.
    pub async fn delete_object_version(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        key: &str,
        version_id: crate::ObjectVersionId,
    ) -> Result<bool, ObjectStorageError> {
        crate::validate_object_key(key)?;
        self.repository
            .delete_object_version(scope, bucket_id, key, version_id)
            .await
    }

    /// Creates one durable multipart upload.
    pub async fn create_multipart_upload(
        &self,
        upload: &crate::MultipartUpload,
    ) -> Result<(), ObjectStorageError> {
        upload.validate()?;
        self.repository.create_multipart_upload(upload).await
    }

    /// Gets one multipart upload.
    pub async fn get_multipart_upload(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: crate::MultipartUploadId,
    ) -> Result<Option<crate::MultipartUpload>, ObjectStorageError> {
        self.repository
            .get_multipart_upload(scope, bucket_id, upload_id)
            .await
    }

    /// Upserts one multipart part.
    pub async fn put_multipart_part(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: crate::MultipartUploadId,
        part: &crate::MultipartPart,
    ) -> Result<(), ObjectStorageError> {
        part.validate()?;
        self.repository
            .put_multipart_part(scope, bucket_id, upload_id, part)
            .await
    }

    /// Lists all parts of one upload.
    pub async fn list_multipart_parts(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: crate::MultipartUploadId,
    ) -> Result<Vec<crate::MultipartPart>, ObjectStorageError> {
        self.repository
            .list_multipart_parts(scope, bucket_id, upload_id)
            .await
    }

    /// Lists active multipart uploads.
    pub async fn list_multipart_uploads(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        prefix: &str,
        after: Option<(&str, crate::MultipartUploadId)>,
        limit: u16,
    ) -> Result<crate::MultipartUploadPage, ObjectStorageError> {
        if prefix.len() > 1_024 || prefix.starts_with('/') || limit == 0 || limit > 100 {
            return Err(ObjectStorageError::InvalidInput);
        }
        self.repository
            .list_multipart_uploads(scope, bucket_id, prefix, after, limit)
            .await
    }

    /// Claims one exact multipart completion body before reading parts or writing final bytes.
    pub async fn claim_multipart_completion(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: crate::MultipartUploadId,
        completion_digest: [u8; 32],
    ) -> Result<(), ObjectStorageError> {
        self.repository
            .claim_multipart_completion(scope, bucket_id, upload_id, completion_digest)
            .await
    }

    /// Marks an upload completed with the exact committed object version.
    pub async fn complete_multipart_upload(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: crate::MultipartUploadId,
        version_id: crate::ObjectVersionId,
        at: TimestampMicros,
    ) -> Result<(), ObjectStorageError> {
        self.repository
            .complete_multipart_upload(scope, bucket_id, upload_id, version_id, at)
            .await
    }

    /// Aborts one incomplete multipart upload idempotently.
    pub async fn abort_multipart_upload(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        upload_id: crate::MultipartUploadId,
    ) -> Result<(), ObjectStorageError> {
        self.repository
            .abort_multipart_upload(scope, bucket_id, upload_id)
            .await
    }

    /// Applies one bounded lifecycle pass.
    pub async fn apply_lifecycle(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        at: TimestampMicros,
        limit: u16,
    ) -> Result<crate::LifecycleResult, ObjectStorageError> {
        if at.get() < 0 || limit == 0 || limit > 100 {
            return Err(ObjectStorageError::InvalidInput);
        }
        self.repository
            .apply_lifecycle(scope, bucket_id, at, limit)
            .await
    }

    /// Deletes one exact current version from the logical namespace.
    pub async fn delete_object(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        operation_id: OperationId,
        command: &DeleteObjectCommand,
    ) -> Result<ObjectOperationResult, ObjectStorageError> {
        self.repository
            .delete_object(scope, bucket_id, operation_id, command)
            .await
    }

    /// Reconciles one uncertain object mutation.
    pub async fn object_operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ObjectOperation>, ObjectStorageError> {
        self.repository.object_operation(scope, operation_id).await
    }

    /// Loads and decrypts currently valid S3 signature candidates for one exact Environment.
    ///
    /// Pre-encryption generations intentionally return `None` and must be rotated before S3 use.
    pub async fn s3_access_key_material(
        &self,
        scope: EnvironmentScope,
        access_key_id: AccessKeyId,
        at: TimestampMicros,
    ) -> Result<Option<S3AccessKeyMaterial>, ObjectStorageError> {
        let generations = self
            .repository
            .encrypted_access_key_generations(scope, access_key_id, at)
            .await?;
        let Some(first) = generations.first() else {
            return Ok(None);
        };
        if generations.len() > 2
            || generations
                .iter()
                .any(|value| value.metadata != first.metadata)
        {
            return Err(ObjectStorageError::Corruption);
        }
        let metadata = first.metadata.clone();
        let mut secrets = Vec::with_capacity(generations.len());
        for generation in generations {
            let aad = access_key_aad(
                scope,
                generation.metadata.bucket_id,
                access_key_id,
                generation.generation,
            );
            let cipher = self.cipher()?;
            let plaintext = cipher
                .decrypt(
                    Nonce::from_slice(generation.secret.nonce()),
                    Payload {
                        msg: generation.secret.ciphertext(),
                        aad: aad.as_bytes(),
                    },
                )
                .map_err(|_| ObjectStorageError::Corruption)?;
            let mut raw: [u8; 32] = plaintext
                .try_into()
                .map_err(|_| ObjectStorageError::Corruption)?;
            let encoded = URL_SAFE_NO_PAD.encode(raw);
            raw.zeroize();
            secrets.push(S3AccessKeySecret(Zeroizing::new(encoded)));
        }
        Ok(Some(S3AccessKeyMaterial { metadata, secrets }))
    }

    fn issue_material(
        &self,
        scope: EnvironmentScope,
        bucket_id: BucketId,
        access_key_id: AccessKeyId,
        generation: u64,
    ) -> Result<(AccessKeySecret, SecretDigest, EncryptedAccessKeySecret), ObjectStorageError> {
        let mut raw = [0_u8; 32];
        fill(&mut raw).map_err(|_| ObjectStorageError::Internal)?;
        let digest = match self.digest_key.digest(&raw) {
            Ok(value) => value,
            Err(error) => {
                raw.zeroize();
                return Err(error);
            }
        };
        let mut nonce = [0_u8; 12];
        if fill(&mut nonce).is_err() {
            raw.zeroize();
            return Err(ObjectStorageError::Internal);
        }
        let aad = access_key_aad(scope, bucket_id, access_key_id, generation);
        let encrypted = if let Ok(value) = self.cipher()?.encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &raw,
                aad: aad.as_bytes(),
            },
        ) {
            EncryptedAccessKeySecret::new(nonce, value)
        } else {
            raw.zeroize();
            return Err(ObjectStorageError::Internal);
        };
        let secret = AccessKeySecret::from_parts(access_key_id, &raw);
        raw.zeroize();
        Ok((secret, digest, encrypted))
    }

    fn cipher(&self) -> Result<Aes256Gcm, ObjectStorageError> {
        let mut digest = Sha256::new();
        digest.update(b"RUNKU_OBJECT_STORAGE_S3_ENCRYPTION_KEY_V1\0");
        digest.update(self.digest_key.0);
        let mut key: [u8; 32] = digest.finalize().into();
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| ObjectStorageError::Internal);
        key.zeroize();
        cipher
    }
}

fn access_key_aad(
    scope: EnvironmentScope,
    bucket_id: BucketId,
    access_key_id: AccessKeyId,
    generation: u64,
) -> String {
    format!(
        "RUNKU_OBJECT_STORAGE_S3_SECRET_V1\n{}\n{}\n{bucket_id}\n{access_key_id}\n{generation}",
        scope.project_id(),
        scope.environment_id(),
    )
}
