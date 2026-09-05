//! Logical bucket, credential, operation, and audit models.

#![allow(clippy::missing_errors_doc)]

use std::{collections::BTreeSet, fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use runku_core::{EnvironmentScope, OperationId};
use runku_value::TimestampMicros;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;
use zeroize::Zeroize;

use crate::ObjectStorageError;

const MAX_PAGE_SIZE: u16 = 100;
const MAX_CORS_RULES: usize = 16;
const MAX_CORS_VALUES: usize = 32;
const MAX_TEXT_BYTES: usize = 256;

macro_rules! storage_id {
    ($name:ident, $prefix:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Ulid);

        impl $name {
            /// Generates a new time-sortable identifier.
            #[must_use]
            pub fn generate() -> Self {
                Self(Ulid::generate())
            }

            /// Builds a deterministic identifier for conformance tests.
            #[must_use]
            pub const fn from_ulid(value: Ulid) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}{}", $prefix, self.0)
            }
        }

        impl FromStr for $name {
            type Err = ObjectStorageError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let payload = value
                    .strip_prefix($prefix)
                    .ok_or(ObjectStorageError::InvalidInput)?;
                let id = payload
                    .parse::<Ulid>()
                    .map_err(|_| ObjectStorageError::InvalidInput)?;
                if id.to_string() != payload {
                    return Err(ObjectStorageError::InvalidInput);
                }
                Ok(Self(id))
            }
        }
    };
}

storage_id!(BucketId, "bkt_", "Identifies one logical bucket.");
storage_id!(
    AccessKeyId,
    "sak_",
    "Identifies one Product Object Storage access key."
);

/// Unique, DNS-label-compatible bucket name within one Environment.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BucketName(String);

impl BucketName {
    /// Returns the canonical name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BucketName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for BucketName {
    type Err = ObjectStorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = value.as_bytes();
        let valid = (3..=63).contains(&bytes.len())
            && bytes.first().is_some_and(u8::is_ascii_lowercase)
            && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
            && !value.contains("--");
        if !valid {
            return Err(ObjectStorageError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Stable non-secret identity of the Product actor that caused a mutation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectStorageActor(String);

impl ObjectStorageActor {
    /// Returns the canonical actor identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ObjectStorageActor {
    type Err = ObjectStorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 128
            || value.chars().any(char::is_control)
            || value.trim() != value
        {
            return Err(ObjectStorageError::InvalidInput);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Bucket read policy. Write access always requires a Product credential.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BucketPolicy {
    /// Every object operation requires authorization.
    Private,
    /// Object reads may be anonymous; mutations still require authorization.
    PublicRead,
}

impl BucketPolicy {
    /// Canonical persisted representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public_read",
        }
    }
}

impl FromStr for BucketPolicy {
    type Err = ObjectStorageError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "private" => Ok(Self::Private),
            "public_read" => Ok(Self::PublicRead),
            _ => Err(ObjectStorageError::InvalidInput),
        }
    }
}

/// CORS request method.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum CorsMethod {
    /// GET.
    Get,
    /// HEAD.
    Head,
    /// PUT.
    Put,
    /// POST.
    Post,
    /// DELETE.
    Delete,
}

impl CorsMethod {
    /// Canonical persisted representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Put => "PUT",
            Self::Post => "POST",
            Self::Delete => "DELETE",
        }
    }
}

impl FromStr for CorsMethod {
    type Err = ObjectStorageError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "GET" => Ok(Self::Get),
            "HEAD" => Ok(Self::Head),
            "PUT" => Ok(Self::Put),
            "POST" => Ok(Self::Post),
            "DELETE" => Ok(Self::Delete),
            _ => Err(ObjectStorageError::InvalidInput),
        }
    }
}

/// One bounded CORS rule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CorsRule {
    /// Exact HTTPS origins, or a sole `*` entry.
    pub origins: Vec<String>,
    /// Allowed methods.
    pub methods: BTreeSet<CorsMethod>,
    /// Allowed request header names, lower-case, or a sole `*` entry.
    pub allowed_headers: Vec<String>,
    /// Exposed response header names, lower-case.
    pub exposed_headers: Vec<String>,
    /// Browser preflight cache duration, at most one day.
    pub max_age_seconds: u32,
}

impl CorsRule {
    /// Validates bounds and canonical ordering/values.
    pub fn validate(&self) -> Result<(), ObjectStorageError> {
        if self.origins.is_empty()
            || self.origins.len() > MAX_CORS_VALUES
            || self.methods.is_empty()
            || self.allowed_headers.len() > MAX_CORS_VALUES
            || self.exposed_headers.len() > MAX_CORS_VALUES
            || self.max_age_seconds > 86_400
            || !is_sorted_unique(&self.origins)
            || !is_sorted_unique(&self.allowed_headers)
            || !is_sorted_unique(&self.exposed_headers)
            || !valid_wildcard_list(&self.origins)
            || !valid_wildcard_list(&self.allowed_headers)
        {
            return Err(ObjectStorageError::InvalidInput);
        }
        for origin in &self.origins {
            if origin != "*"
                && (!origin.starts_with("https://")
                    || origin.len() > MAX_TEXT_BYTES
                    || origin.ends_with('/')
                    || origin.chars().any(char::is_control))
            {
                return Err(ObjectStorageError::InvalidInput);
            }
        }
        for header in self.allowed_headers.iter().chain(&self.exposed_headers) {
            if header != "*" && !valid_header(header) {
                return Err(ObjectStorageError::InvalidInput);
            }
        }
        Ok(())
    }
}

fn is_sorted_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_wildcard_list(values: &[String]) -> bool {
    !values.iter().any(|value| value == "*") || values == ["*"]
}

fn valid_header(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// Bucket versioning policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Versioning {
    /// New writes replace the current logical value.
    Disabled,
    /// New writes retain prior object versions.
    Enabled,
}

impl Versioning {
    /// Canonical persisted representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
        }
    }
}

impl FromStr for Versioning {
    type Err = ObjectStorageError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "enabled" => Ok(Self::Enabled),
            _ => Err(ObjectStorageError::InvalidInput),
        }
    }
}

/// Lifecycle rules interpreted later by a provider adapter.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BucketLifecycle {
    /// Expire current objects after this many days.
    pub expire_current_after_days: Option<u32>,
    /// Expire non-current versions after this many days.
    pub expire_noncurrent_after_days: Option<u32>,
    /// Abort incomplete multipart uploads after this many days.
    pub abort_incomplete_after_days: Option<u32>,
}

impl BucketLifecycle {
    fn validate(self, versioning: Versioning) -> Result<(), ObjectStorageError> {
        for value in [
            self.expire_current_after_days,
            self.expire_noncurrent_after_days,
            self.abort_incomplete_after_days,
        ]
        .into_iter()
        .flatten()
        {
            if !(1..=36_500).contains(&value) {
                return Err(ObjectStorageError::LimitExceeded);
            }
        }
        if versioning == Versioning::Disabled && self.expire_noncurrent_after_days.is_some() {
            return Err(ObjectStorageError::InvalidInput);
        }
        Ok(())
    }
}

/// Logical quota enforced by future object-byte adapters.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BucketQuota {
    /// Maximum bytes in one object.
    pub max_object_bytes: u64,
    /// Maximum logical bytes across current object versions.
    pub max_total_bytes: u64,
    /// Maximum current object count.
    pub max_objects: u64,
}

impl BucketQuota {
    fn validate(self) -> Result<(), ObjectStorageError> {
        const MAX_BYTES: u64 = 1 << 60;
        if self.max_object_bytes == 0
            || self.max_total_bytes == 0
            || self.max_objects == 0
            || self.max_object_bytes > self.max_total_bytes
            || self.max_total_bytes > MAX_BYTES
            || self.max_objects > 1_000_000_000
        {
            return Err(ObjectStorageError::LimitExceeded);
        }
        Ok(())
    }
}

/// Complete revisioned logical bucket configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BucketConfiguration {
    /// Environment-unique logical name.
    pub name: BucketName,
    /// Anonymous read policy.
    pub policy: BucketPolicy,
    /// Bounded CORS rules.
    pub cors: Vec<CorsRule>,
    /// Versioning mode.
    pub versioning: Versioning,
    /// Lifecycle policy.
    pub lifecycle: BucketLifecycle,
    /// Logical quota.
    pub quota: BucketQuota,
}

impl BucketConfiguration {
    /// Validates the complete configuration.
    pub fn validate(&self) -> Result<(), ObjectStorageError> {
        let _: BucketName = self.name.as_str().parse()?;
        if self.cors.len() > MAX_CORS_RULES {
            return Err(ObjectStorageError::LimitExceeded);
        }
        for rule in &self.cors {
            rule.validate()?;
        }
        self.lifecycle.validate(self.versioning)?;
        self.quota.validate()
    }
}

/// Desired bucket lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketState {
    /// Bucket can be configured and used.
    Active,
    /// Bucket is archived and cannot accept new credentials or configuration.
    Archived,
}

impl BucketState {
    /// Canonical persisted representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }
}

impl FromStr for BucketState {
    type Err = ObjectStorageError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "archived" => Ok(Self::Archived),
            _ => Err(ObjectStorageError::InvalidInput),
        }
    }
}

/// One exact-scoped logical bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bucket {
    /// Exact Project/Environment owner.
    pub scope: EnvironmentScope,
    /// Bucket identifier.
    pub id: BucketId,
    /// Complete desired configuration.
    pub configuration: BucketConfiguration,
    /// Monotonic configuration/state revision.
    pub revision: u64,
    /// Desired lifecycle state.
    pub state: BucketState,
    /// Creation time.
    pub created_at: TimestampMicros,
    /// Last mutation time.
    pub updated_at: TimestampMicros,
}

impl Bucket {
    /// Validates persisted bucket invariants.
    pub fn validate(&self) -> Result<(), ObjectStorageError> {
        self.configuration.validate()?;
        if self.revision == 0 || self.created_at.get() < 0 || self.updated_at < self.created_at {
            return Err(ObjectStorageError::Corruption);
        }
        Ok(())
    }
}

/// Bounded bucket list request ordered by bucket ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketPageRequest {
    /// Exclusive cursor.
    pub after: Option<BucketId>,
    /// Result limit from 1 through 100.
    pub limit: u16,
}

impl BucketPageRequest {
    /// Creates a validated request.
    pub fn new(after: Option<BucketId>, limit: u16) -> Result<Self, ObjectStorageError> {
        let value = Self { after, limit };
        value.validate()?;
        Ok(value)
    }
    /// Validates request bounds.
    pub fn validate(self) -> Result<(), ObjectStorageError> {
        if self.limit == 0 || self.limit > MAX_PAGE_SIZE {
            Err(ObjectStorageError::LimitExceeded)
        } else {
            Ok(())
        }
    }
}

/// One stable page of buckets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketPage {
    /// Exact owner scope.
    pub scope: EnvironmentScope,
    /// Ordered buckets.
    pub buckets: Vec<Bucket>,
    /// Continuation cursor.
    pub next: Option<BucketId>,
}

/// Operation allowed by one Product access key.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum AccessKeyOperation {
    /// List object keys.
    List,
    /// Read object bytes and metadata.
    Read,
    /// Create or replace object bytes and metadata.
    Write,
    /// Delete objects or versions.
    Delete,
}

impl AccessKeyOperation {
    /// Canonical persisted representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Read => "read",
            Self::Write => "write",
            Self::Delete => "delete",
        }
    }
}

impl FromStr for AccessKeyOperation {
    type Err = ObjectStorageError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "list" => Ok(Self::List),
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "delete" => Ok(Self::Delete),
            _ => Err(ObjectStorageError::InvalidInput),
        }
    }
}

/// Immutable authorization scope and human label for a Product access key.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AccessKeyConfiguration {
    /// Human-facing label.
    pub label: String,
    /// Object key prefix. Empty means the entire bucket.
    pub prefix: String,
    /// Non-empty allowed operation set.
    pub operations: BTreeSet<AccessKeyOperation>,
}

impl AccessKeyConfiguration {
    /// Validates credential bounds.
    pub fn validate(&self) -> Result<(), ObjectStorageError> {
        if self.label.is_empty()
            || self.label.len() > 128
            || self.label.trim() != self.label
            || self.label.chars().any(char::is_control)
            || self.prefix.len() > 1_024
            || self.prefix.starts_with('/')
            || self.prefix.contains("..")
            || self.prefix.chars().any(char::is_control)
            || self.operations.is_empty()
        {
            return Err(ObjectStorageError::InvalidInput);
        }
        Ok(())
    }
}

/// Public access-key state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessKeyState {
    /// At least one credential generation is active.
    Active,
    /// Every generation is revoked.
    Revoked,
}

impl AccessKeyState {
    /// Canonical persisted representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Revoked => "revoked",
        }
    }
}

impl FromStr for AccessKeyState {
    type Err = ObjectStorageError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "revoked" => Ok(Self::Revoked),
            _ => Err(ObjectStorageError::InvalidInput),
        }
    }
}

/// Non-secret Product access-key metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessKeyMetadata {
    /// Exact Project/Environment owner.
    pub scope: EnvironmentScope,
    /// Owning bucket.
    pub bucket_id: BucketId,
    /// Public access-key identifier.
    pub id: AccessKeyId,
    /// Authorization scope.
    pub configuration: AccessKeyConfiguration,
    /// Monotonic metadata/credential revision.
    pub revision: u64,
    /// Current state.
    pub state: AccessKeyState,
    /// Creation time.
    pub created_at: TimestampMicros,
    /// Last mutation time.
    pub updated_at: TimestampMicros,
    /// Prior generation expiry during rotation overlap.
    pub previous_generation_valid_until: Option<TimestampMicros>,
}

impl AccessKeyMetadata {
    /// Validates persisted access-key invariants.
    pub fn validate(&self) -> Result<(), ObjectStorageError> {
        self.configuration.validate()?;
        if self.revision == 0
            || self.created_at.get() < 0
            || self.updated_at < self.created_at
            || self
                .previous_generation_valid_until
                .is_some_and(|value| value < self.updated_at)
            || (self.state == AccessKeyState::Revoked
                && self.previous_generation_valid_until.is_some())
        {
            return Err(ObjectStorageError::Corruption);
        }
        Ok(())
    }
}

/// Secret shown exactly once after successful issuance or rotation.
pub struct AccessKeySecret(String);

impl AccessKeySecret {
    pub(crate) fn from_parts(id: AccessKeyId, secret: &[u8; 32]) -> Self {
        Self(format!("rk_st_v1_{id}.{}", URL_SAFE_NO_PAD.encode(secret)))
    }

    /// Borrows the secret for immediate delivery. Callers must redact logs and durable state.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AccessKeySecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AccessKeySecret([REDACTED])")
    }
}

impl Drop for AccessKeySecret {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Secret digest safe for authoritative persistence. It is not a provider credential.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretDigest([u8; 32]);

impl SecretDigest {
    /// Reconstructs a validated digest from storage.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectStorageError> {
        let value: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ObjectStorageError::Corruption)?;
        Ok(Self(value))
    }
    /// Returns digest bytes for persistence.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub(crate) const fn new(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl fmt::Debug for SecretDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretDigest([REDACTED])")
    }
}

/// Result of issuing or rotating a key. Replays never contain `secret`.
#[derive(Debug)]
pub struct IssuedAccessKey {
    /// Non-secret metadata.
    pub metadata: AccessKeyMetadata,
    /// One-time secret, present only for the transaction that created its generation.
    pub secret: Option<AccessKeySecret>,
    /// Whether the operation journal supplied this result.
    pub replayed: bool,
}

/// Bounded access-key list request ordered by key ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccessKeyPageRequest {
    /// Exclusive cursor.
    pub after: Option<AccessKeyId>,
    /// Result limit from 1 through 100.
    pub limit: u16,
}

impl AccessKeyPageRequest {
    /// Creates a validated request.
    pub fn new(after: Option<AccessKeyId>, limit: u16) -> Result<Self, ObjectStorageError> {
        let value = Self { after, limit };
        value.validate()?;
        Ok(value)
    }
    /// Validates request bounds.
    pub fn validate(self) -> Result<(), ObjectStorageError> {
        if self.limit == 0 || self.limit > MAX_PAGE_SIZE {
            Err(ObjectStorageError::LimitExceeded)
        } else {
            Ok(())
        }
    }
}

/// One stable page of key metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessKeyPage {
    /// Exact owner scope.
    pub scope: EnvironmentScope,
    /// Exact bucket.
    pub bucket_id: BucketId,
    /// Ordered metadata.
    pub keys: Vec<AccessKeyMetadata>,
    /// Continuation cursor.
    pub next: Option<AccessKeyId>,
}

/// Mutation kind written to the idempotency journal and audit log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStorageOperationKind {
    /// Bucket creation.
    CreateBucket,
    /// Bucket configuration replacement.
    UpdateBucket,
    /// Bucket archival.
    ArchiveBucket,
    /// Access-key issuance.
    IssueAccessKey,
    /// Access-key rotation.
    RotateAccessKey,
    /// Access-key revocation.
    RevokeAccessKey,
}

impl ObjectStorageOperationKind {
    /// Canonical persisted representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateBucket => "create_bucket",
            Self::UpdateBucket => "update_bucket",
            Self::ArchiveBucket => "archive_bucket",
            Self::IssueAccessKey => "issue_access_key",
            Self::RotateAccessKey => "rotate_access_key",
            Self::RevokeAccessKey => "revoke_access_key",
        }
    }
}

impl FromStr for ObjectStorageOperationKind {
    type Err = ObjectStorageError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "create_bucket" => Ok(Self::CreateBucket),
            "update_bucket" => Ok(Self::UpdateBucket),
            "archive_bucket" => Ok(Self::ArchiveBucket),
            "issue_access_key" => Ok(Self::IssueAccessKey),
            "rotate_access_key" => Ok(Self::RotateAccessKey),
            "revoke_access_key" => Ok(Self::RevokeAccessKey),
            _ => Err(ObjectStorageError::InvalidInput),
        }
    }
}

/// One durable completed mutation, usable to reconcile uncertain results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectStorageOperation {
    /// Exact owner scope.
    pub scope: EnvironmentScope,
    /// Idempotency identity.
    pub operation_id: OperationId,
    /// Mutation kind.
    pub kind: ObjectStorageOperationKind,
    /// Bucket affected.
    pub bucket_id: BucketId,
    /// Access key affected, when applicable.
    pub access_key_id: Option<AccessKeyId>,
    /// Resulting resource revision.
    pub revision: u64,
    /// Completion time.
    pub completed_at: TimestampMicros,
}

/// Repository mutation result. It intentionally never contains plaintext secrets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectStorageOperationResult {
    /// Durable operation.
    pub operation: ObjectStorageOperation,
    /// Whether the journal supplied the result.
    pub replayed: bool,
}

/// One append-only mutation audit event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditEvent {
    /// Exact owner scope.
    pub scope: EnvironmentScope,
    /// Monotonic event identity.
    pub sequence: u64,
    /// Operation identity.
    pub operation_id: OperationId,
    /// Mutation kind.
    pub kind: ObjectStorageOperationKind,
    /// Actor recorded by the trusted transport.
    pub actor: ObjectStorageActor,
    /// Bucket affected.
    pub bucket_id: BucketId,
    /// Key affected, when applicable.
    pub access_key_id: Option<AccessKeyId>,
    /// Resulting revision.
    pub revision: u64,
    /// Completion time.
    pub occurred_at: TimestampMicros,
}

/// Bounded audit request ordered by sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuditPageRequest {
    /// Exclusive sequence cursor.
    pub after: Option<u64>,
    /// Result limit from 1 through 100.
    pub limit: u16,
}

impl AuditPageRequest {
    /// Creates a validated request.
    pub fn new(after: Option<u64>, limit: u16) -> Result<Self, ObjectStorageError> {
        let value = Self { after, limit };
        value.validate()?;
        Ok(value)
    }
    /// Validates request bounds.
    pub fn validate(self) -> Result<(), ObjectStorageError> {
        if self.limit == 0 || self.limit > MAX_PAGE_SIZE {
            Err(ObjectStorageError::LimitExceeded)
        } else {
            Ok(())
        }
    }
}

/// One stable page of audit events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditPage {
    /// Exact owner scope.
    pub scope: EnvironmentScope,
    /// Ordered events.
    pub events: Vec<AuditEvent>,
    /// Continuation cursor.
    pub next: Option<u64>,
}

/// Complete idempotent mutation intent consumed by repositories.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectStorageCommand {
    /// Create a bucket.
    CreateBucket {
        /// Server-generated bucket ID, excluded from the intent digest.
        bucket_id: BucketId,
        /// Complete configuration.
        configuration: BucketConfiguration,
        /// Trusted actor.
        actor: ObjectStorageActor,
        /// Mutation time.
        at: TimestampMicros,
    },
    /// Replace a bucket configuration.
    UpdateBucket {
        /// Bucket ID.
        bucket_id: BucketId,
        /// Required current revision.
        expected_revision: u64,
        /// Complete replacement.
        configuration: BucketConfiguration,
        /// Trusted actor.
        actor: ObjectStorageActor,
        /// Mutation time.
        at: TimestampMicros,
    },
    /// Archive a bucket.
    ArchiveBucket {
        /// Bucket ID.
        bucket_id: BucketId,
        /// Required current revision.
        expected_revision: u64,
        /// Trusted actor.
        actor: ObjectStorageActor,
        /// Mutation time.
        at: TimestampMicros,
    },
    /// Issue an access key.
    IssueAccessKey {
        /// Bucket ID.
        bucket_id: BucketId,
        /// Server-generated key ID, excluded from intent digest.
        access_key_id: AccessKeyId,
        /// Scope and label.
        configuration: AccessKeyConfiguration,
        /// Server-generated digest, excluded from intent digest.
        secret_digest: SecretDigest,
        /// Trusted actor.
        actor: ObjectStorageActor,
        /// Mutation time.
        at: TimestampMicros,
    },
    /// Rotate an access key, retaining the previous generation for a bounded overlap.
    RotateAccessKey {
        /// Bucket ID.
        bucket_id: BucketId,
        /// Key ID.
        access_key_id: AccessKeyId,
        /// Required key revision.
        expected_revision: u64,
        /// Server-generated digest, excluded from intent digest.
        secret_digest: SecretDigest,
        /// Prior generation validity cutoff.
        overlap_until: TimestampMicros,
        /// Trusted actor.
        actor: ObjectStorageActor,
        /// Mutation time.
        at: TimestampMicros,
    },
    /// Revoke every generation of an access key.
    RevokeAccessKey {
        /// Bucket ID.
        bucket_id: BucketId,
        /// Key ID.
        access_key_id: AccessKeyId,
        /// Required key revision.
        expected_revision: u64,
        /// Trusted actor.
        actor: ObjectStorageActor,
        /// Mutation time.
        at: TimestampMicros,
    },
}

impl ObjectStorageCommand {
    /// Returns the mutation kind.
    #[must_use]
    pub const fn kind(&self) -> ObjectStorageOperationKind {
        match self {
            Self::CreateBucket { .. } => ObjectStorageOperationKind::CreateBucket,
            Self::UpdateBucket { .. } => ObjectStorageOperationKind::UpdateBucket,
            Self::ArchiveBucket { .. } => ObjectStorageOperationKind::ArchiveBucket,
            Self::IssueAccessKey { .. } => ObjectStorageOperationKind::IssueAccessKey,
            Self::RotateAccessKey { .. } => ObjectStorageOperationKind::RotateAccessKey,
            Self::RevokeAccessKey { .. } => ObjectStorageOperationKind::RevokeAccessKey,
        }
    }

    /// Returns the affected bucket.
    #[must_use]
    pub const fn bucket_id(&self) -> BucketId {
        match self {
            Self::CreateBucket { bucket_id, .. }
            | Self::UpdateBucket { bucket_id, .. }
            | Self::ArchiveBucket { bucket_id, .. }
            | Self::IssueAccessKey { bucket_id, .. }
            | Self::RotateAccessKey { bucket_id, .. }
            | Self::RevokeAccessKey { bucket_id, .. } => *bucket_id,
        }
    }

    /// Returns the mutation time.
    #[must_use]
    pub const fn at(&self) -> TimestampMicros {
        match self {
            Self::CreateBucket { at, .. }
            | Self::UpdateBucket { at, .. }
            | Self::ArchiveBucket { at, .. }
            | Self::IssueAccessKey { at, .. }
            | Self::RotateAccessKey { at, .. }
            | Self::RevokeAccessKey { at, .. } => *at,
        }
    }

    /// Returns the trusted actor.
    #[must_use]
    pub fn actor(&self) -> &ObjectStorageActor {
        match self {
            Self::CreateBucket { actor, .. }
            | Self::UpdateBucket { actor, .. }
            | Self::ArchiveBucket { actor, .. }
            | Self::IssueAccessKey { actor, .. }
            | Self::RotateAccessKey { actor, .. }
            | Self::RevokeAccessKey { actor, .. } => actor,
        }
    }

    /// Returns a canonical command digest. Server-generated IDs, processing timestamps, and
    /// secret digests are excluded so transport retries preserve the same client intent.
    #[must_use]
    pub fn digest(&self, scope: EnvironmentScope) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"RUNKU_OBJECT_STORAGE_COMMAND_V1\0");
        field(&mut digest, &scope.project_id().to_string());
        field(&mut digest, &scope.environment_id().to_string());
        field(&mut digest, self.kind().as_str());
        match self {
            Self::CreateBucket {
                configuration,
                actor,
                ..
            } => {
                encode_configuration(&mut digest, configuration);
                field(&mut digest, actor.as_str());
            }
            Self::UpdateBucket {
                bucket_id,
                expected_revision,
                configuration,
                actor,
                ..
            } => {
                field(&mut digest, &bucket_id.to_string());
                unsigned(&mut digest, *expected_revision);
                encode_configuration(&mut digest, configuration);
                field(&mut digest, actor.as_str());
            }
            Self::ArchiveBucket {
                bucket_id,
                expected_revision,
                actor,
                ..
            } => {
                field(&mut digest, &bucket_id.to_string());
                unsigned(&mut digest, *expected_revision);
                field(&mut digest, actor.as_str());
            }
            Self::IssueAccessKey {
                bucket_id,
                configuration,
                actor,
                ..
            } => {
                field(&mut digest, &bucket_id.to_string());
                encode_key_configuration(&mut digest, configuration);
                field(&mut digest, actor.as_str());
            }
            Self::RotateAccessKey {
                bucket_id,
                access_key_id,
                expected_revision,
                overlap_until,
                actor,
                ..
            } => {
                field(&mut digest, &bucket_id.to_string());
                field(&mut digest, &access_key_id.to_string());
                unsigned(&mut digest, *expected_revision);
                number(&mut digest, overlap_until.get());
                field(&mut digest, actor.as_str());
            }
            Self::RevokeAccessKey {
                bucket_id,
                access_key_id,
                expected_revision,
                actor,
                ..
            } => {
                field(&mut digest, &bucket_id.to_string());
                field(&mut digest, &access_key_id.to_string());
                unsigned(&mut digest, *expected_revision);
                field(&mut digest, actor.as_str());
            }
        }
        digest.finalize().into()
    }

    /// Validates a command that was not already found in the operation journal.
    pub fn validate_new(&self) -> Result<(), ObjectStorageError> {
        if self.at().get() < 0 {
            return Err(ObjectStorageError::InvalidInput);
        }
        match self {
            Self::CreateBucket { configuration, .. } => configuration.validate(),
            Self::UpdateBucket {
                expected_revision,
                configuration,
                ..
            } => {
                if *expected_revision == 0 {
                    return Err(ObjectStorageError::InvalidInput);
                }
                configuration.validate()
            }
            Self::ArchiveBucket {
                expected_revision, ..
            }
            | Self::RevokeAccessKey {
                expected_revision, ..
            } => {
                if *expected_revision == 0 {
                    Err(ObjectStorageError::InvalidInput)
                } else {
                    Ok(())
                }
            }
            Self::IssueAccessKey { configuration, .. } => configuration.validate(),
            Self::RotateAccessKey {
                expected_revision,
                overlap_until,
                at,
                ..
            } => {
                const MAX_OVERLAP_MICROS: i64 = 86_400_000_000;
                if *expected_revision == 0
                    || overlap_until <= at
                    || overlap_until.get().saturating_sub(at.get()) > MAX_OVERLAP_MICROS
                {
                    Err(ObjectStorageError::InvalidInput)
                } else {
                    Ok(())
                }
            }
        }
    }
}

fn field(digest: &mut Sha256, value: &str) {
    digest.update(value.len().to_be_bytes());
    digest.update(value.as_bytes());
}
fn number(digest: &mut Sha256, value: i64) {
    digest.update(value.to_be_bytes());
}
fn unsigned(digest: &mut Sha256, value: u64) {
    digest.update(value.to_be_bytes());
}
fn optional_u32(digest: &mut Sha256, value: Option<u32>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn encode_configuration(digest: &mut Sha256, value: &BucketConfiguration) {
    field(digest, value.name.as_str());
    field(digest, value.policy.as_str());
    field(digest, value.versioning.as_str());
    optional_u32(digest, value.lifecycle.expire_current_after_days);
    optional_u32(digest, value.lifecycle.expire_noncurrent_after_days);
    optional_u32(digest, value.lifecycle.abort_incomplete_after_days);
    unsigned(digest, value.quota.max_object_bytes);
    unsigned(digest, value.quota.max_total_bytes);
    unsigned(digest, value.quota.max_objects);
    unsigned(digest, value.cors.len() as u64);
    for rule in &value.cors {
        unsigned(digest, rule.origins.len() as u64);
        for origin in &rule.origins {
            field(digest, origin);
        }
        unsigned(digest, rule.methods.len() as u64);
        for method in &rule.methods {
            field(digest, method.as_str());
        }
        unsigned(digest, rule.allowed_headers.len() as u64);
        for header in &rule.allowed_headers {
            field(digest, header);
        }
        unsigned(digest, rule.exposed_headers.len() as u64);
        for header in &rule.exposed_headers {
            field(digest, header);
        }
        digest.update(rule.max_age_seconds.to_be_bytes());
    }
}

fn encode_key_configuration(digest: &mut Sha256, value: &AccessKeyConfiguration) {
    field(digest, &value.label);
    field(digest, &value.prefix);
    unsigned(digest, value.operations.len() as u64);
    for operation in &value.operations {
        field(digest, operation.as_str());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use runku_core::{EnvironmentId, EnvironmentScope, ProjectId};

    use super::*;

    fn configuration() -> Result<BucketConfiguration, ObjectStorageError> {
        Ok(BucketConfiguration {
            name: "media".parse()?,
            policy: BucketPolicy::Private,
            cors: Vec::new(),
            versioning: Versioning::Disabled,
            lifecycle: BucketLifecycle::default(),
            quota: BucketQuota {
                max_object_bytes: 10,
                max_total_bytes: 100,
                max_objects: 10,
            },
        })
    }

    #[test]
    fn generated_values_do_not_change_idempotent_intent() -> Result<(), ObjectStorageError> {
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let actor: ObjectStorageActor = "operator:01".parse()?;
        let first = ObjectStorageCommand::CreateBucket {
            bucket_id: BucketId::generate(),
            configuration: configuration()?,
            actor: actor.clone(),
            at: TimestampMicros::new(9),
        };
        let second = ObjectStorageCommand::CreateBucket {
            bucket_id: BucketId::generate(),
            configuration: configuration()?,
            actor: actor.clone(),
            at: TimestampMicros::new(1),
        };
        assert_eq!(first.digest(scope), second.digest(scope));

        let key_configuration = AccessKeyConfiguration {
            label: "reader".to_owned(),
            prefix: "public/".to_owned(),
            operations: BTreeSet::from([AccessKeyOperation::Read]),
        };
        let first = ObjectStorageCommand::IssueAccessKey {
            bucket_id: BucketId::generate(),
            access_key_id: AccessKeyId::generate(),
            configuration: key_configuration.clone(),
            secret_digest: SecretDigest::new([1; 32]),
            actor: actor.clone(),
            at: TimestampMicros::new(7),
        };
        let second = ObjectStorageCommand::IssueAccessKey {
            bucket_id: first.bucket_id(),
            access_key_id: AccessKeyId::generate(),
            configuration: key_configuration,
            secret_digest: SecretDigest::new([2; 32]),
            actor,
            at: TimestampMicros::new(2),
        };
        assert_eq!(first.digest(scope), second.digest(scope));
        Ok(())
    }

    #[test]
    fn secret_debug_is_always_redacted() {
        let secret = AccessKeySecret::from_parts(AccessKeyId::generate(), &[9; 32]);
        assert_eq!(format!("{secret:?}"), "AccessKeySecret([REDACTED])");
        assert!(!format!("{secret:?}").contains(secret.expose()));
    }
}
