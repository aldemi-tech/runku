//! Framework-independent authenticated product-management boundary.

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId, OperatorId};
use runku_protocol::WireValueV1;
use serde::{Deserialize, Serialize};

/// Public native-application OIDC settings used by `runku login --browser`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OidcClientConfiguration {
    /// Exact external issuer expected by the Management server.
    pub issuer: String,
    /// Provider authorization endpoint.
    pub authorization_endpoint: String,
    /// Provider token endpoint.
    pub token_endpoint: String,
    /// Public native client identifier; no client secret is used.
    pub client_id: String,
    /// Bounded scopes requested by the native client.
    pub scopes: Vec<String>,
    /// Optional RFC 8707 resource indicator, sent during authorization and token exchange.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
}

/// Sanitized failure returned by a product adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementProductError {
    /// The request is malformed or violates a product invariant.
    Invalid,
    /// The requested resource does not exist inside the configured scope.
    NotFound,
    /// A compare-and-set or lifecycle precondition failed.
    Conflict,
    /// An idempotency key was reused for a different logical intent.
    OperationIdReused,
    /// A logical document does not satisfy the effective schema.
    Validation,
    /// Multiple Releases cannot safely coexist under the requested policy.
    Incompatible,
    /// Durable product storage is unavailable.
    Unavailable,
    /// Durable state failed an integrity check.
    Corruption,
    /// A write may have committed but its result could not be confirmed.
    ResultUncertain,
}

impl std::fmt::Display for ManagementProductError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Invalid => "product request is invalid",
            Self::NotFound => "product resource was not found",
            Self::Conflict => "product operation conflicted",
            Self::OperationIdReused => "product operation ID was reused",
            Self::Validation => "product data validation failed",
            Self::Incompatible => "product release contracts are incompatible",
            Self::Unavailable => "product dependency is unavailable",
            Self::Corruption => "product state is corrupt",
            Self::ResultUncertain => "product operation result is uncertain",
        })
    }
}

/// One non-secret Application Client projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementApplicationClient {
    /// Stable Application Client ID.
    pub client_id: String,
    /// Operator-facing name.
    pub name: String,
    /// `public` or `confidential`.
    pub kind: String,
    /// `active` or `disabled`.
    pub status: String,
    /// Ordered maximum scopes.
    pub scopes: Vec<String>,
    /// Creation time encoded as canonical decimal microseconds.
    pub created_at_micros: String,
}

/// Bounded complete Application Client list for one Environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementApplicationClientList {
    /// Wire version.
    pub version: u8,
    /// Complete identity configuration revision.
    pub configuration_revision: u64,
    /// Stable-ID ordered clients.
    pub clients: Vec<ManagementApplicationClient>,
}

/// Create request with a caller-generated stable ID for exact retry semantics.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementApplicationClientCreate {
    /// Caller-generated canonical `app_*` ID.
    pub client_id: String,
    /// Operator-facing name.
    pub name: String,
    /// `public` or `confidential`.
    pub kind: String,
    /// Non-empty maximum scope set.
    pub scopes: Vec<String>,
    /// Caller-pinned canonical creation time in microseconds for exact replay.
    pub created_at_micros: String,
}

/// Created or exactly replayed Application Client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementCreatedApplicationClient {
    /// Wire version.
    pub version: u8,
    /// Complete identity configuration revision.
    pub configuration_revision: u64,
    /// Non-secret client metadata.
    pub client: ManagementApplicationClient,
    /// True when identical durable content already existed.
    pub replayed: bool,
}

/// One non-secret Application Credential projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementApplicationCredential {
    /// Stable credential ID.
    pub credential_id: String,
    /// Stable owning client.
    pub client_id: String,
    /// `publishable` or `secret`.
    pub kind: String,
    /// Operator-facing label.
    pub label: String,
    /// `active` or `revoked`.
    pub status: String,
    /// Ordered effective scopes.
    pub scopes: Vec<String>,
    /// Creation time encoded as canonical decimal microseconds.
    pub created_at_micros: String,
    /// Optional expiry encoded as canonical decimal microseconds.
    pub expires_at_micros: Option<String>,
    /// Optional revocation time encoded as canonical decimal microseconds.
    pub revoked_at_micros: Option<String>,
}

/// Complete non-secret credential list for one client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementApplicationCredentialList {
    /// Wire version.
    pub version: u8,
    /// Complete identity configuration revision.
    pub configuration_revision: u64,
    /// Stable-ID ordered credentials; deleted tombstones are excluded.
    pub credentials: Vec<ManagementApplicationCredential>,
}

/// Credential creation request with explicit non-retry semantics for confidential material.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementApplicationCredentialCreate {
    /// Caller-generated canonical `crd_*` ID.
    pub credential_id: String,
    /// Operator-facing label.
    pub label: String,
    /// Non-empty effective scope set.
    pub scopes: Vec<String>,
    /// Optional expiry encoded as canonical decimal microseconds.
    pub expires_at_micros: Option<String>,
    /// Caller-pinned canonical creation time in microseconds.
    pub created_at_micros: String,
}

/// Credential rotation request; the source remains unchanged.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementApplicationCredentialRotate {
    /// Caller-generated replacement `crd_*` ID.
    pub replacement_credential_id: String,
    /// Operator-facing replacement label.
    pub label: String,
    /// Optional replacement expiry encoded as canonical decimal microseconds.
    pub expires_at_micros: Option<String>,
    /// Caller-pinned canonical replacement creation time in microseconds.
    pub created_at_micros: String,
}

/// Newly created material. Secret keys appear only on the successful creation response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementCreatedApplicationCredential {
    /// Complete identity configuration revision after creation.
    pub configuration_revision: u64,
    /// Non-secret durable metadata.
    pub credential: ManagementApplicationCredential,
    /// External publishable or confidential key material.
    pub key: String,
    /// True only for deterministically re-derivable publishable keys.
    pub recoverable: bool,
    /// True only when confidential material is being shown for the first and only time.
    pub secret_shown_once: bool,
}

/// Idempotent irreversible credential lifecycle result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementApplicationCredentialLifecycle {
    /// Complete identity configuration revision.
    pub configuration_revision: u64,
    /// Operated credential ID.
    pub credential_id: String,
    /// `revoked` or `deleted`.
    pub status: String,
    /// True when the target state was already durable.
    pub replayed: bool,
}

/// One immutable Release weight in an Environment serving policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementServingRelease {
    /// Canonical Release ID.
    pub release_id: String,
    /// Positive integer percentage.
    pub weight_percent: u8,
}

/// Complete desired serving-policy replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementServingPolicySet {
    /// `null` requires absence; a positive value performs compare-and-set.
    pub expected_revision: Option<u64>,
    /// `atomic` or `gradual`.
    pub mode: String,
    /// Complete weighted Release set.
    pub releases: Vec<ManagementServingRelease>,
    /// Caller-pinned canonical decimal timestamp for exact operation replay.
    pub changed_at_micros: String,
}

/// Desired/observed serving policy projected without provider details.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementServingPolicy {
    /// Wire version.
    pub version: u8,
    /// Positive desired-policy revision.
    pub policy_revision: u64,
    /// `atomic` or `gradual`.
    pub mode: String,
    /// Canonically Release-ID-ordered weighted set.
    pub releases: Vec<ManagementServingRelease>,
    /// `pending`, `ready`, or `failed`.
    pub observed_state: String,
    /// Exact observed revision, when one exists.
    pub observed_policy_revision: Option<u64>,
    /// Whether the exact desired policy is serving-path ready.
    pub converged: bool,
    /// Canonical decimal creation timestamp.
    pub created_at_micros: String,
    /// Canonical decimal desired-update timestamp.
    pub updated_at_micros: String,
    /// Canonical decimal observation timestamp.
    pub observed_at_micros: Option<String>,
}

/// Result of one idempotent serving-policy replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementServingPolicyResult {
    /// Durable policy after the command.
    pub policy: ManagementServingPolicy,
    /// Correlated operation ID.
    pub operation_id: String,
    /// Whether exact durable operation content was replayed.
    pub replayed: bool,
}

/// Compatibility evidence shared by every Release in one desired serving set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementServingCompatibility {
    /// Wire version.
    pub version: u8,
    /// Positive desired serving-policy revision.
    pub policy_revision: u64,
    /// True because incompatible desired sets are rejected before persistence.
    pub compatible: bool,
    /// Whether the exact desired revision is observed on the serving path.
    pub converged: bool,
    /// Shared canonical schema contract hash.
    pub schema_contract_hash: String,
    /// Shared canonical logical-index contract hash.
    pub index_contract_hash: String,
    /// Shared canonical ordered Cron-declaration hash.
    pub cron_declarations_hash: String,
    /// Canonical Release-ID-ordered weighted set.
    pub releases: Vec<ManagementServingRelease>,
    /// Stable blocker codes; empty for every persisted v1 policy.
    pub diagnostics: Vec<String>,
}

/// Durable serving operation projection used after an uncertain response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementServingOperation {
    /// Correlated operation ID.
    pub operation_id: String,
    /// `setDesired` or `materialize`.
    pub kind: String,
    /// Policy revision produced or observed.
    pub policy_revision: u64,
    /// `pending`, `ready`, or `failed`.
    pub observed_state: String,
    /// Exact observed revision, when one exists.
    pub observed_policy_revision: Option<u64>,
    /// Canonical decimal completion timestamp.
    pub completed_at_micros: String,
}

/// Complete portable Product Environment configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementEnvironmentConfiguration {
    /// Human-readable display name.
    pub name: String,
    /// Project-unique DNS-label slug.
    pub slug: String,
    /// Logical region, never a provider placement identifier.
    pub region: String,
    /// `development`, `preview`, `staging`, or `production`.
    pub purpose: String,
    /// `open`, `protected`, or `production`.
    pub protection: String,
    /// `local`, `managed`, or `selfHosted`.
    pub location: String,
    /// Whether Workspace targets are allowed by Product policy.
    pub workspace_targets_enabled: bool,
}

/// Initial Environment creation request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementEnvironmentCreate {
    /// Complete initial configuration.
    pub configuration: ManagementEnvironmentConfiguration,
    /// Caller-pinned canonical decimal creation timestamp for exact replay.
    pub created_at_micros: String,
}

/// Complete Environment configuration replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementEnvironmentUpdate {
    /// Required positive current configuration revision.
    pub expected_revision: u64,
    /// Complete replacement configuration.
    pub configuration: ManagementEnvironmentConfiguration,
    /// Caller-pinned canonical decimal update timestamp for exact replay.
    pub updated_at_micros: String,
}

/// One exact portable Product Environment projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementEnvironment {
    /// Wire version.
    pub version: u8,
    /// Exact Project ID.
    pub project_id: String,
    /// Exact Environment ID.
    pub environment_id: String,
    /// Complete desired configuration.
    pub configuration: ManagementEnvironmentConfiguration,
    /// Positive desired-configuration revision.
    pub configuration_revision: u64,
    /// `active` or `archived`.
    pub desired_state: String,
    /// `pending`, `ready`, or `failed`.
    pub observed_state: String,
    /// Exact observed configuration revision, when materialized.
    pub observed_configuration_revision: Option<u64>,
    /// Whether the exact desired revision is observed ready.
    pub converged: bool,
    /// Canonical decimal creation timestamp.
    pub created_at_micros: String,
    /// Canonical decimal desired-update timestamp.
    pub updated_at_micros: String,
    /// Canonical decimal materializer observation timestamp.
    pub observed_at_micros: Option<String>,
}

/// Result of one idempotent Environment mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementEnvironmentResult {
    /// Durable Environment after the command.
    pub environment: ManagementEnvironment,
    /// Correlated operation ID.
    pub operation_id: String,
    /// Whether exact durable operation content was replayed.
    pub replayed: bool,
}

/// One fixed-name aggregate Product metric without tenant-controlled labels.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementMetric {
    /// Stable dotted metric name.
    pub name: String,
    /// Canonical decimal unsigned value safe for arbitrary-precision consumers.
    pub value: String,
    /// Stable unit such as `count`, `bytes`, or `entries`.
    pub unit: String,
}

/// Bounded aggregate metrics for one exact Product Environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementMetrics {
    /// Wire version.
    pub version: u8,
    /// Fixed-name metric set ordered by name.
    pub metrics: Vec<ManagementMetric>,
}

/// One non-secret Product instance health component.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementHealthComponent {
    /// Stable component name.
    pub name: String,
    /// `ready`, `idle`, or `unavailable`.
    pub status: String,
}

/// Sanitized health projection for the Product instance serving one Environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementInstanceHealth {
    /// Wire version.
    pub version: u8,
    /// Product-owned opaque instance identifier.
    pub instance_id: String,
    /// `ready` or `unavailable`.
    pub status: String,
    /// Fixed component set ordered by name.
    pub components: Vec<ManagementHealthComponent>,
}

/// Non-secret Environment operation used to reconcile an uncertain result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementEnvironmentOperation {
    /// Correlated operation ID.
    pub operation_id: String,
    /// `create`, `update`, or `materialize`.
    pub kind: String,
    /// Desired configuration revision produced or observed.
    pub configuration_revision: u64,
    /// `active` or `archived`.
    pub desired_state: String,
    /// `pending`, `ready`, or `failed`.
    pub observed_state: String,
    /// Exact observed configuration revision, when present.
    pub observed_configuration_revision: Option<u64>,
    /// Canonical decimal completion timestamp.
    pub completed_at_micros: String,
}

/// One bounded logical bucket CORS rule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketCorsRule {
    /// Exact HTTPS origins, or only `*`.
    pub origins: Vec<String>,
    /// Canonical HTTP methods.
    pub methods: Vec<String>,
    /// Canonical lower-case request headers, or only `*`.
    pub allowed_headers: Vec<String>,
    /// Canonical lower-case exposed headers.
    pub exposed_headers: Vec<String>,
    /// Browser preflight cache bound.
    pub max_age_seconds: u32,
}

/// Logical bucket lifecycle configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketLifecycle {
    /// Expiry for current objects.
    pub expire_current_after_days: Option<u32>,
    /// Expiry for non-current versions.
    pub expire_noncurrent_after_days: Option<u32>,
    /// Multipart-abort deadline.
    pub abort_incomplete_after_days: Option<u32>,
}

/// Logical bucket quota encoded losslessly for JavaScript clients.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketQuota {
    /// Maximum bytes in one object.
    pub max_object_bytes: String,
    /// Maximum logical current bytes.
    pub max_total_bytes: String,
    /// Maximum current object count.
    pub max_objects: String,
}

/// Complete replace-only logical bucket configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketConfiguration {
    /// Environment-unique DNS-label name.
    pub name: String,
    /// `private` or `publicRead`.
    pub policy: String,
    /// Complete CORS rules.
    pub cors: Vec<ManagementBucketCorsRule>,
    /// `disabled` or `enabled`.
    pub versioning: String,
    /// Complete lifecycle configuration.
    pub lifecycle: ManagementBucketLifecycle,
    /// Complete quota.
    pub quota: ManagementBucketQuota,
}

/// One non-provider logical bucket projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucket {
    /// Stable logical bucket ID.
    pub bucket_id: String,
    /// Complete configuration.
    pub configuration: ManagementBucketConfiguration,
    /// Positive CAS revision.
    pub revision: u64,
    /// `active` or `archived`.
    pub state: String,
    /// Canonical decimal creation time.
    pub created_at_micros: String,
    /// Canonical decimal update time.
    pub updated_at_micros: String,
}

/// Stable bounded bucket page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketPage {
    /// Wire version.
    pub version: u8,
    /// Ordered buckets.
    pub buckets: Vec<ManagementBucket>,
    /// Exclusive next cursor.
    pub next: Option<String>,
}

/// Bucket creation request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketCreate {
    /// Complete initial configuration.
    pub configuration: ManagementBucketConfiguration,
    /// Caller-pinned canonical decimal operation time.
    pub at_micros: String,
}

/// Complete bucket replacement request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketUpdate {
    /// Required positive current revision.
    pub expected_revision: u64,
    /// Complete replacement configuration.
    pub configuration: ManagementBucketConfiguration,
    /// Caller-pinned canonical decimal operation time.
    pub at_micros: String,
}

/// Irreversible bucket archive request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketArchive {
    /// Required positive current revision.
    pub expected_revision: u64,
    /// Caller-pinned canonical decimal operation time.
    pub at_micros: String,
}

/// Product Object Storage access-key scope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementStorageAccessKeyConfiguration {
    /// Human-facing label.
    pub label: String,
    /// Object-key prefix; empty means the whole bucket.
    pub prefix: String,
    /// Non-empty subset of `list`, `read`, `write`, and `delete`.
    pub operations: Vec<String>,
}

/// Non-secret Product Object Storage access-key metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementStorageAccessKey {
    /// Stable key ID.
    pub access_key_id: String,
    /// Owning bucket ID.
    pub bucket_id: String,
    /// Immutable scope and label.
    pub configuration: ManagementStorageAccessKeyConfiguration,
    /// Positive CAS revision.
    pub revision: u64,
    /// `active` or `revoked`.
    pub state: String,
    /// Canonical decimal creation time.
    pub created_at_micros: String,
    /// Canonical decimal update time.
    pub updated_at_micros: String,
    /// Prior-generation validity cutoff during rotation overlap.
    pub previous_generation_valid_until_micros: Option<String>,
}

/// Bounded non-secret access-key page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementStorageAccessKeyPage {
    /// Wire version.
    pub version: u8,
    /// Ordered key metadata.
    pub keys: Vec<ManagementStorageAccessKey>,
    /// Exclusive next cursor.
    pub next: Option<String>,
}

/// Access-key issuance request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementStorageAccessKeyIssue {
    /// Immutable scope and label.
    pub configuration: ManagementStorageAccessKeyConfiguration,
    /// Caller-pinned canonical decimal operation time.
    pub at_micros: String,
}

/// Access-key rotation request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementStorageAccessKeyRotate {
    /// Required positive current revision.
    pub expected_revision: u64,
    /// Prior generation validity cutoff, no more than 24 hours after `atMicros`.
    pub overlap_until_micros: String,
    /// Caller-pinned canonical decimal operation time.
    pub at_micros: String,
}

/// Access-key revoke request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementStorageAccessKeyRevoke {
    /// Required positive current revision.
    pub expected_revision: u64,
    /// Caller-pinned canonical decimal operation time.
    pub at_micros: String,
}

/// One-time Product Object Storage key issuance/rotation result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementIssuedStorageAccessKey {
    /// Durable non-secret metadata.
    pub key: ManagementStorageAccessKey,
    /// One-time `rk_st_*` secret; absent on replay.
    pub secret: Option<String>,
    /// Correlated operation ID.
    pub operation_id: String,
    /// Whether exact durable operation content was replayed.
    pub replayed: bool,
}

/// Bucket mutation result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementBucketResult {
    /// Durable bucket after the command.
    pub bucket: ManagementBucket,
    /// Correlated operation ID.
    pub operation_id: String,
    /// Whether exact durable operation content was replayed.
    pub replayed: bool,
}

/// Non-secret storage operation used to reconcile uncertain results.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementStorageOperation {
    /// Correlated operation ID.
    pub operation_id: String,
    /// Stable operation kind.
    pub kind: String,
    /// Affected bucket ID.
    pub bucket_id: String,
    /// Affected access-key ID, when applicable.
    pub access_key_id: Option<String>,
    /// Resulting resource revision.
    pub revision: u64,
    /// Canonical decimal completion time.
    pub completed_at_micros: String,
}

/// Query selecting one exact code target for the Cron catalog.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementCronQuery {
    /// Explicit `release:`, `channel:`, or `workspace:` target.
    pub target: String,
}

/// One code-owned Cron declaration with its current activation projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementCronEntry {
    /// Stable logical Cron name.
    pub name: String,
    /// Canonical UTC schedule.
    pub schedule: String,
    /// Internal Mutation or Action destination.
    pub function: String,
    /// Canonical arguments copied into each tick.
    pub args: WireValueV1,
    /// Whether this exact declaration is active.
    pub enabled: bool,
    /// Activation repository revision, when enabled.
    pub activation_revision: Option<u64>,
    /// Immutable active code pin, when enabled.
    pub active_pinned_code: Option<String>,
    /// Next logical tick, when enabled.
    pub next_tick_micros: Option<String>,
}

/// Complete bounded Cron catalog for one resolved target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementCronCatalog {
    /// Wire version.
    pub version: u8,
    /// Exact resolved artifact metadata.
    pub target: ManagementResolvedTarget,
    /// Current activation repository revision.
    pub activation_revision: u64,
    /// Declarations in canonical name order.
    pub crons: Vec<ManagementCronEntry>,
}

/// Per-declaration Cron activation replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementCronActivationSet {
    /// Exact code target containing the immutable declaration.
    pub target: String,
    /// Required current activation repository revision.
    pub expected_revision: u64,
    /// Desired enabled state.
    pub enabled: bool,
    /// Caller-pinned canonical decimal change timestamp for exact replay.
    pub changed_at_micros: String,
}

/// Result of one idempotent Cron activation command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementCronActivationResult {
    /// Correlated operation ID.
    pub operation_id: String,
    /// Repository revision produced by the original command.
    pub repository_revision: u64,
    /// Number of enabled declarations after the command.
    pub active_definitions: u32,
    /// Whether the immutable prior result was replayed.
    pub replayed: bool,
}

/// One non-secret Scheduled Invocation projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementScheduledInvocation {
    /// Stable Scheduled Invocation ID.
    pub scheduled_invocation_id: String,
    /// Exact immutable code pin.
    pub pinned_code: String,
    /// Destination Mutation or Action.
    pub function: String,
    /// Canonical persisted arguments.
    pub args: WireValueV1,
    /// Next eligible execution time.
    pub execute_at_micros: String,
    /// `pending`, `running`, `succeeded`, `failed`, or `cancelled`.
    pub status: String,
    /// Number of durable claims.
    pub attempts: u32,
    /// Last bounded error code, when present.
    pub last_error_code: Option<String>,
    /// Commit sequence that created the record.
    pub commit_sequence: String,
}

/// Stable bounded Scheduled Invocation page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementScheduledPage {
    /// Wire version.
    pub version: u8,
    /// Scheduled Invocations ordered by stable ID.
    pub scheduled: Vec<ManagementScheduledInvocation>,
    /// Exclusive continuation cursor, absent at the end.
    pub next: Option<String>,
}

impl std::error::Error for ManagementProductError {}

/// Bounded catalog page query resolved against one explicit code target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementCatalogQuery {
    /// Explicit `release:`, `channel:`, or `workspace:` target.
    pub target: String,
    /// Exclusive stable logical cursor.
    pub after: Option<String>,
    /// Page size in `1..=200`.
    pub limit: u16,
}

/// Metadata proving which immutable artifact supplied a catalog or Data Admin operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementResolvedTarget {
    /// Target supplied by the operator.
    pub requested: String,
    /// Immutable `release:` or `dev_revision:` pin selected once.
    pub resolved: String,
    /// Release manifest identity containing the effective schema and Functions.
    pub release_id: String,
    /// Repository revision used to resolve a moving target.
    pub serving_revision: u64,
    /// Exact logical schema contract digest.
    pub schema_contract_hash: String,
}

/// One Function projected from an integrity-checked effective artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementFunctionEntry {
    /// Stable Function identity.
    pub function_id: String,
    /// Logical Function name.
    pub name: String,
    /// `query`, `mutation`, or `action`.
    pub kind: String,
    /// `public` or `internal`.
    pub visibility: String,
    /// Functional principal policy.
    pub auth: String,
    /// `safe-v8` or `full-node`.
    pub runtime: String,
    /// Ordered declared Function capabilities.
    pub capabilities: Vec<String>,
    /// Canonical arguments Contract v1.
    pub arguments: serde_json::Value,
    /// Canonical result Contract v1.
    pub result: serde_json::Value,
}

/// One bounded Function catalog page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementFunctionPage {
    /// Wire response version.
    pub version: u8,
    /// Exact resolved target metadata.
    pub target: ManagementResolvedTarget,
    /// Function entries ordered by logical name.
    pub functions: Vec<ManagementFunctionEntry>,
    /// Exclusive cursor for another page, absent at end.
    pub next: Option<String>,
}

/// One logical index belonging to a schema table.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementSchemaIndex {
    /// Stable logical Index identity.
    pub index_id: String,
    /// Logical Index name.
    pub name: String,
    /// Ordered object-property paths.
    pub fields: Vec<Vec<String>>,
}

/// One table projected from the effective logical schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementSchemaTable {
    /// Stable logical Table identity.
    pub table_id: String,
    /// Logical Table name.
    pub name: String,
    /// Canonical document Contract v1.
    pub document: serde_json::Value,
    /// Ordered logical indexes for this table.
    pub indexes: Vec<ManagementSchemaIndex>,
}

/// One bounded effective-schema page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementSchemaPage {
    /// Wire response version.
    pub version: u8,
    /// Exact resolved target metadata.
    pub target: ManagementResolvedTarget,
    /// Tables ordered by stable identity.
    pub tables: Vec<ManagementSchemaTable>,
    /// Exclusive Table ID cursor for another page, absent at end.
    pub next: Option<String>,
}

/// One logical document projected without physical storage details.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementDataDocument {
    /// Stable logical Table identity.
    pub table_id: String,
    /// Logical Table name resolved from the effective schema.
    pub table: String,
    /// Opaque Document identity.
    pub document_id: String,
    /// Positive OCC revision encoded as decimal text.
    pub revision: String,
    /// Environment commit sequence encoded as decimal text.
    pub commit_sequence: String,
    /// Creation timestamp in microseconds encoded as decimal text.
    pub created_at_micros: String,
    /// Last-update timestamp in microseconds encoded as decimal text.
    pub updated_at_micros: String,
    /// Lossless Canonical Value v1 projection.
    pub value: WireValueV1,
}

/// Bounded logical-index query used by the Data Explorer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementDataQuery {
    /// Explicit code target supplying the trusted schema.
    pub target: String,
    /// Logical table name.
    pub table: String,
    /// Logical index name within the table.
    pub index: String,
    /// Optional leading compound-key components; empty scans the complete logical index.
    #[serde(default)]
    pub prefix: Vec<WireValueV1>,
    /// Result bound in `1..=200`.
    pub limit: u16,
}

/// Result of one bounded logical-index query.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementDataPage {
    /// Wire response version.
    pub version: u8,
    /// Exact resolved target metadata.
    pub target: ManagementResolvedTarget,
    /// Snapshot sequence shared by every returned document.
    pub snapshot_sequence: String,
    /// Documents ordered by logical index key and Document ID.
    pub documents: Vec<ManagementDataDocument>,
    /// True when the bounded scan may have additional entries.
    pub truncated: bool,
}

/// Insert request; the Document ID is derived deterministically from `Idempotency-Key`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementDataInsertRequest {
    /// Explicit code target supplying the trusted schema.
    pub target: String,
    /// Complete logical document value.
    pub value: WireValueV1,
}

/// OCC replace request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementDataReplaceRequest {
    /// Explicit code target supplying the trusted schema.
    pub target: String,
    /// Exact current positive revision encoded as decimal text.
    pub expected_revision: String,
    /// Complete value observed at `expectedRevision`, used to derive trusted old index entries and
    /// to make an exact retry reconstruct the original commit batch.
    pub previous_value: WireValueV1,
    /// Complete replacement document value.
    pub value: WireValueV1,
}

/// OCC delete request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementDataDeleteRequest {
    /// Explicit code target supplying the trusted schema.
    pub target: String,
    /// Exact current positive revision encoded as decimal text.
    pub expected_revision: String,
    /// Complete value observed at `expectedRevision`, required for index removal and exact replay.
    pub previous_value: WireValueV1,
}

/// Known result of an idempotent Data Admin write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementDataWriteResult {
    /// Wire response version.
    pub version: u8,
    /// Exact resolved target metadata.
    pub target: ManagementResolvedTarget,
    /// Affected logical Table identity.
    pub table_id: String,
    /// Affected Document identity.
    pub document_id: String,
    /// New revision, absent after delete.
    pub revision: Option<String>,
    /// Environment commit sequence encoded as decimal text.
    pub commit_sequence: String,
    /// True when recovered from the operation journal.
    pub replayed: bool,
}

/// Result of an authenticated Workspace publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementWorkspacePublish {
    /// Immutable Release created or replayed.
    pub release_id: String,
    /// Immutable development revision selected as Workspace HEAD.
    pub revision_id: String,
    /// Whether the exact result already existed.
    pub replayed: bool,
}

/// Result of release validation or a Channel movement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementReleaseOutcome {
    /// Immutable Release operated on.
    pub release_id: String,
    /// Channel moved, when applicable.
    pub channel: Option<String>,
    /// Final lifecycle status.
    pub status: String,
    /// Durable serving revision.
    pub serving_revision: u64,
    /// Whether the requested final state already existed.
    pub replayed: bool,
    /// Stable compatibility blocker codes.
    pub diagnostics: Vec<String>,
}

/// Coherent release and Channel snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementReleaseStatus {
    /// Durable serving revision.
    pub serving_revision: u64,
    /// Default Channel, if configured.
    pub default_channel: Option<String>,
    /// Safe release entries encoded for the stable CLI contract.
    pub releases: Vec<serde_json::Value>,
    /// Safe Channel entries encoded for the stable CLI contract.
    pub channels: Vec<serde_json::Value>,
}

/// Bounded exact-scope operational-log query.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementLogQuery {
    /// Exclusive durable cursor.
    pub after: String,
    /// Page size in `1..=1000`.
    pub limit: u16,
    /// Optional stream filter.
    pub stream: Option<String>,
    /// Optional minimum severity.
    pub level: Option<String>,
    /// Optional exact Function ID.
    pub function_id: Option<String>,
    /// Optional exact Request ID.
    pub request_id: Option<String>,
    /// Optional exact Invocation ID.
    pub invocation_id: Option<String>,
    /// Optional exact Application Client ID.
    pub client_id: Option<String>,
    /// Optional exact credential ID.
    pub credential_id: Option<String>,
    /// Optional exact Release ID.
    pub release_id: Option<String>,
}

/// One ordered log page; records are already sanitized stable JSON objects.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementLogPage {
    /// Ordered records.
    pub records: Vec<serde_json::Value>,
    /// Continuation cursor.
    pub next: String,
}

/// Verified immutable Operational Log archive coverage for one exact Environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementLogArchiveStatus {
    /// Total committed Parquet bytes.
    pub parquet_bytes: u64,
    /// Total committed records.
    pub records: u64,
    /// Total committed immutable segments.
    pub segments: u32,
    /// Highest contiguous committed cursor.
    pub through: String,
}

/// Bounded Operational Log retention request for one authenticated Environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementLogPruneRequest {
    /// Delete only records strictly older than this timestamp.
    pub before_micros: i64,
    /// Maximum rows inspected or deleted in one transaction.
    pub maximum: u32,
    /// False performs a dry run; true deletes archive-covered hot rows.
    pub apply: bool,
    /// Exact Environment confirmation required when applying.
    pub environment_id: Option<String>,
}

/// Result of one bounded Operational Log retention request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagementLogPruneResult {
    /// Whether deletion was requested and applied.
    pub applied: bool,
    /// Rows deleted; always zero for a dry run.
    pub deleted: u32,
    /// Exact Environment operated on.
    pub environment_id: String,
    /// Rows matched by this bounded call.
    pub matched: u32,
    /// Whether another bounded call may match more rows.
    pub more: bool,
}

/// Product operations exposed behind Platform Identity.
#[async_trait]
pub trait ManagementProduct: std::fmt::Debug + Send + Sync {
    /// Exact Environment owned by this adapter.
    fn scope(&self) -> EnvironmentScope;

    /// Checks the authoritative Product dependencies required by this adapter.
    ///
    /// The default preserves compatibility for adapters whose construction already proves
    /// readiness. Networked persistence adapters override it so `/health/ready` reflects a live
    /// dependency failure rather than only Platform Identity health.
    async fn health(&self) -> Result<(), ManagementProductError> {
        Ok(())
    }

    /// Reads bounded aggregate operational metrics for this exact Environment.
    async fn metrics(&self) -> Result<ManagementMetrics, ManagementProductError> {
        Err(ManagementProductError::NotFound)
    }

    /// Reads sanitized component health for this Product instance.
    async fn instance_health(&self) -> Result<ManagementInstanceHealth, ManagementProductError> {
        Err(ManagementProductError::NotFound)
    }

    /// Gets the exact Product Environment owned by this adapter.
    async fn environment(&self) -> Result<ManagementEnvironment, ManagementProductError> {
        Err(ManagementProductError::NotFound)
    }

    /// Creates the exact Product Environment idempotently.
    async fn environment_create(
        &self,
        operation_id: OperationId,
        request: &ManagementEnvironmentCreate,
    ) -> Result<ManagementEnvironmentResult, ManagementProductError> {
        let _ = (operation_id, request);
        Err(ManagementProductError::NotFound)
    }

    /// Replaces the exact Product Environment configuration using CAS.
    async fn environment_update(
        &self,
        operation_id: OperationId,
        request: &ManagementEnvironmentUpdate,
    ) -> Result<ManagementEnvironmentResult, ManagementProductError> {
        let _ = (operation_id, request);
        Err(ManagementProductError::NotFound)
    }

    /// Looks up one exact-scope Environment operation after uncertainty.
    async fn environment_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<ManagementEnvironmentOperation, ManagementProductError> {
        let _ = operation_id;
        Err(ManagementProductError::NotFound)
    }

    /// Lists non-secret Application Clients.
    async fn application_clients(
        &self,
    ) -> Result<ManagementApplicationClientList, ManagementProductError> {
        Err(ManagementProductError::NotFound)
    }

    /// Creates or exactly replays one Application Client.
    async fn application_client_create(
        &self,
        request: &ManagementApplicationClientCreate,
    ) -> Result<ManagementCreatedApplicationClient, ManagementProductError> {
        let _ = request;
        Err(ManagementProductError::NotFound)
    }

    /// Lists non-secret credentials for one exact client.
    async fn application_credentials(
        &self,
        client_id: &str,
    ) -> Result<ManagementApplicationCredentialList, ManagementProductError> {
        let _ = client_id;
        Err(ManagementProductError::NotFound)
    }

    /// Creates one independently revocable Application Credential.
    async fn application_credential_create(
        &self,
        client_id: &str,
        request: &ManagementApplicationCredentialCreate,
    ) -> Result<ManagementCreatedApplicationCredential, ManagementProductError> {
        let _ = (client_id, request);
        Err(ManagementProductError::NotFound)
    }

    /// Re-derives one non-secret publishable key after checking its durable digest.
    async fn application_credential_reveal(
        &self,
        client_id: &str,
        credential_id: &str,
    ) -> Result<ManagementCreatedApplicationCredential, ManagementProductError> {
        let _ = (client_id, credential_id);
        Err(ManagementProductError::NotFound)
    }

    /// Creates one replacement with the source credential's exact scopes.
    async fn application_credential_rotate(
        &self,
        client_id: &str,
        credential_id: &str,
        request: &ManagementApplicationCredentialRotate,
    ) -> Result<ManagementCreatedApplicationCredential, ManagementProductError> {
        let _ = (client_id, credential_id, request);
        Err(ManagementProductError::NotFound)
    }

    /// Irreversibly revokes one credential idempotently.
    async fn application_credential_revoke(
        &self,
        client_id: &str,
        credential_id: &str,
        revoked_at_micros: i64,
    ) -> Result<ManagementApplicationCredentialLifecycle, ManagementProductError> {
        let _ = (client_id, credential_id, revoked_at_micros);
        Err(ManagementProductError::NotFound)
    }

    /// Tombstones one already-revoked credential idempotently.
    async fn application_credential_delete(
        &self,
        client_id: &str,
        credential_id: &str,
        deleted_at_micros: i64,
    ) -> Result<ManagementApplicationCredentialLifecycle, ManagementProductError> {
        let _ = (client_id, credential_id, deleted_at_micros);
        Err(ManagementProductError::NotFound)
    }

    /// Reads the exact Environment desired/observed serving policy.
    async fn serving_policy(&self) -> Result<ManagementServingPolicy, ManagementProductError> {
        Err(ManagementProductError::NotFound)
    }

    /// Reads the canonical compatibility evidence for the desired serving set.
    async fn serving_compatibility(
        &self,
    ) -> Result<ManagementServingCompatibility, ManagementProductError> {
        Err(ManagementProductError::NotFound)
    }

    /// Creates or replaces the complete serving policy using CAS and idempotency.
    async fn serving_policy_set(
        &self,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementServingPolicySet,
    ) -> Result<ManagementServingPolicyResult, ManagementProductError> {
        let _ = (operation_id, actor, request);
        Err(ManagementProductError::NotFound)
    }

    /// Looks up an exact-scope serving operation after an uncertain result.
    async fn serving_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<ManagementServingOperation, ManagementProductError> {
        let _ = operation_id;
        Err(ManagementProductError::NotFound)
    }

    /// Lists one bounded stable logical bucket page.
    async fn buckets(
        &self,
        after: Option<&str>,
        limit: u16,
    ) -> Result<ManagementBucketPage, ManagementProductError> {
        let _ = (after, limit);
        Err(ManagementProductError::NotFound)
    }

    /// Gets one exact logical bucket.
    async fn bucket(&self, bucket_id: &str) -> Result<ManagementBucket, ManagementProductError> {
        let _ = bucket_id;
        Err(ManagementProductError::NotFound)
    }

    /// Creates one logical bucket idempotently.
    async fn bucket_create(
        &self,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementBucketCreate,
    ) -> Result<ManagementBucketResult, ManagementProductError> {
        let _ = (operation_id, actor, request);
        Err(ManagementProductError::NotFound)
    }

    /// Replaces one complete logical bucket configuration with CAS.
    async fn bucket_update(
        &self,
        bucket_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementBucketUpdate,
    ) -> Result<ManagementBucketResult, ManagementProductError> {
        let _ = (bucket_id, operation_id, actor, request);
        Err(ManagementProductError::NotFound)
    }

    /// Irreversibly archives one logical bucket with CAS.
    async fn bucket_archive(
        &self,
        bucket_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementBucketArchive,
    ) -> Result<ManagementBucketResult, ManagementProductError> {
        let _ = (bucket_id, operation_id, actor, request);
        Err(ManagementProductError::NotFound)
    }

    /// Lists one bounded non-secret Product storage-key page.
    async fn storage_access_keys(
        &self,
        bucket_id: &str,
        after: Option<&str>,
        limit: u16,
    ) -> Result<ManagementStorageAccessKeyPage, ManagementProductError> {
        let _ = (bucket_id, after, limit);
        Err(ManagementProductError::NotFound)
    }

    /// Issues one Product storage key with a one-time secret.
    async fn storage_access_key_issue(
        &self,
        bucket_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementStorageAccessKeyIssue,
    ) -> Result<ManagementIssuedStorageAccessKey, ManagementProductError> {
        let _ = (bucket_id, operation_id, actor, request);
        Err(ManagementProductError::NotFound)
    }

    /// Rotates one Product storage key with bounded generation overlap.
    async fn storage_access_key_rotate(
        &self,
        bucket_id: &str,
        access_key_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementStorageAccessKeyRotate,
    ) -> Result<ManagementIssuedStorageAccessKey, ManagementProductError> {
        let _ = (bucket_id, access_key_id, operation_id, actor, request);
        Err(ManagementProductError::NotFound)
    }

    /// Revokes every generation of one Product storage key with CAS.
    async fn storage_access_key_revoke(
        &self,
        bucket_id: &str,
        access_key_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementStorageAccessKeyRevoke,
    ) -> Result<ManagementStorageOperation, ManagementProductError> {
        let _ = (bucket_id, access_key_id, operation_id, actor, request);
        Err(ManagementProductError::NotFound)
    }

    /// Looks up one exact-scope storage operation after uncertainty.
    async fn storage_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<ManagementStorageOperation, ManagementProductError> {
        let _ = operation_id;
        Err(ManagementProductError::NotFound)
    }

    /// Reads code-owned Cron declarations and current activation state.
    async fn crons(
        &self,
        query: &ManagementCronQuery,
    ) -> Result<ManagementCronCatalog, ManagementProductError> {
        let _ = query;
        Err(ManagementProductError::NotFound)
    }

    /// Enables or disables one exact code-owned Cron declaration using CAS.
    async fn cron_activation_set(
        &self,
        name: &str,
        operation_id: OperationId,
        request: &ManagementCronActivationSet,
    ) -> Result<ManagementCronActivationResult, ManagementProductError> {
        let _ = (name, operation_id, request);
        Err(ManagementProductError::NotFound)
    }

    /// Looks up one successful Cron activation command after uncertainty.
    async fn cron_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<ManagementCronActivationResult, ManagementProductError> {
        let _ = operation_id;
        Err(ManagementProductError::NotFound)
    }

    /// Lists one bounded stable Scheduled Invocation page.
    async fn scheduled(
        &self,
        after: Option<&str>,
        limit: u16,
    ) -> Result<ManagementScheduledPage, ManagementProductError> {
        let _ = (after, limit);
        Err(ManagementProductError::NotFound)
    }

    /// Publishes one canonical package request.
    async fn publish(
        &self,
        actor: &str,
        request: &[u8],
    ) -> Result<ManagementWorkspacePublish, ManagementProductError>;

    /// Validates one candidate Release.
    async fn release(
        &self,
        release_id: &str,
        against: Option<&str>,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError>;

    /// Moves one Channel with an optional exact precondition.
    async fn promote(
        &self,
        channel: &str,
        release_id: &str,
        expected: Option<Option<&str>>,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError>;

    /// Rolls one Channel back with an exact current-Release precondition.
    async fn rollback(
        &self,
        channel: &str,
        expected: &str,
        target: &str,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError>;

    /// Reads one coherent release and Channel snapshot.
    async fn status(&self) -> Result<ManagementReleaseStatus, ManagementProductError>;

    /// Lists Functions from one exact effective artifact.
    async fn functions(
        &self,
        query: &ManagementCatalogQuery,
    ) -> Result<ManagementFunctionPage, ManagementProductError> {
        let _ = query;
        Err(ManagementProductError::NotFound)
    }

    /// Lists tables and indexes from one exact effective artifact.
    async fn schema_tables(
        &self,
        query: &ManagementCatalogQuery,
    ) -> Result<ManagementSchemaPage, ManagementProductError> {
        let _ = query;
        Err(ManagementProductError::NotFound)
    }

    /// Gets one logical document using the exact target schema.
    async fn data_get(
        &self,
        target: &str,
        table: &str,
        document_id: &str,
    ) -> Result<ManagementDataDocument, ManagementProductError> {
        let _ = (target, table, document_id);
        Err(ManagementProductError::NotFound)
    }

    /// Runs one bounded logical-index query.
    async fn data_query(
        &self,
        request: &ManagementDataQuery,
    ) -> Result<ManagementDataPage, ManagementProductError> {
        let _ = request;
        Err(ManagementProductError::NotFound)
    }

    /// Inserts one schema-validated logical document idempotently.
    async fn data_insert(
        &self,
        operation_id: OperationId,
        table: &str,
        request: &ManagementDataInsertRequest,
    ) -> Result<ManagementDataWriteResult, ManagementProductError> {
        let _ = (operation_id, table, request);
        Err(ManagementProductError::NotFound)
    }

    /// Replaces one schema-validated logical document with exact OCC.
    async fn data_replace(
        &self,
        operation_id: OperationId,
        table: &str,
        document_id: &str,
        request: &ManagementDataReplaceRequest,
    ) -> Result<ManagementDataWriteResult, ManagementProductError> {
        let _ = (operation_id, table, document_id, request);
        Err(ManagementProductError::NotFound)
    }

    /// Deletes one logical document with exact OCC.
    async fn data_delete(
        &self,
        operation_id: OperationId,
        table: &str,
        document_id: &str,
        request: &ManagementDataDeleteRequest,
    ) -> Result<ManagementDataWriteResult, ManagementProductError> {
        let _ = (operation_id, table, document_id, request);
        Err(ManagementProductError::NotFound)
    }

    /// Reads one exact-scope operational-log page.
    async fn logs(
        &self,
        query: &ManagementLogQuery,
    ) -> Result<ManagementLogPage, ManagementProductError>;

    /// Verifies immutable archive coverage for this exact Environment.
    async fn log_archive_status(
        &self,
    ) -> Result<ManagementLogArchiveStatus, ManagementProductError>;

    /// Dry-runs or applies archive-bounded hot-log retention.
    async fn log_prune(
        &self,
        request: &ManagementLogPruneRequest,
    ) -> Result<ManagementLogPruneResult, ManagementProductError>;
}
