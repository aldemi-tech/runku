//! Framework-independent authenticated product-management boundary.

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId};
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
