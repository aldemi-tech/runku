//! Framework-independent authenticated product-management boundary.

use async_trait::async_trait;
use runku_core::EnvironmentScope;
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
    /// Durable product storage is unavailable.
    Unavailable,
    /// Durable state failed an integrity check.
    Corruption,
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

/// Product operations exposed behind Platform Identity.
#[async_trait]
pub trait ManagementProduct: std::fmt::Debug + Send + Sync {
    /// Exact Environment owned by this adapter.
    fn scope(&self) -> EnvironmentScope;

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

    /// Reads one exact-scope operational-log page.
    async fn logs(
        &self,
        query: &ManagementLogQuery,
    ) -> Result<ManagementLogPage, ManagementProductError>;
}
