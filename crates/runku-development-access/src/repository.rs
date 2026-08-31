use std::fmt;

use async_trait::async_trait;
use runku_core::{DevelopmentCredentialId, EnvironmentScope};
use runku_value::TimestampMicros;

use crate::{
    DevelopmentAccessError, DevelopmentCredential, DevelopmentIdentity, DevelopmentKeyCrypto,
    DevelopmentLifecycleResult, ParsedDevelopmentKey,
};

/// Explicit physical backend for Development Access metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevelopmentAccessBackend {
    /// Embedded local `SQLite`.
    SQLite,
    /// Authoritative shared `PostgreSQL`.
    PostgreSQL,
}

/// Aggregate process-local counters without actor/workspace labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DevelopmentAccessTelemetrySnapshot {
    /// Newly created credentials.
    pub credentials_created: u64,
    /// Exact create replays.
    pub create_replays: u64,
    /// Successful key resolutions.
    pub resolutions: u64,
    /// Invalid/inactive/missing resolution attempts.
    pub resolution_failures: u64,
    /// Newly revoked credentials.
    pub credentials_revoked: u64,
    /// Newly tombstoned credentials.
    pub credentials_deleted: u64,
    /// Retryable repository failures.
    pub retryable_errors: u64,
}

/// Hot-path key verification boundary for remote development handlers.
#[async_trait]
pub trait DevelopmentAccessResolver: fmt::Debug + Send + Sync {
    /// Resolves one exact-scope bearer without degrading invalid credentials to absence.
    async fn resolve_key(
        &self,
        scope: EnvironmentScope,
        key: &ParsedDevelopmentKey,
        crypto: &DevelopmentKeyCrypto,
        now: TimestampMicros,
    ) -> Result<DevelopmentIdentity, DevelopmentAccessError>;
}

/// Durable Development Access management and verification contract.
#[async_trait]
pub trait DevelopmentAccessRepository: DevelopmentAccessResolver {
    /// Physical backend selected by composition.
    fn backend(&self) -> DevelopmentAccessBackend;

    /// Creates a credential; exact repetition is an idempotent replay.
    async fn create_credential(
        &self,
        credential: &DevelopmentCredential,
    ) -> Result<bool, DevelopmentAccessError>;

    /// Gets non-secret metadata in one exact scope, including a deleted tombstone.
    async fn get_credential(
        &self,
        scope: EnvironmentScope,
        id: DevelopmentCredentialId,
    ) -> Result<Option<DevelopmentCredential>, DevelopmentAccessError>;

    /// Lists active/revoked credentials in stable ID order and excludes deleted tombstones.
    async fn list_credentials(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Vec<DevelopmentCredential>, DevelopmentAccessError>;

    /// Irreversibly revokes one credential, idempotently.
    async fn revoke_credential(
        &self,
        scope: EnvironmentScope,
        id: DevelopmentCredentialId,
        revoked_at: TimestampMicros,
    ) -> Result<DevelopmentLifecycleResult, DevelopmentAccessError>;

    /// Tombstones one already-revoked credential, idempotently.
    async fn delete_credential(
        &self,
        scope: EnvironmentScope,
        id: DevelopmentCredentialId,
        deleted_at: TimestampMicros,
    ) -> Result<DevelopmentLifecycleResult, DevelopmentAccessError>;

    /// Returns the monotonic keyring configuration revision for one scope.
    async fn configuration_revision(
        &self,
        scope: EnvironmentScope,
    ) -> Result<u64, DevelopmentAccessError>;

    /// Performs one bounded backend health query.
    async fn health(&self) -> Result<(), DevelopmentAccessError>;

    /// Aggregate non-sensitive counters.
    fn telemetry(&self) -> DevelopmentAccessTelemetrySnapshot;

    /// Closes pooled resources.
    async fn close(&self);
}
