//! Object-safe repository contract for Application Clients and credentials.

use async_trait::async_trait;
use runku_core::{ApplicationClientId, CredentialId, EnvironmentScope};
use runku_value::TimestampMicros;

use crate::{
    ApplicationClient, ApplicationContext, ApplicationCredential, CredentialLifecycleResult,
    IdentityError, KeyringCrypto, ParsedApplicationKey,
};

/// Explicit physical backend used by the identity repository.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityRepositoryBackend {
    /// Embedded local `SQLite`.
    SQLite,
    /// Authoritative `PostgreSQL`.
    PostgreSQL,
}

/// Bounded process-local counters without per-client/key metric labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IdentityTelemetrySnapshot {
    /// Newly created clients.
    pub clients_created: u64,
    /// Newly created credentials.
    pub credentials_created: u64,
    /// Exact create replays.
    pub create_replays: u64,
    /// Successful key resolutions.
    pub resolutions: u64,
    /// Invalid, inactive, or missing key resolutions.
    pub resolution_failures: u64,
    /// Credentials newly revoked.
    pub credentials_revoked: u64,
    /// Credentials newly tombstoned.
    pub credentials_deleted: u64,
    /// Retryable repository failures.
    pub retryable_errors: u64,
}

/// Minimal hot-path application-key verifier consumed by the Auth Gateway.
#[async_trait]
pub trait ApplicationCredentialResolver: Send + Sync {
    /// Resolves and verifies a presented key without degrading failures to absence.
    async fn resolve_key(
        &self,
        scope: EnvironmentScope,
        key: &ParsedApplicationKey,
        crypto: &KeyringCrypto,
        now: TimestampMicros,
    ) -> Result<ApplicationContext, IdentityError>;
}

/// Durable management lifecycle contract in addition to hot-path resolution.
#[async_trait]
pub trait ApplicationIdentityRepository: ApplicationCredentialResolver {
    /// Physical backend selected by composition.
    fn backend(&self) -> IdentityRepositoryBackend;

    /// Creates a stable logical client; exact repetition is an idempotent replay.
    async fn create_client(&self, client: &ApplicationClient) -> Result<bool, IdentityError>;

    /// Gets one client only within its trusted Environment scope.
    async fn get_client(
        &self,
        scope: EnvironmentScope,
        id: ApplicationClientId,
    ) -> Result<Option<ApplicationClient>, IdentityError>;

    /// Lists non-secret clients in stable ID order.
    async fn list_clients(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Vec<ApplicationClient>, IdentityError>;

    /// Adds another independently revocable credential; exact repetition is a replay.
    async fn create_credential(
        &self,
        credential: &ApplicationCredential,
    ) -> Result<bool, IdentityError>;

    /// Lists credentials, including revoked records but excluding deleted tombstones.
    async fn list_credentials(
        &self,
        scope: EnvironmentScope,
        client_id: ApplicationClientId,
    ) -> Result<Vec<ApplicationCredential>, IdentityError>;

    /// Irreversibly revokes one credential, idempotently.
    async fn revoke_credential(
        &self,
        scope: EnvironmentScope,
        id: CredentialId,
        revoked_at: TimestampMicros,
    ) -> Result<CredentialLifecycleResult, IdentityError>;

    /// Tombstones one already-revoked credential, idempotently.
    async fn delete_credential(
        &self,
        scope: EnvironmentScope,
        id: CredentialId,
        deleted_at: TimestampMicros,
    ) -> Result<CredentialLifecycleResult, IdentityError>;

    /// Returns the complete identity configuration revision for one Environment.
    async fn configuration_revision(&self, scope: EnvironmentScope) -> Result<u64, IdentityError>;

    /// Performs a lightweight backend health query.
    async fn health(&self) -> Result<(), IdentityError>;

    /// Returns bounded process-local counters.
    fn telemetry(&self) -> IdentityTelemetrySnapshot;
}
