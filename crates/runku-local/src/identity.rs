//! Safe local management surface for Application Clients and replaceable keys.

use std::{collections::BTreeSet, path::Path};

use runku_core::{ApplicationClientId, CredentialId};
use runku_identity::{
    ApplicationClient, ApplicationClientName, ApplicationClientStatus, ApplicationContext,
    ApplicationCredential, ApplicationCredentialResolver, ApplicationIdentityRepository,
    ApplicationKey, ApplicationScope, ClientKind, CredentialKind, CredentialLabel,
    CredentialLifecycleResult, CredentialStatus, IdentityError, KeyringCrypto,
    ParsedApplicationKey,
};
use runku_identity_repository::{IdentityRepositoryConfig, SqlApplicationIdentityRepository};
use runku_value::TimestampMicros;
use thiserror::Error;

use crate::state::{LocalProjectState, load_identity_pepper, sqlite_url};
use crate::{LocalPaths, LocalStateError, load_local};

/// Stable failure for local Application Client/key management.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalIdentityError {
    /// The initialized local state, path, or pepper is invalid.
    #[error("local identity state is invalid")]
    InvalidState,
    /// A name, scope, label, timestamp, type, or lifecycle request is invalid.
    #[error("local identity input is invalid")]
    InvalidInput,
    /// The requested client or credential does not exist in this Environment.
    #[error("local identity record was not found")]
    NotFound,
    /// Existing durable content conflicts with the requested operation.
    #[error("local identity operation conflicts with durable state")]
    Conflict,
    /// The repository or filesystem is temporarily unavailable.
    #[error("local identity storage is unavailable")]
    Unavailable,
    /// A write outcome is unknown and must be inspected before retrying.
    #[error("local identity write result is uncertain")]
    ResultUncertain,
    /// Durable identity state violates a trusted invariant.
    #[error("local identity storage is corrupt")]
    Corruption,
    /// Secure credential material could not be generated.
    #[error("local identity entropy is unavailable")]
    EntropyUnavailable,
}

impl LocalIdentityError {
    /// Stable machine-readable CLI/operational code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidState => "LOCAL_IDENTITY_STATE_INVALID",
            Self::InvalidInput => "LOCAL_IDENTITY_INPUT_INVALID",
            Self::NotFound => "LOCAL_IDENTITY_NOT_FOUND",
            Self::Conflict => "LOCAL_IDENTITY_CONFLICT",
            Self::Unavailable => "LOCAL_IDENTITY_UNAVAILABLE",
            Self::ResultUncertain => "LOCAL_IDENTITY_RESULT_UNCERTAIN",
            Self::Corruption => "LOCAL_IDENTITY_CORRUPT",
            Self::EntropyUnavailable => "LOCAL_IDENTITY_ENTROPY_UNAVAILABLE",
        }
    }

    /// Whether retrying after external recovery may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::Unavailable | Self::ResultUncertain | Self::EntropyUnavailable
        )
    }
}

/// Newly persisted credential plus its external material for deliberate one-time delivery.
pub struct LocalCreatedCredential {
    /// Non-secret durable metadata.
    pub credential: LocalCredentialMetadata,
    /// Publishable material or a one-time service bearer secret.
    pub key: ApplicationKey,
}

/// Credential metadata safe for administration, logs, and JSON listings.
///
/// The full bearer and its keyed digest are intentionally absent from this type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalCredentialMetadata {
    /// Stable independently revocable identifier.
    pub id: CredentialId,
    /// Owning logical Application Client.
    pub client_id: ApplicationClientId,
    /// Publishable declaration or confidential service secret.
    pub kind: CredentialKind,
    /// Operator-facing deployment/instance label.
    pub label: CredentialLabel,
    /// Current irreversible lifecycle state.
    pub status: CredentialStatus,
    /// Effective least-privilege scopes.
    pub scopes: BTreeSet<ApplicationScope>,
    /// Creation timestamp.
    pub created_at: TimestampMicros,
    /// Optional absolute expiry.
    pub expires_at: Option<TimestampMicros>,
    /// Optional irreversible revocation timestamp.
    pub revoked_at: Option<TimestampMicros>,
}

impl From<&ApplicationCredential> for LocalCredentialMetadata {
    fn from(credential: &ApplicationCredential) -> Self {
        Self {
            id: credential.id,
            client_id: credential.client_id,
            kind: credential.kind,
            label: credential.label.clone(),
            status: credential.status,
            scopes: credential.scopes.clone(),
            created_at: credential.created_at,
            expires_at: credential.expires_at,
            revoked_at: credential.revoked_at,
        }
    }
}

impl std::fmt::Debug for LocalCreatedCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalCreatedCredential")
            .field("credential", &self.credential)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// Local management composition sharing the exact repository and pepper used by `runku dev`.
pub struct LocalIdentityManager {
    state: LocalProjectState,
    repository: SqlApplicationIdentityRepository,
    crypto: KeyringCrypto,
}

impl std::fmt::Debug for LocalIdentityManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalIdentityManager")
            .field("state", &self.state)
            .field("repository", &self.repository)
            .field("crypto", &"[REDACTED]")
            .finish()
    }
}

impl LocalIdentityManager {
    /// Opens an initialized project without creating or repairing identity state.
    ///
    /// # Errors
    ///
    /// Rejects missing, symlinked, malformed, corrupt, or unavailable local state.
    pub async fn open(root: &Path) -> Result<Self, LocalIdentityError> {
        let (state, paths) = load_local(root).await.map_err(map_state)?;
        validate_identity_database(&paths).await?;
        let pepper = load_identity_pepper(&paths).await.map_err(map_state)?;
        let repository = SqlApplicationIdentityRepository::connect_sqlite(
            &sqlite_url(&paths.identity_database),
            IdentityRepositoryConfig::LOCAL,
        )
        .await
        .map_err(map_identity)?;
        repository.health().await.map_err(map_identity)?;
        Ok(Self {
            state,
            repository,
            crypto: KeyringCrypto::new(pepper),
        })
    }

    /// Returns the trusted local Project/Environment state.
    #[must_use]
    pub const fn state(&self) -> &LocalProjectState {
        &self.state
    }

    /// Verifies one complete Application Key against this exact local Environment.
    ///
    /// # Errors
    ///
    /// Rejects malformed, foreign, inactive, expired, or digest-mismatched credentials without
    /// degrading them to absence.
    pub async fn resolve_key(
        &self,
        key: &str,
        now: TimestampMicros,
    ) -> Result<ApplicationContext, LocalIdentityError> {
        let parsed = key.parse::<ParsedApplicationKey>().map_err(map_identity)?;
        self.repository
            .resolve_key(self.state.scope(), &parsed, &self.crypto, now)
            .await
            .map_err(map_identity)
    }

    /// Creates one logical caller with an explicit stable identifier.
    ///
    /// # Errors
    ///
    /// Rejects invalid/duplicate data, repository failures, or conflicting content.
    pub async fn create_client(
        &self,
        id: ApplicationClientId,
        name: ApplicationClientName,
        kind: ClientKind,
        scope_ceiling: BTreeSet<ApplicationScope>,
        created_at: TimestampMicros,
    ) -> Result<ApplicationClient, LocalIdentityError> {
        let client = ApplicationClient {
            scope: self.state.scope(),
            id,
            name,
            kind,
            status: ApplicationClientStatus::Active,
            scope_ceiling,
            created_at,
        };
        if !self
            .repository
            .create_client(&client)
            .await
            .map_err(map_identity)?
        {
            return Ok(client);
        }
        Ok(client)
    }

    /// Lists every logical caller in stable identifier order.
    ///
    /// # Errors
    ///
    /// Returns repository availability/corruption failures without partial results.
    pub async fn list_clients(&self) -> Result<Vec<ApplicationClient>, LocalIdentityError> {
        self.repository
            .list_clients(self.state.scope())
            .await
            .map_err(map_identity)
    }

    /// Creates one independently revocable credential under an existing client.
    ///
    /// # Errors
    ///
    /// Rejects mismatched client/key kinds, scope escalation, invalid expiry, or persistence failure.
    pub async fn create_credential(
        &self,
        id: CredentialId,
        client_id: ApplicationClientId,
        label: CredentialLabel,
        scopes: BTreeSet<ApplicationScope>,
        created_at: TimestampMicros,
        expires_at: Option<TimestampMicros>,
    ) -> Result<LocalCreatedCredential, LocalIdentityError> {
        let client = self
            .repository
            .get_client(self.state.scope(), client_id)
            .await
            .map_err(map_identity)?
            .ok_or(LocalIdentityError::NotFound)?;
        let generated = match client.kind {
            ClientKind::Public => self.crypto.generate_publishable(id),
            ClientKind::Confidential => self.crypto.generate_secret(id),
        }
        .map_err(map_identity)?;
        let credential = ApplicationCredential {
            scope: self.state.scope(),
            id,
            client_id,
            kind: generated.kind,
            label,
            status: CredentialStatus::Active,
            digest: generated.digest,
            scopes,
            created_at,
            expires_at,
            revoked_at: None,
            deleted_at: None,
        };
        self.repository
            .create_credential(&credential)
            .await
            .map_err(map_identity)?;
        Ok(LocalCreatedCredential {
            credential: LocalCredentialMetadata::from(&credential),
            key: generated.key,
        })
    }

    /// Lists non-secret credential metadata, including revoked entries and excluding tombstones.
    ///
    /// # Errors
    ///
    /// Returns not-found, availability, or corruption failures without exposing key digests.
    pub async fn list_credentials(
        &self,
        client_id: ApplicationClientId,
    ) -> Result<Vec<LocalCredentialMetadata>, LocalIdentityError> {
        self.repository
            .list_credentials(self.state.scope(), client_id)
            .await
            .map(|credentials| {
                credentials
                    .iter()
                    .map(LocalCredentialMetadata::from)
                    .collect()
            })
            .map_err(map_identity)
    }

    /// Re-derives one publishable key and checks it against the durable digest before disclosure.
    ///
    /// # Errors
    ///
    /// Secret, missing, deleted, or corrupt credentials fail without returning any material.
    pub async fn reveal_publishable(
        &self,
        client_id: ApplicationClientId,
        credential_id: CredentialId,
    ) -> Result<LocalCreatedCredential, LocalIdentityError> {
        let credential = self.find_credential(client_id, credential_id).await?;
        if credential.kind != CredentialKind::Publishable {
            return Err(LocalIdentityError::InvalidInput);
        }
        let generated = self
            .crypto
            .generate_publishable(credential_id)
            .map_err(map_identity)?;
        if generated.kind != credential.kind
            || !self.crypto.verify(&generated.key, credential.digest)
        {
            return Err(LocalIdentityError::Corruption);
        }
        Ok(LocalCreatedCredential {
            credential: LocalCredentialMetadata::from(&credential),
            key: generated.key,
        })
    }

    /// Creates a replacement key with the exact scopes of an existing non-deleted key.
    ///
    /// The source key remains unchanged and active when it was active.
    ///
    /// # Errors
    ///
    /// Rejects missing keys, invalid metadata, scope drift, or persistence failure.
    pub async fn rotate_credential(
        &self,
        client_id: ApplicationClientId,
        source_id: CredentialId,
        replacement_id: CredentialId,
        label: CredentialLabel,
        created_at: TimestampMicros,
        expires_at: Option<TimestampMicros>,
    ) -> Result<LocalCreatedCredential, LocalIdentityError> {
        if source_id == replacement_id {
            return Err(LocalIdentityError::InvalidInput);
        }
        let source = self.find_credential(client_id, source_id).await?;
        self.create_credential(
            replacement_id,
            client_id,
            label,
            source.scopes,
            created_at,
            expires_at,
        )
        .await
    }

    /// Irreversibly revokes one key and returns whether this invocation changed state.
    ///
    /// # Errors
    ///
    /// Returns not-found, invalid transition, or persistence failures.
    pub async fn revoke_credential(
        &self,
        id: CredentialId,
        revoked_at: TimestampMicros,
    ) -> Result<CredentialLifecycleResult, LocalIdentityError> {
        self.repository
            .revoke_credential(self.state.scope(), id, revoked_at)
            .await
            .map_err(map_identity)
    }

    /// Tombstones one already-revoked key and returns whether this invocation changed state.
    ///
    /// # Errors
    ///
    /// Active keys, missing keys, and persistence failures are rejected.
    pub async fn delete_credential(
        &self,
        id: CredentialId,
        deleted_at: TimestampMicros,
    ) -> Result<CredentialLifecycleResult, LocalIdentityError> {
        self.repository
            .delete_credential(self.state.scope(), id, deleted_at)
            .await
            .map_err(map_identity)
    }

    /// Returns the monotonic identity configuration revision for cache invalidation.
    ///
    /// # Errors
    ///
    /// Returns availability or corruption failures.
    pub async fn configuration_revision(&self) -> Result<u64, LocalIdentityError> {
        self.repository
            .configuration_revision(self.state.scope())
            .await
            .map_err(map_identity)
    }

    async fn find_credential(
        &self,
        client_id: ApplicationClientId,
        credential_id: CredentialId,
    ) -> Result<ApplicationCredential, LocalIdentityError> {
        self.repository
            .list_credentials(self.state.scope(), client_id)
            .await
            .map_err(map_identity)?
            .into_iter()
            .find(|credential| credential.id == credential_id)
            .ok_or(LocalIdentityError::NotFound)
    }
}

async fn validate_identity_database(paths: &LocalPaths) -> Result<(), LocalIdentityError> {
    let metadata = tokio::fs::symlink_metadata(&paths.identity_database)
        .await
        .map_err(|_| LocalIdentityError::InvalidState)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
        return Err(LocalIdentityError::InvalidState);
    }
    Ok(())
}

const fn map_state(error: LocalStateError) -> LocalIdentityError {
    match error {
        LocalStateError::InvalidPath | LocalStateError::InvalidState => {
            LocalIdentityError::InvalidState
        }
        LocalStateError::Conflict => LocalIdentityError::Conflict,
        LocalStateError::Unavailable => LocalIdentityError::Unavailable,
        LocalStateError::Corruption => LocalIdentityError::Corruption,
    }
}

const fn map_identity(error: IdentityError) -> LocalIdentityError {
    match error {
        IdentityError::ClientNotFound | IdentityError::CredentialNotFound => {
            LocalIdentityError::NotFound
        }
        IdentityError::Conflict => LocalIdentityError::Conflict,
        IdentityError::Unavailable => LocalIdentityError::Unavailable,
        IdentityError::ResultUncertain => LocalIdentityError::ResultUncertain,
        IdentityError::EntropyUnavailable => LocalIdentityError::EntropyUnavailable,
        IdentityError::Corruption => LocalIdentityError::Corruption,
        IdentityError::InvalidInput
        | IdentityError::InvalidCredential
        | IdentityError::ClientInactive
        | IdentityError::CredentialInactive
        | IdentityError::ScopeEscalation
        | IdentityError::CredentialTypeMismatch
        | IdentityError::InvalidTransition
        | IdentityError::LimitExceeded
        | IdentityError::ProductionBackendUnsupported
        | IdentityError::Unsupported
        | IdentityError::InvalidPrincipal
        | IdentityError::JwksRefreshRequired
        | IdentityError::JwksSnapshotExpired
        | IdentityError::ApplicationMismatch
        | IdentityError::InternalFunctionDenied
        | IdentityError::PolicyDenied => LocalIdentityError::InvalidInput,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, net::SocketAddr, str::FromStr};

    use runku_core::{ApplicationClientId, CredentialId, WorkspaceRef};
    use runku_identity::{
        ApplicationClientName, ApplicationScope, ClientKind, CredentialLabel,
        CredentialLifecycleResult, CredentialStatus,
    };
    use runku_value::TimestampMicros;
    use tempfile::tempdir;

    use super::{LocalIdentityError, LocalIdentityManager};
    use crate::initialize_local;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn scopes(values: &[&str]) -> Result<BTreeSet<ApplicationScope>, Box<dyn std::error::Error>> {
        values
            .iter()
            .map(|value| ApplicationScope::from_str(value).map_err(Into::into))
            .collect()
    }

    async fn initialized(
        root: &std::path::Path,
    ) -> Result<LocalIdentityManager, Box<dyn std::error::Error>> {
        initialize_local(
            root,
            WorkspaceRef::from_str("default")?,
            SocketAddr::from(([127, 0, 0, 1], 3210)),
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?;
        Ok(LocalIdentityManager::open(root).await?)
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn complete_multi_key_rotation_revoke_delete_and_reopen() -> TestResult {
        let directory = tempdir()?;
        let manager = initialized(directory.path()).await?;
        let public_id = ApplicationClientId::generate();
        let service_id = ApplicationClientId::generate();
        let all_scopes = scopes(&["documents:read", "documents:write"])?;
        manager
            .create_client(
                public_id,
                ApplicationClientName::from_str("web-storefront")?,
                ClientKind::Public,
                all_scopes.clone(),
                TimestampMicros::new(1_800_000_000_000_010),
            )
            .await?;
        manager
            .create_client(
                service_id,
                ApplicationClientName::from_str("billing-worker")?,
                ClientKind::Confidential,
                all_scopes.clone(),
                TimestampMicros::new(1_800_000_000_000_011),
            )
            .await?;

        let first_id = CredentialId::generate();
        let first = manager
            .create_credential(
                first_id,
                public_id,
                CredentialLabel::from_str("web-primary")?,
                scopes(&["documents:read"])?,
                TimestampMicros::new(1_800_000_000_000_020),
                None,
            )
            .await?;
        let first_material = first.key.expose().to_owned();
        assert!(!format!("{first:?}").contains(&first_material));
        let second_id = CredentialId::generate();
        manager
            .create_credential(
                second_id,
                public_id,
                CredentialLabel::from_str("web-preview")?,
                scopes(&["documents:read"])?,
                TimestampMicros::new(1_800_000_000_000_021),
                None,
            )
            .await?;
        assert_eq!(
            manager
                .reveal_publishable(public_id, first_id)
                .await?
                .key
                .expose(),
            first_material
        );

        let secret_id = CredentialId::generate();
        let secret = manager
            .create_credential(
                secret_id,
                service_id,
                CredentialLabel::from_str("billing-blue")?,
                scopes(&["documents:read", "documents:write"])?,
                TimestampMicros::new(1_800_000_000_000_022),
                None,
            )
            .await?;
        let secret_material = secret.key.expose().to_owned();
        assert!(secret_material.starts_with("rk_sec_v1_"));
        assert!(!format!("{secret:?}").contains(&secret_material));
        assert!(matches!(
            manager.reveal_publishable(service_id, secret_id).await,
            Err(LocalIdentityError::InvalidInput)
        ));

        let replacement_id = CredentialId::generate();
        let replacement = manager
            .rotate_credential(
                public_id,
                first_id,
                replacement_id,
                CredentialLabel::from_str("web-rotated")?,
                TimestampMicros::new(1_800_000_000_000_030),
                Some(TimestampMicros::new(1_900_000_000_000_000)),
            )
            .await?;
        assert_eq!(replacement.credential.scopes, first.credential.scopes);
        assert_eq!(
            manager
                .list_credentials(public_id)
                .await?
                .iter()
                .filter(|item| item.status == CredentialStatus::Active)
                .count(),
            3
        );
        assert_eq!(
            manager
                .revoke_credential(first_id, TimestampMicros::new(1_800_000_000_000_040))
                .await?,
            CredentialLifecycleResult::Changed
        );
        assert_eq!(
            manager
                .revoke_credential(first_id, TimestampMicros::new(1_800_000_000_000_041))
                .await?,
            CredentialLifecycleResult::Replayed
        );
        assert_eq!(
            manager
                .delete_credential(second_id, TimestampMicros::new(1_800_000_000_000_042))
                .await,
            Err(LocalIdentityError::InvalidInput)
        );
        assert_eq!(
            manager
                .delete_credential(first_id, TimestampMicros::new(1_800_000_000_000_043))
                .await?,
            CredentialLifecycleResult::Changed
        );
        assert!(
            manager
                .list_credentials(public_id)
                .await?
                .iter()
                .all(|item| item.id != first_id)
        );
        assert_eq!(manager.configuration_revision().await?, 8);

        drop(manager);
        let reopened = LocalIdentityManager::open(directory.path()).await?;
        assert_eq!(reopened.list_clients().await?.len(), 2);
        assert_eq!(reopened.list_credentials(public_id).await?.len(), 2);
        assert!(matches!(
            reopened.reveal_publishable(public_id, first_id).await,
            Err(LocalIdentityError::NotFound)
        ));
        assert!(matches!(
            reopened.reveal_publishable(service_id, secret_id).await,
            Err(LocalIdentityError::InvalidInput)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn invalid_scope_expiry_ids_and_cross_client_access_fail_closed() -> TestResult {
        let directory = tempdir()?;
        let manager = initialized(directory.path()).await?;
        let client_id = ApplicationClientId::generate();
        manager
            .create_client(
                client_id,
                ApplicationClientName::from_str("web")?,
                ClientKind::Public,
                scopes(&["read"])?,
                TimestampMicros::new(100),
            )
            .await?;
        assert!(matches!(
            manager
                .create_credential(
                    CredentialId::generate(),
                    client_id,
                    CredentialLabel::from_str("escalated")?,
                    scopes(&["write"])?,
                    TimestampMicros::new(200),
                    None,
                )
                .await,
            Err(LocalIdentityError::InvalidInput)
        ));
        assert!(matches!(
            manager
                .create_credential(
                    CredentialId::generate(),
                    client_id,
                    CredentialLabel::from_str("expired")?,
                    scopes(&["read"])?,
                    TimestampMicros::new(200),
                    Some(TimestampMicros::new(200)),
                )
                .await,
            Err(LocalIdentityError::InvalidInput)
        ));
        assert!(matches!(
            manager
                .list_credentials(ApplicationClientId::generate())
                .await,
            Err(LocalIdentityError::NotFound)
        ));
        let id = CredentialId::generate();
        manager
            .create_credential(
                id,
                client_id,
                CredentialLabel::from_str("valid")?,
                scopes(&["read"])?,
                TimestampMicros::new(201),
                None,
            )
            .await?;
        assert!(matches!(
            manager
                .rotate_credential(
                    client_id,
                    id,
                    id,
                    CredentialLabel::from_str("same")?,
                    TimestampMicros::new(202),
                    None,
                )
                .await,
            Err(LocalIdentityError::InvalidInput)
        ));
        assert_eq!(manager.configuration_revision().await?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_managers_add_independent_keys_without_lost_updates() -> TestResult {
        let directory = tempdir()?;
        let first_manager = initialized(directory.path()).await?;
        let client_id = ApplicationClientId::generate();
        first_manager
            .create_client(
                client_id,
                ApplicationClientName::from_str("parallel-workers")?,
                ClientKind::Confidential,
                scopes(&["events:write"])?,
                TimestampMicros::new(100),
            )
            .await?;
        let second_manager = LocalIdentityManager::open(directory.path()).await?;
        let first = first_manager.create_credential(
            CredentialId::generate(),
            client_id,
            CredentialLabel::from_str("worker-a")?,
            scopes(&["events:write"])?,
            TimestampMicros::new(200),
            None,
        );
        let second = second_manager.create_credential(
            CredentialId::generate(),
            client_id,
            CredentialLabel::from_str("worker-b")?,
            scopes(&["events:write"])?,
            TimestampMicros::new(201),
            None,
        );
        let (first, second) = tokio::join!(first, second);
        assert!(first?.key.expose().starts_with("rk_sec_v1_"));
        assert!(second?.key.expose().starts_with("rk_sec_v1_"));
        assert_eq!(first_manager.list_credentials(client_id).await?.len(), 2);
        assert_eq!(first_manager.configuration_revision().await?, 3);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_identity_database_is_rejected_before_open() -> TestResult {
        use std::os::unix::fs::symlink;

        let directory = tempdir()?;
        let manager = initialized(directory.path()).await?;
        drop(manager);
        let database = directory.path().join(".runku/identity.sqlite3");
        let relocated = directory.path().join("identity-real.sqlite3");
        std::fs::rename(&database, &relocated)?;
        symlink(&relocated, &database)?;
        assert!(matches!(
            LocalIdentityManager::open(directory.path()).await,
            Err(LocalIdentityError::InvalidState)
        ));
        Ok(())
    }
}
