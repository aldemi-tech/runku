//! Stable identity errors.

use thiserror::Error;

/// Failure returned by application identity validation or persistence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum IdentityError {
    /// A client name, label, scope, state, timestamp, or combination is invalid.
    #[error("application identity input is invalid")]
    InvalidInput,
    /// A presented application key is malformed or fails verification.
    #[error("application credential is invalid")]
    InvalidCredential,
    /// A client does not exist in the exact Environment scope.
    #[error("application client was not found")]
    ClientNotFound,
    /// A credential does not exist in the exact Environment scope.
    #[error("application credential was not found")]
    CredentialNotFound,
    /// A disabled/deleted client cannot authorize new requests.
    #[error("application client is inactive")]
    ClientInactive,
    /// A revoked/deleted/expired credential cannot authorize new requests.
    #[error("application credential is inactive")]
    CredentialInactive,
    /// Credential scopes would exceed the owning client's ceiling.
    #[error("application credential scopes exceed the client ceiling")]
    ScopeEscalation,
    /// A public/confidential client was paired with the wrong key type.
    #[error("application credential type does not match the client kind")]
    CredentialTypeMismatch,
    /// The same stable identifier already names incompatible content.
    #[error("application identity record conflicts with existing content")]
    Conflict,
    /// An irreversible lifecycle transition was requested.
    #[error("application credential lifecycle transition is invalid")]
    InvalidTransition,
    /// A configured or protocol limit was exceeded.
    #[error("application identity limit exceeded")]
    LimitExceeded,
    /// The operating system CSPRNG failed.
    #[error("cryptographic entropy is unavailable")]
    EntropyUnavailable,
    /// Persistent data or migration metadata violates an invariant.
    #[error("application identity storage is corrupt")]
    Corruption,
    /// The configured repository is temporarily unavailable.
    #[error("application identity repository is unavailable")]
    Unavailable,
    /// A write may or may not have committed; blind retry could be unsafe.
    #[error("application identity write result is uncertain")]
    ResultUncertain,
    /// The selected backend cannot be used for the declared production role.
    #[error("application identity backend is unsupported for this role")]
    ProductionBackendUnsupported,
    /// The connected database version or capability is unsupported.
    #[error("application identity backend version is unsupported")]
    Unsupported,
    /// Functional principal evidence was supplied but is invalid.
    #[error("functional principal evidence is invalid")]
    InvalidPrincipal,
    /// A JWT referenced a key not present in the current immutable JWKS snapshot.
    #[error("JWT verification key is not present in the current JWKS snapshot")]
    JwksRefreshRequired,
    /// The immutable JWKS snapshot is no longer fresh enough for verification.
    #[error("JWT verification key snapshot is expired")]
    JwksSnapshotExpired,
    /// Application identity and functional evidence refer to incompatible clients.
    #[error("application identity conflicts with functional principal evidence")]
    ApplicationMismatch,
    /// An external caller attempted to address an internal Function.
    #[error("internal function is not externally invocable")]
    InternalFunctionDenied,
    /// The normalized principal does not satisfy the Function authentication policy.
    #[error("functional principal does not satisfy authentication policy")]
    PolicyDenied,
}

impl IdentityError {
    /// Stable machine-readable public/operational code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "IDENTITY_INPUT_INVALID",
            Self::InvalidCredential => "APPLICATION_CREDENTIAL_INVALID",
            Self::ClientNotFound => "APPLICATION_CLIENT_NOT_FOUND",
            Self::CredentialNotFound => "APPLICATION_CREDENTIAL_NOT_FOUND",
            Self::ClientInactive => "APPLICATION_CLIENT_INACTIVE",
            Self::CredentialInactive => "APPLICATION_CREDENTIAL_INACTIVE",
            Self::ScopeEscalation => "APPLICATION_SCOPE_ESCALATION",
            Self::CredentialTypeMismatch => "APPLICATION_CREDENTIAL_TYPE_MISMATCH",
            Self::Conflict => "APPLICATION_IDENTITY_CONFLICT",
            Self::InvalidTransition => "APPLICATION_CREDENTIAL_TRANSITION_INVALID",
            Self::LimitExceeded => "IDENTITY_LIMIT_EXCEEDED",
            Self::EntropyUnavailable => "IDENTITY_ENTROPY_UNAVAILABLE",
            Self::Corruption => "IDENTITY_STORAGE_CORRUPT",
            Self::Unavailable => "IDENTITY_STORAGE_UNAVAILABLE",
            Self::ResultUncertain => "IDENTITY_RESULT_UNCERTAIN",
            Self::ProductionBackendUnsupported => "IDENTITY_PRODUCTION_BACKEND_UNSUPPORTED",
            Self::Unsupported => "IDENTITY_BACKEND_UNSUPPORTED",
            Self::InvalidPrincipal => "PRINCIPAL_INVALID",
            Self::JwksRefreshRequired => "JWKS_REFRESH_REQUIRED",
            Self::JwksSnapshotExpired => "JWKS_SNAPSHOT_EXPIRED",
            Self::ApplicationMismatch => "APPLICATION_PRINCIPAL_MISMATCH",
            Self::InternalFunctionDenied => "FUNCTION_INTERNAL",
            Self::PolicyDenied => "AUTH_POLICY_DENIED",
        }
    }

    /// Whether retrying may succeed without changing the request.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::Unavailable
                | Self::ResultUncertain
                | Self::EntropyUnavailable
                | Self::JwksRefreshRequired
                | Self::JwksSnapshotExpired
        )
    }
}
