//! Platform identity for a self-hosted Runku installation.
//!
//! This boundary authenticates human operators. Application keys (`rk_pub`/`rk_sec`) and
//! Development Access keys (`rk_dev`) remain separate identities with separate authority.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod key;
mod model;
mod repository;
mod service;
mod sql;

pub use error::PlatformIdentityError;
pub use key::{
    AccessToken, GeneratedAccessToken, GeneratedInvitationCode, GeneratedRefreshToken,
    InvitationCode, PlatformDigest, PlatformIdentityCrypto, RefreshToken,
};
pub use model::{
    AccessScope, DeviceName, ExternalOperatorIdentity, InvitationKind, InvitationStatus, Operator,
    OperatorContext, OperatorGrant, OperatorInvitation, OperatorName, OperatorRole,
    OperatorSession, OperatorStatus, PlatformCapability, SessionStatus,
};
pub use repository::{
    BootstrapCreate, ConsumedInvitation, IdempotentInvitationCreate, NewInvitation,
    NewOperatorSession, PlatformIdentityBackend, PlatformIdentityRepository,
    PlatformIdentityTelemetrySnapshot, RefreshedSession,
};
pub use service::{
    BootstrapResult, IdempotentInvitationResult, LoginResult, PlatformIdentityService,
    SessionTokenPolicy,
};
pub use sql::{
    PlatformIdentityRepositoryConfig, PlatformIdentityRepositoryRole, SqlPlatformIdentityRepository,
};

pub use runku_core::{OperatorId, OperatorInvitationId, OperatorSessionId};
