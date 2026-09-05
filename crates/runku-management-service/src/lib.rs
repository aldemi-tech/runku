//! Authenticated, bounded HTTP boundary for the self-hosted Runku Management API.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod oidc;
mod product;
mod router;

pub use oidc::JwtExternalIdentityAuthenticator;
pub use product::{
    ManagementCatalogQuery, ManagementDataDeleteRequest, ManagementDataDocument,
    ManagementDataInsertRequest, ManagementDataPage, ManagementDataQuery,
    ManagementDataReplaceRequest, ManagementDataWriteResult, ManagementFunctionEntry,
    ManagementFunctionPage, ManagementLogArchiveStatus, ManagementLogPage,
    ManagementLogPruneRequest, ManagementLogPruneResult, ManagementLogQuery, ManagementProduct,
    ManagementProductError, ManagementReleaseOutcome, ManagementReleaseStatus,
    ManagementResolvedTarget, ManagementSchemaIndex, ManagementSchemaPage, ManagementSchemaTable,
    ManagementWorkspacePublish, OidcClientConfiguration,
};
pub use router::{
    ExternalIdentityAuthenticator, ManagedEnrollmentKey, ManagementHttpConfig,
    ManagementHttpExposure, build_management_router, build_management_router_with_product,
    serve_management,
};
