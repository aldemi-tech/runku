//! Authenticated, bounded HTTP boundary for the self-hosted Runku Management API.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod oidc;
mod product;
mod router;

pub use oidc::JwtExternalIdentityAuthenticator;
pub use product::{
    ManagementApplicationClient, ManagementApplicationClientCreate,
    ManagementApplicationClientList, ManagementApplicationCredential,
    ManagementApplicationCredentialCreate, ManagementApplicationCredentialLifecycle,
    ManagementApplicationCredentialList, ManagementApplicationCredentialRotate, ManagementBucket,
    ManagementBucketArchive, ManagementBucketConfiguration, ManagementBucketCorsRule,
    ManagementBucketCreate, ManagementBucketLifecycle, ManagementBucketPage, ManagementBucketQuota,
    ManagementBucketResult, ManagementBucketUpdate, ManagementCatalogQuery,
    ManagementCreatedApplicationClient, ManagementCreatedApplicationCredential,
    ManagementDataDeleteRequest, ManagementDataDocument, ManagementDataInsertRequest,
    ManagementDataPage, ManagementDataQuery, ManagementDataReplaceRequest,
    ManagementDataWriteResult, ManagementEnvironment, ManagementEnvironmentConfiguration,
    ManagementEnvironmentCreate, ManagementEnvironmentOperation, ManagementEnvironmentResult,
    ManagementEnvironmentUpdate, ManagementFunctionEntry, ManagementFunctionPage,
    ManagementIssuedStorageAccessKey, ManagementLogArchiveStatus, ManagementLogPage,
    ManagementLogPruneRequest, ManagementLogPruneResult, ManagementLogQuery, ManagementProduct,
    ManagementProductError, ManagementReleaseOutcome, ManagementReleaseStatus,
    ManagementResolvedTarget, ManagementSchemaIndex, ManagementSchemaPage, ManagementSchemaTable,
    ManagementServingOperation, ManagementServingPolicy, ManagementServingPolicyResult,
    ManagementServingPolicySet, ManagementServingRelease, ManagementStorageAccessKey,
    ManagementStorageAccessKeyConfiguration, ManagementStorageAccessKeyIssue,
    ManagementStorageAccessKeyPage, ManagementStorageAccessKeyRevoke,
    ManagementStorageAccessKeyRotate, ManagementStorageOperation, ManagementWorkspacePublish,
    OidcClientConfiguration,
};
pub use router::{
    ExternalIdentityAuthenticator, ManagedEnrollmentKey, ManagementHttpConfig,
    ManagementHttpExposure, build_management_router, build_management_router_with_product,
    serve_management,
};
