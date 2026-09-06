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
    ManagementCronActivationResult, ManagementCronActivationSet, ManagementCronCatalog,
    ManagementCronEntry, ManagementCronQuery, ManagementDataDeleteRequest, ManagementDataDocument,
    ManagementDataInsertRequest, ManagementDataPage, ManagementDataQuery,
    ManagementDataReplaceRequest, ManagementDataWriteResult, ManagementEnvironment,
    ManagementEnvironmentConfiguration, ManagementEnvironmentCreate,
    ManagementEnvironmentLifecycleChange, ManagementEnvironmentOperation,
    ManagementEnvironmentResult, ManagementEnvironmentUpdate, ManagementFunctionEntry,
    ManagementFunctionPage, ManagementHealthComponent, ManagementInstanceHealth,
    ManagementIssuedStorageAccessKey, ManagementLogArchiveStatus, ManagementLogPage,
    ManagementLogPruneRequest, ManagementLogPruneResult, ManagementLogQuery, ManagementMetric,
    ManagementMetrics, ManagementObject, ManagementObjectDownload, ManagementObjectPage,
    ManagementObjectPut, ManagementObjectResult, ManagementProduct, ManagementProductError,
    ManagementReleaseOutcome, ManagementReleaseStatus, ManagementResolvedTarget,
    ManagementScheduledInvocation, ManagementScheduledPage, ManagementSchemaIndex,
    ManagementSchemaPage, ManagementSchemaTable, ManagementServingCompatibility,
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
