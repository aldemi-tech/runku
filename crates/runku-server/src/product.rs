//! Product Base adapter behind the authenticated Management API.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use runku_contracts::{Contract, DocumentSchemaV1, decode_contract, decode_document_schema};
use runku_core::{
    ApplicationClientId, ChannelName, CodeTarget, CredentialId, DocumentId, EnvironmentScope,
    FunctionName, OperationId, OperatorId, OutboxEventId, ReleaseId, TableId,
};
use runku_data::{
    CommitBatch, DocumentMutation, ExpectedRevision, IndexRange, LogicalStore, OutboxAppend,
    StoreError,
};
use runku_data_postgres::{PostgresStore, PostgresStoreConfig};
use runku_data_sqlite::{SqliteStore, SqliteStoreConfig};
use runku_development::DevelopmentActor;
use runku_execution::{document_write_set_payload, plan_document_index_mutations};
use runku_file_storage::{FileObjectStore, FileStorageLimits, FileUsageSink};
use runku_gateway::CorsOrigin;
use runku_identity::{
    ApplicationClient, ApplicationClientName, ApplicationClientStatus, ApplicationScope,
    ClientKind, CredentialKind, CredentialLabel, CredentialLifecycleResult, CredentialStatus,
};
use runku_local::{
    LocalChannelExpectation, LocalCodeResolution, LocalCreatedCredential, LocalCredentialMetadata,
    LocalIdentityError, LocalIdentityManager, LocalLogError, LocalLogManager, LocalProcess,
    LocalProcessConfig, LocalPublishError, LocalReleaseError, LocalReleaseManager,
    LocalReleaseOutcome, LocalReleaseStatusReport, derive_local_object_storage_digest_key,
    load_local, publish_local_if_head,
};
use runku_management_service::{
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
    ManagementDataWriteResult, ManagementFunctionEntry, ManagementFunctionPage,
    ManagementIssuedStorageAccessKey, ManagementLogArchiveStatus, ManagementLogPage,
    ManagementLogPruneRequest, ManagementLogPruneResult, ManagementLogQuery, ManagementProduct,
    ManagementProductError, ManagementReleaseOutcome, ManagementReleaseStatus,
    ManagementResolvedTarget, ManagementSchemaIndex, ManagementSchemaPage, ManagementSchemaTable,
    ManagementServingOperation, ManagementServingPolicy, ManagementServingPolicyResult,
    ManagementServingPolicySet, ManagementServingRelease, ManagementStorageAccessKey,
    ManagementStorageAccessKeyConfiguration, ManagementStorageAccessKeyIssue,
    ManagementStorageAccessKeyPage, ManagementStorageAccessKeyRevoke,
    ManagementStorageAccessKeyRotate, ManagementStorageOperation, ManagementWorkspacePublish,
};
use runku_object_storage::{
    AccessKeyConfiguration, AccessKeyId, AccessKeyMetadata, AccessKeyOperation, Bucket,
    BucketConfiguration, BucketId, BucketLifecycle, BucketPolicy, BucketQuota, CorsMethod,
    CorsRule, ObjectStorageActor, ObjectStorageError, ObjectStorageOperation,
    ObjectStorageOperationKind, ObjectStorageService, SecretDigestKey, Versioning,
};
use runku_object_storage_repository::{ObjectStorageRepositoryConfig, SqlObjectStorageRepository};
use runku_observability::{
    LogArchive, LogLevel, LogQuery, LogStream, NatsLogJournal, SequencedOperationalEvent,
};
use runku_protocol::{WireValueV1, decode_development_publish_request_v1};
use runku_releases::{
    ArtifactFormat, AuthPolicy, Capability, FunctionType, FunctionVisibility, RuntimeClass,
    Sha256Digest, decode_node_esm_bundle, decode_safe_esm_bundle,
};
use runku_schema::{SchemaCatalog, decode_schema_catalog};
use runku_serving::{
    ServingCommandKind, ServingMode, ServingOperation, ServingPolicy, ServingPolicyError,
    ServingPolicyRecord, ServingPolicyService,
};
use runku_serving_repository::{ServingRepositoryConfig, SqlServingPolicyRepository};
use runku_value::{CanonicalValue, IndexKey, IndexValue, TimestampMicros};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use zeroize::Zeroizing;

/// One configured Product Environment and its lazily started serving process.
pub struct ProductAdapter {
    root: PathBuf,
    scope: EnvironmentScope,
    log_archive: Option<LogArchive>,
    log_journal: Option<NatsLogJournal>,
    process_config: LocalProcessConfig,
    process: Mutex<Option<LocalProcess>>,
    data_store: Arc<dyn LogicalStore>,
    identity: LocalIdentityManager,
    serving: ServingPolicyService,
    storage: ObjectStorageService,
}

/// Validated server-owned configuration for one Product adapter.
pub struct ProductAdapterConfig {
    /// Optional secret PostgreSQL DSN for Environment-scoped Function platform data.
    pub platform_database_url: Option<Zeroizing<String>>,
    /// Optional historical Operational Log archive.
    pub log_archive: Option<LogArchive>,
    /// Optional replicated Operational Log journal.
    pub log_journal: Option<NatsLogJournal>,
    /// Exact browser origins admitted by the Product gateway.
    pub allowed_origins: BTreeSet<CorsOrigin>,
    /// Optional relative Product authentication configuration path.
    pub auth_config: Option<PathBuf>,
    /// Optional externally configured application-file object backend.
    pub file_object_store: Option<FileObjectStore>,
    /// Validated application-file resource limits.
    pub file_storage_limits: FileStorageLimits,
    /// Optional authoritative application-file usage destination.
    pub file_usage_sink: Option<std::sync::Arc<dyn FileUsageSink>>,
    /// Bounded cadence for delivering the application-file usage outbox.
    pub file_usage_interval: std::time::Duration,
}

impl std::fmt::Debug for ProductAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductAdapter")
            .field("root", &self.root)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl ProductAdapter {
    pub async fn open(root: PathBuf, config: ProductAdapterConfig) -> Result<Self, &'static str> {
        let (state, paths) = load_local(&root)
            .await
            .map_err(|_| "SERVER_PRODUCT_ROOT_INVALID")?;
        let data_store: Arc<dyn LogicalStore> = match config.platform_database_url.as_ref() {
            Some(url) => Arc::new(
                PostgresStore::connect_scoped(
                    url.as_str(),
                    PostgresStoreConfig::PRODUCTION,
                    state.scope(),
                )
                .await
                .map_err(map_product_database)?,
            ),
            None => Arc::new(
                SqliteStore::open(&paths.data_database, SqliteStoreConfig::LOCAL)
                    .await
                    .map_err(map_product_database)?,
            ),
        };
        let identity = LocalIdentityManager::open(&root)
            .await
            .map_err(|_| "SERVER_PRODUCT_ROOT_INVALID")?;
        let serving_repository = SqlServingPolicyRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", paths.identity_database.display()),
            ServingRepositoryConfig::LOCAL,
        )
        .await
        .map_err(|_| "SERVER_PRODUCT_ROOT_INVALID")?;
        let storage_repository = SqlObjectStorageRepository::connect_sqlite(
            &format!("sqlite://{}?mode=rwc", paths.identity_database.display()),
            ObjectStorageRepositoryConfig::LOCAL,
        )
        .await
        .map_err(|_| "SERVER_PRODUCT_ROOT_INVALID")?;
        let storage_digest_key = derive_local_object_storage_digest_key(&root)
            .await
            .map_err(|_| "SERVER_PRODUCT_ROOT_INVALID")?;
        let adapter = Self {
            root,
            scope: state.scope(),
            log_archive: config.log_archive,
            log_journal: config.log_journal,
            process_config: LocalProcessConfig {
                allowed_origins: config.allowed_origins,
                auth_config: config.auth_config,
                file_object_store: config.file_object_store,
                data_store: Some(Arc::clone(&data_store)),
                file_storage_limits: config.file_storage_limits,
                file_usage_sink: config.file_usage_sink,
                file_usage_interval: config.file_usage_interval,
                ..LocalProcessConfig::default()
            },
            process: Mutex::new(None),
            data_store,
            identity,
            serving: ServingPolicyService::new(Arc::new(serving_repository)),
            storage: ObjectStorageService::new(
                Arc::new(storage_repository),
                SecretDigestKey::new(storage_digest_key),
            ),
        };
        let releases = LocalReleaseManager::open(&adapter.root)
            .await
            .map_err(|_| "SERVER_PRODUCT_ROOT_INVALID")?;
        let has_channels = match releases.status().await {
            Ok(status) => !status.channels.is_empty(),
            // A freshly initialized Product root has no Release Environment row until its first
            // publish. That is a valid idle state, not corrupt Product state.
            Err(LocalReleaseError::NotFound) => false,
            Err(_) => return Err("SERVER_PRODUCT_ROOT_INVALID"),
        };
        if has_channels {
            Box::pin(adapter.ensure_serving())
                .await
                .map_err(|_| "SERVER_PRODUCT_UNAVAILABLE")?;
        }
        Ok(adapter)
    }

    /// Stops the attached Product listener and every background loop within its grace period.
    pub async fn shutdown(&self) {
        if let Some(process) = self.process.lock().await.take() {
            process.shutdown().await;
        }
    }

    async fn ensure_serving(&self) -> Result<(), ManagementProductError> {
        let mut process = self.process.lock().await;
        if process.is_none() {
            *process = Some(
                LocalProcess::start(
                    &self.root,
                    LocalProcessConfig {
                        log_archive: self.log_archive.clone(),
                        log_journal: self.log_journal.clone(),
                        ..self.process_config.clone()
                    },
                )
                .await
                .map_err(|_| ManagementProductError::Unavailable)?,
            );
        }
        Ok(())
    }

    async fn effective_catalog(
        &self,
        requested: &str,
    ) -> Result<EffectiveCatalog, ManagementProductError> {
        let target = requested
            .parse::<CodeTarget>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let manager = LocalReleaseManager::open(&self.root)
            .await
            .map_err(map_release)?;
        let resolution = manager.resolve_code(&target).await.map_err(map_release)?;
        decode_effective_catalog(&target, resolution)
    }

    async fn created_application_credential(
        &self,
        created: LocalCreatedCredential,
        creation: bool,
    ) -> Result<ManagementCreatedApplicationCredential, ManagementProductError> {
        let recoverable = created.credential.kind == CredentialKind::Publishable;
        let secret_shown_once = creation && created.credential.kind == CredentialKind::Secret;
        let key = created.key.expose().to_owned();
        Ok(ManagementCreatedApplicationCredential {
            configuration_revision: self
                .identity
                .configuration_revision()
                .await
                .map_err(map_identity)?,
            credential: application_credential(&created.credential),
            key,
            recoverable,
            secret_shown_once,
        })
    }

    async fn require_credential_owner(
        &self,
        client_id: ApplicationClientId,
        credential_id: CredentialId,
    ) -> Result<(), ManagementProductError> {
        if self
            .identity
            .list_credentials(client_id)
            .await
            .map_err(map_identity)?
            .iter()
            .any(|credential| credential.id == credential_id)
        {
            Ok(())
        } else {
            Err(ManagementProductError::NotFound)
        }
    }

    async fn credential_lifecycle(
        &self,
        credential_id: CredentialId,
        status: &'static str,
        result: CredentialLifecycleResult,
    ) -> Result<ManagementApplicationCredentialLifecycle, ManagementProductError> {
        Ok(ManagementApplicationCredentialLifecycle {
            configuration_revision: self
                .identity
                .configuration_revision()
                .await
                .map_err(map_identity)?,
            credential_id: credential_id.to_string(),
            status: status.to_owned(),
            replayed: result == CredentialLifecycleResult::Replayed,
        })
    }

    async fn bucket_result(
        &self,
        bucket_id: BucketId,
        operation_id: OperationId,
        replayed: bool,
    ) -> Result<ManagementBucketResult, ManagementProductError> {
        let bucket = self
            .storage
            .get_bucket(self.scope, bucket_id)
            .await
            .map_err(map_storage)?
            .ok_or(ManagementProductError::Corruption)?;
        Ok(ManagementBucketResult {
            bucket: management_bucket(&bucket),
            operation_id: operation_id.to_string(),
            replayed,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_data_write(
        &self,
        operation_id: OperationId,
        table_name: &str,
        document_id: DocumentId,
        target: &str,
        write: DataWrite,
    ) -> Result<ManagementDataWriteResult, ManagementProductError> {
        let catalog = self.effective_catalog(target).await?;
        let table = resolve_table(&catalog.schema, table_name)?;
        let (mutation, previous_value) = match write {
            DataWrite::Insert { value } => {
                catalog
                    .schema
                    .validate_document(table.id, &value)
                    .map_err(|_| ManagementProductError::Validation)?;
                (
                    DocumentMutation::Upsert {
                        table_id: table.id,
                        document_id,
                        expected: ExpectedRevision::Absent,
                        value,
                    },
                    None,
                )
            }
            DataWrite::Replace {
                expected_revision,
                previous_value,
                value,
            } => {
                catalog
                    .schema
                    .validate_document(table.id, &previous_value)
                    .map_err(|_| ManagementProductError::Validation)?;
                catalog
                    .schema
                    .validate_document(table.id, &value)
                    .map_err(|_| ManagementProductError::Validation)?;
                self.verify_previous_value(
                    table.id,
                    document_id,
                    expected_revision,
                    &previous_value,
                )
                .await?;
                (
                    DocumentMutation::Upsert {
                        table_id: table.id,
                        document_id,
                        expected: ExpectedRevision::Exact(expected_revision),
                        value,
                    },
                    Some(previous_value),
                )
            }
            DataWrite::Delete {
                expected_revision,
                previous_value,
            } => {
                catalog
                    .schema
                    .validate_document(table.id, &previous_value)
                    .map_err(|_| ManagementProductError::Validation)?;
                self.verify_previous_value(
                    table.id,
                    document_id,
                    expected_revision,
                    &previous_value,
                )
                .await?;
                (
                    DocumentMutation::Delete {
                        table_id: table.id,
                        document_id,
                        expected_revision,
                    },
                    Some(previous_value),
                )
            }
        };
        let old_values = BTreeMap::from([((table.id, document_id), previous_value)]);
        let indexes = plan_document_index_mutations(
            &catalog.indexes,
            std::iter::once(&mutation),
            &old_values,
        )
        .map_err(map_mutation_planning)?;
        let payload = document_write_set_payload(std::slice::from_ref(&mutation), &indexes);
        let mut batch = CommitBatch::new(self.scope, operation_id);
        batch.push_document(mutation);
        for index in indexes {
            batch.push_index(index);
        }
        batch.push_outbox(OutboxAppend {
            event_id: OutboxEventId::from_ulid(operation_id.as_ulid()),
            payload,
        });
        batch.set_intent_context(
            format!(
                "RUNKU_DATA_ADMIN_V1\0{}\0{}\0{}",
                catalog.target.requested,
                catalog.target.resolved,
                catalog.target.schema_contract_hash
            )
            .into_bytes(),
        );
        let result = self.data_store.commit(&batch).await.map_err(map_store)?;
        let document = result
            .documents
            .iter()
            .find(|result| result.table_id == table.id && result.document_id == document_id)
            .ok_or(ManagementProductError::Corruption)?;
        Ok(ManagementDataWriteResult {
            version: 1,
            target: catalog.target,
            table_id: table.id.to_string(),
            document_id: document_id.to_string(),
            revision: document.revision.map(|revision| revision.to_string()),
            commit_sequence: result.commit_sequence.to_string(),
            replayed: result.replayed,
        })
    }

    async fn verify_previous_value(
        &self,
        table_id: TableId,
        document_id: DocumentId,
        expected_revision: u64,
        previous_value: &CanonicalValue,
    ) -> Result<(), ManagementProductError> {
        let mut snapshot = self
            .data_store
            .begin_read(self.scope)
            .await
            .map_err(map_store)?;
        let current = snapshot
            .get_document(table_id, document_id)
            .await
            .map_err(map_store)?;
        snapshot.close().await.map_err(map_store)?;
        if current.as_ref().is_some_and(|current| {
            current.revision == expected_revision && current.value != *previous_value
        }) {
            return Err(ManagementProductError::Conflict);
        }
        Ok(())
    }
}

enum DataWrite {
    Insert {
        value: CanonicalValue,
    },
    Replace {
        expected_revision: u64,
        previous_value: CanonicalValue,
        value: CanonicalValue,
    },
    Delete {
        expected_revision: u64,
        previous_value: CanonicalValue,
    },
}

struct EffectiveCatalog {
    target: ManagementResolvedTarget,
    manifest: runku_releases::ReleaseManifestV1,
    schema: DocumentSchemaV1,
    indexes: SchemaCatalog,
    contracts: BTreeMap<Sha256Digest, Contract>,
}

fn decode_effective_catalog(
    requested: &CodeTarget,
    resolution: LocalCodeResolution,
) -> Result<EffectiveCatalog, ManagementProductError> {
    let LocalCodeResolution {
        serving_revision,
        pinned_code,
        manifest,
        artifact_bytes,
    } = resolution;
    let resources = match manifest.artifact.format {
        ArtifactFormat::SafeEsmBundleV1 => {
            let bundle = decode_safe_esm_bundle(&artifact_bytes)
                .map_err(|_| ManagementProductError::Corruption)?;
            bundle
                .verify_manifest(&manifest, &artifact_bytes)
                .map_err(|_| ManagementProductError::Corruption)?;
            CatalogResources::Safe(bundle)
        }
        ArtifactFormat::NodeEsmBundleV1 => {
            let bundle = decode_node_esm_bundle(&artifact_bytes)
                .map_err(|_| ManagementProductError::Corruption)?;
            bundle
                .verify_manifest(&manifest, &artifact_bytes)
                .map_err(|_| ManagementProductError::Corruption)?;
            CatalogResources::Node(bundle)
        }
        ArtifactFormat::NodeOciDescriptorV1 | ArtifactFormat::HybridOciArtifactV1 => {
            return Err(ManagementProductError::Invalid);
        }
    };
    let schema = decode_document_schema(
        resources
            .get(manifest.schema_contract_hash)
            .ok_or(ManagementProductError::Corruption)?
            .as_bytes(),
    )
    .map_err(|_| ManagementProductError::Corruption)?;
    let indexes = decode_schema_catalog(
        resources
            .get(manifest.index_contract_hash)
            .ok_or(ManagementProductError::Corruption)?
            .as_bytes(),
    )
    .map_err(|_| ManagementProductError::Corruption)?;
    if indexes.project_id() != manifest.project_id
        || indexes.digest().as_slice() != manifest.index_contract_hash.as_bytes()
    {
        return Err(ManagementProductError::Corruption);
    }
    let mut contracts = BTreeMap::new();
    for function in &manifest.functions {
        for digest in [
            function.arguments_contract_hash,
            function.result_contract_hash,
        ] {
            if let std::collections::btree_map::Entry::Vacant(entry) = contracts.entry(digest) {
                let contract = decode_contract(
                    resources
                        .get(digest)
                        .ok_or(ManagementProductError::Corruption)?
                        .as_bytes(),
                )
                .map_err(|_| ManagementProductError::Corruption)?;
                entry.insert(contract);
            }
        }
    }
    Ok(EffectiveCatalog {
        target: ManagementResolvedTarget {
            requested: requested.to_string(),
            resolved: pinned_code.to_string(),
            release_id: manifest.release_id.to_string(),
            serving_revision,
            schema_contract_hash: manifest.schema_contract_hash.to_string(),
        },
        manifest,
        schema,
        indexes,
        contracts,
    })
}

enum CatalogResources {
    Safe(runku_releases::SafeEsmBundleV1),
    Node(runku_releases::NodeEsmBundleV1),
}

impl CatalogResources {
    fn get(&self, digest: Sha256Digest) -> Option<&str> {
        match self {
            Self::Safe(bundle) => bundle.resource(digest),
            Self::Node(bundle) => bundle.resource(digest),
        }
    }
}

pub async fn migrate_platform_database(
    root: &std::path::Path,
    url: &str,
) -> Result<(), &'static str> {
    let state = load_local(root)
        .await
        .map_err(|_| "SERVER_PRODUCT_ROOT_INVALID")?
        .0;
    let store = PostgresStore::connect_scoped(url, PostgresStoreConfig::PRODUCTION, state.scope())
        .await
        .map_err(map_product_database)?;
    store.close().await;
    Ok(())
}

#[async_trait]
impl ManagementProduct for ProductAdapter {
    fn scope(&self) -> EnvironmentScope {
        self.scope
    }

    async fn health(&self) -> Result<(), ManagementProductError> {
        self.data_store.health().await.map_err(map_store_health)?;
        self.identity
            .configuration_revision()
            .await
            .map_err(map_identity)?;
        self.serving.health().await.map_err(map_serving)?;
        self.storage.health().await.map_err(map_storage)
    }

    async fn serving_policy(&self) -> Result<ManagementServingPolicy, ManagementProductError> {
        self.serving
            .get(self.scope)
            .await
            .map_err(map_serving)?
            .as_ref()
            .map(management_serving_policy)
            .ok_or(ManagementProductError::NotFound)
    }

    async fn serving_policy_set(
        &self,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementServingPolicySet,
    ) -> Result<ManagementServingPolicyResult, ManagementProductError> {
        if request.expected_revision == Some(0) {
            return Err(ManagementProductError::Invalid);
        }
        let mode = match request.mode.as_str() {
            "atomic" => ServingMode::Atomic,
            "gradual" => ServingMode::Gradual,
            _ => return Err(ManagementProductError::Invalid),
        };
        let manager = LocalReleaseManager::open(&self.root)
            .await
            .map_err(map_release)?;
        let mut manifests = Vec::with_capacity(request.releases.len());
        for entry in &request.releases {
            let release_id = entry
                .release_id
                .parse::<ReleaseId>()
                .map_err(|_| ManagementProductError::Invalid)?;
            let resolved = manager
                .resolve_code(&CodeTarget::Release(release_id))
                .await
                .map_err(map_release)?;
            manifests.push((resolved.manifest, entry.weight_percent));
        }
        let policy = ServingPolicy::from_manifests(
            self.scope,
            mode,
            manifests
                .iter()
                .map(|(manifest, weight)| (manifest, *weight)),
        )
        .map_err(map_serving)?;
        let changed_at = parse_timestamp(&request.changed_at_micros)?;
        let result = self
            .serving
            .set_desired(
                self.scope,
                operation_id,
                actor,
                request.expected_revision,
                policy,
                changed_at,
            )
            .await
            .map_err(map_serving)?;
        let record = self
            .serving
            .get(self.scope)
            .await
            .map_err(map_serving)?
            .ok_or(ManagementProductError::Corruption)?;
        if record.policy_revision != result.operation.policy_revision {
            return Err(ManagementProductError::Corruption);
        }
        Ok(ManagementServingPolicyResult {
            policy: management_serving_policy(&record),
            operation_id: result.operation.operation_id.to_string(),
            replayed: result.replayed,
        })
    }

    async fn serving_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<ManagementServingOperation, ManagementProductError> {
        self.serving
            .operation(self.scope, operation_id)
            .await
            .map_err(map_serving)?
            .as_ref()
            .map(management_serving_operation)
            .ok_or(ManagementProductError::NotFound)
    }

    async fn buckets(
        &self,
        after: Option<&str>,
        limit: u16,
    ) -> Result<ManagementBucketPage, ManagementProductError> {
        let after = after
            .map(str::parse::<BucketId>)
            .transpose()
            .map_err(map_storage)?;
        let request =
            runku_object_storage::BucketPageRequest::new(after, limit).map_err(map_storage)?;
        let page = self
            .storage
            .list_buckets(self.scope, request)
            .await
            .map_err(map_storage)?;
        Ok(ManagementBucketPage {
            version: 1,
            buckets: page.buckets.iter().map(management_bucket).collect(),
            next: page.next.map(|value| value.to_string()),
        })
    }

    async fn bucket(&self, bucket_id: &str) -> Result<ManagementBucket, ManagementProductError> {
        let bucket_id = bucket_id.parse::<BucketId>().map_err(map_storage)?;
        self.storage
            .get_bucket(self.scope, bucket_id)
            .await
            .map_err(map_storage)?
            .as_ref()
            .map(management_bucket)
            .ok_or(ManagementProductError::NotFound)
    }

    async fn bucket_create(
        &self,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementBucketCreate,
    ) -> Result<ManagementBucketResult, ManagementProductError> {
        let result = self
            .storage
            .create_bucket(
                self.scope,
                operation_id,
                bucket_configuration(&request.configuration)?,
                storage_actor(actor)?,
                parse_timestamp(&request.at_micros)?,
            )
            .await
            .map_err(map_storage)?;
        self.bucket_result(result.operation.bucket_id, operation_id, result.replayed)
            .await
    }

    async fn bucket_update(
        &self,
        bucket_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementBucketUpdate,
    ) -> Result<ManagementBucketResult, ManagementProductError> {
        let bucket_id = bucket_id.parse::<BucketId>().map_err(map_storage)?;
        let result = self
            .storage
            .update_bucket(
                self.scope,
                bucket_id,
                operation_id,
                request.expected_revision,
                bucket_configuration(&request.configuration)?,
                storage_actor(actor)?,
                parse_timestamp(&request.at_micros)?,
            )
            .await
            .map_err(map_storage)?;
        self.bucket_result(bucket_id, operation_id, result.replayed)
            .await
    }

    async fn bucket_archive(
        &self,
        bucket_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementBucketArchive,
    ) -> Result<ManagementBucketResult, ManagementProductError> {
        let bucket_id = bucket_id.parse::<BucketId>().map_err(map_storage)?;
        let result = self
            .storage
            .archive_bucket(
                self.scope,
                bucket_id,
                operation_id,
                request.expected_revision,
                storage_actor(actor)?,
                parse_timestamp(&request.at_micros)?,
            )
            .await
            .map_err(map_storage)?;
        self.bucket_result(bucket_id, operation_id, result.replayed)
            .await
    }

    async fn storage_access_keys(
        &self,
        bucket_id: &str,
        after: Option<&str>,
        limit: u16,
    ) -> Result<ManagementStorageAccessKeyPage, ManagementProductError> {
        let bucket_id = bucket_id.parse::<BucketId>().map_err(map_storage)?;
        let after = after
            .map(str::parse::<AccessKeyId>)
            .transpose()
            .map_err(map_storage)?;
        let request =
            runku_object_storage::AccessKeyPageRequest::new(after, limit).map_err(map_storage)?;
        let page = self
            .storage
            .list_access_keys(self.scope, bucket_id, request)
            .await
            .map_err(map_storage)?;
        Ok(ManagementStorageAccessKeyPage {
            version: 1,
            keys: page.keys.iter().map(management_storage_key).collect(),
            next: page.next.map(|value| value.to_string()),
        })
    }

    async fn storage_access_key_issue(
        &self,
        bucket_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementStorageAccessKeyIssue,
    ) -> Result<ManagementIssuedStorageAccessKey, ManagementProductError> {
        let bucket_id = bucket_id.parse::<BucketId>().map_err(map_storage)?;
        let issued = self
            .storage
            .issue_access_key(
                self.scope,
                bucket_id,
                operation_id,
                storage_access_key_configuration(&request.configuration)?,
                storage_actor(actor)?,
                parse_timestamp(&request.at_micros)?,
            )
            .await
            .map_err(map_storage)?;
        Ok(management_issued_storage_key(&issued, operation_id))
    }

    async fn storage_access_key_rotate(
        &self,
        bucket_id: &str,
        access_key_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementStorageAccessKeyRotate,
    ) -> Result<ManagementIssuedStorageAccessKey, ManagementProductError> {
        let bucket_id = bucket_id.parse::<BucketId>().map_err(map_storage)?;
        let access_key_id = access_key_id.parse::<AccessKeyId>().map_err(map_storage)?;
        let issued = self
            .storage
            .rotate_access_key(
                self.scope,
                bucket_id,
                access_key_id,
                operation_id,
                request.expected_revision,
                parse_timestamp(&request.overlap_until_micros)?,
                storage_actor(actor)?,
                parse_timestamp(&request.at_micros)?,
            )
            .await
            .map_err(map_storage)?;
        Ok(management_issued_storage_key(&issued, operation_id))
    }

    async fn storage_access_key_revoke(
        &self,
        bucket_id: &str,
        access_key_id: &str,
        operation_id: OperationId,
        actor: OperatorId,
        request: &ManagementStorageAccessKeyRevoke,
    ) -> Result<ManagementStorageOperation, ManagementProductError> {
        let bucket_id = bucket_id.parse::<BucketId>().map_err(map_storage)?;
        let access_key_id = access_key_id.parse::<AccessKeyId>().map_err(map_storage)?;
        let result = self
            .storage
            .revoke_access_key(
                self.scope,
                bucket_id,
                access_key_id,
                operation_id,
                request.expected_revision,
                storage_actor(actor)?,
                parse_timestamp(&request.at_micros)?,
            )
            .await
            .map_err(map_storage)?;
        Ok(management_storage_operation(&result.operation))
    }

    async fn storage_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<ManagementStorageOperation, ManagementProductError> {
        self.storage
            .operation(self.scope, operation_id)
            .await
            .map_err(map_storage)?
            .as_ref()
            .map(management_storage_operation)
            .ok_or(ManagementProductError::NotFound)
    }

    async fn application_clients(
        &self,
    ) -> Result<ManagementApplicationClientList, ManagementProductError> {
        let clients = self
            .identity
            .list_clients()
            .await
            .map_err(map_identity)?
            .iter()
            .map(application_client)
            .collect();
        Ok(ManagementApplicationClientList {
            version: 1,
            configuration_revision: self
                .identity
                .configuration_revision()
                .await
                .map_err(map_identity)?,
            clients,
        })
    }

    async fn application_client_create(
        &self,
        request: &ManagementApplicationClientCreate,
    ) -> Result<ManagementCreatedApplicationClient, ManagementProductError> {
        let id = request
            .client_id
            .parse::<ApplicationClientId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let name = request
            .name
            .parse::<ApplicationClientName>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let kind = parse_client_kind(&request.kind)?;
        let scopes = parse_application_scopes(&request.scopes)?;
        let created_at = parse_timestamp(&request.created_at_micros)?;
        let (client, replayed) = self
            .identity
            .create_client_with_replay(id, name, kind, scopes, created_at)
            .await
            .map_err(map_identity)?;
        Ok(ManagementCreatedApplicationClient {
            version: 1,
            configuration_revision: self
                .identity
                .configuration_revision()
                .await
                .map_err(map_identity)?,
            client: application_client(&client),
            replayed,
        })
    }

    async fn application_credentials(
        &self,
        client_id: &str,
    ) -> Result<ManagementApplicationCredentialList, ManagementProductError> {
        let client_id = client_id
            .parse::<ApplicationClientId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let credentials = self
            .identity
            .list_credentials(client_id)
            .await
            .map_err(map_identity)?
            .iter()
            .map(application_credential)
            .collect();
        Ok(ManagementApplicationCredentialList {
            version: 1,
            configuration_revision: self
                .identity
                .configuration_revision()
                .await
                .map_err(map_identity)?,
            credentials,
        })
    }

    async fn application_credential_create(
        &self,
        client_id: &str,
        request: &ManagementApplicationCredentialCreate,
    ) -> Result<ManagementCreatedApplicationCredential, ManagementProductError> {
        let client_id = client_id
            .parse::<ApplicationClientId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let credential_id = request
            .credential_id
            .parse::<CredentialId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let label = request
            .label
            .parse::<CredentialLabel>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let scopes = parse_application_scopes(&request.scopes)?;
        let created_at = parse_timestamp(&request.created_at_micros)?;
        let expires_at = request
            .expires_at_micros
            .as_deref()
            .map(parse_timestamp)
            .transpose()?;
        let created = self
            .identity
            .create_credential(
                credential_id,
                client_id,
                label,
                scopes,
                created_at,
                expires_at,
            )
            .await
            .map_err(map_identity)?;
        self.created_application_credential(created, true).await
    }

    async fn application_credential_reveal(
        &self,
        client_id: &str,
        credential_id: &str,
    ) -> Result<ManagementCreatedApplicationCredential, ManagementProductError> {
        let client_id = client_id
            .parse::<ApplicationClientId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let credential_id = credential_id
            .parse::<CredentialId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let created = self
            .identity
            .reveal_publishable(client_id, credential_id)
            .await
            .map_err(map_identity)?;
        self.created_application_credential(created, false).await
    }

    async fn application_credential_rotate(
        &self,
        client_id: &str,
        credential_id: &str,
        request: &ManagementApplicationCredentialRotate,
    ) -> Result<ManagementCreatedApplicationCredential, ManagementProductError> {
        let client_id = client_id
            .parse::<ApplicationClientId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let credential_id = credential_id
            .parse::<CredentialId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let replacement_id = request
            .replacement_credential_id
            .parse::<CredentialId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let label = request
            .label
            .parse::<CredentialLabel>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let created_at = parse_timestamp(&request.created_at_micros)?;
        let expires_at = request
            .expires_at_micros
            .as_deref()
            .map(parse_timestamp)
            .transpose()?;
        let created = self
            .identity
            .rotate_credential(
                client_id,
                credential_id,
                replacement_id,
                label,
                created_at,
                expires_at,
            )
            .await
            .map_err(map_identity)?;
        self.created_application_credential(created, true).await
    }

    async fn application_credential_revoke(
        &self,
        client_id: &str,
        credential_id: &str,
        revoked_at_micros: i64,
    ) -> Result<ManagementApplicationCredentialLifecycle, ManagementProductError> {
        let (client_id, credential_id) = parse_credential_path(client_id, credential_id)?;
        self.require_credential_owner(client_id, credential_id)
            .await?;
        let result = self
            .identity
            .revoke_credential(credential_id, TimestampMicros::new(revoked_at_micros))
            .await
            .map_err(map_identity)?;
        self.credential_lifecycle(credential_id, "revoked", result)
            .await
    }

    async fn application_credential_delete(
        &self,
        client_id: &str,
        credential_id: &str,
        deleted_at_micros: i64,
    ) -> Result<ManagementApplicationCredentialLifecycle, ManagementProductError> {
        let (client_id, credential_id) = parse_credential_path(client_id, credential_id)?;
        self.require_credential_owner(client_id, credential_id)
            .await?;
        let result = self
            .identity
            .delete_credential(credential_id, TimestampMicros::new(deleted_at_micros))
            .await
            .map_err(map_identity)?;
        self.credential_lifecycle(credential_id, "deleted", result)
            .await
    }

    async fn publish(
        &self,
        actor: &str,
        bytes: &[u8],
    ) -> Result<ManagementWorkspacePublish, ManagementProductError> {
        let request = decode_development_publish_request_v1(bytes)
            .map_err(|_| ManagementProductError::Invalid)?;
        if request.project_id != self.scope.project_id() {
            return Err(ManagementProductError::Invalid);
        }
        let actor =
            DevelopmentActor::from_str(actor).map_err(|_| ManagementProductError::Invalid)?;
        let result = publish_local_if_head(
            &self.root,
            &request.workspace_ref,
            &actor,
            request.expected_head,
            &request.manifest_bytes,
            &request.artifact_bytes,
        )
        .await
        .map_err(map_publish)?;
        Ok(ManagementWorkspacePublish {
            release_id: result.release_id.to_string(),
            revision_id: result.revision_id.to_string(),
            replayed: result.replayed,
        })
    }

    async fn release(
        &self,
        release_id: &str,
        against: Option<&str>,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        let release = release_id
            .parse::<ReleaseId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let against = against
            .map(ChannelName::from_str)
            .transpose()
            .map_err(|_| ManagementProductError::Invalid)?;
        let manager = LocalReleaseManager::open(&self.root)
            .await
            .map_err(map_release)?;
        Ok(outcome(
            manager
                .release(release, against.as_ref())
                .await
                .map_err(map_release)?,
        ))
    }

    async fn promote(
        &self,
        channel: &str,
        release_id: &str,
        expected: Option<Option<&str>>,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        let channel =
            ChannelName::from_str(channel).map_err(|_| ManagementProductError::Invalid)?;
        let release = release_id
            .parse::<ReleaseId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let expected = match expected {
            None => LocalChannelExpectation::Current,
            Some(None) => LocalChannelExpectation::Empty,
            Some(Some(value)) => LocalChannelExpectation::Release(
                value
                    .parse::<ReleaseId>()
                    .map_err(|_| ManagementProductError::Invalid)?,
            ),
        };
        let manager = LocalReleaseManager::open(&self.root)
            .await
            .map_err(map_release)?;
        let result = manager
            .promote(channel, release, expected)
            .await
            .map_err(map_release)?;
        Box::pin(self.ensure_serving()).await?;
        Ok(outcome(result))
    }

    async fn rollback(
        &self,
        channel: &str,
        expected: &str,
        target: &str,
    ) -> Result<ManagementReleaseOutcome, ManagementProductError> {
        let channel =
            ChannelName::from_str(channel).map_err(|_| ManagementProductError::Invalid)?;
        let expected = expected
            .parse::<ReleaseId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let target = target
            .parse::<ReleaseId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let manager = LocalReleaseManager::open(&self.root)
            .await
            .map_err(map_release)?;
        Ok(outcome(
            manager
                .rollback(channel, expected, target)
                .await
                .map_err(map_release)?,
        ))
    }

    async fn status(&self) -> Result<ManagementReleaseStatus, ManagementProductError> {
        let manager = LocalReleaseManager::open(&self.root)
            .await
            .map_err(map_release)?;
        Ok(status(manager.status().await.map_err(map_release)?))
    }

    async fn functions(
        &self,
        query: &ManagementCatalogQuery,
    ) -> Result<ManagementFunctionPage, ManagementProductError> {
        let limit = catalog_limit(query.limit)?;
        let after = query
            .after
            .as_deref()
            .map(FunctionName::from_str)
            .transpose()
            .map_err(|_| ManagementProductError::Invalid)?;
        let catalog = self.effective_catalog(&query.target).await?;
        let mut entries = catalog
            .manifest
            .functions
            .iter()
            .filter(|function| after.as_ref().is_none_or(|after| function.name > *after))
            .take(limit + 1)
            .map(|function| function_entry(function, &catalog.contracts))
            .collect::<Result<Vec<_>, _>>()?;
        let next = if entries.len() > limit {
            entries.truncate(limit);
            entries.last().map(|entry| entry.name.clone())
        } else {
            None
        };
        Ok(ManagementFunctionPage {
            version: 1,
            target: catalog.target,
            functions: entries,
            next,
        })
    }

    async fn schema_tables(
        &self,
        query: &ManagementCatalogQuery,
    ) -> Result<ManagementSchemaPage, ManagementProductError> {
        let limit = catalog_limit(query.limit)?;
        let after = query
            .after
            .as_deref()
            .map(TableId::from_str)
            .transpose()
            .map_err(|_| ManagementProductError::Invalid)?;
        let catalog = self.effective_catalog(&query.target).await?;
        let mut tables = catalog
            .schema
            .tables
            .iter()
            .filter(|table| after.is_none_or(|after| table.id > after))
            .take(limit + 1)
            .map(|table| schema_table(table, &catalog.indexes))
            .collect::<Result<Vec<_>, _>>()?;
        let next = if tables.len() > limit {
            tables.truncate(limit);
            tables.last().map(|table| table.table_id.clone())
        } else {
            None
        };
        Ok(ManagementSchemaPage {
            version: 1,
            target: catalog.target,
            tables,
            next,
        })
    }

    async fn data_get(
        &self,
        target: &str,
        table: &str,
        document_id: &str,
    ) -> Result<ManagementDataDocument, ManagementProductError> {
        let catalog = self.effective_catalog(target).await?;
        let table = resolve_table(&catalog.schema, table)?;
        let document_id = document_id
            .parse::<DocumentId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let mut snapshot = self
            .data_store
            .begin_read(self.scope)
            .await
            .map_err(map_store)?;
        let result = snapshot
            .get_document(table.id, document_id)
            .await
            .map_err(map_store)?;
        snapshot.close().await.map_err(map_store)?;
        let document = result.ok_or(ManagementProductError::NotFound)?;
        data_document(table.name.as_str(), &document)
    }

    async fn data_query(
        &self,
        request: &ManagementDataQuery,
    ) -> Result<ManagementDataPage, ManagementProductError> {
        let limit = data_limit(request.limit)?;
        let catalog = self.effective_catalog(&request.target).await?;
        let table = resolve_table(&catalog.schema, &request.table)?;
        let index = catalog
            .indexes
            .indexes_for_table(table.id)
            .find(|index| index.name() == request.index)
            .ok_or(ManagementProductError::NotFound)?;
        if request.prefix.len() > index.fields().len() {
            return Err(ManagementProductError::Invalid);
        }
        let range = if request.prefix.is_empty() {
            IndexRange::all()
        } else {
            let components = request
                .prefix
                .clone()
                .into_iter()
                .map(WireValueV1::into_canonical)
                .map(|value| {
                    value
                        .map_err(|_| ManagementProductError::Invalid)
                        .and_then(|value| {
                            IndexValue::try_from(&value)
                                .map_err(|_| ManagementProductError::Invalid)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let key = IndexKey::encode(&components).map_err(|_| ManagementProductError::Invalid)?;
            let prefix = key
                .prefix(components.len())
                .map_err(|_| ManagementProductError::Invalid)?;
            IndexRange::prefix(&prefix).map_err(map_store)?
        };
        let mut snapshot = self
            .data_store
            .begin_read(self.scope)
            .await
            .map_err(map_store)?;
        let sequence = snapshot.commit_sequence();
        let mut entries = snapshot
            .scan_index(
                index.index_id(),
                &range,
                u32::try_from(limit + 1).map_err(|_| ManagementProductError::Invalid)?,
            )
            .await
            .map_err(map_store)?;
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let mut documents = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.table_id != table.id {
                return Err(ManagementProductError::Corruption);
            }
            let document = snapshot
                .get_document(table.id, entry.document_id)
                .await
                .map_err(map_store)?
                .ok_or(ManagementProductError::Corruption)?;
            if document.revision != entry.document_revision {
                return Err(ManagementProductError::Corruption);
            }
            documents.push(data_document(table.name.as_str(), &document)?);
        }
        snapshot.close().await.map_err(map_store)?;
        Ok(ManagementDataPage {
            version: 1,
            target: catalog.target,
            snapshot_sequence: sequence.to_string(),
            documents,
            truncated,
        })
    }

    async fn data_insert(
        &self,
        operation_id: OperationId,
        table: &str,
        request: &ManagementDataInsertRequest,
    ) -> Result<ManagementDataWriteResult, ManagementProductError> {
        let value = request
            .value
            .clone()
            .into_canonical()
            .map_err(|_| ManagementProductError::Invalid)?;
        self.commit_data_write(
            operation_id,
            table,
            DocumentId::from_ulid(operation_id.as_ulid()),
            &request.target,
            DataWrite::Insert { value },
        )
        .await
    }

    async fn data_replace(
        &self,
        operation_id: OperationId,
        table: &str,
        document_id: &str,
        request: &ManagementDataReplaceRequest,
    ) -> Result<ManagementDataWriteResult, ManagementProductError> {
        let document_id = document_id
            .parse::<DocumentId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let expected_revision = parse_revision(&request.expected_revision)?;
        let previous_value = request
            .previous_value
            .clone()
            .into_canonical()
            .map_err(|_| ManagementProductError::Invalid)?;
        let value = request
            .value
            .clone()
            .into_canonical()
            .map_err(|_| ManagementProductError::Invalid)?;
        self.commit_data_write(
            operation_id,
            table,
            document_id,
            &request.target,
            DataWrite::Replace {
                expected_revision,
                previous_value,
                value,
            },
        )
        .await
    }

    async fn data_delete(
        &self,
        operation_id: OperationId,
        table: &str,
        document_id: &str,
        request: &ManagementDataDeleteRequest,
    ) -> Result<ManagementDataWriteResult, ManagementProductError> {
        let document_id = document_id
            .parse::<DocumentId>()
            .map_err(|_| ManagementProductError::Invalid)?;
        let expected_revision = parse_revision(&request.expected_revision)?;
        let previous_value = request
            .previous_value
            .clone()
            .into_canonical()
            .map_err(|_| ManagementProductError::Invalid)?;
        self.commit_data_write(
            operation_id,
            table,
            document_id,
            &request.target,
            DataWrite::Delete {
                expected_revision,
                previous_value,
            },
        )
        .await
    }

    async fn logs(
        &self,
        query: &ManagementLogQuery,
    ) -> Result<ManagementLogPage, ManagementProductError> {
        let manager = LocalLogManager::open_with_archive(&self.root, self.log_archive.clone())
            .await
            .map_err(map_logs)?;
        let query = parse_log_query(self.scope, query)?;
        let result = manager.query(&query).await.map_err(map_logs);
        manager.close().await;
        let page = result?;
        Ok(ManagementLogPage {
            records: page
                .records
                .iter()
                .map(log_record)
                .collect::<Result<Vec<_>, _>>()?,
            next: page.next.to_string(),
        })
    }

    async fn log_archive_status(
        &self,
    ) -> Result<ManagementLogArchiveStatus, ManagementProductError> {
        let manager = LocalLogManager::open_with_archive(&self.root, self.log_archive.clone())
            .await
            .map_err(map_logs)?;
        let result = manager.archive_status().await.map_err(map_logs);
        manager.close().await;
        let status = result?;
        Ok(ManagementLogArchiveStatus {
            parquet_bytes: status.parquet_bytes,
            records: status.records,
            segments: status.segments,
            through: status.through.to_string(),
        })
    }

    async fn log_prune(
        &self,
        request: &ManagementLogPruneRequest,
    ) -> Result<ManagementLogPruneResult, ManagementProductError> {
        if !(1..=10_000).contains(&request.maximum)
            || request.before_micros < 0
            || request.apply != request.environment_id.is_some()
        {
            return Err(ManagementProductError::Invalid);
        }
        let confirmation = request
            .environment_id
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|_| ManagementProductError::Invalid)?;
        let manager = LocalLogManager::open_with_archive(&self.root, self.log_archive.clone())
            .await
            .map_err(map_logs)?;
        let result = manager
            .prune_before(
                TimestampMicros::new(request.before_micros),
                request.maximum,
                request.apply,
                confirmation,
            )
            .await;
        manager.close().await;
        let result = result.map_err(map_logs)?;
        Ok(ManagementLogPruneResult {
            applied: request.apply,
            deleted: result.deleted,
            environment_id: self.scope.environment_id().to_string(),
            matched: result.matched,
            more: result.more,
        })
    }
}

fn application_client(client: &ApplicationClient) -> ManagementApplicationClient {
    ManagementApplicationClient {
        client_id: client.id.to_string(),
        name: client.name.to_string(),
        kind: match client.kind {
            ClientKind::Public => "public",
            ClientKind::Confidential => "confidential",
        }
        .to_owned(),
        status: match client.status {
            ApplicationClientStatus::Active => "active",
            ApplicationClientStatus::Disabled => "disabled",
        }
        .to_owned(),
        scopes: client
            .scope_ceiling
            .iter()
            .map(ToString::to_string)
            .collect(),
        created_at_micros: client.created_at.get().to_string(),
    }
}

fn application_credential(credential: &LocalCredentialMetadata) -> ManagementApplicationCredential {
    ManagementApplicationCredential {
        credential_id: credential.id.to_string(),
        client_id: credential.client_id.to_string(),
        kind: match credential.kind {
            CredentialKind::Publishable => "publishable",
            CredentialKind::Secret => "secret",
        }
        .to_owned(),
        label: credential.label.to_string(),
        status: match credential.status {
            CredentialStatus::Active => "active",
            CredentialStatus::Revoked => "revoked",
            CredentialStatus::Deleted => "deleted",
        }
        .to_owned(),
        scopes: credential.scopes.iter().map(ToString::to_string).collect(),
        created_at_micros: credential.created_at.get().to_string(),
        expires_at_micros: credential.expires_at.map(|value| value.get().to_string()),
        revoked_at_micros: credential.revoked_at.map(|value| value.get().to_string()),
    }
}

fn parse_client_kind(value: &str) -> Result<ClientKind, ManagementProductError> {
    match value {
        "public" => Ok(ClientKind::Public),
        "confidential" => Ok(ClientKind::Confidential),
        _ => Err(ManagementProductError::Invalid),
    }
}

fn parse_application_scopes(
    values: &[String],
) -> Result<BTreeSet<ApplicationScope>, ManagementProductError> {
    let scopes = values
        .iter()
        .map(|value| {
            value
                .parse::<ApplicationScope>()
                .map_err(|_| ManagementProductError::Invalid)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if scopes.len() != values.len() {
        return Err(ManagementProductError::Invalid);
    }
    Ok(scopes)
}

fn parse_timestamp(value: &str) -> Result<TimestampMicros, ManagementProductError> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| ManagementProductError::Invalid)?;
    if parsed < 0 || parsed.to_string() != value {
        return Err(ManagementProductError::Invalid);
    }
    Ok(TimestampMicros::new(parsed))
}

fn parse_credential_path(
    client_id: &str,
    credential_id: &str,
) -> Result<(ApplicationClientId, CredentialId), ManagementProductError> {
    Ok((
        client_id
            .parse::<ApplicationClientId>()
            .map_err(|_| ManagementProductError::Invalid)?,
        credential_id
            .parse::<CredentialId>()
            .map_err(|_| ManagementProductError::Invalid)?,
    ))
}

fn management_serving_policy(record: &ServingPolicyRecord) -> ManagementServingPolicy {
    ManagementServingPolicy {
        version: 1,
        policy_revision: record.policy_revision,
        mode: record.desired_policy.mode().as_str().to_owned(),
        releases: record
            .desired_policy
            .releases()
            .iter()
            .map(|release| ManagementServingRelease {
                release_id: release.release_id().to_string(),
                weight_percent: release.weight_percent(),
            })
            .collect(),
        observed_state: record.observed_state.as_str().to_owned(),
        observed_policy_revision: record.observed_policy_revision,
        converged: record.is_converged(),
        created_at_micros: record.created_at.get().to_string(),
        updated_at_micros: record.updated_at.get().to_string(),
        observed_at_micros: record.observed_at.map(|value| value.get().to_string()),
    }
}

fn management_serving_operation(operation: &ServingOperation) -> ManagementServingOperation {
    ManagementServingOperation {
        operation_id: operation.operation_id.to_string(),
        kind: match operation.kind {
            ServingCommandKind::SetDesired => "setDesired",
            ServingCommandKind::Materialize => "materialize",
        }
        .to_owned(),
        policy_revision: operation.policy_revision,
        observed_state: operation.observed_state.as_str().to_owned(),
        observed_policy_revision: operation.observed_policy_revision,
        completed_at_micros: operation.completed_at.get().to_string(),
    }
}

fn management_bucket(bucket: &Bucket) -> ManagementBucket {
    ManagementBucket {
        bucket_id: bucket.id.to_string(),
        configuration: management_bucket_configuration(&bucket.configuration),
        revision: bucket.revision,
        state: bucket.state.as_str().to_owned(),
        created_at_micros: bucket.created_at.get().to_string(),
        updated_at_micros: bucket.updated_at.get().to_string(),
    }
}

fn management_bucket_configuration(
    configuration: &BucketConfiguration,
) -> ManagementBucketConfiguration {
    ManagementBucketConfiguration {
        name: configuration.name.to_string(),
        policy: match configuration.policy {
            BucketPolicy::Private => "private",
            BucketPolicy::PublicRead => "publicRead",
        }
        .to_owned(),
        cors: configuration
            .cors
            .iter()
            .map(|rule| ManagementBucketCorsRule {
                origins: rule.origins.clone(),
                methods: rule
                    .methods
                    .iter()
                    .map(|method| method.as_str().to_owned())
                    .collect(),
                allowed_headers: rule.allowed_headers.clone(),
                exposed_headers: rule.exposed_headers.clone(),
                max_age_seconds: rule.max_age_seconds,
            })
            .collect(),
        versioning: configuration.versioning.as_str().to_owned(),
        lifecycle: ManagementBucketLifecycle {
            expire_current_after_days: configuration.lifecycle.expire_current_after_days,
            expire_noncurrent_after_days: configuration.lifecycle.expire_noncurrent_after_days,
            abort_incomplete_after_days: configuration.lifecycle.abort_incomplete_after_days,
        },
        quota: ManagementBucketQuota {
            max_object_bytes: configuration.quota.max_object_bytes.to_string(),
            max_total_bytes: configuration.quota.max_total_bytes.to_string(),
            max_objects: configuration.quota.max_objects.to_string(),
        },
    }
}

fn bucket_configuration(
    value: &ManagementBucketConfiguration,
) -> Result<BucketConfiguration, ManagementProductError> {
    let cors = value
        .cors
        .iter()
        .map(|rule| {
            let methods = rule
                .methods
                .iter()
                .map(|method| method.parse::<CorsMethod>().map_err(map_storage))
                .collect::<Result<BTreeSet<_>, _>>()?;
            if methods.len() != rule.methods.len() {
                return Err(ManagementProductError::Invalid);
            }
            Ok(CorsRule {
                origins: rule.origins.clone(),
                methods,
                allowed_headers: rule.allowed_headers.clone(),
                exposed_headers: rule.exposed_headers.clone(),
                max_age_seconds: rule.max_age_seconds,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let configuration = BucketConfiguration {
        name: value.name.parse().map_err(map_storage)?,
        policy: match value.policy.as_str() {
            "private" => BucketPolicy::Private,
            "publicRead" => BucketPolicy::PublicRead,
            _ => return Err(ManagementProductError::Invalid),
        },
        cors,
        versioning: value
            .versioning
            .parse::<Versioning>()
            .map_err(map_storage)?,
        lifecycle: BucketLifecycle {
            expire_current_after_days: value.lifecycle.expire_current_after_days,
            expire_noncurrent_after_days: value.lifecycle.expire_noncurrent_after_days,
            abort_incomplete_after_days: value.lifecycle.abort_incomplete_after_days,
        },
        quota: BucketQuota {
            max_object_bytes: parse_canonical_u64(&value.quota.max_object_bytes)?,
            max_total_bytes: parse_canonical_u64(&value.quota.max_total_bytes)?,
            max_objects: parse_canonical_u64(&value.quota.max_objects)?,
        },
    };
    configuration.validate().map_err(map_storage)?;
    Ok(configuration)
}

fn storage_access_key_configuration(
    value: &ManagementStorageAccessKeyConfiguration,
) -> Result<AccessKeyConfiguration, ManagementProductError> {
    let operations = value
        .operations
        .iter()
        .map(|operation| operation.parse::<AccessKeyOperation>().map_err(map_storage))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if operations.len() != value.operations.len() {
        return Err(ManagementProductError::Invalid);
    }
    let configuration = AccessKeyConfiguration {
        label: value.label.clone(),
        prefix: value.prefix.clone(),
        operations,
    };
    configuration.validate().map_err(map_storage)?;
    Ok(configuration)
}

fn management_storage_key(metadata: &AccessKeyMetadata) -> ManagementStorageAccessKey {
    ManagementStorageAccessKey {
        access_key_id: metadata.id.to_string(),
        bucket_id: metadata.bucket_id.to_string(),
        configuration: ManagementStorageAccessKeyConfiguration {
            label: metadata.configuration.label.clone(),
            prefix: metadata.configuration.prefix.clone(),
            operations: metadata
                .configuration
                .operations
                .iter()
                .map(|operation| operation.as_str().to_owned())
                .collect(),
        },
        revision: metadata.revision,
        state: metadata.state.as_str().to_owned(),
        created_at_micros: metadata.created_at.get().to_string(),
        updated_at_micros: metadata.updated_at.get().to_string(),
        previous_generation_valid_until_micros: metadata
            .previous_generation_valid_until
            .map(|value| value.get().to_string()),
    }
}

fn management_issued_storage_key(
    issued: &runku_object_storage::IssuedAccessKey,
    operation_id: OperationId,
) -> ManagementIssuedStorageAccessKey {
    let secret = issued
        .secret
        .as_ref()
        .map(|value| value.expose().to_owned());
    ManagementIssuedStorageAccessKey {
        key: management_storage_key(&issued.metadata),
        secret,
        operation_id: operation_id.to_string(),
        replayed: issued.replayed,
    }
}

fn management_storage_operation(operation: &ObjectStorageOperation) -> ManagementStorageOperation {
    ManagementStorageOperation {
        operation_id: operation.operation_id.to_string(),
        kind: match operation.kind {
            ObjectStorageOperationKind::CreateBucket => "createBucket",
            ObjectStorageOperationKind::UpdateBucket => "updateBucket",
            ObjectStorageOperationKind::ArchiveBucket => "archiveBucket",
            ObjectStorageOperationKind::IssueAccessKey => "issueAccessKey",
            ObjectStorageOperationKind::RotateAccessKey => "rotateAccessKey",
            ObjectStorageOperationKind::RevokeAccessKey => "revokeAccessKey",
        }
        .to_owned(),
        bucket_id: operation.bucket_id.to_string(),
        access_key_id: operation.access_key_id.map(|value| value.to_string()),
        revision: operation.revision,
        completed_at_micros: operation.completed_at.get().to_string(),
    }
}

fn parse_canonical_u64(value: &str) -> Result<u64, ManagementProductError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| ManagementProductError::Invalid)?;
    if parsed.to_string() != value {
        return Err(ManagementProductError::Invalid);
    }
    Ok(parsed)
}

fn storage_actor(actor: OperatorId) -> Result<ObjectStorageActor, ManagementProductError> {
    actor.to_string().parse().map_err(map_storage)
}

fn outcome(value: LocalReleaseOutcome) -> ManagementReleaseOutcome {
    ManagementReleaseOutcome {
        release_id: value.release_id.to_string(),
        channel: value.channel.map(|channel| channel.to_string()),
        status: value.status.as_str().to_owned(),
        serving_revision: value.serving_revision,
        replayed: value.replayed,
        diagnostics: value
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code.to_owned())
            .collect(),
    }
}

fn status(value: LocalReleaseStatusReport) -> ManagementReleaseStatus {
    ManagementReleaseStatus {
        serving_revision: value.serving_revision,
        default_channel: value.default_channel.map(|channel| channel.to_string()),
        releases: value
            .releases
            .into_iter()
            .map(|release| {
                json!({
                    "releaseId": release.release_id.to_string(),
                    "runtimeVersion": release.runtime_version,
                    "status": release.status.as_str(),
                })
            })
            .collect(),
        channels: value
            .channels
            .into_iter()
            .map(|channel| {
                json!({
                    "channel": channel.channel.to_string(),
                    "default": channel.default,
                    "releaseId": channel.release_id.to_string(),
                })
            })
            .collect(),
    }
}

fn catalog_limit(limit: u16) -> Result<usize, ManagementProductError> {
    if !(1..=200).contains(&limit) {
        return Err(ManagementProductError::Invalid);
    }
    Ok(usize::from(limit))
}

fn data_limit(limit: u16) -> Result<usize, ManagementProductError> {
    catalog_limit(limit)
}

fn function_entry(
    function: &runku_releases::FunctionManifest,
    contracts: &BTreeMap<Sha256Digest, Contract>,
) -> Result<ManagementFunctionEntry, ManagementProductError> {
    let arguments = contracts
        .get(&function.arguments_contract_hash)
        .ok_or(ManagementProductError::Corruption)?;
    let result = contracts
        .get(&function.result_contract_hash)
        .ok_or(ManagementProductError::Corruption)?;
    Ok(ManagementFunctionEntry {
        function_id: function.id.to_string(),
        name: function.name.to_string(),
        kind: function_type(function.function_type).to_owned(),
        visibility: match function.visibility {
            FunctionVisibility::Public => "public",
            FunctionVisibility::Internal => "internal",
        }
        .to_owned(),
        auth: match function.auth_policy {
            AuthPolicy::None => "none",
            AuthPolicy::Optional => "optional",
            AuthPolicy::Guest => "guest",
            AuthPolicy::User => "user",
            AuthPolicy::Service => "service",
        }
        .to_owned(),
        runtime: match function.runtime_class {
            RuntimeClass::SafeV8 => "safe-v8",
            RuntimeClass::FullNode => "full-node",
        }
        .to_owned(),
        capabilities: function.capabilities.iter().map(capability).collect(),
        arguments: serde_json::to_value(arguments)
            .map_err(|_| ManagementProductError::Corruption)?,
        result: serde_json::to_value(result).map_err(|_| ManagementProductError::Corruption)?,
    })
}

fn capability(value: &Capability) -> String {
    match value {
        Capability::DbRead => "db:read".to_owned(),
        Capability::DbWrite => "db:write".to_owned(),
        Capability::AuthRead => "auth:read".to_owned(),
        Capability::FunctionQuery => "function:query".to_owned(),
        Capability::FunctionMutation => "function:mutation".to_owned(),
        Capability::FunctionAction => "function:action".to_owned(),
        Capability::NetworkHttps => "network:https".to_owned(),
        Capability::SchedulerCreate => "scheduler:create".to_owned(),
        Capability::FileRead => "storage:read".to_owned(),
        Capability::FileWrite => "storage:write".to_owned(),
        Capability::Secret(name) => format!("secret:{name}"),
    }
}

const fn function_type(value: FunctionType) -> &'static str {
    match value {
        FunctionType::Query => "query",
        FunctionType::Mutation => "mutation",
        FunctionType::Action => "action",
    }
}

fn schema_table(
    table: &runku_contracts::DocumentTableContract,
    indexes: &SchemaCatalog,
) -> Result<ManagementSchemaTable, ManagementProductError> {
    Ok(ManagementSchemaTable {
        table_id: table.id.to_string(),
        name: table.name.clone(),
        document: serde_json::to_value(&table.document_contract)
            .map_err(|_| ManagementProductError::Corruption)?,
        indexes: indexes
            .indexes_for_table(table.id)
            .map(|index| ManagementSchemaIndex {
                index_id: index.index_id().to_string(),
                name: index.name().to_owned(),
                fields: index
                    .fields()
                    .iter()
                    .map(|field| field.segments().to_vec())
                    .collect(),
            })
            .collect(),
    })
}

fn resolve_table<'a>(
    schema: &'a DocumentSchemaV1,
    name: &str,
) -> Result<&'a runku_contracts::DocumentTableContract, ManagementProductError> {
    schema
        .tables
        .iter()
        .find(|table| table.name == name)
        .ok_or(ManagementProductError::NotFound)
}

fn parse_revision(value: &str) -> Result<u64, ManagementProductError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| ManagementProductError::Invalid)?;
    if parsed == 0 || parsed.to_string() != value {
        return Err(ManagementProductError::Invalid);
    }
    Ok(parsed)
}

fn data_document(
    table: &str,
    document: &runku_data::DocumentRecord,
) -> Result<ManagementDataDocument, ManagementProductError> {
    Ok(ManagementDataDocument {
        table_id: document.table_id.to_string(),
        table: table.to_owned(),
        document_id: document.document_id.to_string(),
        revision: document.revision.to_string(),
        commit_sequence: document.commit_sequence.to_string(),
        created_at_micros: document.created_at.get().to_string(),
        updated_at_micros: document.updated_at.get().to_string(),
        value: WireValueV1::from_canonical(&document.value)
            .map_err(|_| ManagementProductError::Corruption)?,
    })
}

fn map_mutation_planning(error: runku_execution::MutationExecutionError) -> ManagementProductError {
    match error {
        runku_execution::MutationExecutionError::Storage(error) => map_store(error),
        runku_execution::MutationExecutionError::Schema(_) => ManagementProductError::Validation,
        runku_execution::MutationExecutionError::Runtime(_)
        | runku_execution::MutationExecutionError::Data(_)
        | runku_execution::MutationExecutionError::Schedule(_) => {
            ManagementProductError::Corruption
        }
    }
}

fn map_store(error: StoreError) -> ManagementProductError {
    match error {
        StoreError::OperationIdReused => ManagementProductError::OperationIdReused,
        StoreError::MutationConflict => ManagementProductError::Conflict,
        StoreError::NotFound => ManagementProductError::NotFound,
        StoreError::LimitExceeded
        | StoreError::InvalidRange
        | StoreError::EmptyBatch
        | StoreError::DuplicateMutation => ManagementProductError::Invalid,
        StoreError::Corruption | StoreError::MigrationFailed => ManagementProductError::Corruption,
        StoreError::Busy
        | StoreError::SerializationFailure
        | StoreError::ResultUncertain
        | StoreError::Unavailable
        | StoreError::LeaseLost
        | StoreError::OutboxLeaseLost
        | StoreError::ProductionBackendUnsupported
        | StoreError::Internal => ManagementProductError::Unavailable,
    }
}

fn parse_log_query(
    scope: EnvironmentScope,
    query: &ManagementLogQuery,
) -> Result<LogQuery, ManagementProductError> {
    Ok(LogQuery {
        scope,
        after: query
            .after
            .parse()
            .map_err(|_| ManagementProductError::Invalid)?,
        limit: query.limit,
        stream: query
            .stream
            .as_deref()
            .map(|value| match value {
                "platform" => Ok(LogStream::Platform),
                "function" => Ok(LogStream::Function),
                _ => Err(ManagementProductError::Invalid),
            })
            .transpose()?,
        minimum_level: query
            .level
            .as_deref()
            .map(|value| match value {
                "debug" => Ok(LogLevel::Debug),
                "info" => Ok(LogLevel::Info),
                "warn" => Ok(LogLevel::Warn),
                "error" => Ok(LogLevel::Error),
                _ => Err(ManagementProductError::Invalid),
            })
            .transpose()?,
        function_id: parse_optional(query.function_id.as_deref())?,
        request_id: parse_optional(query.request_id.as_deref())?,
        invocation_id: parse_optional(query.invocation_id.as_deref())?,
        client_id: parse_optional(query.client_id.as_deref())?,
        credential_id: parse_optional(query.credential_id.as_deref())?,
        release_id: parse_optional(query.release_id.as_deref())?,
    })
}

fn parse_optional<T: FromStr>(value: Option<&str>) -> Result<Option<T>, ManagementProductError> {
    value
        .map(T::from_str)
        .transpose()
        .map_err(|_| ManagementProductError::Invalid)
}

fn log_record(record: &SequencedOperationalEvent) -> Result<Value, ManagementProductError> {
    let event = &record.event;
    let fields = event
        .fields
        .as_ref()
        .map(WireValueV1::from_canonical)
        .transpose()
        .map_err(|_| ManagementProductError::Corruption)?;
    Ok(json!({
        "cursor": record.cursor.to_string(),
        "eventId": event.id.to_string(),
        "occurredAtMicros": event.occurred_at.get().to_string(),
        "projectId": event.scope.project_id().to_string(),
        "environmentId": event.scope.environment_id().to_string(),
        "requestId": event.request_id.to_string(),
        "invocationId": event.invocation_id.to_string(),
        "parentInvocationId": event.parent_invocation_id.map(|value| value.to_string()),
        "releaseId": event.release_id.to_string(),
        "devRevisionId": event.dev_revision_id.map(|value| value.to_string()),
        "functionId": event.function_id.to_string(),
        "functionName": event.function_name.to_string(),
        "functionType": match event.function_type {
            FunctionType::Query => "query",
            FunctionType::Mutation => "mutation",
            FunctionType::Action => "action",
        },
        "clientId": event.client_id.map(|value| value.to_string()),
        "credentialId": event.credential_id.map(|value| value.to_string()),
        "principalKind": event.principal_kind.as_str(),
        "stream": event.stream.as_str(),
        "level": event.level.as_str(),
        "eventKind": event.kind.as_str(),
        "message": event.message.as_ref().map(ToString::to_string),
        "fields": fields,
        "durationMicros": event.duration_micros.map(|value| value.to_string()),
        "outcomeCode": event.outcome_code.as_ref().map(runku_observability::OutcomeCode::as_str),
    }))
}

fn map_publish(error: LocalPublishError) -> ManagementProductError {
    match error {
        LocalPublishError::Conflict => ManagementProductError::Conflict,
        LocalPublishError::Unavailable => ManagementProductError::Unavailable,
        LocalPublishError::Corruption => ManagementProductError::Corruption,
        _ => ManagementProductError::Invalid,
    }
}

fn map_release(error: LocalReleaseError) -> ManagementProductError {
    match error {
        LocalReleaseError::InvalidRequest => ManagementProductError::Invalid,
        LocalReleaseError::NotFound => ManagementProductError::NotFound,
        LocalReleaseError::Conflict => ManagementProductError::Conflict,
        LocalReleaseError::Unavailable => ManagementProductError::Unavailable,
        LocalReleaseError::Corruption => ManagementProductError::Corruption,
    }
}

fn map_logs(error: LocalLogError) -> ManagementProductError {
    match error {
        LocalLogError::InvalidRequest | LocalLogError::InvalidState => {
            ManagementProductError::Invalid
        }
        LocalLogError::Unavailable => ManagementProductError::Unavailable,
        LocalLogError::Corruption => ManagementProductError::Corruption,
    }
}

fn map_product_database(error: StoreError) -> &'static str {
    match error {
        StoreError::Corruption => "SERVER_PRODUCT_DATABASE_SCOPE_CONFLICT",
        StoreError::MigrationFailed => "SERVER_PRODUCT_DATABASE_MIGRATION_FAILED",
        _ => "SERVER_PRODUCT_DATABASE_UNAVAILABLE",
    }
}

fn map_store_health(error: StoreError) -> ManagementProductError {
    match error {
        StoreError::Corruption | StoreError::MigrationFailed => ManagementProductError::Corruption,
        _ => ManagementProductError::Unavailable,
    }
}

const fn map_identity(error: LocalIdentityError) -> ManagementProductError {
    match error {
        LocalIdentityError::InvalidInput | LocalIdentityError::EntropyUnavailable => {
            ManagementProductError::Invalid
        }
        LocalIdentityError::NotFound => ManagementProductError::NotFound,
        LocalIdentityError::Conflict => ManagementProductError::Conflict,
        LocalIdentityError::Unavailable => ManagementProductError::Unavailable,
        LocalIdentityError::ResultUncertain => ManagementProductError::ResultUncertain,
        LocalIdentityError::InvalidState | LocalIdentityError::Corruption => {
            ManagementProductError::Corruption
        }
    }
}

const fn map_serving(error: ServingPolicyError) -> ManagementProductError {
    match error {
        ServingPolicyError::InvalidInput | ServingPolicyError::LimitExceeded => {
            ManagementProductError::Invalid
        }
        ServingPolicyError::IncompatibleContracts => ManagementProductError::Incompatible,
        ServingPolicyError::NotFound => ManagementProductError::NotFound,
        ServingPolicyError::Conflict => ManagementProductError::Conflict,
        ServingPolicyError::OperationIdReused => ManagementProductError::OperationIdReused,
        ServingPolicyError::Busy | ServingPolicyError::Unavailable => {
            ManagementProductError::Unavailable
        }
        ServingPolicyError::ResultUncertain => ManagementProductError::ResultUncertain,
        ServingPolicyError::Corruption
        | ServingPolicyError::Unsupported
        | ServingPolicyError::ProductionBackendUnsupported
        | ServingPolicyError::Internal => ManagementProductError::Corruption,
    }
}

const fn map_storage(error: ObjectStorageError) -> ManagementProductError {
    match error {
        ObjectStorageError::InvalidInput | ObjectStorageError::LimitExceeded => {
            ManagementProductError::Invalid
        }
        ObjectStorageError::NotFound => ManagementProductError::NotFound,
        ObjectStorageError::Conflict => ManagementProductError::Conflict,
        ObjectStorageError::OperationIdReused => ManagementProductError::OperationIdReused,
        ObjectStorageError::Busy | ObjectStorageError::Unavailable => {
            ManagementProductError::Unavailable
        }
        ObjectStorageError::ResultUncertain => ManagementProductError::ResultUncertain,
        ObjectStorageError::Corruption
        | ObjectStorageError::Unsupported
        | ObjectStorageError::ProductionBackendUnsupported
        | ObjectStorageError::Internal => ManagementProductError::Corruption,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::Path, time::Duration};

    use runku_build::{BuildMetadata, build_project};
    use runku_core::{ProjectId, WorkspaceRef};
    use runku_development::DevelopmentActor;
    use runku_local::{initialize_local, publish_local};
    use runku_protocol::{WireObjectEntryV1, WireValueV1};

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const SCHEMA: &str = r#"
import { defineSchema, defineTable, v } from "@runku/server"
export const note = v.object({
  body: v.string({ minBytes: 1, maxBytes: 200 }),
  rank: v.int64({ minimum: 0, maximum: 100 }),
})
export default defineSchema({
  notes: defineTable(note).index("by_rank", ["rank"]),
  archivedNotes: defineTable(note).index("by_rank", ["rank"]),
})
"#;

    const FUNCTIONS: &str = r#"
import { query, v } from "@runku/server"
export const list = query({
  auth: "none", visibility: "public", capabilities: ["db:read"],
  args: v.null(), returns: v.null(), async handler() { return null },
})
export const summary = query({
  auth: "none", visibility: "internal", capabilities: ["db:read"],
  args: v.null(), returns: v.null(), async handler() { return null },
})
"#;

    fn value(body: &str, rank: i64) -> WireValueV1 {
        WireValueV1::Object {
            value: vec![
                WireObjectEntryV1 {
                    key: "body".to_owned(),
                    value: WireValueV1::String {
                        value: body.to_owned(),
                    },
                },
                WireObjectEntryV1 {
                    key: "rank".to_owned(),
                    value: WireValueV1::Int64 {
                        value: rank.to_string(),
                    },
                },
            ],
        }
    }

    async fn adapter(root: &Path) -> TestResult {
        let workspace: WorkspaceRef = "local".parse()?;
        let (state, _) = initialize_local(
            root,
            workspace.clone(),
            "127.0.0.1:0".parse::<SocketAddr>()?,
            TimestampMicros::new(1_800_000_000_000_000),
        )
        .await?;
        std::fs::create_dir_all(root.join("runku"))?;
        std::fs::write(root.join("runku/schema.ts"), SCHEMA)?;
        std::fs::write(root.join("runku/functions.ts"), FUNCTIONS)?;
        let output = build_project(
            root,
            Path::new("runku"),
            state.project_id,
            BuildMetadata::generate(TimestampMicros::new(1_800_000_000_000_001)),
        )?;
        publish_local(
            root,
            &workspace,
            &DevelopmentActor::from_str("console-test")?,
            &std::fs::read(output.manifest_path)?,
            &std::fs::read(output.artifact_path)?,
        )
        .await?;
        Ok(())
    }

    async fn open_adapter(root: &Path) -> Result<ProductAdapter, &'static str> {
        Box::pin(ProductAdapter::open(
            root.to_path_buf(),
            ProductAdapterConfig {
                platform_database_url: None,
                log_archive: None,
                log_journal: None,
                allowed_origins: BTreeSet::new(),
                auth_config: None,
                file_object_store: None,
                file_storage_limits: FileStorageLimits::DEFAULT,
                file_usage_sink: None,
                file_usage_interval: Duration::from_secs(1),
            },
        ))
        .await
    }

    async fn product_database(
        base_url: &str,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        use sqlx::{Connection as _, PgConnection};

        let name = format!(
            "runku_console_{}",
            ProjectId::generate().to_string().trim_start_matches("prj_")
        );
        let mut admin_url = url::Url::parse(base_url)?;
        admin_url.set_path("/postgres");
        admin_url.set_query(None);
        let mut connection = PgConnection::connect(admin_url.as_str()).await?;
        let statement = format!("CREATE DATABASE \"{name}\"");
        // The identifier is generated from a lowercase ULID and cannot contain SQL syntax.
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(&mut connection)
            .await?;
        connection.close().await?;
        let mut product_url = url::Url::parse(base_url)?;
        product_url.set_path(&format!("/{name}"));
        product_url.set_query(None);
        Ok((name, product_url.to_string()))
    }

    async fn drop_product_database(
        base_url: &str,
        name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use sqlx::{Connection as _, PgConnection};

        let mut admin_url = url::Url::parse(base_url)?;
        admin_url.set_path("/postgres");
        admin_url.set_query(None);
        let mut connection = PgConnection::connect(admin_url.as_str()).await?;
        let statement = format!("DROP DATABASE \"{name}\" WITH (FORCE)");
        // The identifier is the exact value generated by `product_database` in this test.
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(&mut connection)
            .await?;
        connection.close().await?;
        Ok(())
    }

    fn test_bucket_configuration(policy: &str) -> ManagementBucketConfiguration {
        ManagementBucketConfiguration {
            name: "media-assets".to_owned(),
            policy: policy.to_owned(),
            cors: Vec::new(),
            versioning: "enabled".to_owned(),
            lifecycle: ManagementBucketLifecycle {
                expire_current_after_days: None,
                expire_noncurrent_after_days: Some(30),
                abort_incomplete_after_days: Some(7),
            },
            quota: ManagementBucketQuota {
                max_object_bytes: "1048576".to_owned(),
                max_total_bytes: "10485760".to_owned(),
                max_objects: "1000".to_owned(),
            },
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn console_bucket_and_storage_key_lifecycle_is_scoped_cas_and_one_time() -> TestResult {
        let directory = tempfile::tempdir()?;
        adapter(directory.path()).await?;
        let product = Box::pin(open_adapter(directory.path())).await?;
        let actor = OperatorId::generate();
        let create_operation = OperationId::generate();
        let create_request = ManagementBucketCreate {
            configuration: test_bucket_configuration("private"),
            at_micros: "1800000000000200".to_owned(),
        };
        let created = product
            .bucket_create(create_operation, actor, &create_request)
            .await?;
        assert_eq!(created.bucket.revision, 1);
        assert_eq!(created.bucket.configuration.policy, "private");
        assert!(!created.replayed);
        let replay = product
            .bucket_create(create_operation, actor, &create_request)
            .await?;
        assert!(replay.replayed);
        assert_eq!(replay.bucket.bucket_id, created.bucket.bucket_id);
        assert_eq!(product.buckets(None, 10).await?.buckets.len(), 1);

        let updated = product
            .bucket_update(
                &created.bucket.bucket_id,
                OperationId::generate(),
                actor,
                &ManagementBucketUpdate {
                    expected_revision: 1,
                    configuration: test_bucket_configuration("publicRead"),
                    at_micros: "1800000000000201".to_owned(),
                },
            )
            .await?;
        assert_eq!(updated.bucket.revision, 2);
        assert_eq!(updated.bucket.configuration.policy, "publicRead");

        let issue_operation = OperationId::generate();
        let issue_request = ManagementStorageAccessKeyIssue {
            configuration: ManagementStorageAccessKeyConfiguration {
                label: "uploader".to_owned(),
                prefix: "public/".to_owned(),
                operations: vec!["read".to_owned(), "write".to_owned()],
            },
            at_micros: "1800000000000202".to_owned(),
        };
        let issued = product
            .storage_access_key_issue(
                &created.bucket.bucket_id,
                issue_operation,
                actor,
                &issue_request,
            )
            .await?;
        assert!(
            issued
                .secret
                .as_deref()
                .is_some_and(|value| value.starts_with("rk_st_v1_"))
        );
        assert!(!issued.replayed);
        let issue_replay = product
            .storage_access_key_issue(
                &created.bucket.bucket_id,
                issue_operation,
                actor,
                &issue_request,
            )
            .await?;
        assert!(issue_replay.replayed);
        assert_eq!(issue_replay.secret, None);
        assert_eq!(issue_replay.key.access_key_id, issued.key.access_key_id);

        let rotated = product
            .storage_access_key_rotate(
                &created.bucket.bucket_id,
                &issued.key.access_key_id,
                OperationId::generate(),
                actor,
                &ManagementStorageAccessKeyRotate {
                    expected_revision: 1,
                    overlap_until_micros: "1800003600000203".to_owned(),
                    at_micros: "1800000000000203".to_owned(),
                },
            )
            .await?;
        assert_eq!(rotated.key.revision, 2);
        assert!(rotated.secret.is_some());
        let revoke_operation = OperationId::generate();
        let revoked = product
            .storage_access_key_revoke(
                &created.bucket.bucket_id,
                &issued.key.access_key_id,
                revoke_operation,
                actor,
                &ManagementStorageAccessKeyRevoke {
                    expected_revision: 2,
                    at_micros: "1800000000000204".to_owned(),
                },
            )
            .await?;
        assert_eq!(revoked.revision, 3);
        assert_eq!(
            product.storage_operation(revoke_operation).await?.kind,
            "revokeAccessKey"
        );
        let archived = product
            .bucket_archive(
                &created.bucket.bucket_id,
                OperationId::generate(),
                actor,
                &ManagementBucketArchive {
                    expected_revision: 2,
                    at_micros: "1800000000000205".to_owned(),
                },
            )
            .await?;
        assert_eq!(archived.bucket.state, "archived");
        assert_eq!(archived.bucket.revision, 3);
        assert_eq!(
            product
                .storage_access_keys(&created.bucket.bucket_id, None, 10)
                .await?
                .keys[0]
                .state,
            "revoked"
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn console_application_identity_preserves_one_time_secret_and_exact_ownership()
    -> TestResult {
        let directory = tempfile::tempdir()?;
        adapter(directory.path()).await?;
        let product = Box::pin(open_adapter(directory.path())).await?;
        let public_client = ApplicationClientId::generate();
        let public_request = ManagementApplicationClientCreate {
            client_id: public_client.to_string(),
            name: "browser".to_owned(),
            kind: "public".to_owned(),
            scopes: vec!["functions:invoke".to_owned()],
            created_at_micros: "1800000000000010".to_owned(),
        };
        let first_client = product.application_client_create(&public_request).await?;
        assert!(!first_client.replayed);
        assert!(
            product
                .application_client_create(&public_request)
                .await?
                .replayed
        );

        let public_credential = CredentialId::generate();
        let created = product
            .application_credential_create(
                &public_client.to_string(),
                &ManagementApplicationCredentialCreate {
                    credential_id: public_credential.to_string(),
                    label: "web".to_owned(),
                    scopes: vec!["functions:invoke".to_owned()],
                    expires_at_micros: None,
                    created_at_micros: "1800000000000020".to_owned(),
                },
            )
            .await?;
        assert!(created.key.starts_with("rk_pub_v1_"));
        assert!(created.recoverable);
        assert!(!created.secret_shown_once);
        assert_eq!(
            product
                .application_credential_reveal(
                    &public_client.to_string(),
                    &public_credential.to_string(),
                )
                .await?
                .key,
            created.key
        );

        let confidential_client = ApplicationClientId::generate();
        product
            .application_client_create(&ManagementApplicationClientCreate {
                client_id: confidential_client.to_string(),
                name: "worker".to_owned(),
                kind: "confidential".to_owned(),
                scopes: vec!["functions:invoke".to_owned()],
                created_at_micros: "1800000000000030".to_owned(),
            })
            .await?;
        let secret_credential = CredentialId::generate();
        let secret = product
            .application_credential_create(
                &confidential_client.to_string(),
                &ManagementApplicationCredentialCreate {
                    credential_id: secret_credential.to_string(),
                    label: "production".to_owned(),
                    scopes: vec!["functions:invoke".to_owned()],
                    expires_at_micros: None,
                    created_at_micros: "1800000000000040".to_owned(),
                },
            )
            .await?;
        assert!(secret.key.starts_with("rk_sec_v1_"));
        assert!(!secret.recoverable);
        assert!(secret.secret_shown_once);
        assert_eq!(
            product
                .application_credential_reveal(
                    &confidential_client.to_string(),
                    &secret_credential.to_string(),
                )
                .await,
            Err(ManagementProductError::Invalid)
        );
        assert_eq!(
            product
                .application_credential_revoke(
                    &public_client.to_string(),
                    &secret_credential.to_string(),
                    1_800_000_000_000_050,
                )
                .await,
            Err(ManagementProductError::NotFound)
        );
        let revoked = product
            .application_credential_revoke(
                &confidential_client.to_string(),
                &secret_credential.to_string(),
                1_800_000_000_000_050,
            )
            .await?;
        assert_eq!(revoked.status, "revoked");
        assert!(!revoked.replayed);
        let deleted = product
            .application_credential_delete(
                &confidential_client.to_string(),
                &secret_credential.to_string(),
                1_800_000_000_000_060,
            )
            .await?;
        assert_eq!(deleted.status, "deleted");
        assert_eq!(
            product
                .application_credentials(&confidential_client.to_string())
                .await?
                .credentials,
            Vec::new()
        );
        Ok(())
    }

    #[tokio::test]
    async fn console_serving_policy_uses_manifest_evidence_cas_and_operation_replay() -> TestResult
    {
        let directory = tempfile::tempdir()?;
        adapter(directory.path()).await?;
        let product = Box::pin(open_adapter(directory.path())).await?;
        let release_id = product
            .status()
            .await?
            .releases
            .first()
            .ok_or("missing release")?
            .get("releaseId")
            .and_then(Value::as_str)
            .ok_or("missing release ID")?
            .to_owned();
        let released = product.release(&release_id, None).await?;
        assert_eq!(released.status, "servable");
        let request = ManagementServingPolicySet {
            expected_revision: None,
            mode: "atomic".to_owned(),
            releases: vec![ManagementServingRelease {
                release_id,
                weight_percent: 100,
            }],
            changed_at_micros: "1800000000000100".to_owned(),
        };
        let operation_id = OperationId::generate();
        let actor = OperatorId::generate();
        let first = product
            .serving_policy_set(operation_id, actor, &request)
            .await?;
        assert_eq!(first.policy.policy_revision, 1);
        assert_eq!(first.policy.observed_state, "pending");
        assert!(!first.policy.converged);
        assert!(!first.replayed);
        let replay = product
            .serving_policy_set(operation_id, actor, &request)
            .await?;
        assert!(replay.replayed);
        assert_eq!(replay.policy, first.policy);
        assert_eq!(product.serving_policy().await?, first.policy);
        let operation = product.serving_operation(operation_id).await?;
        assert_eq!(operation.kind, "setDesired");
        assert_eq!(operation.policy_revision, 1);
        assert_eq!(operation.observed_state, "pending");
        assert_eq!(
            product
                .serving_policy_set(OperationId::generate(), actor, &request)
                .await,
            Err(ManagementProductError::Conflict)
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn console_catalog_and_data_admin_are_exact_idempotent_and_occ_safe() -> TestResult {
        let directory = tempfile::tempdir()?;
        adapter(directory.path()).await?;
        let product = Box::pin(open_adapter(directory.path())).await?;
        let catalog_query = ManagementCatalogQuery {
            target: "workspace:local".to_owned(),
            after: None,
            limit: 1,
        };
        let functions = product.functions(&catalog_query).await?;
        assert_eq!(functions.version, 1);
        assert_eq!(functions.functions[0].name, "functions.list");
        let function_cursor = functions.next.as_deref().ok_or("missing Function cursor")?;
        let remaining_functions = product
            .functions(&ManagementCatalogQuery {
                target: catalog_query.target.clone(),
                after: Some(function_cursor.to_owned()),
                limit: 1,
            })
            .await?;
        assert_eq!(remaining_functions.functions[0].name, "functions.summary");
        assert_eq!(remaining_functions.next, None);
        assert!(functions.target.resolved.starts_with("dev_revision:drv_"));
        let schema = product.schema_tables(&catalog_query).await?;
        let schema_cursor = schema.next.as_deref().ok_or("missing schema cursor")?;
        let remaining_schema = product
            .schema_tables(&ManagementCatalogQuery {
                target: catalog_query.target.clone(),
                after: Some(schema_cursor.to_owned()),
                limit: 1,
            })
            .await?;
        assert_eq!(remaining_schema.tables.len(), 1);
        assert_ne!(schema.tables[0].name, remaining_schema.tables[0].name);
        assert_eq!(schema.tables[0].indexes[0].name, "by_rank");
        assert_eq!(remaining_schema.next, None);
        assert_eq!(
            product
                .functions(&ManagementCatalogQuery {
                    target: catalog_query.target.clone(),
                    after: None,
                    limit: 0,
                })
                .await,
            Err(ManagementProductError::Invalid)
        );

        let operation = OperationId::generate();
        let original = value("first", 1);
        let inserted = product
            .data_insert(
                operation,
                "notes",
                &ManagementDataInsertRequest {
                    target: "workspace:local".to_owned(),
                    value: original.clone(),
                },
            )
            .await?;
        assert_eq!(inserted.revision.as_deref(), Some("1"));
        assert!(!inserted.replayed);
        let insert_replay = product
            .data_insert(
                operation,
                "notes",
                &ManagementDataInsertRequest {
                    target: "workspace:local".to_owned(),
                    value: original.clone(),
                },
            )
            .await?;
        assert!(insert_replay.replayed);
        assert_eq!(insert_replay.commit_sequence, inserted.commit_sequence);

        let loaded = product
            .data_get("workspace:local", "notes", &inserted.document_id)
            .await?;
        assert_eq!(loaded.value, original);
        assert_eq!(
            product
                .data_get("channel:missing", "notes", &inserted.document_id)
                .await,
            Err(ManagementProductError::NotFound)
        );
        assert_eq!(
            product
                .data_insert(
                    OperationId::generate(),
                    "notes",
                    &ManagementDataInsertRequest {
                        target: "workspace:local".to_owned(),
                        value: value("invalid", 101),
                    },
                )
                .await,
            Err(ManagementProductError::Validation)
        );
        let page = product
            .data_query(&ManagementDataQuery {
                target: "workspace:local".to_owned(),
                table: "notes".to_owned(),
                index: "by_rank".to_owned(),
                prefix: vec![WireValueV1::Int64 {
                    value: "1".to_owned(),
                }],
                limit: 20,
            })
            .await?;
        assert_eq!(page.documents.len(), 1);

        let replacement_operation = OperationId::generate();
        let replacement = value("second", 2);
        assert_eq!(
            product
                .data_replace(
                    OperationId::generate(),
                    "notes",
                    &inserted.document_id,
                    &ManagementDataReplaceRequest {
                        target: "workspace:local".to_owned(),
                        expected_revision: "1".to_owned(),
                        previous_value: value("forged", 1),
                        value: replacement.clone(),
                    },
                )
                .await,
            Err(ManagementProductError::Conflict)
        );
        let replaced = product
            .data_replace(
                replacement_operation,
                "notes",
                &inserted.document_id,
                &ManagementDataReplaceRequest {
                    target: "workspace:local".to_owned(),
                    expected_revision: "1".to_owned(),
                    previous_value: original.clone(),
                    value: replacement.clone(),
                },
            )
            .await?;
        assert_eq!(replaced.revision.as_deref(), Some("2"));
        assert!(
            product
                .data_replace(
                    replacement_operation,
                    "notes",
                    &inserted.document_id,
                    &ManagementDataReplaceRequest {
                        target: "workspace:local".to_owned(),
                        expected_revision: "1".to_owned(),
                        previous_value: original.clone(),
                        value: replacement.clone(),
                    },
                )
                .await?
                .replayed
        );
        assert_eq!(
            product
                .data_replace(
                    replacement_operation,
                    "notes",
                    &inserted.document_id,
                    &ManagementDataReplaceRequest {
                        target: "workspace:local".to_owned(),
                        expected_revision: "1".to_owned(),
                        previous_value: original,
                        value: value("changed intent", 3),
                    },
                )
                .await,
            Err(ManagementProductError::OperationIdReused)
        );

        let first_operation = OperationId::generate();
        let second_operation = OperationId::generate();
        let first_request = ManagementDataReplaceRequest {
            target: "workspace:local".to_owned(),
            expected_revision: "2".to_owned(),
            previous_value: replacement.clone(),
            value: value("winner one", 4),
        };
        let second_request = ManagementDataReplaceRequest {
            target: "workspace:local".to_owned(),
            expected_revision: "2".to_owned(),
            previous_value: replacement,
            value: value("winner two", 5),
        };
        let (first, second) = tokio::join!(
            product.data_replace(
                first_operation,
                "notes",
                &inserted.document_id,
                &first_request
            ),
            product.data_replace(
                second_operation,
                "notes",
                &inserted.document_id,
                &second_request
            )
        );
        assert!(matches!(
            (&first, &second),
            (Ok(_), Err(ManagementProductError::Conflict))
                | (Err(ManagementProductError::Conflict), Ok(_))
        ));
        let current = product
            .data_get("workspace:local", "notes", &inserted.document_id)
            .await?;
        let delete_operation = OperationId::generate();
        let deleted = product
            .data_delete(
                delete_operation,
                "notes",
                &inserted.document_id,
                &ManagementDataDeleteRequest {
                    target: "workspace:local".to_owned(),
                    expected_revision: current.revision.clone(),
                    previous_value: current.value.clone(),
                },
            )
            .await?;
        assert_eq!(deleted.revision, None);
        assert!(
            product
                .data_delete(
                    delete_operation,
                    "notes",
                    &inserted.document_id,
                    &ManagementDataDeleteRequest {
                        target: "workspace:local".to_owned(),
                        expected_revision: current.revision,
                        previous_value: current.value,
                    },
                )
                .await?
                .replayed
        );
        assert_eq!(
            product
                .data_get("workspace:local", "notes", &inserted.document_id)
                .await,
            Err(ManagementProductError::NotFound)
        );
        Ok(())
    }

    #[tokio::test]
    async fn console_data_admin_uses_the_same_postgres_logical_store_contract() -> TestResult {
        let Ok(base_url) = std::env::var("RUNKU_TEST_POSTGRES_URL") else {
            return Ok(());
        };
        let (database_name, database_url) = product_database(&base_url).await?;
        let result: TestResult = Box::pin(async {
            let directory = tempfile::tempdir()?;
            adapter(directory.path()).await?;
            let product = Box::pin(ProductAdapter::open(
                directory.path().to_path_buf(),
                ProductAdapterConfig {
                    platform_database_url: Some(Zeroizing::new(database_url)),
                    log_archive: None,
                    log_journal: None,
                    allowed_origins: BTreeSet::new(),
                    auth_config: None,
                    file_object_store: None,
                    file_storage_limits: FileStorageLimits::DEFAULT,
                    file_usage_sink: None,
                    file_usage_interval: Duration::from_secs(1),
                },
            ))
            .await?;
            let value = value("postgres", 8);
            let inserted = product
                .data_insert(
                    OperationId::generate(),
                    "notes",
                    &ManagementDataInsertRequest {
                        target: "workspace:local".to_owned(),
                        value: value.clone(),
                    },
                )
                .await?;
            let loaded = product
                .data_get("workspace:local", "notes", &inserted.document_id)
                .await?;
            assert_eq!(loaded.value, value);
            let page = product
                .data_query(&ManagementDataQuery {
                    target: "workspace:local".to_owned(),
                    table: "notes".to_owned(),
                    index: "by_rank".to_owned(),
                    prefix: vec![WireValueV1::Int64 {
                        value: "8".to_owned(),
                    }],
                    limit: 1,
                })
                .await?;
            assert_eq!(page.documents.len(), 1);
            Ok(())
        })
        .await;
        drop_product_database(&base_url, &database_name).await?;
        result
    }
}
