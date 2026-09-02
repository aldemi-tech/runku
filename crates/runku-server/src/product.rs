//! Product Base adapter behind the authenticated Management API.

use std::{collections::BTreeSet, path::PathBuf, str::FromStr};

use async_trait::async_trait;
use runku_core::{ChannelName, EnvironmentScope, ReleaseId};
use runku_development::DevelopmentActor;
use runku_gateway::CorsOrigin;
use runku_local::{
    LocalChannelExpectation, LocalLogError, LocalLogManager, LocalProcess, LocalProcessConfig,
    LocalPublishError, LocalReleaseError, LocalReleaseManager, LocalReleaseOutcome,
    LocalReleaseStatusReport, load_local, publish_local_if_head,
};
use runku_management_service::{
    ManagementLogArchiveStatus, ManagementLogPage, ManagementLogPruneRequest,
    ManagementLogPruneResult, ManagementLogQuery, ManagementProduct, ManagementProductError,
    ManagementReleaseOutcome, ManagementReleaseStatus, ManagementWorkspacePublish,
};
use runku_observability::{
    LogArchive, LogLevel, LogQuery, LogStream, NatsLogJournal, SequencedOperationalEvent,
};
use runku_protocol::{WireValueV1, decode_development_publish_request_v1};
use runku_releases::FunctionType;
use runku_value::TimestampMicros;
use serde_json::{Value, json};
use tokio::sync::Mutex;

/// One configured Product Environment and its lazily started serving process.
pub struct ProductAdapter {
    root: PathBuf,
    scope: EnvironmentScope,
    log_archive: Option<LogArchive>,
    log_journal: Option<NatsLogJournal>,
    process_config: LocalProcessConfig,
    process: Mutex<Option<LocalProcess>>,
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
    pub async fn open(
        root: PathBuf,
        log_archive: Option<LogArchive>,
        log_journal: Option<NatsLogJournal>,
        allowed_origins: BTreeSet<CorsOrigin>,
        auth_config: Option<PathBuf>,
    ) -> Result<Self, &'static str> {
        let state = load_local(&root)
            .await
            .map_err(|_| "SERVER_PRODUCT_ROOT_INVALID")?
            .0;
        let adapter = Self {
            root,
            scope: state.scope(),
            log_archive,
            log_journal,
            process_config: LocalProcessConfig {
                allowed_origins,
                auth_config,
                ..LocalProcessConfig::default()
            },
            process: Mutex::new(None),
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
}

#[async_trait]
impl ManagementProduct for ProductAdapter {
    fn scope(&self) -> EnvironmentScope {
        self.scope
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
