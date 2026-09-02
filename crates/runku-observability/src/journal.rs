//! Replicated NATS `JetStream` journal for accepted Operational Log records.

use std::{collections::BTreeMap, fmt, str::FromStr, sync::Arc, time::Duration};

use async_nats::jetstream::{
    self, AckKind,
    consumer::{AckPolicy, DeliverPolicy, PullConsumer, ReplayPolicy, pull},
    stream::{DiscardPolicy, RetentionPolicy, StorageType},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt as _;
use runku_core::{EnvironmentScope, FunctionName};
use runku_releases::FunctionType;
use runku_value::{TimestampMicros, decode_stored_value, encode_stored_value};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    LogArchive, LogCursor, LogEventKind, LogLevel, LogMessage, LogPrincipalKind, LogQuery,
    LogRepository, LogRepositoryError, LogStream, OperationalEventV1, OutcomeCode,
    SequencedOperationalEvent,
};

const JOURNAL_FORMAT_VERSION: u8 = 1;
const JOURNAL_MESSAGE_MAX_BYTES: usize = 1_048_576;
const JOURNAL_ARCHIVE_MAX_BATCH: usize = 256;
const JOURNAL_ARCHIVE_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Sanitized replicated journal failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LogJournalError {
    /// Configuration, subject, cursor, or event violates the journal contract.
    #[error("operational log journal request is invalid")]
    Invalid,
    /// The bounded stream rejected new admission.
    #[error("operational log journal is full")]
    Full,
    /// NATS or `JetStream` is temporarily unavailable.
    #[error("operational log journal is unavailable")]
    Unavailable,
    /// A persisted journal payload violates the versioned contract.
    #[error("operational log journal payload is corrupt")]
    Corruption,
}

impl LogJournalError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Invalid => "LOG_JOURNAL_INVALID",
            Self::Full => "LOG_JOURNAL_FULL",
            Self::Unavailable => "LOG_JOURNAL_UNAVAILABLE",
            Self::Corruption => "LOG_JOURNAL_CORRUPT",
        }
    }
}

/// Durable stream and archive-consumer policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsLogJournalConfig {
    /// `JetStream` stream name.
    pub stream_name: String,
    /// Subject namespace owned by Operational Logs.
    pub subject_prefix: String,
    /// Durable archive consumer name shared by archive-worker replicas.
    pub consumer_name: String,
    /// Maximum admitted messages before publishers receive backpressure.
    pub max_messages: i64,
    /// Maximum aggregate persisted bytes.
    pub max_bytes: i64,
    /// Maximum age before unconsumed records expire.
    pub max_age: Duration,
    /// Stream replica count; production HA requires at least three.
    pub replicas: usize,
    /// Redelivery window for an archive worker.
    pub ack_wait: Duration,
    /// Maximum outstanding archive records.
    pub max_ack_pending: i64,
}

impl Default for NatsLogJournalConfig {
    fn default() -> Self {
        Self {
            stream_name: "RUNKU_LOGS".to_owned(),
            subject_prefix: "runku.logs.v1".to_owned(),
            consumer_name: "runku_log_archive_v1".to_owned(),
            max_messages: 1_000_000,
            max_bytes: 10_737_418_240,
            max_age: Duration::from_hours(168),
            replicas: 1,
            ack_wait: Duration::from_secs(120),
            max_ack_pending: 1_000,
        }
    }
}

/// Shared replicated journal. TLS and authentication are enforced by the supplied NATS client.
#[derive(Clone)]
pub struct NatsLogJournal {
    context: jetstream::Context,
    stream: jetstream::stream::Stream,
    consumer: PullConsumer,
    config: NatsLogJournalConfig,
}

impl fmt::Debug for NatsLogJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsLogJournal")
            .field("stream_name", &self.config.stream_name)
            .field("subject_prefix", &self.config.subject_prefix)
            .field("consumer_name", &self.config.consumer_name)
            .finish_non_exhaustive()
    }
}

impl NatsLogJournal {
    /// Opens or creates the exact durable stream and archive consumer.
    ///
    /// # Errors
    ///
    /// Rejects unsafe or drifted configuration and unavailable `JetStream` state.
    pub async fn open(
        client: async_nats::Client,
        config: NatsLogJournalConfig,
    ) -> Result<Self, LogJournalError> {
        validate_config(&config)?;
        let context = jetstream::new(client);
        let max_message_size =
            i32::try_from(JOURNAL_MESSAGE_MAX_BYTES).map_err(|_| LogJournalError::Invalid)?;
        let subject = format!("{}.>", config.subject_prefix);
        let stream = context
            .get_or_create_stream(jetstream::stream::Config {
                name: config.stream_name.clone(),
                description: Some("Runku replicated Operational Log journal".to_owned()),
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                max_messages: config.max_messages,
                max_bytes: config.max_bytes,
                max_age: config.max_age,
                max_message_size,
                discard: DiscardPolicy::New,
                storage: StorageType::File,
                num_replicas: config.replicas,
                duplicate_window: config.max_age.min(Duration::from_secs(120)),
                ..Default::default()
            })
            .await
            .map_err(|_| LogJournalError::Unavailable)?;
        let existing = &stream.cached_info().config;
        if existing.subjects != [subject.clone()]
            || existing.retention != RetentionPolicy::WorkQueue
            || existing.discard != DiscardPolicy::New
            || existing.storage != StorageType::File
            || existing.num_replicas != config.replicas
            || existing.max_messages != config.max_messages
            || existing.max_bytes != config.max_bytes
            || existing.max_age != config.max_age
            || existing.max_message_size != max_message_size
        {
            return Err(LogJournalError::Invalid);
        }
        let consumer = stream
            .get_or_create_consumer(
                &config.consumer_name,
                pull::Config {
                    durable_name: Some(config.consumer_name.clone()),
                    name: Some(config.consumer_name.clone()),
                    description: Some("Runku Parquet archive workers".to_owned()),
                    deliver_policy: DeliverPolicy::All,
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: config.ack_wait,
                    filter_subject: subject,
                    replay_policy: ReplayPolicy::Instant,
                    max_ack_pending: config.max_ack_pending,
                    max_batch: i64::try_from(JOURNAL_ARCHIVE_MAX_BATCH)
                        .map_err(|_| LogJournalError::Invalid)?,
                    max_bytes: i64::try_from(JOURNAL_ARCHIVE_MAX_BATCH_BYTES)
                        .map_err(|_| LogJournalError::Invalid)?,
                    max_expires: config.ack_wait,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| LogJournalError::Unavailable)?;
        let existing = &consumer.cached_info().config;
        if existing.durable_name.as_deref() != Some(config.consumer_name.as_str())
            || existing.ack_policy != AckPolicy::Explicit
            || existing.ack_wait != config.ack_wait
            || existing.filter_subject != format!("{}.>", config.subject_prefix)
            || existing.max_ack_pending != config.max_ack_pending
        {
            return Err(LogJournalError::Invalid);
        }
        Ok(Self {
            context,
            stream,
            consumer,
            config,
        })
    }

    /// Publishes one sequenced record and waits for the `JetStream` persistence acknowledgement.
    ///
    /// # Errors
    ///
    /// Rejects invalid records and reports bounded admission or availability failures.
    pub async fn publish(&self, record: &SequencedOperationalEvent) -> Result<(), LogJournalError> {
        if record.cursor == LogCursor::START {
            return Err(LogJournalError::Invalid);
        }
        record
            .event
            .validate()
            .map_err(|_| LogJournalError::Invalid)?;
        let payload = encode_record(record)?;
        if payload.len() > JOURNAL_MESSAGE_MAX_BYTES {
            return Err(LogJournalError::Invalid);
        }
        self.context
            .send_publish(
                subject_for(&self.config.subject_prefix, record.event.scope),
                jetstream::message::PublishMessage::build()
                    .payload(payload.into())
                    .message_id(record.event.id.to_string()),
            )
            .await
            .map_err(|_| LogJournalError::Unavailable)?
            .await
            .map_err(map_publish_error)?;
        Ok(())
    }

    /// Pulls at most one record for an archive worker.
    ///
    /// # Errors
    ///
    /// Rejects invalid wait bounds and terminates corrupt persisted messages.
    pub async fn pull(
        &self,
        wait: Duration,
    ) -> Result<Option<LogJournalDelivery>, LogJournalError> {
        Ok(self.pull_batch(wait, 1).await?.pop())
    }

    async fn pull_batch(
        &self,
        wait: Duration,
        maximum: usize,
    ) -> Result<Vec<LogJournalDelivery>, LogJournalError> {
        if wait.is_zero() || wait > self.config.ack_wait {
            return Err(LogJournalError::Invalid);
        }
        if !(1..=JOURNAL_ARCHIVE_MAX_BATCH).contains(&maximum) {
            return Err(LogJournalError::Invalid);
        }
        let mut messages = self
            .consumer
            .fetch()
            .max_messages(maximum)
            .max_bytes(JOURNAL_ARCHIVE_MAX_BATCH_BYTES)
            .expires(wait)
            .messages()
            .await
            .map_err(|_| LogJournalError::Unavailable)?;
        let mut deliveries = Vec::with_capacity(maximum);
        while let Some(message) = messages.next().await {
            let message = message.map_err(|_| LogJournalError::Unavailable)?;
            let record = match decode_record(&message.payload) {
                Ok(record)
                    if message.subject.as_str()
                        == subject_for(&self.config.subject_prefix, record.event.scope) =>
                {
                    record
                }
                _ => {
                    message
                        .ack_with(AckKind::Term)
                        .await
                        .map_err(|_| LogJournalError::Unavailable)?;
                    return Err(LogJournalError::Corruption);
                }
            };
            deliveries.push(LogJournalDelivery { message, record });
        }
        Ok(deliveries)
    }

    /// Returns the last cached stream state for bounded operator telemetry.
    #[must_use]
    pub fn stream_name(&self) -> &str {
        self.stream.cached_info().config.name.as_str()
    }
}

/// One explicitly acknowledged journal record.
pub struct LogJournalDelivery {
    message: jetstream::Message,
    record: SequencedOperationalEvent,
}

impl fmt::Debug for LogJournalDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogJournalDelivery")
            .field("cursor", &self.record.cursor)
            .field("scope", &self.record.event.scope)
            .finish_non_exhaustive()
    }
}

impl LogJournalDelivery {
    /// Record that remains pending until this delivery is acknowledged.
    #[must_use]
    pub const fn record(&self) -> &SequencedOperationalEvent {
        &self.record
    }

    /// Confirms processing to the replicated stream and waits for the acknowledgement round trip.
    ///
    /// # Errors
    ///
    /// Reports an unavailable acknowledgement path; callers must treat the effect as uncertain.
    pub async fn ack(self) -> Result<(), LogJournalError> {
        self.message
            .double_ack()
            .await
            .map_err(|_| LogJournalError::Unavailable)
    }

    /// Requests redelivery after an optional bounded delay.
    ///
    /// # Errors
    ///
    /// Reports an unavailable acknowledgement path.
    pub async fn retry(self, delay: Option<Duration>) -> Result<(), LogJournalError> {
        if delay.is_some_and(|value| value > Duration::from_secs(300)) {
            return Err(LogJournalError::Invalid);
        }
        self.message
            .ack_with(AckKind::Nak(delay))
            .await
            .map_err(|_| LogJournalError::Unavailable)
    }
}

/// Result of one bounded hot-repository to replicated-journal cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalForwardOutcome {
    /// No source record follows the in-process forwarding cursor.
    Idle {
        /// Last source cursor confirmed by `JetStream`.
        through: LogCursor,
    },
    /// A bounded page was confirmed by `JetStream`.
    Forwarded {
        /// Number of newly confirmed source records.
        records: u16,
        /// Last source cursor confirmed by `JetStream`.
        through: LogCursor,
    },
}

/// Bounded forwarder from one exact hot repository scope into `JetStream`.
///
/// The cursor is intentionally process-local. A process may resume from the verified immutable
/// archive frontier; any still-unarchived source tail is safely replayed through stable event IDs.
#[derive(Debug)]
pub struct LogJournalForwarder {
    source: Arc<dyn LogRepository>,
    journal: NatsLogJournal,
    scope: EnvironmentScope,
    through: LogCursor,
    maximum_batch: u16,
}

impl LogJournalForwarder {
    /// Creates a forwarder for one exact Environment.
    ///
    /// # Errors
    ///
    /// Rejects zero or oversized batches.
    pub fn new(
        source: Arc<dyn LogRepository>,
        journal: NatsLogJournal,
        scope: EnvironmentScope,
        maximum_batch: u16,
    ) -> Result<Self, LogJournalError> {
        Self::resume_after(source, journal, scope, maximum_batch, LogCursor::START)
    }

    /// Creates a forwarder from a previously verified archive frontier.
    ///
    /// # Errors
    ///
    /// Rejects zero or oversized batches. Callers must obtain `through` from the immutable archive,
    /// never from an unverified local checkpoint.
    pub fn resume_after(
        source: Arc<dyn LogRepository>,
        journal: NatsLogJournal,
        scope: EnvironmentScope,
        maximum_batch: u16,
        through: LogCursor,
    ) -> Result<Self, LogJournalError> {
        if !(1..=1_000).contains(&maximum_batch) {
            return Err(LogJournalError::Invalid);
        }
        Ok(Self {
            source,
            journal,
            scope,
            through,
            maximum_batch,
        })
    }

    /// Publishes one bounded page in source-cursor order and advances after each `PubAck`.
    ///
    /// # Errors
    ///
    /// Leaves the first unconfirmed record eligible for retry after source/journal failure.
    pub async fn run_once(&mut self) -> Result<JournalForwardOutcome, LogJournalError> {
        let page = self
            .source
            .query(&LogQuery {
                scope: self.scope,
                after: self.through,
                limit: self.maximum_batch,
                stream: None,
                minimum_level: None,
                function_id: None,
                request_id: None,
                invocation_id: None,
                client_id: None,
                credential_id: None,
                release_id: None,
            })
            .await
            .map_err(|_| LogJournalError::Unavailable)?;
        if page.records.is_empty() {
            return Ok(JournalForwardOutcome::Idle {
                through: self.through,
            });
        }
        let mut forwarded = 0_u16;
        for record in &page.records {
            self.journal.publish(record).await?;
            self.through = record.cursor;
            forwarded = forwarded.checked_add(1).ok_or(LogJournalError::Invalid)?;
        }
        Ok(JournalForwardOutcome::Forwarded {
            records: forwarded,
            through: self.through,
        })
    }
}

/// Result of one replicated-journal to immutable-archive cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalArchiveOutcome {
    /// No journal delivery arrived within the bounded wait.
    Idle,
    /// One bounded batch was verified, committed where needed, and acknowledged.
    Processed {
        /// Unique records newly committed to Parquet.
        archived_records: u16,
        /// Unique replayed records verified against the committed archive.
        replayed_records: u16,
        /// Environment-scoped immutable Parquet segments committed by this batch.
        segments: u16,
    },
}

/// Archive worker core that ACKs journal records only after an immutable manifest is visible.
#[derive(Clone, Debug)]
pub struct LogJournalArchiver {
    journal: NatsLogJournal,
    archive: LogArchive,
}

impl LogJournalArchiver {
    /// Creates a replicated-journal archive worker.
    #[must_use]
    pub const fn new(journal: NatsLogJournal, archive: LogArchive) -> Self {
        Self { journal, archive }
    }

    /// Processes one bounded delivery batch, enforcing per-Environment contiguous cursors.
    ///
    /// # Errors
    ///
    /// Requests redelivery on a gap or object-store failure. An acknowledgement failure after a
    /// successful commit is safe because replay validates the same immutable cursor/event.
    pub async fn run_once(&self, wait: Duration) -> Result<JournalArchiveOutcome, LogJournalError> {
        let deliveries = self
            .journal
            .pull_batch(wait, JOURNAL_ARCHIVE_MAX_BATCH)
            .await?;
        if deliveries.is_empty() {
            return Ok(JournalArchiveOutcome::Idle);
        }
        let mut groups: BTreeMap<EnvironmentScope, Vec<LogJournalDelivery>> = BTreeMap::new();
        for delivery in deliveries {
            groups
                .entry(delivery.record.event.scope)
                .or_default()
                .push(delivery);
        }
        let mut archived_records = 0_u16;
        let mut replayed_records = 0_u16;
        let mut segments = 0_u16;
        for (scope, group) in groups {
            match self.process_group(scope, &group).await {
                Ok((archived, replayed, committed_segment)) => {
                    for delivery in group {
                        delivery.ack().await?;
                    }
                    archived_records = archived_records
                        .checked_add(archived)
                        .ok_or(LogJournalError::Invalid)?;
                    replayed_records = replayed_records
                        .checked_add(replayed)
                        .ok_or(LogJournalError::Invalid)?;
                    segments = segments
                        .checked_add(u16::from(committed_segment))
                        .ok_or(LogJournalError::Invalid)?;
                }
                Err(error) => {
                    retry_group(group).await?;
                    return Err(error);
                }
            }
        }
        Ok(JournalArchiveOutcome::Processed {
            archived_records,
            replayed_records,
            segments,
        })
    }

    async fn process_group(
        &self,
        scope: EnvironmentScope,
        group: &[LogJournalDelivery],
    ) -> Result<(u16, u16, bool), LogJournalError> {
        let status = self
            .archive
            .status(scope)
            .await
            .map_err(map_archive_error)?;
        let mut unique = BTreeMap::<u64, SequencedOperationalEvent>::new();
        for delivery in group {
            let record = delivery.record();
            match unique.get(&record.cursor.get()) {
                Some(existing) if existing != record => return Err(LogJournalError::Corruption),
                Some(_) => {}
                None => {
                    unique.insert(record.cursor.get(), record.clone());
                }
            }
        }
        let mut next = status
            .through
            .get()
            .checked_add(1)
            .ok_or(LogJournalError::Corruption)?;
        let mut archived = Vec::new();
        let mut replayed = 0_u16;
        for record in unique.values() {
            if record.cursor <= status.through {
                self.verify_replay(record).await?;
                replayed = replayed.checked_add(1).ok_or(LogJournalError::Invalid)?;
            } else {
                if record.cursor.get() != next {
                    return Err(LogJournalError::Corruption);
                }
                next = next.checked_add(1).ok_or(LogJournalError::Corruption)?;
                archived.push(record.clone());
            }
        }
        if !archived.is_empty() {
            self.archive
                .commit(&archived)
                .await
                .map_err(map_archive_error)?;
        }
        Ok((
            u16::try_from(archived.len()).map_err(|_| LogJournalError::Invalid)?,
            replayed,
            !archived.is_empty(),
        ))
    }

    async fn verify_replay(
        &self,
        record: &SequencedOperationalEvent,
    ) -> Result<(), LogJournalError> {
        let query = LogQuery {
            scope: record.event.scope,
            after: LogCursor::new(record.cursor.get().saturating_sub(1)),
            limit: 1,
            stream: None,
            minimum_level: None,
            function_id: None,
            request_id: None,
            invocation_id: None,
            client_id: None,
            credential_id: None,
            release_id: None,
        };
        let (records, _) = self
            .archive
            .query(&query)
            .await
            .map_err(map_archive_error)?;
        if records.first() != Some(record) {
            return Err(LogJournalError::Corruption);
        }
        Ok(())
    }
}

async fn retry_group(group: Vec<LogJournalDelivery>) -> Result<(), LogJournalError> {
    for delivery in group {
        delivery.retry(Some(Duration::from_secs(1))).await?;
    }
    Ok(())
}

const fn map_archive_error(error: LogRepositoryError) -> LogJournalError {
    match error {
        LogRepositoryError::Corruption
        | LogRepositoryError::InvalidRequest
        | LogRepositoryError::LimitExceeded
        | LogRepositoryError::Unsupported => LogJournalError::Corruption,
        LogRepositoryError::Unavailable => LogJournalError::Unavailable,
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JournalWire {
    format_version: u8,
    sequence: u64,
    event_id: String,
    project_id: String,
    environment_id: String,
    occurred_at_micros: i64,
    request_id: String,
    invocation_id: String,
    parent_invocation_id: Option<String>,
    release_id: String,
    dev_revision_id: Option<String>,
    function_id: String,
    function_name: String,
    function_type: String,
    client_id: Option<String>,
    credential_id: Option<String>,
    principal_kind: String,
    stream: String,
    level: String,
    event_kind: String,
    message: Option<String>,
    fields_base64url: Option<String>,
    duration_micros: Option<u64>,
    outcome_code: Option<String>,
}

fn encode_record(record: &SequencedOperationalEvent) -> Result<Vec<u8>, LogJournalError> {
    let event = &record.event;
    let fields_base64url = event
        .fields
        .as_ref()
        .map(encode_stored_value)
        .transpose()
        .map_err(|_| LogJournalError::Invalid)?
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes));
    serde_json::to_vec(&JournalWire {
        format_version: JOURNAL_FORMAT_VERSION,
        sequence: record.cursor.get(),
        event_id: event.id.to_string(),
        project_id: event.scope.project_id().to_string(),
        environment_id: event.scope.environment_id().to_string(),
        occurred_at_micros: event.occurred_at.get(),
        request_id: event.request_id.to_string(),
        invocation_id: event.invocation_id.to_string(),
        parent_invocation_id: event.parent_invocation_id.map(|value| value.to_string()),
        release_id: event.release_id.to_string(),
        dev_revision_id: event.dev_revision_id.map(|value| value.to_string()),
        function_id: event.function_id.to_string(),
        function_name: event.function_name.as_str().to_owned(),
        function_type: function_type_text(event.function_type).to_owned(),
        client_id: event.client_id.map(|value| value.to_string()),
        credential_id: event.credential_id.map(|value| value.to_string()),
        principal_kind: event.principal_kind.as_str().to_owned(),
        stream: event.stream.as_str().to_owned(),
        level: event.level.as_str().to_owned(),
        event_kind: event.kind.as_str().to_owned(),
        message: event
            .message
            .as_ref()
            .map(|value| value.as_str().to_owned()),
        fields_base64url,
        duration_micros: event.duration_micros,
        outcome_code: event
            .outcome_code
            .as_ref()
            .map(|value| value.as_str().to_owned()),
    })
    .map_err(|_| LogJournalError::Invalid)
}

fn decode_record(payload: &[u8]) -> Result<SequencedOperationalEvent, LogJournalError> {
    if payload.is_empty() || payload.len() > JOURNAL_MESSAGE_MAX_BYTES {
        return Err(LogJournalError::Corruption);
    }
    let wire: JournalWire =
        serde_json::from_slice(payload).map_err(|_| LogJournalError::Corruption)?;
    if wire.format_version != JOURNAL_FORMAT_VERSION || wire.sequence == 0 {
        return Err(LogJournalError::Corruption);
    }
    let event = OperationalEventV1 {
        id: parse(&wire.event_id)?,
        occurred_at: TimestampMicros::new(wire.occurred_at_micros),
        scope: EnvironmentScope::new(parse(&wire.project_id)?, parse(&wire.environment_id)?),
        request_id: parse(&wire.request_id)?,
        invocation_id: parse(&wire.invocation_id)?,
        parent_invocation_id: parse_optional(wire.parent_invocation_id.as_deref())?,
        release_id: parse(&wire.release_id)?,
        dev_revision_id: parse_optional(wire.dev_revision_id.as_deref())?,
        function_id: parse(&wire.function_id)?,
        function_name: FunctionName::from_str(&wire.function_name)
            .map_err(|_| LogJournalError::Corruption)?,
        function_type: parse_function_type(&wire.function_type)?,
        client_id: parse_optional(wire.client_id.as_deref())?,
        credential_id: parse_optional(wire.credential_id.as_deref())?,
        principal_kind: parse_principal(&wire.principal_kind)?,
        stream: parse_stream(&wire.stream)?,
        level: parse_level(&wire.level)?,
        kind: parse_kind(&wire.event_kind)?,
        message: wire
            .message
            .map(LogMessage::new)
            .transpose()
            .map_err(|_| LogJournalError::Corruption)?,
        fields: wire
            .fields_base64url
            .map(|value| URL_SAFE_NO_PAD.decode(value))
            .transpose()
            .map_err(|_| LogJournalError::Corruption)?
            .map(|bytes| decode_stored_value(&bytes))
            .transpose()
            .map_err(|_| LogJournalError::Corruption)?,
        duration_micros: wire.duration_micros,
        outcome_code: wire
            .outcome_code
            .map(OutcomeCode::new)
            .transpose()
            .map_err(|_| LogJournalError::Corruption)?,
    };
    event.validate().map_err(|_| LogJournalError::Corruption)?;
    Ok(SequencedOperationalEvent {
        cursor: LogCursor::new(wire.sequence),
        event,
    })
}

fn subject_for(prefix: &str, scope: EnvironmentScope) -> String {
    format!("{prefix}.{}.{}", scope.project_id(), scope.environment_id())
}

fn validate_config(config: &NatsLogJournalConfig) -> Result<(), LogJournalError> {
    let stream = !config.stream_name.is_empty()
        && config.stream_name.len() <= 64
        && config
            .stream_name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_');
    let consumer = valid_token(&config.consumer_name, 64);
    let prefix = !config.subject_prefix.is_empty()
        && config.subject_prefix.len() <= 128
        && config
            .subject_prefix
            .split('.')
            .all(|token| valid_token(token, 32));
    if !stream
        || !consumer
        || !prefix
        || config.max_messages <= 0
        || config.max_bytes <= 0
        || config.max_age < Duration::from_secs(60)
        || !(1..=5).contains(&config.replicas)
        || config.ack_wait < Duration::from_secs(2)
        || config.max_ack_pending <= 0
    {
        return Err(LogJournalError::Invalid);
    }
    Ok(())
}

fn valid_token(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn parse<T: FromStr>(value: &str) -> Result<T, LogJournalError> {
    value.parse().map_err(|_| LogJournalError::Corruption)
}

fn parse_optional<T: FromStr>(value: Option<&str>) -> Result<Option<T>, LogJournalError> {
    value.map(parse).transpose()
}

const fn function_type_text(value: FunctionType) -> &'static str {
    match value {
        FunctionType::Query => "query",
        FunctionType::Mutation => "mutation",
        FunctionType::Action => "action",
    }
}

fn parse_function_type(value: &str) -> Result<FunctionType, LogJournalError> {
    match value {
        "query" => Ok(FunctionType::Query),
        "mutation" => Ok(FunctionType::Mutation),
        "action" => Ok(FunctionType::Action),
        _ => Err(LogJournalError::Corruption),
    }
}

fn parse_principal(value: &str) -> Result<LogPrincipalKind, LogJournalError> {
    match value {
        "none" => Ok(LogPrincipalKind::None),
        "guest" => Ok(LogPrincipalKind::Guest),
        "user" => Ok(LogPrincipalKind::User),
        "service" => Ok(LogPrincipalKind::Service),
        "system" => Ok(LogPrincipalKind::System),
        _ => Err(LogJournalError::Corruption),
    }
}

fn parse_stream(value: &str) -> Result<LogStream, LogJournalError> {
    match value {
        "platform" => Ok(LogStream::Platform),
        "function" => Ok(LogStream::Function),
        _ => Err(LogJournalError::Corruption),
    }
}

fn parse_level(value: &str) -> Result<LogLevel, LogJournalError> {
    match value {
        "debug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" => Ok(LogLevel::Warn),
        "error" => Ok(LogLevel::Error),
        _ => Err(LogJournalError::Corruption),
    }
}

fn parse_kind(value: &str) -> Result<LogEventKind, LogJournalError> {
    match value {
        "invocation_started" => Ok(LogEventKind::InvocationStarted),
        "invocation_completed" => Ok(LogEventKind::InvocationCompleted),
        "function_message" => Ok(LogEventKind::FunctionMessage),
        _ => Err(LogJournalError::Corruption),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_publish_error(error: jetstream::context::PublishError) -> LogJournalError {
    let message = error.to_string();
    if message.contains("maximum") || message.contains("limit") || message.contains("discard") {
        LogJournalError::Full
    } else {
        LogJournalError::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::{NatsLogJournalConfig, validate_config};

    #[test]
    fn rejects_non_durable_or_unsafe_config() {
        let mut config = NatsLogJournalConfig::default();
        assert!(validate_config(&config).is_ok());
        config.subject_prefix = "runku.logs.*".to_owned();
        assert!(validate_config(&config).is_err());
        config = NatsLogJournalConfig::default();
        config.max_age = std::time::Duration::from_secs(1);
        assert!(validate_config(&config).is_err());
    }
}
