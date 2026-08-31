//! Bounded in-process subscription state with fenced rerun transitions.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use runku_core::{EnvironmentScope, FunctionName, ReleaseId, SubscriptionId};
use runku_data::{OutboxCursor, PinnedCode};
use runku_execution::{QueryOutcome, ReadDependency};
use runku_identity::RequestIdentity;
use runku_releases::Sha256Digest;
use runku_value::{CanonicalValue, TimestampMicros, encode_stored_value};
use tokio::sync::broadcast;

use crate::{ChangeImpact, RealtimeError};

/// Hard limits and retry policy for one process-local registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistryConfig {
    /// Maximum live subscriptions.
    pub max_subscriptions: usize,
    /// Maximum dependencies retained by one subscription.
    pub max_dependencies: usize,
    /// Maximum canonical encoded result size.
    pub max_result_bytes: usize,
    /// Per-subscription delivery buffer.
    pub delivery_buffer: usize,
    /// First retry delay after a failed rerun.
    pub retry_base_micros: u64,
    /// Maximum exponential retry delay.
    pub retry_max_micros: u64,
    /// Failure budget before explicit suspension.
    pub max_consecutive_failures: u32,
}

impl RegistryConfig {
    /// Conservative v1 defaults.
    pub const PRODUCTION: Self = Self {
        max_subscriptions: 100_000,
        max_dependencies: 10_000,
        max_result_bytes: 4 * 1024 * 1024,
        delivery_buffer: 64,
        retry_base_micros: 100_000,
        retry_max_micros: 30_000_000,
        max_consecutive_failures: 10,
    };

    fn validate(self) -> Result<Self, RealtimeError> {
        if self.max_subscriptions == 0
            || self.max_dependencies == 0
            || self.max_result_bytes == 0
            || self.delivery_buffer == 0
            || self.retry_base_micros == 0
            || self.retry_base_micros > self.retry_max_micros
            || self.max_consecutive_failures == 0
        {
            return Err(RealtimeError::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// Immutable rerun identity retained after a moving target has been resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionSpec {
    /// Subscription identity.
    pub id: SubscriptionId,
    /// Data-owning environment.
    pub scope: EnvironmentScope,
    /// Candidate/stable Release whose manifest is executed.
    pub release_id: ReleaseId,
    /// Immutable Release or development revision used on every rerun.
    pub pinned_code: PinnedCode,
    /// Logical Query name.
    pub function: FunctionName,
    /// Canonical Query arguments.
    pub arguments: CanonicalValue,
    /// Effective token-free identity retained for exact policy/result isolation.
    pub identity: Arc<RequestIdentity>,
    /// Absolute time after which reruns must fail closed and require reauthentication.
    pub authorized_until: TimestampMicros,
}

/// Public immutable view of the current subscription state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionSnapshot {
    /// Immutable invocation identity.
    pub spec: SubscriptionSpec,
    /// Last successfully published result.
    pub value: CanonicalValue,
    /// Hash of the canonical Stored Value bytes.
    pub result_hash: Sha256Digest,
    /// Snapshot sequence observed by the successful Query.
    pub snapshot_sequence: Option<u64>,
    /// Dependencies from the successful Query.
    pub dependencies: Vec<ReadDependency>,
    /// Monotonic delivery revision, including sanitized error events.
    pub delivery_revision: u64,
    /// Last outbox position incorporated by a successful rerun.
    pub processed_through: Option<OutboxCursor>,
    /// Whether the failure budget has paused automatic reruns.
    pub suspended: bool,
}

/// Typed event consumed by a future WebSocket/HTTP streaming protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryEvent {
    /// Initial or updated committed Query state.
    State {
        /// Subscription identity.
        subscription_id: SubscriptionId,
        /// Monotonic delivery revision.
        delivery_revision: u64,
        /// Canonical Query result.
        value: CanonicalValue,
        /// Canonical result digest.
        result_hash: Sha256Digest,
        /// Query snapshot sequence, if data was read.
        snapshot_sequence: Option<u64>,
    },
    /// Sanitized rerun failure; last successful state remains authoritative.
    Error {
        /// Subscription identity.
        subscription_id: SubscriptionId,
        /// Monotonic delivery revision.
        delivery_revision: u64,
        /// Stable machine-readable failure category.
        code: &'static str,
        /// Whether the failure is eligible for automatic retry.
        retryable: bool,
        /// Whether the configured failure budget is exhausted.
        suspended: bool,
    },
}

/// Atomic current-state plus future-event subscription used for reconnects.
pub struct SubscriptionHandle {
    /// Current state captured before the receiver can observe later events.
    pub snapshot: SubscriptionSnapshot,
    /// Bounded future deliveries; lag is reported by Tokio rather than hidden.
    pub receiver: broadcast::Receiver<DeliveryEvent>,
}

/// Fenced authorization to perform exactly one registry rerun generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RerunTicket {
    /// Immutable invocation descriptor.
    pub spec: SubscriptionSpec,
    /// Registry generation rejecting stale completion races.
    pub generation: u64,
}

/// Bounded aggregate metrics without user-controlled labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegistryTelemetrySnapshot {
    /// Current live subscriptions.
    pub subscriptions: u64,
    /// Impact/subscription intersections.
    pub matches: u64,
    /// Invalidations merged into an active rerun.
    pub coalesced: u64,
    /// Rerun tickets issued.
    pub reruns_started: u64,
    /// Successful reruns applied.
    pub reruns_succeeded: u64,
    /// Failed reruns retained for retry.
    pub reruns_failed: u64,
    /// Subscriptions suspended by the failure budget.
    pub suspensions: u64,
    /// Delivery events for which no receiver had capacity/presence.
    pub undelivered_events: u64,
}

#[derive(Debug, Default)]
struct RegistryTelemetry {
    subscriptions: AtomicU64,
    matches: AtomicU64,
    coalesced: AtomicU64,
    reruns_started: AtomicU64,
    reruns_succeeded: AtomicU64,
    reruns_failed: AtomicU64,
    suspensions: AtomicU64,
    undelivered_events: AtomicU64,
}

struct Entry {
    snapshot: SubscriptionSnapshot,
    delivery: broadcast::Sender<DeliveryEvent>,
    running_generation: Option<u64>,
    next_generation: u64,
    running_through: Option<OutboxCursor>,
    dirty_through: Option<OutboxCursor>,
    retry_not_before: Option<TimestampMicros>,
    consecutive_failures: u32,
}

/// Process-local registry. Durable recovery is supplied by the outbox cursor, not this cache.
#[derive(Clone)]
pub struct SubscriptionRegistry {
    config: RegistryConfig,
    entries: Arc<Mutex<BTreeMap<SubscriptionId, Entry>>>,
    telemetry: Arc<RegistryTelemetry>,
}

impl std::fmt::Debug for SubscriptionRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriptionRegistry")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl SubscriptionRegistry {
    /// Creates an empty validated registry.
    ///
    /// # Errors
    ///
    /// Rejects zero, inverted, or otherwise unsafe limits.
    pub fn new(config: RegistryConfig) -> Result<Self, RealtimeError> {
        Ok(Self {
            config: config.validate()?,
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            telemetry: Arc::new(RegistryTelemetry::default()),
        })
    }

    /// Registers a successful initial Query result and queues revision one.
    ///
    /// # Errors
    ///
    /// Rejects duplicate IDs, capacity exhaustion, oversized dependencies/results, or lock poison.
    pub fn register(
        &self,
        spec: SubscriptionSpec,
        outcome: QueryOutcome,
    ) -> Result<SubscriptionHandle, RealtimeError> {
        validate_outcome(&outcome, self.config)?;
        let result_hash = result_hash(&outcome.value)?;
        let id = spec.id;
        let snapshot = SubscriptionSnapshot {
            spec,
            value: outcome.value,
            result_hash,
            snapshot_sequence: outcome.snapshot_sequence,
            dependencies: outcome.dependencies,
            delivery_revision: 1,
            processed_through: None,
            suspended: false,
        };
        let (delivery, receiver) = broadcast::channel(self.config.delivery_buffer);
        let mut entries = self.entries.lock().map_err(|_| RealtimeError::Internal)?;
        if entries.contains_key(&id) {
            return Err(RealtimeError::AlreadyExists);
        }
        if entries.len() >= self.config.max_subscriptions {
            return Err(RealtimeError::LimitExceeded);
        }
        entries.insert(
            id,
            Entry {
                snapshot: snapshot.clone(),
                delivery: delivery.clone(),
                running_generation: None,
                next_generation: 0,
                running_through: None,
                dirty_through: None,
                retry_not_before: None,
                consecutive_failures: 0,
            },
        );
        self.telemetry.subscriptions.fetch_add(1, Ordering::Relaxed);
        send_event(
            &delivery,
            DeliveryEvent::State {
                subscription_id: id,
                delivery_revision: 1,
                value: snapshot.value.clone(),
                result_hash,
                snapshot_sequence: snapshot.snapshot_sequence,
            },
            &self.telemetry,
        );
        Ok(SubscriptionHandle { snapshot, receiver })
    }

    /// Atomically captures current state and subscribes to later bounded deliveries.
    ///
    /// # Errors
    ///
    /// Returns not-found or internal lock failure.
    pub fn subscribe(&self, id: SubscriptionId) -> Result<SubscriptionHandle, RealtimeError> {
        let entries = self.entries.lock().map_err(|_| RealtimeError::Internal)?;
        let entry = entries.get(&id).ok_or(RealtimeError::NotFound)?;
        Ok(SubscriptionHandle {
            snapshot: entry.snapshot.clone(),
            receiver: entry.delivery.subscribe(),
        })
    }

    /// Removes a subscription and closes its delivery stream after buffered events drain.
    ///
    /// # Errors
    ///
    /// Returns not-found or internal lock failure.
    pub fn remove(&self, id: SubscriptionId) -> Result<(), RealtimeError> {
        let removed = self
            .entries
            .lock()
            .map_err(|_| RealtimeError::Internal)?
            .remove(&id);
        if removed.is_none() {
            return Err(RealtimeError::NotFound);
        }
        self.telemetry.subscriptions.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// Marks matching subscriptions dirty and issues at most one ticket per idle subscription.
    ///
    /// Already-processed cursors are ignored, making crash-before-ACK replay idempotent.
    ///
    /// # Errors
    ///
    /// Returns an internal lock failure.
    pub fn mark_impacted(
        &self,
        scope: EnvironmentScope,
        cursor: OutboxCursor,
        impact: &ChangeImpact,
        now: TimestampMicros,
    ) -> Result<Vec<RerunTicket>, RealtimeError> {
        let mut entries = self.entries.lock().map_err(|_| RealtimeError::Internal)?;
        let mut tickets = Vec::new();
        for entry in entries.values_mut() {
            if entry.snapshot.spec.scope != scope
                || entry
                    .snapshot
                    .processed_through
                    .is_some_and(|seen| seen >= cursor)
                || !impact.invalidates(&entry.snapshot.dependencies)
            {
                continue;
            }
            self.telemetry.matches.fetch_add(1, Ordering::Relaxed);
            if entry.running_generation.is_some() {
                entry.dirty_through = Some(max_cursor(entry.dirty_through, cursor));
                self.telemetry.coalesced.fetch_add(1, Ordering::Relaxed);
            } else if entry.snapshot.suspended
                || entry
                    .retry_not_before
                    .is_some_and(|deadline| deadline > now)
            {
                entry.dirty_through = Some(max_cursor(entry.dirty_through, cursor));
            } else {
                tickets.push(start_rerun(entry, cursor, &self.telemetry)?);
            }
        }
        Ok(tickets)
    }

    /// Starts eligible dirty retries after their backoff elapsed.
    ///
    /// # Errors
    ///
    /// Returns an internal lock or generation overflow failure.
    pub fn ready_retries(&self, now: TimestampMicros) -> Result<Vec<RerunTicket>, RealtimeError> {
        let mut entries = self.entries.lock().map_err(|_| RealtimeError::Internal)?;
        let mut tickets = Vec::new();
        for entry in entries.values_mut() {
            if entry.running_generation.is_none()
                && !entry.snapshot.suspended
                && entry
                    .retry_not_before
                    .is_none_or(|deadline| deadline <= now)
                && let Some(cursor) = entry.dirty_through.take()
            {
                tickets.push(start_rerun(entry, cursor, &self.telemetry)?);
            }
        }
        Ok(tickets)
    }

    /// Applies a successful rerun and returns a coalesced follow-up ticket when needed.
    ///
    /// # Errors
    ///
    /// Rejects stale tickets and invalid/oversized outcomes without replacing valid state.
    pub fn complete_success(
        &self,
        ticket: &RerunTicket,
        outcome: QueryOutcome,
    ) -> Result<Option<RerunTicket>, RealtimeError> {
        validate_outcome(&outcome, self.config)?;
        let hash = result_hash(&outcome.value)?;
        let mut entries = self.entries.lock().map_err(|_| RealtimeError::Internal)?;
        let entry = entries
            .get_mut(&ticket.spec.id)
            .ok_or(RealtimeError::NotFound)?;
        require_generation(entry, ticket.generation)?;
        let through = entry.running_through.ok_or(RealtimeError::StaleTicket)?;
        entry.snapshot.value = outcome.value;
        entry.snapshot.result_hash = hash;
        entry.snapshot.snapshot_sequence = outcome.snapshot_sequence;
        entry.snapshot.dependencies = outcome.dependencies;
        entry.snapshot.delivery_revision = entry
            .snapshot
            .delivery_revision
            .checked_add(1)
            .ok_or(RealtimeError::Internal)?;
        entry.snapshot.processed_through =
            Some(max_cursor(entry.snapshot.processed_through, through));
        entry.snapshot.suspended = false;
        entry.consecutive_failures = 0;
        entry.retry_not_before = None;
        entry.running_generation = None;
        entry.running_through = None;
        self.telemetry
            .reruns_succeeded
            .fetch_add(1, Ordering::Relaxed);
        send_event(
            &entry.delivery,
            DeliveryEvent::State {
                subscription_id: ticket.spec.id,
                delivery_revision: entry.snapshot.delivery_revision,
                value: entry.snapshot.value.clone(),
                result_hash: hash,
                snapshot_sequence: entry.snapshot.snapshot_sequence,
            },
            &self.telemetry,
        );
        let Some(dirty) = entry.dirty_through.take() else {
            return Ok(None);
        };
        if entry
            .snapshot
            .processed_through
            .is_some_and(|seen| seen >= dirty)
        {
            return Ok(None);
        }
        start_rerun(entry, dirty, &self.telemetry).map(Some)
    }

    /// Retains last valid state, emits a sanitized error, and schedules bounded retry.
    ///
    /// # Errors
    ///
    /// Rejects stale tickets or timestamp/generation overflow.
    pub fn complete_failure(
        &self,
        ticket: &RerunTicket,
        code: &'static str,
        retryable: bool,
        now: TimestampMicros,
    ) -> Result<(), RealtimeError> {
        let mut entries = self.entries.lock().map_err(|_| RealtimeError::Internal)?;
        let entry = entries
            .get_mut(&ticket.spec.id)
            .ok_or(RealtimeError::NotFound)?;
        require_generation(entry, ticket.generation)?;
        let through = entry.running_through.ok_or(RealtimeError::StaleTicket)?;
        entry.dirty_through = Some(max_cursor(entry.dirty_through, through));
        entry.running_generation = None;
        entry.running_through = None;
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        let budget_exhausted = entry.consecutive_failures >= self.config.max_consecutive_failures;
        entry.snapshot.suspended = !retryable || budget_exhausted;
        if retryable && !budget_exhausted {
            let delay = retry_delay(self.config, entry.consecutive_failures);
            let deadline = now
                .get()
                .checked_add(i64::try_from(delay).map_err(|_| RealtimeError::Internal)?)
                .ok_or(RealtimeError::Internal)?;
            entry.retry_not_before = Some(TimestampMicros::new(deadline));
        } else {
            entry.retry_not_before = None;
            entry.snapshot.processed_through =
                Some(max_cursor(entry.snapshot.processed_through, through));
        }
        entry.snapshot.delivery_revision = entry
            .snapshot
            .delivery_revision
            .checked_add(1)
            .ok_or(RealtimeError::Internal)?;
        self.telemetry.reruns_failed.fetch_add(1, Ordering::Relaxed);
        if entry.snapshot.suspended {
            self.telemetry.suspensions.fetch_add(1, Ordering::Relaxed);
        }
        send_event(
            &entry.delivery,
            DeliveryEvent::Error {
                subscription_id: ticket.spec.id,
                delivery_revision: entry.snapshot.delivery_revision,
                code,
                retryable,
                suspended: entry.snapshot.suspended,
            },
            &self.telemetry,
        );
        Ok(())
    }

    /// Reports whether any subscription still owes work at or before a proposed ACK cursor.
    ///
    /// This closes the race where a lease expires while another dispatcher is still rerunning.
    ///
    /// # Errors
    ///
    /// Returns an internal lock failure.
    pub fn has_pending_through(
        &self,
        scope: EnvironmentScope,
        through: OutboxCursor,
    ) -> Result<bool, RealtimeError> {
        let entries = self.entries.lock().map_err(|_| RealtimeError::Internal)?;
        Ok(entries.values().any(|entry| {
            entry.snapshot.spec.scope == scope
                && !entry.snapshot.suspended
                && [entry.running_through, entry.dirty_through]
                    .into_iter()
                    .flatten()
                    .any(|cursor| cursor <= through)
        }))
    }

    /// Explicitly resets a suspended failure budget and makes dirty work immediately eligible.
    ///
    /// # Errors
    ///
    /// Returns not-found or internal lock failure.
    pub fn resume(&self, id: SubscriptionId) -> Result<(), RealtimeError> {
        let mut entries = self.entries.lock().map_err(|_| RealtimeError::Internal)?;
        let entry = entries.get_mut(&id).ok_or(RealtimeError::NotFound)?;
        entry.snapshot.suspended = false;
        entry.consecutive_failures = 0;
        entry.retry_not_before = None;
        Ok(())
    }

    /// Returns bounded process-local counters.
    #[must_use]
    pub fn telemetry(&self) -> RegistryTelemetrySnapshot {
        RegistryTelemetrySnapshot {
            subscriptions: self.telemetry.subscriptions.load(Ordering::Relaxed),
            matches: self.telemetry.matches.load(Ordering::Relaxed),
            coalesced: self.telemetry.coalesced.load(Ordering::Relaxed),
            reruns_started: self.telemetry.reruns_started.load(Ordering::Relaxed),
            reruns_succeeded: self.telemetry.reruns_succeeded.load(Ordering::Relaxed),
            reruns_failed: self.telemetry.reruns_failed.load(Ordering::Relaxed),
            suspensions: self.telemetry.suspensions.load(Ordering::Relaxed),
            undelivered_events: self.telemetry.undelivered_events.load(Ordering::Relaxed),
        }
    }
}

fn validate_outcome(outcome: &QueryOutcome, config: RegistryConfig) -> Result<(), RealtimeError> {
    if outcome.dependencies.len() > config.max_dependencies {
        return Err(RealtimeError::LimitExceeded);
    }
    let encoded = encode_stored_value(&outcome.value).map_err(|_| RealtimeError::InvalidOutcome)?;
    if encoded.len() > config.max_result_bytes {
        return Err(RealtimeError::LimitExceeded);
    }
    Ok(())
}

fn result_hash(value: &CanonicalValue) -> Result<Sha256Digest, RealtimeError> {
    let encoded = encode_stored_value(value).map_err(|_| RealtimeError::InvalidOutcome)?;
    Ok(Sha256Digest::of(&encoded))
}

fn start_rerun(
    entry: &mut Entry,
    through: OutboxCursor,
    telemetry: &RegistryTelemetry,
) -> Result<RerunTicket, RealtimeError> {
    entry.next_generation = entry
        .next_generation
        .checked_add(1)
        .ok_or(RealtimeError::Internal)?;
    entry.running_generation = Some(entry.next_generation);
    entry.running_through = Some(through);
    entry.retry_not_before = None;
    telemetry.reruns_started.fetch_add(1, Ordering::Relaxed);
    Ok(RerunTicket {
        spec: entry.snapshot.spec.clone(),
        generation: entry.next_generation,
    })
}

fn require_generation(entry: &Entry, generation: u64) -> Result<(), RealtimeError> {
    if entry.running_generation == Some(generation) {
        Ok(())
    } else {
        Err(RealtimeError::StaleTicket)
    }
}

fn max_cursor(current: Option<OutboxCursor>, candidate: OutboxCursor) -> OutboxCursor {
    current.map_or(candidate, |value| value.max(candidate))
}

fn retry_delay(config: RegistryConfig, failures: u32) -> u64 {
    let shift = failures.saturating_sub(1).min(63);
    config
        .retry_base_micros
        .saturating_mul(1_u64 << shift)
        .min(config.retry_max_micros)
}

fn send_event(
    sender: &broadcast::Sender<DeliveryEvent>,
    event: DeliveryEvent,
    telemetry: &RegistryTelemetry,
) {
    if sender.send(event).is_err() {
        telemetry.undelivered_events.fetch_add(1, Ordering::Relaxed);
    }
}
