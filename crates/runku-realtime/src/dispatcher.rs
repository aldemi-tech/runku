//! Durable outbox polling and subscription rerun coordination.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use runku_core::{EnvironmentScope, WorkerId};
use runku_data::{LogicalStore, OutboxConsumerName, OutboxCursor, StoreError};
use runku_execution::{ExecutionError, QueryOutcome};
use runku_value::TimestampMicros;
use thiserror::Error;

use crate::{ChangeImpact, RealtimeError, RerunTicket, SubscriptionRegistry, SubscriptionSpec};

/// Bounded durable dispatcher configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatcherConfig {
    /// Maximum outbox events claimed per transaction.
    pub batch_limit: u32,
    /// Lease duration measured in microseconds.
    pub lease_micros: u64,
}

impl DispatcherConfig {
    /// Conservative v1 defaults.
    pub const PRODUCTION: Self = Self {
        batch_limit: 100,
        lease_micros: 30_000_000,
    };

    fn validate(self) -> Result<Self, RealtimeError> {
        if self.batch_limit == 0 || self.batch_limit > 1_000 || self.lease_micros == 0 {
            return Err(RealtimeError::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// Query rerun boundary implemented by product composition after resolving immutable code.
#[async_trait]
pub trait SubscriptionRunner: Send + Sync {
    /// Executes one already-authorized Query descriptor with fresh request/invocation IDs.
    async fn rerun(&self, spec: &SubscriptionSpec) -> Result<QueryOutcome, SubscriptionRunFailure>;
}

/// Sanitized failure returned by a product-specific subscription runner.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("realtime subscription rerun failed")]
pub struct SubscriptionRunFailure {
    code: &'static str,
    retryable: bool,
}

impl SubscriptionRunFailure {
    /// Fixed fail-closed internal category used only if composition violates its code contract.
    #[must_use]
    pub const fn internal() -> Self {
        Self {
            code: "REALTIME_INTERNAL_ERROR",
            retryable: false,
        }
    }

    /// Fixed non-retryable authorization deadline failure.
    #[must_use]
    pub const fn authorization_expired() -> Self {
        Self {
            code: "AUTHORIZATION_EXPIRED",
            retryable: false,
        }
    }

    /// Constructs a failure from a statically defined public machine code.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, or noncanonical codes before they reach a transport.
    pub fn new(code: &'static str, retryable: bool) -> Result<Self, RealtimeError> {
        if code.is_empty()
            || code.len() > 64
            || !code.as_bytes()[0].is_ascii_uppercase()
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(RealtimeError::InvalidConfiguration);
        }
        Ok(Self { code, retryable })
    }

    /// Stable bounded machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        self.code
    }

    /// Whether a later bounded retry may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        self.retryable
    }
}

impl From<ExecutionError> for SubscriptionRunFailure {
    fn from(error: ExecutionError) -> Self {
        Self {
            code: error.code(),
            retryable: error.retryable(),
        }
    }
}

/// Stable dispatcher failure. Failed batches are deliberately not acknowledged.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DispatcherError {
    /// Durable storage failed.
    #[error("realtime dispatcher storage failed")]
    Storage(StoreError),
    /// Impact/registry state failed validation.
    #[error("realtime dispatcher state failed")]
    Realtime(RealtimeError),
    /// A Query rerun failed after the error was retained in registry state.
    #[error("realtime Query rerun failed")]
    Rerun(SubscriptionRunFailure),
}

impl DispatcherError {
    /// Stable machine-readable category.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Storage(error) => error.code(),
            Self::Realtime(error) => error.code(),
            Self::Rerun(error) => error.code(),
        }
    }

    /// Whether retrying after policy backoff may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        match self {
            Self::Storage(error) => error.retryable(),
            Self::Realtime(error) => error.retryable(),
            Self::Rerun(error) => error.retryable(),
        }
    }
}

/// Result of one bounded outbox poll.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PollOutcome {
    /// Another worker currently owns the consumer lease.
    pub lease_busy: bool,
    /// Events decoded in this claim.
    pub events: u32,
    /// Query reruns completed, including coalesced follow-ups.
    pub reruns: u64,
    /// Cursor durably acknowledged, if the claim was non-empty.
    pub acknowledged_through: Option<OutboxCursor>,
}

/// Bounded aggregate dispatcher metrics without tenant/user labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DispatcherTelemetrySnapshot {
    /// Poll attempts.
    pub polls: u64,
    /// Successful lease claims, including empty claims.
    pub claims: u64,
    /// Claims rejected because another worker owns the lease.
    pub lease_busy: u64,
    /// Outbox events decoded.
    pub events: u64,
    /// Durable cursor acknowledgements.
    pub acknowledgements: u64,
    /// Query reruns completed successfully.
    pub reruns: u64,
    /// Query rerun failures.
    pub rerun_failures: u64,
    /// Invalid durable impact payloads.
    pub invalid_impacts: u64,
    /// Storage failures other than expected lease contention.
    pub storage_failures: u64,
    /// Maximum observed claimed sequence distance from prior cursor.
    pub max_sequence_lag: u64,
}

#[derive(Debug, Default)]
struct DispatcherTelemetry {
    polls: AtomicU64,
    claims: AtomicU64,
    lease_busy: AtomicU64,
    events: AtomicU64,
    acknowledgements: AtomicU64,
    reruns: AtomicU64,
    rerun_failures: AtomicU64,
    invalid_impacts: AtomicU64,
    storage_failures: AtomicU64,
    max_sequence_lag: AtomicU64,
}

/// At-least-once Change Dispatcher for one named durable consumer.
#[derive(Clone)]
pub struct ChangeDispatcher {
    store: Arc<dyn LogicalStore>,
    registry: SubscriptionRegistry,
    runner: Arc<dyn SubscriptionRunner>,
    consumer: OutboxConsumerName,
    worker_id: WorkerId,
    config: DispatcherConfig,
    telemetry: Arc<DispatcherTelemetry>,
}

impl std::fmt::Debug for ChangeDispatcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChangeDispatcher")
            .field("backend", &self.store.backend())
            .field("consumer", &self.consumer)
            .field("worker_id", &self.worker_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ChangeDispatcher {
    /// Composes one durable consumer worker from validated bounded components.
    ///
    /// # Errors
    ///
    /// Rejects an invalid dispatcher configuration.
    pub fn new(
        store: Arc<dyn LogicalStore>,
        registry: SubscriptionRegistry,
        runner: Arc<dyn SubscriptionRunner>,
        consumer: OutboxConsumerName,
        worker_id: WorkerId,
        config: DispatcherConfig,
    ) -> Result<Self, RealtimeError> {
        Ok(Self {
            store,
            registry,
            runner,
            consumer,
            worker_id,
            config: config.validate()?,
            telemetry: Arc::new(DispatcherTelemetry::default()),
        })
    }

    /// Retries eligible dirty subscriptions, claims one ordered batch, reruns impacts, then ACKs.
    ///
    /// Lease contention is a normal `PollOutcome`; all other errors leave the cursor unchanged.
    ///
    /// # Errors
    ///
    /// Returns storage, strict decoding, registry, or Query execution failures.
    #[allow(clippy::too_many_lines)]
    pub async fn poll_once(
        &self,
        scope: EnvironmentScope,
        now: TimestampMicros,
    ) -> Result<PollOutcome, DispatcherError> {
        self.telemetry.polls.fetch_add(1, Ordering::Relaxed);
        let mut reruns = self
            .execute_tickets(
                self.registry
                    .ready_retries(now)
                    .map_err(DispatcherError::Realtime)?,
                now,
            )
            .await?;
        let lease_delta = i64::try_from(self.config.lease_micros)
            .map_err(|_| DispatcherError::Realtime(RealtimeError::InvalidConfiguration))?;
        let lease_until = now
            .get()
            .checked_add(lease_delta)
            .map(TimestampMicros::new)
            .ok_or(DispatcherError::Realtime(
                RealtimeError::InvalidConfiguration,
            ))?;
        let claim = match self
            .store
            .claim_outbox(
                scope,
                &self.consumer,
                self.worker_id,
                now,
                lease_until,
                self.config.batch_limit,
            )
            .await
        {
            Ok(claim) => claim,
            Err(StoreError::Busy) => {
                self.telemetry.lease_busy.fetch_add(1, Ordering::Relaxed);
                return Ok(PollOutcome {
                    lease_busy: true,
                    reruns,
                    ..PollOutcome::default()
                });
            }
            Err(error) => {
                self.telemetry
                    .storage_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(DispatcherError::Storage(error));
            }
        };
        self.telemetry.claims.fetch_add(1, Ordering::Relaxed);
        self.telemetry.events.fetch_add(
            u64::try_from(claim.events.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        observe_lag(&self.telemetry, &claim);
        if claim.events.is_empty() {
            return Ok(PollOutcome {
                reruns,
                ..PollOutcome::default()
            });
        }

        let mut tickets = Vec::new();
        for event in &claim.events {
            let impact = match ChangeImpact::decode(&event.payload) {
                Ok(impact) => impact,
                Err(error) => {
                    self.telemetry
                        .invalid_impacts
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(DispatcherError::Realtime(error));
                }
            };
            tickets.extend(
                self.registry
                    .mark_impacted(scope, event.cursor(), &impact, now)
                    .map_err(DispatcherError::Realtime)?,
            );
        }
        reruns = reruns
            .checked_add(self.execute_tickets(tickets, now).await?)
            .ok_or(DispatcherError::Realtime(RealtimeError::Internal))?;
        let through = claim
            .events
            .last()
            .map(runku_data::OutboxEventRecord::cursor)
            .ok_or(DispatcherError::Realtime(RealtimeError::Internal))?;
        if self
            .registry
            .has_pending_through(scope, through)
            .map_err(DispatcherError::Realtime)?
        {
            return Err(DispatcherError::Realtime(RealtimeError::PendingWork));
        }
        self.store
            .ack_outbox(
                scope,
                &self.consumer,
                self.worker_id,
                claim.lease_generation,
                through,
            )
            .await
            .map_err(|error| {
                self.telemetry
                    .storage_failures
                    .fetch_add(1, Ordering::Relaxed);
                DispatcherError::Storage(error)
            })?;
        self.telemetry
            .acknowledgements
            .fetch_add(1, Ordering::Relaxed);
        Ok(PollOutcome {
            lease_busy: false,
            events: u32::try_from(claim.events.len()).unwrap_or(u32::MAX),
            reruns,
            acknowledged_through: Some(through),
        })
    }

    async fn execute_tickets(
        &self,
        tickets: Vec<RerunTicket>,
        now: TimestampMicros,
    ) -> Result<u64, DispatcherError> {
        let mut completed = 0_u64;
        for mut ticket in tickets {
            loop {
                if ticket.spec.authorized_until <= now {
                    let error = SubscriptionRunFailure::authorization_expired();
                    self.registry
                        .complete_failure(&ticket, error.code(), error.retryable(), now)
                        .map_err(DispatcherError::Realtime)?;
                    self.telemetry
                        .rerun_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(DispatcherError::Rerun(error));
                }
                match self.runner.rerun(&ticket.spec).await {
                    Ok(outcome) => {
                        completed = completed
                            .checked_add(1)
                            .ok_or(DispatcherError::Realtime(RealtimeError::Internal))?;
                        self.telemetry.reruns.fetch_add(1, Ordering::Relaxed);
                        let next = self
                            .registry
                            .complete_success(&ticket, outcome)
                            .map_err(DispatcherError::Realtime)?;
                        let Some(next) = next else {
                            break;
                        };
                        ticket = next;
                    }
                    Err(error) => {
                        self.registry
                            .complete_failure(&ticket, error.code(), error.retryable(), now)
                            .map_err(DispatcherError::Realtime)?;
                        self.telemetry
                            .rerun_failures
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(DispatcherError::Rerun(error));
                    }
                }
            }
        }
        Ok(completed)
    }

    /// Returns bounded aggregate dispatcher telemetry.
    #[must_use]
    pub fn telemetry(&self) -> DispatcherTelemetrySnapshot {
        DispatcherTelemetrySnapshot {
            polls: self.telemetry.polls.load(Ordering::Relaxed),
            claims: self.telemetry.claims.load(Ordering::Relaxed),
            lease_busy: self.telemetry.lease_busy.load(Ordering::Relaxed),
            events: self.telemetry.events.load(Ordering::Relaxed),
            acknowledgements: self.telemetry.acknowledgements.load(Ordering::Relaxed),
            reruns: self.telemetry.reruns.load(Ordering::Relaxed),
            rerun_failures: self.telemetry.rerun_failures.load(Ordering::Relaxed),
            invalid_impacts: self.telemetry.invalid_impacts.load(Ordering::Relaxed),
            storage_failures: self.telemetry.storage_failures.load(Ordering::Relaxed),
            max_sequence_lag: self.telemetry.max_sequence_lag.load(Ordering::Relaxed),
        }
    }
}

fn observe_lag(telemetry: &DispatcherTelemetry, claim: &runku_data::ClaimedOutboxBatch) {
    let previous = claim
        .acknowledged_through
        .map_or(0, |cursor| cursor.commit_sequence);
    if let Some(last) = claim.events.last() {
        telemetry.max_sequence_lag.fetch_max(
            last.commit_sequence.saturating_sub(previous),
            Ordering::Relaxed,
        );
    }
}
