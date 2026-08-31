//! Bounded crash-safe Cron tick materializer.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use runku_core::{OperationId, ScheduledInvocationId, WorkerId};
use runku_data::{CommitBatch, LogicalStore, ScheduledInvocationInsert, StoreError};
use runku_releases::CronName;
use runku_value::TimestampMicros;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{CronContext, CronError, CronRepository};

/// Bounded materializer lease and batch limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CronMaterializerConfig {
    /// Maximum activations claimed per poll.
    pub batch_size: u32,
    /// Fenced activation lease duration.
    pub lease_duration: Duration,
}

impl CronMaterializerConfig {
    /// Conservative MVP defaults.
    pub const DEFAULT: Self = Self {
        batch_size: 100,
        lease_duration: Duration::from_secs(30),
    };

    fn validate(self) -> Result<Self, CronError> {
        if self.batch_size == 0
            || self.batch_size > 1_000
            || self.lease_duration.is_zero()
            || self.lease_duration > Duration::from_mins(5)
        {
            return Err(CronError::InvalidInput);
        }
        Ok(self)
    }
}

/// Result of one bounded materializer poll.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CronPollOutcome {
    /// Due activations claimed.
    pub claimed: u32,
    /// Newly committed tick records.
    pub materialized: u32,
    /// Exact tick commit replays recovered after a crash/uncertain response.
    pub replayed: u32,
    /// Activation cursors advanced.
    pub completed: u32,
    /// Lease lost after the tick was already durable.
    pub lease_lost: u32,
}

/// Aggregate non-cardinal materializer counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CronMaterializerTelemetrySnapshot {
    /// Poll calls.
    pub polls: u64,
    /// Claimed activations.
    pub claimed: u64,
    /// Newly materialized ticks.
    pub materialized: u64,
    /// Replayed tick commits.
    pub replayed: u64,
    /// Advanced cursors.
    pub completed: u64,
    /// Fenced completion failures.
    pub lease_lost: u64,
    /// Sum of nonnegative materialization lag in microseconds.
    pub lag_micros: u64,
}

#[derive(Debug, Default)]
struct Telemetry {
    polls: AtomicU64,
    claimed: AtomicU64,
    materialized: AtomicU64,
    replayed: AtomicU64,
    completed: AtomicU64,
    lease_lost: AtomicU64,
    lag_micros: AtomicU64,
}

/// Coordinates Cron activation leases with exact-idempotent `LogicalStore` commits.
pub struct CronMaterializer {
    repository: Arc<dyn CronRepository>,
    store: Arc<dyn LogicalStore>,
    context: CronContext,
    worker_id: WorkerId,
    config: CronMaterializerConfig,
    telemetry: Arc<Telemetry>,
}

impl std::fmt::Debug for CronMaterializer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CronMaterializer")
            .field("context", &self.context)
            .field("worker_id", &self.worker_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl CronMaterializer {
    /// Constructs a materializer fixed to one Environment and worker identity.
    ///
    /// # Errors
    ///
    /// Rejects mismatched Environment context or unsafe limits.
    pub fn new(
        repository: Arc<dyn CronRepository>,
        store: Arc<dyn LogicalStore>,
        context: CronContext,
        worker_id: WorkerId,
        config: CronMaterializerConfig,
    ) -> Result<Self, CronError> {
        context.validate()?;
        Ok(Self {
            repository,
            store,
            context,
            worker_id,
            config: config.validate()?,
            telemetry: Arc::new(Telemetry::default()),
        })
    }

    /// Polls using the system UTC clock.
    ///
    /// # Errors
    ///
    /// Returns a stable error when the clock precedes Unix epoch or a dependency fails.
    pub async fn poll(&self) -> Result<CronPollOutcome, CronError> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CronError::Unavailable)?;
        let micros = i64::try_from(elapsed.as_micros()).map_err(|_| CronError::LimitExceeded)?;
        self.poll_at(TimestampMicros::new(micros)).await
    }

    /// Performs one bounded deterministic poll at an injected UTC time.
    ///
    /// A storage failure leaves the activation leased until expiry. A tick committed before a
    /// process crash replays through the same deterministic operation ID and then advances.
    ///
    /// # Errors
    ///
    /// Returns repository/storage/calendar errors without skipping the activation cursor.
    pub async fn poll_at(&self, now: TimestampMicros) -> Result<CronPollOutcome, CronError> {
        if now.get() < 0 {
            return Err(CronError::InvalidInput);
        }
        let lease_micros = i64::try_from(self.config.lease_duration.as_micros())
            .map_err(|_| CronError::LimitExceeded)?;
        let lease_until = now
            .get()
            .checked_add(lease_micros)
            .map(TimestampMicros::new)
            .ok_or(CronError::LimitExceeded)?;
        self.telemetry.polls.fetch_add(1, Ordering::Relaxed);
        let claimed = self
            .repository
            .claim_due(
                self.context,
                self.worker_id,
                now,
                lease_until,
                self.config.batch_size,
            )
            .await?;
        self.telemetry.claimed.fetch_add(
            u64::try_from(claimed.len()).map_err(|_| CronError::LimitExceeded)?,
            Ordering::Relaxed,
        );
        let mut outcome = CronPollOutcome {
            claimed: u32::try_from(claimed.len()).map_err(|_| CronError::LimitExceeded)?,
            ..CronPollOutcome::default()
        };
        for claimed in claimed {
            let activation = claimed.activation;
            let (operation_id, schedule_id) = tick_ids(
                self.context,
                &activation.name,
                activation.activation_revision,
                activation.next_tick,
            );
            let key = tick_key(
                &activation.name,
                activation.activation_revision,
                activation.next_tick,
            )?;
            let mut batch = CommitBatch::new(self.context.scope, operation_id);
            batch.push_schedule(ScheduledInvocationInsert {
                id: schedule_id,
                pinned_code: activation.pinned_code,
                function: activation.function.clone(),
                args: activation.args.clone(),
                execute_at: activation.next_tick,
                idempotency_key: Some(key),
            });
            let commit = self.store.commit(&batch).await.map_err(map_store_error)?;
            if commit.replayed {
                outcome.replayed = outcome.replayed.saturating_add(1);
                self.telemetry.replayed.fetch_add(1, Ordering::Relaxed);
            } else {
                outcome.materialized = outcome.materialized.saturating_add(1);
                self.telemetry.materialized.fetch_add(1, Ordering::Relaxed);
            }
            self.telemetry.lag_micros.fetch_add(
                u64::try_from(now.get().saturating_sub(activation.next_tick.get()))
                    .unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let next_tick = activation
                .schedule
                .next_after(activation.next_tick)
                .map_err(|_| CronError::Corruption)?;
            match self
                .repository
                .complete_tick(
                    self.context,
                    &activation.name,
                    self.worker_id,
                    activation.lease_generation,
                    activation.next_tick,
                    next_tick,
                    now,
                )
                .await
            {
                Ok(()) => {
                    outcome.completed = outcome.completed.saturating_add(1);
                    self.telemetry.completed.fetch_add(1, Ordering::Relaxed);
                }
                Err(CronError::LeaseLost) => {
                    outcome.lease_lost = outcome.lease_lost.saturating_add(1);
                    self.telemetry.lease_lost.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(outcome)
    }

    /// Returns aggregate counters without user-controlled labels.
    #[must_use]
    pub fn telemetry(&self) -> CronMaterializerTelemetrySnapshot {
        CronMaterializerTelemetrySnapshot {
            polls: self.telemetry.polls.load(Ordering::Relaxed),
            claimed: self.telemetry.claimed.load(Ordering::Relaxed),
            materialized: self.telemetry.materialized.load(Ordering::Relaxed),
            replayed: self.telemetry.replayed.load(Ordering::Relaxed),
            completed: self.telemetry.completed.load(Ordering::Relaxed),
            lease_lost: self.telemetry.lease_lost.load(Ordering::Relaxed),
            lag_micros: self.telemetry.lag_micros.load(Ordering::Relaxed),
        }
    }
}

fn tick_ids(
    context: CronContext,
    name: &CronName,
    revision: u64,
    tick: TimestampMicros,
) -> (OperationId, ScheduledInvocationId) {
    let input = tick_identity_bytes(context, name, revision, tick);
    let mut operation = Sha256::new();
    operation.update(b"RUNKU_CRON_OPERATION_ID_V1\0");
    operation.update(&input);
    let operation = operation.finalize();
    let mut scheduled = Sha256::new();
    scheduled.update(b"RUNKU_CRON_SCHEDULE_ID_V1\0");
    scheduled.update(&input);
    let scheduled = scheduled.finalize();
    (
        OperationId::from_ulid(Ulid::from(u128::from_be_bytes(
            operation[..16].try_into().unwrap_or([0; 16]),
        ))),
        ScheduledInvocationId::from_ulid(Ulid::from(u128::from_be_bytes(
            scheduled[..16].try_into().unwrap_or([0; 16]),
        ))),
    )
}

fn tick_identity_bytes(
    context: CronContext,
    name: &CronName,
    revision: u64,
    tick: TimestampMicros,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(160);
    bytes.extend_from_slice(context.scope.project_id().to_string().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(context.scope.environment_id().to_string().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(name.as_str().as_bytes());
    bytes.extend_from_slice(&revision.to_be_bytes());
    bytes.extend_from_slice(&tick.get().to_be_bytes());
    bytes
}

fn tick_key(name: &CronName, revision: u64, tick: TimestampMicros) -> Result<String, CronError> {
    let key = format!("cron:{}:{revision}:{}", name.as_str(), tick.get());
    if key.len() > 128 {
        return Err(CronError::LimitExceeded);
    }
    Ok(key)
}

const fn map_store_error(error: StoreError) -> CronError {
    match error {
        StoreError::ResultUncertain => CronError::ResultUncertain,
        StoreError::LimitExceeded => CronError::LimitExceeded,
        StoreError::OperationIdReused
        | StoreError::DuplicateMutation
        | StoreError::MutationConflict => CronError::Conflict,
        StoreError::Corruption | StoreError::MigrationFailed => CronError::Corruption,
        StoreError::ProductionBackendUnsupported => CronError::Unsupported,
        StoreError::EmptyBatch
        | StoreError::InvalidRange
        | StoreError::NotFound
        | StoreError::LeaseLost
        | StoreError::OutboxLeaseLost
        | StoreError::Busy
        | StoreError::SerializationFailure
        | StoreError::Unavailable
        | StoreError::Internal => CronError::Unavailable,
    }
}
