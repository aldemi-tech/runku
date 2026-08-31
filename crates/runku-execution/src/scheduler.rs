//! Bounded at-least-once Scheduled Invocation worker.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_core::{EnvironmentScope, WorkerId};
use runku_data::{LogicalStore, ScheduleCompletion, ScheduledInvocationRecord, StoreError};
use runku_value::TimestampMicros;
use thiserror::Error;

/// Hard worker bounds and retry policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledWorkerConfig {
    /// Maximum records claimed and executed sequentially per poll.
    pub batch_limit: u32,
    /// Lease duration covering the complete worst-case sequential batch.
    pub lease_micros: u64,
    /// Maximum wall time for one destination execution.
    pub invocation_timeout: Duration,
    /// Maximum total claims before terminal failure.
    pub max_attempts: u32,
    /// First retry delay.
    pub retry_base_micros: u64,
    /// Maximum exponential retry delay.
    pub retry_max_micros: u64,
}

impl ScheduledWorkerConfig {
    /// Conservative single-record production defaults.
    pub const PRODUCTION: Self = Self {
        batch_limit: 1,
        lease_micros: 330_000_000,
        invocation_timeout: Duration::from_mins(5),
        max_attempts: 10,
        retry_base_micros: 1_000_000,
        retry_max_micros: 300_000_000,
    };

    fn validate(self) -> Result<Self, ScheduledWorkerError> {
        let timeout = u64::try_from(self.invocation_timeout.as_micros())
            .map_err(|_| ScheduledWorkerError::InvalidConfiguration)?;
        let required_lease = timeout
            .checked_mul(u64::from(self.batch_limit))
            .and_then(|value| value.checked_add(1_000_000))
            .ok_or(ScheduledWorkerError::InvalidConfiguration)?;
        if self.batch_limit == 0
            || self.batch_limit > 100
            || self.invocation_timeout.is_zero()
            || self.max_attempts == 0
            || self.retry_base_micros == 0
            || self.retry_base_micros > self.retry_max_micros
            || self.lease_micros < required_lease
        {
            return Err(ScheduledWorkerError::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// Sanitized destination execution failure used to decide retry versus terminal completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledRunFailure {
    code: String,
    retryable: bool,
}

impl ScheduledRunFailure {
    /// Creates a validated stable failure category.
    ///
    /// # Errors
    ///
    /// Rejects empty/long codes or characters outside `A-Z0-9_`.
    pub fn new(code: impl Into<String>, retryable: bool) -> Result<Self, ScheduledWorkerError> {
        let code = code.into();
        if code.is_empty()
            || code.len() > 64
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ScheduledWorkerError::InvalidFailureCode);
        }
        Ok(Self { code, retryable })
    }

    /// Stable bounded code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Whether unchanged execution may succeed later.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    /// Returns a terminal sanitized fallback for runner adapter failures.
    #[must_use]
    pub fn internal() -> Self {
        Self {
            code: "SCHEDULE_RUNNER_INTERNAL".to_owned(),
            retryable: false,
        }
    }
}

/// Composition boundary that executes the exact pinned record; Channel lookup is forbidden here.
#[async_trait]
pub trait ScheduledInvocationRunner: Send + Sync {
    /// Executes the record's pinned Release/DevRevision, function, and canonical arguments.
    async fn execute(
        &self,
        scope: EnvironmentScope,
        record: &ScheduledInvocationRecord,
    ) -> Result<(), ScheduledRunFailure>;
}

/// Injectable UTC clock used for claims and retry timestamps.
pub trait SchedulerClock: fmt::Debug + Send + Sync {
    /// Returns current signed Unix-epoch microseconds.
    ///
    /// # Errors
    ///
    /// Returns a sanitized clock failure when UTC cannot be obtained or represented.
    fn now(&self) -> Result<TimestampMicros, ScheduledWorkerError>;
}

/// Production system UTC clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemSchedulerClock;

impl SchedulerClock for SystemSchedulerClock {
    fn now(&self) -> Result<TimestampMicros, ScheduledWorkerError> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ScheduledWorkerError::ClockUnavailable)?;
        i64::try_from(elapsed.as_micros())
            .map(TimestampMicros::new)
            .map_err(|_| ScheduledWorkerError::ClockUnavailable)
    }
}

/// Stable worker/configuration failure. Destination failures are durable outcomes, not poll errors.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ScheduledWorkerError {
    /// Configuration is zero, inverted, or cannot cover the full claimed batch.
    #[error("scheduled worker configuration is invalid")]
    InvalidConfiguration,
    /// A runner returned an unsafe error category.
    #[error("scheduled runner failure code is invalid")]
    InvalidFailureCode,
    /// Logical storage failed.
    #[error("scheduled worker storage failed")]
    Storage(StoreError),
    /// UTC time could not be obtained or represented.
    #[error("scheduled worker clock is unavailable")]
    ClockUnavailable,
}

impl ScheduledWorkerError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "SCHEDULE_WORKER_CONFIGURATION_INVALID",
            Self::InvalidFailureCode => "SCHEDULE_RUNNER_ERROR_CODE_INVALID",
            Self::Storage(error) => error.code(),
            Self::ClockUnavailable => "SCHEDULE_CLOCK_UNAVAILABLE",
        }
    }

    /// Whether retrying the poll may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        match self {
            Self::Storage(error) => error.retryable(),
            Self::ClockUnavailable => true,
            Self::InvalidConfiguration | Self::InvalidFailureCode => false,
        }
    }
}

/// Aggregate result of one bounded worker poll.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScheduledPollOutcome {
    /// Records claimed.
    pub claimed: u32,
    /// Records completed successfully.
    pub succeeded: u32,
    /// Records returned to pending with backoff.
    pub retried: u32,
    /// Records completed terminally.
    pub failed: u32,
}

/// Aggregate worker counters without tenant/function labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScheduledWorkerTelemetrySnapshot {
    /// Poll attempts.
    pub polls: u64,
    /// Records claimed.
    pub claimed: u64,
    /// Destination executions started.
    pub executions: u64,
    /// Successful completions.
    pub succeeded: u64,
    /// Retry completions.
    pub retried: u64,
    /// Terminal failure completions.
    pub failed: u64,
    /// Execution timeouts.
    pub timeouts: u64,
    /// Completion attempts rejected by fencing.
    pub lease_lost: u64,
    /// Storage/clock poll failures.
    pub poll_failures: u64,
    /// Maximum observed due lag in microseconds.
    pub max_lag_micros: u64,
}

#[derive(Debug, Default)]
struct ScheduledWorkerTelemetry {
    polls: AtomicU64,
    claimed: AtomicU64,
    executions: AtomicU64,
    succeeded: AtomicU64,
    retried: AtomicU64,
    failed: AtomicU64,
    timeouts: AtomicU64,
    lease_lost: AtomicU64,
    poll_failures: AtomicU64,
    max_lag_micros: AtomicU64,
}

/// Durable at-least-once worker for one logical Environment.
#[derive(Clone)]
pub struct ScheduledWorker {
    store: Arc<dyn LogicalStore>,
    runner: Arc<dyn ScheduledInvocationRunner>,
    clock: Arc<dyn SchedulerClock>,
    worker_id: WorkerId,
    config: ScheduledWorkerConfig,
    telemetry: Arc<ScheduledWorkerTelemetry>,
}

impl fmt::Debug for ScheduledWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScheduledWorker")
            .field("backend", &self.store.backend())
            .field("worker_id", &self.worker_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ScheduledWorker {
    /// Creates a worker using the production system clock.
    ///
    /// # Errors
    ///
    /// Rejects configuration whose lease cannot cover its worst-case sequential batch.
    pub fn new(
        store: Arc<dyn LogicalStore>,
        runner: Arc<dyn ScheduledInvocationRunner>,
        worker_id: WorkerId,
        config: ScheduledWorkerConfig,
    ) -> Result<Self, ScheduledWorkerError> {
        Self::with_clock(
            store,
            runner,
            Arc::new(SystemSchedulerClock),
            worker_id,
            config,
        )
    }

    /// Creates a worker with an injected deterministic clock.
    ///
    /// # Errors
    ///
    /// Applies the same production configuration validation as [`Self::new`].
    pub fn with_clock(
        store: Arc<dyn LogicalStore>,
        runner: Arc<dyn ScheduledInvocationRunner>,
        clock: Arc<dyn SchedulerClock>,
        worker_id: WorkerId,
        config: ScheduledWorkerConfig,
    ) -> Result<Self, ScheduledWorkerError> {
        Ok(Self {
            store,
            runner,
            clock,
            worker_id,
            config: config.validate()?,
            telemetry: Arc::new(ScheduledWorkerTelemetry::default()),
        })
    }

    /// Claims and executes one bounded sequential batch.
    ///
    /// # Errors
    ///
    /// Returns clock/storage failures. Destination failures are durably retried or failed.
    #[allow(clippy::too_many_lines)]
    pub async fn poll_once(
        &self,
        scope: EnvironmentScope,
    ) -> Result<ScheduledPollOutcome, ScheduledWorkerError> {
        self.telemetry.polls.fetch_add(1, Ordering::Relaxed);
        let now = self.clock.now().inspect_err(|_| {
            self.telemetry.poll_failures.fetch_add(1, Ordering::Relaxed);
        })?;
        let lease_delta = i64::try_from(self.config.lease_micros)
            .map_err(|_| ScheduledWorkerError::InvalidConfiguration)?;
        let lease_until = now
            .get()
            .checked_add(lease_delta)
            .map(TimestampMicros::new)
            .ok_or(ScheduledWorkerError::ClockUnavailable)?;
        let claimed = self
            .store
            .claim_due_scheduled(
                scope,
                self.worker_id,
                now,
                lease_until,
                self.config.batch_limit,
            )
            .await
            .inspect_err(|_| {
                self.telemetry.poll_failures.fetch_add(1, Ordering::Relaxed);
            })
            .map_err(ScheduledWorkerError::Storage)?;
        self.telemetry.claimed.fetch_add(
            u64::try_from(claimed.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        for claimed in &claimed {
            observe_lag(&self.telemetry, now, claimed.record.execute_at);
        }
        let mut outcome = ScheduledPollOutcome {
            claimed: u32::try_from(claimed.len()).unwrap_or(u32::MAX),
            ..ScheduledPollOutcome::default()
        };
        for claimed in claimed {
            self.telemetry.executions.fetch_add(1, Ordering::Relaxed);
            let execution = tokio::time::timeout(
                self.config.invocation_timeout,
                self.runner.execute(scope, &claimed.record),
            )
            .await;
            let completion = match execution {
                Ok(Ok(())) => {
                    outcome.succeeded = outcome.succeeded.saturating_add(1);
                    ScheduleCompletion::Succeeded
                }
                Err(_) => {
                    self.telemetry.timeouts.fetch_add(1, Ordering::Relaxed);
                    retry_or_fail(
                        self.config,
                        &claimed.record,
                        "SCHEDULE_EXECUTION_TIMEOUT",
                        true,
                        self.clock.now()?,
                        &mut outcome,
                    )?
                }
                Ok(Err(failure)) => retry_or_fail(
                    self.config,
                    &claimed.record,
                    failure.code(),
                    failure.retryable(),
                    self.clock.now()?,
                    &mut outcome,
                )?,
            };
            let completed = self
                .store
                .complete_scheduled(
                    scope,
                    claimed.record.id,
                    self.worker_id,
                    claimed.record.lease_generation,
                    &completion,
                )
                .await;
            if let Err(error) = completed {
                if error == StoreError::LeaseLost {
                    self.telemetry.lease_lost.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.telemetry.poll_failures.fetch_add(1, Ordering::Relaxed);
                }
                return Err(ScheduledWorkerError::Storage(error));
            }
            match completion {
                ScheduleCompletion::Succeeded => {
                    self.telemetry.succeeded.fetch_add(1, Ordering::Relaxed);
                }
                ScheduleCompletion::Retry { .. } => {
                    self.telemetry.retried.fetch_add(1, Ordering::Relaxed);
                }
                ScheduleCompletion::Failed { .. } => {
                    self.telemetry.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(outcome)
    }

    /// Returns bounded aggregate worker telemetry.
    #[must_use]
    pub fn telemetry(&self) -> ScheduledWorkerTelemetrySnapshot {
        ScheduledWorkerTelemetrySnapshot {
            polls: self.telemetry.polls.load(Ordering::Relaxed),
            claimed: self.telemetry.claimed.load(Ordering::Relaxed),
            executions: self.telemetry.executions.load(Ordering::Relaxed),
            succeeded: self.telemetry.succeeded.load(Ordering::Relaxed),
            retried: self.telemetry.retried.load(Ordering::Relaxed),
            failed: self.telemetry.failed.load(Ordering::Relaxed),
            timeouts: self.telemetry.timeouts.load(Ordering::Relaxed),
            lease_lost: self.telemetry.lease_lost.load(Ordering::Relaxed),
            poll_failures: self.telemetry.poll_failures.load(Ordering::Relaxed),
            max_lag_micros: self.telemetry.max_lag_micros.load(Ordering::Relaxed),
        }
    }
}

fn retry_or_fail(
    config: ScheduledWorkerConfig,
    record: &ScheduledInvocationRecord,
    code: &str,
    retryable: bool,
    now: TimestampMicros,
    outcome: &mut ScheduledPollOutcome,
) -> Result<ScheduleCompletion, ScheduledWorkerError> {
    if retryable && record.attempts < config.max_attempts {
        let shift = record.attempts.saturating_sub(1).min(63);
        let delay = config
            .retry_base_micros
            .saturating_mul(1_u64 << shift)
            .min(config.retry_max_micros);
        let execute_at = now
            .get()
            .checked_add(i64::try_from(delay).map_err(|_| ScheduledWorkerError::ClockUnavailable)?)
            .map(TimestampMicros::new)
            .ok_or(ScheduledWorkerError::ClockUnavailable)?;
        outcome.retried = outcome.retried.saturating_add(1);
        Ok(ScheduleCompletion::Retry {
            execute_at,
            error_code: code.to_owned(),
        })
    } else {
        outcome.failed = outcome.failed.saturating_add(1);
        Ok(ScheduleCompletion::Failed {
            error_code: code.to_owned(),
        })
    }
}

fn observe_lag(
    telemetry: &ScheduledWorkerTelemetry,
    now: TimestampMicros,
    execute_at: TimestampMicros,
) {
    let lag = now.get().saturating_sub(execute_at.get());
    telemetry
        .max_lag_micros
        .fetch_max(u64::try_from(lag).unwrap_or(0), Ordering::Relaxed);
}
