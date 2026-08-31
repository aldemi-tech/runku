//! Bounded process-local store telemetry.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Snapshot of adapter counters without tenant-cardinality labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreTelemetrySnapshot {
    /// Read snapshots opened.
    pub snapshots_opened: u64,
    /// Logical read operations attempted.
    pub reads: u64,
    /// Newly committed batches.
    pub commits: u64,
    /// Idempotent commit replays.
    pub commit_replays: u64,
    /// OCC conflicts observed.
    pub conflicts: u64,
    /// Retryable backend failures observed.
    pub retryable_errors: u64,
    /// Non-retryable backend failures observed.
    pub terminal_errors: u64,
    /// Scheduled records claimed.
    pub schedules_claimed: u64,
    /// Scheduled records transitioned to cancelled.
    pub schedules_cancelled: u64,
    /// Outbox events returned in successful claims.
    pub outbox_events_claimed: u64,
    /// Durable outbox cursor acknowledgements.
    pub outbox_acks: u64,
    /// Sum of completed commit latency in microseconds.
    pub commit_latency_micros_total: u64,
    /// Maximum completed commit latency in microseconds.
    pub commit_latency_micros_max: u64,
    /// Current physical pool size.
    pub pool_size: u32,
    /// Current idle physical connections.
    pub pool_idle: u32,
}

#[derive(Debug, Default)]
struct Counters {
    snapshots_opened: AtomicU64,
    reads: AtomicU64,
    commits: AtomicU64,
    commit_replays: AtomicU64,
    conflicts: AtomicU64,
    retryable_errors: AtomicU64,
    terminal_errors: AtomicU64,
    schedules_claimed: AtomicU64,
    schedules_cancelled: AtomicU64,
    outbox_events_claimed: AtomicU64,
    outbox_acks: AtomicU64,
    commit_latency_micros_total: AtomicU64,
    commit_latency_micros_max: AtomicU64,
}

/// Shared telemetry owner embedded by one adapter instance.
#[derive(Clone, Debug, Default)]
pub struct StoreTelemetry {
    inner: Arc<Counters>,
}

impl StoreTelemetry {
    /// Returns a lightweight recorder for adapter operations.
    #[must_use]
    pub fn recorder(&self) -> StoreTelemetryRecorder {
        StoreTelemetryRecorder {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Captures counters and caller-supplied pool gauges.
    #[must_use]
    pub fn snapshot(&self, pool_size: u32, pool_idle: u32) -> StoreTelemetrySnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        StoreTelemetrySnapshot {
            snapshots_opened: load(&self.inner.snapshots_opened),
            reads: load(&self.inner.reads),
            commits: load(&self.inner.commits),
            commit_replays: load(&self.inner.commit_replays),
            conflicts: load(&self.inner.conflicts),
            retryable_errors: load(&self.inner.retryable_errors),
            terminal_errors: load(&self.inner.terminal_errors),
            schedules_claimed: load(&self.inner.schedules_claimed),
            schedules_cancelled: load(&self.inner.schedules_cancelled),
            outbox_events_claimed: load(&self.inner.outbox_events_claimed),
            outbox_acks: load(&self.inner.outbox_acks),
            commit_latency_micros_total: load(&self.inner.commit_latency_micros_total),
            commit_latency_micros_max: load(&self.inner.commit_latency_micros_max),
            pool_size,
            pool_idle,
        }
    }
}

/// Mutation API used internally by adapters while keeping counters common.
#[derive(Clone, Debug)]
pub struct StoreTelemetryRecorder {
    inner: Arc<Counters>,
}

impl StoreTelemetryRecorder {
    /// Records one opened snapshot.
    pub fn snapshot_opened(&self) {
        self.inner.snapshots_opened.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one logical read.
    pub fn read(&self) {
        self.inner.reads.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one new or replayed commit and its latency.
    pub fn commit(&self, replayed: bool, latency_micros: u64) {
        let counter = if replayed {
            &self.inner.commit_replays
        } else {
            &self.inner.commits
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.inner
            .commit_latency_micros_total
            .fetch_add(latency_micros, Ordering::Relaxed);
        self.inner
            .commit_latency_micros_max
            .fetch_max(latency_micros, Ordering::Relaxed);
    }

    /// Records one OCC conflict.
    pub fn conflict(&self) {
        self.inner.conflicts.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one stable error by retryability.
    pub fn error(&self, retryable: bool) {
        let counter = if retryable {
            &self.inner.retryable_errors
        } else {
            &self.inner.terminal_errors
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds successfully claimed scheduler records.
    pub fn schedules_claimed(&self, count: usize) {
        self.inner
            .schedules_claimed
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// Records one newly cancelled scheduled invocation.
    pub fn schedule_cancelled(&self) {
        self.inner
            .schedules_cancelled
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Adds outbox events returned from one successful lease claim.
    pub fn outbox_claimed(&self, count: usize) {
        self.inner
            .outbox_events_claimed
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// Records one durable outbox cursor acknowledgement.
    pub fn outbox_ack(&self) {
        self.inner.outbox_acks.fetch_add(1, Ordering::Relaxed);
    }
}
