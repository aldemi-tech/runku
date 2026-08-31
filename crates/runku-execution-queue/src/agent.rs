//! Capacity-aware runner agent shared by host and OCI execution backends.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use runku_observability::{
    InvocationPerformanceRecorder, InvocationPerformanceSink, PerformanceComponent,
    PerformanceOperation, PerformanceOutcome, PerformanceRuntime,
};
use thiserror::Error;
use tokio::{sync::watch, task::JoinSet};

use crate::{ExecutionClass, ExecutionJobV1, ExecutionQueue, ExecutionQueueError};

/// Stable post-admission execution failure category.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionHandlerError {
    /// Runtime backend or result transport is temporarily unavailable.
    #[error("execution handler is unavailable")]
    Unavailable,
    /// Runtime failed after the queue lease was acknowledged.
    #[error("execution outcome is uncertain")]
    OutcomeUncertain,
}

/// Failure while validating and materializing a job before queue acknowledgement.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionPreparationError {
    /// Job references invalid, incompatible, or corrupt immutable state and must terminate.
    #[error("execution preparation rejected the job")]
    Invalid,
    /// Dependency is temporarily unavailable and the unstarted job may be redelivered.
    #[error("execution preparation is temporarily unavailable")]
    Unavailable,
}

/// Runtime-specific execution materialized and verified while the queue lease is still held.
#[async_trait]
pub trait PreparedExecution: Send {
    /// Starts user code after the caller has durably acknowledged queue admission.
    async fn execute(self: Box<Self>) -> Result<(), ExecutionHandlerError>;
}

/// Two-phase runtime-specific handler used around the queue acknowledgement boundary.
#[async_trait]
pub trait ExecutionHandler: Send + Sync {
    /// Loads and verifies manifest/artifact/policy without starting user code.
    async fn prepare(
        &self,
        job: ExecutionJobV1,
    ) -> Result<Box<dyn PreparedExecution>, ExecutionPreparationError>;

    /// Publishes the terminal outcome for a job whose deadline elapsed before admission.
    async fn expire(&self, _job: &ExecutionJobV1) -> Result<(), ExecutionPreparationError> {
        Ok(())
    }
}

/// Fixed capacity and pull-window settings for one runner agent.
#[derive(Clone, Debug)]
pub struct ExecutionAgentConfig {
    /// Exact compatible runner class.
    pub class: ExecutionClass,
    /// Number of executions this process may run concurrently.
    pub slots: usize,
    /// Maximum concurrent executions from one Project on this agent.
    pub max_concurrent_per_project: usize,
    /// Duration of each outstanding pull before refreshing it.
    pub pull_wait: Duration,
}

impl ExecutionAgentConfig {
    fn validate(&self) -> Result<(), ExecutionQueueError> {
        if !(1..=1_024).contains(&self.slots)
            || !(1..=self.slots).contains(&self.max_concurrent_per_project)
            || self.pull_wait.is_zero()
            || self.pull_wait > Duration::from_secs(30)
        {
            return Err(ExecutionQueueError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Telemetry {
    deliveries: AtomicU64,
    expired: AtomicU64,
    ack_failures: AtomicU64,
    preparation_retries: AtomicU64,
    rejected: AtomicU64,
    completed: AtomicU64,
    uncertain: AtomicU64,
    fairness_deferrals: AtomicU64,
    active_executions: AtomicU64,
    peak_concurrent_executions: AtomicU64,
}

/// Bounded process-local agent counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutionAgentTelemetrySnapshot {
    /// Jobs delivered to free slots.
    pub deliveries: u64,
    /// Jobs terminated because their absolute deadline elapsed while queued.
    pub expired: u64,
    /// Jobs that never started because durable acknowledgement failed.
    pub ack_failures: u64,
    /// Unstarted jobs released for retry after a temporary preparation failure.
    pub preparation_retries: u64,
    /// Invalid/corrupt jobs permanently terminated before execution.
    pub rejected: u64,
    /// Handler completions observed after admission.
    pub completed: u64,
    /// Handler failures after admission, whose external effect may be uncertain.
    pub uncertain: u64,
    /// Deliveries released briefly so another Project can consume available capacity.
    pub fairness_deferrals: u64,
    /// Executions currently running inside this Agent process.
    pub active_executions: u64,
    /// Highest number of simultaneous executions observed since Agent creation.
    pub peak_concurrent_executions: u64,
}

/// Agent that keeps one outstanding pull for each free runtime slot.
pub struct ExecutionAgent {
    queue: Arc<dyn ExecutionQueue>,
    handler: Arc<dyn ExecutionHandler>,
    config: ExecutionAgentConfig,
    telemetry: Telemetry,
    project_admission: Arc<ProjectAdmission>,
    performance: Option<Arc<dyn InvocationPerformanceSink>>,
}

impl std::fmt::Debug for ExecutionAgent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionAgent")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ExecutionAgent {
    /// Creates an agent without starting pulls.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive slots and unsafe pull windows.
    pub fn new(
        queue: Arc<dyn ExecutionQueue>,
        handler: Arc<dyn ExecutionHandler>,
        config: ExecutionAgentConfig,
    ) -> Result<Self, ExecutionQueueError> {
        config.validate()?;
        Ok(Self {
            queue,
            handler,
            config,
            telemetry: Telemetry::default(),
            project_admission: Arc::new(ProjectAdmission::default()),
            performance: None,
        })
    }

    /// Enables bounded Agent delivery/ACK/completion spans.
    #[must_use]
    pub fn with_performance_sink(mut self, sink: Arc<dyn InvocationPerformanceSink>) -> Self {
        self.performance = Some(sink);
        self
    }

    /// Runs one worker per slot until shutdown is requested, then drains admitted handlers.
    ///
    /// Jobs are prepared with lease heartbeats, then acknowledged immediately before starting user
    /// code. Therefore a process crash after acknowledgement is an uncertain outcome and is never
    /// automatically replayed.
    ///
    /// # Errors
    ///
    /// Returns unavailable if a worker task exits abnormally.
    pub async fn run(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), ExecutionQueueError> {
        let mut workers = JoinSet::new();
        for _ in 0..self.config.slots {
            workers.spawn(Arc::clone(&self).run_slot(shutdown.clone()));
        }
        while let Some(result) = workers.join_next().await {
            result.map_err(|_| ExecutionQueueError::Unavailable)?;
        }
        Ok(())
    }

    /// Returns current bounded counters.
    #[must_use]
    pub fn telemetry(&self) -> ExecutionAgentTelemetrySnapshot {
        ExecutionAgentTelemetrySnapshot {
            deliveries: self.telemetry.deliveries.load(Ordering::Relaxed),
            expired: self.telemetry.expired.load(Ordering::Relaxed),
            ack_failures: self.telemetry.ack_failures.load(Ordering::Relaxed),
            preparation_retries: self.telemetry.preparation_retries.load(Ordering::Relaxed),
            rejected: self.telemetry.rejected.load(Ordering::Relaxed),
            completed: self.telemetry.completed.load(Ordering::Relaxed),
            uncertain: self.telemetry.uncertain.load(Ordering::Relaxed),
            fairness_deferrals: self.telemetry.fairness_deferrals.load(Ordering::Relaxed),
            active_executions: self.telemetry.active_executions.load(Ordering::Relaxed),
            peak_concurrent_executions: self
                .telemetry
                .peak_concurrent_executions
                .load(Ordering::Relaxed),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn run_slot(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let delivery = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                    continue;
                }
                result = self.queue.pull(&self.config.class, self.config.pull_wait) => {
                    if let Ok(delivery) = result {
                        delivery
                    } else {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            };
            let Some(delivery) = delivery else {
                continue;
            };
            self.telemetry.deliveries.fetch_add(1, Ordering::Relaxed);
            if is_expired(delivery.job().deadline_unix_ms) {
                if self.expire(delivery).await {
                    self.telemetry.expired.fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
            let job = delivery.job().clone();
            let recorder = self.performance.as_ref().map(|sink| {
                InvocationPerformanceRecorder::new(
                    job.request_id,
                    job.invocation_id,
                    PerformanceRuntime::RemoteAgent,
                    Arc::clone(sink),
                )
            });
            let mut invocation_timer = recorder.as_ref().map(|recorder| {
                recorder.start(
                    PerformanceComponent::Agent,
                    PerformanceOperation::Invocation,
                    u64::try_from(job.payload.len()).ok(),
                )
            });
            let Some(_project_permit) = self
                .project_admission
                .acquire(job.project_id, self.config.max_concurrent_per_project)
            else {
                if delivery
                    .retry(Some(Duration::from_millis(25)))
                    .await
                    .is_ok()
                {
                    self.telemetry
                        .fairness_deferrals
                        .fetch_add(1, Ordering::Relaxed);
                }
                finish_agent_invocation(
                    invocation_timer.take(),
                    PerformanceOutcome::Busy,
                    Some("PROJECT_ADMISSION_BUSY"),
                );
                continue;
            };
            let prepared = match self.prepare_with_heartbeat(delivery.as_ref(), job).await {
                Ok(prepared) => prepared,
                Err(ExecutionPreparationError::Invalid) => {
                    if delivery.terminate().await.is_ok() {
                        self.telemetry.rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    finish_agent_invocation(
                        invocation_timer.take(),
                        PerformanceOutcome::Failed,
                        Some("EXECUTION_PREPARATION_INVALID"),
                    );
                    continue;
                }
                Err(ExecutionPreparationError::Unavailable) => {
                    if delivery
                        .retry(Some(Duration::from_millis(100)))
                        .await
                        .is_ok()
                    {
                        self.telemetry
                            .preparation_retries
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    finish_agent_invocation(
                        invocation_timer.take(),
                        PerformanceOutcome::Failed,
                        Some("EXECUTION_PREPARATION_UNAVAILABLE"),
                    );
                    continue;
                }
            };
            if is_expired(delivery.job().deadline_unix_ms) {
                if self.expire(delivery).await {
                    self.telemetry.expired.fetch_add(1, Ordering::Relaxed);
                }
                finish_agent_invocation(
                    invocation_timer.take(),
                    PerformanceOutcome::DeadlineExceeded,
                    Some("EXECUTION_DEADLINE_EXCEEDED"),
                );
                continue;
            }
            let ack_timer = recorder.as_ref().map(|recorder| {
                recorder.start(
                    PerformanceComponent::Queue,
                    PerformanceOperation::Acknowledge,
                    None,
                )
            });
            let acknowledged = delivery.ack().await;
            finish_agent_timer(ack_timer, &acknowledged);
            if acknowledged.is_err() {
                self.telemetry.ack_failures.fetch_add(1, Ordering::Relaxed);
                finish_agent_invocation(
                    invocation_timer.take(),
                    PerformanceOutcome::Failed,
                    Some("EXECUTION_ACK_UNAVAILABLE"),
                );
                continue;
            }
            let _active_execution = ActiveExecutionGuard::enter(&self.telemetry);
            let executed = prepared.execute().await;
            if executed.is_ok() {
                self.telemetry.completed.fetch_add(1, Ordering::Relaxed);
                finish_agent_invocation(
                    invocation_timer.take(),
                    PerformanceOutcome::Succeeded,
                    None,
                );
            } else {
                self.telemetry.uncertain.fetch_add(1, Ordering::Relaxed);
                finish_agent_invocation(
                    invocation_timer.take(),
                    PerformanceOutcome::Uncertain,
                    Some("EXECUTION_OUTCOME_UNCERTAIN"),
                );
            }
        }
    }

    async fn expire(&self, delivery: Box<dyn crate::ExecutionDelivery>) -> bool {
        match self.handler.expire(delivery.job()).await {
            Ok(()) | Err(ExecutionPreparationError::Invalid) => delivery.terminate().await.is_ok(),
            Err(ExecutionPreparationError::Unavailable) => {
                let _ = delivery.retry(Some(Duration::from_millis(100))).await;
                false
            }
        }
    }

    async fn prepare_with_heartbeat(
        &self,
        delivery: &dyn crate::ExecutionDelivery,
        job: ExecutionJobV1,
    ) -> Result<Box<dyn PreparedExecution>, ExecutionPreparationError> {
        let preparation = self.handler.prepare(job);
        tokio::pin!(preparation);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
        heartbeat.tick().await;
        loop {
            tokio::select! {
                result = &mut preparation => return result,
                _ = heartbeat.tick() => {
                    delivery
                        .progress()
                        .await
                        .map_err(|_| ExecutionPreparationError::Unavailable)?;
                }
            }
        }
    }
}

struct ActiveExecutionGuard<'a> {
    active: &'a AtomicU64,
}

impl<'a> ActiveExecutionGuard<'a> {
    fn enter(telemetry: &'a Telemetry) -> Self {
        let active = telemetry.active_executions.fetch_add(1, Ordering::Relaxed) + 1;
        telemetry
            .peak_concurrent_executions
            .fetch_max(active, Ordering::Relaxed);
        Self {
            active: &telemetry.active_executions,
        }
    }
}

impl Drop for ActiveExecutionGuard<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct ProjectAdmission {
    active: Mutex<HashMap<runku_core::ProjectId, usize>>,
}

impl ProjectAdmission {
    fn acquire(
        self: &Arc<Self>,
        project_id: runku_core::ProjectId,
        maximum: usize,
    ) -> Option<ProjectPermit> {
        let mut active = self.active.lock().ok()?;
        let count = active.entry(project_id).or_default();
        if *count >= maximum {
            return None;
        }
        *count += 1;
        Some(ProjectPermit {
            admission: Arc::clone(self),
            project_id,
        })
    }
}

struct ProjectPermit {
    admission: Arc<ProjectAdmission>,
    project_id: runku_core::ProjectId,
}

impl Drop for ProjectPermit {
    fn drop(&mut self) {
        let Ok(mut active) = self.admission.active.lock() else {
            return;
        };
        if let Some(count) = active.get_mut(&self.project_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(&self.project_id);
            }
        }
    }
}

fn is_expired(deadline_unix_ms: u64) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(u128::MAX, |duration| duration.as_millis());
    now >= u128::from(deadline_unix_ms)
}

fn finish_agent_timer<T, E>(
    timer: Option<runku_observability::InvocationPerformanceTimer>,
    result: &Result<T, E>,
) {
    let Some(timer) = timer else { return };
    if result.is_ok() {
        timer.finish(PerformanceOutcome::Succeeded, None, None, None);
    } else {
        timer.finish(
            PerformanceOutcome::Failed,
            Some("EXECUTION_QUEUE_UNAVAILABLE"),
            None,
            None,
        );
    }
}

fn finish_agent_invocation(
    timer: Option<runku_observability::InvocationPerformanceTimer>,
    outcome: PerformanceOutcome,
    error_code: Option<&'static str>,
) {
    if let Some(timer) = timer {
        timer.finish(outcome, error_code, None, None);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use runku_core::{EnvironmentId, InvocationId, ProjectId, ReleaseId, RequestId};
    use tokio::sync::Semaphore;

    use super::*;
    use crate::{EXECUTION_JOB_FORMAT_VERSION, ExecutionQueue, InMemoryExecutionQueue};

    struct BlockingState {
        active: AtomicUsize,
        maximum: AtomicUsize,
        started: Semaphore,
        release: Semaphore,
    }

    struct BlockingHandler {
        state: Arc<BlockingState>,
    }

    struct BlockingExecution {
        state: Arc<BlockingState>,
    }

    #[async_trait]
    impl ExecutionHandler for BlockingHandler {
        async fn prepare(
            &self,
            _job: ExecutionJobV1,
        ) -> Result<Box<dyn PreparedExecution>, ExecutionPreparationError> {
            Ok(Box::new(BlockingExecution {
                state: Arc::clone(&self.state),
            }))
        }
    }

    #[async_trait]
    impl PreparedExecution for BlockingExecution {
        async fn execute(self: Box<Self>) -> Result<(), ExecutionHandlerError> {
            let active = self.state.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.state.maximum.fetch_max(active, Ordering::SeqCst);
            self.state.started.add_permits(1);
            let permit = self
                .state
                .release
                .acquire()
                .await
                .map_err(|_| ExecutionHandlerError::Unavailable)?;
            permit.forget();
            self.state.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn job() -> ExecutionJobV1 {
        job_for(ProjectId::generate())
    }

    fn job_for(project_id: ProjectId) -> ExecutionJobV1 {
        let deadline = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(u64::MAX, |duration| {
                u64::try_from((duration + Duration::from_secs(60)).as_millis()).unwrap_or(u64::MAX)
            });
        ExecutionJobV1 {
            format_version: EXECUTION_JOB_FORMAT_VERSION,
            invocation_id: InvocationId::generate(),
            request_id: RequestId::generate(),
            project_id,
            environment_id: EnvironmentId::generate(),
            release_id: ReleaseId::generate(),
            deadline_unix_ms: deadline,
            payload: vec![1],
        }
    }

    #[tokio::test]
    async fn agent_never_exceeds_declared_slots() -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryExecutionQueue::new(10)?);
        let class = ExecutionClass::new("node_oci_v1")?;
        let state = Arc::new(BlockingState {
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let handler = Arc::new(BlockingHandler {
            state: Arc::clone(&state),
        });
        let agent = Arc::new(ExecutionAgent::new(
            queue.clone(),
            handler.clone(),
            ExecutionAgentConfig {
                class: class.clone(),
                slots: 2,
                max_concurrent_per_project: 2,
                pull_wait: Duration::from_millis(50),
            },
        )?);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn(Arc::clone(&agent).run(shutdown_rx));
        for _ in 0..3 {
            queue.enqueue(&class, &job()).await?;
        }
        state.started.acquire().await?.forget();
        state.started.acquire().await?.forget();
        assert_eq!(state.maximum.load(Ordering::SeqCst), 2);
        state.release.add_permits(2);
        state.started.acquire().await?.forget();
        state.release.add_permits(1);
        shutdown_tx.send(true)?;
        running.await??;
        let telemetry = agent.telemetry();
        assert_eq!(telemetry.completed, 3);
        assert_eq!(telemetry.active_executions, 0);
        assert_eq!(telemetry.peak_concurrent_executions, 2);
        Ok(())
    }

    #[tokio::test]
    async fn project_cap_prevents_one_project_from_consuming_every_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(InMemoryExecutionQueue::new(10)?);
        let class = ExecutionClass::new("node_oci_v1")?;
        let state = Arc::new(BlockingState {
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let agent = Arc::new(ExecutionAgent::new(
            queue.clone(),
            Arc::new(BlockingHandler {
                state: Arc::clone(&state),
            }),
            ExecutionAgentConfig {
                class: class.clone(),
                slots: 2,
                max_concurrent_per_project: 1,
                pull_wait: Duration::from_millis(25),
            },
        )?);
        let project_a = ProjectId::generate();
        let project_b = ProjectId::generate();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn(Arc::clone(&agent).run(shutdown_rx));
        queue.enqueue(&class, &job_for(project_a)).await?;
        queue.enqueue(&class, &job_for(project_a)).await?;
        queue.enqueue(&class, &job_for(project_b)).await?;
        state.started.acquire().await?.forget();
        state.started.acquire().await?.forget();
        assert_eq!(state.maximum.load(Ordering::SeqCst), 2);
        assert!(agent.telemetry().fairness_deferrals > 0);
        state.release.add_permits(2);
        state.started.acquire().await?.forget();
        state.release.add_permits(1);
        shutdown_tx.send(true)?;
        running.await??;
        let telemetry = agent.telemetry();
        assert_eq!(telemetry.completed, 3);
        assert_eq!(telemetry.active_executions, 0);
        assert_eq!(telemetry.peak_concurrent_executions, 2);
        Ok(())
    }
}
