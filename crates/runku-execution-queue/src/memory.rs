//! Process-local queue adapter for development and deterministic tests.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};

use crate::{
    ExecutionClass, ExecutionDelivery, ExecutionJobV1, ExecutionQueue, ExecutionQueueError,
};

#[derive(Debug)]
struct State {
    capacity: usize,
    jobs: Mutex<VecDeque<(ExecutionClass, ExecutionJobV1)>>,
    available: Notify,
}

/// Bounded non-durable adapter used by local mode and unit tests.
#[derive(Clone, Debug)]
pub struct InMemoryExecutionQueue {
    state: Arc<State>,
}

impl InMemoryExecutionQueue {
    /// Creates a queue with a hard maximum number of waiting jobs.
    ///
    /// # Errors
    ///
    /// Rejects zero capacity.
    pub fn new(capacity: usize) -> Result<Self, ExecutionQueueError> {
        if capacity == 0 {
            return Err(ExecutionQueueError::InvalidJob);
        }
        Ok(Self {
            state: Arc::new(State {
                capacity,
                jobs: Mutex::new(VecDeque::new()),
                available: Notify::new(),
            }),
        })
    }

    async fn try_take(&self, class: &ExecutionClass) -> Option<ExecutionJobV1> {
        let mut jobs = self.state.jobs.lock().await;
        let position = jobs.iter().position(|(candidate, _)| candidate == class)?;
        jobs.remove(position).map(|(_, job)| job)
    }
}

#[async_trait]
impl ExecutionQueue for InMemoryExecutionQueue {
    async fn enqueue(
        &self,
        class: &ExecutionClass,
        job: &ExecutionJobV1,
    ) -> Result<(), ExecutionQueueError> {
        job.validate()?;
        let mut jobs = self.state.jobs.lock().await;
        if jobs.len() >= self.state.capacity {
            return Err(ExecutionQueueError::Full);
        }
        jobs.push_back((class.clone(), job.clone()));
        drop(jobs);
        self.state.available.notify_waiters();
        Ok(())
    }

    async fn pull(
        &self,
        class: &ExecutionClass,
        wait: Duration,
    ) -> Result<Option<Box<dyn ExecutionDelivery>>, ExecutionQueueError> {
        if wait.is_zero() {
            return Ok(self.try_take(class).await.map(|job| {
                Box::new(MemoryDelivery {
                    state: Arc::clone(&self.state),
                    class: class.clone(),
                    job,
                }) as Box<dyn ExecutionDelivery>
            }));
        }
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.state.available.notified();
            if let Some(job) = self.try_take(class).await {
                return Ok(Some(Box::new(MemoryDelivery {
                    state: Arc::clone(&self.state),
                    class: class.clone(),
                    job,
                })));
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Ok(None);
            }
        }
    }
}

struct MemoryDelivery {
    state: Arc<State>,
    class: ExecutionClass,
    job: ExecutionJobV1,
}

#[async_trait]
impl ExecutionDelivery for MemoryDelivery {
    fn job(&self) -> &ExecutionJobV1 {
        &self.job
    }

    async fn progress(&self) -> Result<(), ExecutionQueueError> {
        Ok(())
    }

    async fn ack(self: Box<Self>) -> Result<(), ExecutionQueueError> {
        Ok(())
    }

    async fn retry(self: Box<Self>, delay: Option<Duration>) -> Result<(), ExecutionQueueError> {
        if let Some(delay) = delay
            && !delay.is_zero()
        {
            tokio::time::sleep(delay).await;
        }
        let Self { state, class, job } = *self;
        state.jobs.lock().await.push_back((class, job));
        state.available.notify_waiters();
        Ok(())
    }

    async fn terminate(self: Box<Self>) -> Result<(), ExecutionQueueError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use runku_core::{EnvironmentId, InvocationId, ProjectId, ReleaseId, RequestId};

    use super::*;
    use crate::EXECUTION_JOB_FORMAT_VERSION;

    fn job() -> ExecutionJobV1 {
        ExecutionJobV1 {
            format_version: EXECUTION_JOB_FORMAT_VERSION,
            invocation_id: InvocationId::generate(),
            request_id: RequestId::generate(),
            project_id: ProjectId::generate(),
            environment_id: EnvironmentId::generate(),
            release_id: ReleaseId::generate(),
            deadline_unix_ms: 1,
            payload: vec![1],
        }
    }

    #[tokio::test]
    async fn outstanding_pull_receives_new_job_without_poll_delay()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryExecutionQueue::new(10)?;
        let class = ExecutionClass::new("node_oci_v1")?;
        let receiver = queue.clone();
        let receiver_class = class.clone();
        let started = Instant::now();
        let pull =
            tokio::spawn(
                async move { receiver.pull(&receiver_class, Duration::from_secs(2)).await },
            );
        tokio::task::yield_now().await;
        queue.enqueue(&class, &job()).await?;
        let delivery = pull.await??.ok_or("delivery absent")?;
        assert!(started.elapsed() < Duration::from_secs(1));
        delivery.ack().await?;
        Ok(())
    }

    #[tokio::test]
    async fn retry_returns_unstarted_job_to_queue() -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryExecutionQueue::new(10)?;
        let class = ExecutionClass::new("node_host_v1")?;
        let expected = job();
        queue.enqueue(&class, &expected).await?;
        queue
            .pull(&class, Duration::ZERO)
            .await?
            .ok_or("delivery absent")?
            .retry(None)
            .await?;
        let delivery = queue
            .pull(&class, Duration::ZERO)
            .await?
            .ok_or("redelivery absent")?;
        assert_eq!(delivery.job(), &expected);
        delivery.ack().await?;
        Ok(())
    }
}
