//! Process-local execution control plane used by deterministic vertical tests.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use runku_core::InvocationId;
use tokio::sync::{Mutex, Notify};

use crate::{
    EXECUTION_CONTROL_FORMAT_VERSION, ExecutionCompletion, ExecutionControlError,
    ExecutionControlPlane, ExecutionRecordV1, ExecutionState, VersionedExecutionRecord,
    control::{Transition, transition},
};

#[derive(Debug)]
struct Entry {
    revision: u64,
    record: ExecutionRecordV1,
    changed: Arc<Notify>,
}

/// Non-durable latest-state adapter for tests and one-process self-hosting.
#[derive(Clone, Debug, Default)]
pub struct InMemoryExecutionControlPlane {
    entries: Arc<Mutex<HashMap<InvocationId, Entry>>>,
}

impl InMemoryExecutionControlPlane {
    async fn apply(
        &self,
        invocation_id: InvocationId,
        next: Transition,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        let mut entries = self.entries.lock().await;
        let entry = entries
            .get_mut(&invocation_id)
            .ok_or(ExecutionControlError::NotFound)?;
        let candidate = transition(&entry.record, next)?;
        if candidate != entry.record {
            entry.revision = entry.revision.saturating_add(1);
            entry.record = candidate;
            entry.changed.notify_waiters();
        }
        Ok(VersionedExecutionRecord {
            revision: entry.revision,
            record: entry.record.clone(),
        })
    }
}

#[async_trait]
impl ExecutionControlPlane for InMemoryExecutionControlPlane {
    async fn register(
        &self,
        invocation_id: InvocationId,
        deadline_unix_ms: u64,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        let record = ExecutionRecordV1 {
            format_version: EXECUTION_CONTROL_FORMAT_VERSION,
            invocation_id,
            deadline_unix_ms,
            state: ExecutionState::Queued,
            result: None,
            error_code: None,
        };
        record.validate()?;
        let mut entries = self.entries.lock().await;
        if let Some(existing) = entries.get(&invocation_id) {
            return if existing.record.deadline_unix_ms == deadline_unix_ms {
                Ok(VersionedExecutionRecord {
                    revision: existing.revision,
                    record: existing.record.clone(),
                })
            } else {
                Err(ExecutionControlError::Conflict)
            };
        }
        entries.insert(
            invocation_id,
            Entry {
                revision: 1,
                record: record.clone(),
                changed: Arc::new(Notify::new()),
            },
        );
        Ok(VersionedExecutionRecord {
            revision: 1,
            record,
        })
    }

    async fn begin_preparing(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.apply(invocation_id, Transition::Preparing).await
    }

    async fn begin_running(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.apply(invocation_id, Transition::Running).await
    }

    async fn request_cancel(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.apply(invocation_id, Transition::Cancel).await
    }

    async fn complete(
        &self,
        invocation_id: InvocationId,
        completion: ExecutionCompletion,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.apply(invocation_id, Transition::Complete(completion))
            .await
    }

    async fn get(
        &self,
        invocation_id: InvocationId,
    ) -> Result<VersionedExecutionRecord, ExecutionControlError> {
        self.entries
            .lock()
            .await
            .get(&invocation_id)
            .map(|entry| VersionedExecutionRecord {
                revision: entry.revision,
                record: entry.record.clone(),
            })
            .ok_or(ExecutionControlError::NotFound)
    }

    async fn wait_changed(
        &self,
        invocation_id: InvocationId,
        after: u64,
        wait: Duration,
    ) -> Result<Option<VersionedExecutionRecord>, ExecutionControlError> {
        if wait.is_zero() {
            return Ok(None);
        }
        let changed = {
            let entries = self.entries.lock().await;
            let entry = entries
                .get(&invocation_id)
                .ok_or(ExecutionControlError::NotFound)?;
            if entry.revision > after {
                return Ok(Some(VersionedExecutionRecord {
                    revision: entry.revision,
                    record: entry.record.clone(),
                }));
            }
            Arc::clone(&entry.changed)
        };
        if tokio::time::timeout(wait, changed.notified())
            .await
            .is_err()
        {
            return Ok(None);
        }
        self.get(invocation_id).await.map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_is_durable_and_terminal_completion_is_idempotent()
    -> Result<(), Box<dyn std::error::Error>> {
        let control = InMemoryExecutionControlPlane::default();
        let id = InvocationId::generate();
        control.register(id, 10).await?;
        control.begin_preparing(id).await?;
        assert_eq!(
            control.request_cancel(id).await?.record.state,
            ExecutionState::CancelRequested
        );
        assert_eq!(
            control.begin_running(id).await?.record.state,
            ExecutionState::CancelRequested
        );
        let terminal = control.complete(id, ExecutionCompletion::Cancelled).await?;
        assert_eq!(terminal.record.state, ExecutionState::Cancelled);
        assert_eq!(
            control.complete(id, ExecutionCompletion::Cancelled).await?,
            terminal
        );
        Ok(())
    }

    #[tokio::test]
    async fn waiter_observes_a_later_terminal_revision() -> Result<(), Box<dyn std::error::Error>> {
        let control = InMemoryExecutionControlPlane::default();
        let id = InvocationId::generate();
        let initial = control.register(id, 10).await?;
        let waiter = control.clone();
        let task = tokio::spawn(async move {
            waiter
                .wait_changed(id, initial.revision, Duration::from_secs(1))
                .await
        });
        tokio::task::yield_now().await;
        control
            .complete(
                id,
                ExecutionCompletion::Failed("RUNTIME_JAVASCRIPT_ERROR".to_owned()),
            )
            .await?;
        assert_eq!(
            task.await??.ok_or("missing update")?.record.state,
            ExecutionState::Failed
        );
        Ok(())
    }
}
