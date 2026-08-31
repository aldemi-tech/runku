//! Bounded host worker pool and admission queue.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::Instant,
};

use runku_observability::{
    InvocationPerformanceTimer, PerformanceComponent, PerformanceOperation, PerformanceOutcome,
};
use runku_value::CanonicalValue;
use runku_value::encode_stored_value;
use tokio::sync::oneshot;

use crate::{
    RuntimeError,
    invocation::{InvocationRequest, RuntimeLimits, RuntimeTelemetry, RuntimeTelemetrySnapshot},
    worker,
};

struct WorkItem {
    request: InvocationRequest,
    deadline: Instant,
    queue_timer: Option<InvocationPerformanceTimer>,
    response: oneshot::Sender<Result<CanonicalValue, RuntimeError>>,
}

struct SupervisorInner {
    limits: RuntimeLimits,
    sender: Mutex<Option<SyncSender<WorkItem>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    telemetry: Arc<RuntimeTelemetry>,
    nested_active: Arc<AtomicUsize>,
}

impl Drop for SupervisorInner {
    fn drop(&mut self) {
        if let Ok(sender) = self.sender.get_mut() {
            sender.take();
        }
        if let Ok(workers) = self.workers.get_mut() {
            for worker in workers.drain(..) {
                drop(worker.join());
            }
        }
    }
}

/// Cloneable handle to a bounded Safe Runtime worker pool.
#[derive(Clone)]
pub struct RuntimeSupervisor {
    inner: Arc<SupervisorInner>,
}

impl RuntimeSupervisor {
    /// Starts exactly the configured number of worker threads and one bounded admission queue.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Unavailable`] if the host cannot create every configured worker;
    /// already-created workers are stopped before returning.
    pub fn start(limits: RuntimeLimits) -> Result<Self, RuntimeError> {
        deno_core::JsRuntime::init_platform(None);
        let (sender, receiver) = mpsc::sync_channel(limits.queue_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let telemetry = Arc::new(RuntimeTelemetry::default());
        let mut workers = Vec::with_capacity(limits.worker_count);
        for index in 0..limits.worker_count {
            let worker_receiver = Arc::clone(&receiver);
            let worker_telemetry = Arc::clone(&telemetry);
            let worker = thread::Builder::new()
                .name(format!("runku-v8-worker-{index}"))
                .spawn(move || worker_loop(&worker_receiver, &worker_telemetry, limits));
            if let Ok(worker) = worker {
                workers.push(worker);
            } else {
                drop(sender);
                for worker in workers {
                    drop(worker.join());
                }
                return Err(RuntimeError::Unavailable);
            }
        }
        Ok(Self {
            inner: Arc::new(SupervisorInner {
                limits,
                sender: Mutex::new(Some(sender)),
                workers: Mutex::new(workers),
                telemetry,
                nested_active: Arc::new(AtomicUsize::new(0)),
            }),
        })
    }

    /// Admits and executes one immutable invocation.
    ///
    /// The wall timeout begins before queue admission. A full queue fails immediately with
    /// [`RuntimeError::Busy`]; no unbounded task/thread/isolate is created.
    ///
    /// # Errors
    ///
    /// Returns stable validation, admission, JavaScript, cancellation, or resource-limit errors.
    pub async fn invoke(
        &self,
        mut request: InvocationRequest,
    ) -> Result<CanonicalValue, RuntimeError> {
        let recorder = request.performance().cloned();
        let input_bytes = recorder.as_ref().and_then(|_| {
            encode_stored_value(request.arguments())
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok())
        });
        let total_timer = recorder.as_ref().map(|recorder| {
            recorder.start(
                PerformanceComponent::Runtime,
                PerformanceOperation::Invocation,
                input_bytes,
            )
        });
        if request.wall_timeout > self.inner.limits.max_wall_time {
            let result = Err(RuntimeError::InvalidInvocation);
            self.inner.telemetry.record(&result);
            finish_timer(total_timer, &result);
            return result;
        }
        let deadline = Instant::now()
            .checked_add(request.wall_timeout)
            .ok_or(RuntimeError::InvalidInvocation)?;
        request.telemetry = Some(Arc::clone(&self.inner.telemetry));
        let (response, receiver) = oneshot::channel();
        let queue_timer = recorder.as_ref().map(|recorder| {
            recorder.start(
                PerformanceComponent::Runtime,
                PerformanceOperation::Admission,
                None,
            )
        });
        let item = WorkItem {
            request,
            deadline,
            queue_timer,
            response,
        };
        let send_result = self
            .inner
            .sender
            .lock()
            .map_err(|_| RuntimeError::Unavailable)?
            .as_ref()
            .ok_or(RuntimeError::Unavailable)?
            .try_send(item);
        match send_result {
            Ok(()) => self.inner.telemetry.admitted(),
            Err(TrySendError::Full(item)) => {
                self.inner.telemetry.busy();
                if let Some(timer) = item.queue_timer {
                    timer.finish(
                        PerformanceOutcome::Busy,
                        Some(RuntimeError::Busy.code()),
                        None,
                        None,
                    );
                }
                if let Some(timer) = total_timer {
                    timer.finish(
                        PerformanceOutcome::Busy,
                        Some(RuntimeError::Busy.code()),
                        None,
                        None,
                    );
                }
                return Err(RuntimeError::Busy);
            }
            Err(TrySendError::Disconnected(item)) => {
                let result = Err(RuntimeError::Unavailable);
                self.inner.telemetry.record(&result);
                finish_timer(item.queue_timer, &result);
                finish_timer(total_timer, &result);
                return result;
            }
        }
        let result = receiver.await.unwrap_or_else(|_| {
            let result = Err(RuntimeError::Unavailable);
            self.inner.telemetry.record(&result);
            result
        });
        finish_timer(total_timer, &result);
        result
    }

    /// Executes a previously-derived child invocation in separate bounded capacity.
    ///
    /// No queue is used: saturation fails immediately before a thread or isolate is created. Each
    /// admitted child owns one temporary execution thread, so recursive callers never wait for a
    /// child queued behind the same primary or nested worker that is currently awaiting it.
    ///
    /// # Errors
    ///
    /// Returns stable tree-limit, busy, deadline, cancellation, validation, or execution errors.
    pub async fn invoke_nested(
        &self,
        request: InvocationRequest,
    ) -> Result<CanonicalValue, RuntimeError> {
        let deadline = Instant::now()
            .checked_add(request.wall_timeout)
            .ok_or(RuntimeError::InvalidInvocation)?;
        self.invoke_nested_until(request, deadline).await
    }

    /// Executes a child invocation under an absolute deadline inherited from its parent.
    ///
    /// This is the broker-facing form of [`Self::invoke_nested`]; it prevents repeated nesting
    /// from extending the root deadline by coordinator or thread-admission latency.
    ///
    /// # Errors
    ///
    /// Returns the same bounded failures as [`Self::invoke_nested`] and rejects a deadline beyond
    /// the child envelope's remaining wall budget.
    pub async fn invoke_nested_until(
        &self,
        mut request: InvocationRequest,
        deadline: Instant,
    ) -> Result<CanonicalValue, RuntimeError> {
        let now = Instant::now();
        if request.wall_timeout > self.inner.limits.max_wall_time
            || deadline <= now
            || deadline.saturating_duration_since(now) > request.wall_timeout
        {
            let result = Err(RuntimeError::InvalidInvocation);
            self.inner.telemetry.nested_result(&result);
            return result;
        }
        if let Err(error) = request.admit_nested_call(self.inner.limits) {
            let result = Err(error);
            self.inner.telemetry.nested_result(&result);
            return result;
        }
        request.telemetry = Some(Arc::clone(&self.inner.telemetry));
        let admitted =
            self.inner
                .nested_active
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                    (active < self.inner.limits.max_nested_concurrency).then_some(active + 1)
                });
        if admitted.is_err() {
            self.inner.telemetry.nested_busy();
            return Err(RuntimeError::Busy);
        }

        let active = Arc::clone(&self.inner.nested_active);
        let telemetry = Arc::clone(&self.inner.telemetry);
        let limits = self.inner.limits;
        let (response, receiver) = oneshot::channel();
        let spawn_result = thread::Builder::new()
            .name(format!("runku-v8-nested-{}", request.nested_depth()))
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_io()
                        .enable_time()
                        .build()
                        .map_err(|_| RuntimeError::Unavailable)?;
                    runtime.block_on(worker::execute(request, limits, deadline))
                }))
                .unwrap_or(Err(RuntimeError::Internal));
                telemetry.record(&result);
                telemetry.nested_result(&result);
                active.fetch_sub(1, Ordering::AcqRel);
                drop(response.send(result));
            });
        if spawn_result.is_err() {
            self.inner.nested_active.fetch_sub(1, Ordering::AcqRel);
            let result = Err(RuntimeError::Unavailable);
            self.inner.telemetry.record(&result);
            self.inner.telemetry.nested_result(&result);
            return result;
        }
        self.inner.telemetry.nested_admitted();
        receiver.await.unwrap_or_else(|_| {
            let result = Err(RuntimeError::Unavailable);
            self.inner.telemetry.record(&result);
            result
        })
    }

    /// Returns bounded process-local counters without tenant-controlled labels.
    #[must_use]
    pub fn telemetry(&self) -> RuntimeTelemetrySnapshot {
        self.inner.telemetry.snapshot()
    }

    /// Returns the validated immutable supervisor limits.
    #[must_use]
    pub fn limits(&self) -> RuntimeLimits {
        self.inner.limits
    }
}

fn worker_loop(
    receiver: &Arc<Mutex<Receiver<WorkItem>>>,
    telemetry: &Arc<RuntimeTelemetry>,
    limits: RuntimeLimits,
) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    else {
        return;
    };
    loop {
        let item = match receiver.lock() {
            Ok(receiver) => match receiver.recv() {
                Ok(item) => item,
                Err(_) => return,
            },
            Err(_) => return,
        };
        if let Some(timer) = item.queue_timer {
            timer.finish(PerformanceOutcome::Succeeded, None, None, None);
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                let result = worker::execute(item.request, limits, item.deadline).await;
                // Drivers may return pooled resources through spawned ready tasks when an Op
                // future drops. A current-thread runtime freezes those tasks as soon as
                // `block_on` returns, so yield once before acknowledging the invocation.
                tokio::task::yield_now().await;
                result
            })
        }))
        .unwrap_or(Err(RuntimeError::Internal));
        telemetry.record(&result);
        drop(item.response.send(result));
    }
}

fn finish_timer(
    timer: Option<InvocationPerformanceTimer>,
    result: &Result<CanonicalValue, RuntimeError>,
) {
    let Some(timer) = timer else { return };
    let output_bytes = result.as_ref().ok().and_then(|value| {
        encode_stored_value(value)
            .ok()
            .and_then(|bytes| u64::try_from(bytes.len()).ok())
    });
    let (outcome, code) = match result {
        Ok(_) => (PerformanceOutcome::Succeeded, None),
        Err(RuntimeError::Busy) => (PerformanceOutcome::Busy, Some(RuntimeError::Busy.code())),
        Err(RuntimeError::DeadlineExceeded) => (
            PerformanceOutcome::DeadlineExceeded,
            Some(RuntimeError::DeadlineExceeded.code()),
        ),
        Err(RuntimeError::Cancelled) => (
            PerformanceOutcome::Cancelled,
            Some(RuntimeError::Cancelled.code()),
        ),
        Err(error) => (PerformanceOutcome::Failed, Some(error.code())),
    };
    timer.finish(outcome, code, output_bytes, None);
}
