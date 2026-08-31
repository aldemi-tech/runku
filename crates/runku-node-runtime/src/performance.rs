use runku_observability::{
    InvocationPerformanceTimer, PerformanceOutcome, PerformanceResourceUsage,
};
use runku_runtime::RuntimeError;

pub(crate) fn finish<T>(
    timer: Option<InvocationPerformanceTimer>,
    result: &Result<T, RuntimeError>,
    output_bytes: Option<u64>,
    resources: Option<PerformanceResourceUsage>,
) {
    let Some(timer) = timer else { return };
    let (outcome, error_code) = match result {
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
    timer.finish(outcome, error_code, output_bytes, resources);
}
