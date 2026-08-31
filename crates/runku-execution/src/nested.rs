//! Shared validation and immutable child-envelope derivation for nested Function coordinators.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use runku_identity::IdentityError;
use runku_releases::{AuthPolicy, FunctionManifest, FunctionType};
use runku_runtime::{
    FunctionCallError, FunctionCallKind, FunctionCallRequest, InvocationRequest, RuntimeError,
};
use runku_value::TimestampMicros;

pub(crate) fn prepare_child(
    root: &InvocationRequest,
    call: FunctionCallRequest,
    deadline: Instant,
) -> Result<(InvocationRequest, FunctionManifest), FunctionCallError> {
    let expected_type = match call.kind {
        FunctionCallKind::Query => FunctionType::Query,
        FunctionCallKind::Mutation => FunctionType::Mutation,
        FunctionCallKind::Action => FunctionType::Action,
    };
    let target = root
        .manifest()
        .functions
        .iter()
        .find(|function| function.name == call.function)
        .cloned()
        .ok_or(FunctionCallError::NotFound)?;
    if target.function_type != expected_type {
        return Err(FunctionCallError::InvalidRequest);
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(FunctionCallError::Timeout);
    }
    let mut child = root
        .nested_child(target.id, call.arguments, remaining)
        .map_err(map_runtime_error)?;
    match root.identity() {
        Some(identity) => {
            let derived = identity
                .derive_for_nested(target.auth_policy, now_micros()?)
                .map_err(map_identity_error)?;
            child = child.with_identity(derived.into());
        }
        None if !matches!(target.auth_policy, AuthPolicy::None) => {
            return Err(FunctionCallError::Denied);
        }
        None => {}
    }
    Ok((child, target))
}

pub(crate) const fn map_runtime_error(error: RuntimeError) -> FunctionCallError {
    match error {
        RuntimeError::Busy => FunctionCallError::Busy,
        RuntimeError::Unavailable | RuntimeError::Internal => FunctionCallError::Unavailable,
        RuntimeError::DeadlineExceeded => FunctionCallError::Timeout,
        RuntimeError::Cancelled => FunctionCallError::Cancelled,
        RuntimeError::InvalidInvocation => FunctionCallError::LimitExceeded,
        RuntimeError::FunctionNotFound => FunctionCallError::NotFound,
        RuntimeError::InvalidArguments => FunctionCallError::InvalidRequest,
        RuntimeError::InvalidConfiguration
        | RuntimeError::UnsupportedRuntime
        | RuntimeError::InvalidArtifact
        | RuntimeError::HeapLimitExceeded
        | RuntimeError::JavaScript
        | RuntimeError::InvalidResult => FunctionCallError::Execution,
    }
}

fn map_identity_error(_error: IdentityError) -> FunctionCallError {
    FunctionCallError::Denied
}

fn now_micros() -> Result<TimestampMicros, FunctionCallError> {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| FunctionCallError::Unavailable)?
        .as_micros();
    let micros = i64::try_from(micros).map_err(|_| FunctionCallError::Unavailable)?;
    Ok(TimestampMicros::new(micros))
}
