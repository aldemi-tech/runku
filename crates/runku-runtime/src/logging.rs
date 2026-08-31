//! Runtime-owned construction, budgets, and best-effort emission of operational events.

use std::{
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use runku_core::{OperationalEventId, PinnedCode};
use runku_identity::PrincipalKind;
use runku_observability::{
    FUNCTION_LOGS_MAX_BYTES, FUNCTION_LOGS_MAX_RECORDS, LogEventKind, LogLevel, LogMessage,
    LogPrincipalKind, LogStream, OperationalEventError, OperationalEventV1, OperationalLogSink,
    OutcomeCode, sanitize_function_fields,
};
use runku_releases::FunctionManifest;
use runku_value::{CanonicalValue, TimestampMicros};

use crate::{InvocationRequest, RuntimeError, invocation::RuntimeTelemetry};

#[derive(Debug, Default)]
struct FunctionLogBudget {
    records: u64,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FunctionLogError {
    Invalid,
    LimitExceeded,
    Unavailable,
}

impl FunctionLogError {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Invalid => "LOG_INVALID",
            Self::LimitExceeded => "LOG_LIMIT_EXCEEDED",
            Self::Unavailable => "LOG_UNAVAILABLE",
        }
    }
}

#[derive(Debug)]
pub(crate) struct InvocationLogContext {
    sink: Arc<dyn OperationalLogSink>,
    template: OperationalEventV1,
    budget: Mutex<FunctionLogBudget>,
    telemetry: Option<Arc<RuntimeTelemetry>>,
}

impl InvocationLogContext {
    pub(crate) fn new(
        request: &InvocationRequest,
        function: &FunctionManifest,
    ) -> Option<Arc<Self>> {
        let sink = request.operational_logs.clone()?;
        let application = request
            .identity
            .as_ref()
            .and_then(|identity| identity.application.as_ref());
        let principal_kind = request
            .identity
            .as_ref()
            .and_then(|identity| identity.principal.kind())
            .map_or(LogPrincipalKind::None, map_principal_kind);
        Some(Arc::new(Self {
            sink,
            template: OperationalEventV1 {
                id: OperationalEventId::generate(),
                occurred_at: now(),
                scope: request.scope,
                request_id: request.request_id,
                invocation_id: request.invocation_id,
                parent_invocation_id: request.parent_invocation_id,
                release_id: request.release_id,
                dev_revision_id: match request.pinned_code {
                    PinnedCode::Release(_) => None,
                    PinnedCode::DevRevision(value) => Some(value),
                },
                function_id: function.id,
                function_name: function.name.clone(),
                function_type: function.function_type,
                client_id: application.map(|value| value.client_id),
                credential_id: application.map(|value| value.credential_id),
                principal_kind,
                stream: LogStream::Platform,
                level: LogLevel::Info,
                kind: LogEventKind::InvocationStarted,
                message: None,
                fields: None,
                duration_micros: None,
                outcome_code: None,
            },
            budget: Mutex::new(FunctionLogBudget::default()),
            telemetry: request.telemetry.clone(),
        }))
    }

    pub(crate) fn started(&self) {
        self.emit_platform(self.template.clone());
    }

    pub(crate) fn completed(
        &self,
        result: &Result<CanonicalValue, RuntimeError>,
        started: Instant,
    ) {
        let mut event = self.template.clone();
        event.id = OperationalEventId::generate();
        event.occurred_at = now();
        event.kind = LogEventKind::InvocationCompleted;
        event.level = if result.is_ok() {
            LogLevel::Info
        } else {
            LogLevel::Error
        };
        event.duration_micros =
            Some(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        let code = result.as_ref().map_or_else(|error| error.code(), |_| "OK");
        event.outcome_code = OutcomeCode::new(code.to_owned()).ok();
        self.emit_platform(event);
    }

    pub(crate) fn function(
        &self,
        level: LogLevel,
        message: String,
        fields: Option<CanonicalValue>,
    ) -> Result<(), FunctionLogError> {
        let mut event = self.template.clone();
        event.id = OperationalEventId::generate();
        event.occurred_at = now();
        event.stream = LogStream::Function;
        event.level = level;
        event.kind = LogEventKind::FunctionMessage;
        event.message = Some(LogMessage::new(message).map_err(|_| FunctionLogError::Invalid)?);
        event.fields =
            fields
                .map(sanitize_function_fields)
                .transpose()
                .map_err(|error| match error {
                    OperationalEventError::LimitExceeded => FunctionLogError::LimitExceeded,
                    OperationalEventError::InvalidMessage
                    | OperationalEventError::InvalidFields
                    | OperationalEventError::InvalidShape
                    | OperationalEventError::InvalidCorrelation
                    | OperationalEventError::InvalidOutcome => FunctionLogError::Invalid,
                })?;
        event.validate().map_err(|_| FunctionLogError::Invalid)?;
        let bytes = event
            .function_payload_bytes()
            .map_err(|_| FunctionLogError::Invalid)?;
        {
            let mut budget = self
                .budget
                .lock()
                .map_err(|_| FunctionLogError::Unavailable)?;
            let next_records = budget
                .records
                .checked_add(1)
                .ok_or(FunctionLogError::LimitExceeded)?;
            let next_bytes = budget
                .bytes
                .checked_add(bytes)
                .ok_or(FunctionLogError::LimitExceeded)?;
            if next_records > FUNCTION_LOGS_MAX_RECORDS || next_bytes > FUNCTION_LOGS_MAX_BYTES {
                if let Some(telemetry) = &self.telemetry {
                    telemetry.function_log_limited();
                }
                return Err(FunctionLogError::LimitExceeded);
            }
            budget.records = next_records;
            budget.bytes = next_bytes;
        }
        match self.sink.try_emit(event) {
            Ok(()) => {
                if let Some(telemetry) = &self.telemetry {
                    telemetry.function_log_emitted();
                }
            }
            Err(_) => {
                if let Some(telemetry) = &self.telemetry {
                    telemetry.function_log_dropped();
                }
            }
        }
        Ok(())
    }

    fn emit_platform(&self, event: OperationalEventV1) {
        if (event.validate().is_err() || self.sink.try_emit(event).is_err())
            && let Some(telemetry) = &self.telemetry
        {
            telemetry.platform_log_dropped();
        }
    }
}

const fn map_principal_kind(value: PrincipalKind) -> LogPrincipalKind {
    match value {
        PrincipalKind::Guest => LogPrincipalKind::Guest,
        PrincipalKind::User => LogPrincipalKind::User,
        PrincipalKind::Service => LogPrincipalKind::Service,
        PrincipalKind::System => LogPrincipalKind::System,
    }
}

fn now() -> TimestampMicros {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_micros());
    TimestampMicros::new(i64::try_from(micros).unwrap_or(i64::MAX))
}
