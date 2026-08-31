use std::{collections::BTreeMap, fmt};

use runku_core::{
    ApplicationClientId, CredentialId, DevRevisionId, EnvironmentScope, FunctionId, FunctionName,
    InvocationId, OperationalEventId, ReleaseId, RequestId,
};
use runku_releases::FunctionType;
use runku_value::{CanonicalValue, TimestampMicros, encode_stored_value};
use thiserror::Error;

/// Maximum UTF-8 bytes in one Function log message.
pub const FUNCTION_MESSAGE_MAX_BYTES: usize = 4 * 1024;
/// Maximum canonical encoded bytes in one Function log fields object.
pub const FUNCTION_FIELDS_MAX_BYTES: usize = 16 * 1024;
/// Maximum Function records requested by one invocation.
pub const FUNCTION_LOGS_MAX_RECORDS: u64 = 100;
/// Maximum aggregate message/fields bytes requested by one invocation.
pub const FUNCTION_LOGS_MAX_BYTES: u64 = 64 * 1024;
const MAX_OUTCOME_CODE_BYTES: usize = 96;
const REDACTED: &str = "[REDACTED]";

/// Logical access/retention stream for one operational record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LogStream {
    /// Platform-generated Function invocation lifecycle.
    Platform,
    /// Application-controlled records requested by Function code.
    Function,
}

impl LogStream {
    /// Stable lowercase wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Platform => "platform",
            Self::Function => "function",
        }
    }
}

/// Severity selected by platform policy or Function code.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LogLevel {
    /// Diagnostic detail normally filtered outside development.
    Debug,
    /// Expected lifecycle or application information.
    Info,
    /// Recoverable or degraded behavior requiring attention.
    Warn,
    /// Failed operation or application-declared error.
    Error,
}

impl LogLevel {
    /// Stable lowercase wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Exact event shape within an operational stream.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LogEventKind {
    /// Runtime admitted and started a validated invocation.
    InvocationStarted,
    /// Runtime reached one terminal invocation result.
    InvocationCompleted,
    /// Function code requested an explicit structured record.
    FunctionMessage,
}

impl LogEventKind {
    /// Stable snake-case wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvocationStarted => "invocation_started",
            Self::InvocationCompleted => "invocation_completed",
            Self::FunctionMessage => "function_message",
        }
    }
}

/// Non-identifying functional principal class safe for operational correlation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LogPrincipalKind {
    /// No functional principal was attached.
    None,
    /// Stable guest principal.
    Guest,
    /// Identified end user; its subject is not logged.
    User,
    /// Machine/service principal; its subject is not logged.
    Service,
    /// Trusted system execution.
    System,
}

impl LogPrincipalKind {
    /// Stable lowercase wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Guest => "guest",
            Self::User => "user",
            Self::Service => "service",
            Self::System => "system",
        }
    }
}

/// Validated bounded Function-authored message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogMessage(String);

impl LogMessage {
    /// Validates a non-empty bounded single record message.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, NUL-containing, or control-character messages.
    pub fn new(value: String) -> Result<Self, OperationalEventError> {
        if value.is_empty()
            || value.len() > FUNCTION_MESSAGE_MAX_BYTES
            || value.bytes().any(|byte| byte == 0)
            || value
                .chars()
                .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
        {
            return Err(OperationalEventError::InvalidMessage);
        }
        Ok(Self(value))
    }

    /// Returns the validated UTF-8 message.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Sanitized stable terminal code, never a JavaScript exception message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeCode(String);

impl OutcomeCode {
    /// Validates an uppercase machine code.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, or noncanonical codes.
    pub fn new(value: String) -> Result<Self, OperationalEventError> {
        if value.is_empty()
            || value.len() > MAX_OUTCOME_CODE_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(OperationalEventError::InvalidOutcome);
        }
        Ok(Self(value))
    }

    /// Returns the stable code.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One complete, validated Product Base operational event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalEventV1 {
    /// Globally unique event identity, independent from repository sequence.
    pub id: OperationalEventId,
    /// Host-generated occurrence time.
    pub occurred_at: TimestampMicros,
    /// Explicit tenant/environment boundary.
    pub scope: EnvironmentScope,
    /// Transport/scheduled request correlation.
    pub request_id: RequestId,
    /// Exact Function execution.
    pub invocation_id: InvocationId,
    /// Immediate caller invocation for nested Functions.
    pub parent_invocation_id: Option<InvocationId>,
    /// Immutable Release selected for this execution.
    pub release_id: ReleaseId,
    /// Immutable development revision when a Workspace was selected.
    pub dev_revision_id: Option<DevRevisionId>,
    /// Stable Function identity.
    pub function_id: FunctionId,
    /// Stable Function address.
    pub function_name: FunctionName,
    /// Function execution semantics.
    pub function_type: FunctionType,
    /// Logical application identity; no credential material.
    pub client_id: Option<ApplicationClientId>,
    /// Exact key identity; no credential material.
    pub credential_id: Option<CredentialId>,
    /// Functional principal class without its ID/subject.
    pub principal_kind: LogPrincipalKind,
    /// Access/retention stream.
    pub stream: LogStream,
    /// Record severity.
    pub level: LogLevel,
    /// Exact event kind.
    pub kind: LogEventKind,
    /// Function-controlled message only for `FunctionMessage`.
    pub message: Option<LogMessage>,
    /// Sanitized canonical object only for `FunctionMessage`.
    pub fields: Option<CanonicalValue>,
    /// Terminal host-measured duration only for completion.
    pub duration_micros: Option<u64>,
    /// Terminal sanitized result/error code only for completion.
    pub outcome_code: Option<OutcomeCode>,
}

impl OperationalEventV1 {
    /// Revalidates all cross-field and bounded-value invariants.
    ///
    /// # Errors
    ///
    /// Rejects inconsistent stream/kind payloads, negative timestamps, oversized fields, or
    /// contradictory application attribution.
    pub fn validate(&self) -> Result<(), OperationalEventError> {
        if self.occurred_at.get() < 0
            || self.credential_id.is_some() && self.client_id.is_none()
            || self.parent_invocation_id == Some(self.invocation_id)
        {
            return Err(OperationalEventError::InvalidCorrelation);
        }
        match self.kind {
            LogEventKind::InvocationStarted => {
                if self.stream != LogStream::Platform
                    || self.level != LogLevel::Info
                    || self.message.is_some()
                    || self.fields.is_some()
                    || self.duration_micros.is_some()
                    || self.outcome_code.is_some()
                {
                    return Err(OperationalEventError::InvalidShape);
                }
            }
            LogEventKind::InvocationCompleted => {
                if self.stream != LogStream::Platform
                    || self.message.is_some()
                    || self.fields.is_some()
                    || self.duration_micros.is_none()
                    || self.outcome_code.is_none()
                    || !matches!(self.level, LogLevel::Info | LogLevel::Error)
                {
                    return Err(OperationalEventError::InvalidShape);
                }
            }
            LogEventKind::FunctionMessage => {
                if self.stream != LogStream::Function
                    || self.message.is_none()
                    || self.duration_micros.is_some()
                    || self.outcome_code.is_some()
                {
                    return Err(OperationalEventError::InvalidShape);
                }
                if let Some(fields) = &self.fields {
                    validate_fields(fields)?;
                }
            }
        }
        Ok(())
    }

    /// Exact bytes charged to an invocation Function-log budget.
    ///
    /// # Errors
    ///
    /// Returns the same field validation failure as [`Self::validate`].
    pub fn function_payload_bytes(&self) -> Result<u64, OperationalEventError> {
        if self.kind != LogEventKind::FunctionMessage {
            return Ok(0);
        }
        let message = self
            .message
            .as_ref()
            .ok_or(OperationalEventError::InvalidShape)?;
        let field_bytes = self
            .fields
            .as_ref()
            .map(encode_stored_value)
            .transpose()
            .map_err(|_| OperationalEventError::InvalidFields)?
            .map_or(0, |bytes| bytes.len());
        u64::try_from(message.as_str().len().saturating_add(field_bytes))
            .map_err(|_| OperationalEventError::LimitExceeded)
    }
}

/// Contract validation failure before an event reaches a sink.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OperationalEventError {
    /// Message is empty, malformed, or too large.
    #[error("operational log message is invalid")]
    InvalidMessage,
    /// Structured fields are not a bounded canonical object.
    #[error("operational log fields are invalid")]
    InvalidFields,
    /// Event kind, stream, level, or optional payloads contradict each other.
    #[error("operational event shape is invalid")]
    InvalidShape,
    /// Scope/identity/timestamp correlation is contradictory.
    #[error("operational event correlation is invalid")]
    InvalidCorrelation,
    /// Terminal outcome code is not canonical.
    #[error("operational event outcome is invalid")]
    InvalidOutcome,
    /// An exact v1 budget was exceeded.
    #[error("operational event exceeds a limit")]
    LimitExceeded,
}

impl OperationalEventError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidMessage => "LOG_MESSAGE_INVALID",
            Self::InvalidFields => "LOG_FIELDS_INVALID",
            Self::InvalidShape => "LOG_EVENT_SHAPE_INVALID",
            Self::InvalidCorrelation => "LOG_EVENT_CORRELATION_INVALID",
            Self::InvalidOutcome => "LOG_OUTCOME_INVALID",
            Self::LimitExceeded => "LOG_LIMIT_EXCEEDED",
        }
    }
}

/// Validates and recursively redacts known secret-bearing keys in Function-controlled fields.
///
/// Redaction is defense in depth; arbitrary application values remain application data and are
/// never promoted into platform/audit streams.
///
/// # Errors
///
/// Rejects non-object, oversized, or otherwise noncanonical fields before recursion.
pub fn sanitize_function_fields(
    fields: CanonicalValue,
) -> Result<CanonicalValue, OperationalEventError> {
    validate_fields(&fields)?;
    Ok(redact(fields))
}

fn validate_fields(fields: &CanonicalValue) -> Result<(), OperationalEventError> {
    if !matches!(fields, CanonicalValue::Object(_)) {
        return Err(OperationalEventError::InvalidFields);
    }
    let encoded = encode_stored_value(fields).map_err(|_| OperationalEventError::InvalidFields)?;
    if encoded.len() > FUNCTION_FIELDS_MAX_BYTES {
        return Err(OperationalEventError::LimitExceeded);
    }
    Ok(())
}

fn redact(value: CanonicalValue) -> CanonicalValue {
    match value {
        CanonicalValue::Array(values) => {
            CanonicalValue::Array(values.into_iter().map(redact).collect())
        }
        CanonicalValue::Object(values) => CanonicalValue::Object(
            values
                .into_iter()
                .map(|(key, value)| {
                    let value = if sensitive_key(&key) {
                        CanonicalValue::String(REDACTED.to_owned())
                    } else {
                        redact(value)
                    };
                    (key, value)
                })
                .collect::<BTreeMap<_, _>>(),
        ),
        scalar => scalar,
    }
}

fn sensitive_key(key: &str) -> bool {
    let normalized = key
        .bytes()
        .filter(u8::is_ascii_alphanumeric)
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<_>>();
    matches!(
        normalized.as_slice(),
        b"authorization"
            | b"cookie"
            | b"setcookie"
            | b"password"
            | b"secret"
            | b"apikey"
            | b"accesskey"
            | b"token"
            | b"accesstoken"
            | b"refreshtoken"
            | b"clientsecret"
            | b"privatekey"
    )
}

impl fmt::Display for LogMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
