use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    resource::v1::Resource,
};
use prost::Message as _;
use runku_observability::{LogEventKind, LogLevel, SequencedOperationalEvent};
use runku_value::CanonicalValue;

use crate::OtlpTransportError;

/// Stable OpenTelemetry instrumentation scope for Operational Event v1.
pub const OTLP_INSTRUMENTATION_SCOPE: &str = "io.runku.operational_logs";

/// Encodes one non-empty, single-scope ordered page as an OTLP `ExportLogs` request.
///
/// # Errors
///
/// Rejects an empty/mixed-scope page, invalid timestamps, non-ascending cursors, or an encoded
/// payload larger than `maximum_bytes`.
pub fn encode_otlp_logs(
    records: &[SequencedOperationalEvent],
    maximum_bytes: usize,
) -> Result<Vec<u8>, OtlpTransportError> {
    if records.is_empty() || maximum_bytes == 0 {
        return Err(OtlpTransportError::InvalidInput);
    }
    let scope = records[0].event.scope;
    let mut previous = None;
    let mut log_records = Vec::with_capacity(records.len());
    for record in records {
        if record.event.scope != scope
            || previous.is_some_and(|cursor| cursor >= record.cursor)
            || record.event.validate().is_err()
        {
            return Err(OtlpTransportError::InvalidInput);
        }
        previous = Some(record.cursor);
        log_records.push(map_record(record)?);
    }
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_attribute("service.name", "runku-functions"),
                    string_attribute("service.version", env!("CARGO_PKG_VERSION")),
                    string_attribute("runku.project.id", &scope.project_id().to_string()),
                    string_attribute("runku.environment.id", &scope.environment_id().to_string()),
                ],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: OTLP_INSTRUMENTATION_SCOPE.to_owned(),
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let encoded = request.encode_to_vec();
    if encoded.len() > maximum_bytes {
        return Err(OtlpTransportError::LimitExceeded);
    }
    Ok(encoded)
}

fn map_record(record: &SequencedOperationalEvent) -> Result<LogRecord, OtlpTransportError> {
    let event = &record.event;
    let micros =
        u64::try_from(event.occurred_at.get()).map_err(|_| OtlpTransportError::InvalidInput)?;
    let nanos = micros
        .checked_mul(1_000)
        .ok_or(OtlpTransportError::LimitExceeded)?;
    let mut attributes = vec![
        string_attribute("runku.log.event_id", &event.id.to_string()),
        string_attribute("runku.log.cursor", &record.cursor.to_string()),
        string_attribute("runku.log.stream", event.stream.as_str()),
        string_attribute("runku.log.event_kind", event.kind.as_str()),
        string_attribute("runku.request.id", &event.request_id.to_string()),
        string_attribute("runku.invocation.id", &event.invocation_id.to_string()),
        string_attribute("runku.release.id", &event.release_id.to_string()),
        string_attribute("runku.function.id", &event.function_id.to_string()),
        string_attribute("runku.function.name", event.function_name.as_str()),
        string_attribute(
            "runku.function.type",
            match event.function_type {
                runku_releases::FunctionType::Query => "query",
                runku_releases::FunctionType::Mutation => "mutation",
                runku_releases::FunctionType::Action => "action",
            },
        ),
        string_attribute("runku.principal.kind", event.principal_kind.as_str()),
    ];
    optional_id(
        &mut attributes,
        "runku.parent_invocation.id",
        event.parent_invocation_id,
    );
    optional_id(
        &mut attributes,
        "runku.dev_revision.id",
        event.dev_revision_id,
    );
    optional_id(
        &mut attributes,
        "runku.application_client.id",
        event.client_id,
    );
    optional_id(&mut attributes, "runku.credential.id", event.credential_id);
    if let Some(duration) = event.duration_micros {
        attributes.push(KeyValue {
            key: "runku.invocation.duration_micros".to_owned(),
            value: Some(any(any_value::Value::IntValue(
                i64::try_from(duration).map_err(|_| OtlpTransportError::LimitExceeded)?,
            ))),
            key_strindex: 0,
        });
    }
    if let Some(outcome) = &event.outcome_code {
        attributes.push(string_attribute(
            "runku.invocation.outcome_code",
            outcome.as_str(),
        ));
    }
    if let Some(fields) = &event.fields {
        attributes.push(KeyValue {
            key: "runku.log.fields".to_owned(),
            value: Some(canonical_value(fields)),
            key_strindex: 0,
        });
    }
    let body = event.message.as_ref().map_or_else(
        || {
            any(any_value::Value::StringValue(
                event.kind.as_str().to_owned(),
            ))
        },
        |message| any(any_value::Value::StringValue(message.as_str().to_owned())),
    );
    Ok(LogRecord {
        time_unix_nano: nanos,
        observed_time_unix_nano: nanos,
        severity_number: severity(event.level) as i32,
        severity_text: event.level.as_str().to_owned(),
        body: Some(body),
        attributes,
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![],
        span_id: vec![],
        event_name: match event.kind {
            LogEventKind::InvocationStarted => "runku.invocation.started",
            LogEventKind::InvocationCompleted => "runku.invocation.completed",
            LogEventKind::FunctionMessage => "runku.function.message",
        }
        .to_owned(),
    })
}

fn severity(level: LogLevel) -> SeverityNumber {
    match level {
        LogLevel::Debug => SeverityNumber::Debug,
        LogLevel::Info => SeverityNumber::Info,
        LogLevel::Warn => SeverityNumber::Warn,
        LogLevel::Error => SeverityNumber::Error,
    }
}

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(any(any_value::Value::StringValue(value.to_owned()))),
        key_strindex: 0,
    }
}

fn optional_id<T: std::fmt::Display>(attributes: &mut Vec<KeyValue>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        attributes.push(string_attribute(key, &value.to_string()));
    }
}

fn any(value: any_value::Value) -> AnyValue {
    AnyValue { value: Some(value) }
}

fn canonical_value(value: &CanonicalValue) -> AnyValue {
    any(match value {
        CanonicalValue::Null => any_value::Value::KvlistValue(KeyValueList {
            values: vec![string_attribute("runku.value.type", "null")],
        }),
        CanonicalValue::Boolean(value) => any_value::Value::BoolValue(*value),
        CanonicalValue::Int64(value) => any_value::Value::IntValue(*value),
        CanonicalValue::Float64(value) => any_value::Value::DoubleValue(value.get()),
        CanonicalValue::String(value) => any_value::Value::StringValue(value.clone()),
        CanonicalValue::Bytes(value) => any_value::Value::BytesValue(value.clone()),
        CanonicalValue::Timestamp(value) => any_value::Value::KvlistValue(KeyValueList {
            values: vec![
                string_attribute("runku.value.type", "timestamp_micros"),
                KeyValue {
                    key: "runku.value.micros".to_owned(),
                    value: Some(any(any_value::Value::IntValue(value.get()))),
                    key_strindex: 0,
                },
            ],
        }),
        CanonicalValue::TypedId(value) => any_value::Value::KvlistValue(KeyValueList {
            values: vec![
                string_attribute("runku.value.type", "typed_id"),
                string_attribute("runku.value.text", &value.to_string()),
            ],
        }),
        CanonicalValue::Array(values) => any_value::Value::ArrayValue(ArrayValue {
            values: values.iter().map(canonical_value).collect(),
        }),
        CanonicalValue::Object(values) => any_value::Value::KvlistValue(KeyValueList {
            values: values
                .iter()
                .map(|(key, value)| KeyValue {
                    key: key.clone(),
                    value: Some(canonical_value(value)),
                    key_strindex: 0,
                })
                .collect(),
        }),
    })
}
