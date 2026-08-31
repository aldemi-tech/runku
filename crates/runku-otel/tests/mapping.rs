//! OTLP Protobuf mapping conformance for every Operational Event dimension/value kind.

use std::{collections::BTreeMap, error::Error, str::FromStr};

use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value, logs::v1::SeverityNumber,
};
use prost::Message as _;
use runku_core::{
    ApplicationClientId, CredentialId, DevRevisionId, EnvironmentId, EnvironmentScope, FunctionId,
    FunctionName, InvocationId, OperationalEventId, ProjectId, ReleaseId, RequestId,
};
use runku_observability::{
    LogCursor, LogEventKind, LogLevel, LogMessage, LogPrincipalKind, LogStream, OperationalEventV1,
    SequencedOperationalEvent,
};
use runku_otel::{OTLP_INSTRUMENTATION_SCOPE, OtlpTransportError, encode_otlp_logs};
use runku_releases::FunctionType;
use runku_value::{CanonicalValue, FiniteF64, TimestampMicros, TypedId};
use ulid::Ulid;

type TestResult = Result<(), Box<dyn Error>>;

fn event(sequence: u128) -> Result<SequencedOperationalEvent, Box<dyn Error>> {
    Ok(SequencedOperationalEvent {
        cursor: LogCursor::new(u64::try_from(sequence)?),
        event: OperationalEventV1 {
            id: OperationalEventId::from_ulid(Ulid::from(sequence)),
            occurred_at: TimestampMicros::new(1_800_000_000_000_000),
            scope: EnvironmentScope::new(
                ProjectId::from_ulid(Ulid::from(100)),
                EnvironmentId::from_ulid(Ulid::from(101)),
            ),
            request_id: RequestId::from_ulid(Ulid::from(102)),
            invocation_id: InvocationId::from_ulid(Ulid::from(103)),
            parent_invocation_id: Some(InvocationId::from_ulid(Ulid::from(104))),
            release_id: ReleaseId::from_ulid(Ulid::from(105)),
            dev_revision_id: Some(DevRevisionId::from_ulid(Ulid::from(106))),
            function_id: FunctionId::from_ulid(Ulid::from(107)),
            function_name: FunctionName::from_str("orders.export")?,
            function_type: FunctionType::Action,
            client_id: Some(ApplicationClientId::from_ulid(Ulid::from(108))),
            credential_id: Some(CredentialId::from_ulid(Ulid::from(109))),
            principal_kind: LogPrincipalKind::Service,
            stream: LogStream::Function,
            level: LogLevel::Warn,
            kind: LogEventKind::FunctionMessage,
            message: Some(LogMessage::new("export me".to_owned())?),
            fields: Some(CanonicalValue::Object(BTreeMap::from([
                (
                    "array".to_owned(),
                    CanonicalValue::Array(vec![CanonicalValue::Null]),
                ),
                ("boolean".to_owned(), CanonicalValue::Boolean(true)),
                ("bytes".to_owned(), CanonicalValue::Bytes(vec![0, 1, 255])),
                (
                    "float".to_owned(),
                    CanonicalValue::Float64(FiniteF64::new(1.5)?),
                ),
                ("int".to_owned(), CanonicalValue::Int64(i64::MIN)),
                (
                    "string".to_owned(),
                    CanonicalValue::String("value".to_owned()),
                ),
                (
                    "timestamp".to_owned(),
                    CanonicalValue::Timestamp(TimestampMicros::new(73)),
                ),
                (
                    "typed".to_owned(),
                    CanonicalValue::TypedId("doc_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse::<TypedId>()?),
                ),
            ]))),
            duration_micros: None,
            outcome_code: None,
        },
    })
}

#[test]
fn binary_request_is_stable_complete_and_does_not_fabricate_trace_ids() -> TestResult {
    let record = event(1)?;
    let encoded = encode_otlp_logs(std::slice::from_ref(&record), 64 * 1024)?;
    let decoded = ExportLogsServiceRequest::decode(encoded.as_slice())?;
    assert_eq!(decoded.resource_logs.len(), 1);
    let resource = &decoded.resource_logs[0];
    let resource_attributes = &resource
        .resource
        .as_ref()
        .ok_or("resource missing")?
        .attributes;
    assert!(resource_attributes.iter().any(|attribute| {
        attribute.key == "service.name"
            && matches!(
                attribute.value.as_ref().and_then(|value| value.value.as_ref()),
                Some(any_value::Value::StringValue(value)) if value == "runku-functions"
            )
    }));
    let scope = &resource.scope_logs[0];
    assert_eq!(
        scope.scope.as_ref().ok_or("scope missing")?.name,
        OTLP_INSTRUMENTATION_SCOPE
    );
    let exported = &scope.log_records[0];
    assert_eq!(exported.time_unix_nano, 1_800_000_000_000_000_000);
    assert_eq!(exported.severity_number, SeverityNumber::Warn as i32);
    assert_eq!(exported.severity_text, "warn");
    assert_eq!(exported.event_name, "runku.function.message");
    assert!(exported.trace_id.is_empty());
    assert!(exported.span_id.is_empty());
    assert!(
        exported
            .attributes
            .iter()
            .any(|value| value.key == "runku.log.fields")
    );
    assert!(exported.attributes.iter().any(|value| {
        value.key == "runku.application_client.id"
            && value
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref())
                .is_some()
    }));
    let fields = exported
        .attributes
        .iter()
        .find(|attribute| attribute.key == "runku.log.fields")
        .and_then(|attribute| attribute.value.as_ref())
        .and_then(|value| value.value.as_ref());
    let Some(any_value::Value::KvlistValue(fields)) = fields else {
        return Err("fields did not map to a key/value list".into());
    };
    let field = |name: &str| {
        fields
            .values
            .iter()
            .find(|value| value.key == name)
            .and_then(|value| value.value.as_ref())
            .and_then(|value| value.value.as_ref())
    };
    assert!(matches!(
        field("boolean"),
        Some(any_value::Value::BoolValue(true))
    ));
    assert!(
        matches!(field("bytes"), Some(any_value::Value::BytesValue(value)) if value == &[0, 1, 255])
    );
    assert!(
        matches!(field("float"), Some(any_value::Value::DoubleValue(value)) if value.to_bits() == 1.5_f64.to_bits())
    );
    assert!(matches!(
        field("int"),
        Some(any_value::Value::IntValue(i64::MIN))
    ));
    assert!(
        matches!(field("string"), Some(any_value::Value::StringValue(value)) if value == "value")
    );
    assert!(
        matches!(field("array"), Some(any_value::Value::ArrayValue(value)) if value.values.len() == 1)
    );
    assert!(
        matches!(field("timestamp"), Some(any_value::Value::KvlistValue(value)) if value.values.len() == 2)
    );
    assert!(
        matches!(field("typed"), Some(any_value::Value::KvlistValue(value)) if value.values.len() == 2)
    );
    Ok(())
}

#[test]
fn empty_mixed_unordered_negative_and_oversized_pages_fail_closed() -> TestResult {
    assert_eq!(
        encode_otlp_logs(&[], 1),
        Err(OtlpTransportError::InvalidInput)
    );
    let first = event(2)?;
    let mut unordered = event(1)?;
    unordered.event.id = OperationalEventId::from_ulid(Ulid::from(999));
    assert_eq!(
        encode_otlp_logs(&[first.clone(), unordered], 64 * 1024),
        Err(OtlpTransportError::InvalidInput)
    );
    let mut mixed = event(3)?;
    mixed.event.scope = EnvironmentScope::new(
        ProjectId::from_ulid(Ulid::from(200)),
        EnvironmentId::from_ulid(Ulid::from(201)),
    );
    assert_eq!(
        encode_otlp_logs(&[first.clone(), mixed], 64 * 1024),
        Err(OtlpTransportError::InvalidInput)
    );
    let mut negative = first.clone();
    negative.event.occurred_at = TimestampMicros::new(-1);
    assert_eq!(
        encode_otlp_logs(&[negative], 64 * 1024),
        Err(OtlpTransportError::InvalidInput)
    );
    assert_eq!(
        encode_otlp_logs(&[first], 1),
        Err(OtlpTransportError::LimitExceeded)
    );
    Ok(())
}
