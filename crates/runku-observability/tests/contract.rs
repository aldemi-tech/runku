//! Operational Event v1 validation and adversarial redaction properties.

use std::{collections::BTreeMap, error::Error, str::FromStr};

use proptest::prelude::*;
use runku_observability::{
    FUNCTION_FIELDS_MAX_BYTES, FUNCTION_MESSAGE_MAX_BYTES, LogCursor, LogMessage,
    OperationalEventError, OutcomeCode, sanitize_function_fields,
};
use runku_value::CanonicalValue;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn message_outcome_cursor_and_fields_enforce_exact_boundaries() -> TestResult {
    assert!(LogMessage::new("a".repeat(FUNCTION_MESSAGE_MAX_BYTES)).is_ok());
    assert_eq!(
        LogMessage::new("a".repeat(FUNCTION_MESSAGE_MAX_BYTES + 1)),
        Err(OperationalEventError::InvalidMessage)
    );
    assert_eq!(
        LogMessage::new("line\u{0000}".to_owned()),
        Err(OperationalEventError::InvalidMessage)
    );
    assert!(LogMessage::new("line one\nline two\tvalue".to_owned()).is_ok());

    assert!(OutcomeCode::new("RUNTIME_TIMEOUT_2".to_owned()).is_ok());
    assert_eq!(
        OutcomeCode::new("runtime-timeout".to_owned()),
        Err(OperationalEventError::InvalidOutcome)
    );
    assert_eq!(LogCursor::from_str("logc_0")?, LogCursor::START);
    assert_eq!(
        LogCursor::from_str("logc_18446744073709551615")?.get(),
        u64::MAX
    );
    for invalid in ["0", "logc_", "logc_00", "logc_01", "logc_-1", "logc_+1"] {
        assert!(LogCursor::from_str(invalid).is_err());
    }

    assert_eq!(
        sanitize_function_fields(CanonicalValue::String("not an object".to_owned())),
        Err(OperationalEventError::InvalidFields)
    );
    let oversized = CanonicalValue::Object(BTreeMap::from([(
        "value".to_owned(),
        CanonicalValue::String("x".repeat(FUNCTION_FIELDS_MAX_BYTES)),
    )]));
    assert_eq!(
        sanitize_function_fields(oversized),
        Err(OperationalEventError::LimitExceeded)
    );
    Ok(())
}

#[test]
fn recursive_redaction_covers_objects_arrays_and_normalized_sensitive_keys() -> TestResult {
    let fields = CanonicalValue::Object(BTreeMap::from([
        (
            "Authorization".to_owned(),
            CanonicalValue::String("bearer secret".to_owned()),
        ),
        (
            "nested".to_owned(),
            CanonicalValue::Array(vec![CanonicalValue::Object(BTreeMap::from([
                (
                    "client-secret".to_owned(),
                    CanonicalValue::String("secret".to_owned()),
                ),
                ("safe".to_owned(), CanonicalValue::String("kept".to_owned())),
            ]))]),
        ),
    ]));
    let redacted = sanitize_function_fields(fields)?;
    let CanonicalValue::Object(root) = redacted else {
        return Err("root object was not preserved".into());
    };
    assert_eq!(
        root["Authorization"],
        CanonicalValue::String("[REDACTED]".to_owned())
    );
    let CanonicalValue::Array(nested) = &root["nested"] else {
        return Err("nested array was not preserved".into());
    };
    let CanonicalValue::Object(nested) = &nested[0] else {
        return Err("nested object was not preserved".into());
    };
    assert_eq!(
        nested["client-secret"],
        CanonicalValue::String("[REDACTED]".to_owned())
    );
    assert_eq!(nested["safe"], CanonicalValue::String("kept".to_owned()));
    Ok(())
}

proptest! {
    #[test]
    fn every_ascii_case_and_separator_spelling_of_access_token_is_redacted(
        uppercase in prop::collection::vec(any::<bool>(), 11),
        separators in prop::collection::vec(0_u8..=4, 10),
    ) {
        let letters = "accesstoken".bytes().collect::<Vec<_>>();
        let mut key = String::new();
        for (index, letter) in letters.into_iter().enumerate() {
            let letter = if uppercase[index] {
                letter.to_ascii_uppercase()
            } else {
                letter
            };
            key.push(char::from(letter));
            if let Some(separator) = separators.get(index) {
                match separator {
                    1 => key.push('_'),
                    2 => key.push('-'),
                    3 => key.push('.'),
                    4 => key.push(' '),
                    _ => {}
                }
            }
        }
        let sanitized = sanitize_function_fields(CanonicalValue::Object(BTreeMap::from([(
            key.clone(),
            CanonicalValue::String("must-not-survive".to_owned()),
        )])))?;
        let CanonicalValue::Object(values) = sanitized else {
            return Err(TestCaseError::fail("sanitized fields are not an object"));
        };
        prop_assert_eq!(
            values.get(&key),
            Some(&CanonicalValue::String("[REDACTED]".to_owned()))
        );
    }
}
