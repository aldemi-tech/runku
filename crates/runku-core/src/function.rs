//! Stable logical function names.

use std::{fmt, str::FromStr};

use thiserror::Error;

/// Validated logical function name shared by releases, routing, and scheduling.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FunctionName(String);

impl FunctionName {
    /// Maximum UTF-8 byte length in v1.
    pub const MAX_BYTES: usize = 128;

    /// Returns the canonical name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FunctionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for FunctionName {
    type Err = ParseFunctionNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(ParseFunctionNameError::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(ParseFunctionNameError::TooLong);
        }
        let mut bytes = value.bytes();
        let first = bytes.next().ok_or(ParseFunctionNameError::Empty)?;
        if !first.is_ascii_alphabetic()
            || !bytes.all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/')
            })
        {
            return Err(ParseFunctionNameError::InvalidCharacter);
        }
        Ok(Self(value.to_owned()))
    }
}

/// Error returned for a non-canonical function name.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParseFunctionNameError {
    /// Name is empty.
    #[error("function name must not be empty")]
    Empty,
    /// Name exceeds 128 bytes.
    #[error("function name exceeds the v1 limit")]
    TooLong,
    /// Name contains unsupported characters or does not start with a letter.
    #[error("function name is not canonical")]
    InvalidCharacter,
}

impl ParseFunctionNameError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Empty => "FUNCTION_NAME_EMPTY",
            Self::TooLong => "FUNCTION_NAME_TOO_LONG",
            Self::InvalidCharacter => "FUNCTION_NAME_INVALID",
        }
    }
}
