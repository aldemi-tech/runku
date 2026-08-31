//! Stable sanitized protocol and public error categories.

use thiserror::Error;

/// Failure while decoding or encoding a protocol v1 envelope.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProtocolError {
    /// JSON/envelope/value input is malformed or non-canonical.
    #[error("public protocol request is invalid")]
    InvalidRequest,
    /// The requested protocol version is not supported by this endpoint.
    #[error("public protocol version is unsupported")]
    UnsupportedVersion,
    /// Request, value, or encoded response exceeds a hard v1 limit.
    #[error("public protocol limit exceeded")]
    LimitExceeded,
    /// A success/error response could not be represented under protocol invariants.
    #[error("public protocol response is invalid")]
    InvalidResponse,
}

impl ProtocolError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "PROTOCOL_REQUEST_INVALID",
            Self::UnsupportedVersion => "PROTOCOL_VERSION_UNSUPPORTED",
            Self::LimitExceeded => "PROTOCOL_LIMIT_EXCEEDED",
            Self::InvalidResponse => "PROTOCOL_RESPONSE_INVALID",
        }
    }

    /// Retry cannot repair the same invalid bytes or unsupported version.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }

    /// Safe public classification for an HTTP adapter.
    #[must_use]
    pub const fn public_error(self) -> PublicErrorV1 {
        let class = match self {
            Self::LimitExceeded => ErrorClassV1::LimitExceeded,
            Self::InvalidRequest | Self::UnsupportedVersion | Self::InvalidResponse => {
                ErrorClassV1::InvalidRequest
            }
        };
        PublicErrorV1 {
            class,
            code: self.code(),
            retryable: false,
        }
    }
}

/// Closed public error class controlling generic message and HTTP status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClassV1 {
    /// Malformed or semantically invalid caller input.
    InvalidRequest,
    /// Missing or invalid authentication evidence.
    Unauthenticated,
    /// Verified caller lacks authority or function visibility.
    Forbidden,
    /// Addressed public resource/function does not exist.
    NotFound,
    /// Idempotency, revision, or another caller-visible state conflict.
    Conflict,
    /// An explicitly bound Release is retired.
    Gone,
    /// Caller/request exceeds a hard payload or resource limit.
    LimitExceeded,
    /// Caller-specific request rate is temporarily exhausted.
    RateLimited,
    /// Node admission capacity is temporarily exhausted.
    Busy,
    /// A required Product Base dependency is temporarily unavailable.
    Unavailable,
    /// Invocation deadline elapsed.
    Timeout,
    /// Sanitized unexpected server failure.
    Internal,
}

impl ErrorClassV1 {
    /// Stable HTTP status for this class.
    #[must_use]
    pub const fn http_status(self) -> u16 {
        match self {
            Self::InvalidRequest => 400,
            Self::Unauthenticated => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::Conflict => 409,
            Self::Gone => 410,
            Self::LimitExceeded => 413,
            Self::RateLimited => 429,
            Self::Busy | Self::Unavailable => 503,
            Self::Timeout => 504,
            Self::Internal => 500,
        }
    }

    /// Fixed non-sensitive English message; remote/internal detail is never interpolated.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "The request is invalid.",
            Self::Unauthenticated => "Authentication is required or invalid.",
            Self::Forbidden => "The request is not permitted.",
            Self::NotFound => "The requested resource was not found.",
            Self::Conflict => "The request conflicts with current state.",
            Self::Gone => "The requested release is no longer available.",
            Self::LimitExceeded => "The request exceeds a protocol limit.",
            Self::RateLimited => "The request rate limit was exceeded.",
            Self::Busy => "The service is temporarily busy.",
            Self::Unavailable => "The service is temporarily unavailable.",
            Self::Timeout => "The request deadline elapsed.",
            Self::Internal => "The request failed unexpectedly.",
        }
    }
}

/// Validated public error descriptor supplied by a gateway error mapper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicErrorV1 {
    class: ErrorClassV1,
    code: &'static str,
    retryable: bool,
}

impl PublicErrorV1 {
    /// Creates a sanitized descriptor from a stable code and closed class.
    ///
    /// # Errors
    ///
    /// Rejects non-canonical codes before response encoding.
    pub fn new(
        class: ErrorClassV1,
        code: &'static str,
        retryable: bool,
    ) -> Result<Self, ProtocolError> {
        if !valid_error_code(code) {
            return Err(ProtocolError::InvalidResponse);
        }
        Ok(Self {
            class,
            code,
            retryable,
        })
    }

    /// Public HTTP status.
    #[must_use]
    pub const fn http_status(self) -> u16 {
        self.class.http_status()
    }

    /// Stable machine code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        self.code
    }

    /// Safe generic message selected only by class.
    #[must_use]
    pub const fn message(self) -> &'static str {
        self.class.message()
    }

    /// Whether retrying later may succeed.
    #[must_use]
    pub const fn retryable(self) -> bool {
        self.retryable
    }
}

fn valid_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}
