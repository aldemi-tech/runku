//! Strict public WebSocket message protocol v1.

use std::fmt;

use runku_core::{CodeTarget, FunctionName, ReleaseId, RequestId, SubscriptionId};
use runku_releases::Sha256Digest;
use runku_value::{CanonicalValue, TimestampMicros};
use serde::{Deserialize, Serialize};

use crate::{PUBLIC_PROTOCOL_VERSION, ProtocolError, value::WireValueV1};

/// Maximum one-message UTF-8 JSON payload accepted/emitted by Realtime v1.
pub const REALTIME_MESSAGE_MAX_BYTES: usize = 64 * 1024;
const MAX_APPLICATION_KEY_BYTES: usize = 256;
const MAX_BEARER_BYTES: usize = 16 * 1024;

/// Redacted credentials received in an `authenticate` frame.
pub struct RealtimeCredentialsV1 {
    /// Optional publishable/service application key.
    pub application_key: Option<String>,
    /// Optional functional bearer without an HTTP scheme prefix.
    pub bearer: Option<String>,
}

impl fmt::Debug for RealtimeCredentialsV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealtimeCredentialsV1")
            .field(
                "application_key",
                &self.application_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("bearer", &self.bearer.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Strict client-to-server WebSocket message union.
pub enum RealtimeClientMessageV1 {
    /// Installs structurally validated credentials for later subscribe calls.
    Authenticate {
        /// Client-generated correlation ID.
        request_id: RequestId,
        /// Redacted credentials.
        credentials: RealtimeCredentialsV1,
    },
    /// Creates a server-owned live Query subscription.
    Subscribe {
        /// Client-generated correlation ID.
        request_id: RequestId,
        /// Explicit code target resolved once.
        target: CodeTarget,
        /// Public Query function.
        function: FunctionName,
        /// Canonical Query arguments.
        arguments: CanonicalValue,
    },
    /// Removes one subscription owned by this connection.
    Unsubscribe {
        /// Client-generated correlation ID.
        request_id: RequestId,
        /// Server-generated subscription identity.
        subscription_id: SubscriptionId,
    },
    /// Application-level liveness probe.
    Ping {
        /// Correlation ID echoed by `pong`.
        request_id: RequestId,
    },
}

impl fmt::Debug for RealtimeClientMessageV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Authenticate { request_id, .. } => formatter
                .debug_struct("Authenticate")
                .field("request_id", request_id)
                .field("credentials", &"[REDACTED]")
                .finish(),
            Self::Subscribe {
                request_id,
                target,
                function,
                ..
            } => formatter
                .debug_struct("Subscribe")
                .field("request_id", request_id)
                .field("target", target)
                .field("function", function)
                .finish_non_exhaustive(),
            Self::Unsubscribe {
                request_id,
                subscription_id,
            } => formatter
                .debug_struct("Unsubscribe")
                .field("request_id", request_id)
                .field("subscription_id", subscription_id)
                .finish(),
            Self::Ping { request_id } => formatter
                .debug_struct("Ping")
                .field("request_id", request_id)
                .finish(),
        }
    }
}

/// Server-to-client Realtime event union.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RealtimeServerMessageV1 {
    /// Credentials were accepted structurally and stored for policy evaluation on subscribe.
    AuthenticationAccepted {
        /// Authenticate request correlation.
        request_id: RequestId,
    },
    /// Initial or updated committed Query state.
    State {
        /// Subscribe request correlation on initial state; absent on later deliveries.
        request_id: Option<RequestId>,
        /// Server-generated subscription identity.
        subscription_id: SubscriptionId,
        /// Candidate/stable Release whose manifest executed.
        release_id: ReleaseId,
        /// Monotonic registry delivery revision.
        delivery_revision: u64,
        /// Canonical Query value.
        value: CanonicalValue,
        /// Digest of canonical result bytes.
        result_hash: Sha256Digest,
        /// Query snapshot sequence, if data was read.
        snapshot_sequence: Option<u64>,
        /// Absolute time when the socket must reauthenticate/resubscribe.
        authorized_until: TimestampMicros,
    },
    /// Unsubscribe completed and registry state was removed.
    Unsubscribed {
        /// Unsubscribe request correlation.
        request_id: RequestId,
        /// Removed subscription.
        subscription_id: SubscriptionId,
    },
    /// Sanitized command or asynchronous subscription failure.
    Error {
        /// Related request, absent for asynchronous errors.
        request_id: Option<RequestId>,
        /// Related subscription, if any.
        subscription_id: Option<SubscriptionId>,
        /// Monotonic delivery revision for asynchronous registry failures.
        delivery_revision: Option<u64>,
        /// Stable bounded public code.
        code: String,
        /// Whether a later retry may succeed.
        retryable: bool,
    },
    /// Delivery continuity was lost and a fresh subscribe is mandatory.
    ResyncRequired {
        /// Subscription whose continuity was lost.
        subscription_id: SubscriptionId,
        /// Stable bounded reason code.
        code: String,
    },
    /// Response to an application-level ping.
    Pong {
        /// Echoed ping correlation.
        request_id: RequestId,
    },
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum ClientDto {
    Authenticate {
        version: u8,
        request_id: RequestId,
        application_key: Option<String>,
        bearer: Option<String>,
    },
    Subscribe {
        version: u8,
        request_id: RequestId,
        target: CodeTarget,
        function: String,
        arguments: WireValueV1,
    },
    Unsubscribe {
        version: u8,
        request_id: RequestId,
        subscription_id: SubscriptionId,
    },
    Ping {
        version: u8,
        request_id: RequestId,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum ServerDto {
    AuthenticationAccepted {
        version: u8,
        request_id: RequestId,
    },
    State {
        version: u8,
        request_id: Option<RequestId>,
        subscription_id: SubscriptionId,
        release_id: ReleaseId,
        delivery_revision: String,
        value: WireValueV1,
        result_hash: String,
        snapshot_sequence: Option<String>,
        authorized_until_micros: String,
    },
    Unsubscribed {
        version: u8,
        request_id: RequestId,
        subscription_id: SubscriptionId,
    },
    Error {
        version: u8,
        request_id: Option<RequestId>,
        subscription_id: Option<SubscriptionId>,
        delivery_revision: Option<String>,
        code: String,
        retryable: bool,
    },
    ResyncRequired {
        version: u8,
        subscription_id: SubscriptionId,
        code: String,
    },
    Pong {
        version: u8,
        request_id: RequestId,
    },
}

/// Decodes one strict client JSON message.
///
/// # Errors
///
/// Rejects empty/oversized/noncanonical JSON, unknown fields/types/version, invalid credentials,
/// IDs, Function names, targets, or Wire Values.
pub fn decode_realtime_client_v1(bytes: &[u8]) -> Result<RealtimeClientMessageV1, ProtocolError> {
    if bytes.is_empty() || bytes.len() > REALTIME_MESSAGE_MAX_BYTES {
        return Err(if bytes.is_empty() {
            ProtocolError::InvalidRequest
        } else {
            ProtocolError::LimitExceeded
        });
    }
    let dto: ClientDto =
        serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidRequest)?;
    Ok(match dto {
        ClientDto::Authenticate {
            version,
            request_id,
            application_key,
            bearer,
        } => {
            validate_version(version)?;
            validate_credential(application_key.as_deref(), MAX_APPLICATION_KEY_BYTES)?;
            validate_credential(bearer.as_deref(), MAX_BEARER_BYTES)?;
            RealtimeClientMessageV1::Authenticate {
                request_id,
                credentials: RealtimeCredentialsV1 {
                    application_key,
                    bearer,
                },
            }
        }
        ClientDto::Subscribe {
            version,
            request_id,
            target,
            function,
            arguments,
        } => {
            validate_version(version)?;
            RealtimeClientMessageV1::Subscribe {
                request_id,
                target,
                function: function
                    .parse()
                    .map_err(|_| ProtocolError::InvalidRequest)?,
                arguments: arguments.into_canonical()?,
            }
        }
        ClientDto::Unsubscribe {
            version,
            request_id,
            subscription_id,
        } => {
            validate_version(version)?;
            RealtimeClientMessageV1::Unsubscribe {
                request_id,
                subscription_id,
            }
        }
        ClientDto::Ping {
            version,
            request_id,
        } => {
            validate_version(version)?;
            RealtimeClientMessageV1::Ping { request_id }
        }
    })
}

/// Encodes one deterministic strict server JSON message.
///
/// # Errors
///
/// Rejects invalid codes/timestamps/sequence or messages above the v1 bound.
pub fn encode_realtime_server_v1(
    message: &RealtimeServerMessageV1,
) -> Result<Vec<u8>, ProtocolError> {
    let dto = server_to_dto(message)?;
    let bytes = serde_json::to_vec(&dto).map_err(|_| ProtocolError::InvalidResponse)?;
    if bytes.len() > REALTIME_MESSAGE_MAX_BYTES {
        return Err(ProtocolError::LimitExceeded);
    }
    Ok(bytes)
}

/// Decodes a strict server message for conformance clients/SDKs.
///
/// # Errors
///
/// Rejects malformed, oversized, unknown, or noncanonical response fields.
pub fn decode_realtime_server_v1(bytes: &[u8]) -> Result<RealtimeServerMessageV1, ProtocolError> {
    if bytes.is_empty() || bytes.len() > REALTIME_MESSAGE_MAX_BYTES {
        return Err(ProtocolError::InvalidResponse);
    }
    let dto: ServerDto =
        serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidResponse)?;
    server_from_dto(dto)
}

#[allow(clippy::too_many_lines)]
fn server_to_dto(message: &RealtimeServerMessageV1) -> Result<ServerDto, ProtocolError> {
    Ok(match message {
        RealtimeServerMessageV1::AuthenticationAccepted { request_id } => {
            ServerDto::AuthenticationAccepted {
                version: PUBLIC_PROTOCOL_VERSION,
                request_id: *request_id,
            }
        }
        RealtimeServerMessageV1::State {
            request_id,
            subscription_id,
            release_id,
            delivery_revision,
            value,
            result_hash,
            snapshot_sequence,
            authorized_until,
        } => {
            if *delivery_revision == 0 || authorized_until.get() < 0 {
                return Err(ProtocolError::InvalidResponse);
            }
            ServerDto::State {
                version: PUBLIC_PROTOCOL_VERSION,
                request_id: *request_id,
                subscription_id: *subscription_id,
                release_id: *release_id,
                delivery_revision: delivery_revision.to_string(),
                value: WireValueV1::from_canonical(value)?,
                result_hash: result_hash.to_string(),
                snapshot_sequence: snapshot_sequence.map(|value| value.to_string()),
                authorized_until_micros: authorized_until.get().to_string(),
            }
        }
        RealtimeServerMessageV1::Unsubscribed {
            request_id,
            subscription_id,
        } => ServerDto::Unsubscribed {
            version: PUBLIC_PROTOCOL_VERSION,
            request_id: *request_id,
            subscription_id: *subscription_id,
        },
        RealtimeServerMessageV1::Error {
            request_id,
            subscription_id,
            delivery_revision,
            code,
            retryable,
        } => {
            if delivery_revision == &Some(0) {
                return Err(ProtocolError::InvalidResponse);
            }
            validate_code(code)?;
            ServerDto::Error {
                version: PUBLIC_PROTOCOL_VERSION,
                request_id: *request_id,
                subscription_id: *subscription_id,
                delivery_revision: delivery_revision.map(|value| value.to_string()),
                code: code.clone(),
                retryable: *retryable,
            }
        }
        RealtimeServerMessageV1::ResyncRequired {
            subscription_id,
            code,
        } => {
            validate_code(code)?;
            ServerDto::ResyncRequired {
                version: PUBLIC_PROTOCOL_VERSION,
                subscription_id: *subscription_id,
                code: code.clone(),
            }
        }
        RealtimeServerMessageV1::Pong { request_id } => ServerDto::Pong {
            version: PUBLIC_PROTOCOL_VERSION,
            request_id: *request_id,
        },
    })
}

#[allow(clippy::too_many_lines)]
fn server_from_dto(dto: ServerDto) -> Result<RealtimeServerMessageV1, ProtocolError> {
    Ok(match dto {
        ServerDto::AuthenticationAccepted {
            version,
            request_id,
        } => {
            validate_response_version(version)?;
            RealtimeServerMessageV1::AuthenticationAccepted { request_id }
        }
        ServerDto::State {
            version,
            request_id,
            subscription_id,
            release_id,
            delivery_revision,
            value,
            result_hash,
            snapshot_sequence,
            authorized_until_micros,
        } => {
            validate_response_version(version)?;
            let delivery_revision = parse_u64(&delivery_revision)?;
            if delivery_revision == 0 {
                return Err(ProtocolError::InvalidResponse);
            }
            let authorized_until = parse_i64(&authorized_until_micros)?;
            if authorized_until < 0 {
                return Err(ProtocolError::InvalidResponse);
            }
            RealtimeServerMessageV1::State {
                request_id,
                subscription_id,
                release_id,
                delivery_revision,
                value: value
                    .into_canonical()
                    .map_err(|_| ProtocolError::InvalidResponse)?,
                result_hash: result_hash
                    .parse()
                    .map_err(|_| ProtocolError::InvalidResponse)?,
                snapshot_sequence: snapshot_sequence
                    .map(|value| parse_u64(&value))
                    .transpose()?,
                authorized_until: TimestampMicros::new(authorized_until),
            }
        }
        ServerDto::Unsubscribed {
            version,
            request_id,
            subscription_id,
        } => {
            validate_response_version(version)?;
            RealtimeServerMessageV1::Unsubscribed {
                request_id,
                subscription_id,
            }
        }
        ServerDto::Error {
            version,
            request_id,
            subscription_id,
            delivery_revision,
            code,
            retryable,
        } => {
            validate_response_version(version)?;
            validate_code(&code)?;
            let delivery_revision = delivery_revision
                .map(|value| parse_u64(&value))
                .transpose()?;
            if delivery_revision == Some(0) {
                return Err(ProtocolError::InvalidResponse);
            }
            RealtimeServerMessageV1::Error {
                request_id,
                subscription_id,
                delivery_revision,
                code,
                retryable,
            }
        }
        ServerDto::ResyncRequired {
            version,
            subscription_id,
            code,
        } => {
            validate_response_version(version)?;
            validate_code(&code)?;
            RealtimeServerMessageV1::ResyncRequired {
                subscription_id,
                code,
            }
        }
        ServerDto::Pong {
            version,
            request_id,
        } => {
            validate_response_version(version)?;
            RealtimeServerMessageV1::Pong { request_id }
        }
    })
}

fn validate_credential(value: Option<&str>, maximum: usize) -> Result<(), ProtocolError> {
    if value.is_some_and(|value| {
        value.is_empty()
            || value.len() > maximum
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
    }) {
        return Err(ProtocolError::InvalidRequest);
    }
    Ok(())
}

const fn validate_version(version: u8) -> Result<(), ProtocolError> {
    if version == PUBLIC_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion)
    }
}

const fn validate_response_version(version: u8) -> Result<(), ProtocolError> {
    if version == PUBLIC_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::InvalidResponse)
    }
}

fn validate_code(code: &str) -> Result<(), ProtocolError> {
    if code.is_empty()
        || code.len() > 64
        || !code.as_bytes()[0].is_ascii_uppercase()
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ProtocolError::InvalidResponse);
    }
    Ok(())
}

fn parse_u64(value: &str) -> Result<u64, ProtocolError> {
    if value.is_empty()
        || value.len() > 1 && value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ProtocolError::InvalidResponse);
    }
    value.parse().map_err(|_| ProtocolError::InvalidResponse)
}

fn parse_i64(value: &str) -> Result<i64, ProtocolError> {
    if value.is_empty()
        || value == "-0"
        || value.starts_with('+')
        || value.len() > 1 && value.starts_with('0')
        || value.starts_with("-0")
        || !value
            .bytes()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_digit() || index == 0 && byte == b'-')
    {
        return Err(ProtocolError::InvalidResponse);
    }
    value.parse().map_err(|_| ProtocolError::InvalidResponse)
}
