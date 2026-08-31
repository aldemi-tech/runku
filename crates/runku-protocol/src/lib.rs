//! Public protocol v1 codecs independent from an HTTP framework or `SaaS` control plane.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod development;
mod error;
mod realtime;
mod value;
mod wire;

pub use development::{
    DEVELOPMENT_JSON_MAX_BYTES, DEVELOPMENT_PROTOCOL_VERSION, DEVELOPMENT_PUBLISH_MAX_BYTES,
    DEVELOPMENT_PUBLISH_METADATA_MAX_BYTES, DevelopmentAdminErrorCodeV1,
    DevelopmentCreateWorkspaceRequestV1, DevelopmentCreateWorkspaceResponseV1,
    DevelopmentErrorResponseV1, DevelopmentFreezeDiagnosticV1, DevelopmentFreezeOutcomeV1,
    DevelopmentFreezeRequestV1, DevelopmentFreezeResponseV1, DevelopmentFreezeStageV1,
    DevelopmentPublishRequestV1, DevelopmentPublishResponseV1, DevelopmentStateRequestV1,
    DevelopmentStateResponseV1, DevelopmentWorkspaceStateV1, decode_development_create_request_v1,
    decode_development_create_response_v1, decode_development_error_v1,
    decode_development_freeze_request_v1, decode_development_freeze_response_v1,
    decode_development_publish_request_v1, decode_development_publish_response_v1,
    decode_development_state_request_v1, decode_development_state_response_v1,
    derive_development_freeze_operation_id_v1, derive_development_freeze_request_operation_id_v1,
    derive_development_revision_id_v1, encode_development_create_request_v1,
    encode_development_create_response_v1, encode_development_error_v1,
    encode_development_freeze_request_v1, encode_development_freeze_response_v1,
    encode_development_publish_request_v1, encode_development_publish_response_v1,
    encode_development_state_request_v1, encode_development_state_response_v1,
};
pub use error::{ErrorClassV1, ProtocolError, PublicErrorV1};
pub use realtime::{
    REALTIME_MESSAGE_MAX_BYTES, RealtimeClientMessageV1, RealtimeCredentialsV1,
    RealtimeServerMessageV1, decode_realtime_client_v1, decode_realtime_server_v1,
    encode_realtime_server_v1,
};
pub use value::{WireObjectEntryV1, WireValueV1};
pub use wire::{
    ActionCallV1, ErrorEnvelopeV1, MutationCallV1, QueryCallV1, SuccessEnvelopeV1,
    SuccessMetadataV1, decode_action_call_v1, decode_error_v1, decode_mutation_call_v1,
    decode_query_call_v1, decode_success_v1, encode_action_call_v1, encode_error_v1,
    encode_mutation_call_v1, encode_query_call_v1, encode_success_v1,
};

/// Public JSON protocol version implemented by this crate.
pub const PUBLIC_PROTOCOL_VERSION: u8 = 1;
/// Maximum accepted or emitted JSON envelope bytes.
pub const PUBLIC_ENVELOPE_MAX_BYTES: usize = 2 * 1024 * 1024;
