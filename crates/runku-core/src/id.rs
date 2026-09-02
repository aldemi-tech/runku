//! Strongly typed identifiers.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;
use ulid::Ulid;

/// Error returned when a resource identifier is not canonical.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParseResourceIdError {
    /// The identifier does not start with the prefix required by its type.
    #[error("resource identifier has an unexpected prefix; expected {expected}")]
    UnexpectedPrefix {
        /// Required public prefix.
        expected: &'static str,
    },
    /// The payload after the prefix is not a canonical ULID.
    #[error("resource identifier payload is not a canonical ULID")]
    InvalidUlid,
}

impl ParseResourceIdError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnexpectedPrefix { .. } => "RESOURCE_ID_PREFIX_INVALID",
            Self::InvalidUlid => "RESOURCE_ID_PAYLOAD_INVALID",
        }
    }

    /// Identifier parse failures are deterministic and must not be retried unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

macro_rules! resource_id {
    ($(#[$meta:meta])* $name:ident, $prefix:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Ulid);

        impl $name {
            /// Prefix used by the canonical text representation.
            pub const PREFIX: &'static str = $prefix;

            /// Generates a new time-sortable identifier.
            #[must_use]
            pub fn generate() -> Self {
                Self(Ulid::generate())
            }

            /// Creates a typed identifier from an existing ULID.
            #[must_use]
            pub const fn from_ulid(value: Ulid) -> Self {
                Self(value)
            }

            /// Returns the underlying ULID without losing the resource type at call sites.
            #[must_use]
            pub const fn as_ulid(self) -> Ulid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}{}", Self::PREFIX, self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseResourceIdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let payload = value.strip_prefix(Self::PREFIX).ok_or(
                    ParseResourceIdError::UnexpectedPrefix {
                        expected: Self::PREFIX,
                    },
                )?;
                let ulid = payload
                    .parse::<Ulid>()
                    .map_err(|_| ParseResourceIdError::InvalidUlid)?;
                if ulid.to_string() != payload {
                    return Err(ParseResourceIdError::InvalidUlid);
                }
                Ok(Self(ulid))
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(de::Error::custom)
            }
        }
    };
}

resource_id!(
    /// Identifies a Runku project.
    ProjectId,
    "prj_"
);
resource_id!(
    /// Identifies an Environment, the owner of application state.
    EnvironmentId,
    "env_"
);
resource_id!(
    /// Identifies an immutable, durable Release.
    ReleaseId,
    "rel_"
);
resource_id!(
    /// Identifies one reproducible build attempt.
    BuildId,
    "bld_"
);
resource_id!(
    /// Identifies one logical function across Releases.
    FunctionId,
    "fnc_"
);
resource_id!(
    /// Identifies an immutable development build.
    DevRevisionId,
    "drv_"
);
resource_id!(
    /// Identifies the durable record behind a Development Workspace.
    WorkspaceId,
    "wsp_"
);
resource_id!(
    /// Identifies a logical application caller independently from credentials.
    ApplicationClientId,
    "app_"
);
resource_id!(
    /// Identifies one replaceable application credential.
    CredentialId,
    "crd_"
);
resource_id!(
    /// Identifies one replaceable Development Access credential.
    DevelopmentCredentialId,
    "dvk_"
);
resource_id!(
    /// Identifies one human operator of a Runku installation.
    OperatorId,
    "opr_"
);
resource_id!(
    /// Identifies one independently revocable operator device session.
    OperatorSessionId,
    "ops_"
);
resource_id!(
    /// Identifies one single-use platform access invitation.
    OperatorInvitationId,
    "opi_"
);
resource_id!(
    /// Identifies one request across component boundaries.
    RequestId,
    "req_"
);
resource_id!(
    /// Identifies one Function execution independently from its transport request.
    InvocationId,
    "inv_"
);
resource_id!(
    /// Identifies one logical table.
    TableId,
    "tbl_"
);
resource_id!(
    /// Identifies one application document.
    DocumentId,
    "doc_"
);
resource_id!(
    /// Identifies one logical index.
    IndexId,
    "idx_"
);
resource_id!(
    /// Identifies one durable outbox event.
    OutboxEventId,
    "evt_"
);
resource_id!(
    /// Identifies one operational log event independently from storage sequence.
    OperationalEventId,
    "log_"
);
resource_id!(
    /// Identifies one durable scheduled invocation.
    ScheduledInvocationId,
    "sch_"
);
resource_id!(
    /// Identifies one reactive Query subscription.
    SubscriptionId,
    "sub_"
);
resource_id!(
    /// Identifies one idempotent storage operation.
    OperationId,
    "opn_"
);
resource_id!(
    /// Identifies a scheduler worker that owns a lease.
    WorkerId,
    "wrk_"
);

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    #[test]
    fn typed_id_round_trips_through_text_and_json() -> Result<(), Box<dyn Error>> {
        let parsed: ReleaseId = format!("rel_{ULID}").parse()?;
        assert_eq!(parsed.to_string(), format!("rel_{ULID}"));

        let json = serde_json::to_string(&parsed)?;
        assert_eq!(json, format!("\"rel_{ULID}\""));
        let decoded: ReleaseId = serde_json::from_str(&json)?;
        assert_eq!(decoded, parsed);
        Ok(())
    }

    #[test]
    fn wrong_resource_prefix_is_rejected() {
        let error = format!("env_{ULID}").parse::<ReleaseId>();
        assert_eq!(
            error,
            Err(ParseResourceIdError::UnexpectedPrefix { expected: "rel_" })
        );
    }

    #[test]
    fn non_canonical_ulid_is_rejected() {
        let error = format!("rel_{}", ULID.to_ascii_lowercase()).parse::<ReleaseId>();
        assert_eq!(error, Err(ParseResourceIdError::InvalidUlid));
    }

    #[test]
    fn malformed_or_truncated_ulid_is_rejected_with_stable_error() {
        for value in ["rel_", "rel_not-a-ulid", "rel_01ARZ3NDEKTSV4RRFFQ69G5FA"] {
            let error = value.parse::<ReleaseId>();
            assert_eq!(error, Err(ParseResourceIdError::InvalidUlid));
        }
        assert_eq!(
            ParseResourceIdError::InvalidUlid.code(),
            "RESOURCE_ID_PAYLOAD_INVALID"
        );
        assert!(!ParseResourceIdError::InvalidUlid.retryable());
    }

    #[test]
    fn generated_id_is_canonical_and_round_trips() -> Result<(), Box<dyn Error>> {
        let generated = RequestId::generate();
        let wire = generated.to_string();
        assert!(wire.starts_with(RequestId::PREFIX));
        assert_eq!(wire.parse::<RequestId>()?, generated);
        Ok(())
    }

    #[test]
    fn resource_kinds_have_distinct_prefixes() {
        let prefixes = [
            ProjectId::PREFIX,
            EnvironmentId::PREFIX,
            ReleaseId::PREFIX,
            BuildId::PREFIX,
            FunctionId::PREFIX,
            DevRevisionId::PREFIX,
            WorkspaceId::PREFIX,
            ApplicationClientId::PREFIX,
            CredentialId::PREFIX,
            DevelopmentCredentialId::PREFIX,
            RequestId::PREFIX,
            TableId::PREFIX,
            DocumentId::PREFIX,
            IndexId::PREFIX,
            OutboxEventId::PREFIX,
            OperationalEventId::PREFIX,
            ScheduledInvocationId::PREFIX,
            OperationId::PREFIX,
            WorkerId::PREFIX,
        ];
        for (index, prefix) in prefixes.iter().enumerate() {
            assert!(!prefix.is_empty());
            assert!(!prefixes[..index].contains(prefix));
        }
    }
}
