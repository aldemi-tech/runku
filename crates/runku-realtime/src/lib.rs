//! Strict durable change-impact decoding and reactive dependency matching.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeSet;

use runku_core::{DocumentId, IndexId, TableId};
use runku_execution::{DependencyBound, ReadDependency};
use runku_value::{CanonicalValue, IndexKey};
use thiserror::Error;

mod dispatcher;
mod registry;

pub use dispatcher::{
    ChangeDispatcher, DispatcherConfig, DispatcherError, DispatcherTelemetrySnapshot, PollOutcome,
    SubscriptionRunFailure, SubscriptionRunner,
};
pub use registry::{
    DeliveryEvent, RegistryConfig, RegistryTelemetrySnapshot, RerunTicket, SubscriptionHandle,
    SubscriptionRegistry, SubscriptionSnapshot, SubscriptionSpec,
};

const MAX_DOCUMENT_IMPACTS: usize = 1_000;
const MAX_INDEX_IMPACTS: usize = 10_000;

/// Stable durable-impact decoding failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RealtimeError {
    /// Payload shape, version, IDs, operation kind, or key bytes are invalid.
    #[error("realtime change impact is invalid")]
    InvalidImpact,
    /// Payload count exceeds a v1 bound.
    #[error("realtime change impact exceeds a v1 limit")]
    LimitExceeded,
    /// Configuration contains zero or inverted bounds.
    #[error("realtime configuration is invalid")]
    InvalidConfiguration,
    /// A Query result cannot be retained canonically.
    #[error("realtime Query outcome is invalid")]
    InvalidOutcome,
    /// Subscription identity already exists.
    #[error("realtime subscription already exists")]
    AlreadyExists,
    /// Subscription identity does not exist.
    #[error("realtime subscription was not found")]
    NotFound,
    /// A rerun completion no longer owns the active generation.
    #[error("realtime rerun ticket is stale")]
    StaleTicket,
    /// Unexpected internal state or synchronization failure.
    #[error("realtime failed internally")]
    Internal,
    /// A cursor still has an active or backed-off subscription rerun.
    #[error("realtime cursor still has pending reruns")]
    PendingWork,
}

impl RealtimeError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidImpact => "REALTIME_IMPACT_INVALID",
            Self::LimitExceeded => "REALTIME_IMPACT_LIMIT_EXCEEDED",
            Self::InvalidConfiguration => "REALTIME_CONFIGURATION_INVALID",
            Self::InvalidOutcome => "REALTIME_OUTCOME_INVALID",
            Self::AlreadyExists => "REALTIME_SUBSCRIPTION_EXISTS",
            Self::NotFound => "REALTIME_SUBSCRIPTION_NOT_FOUND",
            Self::StaleTicket => "REALTIME_RERUN_STALE",
            Self::Internal => "REALTIME_INTERNAL_ERROR",
            Self::PendingWork => "REALTIME_PENDING_WORK",
        }
    }

    /// Durable malformed payloads do not become valid on retry.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Internal | Self::PendingWork)
    }
}

/// Document operation represented by one committed Mutation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DocumentImpactKind {
    /// Absent document became present.
    Insert,
    /// Existing document value/revision changed.
    Replace,
    /// Existing document became absent.
    Delete,
}

/// One committed document identity impact.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DocumentImpact {
    /// Logical table.
    pub table_id: TableId,
    /// Logical document.
    pub document_id: DocumentId,
    /// Operation kind.
    pub kind: DocumentImpactKind,
}

/// Old-key removal or new-key insertion represented in the outbox.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IndexImpactKind {
    /// Remove the old entry.
    Delete,
    /// Insert/update the new entry.
    Put,
}

/// One committed logical index-key impact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexImpact {
    /// Logical index.
    pub index_id: IndexId,
    /// Canonical Index Key v1.
    pub key: IndexKey,
    /// Affected document.
    pub document_id: DocumentId,
    /// Old delete or new put.
    pub kind: IndexImpactKind,
}

/// Strict decoded `document_write_set_v2` payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeImpact {
    documents: Vec<DocumentImpact>,
    indexes: Vec<IndexImpact>,
}

impl ChangeImpact {
    /// Decodes and validates a complete durable v2 payload.
    ///
    /// # Errors
    ///
    /// Rejects unknown/missing fields, malformed IDs/keys, duplicates and bounded counts.
    pub fn decode(value: &CanonicalValue) -> Result<Self, RealtimeError> {
        let object = exact_object(value, &["indexes", "type", "writes"])?;
        if string(object.get("type"))? != "document_write_set_v2" {
            return Err(RealtimeError::InvalidImpact);
        }
        let writes = array(object.get("writes"))?;
        let index_values = array(object.get("indexes"))?;
        if writes.len() > MAX_DOCUMENT_IMPACTS || index_values.len() > MAX_INDEX_IMPACTS {
            return Err(RealtimeError::LimitExceeded);
        }

        let mut documents = Vec::with_capacity(writes.len());
        let mut document_keys = BTreeSet::new();
        for value in writes {
            let raw = exact_object(value, &["documentId", "kind", "tableId"])?;
            let table_id = parse_id(string(raw.get("tableId"))?)?;
            let document_id = parse_id(string(raw.get("documentId"))?)?;
            let kind = match string(raw.get("kind"))? {
                "insert" => DocumentImpactKind::Insert,
                "replace" => DocumentImpactKind::Replace,
                "delete" => DocumentImpactKind::Delete,
                _ => return Err(RealtimeError::InvalidImpact),
            };
            if !document_keys.insert((table_id, document_id)) {
                return Err(RealtimeError::InvalidImpact);
            }
            documents.push(DocumentImpact {
                table_id,
                document_id,
                kind,
            });
        }

        let mut indexes = Vec::with_capacity(index_values.len());
        let mut index_keys = BTreeSet::new();
        for value in index_values {
            let raw = exact_object(value, &["documentId", "indexId", "key", "kind"])?;
            let index_id = parse_id(string(raw.get("indexId"))?)?;
            let document_id = parse_id(string(raw.get("documentId"))?)?;
            let key = IndexKey::decode(bytes(raw.get("key"))?)
                .map_err(|_| RealtimeError::InvalidImpact)?;
            let kind = match string(raw.get("kind"))? {
                "delete" => IndexImpactKind::Delete,
                "put" => IndexImpactKind::Put,
                _ => return Err(RealtimeError::InvalidImpact),
            };
            if !index_keys.insert((index_id, key.as_bytes().to_vec(), document_id, kind)) {
                return Err(RealtimeError::InvalidImpact);
            }
            indexes.push(IndexImpact {
                index_id,
                key,
                document_id,
                kind,
            });
        }
        Ok(Self { documents, indexes })
    }

    /// Document impacts in durable payload order.
    #[must_use]
    pub fn documents(&self) -> &[DocumentImpact] {
        &self.documents
    }

    /// Index impacts in durable payload order.
    #[must_use]
    pub fn indexes(&self) -> &[IndexImpact] {
        &self.indexes
    }

    /// Returns true when any impact intersects any dependency.
    #[must_use]
    pub fn invalidates(&self, dependencies: &[ReadDependency]) -> bool {
        dependencies.iter().any(|dependency| match dependency {
            ReadDependency::Point {
                table_id,
                document_id,
                ..
            } => self
                .documents
                .iter()
                .any(|impact| impact.table_id == *table_id && impact.document_id == *document_id),
            ReadDependency::Range {
                index_id,
                lower,
                upper,
                ..
            } => self.indexes.iter().any(|impact| {
                impact.index_id == *index_id && key_in_bounds(impact.key.as_bytes(), lower, upper)
            }),
        })
    }
}

fn parse_id<T: std::str::FromStr>(value: &str) -> Result<T, RealtimeError> {
    value.parse().map_err(|_| RealtimeError::InvalidImpact)
}

fn key_in_bounds(key: &[u8], lower: &DependencyBound, upper: &DependencyBound) -> bool {
    let above_lower = match lower {
        DependencyBound::Unbounded => true,
        DependencyBound::Inclusive(value) => key >= value.as_slice(),
        DependencyBound::Exclusive(value) => key > value.as_slice(),
    };
    let below_upper = match upper {
        DependencyBound::Unbounded => true,
        DependencyBound::Inclusive(value) => key <= value.as_slice(),
        DependencyBound::Exclusive(value) => key < value.as_slice(),
    };
    above_lower && below_upper
}

fn exact_object<'a>(
    value: &'a CanonicalValue,
    keys: &[&str],
) -> Result<&'a std::collections::BTreeMap<String, CanonicalValue>, RealtimeError> {
    let CanonicalValue::Object(object) = value else {
        return Err(RealtimeError::InvalidImpact);
    };
    if object.len() != keys.len() || keys.iter().any(|key| !object.contains_key(*key)) {
        return Err(RealtimeError::InvalidImpact);
    }
    Ok(object)
}

fn string(value: Option<&CanonicalValue>) -> Result<&str, RealtimeError> {
    match value {
        Some(CanonicalValue::String(value)) => Ok(value),
        _ => Err(RealtimeError::InvalidImpact),
    }
}

fn bytes(value: Option<&CanonicalValue>) -> Result<&[u8], RealtimeError> {
    match value {
        Some(CanonicalValue::Bytes(value)) => Ok(value),
        _ => Err(RealtimeError::InvalidImpact),
    }
}

fn array(value: Option<&CanonicalValue>) -> Result<&[CanonicalValue], RealtimeError> {
    match value {
        Some(CanonicalValue::Array(value)) => Ok(value),
        _ => Err(RealtimeError::InvalidImpact),
    }
}
