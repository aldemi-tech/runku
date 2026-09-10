//! Deterministic, fail-closed compatibility checks between immutable Runku Releases.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;

use runku_contracts::{Contract, DocumentSchemaV1, decode_contract, decode_document_schema};
use runku_core::{FunctionName, TableId};
use runku_releases::{
    ArtifactFormat, FunctionVisibility, ReleaseManifestV1, SafeEsmBundleV1, Sha256Digest,
    decode_hybrid_oci_artifact, decode_safe_esm_bundle,
};
use runku_schema::decode_schema_catalog;
use thiserror::Error;

const MAX_RELATION_STEPS: usize = 200_000;
const MAX_DIAGNOSTICS: usize = 4_096;

/// A fully integrity-checked Release input ready for pure compatibility analysis.
#[derive(Clone, Debug)]
pub struct ReleasePackage {
    manifest: ReleaseManifestV1,
    bundle: SafeEsmBundleV1,
    contracts: BTreeMap<Sha256Digest, Contract>,
    schema: Option<DocumentSchemaV1>,
}

impl ReleasePackage {
    /// Decodes and verifies a manifest/artifact pair and all v2 contract resources.
    ///
    /// # Errors
    ///
    /// Fails closed on malformed bytes, descriptor/hash drift, unsupported runtime, absent
    /// resources, or noncanonical Contract/Schema encodings.
    pub fn load(
        manifest: ReleaseManifestV1,
        artifact_bytes: &[u8],
    ) -> Result<Self, CompatibilityError> {
        let bundle = match manifest.artifact.format {
            ArtifactFormat::SafeEsmBundleV1 => {
                manifest
                    .ensure_mvp_runtime_supported()
                    .map_err(|_| CompatibilityError::InvalidRelease)?;
                let bundle = decode_safe_esm_bundle(artifact_bytes)
                    .map_err(|_| CompatibilityError::InvalidArtifact)?;
                bundle
                    .verify_manifest(&manifest, artifact_bytes)
                    .map_err(|_| CompatibilityError::InvalidArtifact)?;
                bundle
            }
            ArtifactFormat::HybridOciArtifactV1 => {
                manifest
                    .ensure_full_node_supported()
                    .map_err(|_| CompatibilityError::InvalidRelease)?;
                if manifest.artifact.size_bytes
                    != u64::try_from(artifact_bytes.len())
                        .map_err(|_| CompatibilityError::InvalidArtifact)?
                    || manifest.artifact.digest != Sha256Digest::of(artifact_bytes)
                {
                    return Err(CompatibilityError::InvalidArtifact);
                }
                let (resources, _) = decode_hybrid_oci_artifact(artifact_bytes)
                    .map_err(|_| CompatibilityError::InvalidArtifact)?;
                let bundle = decode_safe_esm_bundle(resources)
                    .map_err(|_| CompatibilityError::InvalidArtifact)?;
                if manifest.functions.iter().any(|function| {
                    bundle.source(function.implementation_hash).is_none()
                        || bundle.resource(function.arguments_contract_hash).is_none()
                        || bundle.resource(function.result_contract_hash).is_none()
                }) || bundle.resource(manifest.schema_contract_hash).is_none()
                    || bundle.resource(manifest.index_contract_hash).is_none()
                {
                    return Err(CompatibilityError::InvalidArtifact);
                }
                bundle
            }
            _ => return Err(CompatibilityError::InvalidRelease),
        };

        let mut contracts = BTreeMap::new();
        let schema = if matches!(
            manifest.runtime_version.as_str(),
            "runku-js-1"
                | "runku-js-2"
                | "runku-js-3"
                | "runku-js"
                | "runku-hybrid-1"
                | "runku-hybrid-2"
                | "runku-hybrid-3"
                | "runku-hybrid"
        ) {
            for function in &manifest.functions {
                load_contract(&bundle, function.arguments_contract_hash, &mut contracts)?;
                load_contract(&bundle, function.result_contract_hash, &mut contracts)?;
            }
            let source = bundle
                .resource(manifest.schema_contract_hash)
                .ok_or(CompatibilityError::InvalidContract)?;
            let schema = Some(
                decode_document_schema(source.as_bytes())
                    .map_err(|_| CompatibilityError::InvalidContract)?,
            );
            let index_source = bundle
                .resource(manifest.index_contract_hash)
                .ok_or(CompatibilityError::InvalidContract)?;
            let indexes = decode_schema_catalog(index_source.as_bytes())
                .map_err(|_| CompatibilityError::InvalidContract)?;
            if indexes.project_id() != manifest.project_id
                || indexes.digest().as_slice() != manifest.index_contract_hash.as_bytes()
            {
                return Err(CompatibilityError::InvalidContract);
            }
            schema
        } else {
            None
        };
        Ok(Self {
            manifest,
            bundle,
            contracts,
            schema,
        })
    }

    /// Returns the verified immutable manifest.
    #[must_use]
    pub const fn manifest(&self) -> &ReleaseManifestV1 {
        &self.manifest
    }

    /// Returns the verified bundle, useful to pass the package to a runtime loader.
    #[must_use]
    pub const fn bundle(&self) -> &SafeEsmBundleV1 {
        &self.bundle
    }

    fn contract(&self, digest: Sha256Digest) -> Option<&Contract> {
        self.contracts.get(&digest)
    }
}

fn load_contract(
    bundle: &SafeEsmBundleV1,
    digest: Sha256Digest,
    contracts: &mut BTreeMap<Sha256Digest, Contract>,
) -> Result<(), CompatibilityError> {
    if contracts.contains_key(&digest) {
        return Ok(());
    }
    let source = bundle
        .resource(digest)
        .ok_or(CompatibilityError::InvalidContract)?;
    let contract =
        decode_contract(source.as_bytes()).map_err(|_| CompatibilityError::InvalidContract)?;
    contracts.insert(digest, contract);
    Ok(())
}

/// Stable category produced while loading or comparing Release packages.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CompatibilityError {
    /// Manifest or requested comparison violates Release invariants.
    #[error("release compatibility input is invalid")]
    InvalidRelease,
    /// Artifact bytes do not match or cannot be decoded canonically.
    #[error("release artifact is invalid")]
    InvalidArtifact,
    /// A required Contract/Schema resource is absent or noncanonical.
    #[error("release contract resource is invalid")]
    InvalidContract,
    /// The comparison exceeded a hard work or diagnostic bound.
    #[error("release compatibility comparison exceeds limits")]
    LimitExceeded,
}

impl CompatibilityError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRelease => "COMPATIBILITY_RELEASE_INVALID",
            Self::InvalidArtifact => "COMPATIBILITY_ARTIFACT_INVALID",
            Self::InvalidContract => "COMPATIBILITY_CONTRACT_INVALID",
            Self::LimitExceeded => "COMPATIBILITY_LIMIT_EXCEEDED",
        }
    }
}

/// One deterministic incompatibility attached to a bounded logical subject.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CompatibilityDiagnostic {
    /// Stable machine-readable reason.
    pub code: &'static str,
    /// Canonical function name, table ID, or `release`.
    pub subject: String,
}

/// Complete ordered compatibility decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilityReport {
    /// Whether no blocking diagnostic exists.
    pub compatible: bool,
    /// Sorted, deduplicated blocking diagnostics.
    pub diagnostics: Vec<CompatibilityDiagnostic>,
}

/// Pure compatibility policy for safe coexistence and Channel movement.
#[derive(Clone, Copy, Debug, Default)]
pub struct CompatibilityEngine;

impl CompatibilityEngine {
    /// Proves that `candidate` may replace `base` for existing callers and documents.
    ///
    /// # Errors
    ///
    /// Returns an input/limit error. Semantic incompatibility is a successful report with
    /// `compatible == false` and stable diagnostics.
    pub fn compare(
        base: &ReleasePackage,
        candidate: &ReleasePackage,
    ) -> Result<CompatibilityReport, CompatibilityError> {
        if base.manifest.project_id != candidate.manifest.project_id
            || base.manifest.release_id == candidate.manifest.release_id
        {
            return Err(CompatibilityError::InvalidRelease);
        }
        let mut diagnostics = Vec::new();
        compare_functions(base, candidate, &mut diagnostics)?;
        compare_schema(base, candidate, &mut diagnostics)?;
        diagnostics.sort();
        diagnostics.dedup();
        Ok(CompatibilityReport {
            compatible: diagnostics.is_empty(),
            diagnostics,
        })
    }

    /// Proves that `candidate` can replace `base` for callers of the same Channel.
    ///
    /// This comparison covers the public Function surface only. Environment data coexistence is
    /// an independent symmetric proof performed by [`Self::coexist`]. Keeping the decisions
    /// separate permits a rollback to an older read view after an optional schema expansion while
    /// still rejecting an API change that would break callers of the moved Channel.
    ///
    /// # Errors
    ///
    /// Returns an input/limit error. Semantic incompatibility is returned as diagnostics.
    pub fn compare_api(
        base: &ReleasePackage,
        candidate: &ReleasePackage,
    ) -> Result<CompatibilityReport, CompatibilityError> {
        validate_pair(base, candidate)?;
        let mut diagnostics = Vec::new();
        compare_functions(base, candidate, &mut diagnostics)?;
        Ok(finish_report(diagnostics))
    }

    /// Proves symmetric coexistence of two immutable Release data views in one Environment.
    ///
    /// A field present in only one view is safe only when optional. Fields present in both views
    /// must accept the same value set. Logical-index changes are admitted here because the Release
    /// lifecycle independently requires durable backfill readiness before `SERVABLE`.
    ///
    /// # Errors
    ///
    /// Returns an input/limit error. Semantic incompatibility is returned as diagnostics.
    pub fn coexist(
        left: &ReleasePackage,
        right: &ReleasePackage,
    ) -> Result<CompatibilityReport, CompatibilityError> {
        validate_pair(left, right)?;
        let mut diagnostics = Vec::new();
        compare_schema_coexistence(left, right, &mut diagnostics)?;
        if left.manifest.cron_definitions != right.manifest.cron_definitions {
            push_diagnostic(&mut diagnostics, "CRON_DECLARATIONS_DIVERGED", "release")?;
        }
        Ok(finish_report(diagnostics))
    }
}

fn validate_pair(
    base: &ReleasePackage,
    candidate: &ReleasePackage,
) -> Result<(), CompatibilityError> {
    if base.manifest.project_id != candidate.manifest.project_id
        || base.manifest.release_id == candidate.manifest.release_id
    {
        Err(CompatibilityError::InvalidRelease)
    } else {
        Ok(())
    }
}

fn finish_report(mut diagnostics: Vec<CompatibilityDiagnostic>) -> CompatibilityReport {
    diagnostics.sort();
    diagnostics.dedup();
    CompatibilityReport {
        compatible: diagnostics.is_empty(),
        diagnostics,
    }
}

fn compare_functions(
    base: &ReleasePackage,
    candidate: &ReleasePackage,
    diagnostics: &mut Vec<CompatibilityDiagnostic>,
) -> Result<(), CompatibilityError> {
    let candidates: BTreeMap<&FunctionName, _> = candidate
        .manifest
        .functions
        .iter()
        .map(|function| (&function.name, function))
        .collect();
    for previous in base
        .manifest
        .functions
        .iter()
        .filter(|function| function.visibility == FunctionVisibility::Public)
    {
        let subject = previous.name.as_str();
        let Some(next) = candidates.get(&previous.name) else {
            push_diagnostic(diagnostics, "PUBLIC_FUNCTION_REMOVED", subject)?;
            continue;
        };
        if previous.id != next.id
            || previous.function_type != next.function_type
            || previous.visibility != next.visibility
            || previous.auth_policy != next.auth_policy
            || previous.runtime_class != next.runtime_class
            || previous.capabilities != next.capabilities
        {
            push_diagnostic(diagnostics, "PUBLIC_FUNCTION_METADATA_CHANGED", subject)?;
        }
        let previous_arguments = resolved_contract(base, previous.arguments_contract_hash);
        let next_arguments = resolved_contract(candidate, next.arguments_contract_hash);
        if !contract_subset(previous_arguments, next_arguments)? {
            push_diagnostic(diagnostics, "FUNCTION_ARGUMENTS_NARROWED", subject)?;
        }
        let previous_result = resolved_contract(base, previous.result_contract_hash);
        let next_result = resolved_contract(candidate, next.result_contract_hash);
        if !contract_subset(next_result, previous_result)? {
            push_diagnostic(diagnostics, "FUNCTION_RESULT_WIDENED", subject)?;
        }
    }
    Ok(())
}

fn resolved_contract(package: &ReleasePackage, digest: Sha256Digest) -> &Contract {
    static ANY: Contract = Contract::Any;
    package.contract(digest).unwrap_or(&ANY)
}

fn compare_schema(
    base: &ReleasePackage,
    candidate: &ReleasePackage,
    diagnostics: &mut Vec<CompatibilityDiagnostic>,
) -> Result<(), CompatibilityError> {
    match (&base.schema, &candidate.schema) {
        (Some(previous), Some(next)) => {
            let next_tables: BTreeMap<TableId, _> =
                next.tables.iter().map(|table| (table.id, table)).collect();
            for table in &previous.tables {
                let subject = table.id.to_string();
                let Some(next_table) = next_tables.get(&table.id) else {
                    push_diagnostic(diagnostics, "SCHEMA_TABLE_REMOVED", &subject)?;
                    continue;
                };
                if table.name != next_table.name {
                    push_diagnostic(diagnostics, "SCHEMA_TABLE_RENAMED", &subject)?;
                }
                if !contract_subset(&table.document_contract, &next_table.document_contract)? {
                    push_diagnostic(diagnostics, "SCHEMA_DOCUMENT_NARROWED", &subject)?;
                }
            }
        }
        (None, None | Some(_)) => {
            if base.manifest.schema_contract_hash != candidate.manifest.schema_contract_hash {
                push_diagnostic(diagnostics, "SCHEMA_CHANGE_UNPROVEN", "release")?;
            }
        }
        (Some(_), None) => {
            push_diagnostic(diagnostics, "SCHEMA_VALIDATION_REMOVED", "release")?;
        }
    }
    Ok(())
}

fn compare_schema_coexistence(
    left: &ReleasePackage,
    right: &ReleasePackage,
    diagnostics: &mut Vec<CompatibilityDiagnostic>,
) -> Result<(), CompatibilityError> {
    match (&left.schema, &right.schema) {
        (Some(left), Some(right)) => {
            let right_tables: BTreeMap<TableId, _> =
                right.tables.iter().map(|table| (table.id, table)).collect();
            for table in &left.tables {
                let Some(other) = right_tables.get(&table.id) else {
                    continue;
                };
                let subject = table.id.to_string();
                if table.name != other.name {
                    push_diagnostic(diagnostics, "SCHEMA_TABLE_ID_RENAMED", &subject)?;
                    continue;
                }
                if !contracts_can_coexist(&table.document_contract, &other.document_contract)? {
                    push_diagnostic(diagnostics, "SCHEMA_VIEWS_CANNOT_COEXIST", &subject)?;
                }
            }
        }
        (None, None) => {}
        (None, Some(_)) | (Some(_), None) => {
            push_diagnostic(diagnostics, "SCHEMA_VIEW_UNAVAILABLE", "release")?;
        }
    }
    Ok(())
}

fn contracts_can_coexist(left: &Contract, right: &Contract) -> Result<bool, CompatibilityError> {
    let mut steps = 0_usize;
    contracts_can_coexist_at(left, right, &mut steps)
}

fn contracts_can_coexist_at(
    left: &Contract,
    right: &Contract,
    steps: &mut usize,
) -> Result<bool, CompatibilityError> {
    *steps = steps.saturating_add(1);
    if *steps > MAX_RELATION_STEPS {
        return Err(CompatibilityError::LimitExceeded);
    }
    match (left, right) {
        (
            Contract::Object {
                fields: left_fields,
                optional: left_optional,
            },
            Contract::Object {
                fields: right_fields,
                optional: right_optional,
            },
        ) => {
            for (name, contract) in left_fields {
                match right_fields.get(name) {
                    Some(other) => {
                        if left_optional.contains(name) != right_optional.contains(name)
                            || !contracts_can_coexist_at(contract, other, steps)?
                        {
                            return Ok(false);
                        }
                    }
                    None if left_optional.contains(name) => {}
                    None => return Ok(false),
                }
            }
            Ok(right_fields
                .keys()
                .all(|name| left_fields.contains_key(name) || right_optional.contains(name)))
        }
        (
            Contract::Array {
                items: left_items,
                minimum_items: left_minimum,
                maximum_items: left_maximum,
            },
            Contract::Array {
                items: right_items,
                minimum_items: right_minimum,
                maximum_items: right_maximum,
            },
        ) => Ok(left_minimum == right_minimum
            && left_maximum == right_maximum
            && contracts_can_coexist_at(left_items, right_items, steps)?),
        (Contract::Union { variants: left }, Contract::Union { variants: right }) => {
            if left.len() != right.len() {
                return Ok(false);
            }
            for variant in left {
                let mut matched = false;
                for other in right {
                    if contract_subset(variant, other)? && contract_subset(other, variant)? {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(contract_subset(left, right)? && contract_subset(right, left)?),
    }
}

fn push_diagnostic(
    diagnostics: &mut Vec<CompatibilityDiagnostic>,
    code: &'static str,
    subject: &str,
) -> Result<(), CompatibilityError> {
    if diagnostics.len() >= MAX_DIAGNOSTICS {
        return Err(CompatibilityError::LimitExceeded);
    }
    diagnostics.push(CompatibilityDiagnostic {
        code,
        subject: subject.to_owned(),
    });
    Ok(())
}

/// Proves that every value accepted by `subset` is also accepted by `superset`.
///
/// The algorithm is deliberately conservative for unions: if coverage requires combining
/// multiple superset variants, it returns false rather than accepting an unproven relation.
///
/// # Errors
///
/// Returns a limit error for adversarially expensive recursive comparisons.
pub fn contract_is_subset(
    subset: &Contract,
    superset: &Contract,
) -> Result<bool, CompatibilityError> {
    contract_subset(subset, superset)
}

fn contract_subset(subset: &Contract, superset: &Contract) -> Result<bool, CompatibilityError> {
    let mut steps = 0_usize;
    contract_subset_at(subset, superset, &mut steps)
}

#[allow(clippy::too_many_lines)]
fn contract_subset_at(
    subset: &Contract,
    superset: &Contract,
    steps: &mut usize,
) -> Result<bool, CompatibilityError> {
    *steps = steps.saturating_add(1);
    if *steps > MAX_RELATION_STEPS {
        return Err(CompatibilityError::LimitExceeded);
    }
    if matches!(superset, Contract::Any) {
        return Ok(true);
    }
    if let Contract::Union { variants } = subset {
        for variant in variants {
            if !contract_subset_at(variant, superset, steps)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if let Contract::Union { variants } = superset {
        for variant in variants {
            if contract_subset_at(subset, variant, steps)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    match (subset, superset) {
        (Contract::Null, Contract::Null)
        | (Contract::Boolean, Contract::Boolean)
        | (Contract::Timestamp, Contract::Timestamp) => Ok(true),
        (
            Contract::Int64 {
                minimum: left_min,
                maximum: left_max,
            },
            Contract::Int64 {
                minimum: right_min,
                maximum: right_max,
            },
        ) => Ok(lower_inside(*left_min, *right_min) && upper_inside(*left_max, *right_max)),
        (
            Contract::Float64 {
                minimum: left_min,
                maximum: left_max,
            },
            Contract::Float64 {
                minimum: right_min,
                maximum: right_max,
            },
        ) => Ok(lower_inside(
            left_min.map(runku_contracts::FiniteBound::get),
            right_min.map(runku_contracts::FiniteBound::get),
        ) && upper_inside(
            left_max.map(runku_contracts::FiniteBound::get),
            right_max.map(runku_contracts::FiniteBound::get),
        )),
        (
            Contract::String {
                minimum_length: left_min_length,
                maximum_length: left_max_length,
                minimum_bytes: left_min_bytes,
                maximum_bytes: left_max_bytes,
            },
            Contract::String {
                minimum_length: right_min_length,
                maximum_length: right_max_length,
                minimum_bytes: right_min_bytes,
                maximum_bytes: right_max_bytes,
            },
        ) => Ok(lower_inside(*left_min_length, *right_min_length)
            && upper_inside(*left_max_length, *right_max_length)
            && lower_inside(*left_min_bytes, *right_min_bytes)
            && upper_inside(*left_max_bytes, *right_max_bytes)),
        (
            Contract::Bytes {
                minimum_bytes: left_min_bytes,
                maximum_bytes: left_max_bytes,
            },
            Contract::Bytes {
                minimum_bytes: right_min_bytes,
                maximum_bytes: right_max_bytes,
            },
        ) => Ok(lower_inside(*left_min_bytes, *right_min_bytes)
            && upper_inside(*left_max_bytes, *right_max_bytes)),
        (Contract::TypedId { kind: left }, Contract::TypedId { kind: right }) => {
            Ok(right.is_none() || left == right)
        }
        (Contract::DocumentId { table: left }, Contract::DocumentId { table: right }) => {
            Ok(left == right)
        }
        (
            Contract::Array {
                items: left_items,
                minimum_items: left_min,
                maximum_items: left_max,
            },
            Contract::Array {
                items: right_items,
                minimum_items: right_min,
                maximum_items: right_max,
            },
        ) => Ok(lower_inside(*left_min, *right_min)
            && upper_inside(*left_max, *right_max)
            && contract_subset_at(left_items, right_items, steps)?),
        (
            Contract::Object {
                fields: left_fields,
                optional: left_optional,
            },
            Contract::Object {
                fields: right_fields,
                optional: right_optional,
            },
        ) => {
            for (name, left_contract) in left_fields {
                let Some(right_contract) = right_fields.get(name) else {
                    return Ok(false);
                };
                if !contract_subset_at(left_contract, right_contract, steps)? {
                    return Ok(false);
                }
            }
            for name in right_fields.keys() {
                if !left_fields.contains_key(name) && !right_optional.contains(name) {
                    return Ok(false);
                }
                if !right_optional.contains(name) && left_optional.contains(name) {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn lower_inside<T: PartialOrd>(subset: Option<T>, superset: Option<T>) -> bool {
    match (subset, superset) {
        (_, None) => true,
        (Some(left), Some(right)) => left >= right,
        (None, Some(_)) => false,
    }
}

fn upper_inside<T: PartialOrd>(subset: Option<T>, superset: Option<T>) -> bool {
    match (subset, superset) {
        (_, None) => true,
        (Some(left), Some(right)) => left <= right,
        (None, Some(_)) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use runku_contracts::{
        Contract, DocumentSchemaV1, DocumentTableContract, encode_contract, encode_document_schema,
    };
    use runku_core::{BuildId, FunctionId, ProjectId, ReleaseId, TableId};
    use runku_releases::{
        AuthPolicy, FunctionManifest, FunctionType, FunctionVisibility, NodeEsmBundleV1,
        NodeOciDescriptorV1, ReleaseManifestV1, RuntimeClass, SafeEsmBundleV1, Sha256Digest,
        encode_hybrid_oci_artifact, encode_node_esm_bundle, encode_node_oci_descriptor,
        encode_safe_esm_bundle, hybrid_oci_descriptor,
    };
    use runku_value::TimestampMicros;

    use super::{CompatibilityEngine, ReleasePackage, contract_is_subset};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn int(minimum: Option<i64>, maximum: Option<i64>) -> Contract {
        Contract::Int64 { minimum, maximum }
    }

    #[test]
    fn scalar_bounds_have_exact_subset_direction() -> TestResult {
        assert!(contract_is_subset(
            &int(Some(0), Some(10)),
            &int(None, Some(20))
        )?);
        assert!(!contract_is_subset(
            &int(None, Some(10)),
            &int(Some(0), Some(20))
        )?);
        assert!(contract_is_subset(
            &Contract::String {
                minimum_length: None,
                maximum_length: None,
                minimum_bytes: Some(2),
                maximum_bytes: Some(8),
            },
            &Contract::String {
                minimum_length: None,
                maximum_length: None,
                minimum_bytes: None,
                maximum_bytes: Some(10),
            }
        )?);
        assert!(contract_is_subset(
            &Contract::TypedId {
                kind: Some("doc".to_owned())
            },
            &Contract::TypedId { kind: None }
        )?);
        assert!(!contract_is_subset(
            &Contract::TypedId { kind: None },
            &Contract::TypedId {
                kind: Some("doc".to_owned())
            }
        )?);
        assert!(contract_is_subset(
            &Contract::DocumentId {
                table: "rooms".to_owned()
            },
            &Contract::DocumentId {
                table: "rooms".to_owned()
            }
        )?);
        assert!(!contract_is_subset(
            &Contract::DocumentId {
                table: "rooms".to_owned()
            },
            &Contract::DocumentId {
                table: "profiles".to_owned()
            }
        )?);
        Ok(())
    }

    #[test]
    fn exact_objects_account_for_required_and_optional_fields() -> TestResult {
        let old = Contract::Object {
            fields: BTreeMap::from([(
                "name".to_owned(),
                Contract::String {
                    minimum_length: None,
                    maximum_length: None,
                    minimum_bytes: None,
                    maximum_bytes: None,
                },
            )]),
            optional: BTreeSet::new(),
        };
        let expanded = Contract::Object {
            fields: BTreeMap::from([
                (
                    "name".to_owned(),
                    Contract::String {
                        minimum_length: None,
                        maximum_length: None,
                        minimum_bytes: None,
                        maximum_bytes: None,
                    },
                ),
                (
                    "tag".to_owned(),
                    Contract::String {
                        minimum_length: None,
                        maximum_length: None,
                        minimum_bytes: None,
                        maximum_bytes: None,
                    },
                ),
            ]),
            optional: BTreeSet::from(["tag".to_owned()]),
        };
        assert!(contract_is_subset(&old, &expanded)?);
        assert!(!contract_is_subset(&expanded, &old)?);
        Ok(())
    }

    #[test]
    fn unions_are_conservative_and_directional() -> TestResult {
        let scalar = int(Some(0), Some(5));
        let union = Contract::Union {
            variants: vec![scalar.clone(), Contract::Null],
        };
        assert!(contract_is_subset(&scalar, &union)?);
        assert!(!contract_is_subset(&union, &scalar)?);
        assert!(contract_is_subset(&union, &Contract::Any)?);
        Ok(())
    }

    fn package(
        project_id: ProjectId,
        sequence: u128,
        arguments: &Contract,
        result: &Contract,
        document: &Contract,
        auth_policy: AuthPolicy,
    ) -> Result<ReleasePackage, Box<dyn std::error::Error>> {
        let source = format!("export default () => ({sequence});");
        let arguments_bytes = encode_contract(arguments)?;
        let result_bytes = encode_contract(result)?;
        let schema = DocumentSchemaV1::new(vec![DocumentTableContract {
            id: TableId::from_ulid(ulid::Ulid::from(700)),
            name: "users".to_owned(),
            mode: runku_contracts::TableMode::Queryable,
            document_contract: document.clone(),
        }])?;
        let schema_bytes = encode_document_schema(&schema)?;
        let index_bytes = runku_schema::encode_schema_catalog(&runku_schema::SchemaCatalog::new(
            project_id,
            Vec::new(),
        )?)?;
        let bundle = SafeEsmBundleV1::from_sources([
            source.clone(),
            String::from_utf8(arguments_bytes.clone())?,
            String::from_utf8(result_bytes.clone())?,
            String::from_utf8(schema_bytes.clone())?,
            String::from_utf8(index_bytes.clone())?,
        ])?;
        let artifact = encode_safe_esm_bundle(&bundle)?;
        let manifest = ReleaseManifestV1 {
            release_id: ReleaseId::from_ulid(ulid::Ulid::from(sequence + 100)),
            project_id,
            build_id: BuildId::from_ulid(ulid::Ulid::from(sequence + 200)),
            created_at: TimestampMicros::new(i64::try_from(sequence)?),
            runtime_version: "runku-js-1".parse()?,
            artifact: bundle.descriptor()?,
            function_contract_hash: Sha256Digest::of(b"function-set"),
            schema_contract_hash: Sha256Digest::of(&schema_bytes),
            index_contract_hash: Sha256Digest::of(&index_bytes),
            functions: vec![FunctionManifest {
                id: FunctionId::from_ulid(ulid::Ulid::from(800)),
                name: "queries.user".parse()?,
                function_type: FunctionType::Query,
                visibility: FunctionVisibility::Public,
                auth_policy,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(source.as_bytes()),
                arguments_contract_hash: Sha256Digest::of(&arguments_bytes),
                result_contract_hash: Sha256Digest::of(&result_bytes),
                capabilities: Vec::new(),
            }],
            cron_definitions: Vec::new(),
        };
        Ok(ReleasePackage::load(manifest, &artifact)?)
    }

    #[test]
    fn loads_hybrid_oci_resources_for_compatibility() -> TestResult {
        let project_id = ProjectId::from_ulid(ulid::Ulid::from(600));
        let contract = Contract::Any;
        let contract_bytes = encode_contract(&contract)?;
        let schema = DocumentSchemaV1::new(vec![])?;
        let schema_bytes = encode_document_schema(&schema)?;
        let index_bytes = runku_schema::encode_schema_catalog(&runku_schema::SchemaCatalog::new(
            project_id,
            Vec::new(),
        )?)?;
        let safe_source = "export default () => null;";
        let node_source = "export default async () => null;";
        let resources = NodeEsmBundleV1::from_sources([
            safe_source.to_owned(),
            node_source.to_owned(),
            String::from_utf8(contract_bytes.clone())?,
            String::from_utf8(schema_bytes.clone())?,
            String::from_utf8(index_bytes.clone())?,
        ])?;
        let resources_bytes = encode_node_esm_bundle(&resources)?;
        let descriptor = NodeOciDescriptorV1::new(format!("sha256:{}", "a".repeat(64)))?;
        let descriptor_bytes = encode_node_oci_descriptor(&descriptor)?;
        let artifact = encode_hybrid_oci_artifact(&resources_bytes, &descriptor_bytes)?;
        let manifest = ReleaseManifestV1 {
            release_id: ReleaseId::from_ulid(ulid::Ulid::from(901)),
            project_id,
            build_id: BuildId::from_ulid(ulid::Ulid::from(902)),
            created_at: TimestampMicros::new(1),
            runtime_version: "runku-hybrid".parse()?,
            artifact: hybrid_oci_descriptor(&artifact)?,
            function_contract_hash: Sha256Digest::of(b"hybrid-functions"),
            schema_contract_hash: Sha256Digest::of(&schema_bytes),
            index_contract_hash: Sha256Digest::of(&index_bytes),
            functions: vec![
                FunctionManifest {
                    id: FunctionId::from_ulid(ulid::Ulid::from(903)),
                    name: "actions.node".parse()?,
                    function_type: FunctionType::Action,
                    visibility: FunctionVisibility::Public,
                    auth_policy: AuthPolicy::None,
                    runtime_class: RuntimeClass::FullNode,
                    implementation_hash: Sha256Digest::of(node_source.as_bytes()),
                    arguments_contract_hash: Sha256Digest::of(&contract_bytes),
                    result_contract_hash: Sha256Digest::of(&contract_bytes),
                    capabilities: Vec::new(),
                },
                FunctionManifest {
                    id: FunctionId::from_ulid(ulid::Ulid::from(904)),
                    name: "queries.safe".parse()?,
                    function_type: FunctionType::Query,
                    visibility: FunctionVisibility::Public,
                    auth_policy: AuthPolicy::None,
                    runtime_class: RuntimeClass::SafeV8,
                    implementation_hash: Sha256Digest::of(safe_source.as_bytes()),
                    arguments_contract_hash: Sha256Digest::of(&contract_bytes),
                    result_contract_hash: Sha256Digest::of(&contract_bytes),
                    capabilities: Vec::new(),
                },
            ],
            cron_definitions: Vec::new(),
        };
        let package = ReleasePackage::load(manifest, &artifact)?;
        assert_eq!(package.manifest().runtime_version.as_str(), "runku-hybrid");
        assert!(
            package
                .bundle()
                .resource(Sha256Digest::of(&schema_bytes))
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn release_policy_accepts_input_and_schema_widening_with_result_narrowing() -> TestResult {
        let project = ProjectId::from_ulid(ulid::Ulid::from(600));
        let old = package(
            project,
            1,
            &int(Some(0), Some(10)),
            &int(None, None),
            &int(Some(0), Some(10)),
            AuthPolicy::None,
        )?;
        let candidate = package(
            project,
            2,
            &int(None, Some(20)),
            &int(Some(0), Some(10)),
            &int(None, Some(20)),
            AuthPolicy::None,
        )?;
        let report = CompatibilityEngine::compare(&old, &candidate)?;
        assert!(report.compatible);
        assert!(report.diagnostics.is_empty());
        Ok(())
    }

    #[test]
    fn release_policy_reports_all_ordered_public_and_schema_breaks() -> TestResult {
        let project = ProjectId::from_ulid(ulid::Ulid::from(601));
        let old = package(
            project,
            3,
            &int(None, Some(20)),
            &int(Some(0), Some(10)),
            &int(None, Some(20)),
            AuthPolicy::None,
        )?;
        let candidate = package(
            project,
            4,
            &int(Some(0), Some(10)),
            &int(None, None),
            &int(Some(0), Some(10)),
            AuthPolicy::User,
        )?;
        let report = CompatibilityEngine::compare(&old, &candidate)?;
        assert!(!report.compatible);
        assert_eq!(
            report
                .diagnostics
                .iter()
                .map(|diagnostic| diagnostic.code)
                .collect::<Vec<_>>(),
            vec![
                "FUNCTION_ARGUMENTS_NARROWED",
                "FUNCTION_RESULT_WIDENED",
                "PUBLIC_FUNCTION_METADATA_CHANGED",
                "SCHEMA_DOCUMENT_NARROWED",
            ]
        );
        Ok(())
    }

    #[test]
    fn storage_coexistence_accepts_optional_add_hide_and_rejects_required_add() -> TestResult {
        let project = ProjectId::from_ulid(ulid::Ulid::from(602));
        let string = Contract::String {
            minimum_length: None,
            maximum_length: None,
            minimum_bytes: None,
            maximum_bytes: Some(100),
        };
        let old_document = Contract::Object {
            fields: BTreeMap::from([("name".to_owned(), string.clone())]),
            optional: BTreeSet::new(),
        };
        let optional_document = Contract::Object {
            fields: BTreeMap::from([
                ("avatar".to_owned(), string.clone()),
                ("name".to_owned(), string.clone()),
            ]),
            optional: BTreeSet::from(["avatar".to_owned()]),
        };
        let required_document = Contract::Object {
            fields: BTreeMap::from([
                ("avatar".to_owned(), string),
                ("name".to_owned(), Contract::Any),
            ]),
            optional: BTreeSet::new(),
        };
        let arguments = Contract::Any;
        let result = Contract::Any;
        let old = package(
            project,
            5,
            &arguments,
            &result,
            &old_document,
            AuthPolicy::None,
        )?;
        let optional = package(
            project,
            6,
            &arguments,
            &result,
            &optional_document,
            AuthPolicy::None,
        )?;
        let required = package(
            project,
            7,
            &arguments,
            &result,
            &required_document,
            AuthPolicy::None,
        )?;

        assert!(CompatibilityEngine::coexist(&old, &optional)?.compatible);
        assert!(CompatibilityEngine::coexist(&optional, &old)?.compatible);
        let blocked = CompatibilityEngine::coexist(&old, &required)?;
        assert!(!blocked.compatible);
        assert_eq!(blocked.diagnostics[0].code, "SCHEMA_VIEWS_CANNOT_COEXIST");
        Ok(())
    }
}
