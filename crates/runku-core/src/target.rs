//! Canonical selectors for code executed by an invocation.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{DevRevisionId, ParseResourceIdError, ReleaseId};

const MAX_WORKSPACE_REF_BYTES: usize = 100;
const MAX_CHANNEL_NAME_BYTES: usize = 63;

/// A stable, human-readable reference to a Development Workspace.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkspaceRef(String);

impl WorkspaceRef {
    /// Returns the canonical workspace reference.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkspaceRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for WorkspaceRef {
    type Err = ParseWorkspaceRefError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        validate_hierarchical_name(value, MAX_WORKSPACE_REF_BYTES)
            .map_err(ParseWorkspaceRefError::from_name_error)?;
        Ok(Self(value.to_owned()))
    }
}

impl Serialize for WorkspaceRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for WorkspaceRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

/// Error returned for a non-canonical workspace reference.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParseWorkspaceRefError {
    /// The reference is empty.
    #[error("workspace reference must not be empty")]
    Empty,
    /// The reference exceeds the protocol limit.
    #[error("workspace reference exceeds 100 bytes")]
    TooLong,
    /// The reference contains a character outside lowercase ASCII, digits, `-`, and `/`.
    #[error("workspace reference contains an unsupported character")]
    InvalidCharacter,
    /// The reference has an empty path segment.
    #[error("workspace reference contains an empty path segment")]
    EmptySegment,
}

impl ParseWorkspaceRefError {
    fn from_name_error(error: NameError) -> Self {
        match error {
            NameError::Empty => Self::Empty,
            NameError::TooLong => Self::TooLong,
            NameError::InvalidCharacter => Self::InvalidCharacter,
            NameError::EmptySegment => Self::EmptySegment,
        }
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Empty => "WORKSPACE_REF_EMPTY",
            Self::TooLong => "WORKSPACE_REF_TOO_LONG",
            Self::InvalidCharacter => "WORKSPACE_REF_CHARACTER_INVALID",
            Self::EmptySegment => "WORKSPACE_REF_SEGMENT_EMPTY",
        }
    }

    /// Workspace parse failures are deterministic and not retryable unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

/// A stable Channel name used for intentional moving routing.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChannelName(String);

impl ChannelName {
    /// Returns the canonical channel name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChannelName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ChannelName {
    type Err = ParseChannelNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        validate_flat_name(value, MAX_CHANNEL_NAME_BYTES)
            .map_err(ParseChannelNameError::from_name_error)?;
        Ok(Self(value.to_owned()))
    }
}

impl Serialize for ChannelName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ChannelName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

/// Error returned for a non-canonical Channel name.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParseChannelNameError {
    /// The name is empty.
    #[error("channel name must not be empty")]
    Empty,
    /// The name exceeds the protocol limit.
    #[error("channel name exceeds 63 bytes")]
    TooLong,
    /// The name contains a character outside lowercase ASCII, digits, and `-`.
    #[error("channel name contains an unsupported character")]
    InvalidCharacter,
}

impl ParseChannelNameError {
    fn from_name_error(error: NameError) -> Self {
        match error {
            NameError::Empty => Self::Empty,
            NameError::TooLong => Self::TooLong,
            NameError::InvalidCharacter | NameError::EmptySegment => Self::InvalidCharacter,
        }
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Empty => "CHANNEL_NAME_EMPTY",
            Self::TooLong => "CHANNEL_NAME_TOO_LONG",
            Self::InvalidCharacter => "CHANNEL_NAME_CHARACTER_INVALID",
        }
    }

    /// Channel parse failures are deterministic and not retryable unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

/// Selects the code to execute independently from the Environment that owns data.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CodeTarget {
    /// An immutable durable Release.
    Release(ReleaseId),
    /// An intentionally moving traffic Channel.
    Channel(ChannelName),
    /// The mutable HEAD of a specific Development Workspace.
    Workspace(WorkspaceRef),
}

/// Immutable code identity captured after resolving a moving target.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PinnedCode {
    /// Durable Release identity.
    Release(ReleaseId),
    /// Immutable Development Revision identity.
    DevRevision(DevRevisionId),
}

impl fmt::Display for PinnedCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Release(value) => write!(formatter, "release:{value}"),
            Self::DevRevision(value) => write!(formatter, "dev_revision:{value}"),
        }
    }
}

impl FromStr for PinnedCode {
    type Err = ParsePinnedCodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(id) = value.strip_prefix("release:") {
            return id
                .parse()
                .map(Self::Release)
                .map_err(ParsePinnedCodeError::InvalidRelease);
        }
        if let Some(id) = value.strip_prefix("dev_revision:") {
            return id
                .parse()
                .map(Self::DevRevision)
                .map_err(ParsePinnedCodeError::InvalidDevRevision);
        }
        Err(ParsePinnedCodeError::UnknownKind)
    }
}

/// Error returned for a malformed immutable code identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParsePinnedCodeError {
    /// The discriminator is outside the closed v1 union.
    #[error("pinned code kind is unsupported")]
    UnknownKind,
    /// The Release identifier is malformed.
    #[error("pinned Release is invalid: {0}")]
    InvalidRelease(ParseResourceIdError),
    /// The Development Revision identifier is malformed.
    #[error("pinned Development Revision is invalid: {0}")]
    InvalidDevRevision(ParseResourceIdError),
}

impl ParsePinnedCodeError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnknownKind => "PINNED_CODE_KIND_UNSUPPORTED",
            Self::InvalidRelease(_) => "PINNED_CODE_RELEASE_INVALID",
            Self::InvalidDevRevision(_) => "PINNED_CODE_DEV_REVISION_INVALID",
        }
    }

    /// Parsing cannot succeed when retried unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

impl CodeTarget {
    /// Returns whether resolving this target requires reading a mutable pointer.
    #[must_use]
    pub const fn is_moving(&self) -> bool {
        matches!(self, Self::Channel(_) | Self::Workspace(_))
    }

    /// Returns whether this target represents interactive development.
    #[must_use]
    pub const fn is_workspace(&self) -> bool {
        matches!(self, Self::Workspace(_))
    }
}

impl fmt::Display for CodeTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Release(release) => write!(formatter, "release:{release}"),
            Self::Channel(channel) => write!(formatter, "channel:{channel}"),
            Self::Workspace(workspace) => write!(formatter, "workspace:{workspace}"),
        }
    }
}

impl FromStr for CodeTarget {
    type Err = ParseCodeTargetError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (kind, reference) = value
            .split_once(':')
            .ok_or(ParseCodeTargetError::MissingKindSeparator)?;
        match kind {
            "release" => reference
                .parse()
                .map(Self::Release)
                .map_err(ParseCodeTargetError::InvalidRelease),
            "channel" => reference
                .parse()
                .map(Self::Channel)
                .map_err(ParseCodeTargetError::InvalidChannel),
            "workspace" => reference
                .parse()
                .map(Self::Workspace)
                .map_err(ParseCodeTargetError::InvalidWorkspace),
            _ => Err(ParseCodeTargetError::UnknownKind),
        }
    }
}

impl Serialize for CodeTarget {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for CodeTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

/// Error returned when parsing the external Code Target representation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParseCodeTargetError {
    /// The required `<kind>:<reference>` separator is absent.
    #[error("code target must use '<kind>:<reference>'")]
    MissingKindSeparator,
    /// The target kind is not part of the closed protocol union.
    #[error("code target kind is unsupported")]
    UnknownKind,
    /// The Release identifier is invalid.
    #[error("release target is invalid: {0}")]
    InvalidRelease(ParseResourceIdError),
    /// The Channel name is invalid.
    #[error("channel target is invalid: {0}")]
    InvalidChannel(ParseChannelNameError),
    /// The Workspace reference is invalid.
    #[error("workspace target is invalid: {0}")]
    InvalidWorkspace(ParseWorkspaceRefError),
}

impl ParseCodeTargetError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingKindSeparator => "CODE_TARGET_FORMAT_INVALID",
            Self::UnknownKind => "CODE_TARGET_KIND_UNSUPPORTED",
            Self::InvalidRelease(_) => "CODE_TARGET_RELEASE_INVALID",
            Self::InvalidChannel(_) => "CODE_TARGET_CHANNEL_INVALID",
            Self::InvalidWorkspace(_) => "CODE_TARGET_WORKSPACE_INVALID",
        }
    }

    /// Code Target parse failures are deterministic and not retryable unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NameError {
    Empty,
    TooLong,
    InvalidCharacter,
    EmptySegment,
}

fn validate_flat_name(value: &str, max_bytes: usize) -> Result<(), NameError> {
    if value.is_empty() {
        return Err(NameError::Empty);
    }
    if value.len() > max_bytes {
        return Err(NameError::TooLong);
    }
    if !value.bytes().all(|character| {
        character.is_ascii_lowercase() || character.is_ascii_digit() || character == b'-'
    }) {
        return Err(NameError::InvalidCharacter);
    }
    Ok(())
}

fn validate_hierarchical_name(value: &str, max_bytes: usize) -> Result<(), NameError> {
    if value.is_empty() {
        return Err(NameError::Empty);
    }
    if value.len() > max_bytes {
        return Err(NameError::TooLong);
    }
    if value.starts_with('/') || value.ends_with('/') || value.contains("//") {
        return Err(NameError::EmptySegment);
    }
    if !value.bytes().all(|character| {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || character == b'-'
            || character == b'/'
    }) {
        return Err(NameError::InvalidCharacter);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use proptest::prelude::*;

    use super::*;

    const RELEASE: &str = "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV";

    #[test]
    fn every_target_round_trips() -> Result<(), Box<dyn Error>> {
        for wire in [
            format!("release:{RELEASE}"),
            "channel:stable".to_owned(),
            "workspace:dev/manuel/bug-241".to_owned(),
        ] {
            let parsed: CodeTarget = wire.parse()?;
            assert_eq!(parsed.to_string(), wire);
            let encoded = serde_json::to_string(&parsed)?;
            let decoded: CodeTarget = serde_json::from_str(&encoded)?;
            assert_eq!(decoded, parsed);
        }
        Ok(())
    }

    #[test]
    fn latest_is_not_a_target() {
        assert_eq!(
            "latest".parse::<CodeTarget>(),
            Err(ParseCodeTargetError::MissingKindSeparator)
        );
        assert_eq!(
            "release:latest".parse::<CodeTarget>(),
            Err(ParseCodeTargetError::InvalidRelease(
                ParseResourceIdError::UnexpectedPrefix { expected: "rel_" }
            ))
        );
    }

    #[test]
    fn workspace_grammar_rejects_ambiguous_or_unsafe_paths() {
        for value in ["", "/dev", "dev/", "dev//ana", "dev/../prod", "Dev/ana"] {
            assert!(value.parse::<WorkspaceRef>().is_err(), "accepted {value}");
        }
    }

    #[test]
    fn workspace_limits_map_to_stable_errors() {
        let too_long = "a".repeat(MAX_WORKSPACE_REF_BYTES + 1);
        assert_eq!(
            "".parse::<WorkspaceRef>(),
            Err(ParseWorkspaceRefError::Empty)
        );
        assert_eq!(
            too_long.parse::<WorkspaceRef>(),
            Err(ParseWorkspaceRefError::TooLong)
        );
        assert_eq!(
            "dev//ana".parse::<WorkspaceRef>(),
            Err(ParseWorkspaceRefError::EmptySegment)
        );
        assert_eq!(
            "dev_ana".parse::<WorkspaceRef>(),
            Err(ParseWorkspaceRefError::InvalidCharacter)
        );
        assert_eq!(
            ParseWorkspaceRefError::TooLong.code(),
            "WORKSPACE_REF_TOO_LONG"
        );
        assert!(!ParseWorkspaceRefError::TooLong.retryable());
    }

    #[test]
    fn channel_is_flat_and_canonical() {
        for value in ["", "Stable", "preview/main", "with_underscore"] {
            assert!(value.parse::<ChannelName>().is_err(), "accepted {value}");
        }
        assert!("canary-10".parse::<ChannelName>().is_ok());
    }

    #[test]
    fn channel_limits_map_to_stable_errors() {
        let too_long = "a".repeat(MAX_CHANNEL_NAME_BYTES + 1);
        assert_eq!("".parse::<ChannelName>(), Err(ParseChannelNameError::Empty));
        assert_eq!(
            too_long.parse::<ChannelName>(),
            Err(ParseChannelNameError::TooLong)
        );
        assert_eq!(
            "stable/main".parse::<ChannelName>(),
            Err(ParseChannelNameError::InvalidCharacter)
        );
        assert_eq!(
            ParseChannelNameError::InvalidCharacter.code(),
            "CHANNEL_NAME_CHARACTER_INVALID"
        );
        assert!(!ParseChannelNameError::InvalidCharacter.retryable());
    }

    #[test]
    fn target_parse_failures_are_distinct_and_stable() {
        let cases = [
            (
                "latest",
                ParseCodeTargetError::MissingKindSeparator,
                "CODE_TARGET_FORMAT_INVALID",
            ),
            (
                "unknown:value",
                ParseCodeTargetError::UnknownKind,
                "CODE_TARGET_KIND_UNSUPPORTED",
            ),
            (
                "channel:Stable",
                ParseCodeTargetError::InvalidChannel(ParseChannelNameError::InvalidCharacter),
                "CODE_TARGET_CHANNEL_INVALID",
            ),
            (
                "workspace:",
                ParseCodeTargetError::InvalidWorkspace(ParseWorkspaceRefError::Empty),
                "CODE_TARGET_WORKSPACE_INVALID",
            ),
        ];

        for (wire, expected, code) in cases {
            let result = wire.parse::<CodeTarget>();
            assert_eq!(result, Err(expected));
            assert_eq!(expected.code(), code);
            assert!(!expected.retryable());
        }
    }

    #[test]
    fn pinned_code_round_trips_without_collapsing_dev_revision() -> Result<(), Box<dyn Error>> {
        for pinned in [
            PinnedCode::Release(ReleaseId::from_ulid(ulid::Ulid::from(40_u128))),
            PinnedCode::DevRevision(DevRevisionId::from_ulid(ulid::Ulid::from(41_u128))),
        ] {
            assert_eq!(pinned.to_string().parse::<PinnedCode>()?, pinned);
        }
        assert_eq!(
            "workspace:dev/me".parse::<PinnedCode>(),
            Err(ParsePinnedCodeError::UnknownKind)
        );
        assert!(!ParsePinnedCodeError::UnknownKind.retryable());
        Ok(())
    }

    proptest! {
        #[test]
        fn successful_arbitrary_parse_is_canonical(value in any::<String>()) {
            if let Ok(target) = value.parse::<CodeTarget>() {
                prop_assert_eq!(target.to_string(), value);
            }
        }
    }
}
