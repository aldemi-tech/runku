//! Canonical immutable OCI image and egress descriptor for the Full Node runtime.

use std::{net::IpAddr, str::FromStr};

use crate::{ArtifactDescriptor, ArtifactFormat, ReleaseError, Sha256Digest};

/// Full Node OCI descriptor codec version.
pub const NODE_OCI_DESCRIPTOR_FORMAT_VERSION: u8 = 1;
/// Maximum immutable OCI image reference length.
pub const NODE_OCI_IMAGE_REFERENCE_MAX_BYTES: usize = 512;
/// Maximum application allow/deny rules in either TCP policy list.
pub const FULL_NODE_TCP_RULES_MAX: usize = 64;
/// Network ranges that no application Release can make reachable.
pub const FULL_NODE_HARD_DENIED_CIDRS: &[&str] = &[
    "0.0.0.0/8",
    "100.100.100.200/32",
    "127.0.0.0/8",
    "168.63.129.16/32",
    "169.254.0.0/16",
    "224.0.0.0/4",
    "240.0.0.0/4",
    "::/128",
    "::1/128",
    "fe80::/10",
    "ff00::/8",
];
/// Private ranges denied by `public` mode unless an Environment selects restricted private egress.
pub const FULL_NODE_PUBLIC_DENIED_CIDRS: &[&str] = &[
    "10.0.0.0/8",
    "100.64.0.0/10",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "fc00::/7",
];
const MAGIC: &[u8; 5] = b"NOCI\x01";
const MAX_DESTINATION_BYTES: usize = 253;
const MAX_PORTS_PER_RULE: usize = 32;

/// Application-requested Full Node network posture.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FullNodeNetworkMode {
    /// No network namespace egress except platform-provided DNS plumbing when needed.
    None,
    /// TCP to public addresses, still subject to mandatory platform/Environment denials.
    Public,
    /// TCP only to explicit allow rules after every policy layer is intersected.
    Restricted,
}

/// One canonical TCP hostname/IP/CIDR and port rule.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FullNodeTcpRule {
    destination: String,
    ports: Vec<u16>,
}

impl FullNodeTcpRule {
    /// Creates a canonical TCP rule.
    ///
    /// # Errors
    ///
    /// Rejects noncanonical host/IP/CIDR text, port zero, duplicates, and excessive ports.
    pub fn new(destination: impl Into<String>, mut ports: Vec<u16>) -> Result<Self, ReleaseError> {
        let destination = destination.into();
        validate_destination(&destination)?;
        ports.sort_unstable();
        if ports.is_empty()
            || ports.len() > MAX_PORTS_PER_RULE
            || ports[0] == 0
            || ports.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(ReleaseError::InvalidArtifact);
        }
        Ok(Self { destination, ports })
    }

    /// Exact canonical hostname, IP, or CIDR.
    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Strictly increasing allowed/denied TCP ports.
    #[must_use]
    pub fn ports(&self) -> &[u16] {
        &self.ports
    }
}

/// Immutable application-requested TCP egress policy embedded in the OCI descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullNodeEgressPolicy {
    mode: FullNodeNetworkMode,
    allow: Vec<FullNodeTcpRule>,
    deny: Vec<FullNodeTcpRule>,
}

impl FullNodeEgressPolicy {
    /// Returns the fail-closed default policy.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            mode: FullNodeNetworkMode::None,
            allow: Vec::new(),
            deny: Vec::new(),
        }
    }

    /// Builds one canonical application policy.
    ///
    /// `none` accepts no rules, `public` accepts deny rules only, and `restricted` requires at
    /// least one allow rule. UDP is intentionally not representable in v1.
    ///
    /// # Errors
    ///
    /// Rejects duplicate/excessive rules and mode/list contradictions.
    pub fn new(
        mode: FullNodeNetworkMode,
        mut allow: Vec<FullNodeTcpRule>,
        mut deny: Vec<FullNodeTcpRule>,
    ) -> Result<Self, ReleaseError> {
        allow.sort();
        deny.sort();
        if allow.len() > FULL_NODE_TCP_RULES_MAX
            || deny.len() > FULL_NODE_TCP_RULES_MAX
            || allow.windows(2).any(|pair| pair[0] == pair[1])
            || deny.windows(2).any(|pair| pair[0] == pair[1])
            || match mode {
                FullNodeNetworkMode::None => !allow.is_empty() || !deny.is_empty(),
                FullNodeNetworkMode::Public => !allow.is_empty(),
                FullNodeNetworkMode::Restricted => allow.is_empty(),
            }
        {
            return Err(ReleaseError::InvalidArtifact);
        }
        if allow
            .iter()
            .any(|rule| destination_overlaps_hard_denial(rule.destination()))
        {
            return Err(ReleaseError::InvalidArtifact);
        }
        Ok(Self { mode, allow, deny })
    }

    /// Requested network mode.
    #[must_use]
    pub const fn mode(&self) -> FullNodeNetworkMode {
        self.mode
    }

    /// Strictly ordered application allow rules.
    #[must_use]
    pub fn allow(&self) -> &[FullNodeTcpRule] {
        &self.allow
    }

    /// Strictly ordered application deny rules; deny always wins.
    #[must_use]
    pub fn deny(&self) -> &[FullNodeTcpRule] {
        &self.deny
    }
}

/// A content-addressed OCI image reference consumed by a Full Node runner.
///
/// Registry-backed deployments use `registry/repository@sha256:<digest>`. The exact
/// `sha256:<digest>` form is also accepted for a local Docker image ID in conformance tests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeOciDescriptorV1 {
    image_reference: String,
    egress: FullNodeEgressPolicy,
}

impl NodeOciDescriptorV1 {
    /// Creates a descriptor after enforcing an immutable SHA-256 image reference.
    ///
    /// # Errors
    ///
    /// Rejects mutable tags, uppercase/non-hex digests, whitespace, and oversized references.
    pub fn new(image_reference: impl Into<String>) -> Result<Self, ReleaseError> {
        let image_reference = image_reference.into();
        validate_image_reference(&image_reference)?;
        Ok(Self {
            image_reference,
            egress: FullNodeEgressPolicy::none(),
        })
    }

    /// Returns the exact immutable image reference.
    #[must_use]
    pub fn image_reference(&self) -> &str {
        &self.image_reference
    }

    /// Returns the immutable SHA-256 image identity without registry/repository text.
    ///
    /// # Errors
    ///
    /// Rejects an internally inconsistent image reference.
    pub fn image_digest(&self) -> Result<Sha256Digest, ReleaseError> {
        self.image_reference
            .rsplit_once("@sha256:")
            .map_or_else(
                || self.image_reference.strip_prefix("sha256:"),
                |(_, digest)| Some(digest),
            )
            .ok_or(ReleaseError::InvalidArtifact)?
            .parse()
            .map_err(|_| ReleaseError::InvalidArtifact)
    }

    /// Sets the immutable application-requested egress policy.
    #[must_use]
    pub fn with_egress_policy(mut self, policy: FullNodeEgressPolicy) -> Self {
        self.egress = policy;
        self
    }

    /// Returns the immutable application-requested egress policy.
    #[must_use]
    pub const fn egress_policy(&self) -> &FullNodeEgressPolicy {
        &self.egress
    }

    /// Returns the artifact descriptor for the canonical encoded bytes.
    ///
    /// # Errors
    ///
    /// Propagates canonical encoding validation failures.
    pub fn descriptor(&self) -> Result<ArtifactDescriptor, ReleaseError> {
        let bytes = encode_node_oci_descriptor(self)?;
        Ok(ArtifactDescriptor {
            format: ArtifactFormat::NodeOciDescriptorV1,
            digest: Sha256Digest::of(&bytes),
            size_bytes: u64::try_from(bytes.len()).map_err(|_| ReleaseError::Internal)?,
        })
    }
}

/// Encodes one canonical Full Node OCI descriptor.
///
/// # Errors
///
/// Rejects a descriptor whose reference no longer satisfies the v1 contract.
pub fn encode_node_oci_descriptor(
    descriptor: &NodeOciDescriptorV1,
) -> Result<Vec<u8>, ReleaseError> {
    validate_image_reference(descriptor.image_reference())?;
    let length =
        u16::try_from(descriptor.image_reference.len()).map_err(|_| ReleaseError::LimitExceeded)?;
    let mut bytes = Vec::with_capacity(MAGIC.len() + 8 + descriptor.image_reference.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(descriptor.image_reference.as_bytes());
    bytes.push(network_mode_tag(descriptor.egress.mode));
    encode_rules(&mut bytes, &descriptor.egress.allow)?;
    encode_rules(&mut bytes, &descriptor.egress.deny)?;
    Ok(bytes)
}

/// Decodes one strict canonical Full Node OCI descriptor.
///
/// # Errors
///
/// Rejects unknown versions, truncation, trailing bytes, invalid UTF-8, and mutable references.
pub fn decode_node_oci_descriptor(bytes: &[u8]) -> Result<NodeOciDescriptorV1, ReleaseError> {
    if bytes.len() < MAGIC.len() + 2 || &bytes[..MAGIC.len()] != MAGIC {
        return Err(ReleaseError::InvalidArtifact);
    }
    let length = usize::from(u16::from_be_bytes(
        bytes[MAGIC.len()..MAGIC.len() + 2]
            .try_into()
            .map_err(|_| ReleaseError::InvalidArtifact)?,
    ));
    let start = MAGIC.len() + 2;
    let end = start
        .checked_add(length)
        .ok_or(ReleaseError::InvalidArtifact)?;
    if end >= bytes.len() {
        return Err(ReleaseError::InvalidArtifact);
    }
    let image_reference =
        std::str::from_utf8(&bytes[start..end]).map_err(|_| ReleaseError::InvalidArtifact)?;
    let mut cursor = Cursor::new(&bytes[end..]);
    let mode = decode_network_mode(cursor.byte()?)?;
    let allow = decode_rules(&mut cursor)?;
    let deny = decode_rules(&mut cursor)?;
    if !cursor.is_empty() {
        return Err(ReleaseError::InvalidArtifact);
    }
    let egress = FullNodeEgressPolicy::new(mode, allow, deny)?;
    let descriptor = NodeOciDescriptorV1::new(image_reference)?.with_egress_policy(egress);
    if encode_node_oci_descriptor(&descriptor)? != bytes {
        return Err(ReleaseError::InvalidArtifact);
    }
    Ok(descriptor)
}

fn encode_rules(output: &mut Vec<u8>, rules: &[FullNodeTcpRule]) -> Result<(), ReleaseError> {
    output.extend_from_slice(
        &u16::try_from(rules.len())
            .map_err(|_| ReleaseError::LimitExceeded)?
            .to_be_bytes(),
    );
    for rule in rules {
        output.extend_from_slice(
            &u16::try_from(rule.destination.len())
                .map_err(|_| ReleaseError::LimitExceeded)?
                .to_be_bytes(),
        );
        output.extend_from_slice(rule.destination.as_bytes());
        output.extend_from_slice(
            &u16::try_from(rule.ports.len())
                .map_err(|_| ReleaseError::LimitExceeded)?
                .to_be_bytes(),
        );
        for port in &rule.ports {
            output.extend_from_slice(&port.to_be_bytes());
        }
    }
    Ok(())
}

fn decode_rules(cursor: &mut Cursor<'_>) -> Result<Vec<FullNodeTcpRule>, ReleaseError> {
    let count = usize::from(cursor.u16()?);
    if count > FULL_NODE_TCP_RULES_MAX {
        return Err(ReleaseError::LimitExceeded);
    }
    let mut rules = Vec::with_capacity(count);
    for _ in 0..count {
        let destination = cursor.text(MAX_DESTINATION_BYTES)?.to_owned();
        let port_count = usize::from(cursor.u16()?);
        if port_count == 0 || port_count > MAX_PORTS_PER_RULE {
            return Err(ReleaseError::InvalidArtifact);
        }
        let ports = (0..port_count)
            .map(|_| cursor.u16())
            .collect::<Result<Vec<_>, _>>()?;
        rules.push(FullNodeTcpRule::new(destination, ports)?);
    }
    Ok(rules)
}

fn validate_destination(value: &str) -> Result<(), ReleaseError> {
    if value.is_empty()
        || value.len() > MAX_DESTINATION_BYTES
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(ReleaseError::InvalidArtifact);
    }
    if let Some((address, prefix)) = value.rsplit_once('/') {
        let address = IpAddr::from_str(address).map_err(|_| ReleaseError::InvalidArtifact)?;
        let prefix = prefix
            .parse::<u8>()
            .map_err(|_| ReleaseError::InvalidArtifact)?;
        let maximum = if address.is_ipv4() { 32 } else { 128 };
        if prefix > maximum || value != format!("{address}/{prefix}") {
            return Err(ReleaseError::InvalidArtifact);
        }
        return Ok(());
    }
    if let Ok(address) = IpAddr::from_str(value) {
        return if value == address.to_string() {
            Ok(())
        } else {
            Err(ReleaseError::InvalidArtifact)
        };
    }
    if value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
        || value.len() > 253
        || value.starts_with('.')
        || value.ends_with('.')
        || value.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
    {
        return Err(ReleaseError::InvalidArtifact);
    }
    Ok(())
}

fn destination_overlaps_hard_denial(value: &str) -> bool {
    if matches!(value, "localhost" | "metadata.google.internal") || value.ends_with(".localhost") {
        return true;
    }
    let Some((address, prefix)) = parse_ip_network(value) else {
        return false;
    };
    FULL_NODE_HARD_DENIED_CIDRS.iter().any(|denied| {
        parse_ip_network(denied).is_some_and(|(denied_address, denied_prefix)| {
            networks_overlap(address, prefix, denied_address, denied_prefix)
        })
    })
}

fn parse_ip_network(value: &str) -> Option<(IpAddr, u8)> {
    if let Some((address, prefix)) = value.rsplit_once('/') {
        Some((address.parse().ok()?, prefix.parse().ok()?))
    } else {
        let address = value.parse::<IpAddr>().ok()?;
        Some((address, if address.is_ipv4() { 32 } else { 128 }))
    }
}

fn networks_overlap(left: IpAddr, left_prefix: u8, right: IpAddr, right_prefix: u8) -> bool {
    match (left, right) {
        (IpAddr::V4(left), IpAddr::V4(right)) => prefix_equal(
            u128::from(u32::from(left)),
            left_prefix,
            u128::from(u32::from(right)),
            right_prefix,
            32,
        ),
        (IpAddr::V6(left), IpAddr::V6(right)) => prefix_equal(
            u128::from(left),
            left_prefix,
            u128::from(right),
            right_prefix,
            128,
        ),
        _ => false,
    }
}

fn prefix_equal(left: u128, left_prefix: u8, right: u128, right_prefix: u8, width: u8) -> bool {
    let compared = left_prefix.min(right_prefix);
    compared == 0 || left >> (width - compared) == right >> (width - compared)
}

const fn network_mode_tag(mode: FullNodeNetworkMode) -> u8 {
    match mode {
        FullNodeNetworkMode::None => 1,
        FullNodeNetworkMode::Public => 2,
        FullNodeNetworkMode::Restricted => 3,
    }
}

const fn decode_network_mode(tag: u8) -> Result<FullNodeNetworkMode, ReleaseError> {
    match tag {
        1 => Ok(FullNodeNetworkMode::None),
        2 => Ok(FullNodeNetworkMode::Public),
        3 => Ok(FullNodeNetworkMode::Restricted),
        _ => Err(ReleaseError::Unsupported),
    }
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ReleaseError> {
        if self.remaining.len() < count {
            return Err(ReleaseError::InvalidArtifact);
        }
        let (value, remaining) = self.remaining.split_at(count);
        self.remaining = remaining;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, ReleaseError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ReleaseError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| ReleaseError::InvalidArtifact)?,
        ))
    }

    fn text(&mut self, maximum: usize) -> Result<&'a str, ReleaseError> {
        let length = usize::from(self.u16()?);
        if length > maximum {
            return Err(ReleaseError::LimitExceeded);
        }
        std::str::from_utf8(self.take(length)?).map_err(|_| ReleaseError::InvalidArtifact)
    }
}

fn validate_image_reference(value: &str) -> Result<(), ReleaseError> {
    if value.is_empty()
        || value.len() > NODE_OCI_IMAGE_REFERENCE_MAX_BYTES
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(ReleaseError::InvalidArtifact);
    }
    let digest = if let Some(digest) = value.strip_prefix("sha256:") {
        digest
    } else {
        let (repository, digest) = value
            .rsplit_once("@sha256:")
            .ok_or(ReleaseError::InvalidArtifact)?;
        if repository.is_empty()
            || !repository.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'/' | b'_' | b'-' | b':')
            })
        {
            return Err(ReleaseError::InvalidArtifact);
        }
        digest
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReleaseError::InvalidArtifact);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_round_trips_registry_and_local_references() -> Result<(), ReleaseError> {
        for reference in [
            format!("sha256:{}", "a".repeat(64)),
            format!("registry.example/runku/release@sha256:{}", "b".repeat(64)),
        ] {
            let descriptor = NodeOciDescriptorV1::new(reference)?;
            let bytes = encode_node_oci_descriptor(&descriptor)?;
            assert_eq!(decode_node_oci_descriptor(&bytes)?, descriptor);
            assert_eq!(descriptor.descriptor()?.size_bytes, bytes.len() as u64);
        }
        Ok(())
    }

    #[test]
    fn descriptor_rejects_mutable_or_noncanonical_references() {
        for reference in [
            "node:22".to_owned(),
            format!("sha256:{}", "A".repeat(64)),
            format!("repo@sha256:{} ", "a".repeat(64)),
            format!("repo with space@sha256:{}", "a".repeat(64)),
        ] {
            assert_eq!(
                NodeOciDescriptorV1::new(reference),
                Err(ReleaseError::InvalidArtifact)
            );
        }
    }

    #[test]
    fn egress_policy_is_canonical_bounded_and_udp_is_not_representable() -> Result<(), ReleaseError>
    {
        let postgres = FullNodeTcpRule::new("db.example.com", vec![5432])?;
        let denied = FullNodeTcpRule::new("203.0.113.0/24", vec![5432, 443])?;
        let policy = FullNodeEgressPolicy::new(
            FullNodeNetworkMode::Restricted,
            vec![postgres.clone()],
            vec![denied.clone()],
        )?;
        let descriptor = NodeOciDescriptorV1::new(format!("sha256:{}", "c".repeat(64)))?
            .with_egress_policy(policy.clone());
        let bytes = encode_node_oci_descriptor(&descriptor)?;
        assert_eq!(decode_node_oci_descriptor(&bytes)?, descriptor);
        assert_eq!(descriptor.egress_policy(), &policy);
        assert_eq!(policy.allow(), &[postgres]);
        assert_eq!(policy.deny(), &[denied]);
        assert!(FULL_NODE_HARD_DENIED_CIDRS.contains(&"127.0.0.0/8"));
        assert!(FULL_NODE_PUBLIC_DENIED_CIDRS.contains(&"10.0.0.0/8"));
        Ok(())
    }

    #[test]
    fn egress_policy_rejects_mode_confusion_and_noncanonical_destinations()
    -> Result<(), ReleaseError> {
        let rule = FullNodeTcpRule::new("db.example.com", vec![5432])?;
        assert_eq!(
            FullNodeEgressPolicy::new(FullNodeNetworkMode::None, vec![rule.clone()], vec![]),
            Err(ReleaseError::InvalidArtifact)
        );
        assert_eq!(
            FullNodeEgressPolicy::new(FullNodeNetworkMode::Restricted, vec![], vec![]),
            Err(ReleaseError::InvalidArtifact)
        );
        for destination in [
            "DB.example.com",
            "example.com.",
            "127.000.000.001",
            "10.0.0.0/33",
        ] {
            assert_eq!(
                FullNodeTcpRule::new(destination, vec![5432]),
                Err(ReleaseError::InvalidArtifact)
            );
        }
        assert_eq!(
            FullNodeTcpRule::new("db.example.com", vec![0]),
            Err(ReleaseError::InvalidArtifact)
        );
        for destination in ["localhost", "127.0.0.1", "127.1.0.0/16", "::1"] {
            let rule = FullNodeTcpRule::new(destination, vec![443])?;
            assert_eq!(
                FullNodeEgressPolicy::new(FullNodeNetworkMode::Restricted, vec![rule], vec![]),
                Err(ReleaseError::InvalidArtifact)
            );
        }
        Ok(())
    }
}
