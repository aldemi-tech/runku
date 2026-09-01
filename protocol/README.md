# Protocol vectors

This directory contains versioned, language-independent vectors for Runku's persisted and wire
contracts. Implementations must produce and accept the exact values represented here.

The v1 vectors cover code targets, development administration, index catalogs and ordered keys,
public HTTP envelopes, realtime messages, Release manifests and routing, Safe ESM bundles, source
builds, and stored values.

Changing an existing vector is a compatibility change. Add a new version instead of silently
reinterpreting persisted or transmitted bytes.

## Vector and change contract

Vectors record version/discriminant, canonical input, exact encoding, decoded meaning, and rejection
cases. Rust/TypeScript implementations agree exactly and bound envelope/depth/items/strings/bytes/
IDs/unknown versions.

For change: classify compatibility; preserve existing vectors; define a new version or safe optional
field/default; add round-trip/canonical/rejection/property tests; test required old↔new combinations;
document migration, rollout, rollback limit, and support; update every component crossing the
boundary. Vectors are authority, not customizable examples.
