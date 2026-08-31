# Protocol vectors

This directory contains versioned, language-independent vectors for Runku's persisted and wire
contracts. Implementations must produce and accept the exact values represented here.

The v1 vectors cover code targets, development administration, index catalogs and ordered keys,
public HTTP envelopes, realtime messages, Release manifests and routing, Safe ESM bundles, source
builds, and stored values.

Changing an existing vector is a compatibility change. Add a new version instead of silently
reinterpreting persisted or transmitted bytes.
