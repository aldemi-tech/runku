# Changelog

All notable changes are documented in this file.

## 0.1.0 - 2026-09-01

### Added

- Cross-platform `runku` CLI archives for macOS, Linux GNU, and Windows on ARM64 and x86_64.
- Global npm installation through `@runku/cli` and exact-version native packages.
- Public `@runku/client` and `@runku/server` packages coordinated with the CLI version.
- SHA-256 checksums, GitHub artifact attestations, npm integrity/provenance, and resumable release
  publication that rejects divergent bytes.
- Complete public documentation for local use, SDKs, operations, recovery, self-hosting evaluation,
  product evolution, and release maintenance.

### Compatibility

- The `0.x` line has no stable compatibility window. Upgrade CLI and both SDKs together and read
  the exact release notes before changing versions.
- Published CLI startup is validated natively on all six targets. Windows 32-bit x86, Linux musl,
  and other operating-system/architecture combinations are not distributed.

## Unreleased

Use Git commits for unreleased source reproducibility. Entries are grouped as Added, Changed,
Deprecated, Removed, Fixed, Security, and Migration. Behavioral/breaking entries link compatibility
and upgrade guidance and name affected CLI, server, Agent, SDK, protocol, format, runtime, or profile.

Tagged releases will include date, artifact verification reference, support matrix, upgrade/rollback
limits, known issues, and security notices.
