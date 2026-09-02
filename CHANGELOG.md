# Changelog

All notable changes are documented in this file.

## 0.3.0 - 2026-09-02

### Added

- Embedded Operational Log history with SQLite hot storage, immutable filesystem or S3-compatible
  Parquet segments, strict manifests, DuckDB historical query, archive-frontier retention, and
  authenticated one-connection live streaming.
- Optional HA log admission and archival using replicated NATS JetStream and the same
  `runku-server logs-worker` artifact, including PubAck, redelivery, create-or-verify replay, and
  explicit NATS/MinIO failure-path acceptance.
- A release-packaged Docker standalone profile for one Safe V8 Product Environment, with
  digest-pinned images, mounted secret files, non-root/read-only execution, bounded resources,
  liveness/readiness probes, setup, backup, offline verification, empty-install restore, upgrade,
  and guarded uninstall operations.
- An optional Compose overlay for browser origins/Product JWT verification and optional HA log
  overlays for AWS S3 or an HTTPS S3-compatible endpoint.
- Remote archive inspection and dry-run/confirmed hot-log pruning through the same scoped operator
  session used for release lifecycle operations.

### Changed

- `runku-server` accepts `RUNKU_DATABASE_URL_FILE` and
  `RUNKU_PLATFORM_IDENTITY_PEPPER_FILE` as mutually exclusive alternatives to direct secret
  variables. Mounted files must be absolute, regular, non-symlinked, bounded, and contain one
  canonical line.
- The compact server accepts exact Product browser origins and a Product-root-relative JWT
  descriptor without weakening its loopback-only Product listener.
- Tagged releases now include `runku-selfhost-vX.Y.Z.tar.gz` beside the CLI/server assets and
  validate that its image example matches the coordinated release version.

### Security

- Log manifests, paths, queries, NATS subjects, Management authorization, and retention preserve
  exact Project/Environment scope; changed archive bytes, gaps, overlaps, stale grants, unsafe NATS
  endpoints, and ambiguous secret sources fail closed.
- The installation helper never overwrites a complete secret set, rejects partial secret state,
  excludes external peppers from backup payloads, verifies their fingerprint before restore, and
  requires exact confirmations for restore and data deletion.

### Compatibility and rollback

- Existing Product, Management HTTP v1, Platform Identity schema v1, Release, and Operational Log
  hot-row formats remain readable. Parquet archive/manifests begin at version 1 and reject unknown
  versions.
- Version 0.3.0 establishes the first packaged compact-installation upgrade floor. Source-managed
  0.2.0 deployments must take a verified backup and adopt the 0.3.0 package deliberately; no
  automated mixed-version or database downgrade window is claimed.
- Channel rollback continues to change future code routing only. It cannot reverse a database
  migration, restored data, completed Action effect, or archive retention.

## 0.2.0 - 2026-09-01

### Added

- PostgreSQL-backed Platform Identity with initial-owner bootstrap, scoped invitations, rotating
  operator sessions, external OIDC, authenticated remote lifecycle, and streaming Product logs.
- Downloadable `runku-server` binaries for Linux GNU on ARM64 and x86_64.
- A non-root, shell-free `ghcr.io/aldemi-tech/runku-server` multi-platform image with SBOM and
  provenance, containing the matching CLI for controlled init/diagnostic jobs.
- Explicit offline recovery for a lost pre-enrollment bootstrap file; replacement revokes prior
  pending material atomically and closes permanently after the first operator exists.
- Full PostgreSQL + Keycloak reference campaigns for identity and browser-driven Product lifecycle.
- Optional server-selected RFC 8707 resource indication in browser authorization and token
  exchange for providers that mint audience-bound JWT access tokens only for an explicit resource.

### Changed

- `runku login` now performs authentication-method and Management-origin discovery. Its default
  public entry point is `https://api.runku.app`; self-hosted installations continue to use
  `--url` and saved profiles.
- Hosted CI remains compile/package-only; Docker/browser acceptance remains an explicit maintainer
  gate so ordinary pull requests do not start the complete server lifecycle.

### Security

- Browser login now confirms success only after Runku verifies the external token, commits the
  operator session, and persists the protected CLI profile.
- Callback Host/path/method/state/issuer and duplicate parameters fail closed; redirects, ambient
  proxies, unsafe remote HTTP, shell-based Windows browser launch, and malformed stored sessions
  are rejected.
- JWT verification uses the `aws_lc_rs` backend and removes the unfixed RustCrypto `rsa` dependency.

### Compatibility and rollback

- Existing v1 Product protocols and persisted formats are unchanged.
- CLI session files are upgraded from schema v1 to v2 after login/refresh so authentication and
  Management origins can differ; v1 remains readable.
- Rolling the CLI back to 0.1.0 loses support for the v2 profile and new login flow. Preserve the
  profile and log in again with an explicitly supported version rather than editing it.

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
