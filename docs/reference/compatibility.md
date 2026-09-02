# Compatibility

Runku versions contracts at every boundary that can outlive one process:

- public HTTP and WebSocket protocols;
- canonical values, document IDs, and index keys;
- Release manifests and artifacts;
- runtime and Platform Ops versions;
- Workspace and development administration protocols;
- Platform Identity Management API, operator credential formats, and schema checksum;
- generated TypeScript API contracts.

Unknown versions fail closed. A client-selected Release is served only while its contract and
runtime remain supported. Channel routing cannot silently replace an explicit incompatible Release.

The source line reports version `0.2.0` and has not established a stable compatibility window.
Tagged releases coordinate the CLI, both TypeScript SDKs, Linux compact server binaries, and the
compact server image. Agent, distributed deployment, protocol, storage, and runtime support windows
remain separate distribution gates.

## Pre-release matrix

| Boundary | Current rule |
|---|---|
| Published CLI | Same version on GitHub and npm; macOS/Linux GNU/Windows on ARM64/x86_64 |
| Source CLI | Record the Git commit; a modified checkout is not identified by `0.2.0` alone |
| Rust | Exact repository toolchain; workspace MSRV is a separate crate contract |
| Node | 20.18.1+ for current SDK/examples; build/runtime contracts must agree |
| TypeScript packages | `@runku/client`, `@runku/server`, and `@runku/cli` update together |
| HTTP/WebSocket | v1 envelopes; unknown versions rejected |
| Values/index keys | v1 canonical encodings; existing vectors immutable |
| Release/artifact | Version/digest/size/runtime descriptors verified |
| SQLite/PostgreSQL | Same logical contract; physical schema/files are internal |
| Compact server | Linux GNU ARM64/x86_64 binary and multi-platform OCI image; one attached Product Environment, Safe V8 profile |
| Deployment | No published distributed role/Agent support window yet |
| Platform Identity | Management HTTP v1, native OIDC configuration, authenticated Product lifecycle/log stream, schema v1; no mixed-version or downgrade window |

## Change rules

Additive fields require old/new reader tests and safe defaults. Auth, retry, ordering, limits,
pinning, and failure-outcome changes are compatibility changes even without shape changes. Breaking
wire/persisted behavior requires a new version and migration; existing vectors are never rewritten.

Release compatibility includes Function kind/visibility/contracts, schema/index prerequisites,
runtime/Platform Ops, artifacts, Cron, and pending code pins. Channel promotion fails if a candidate
cannot safely share data. Rollback cannot undo migrations; use expand/migrate/contract.

A stable release matrix must publish CLI↔server↔agent↔SDK versions, protocol/persisted readers,
dependency/OS/architecture profiles, upgrade paths, deprecation/security window, and provenance.
