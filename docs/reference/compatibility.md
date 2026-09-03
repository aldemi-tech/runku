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

The source line reports version `0.4.2` and has not established a general stable compatibility
window. Version 0.3.0 is the first supported compact Docker installation floor; 0.4.2 supports a
deliberate forward upgrade from that floor.
Tagged releases coordinate the CLI, both TypeScript SDKs, Linux compact server binaries, and the
compact server image. Agent, distributed deployment, protocol, storage, and runtime support windows
remain separate distribution gates.

## Pre-release matrix

| Boundary | Current rule |
|---|---|
| Published CLI | Same version on GitHub and npm; macOS/Linux GNU/Windows on ARM64/x86_64 |
| Source CLI | Record the Git commit; a modified checkout is not identified by `0.4.2` alone |
| Rust | Exact repository toolchain; workspace MSRV is a separate crate contract |
| Node | 20.18.1+ for current SDK/examples; build/runtime contracts must agree |
| TypeScript packages | `@runku/client`, `@runku/server`, and `@runku/cli` update together |
| HTTP/WebSocket | v1 envelopes; unknown versions rejected |
| Values/index keys | v1 canonical encodings; existing vectors immutable |
| Release/artifact | Version/digest/size/runtime descriptors verified |
| SQLite/PostgreSQL | Same logical contract; physical schema/files are internal |
| Compact server | Linux GNU ARM64/x86_64 binary and multi-platform OCI image; one attached Product Environment, Safe V8 profile |
| Compact deployment | Dedicated Linux host, Compose v2, one active Environment writer, PostgreSQL 16, host TLS proxy, backup/empty restore |
| Distributed deployment | No published separated-role/Agent/Kubernetes support window yet |
| Platform Identity | Management HTTP v1, native OIDC configuration, authenticated Product lifecycle/log stream, schema v1; no mixed-version or downgrade window |

The source line adds optional `runku init --project-id/--environment-id` flags as a compatible CLI
extension. Existing invocations keep generated IDs. Provisioners that use the extension must require
both IDs and must require a 0.4.0-or-newer binary.

Version 0.4.2 includes `runku link` as a compatible CLI extension. It writes a separate
`management-link-v1.json` descriptor after an authenticated exact-scope status check; existing
local Product state and protocol formats are unchanged. New CLIs enforce the descriptor's pinned
Management origin on remote commands. A CLI rollback to 0.4.0 can still read the Product root but
does not enforce that additional local origin pin, so operators should not downgrade linked
workstations during an origin-substitution incident.

Application files are a compatible additive SDK/HTTP surface but introduce new manifest capability
tags and runtime versions `runku-js-2`, `runku-node-2`, and `runku-hybrid-2`. Version 1 manifests
cannot declare `storage:read`/`storage:write`; old binaries fail closed on the new version/tags.
Safe V8 and local Full Node implement version 2. Production OCI/distributed Full Node remains on
version 1 until its mediated Agent channel is versioned, so promotion of a Node storage manifest to
that profile is rejected rather than silently dropping the capability. File metadata schema v1 and
generated S3 key layout `v1/projects/{project}/environments/{environment}/files/{file}` are durable;
future changes require expand/migrate/contract and rollback documentation.

## Change rules

Additive fields require old/new reader tests and safe defaults. Auth, retry, ordering, limits,
pinning, and failure-outcome changes are compatibility changes even without shape changes. Breaking
wire/persisted behavior requires a new version and migration; existing vectors are never rewritten.

Release compatibility includes Function kind/visibility/contracts, schema/index prerequisites,
runtime/Platform Ops, artifacts, Cron, and pending code pins. Channel promotion fails if a candidate
cannot safely share data. Rollback cannot undo migrations; use expand/migrate/contract.

A stable release matrix must publish CLI↔server↔agent↔SDK versions, protocol/persisted readers,
dependency/OS/architecture profiles, upgrade paths, deprecation/security window, and provenance.
