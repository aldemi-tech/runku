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

The source line reports version `0.4.5` and has not established a general stable compatibility
window. Version 0.3.0 is the first supported compact Docker installation floor; 0.4.5 supports a
deliberate forward upgrade from that floor.
Tagged releases coordinate the CLI, both TypeScript SDKs, Linux compact server binaries, and the
compact server image. Agent, distributed deployment, protocol, storage, and runtime support windows
remain separate distribution gates.

Version 0.4.5 adds only additive auth response fields and endpoints. A 0.4.5 CLI can still link
non-interactively to an older server with explicit IDs; parameterless interactive linking requires
the new resource catalog. Managed enrollment is disabled unless both gateway and server configure
their separate shared secret, so upgrading invitation-only Self-Hosted preserves its policy.

The post-0.4.5 source line adds versioned Function/schema catalog and logical Data Admin Management
endpoints without changing existing endpoint or persisted-row meanings. It also adds explicit
`data:read` and `data:write` Platform capabilities. Existing grants are intentionally not backfilled
and receive no new document authority; administrators must opt in by issuing/reconciling an updated
grant. Newly expanded role presets may include Data Admin authority, while custom grants remain
exact. Older clients can ignore the additive endpoints and capabilities. Data writes reuse the
existing logical operation journal and storage schemas, so no migration or release-version change
is introduced by this source change.

The same post-0.4.5 source line makes managed grant reconciliation source-owned and revisioned.
Managed OIDC and `PUT /v1/auth/managed/operators/{operatorId}/grants` share one transactional
contract: greater `u64` revisions replace only the configured HTTPS authority's subset, exact
digest replays succeed, and stale/divergent revisions conflict. Platform Identity schema v3 is an
append-only ownership migration. It snapshots existing grants as unmanaged and does not grant new
authority; the first trusted reconciliation adopts only its explicit operator. Existing sessions
and invitation-only operators remain valid. Older servers do not understand this ordering or
ownership contract, so server rollback after schema v3 is unsupported. No release number or
artifact is assigned by this source change.

This is additive for invitation login, ordinary linked OIDC, operator sessions, and Product API
clients. It is a coordinated contract upgrade for the opt-in managed control plane: the gateway
must configure the same source authority and send `sourceRevision` before deploying this server;
an older unversioned `managedEnrollment` body is rejected instead of being assigned an ambiguous
revision. Deploy the gateway first (or atomically), then migrate/start the server, and do not roll
the server back after schema v3.

## Pre-release matrix

| Boundary | Current rule |
|---|---|
| Published CLI | Same version on GitHub and npm; macOS/Linux GNU/Windows on ARM64/x86_64 |
| Source CLI | Record the Git commit; a modified checkout is not identified by `0.4.5` alone |
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
| Platform Identity | Management HTTP v1, native OIDC configuration, source-owned managed reconciliation, authenticated Product lifecycle/catalog/Data Admin/log stream, schema v3; no mixed-version or downgrade window |

The source line adds optional `runku init --project-id/--environment-id` flags as a compatible CLI
extension. Existing invocations keep generated IDs. Provisioners that use the extension must require
both IDs and must require a 0.4.0-or-newer binary.

Version 0.4.2 introduced `runku link` as a compatible CLI extension. It writes a separate
`management-link-v1.json` descriptor after an authenticated exact-scope status check; existing
local Product state and protocol formats are unchanged. New CLIs enforce the descriptor's pinned
Management origin on remote commands. A CLI rollback to 0.4.0 can still read the Product root but
does not enforce that additional local origin pin, so operators should not downgrade linked
workstations during an origin-substitution incident.

Version 0.4.3 adds an opt-in PostgreSQL backend to the attached server Environment's logical
document/index/Mutation/outbox/schedule store. SQLite remains the default. The PostgreSQL schema v3
singleton binding is additive and prevents one database from being attached to a different scope;
it does not change public Product protocols. The remaining Product repositories stay under the
Product root, so backup/restore must coordinate both authorities. Older binaries must not serve an
Environment after this profile is adopted.

The post-0.4.5 Management API additions for application credentials, weighted serving policy, and
logical Object Storage are additive HTTP contracts. Operators that grant the new `storage:read`
or `storage:manage` capabilities must upgrade Platform Identity and the Management API together;
older binaries do not recognize those capability names and fail closed. Storage quotas are encoded
as canonical decimal strings so JavaScript clients do not lose integer precision. Application-key
secrets remain one-time responses and are intentionally absent from idempotent replay payloads.
The additive `cron:read`, `cron:activate`, and `schedules:read` names likewise require Identity and
Management binaries that recognize the same catalog. The post-0.4.5 source line implements
`cron:activate` as a per-declaration CAS/idempotency contract and adds Cron repository schema v2 for
durable disabled-definition intent. After that migration, an older binary must not serve the same
Cron repository. Declaration editing and Scheduled retry/cancel remain outside this contract.

The additive `functions:invoke` Platform capability is intended for an operator-facing Runner BFF.
It authorizes only the human control-plane step and never authenticates Product code by itself: the
canonical invocation still requires a separately scoped Application credential and any declared
functional principal. Existing grants are not backfilled; operator/developer role expansion affects
only newly issued or source-reconciled grants, while custom grants remain exact.

The later post-0.4.5 Object Storage extension adds current/version metadata schema v2 and bounded
administrative object routes. After schema v2 is applied, an older binary must not serve the same
registry. PUT writes a SHA-256 content address before the metadata transaction, so an uncertain
response is reconciled by `object-operations`; DELETE is exact-version CAS. These routes do not yet
claim S3 wire compatibility or coordinated backup semantics.

The post-0.4.5 public gateway adds `x-runku-invocation-id` after runtime invocation allocation on
both success and sanitized failure responses. The header is additive and CORS-exposed; the v1 JSON
success/error envelopes remain byte-contract compatible with 0.4.5 SDK decoders. A failure before
allocation has only `x-runku-request-id`.

The post-0.4.5 Code Target grammar adds the exact `environment:default` value. Older SDKs reject it
locally and older gateways reject it during decoding; explicit `release:`, `channel:`, and
`workspace:` targets are unchanged. A default target is serveable only with an exactly converged
weighted policy. Mutation routing is derived from `OperationId`, so the same logical retry cannot
select a different Release.

`GET .../schemas/compatibility` is an additive authenticated projection of the evidence already
enforced by the serving registry. It requires `releases:read` and returns the desired policy
revision, convergence, Release weights, and the common schema/index/Cron hashes. Persisted v1 sets
are always compatible because an incompatible mutation is rejected atomically; clients must still
check `converged` before treating the set as active traffic.

The post-0.4.5 Management API also adds exact-Environment `metrics` and `instances/healthz` reads.
They reuse the existing `usage:read` and `environments:read` capabilities respectively, so no grant
migration is required. Metric values are canonical decimal strings rather than JSON numbers, and
both responses reject unknown fields. Metrics are process-local diagnostic aggregates and never
become usage or billing authority; instance health deliberately uses an opaque Product identifier
and sanitized fixed component statuses.

Version 0.4.4 gives the two PostgreSQL roles unambiguous canonical configuration names:
`RUNKU_IDENTITY_DATABASE_URL` for Platform Identity and `RUNKU_PLATFORM_DATABASE_URL` for Function
platform data. Their `_FILE` forms contain a path to the same secret, not another connection.
The 0.4.3 names remain deprecated aliases for the 0.4 line; canonical and legacy sources cannot be
mixed. The same release adds optional `opn_*` idempotency to delegated-invitation creation,
non-secret operation reconciliation, and idempotent pending-invitation revocation. Platform
Identity schema v2 appends the operation mapping, revocation time, and audit correlation without
reinterpreting v1 rows. Product Function/storage protocols remain unchanged; after migration,
server rollback is unsupported.

Application files are a compatible additive SDK/HTTP surface but introduce new manifest capability
tags and runtime versions `runku-js-2`, `runku-node-2`, and `runku-hybrid-2`. Version 1 manifests
cannot declare `storage:read`/`storage:write`; old binaries fail closed on the new version/tags.
Safe V8 and local Full Node implement version 2. Production OCI/distributed Full Node remains on
version 1 until its mediated Agent channel is versioned, so promotion of a Node storage manifest to
that profile is rejected rather than silently dropping the capability. File metadata schema v1 and
generated S3 key layout `v1/projects/{project}/environments/{environment}/files/{file}` are durable;
future changes require expand/migrate/contract and rollback documentation.

The source line also contains an Environment lifecycle domain and repository. Its schema
v1 creates only new `runku_environments`, `runku_environment_operations`, and
`runku_environment_schema_migrations` tables; it does not reinterpret existing Product rows or
change existing Product rows. Migrations are ordered and checksum-protected. The compact server and
Management API now compose it as an additive exact-scope authority. Operators must upgrade
Platform Identity and Management together before granting the new `environments:read` capability;
adopting the registry still requires a coordinated backup and rollback decision.

The post-0.4.5 Environment schema v2 extends only the operation-kind constraint with `archive` and
`restore`; it transactionally copies all v1 journal rows and does not reinterpret Environment
configuration. Both commands increment the existing configuration/state revision, use the same
idempotency and operation reconciliation contract, and preserve subordinate data. An older binary
must not write a v2 registry. Cloud placement drain/restoration remains separately reconciled from
this portable Product desired state.

The standalone serving-policy registry is another compatible additive source-line capability. Its
schema v1 adds only namespaced policy, weighted-Release, operation, audit, and migration tables.
Each policy stores canonical schema, logical-index, and Cron-declaration hashes derived from
validated Release Manifest v1 values. Multiple Releases fail closed unless all three hashes are
byte-identical. The registry is not attached to Channel/request routing; a future composition must
define runtime selection, `EnvironmentDefault`, the effective write contract, mixed-version
behavior, and coordinated backup/rollback before it changes traffic.

## Change rules

Additive fields require old/new reader tests and safe defaults. Auth, retry, ordering, limits,
pinning, and failure-outcome changes are compatibility changes even without shape changes. Breaking
wire/persisted behavior requires a new version and migration; existing vectors are never rewritten.

Release compatibility includes Function kind/visibility/contracts, schema/index prerequisites,
runtime/Platform Ops, artifacts, Cron, and pending code pins. Channel promotion fails if a candidate
cannot safely share data. Rollback cannot undo migrations; use expand/migrate/contract.

A stable release matrix must publish CLI↔server↔agent↔SDK versions, protocol/persisted readers,
dependency/OS/architecture profiles, upgrade paths, deprecation/security window, and provenance.
