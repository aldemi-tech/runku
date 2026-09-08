# Compatibility and upgrade boundaries

Runku is pre-release and does not promise a general long-term compatibility window yet. Operate a
tagged product distribution as one coordinated set: CLI, `@runku/server`, compact server, Docker
package, protocol, runtime contract, and persisted schema versions. The frontend SDK track may
publish `@runku/client` and `@runku/react` independently as one exact-version pair when its release
notes explicitly retain compatibility with the current Public Protocol.

Unknown wire, manifest, runtime, configuration, or persisted versions fail closed. Runku never
silently falls back to `latest`, another Release, a weaker runtime, or a different credential role.

## Current distribution matrix

The latest published coordinated product distribution and frontend SDK pair report version
`0.5.0`. The source tree is preparing an OCI-only `0.5.1` candidate; it is not a published SDK,
CLI, Git tag, GitHub Release, or final distribution. Neither track has established a general stable
compatibility window. Version 0.3.0 is the first supported compact Docker installation floor;
0.5.0 supports a deliberate forward upgrade from that floor.
Product tags coordinate the CLI, Function SDK, Linux compact server binaries, and compact server
image. Frontend SDK tags coordinate client and React only. Agent, distributed deployment, protocol,
storage, and runtime support windows remain separate distribution gates.

Frontend SDK 0.4.8 adds generated Function-reference values and React/Next.js bindings without
changing Public HTTP/WebSocket v1. It is compatible with the 0.4.7 gateway. The published 0.4.7 CLI
predates the reference generator; applications need references produced by the current source CLI
until a later product distribution includes that generator.

Version 0.5.0 includes that frontend codegen/SDK line and adds the cell-member server composition.
`RUNKU_CELL_CONFIG` selects a strict manifest for several warm, isolated Environments in `shared`
mode or exactly one in `dedicated` mode. It is mutually exclusive with `RUNKU_PRODUCT_ROOT` and
singleton Product database/origin/auth variables. Public HTTP/WebSocket v1 and Product persisted
formats do not change.

Application ingress selects a candidate Environment from one canonical `Host` header, then runs
the existing Product authorization stack. Management dispatch selects the exact Product only after
operator authorization of the Project/Environment path. Version 0.5.0 intentionally preserves one
active writer per Environment: it does not provide same-Environment active-active, distributed
scheduler fencing, or a Kubernetes control plane. Provider fleets may prefer the currently
assigned warm member, but must fence it before replacement.

The 0.5.1 candidate adds Release-scoped document read views and non-destructive full replace
semantics.
Freeze and Channel movement check every `servable`/`active`/`deprecated` Release, including
Releases reachable only by an explicit target. Optional field addition/hiding can coexist across named Channels and a
weighted policy; reads project the selected view and an older replacement preserves newer unknown
fields. Required additions and shared field-contract changes fail closed. Logical index and Cron
contract changes still require an atomic staged operation because durable index readiness is not
implemented in this version.

Management compatibility evidence is v2. Candidate preflight is available before freeze and a
revision-bound POST prevents applying evidence after Release/Channel or serving-policy state has
advanced. This is an additive Management surface but a behavioral hardening of Release activation:
a candidate that previously bypassed comparison through a null baseline or empty Channel is now
blocked when any active-closure member is incompatible.

Version 0.4.5 adds only additive auth response fields and endpoints. A 0.4.5 CLI can still link
non-interactively to an older server with explicit IDs; parameterless interactive linking requires
the new resource catalog. Managed enrollment is disabled unless both gateway and server configure
their separate shared secret, so upgrading invitation-only Self-Hosted preserves its policy.

Version 0.4.6 adds versioned Function/schema catalog and logical Data Admin Management
endpoints without changing existing endpoint or persisted-row meanings. It also adds explicit
`data:read` and `data:write` Platform capabilities. Existing grants are intentionally not backfilled
and receive no new document authority; administrators must opt in by issuing/reconciling an updated
grant. Newly expanded role presets may include Data Admin authority, while custom grants remain
exact. Older clients can ignore the additive endpoints and capabilities. Data writes reuse the
existing logical operation journal and storage schemas, so no migration or release-version change
is introduced by this source change.

The same release makes managed grant reconciliation source-owned and revisioned.
Managed OIDC and `PUT /v1/auth/managed/operators/{operatorId}/grants` share one transactional
contract: greater `u64` revisions replace only the configured HTTPS authority's subset, exact
digest replays succeed, and stale/divergent revisions conflict. Platform Identity schema v3 is an
append-only ownership migration. It snapshots existing grants as unmanaged and does not grant new
authority; the first trusted reconciliation adopts only its explicit operator. Existing sessions
and invitation-only operators remain valid. Older servers do not understand this ordering or
ownership contract, so server rollback after schema v3 is unsupported.

This is additive for invitation login, ordinary linked OIDC, operator sessions, and Product API
clients. It is a coordinated contract upgrade for the opt-in managed control plane: the gateway
must configure the same source authority and send `sourceRevision` before deploying this server;
an older unversioned `managedEnrollment` body is rejected instead of being assigned an ambiguous
revision. Deploy the gateway first (or atomically), then migrate/start the server, and do not roll
the server back after schema v3.

Version 0.4.7 preserves the local CLI's loopback-only listener and adds a separate compact-server
application listener for provider-owned networks. `RUNKU_APPLICATION_LISTEN` is accepted only when
paired with `RUNKU_APPLICATION_TLS_TERMINATED=true` and an attached Product root; incomplete,
invalid, or non-TLS configuration fails before readiness. This lets an ingress/TLS boundary reach
the application port without weakening local development or changing persisted Product identity.
The compact Management adapter also completes local materialization of both Environment creation
and full-configuration updates before reporting success. Wire and persisted shapes are unchanged;
an exact idempotent replay resumes convergence only while its revision remains current. This is a
behavioral fix for 0.4.6 responses that could otherwise leave the desired revision pending.

## Pre-release matrix

| Boundary | Current rule |
|---|---|
| Published CLI | Same version on GitHub and npm; macOS/Linux GNU glibc 2.31+/Windows on ARM64/x86_64 |
| Source CLI | Record the Git commit; a modified checkout or 0.5.1 candidate is not identified by a published version alone |
| Rust | Exact repository toolchain; workspace MSRV is a separate crate contract |
| Node | 20.18.1+ for current SDK/examples; build/runtime contracts must agree |
| Frontend SDK packages | `@runku/client` and `@runku/react` update together with exact peer versions; currently 0.5.0 |
| Distribution JavaScript packages | `@runku/server` and `@runku/cli` remain coordinated with the product distribution; currently 0.5.0 |
| HTTP/WebSocket | v1 envelopes; unknown versions rejected |
| Values/index keys | v1 canonical encodings; existing vectors immutable |
| Release/artifact | Version/digest/size/runtime descriptors verified |
| SQLite/PostgreSQL | Same logical contract; physical schema/files are internal |
| Server composition | Linux GNU glibc 2.31+ ARM64/x86_64 binary and multi-platform OCI image; Safe V8 by default; opt-in in-container Full Node only for one Product root or a `dedicated` one-Environment cell |
| Compact deployment | Dedicated Linux host, Compose v2, one active Environment writer, PostgreSQL 16, host TLS proxy, backup/empty restore |
| Distributed deployment | No published separated-role/Agent/Kubernetes support window yet |
| Platform Identity | Management HTTP v1, native OIDC configuration, source-owned managed reconciliation, authenticated Product lifecycle/catalog/Data Admin/log stream, schema v3; no mixed-version or downgrade window |

### Consumer summary

| Boundary | Current contract |
|---|---|
| CLI | tagged macOS/Linux GNU glibc 2.31+/Windows binaries for ARM64/x86-64 plus exact-version npm launcher |
| Application authoring | `@runku/server@0.5.0` declaration/validator contract |
| TypeScript application client | `@runku/client@0.5.0` HTTP/Realtime/file/reference contract |
| React and Next.js bindings | `@runku/react@0.5.0` with exact `@runku/client@0.5.0` peer |
| Public API | strict HTTP/WebSocket v1 envelopes and canonical values |
| Compact server | Linux GNU glibc 2.31+ ARM64/x86-64 binary and multi-platform non-root image |
| Deployment | Docker Compose v2 compact profile or externally orchestrated cell member; PostgreSQL 16 Platform Identity, Safe runtime by default, optional dedicated-host Node, one active writer per Environment |
| Function data | Product-root SQLite by default; optional exact-scope PostgreSQL 16 profile |
| Distributed roles/Kubernetes | no published general-purpose Agent, active-active, or Helm support window |

Node.js 20.18.1 or newer is required for the current npm/application tooling. The native CLI does
not require Node after direct archive installation.

## Coordinate versions

For production and CI, pin:

The 0.4.6 Management API additions for application credentials, weighted serving policy, and
logical Object Storage are additive HTTP contracts. Operators that grant the new `storage:read`
or `storage:manage` capabilities must upgrade Platform Identity and the Management API together;
older binaries do not recognize those capability names and fail closed. Storage quotas are encoded
as canonical decimal strings so JavaScript clients do not lose integer precision. Application-key
secrets remain one-time responses and are intentionally absent from idempotent replay payloads.
The additive `cron:read`, `cron:activate`, and `schedules:read` names likewise require Identity and
Management binaries that recognize the same catalog. Version 0.4.6 implements
`cron:activate` as a per-declaration CAS/idempotency contract and adds Cron repository schema v2 for
durable disabled-definition intent. After that migration, an older binary must not serve the same
Cron repository. Declaration editing and Scheduled retry/cancel remain outside this contract.

- exact Runku release tag/version;
- exact product-distribution and frontend-SDK package versions;
- server OCI image by version and digest;
- Docker package from the same release;
- deployment configuration and secret-file layout;
- current persisted schema/migration status;
- application Release manifest/runtime versions.

Do not combine an undocumented SDK, CLI, or server version merely because its method or JSON fields
appear similar. The frontend SDK pair is the narrow exception to same-version coordination: its
release notes must explicitly name a compatible Public Protocol/server range, and React must use
its exact client peer. Additive fields are safe only where the consuming version explicitly
documents that it ignores/accepts them.

The 0.4.6 Object Storage extension adds current/version metadata schema v2 and bounded
administrative object routes. After schema v2 is applied, an older binary must not serve the same
registry. PUT writes a SHA-256 content address before the metadata transaction, so an uncertain
response is reconciled by `object-operations`; DELETE is exact-version CAS. Compact filesystem
composition includes the registry and object bytes in its coordinated backup.

## Public API compatibility

Public Function calls use v1 strict envelopes. A request:

Object Storage schema v4 adds durable multipart upload/part state, a completion claim digest, and
terminal completed/aborted state without rewriting earlier rows. Once v4 is applied, older binaries
must not serve the same registry. Exact completion replay reconciles only the same completion body;
part replacement and abort stop after completion is claimed.

The corresponding 0.4.6 Product listener adds `/s3/{bucket}/{key}` with logical signing region
`runku`. The implemented Runku S3 profile is ListObjectsV2, ListObjectVersions, HEAD/GET, bounded
PUT, same-bucket COPY, current/exact-version DELETE, multipart create/upload/list/complete/abort,
public read, bucket CORS, query-presigned SigV4, single byte ranges, conditional reads, and immutable
version-addressed reads. Lifecycle rules execute in bounded batches. The exact profile has an
official AWS CLI campaign; it does not claim bucket ACL/tagging/website/replication, cross-bucket
copy, delete-marker resources, UploadPartCopy, or every AWS SDK. Cloud must preserve the original
signed host through its opaque Product route; proxying this protocol through the global Control API
is not compatible.

The 0.4.6 public gateway adds `x-runku-invocation-id` after runtime invocation allocation on
both success and sanitized failure responses. The header is additive and CORS-exposed; the v1 JSON
success/error envelopes remain byte-contract compatible with 0.4.5 SDK decoders. A failure before
allocation has only `x-runku-request-id`.

The 0.4.6 Code Target grammar adds the exact `environment:default` value. Older SDKs reject it
locally and older gateways reject it during decoding; explicit `release:`, `channel:`, and
`workspace:` targets are unchanged. A default target is serveable only with an exactly converged
weighted policy. Mutation routing is derived from `OperationId`, so the same logical retry cannot
select a different Release.

- must include exact `version: 1`, target, Function, and canonical arguments;
- must include a canonical operation ID for Mutation;
- rejects unknown fields and non-canonical alternate value encodings;
- returns exact Release identity and kind-specific metadata;
- returns sanitized stable error code/retryability.

Canonical values preserve int64, float bits, bytes, timestamps, and typed IDs across languages.
Existing encodings cannot be reinterpreted in place; a future incompatible protocol requires a new
version and migration/client strategy.

See [HTTP without an SDK](public-api.md) and [TypeScript client](typescript-client.md).

## Application declaration compatibility

The 0.4.6 Management API also adds exact-Environment `metrics` and `instances/healthz` reads.
They reuse the existing `usage:read` and `environments:read` capabilities respectively, so no grant
migration is required. Metric values are canonical decimal strings rather than JSON numbers, and
both responses reject unknown fields. Metrics are process-local diagnostic aggregates and never
become usage or billing authority; instance health deliberately uses an opaque Product identifier
and sanitized fixed component statuses.

`runku build` binds schema, logical indexes, Function auth/visibility/capabilities/args/returns,
runtime selection, and Cron declarations into immutable Release contracts.

Classify an application change:

Application files are a compatible additive SDK/HTTP surface. The historical generation-2 wire
identifiers introduced `storage:read`/`storage:write`, but new builds no longer select a reduced
runtime from their capabilities. File metadata schema v1 and generated S3 key layout
`v1/projects/{project}/environments/{environment}/files/{file}` are durable; future changes require
expand/migrate/contract and rollback documentation.

Environment variables and encrypted secrets add the `variable:NAME` capability and activate the
previously reserved `secret:NAME` capability. Every new build now emits the cumulative current wire
identifier—`runku-js`, `runku-node`, or `runku-hybrid` according to artifact class—even when it
uses only earlier capabilities. The current runtime is a superset of the base and application-file
Platform Ops. Legacy numeric manifests remain decodable as persisted compatibility inputs, not
parallel runtime products. Safe V8 and local Full Node expose the cumulative API; OCI/dedicated-
host/Docker/Firecracker execution pre-resolves exact declared configuration through the agent-side
Environment broker. Configuration registry schema v1 is additive, checksum-protected, and stores
idempotent result snapshots plus value-free audit. Older binaries must not write a registry after
it is adopted. The authenticated Management routes and `configuration:read`/
`configuration:manage` capabilities must be upgraded together.

| Class | Examples | Deployment consequence |
|---|---|---|
| additive | new Function, optional field, new table | optional schema views may coexist after full active-closure preflight; caller API compatibility still applies |
| behavioral | changed auth, permission, limit, retry/effect/timing | coordinate callers/operations even when TypeScript shape is unchanged |
| breaking | removed Function, required field, incompatible return, renamed table/index | staged migration or atomic cutover; rollback may be limited |
| security fix | newly rejects formerly accepted behavior | prioritize safety; communicate intentional incompatibility |

The current gradual serving policy permits different schema hashes only after immutable artifact
preflight proves symmetric Release-view coexistence. Index and Cron contract hashes remain
byte-identical. Required-field additions, type/bound changes, Table-ID renames, and unready index
changes remain blocked.

For durable schema, use expand → backfill → contract and preserve backward reads through the entire
rollout/rollback window. Channel rollback never rewrites stored documents.

The 0.4.6 Environment schema v2 extends only the operation-kind constraint with `archive` and
`restore`; it transactionally copies all v1 journal rows and does not reinterpret Environment
configuration. Both commands increment the existing configuration/state revision, use the same
idempotency and operation reconciliation contract, and preserve subordinate data. An older binary
must not write a v2 registry. Cloud placement drain/restoration remains separately reconciled from
this portable Product desired state.

The standalone serving-policy registry is another compatible additive source-line capability. Its
schema v1 adds only namespaced policy, weighted-Release, operation, audit, and migration tables.
Each policy stores canonical schema, logical-index, and Cron-declaration hashes derived from
validated Release Manifest v1 values. Product lifecycle validates schema pairs from verified
artifacts before persisting a policy; logical-index and Cron hashes remain identical. The compact gateway attaches exactly converged policies to
`environment:default`: request/subscription identity selects Query, Action, and Realtime traffic,
while Mutation selection derives from `OperationId`. Explicit Release and Channel targets
participate in the same invocable closure until an explicitly fenced retirement verifies that no
Channel, policy, Cron activation, or nonterminal Scheduled Invocation retains the Release;
Workspace targets remain development-scoped.
Index-build readiness and distributed-runtime qualification remain separate compatibility gates.

## Target compatibility

| Target | Compatibility responsibility |
|---|---|
| `release:rel_*` | caller explicitly chooses immutable code that server/runtime must still support |
| `channel:<name>` | operator moves policy only to an eligible compatible Release/set |
| `environment:default` | requires a configured converged serving policy; no fallback |
| `workspace:<name>` | development only where Environment policy permits |

An individual request, subscription, nested call tree, Cron activation, or scheduled invocation
pins exact code for its lifetime. Upgrades must retain the runtime/artifact versions needed by
still-live pinned work or deliberately drain/migrate that work first.

## CLI/server behavior

The CLI and server exchange strict Management contracts. Use the CLI version shipped for the
server release whenever possible. New CLI features can require Management endpoints unavailable on
older servers even when basic login/status still works.

`runku link` pins the Management origin and exact Project/Environment in the application root. Do
not downgrade to a CLI that ignores this trust binding during an origin-substitution incident.

Automation must honor command-specific exit codes, compare-and-set fields, operation identity, and
one-time secret handling. A parser accepting a flag does not prove the remote server implements the
corresponding capability.

## Persisted-state and downgrade rules

Runku applies append-only/checksum-protected migrations and rejects unknown future versions. Before
upgrade, create/verify a complete compatible backup and determine the last point at which the old
binary can still open every authority.

Known current forward-only boundaries include:

| State boundary | Downgrade consequence after adoption |
|---|---|
| optional Function PostgreSQL singleton/scope binding schema | older binary must not serve that database |
| Platform Identity managed-grant ownership schema | older server does not understand revision/ownership ordering |
| Cron durable disabled-declaration schema | older server must not resume the same Cron authority |
| Object Storage current/version and encrypted SigV4 key schemas | older server cannot safely interpret full registry/key state |

Do not infer downgrade safety from an unchanged public API. If a migration crosses one of these
boundaries, application traffic rollback may remain possible through a Channel while server-binary
rollback is not.

## Compact-server upgrade floor

The current documented compact path supports a deliberate forward upgrade from the 0.3.0 compact
installation floor to 0.4.7. It is not a promise that every arbitrary intermediate/newer pre-release
combination can skip directly.

Use [Upgrades and rollback](../operations/upgrades.md) for preflight, backup, migration, canaries,
and rollback-decision procedure. Follow the release notes for the exact from/to pair.

## Database/backend compatibility

| Dependency | Supported use |
|---|---|
| PostgreSQL | version 16+ for Platform Identity and optional exact-scope Function logical store |
| SQLite | local/Product-root authorities in the compact profile; files are internal state |
| filesystem Application/Object bytes | supported compact recovery layout when using dedicated mounted `files/` root |
| external S3-compatible backend for Runku Storage bytes | supported adapter/profile with provider-operated recovery; not copied by compact backup |
| Operational Log external S3/NATS | optional separate HA log profile; does not make Product data HA |

Changing SQLite/PostgreSQL or the Runku Storage physical backend is not a transparent configuration toggle for existing
state. Use the documented migration/cutover boundary or remain on the current backend.

## Credential compatibility

Credential formats are role-specific and never interchangeable:

- Application publishable/secret keys call public Functions;
- development credentials publish authorized Workspaces;
- operator access/refresh tokens call Authentication/Management;
- file transfer grants authorize one bounded transfer;
- Runku Storage Product keys sign the Runku route through its S3-compatible protocol;
- physical external-object-store/PostgreSQL credentials are held by server/deployment configuration.

An upgrade must preserve the pepper/encryption material required to verify or decrypt current
credential state. Restoring database rows without matching peppers/keys can make credentials
unusable; restoring old identity state can resurrect later-revoked authority and requires explicit
reconciliation/revocation before traffic.

## Upgrade acceptance

For the exact selected version/profile:

1. read its release notes and verify checksums/provenance/image digest;
2. inventory server/CLI/SDK/application manifests and persisted schema versions;
3. identify forward-only migrations and binary rollback cutoff;
4. create and verify a complete recovery point, including external dependencies/secrets by reference;
5. test restore into an empty isolated installation before the production window;
6. run server configuration/migration checks;
7. upgrade one controlled boundary following the packaged procedure;
8. verify identity, Management scope, Release/Channel, Query/Mutation replay, Action uncertainty,
   Realtime, schedules/Cron, Application Files/Object Storage, logs, and metrics;
9. keep rollback traffic/data/backend consequences explicit;
10. record the observed outcome and remaining rollback window.

Runku SaaS can help compare application protocol behavior across a supported service upgrade, but
it does not validate Self-Hosted database migrations, secret preservation, storage recovery, proxy,
or host rollback.
