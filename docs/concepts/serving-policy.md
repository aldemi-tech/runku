# Weighted serving policy

An Environment may need an atomic Release cutover or a controlled gradual rollout without making
provider placement part of Product semantics. The serving-policy registry records desired intent,
its serving-path observation, idempotent operation results, and an immutable audit trail. The
Product gateway consumes a converged policy through the explicit `environment:default` code target.

## Current status and boundary

The following provider-independent library behavior is implemented and test-covered:

- `runku-serving` owns validated atomic/gradual policies, Release weights, canonical compatibility
  evidence, compare-and-set revisions, desired/observed transitions, stable errors, operation
  results, and audit values;
- `runku-serving-repository` implements the same exact-Environment contract over SQLite and
  PostgreSQL 16+ with bounded pools and checksum-protected append-only migrations;
- exact policy reads, trusted materializer observations, uncertain-operation lookup, and bounded
  audit pagination are available through Rust APIs.

The compact server opens this registry from the Product root and the authenticated Management
API exposes exact-scope read, idempotent CAS replacement, and operation lookup. Replacement derives
all compatibility evidence by loading each requested servable Release through the Environment's
Release authority; clients cannot submit hashes. `releases:read` authorizes policy and operation
reads, while `channels:promote` authorizes replacement and the verified operator is the audit actor.

After persistence, the compact server records a trusted ready observation only after every Release
was resolved and verified through the same authority used by the runtime. Queries and Actions map
their request identity to a deterministic percentile; Realtime uses its subscription identity and
then pins the selected Release for all reruns. Mutations use `OperationId`, so transport retries and
uncertain-result reconciliation select the same Release. Explicit Release, Channel, and Workspace
targets preserve their existing semantics and never consult this policy.

## Policy contract

Every record and operation uses the exact `(ProjectId, EnvironmentId)` scope. A policy contains a
canonical Release-ID-ordered set with positive integer percentage weights:

| Mode | Required shape |
|---|---|
| `atomic` | exactly one Release with weight 100 |
| `gradual` | 2–16 distinct Releases; every weight is 1–100 and the sum is exactly 100 |

Zero-weight placeholders, duplicate Releases, empty policies, fractional weights, totals other
than 100, and an atomic multi-Release set fail before persistence. The desired revision starts at 1
and every complete replacement uses compare-and-set and increments it once.

`pending`, `ready`, and `failed` describe observation of the desired revision. A policy update
returns to `pending` while retaining an older observed revision, if one exists. Only a trusted
serving reconciler may mark the exact current revision `ready` or `failed`; materialization cannot
create an absent policy.

## Conservative Release compatibility gate

`ServingPolicy::from_manifests` consumes validated `ReleaseManifestV1` values. A caller must load
each manifest from the authoritative Release repository under the same exact Environment scope;
the manifest contains Project identity but does not itself prove Environment association or
servability status.

For a policy containing multiple Releases, all entries must have byte-identical:

1. `schema_contract_hash` from the canonical Release Manifest;
2. `index_contract_hash` from the canonical Release Manifest;
3. the serving-policy Cron-declaration hash.

The Cron hash is domain-separated as `RUNKU_CRON_DECLARATIONS_V1`, covers the complete ordered
declaration set, and length-prefixes every canonical name, normalized UTC schedule, destination
Function, and existing Stored Value v1 argument encoding. Release identity, implementation bytes,
and unrelated Function contracts do not affect it.

This is the first fail-closed coexistence rule, not a general schema compatibility engine. A single
Release atomic policy has no peer to compare. A gradual policy with any schema, index, or Cron
difference returns `SERVING_POLICY_INCOMPATIBLE_CONTRACTS` before a repository transaction begins,
so desired state and audit remain unchanged.

## Idempotency, observation, and audit

Every mutation carries a caller-generated `opn_*` identity. Its digest binds the exact Project,
Environment, command, CAS precondition, complete canonical desired-policy digest, outcome, and
trusted timestamp.

| Result | Meaning and safe action |
|---|---|
| exact replay | same operation ID and identical command returns the immutable prior result without adding audit rows |
| `SERVING_POLICY_OPERATION_ID_REUSED` | operation ID has different intent; stop and investigate or use a new ID only for new intent |
| `SERVING_POLICY_CONFLICT` | policy revision or state changed; reload and decide again |
| `SERVING_POLICY_NOT_FOUND` | exact policy is absent; materialization never creates it |
| `SERVING_POLICY_RESULT_UNCERTAIN` | commit acknowledgement was lost; look up the same operation in the same exact scope before retrying |
| busy/unavailable | retry with bounded backoff and the identical operation ID and body |
| corrupt/unsupported | stop writes and preserve database/migration evidence for recovery |

Every newly committed command writes its policy mutation, immutable operation result, and immutable
audit event in one transaction. Desired-policy events carry the authenticated `OperatorId` supplied
by the trusted adapter; materializer observations are explicitly system-attributed without an
operator. Audit pages are bounded to 100 and ordered by trusted timestamp plus operation identity.
Audit attribution and operation lookup are not authorization: a future Management adapter must
check current exact-scope operator authority before every read or write and may only pass the
verified session operator as the actor.

## Persistence and recovery

Schema v1 creates five namespaced tables:

- `runku_serving_policies` for one desired/observed record per exact Environment;
- `runku_serving_releases` for the current canonical weighted set and three compatibility hashes;
- `runku_serving_operations` for replay and uncertain-result reconciliation;
- `runku_serving_audit` for immutable successful-operation evidence;
- `runku_serving_schema_migrations` for ordered version/checksum evidence.

Header and weighted entries are read in one database snapshot and changed atomically. Future schema
changes append a migration; applied migration text or checksums are never rewritten. Unknown future
migration versions fail closed. SQLite is Local/test-only and PostgreSQL is Production-role-only.

The compact process stores the registry tables in its coordinated identity database, but the
current backup manifest does not yet declare or verify them as a recovery component. Backup work
must quiesce policy and Release writers and coordinate policy,
operation/audit, Release metadata/artifacts, Environment records, Cron activation state, and
subordinate Product data at a verified recovery point. An older binary that does not understand an
adopted serving authority must not resume writes after rollback.

## Security and serving semantics

- Scope is included in every key, command digest, lookup, audit query, and SQL predicate.
- Hash equality is compatibility evidence, not Release ownership, lifecycle, artifact integrity,
  runtime support, or authorization. Those checks remain mandatory at the Release/Management
  boundary.
- Telemetry contains only aggregate counters and pool gauges; it never labels Projects,
  Environments, Releases, operations, or policy digests.
- `environment:default` requires a configured, exactly converged policy. Missing, pending, failed,
  unknown, or incompatible state fails closed; it never falls back to a Channel, Release, or
  `latest` target.
- Selection is an integral percentile over canonical Release-ID order. Every root invocation pins
  one exact Release before auth/execution; nested calls and Scheduled work inherit that pin.
- Mutation selection derives from the idempotent operation identity rather than transport request
  identity, preventing a retry from crossing Release weights.

## Evidence

`cargo test -p runku-serving -p runku-serving-repository` covers atomic/gradual shape, canonical
ordering, every compatibility hash, Cron hashing, CAS and no-op rejection, replay/reuse, exact-scope
isolation, absent-scope materialization, desired/observed transitions, audit pagination/replay,
reopen, checksum tampering, and concurrent writers. SQLite runs by default. The identical
repository conformance and concurrency campaign runs against PostgreSQL 16+ when
`RUNKU_TEST_POSTGRES_URL` names an explicitly managed test database.
