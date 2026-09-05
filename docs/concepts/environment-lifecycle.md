# Environment lifecycle registry

Runku needs one Product authority for Environment identity and desired configuration before a
management surface can safely create or reconcile subordinate Release, identity, schedule, or
storage state. The standalone Environment lifecycle registry provides that authority without
embedding provider placement, DNS, billing, or HTTP behavior.

## Current status and boundary

The following behavior is implemented and test-covered:

- `runku-environments` owns validated names, Project-unique slugs, logical regions, protection and
  purpose policy, desired/observed state, compare-and-set revisions, idempotent commands, operation
  lookup, and the storage-independent service;
- `runku-environment-repository` implements the same contract over SQLite and PostgreSQL 16+ with
  bounded pools and checksum-protected append-only migrations;
- create, get, bounded list, full-configuration update, and trusted materializer observation are
  available through Rust APIs;
- the compact server composes the registry in the protected Product state database and reports its
  health through readiness;
- authenticated exact-scope Management routes create, get, update, and reconcile operations using
  independent `environments:read` and `environments:manage` capabilities.

Project-wide list, CLI commands, archive/restore, provider provisioning, and automatic population
of a registry record for pre-existing local roots remain outside this slice. A Product adapter owns
one configured exact Environment: creation therefore uses the exact Environment URL and cannot
allocate or infer an ID.

## Authenticated Management API

All routes use the exact configured `(ProjectId, EnvironmentId)` and reject another scope before
touching the registry:

- `GET /v1/projects/{project}/environments/{environment}` requires `environments:read`;
- `POST .../environments/{environment}` requires `environments:manage` and
  `Idempotency-Key: opn_*` to create the caller-selected canonical Environment;
- `PUT .../environments/{environment}` requires `environments:manage`, the same idempotency header,
  and an exact positive `expectedRevision` for complete configuration replacement;
- `GET .../environment-operations/{opn_*}` requires `environments:read` and reconciles an uncertain
  result without making a new write.

Timestamps are canonical decimal microseconds and are pinned by the caller because they participate
in the operation digest. The response never contains provider, host, database, DNS, or placement
details. Creation produces observed `pending`; only a trusted materializer may record `ready` or
`failed` through the internal Rust service.

## Model

Every record key is the exact `(ProjectId, EnvironmentId)` pair. Public IDs and slugs never prove
ownership; a future transport adapter must authorize the Project and Environment before calling
the service.

| Field | Contract |
|---|---|
| name | Trimmed UTF-8, 1–120 bytes, no control characters |
| slug | Lowercase DNS-label shape, 1–63 bytes, unique within the Project |
| region | Logical lowercase label, 1–64 bytes; never a provider account, cell, or host |
| purpose/protection/location | Reuses the existing Environment policy axes |
| configuration revision | Positive compare-and-set revision; starts at 1 |
| desired state | `active`; `archived` is reserved for the later archive state machine |
| observed state | `pending`, `ready`, or `failed` |
| observed revision | Exact desired revision for `ready`/`failed`; an older revision may remain visible while a new configuration is `pending` |

Creation writes desired `active`, observed `pending`, and revision 1. A configuration update
requires the exact current revision, replaces the complete configuration, increments the revision,
and returns observed state to `pending`. It retains the last older observed revision so an operator
can distinguish “never materialized” from “updating a previously materialized Environment.”

`materialize` does not provision infrastructure. A trusted reconciler calls it only after applying
the desired configuration and records `ready` or `failed` for the exact revision. It cannot create
an unknown Environment. Provider capacity, placement, physical region identifiers, and DNS remain
outside this portable Product record.

## Rust service workflow

The intended adapter flow is:

```rust,no_run
use std::sync::Arc;
use runku_core::{EnvironmentId, EnvironmentLocation, EnvironmentProtection,
    EnvironmentPurpose, EnvironmentScope, OperationId, ProjectId};
use runku_environment_repository::{EnvironmentRepositoryConfig, SqlEnvironmentRepository};
use runku_environments::{EnvironmentConfiguration, EnvironmentPageRequest, EnvironmentService};
use runku_value::TimestampMicros;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let repository = SqlEnvironmentRepository::connect_sqlite(
    "sqlite://environment-registry.sqlite3?mode=rwc",
    EnvironmentRepositoryConfig::LOCAL,
).await?;
let service = EnvironmentService::new(Arc::new(repository));
let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
let operation_id = OperationId::generate();
let configuration = EnvironmentConfiguration {
    name: "Production".parse()?,
    slug: "production".parse()?,
    region: "us-east-1".parse()?,
    purpose: EnvironmentPurpose::Production,
    protection: EnvironmentProtection::Production,
    location: EnvironmentLocation::SelfHosted,
    workspace_targets_enabled: false,
};

service.create(scope, operation_id, configuration, TimestampMicros::new(1)).await?;
let page = service.list(scope.project_id(), EnvironmentPageRequest::new(None, 50)?).await?;
assert_eq!(page.environments.len(), 1);
# Ok(())
# }
```

A production composition selects `connect_postgres` with `EnvironmentRepositoryConfig::PRODUCTION`.
SQLite rejects the Production role and PostgreSQL rejects the Local role. PostgreSQL older than 16
fails closed.

## Idempotency, conflict, and recovery

Every write carries a caller-generated `opn_*` identity. The command digest binds the exact Project,
Environment, complete intent, precondition, and trusted timestamp.

| Result | Meaning and safe action |
|---|---|
| exact replay | The same operation ID and identical command returns the immutable prior outcome |
| `ENVIRONMENT_OPERATION_ID_REUSED` | The operation ID was presented with different intent; stop and allocate a new ID only for a genuinely new intent |
| `ENVIRONMENT_CONFLICT` | Revision, slug, or lifecycle state changed; read current state and decide again |
| `ENVIRONMENT_NOT_FOUND` | The exact scope does not exist; update/materialize never creates it |
| `ENVIRONMENT_RESULT_UNCERTAIN` | Commit acknowledgement was lost; look up the same operation under the same exact scope before retrying |
| busy/unavailable | Retry with bounded backoff and the same operation ID/body |
| corrupt/unsupported | Stop writes, preserve the database and migration evidence, and recover rather than editing rows |

Operation lookup is not authorization. A caller must still hold current authority for the exact
stored scope. Looking up an operation under another Project or Environment returns no record and
never leaks the original outcome.

## Persistence, upgrade, and rollback

Schema v1 adds three namespaced tables:

- `runku_environments` for authoritative desired/observed records;
- `runku_environment_operations` for exact-scope replay and uncertain-result reconciliation;
- `runku_environment_schema_migrations` for ordered version/checksum evidence.

The schema is additive and independent from existing local/release/identity Environment helper
rows. Future changes append a new migration; applied migration text/checksums must never be edited.
Unknown future migration versions fail closed.

Because no released process composes this repository yet, current compact backup and restore does
not claim it as an active authority. A composition that adopts it must quiesce writers and back up
the complete registry database, operation journal, migration rows, and the subordinate Product
stores at one coordinated recovery point. Restoring only the registry or only subordinate stores is
invalid. Rollback to a binary that does not understand an adopted registry must not resume writes;
use the composition's documented forward recovery or verified coordinated restore.

## Security and operational limits

- Scope is present in every record, operation key, lookup, uniqueness rule, and query predicate.
- Slug uniqueness never substitutes for exact Environment identity.
- List requests are bounded to 100 records and use an exclusive Environment ID cursor.
- Names, slugs, regions, revisions, states, timestamps, and decoded rows are revalidated on read.
- Pool, lock, statement, and idle-transaction timeouts are bounded.
- Telemetry contains aggregate counts and pool gauges only; it does not label names, slugs, regions,
  operation IDs, Projects, or Environments.
- The library performs no authorization. The Management adapter enforces current
  `environments:read`/`environments:manage` grants before every call and does not accept Application
  or Development credentials.

## Evidence

`cargo test -p runku-environments -p runku-environment-repository` covers pure lifecycle rules,
scope-bound command digests, replay/reuse conflicts, exact-scope isolation, absent-scope behavior,
slug isolation, pagination, materialization, reopen, checksum tampering, and concurrent CAS. SQLite
runs by default. The same repository conformance and concurrency campaign runs against PostgreSQL
when `RUNKU_TEST_POSTGRES_URL` names an explicitly managed PostgreSQL 16+ test database.

Strict lint evidence is provided by:

```sh
cargo clippy -p runku-environments -p runku-environment-repository --all-targets -- -D warnings
```
