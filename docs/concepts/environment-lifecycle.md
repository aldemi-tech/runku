# Administer an Environment lifecycle

An Environment is the persistent-state boundary for application data, identity, configuration,
storage metadata, Releases/Channels, schedules, and operational state. Its lifecycle record
describes desired Product state; it does not expose host, DNS, database, billing, or provider
placement.

Use the Management API when an operator/controller must create, replace, archive, restore, or
reconcile the exact Environment record. The compact server operates one preconfigured exact
Project/Environment scope; the URL never allocates or guesses IDs.

## Permissions and endpoint

- `runku-environments` owns validated names, Project-unique slugs, logical regions, protection and
  purpose policy, desired/observed state, compare-and-set revisions, idempotent commands, operation
  lookup, and the storage-independent service;
- `runku-environment-repository` implements the same contract over SQLite and PostgreSQL 16+ with
  bounded pools and checksum-protected append-only migrations;
- create, get, bounded list, full-configuration update, archive, restore, and trusted materializer
  observation are available through Rust APIs;
- the compact server composes the registry in the protected Product state database, reports its
  health through readiness, and acts as the trusted local materializer for creation;
- authenticated exact-scope Management routes create, get, update, and reconcile operations using
  independent `environments:read` and `environments:manage` capabilities.

Project-wide list, CLI commands, provider provisioning, and automatic population
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
- `POST .../environments/{environment}/archive` and `/restore` require `environments:manage`, the
  same idempotency header, an exact positive `expectedRevision`, and a pinned
  `changedAtMicros`; archive/restore operation lookup is identical to other lifecycle writes;
- `GET .../environment-operations/{opn_*}` requires `environments:read` and reconciles an uncertain
  result without making a new write.

Timestamps are canonical decimal microseconds and are pinned by the caller because they participate
in the operation digest. The response never contains provider, host, database, DNS, or placement
details. The standalone registry creates an observed `pending` record; the compact server is its
trusted local materializer and records `ready` before returning a successful create response.

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
| desired state | `active` or `archived`, changed only through revisioned lifecycle commands |
| observed state | `pending`, `ready`, or `failed` |
| observed revision | Exact desired revision for `ready`/`failed`; an older revision may remain visible while a new configuration is `pending` |

The registry creation transition writes desired `active`, observed `pending`, and revision 1. The
compact server immediately materializes that revision to `ready` before returning success. A
configuration update requires the exact current revision, replaces the complete configuration,
increments the revision, and first returns the registry state to `pending`. The compact server then
applies the local effect and records that exact revision `ready` before returning success. During
that bounded transition, the registry retains the last older observed revision so recovery can
distinguish “never materialized” from “updating a previously materialized Environment.”

`materialize` does not provision infrastructure. A trusted reconciler calls it only after applying
the desired configuration and records `ready` or `failed` for the exact revision. It cannot create
an unknown Environment. Provider capacity, placement, physical region identifiers, and DNS remain
outside this portable Product record.

Archive and restore preserve configuration and subordinate Product state. Each changes desired
state, increments the same CAS revision, returns observation to `pending`, and retains the older
observation until materialized. The compact server applies the local effect synchronously: archive
stops the Product listener before recording `ready`; restore starts it when a Channel exists (or
keeps a valid no-Release Environment idle) before recording `ready`. An archived root does not
restart serving after process restart. Cloud provider drain, DNS, placement, and resource teardown
remain Cloud lifecycle effects and must converge before Cloud presents its placement as ready.

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

Base path:

```text
/v1/projects/{projectId}/environments/{environmentId}
```

| Operation | Method/path | Capability |
|---|---|---|
| read Environment | `GET <base>` | `environments:read` |
| create exact record | `POST <base>` | `environments:manage` |
| replace configuration | `PUT <base>` | `environments:manage` |
| archive | `POST <base>/archive` | `environments:manage` |
| restore | `POST <base>/restore` | `environments:manage` |
| reconcile operation | `GET <base>/environment-operations/{opn_*}` | `environments:read` |

Protected calls use `Authorization: Bearer rk_at_v1_*`. Every mutation also uses one canonical
`Idempotency-Key: opn_*`. Application Keys, functional JWTs, development credentials, and Product
storage keys are rejected.

## Configuration fields

```json
{
  "name": "Production",
  "slug": "production",
  "region": "cl-santiago",
  "purpose": "production",
  "protection": "production",
  "location": "selfHosted",
  "workspaceTargetsEnabled": false
}
```

The compact Management adapter may persist a create or update intent before local materialization
fails. Retry the identical request with the same operation ID: the durable operation replay is
recognized and the adapter retries convergence only when that operation still names the current
pending revision. If a later revision already exists, read current state and reconcile that newer
intent instead.

Operation lookup is not authorization. A caller must still hold current authority for the exact
stored scope. Looking up an operation under another Project or Environment returns no record and
never leaks the original outcome.

| Field | Accepted value | Operational meaning |
|---|---|---|
| `name` | trimmed UTF-8, 1–120 bytes, no controls | operator-facing display name |
| `slug` | lowercase DNS-label, 1–63 bytes, Project-unique | stable human routing/catalog label |
| `region` | lowercase logical label, 1–64 bytes | portable region choice; not a provider host/account |
| `purpose` | `development`, `preview`, `staging`, `production` | intended workload/lifecycle class |
| `protection` | `open`, `protected`, `production` | change-protection policy axis |
| `location` | `local`, `managed`, `selfHosted` | Product ownership/location classification |
| `workspaceTargetsEnabled` | boolean | whether development Workspace targets may serve |

An update replaces this complete configuration. It is not a partial patch. Keep fields unchanged
when you do not intend to modify them.

## Read current state

```sh
curl --fail-with-body \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}"
```

The response contains:

| Field | Meaning |
|---|---|
| `projectId`, `environmentId` | exact persistent scope |
| `configuration` | complete desired configuration |
| `configurationRevision` | positive compare-and-set revision |
| `desiredState` | `active` or `archived` |
| `observedState` | `pending`, `ready`, or `failed` |
| `observedConfigurationRevision` | exact revision most recently observed, or null |
| `converged` | true only when current desired revision is observed ready |
| timestamps | canonical decimal Unix microseconds |

Do not equate HTTP success with serving readiness. For an active change, wait until `converged` is
true and `observedConfigurationRevision === configurationRevision`, then check Product readiness
and a representative application request.

## Create the exact Environment record

The Project and Environment IDs are already allocated/configured outside this route. Create their
portable record:

```sh
curl --fail-with-body \
  -X POST \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "idempotency-key: opn_01ARZ3NDEKTSV4RRFFQ69G5FAY" \
  -H "content-type: application/json" \
  --data-binary @- \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}" <<'JSON'
{
  "configuration": {
    "name": "Production",
    "slug": "production",
    "region": "cl-santiago",
    "purpose": "production",
    "protection": "production",
    "location": "selfHosted",
    "workspaceTargetsEnabled": false
  },
  "createdAtMicros": "1800000000000000"
}
JSON
```

Creation starts `configurationRevision: 1`, `desiredState: "active"`, and observation pending until
the compact Product applies/records the matching state. Repeat the exact request with the same
operation ID after an uncertain result.

## Replace configuration safely

First GET and record the current revision. Then send the complete new configuration:

```sh
curl --fail-with-body \
  -X PUT \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "idempotency-key: opn_01ARZ3NDEKTSV4RRFFQ69G5FAZ" \
  -H "content-type: application/json" \
  --data-binary @- \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}" <<'JSON'
{
  "expectedRevision": 1,
  "configuration": {
    "name": "Production",
    "slug": "production",
    "region": "cl-santiago",
    "purpose": "production",
    "protection": "production",
    "location": "selfHosted",
    "workspaceTargetsEnabled": false
  },
  "updatedAtMicros": "1800000000000001"
}
JSON
```

A successful replacement increments the revision and returns observation to `pending`. The prior
observed revision can remain visible so operators can distinguish a newly pending update from an
Environment that was never ready.

On conflict, GET again and decide whether your intent is still valid. Do not substitute the new
revision into an old request automatically.

## Archive

Archive preserves configuration and subordinate durable state but requests that the Environment
stop serving:

```sh
curl --fail-with-body \
  -X POST \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "idempotency-key: opn_01ARZ3NDEKTSV4RRFFQ69G5FB0" \
  -H "content-type: application/json" \
  --data-binary '{"expectedRevision":2,"changedAtMicros":"1800000000000002"}' \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/archive"
```

The compact server stops the Product listener before recording the archived revision ready. An
archived Product remains non-serving after process restart.

Before archive:

1. stop new deployments/configuration/storage mutations;
2. decide how pending scheduled/Cron/external work is handled;
3. verify and retain the required backup;
4. record current Release/Channel/configuration/credential/storage state;
5. communicate that Application API traffic will stop.

Archive does not delete Product data, revoke every credential, remove storage bytes, or destroy
infrastructure. Apply those separately under the retention/offboarding plan.

## Restore

Restore changes desired state back to active under the current revision:

```sh
curl --fail-with-body \
  -X POST \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "idempotency-key: opn_01ARZ3NDEKTSV4RRFFQ69G5FB1" \
  -H "content-type: application/json" \
  --data-binary '{"expectedRevision":3,"changedAtMicros":"1800000000000003"}' \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/restore"
```

The Product becomes ready to serve only when its dependencies and an eligible Channel/serving
state exist. A valid no-Release Environment may remain operationally idle rather than serving an
invented target.

After restore, verify exact IDs/revision, Product readiness, Channel/Release, Application Client and
functional identity, Query/Mutation replay, Realtime resync, schedules/Cron, files/Object Storage,
logs, and recovery monitoring.

## Desired versus observed state

| Desired | Observed | Interpretation |
|---|---|---|
| active | pending | activation/configuration requested but not yet applied |
| active | ready at current revision | converged and eligible for readiness/traffic checks |
| active | failed at current revision | desired activation could not be applied |
| archived | pending | drain/stop requested but not yet confirmed |
| archived | ready at current revision | Product serving stopped for this lifecycle intent |
| any | ready/failed at older revision | a newer desired change remains unconverged |

Never route traffic based only on `desiredState`. `converged` plus Product readiness and serving
policy are separate conditions.

## Idempotency and uncertain results

Every write binds the operation ID to exact Project, Environment, path, complete body, CAS
precondition, and caller-pinned timestamp.

| Result | Safe action |
|---|---|
| exact replay | accept the immutable prior result; `replayed` is true |
| operation ID reused | stop; use a new ID only for genuinely new intent |
| revision/slug conflict | read current state and reconcile |
| result uncertain/timeout | GET the exact operation; repeat only the identical request/ID when needed |
| busy/unavailable | bounded backoff with identical body/ID |
| corruption/unsupported state | stop lifecycle writes, preserve evidence, restore/repair deliberately |

Operation lookup:

```sh
curl --fail-with-body \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/environment-operations/opn_..."
```

Lookup still requires current exact-scope `environments:read` permission and does not expose an
operation from another Environment.

## Backup, upgrades, and removal

The Environment lifecycle record is only one part of a recovery point. A valid backup coordinates
it with Product repositories, documents/indexes/outbox/schedules, Release/Workspace/Channel state,
application identity, configuration/secrets, Cron state, logs, artifacts, and storage bytes.

Do not restore only the lifecycle record or manually edit its revision/observation. Use the
[Backup and recovery](../operations/backup-and-recovery.md) and
[Upgrade](../operations/upgrades.md) procedures for the supported compact profile.

Archival is reversible lifecycle state; uninstall/delete-data is destructive installation removal.
Do not substitute one for the other.

## Current limitations

- There is no general Environment-create CLI command; ordinary CLI `link` selects an already
  authorized Environment.
- The compact server operates one configured exact Product Environment rather than allocating a
  fleet through this route.
- Provider provisioning, DNS, physical placement, and billing are not fields or effects of this
  Product lifecycle API.
- General distributed reconciliation is not a shipped Self-Hosted role topology.

Use the [Management API reference](../reference/management-api.md) for common transport/security
rules and the [operator handbook](../operations/operator-handbook.md) for maintenance/incident flow.
