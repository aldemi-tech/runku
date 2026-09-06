# Route traffic with a serving policy

The Environment serving policy controls which immutable Release serves
`target: "environment:default"`. Use an atomic policy for a complete cutover or a gradual policy
for a bounded percentage rollout. The policy changes traffic; it never modifies Release code or
Environment data.

Applications may also target an explicit `channel:`, `release:`, or authorized `workspace:`. Those
targets do not consult the default serving policy.

## Permissions and routes

Base path:

```text
/v1/projects/{projectId}/environments/{environmentId}
```

| Task | Route | Capability |
|---|---|---|
| read current policy | `GET <base>/serving-policy` | `releases:read` |
| replace policy | `PUT <base>/serving-policy` | `channels:promote` |
| reconcile uncertain operation | `GET <base>/serving-policy-operations/{opn_*}` | `releases:read` |
| inspect compatibility | `GET <base>/schemas/compatibility` | `releases:read` |

All calls use an operator `rk_at_v1_*` bearer. PUT also requires one canonical
`Idempotency-Key: opn_*` and current compare-and-set revision.

## Policy shapes

| Mode | Required Release set |
|---|---|
| `atomic` | exactly one Release with `weightPercent: 100` |
| `gradual` | 2–16 distinct Releases; each weight `1..100`; total exactly 100 |

The server rejects zero-weight entries, duplicates, fractions, totals other than 100, and an atomic
policy with multiple Releases. Returned Releases are ordered canonically by Release ID; do not rely
on request order.

## Read the current policy

```sh
curl --fail-with-body \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/serving-policy"
```

Important fields:

| Field | Meaning |
|---|---|
| `policyRevision` | positive desired-policy CAS revision |
| `mode`, `releases` | complete desired routing policy |
| `observedState` | `pending`, `ready`, or `failed` |
| `observedPolicyRevision` | exact revision observed by serving path, or null |
| `converged` | true only when desired revision is observed ready |
| timestamps | canonical decimal Unix microseconds |

Do not send production traffic to `environment:default` until the desired policy is converged and
Product readiness/application canaries pass.

## Set an initial atomic policy

Use `expectedRevision: null` only when no policy exists:

```sh
curl --fail-with-body \
  -X PUT \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "idempotency-key: opn_01ARZ3NDEKTSV4RRFFQ69G5FAY" \
  -H "content-type: application/json" \
  --data-binary @- \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/serving-policy" <<'JSON'
{
  "expectedRevision": null,
  "mode": "atomic",
  "releases": [
    {"releaseId": "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV", "weightPercent": 100}
  ],
  "changedAtMicros": "1800000000000000"
}
JSON
```

The Release must belong to the exact Environment, be servable, have a verified artifact, and be
compatible with every other Release in the requested set. Clients cannot submit compatibility
hashes; Runku derives them from the Release authority.

The compact process stores the registry under the coordinated Product state. Backup format v2
quiesces the writer and archives the complete `product`, `platform`, and `files` roots with the
Platform PostgreSQL dump, so policy, operation/audit, Release metadata/artifacts, Environment
records, Cron activation, and subordinate Product data share one verified recovery point. External
storage profiles still require their provider recovery contract. An older binary that does not
understand an adopted serving authority must not resume writes after rollback.

## Start a gradual rollout

Read the current `policyRevision`, then completely replace the policy:

```sh
curl --fail-with-body \
  -X PUT \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "idempotency-key: opn_01ARZ3NDEKTSV4RRFFQ69G5FAZ" \
  -H "content-type: application/json" \
  --data-binary @- \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/serving-policy" <<'JSON'
{
  "expectedRevision": 1,
  "mode": "gradual",
  "releases": [
    {"releaseId": "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV", "weightPercent": 90},
    {"releaseId": "rel_01ARZ3NDEKTSV4RRFFQ69G5FB0", "weightPercent": 10}
  ],
  "changedAtMicros": "1800000000000001"
}
JSON
```

Success persists desired intent and increments the policy revision. Wait for `converged: true` at
that new revision before evaluating rollout metrics.

## Compatibility gate

A multi-Release policy requires every Release to have identical:

1. schema contract hash;
2. logical index contract hash;
3. complete ordered Cron declaration hash.

If any differs, PUT fails with `SERVING_POLICY_INCOMPATIBLE_CONTRACTS` and does not change desired
state. This is deliberately conservative: adding even a compatible optional schema field/index/
Cron declaration prevents those Releases from sharing one gradual policy in the current contract.

Use:

```sh
curl --fail-with-body \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/schemas/compatibility"
```

to inspect shared hashes, Releases, diagnostics, and convergence for the persisted policy. Hash
equality does not replace Release ownership, lifecycle, artifact-integrity, runtime, or
authorization checks.

## How a Release is selected

Selection is deterministic for one logical root operation:

- ordinary Query/Action: derived from request identity;
- Realtime: derived from subscription identity and pinned for reruns;
- Mutation: derived from operation ID so transport retry cannot cross weights;
- nested calls: inherit the already selected exact Release;
- scheduled work: stores/inherits an exact code pin.

One request never switches Releases mid-execution. A Channel or policy change does not retarget an
already active invocation/subscription/scheduled item.

Because selection is deterministic and not a globally random counter, a small validation sample
may not exactly match the configured percentage. Evaluate a sufficiently representative request/
operation population and record returned `releaseId`.

## Observe a rollout

Before increasing weight, compare candidate and current Release for:

- public status/error/latency and Function outcome codes;
- runtime queue, deadlines, heap/limit failures, and nested-call pressure;
- Mutation conflicts, attempts, commits, replay, and uncertain results;
- Realtime reconnect/resync and outbox/dispatcher lag;
- schedules/Cron outcomes and external effect reconciliation;
- storage/backend errors and quota/capacity;
- functional/application authorization denial changes;
- returned exact Release distribution.

Use bounded stable dimensions; do not turn user IDs, document IDs, arguments, object keys, or error
messages into metric labels. A policy response alone is not rollout success.

## Increase or finish rollout

Every change is another complete replacement using the current positive revision and a new
operation ID. Example finish:

```json
{
  "expectedRevision": 4,
  "mode": "atomic",
  "releases": [
    {"releaseId": "rel_01ARZ3NDEKTSV4RRFFQ69G5FB0", "weightPercent": 100}
  ],
  "changedAtMicros": "1800000000000004"
}
```

Retain the prior Release and data compatibility through the rollback window. Removing it from the
policy does not retire/delete it automatically.

## Roll back traffic

Rollback is another reviewed policy/Channel decision. For the serving-policy API, replace the
current policy with an atomic/gradual set containing the eligible known-good Release under the
current revision.

Rollback does not:

- undo documents or indexes written by candidate code;
- reverse configuration, file/Object Storage, or external-service effects;
- cancel already pinned schedules/Cron activations;
- downgrade `runku-server` or its databases.

Verify old code can read current data before shifting weight back.

## Desired versus observed policy

| State | Meaning |
|---|---|
| `pending` | desired policy committed but serving path has not confirmed it |
| `ready` at desired revision | exact policy resolved/verified and serves `environment:default` |
| `failed` at desired revision | serving path could not apply desired policy |
| observed older revision | the new desired revision remains unconverged |

Missing, pending, failed, unknown, or incompatible default policy fails closed. Runku does not fall
back to a Channel, arbitrary Release, or `latest`.

## Idempotency and conflicts

| Result | Safe operator action |
|---|---|
| exact replay | accept immutable result; no second policy revision is created |
| operation ID reused | stop; ID/body/path/precondition do not match |
| CAS conflict | GET current policy and decide again |
| result uncertain/timeout | query exact operation before repeating |
| Release not servable/not found | verify Release lifecycle/artifact/scope |
| incompatible contracts | use atomic cutover or redesign staged contract rollout |
| failed/unavailable observation | keep traffic on known path, inspect readiness/logs, correct cause |

Operation reconciliation:

```sh
curl --fail-with-body \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/serving-policy-operations/opn_..."
```

Use the same operation ID/body only for an exact retry. A new operation ID means new routing
intent.

## Backup and upgrade consequences

A valid recovery point coordinates serving policy/operations with Releases/artifacts, Channel
history, schema/index/Cron state, Environment lifecycle, and Product data. Restoring only routing
metadata can select missing or incompatible code.

Before upgrading the server, confirm the target version understands the persisted policy format.
After restore/upgrade, verify policy revision, convergence, compatibility hashes, Release artifact
integrity, deterministic Mutation replay, and representative traffic before reopening.

See [Releases and Workspaces](../development/releases-and-workspaces.md),
[remote lifecycle](../operations/remote-lifecycle.md), and
[operator handbook](../operations/operator-handbook.md).
