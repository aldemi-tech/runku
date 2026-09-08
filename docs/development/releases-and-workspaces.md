# Releases and Workspaces

## Development flow

`runku dev` publishes source snapshots to `workspace:local`. Remote development uses an `rk_dev_*`
credential and compare-and-swap Workspace revisions so multiple developers can share one
Environment without silently overwriting each other.

A failed source build does not replace the currently served Dev Revision. The CLI reports the build
failure and continues watching.

## Immutable deployment

A Release contains a canonical manifest, typed contracts, runtime descriptors, and content-addressed
artifacts. Publishing the same inputs produces the same build identity. Artifact reads verify size
and digest.

Promotion changes a Channel pointer after compatibility and readiness checks. Rollback selects a
previous immutable Release; it does not rebuild source. Freeze and every Channel move check the
complete `servable`/`active` Release closure, including exact-target reachability; a null baseline
or newly created Channel is not a compatibility bypass.

The current product also includes a [weighted serving policy](../concepts/serving-policy.md).
It records atomic or gradual desired intent, rejects incompatible Release sets, and resolves
`environment:default` to one deterministic exact Release before authorization and execution.
Explicit `release:*`, `channel:*`, and `workspace:*` targets keep their own semantics; a policy
update never silently rewrites one of those targets.

## Shared data

Workspaces and Releases in one Environment intentionally operate over the same data. This supports
debugging or previewing a fix against representative shared state. Protection rules can prohibit
Workspace targets and development synchronization in production Environments.

## Scheduled work

Scheduled invocations pin the exact Release or Dev Revision that created them. A later Channel move
does not change pending work. Cron activation similarly materializes work from a versioned manifest.

The authenticated Management API exposes console-safe Cron control and Scheduled history:

- `GET /v1/projects/{project}/environments/{environment}/crons?target=...` resolves the target once,
  verifies its artifact, returns code-owned declarations, and correlates each declaration with the
  current durable activation. It requires `cron:read`.
- `GET /v1/projects/{project}/environments/{environment}/scheduled?limit=...&after=...` returns at
  most 200 records in stable Scheduled Invocation ID order. It requires `schedules:read`, exposes
  canonical arguments and bounded error codes, and deliberately omits worker/lease identity.
- `PUT /v1/projects/{project}/environments/{environment}/crons/{name}/activation` requires
  `cron:activate`, `Idempotency-Key: opn_*`, the exact current activation revision, a verified code
  target, and a caller-pinned timestamp. It changes only the enabled state of a declaration already
  present in that immutable target. `GET .../cron-operations/{opn_*}` recovers a successful result
  after an uncertain response under `cron:read`.

These reads use the same Cron repository and logical store as the running local process. The
Management API does not create or edit code-owned Cron declarations, nor retry or cancel Scheduled
work. A disabled declaration is stored as durable operator intent: restart and manifest
reconciliation preserve it, while an explicit enable removes that override. Publishing still
atomically reconciles every other declaration from the exact manifest.

## Remote publication safety

Remote synchronization uses `rk_dev_*`, exact-origin HTTPS, bounded packages, artifact-first staged
persistence, compare-and-set Workspace HEAD, and state reconciliation after uncertain network
outcomes. Development credentials cannot invoke Functions.

## Compatibility and data evolution

Compatibility covers Function kinds/visibility/contracts, schema/index requirements,
runtime/artifact support, and pinned work. Optional schema fields may differ across Releases: reads
use the selected Release projection, and full replace preserves fields outside that Release view.
Shared data still requires expand → migrate/backfill → contract for required fields, types, and
indexes.
Do not expose code requiring unavailable data/indexes or remove a contract while a Release,
subscription, Cron activation, or scheduled invocation can still use it.
`servable`, `active`, and `deprecated` Releases therefore remain in compatibility preflight. After
the rollback window, the Management retirement operation fails closed until Channels, serving
policy, Cron activations, and pending/running schedules no longer retain the exact Release.

## Explicit local lifecycle

The named `make release-schema-coexistence-check` campaign proves the 0.5.1 candidate boundary in
one repeatable gate: two compatible Release schemas across Channels/rollback, a real V8 Function
read and old-Release replace that preserves a newer field, revision-bound serving evidence, and
streaming multipart composition. It deliberately keeps external databases, Docker, and browsers
out of the ordinary hosted check.

```sh
runku build
runku publish --manifest PATH_FROM_BUILD --artifact PATH_FROM_BUILD \
  --expected-head drv_observed
runku release --release rel_candidate --against stable
runku promote --channel stable --release rel_candidate --expected rel_current
runku status
```

Use paths returned by build. A stale pointer is an observable conflict requiring re-read and intent
reconciliation.

```sh
runku rollback --channel stable --expected rel_current --to rel_previous
```

Rollback changes routing only; it does not revert data, indexes, migrations, completed Actions, or
pending work pins.

## Release acceptance

- artifact digest/size/runtime descriptor verify;
- generated client types match the candidate;
- compatibility and data/index prerequisites are reviewed;
- identity/origin/capability changes are intentional;
- HTTP/Realtime smoke tests pass on `release:<id>`;
- scheduled/Cron coexistence is tested;
- rollback/forward-recovery limits are recorded;
- operator/status evidence is saved before and after promotion.
