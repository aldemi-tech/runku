# Application development workflow

This guide connects everyday application work to Runku's durable contracts. The short loop is
`edit → build → Dev Revision`; the delivery loop is `freeze → Release → Channel`. Neither loop has
an implicit `latest` target.

## Responsibilities and artifacts

| Actor or component | Responsibility | Durable result |
|---|---|---|
| Application developer | Define schema, functions, capabilities, auth, returns, and Cron declarations | Source under `runku/` |
| `@runku/server` | Provide typed declarations and validators | Compile-time handler context and build metadata |
| Builder | Snapshot source, evaluate declarations, validate contracts, bundle code, generate client types | Content-addressed artifact + versioned manifest |
| Workspace publisher | Store immutable Dev Revisions and move one mutable Workspace HEAD under CAS | Revision and operation journal |
| Release service | Freeze an eligible Dev Revision and register its immutable code/contracts | Release record |
| Serving service | Validate compatible Release sets and change Channel policy | Monotonic serving policy revision |
| `@runku/client` | Encode public calls, preserve operation identity, reconnect Realtime | Protocol requests; no server authority |

## Structure an application

```text
application-root/
├── package.json
├── tsconfig.json
├── runku/
│   ├── schema.ts
│   ├── orders.ts
│   ├── billing.ts
│   └── _generated/
│       ├── api.js
│       ├── api.d.ts
│       ├── server.js
│       └── server.d.ts
└── src/
```

The root and `runku/` directory must be regular, non-symlinked paths and cannot be a filesystem or
home root. Runku discovers `.ts`, `.mts`, `.js`, and `.mjs` modules. A function's stable logical
name is the relative module path plus export name: `runku/admin/users.ts` export `disable` becomes
`admin.users.disable`.

Exactly one default schema export defines logical tables and indexes. Application code uses the
generated schema references, not physical repository identifiers. The builder rejects path escape,
unstable source snapshots, ambiguous schema, unsupported imports, invalid declarations, and
unknown format/runtime versions.

## Design each Function deliberately

Every declaration answers six questions:

| Field | Decision |
|---|---|
| `auth` | Which functional principal kind is required? |
| `visibility` | Can the public protocol call it, or only another Function? |
| `capabilities` | Which context surfaces and exact configuration names are available? |
| `args` | Which bounded canonical input is accepted? |
| `returns` | Which bounded canonical result is promised? |
| `handler` | Which code implements the contract for this artifact? |

Use the smallest function type that fits:

- **Query** reads one snapshot, records dependencies for Realtime, and cannot write or perform
  external effects.
- **Mutation** may read/write documents and schedules. Its document/index/outbox/schedule changes
  commit atomically under optimistic concurrency. Internal retries are possible before commit.
- **Action** may use mediated HTTPS/files and call other Functions when declared. It is not
  automatically retried because an external effect may already have occurred.

`auth: "user"` authenticates a caller; it does not authorize access to a particular document. Keep
ownership/tenant checks inside the Function and bind indexed queries to the verified principal.

## Capabilities are part of the Release contract

Capabilities narrow TypeScript context and are validated again by the builder and runtime. Safe V8
provides no ambient filesystem, process, Node package, environment, or network authority.

| Intent | Capability | Context surface |
|---|---|---|
| Read documents | `db:read` | `ctx.db.get`, `scan`, `documentId` |
| Commit documents | `db:write` with `db:read` as required | `ctx.db.insert`, `replace`, `delete` |
| Inspect functional identity | `auth:read` | `ctx.auth` |
| Nested call | `function:query`, `function:mutation`, `function:action` | matching `ctx.run*` method |
| Schedule work | `scheduler:create` | `ctx.scheduler` |
| Mediated HTTPS | `network:https` | `ctx.https` |
| Application files | `storage:read`, `storage:write` | `ctx.storage` |
| Non-secret configuration | `variable:NAME` | `ctx.env.get("NAME")` |
| Secret configuration (Action only) | `secret:NAME` | `ctx.secrets.get("NAME")` |

Adding a capability broadens authority and changes the manifest. Review it like an API change.
Renaming a configuration name or changing function visibility can break callers even if the
TypeScript signature looks similar.

## Run the local loop

Prepare state and inspect generated configuration without starting a long-running process:

```sh
runku dev --prepare
runku doctor
```

Then start the full loop:

```sh
runku dev --origin http://localhost:3000
```

The CLI builds current source, publishes it as an immutable Dev Revision, advances Workspace
`local` only after success, starts the gateway/runtime/workers on loopback, and watches source.
Failed builds preserve the last valid HEAD. Stop with the normal process signal so the supervisor
can drain and close repositories; do not delete `.runku/` to resolve a build error.

Use `--prebuilt` only to serve already-published state. Use `--auth-config RELATIVE` for a
Product-root-relative JWT provider descriptor. Each `--origin` is an exact browser origin; requests
without Origin remain eligible for server-to-server use.

Success evidence:

- `/healthz` answers liveness and `/readyz` answers admission readiness;
- `runku status` shows the intended Workspace HEAD;
- generated types correspond to the current immutable build;
- a public Query succeeds with the expected application/functional identities;
- `runku logs` correlates request, invocation, function, and exact target.

## Build reproducibly

```sh
runku build
```

Read `manifestPath`, `artifactPath`, Release ID, and build metadata from JSON stdout. The build lives
under `.runku/builds-v1/rel_*`; generated Release-specific types are preserved and
each runtime/declaration file below `runku/_generated` is replaced through an atomic rename for
application imports. Never edit generated or immutable build output.

For reproducible automation, provide the complete metadata tuple together:

```sh
runku build \
  --release-id rel_... \
  --build-id bld_... \
  --created-at-micros 1780000000000000
```

Partial tuples fail. Repeating an exact tuple over identical source should produce the same
canonical contract and content digests; changed input must not masquerade as the same artifact.

## Test at the correct boundaries

An application test suite should cover:

1. validator boundary values and malformed canonical values;
2. authentication absent/wrong kind/expired and document authorization denial;
3. Query snapshot behavior and index range boundaries;
4. Mutation conflict, replay with the same operation ID, and atomic rollback on failure;
5. Action partial-effect behavior and downstream idempotency keys;
6. nested-call pinning, visibility, recursion/depth, and capability denial;
7. Realtime initial value, irrelevant commit, relevant commit, reconnect, reauth, and resync;
8. schedule/Cron at-least-once delivery and idempotent handler behavior;
9. file grant expiry/replay, size/hash mismatch, range requests, and cleanup;
10. configuration missing, wrong kind, rotation during invocations, and absence from logs.

Unit-test pure logic outside a Function where practical. Run integration tests against `runku dev`
using explicit base URL, target, application credential, and bearer supplier. Do not make tests
depend on an implicit current Channel.

## Publish to a linked Environment

```sh
runku login --url https://management.example.com
runku link --project-id prj_... --environment-id env_...
runku build
runku publish --remote \
  --manifest /path/from-build/manifest \
  --artifact /path/from-build/artifact \
  --expected-head empty
```

Publication creates an immutable Dev Revision and updates the selected Workspace HEAD under
compare-and-set. If another developer won the race, exit `4` is a request to read and reconcile—not
to retry with a fabricated expected revision.

For a Node/hybrid build, the target Environment must explicitly enable a compatible Full Node
runtime; otherwise publication fails before HEAD moves. The direct bundle carries built source and
contracts but not `node_modules`, so use Node built-ins or source already included in the compiled
graph. External npm dependencies continue to require the package-lock-bound OCI publication path.

Freeze and promote only after application tests and schema compatibility evidence:

```sh
runku release --remote --release rel_...
runku promote --remote --channel preview --release rel_... --expected empty
runku status --remote
```

A Channel can target a compatible weighted set through the Management serving-policy API. Every
individual request, subscription lifetime, nested call chain, Cron activation, and scheduled
invocation still pins one exact Release or Dev Revision.

## Change contracts safely

Classify changes before publishing:

- **compatible additive:** optional field, new Function, or index addition that old consumers can
  ignore;
- **behavioral:** same shape but changed authorization, timing, retry, limit, or effect;
- **breaking:** old clients, stored values, manifests, or Release sets cannot operate;
- **security fix:** intentionally rejects behavior that was previously accepted.

For stored schema, use expand → migrate/backfill → contract. Keep old and new Release contracts
compatible for the full mixed-serving and rollback window. Do not delete an index or required field
while any eligible Release or scheduled item still depends on it.

## Diagnose without destroying evidence

| Symptom | Read first | Safe response |
|---|---|---|
| Build rejected | stable CLI code + source path/module diagnostics | fix source; last valid Workspace HEAD remains |
| Publish conflict | remote status and expected HEAD | reconcile ownership; use a new operation only for new intent |
| Invocation denied | request ID, target, application scope, principal, manifest capability | correct the narrowest failing axis |
| Mutation uncertain | operation ID and current durable state | reconcile; never issue a new operation blindly |
| Action response lost | downstream idempotency record and logs | assume the effect may have happened |
| Realtime gap | reconnect/resync signal and Query result | accept the fresh snapshot; do not synthesize missed changes |

Use [Functions and runtimes](functions-and-runtimes.md) for runtime details,
[Data and Realtime](../data/data-and-realtime.md) for consistency, and
[Releases and Workspaces](../development/releases-and-workspaces.md) for the delivery state machine.
