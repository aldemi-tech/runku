# Choose Query, Mutation, or Action

Every public operation in a Runku application is a Query, Mutation, or Action. The choice defines
what the handler may do, how Runku executes it, and what a caller may safely retry.

## Decision table

| Need | Use | Why |
|---|---|---|
| Read application data | Query | one read-only snapshot; eligible for Realtime |
| Create, replace, or delete documents | Mutation | atomic commit with optimistic-concurrency retry and durable operation identity |
| Schedule work as part of a data change | Mutation | schedules commit atomically with document/index/outbox changes |
| Upload/download application files | Action | storage is an external capability, outside the document transaction |
| Call an external HTTPS service | Action | external effects cannot be rolled back with application data |
| Run Node.js/npm code | Full Node Action | Query and Mutation always use the Safe runtime |
| Subscribe to changing results | public Query | Realtime reruns the Query after relevant committed changes |

The shortest correct rule is: **read with Query, transact with Mutation, perform effects with
Action**.

## Function layout and name

Create modules below `runku/` and export declarations:

```text
runku/orders.ts        export create  → orders.create
runku/admin/users.ts   export disable → admin.users.disable
```

A logical Function name is its relative module path plus export name. It is at most 128 UTF-8
bytes, begins with an ASCII letter, and may contain ASCII letters, digits, `_`, `.`, `/`, or `-`.
Treat it as a public API name.

Every declaration accepts six fields; only `handler` is required. The defaults are `auth: "none"`,
`visibility: "public"`, `capabilities: []`, `args: v.null()`, and `returns: v.any()`:

```ts
export const example = query({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read"],
  args: v.object({ id: v.documentId("notes") }),
  returns: v.union(v.null(), note),
  async handler(ctx, input) {
    // implementation
  },
})
```

| Field | What you decide |
|---|---|
| `auth` | Which functional principal is accepted |
| `visibility` | Whether an external client or only another Function can call it |
| `capabilities` | Which members Runku exposes on `ctx` |
| `args` | Exact runtime-validated input |
| `returns` | Exact runtime-validated result |
| `handler` | Application behavior |

Declarations must be statically extractable. Use literal metadata and validators declared with
`@runku/server`; do not compute `auth`, `visibility`, capabilities, or validators at runtime.

## Query

A Query reads one logical snapshot. All `get` and `scan` calls in one Query see that same snapshot,
even if another Mutation commits while the Query is running.

```ts
// runku/notes.ts
import { query, v } from "@runku/server"
import schema, { note } from "./schema.js"

export const get = query({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read"],
  args: v.object({ id: v.documentId("notes") }),
  returns: v.union(v.null(), note),
  async handler(ctx, input) {
    const principal = ctx.auth.principal
    if (principal === null || principal.kind !== "user") {
      throw new Error("user required")
    }

    const document = await ctx.db.get(schema.tables.notes, input.id)
    if (document === null) return null
    if (document.value.ownerId !== principal.id) {
      throw new Error("note access denied")
    }
    return document.value
  },
})
```

`ctx.db.get` returns `null` or:

| Field | Meaning |
|---|---|
| `documentId` | typed `doc_*` identity |
| `revision` | positive document revision used by replace/delete |
| `commitSequence` | Environment commit that produced this revision |
| `createdAt`, `updatedAt` | signed microsecond timestamps |
| `value` | schema-validated document |

A Query may derive deterministic IDs with `ctx.db.documentId(table, stableKey)` and scan a declared
index. It cannot insert, replace, delete, schedule work, use file storage, or perform HTTPS effects.

### Query and Realtime

A public Query is the only subscribable Function kind. Runku records its point/range dependencies.
After a Mutation commits a relevant change, Runku reruns the Query and sends a new authoritative
result. A Query must therefore remain deterministic for a given snapshot, identity, arguments,
configuration revision, and exact code pin.

Do not read the current wall clock, random state, filesystem, or a remote service from a Query.
Those capabilities are not present in the Safe context.

### Query limits

- each index scan requests `1..=1000` entries;
- all scans in one Query may return at most 10,000 entries in total;
- one Query may record at most 10,000 distinct dependencies;
- the configured invocation deadline includes queueing and Platform operations.

See [Documents, indexes, and concurrency](../data/documents-and-indexes.md) for scan behavior and
the current range-key limitation.

## Mutation

A Mutation is the only Function kind that writes application documents. Reads establish an
optimistic read set; writes remain buffered until the handler returns a valid result and the entire
commit succeeds.

```ts
import { mutation, v } from "@runku/server"
import schema, { note } from "./schema.js"

export const create = mutation({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read", "db:write"],
  args: v.object({
    title: v.string({ minLength: 1, maxLength: 200 }),
    body: v.string({ maxLength: 20_000 }),
  }),
  returns: v.object({ id: v.documentId("notes"), note }),
  async handler(ctx, input) {
    const principal = ctx.auth.principal
    if (principal === null || principal.kind !== "user") {
      throw new Error("user required")
    }

    // The same invocation intent derives the same ID across an OCC rerun.
    const id = ctx.db.documentId(schema.tables.notes, ctx.invocation.invocationId)
    const value = {
      ownerId: principal.id,
      title: input.title.trim(),
      body: input.body,
      priority: 0n,
      labels: [],
    }
    await ctx.db.insert(schema.tables.notes, id, value)
    return { id, note: value }
  },
})
```

### Insert, replace, and delete

```ts
// Insert requires the ID to be absent.
await ctx.db.insert(schema.tables.notes, id, value)

// Replace the complete document only if revision is still current.
const current = await ctx.db.get(schema.tables.notes, id)
if (current === null) return false
await ctx.db.replace(schema.tables.notes, id, current.revision, {
  ...current.value,
  title: nextTitle,
})

// Delete only the exact revision you observed.
await ctx.db.delete(schema.tables.notes, id, current.revision)
```

`replace` is a full replacement, not a partial patch. The replacement must satisfy the complete
table schema. Never fabricate a revision or catch a conflict and force the write: let Runku rerun
the Mutation from a fresh snapshot.

Mutations do not expose index scans in the current public Function API. Design writes around point
reads by deterministic/document IDs. When a write needs a lookup that only an index can provide,
resolve the document through a Query before the Mutation and re-check every security/precondition
inside the Mutation, or maintain an explicit deterministic lookup document.

### Atomic commit

One successful Mutation commit includes all buffered:

- document inserts, replacements, and deletes;
- derived index additions/removals;
- Realtime outbox records;
- schedules created by the Mutation;
- durable operation result used for replay.

If validation, application code, a limit, or commit fails, none of those buffered changes becomes
visible. Realtime never observes a pre-commit state.

### OCC retry and operation replay

Runku may rerun the complete Mutation handler up to three times after an optimistic-concurrency
conflict. Mutation code must not perform external effects; its available capabilities enforce that
rule.

The public caller supplies one `opn_*` operation ID. `@runku/client` creates it and preserves it
across retryable attempts. A committed replay with the same target, Function, arguments, scope, and
operation ID returns the original durable result. Reusing the ID for different intent fails.

Mutation limits in the current execution contract are 10,000 distinct document reads, 1,000
document writes, and 100 schedules per invocation. These are hard ceilings, not batch-size
recommendations; split large jobs into bounded, idempotent scheduled work.

## Action

An Action coordinates work that is not one atomic document transaction. It may use file storage,
scheduling, nested Functions, secrets, and—when the deployment provides it—mediated HTTPS.

```ts
import { action, v } from "@runku/server"

export const notifyLater = action({
  auth: "user",
  visibility: "public",
  capabilities: ["scheduler:create"],
  args: v.object({ notificationId: v.string({ minLength: 1, maxLength: 128 }) }),
  returns: v.string(),
  handler(ctx, input) {
    return ctx.scheduler.runAfter(
      5_000_000n,
      "notifications.deliver",
      input,
      { idempotencyKey: `notification:${input.notificationId}` },
    )
  },
})
```

Actions do not expose `ctx.db`. Read or change documents through explicit nested calls:

```ts
const order = await ctx.runQuery("orders.getInternal", { orderId })
await chargeExternalSystem(order)
await ctx.runMutation("orders.markPaid", { orderId, receiptId })
```

The Action must declare `function:query` and `function:mutation`. The called Functions should be
`visibility: "internal"` when clients must not call them directly.

### Effects and retry

An Action is never automatically retried by `@runku/client`. A timeout, cancellation, lost
response, or worker termination does not prove that an external effect did not happen.

For every effect:

1. create a stable application-level idempotency key before the call;
2. pass it to the downstream service when supported;
3. record/reconcile the downstream result through an idempotent Mutation;
4. on an uncertain result, query the downstream system before attempting the effect again.

### Safe Action and Full Node Action

Actions use the Safe runtime by default. It has no ambient filesystem, process, environment, Node
built-ins, npm resolution, or arbitrary sockets. Prefer Safe Actions and declared Platform
capabilities.

When Node APIs or production npm dependencies are essential, put this directive on the first line
of the Action module:

```ts
"use runku node"

import { createHash } from "node:crypto"
import { action, v } from "@runku/server"

export const digest = action({
  auth: "none",
  visibility: "public",
  capabilities: [],
  args: v.string({ maxLength: 1_000_000 }),
  returns: v.string({ minLength: 64, maxLength: 64 }),
  handler(_ctx, input) {
    return createHash("sha256").update(input).digest("hex")
  },
})
```

Local development supports Full Node on the developer machine. The compact Self-Hosted release
does not publish the shared-untrusted Full Node Agent/VM profile; do not treat its container as a
multi-tenant Node isolation boundary.

### HTTPS availability in the compact profile

`network:https` and `ctx.https.request` are public application contracts, but the current compact
Self-Hosted composition does not attach an HTTPS egress broker. A Function declaring it fails with
`ACTION_HTTPS_UNAVAILABLE`. Do not design a compact deployment around this capability until a
published profile explicitly supplies and documents the broker.

When available in a compatible deployment, the request surface is:

```ts
const response = await ctx.https.request({
  method: "POST",
  url: "https://payments.example.com/charges",
  headers: {
    "content-type": ["application/json"],
    "idempotency-key": [paymentId],
  },
  body: new TextEncoder().encode(JSON.stringify(payload)),
  idempotencyKey: paymentId,
})
```

The mediated contract bounds request/response bodies to at most 16 MiB, header aggregate to 64
KiB/256 values, an idempotency key to 128 bytes, and a call to at most one minute; deployment policy
may be stricter.

Do not raise the byte-array limit to move large payloads through the V8 heap. The current HTTPS
wire/runtime has no streaming or Application File/Object handle body mode, and compact Self-Hosted
still has no broker. A production large-body path remains gated on an additive handle contract that
authorizes an immutable Environment-scoped object, streams it through the egress broker, stores or
streams the response under an explicit quota, propagates cancellation, verifies size/digest, and
preserves Action uncertain-effect semantics. Use Application File grants or Runku Object Storage
multipart as the implemented large-byte path outside the Function envelope.

## Authentication and visibility

Authentication identifies the functional principal. It does not replace document authorization.

| `auth` | External caller accepted |
|---|---|
| `none` | no functional bearer required; any presented principal is discarded for this call |
| `optional` | no bearer, guest, user, or service |
| `guest` | guest, user, or service; absent is rejected |
| `user` | user only |
| `service` | service only for external calls |

Use `capabilities: ["auth:read"]` to receive `ctx.auth`. Then verify resource ownership, membership,
and application-specific scopes inside the Function. `auth: "user"` proves the principal kind; it
does not prove that the user owns `input.id`.

`visibility: "public"` allows the public Function API. `visibility: "internal"` permits only a
nested call in the already-authorized invocation tree. Internal visibility is not a substitute for
authorization checks: nested calls still apply their own `auth` policy.

## Capability matrix

| Capability | Query | Mutation | Action | Context member |
|---|:---:|:---:|:---:|---|
| `db:read` | yes | yes | no | `ctx.db.get`, `documentId`; Query also `scan` |
| `db:write` | no | yes | no | `ctx.db.insert`, `replace`, `delete` |
| `auth:read` | yes | yes | yes | `ctx.auth` |
| `function:query` | yes | yes | yes | `ctx.runQuery` |
| `function:mutation` | no | yes | yes | `ctx.runMutation` |
| `function:action` | no | no | yes | `ctx.runAction` |
| `scheduler:create` | no | yes | yes | `ctx.scheduler` |
| `storage:read` | no | no | yes | read methods on `ctx.storage` |
| `storage:write` | no | no | yes | write methods on `ctx.storage` |
| `network:https` | no | no | yes | `ctx.https` when the deployment attaches a broker |
| `variable:NAME` | yes | yes | yes | `ctx.env.get("NAME")` |
| `secret:NAME` | no | no | yes | `ctx.secrets.get("NAME")` |

The context member is absent unless the exact capability is declared. Configuration capabilities
are name-specific: `variable:API_ORIGIN` does not authorize `ctx.env.get("OTHER_NAME")`.

## Nested calls and exact code pinning

Use `ctx.runQuery`, `ctx.runMutation`, and `ctx.runAction`; do not import another Function handler
and call it directly. A nested call preserves:

- Project and Environment;
- exact Release or Dev Revision;
- application and functional identity subject to the child auth policy;
- deadline and cancellation;
- capability and visibility checks.

It never resolves a moved Channel again in the middle of the call tree. Default runtime limits are
8 nested edges and 100 nested calls for one root tree in the compact/local composition.

## Scheduling and Cron

Mutation and Action can declare `scheduler:create`:

```ts
await ctx.scheduler.runAt(
  executeAt.value,
  "orders.expire",
  { orderId },
  { idempotencyKey: `expire:${orderId}` },
)
```

`runAfter` receives relative microseconds; `runAt` receives absolute Unix microseconds. The maximum
scheduled delay is currently ten years. The target Function and arguments are validated before
durable creation, and the work item pins the exact code version. Delivery is at-least-once, so the
handler must deduplicate effects.

Declare recurring UTC work with `cron`:

```ts
import { cron, value } from "@runku/server"

export const hourlyCleanup = cron({
  schedule: "0 * * * *",
  function: "maintenance.cleanup",
  args: { batchSize: value.int64(100n) },
})
```

Cron targets an existing Mutation or Action. An operator separately activates/deactivates the
declaration. Use `value.int64`, `value.float64`, `value.timestamp`, `value.id`, and `value.bytes`
for non-JSON constants in Cron arguments.

## Context available to every Function

Every handler receives:

- `ctx.invocation`: exact Project, Environment, Release, request, invocation, Function, kind, and
  effective capabilities;
- `ctx.log.debug/info/warn/error`: bounded structured operational logs;
- `ctx.cooperate()`: an explicit cooperative yield/checkpoint for CPU loops.

Do not log arguments wholesale, credentials, configuration secrets, file grants, or customer
documents. Use stable bounded identifiers for correlation.

## Failure and retry checklist

| Situation | Safe caller behavior |
|---|---|
| Query transport/retryable error | retry under a bounded policy; result may reflect a newer snapshot |
| Mutation response lost | repeat exact target, Function, arguments, identity, and operation ID |
| Mutation conflict returned | read/reconcile current state; create a new operation ID only for new intent |
| Action response lost/timeout | assume effects may have happened; reconcile externally before retry |
| Scheduled handler repeats | deduplicate using durable application/downstream identity |
| Capability denied | change the declaration only if the Function genuinely requires that authority |
| Return validation fails | return a value matching `returns`; no Mutation commit is exposed |

Next: [Documents, indexes, and concurrency](../data/documents-and-indexes.md),
[TypeScript client](../reference/typescript-client.md), or
[HTTP API without an SDK](../reference/public-api.md).
