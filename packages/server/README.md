# `@runku/server`

Declarative TypeScript SDK for schema, validators, indexes, Query/Mutation/Action, Cron, canonical
values, and capability-scoped Function context. Source under `runku/` is authoritative;
`runku build` extracts static metadata and generates client contracts.

The helper objects support TypeScript authoring. The Rust builder/runtime independently validate
metadata, contracts, capabilities, source policy, and values.

```sh
npm install @runku/server
```

## Schema

Exactly one module under `runku/` must default-export a schema:

```ts
import { defineSchema, defineTable, v } from "@runku/server"

export const note = v.object({
  ownerId: v.string({ minBytes: 1, maxBytes: 256 }),
  title: v.string({ minBytes: 1, maxBytes: 200 }),
  archived: v.boolean(),
})

export default defineSchema({
  notes: defineTable(note)
    .index("by_owner", ["ownerId"])
    .index("by_owner_archived", ["ownerId", "archived"]),
})
```

`schema.tables.notes` is a typed table reference; `schema.indexes.notes.by_owner` is a typed index
reference. IDs are derived from Project and logical names. Never hard-code physical `tbl_*`/`idx_*`
values.

## Validators

`v` exposes:

- `any`, `null`, `boolean`;
- `int64({ minimum, maximum })`, represented by `bigint`;
- `float64({ minimum, maximum })`, represented by `number`;
- `string({ minBytes, maxBytes })`, `bytes({ minBytes, maxBytes })`;
- `timestamp`, `id(kind?)`, `documentId(table)`;
- `array(item, { minItems, maxItems })`;
- `object(fields)`, `pick(object, keys)`, `union(...)`, `optional(value)`.

Use `Infer<typeof validator>` for helpers without duplicating interfaces. Bounds are part of the
runtime contract, not TypeScript-only documentation.

## Functions

```ts
import { mutation, v } from "@runku/server"
import schema, { note } from "./schema.js"

export const create = mutation({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read", "db:write"],
  args: v.object({ title: v.string({ minBytes: 1, maxBytes: 200 }) }),
  returns: v.object({ id: v.documentId("notes"), note }),
  async handler(ctx, input) {
    const principal = ctx.auth.principal
    if (principal === null || principal.kind !== "user") throw new Error("user required")
    const id = ctx.db.documentId(schema.tables.notes, ctx.invocation.invocationId)
    const value = { ownerId: principal.id, title: input.title, archived: false }
    await ctx.db.insert(schema.tables.notes, id, value)
    return { id, note: value }
  },
})
```

All fields are required and statically extractable. `auth` is `none|optional|guest|user|service`;
`visibility` is `public|internal`.

### Capability matrix

| Capability | Query | Mutation | Action | Context member |
|---|:---:|:---:|:---:|---|
| `db:read` | yes | yes | no | `ctx.db.get/documentId/scan` as applicable |
| `db:write` | no | yes | no | `ctx.db.insert/replace/delete` |
| `auth:read` | yes | yes | yes | `ctx.auth` |
| `function:query` | yes | yes | yes | `ctx.runQuery` |
| `function:mutation` | no | yes | yes | `ctx.runMutation` |
| `function:action` | no | no | yes | `ctx.runAction` |
| `network:https` | no | no | yes | `ctx.https.request` |
| `scheduler:create` | no | yes | yes | `ctx.scheduler.runAfter/runAt` |

Every context also exposes `ctx.invocation`, cooperative yield, and bounded structured `ctx.log`.

## Data operations

Queries can `get`, derive a typed `documentId`, and `scan` a typed index with explicit bounds/limit.
Mutations read documents and `insert`, `replace(expectedRevision)`, or
`delete(expectedRevision)`. Mutation writes commit atomically with logical indexes, outbox, and
schedules. Actions access data through nested Query/Mutation instead of direct writes.

## Full Node Actions

```ts
"use runku node"

import { createHash } from "node:crypto"
import { action, v } from "@runku/server"

export const digest = action({
  auth: "none",
  visibility: "public",
  capabilities: [],
  args: v.string(),
  returns: v.string(),
  handler(_ctx, input) {
    return createHash("sha256").update(input).digest("hex")
  },
})
```

The directive must be first and applies to the reachable module graph. It does not change the
declaration API. Query, Mutation, and Cron remain Safe. Cross-runtime calls use `ctx.run*`, not
Function imports. Remote OCI builds require `package-lock.json`.

## HTTPS and scheduling

An Action with `network:https` calls the mediated HTTPS broker. A Mutation/Action with
`scheduler:create` can schedule an eligible Function:

```ts
await ctx.scheduler.runAfter(
  5_000_000n,
  "notifications.deliver",
  argumentsValue,
  { idempotencyKey: "notification:123" },
)
```

Times are microseconds. Durable delivery is at-least-once; external effects require idempotency.

## Cron and canonical constants

```ts
import { cron, value } from "@runku/server"

export const hourly = cron({
  schedule: "0 * * * *",
  function: "maintenance.compact",
  args: { attempt: value.int64(1n) },
})
```

`value.int64`, `float64`, `timestamp`, `id`, and `bytes` represent non-JSON canonical constants in
Cron arguments.

## Source constraints

Safe source accepts static relative imports inside `runku/` plus `@runku/server`. Dynamic imports,
path escapes, source symlinks, ambiguous re-exports, top-level await, unsupported runtime mixing,
and computed declaration metadata fail closed. Full Node may resolve built-ins/npm within its
isolated module graph.

## Generated client registry

`runku build` writes immutable Release-specific types and updates
`runku/_generated/api.d.ts`. It includes public/internal Function kind, visibility, canonical
arguments, and result types. Do not edit it.

## Development

```sh
pnpm --dir packages/server check
```

The package check builds and runs type conformance. Changes to declarations also require builder,
runtime, generated-contract, and example gates described in [`AGENTS.md`](../../AGENTS.md).
