# `@runku/server`

Declarative SDK for Runku schema, validators, indexes, Functions, and Cron. Application source under
`runku/` is the source of truth; `runku build` extracts static metadata and generates client types.

## Schema

```ts
import { defineSchema, defineTable, v } from "@runku/server"

export const schema = defineSchema({
  rooms: defineTable({
    name: v.string(),
    ownerId: v.string(),
  }).index("by_name", ["name"]),
})
```

Table IDs, documents, indexes, and validators flow into Function context and generated API types.

## Functions

```ts
import { mutation, v } from "@runku/server"

export const create = mutation({
  args: { name: v.string() },
  handler: async (ctx, input) => {
    return ctx.db.insert("rooms", { name: input.name, ownerId: ctx.identity.subject })
  },
})
```

Capability-scoped contexts expose only the operations allowed by Query, Mutation, or Action.

## Full Node modules

```ts
"use runku node"

import { action, v } from "@runku/server"
import { createHash } from "node:crypto"

export const digest = action({
  args: { value: v.string() },
  handler: async (_ctx, input) => createHash("sha256").update(input.value).digest("hex"),
})
```

The directive applies to the complete file. Safe and Node Functions can call each other through
`ctx.runQuery`, `ctx.runMutation`, `ctx.runAction`, and `ctx.scheduler`.

## Development

```bash
pnpm --dir packages/server check
```
