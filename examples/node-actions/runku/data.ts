import { mutation, query, v } from "@runku/server"
import { storedKey, storedValue } from "./contracts.js"
import schema from "./schema.js"

export const put = mutation({
  auth: "none",
  visibility: "internal",
  capabilities: ["db:read", "db:write"],
  args: storedValue,
  returns: v.string(),
  async handler(ctx, input) {
    const id = ctx.db.documentId(schema.tables.values, input.key)
    const current = await ctx.db.get(schema.tables.values, id)
    if (current === null) await ctx.db.insert(schema.tables.values, id, input)
    else await ctx.db.replace(schema.tables.values, id, current.revision, input)
    return input.value
  },
})

export const get = query({
  auth: "none",
  visibility: "internal",
  capabilities: ["db:read"],
  args: storedKey,
  returns: v.union(v.null(), v.string()),
  async handler(ctx, input) {
    const id = ctx.db.documentId(schema.tables.values, input.key)
    return (await ctx.db.get(schema.tables.values, id))?.value.value ?? null
  },
})
