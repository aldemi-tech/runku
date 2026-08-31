import { mutation, query, v } from "@runku/server"
import type { PrincipalContext } from "@runku/server"
import schema from "./schema"
import { profile, upsertProfileArguments } from "./model"

function user(principal: PrincipalContext | null): PrincipalContext {
  if (principal === null || principal.kind !== "user") throw new Error("user required")
  return principal
}

export const upsert = mutation({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read", "db:write"],
  args: upsertProfileArguments,
  returns: profile,
  async handler(ctx, input) {
    if (ctx.auth.application?.assurance !== "verified") {
      throw new Error("verified application required")
    }
    const principal = user(ctx.auth.principal)
    const documentId = ctx.db.documentId(schema.tables.profiles, principal.id)
    const displayName = input.displayName.trim()
    if (displayName.length === 0) throw new Error("display name required")
    const next = { principalId: principal.id, displayName }
    const current = await ctx.db.get(schema.tables.profiles, documentId)
    if (current === null) {
      await ctx.db.insert(schema.tables.profiles, documentId, next)
    } else {
      const currentProfile = current.value
      if (currentProfile.principalId !== principal.id) throw new Error("profile ownership mismatch")
      if (currentProfile.displayName !== next.displayName) {
        await ctx.db.replace(schema.tables.profiles, documentId, current.revision, next)
      }
    }
    return next
  },
})

export const me = query({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read"],
  args: v.null(),
  returns: v.union(v.null(), profile),
  async handler(ctx) {
    const principal = user(ctx.auth.principal)
    const documentId = ctx.db.documentId(schema.tables.profiles, principal.id)
    const document = await ctx.db.get(schema.tables.profiles, documentId)
    if (document === null) return null
    const currentProfile = document.value
    if (currentProfile.principalId !== principal.id) throw new Error("profile ownership mismatch")
    return currentProfile
  },
})
