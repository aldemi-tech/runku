import { mutation } from "@runku/server"
import type { Infer, PrincipalContext } from "@runku/server"
import schema from "./schema"
import { room, sendMessageArguments } from "./model"

function user(principal: PrincipalContext | null): PrincipalContext {
  if (principal === null || principal.kind !== "user") throw new Error("user required")
  return principal
}

export const send = mutation({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read", "db:write"],
  args: sendMessageArguments,
  returns: room,
  async handler(ctx, input) {
    const principal = user(ctx.auth.principal)
    const roomDocument = await ctx.db.get(schema.tables.rooms, input.roomId)
    if (roomDocument === null) throw new Error("room not found")
    const current = roomDocument.value
    const sender = current.members.find((candidate) => candidate.principalId === principal.id)
    if (sender === undefined) throw new Error("membership required")
    const body = input.body.trim()
    if (body.length === 0) throw new Error("message body required")
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(input.messageId)) {
      throw new Error("message id invalid")
    }
    if (current.messages.some((existing) => existing.id === input.messageId)) return current
    const next = {
      ...current,
      messages: [
        ...current.messages,
        {
          id: input.messageId,
          senderId: principal.id,
          senderName: sender.displayName,
          body,
          clientSentAt: input.clientSentAt,
        },
      ].slice(-200),
    } satisfies Infer<typeof room>
    await ctx.db.replace(
      schema.tables.rooms,
      input.roomId,
      roomDocument.revision,
      next,
    )
    return next
  },
})
