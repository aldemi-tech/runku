import { mutation, query, v } from "@runku/server"
import type { DocumentId, Infer, MutationReadDatabase, PrincipalContext } from "@runku/server"
import schema from "./schema"
import {
  createRoomArguments,
  createdRoom,
  room,
  roomDirectory,
  roomReference,
} from "./model"

type ReadDatabase = Pick<MutationReadDatabase, "documentId" | "get">

function user(principal: PrincipalContext | null): PrincipalContext {
  if (principal === null || principal.kind !== "user") throw new Error("user required")
  return principal
}

async function requireProfile(
  db: ReadDatabase,
  principal: PrincipalContext,
){
  const documentId = db.documentId(schema.tables.profiles, principal.id)
  const document = await db.get(schema.tables.profiles, documentId)
  if (document === null) throw new Error("profile required")
  const profile = document.value
  if (profile.principalId !== principal.id) throw new Error("profile ownership mismatch")
  return profile
}

async function requireRoom(
  db: ReadDatabase,
  id: DocumentId<"rooms">,
) {
  const document = await db.get(schema.tables.rooms, id)
  if (document === null) throw new Error("room not found")
  return { room: document.value, revision: document.revision }
}

export const create = mutation({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read", "db:write"],
  args: createRoomArguments,
  returns: createdRoom,
  async handler(ctx, input) {
    const principal = user(ctx.auth.principal)
    const profile = await requireProfile(ctx.db, principal)
    const name = input.name.trim()
    if (name.length === 0) throw new Error("room name required")
    const next = {
      name,
      ownerId: principal.id,
      members: [{ principalId: principal.id, displayName: profile.displayName }],
      messages: [],
    } satisfies Infer<typeof room>
    const roomId = ctx.db.documentId(schema.tables.rooms, ctx.invocation.invocationId)
    await ctx.db.insert(schema.tables.rooms, roomId, next)
    return { roomId, room: next }
  },
})

export const join = mutation({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read", "db:write"],
  args: roomReference,
  returns: room,
  async handler(ctx, input) {
    const principal = user(ctx.auth.principal)
    const profile = await requireProfile(ctx.db, principal)
    const current = await requireRoom(ctx.db, input.roomId)
    if (current.room.members.some((candidate) => candidate.principalId === principal.id)) {
      return current.room
    }
    if (current.room.members.length >= 100) throw new Error("room is full")
    const next = {
      ...current.room,
      members: [
        ...current.room.members,
        { principalId: principal.id, displayName: profile.displayName },
      ],
    } satisfies Infer<typeof room>
    await ctx.db.replace(schema.tables.rooms, input.roomId, current.revision, next)
    return next
  },
})

export const get = query({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read"],
  args: roomReference,
  returns: room,
  async handler(ctx, input) {
    const principal = user(ctx.auth.principal)
    const current = await requireRoom(ctx.db, input.roomId)
    if (!current.room.members.some((candidate) => candidate.principalId === principal.id)) {
      throw new Error("membership required")
    }
    return current.room
  },
})

export const list = query({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read"],
  args: v.null(),
  returns: roomDirectory,
  async handler(ctx) {
    const principal = user(ctx.auth.principal)
    const entries = await ctx.db.scan(schema.indexes.rooms.by_name, { limit: 100 })
    const documents = await Promise.all(
      entries.map((entry) => ctx.db.get(schema.tables.rooms, entry.documentId)),
    )
    return documents.flatMap((document) => {
      if (document === null) return []
      return [{
        roomId: document.documentId,
        name: document.value.name,
        memberCount: BigInt(document.value.members.length),
        joined: document.value.members.some(
          (candidate) => candidate.principalId === principal.id,
        ),
      }]
    })
  },
})
