import { v } from "@runku/server"

export const principalId = v.string({ minBytes: 16, maxBytes: 96 })
export const displayName = v.string({ minBytes: 1, maxBytes: 48 })
export const roomName = v.string({ minBytes: 1, maxBytes: 80 })
export const messageId = v.string({ minBytes: 36, maxBytes: 36 })
export const messageBody = v.string({ minBytes: 1, maxBytes: 1_000 })

export const profile = v.object({ principalId, displayName })
export const member = v.object({ principalId, displayName })
export const message = v.object({
  id: messageId,
  senderId: principalId,
  senderName: displayName,
  body: messageBody,
  clientSentAt: v.timestamp(),
})
export const room = v.object({
  name: roomName,
  ownerId: principalId,
  members: v.array(member, { minItems: 1, maxItems: 100 }),
  messages: v.array(message, { maxItems: 200 }),
})

export const roomId = v.documentId("rooms")
export const roomReference = v.object({ roomId })
export const roomSummary = v.object({
  roomId,
  name: roomName,
  memberCount: v.int64({ minimum: 1, maximum: 100 }),
  joined: v.boolean(),
})
export const roomDirectory = v.array(roomSummary, { maxItems: 100 })
export const createdRoom = v.object({ roomId, room })
export const createRoomArguments = v.pick(room, ["name"])
export const upsertProfileArguments = v.pick(profile, ["displayName"])
export const sendMessageArguments = v.object({
  roomId,
  messageId,
  body: messageBody,
  clientSentAt: v.timestamp(),
})
