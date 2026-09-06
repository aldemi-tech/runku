import { action, v } from "@runku/server"
import type { Infer } from "@runku/server"
import { downloadGrant, taskList, uploadGrant } from "./model"

function utf8(value: string): Uint8Array {
  const bytes: number[] = []
  for (const character of value) {
    const point = character.codePointAt(0)
    if (point === undefined) continue
    if (point <= 0x7f) bytes.push(point)
    else if (point <= 0x7ff) bytes.push(0xc0 | (point >> 6), 0x80 | (point & 0x3f))
    else if (point <= 0xffff) bytes.push(
      0xe0 | (point >> 12),
      0x80 | ((point >> 6) & 0x3f),
      0x80 | (point & 0x3f),
    )
    else bytes.push(
      0xf0 | (point >> 18),
      0x80 | ((point >> 12) & 0x3f),
      0x80 | ((point >> 6) & 0x3f),
      0x80 | (point & 0x3f),
    )
  }
  return Uint8Array.from(bytes)
}

export const beginUpload = action({
  auth: "none",
  visibility: "public",
  capabilities: ["storage:write"],
  args: v.object({ sizeBytes: v.int64({ minimum: 1, maximum: 5_000_000 }), contentType: v.string() }),
  returns: uploadGrant,
  handler: (ctx, input) => ctx.storage.createUpload({
    maxBytes: Number(input.sizeBytes),
    contentType: input.contentType,
  }),
})

export const beginDownload = action({
  auth: "none",
  visibility: "public",
  capabilities: ["storage:read"],
  args: v.object({ fileId: v.string() }),
  returns: downloadGrant,
  handler: (ctx, input) => ctx.storage.createDownload(input.fileId, { expiresInMicros: 60_000_000n }),
})

export const exportBoard = action({
  auth: "none",
  visibility: "public",
  capabilities: ["function:query", "storage:write", "storage:read"],
  args: v.null(),
  returns: downloadGrant,
  async handler(ctx) {
    const rows = await ctx.runQuery("tasks.exportData", null) as Infer<typeof taskList>
    const text = rows.map((row) => `${row.task.done ? "x" : " "} ${row.task.title}`).join("\n")
    const stored = await ctx.storage.store(utf8(text), { contentType: "text/plain" })
    return ctx.storage.createDownload(stored.fileId, { expiresInMicros: 60_000_000n })
  },
})

export const serviceHealth = action({
  auth: "service",
  visibility: "public",
  capabilities: [],
  args: v.null(),
  returns: v.string(),
  handler: () => "service-only-ok",
})
