import { v } from "@runku/server"

export const task = v.object({
  title: v.string({ minBytes: 1, maxBytes: 120 }),
  done: v.boolean(),
  attachmentFileId: v.optional(v.string({ minBytes: 30, maxBytes: 30 })),
})
export const taskId = v.documentId("tasks")
export const taskView = v.object({ taskId, task })
export const taskList = v.array(taskView, { maxItems: 200 })
export const taskReference = v.object({ taskId })
export const uploadGrant = v.object({
  uploadId: v.string(),
  path: v.string(),
  token: v.string(),
  expiresAtMicros: v.string(),
  maxBytes: v.string(),
})
export const fileMetadata = v.object({
  fileId: v.string(),
  sizeBytes: v.string(),
  sha256: v.string(),
  contentType: v.string(),
  createdAtMicros: v.string(),
})
export const downloadGrant = v.object({
  path: v.string(),
  token: v.string(),
  expiresAtMicros: v.string(),
  metadata: fileMetadata,
})
