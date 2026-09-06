import { mutation, query, v } from "@runku/server"
import schema from "./schema"
import { task, taskId, taskList, taskReference, taskView } from "./model"

export const list = query({
  auth: "none",
  visibility: "public",
  capabilities: ["db:read"],
  args: v.null(),
  returns: taskList,
  async handler(ctx) {
    const entries = await ctx.db.scan(schema.indexes.tasks.by_title, { limit: 200 })
    const documents = await Promise.all(entries.map((entry) => ctx.db.get(schema.tables.tasks, entry.documentId)))
    return documents.flatMap((document) => document === null ? [] : [{
      taskId: document.documentId,
      task: document.value,
    }])
  },
})

export const create = mutation({
  auth: "none",
  visibility: "public",
  capabilities: ["db:read", "db:write"],
  args: v.object({ title: v.string({ minBytes: 1, maxBytes: 120 }) }),
  returns: taskView,
  async handler(ctx, input) {
    const next = { title: input.title.trim(), done: false }
    if (next.title.length === 0) throw new Error("title required")
    const taskId = ctx.db.documentId(schema.tables.tasks, ctx.invocation.invocationId)
    await ctx.db.insert(schema.tables.tasks, taskId, next)
    return { taskId, task: next }
  },
})

export const toggle = mutation({
  auth: "none",
  visibility: "public",
  capabilities: ["db:read", "db:write"],
  args: taskReference,
  returns: task,
  async handler(ctx, input) {
    const current = await ctx.db.get(schema.tables.tasks, input.taskId)
    if (current === null) throw new Error("task not found")
    const next = { ...current.value, done: !current.value.done }
    await ctx.db.replace(schema.tables.tasks, input.taskId, current.revision, next)
    return next
  },
})

export const attach = mutation({
  auth: "none",
  visibility: "public",
  capabilities: ["db:read", "db:write"],
  args: v.object({ taskId, fileId: v.string() }),
  returns: task,
  async handler(ctx, input) {
    const current = await ctx.db.get(schema.tables.tasks, input.taskId)
    if (current === null) throw new Error("task not found")
    const next = { ...current.value, attachmentFileId: input.fileId }
    await ctx.db.replace(schema.tables.tasks, input.taskId, current.revision, next)
    return next
  },
})

export const exportData = query({
  auth: "none",
  visibility: "internal",
  capabilities: ["db:read"],
  args: v.null(),
  returns: taskList,
  async handler(ctx) {
    const entries = await ctx.db.scan(schema.indexes.tasks.by_title, { limit: 200 })
    const documents = await Promise.all(entries.map((entry) => ctx.db.get(schema.tables.tasks, entry.documentId)))
    return documents.flatMap((document) => document === null ? [] : [{
      taskId: document.documentId,
      task: document.value,
    }])
  },
})
