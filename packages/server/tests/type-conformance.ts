import type {
  ActionHandler,
  MutationHandler,
  QueryHandler,
  QueryDatabase,
  DocumentId,
  RunkuValue,
  TableReference,
  Infer,
} from "../src/index.js"

declare const rawTable: TableReference<"raw", RunkuValue, never>
declare const rawDocumentId: DocumentId<"raw">
import {
  action as defineAction,
  cron,
  defineSchema,
  defineTable,
  mutation as defineMutation,
  query as defineQuery,
  v,
  value,
} from "../src/index.js"

const query = (async (ctx) => {
  await ctx.log.debug("query input", { documentId: rawDocumentId })
  const document = await ctx.db.get(rawTable, rawDocumentId)
  await ctx.cooperate()
  return document?.value ?? null
}) satisfies QueryHandler<"db:read", null, RunkuValue>

const mutation = (async (ctx) => {
  await ctx.db.insert(rawTable, rawDocumentId, { status: "created" })
  return ctx.scheduler.runAfter(0n, "internal.finish", null)
}) satisfies MutationHandler<
  "db:write" | "scheduler:create",
  null,
  string
>

const action = (async (ctx) => {
  const response = await ctx.https.request({ method: "GET", url: "https://example.com" })
  return BigInt(response.status)
}) satisfies ActionHandler<"network:https", null, bigint>

const nestedQuery = (async (ctx, input) => {
  const result = await ctx.runQuery("queries.child", input)
  // @ts-expect-error Query callers cannot invoke a Mutation.
  void ctx.runMutation
  // @ts-expect-error Query callers cannot invoke an Action.
  void ctx.runAction
  return result
}) satisfies QueryHandler<"function:query", RunkuValue, RunkuValue>

const nestedMutation = (async (ctx, input) => {
  await ctx.runQuery("queries.child", input)
  const result = await ctx.runMutation("mutations.child", input)
  // @ts-expect-error Mutation callers cannot invoke an Action.
  void ctx.runAction
  return result
}) satisfies MutationHandler<"function:query" | "function:mutation", RunkuValue, RunkuValue>

const nestedAction = (async (ctx, input) => {
  await ctx.runQuery("queries.child", input)
  await ctx.runMutation("mutations.child", input)
  return ctx.runAction("actions.child", input)
}) satisfies ActionHandler<
  "function:query" | "function:mutation" | "function:action",
  RunkuValue,
  RunkuValue
>

const noNestedCapability = (async (ctx) => {
  // @ts-expect-error Nested methods require an explicit capability.
  void ctx.runQuery
  return null
}) satisfies ActionHandler<"network:https", null, null>

const messageValidator = v.object({
  body: v.string({ minBytes: 1, maxBytes: 200 }),
  tag: v.optional(v.string()),
})
const schema = defineSchema({
  messages: defineTable(messageValidator).index("by_body", ["body"]),
})

const messageInput = v.pick(messageValidator, ["body"])
type MessageInput = Infer<typeof messageInput>
const validMessageInput: MessageInput = { body: "hello" }
void validMessageInput
schema.indexes.messages.by_body satisfies string
// @ts-expect-error undeclared index names are absent
void schema.indexes.messages.missing

// @ts-expect-error indexes can reference only declared top-level document fields
defineTable(v.object({ body: v.string() })).index("invalid", ["missing"])

const declaredQuery = defineQuery({
  auth: "optional",
  visibility: "public",
  capabilities: ["auth:read", "db:read"],
  args: v.object({ documentId: v.documentId("messages"), tag: v.optional(v.string()) }),
  returns: v.union(v.null(), v.string()),
  async handler(ctx, input) {
    const document = await ctx.db.get(schema.tables.messages, input.documentId)
    const optionalTag: string | undefined = input.tag
    void optionalTag
    return document === null ? null : document.value.body
  },
})

const declaredMutation = defineMutation({
  auth: "service",
  visibility: "internal",
  capabilities: ["db:write", "scheduler:create"],
  args: v.object({ documentId: v.documentId("messages"), body: v.string() }),
  returns: v.string(),
  async handler(ctx, input) {
    await ctx.db.insert(schema.tables.messages, input.documentId, {
      body: input.body,
    })
    return ctx.scheduler.runAfter(0n, "internal.finish", null)
  },
})

declare const otherDocumentId: DocumentId<"other">
declare const queryDatabase: QueryDatabase
// @ts-expect-error a document ID for another table is rejected by a typed database call
void queryDatabase.get(schema.tables.messages, otherDocumentId)
declare const writableDatabase: import("../src/index.js").MutationWriteDatabase
declare const messageDocumentId: DocumentId<"messages">
// @ts-expect-error writes must satisfy the table document validator
void writableDatabase.insert(schema.tables.messages, messageDocumentId, { tag: "missing body" })

const declaredAction = defineAction({
  auth: "user",
  visibility: "public",
  capabilities: ["network:https"],
  args: v.null(),
  returns: v.int64(),
  async handler(ctx) {
    const response = await ctx.https.request({ method: "GET", url: "https://example.com" })
    // @ts-expect-error An Action cannot use the document database directly.
    void ctx.db
    return BigInt(response.status)
  },
})

const hourly = cron({
  schedule: "0 * * * *",
  function: "internal.finish",
  args: {
    attempt: value.int64(1n),
    at: value.timestamp(1n),
    id: value.id("doc_00000000000000000000000001"),
    bytes: value.bytes([1, 2, 3]),
  },
})

void [
  query,
  mutation,
  action,
  nestedQuery,
  nestedMutation,
  nestedAction,
  noNestedCapability,
  declaredQuery,
  declaredMutation,
  declaredAction,
  hourly,
  Runku.timestamp(1n),
  Runku.id("doc_00000000000000000000000001"),
]
