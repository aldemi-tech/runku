# Documents, indexes, and concurrency

Runku stores schema-validated documents in logical tables. Queries read a consistent snapshot;
Mutations change documents and their derived indexes atomically. Application code never opens a
database or external object-storage provider directly for document data.

## The document model

A document has an opaque typed ID and a complete value:

```ts
interface DataDocument<T> {
  readonly tableId: string
  readonly documentId: DocumentId<"notes">
  readonly revision: bigint
  readonly commitSequence: bigint
  readonly createdAt: RunkuTimestamp
  readonly updatedAt: RunkuTimestamp
  readonly value: T
}
```

| Field | Use it for |
|---|---|
| `documentId` | stable application reference; never infer authorization from it |
| `revision` | compare-and-set precondition for replace/delete |
| `commitSequence` | correlate the Environment commit that produced this revision |
| timestamps | display/audit decisions; values are Unix microseconds |
| `value` | the complete document validated by the selected schema |

Document IDs are table-typed in TypeScript but use the canonical `doc_*` wire form. A document ID
does not embed its owner, table, or Environment in readable text.

## Choose document identity deliberately

Derive a deterministic ID when the application already has a stable unique key:

```ts
const id = ctx.db.documentId(schema.tables.profiles, principal.id)
```

The stable key must be non-empty and at most 1,024 bytes. The derived ID is deterministic for the
logical table and key, and the same key in another table produces a different document ID.

Common patterns:

| Entity | Stable key example |
|---|---|
| one profile per principal | verified `principal.id` |
| one cart per user | `principal.id` |
| idempotent creation inside a Mutation | `ctx.invocation.invocationId` |
| external object mirrored once | provider + canonical external ID |

Do not use display names, mutable email addresses, unnormalized URLs, or secrets as stable keys.
When clients need a random-looking document, derive it from the Mutation invocation ID and return
the resulting `DocumentId`.

## Read one document

Queries and Mutations with `db:read` can perform point reads:

```ts
const document = await ctx.db.get(schema.tables.notes, input.id)
if (document === null) return null

if (document.value.ownerId !== principal.id) {
  throw new Error("note access denied")
}

return {
  note: document.value,
  revision: document.revision,
}
```

All Query reads share one snapshot. Mutation reads share the attempt's snapshot and become OCC
preconditions. A miss is also meaningful: if another writer inserts that document before commit,
the Mutation attempt conflicts and reruns.

## Insert a document

`insert` is available only to a Mutation declaring `db:write`:

```ts
const id = ctx.db.documentId(schema.tables.notes, ctx.invocation.invocationId)

await ctx.db.insert(schema.tables.notes, id, {
  ownerId: principal.id,
  title: input.title,
  body: input.body,
  priority: 0n,
  labels: [],
})
```

The ID must be absent. The value must match the complete table schema. Runku derives every index
entry from this value; there is no separate index update.

## Replace a document

Runku exposes full replacement rather than patch semantics:

```ts
const current = await ctx.db.get(schema.tables.notes, input.id)
if (current === null) return { updated: false }

if (current.value.ownerId !== principal.id) {
  throw new Error("note access denied")
}

await ctx.db.replace(
  schema.tables.notes,
  input.id,
  current.revision,
  {
    ...current.value,
    title: input.title,
  },
)

return { updated: true }
```

Pass the exact positive revision returned by `get`. If another Mutation changes the document,
commit conflicts and Runku reruns the handler from a fresh snapshot, up to the current attempt
limit. The replacement value is validated again and its old/new index entries change atomically.

Do not implement a blind last-write-wins update by accepting a revision from an untrusted client
without reading and authorizing the current document in the Mutation.

## Delete a document

```ts
const current = await ctx.db.get(schema.tables.notes, input.id)
if (current === null) return false
if (current.value.ownerId !== principal.id) throw new Error("note access denied")

await ctx.db.delete(schema.tables.notes, input.id, current.revision)
return true
```

Delete removes the document and every derived index entry in the same commit. An already-missing
document can be treated as success when that matches application semantics.

## Mutation concurrency and idempotency

There are two separate mechanisms:

1. **Optimistic concurrency** checks the documents read and the revisions passed to writes. Runku
   may rerun the handler when another commit wins first.
2. **Operation identity** makes a public Mutation request replayable after a transport failure. An
   exact committed replay returns the original result.

Application Mutation code must therefore be deterministic and effect-free. Do not send email,
charge a card, write a file, or call a remote service from a Mutation. Use an Action and reconcile
the effect with a separate idempotent Mutation.

One Mutation can currently read at most 10,000 distinct documents, write at most 1,000 documents,
create at most 100 schedules, and run at most three complete OCC attempts. These ceilings protect
the Environment; ordinary business transactions should be much smaller.

## Declare indexes

```ts
export const order = v.object({
  accountId: v.string({ minLength: 1, maxLength: 128 }),
  state: v.string({ minLength: 1, maxLength: 32 }),
  createdAt: v.timestamp(),
  totalCents: v.int64({ minimum: 0 }),
})

export default defineSchema({
  orders: defineTable(order)
    .index("by_account", ["accountId"])
    .index("by_account_state", ["accountId", "state"])
    .index("by_created_at", ["createdAt"]),
})
```

Tables default to `mode: "queryable"`. Set `{ mode: "keyValue" }` for caches or lookup-only data
that must expose only `get`/write-by-ID operations. Switching later to `queryable` keeps the same
documents and backfills only the declared projections before the new Release becomes servable.
There is no second copy or table-to-table document migration. A Release with pending projections
remains `building`; repeating the same release operation resumes a bounded, idempotent batch until
it becomes `servable`, so an immediate rollout is intentionally unavailable while backfill remains.

`v.union` requires at least two variants. For a fixed string enum in the current validator surface,
use a bounded string and validate its allowed values in the Function, or model the alternatives as
distinct object shapes.

Index order follows the declared component order. A compound index on `accountId, state` groups all
orders for an account and then orders values by state.

### Indexable values and ordering

| Type order | Ordering within the type |
|---:|---|
| null | one value |
| false, true | boolean order |
| int64 | signed numeric order |
| float64 | finite numeric total order |
| timestamp | signed microseconds |
| string | unsigned UTF-8 byte order |
| bytes | unsigned lexicographic byte order |
| typed ID/document ID | canonical ASCII representation |

Arrays and objects are not indexable. An encoded index key has at most 16 components and 4 KiB.
Bound strings/bytes used in indexes much more tightly than the document maximum.

Indexes are sparse. If any indexed property is absent, Runku emits no entry for that index. An
explicit `null` is a present, indexable value.

## Query a table

Application code uses one contract for indexed and bounded-scan execution:

```ts
const first = await ctx.db.query(schema.tables.notes)

const page = await ctx.db.query(schema.tables.notes, {
  where: [{ field: "ownerId", value: principal.id }], // `eq` is the default operator
  orderBy: [{ field: "createdAt", direction: "desc" }],
  limit: 100,
  cursor: input.cursor,
})
```

The defaults are `where: []`, `orderBy: []`, `limit: 100`, and no cursor. An empty query—or the
equivalent explicit `$createdAt DESC, $id DESC` order—uses the physical table index and a keyset
cursor, so it can page a table with millions of rows without sorting the whole table. For a
filtered/sorted query, the planner automatically uses a matching logical index whose equality
prefix and ordered suffix cover the request. `gt`, `gte`, `lt`, and `lte` on the first ordered
suffix field narrow that index range rather than scanning the complete prefix. Otherwise it
evaluates at most 2,000 documents and either returns the result or
`DATA_QUERY_REQUIRES_INDEX`. The public maximum page size is 200.

Supported predicates are `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, and `search`.
`contains` means a case-sensitive string substring or exact array membership and may use the
bounded fallback. `search` requires a declared word index:

```ts
export const call = v.object({
  transcript: v.string({ maxLength: 100_000 }),
})

export default defineSchema({
  calls: defineTable(call).searchIndex("transcript_words", "transcript"),
})

const intent = await ctx.db.query(schema.tables.calls, {
  where: [{ field: "transcript", operator: "search", value: "comprar" }],
  limit: 100,
})
```

Search lowercases and splits Unicode text on non-alphanumeric characters, indexes distinct words,
and performs a whole-word match. It supports up to 4,096 distinct words per indexed document.
Search results have stable document-ID order (or explicit `$id` asc/desc); phrase, stemming,
substring, relevance, and arbitrary secondary sorts require a dedicated future search facility.

## Scan an index

Only Query exposes `scan`:

```ts
const entries = await ctx.db.scan(schema.indexes.rooms.by_name, {
  limit: 100,
})

const rooms = await Promise.all(
  entries.map((entry) => ctx.db.get(schema.tables.rooms, entry.documentId)),
)
```

Each entry contains the encoded `key`, typed `documentId`, `documentRevision`, and
`commitSequence`. Read the document before returning it; an index entry is a lookup reference, not
the document value or an authorization decision.

Optional bounds accept complete encoded keys:

```ts
const page = await ctx.db.scan(schema.indexes.rooms.by_name, {
  lower: { kind: "exclusive", key: previousKey },
  limit: 100,
})
```

`kind` is `inclusive` or `exclusive`; `key` is the canonical `Uint8Array` returned by an index
entry.

### Low-level scan limitation

The current `@runku/server` API does **not** expose a public encoder that turns domain values such
as `[accountId, state]` into an index key. It also does not expose a `(key, documentId)` continuation
token for duplicate keys. Consequently:

- unbounded first-page scans and ranges resumed from previously returned unique keys work;
- application code cannot currently construct an exact/prefix range directly from domain values;
- resuming after a key shared by multiple documents can skip remaining duplicates;
- Mutation does not support index scan at all.

`scan` remains a low-level encoded-key API. Prefer `query`, which accepts domain values, selects a
logical index automatically, preserves duplicates with an opaque cursor, and falls back only within
the documented bounded threshold.

### Scan limits

| Boundary | Limit |
|---|---:|
| Requested rows per `scan` | 1–1,000 |
| Total rows returned across one Query | 10,000 |
| Distinct Query dependencies | 10,000 |

An empty scan still records a range dependency for Realtime. A later Mutation inserting into that
range can cause the subscribed Query to rerun.

## Realtime dependency behavior

A Query records:

- point dependencies for document hits and misses;
- range dependencies for index scans, including empty scans.

After commit, the outbox compares logical document/index changes with active subscriptions. A
matching subscription is rerun and receives a new authoritative result. Intermediate states may
be coalesced; Realtime is not a domain event log.

Keep a subscribed Query's dependency set small. An unbounded scan makes every matching index
change relevant and consumes more read/Realtime capacity than a narrow point query.

## Stored-value limits

| Boundary | v1 limit |
|---|---:|
| Encoded document/canonical value | 1 MiB |
| Value depth | 64 |
| Items in one array/object | 10,000 |
| Object property name | 256 UTF-8 bytes in stored form; schema fields are limited to 128 bytes |
| Public call/response JSON envelope | 2 MiB |

These are failure boundaries, not recommended sizes. Store large immutable bytes through
[Application file storage](../functions/file-storage.md), not in document `bytes` fields. Keep a
document small enough that common Queries, Realtime reruns, logs, and client state remain bounded.

## Schema changes and live Releases

An Environment may temporarily serve multiple compatible Releases. A safe sequence is:

1. add optional fields/new tables/new indexes;
2. deploy code that understands old and new documents;
3. backfill through bounded idempotent Mutations or scheduled batches;
4. verify all eligible Releases and rollback code;
5. only then make a field required or retire an old index/field.

Channel rollback does not undo a document written by newer code. Keep the read contract backward
compatible for the complete rollout and rollback window.

Runku retains every registered index projection needed for dual writes across building and active
Release views. Version 0.5.3 does not automatically garbage-collect an index after its last Release
is retired; removing a declaration therefore stops new code from selecting it but does not yet
reclaim its existing projection. Budget that storage/write amplification and use a later explicit,
fenced cleanup facility rather than deleting internal rows manually.

## Application design checklist

- use deterministic IDs for exact unique lookups;
- verify ownership after every read and before every write;
- pass the observed revision to replace/delete;
- return small application projections rather than raw administrative records;
- keep one public Mutation intent bound to one operation ID;
- keep external effects out of Mutations;
- bound every schema collection and indexed string/byte field;
- treat Realtime output as refreshed Query state;
- account for the current Function index-range/pagination limitation.
