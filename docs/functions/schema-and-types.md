# Define schemas and value types

A Runku schema is the durable contract for documents stored by an application. Define it before
writing Mutations: Runku validates every stored document against the schema selected by the exact
Release or Dev Revision that executes the request.

This guide covers the public `@runku/server` authoring API. It does not require knowledge of the
Runku implementation.

## Install the authoring package

```sh
npm install @runku/server
```

Application backend modules live below `runku/`. Exactly one module in that directory must
default-export `defineSchema(...)`.

```text
my-app/
├── package.json
├── runku/
│   ├── schema.ts
│   ├── notes.ts
│   └── _generated/
│       ├── api.js
│       ├── api.d.ts
│       ├── server.js
│       └── server.d.ts
└── src/
```

## Define a complete schema

```ts
// runku/schema.ts
import { defineSchema, defineTable, v, type Infer } from "@runku/server"

export const attachment = v.object({
  fileId: v.string({ minBytes: 1, maxBytes: 128 }),
  contentType: v.string({ minBytes: 1, maxBytes: 255 }),
})

export const note = v.object({
  ownerId: v.string({ minBytes: 1, maxBytes: 256 }),
  title: v.string({ minBytes: 1, maxBytes: 200 }),
  body: v.string({ maxBytes: 20_000 }),
  priority: v.int64({ minimum: 0, maximum: 5 }),
  publishedAt: v.optional(v.timestamp()),
  attachment: v.optional(attachment),
  labels: v.array(v.string({ minBytes: 1, maxBytes: 40 }), { maxItems: 20 }),
})

export type Note = Infer<typeof note>

export default defineSchema({
  notes: defineTable(note)
    .index("by_owner", ["ownerId"])
    .index("by_owner_priority", ["ownerId", "priority"]),
})
```

The object passed to `defineSchema` names the logical tables. `defineTable` assigns the complete
document validator, and each chained `index` declares one ordered logical index. Import the schema
in Functions and use its references:

```ts
import schema from "./schema.js"

schema.tables.notes
schema.indexes.notes.by_owner
```

Never copy or hard-code physical `tbl_*` or `idx_*` values. Logical references remain the
application-facing contract.

## Validator reference

| Validator | Handler value | Meaning and options | Indexable |
|---|---|---|:---:|
| `v.any()` | `RunkuValue` | Any canonical Runku value | depends on actual value |
| `v.null()` | `null` | Exactly `null` | yes |
| `v.boolean()` | `boolean` | `true` or `false` | yes |
| `v.int64({minimum, maximum})` | `bigint` | Signed 64-bit integer; bounds are inclusive numeric literals | yes |
| `v.float64({minimum, maximum})` | `number` | Finite IEEE-754 binary64; bounds are inclusive | yes |
| `v.string({minBytes, maxBytes})` | `string` | Unicode scalar string; bounds count UTF-8 bytes | yes |
| `v.bytes({minBytes, maxBytes})` | `Uint8Array` | Opaque bytes, distinct from a string | yes |
| `v.timestamp()` | `RunkuTimestamp` | Signed Unix-epoch microseconds | yes |
| `v.id(kind?)` | `RunkuId` | Canonical `<kind>_<ULID>`; optionally require one kind | yes |
| `v.documentId("notes")` | `DocumentId<"notes">` | A `doc_*` ID statically associated with one table | yes |
| `v.array(item, bounds?)` | readonly array | Homogeneous items; optional `minItems` and `maxItems` | no |
| `v.object(fields)` | readonly object | Exact declared shape | no |
| `v.union(a, b, ...)` | union | Matches at least one of 2–16 distinct validators | only when actual value is scalar |
| `v.optional(value)` | optional property | Property may be absent; use only inside `v.object` | follows wrapped value |
| `v.pick(object, keys)` | object subset | Reuses selected fields and their optionality | no |

### Integers and floats are different

Stored integers are signed 64-bit values and appear as `bigint` in a Function:

```ts
const counter = v.int64({ minimum: 0, maximum: 1_000_000 })

// A handler returns/accepts 42n, not 42.
```

Declaration bounds are ordinary integer literals. Runtime values are `bigint`. Use `v.float64`
when a value is intentionally fractional. Floats must be finite; `NaN`, positive/negative
infinity, and negative zero are not canonical inputs.

### Strings are bounded by UTF-8 bytes

`maxBytes` is not JavaScript `string.length`. Non-ASCII characters may use multiple bytes:

```ts
const displayName = v.string({ minBytes: 1, maxBytes: 80 })
```

Choose a byte bound that accommodates the scripts and emoji your application accepts. Perform
business normalization deliberately; Runku does not trim or case-fold strings for you.

### Timestamps

Function handlers receive a timestamp object whose `.value` is signed microseconds:

```ts
const createdAt = v.timestamp()

export function toDate(value: { readonly value: bigint }): Date {
  return new Date(Number(value.value / 1_000n))
}
```

Construct a timestamp in Function code with `Runku.timestamp(micros)`. In `@runku/client`, the
corresponding `RunkuTimestamp` exposes `.micros`.

### Typed IDs and document IDs

`v.id("rel")` accepts only a canonical Release ID. `v.id()` accepts any canonical typed ID but
does not grant access to the referenced resource. A kind is 1–16 lowercase ASCII letters or
digits, followed by `_` and a canonical uppercase ULID.

Prefer `v.documentId("notes")` for document arguments. It prevents passing a profile document ID
to a note Function at compile time and verifies the `doc_*` wire shape at runtime.

### Exact objects, optional fields, and null

Objects reject unknown keys and reject a missing required key. Optional and nullable are separate:

```ts
const profile = v.object({
  // Must exist and contain a string.
  name: v.string({ minBytes: 1, maxBytes: 80 }),

  // May be absent; when present it must be a timestamp.
  verifiedAt: v.optional(v.timestamp()),

  // Must exist; its value can be null or a string.
  avatarUrl: v.union(v.null(), v.string({ maxBytes: 2_048 })),
})
```

Adding an optional field is usually easier to roll out across mixed Releases than adding a
required field. Do not use `v.any()` merely to avoid designing a contract: it gives up generated
types and makes later compatibility analysis weaker.

### Reuse contracts without duplicating interfaces

```ts
import { v, type Infer } from "@runku/server"

export const account = v.object({
  email: v.string({ minBytes: 3, maxBytes: 320 }),
  displayName: v.string({ minBytes: 1, maxBytes: 80 }),
  disabled: v.boolean(),
})

export const createAccount = v.pick(account, ["email", "displayName"])
export type Account = Infer<typeof account>
export type CreateAccount = Infer<typeof createAccount>
```

Validators must be statically extractable. Define them as module constants and compose them with
the public helpers. Runtime-generated validators, conditional declaration metadata, spreads, and
computed Function definitions are rejected by `runku build`.

## Table, field, and index naming

| Name | Current v1 rule |
|---|---|
| Table | 1–64 bytes; begins with lowercase ASCII; remaining characters are ASCII letters, digits, or `_` |
| Object field | 1–128 bytes; must not contain NUL |
| Index | 1–64 bytes; begins with an ASCII letter or `_`; remaining characters are ASCII letters, digits, or `_` |
| Indexed field path | 1–16 segments; each segment follows the index-name character rule |

The typed `defineTable(...).index(...)` API currently guides application code to top-level object
fields. Choose stable names: table and index identities derive from the Project and logical name,
so renaming is a schema change rather than a display-only edit.

## Index behavior

Indexes are compound and ordered in the same order as their field list:

```ts
defineTable(note).index("by_owner_priority", ["ownerId", "priority"])
```

This is ordered first by `ownerId`, then by `priority`. Index values may be null, boolean, int64,
float64, timestamp, string, bytes, or typed ID. Arrays and objects cannot be index components.

Indexes are sparse: if any indexed field is absent, that document has no entry in that index. A
present field containing `null` is indexed as null. A Mutation never supplies index keys; Runku
derives old and new entries from the validated document in the same atomic commit.

See [Documents, indexes, and concurrency](../data/documents-and-indexes.md) for reads, scans,
pagination constraints, ordering, and write examples.

## Current hard limits

These are v1 format limits, not recommended application targets:

| Boundary | Limit |
|---|---:|
| Tables in one schema | 1,000 |
| Logical indexes in one schema | 1,000 |
| Fields in one object validator | 1,000 |
| Components in one index | 16 |
| Encoded index key | 4 KiB |
| Variants in one union | 16 |
| Validator depth | 32 |
| Nodes in one validator | 10,000 |
| Encoded validator or schema contract | 256 KiB |
| Encoded stored value/document | 1 MiB |
| Array items or object properties in a canonical value | 10,000 |
| Canonical value depth | 64 |
| Stored object key | 256 UTF-8 bytes |

Set much smaller application-specific bounds on strings, bytes, and arrays. A Function call also
has a 2 MiB public envelope, so the stored-value maximum is not a reason to send 1 MiB arguments
through every request.

## Build and verify the schema

```sh
runku dev --prepare
runku build
```

A successful build emits immutable artifact/manifest paths and updates
the `api.js`/`api.d.ts` browser pair and `server.js`/`server.d.ts` server pair below
`runku/_generated`. Build failure leaves an already-running Workspace on its last valid Dev
Revision.

Before shipping a schema change, verify:

1. old stored documents satisfy the new validators;
2. every Release that may receive traffic understands the required/optional fields;
3. a new index is ready before a Query depends on it;
4. an index or field is retained while any live Release, subscription, Cron, or scheduled call
   still uses it;
5. rollback code can read documents written by the new Release.

Use an expand → backfill → contract rollout for breaking stored-data changes. A Channel rollback
changes code routing; it does not reverse stored documents.

## Common schema failures

| Symptom | Likely cause | Resolution |
|---|---|---|
| Build reports an ambiguous schema | zero or multiple default `defineSchema` exports | keep exactly one default schema export below `runku/` |
| Contract definition is invalid | inverted bounds, duplicate union variant, invalid name, or unsupported composition | simplify the validator and check the rules above |
| Document validation fails | missing required field, unknown field, wrong type, or exceeded bound | correct the complete document value; do not bypass validation |
| Index value is unsupported | indexed path reaches an array/object | index a bounded scalar field instead |
| Index limit/key limit exceeded | too many components or large string/byte components | reduce components and bound indexed values tightly |
| Old Release cannot serve with new Release | stored schema/index contracts are incompatible | use staged expansion and serve only a compatible Release set |

Next: [Choose Query, Mutation, or Action](query-mutation-action.md).
