# Function API reference

This reference describes the application-facing `@runku/server` contract. Use it when you need
the exact declaration field, handler parameter, capability, method signature, value type, or limit.
It does not describe the Runku implementation or source-code architecture.

## Install and import

Install the package in the application that contains the `runku/` directory:

```sh
pnpm add @runku/server
```

The package is a declaration/build SDK. Runku supplies the actual runtime context when it invokes
a Function; application code must not construct a context itself.

```ts
import {
  action,
  cron,
  defineSchema,
  defineTable,
  mutation,
  query,
  value,
  v,
  type DocumentId,
  type Infer,
} from "@runku/server"
```

## Function declaration

`query()`, `mutation()`, and `action()` accept the same six declaration fields. Only `handler` is
required; omitted metadata defaults to `auth: "none"`, `visibility: "public"`,
`capabilities: []`, `args: v.null()`, and `returns: v.any()`.

```ts
export const getProfile = query({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read"],
  args: v.object({ profileId: v.documentId("profiles") }),
  returns: v.union(v.null(), profile),
  async handler(ctx, input) {
    // ...
  },
})
```

| Field | Type | Meaning |
|---|---|---|
| `auth` | `"none" \| "optional" \| "guest" \| "user" \| "service"` | Functional identity requirement |
| `visibility` | `"public" \| "internal"` | Whether the public API may call the Function |
| `capabilities` | readonly array of allowed capability strings | Exact privileged surfaces added to `ctx` |
| `args` | `Validator<A>` | Runtime contract for the second handler parameter |
| `returns` | `Validator<R>` | Runtime contract for the returned/resolved value |
| `handler` | `(ctx, input) => R \| Promise<R>` | Function implementation |

Declarations must be statically extractable by `runku build`. Keep declaration metadata literal,
use validators from `v`, and do not select validators or capabilities dynamically.

### Function names

The logical name is derived from the module path below `runku/` and the export name:

| Module/export | Logical name |
|---|---|
| `runku/orders.ts` → `export const create` | `orders.create` |
| `runku/admin/users.ts` → `export const disable` | `admin.users.disable` |

A Function name:

- starts with an ASCII letter;
- contains only ASCII letters, digits, `_`, `.`, `/`, or `-`;
- occupies at most 128 UTF-8 bytes;
- is stable application API once clients or other Functions use it.

### Runtime selection

Safe V8 is the default and the only runtime for Query and Mutation. An Action that needs Node.js
built-ins or npm dependencies selects Full Node with an exact directive on the first line of its
module:

```ts
"use runku node"

import { action, v } from "@runku/server"
```

The directive applies to the complete reachable module graph. It does not grant network, secret,
storage, or other Platform access: those surfaces still require the corresponding declared
capability. Cron declarations remain Safe. Local development can execute Full Node on the
developer machine; the compact Self-Hosted distribution does not include the VM-isolated Agent
required to run mutually untrusted Full Node workloads on shared infrastructure. See
[Functions and runtimes](../functions/functions-and-runtimes.md) for the deployment decision.

## Handler parameters and return value

Every handler receives exactly two parameters:

```ts
async handler(ctx, input) {
  // ctx: capability-scoped context for this Function kind
  // input: value validated by args
  return result // must satisfy returns
}
```

`input` is already decoded to canonical JavaScript values. For example, `v.int64()` produces a
`bigint`, `v.bytes()` produces `Uint8Array`, and `v.timestamp()` produces a `RunkuTimestamp`.
Objects are exact: undeclared input properties are rejected before the handler starts.

The result is validated after the handler resolves. Returning `undefined`, a JavaScript `Date`, a
plain object where an ID is required, a non-finite number, or any shape that differs from `returns`
fails the invocation. Use `v.null()` and return `null` when an operation intentionally has no
value.

## Authentication requirement

Application identity (the Application Client key) and functional identity (guest/user/service)
are independent. `auth` constrains the functional principal; it does not replace the Application
Client credential required by a public request.

| `auth` | Request accepted when | `ctx.auth.principal` with `auth:read` |
|---|---|---|
| `none` | no functional principal is required | `null`; any presented functional principal is discarded |
| `optional` | bearer is absent or valid | `null` or the validated principal |
| `guest` | a valid guest, user, or service principal is present | `guest`, `user`, or `service` |
| `user` | a valid user principal is present | `user` |
| `service` | a valid external service principal is present | `service` |

The trusted `system` principal is reserved for internal Platform execution and may satisfy an
internal service requirement. A public caller cannot mint or exchange into it. Invalid, expired,
wrong-audience, or wrong-kind credentials fail before application code runs.

`auth` enforces the minimum principal class. Declaring `auth: "user"` does not authorize access to
a particular document: the handler must still check ownership, tenant membership, role, and other
application rules.

## Visibility

| Value | Direct public HTTP/client call | Nested Function call | Scheduled/Cron target |
|---|---:|---:|---:|
| `public` | yes | yes | yes, when the kind is supported |
| `internal` | no | yes | yes |

Use `internal` for trusted composition boundaries, validation helpers exposed as Functions, and
scheduled workers. The nested caller still needs the corresponding `function:*` capability and
the callee still applies its own `auth`, argument validator, and capability set.

## Capability matrix

Capabilities are allowlisted by Function kind. A capability absent from `capabilities` is absent
from the TypeScript `ctx` type and denied by the runtime.

| Capability | Query | Mutation | Action | Context surface |
|---|---:|---:|---:|---|
| `auth:read` | yes | yes | yes | `ctx.auth` |
| `db:read` | yes | yes | no | `ctx.db.get`, `documentId`; Query also has `query` and `scan` |
| `db:write` | no | yes | no | `ctx.db.insert`, `replace`, `delete` |
| `function:query` | yes | yes | yes | `ctx.runQuery` |
| `function:mutation` | no | yes | yes | `ctx.runMutation` |
| `function:action` | no | no | yes | `ctx.runAction` |
| `scheduler:create` | no | yes | yes | `ctx.scheduler` |
| `storage:read` | no | no | yes | read methods on `ctx.storage` |
| `storage:write` | no | no | yes | write methods on `ctx.storage` |
| `network:https` | no | no | yes | `ctx.https` when the deployment provides an egress broker |
| `variable:NAME` | yes | yes | yes | `ctx.env.get("NAME")` |
| `secret:NAME` | no | no | yes | `ctx.secrets.get("NAME")` |

The current compact `runku-server` does not attach an HTTPS egress broker. An Action may build
with `network:https`, but invocation fails with `ACTION_HTTPS_UNAVAILABLE`. Validate the capability
in Runku SaaS only when the target SaaS Environment exposes it; do not assume SaaS and Self-Hosted
capability availability are identical.

## Base context: all Functions

All handler contexts include `ctx.invocation`, `ctx.cooperate`, and `ctx.log`.

### `ctx.invocation`

```ts
interface InvocationMetadata {
  projectId: string
  environmentId: string
  releaseId: string
  requestId: string
  invocationId: string
  functionId: string
  functionName: string
  functionType: "query" | "mutation" | "action"
  capabilities: readonly string[]
  variableEnabled: boolean
  secretEnabled: boolean
  httpsEnabled: boolean
  dataEnabled: boolean
  dataWriteEnabled: boolean
  schedulerEnabled: boolean
  storageReadEnabled: boolean
  storageWriteEnabled: boolean
  functionQueryEnabled: boolean
  functionMutationEnabled: boolean
  functionActionEnabled: boolean
  authEnabled: boolean
}
```

Use IDs for correlation and idempotency, not for authorization. The exact Release or development
revision is pinned for the invocation and its nested call tree; there is no implicit `latest`.

### `ctx.cooperate()`

```ts
await ctx.cooperate()
```

Yield during long CPU loops so cancellation and deadlines are observed promptly. Calling it counts
as a Platform operation. It does not extend the deadline.

### `ctx.log`

```ts
await ctx.log.info("order accepted", {
  orderId,
  itemCount: 3n,
})
```

Methods are `debug`, `info`, `warn`, and `error`:

```ts
log.info(message: string, fields?: Readonly<Record<string, RunkuValue>>): Promise<void>
```

The message must be non-empty, at most 4 KiB of UTF-8, contain no NUL, and contain no control
characters other than tab/newline. Structured fields use canonical Runku values and may occupy at
most 16 KiB encoded. One invocation may request at most 100 Function log records and 64 KiB of
aggregate Function log payload. Logging is bounded and best-effort; never place secrets, bearer
tokens, file grants, or unbounded user content in logs.

## Authentication context

Declare `auth:read` to receive:

```ts
interface AuthContext {
  application: {
    clientId: string
    credentialId: string
    assurance: "declared" | "verified"
    scopes: readonly string[]
    configurationRevision: bigint
  } | null
  principal: {
    id: string
    kind: "guest" | "user" | "service" | "system"
    providerId: string
    scopes: readonly string[]
    authTime: RunkuTimestamp | null
    expiresAt: RunkuTimestamp | null
    mappingRevision: bigint
  } | null
}
```

`application` identifies the Application Client credential. `principal` identifies the functional
actor. Check the correct axis for every decision: a client allowlist is not a user role, and a user
role does not authorize a different Application Client.

## Environment variables and secrets

Declare the exact name in the capability:

```ts
capabilities: ["variable:PUBLIC_ORIGIN", "secret:PAYMENT_TOKEN"]

const origin = await ctx.env.get("PUBLIC_ORIGIN")
const token = await ctx.secrets.get("PAYMENT_TOKEN")
```

Rules:

- the requested name must match a declared capability;
- the value comes from the Environment configuration revision pinned to the invocation;
- a missing configured name fails the read; it does not return `undefined`;
- Query and Mutation may read non-secret variables but cannot declare secrets;
- only Action may declare/read secrets;
- never return or log secret values.

## Query database API

With `db:read`, Query receives:

```ts
ctx.db.get(table, documentId): Promise<DataDocument<T> | null>
ctx.db.documentId(table, stableKey): DocumentId<TableName>
ctx.db.query(table, { where?, orderBy?, limit?, cursor? }?): Promise<DataQueryPage<T>>
ctx.db.scan(index, { lower?, upper?, limit }): Promise<readonly DataIndexEntry[]>
```

`stableKey` is a non-empty UTF-8 string of at most 1,024 bytes. A deterministic document ID is
scoped by the logical table. It is an identity helper, not a uniqueness reservation or permission
check.

`get` returns:

```ts
interface DataDocument<T> {
  tableId: string
  documentId: DocumentId<string>
  revision: bigint
  commitSequence: bigint
  createdAt: RunkuTimestamp
  updatedAt: RunkuTimestamp
  value: T
}
```

`scan` parameters:

| Parameter | Required | Contract |
|---|---:|---|
| `index` | yes | reference from `schema.indexes.TABLE.INDEX` |
| `lower` | no | `null` or `{kind: "inclusive" \| "exclusive", key: Uint8Array}` |
| `upper` | no | same shape as `lower` |
| `limit` | yes | integer from 1 through 1,000 |

An entry contains `indexId`, encoded `key`, `tableId`, `documentId`, `documentRevision`, and
`commitSequence`. The current SDK has no public domain-value index-key encoder or duplicate-safe
cursor. See [Documents, indexes, and concurrency](../data/documents-and-indexes.md#low-level-scan-limitation)
before designing pagination.

Query limits are 10,000 aggregate scan rows and 10,000 recorded dependencies per invocation.

## Mutation database API

With `db:read`, Mutation receives `get` and `documentId`. It does not receive `scan`. With
`db:write`, it additionally receives:

```ts
ctx.db.insert(table, documentId, value): Promise<void>
ctx.db.replace(table, documentId, expectedRevision, value): Promise<void>
ctx.db.delete(table, documentId, expectedRevision): Promise<void>
```

| Method | Preconditions | Durable result at commit |
|---|---|---|
| `insert` | ID is absent; complete value passes table schema | document and all derived indexes added |
| `replace` | ID exists at `expectedRevision`; complete replacement passes schema | document revision and indexes replaced |
| `delete` | ID exists at `expectedRevision` | document and index entries removed |

Mutation buffers these changes. A successful return does not become visible until result
validation and the atomic commit succeed. A conflict may rerun the entire handler up to three
attempts. One invocation may read 10,000 distinct documents, write 1,000 documents, and create 100
schedules. Mutation code must remain deterministic and must not perform external effects.

## Nested Function calls

```ts
ctx.runQuery(functionName, argumentsValue): Promise<RunkuValue>
ctx.runMutation(functionName, argumentsValue): Promise<RunkuValue>
ctx.runAction(functionName, argumentsValue): Promise<RunkuValue>
```

Each method requires its matching `function:*` capability and calls a Function from the same exact
pinned code. The target declaration validates its own auth, arguments, result, and capabilities.
Errors propagate to the caller unless application code handles them.

The current compact Safe runtime defaults allow up to 100 nested calls across one invocation tree,
depth 8, and 8 concurrent nested isolates. These budgets are shared by the tree, not reset for
each child. Avoid recursive composition and unbounded `Promise.all`.

## Scheduler API

Mutation and Action may declare `scheduler:create`:

```ts
ctx.scheduler.runAfter(
  delayMicros: bigint,
  functionName: string,
  argumentsValue: RunkuValue,
  options?: { idempotencyKey?: string },
): Promise<string>

ctx.scheduler.runAt(
  timestampMicros: bigint,
  functionName: string,
  argumentsValue: RunkuValue,
  options?: { idempotencyKey?: string },
): Promise<string>
```

The return value is the schedule ID. The target must be a Mutation or Action and its input must
match its declaration. `runAfter` uses signed microseconds; `runAt` uses Unix epoch microseconds.
The current maximum delay is ten years and one invocation may create at most 100 schedules.

Creation inside a Mutation joins the atomic commit. Creation inside an Action is durable as soon
as the call succeeds. Scheduled delivery is at-least-once: make the target idempotent and use a
stable `idempotencyKey` for logically identical schedule creation.

## Action HTTPS API

When `network:https` is both declared and supplied by the deployment:

```ts
const response = await ctx.https.request({
  method: "POST",
  url: "https://api.example.com/events",
  headers: {
    "content-type": ["application/json"],
    authorization: [`Bearer ${token}`],
  },
  body: new TextEncoder().encode(JSON.stringify(payload)),
  idempotencyKey: `event:${eventId}`,
})
```

Request parameters:

| Field | Required | Contract |
|---|---:|---|
| `method` | yes | `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, or `DELETE` |
| `url` | yes | HTTPS URL accepted by the deployment egress policy |
| `headers` | no | lower/normal HTTP names mapped to arrays of string values |
| `body` | no | `Uint8Array` |
| `idempotencyKey` | no | stable key, at most 128 bytes |

The response has numeric `status`, multi-value `headers`, and a `Uint8Array` body. Protocol hard
ceilings are 16 MiB request body, 16 MiB response body, 64 KiB aggregate headers, 256 header values,
10 redirects, and one minute per mediated call; an operator policy may be stricter. Redirects and
resolved IPs are revalidated to prevent policy bypass.

External effects are not transactional and Actions are not automatically retried. Treat a timeout
as an uncertain result and reconcile with the remote service before repeating a write.

**Self-Hosted availability:** the compact server currently returns `ACTION_HTTPS_UNAVAILABLE`.

**Large-body gate:** `body` and the returned `body` are byte arrays copied across the current
runtime broker. They are not streams and the 16 MiB hard ceiling is not raised by configuring file
or object limits. A backward-compatible Application File/Object handle mode is a production
readiness requirement, not an available field in this API. It must bind the exact Environment and
immutable object, require both the relevant storage and network capabilities, stream with bounded
memory, enforce size/digest/deadline/cancellation, and retain uncertain external-effect recovery.
Until that contract and a compatible broker profile ship, transfer large bytes through Application
File grants or Runku Object Storage multipart outside the Function envelope.

## Action file API

`storage:read` adds:

```ts
ctx.storage.getMetadata(fileId): Promise<FileMetadata>
ctx.storage.createDownload(fileId, { expiresInMicros }): Promise<FileDownloadGrant>
ctx.storage.get(fileId): Promise<{ metadata: FileMetadata; bytes: Uint8Array }>
```

`storage:write` adds:

```ts
ctx.storage.createUpload({ maxBytes, contentType?, sha256? }): Promise<FileUploadGrant>
ctx.storage.store(bytes, { contentType?, sha256? }): Promise<FileMetadata>
ctx.storage.delete(fileId): Promise<void>
```

`createUpload` and `createDownload` are preferred for browser-sized files because bytes bypass the
Function heap. Grant tokens are bearer credentials and must never be logged or persisted as file
identity. `store`/`get` are limited to the smaller direct-Action byte limit.

See [Application file storage](../functions/file-storage.md) for grant usage, limits, lifecycle,
HTTP upload/download behavior, quotas, and backup consequences.

## Cron declarations

Cron is declared outside a Function and points to an existing Mutation or Action:

```ts
export const hourlyCleanup = cron({
  schedule: "0 * * * *",
  function: "maintenance.cleanup",
  args: {
    before: value.timestamp(0n),
    batchSize: value.int64(100n),
  },
})
```

| Field | Type | Meaning |
|---|---|---|
| `schedule` | string | statically compiled UTC Cron expression |
| `function` | string | logical name of a Mutation or Action |
| `args` | `RunkuValue` | value validated against the target `args` contract |

Cron export name becomes its stable declaration name. Operators can disable/enable deployed
declarations without editing code. Delivery is at-least-once and pins exact code for each
activation/invocation.

Use `value.int64(bigint)`, `value.float64(number)`, `value.timestamp(micros)`, `value.id(string)`,
and `value.bytes(number[])` for non-JSON constants in `args`.

## Validator signatures

| Validator | Canonical handler type | Parameters |
|---|---|---|
| `v.any()` | `RunkuValue` | none; prefer a specific validator |
| `v.null()` | `null` | none |
| `v.boolean()` | `boolean` | none |
| `v.int64({minimum?, maximum?})` | `bigint` | inclusive numeric bounds |
| `v.float64({minimum?, maximum?})` | finite `number` | inclusive numeric bounds |
| `v.string({minLength?, maxLength?})` | `string` | Unicode code-point length |
| `v.bytes({minBytes?, maxBytes?})` | `Uint8Array` | byte length |
| `v.timestamp()` | `RunkuTimestamp` | signed Unix microseconds |
| `v.id(kind?)` | `RunkuId` | optional typed-ID kind |
| `v.documentId(table)` | `DocumentId<Table>` | exact logical table name |
| `v.array(item, {minItems?, maxItems?})` | readonly array | element validator and item-count bounds |
| `v.object(fields)` | exact readonly object | map of field name to validator |
| `v.pick(object, keys)` | exact object subset | object validator and literal key array |
| `v.union(a, b, ...rest)` | union of variant types | 2–16 variants |
| `v.optional(validator)` | optional object property | valid only as an object field |

`minimum` and `maximum` are declared as TypeScript `number`, even though an `int64` handler value is
`bigint`; use only exactly representable integer bounds in a declaration. `float64` rejects NaN and
positive/negative infinity. Object properties not declared in the shape are rejected.

Use `Infer<typeof validator>` to derive application types:

```ts
const createInput = v.object({
  title: v.string({ minLength: 1, maxLength: 200 }),
  labels: v.array(v.string({ maxLength: 40 }), { maxItems: 20 }),
})

type CreateInput = Infer<typeof createInput>
```

For the full data model, nesting/size limits, object rules, IDs, schema/table/index naming, and
schema rollout behavior, see [Schema and data types](../functions/schema-and-types.md).

## Schema declaration signatures

```ts
const document = v.object({ /* fields */ })

const table = defineTable(document)
  .index("by_owner", ["ownerId"])
  .index("by_owner_created", ["ownerId", "createdAt"])
  .searchIndex("body_words", "body")

export default defineSchema({
  notes: table,
})
```

Exactly one default schema declaration is allowed below `runku/`. `defineTable(document, options?)`
defaults to `{ mode: "queryable" }`; `{ mode: "keyValue" }` disables table queries while retaining
exact ID access. `.index(name, fields)` declares an ordered list of document field paths and
`.searchIndex(name, field)` declares one Unicode whole-word string index. Both return the same table
definition for chaining. `defineSchema()` maps logical table names to table definitions and exposes
typed `schema.tables` and `schema.indexes` references for handler calls.

## Common runtime budgets

These current compact-server defaults apply to Safe Functions and include work across nested calls:

| Boundary | Default/limit |
|---|---:|
| V8 heap per invocation isolate | 64 MiB |
| Invocation wall time, including queue wait | 30 seconds |
| Explicit Platform operations per invocation | 10,000 |
| Nested calls per invocation tree | 100 |
| Nested depth below root | 8 |
| Concurrent nested isolates | 8 |
| Mutation OCC attempts | 3 |
| Mutation document reads | 10,000 |
| Mutation document writes | 1,000 |
| Query aggregate scan rows | 10,000 |
| Query dependencies | 10,000 |
| Schedules created per Mutation/Action | 100 |

The public request envelope has its own 2 MiB encoded limit, and stored documents have a 1 MiB
canonical-value limit. File grants and Runku Object Storage have separate limits; they are
not a way to enlarge a Function argument or result.

## Failure and retry rules

| Failure point | Handler ran? | Caller retry guidance |
|---|---:|---|
| app key/auth/target/argument rejected | no | correct request; do not blind-retry |
| Query transient unavailable/timeout | maybe | safe only when client policy marks it retryable |
| Mutation OCC conflict | yes | Runku reruns internally, up to three attempts |
| Mutation response lost after commit | yes | repeat with the same operation ID |
| Action timeout/lost response | maybe | do not blind-retry; external effect may have occurred |
| return validator failure | yes | code/schema defect; deploy a corrected Release |
| capability unavailable | maybe | change deployment/capability use; retries alone do not help |
| limit exceeded | yes or during validation | reduce/batch work; retries with identical input do not help |

Keep Query deterministic, Mutation effect-free, and Action effects explicitly idempotent. Those
three rules are more important than any client retry count.
