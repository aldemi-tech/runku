# `@runku/client`

Framework-independent TypeScript ESM client for Runku HTTP and Realtime protocols. It uses native
`fetch`, `AbortSignal`, Web Crypto, and browser WebSocket; there are no runtime dependencies. Node
can inject a WebSocket factory.

```sh
npm install @runku/client
```

## Create and type a client

```ts
import { RunkuClient, typedClient, type CodeTarget } from "@runku/client"
import type { RunkuFunctions } from "./runku/_generated/api.js"

const raw = new RunkuClient({
  baseUrl: process.env.RUNKU_URL!,
  target: process.env.RUNKU_TARGET! as CodeTarget,
  applicationKey: process.env.RUNKU_KEY!,
  getBearer: async () => currentUserToken(),
})

export const runku = typedClient<RunkuFunctions>(raw)
```

`RunkuClient` is the runtime class. `typedClient` is a zero-runtime-cost view over the generated
registry. The SDK does not read environment variables or detect frameworks; application code passes
configuration explicitly.

`baseUrl` requires HTTPS except loopback development. Credentials in URL/userinfo are rejected.

## Targets

`target` is mandatory:

```ts
"workspace:local"
"release:rel_01..."
"channel:stable"
```

There is no `latest`. A per-call `options.target` may select another explicit target. A request pins
the resolved Release/Dev Revision.

## Typed calls and result envelopes

```ts
const created = await runku.mutation("notes.create", { title: "Read runbook" })
const noteId = created.value.id
const loaded = await runku.query("notes.get", { id: noteId })
const image = await runku.action("images.render", { width: 64n, height: 64n })
```

Each call returns:

- `requestId` and resolved `releaseId`;
- canonical Function `value`;
- kind-specific metadata: Query snapshot, Mutation commit/replay/attempts, or Action schedules.

Generated contracts restrict public Function names by kind and preserve argument/result/document ID
types. Runtime/server validation remains authoritative.

## Canonical values

| Runku | JavaScript |
|---|---|
| i64 | `bigint` |
| float64 | finite `number` |
| bytes | `Uint8Array` |
| timestamp | `RunkuTimestamp` with signed microseconds |
| typed ID | `RunkuId` / `DocumentId<"table">` |
| array/object | recursively bounded readonly values |

Validate untrusted route/form IDs with `documentId("notes", value)` before calling a typed
Function. Values are checked before network encoding.

## Application and functional identity

`applicationKey` is always required. Use `rk_pub_*` in distributable clients and `rk_sec_*` only in
trusted server configuration. `rk_dev_*` is not accepted by the invocation protocol.

`getBearer` is optional and evaluated on every attempt/reconnect so the application can refresh a
guest/user/service JWT. Never persist bearer tokens in URLs. Authentication and Application Key are
independent checks.

## Retry and cancellation

- Query and Mutation retry only transport or explicitly retryable server errors.
- Mutation generates one operation ID and preserves it across attempts. Supply
  `{ operationId: "opn_..." }` to reconcile the same intent across application restart.
- Action is never retried automatically because an external effect may have happened.
- `AbortSignal` cancels client waiting; it does not prove remote/Action effects did not occur.
- Timeouts/attempts/delay are bounded in `RunkuClientConfig`.

`RunkuError` exposes stable `code`, `retryable`, HTTP `status`, and optional `requestId`. Branch on
code/retryable, not message text.

## Realtime

```ts
const realtime = runku.realtime({
  reconnectInitialDelayMs: 250,
  reconnectMaximumDelayMs: 10_000,
})

const subscription = realtime.subscribe("notes.get", { id: noteId }, {
  onValue: ({ value, deliveryRevision, releaseId }) => {
    render(value, deliveryRevision, releaseId)
  },
  onError: (error) => report(error.code, error.requestId),
})

const initial = await subscription.ready
await subscription.unsubscribe()
realtime.close()
```

Realtime supports public Queries only. Authentication occurs in a WebSocket frame, never the URL.
Reconnect preserves target and refreshes bearer. `resync_required` obtains another authoritative
Query result; intermediate frame replay is not promised.

Browser uses native WebSocket. Node/test runtimes may pass `webSocketFactory` implementing the
documented interface.

## Direct/untyped use

`RunkuClient.query/mutation/action<T>()` is available without a generated registry. It still
validates canonical values but cannot type-check Function names/contracts at compile time. Prefer
`typedClient` in application code.

## Development

```sh
pnpm --dir packages/client check
```

The check builds, runs unit/protocol/retry/Realtime tests, and type conformance. Public changes also
require gateway/protocol vectors and executable example gates.
