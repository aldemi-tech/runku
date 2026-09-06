# TypeScript client

`@runku/client` is the application client for calling public Runku Functions, subscribing to
Queries, and transferring Function-authorized files. It works in browsers and server-side
JavaScript environments that provide `fetch`; Realtime additionally requires `WebSocket` or a
factory supplied by the application.

This SDK is for your application data plane. It does not administer Runku, publish Releases, or
replace the CLI and Management API.

## Install

```sh
pnpm add @runku/client
```

For React hooks and Next.js server hydration, install the version-matched binding as well:

```sh
pnpm add @runku/client@0.4.5 @runku/react@0.4.5
```

See [React and Next.js integration](react-client.md) for the hooks, server facade, and Field Board
example.

## Create a client

```ts
import { RunkuClient } from "@runku/client"

const runku = new RunkuClient({
  baseUrl: "https://api.example.com",
  target: "channel:stable",
  applicationKey: "rk_pub_v1_...",
  getBearer: async () => authSession?.accessToken ?? null,
})
```

| Option | Required | Default | Accepted value |
|---|---:|---:|---|
| `baseUrl` | yes | — | absolute HTTP(S) Product origin; no embedded credentials/query/fragment |
| `target` | yes | — | `environment:default`, `release:rel_*`, `channel:<name>`, or `workspace:<name>` |
| `applicationKey` | yes | — | Application Client key appropriate for the application tier |
| `getBearer` | no | none | sync/async callback returning functional bearer or `null` |
| `timeoutMs` | no | `30000` | integer `1..300000`; covers attempts, delays, and response reading |
| `maxAttempts` | no | `2` | integer `1..5`; applies only to retryable Query/Mutation calls |
| `retryDelayMs` | no | `50` | integer `0..10000`; linear base delay multiplied by attempt |
| `fetch` | no | global `fetch` | injected compatible implementation |
| `webSocketFactory` | no | global `WebSocket` | factory for Realtime connections |

The SDK does not read `process.env`, cookies, local storage, or framework state. `getBearer` is
called when a request/connection authenticates, so token refresh remains the application's
responsibility.

### Which Application Key to use

- Browser/mobile code may contain only an explicitly publishable Application Client key.
- Server code may use a secret Application Client key when its policy requires one.
- Development keys belong only to authorized development workflows.
- No Application Key is a Platform operator credential.

Functional bearer and Application Key answer different questions. The key identifies the calling
application; the bearer identifies the guest, user, or service expected by a Function's `auth`.

## Call Functions

```ts
const notes = await runku.query(api.notes.list, { ownerId })

const created = await runku.mutation(api.notes.create, {
  title: "Operations runbook",
  body: "...",
})

const exportJob = await runku.action(api.exports.start, {
  format: "csv",
})
```

Signatures:

```ts
query<T>(name: string, args: RunkuValue, options?: CallOptions): Promise<RunkuResult<T>>
mutation<T>(name: string, args: RunkuValue, options?: MutationOptions): Promise<RunkuResult<T>>
action<T>(name: string, args: RunkuValue, options?: CallOptions): Promise<RunkuResult<T>>

interface CallOptions {
  signal?: AbortSignal
  target?: CodeTarget
}

interface MutationOptions extends CallOptions {
  operationId?: string
}
```

`options.target` overrides the configured target for that call. Keep overrides explicit and rare;
an accidental Workspace or Release target can produce behavior different from the stable Channel.

`AbortSignal` cancels local waiting and asks the transport to stop. It does not prove that an
Action effect or Mutation commit did not occur.

## Use generated types

`runku build` emits browser and server runtime trees with matching declarations. Import the
generated reference instead of spelling a Function name:

```ts
import { RunkuClient } from "@runku/client"
import { api } from "../runku/_generated/api.js"

const client = new RunkuClient({
  baseUrl: "https://api.example.com",
  target: "channel:stable",
  applicationKey: "rk_pub_v1_...",
})

const result = await client.query(api.notes.get, { id })
// result.value is inferred from the Function's `returns` validator.
```

The generated references:

- expose non-service public Functions through `api`;
- expose every public Function, including `auth: "service"`, through
  `runku/_generated/server.js`;
- exclude internal Functions from both external trees;
- restricts each method to its Function kind;
- derives exact argument/result types from the deployed declarations;
- are small immutable runtime values and make the same HTTP request as a string call.

Keep `serverApi` imports in server-only modules. The gateway still enforces Function visibility,
functional `auth`, and Application Client scopes; generated-module separation is a build-time
guard, not the authorization boundary. Raw string calls and `typedClient` remain available for
compatibility and dynamic tooling.

Regenerate/check the declarations whenever schema or Function declarations change. Compile-time
types do not bypass server validation and do not prove the selected target contains that contract.

## Canonical client values

| Runku type | SDK value |
|---|---|
| null | `null` |
| boolean | `boolean` |
| int64 | `bigint` in signed 64-bit range |
| float64 | finite `number`; negative zero is not canonical |
| string | JavaScript `string` |
| bytes | `Uint8Array` |
| timestamp | `new RunkuTimestamp(micros)`; read `.micros` |
| typed ID | `new RunkuId(canonical)`; read `.value`/`.toString()` |
| document ID | `documentId("table", canonical)` |
| array | readonly array of Runku values |
| object | plain exact string-keyed object of Runku values |

Examples:

```ts
import {
  RunkuId,
  RunkuTimestamp,
  documentId,
} from "@runku/client"

const input = {
  count: 42n,
  capturedAt: new RunkuTimestamp(1_725_000_000_000_000n),
  payload: new Uint8Array([0, 127, 255]),
  ownerId: new RunkuId("usr_01J00000000000000000000000"),
  noteId: documentId("notes", "doc_01J00000000000000000000000"),
}
```

Do not pass `Date`, `undefined`, `Symbol`, functions, class instances other than the SDK value
types, cyclic objects, sparse arrays, NaN, or infinities. Client encoding is bounded to depth 64,
10,000 entries per container, and a 2 MiB final request envelope.

## Understand `RunkuResult`

Every call resolves to:

```ts
interface RunkuResult<T> {
  requestId: string
  releaseId: string
  value: T
  metadata:
    | { kind: "query"; snapshotSequence: bigint | null }
    | { kind: "mutation"; commitSequence: bigint | null; replayed: boolean; attempts: number }
    | { kind: "action"; schedulesCreated: bigint }
}
```

- `requestId` correlates client failures with Runku operational logs.
- `releaseId` proves which immutable Release served the request.
- `snapshotSequence` identifies the Query snapshot when applicable.
- `commitSequence` identifies the committed Mutation state; it may be null when no commit sequence
  is produced by the operation contract.
- `replayed` distinguishes an original Mutation result from durable operation replay.
- `attempts` counts internal optimistic-concurrency attempts, not HTTP attempts.
- `schedulesCreated` reports schedules created by the Action.

## Mutation operation IDs

If omitted, the SDK generates one canonical `opn_*` ID and preserves it across its own retryable
HTTP attempts:

```ts
await runku.mutation("payments.record", input)
```

Generate/persist the operation identity in your application when reconciliation must survive a
process restart or job redelivery:

```ts
await runku.mutation("payments.record", input, {
  operationId: storedOperationId,
})
```

Reuse the same ID only for the exact same Project, Environment, target, Function, caller scope,
and arguments. A committed replay returns the original result. Reusing an ID for a different
intent is rejected.

## Retry behavior

| Operation | SDK attempts | Why |
|---|---:|---|
| Query | up to `maxAttempts` when the normalized error is retryable | read has no application effect |
| Mutation | up to `maxAttempts`, preserving operation ID | committed replay is durable |
| Action | exactly 1 | an external effect may have happened before response loss |
| file upload | exactly 1 | grant is one-shot and partial outcome may be uncertain |
| file download | no automatic repeat | application decides whether grant/time/range remain valid |

The timeout is one total lifecycle, not a fresh timeout per attempt. Do not wrap Actions in a
generic retry library. Design Action effects with downstream idempotency and reconciliation.

## Handle errors

```ts
import { RunkuError } from "@runku/client"

try {
  await runku.query("notes.get", { id })
} catch (error) {
  if (error instanceof RunkuError) {
    report({
      code: error.code,
      retryable: error.retryable,
      status: error.status,
      requestId: error.requestId,
    })
  }
  throw error
}
```

Branch on stable `code` and `retryable`, not human text. Local SDK failures use codes such as
`SDK_CREDENTIAL_INVALID`, `SDK_NETWORK_ERROR`, `SDK_REQUEST_LIMIT_EXCEEDED`, `SDK_TIMEOUT`, and
`SDK_ABORTED`. `status` is `0` for failures without an HTTP response. Never log the Application
Key, bearer, Function arguments, or file grant token when recording an error.

## Realtime Queries

Only public Queries can be subscribed:

```ts
const realtime = runku.realtime({
  reconnectInitialDelayMs: 100,
  reconnectMaximumDelayMs: 10_000,
})

const subscription = realtime.subscribe(
  "notes.list",
  { ownerId },
  {
    onValue(state) {
      render(state.value)
      console.debug(state.releaseId, state.deliveryRevision)
    },
    onError(error) {
      reportRealtime(error.code, error.retryable)
    },
  },
)

const initial = await subscription.ready

// Later:
await subscription.unsubscribe()
realtime.close()
```

Realtime options:

| Option | Default | Range |
|---|---:|---:|
| `reconnectInitialDelayMs` | `100` | `0..60000` |
| `reconnectMaximumDelayMs` | `10000` | `1..300000` |

`initialDelay` must not exceed `maximumDelay`. Each subscription also accepts `signal`, `target`,
required `onValue`, and optional `onError`.

State fields are `subscriptionId`, exact `releaseId`, positive `deliveryRevision`, authoritative
`value`, SHA-256 `resultHash`, optional `snapshotSequence`, and `authorizedUntil`. On disconnect,
the SDK reconnects, obtains the current bearer again, and resubscribes active records. When the
server requires resynchronization, the SDK creates a fresh subscription snapshot. Treat every
delivered state as authoritative; do not interpret it as a complete event log.

## Upload and download files

A Function Action first issues a grant; the client then streams bytes through the Product route:

```ts
const grantResult = await runku.action("files.beginUpload", {
  size: BigInt(file.size),
  contentType: file.type,
})

const uploadGrant = fileUploadGrant(grantResult.value)
const metadata = await runku.uploadFile(uploadGrant, file, {
  contentType: file.type,
})

const download = await runku.action("files.beginDownload", {
  fileId: metadata.fileId,
})

const downloadGrant = fileDownloadGrant(download.value)
const response = await runku.downloadFile(downloadGrant)
const blob = await response.blob()
```

For a range, `range.end` is exclusive:

```ts
await runku.downloadFile(downloadGrant, {
  range: { start: 0n, end: 1024n },
})
```

`fileUploadGrant` and `fileDownloadGrant` validate and refine structural Action results before the
transfer. The client also validates that a grant path is same-origin, attaches only the grant
bearer, bounds the response, and verifies returned file metadata/range headers. Uploads are never
retried. The download timeout stays active until its body closes. See
[Application file storage](../functions/file-storage.md).

## Use the client without generated types

Generated types are recommended but not required:

```ts
type Note = {
  title: string
  priority: bigint
}

const result = await runku.query<readonly Note[]>("notes.list", {})
```

The generic result type is a TypeScript assertion; it does not validate the returned shape. The
server still validates the Function's `returns` contract. For non-TypeScript clients or direct
protocol integration, use the [Public HTTP API](public-api.md).

## Self-Hosted and SaaS

Point `baseUrl` at the Product origin for the selected installation and use a credential issued in
that exact scope. Application code, canonical values, target semantics, and public Function calls
are shared product contracts. Use Runku SaaS to validate application behavior when desired, but
repeat TLS, CORS, identity-provider, quota, storage, timeout, and recovery tests against the actual
Self-Hosted installation.
