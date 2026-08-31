# `@runku/client`

Framework-independent TypeScript client for Runku's public HTTP and realtime protocols. The package
works in browsers, server runtimes, Node.js, Vue, Angular, React, Next.js, and plain JavaScript.

## Configure a client

```ts
import { createRunkuClient } from "@runku/client"
import type { RunkuFunctions } from "./runku/_generated/api"

const runku = createRunkuClient<RunkuFunctions>({
  baseUrl: process.env.RUNKU_URL!,
  target: "channel:stable",
  applicationKey: process.env.RUNKU_PUBLISHABLE_KEY!,
  getBearer: async () => currentUserToken(),
})
```

`baseUrl` identifies an instance already bound to one Project and Environment. The client never
declares an Environment as production; protection is enforced by the server.

Supported targets are `release:<id>`, `channel:<name>`, and `workspace:<name>`. There is no implicit
`latest` target.

## Typed calls

```ts
const room = await runku.query("rooms.get", { roomId })
await runku.mutation("messages.send", { roomId, messageId, body, clientSentAt })
const image = await runku.action("images.render", { width: 64, height: 64 })
```

Generated contracts type Function names, arguments, results, table IDs, documents, and realtime
values. JavaScript `bigint` represents i64, `Uint8Array` represents bytes, and `RunkuTimestamp`
represents microsecond timestamps.

## Identity and retries

Use `rk_pub_*` in distributable clients and `rk_sec_*` only in trusted server configuration.
`getBearer` is evaluated for every attempt so applications can refresh user tokens.

Query and Mutation retry only transport or explicitly retryable errors. Mutation preserves its
operation ID across retries. Action is never retried automatically because an external effect may
already have occurred.

## Realtime

```ts
const realtime = runku.realtime()
const subscription = realtime.subscribe("rooms.get", { roomId }, {
  onValue: ({ value }) => render(value),
  onError: report,
})

await subscription.ready
```

The selected target and identity are preserved across reconnect and resync.

## Development

```bash
pnpm --dir packages/client check
```
