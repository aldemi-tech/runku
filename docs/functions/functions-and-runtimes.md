# Functions and runtimes

Functions are declared in TypeScript under the application's `runku/` directory. File paths and
export names form the public Function name. For example, `runku/rooms/messages.ts` exporting `send`
becomes `rooms.messages.send`.

## Safe V8

Safe V8 is the default runtime. It exposes only Runku Platform Ops and does not provide direct
filesystem, process, socket, FFI, or database access. Query and Mutation execute in this runtime.
Action can use mediated HTTPS when its declared policy allows it.

## Full Node

Place this directive on the first line of a module:

```ts
"use runku node"
```

Every Function in that file then executes with Node.js. The file-level boundary prevents a module
graph containing Node.js built-ins or npm dependencies from being loaded into Safe V8.

Full Node uses the same `action()` declaration and typed context as Safe V8. Calls between Safe and
Node Functions cross an internal broker, so an Action may call another Action, Query, or Mutation
without exposing runtime placement to application code.

Local development uses the machine's Node.js installation. Dedicated deployments may use a native
Node process inside a single trust domain. Shared untrusted execution requires the Firecracker and
jailer profile described by the deployment documentation.

## Scheduling

An Action may detach durable work:

```ts
await ctx.scheduler.runAfter(5_000, "notifications.deliver", { notificationId })
```

The scheduler captures the exact Release or Dev Revision. Delivery is at-least-once; handlers that
perform external effects must use idempotency keys.
