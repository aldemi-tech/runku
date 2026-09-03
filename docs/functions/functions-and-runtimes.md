# Functions and runtimes

Functions are declarative TypeScript modules under `runku/`. A file path plus export name forms the
public name: `runku/rooms/messages.ts` exporting `send` becomes `rooms.messages.send`.

Every Query, Mutation, and Action statically declares:

- `auth`: `none`, `optional`, `guest`, `user`, or `service`;
- `visibility`: `public` or nested-call-only `internal`;
- `capabilities`: least-privilege Platform Ops;
- `args` and `returns`: canonical validators;
- `handler`: implementation selected by the artifact/runtime.

The builder rejects computed metadata and runtime validation remains authoritative. TypeScript is
developer feedback, not the security boundary.

| Function | Semantics | Automatic retry |
|---|---|---|
| Query | Read-only snapshot and Realtime dependency capture | Transport/retryable failures only |
| Mutation | Optimistic concurrency + atomic commit + operation ID | Same operation ID is preserved |
| Action | Mediated/external effects and orchestration | Never automatic |

Capabilities are `db:read`, `db:write`, `auth:read`, `function:query`, `function:mutation`,
`function:action`, `network:https`, `scheduler:create`, `storage:read`, and `storage:write`. Each Function class accepts a safe
subset. An absent capability removes that context member and is rejected at runtime if bypassed.

## Safe V8

Safe V8 is default. It executes bounded ESM artifacts and exposes only declared Platform Ops. It
has no direct filesystem, process, arbitrary socket, FFI, environment-variable, or database access.
Query and Mutation always execute Safe; Action may execute Safe and use mediated HTTPS when policy
allows.

Admission, worker concurrency, deadline/cooperation, artifact cache, and resource accounting are
bounded. Unsupported imports, dynamic code-loading paths, Node built-ins, and capability mismatch
fail closed during build or execution.

## Full Node

Put this directive on the first line of an Action module:

```ts
"use runku node"
```

The directive applies to the complete reachable module graph, including imports and top-level code.
Full Node uses the same `action()` and capability-scoped context as Safe. Query, Mutation, and Cron
remain Safe. A Safe module cannot reach a Node-only helper; a Node module cannot import a module
declaring Safe Functions.

Built-ins and production npm dependencies resolve from the application. Remote builds require
`package-lock.json`, install with `npm ci --omit=dev` and lifecycle scripts disabled, and produce a
digest-bound OCI descriptor. Runtime execution never installs dependencies.

Local development uses the machine's Node.js. A dedicated host/VM/Pod may execute Node only when
the complete unit is one trust domain. Shared untrusted Node code requires a VM-grade isolation
profile with verified artifacts, single-flight workers, bounded resources, default-deny egress, and
destructive replacement after timeout/cancellation/uncertain connection loss. Docker alone is not
that isolation boundary.

## Nested calls

Functions compose through `ctx.runQuery`, `ctx.runMutation`, or `ctx.runAction` with the matching
capability. They do not import another Function implementation. Nested execution preserves exact
Release/Dev Revision, Project/Environment, application/functional identity, deadline, and
cancellation. It cannot re-resolve a moved Channel or broaden privilege.

Depth, admission, and deadline are bounded to avoid recursion and worker deadlock. Mutation
composition uses the defined transactional session; Action effects remain potentially uncertain.

## HTTPS Actions

`network:https` exposes a typed broker, not raw sockets. Deployment policy constrains scheme,
method, destination, port, DNS resolution, redirects, private infrastructure ranges, headers/body,
response size, and deadline. Validate remote responses and use their idempotency mechanism when an
effect may need reconciliation.

## Scheduling

Mutation or Action with `scheduler:create` may create durable work:

```ts
await ctx.scheduler.runAfter(
  5_000_000n,
  "notifications.deliver",
  { notificationId },
  { idempotencyKey: `notification:${notificationId}` },
)
```

`runAfter` is microseconds; `runAt` is absolute Unix microseconds. Target Function and arguments are
validated before durable creation. The scheduled invocation captures the exact Release or Dev
Revision. Delivery is at-least-once; handlers that perform external effects need independent
idempotency/reconciliation.

Cron declares a UTC schedule for an existing Mutation or Action. Activation/cursor state is durable
and tied to the versioned manifest. A Channel move does not retarget pending work.

## Application files

Actions may use capability-scoped immutable file storage. Small objects can cross the runtime
boundary directly; larger bodies use one-shot upload and short-lived download grants over streaming
HTTP. Backends, quotas, token security, retry behavior, and operator-owned recovery are specified in
[Application file storage](file-storage.md).

## Failure design

- Query may be re-executed and must remain effect-free.
- Mutation logic must tolerate OCC retry and replay of the same operation ID.
- Action callers must reconcile uncertain effects before retrying.
- Scheduled handlers deduplicate effects independently of delivery count.
- Cancellation/deadline does not prove a remote/Node effect did not occur.
- Artifact/runtime mismatch fails closed without falling back to other code or weaker isolation.
- Logs must not contain arguments, credentials, tokens, or unrestricted user-controlled fields.

See [`@runku/server`](../../packages/server/README.md) for exact types and
[Full Node Actions](../../examples/node-actions/README.md) for executable cross-runtime behavior.
