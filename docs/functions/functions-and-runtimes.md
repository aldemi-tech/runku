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

Every new Release targets the cumulative current runtime contract. The artifact class still
selects Safe V8, Full Node, or a hybrid artifact, but declared capabilities do not select an older
or reduced runtime. Legacy `*-1`, `*-2`, and `*-3` identifiers are accepted only while reading
persisted Releases; they are not maintained as separate product editions.

| Function | Semantics | Automatic retry |
|---|---|---|
| Query | Read-only snapshot and Realtime dependency capture | Transport/retryable failures only |
| Mutation | Optimistic concurrency + atomic commit + operation ID | Same operation ID is preserved |
| Action | Mediated/external effects and orchestration | Never automatic |

Capabilities are `db:read`, `db:write`, `auth:read`, `function:query`, `function:mutation`,
`function:action`, `network:https`, `scheduler:create`, `storage:read`, `storage:write`,
`variable:NAME`, and `secret:NAME`. Each Function class accepts a safe subset. An absent capability
removes that context member and is rejected at runtime if bypassed. Variables are available to all
Function classes; secrets are Action-only and both require the exact declared name. See
[Environment variables and secrets](../concepts/environment-configuration.md).

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

Node built-ins resolve in both local and dedicated-host execution. The CLI emits one canonical
Node/hybrid ESM resource bundle and can send it directly to the selected Environment's Development
API; that bundle does not contain `node_modules`, so its direct remote form supports built-ins and
source bundled into the compiled module graph, not unresolved external npm package imports. A
dedicated-host server verifies and materializes the bundle into an immutable cache before reuse.
External production npm dependencies and distributed shared-untrusted publication continue to use
the separate package-lock-bound, digest-bound OCI path. Runtime execution never installs
dependencies.

Local development uses the machine's Node.js. `runku-server` may run bounded persistent Node worker
processes inside its own container only in the explicit `dedicated-host` profile and only when the
complete host/container unit is one trust domain. Shared untrusted Node code requires a VM-grade isolation
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

The current broker contract copies request and response bodies as bounded byte arrays and retains a
16 MiB hard ceiling. Raising that ceiling would consume Function/broker memory and is not the
large-payload path. A future production HTTPS profile must add an Environment-scoped immutable
Application File/Object handle request and response mode, stream without exposing physical
credentials, enforce independent network/storage capabilities and quotas, propagate cancellation,
verify length/digest, and document uncertain external-effect reconciliation. That handle/stream
contract and compact HTTPS broker are not implemented in this release. Use Application File grants
or Runku Object Storage multipart outside the Function envelope where the workflow permits it.

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

For exact declarations, parameters, context methods, capabilities, and limits, use the
[Function API reference](../reference/function-api.md). For task-oriented examples, continue with
[Query, Mutation, and Action](query-mutation-action.md).
