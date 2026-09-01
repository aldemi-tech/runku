# Platform model

The platform model separates persistent application state from immutable code and mutable routing.
Every API, storage key, artifact, cache, subscription, schedule, and operational signal must retain
this scope explicitly.

## Project and Environment

A Project groups one application. An Environment owns persistent data, identity configuration,
application keyrings, Releases, Channels, Workspaces, schedules, and operational state.

Environment identity is included in every storage, cache, artifact, realtime, and invocation key.
Production protection is enforced by the server rather than inferred from SDK configuration.

## Release

A Release is immutable code, contracts, runtime requirements, schema metadata, and content-addressed
artifacts. A client that explicitly requests a supported Release is never silently routed to a
different incompatible Release.

## Channel

A Channel is a mutable pointer to a Release. It provides controlled promotion and rollback without
rebuilding artifacts. The resolved Release is pinned for the duration of a request, subscription,
or scheduled invocation.

## Workspace and Dev Revision

A Workspace is a mutable development target. Each accepted source snapshot produces an immutable
Dev Revision. Concurrent writers use compare-and-swap revisions, and serving continues with the
last valid revision if a new build fails.

## Function classes

- Query reads a consistent snapshot and records dependencies for realtime invalidation.
- Mutation validates and commits document and index changes atomically.
- Action performs capability-scoped effects and may call other Functions.
- Scheduled invocation durably executes a Mutation or Action at a pinned code target.
- Cron materializes scheduled invocations from a versioned declaration.

`job()` is not a separate public primitive. Detached work uses `runAfter` or `runAt` until a future
abstraction can provide semantics beyond scheduling.

## Code targets and pinning

Clients select exactly one `workspace:`, `release:`, or `channel:` target. Resolution produces an
exact Release or Dev Revision before execution. That identity is pinned for one request and nested
calls, one Realtime evaluation/delivery revision, one scheduled invocation, or one Cron activation.
A Channel move affects future resolutions only. There is no `latest` fallback.

## Identity and consistency

Application identity answers which software client is calling; functional identity answers on
whose behalf it acts. Function policy evaluates both, and credential roles cannot be exchanged.

- Query reads one logical snapshot and records dependencies.
- Mutation validates an optimistic read-set and atomically commits documents, indexes, outbox, and
  schedules.
- Action may perform external effects and is not automatically retried.
- Realtime sees only committed state and reruns the authoritative Query.
- Durable work is at-least-once and must be idempotent or reconcilable.

## Lifecycle decisions

| Need | Primitive |
|---|---|
| Live iteration | Workspace + immutable Dev Revisions |
| Stable preview/reproducible client | Explicit Release |
| Traffic promotion/rollback | Channel + compare-and-set |
| Delayed/detached work | `runAfter` / `runAt` |
| Repeated UTC schedule | Cron declaration |
| User identity | External JWT/OIDC + Application Key |

## Invariants

1. Environment identity is never inferred from hostname, target, or user input alone.
2. Changing code creates a new immutable Release/Dev Revision.
3. Mutable pointers use compare-and-set and observable conflict.
4. Unknown/incompatible targets and versions fail closed.
5. Simultaneously served Releases must remain data-contract compatible.
6. Rollback changes routing, not data or completed external effects.
