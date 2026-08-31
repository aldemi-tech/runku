# Platform model

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
