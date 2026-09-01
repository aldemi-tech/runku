# System architecture

Runku is a modular Rust workspace deployed initially as a modular monolith with replaceable storage,
artifact, queue, and runtime adapters.

```mermaid
flowchart LR
  Client[Client or server SDK] --> Gateway[HTTP and WebSocket Gateway]
  Gateway --> Identity[Application and functional identity]
  Gateway --> Router[Environment and code-target router]
  Router --> Execution[Query, Mutation, and Action coordinators]
  Execution --> Safe[Safe V8 runtime]
  Execution --> Node[Full Node broker and Agents]
  Execution --> Store[LogicalStore]
  Store --> SQLite[SQLite local adapter]
  Store --> Postgres[PostgreSQL adapter]
  Store --> Outbox[Durable outbox and scheduler]
  Outbox --> Realtime[Realtime matcher]
  Outbox --> Workers[Scheduled and Cron workers]
  Router --> Releases[Release and Workspace repositories]
  Releases --> Artifacts[Filesystem or S3 artifacts]
```

## Serving path

The Gateway authenticates application and functional identity, resolves an Environment and exact
code target, validates Function policy and contracts, and invokes the appropriate coordinator. A
request pins its resolved code identity for its entire lifetime.

## Data path

Query records a snapshot and dependency set. Mutation executes optimistic reads and writes, then
commits documents, logical indexes, outbox events, and schedules atomically. Realtime consumes only
committed outbox state.

## Runtime path

Safe V8 executes bounded ESM artifacts and Platform Ops. Full Node artifacts include an OCI
descriptor and Node bundle. The broker carries typed nested calls so application semantics remain
independent of runtime placement.

## Management path

Projects, Environments, Releases, Channels, Workspaces, keyrings, and serving revisions are durable
state. Serving uses the last valid snapshot during a temporary management-path outage; management
availability is not required for every application request.

## Component boundaries

| Component | Owns | Must not own |
|---|---|---|
| Gateway | transport, identity admission, target resolution, envelopes | direct SQL/business logic |
| Execution coordinators | Query/Mutation/Action/nested semantics | physical storage/runtime placement |
| LogicalStore | snapshots, atomic commits, outbox, schedules | HTTP or Function source parsing |
| Release/Development repositories | manifests, lifecycle, Channels, Workspace revisions | application documents |
| Runtime supervisor/Agents | bounded artifact execution and Platform Ops bridge | target/auth policy decisions |
| Realtime | subscription lifecycle/dependency matching | pre-commit events or business authorization bypass |
| Background workers | outbox/schedules/Cron materialization with leases | changing pinned code |
| Identity | Application/functional trust and policy | application resource ownership |

## Consistency and ownership

Serving resolves one Environment and exact code identity before execution. Mutation commits data,
indexes, outbox, and schedules atomically. Dispatch/worker delivery is at-least-once and uses durable
identity plus lease/fencing. Artifacts publish before pointers. Mutable pointers use compare-and-set.
Unknown scope/version/runtime fails closed.

## Process topology

A packaged distribution may compose `api`, `background`, `management`, and optional Full Node
`agent` roles, or an `all` role for a dedicated instance. Role separation changes process placement,
not semantics or sources of truth. API/background/management do not require KVM. Only the selected
shared-untrusted Full Node Agent profile receives host isolation privileges.

## Failure containment

- management outage: serve last valid known revision; reject unknown changes;
- PostgreSQL outage: affected authoritative operations fail/unready, never fall back to local state;
- artifact outage: cached verified artifacts may serve within policy; unknown content fails;
- outbox/worker crash: durable records replay with idempotent handling;
- Realtime socket loss: clients reconnect and resync authoritative Query state;
- Agent loss/uncertain Node result: queue/reconcile and replace worker; never report uncertain success;
- telemetry outage: bounded buffering/drop accounting without blocking authoritative state;
- incompatible Release/config: reject activation/promotion before serving.

## Scaling constraints

API scales by admission, latency, WebSocket/subscription load, and storage pools. Background scales by
outbox/schedule/Cron lag with lease correctness. Full Node scales by queue age and available slots.
Management scales independently and is not on every request path. Every cache/pool/queue carries
Project/Environment/code scope and a bounded capacity policy.

## Evolution rule

New adapters implement existing traits and pass common conformance. A new public or persisted
contract is versioned before crossing its boundary. Do not implement semantic shortcuts in one
process profile. See [Repository map](repository-map.md) and
[Evolving Runku](../development/evolving-runku.md).
