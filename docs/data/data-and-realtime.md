# Data and realtime

Runku exposes a logical document model instead of direct SQL. The same contract is implemented by
SQLite for local development and PostgreSQL for production-oriented execution.

## Schema and values

The TypeScript schema defines tables, document fields, validators, and logical indexes. Generated
types associate document IDs with their table and reject unknown Function names or invalid
arguments before a request is sent.

Canonical values include null, booleans, signed 64-bit integers, floating-point values, UTF-8
strings, bytes, timestamps, arrays, objects, and typed document IDs. Persisted and wire encodings are
versioned and bounded.

## Transactions

Queries read one snapshot. Mutations use optimistic concurrency control, an idempotent operation ID,
and an atomic commit containing documents, index changes, outbox events, and scheduled invocations.
No realtime notification is visible before commit.

## Management Data Admin

The authenticated Management API exposes a logical Data Admin boundary for an operator console;
it never accepts SQL, physical table names, index bytes, or caller-supplied index mutations. Every
request names an explicit `release:`, `channel:`, or `workspace:` code target. Runku resolves that
target once, verifies the effective artifact, and uses its exact schema and logical indexes for the
whole operation.

`data:read` authorizes exact document reads and bounded logical-index queries. `data:write`
authorizes insert, replace, and delete. Neither capability is implied by `environments:manage`, and
`data:read` never authorizes a write. Function `db:read`/`db:write` capabilities are a separate
application-execution boundary and never authorize Management requests.

The v1 routes are scoped by the URL Project and Environment:

| Method and suffix | Capability | Contract |
|---|---|---|
| `POST /data/query` | `data:read` | table/index/prefix query, `limit` in `1..=200`, one snapshot |
| `GET /data/documents/{table}/{document}` | `data:read` | exact logical document and OCC revision |
| `POST /data/documents/{table}` | `data:write` | schema-validated insert; ID derives from `Idempotency-Key` |
| `PUT /data/documents/{table}/{document}` | `data:write` | full replace with `expectedRevision` and `previousValue` |
| `DELETE /data/documents/{table}/{document}` | `data:write` | delete with `expectedRevision` and `previousValue` |

Every write requires one valid `Idempotency-Key: opn_*`. Exact replay returns the committed result
with `replayed: true`; reuse for another target, schema, document, value, or precondition returns
`PRODUCT_OPERATION_ID_REUSED`. Replace/delete require the complete value observed at the expected
revision so Runku can derive removals from trusted schema metadata and reconstruct the exact atomic
batch after an uncertain response. A stale revision or mismatched observed value returns a
conflict. Validation failures return `PRODUCT_DATA_VALIDATION_FAILED` without echoing stored data.

Catalog reads use `GET /functions` and `GET /schema/tables` with `target`, `after`, and `limit`
(`1..=200`) under `releases:read`. Responses include both the requested target and immutable
resolved pin, Release identity, serving revision, and schema digest. This lets a console retain an
exact provenance record while paginating stable Function-name or Table-ID cursors.

## Indexes

Logical indexes encode ordered compound keys consistently across storage adapters. Mutations derive
old and new index entries from the trusted schema rather than accepting index keys from application
code.

## Realtime

A subscription executes a Query and registers its dependency set. Committed outbox events are
matched against active dependencies, and affected Queries are rerun. WebSocket reconnect and resync
preserve the selected Environment and code target.

Realtime is authorization-aware: the application key, user identity, origin policy, and Function
policy are evaluated at subscription time and again when required by reconnect or credential
changes.

## Canonical limits and document concurrency

Canonical objects are ordered by UTF-8 key bytes. Integer, float, timestamp, bytes, depth, item
count, string, and envelope limits are enforced before storage/wire processing. Unknown encodings
fail closed. Document IDs remain statically associated with a logical table.

Mutation reads establish an optimistic read-set. Replace/delete require exact revision. Conflict
re-runs business logic from a fresh snapshot within bounded attempts. An operation ID identifies
one Mutation intent across retry/replay and cannot be reused for different arguments.

Index scans use explicit bounds and a bounded limit. Schema evolution must make an index ready
before code assumes it and retire it only after live Releases/subscriptions/schedules no longer
reference it.

## Realtime delivery model

A delivered value is an authoritative Query result, not a domain-event log. Reconnect or
`resync_required` reruns the Query; intermediate WebSocket frames are not replay-guaranteed. Clients
replace local state with each result.

Outbox records commit atomically with data/index/schedule changes. Dispatch may repeat after crash
and must be idempotent. Lag delays Realtime but cannot expose uncommitted state.

## Storage and recovery

SQLite is the local single-process adapter. PostgreSQL is the production-oriented adapter for
concurrency/distributed claims. Both pass the same logical conformance contract; Function code does
not access physical SQL.

An attached `runku-server` Environment can select that adapter with the optional
`RUNKU_PLATFORM_DATABASE_URL` or `_FILE` secret. The database is atomically bound to one exact
Project/Environment and readiness checks it without falling back to SQLite. This selection covers
the logical documents/indexes/Mutation operations/outbox/schedule contract, not every repository in
the Product root. See [Environment-scoped Function platform PostgreSQL](../self-hosting/product-postgresql.md)
for the exact storage, isolation, and recovery boundary.

On corruption or cross-store inconsistency, stop writers and preserve state. Never repair
documents, indexes, outbox, or schedules independently. Follow
[Backup and recovery](../operations/backup-and-recovery.md).

For an uncertain Data Admin write, retry the identical request with the identical operation ID.
After an OCC conflict, fetch the document again and submit a new intent with a new operation ID;
never rewrite `previousValue` merely to force a stale operation through.

## Application checklist

- bound every collection/string/bytes contract;
- enforce resource ownership inside every Function;
- handle not-found and OCC conflict explicitly;
- retain one operation ID for one Mutation intent;
- make scheduled/external effects idempotent;
- handle Realtime reconnect/resync as authoritative refresh;
- evolve schema/indexes compatibly across every live code pin.
