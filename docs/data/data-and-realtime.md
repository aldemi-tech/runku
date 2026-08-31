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
