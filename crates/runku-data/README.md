# runku-data

Infrastructure-independent logical storage contract for snapshots, atomic document commits, logical
indexes, durable outbox events, and scheduled invocations. Physical adapters depend on this crate;
the contract does not depend on SQLx or a particular database engine.

See [Data and realtime](../../docs/data/data-and-realtime.md).

Adapters must implement the shared conformance suite for snapshot isolation, OCC/read-set,
idempotent commit/replay, document revisions, ordered index scan, atomic outbox/schedules,
Environment scope, cancellation, and failure mapping. Adding an adapter never changes logical
semantics or exposes physical query APIs to Functions.
