# runku-data

Infrastructure-independent logical storage contract for snapshots, atomic document commits, logical
indexes, durable outbox events, and scheduled invocations. Physical adapters depend on this crate;
the contract does not depend on SQLx or a particular database engine.

See [Data and realtime](../../docs/data/data-and-realtime.md).
