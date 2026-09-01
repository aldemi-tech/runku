# PostgreSQL index regression benchmark

Run `make storage-benchmark`. The target starts the pinned PostgreSQL conformance dependency and
executes the logical-index SQL baseline.

The committed result records the exact local hardware and test profile. Compare changes on the same
class of machine before treating a difference as a regression.

Record PostgreSQL image/settings, schema/data, plan, concurrency, resources, commit, and raw output.
Retain Environment scope, canonical ordering, transaction, and index correctness.
