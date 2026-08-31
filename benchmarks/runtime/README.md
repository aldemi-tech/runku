# Runtime regression benchmark

Run `make runtime-benchmark`. The release-profile benchmark covers first invocation, synchronous
handlers, asynchronous Platform Ops, and deadline termination with valid in-memory artifacts and
manifests.

Related dated files cover HTTPS Actions and Query, Mutation, and schema/index coordinators over
SQLite. They are local regression detectors, not node-capacity or latency promises.
