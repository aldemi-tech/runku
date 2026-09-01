# Docker deployment profile

Docker currently supports local/conformance dependencies and a dedicated trust-domain execution
unit. It is not yet a published complete Runku Compose installation.

## Current uses

- `compose.storage.yml`: pinned PostgreSQL for storage/repository/realtime/scheduling conformance;
- `compose.remote-execution.yml`: local NATS JetStream and MinIO/S3 for execution/artifact gates;
- Full Node OCI tests: prove image, Node, npm, filesystem, crypto, and restricted TCP contracts;
- a future dedicated `all` image when the complete container/VM belongs to one operator trust domain.

Read each Makefile target before starting resources. Stop only the Compose project created for the
test. `down -v` destroys its test volumes.

## Security boundary

Docker isolation is acceptable for development/conformance or a complete dedicated tenant unit. It
is not the boundary for mutually untrusted Node workloads sharing a host. Gateway and ordinary Runku
roles remain non-root and do not receive host/KVM privileges.

The current Compose files use local credentials, plaintext loopback endpoints, published ports,
single replicas, and deterministic test storage. Never expose them on a network or reuse credentials.

## Requirements for a supported Compose profile

- released digest-pinned Runku images and versioned config schema;
- PostgreSQL/S3 dependencies with TLS, authentication, persistent volumes, backup, and health;
- secret files/provider rather than inline environment values;
- non-root/read-only containers, explicit writable paths, resources, and restart/drain behavior;
- ingress/TLS, origin/host/trusted-proxy policy;
- migrations, startup/readiness/liveness, logs/metrics/traces;
- clean install, upgrade, interrupted upgrade, restore, and uninstall tests.

Until these exist, use Docker assets only for the documented gates.
