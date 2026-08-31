# Docker profile

Docker is used for local PostgreSQL, NATS JetStream, MinIO/S3, artifact conformance, and Full Node
artifact tests. A dedicated deployment may also use Docker when the complete container or VM belongs
to one trust domain.

Docker is not the isolation boundary for mutually untrusted Full Node tenants. Shared hostile code
requires the Firecracker and jailer profile.

The Compose files use local credentials, published ports, and single replicas for deterministic
tests. They are not production defaults and must not be exposed publicly.
