# Runku deployment profiles

This directory describes how the Runku product maps to standalone, Docker, and Kubernetes
environments. It also contains bounded conformance assets. It does not currently contain a
production installer: server/Agent binaries and official images are not published yet.

## Product roles

Every profile must eventually compose the same roles and semantics:

| Role | Responsibility |
|---|---|
| API | HTTP/WebSocket, identity, target resolution, admission, Safe runtime |
| Background | Outbox, Realtime dispatch, schedules, Cron, reconciliation |
| Management | Project/Environment/code/identity/config/backup/upgrade/audit lifecycle |
| Full Node Agent | Optional isolated Node execution |

Infrastructure technology does not redefine these responsibilities.

## Profiles

| Profile | Intended use | Current material |
|---|---|---|
| [Standalone](standalone/README.md) | Dedicated machine/VM and host requirements | Trust profiles and host prerequisites |
| [Docker](docker/README.md) | Local dependencies, conformance, dedicated trust-domain packaging | Existing Compose dependencies and boundaries |
| [Kubernetes](kubernetes/README.md) | Dedicated or multi-node single-region topology | Product architecture + Full Node conformance manifests |
| [`full-node-microvm/`](full-node-microvm) | Implementation-specific shared-untrusted worker assets | Guest init and Agent conformance image only |

## State and dependencies

PostgreSQL and S3-compatible artifacts are authoritative in production-oriented topology. NATS is
used by distributed Full Node when enabled. Registry, secret provider/KMS, TLS ingress, and OTLP
integrate as deployment dependencies. Pods/containers may hold only reconstructible caches and
scratch.

## Non-production assets

Compose and Kubernetes conformance files contain loopback/published test ports, local credentials,
single replicas, `emptyDir`, placeholders, and local image policy. They are safe only in isolated
test environments. Do not expose them, promote their credentials, or infer support from applying a
manifest.

A supported profile must satisfy the
[production-readiness contract](../docs/self-hosting/production-readiness.md): published artifacts,
strict configuration, probes/drain, TLS/secrets, migrations, backup/restore, upgrades, observability,
security, capacity, compatibility, and failure-tested runbooks.
