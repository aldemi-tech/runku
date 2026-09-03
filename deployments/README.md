# Runku deployment profiles

This directory describes how the Runku product maps to standalone, Docker, and Kubernetes
environments. It contains the compact server image definition, a supported Docker standalone
package, optional browser/HA-log overlays, and bounded conformance assets. Tagged releases publish
the compact server binary/image and installation archive. General distributed roles, Agent, and
Kubernetes packages remain separate readiness gates.

The compact Docker profile supports Environment-scoped application files on a dedicated filesystem
mount or an externally operated S3-compatible backend. Runku does not provision that object service
or manage its backups, replication, versioning, encryption, or lifecycle; see
[Application file storage](../docs/functions/file-storage.md).

## Product roles

Every profile must eventually compose the same roles and semantics:

| Role | Responsibility |
|---|---|
| API | HTTP/WebSocket, identity, target resolution, admission, Safe runtime |
| Background | Outbox, Realtime dispatch, schedules, Cron, reconciliation |
| Management | Project/Environment/code/identity/config/backup/upgrade/audit lifecycle |
| Log archive worker | Optional HA journal consumption and immutable Parquet commit |
| Full Node Agent | Optional isolated Node execution |

Infrastructure technology does not redefine these responsibilities.

## Profiles

| Profile | Intended use | Current material |
|---|---|---|
| [Standalone](standalone/README.md) | Dedicated machine/VM and host requirements | Native contract and Docker compact package |
| [Docker](docker/README.md) | One Safe V8 Environment on a dedicated Linux host | Supported compact Compose package and optional log overlays |
| [Kubernetes](kubernetes/README.md) | Dedicated or multi-node single-region topology | Product architecture + Full Node conformance manifests |
| [`full-node-microvm/`](full-node-microvm) | Implementation-specific shared-untrusted worker assets | Guest init and Agent conformance image only |

## State and dependencies

The small profile keeps Product state, hot logs, and Parquet history on one persistent Product
volume and embeds DuckDB in `runku-server`; it does not deploy a log database or query daemon. The
HA log profile uses a distinct replicated NATS JetStream journal and S3-compatible immutable
Parquet archive, with `runku-server logs-worker` from the same image. PostgreSQL retains identity
and authoritative application state, not raw Product logs. Registry, secret provider/KMS, TLS
ingress, and OTLP integrate as deployment dependencies.

Read [Operational Log storage and administration](../docs/operations/operational-logs.md) before
choosing the profile. Only reconstructible caches and scratch may use ephemeral Pod/container
storage; standalone Product data and HA JetStream data need persistent storage.

## Conformance-only assets

Root-level conformance Compose files and Kubernetes manifests contain loopback/published test ports,
local credentials, single replicas, `emptyDir`, placeholders, and local image policy. They are safe
only in isolated test environments. They are distinct from `deployments/docker/compose.yaml`; do not
promote their credentials or infer support from applying a conformance manifest.

A supported profile must satisfy the
[production-readiness contract](../docs/self-hosting/production-readiness.md): published artifacts,
strict configuration, probes/drain, TLS/secrets, migrations, backup/restore, upgrades, observability,
security, capacity, compatibility, and failure-tested runbooks.
