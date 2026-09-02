# Runku on Kubernetes

Kubernetes schedules Runku product roles; it does not replace Runku's data, release, execution,
Realtime, or administration semantics. This directory currently contains only Full Node
conformance manifests. A supported Helm/Kustomize package is not published; the released compact
server image is not by itself a Kubernetes installation.

## Complete product topology

```text
Applications ─► Ingress/TLS ─► API Service/Pods ─► PostgreSQL
                                      │                 ▲
                                      ├─ Safe V8        │
                                      ├─ Realtime       │
                                      └─ Background ────┘

Operator/CI ─► Management Service/Pods ─► lifecycle/config/audit

Small profile: one Runku Pod + persistent Product volume
               └─ hot SQLite + embedded Parquet/DuckDB

HA logs: serving cells ─► replicated NATS journal ─► Runku log-worker Pods ─► S3 Parquet

Optional Full Node: Background/API ─► NATS ─► Full Node Agent Pods
                                S3/OCI ◄───────────────┘
```

API, background, and management run on ordinary nodes without KVM. Only the selected
shared-untrusted Full Node Agent profile needs microVM host privileges.

For one small Environment, prefer one Runku Pod with a `ReadWriteOnce` persistent Product volume;
the embedded log archive/query path adds no sidecar. A StatefulSet replica count greater than one
does not make the same SQLite Environment active-active. The HA log profile instead protects
already-admitted logs with a three-replica JetStream journal and S3-compatible history. It can be
added without enabling Full Node or splitting API/background/management.

## Scheduling and security

- non-root, dropped capabilities, RuntimeDefault seccomp, read-only filesystem for ordinary roles;
- distinct ServiceAccounts and network policies per role;
- topology spread/anti-affinity and PDB aligned with actual replica availability;
- resource requests/limits from role-specific measured workloads;
- PostgreSQL/S3/NATS/registry/KMS/OTLP external endpoints over TLS/workload identity;
- exact Ingress hosts/origins/trusted proxies and WebSocket/drain settings;
- immutable images by digest with admission checks, SBOM/provenance/signatures;
- authoritative state outside `emptyDir`/Pod lifecycle.
- separate ServiceAccounts: serving cells publish logs, log workers consume/ACK and write archive;
- three failure-domain JetStream replicas and object storage with a tested zone-loss objective;
- no raw Product log rows in the Platform Identity PostgreSQL database.

Full Node Agent nodes use explicit label/taint for isolation class, expose required KVM/cgroup/netns
only to Agent Pods, keep controller/assets root-owned, prewarm before readiness, stop queue pulls
before drain, and destructively replace uncertain workers. A privileged Agent Pod is not itself the
user-code sandbox. API Pods never inherit Agent privileges.

## Probes and rollout

- startup: migrations/config/snapshot/prewarm complete within bounded time;
- liveness: supervisor makes progress, not every dependency is healthy;
- readiness: role can safely admit its work with valid schema/snapshot/capacity;
- termination: readiness drops, Service/consumer removes work, HTTP/WS/workers/Agents drain.

Rollout order is preflight → compatible migration → new ready replica → remove/drain old replica →
verify serving/lag/errors → continue. No rolling-upgrade support until `N-1 → N`, interruption, mixed
version, and recovery are tested.

## Scaling signals

- API: admission/queue wait, latency, in-flight, WebSocket/subscription load, DB pool pressure;
- background: outbox/schedule/Cron age, lease contention, retry/poison rate;
- Full Node Agent: NATS queue age, available/busy slots, startup/replacement, node provisioning;
- management: operation latency/failure and serving-revision propagation.
- logs: local admission drops, PubAck lag, journal bytes/oldest age/replica health, durable consumer
  pending/redelivery, archive frontier/age, Parquet size/count, and historical query failures.

CPU alone is insufficient. Scaling must preserve leases, connection budgets, and dependency
capacity.

## Current conformance manifests

| File | Bounded purpose | Not included |
|---|---|---|
| `conformance-dependencies.yaml` | Ephemeral NATS/MinIO for an isolated test | TLS, persistence, HA, real secrets |
| `full-node-agent-conformance.yaml` | KVM Agent placement, slots, readiness, recovery | Product API/management package |
| `full-node-load-conformance-job.yaml` | Distributed load/routing benchmark job | User-facing Gateway deployment |

They use placeholders, local test credentials, `emptyDir`, a local conformance image, and
`imagePullPolicy: Never`. Applying all three does not install Runku. Use only in an isolated campaign
with exact source/assets/environment recorded.

## Future package acceptance

The Kubernetes package must install from released artifacts, validate config/secrets, support
dedicated and multi-node roles, integrate probes/Service/Ingress/NetworkPolicy/PDB/topology/resources,
run migrations/preflight, enable Full Node only when selected, export dashboards/alerts/runbooks,
and pass clean install, node/dependency failure, backup/restore, upgrade, and uninstall campaigns.

See [Self-hosting](../../docs/self-hosting/overview.md),
[Production readiness](../../docs/self-hosting/production-readiness.md), and
[Security](../../docs/security/security-model.md). The standalone/HA decision, exact environment
variables, ordering constraint, retention rules, and incident table are in
[Operational Log storage](../../docs/operations/operational-logs.md).
