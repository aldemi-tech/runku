# Self-hosting overview

Runku Self-Hosted includes the application-serving path and the administration required to operate
Projects, Environments, code lifecycle, identity, configuration, data, recovery, and observability.

## Current support boundary

The current product implements the local CLI/product process, gateway, runtimes, data/release/identity
repositories, Realtime, scheduling, remote development protocols, PostgreSQL/external-object-store/NATS adapters, Full
Node isolation adapters, and a PostgreSQL-backed Platform Identity Management API slice with
first-owner invitation bootstrap, sessions, scoped grants, and optional OIDC.
Delegated invitation issuance can be reconciled by a durable Operation ID and pending material can
be revoked without recovering its one-time secret.

The `runku-server` distribution composes PostgreSQL-backed Platform Identity and can attach one
initialized Product Environment through `RUNKU_PRODUCT_ROOT`, or a strict shared/dedicated cell
manifest through `RUNKU_CELL_CONFIG`. A shared member opens several isolated Product roots and
keeps their application processes warm behind one Host-dispatched listener. The Environment's transactional
Function data store is SQLite by default or an optional exact-scope PostgreSQL database. The same process embeds hot log
capture, filesystem or S3-compatible Parquet archival, DuckDB historical query, safe retention, and
authenticated live streaming; a small installation does not need a separate observability service.
In that profile, authenticated
operators use the real Workspace/Release/Channel lifecycle, Product Gateway/runtime/background
process, historical logs, and one-connection log streaming. Tagged releases publish Linux GNU
ARM64/x86_64 server archives plus a matching multi-platform, non-root Safe V8 OCI image. The
server also implements `runku-server logs-worker` for the optional NATS-to-S3 HA log path, using the
same server artifact. The project does not yet publish general distributed role/Agent binaries, a
supported Kubernetes package, active-active Product writers, or
rolling multi-node upgrades. Tagged releases do include a supported Docker standalone package with
mounted secret files, probes, bounded resources, a TLS-proxy boundary, offline backup verification,
empty-install restore, upgrade preflight, guarded removal, and optional browser/HA-log overlays. See
[Authenticated remote lifecycle](../operations/remote-lifecycle.md) for the exact compact profile.

Provider automation may initialize a new persistent Product root with an exact previously allocated
scope through `runku init --project-id prj_* --environment-id env_*`. Both IDs are required
together. Repeating the same command is idempotent; a different scope conflicts without replacing
Product state. This lets an external fleet controller reconcile durable identity without editing
private state files or relying on implementation APIs.

Runku Storage provides Environment-scoped Application Files and Runku Object Storage, backed by a
dedicated filesystem directory or an operator-provided S3-compatible prefix. The compact package implements
both choices. Compact backup includes filesystem-backed bytes; external S3-compatible bytes,
replication, versioning, and provider recovery remain operator-owned. See
[Application file storage](../functions/file-storage.md) and
[Storage configuration](storage-configuration.md) before selecting capacity and recovery.

## Product topology

```text
Applications ──HTTP/WS──► Ingress/TLS ─► API/Gateway ─────► PostgreSQL
                                             │                  ▲
                                             ├── Safe V8        │
                                             ├── Realtime       │
                                             └── Background ────┘
                                                    │
                                                    └── outbox/schedules/Cron

Operator/CI ─► Authentication ─► Management ─► Projects, Environments, Releases,
                 login/refresh              Channels, Workspaces, identity/config/audit

Logs, standalone: Product SQLite ─► embedded Parquet archive/DuckDB query
Logs, optional HA: Product SQLite ─► replicated NATS ─► logs-worker ─► S3 Parquet

Optional Full Node: API/Background ─► execution queue ─► Full Node Agents
                              artifacts/OCI registry ◄──────────┘
```

Authentication and Management may share one origin, which is the normal compact installation, or
use separate canonical HTTPS origins. `runku login` discovers the Management origin from the
authentication service and stores both without following redirects.

The VMM used by the shared-untrusted Full Node Agent is an implementation detail, not the product
topology. Safe V8, Gateway, data, Realtime, management, and ordinary workers do not require KVM.

## Process roles required by the distribution

| Role | Responsibility | Privilege |
|---|---|---|
| `api` | HTTP/WS, identity, target resolution, admission, Safe V8 | Non-root, no KVM |
| `background` | Outbox, Realtime dispatch, schedules, Cron, reconciliation | Non-root, no KVM |
| `management` | Project/Environment/code/key/config/backup/upgrade/audit lifecycle | Administrative network/identity, no KVM |
| `all` | Single-instance composition with identical semantics | Dedicated profile |
| `logs-worker` | Optional replicated-journal to immutable Parquet archive | Non-root, no KVM |
| `agent` | Full Node queued execution and isolated worker lifecycle | Depends on selected trust profile |

The same `runku-server` publishes a compact one-Environment composition and a 0.5 cell-member
composition. `shared` packs several warm Environments into one process; `dedicated` validates one
Environment through the same implementation. Its optional
`logs-worker` command is the same binary/image and does not turn standalone into a multi-service
requirement. Other separated product roles and Agent packages are not published yet.

The compact Docker package is the supported installation path. One server container runs the `all`
role and one PostgreSQL container stores Platform Identity. A host TLS proxy is required because
the default Product and Management listeners remain on loopback. A provider-owned private network
may set an explicit application listener only together with its trusted TLS-termination assertion;
local `init`/`link` state remains loopback-only. See
[Docker standalone installation](../../deployments/docker/README.md).

The cell-member contract is portable to provider containers but is not a scheduler or Helm chart.
Each Environment still has one active writer; external placement must fence ownership and prefer
the member already holding the warm assignment. See
[Multi-Environment cell profile](cell-profile.md).

## Storage and dependency profiles

- SQLite: implemented local/standalone hot Product state and Operational Log tier.
- PostgreSQL: authoritative production-oriented logical Product adapter, optionally selected per
  attached Environment with an atomic Project/Environment database binding; also used separately by
  Platform Identity.
- filesystem Parquet + embedded DuckDB: default standalone Operational Log history/query.
- external S3-compatible object-store backend: immutable distributed artifacts and optional log
  Parquet/manifests. This is infrastructure storage, not the Runku Object Storage product surface.
- NATS JetStream: distributed Full Node queue when enabled and replicated Operational Log journal
  only in the optional HA log profile; these use separate named streams/subjects.
- OCI registry: Full Node images referenced by digest.
- secret provider/KMS: required by packaged secret configuration and master-key rotation.
- OTLP collector: optional telemetry destination, never hot-path authority.

Authoritative state never lives only in a Pod/container filesystem or `emptyDir`.

## Runtime trust profiles

| Workload | Profile | Isolation boundary |
|---|---|---|
| TypeScript without Node packages/process/filesystem | Safe V8 | Deny-by-default isolate + Platform Ops |
| Node code in one trust domain | Dedicated host/VM/Pod | Complete deployment unit |
| Local OCI/runtime conformance | Docker | Test/dedicated container, not hostile tenant boundary |
| Mutually untrusted Node code sharing hosts | MicroVM Full Node Agent | VM-grade worker isolation + jailer/controller |

Do not weaken the boundary to resolve an operational issue. Full Node remains optional; prefer Safe
V8 when capabilities suffice.

## Network and identity domains

- public application traffic reaches only API/Gateway through HTTPS/WS;
- management traffic uses separate administrative authentication and network policy;
- PostgreSQL, S3, registry, queue, KMS, and OTLP use TLS/private connectivity and workload identity
  where available;
- browser origins and trusted proxy headers are exact allowlists;
- Full Node egress is capability/policy mediated and deny-by-default;
- Application, functional, development, and administrative identities remain separate.

## Production package requirements

A supported profile needs published artifacts, strict versioned configuration, migrations,
health/readiness/startup, graceful drain, dependency ordering, TLS/proxy/origin guidance,
least-privilege filesystem/network/security context, capacity limits, metrics/traces/logs/audit,
backup/restore, upgrade/rollback, compatibility matrix, and failure-tested runbooks.

See [Production readiness](production-readiness.md) for the complete gate and
[Docker package](../../deployments/docker/README.md) for the supported compact profile.
For the exact one-process and HA Operational Log layouts, variables, failure modes, and runbook, see
[Operational Log storage and administration](../operations/operational-logs.md).

## Install the supported package

For a released compact installation, download `runku-selfhost-vX.Y.Z.tar.gz`, verify it against
`SHA256SUMS`, pin the OCI manifest digest, and follow the
[compact deployment guide](deployment-guide.md) and packaged
[Docker procedure](../../deployments/docker/README.md). Complete installation, first-owner,
publication, backup/restore, security, capacity, and upgrade acceptance on the actual host before
serving production traffic.
