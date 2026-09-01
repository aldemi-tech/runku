# Self-hosting overview

Runku Self-Hosted includes the application-serving path and the administration required to operate
Projects, Environments, code lifecycle, identity, configuration, data, recovery, and observability.

## Current support boundary

The source tree implements the local CLI/product process, gateway, runtimes, data/release/identity
repositories, Realtime, scheduling, remote development protocols, PostgreSQL/S3/NATS adapters, and
Full Node isolation adapters with extensive component/vertical gates.

It does not yet publish general-purpose `runku-server`/`runku-agent` binaries, official server
images, a production Compose profile, a supported Kubernetes package, complete remote
administration, or a certified backup/upgrade window. `deployments/` contains contracts and bounded
conformance assets, not installation packages.

## Product topology

```text
Applications ──HTTP/WS──► Ingress/TLS ─► API/Gateway ─────► PostgreSQL
                                             │                  ▲
                                             ├── Safe V8        │
                                             ├── Realtime       │
                                             └── Background ────┘
                                                    │
                                                    └── outbox/schedules/Cron

Operator/CI ─────────────► Management ─► Projects, Environments, Releases,
                                        Channels, Workspaces, identity/config/audit

Optional Full Node: API/Background ─► execution queue ─► Full Node Agents
                              artifacts/OCI registry ◄──────────┘
```

The VMM used by the shared-untrusted Full Node Agent is an implementation detail, not the product
topology. Safe V8, Gateway, data, Realtime, management, and ordinary workers do not require KVM.

## Process roles required by the distribution

| Role | Responsibility | Privilege |
|---|---|---|
| `api` | HTTP/WS, identity, target resolution, admission, Safe V8 | Non-root, no KVM |
| `background` | Outbox, Realtime dispatch, schedules, Cron, reconciliation | Non-root, no KVM |
| `management` | Project/Environment/code/key/config/backup/upgrade/audit lifecycle | Administrative network/identity, no KVM |
| `all` | Single-instance composition with identical semantics | Dedicated profile |
| `agent` | Full Node queued execution and isolated worker lifecycle | Depends on selected trust profile |

These role names describe the required package; the binaries/configuration are not published yet.

## Storage and dependency profiles

- SQLite: implemented local single-process Environment.
- PostgreSQL: authoritative production-oriented data/metadata adapter.
- S3-compatible object storage: immutable distributed artifacts by digest.
- NATS JetStream: distributed Full Node queue only when that runtime profile is enabled.
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
[Deployment assets](../../deployments/README.md) for profile-specific boundaries.

## Evaluation path today

```sh
make install-cli-check
make node-example-check
make chat-example-check
make storage-check
make release-repository-check
make realtime-check
make scheduling-check
make remote-execution-infra-check
```

Read each Makefile target before running it; several start Docker dependencies. These gates prove
component and vertical contracts on the stated environment. They do not prove clean installation,
HA, backup/restore, or supported upgrades for the full product.
