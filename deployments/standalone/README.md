# Standalone deployment profile

Standalone means a dedicated machine or VM running one compact Runku instance or one separated
product role. Tagged releases publish the Linux server archive and a supported Docker Compose
package for the compact Safe V8 profile. Native service-unit templates and separated distributed-
role packages are not published. This document fixes the common trust and host contract.

## Recommended small topology

A small installation should run one `runku-server serve` process for API, background, management,
Safe V8, Realtime, schedules, and Operational Logs, with PostgreSQL as the external identity store
and one persistent Product root. Filesystem Parquet history and embedded DuckDB require no extra
daemon. Full Node remains optional.

```text
Internet ─► TLS proxy ─► runku-server serve ─► Platform Identity PostgreSQL
                           │
                           ├─ Product root: repositories and SQLite logical store by default
                           ├─ optional Function platform PostgreSQL: transactional logical store
                           └─ Product root: immutable Parquet log history
```

Use S3-compatible history when off-host log retention is wanted without HA. Add NATS JetStream and
the same image as `runku-server logs-worker` only when the recovery objective requires journaled
logs across node loss. That choice does not require splitting the remaining Product roles.

The installable implementation of this topology is the
[Docker standalone package](../docker/README.md). It deliberately uses Linux host networking so the
Product's loopback-only listener remains reachable only by the host TLS proxy. A native installation
must preserve the same listener, UID, filesystem, secret-file, probe, backup, and upgrade contracts.

## Filesystem and service contract

- install the checksum-verified released binary read-only and run it as a dedicated non-root user;
- mount an absolute `RUNKU_STATE_DIRECTORY` for Platform Identity bootstrap material and an
  absolute `RUNKU_PRODUCT_ROOT` for the initialized Environment;
- persist the entire Product root, including `.runku/observability.sqlite3` and
  `.runku/observability-archive/`;
- inject `RUNKU_PLATFORM_IDENTITY_PEPPER_FILE`, `RUNKU_IDENTITY_DATABASE_URL_FILE`, optional
  `RUNKU_PLATFORM_DATABASE_URL_FILE`, and optional OIDC/storage
  credentials from root-controlled secret files or an equivalent secret provider;
- run `runku-server check` before restart, `runku-server migrate` in a serialized maintenance step,
  and only then `runku-server serve`;
- expose management through an exact HTTPS origin and trusted TLS termination; do not bind a
  plaintext non-loopback listener;
- stop with the normal termination signal and allow graceful drain before killing the process.

The full configuration and lifecycle are in
[Platform operator identity](../../docs/auth/platform-identity.md),
[Environment-scoped Function platform PostgreSQL](../../docs/self-hosting/product-postgresql.md),
[Authenticated remote lifecycle](../../docs/operations/remote-lifecycle.md), and
[Operational Log storage](../../docs/operations/operational-logs.md).

## Full Node trust choices

- **Dedicated:** Node executes directly only when the entire machine/VM/service account belongs to
  one trust domain. Do not mix untrusted applications.
- **Shared untrusted:** requires Linux x86_64, KVM, cgroup v2, PID/mount/network namespaces,
  nftables, verified VMM/jailer/kernel/rootfs/controller assets, default-deny egress, and bounded
  single-flight workers.

[`full-node-microvm.env.example`](full-node-microvm.env.example) documents the current microVM controller
contract. It contains no token and is not complete server configuration. Technology-specific names
remain in this implementation-level file because they map to real controller variables.

## Host hardening requirements

- dedicated unprivileged service identities for ordinary roles;
- root-owned configuration/controller/assets; secrets `0600` or secret-manager mounted;
- read-only binaries/assets and explicit data/cache/scratch paths;
- TLS/private dependency networking and exact firewall flows;
- file descriptor, PID, CPU, memory, disk, and cgroup limits;
- time synchronization, log rotation, audit, and backup agents;
- graceful service stop/drain and startup dependency/readiness ordering;
- verified packages, checksums, SBOM, provenance, and update procedure.

## Operational acceptance

Before support: clean install on a documented OS/architecture; service restart/host reboot; process
kill and dependency outage; backup/total-loss restore; `N-1 → N`; capacity/overload; credential and
isolation-asset rotation; uninstall with explicit data-retention choice.

For logs, acceptance additionally covers process kill/restart, archive frontier verification,
cursor-bounded pruning, full-disk behavior, filesystem backup/restore, and—when HA is selected—NATS
quorum loss, archive-worker replacement, S3 outage, redelivery, and zone loss.
