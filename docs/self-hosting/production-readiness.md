# Production-readiness contract

This is the auditable acceptance contract for declaring a Runku Self-Hosted profile supported. The
current source line has not completed this checklist. Component tests and deployment conformance
assets are evidence inputs, not substitutes for a releasable product package.

## Definition of supported

An independent operator must be able to install, configure, secure, administer, observe, back up,
restore, upgrade, diagnose, and remove a version using only published artifacts and public
documentation. Routine operations must not require compiling private composition code, editing
database rows, or reverse-engineering crates.

## Release artifacts

- [ ] Versioned `runku` CLI, server, and Full Node Agent artifacts are published.
- [ ] OCI images are non-root, minimal, immutable, multi-stage, and referenced by digest.
- [ ] Checksums, SBOM, provenance, licenses, and signatures are published and verifiable offline.
- [ ] Source tag, changelog, compatibility matrix, upgrade notes, and known issues agree.
- [ ] Clean-room installation does not use this workspace's `target/`, local images, or prior state.

## Configuration and process roles

- [ ] Supported `api`, `background`, `management`, `all`, and optional `agent` roles are defined.
- [ ] One versioned configuration schema supports file/environment layering and strict validation.
- [ ] Unknown keys/versions, invalid combinations, missing dependencies, and unsafe defaults fail
      before readiness with redacted actionable errors.
- [ ] Data, cache, config, secret, artifact, and ephemeral paths are separate.
- [ ] Liveness, readiness, startup, version/provenance, migrations, and graceful drain are exposed.
- [ ] TLS termination, trusted proxies, host/origin rules, timeouts, and CORS/WS policies are
      documented and tested.

## Administration

- [ ] Authenticated, versioned Admin API/CLI covers Projects, Environments, Releases, Channels,
      Workspaces, Application/Development credentials, identity providers, config, secrets, limits,
      backups, upgrades, retirement, and audit.
- [ ] Production protection rejects Workspace serving/sync and unsafe lifecycle operations.
- [ ] Credential/secret rotation supports overlap, verification, revocation, and audit without
      downtime.
- [ ] Management-path loss does not stop known application serving or already materialized work.
- [ ] Multiple Projects/Environments demonstrate no cross-scope data, artifact, cache, log, queue,
      identity, or Realtime access.

## Data lifecycle

- [ ] PostgreSQL is authoritative and migrations are forward-only, restartable, and preflighted.
- [ ] Schema/index evolution follows expand → backfill/migrate → contract.
- [ ] Backfills are bounded, resumable, observable, cancelable, and promotion-aware.
- [ ] Index states, Release compatibility, retention roots, retirement, and safe garbage collection
      are explicit.
- [ ] Pagination/cursors, deletion, revision, Realtime causal floor, schedule inspection/cancel, and
      Cron missed-tick behavior have stable public contracts.

## Backup, restore, upgrade, and portability

- [ ] All authoritative, reconstructible, and ephemeral state is inventoried.
- [ ] PostgreSQL + object-storage backup produces a versioned manifest and verified checksums.
- [ ] Offline backup verification detects partial/corrupt/incompatible backups.
- [ ] Total-loss restore preserves intended IDs, targets, keyrings, schedules, and clients.
- [ ] Logical export/import is versioned and does not copy secrets/credentials by default.
- [ ] `N-1 → N`, interrupted upgrade, resume, rollback limit, and mixed-version window are tested.
- [ ] RPO/RTO are measured on each supported profile.

## Multi-node correctness

- [ ] Serving/config revisions propagate monotonically and resync from authority.
- [ ] Outbox, scheduler, and Cron use proven leases/fencing and crash recovery.
- [ ] Realtime handles reconnect, resync, lag, fairness, eviction, and credential revocation across
      API replicas.
- [ ] Rolling drain does not resolve the wrong Release or publish pre-commit notifications.
- [ ] Failure campaigns cover process kill, node loss, dependency outage, partition, overload,
      recovery, and repeated delivery.
- [ ] PostgreSQL pools, HTTP/WS admission, runtime workers, artifacts, queue, and background budgets
      have published safe defaults and limits.

## Observability and operations

- [ ] Operational logs and security audit are distinct, retained, redacted, and queryable.
- [ ] Metrics/traces cover gateway, identity, releases, runtime, data, Realtime, workers, dependencies,
      Full Node, and management.
- [ ] Cardinality budgets prevent user-controlled labels from exhausting collectors.
- [ ] Dashboards and alerts map every page to a public runbook and success/recovery signal.
- [ ] Capacity baselines include raw outputs, exact environment, workload, and non-SLA interpretation.
- [ ] Routine maintenance and incidents can be resolved without direct database inspection.

## Security

- [ ] Threat analysis covers public/admin/development networks, storage, artifacts, build, runtime,
      nested calls, identity, Realtime, egress, logs, backup, and upgrade.
- [ ] Images use least privilege, read-only filesystem where possible, seccomp/AppArmor guidance,
      dedicated ServiceAccounts, and explicit Linux capabilities.
- [ ] Secrets never appear in config maps, arguments, layers, generated types, bundles, errors,
      logs, traces, or backups without declared encryption.
- [ ] Safe V8 and every Full Node profile pass adversarial isolation and resource-exhaustion tests.
- [ ] Network egress is deny-by-default and mediated; DNS/redirect/private-range controls are tested.
- [ ] Supported-version, embargo, signing-key, disclosure, and emergency-upgrade procedures exist.

## Full Node profile

- [ ] Dedicated and shared-untrusted boundaries are named by trust model, not by convenience.
- [ ] Artifact build/install/runtime phases are separated; runtime never installs dependencies.
- [ ] Registry/S3/NATS auth, TLS, replay, timeout, cancellation, uncertain effect, and poison work are
      failure-tested.
- [ ] Shared untrusted workers use a VM-grade boundary with verified kernel/rootfs/VMM/controller,
      single-flight slots, destructive replacement after uncertainty, and default-deny egress.
- [ ] Queue age, slot use, startup, replacement, CPU, RSS, ephemeral disk, and cache signals drive
      published capacity guidance.

## Deployment profiles

For each announced standalone, Docker, or Kubernetes profile:

- [ ] versioned package and exact prerequisites;
- [ ] minimum/supported dependency matrix;
- [ ] configuration and secret examples with no test credentials;
- [ ] resources, limits, placement, network, storage, probes, and drain;
- [ ] clean install, smoke test, upgrade, rollback limit, backup/restore, and uninstall;
- [ ] topology-specific security and failure matrix;
- [ ] ownership/support boundary and known limitations.

## Release decision

The release owner records evidence links for every checked item, unresolved risks, measured limits,
supported versions, and a signed go/no-go decision. Unchecked items remain visible; they are not
converted into vague “beta” language.
