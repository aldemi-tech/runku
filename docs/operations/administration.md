# Administration runbook

Application file capacity is a separate operational domain. Monitor committed/reserved quota,
filesystem free-space floor or provider bucket capacity, S3 error/latency, and abandoned multipart
growth without using File IDs or tokens as metric labels. Canary upload, range download, checksum,
and delete after configuration, restore, credential rotation, backend maintenance, and upgrade.
Runku does not back up this byte store; follow the decision and incident procedures in
[Application file storage](../functions/file-storage.md).

This runbook defines operator habits and evidence for the currently composed local Environment and
the acceptance contract for future packaged server roles. It does not invent unavailable remote
administration commands.

## Current administration boundary

The CLI fully administers one local application root. The compact Docker package additionally
attaches one Product Environment to PostgreSQL-backed Platform Identity for browser/invitation
login, scoped invitations, sessions, authenticated publish/release/promote/rollback/status, and
historical/streaming logs plus package-level backup, restore, upgrade, probes, and guarded removal.
It is not the distributed multi-Environment package. Use the
[authenticated remote lifecycle](remote-lifecycle.md) for exact commands, the
[Platform Identity runbook](../auth/platform-identity.md) for trust configuration, and the
[production-readiness checklist](../self-hosting/production-readiness.md) for the remaining boundary.

For packaged process status use `./runku-selfhost status`. It checks container state, liveness,
authoritative PostgreSQL readiness, and the exact server version. Back up with the package helper
before filesystem, image, schema, or dependency maintenance; do not copy a live bind mount.

## Daily local checks

Run from the application root:

```sh
runku status
runku doctor
runku logs --level warn --limit 100
```

Verify:

- `doctor` completes successfully;
- Workspace HEAD resolves to a valid candidate and complete artifact;
- Release/Channel state matches the deployment record;
- Cron activation and source manifest agree;
- no repeated unavailable/corrupt/identity/policy error codes appear;
- application calls and Realtime reconnect work with expected identities.

`doctor` verifies consistency, not load, latency, backup freshness, or absence of dropped telemetry.

## Start and stop

Start only one `runku dev` per project root:

```sh
runku dev --origin http://localhost:3000
```

The process holds a project lease. A second process must fail rather than share the same SQLite
state. Stop with SIGINT/`Ctrl-C`; Runku drops readiness, drains the listener and loops, then closes
stores. Wait for exit before backup, restore, moving state, or changing file permissions.

After an unclean stop:

1. preserve stderr and the latest operational logs;
2. restart once with the same binary/commit and root;
3. run `doctor` after the process is stopped if reopening fails;
4. preserve `.runku/` before restore;
5. do not delete lock/state files individually.

## Release change procedure

1. Build and capture JSON output:

   ```sh
   runku build
   ```

2. Publish exact returned paths. For `--remote`, an explicit observed Workspace HEAD is required.
3. Validate candidate lifecycle against the target Channel; add `--remote` for the Management API.
4. Record `runku status` before change.
5. Promote with `--expected` equal to the observed binding.
6. Run smoke tests using both `release:<id>` and `channel:<name>` targets.
7. Monitor warnings, auth failures, runtime failures, outbox/schedule behavior, and Realtime resync.
8. Record final status and operator identity.

Rollback uses an exact current binding. It changes routing only. If a deployment included an
irreversible data migration, restore/forward-fix decisions are separate from Channel rollback.

## Credential lifecycle

Use separate Application Clients for each trust boundary and independently deployed consumer.

Rotation:

1. list current metadata and identify the exact source credential;
2. create a replacement with `key rotate` and a descriptive label/expiry;
3. deliver the new secret through the consumer's secret channel;
4. deploy and verify calls/log correlation with the replacement credential ID;
5. revoke the old credential;
6. monitor rejected calls for stale consumers;
7. delete only after revocation, evidence retention, and rollback window decisions.

Never solve a scope problem by reusing a more privileged client. Never expose `rk_sec_*` or
`rk_dev_*` through public frontend configuration.

Operator credentials are separate. Bootstrap and delegated `rk_inv_v1_*` codes are single-use;
`rk_at_v1_*` tokens are short-lived; `rk_rt_v1_*` tokens rotate on refresh; every device has an
independently revocable `ops_*` session. Never use `rk_sec_*` as operator authentication or copy an
operator refresh token into application configuration.

If the initial-owner file is lost before the first enrollment, stop the server and use
`runku-server recover-bootstrap` with the explicit confirmation documented in
[Platform operator identity](../auth/platform-identity.md#enroll-the-initial-owner). The operation
revokes the old pending code atomically and cannot reopen bootstrap after an operator exists.

`runku login` normally starts at `https://api.runku.app`; self-hosted operators pass the
installation authentication origin once and can reuse it on later interactive logins. The public
authentication configuration may point at a separate canonical Management origin. Treat both DNS
names, TLS certificates, ingress policies, and `RUNKU_PUBLIC_MANAGEMENT_URL` as one trust change;
do not migrate either silently or through an HTTP redirect.

## Log investigation and retention

Start from a request or invocation ID:

```sh
runku logs --request req_... --stream platform
runku logs --invocation inv_... --stream function
runku logs --client app_... --credential crd_... --level warn
runku logs --remote --release rel_... --follow
```

Save the last cursor. For retention, calculate an absolute Unix-microsecond cutoff, dry-run, review
matched/more/Environment, then apply bounded batches with exact Environment confirmation. Retention
is not credential revocation and does not erase exported or backed-up copies. Run
Run `runku logs archive-status` for a local Product root, or
`runku logs archive-status --remote` for an attached server, before deletion. Use the matching
local `runku logs prune` or authenticated `runku logs prune --remote` path: Runku will not delete hot rows beyond the verified
archive frontier. Standalone embeds this work; HA runs the archive consumer as the same
`runku-server` artifact with `logs-worker`. Use the complete
[Operational Log runbook](operational-logs.md) for configuration, capacity, failure, and restore.

## Incident workflow

1. **Stabilize:** stop unsafe promotion, key rollout, or destructive maintenance; reduce admission
   only when required to protect integrity.
2. **Scope:** record Project, Environment, target, Release/Revision, request/invocation, client and
   credential IDs, timestamps, binary commit, and error codes.
3. **Preserve:** save logs, status, doctor output, configuration hashes, and a state backup when safe.
4. **Classify:** authentication, authorization, compatibility, dependency, capacity, corruption,
   uncertain effect, or security incident.
5. **Recover:** use idempotent retry, CAS reconciliation, credential rotation, Release rollback, or
   verified restore according to the class.
6. **Validate:** health/readiness, doctor, representative Query/Mutation/Action, Realtime reconnect,
   and pending schedule behavior.
7. **Close:** document cause, blast radius, data/effect uncertainty, remediation, and a regression
   test/runbook update.

Security incidents follow [SECURITY.md](../../SECURITY.md) and the
[security model](../security/security-model.md).

## Capacity and maintenance windows

Local defaults are development defaults, not sizing guidance. Before a packaged deployment is
supported, operators need role-specific limits for HTTP/WS admission, V8 workers, background
leases, PostgreSQL pools, S3 requests, queue age, Full Node slots, file descriptors, memory, PIDs,
and graceful-shutdown deadlines.

Schedule a maintenance window when changing persisted-format support, database schema, artifact
runtime support, identity trust configuration, proxy/TLS policy, or isolation assets. A maintenance
plan must include preflight, backup verification, abort condition, rollback/forward-recovery limit,
success signals, and owner.

## Administration acceptance for packaged deployments

A production package must expose authenticated, versioned operations for Projects, Environments,
Releases, Channels, Workspaces, Application/Development credentials, identity providers,
configuration, secrets, limits, backups, upgrades, and audit events. Operators must not need direct
database writes, host filesystem edits, or private crate composition for routine administration.
