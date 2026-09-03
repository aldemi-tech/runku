# Troubleshooting guide

Start with the stable error code, not a guessed root cause. Preserve current state before any
repair and distinguish safe retry, conflict reconciliation, uncertain external effect, and durable
corruption.

## First five minutes

```sh
runku status
runku doctor
runku logs --level warn --limit 100
runku --version
git rev-parse HEAD
```

Record application root, timestamp/timezone, OS/architecture, listener, requested target,
request/invocation IDs, client/credential IDs, Release/Revision, exit code, and `error:` code. Never
attach keys, JWTs, peppers, dotenv secrets, DSNs, Function arguments, or full `.runku/` state to a
public issue.

## File upload or download fails

1. Record the stable `FILE_STORAGE_*` code and `x-runku-request-id`, but never the transfer token.
2. `FORBIDDEN` means a malformed/expired/wrong-scope token; obtain a fresh grant after repeating
   application ownership authorization. `CONFLICT` on PUT normally means the one-shot grant was
   consumed; reconcile instead of replaying it. `LIMIT_EXCEEDED` means declared size, Environment
   quota, concurrency, live-grant admission, Action-memory, or filesystem free-space policy
   rejected the operation.
3. For `UNAVAILABLE`, check the dedicated directory ownership/free space or S3 TLS, DNS, bucket,
   prefix policy, credentials, throttling, and multipart lifecycle. Do not weaken endpoint TLS or
   broaden credentials as a diagnostic shortcut.
4. For `CORRUPT`, stop issuing grants for the affected workflow, preserve metadata/provider audit
   evidence, verify object length/SHA-256 and the coordinated backup, then restore or remove through
   an application-authorized procedure.
5. Run the bounded canary and relevant tests listed in
   [Application file storage](../functions/file-storage.md#evidence-and-diagnosis).

## Symptom map

| Symptom | Likely class | First check |
|---|---|---|
| `runku dev` says process already running | Lease/conflict | Find the owner; stop cleanly, do not delete locks |
| Listener unavailable | Port conflict/config | Check initialized listener and owning process |
| Source change not served | Build/watch policy | Read build error; last valid revision should remain active |
| 401/403 application call | Key/JWT/policy/origin | Validate each authorization axis separately |
| Function not found/wrong contract | Target/Release mismatch | Inspect target and generated types for that Release |
| Mutation conflict | OCC or stale expectation | Re-read document/current pointer; retry with new intent |
| Action timeout/transport failure | Effect uncertain | Reconcile external idempotency key before retry |
| Realtime stops updating | Connection/resync/dependency/auth | Check WS auth/origin, resync, Query success, outbox path |
| Scheduled work repeats | At-least-once delivery | Verify idempotent handler/effect key; inspect invocation logs |
| `doctor` inconsistent/corrupt | Durable integrity | Stop writes, preserve state, restore verified backup |
| Remote publish exit 9 | Outcome uncertain | Query remote state using operation/revision before retry |
| `runku login` exit 7 | Platform invitation invalid/inactive | Obtain the intended unconsumed code; do not substitute an application key |
| Remote lifecycle exit 7 | Missing, expired, malformed, or revoked operator session | Run `runku login`; the CLI already attempted one refresh and never falls back to `rk_sec` |
| Remote lifecycle exit 8 | Current grant lacks the capability at the root's exact Environment | Delegate only the required capability/scope; do not widen the Application key |
| Remote publish exit 2 with expected-head requirement | No explicit remote Workspace CAS was supplied | Read/reconcile the current head, then pass `--expected-head empty|drv_*` |
| Remote log follow exits after revocation | Session/grant was rechecked during the stream | Re-authenticate or restore the intended `logs:follow` grant; do not reconnect with a stale bearer |
| Management readiness fails | Platform Identity or configured Product PostgreSQL/schema unavailable | Preserve server error code; verify both dependencies and migration checksums |
| OIDC login returns 401 | Issuer/audience/claim/signature/JWKS/link | Check exact provider policy and whether first login included a valid invitation |
| Browser OIDC callback times out | Browser could not complete provider flow or reach loopback callback | Verify native-client loopback redirect policy, local firewall, provider endpoints, and retry with fresh PKCE state |
| `PLATFORM_AUTH_CONFIGURATION_*` | Authentication discovery was unavailable, malformed, contradictory, or advertised an unsafe Management origin | Verify the exact authentication URL, TLS certificate, `/v1/auth/config` v1 response, and public Management URL; do not follow a redirect manually |
| `PLATFORM_LOGIN_SELECTION_REQUIRED` | More than one login method was advertised but stdin is non-interactive | Select explicitly with `--browser`, `--code-env`, or `--oidc-token-env` |

## Local startup failures

### Invalid project path/state

Run from a regular application directory containing `runku/`, or pass `--root`. Do not use `/`, the
home directory, a symlinked root, or source symlinks. If state exists, do not reinitialize with
different Workspace/listener values. Inspect permissions and preserve `.runku/`.

### Listener unavailable

The listener is durable local state. Identify the process using the address. Stop that process or
initialize a different application root before state exists. Do not manually edit
`local-state-v1.json`.

### Dotenv conflict

Runku detects values belonging to a different Environment. Interactive use asks before local
replacement; non-interactive use fails unless `--replace-remote-credentials` is explicit. Choose
whether the application should continue using remote values or local development; do not merge URL,
target, and keys from different Environments.

## Build/watch failures

- syntax invalid: fix the reported source and save again;
- source policy denied: remove unsupported import/dynamic behavior/path/runtime mixing;
- config invalid: verify one schema, static declarations, validators, capabilities, indexes, Cron;
- unsupported feature: use a supported declaration/runtime or update CLI/runtime together;
- limit exceeded: reduce bounded module/source/contract/artifact size;
- unstable snapshot: wait for generators/editors to finish atomic writes;
- output conflict/corrupt: do not edit immutable builds; preserve output and investigate writers/disk.

The source watcher must not replace Workspace HEAD after failure. Confirm `status` still references
the last valid revision.

## Authentication/authorization failures

Check independently:

1. base URL and HTTPS/loopback policy;
2. exact target and Environment protection;
3. Application Key shape, active lifecycle, client kind, and scope;
4. Function `visibility` and `auth` policy;
5. bearer issuer, audience, algorithm, signature, time bounds, principal kind, and JWKS freshness;
6. browser Origin allowlist for HTTP and WebSocket;
7. Function-level ownership/role checks.

A publishable key is not a secret but is still required. A valid user JWT does not replace an
Application Key. A development key cannot invoke Functions.

For Management API failures, validate a different axis: `rk_at_v1_*` authenticates a current
operator session, then capabilities are checked at installation/Project/Environment scope.
`rk_pub_*`, `rk_sec_*`, and `rk_dev_*` are always rejected at this boundary. An external OIDC token
authenticates only the configured provider identity and creates a Runku session; first enrollment
also requires a scoped single-use invitation. See
[Platform operator identity](../auth/platform-identity.md#failure-handling).

## Data and Mutation failures

OCC conflict means a document changed after it was read. Re-run business logic from a fresh read;
do not blindly repeat a stale expected revision. Mutation retries preserve operation ID and may
return a replayed result. Keep the same operation ID only for the same logical intent and arguments.

Never manually update documents/indexes/outbox/schedules in storage. Their atomic relationship is a
product invariant.

## Realtime failures

1. call the subscribed Query over HTTP with the same target/key/bearer;
2. verify WebSocket Origin and authentication frame;
3. inspect reconnect and `resync_required` behavior;
4. check that the triggering Mutation committed successfully;
5. correlate request/invocation/Release IDs and outbox/dispatcher warnings;
6. accept the new authoritative Query value; do not demand frame replay.

## Safe V8 and Full Node failures

Safe V8 failures commonly indicate denied capability, deadline, artifact mismatch, or runtime
limit. Full Node additionally has build/OCI, queue, artifact retrieval, process/microVM startup,
network policy, cancellation, and replacement classes. After cancellation/deadline/connection loss,
assume an external effect may have occurred unless an idempotency/reconciliation protocol proves
otherwise.

Do not move untrusted Node code to a weaker profile as a workaround.

## Dependency and capacity failures

For PostgreSQL, S3-compatible storage, NATS, registry, or OTLP, check DNS, TLS, credentials, time,
connection/pool limits, quotas, latency, and dependency health from the affected role. Readiness
should fail only when that role cannot safely admit its work. Collector failure must not stop the
application hot path.

Capacity evidence requires queue/admission wait and dependency pressure, not CPU alone.

## Compact Docker installation failures

Run `./runku-selfhost status` first. A configuration failure before PostgreSQL access usually means
an unpinned image, unsafe/non-absolute data or secret directory, mismatched directory UID/GID,
partial secret set, simultaneous direct and `_FILE` secret inputs, malformed public Management URL,
or an invalid browser/HA overlay. Correct the exact input; do not copy secret values into `.env` to
bypass a mount problem.

`probe-live` proves the Management process answers on its configured loopback listener.
`probe-ready` additionally performs the authoritative Platform Identity PostgreSQL check and, when
configured, the Environment-scoped Product PostgreSQL check. Product
port 3210 is expected to remain closed until a Channel has been promoted. After a restart with an
existing Channel it must reopen automatically; if it does not, preserve server logs and Product
state rather than re-promoting blindly.

A backup failure restarts serving only when it was running before the attempt. Preserve a partial
backup directory only as diagnostic evidence; it is never a recovery point without a valid manifest
and successful `verify-backup`. Restore intentionally refuses non-empty PostgreSQL/Product/Platform
destinations and a mismatched Platform pepper. Do not weaken those checks or use `pg_restore --clean`
against an existing installation.

`SERVER_PRODUCT_DATABASE_SCOPE_CONFLICT` means the database singleton binding or existing rows name
a different Project/Environment. Never delete or edit the binding. Stop the server, preserve the
database and Product-root identities as evidence, and attach the correct empty or coordinated
restore. See [Environment-scoped Product PostgreSQL](../self-hosting/product-postgresql.md).

## Operational Log failures

For a local Product root run `runku doctor`, `runku logs archive-status`, then a bounded
`runku logs --limit 20`. For an attached server use `runku logs archive-status --remote` and
`runku logs --remote --limit 20`. A manifest
gap, changed Parquet digest, changed manifest, or scope/path mismatch fails closed as corruption;
stop retention and preserve every object. In HA, also capture JetStream stream/consumer state,
replica health, pending/redelivery count, oldest age, and the first pending cursor. NATS/S3 outage
must accumulate retryable work, not be “fixed” by deleting the stream or advancing the consumer.

If live logs stop after a session or grant change, that is expected revocation enforcement. Login
again only after the grant is intentionally restored. `--remote --follow` is one streaming HTTP
connection, so repeated 250 ms requests indicate a proxy/client integration error rather than the
Runku CLI behavior. Follow the symptom/action table in
[Operational Log storage](../operations/operational-logs.md#failure-response).

## Corruption and recovery

Stop all writers. Preserve state and checksums. Run `doctor` read-only. Verify a complete backup and
follow [Backup and recovery](../operations/backup-and-recovery.md). Partial file replacement,
deleting `.runku/`, manual SQL, or reinitialization destroys evidence and may create cross-store
inconsistency.

## Escalation package

Include sanitized command/error output, stable IDs, versions/commit, topology, exact reproduction,
expected/actual behavior, safe retry attempts, doctor summary, relevant redacted log lines, and
whether data or external effects are uncertain. Security-sensitive findings use private reporting.
