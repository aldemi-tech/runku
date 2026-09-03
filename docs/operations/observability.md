# Observability signals

For application files, alert on sustained `FILE_STORAGE_UNAVAILABLE`/`FILE_STORAGE_CORRUPT`, quota
rejection below expected usage, approach to the filesystem free-space floor, S3 authorization or
throttling failures, and incomplete multipart growth reported by the provider. HTTP status,
`x-runku-request-id`, immutable SHA-256/size metadata, and backend audit logs are the correlation
surfaces. Never record transfer tokens, user filenames, object keys, or File IDs as unrestricted
labels. See [Application file storage](../functions/file-storage.md#evidence-and-diagnosis).

This page defines signal ownership and the minimum dashboards/alerts. The storage, streaming,
retention, HA, recovery, and exact configuration runbook is
[Operational Log storage and administration](operational-logs.md).

Runku keeps diagnostic logs separate from security audit events and durable usage accounting.
Best-effort telemetry must never become authoritative billing or scheduling state.

Platform Identity writes operator/invitation/session security audit events in the same PostgreSQL
transaction as each successful state change. Its repository also maintains process-local aggregate
counters for bootstrap, invitation, authentication, refresh, revocation, and retryable failures.
The current source Management API does not yet expose those counters as a metrics endpoint or offer
an audit query endpoint; operators must not treat ordinary logs as a substitute for the durable
audit table.

## Correlation

Requests and nested cross-runtime calls carry request, invocation, Project, Environment, Release or
Dev Revision, Function, and runtime identifiers. Payloads, bearer tokens, Application Keys, and
secret values are excluded from logs and spans.

## Runtime measurements

Safe V8 records invocation duration, queue time, deadline outcome, V8-attributed memory, and Linux
thread CPU where available. Full Node additionally records process or microVM CPU, RSS, queue age,
slot use, replacement, and artifact-cache behavior.

Gateway, PostgreSQL, S3, NATS, Agent, scheduler, outbox, and realtime paths expose bounded labels.
Environment and Function dimensions require explicit cardinality budgets.

## Storage and OTLP

Operational logs are stored natively in a hot SQLite tier and immutable Parquet history. DuckDB is
an embedded historical query engine, not a service. Development and standalone archive inside the
Product root by default. HA inserts a replicated NATS JetStream journal before an S3-compatible
Parquet archive and runs the archive loop as `runku-server logs-worker` from the same image.

Operational logs can additionally be exported over OTLP. Collector failure does not block the
application hot path and OTLP is not the HA durability journal. Operators must configure collector
buffering, retention, sampling, and redaction independently.

The current source exports logs and performance aggregates. A production distribution must publish
its complete metrics, traces, dashboards, alerts, and runbooks together with the supported profile.

## Operational log workflow

Use `runku logs --limit 100`, continue with exclusive `--after logc_N`, and correlate through exact
`--request`, `--invocation`, `--client`, `--credential`, or `--release` filters. Consumers persist
the last confirmed cursor. Function logs are bounded best-effort and never control Mutation success;
persistence failures/drops are separate signals.

For an attached server Product Environment, add `--remote`. Snapshot reads require `logs:read` and
follow requires `logs:follow` at the exact Project/Environment. `--follow` is one NDJSON streaming
HTTP response, not repeated client requests; the server reauthenticates the session during the
stream and terminates it after revocation or grant removal. Product Operational Logs remain in the
Product log repository. Platform Identity PostgreSQL contains operator/session/grant/audit state,
not the Product log payload stream. Raw log payloads do not belong in Platform Identity PostgreSQL.
Use the filesystem/S3 archive for history and OTLP only when another telemetry copy is required.

## Required signal catalog

| Domain | Minimum signals |
|---|---|
| API/identity | admission, latency, status/error, in-flight, bounded auth reason, JWKS age |
| Routing/runtime | serving revision, resolution/artifact failure, queue/admission, deadline, workers |
| Data/workers | transaction/conflict/replay, pool wait, outbox/schedule/Cron lag and leases |
| Realtime | connections, subscriptions, delivery/resync/reconnect, dispatcher lag |
| Full Node | queue age, slots, startup, cancellation, replacement, CPU/RSS/disk/cache |
| Dependencies | PostgreSQL/S3/queue/registry/KMS/OTLP availability and latency |
| Management | authenticated operation, revision propagation, failure, audit outcome |
| Platform Identity | bootstrap state, invitation consume/replay, login/refresh, session revocation, OIDC/JWKS health |

Never emit arguments/results, document contents, JWTs, keys, DSNs, secret headers, source, or
artifacts. User-controlled values are not metric labels; cardinality budgets apply to Project,
Environment, Release, and Function dimensions.

Alerts cover user impact/correctness budgets and link to runbooks: no ready API, sustained errors or
latency, storage/pool failure, outbox/schedule/queue age, Realtime resync spike, artifact integrity,
no Full Node capacity/replacement failure, telemetry loss, and backup verification. Thresholds come
from measured workloads, not local benchmark numbers.

Prune logs in bounded dry-run/apply batches only after `runku logs archive-status` (local) or
`runku logs archive-status --remote` (attached server) confirms coverage.
OTLP checkpoints are durable and retry may repeat unacknowledged events; collectors deduplicate by
event identity. Collector failure never blocks serving. Restore may move cursors backward,
requiring identity-and-content reconciliation.
