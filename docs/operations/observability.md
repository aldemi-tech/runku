# Operational signals

Runku keeps diagnostic logs separate from security audit events and durable usage accounting.
Best-effort telemetry must never become authoritative billing or scheduling state.

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

## OTLP

Operational logs can be exported over OTLP. Collector failure does not block the application hot
path. Operators must configure buffering, retention, sampling, and redaction for their topology.

The current source exports logs and performance aggregates. A production distribution must publish
its complete metrics, traces, dashboards, alerts, and runbooks together with the supported profile.

## Operational log workflow

Use `runku logs --limit 100`, continue with exclusive `--after logc_N`, and correlate through exact
`--request`, `--invocation`, `--client`, `--credential`, or `--release` filters. Consumers persist
the last confirmed cursor. Function logs are bounded best-effort and never control Mutation success;
persistence failures/drops are separate signals.

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

Never emit arguments/results, document contents, JWTs, keys, DSNs, secret headers, source, or
artifacts. User-controlled values are not metric labels; cardinality budgets apply to Project,
Environment, Release, and Function dimensions.

Alerts cover user impact/correctness budgets and link to runbooks: no ready API, sustained errors or
latency, storage/pool failure, outbox/schedule/queue age, Realtime resync spike, artifact integrity,
no Full Node capacity/replacement failure, telemetry loss, and backup verification. Thresholds come
from measured workloads, not local benchmark numbers.

Prune logs in bounded dry-run/apply batches. OTLP checkpoints are durable and retry may repeat
unacknowledged events; collectors deduplicate by event identity. Collector failure never blocks
serving. Restore may move cursors backward, requiring identity-based reconciliation.
