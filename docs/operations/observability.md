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
