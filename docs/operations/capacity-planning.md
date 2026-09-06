# Capacity planning

Runku publishes hard validation limits and measured component evidence, not universal throughput or
latency promises. Size the actual Self-Hosted profile with application-shaped workloads and keep
headroom for failure, backup, compaction, mixed Releases, and restore.

## Start with the supported topology

The compact profile has one active Product writer and one host failure domain. More CPU or external
PostgreSQL/S3 can move a bottleneck; it does not create active-active Product availability. If the
required availability needs rolling upgrades, automatic failover, multiple Product Environments,
or independently scaled roles, treat that as an unmet readiness criterion rather than extrapolating
the compact package.

## Workload inventory

Measure separately:

- Query/Mutation/Action rate, concurrency, duration, payload/result size, and target Release mix;
- document size, index fan-out, scan limits, conflict rate, PostgreSQL pool wait, and growth;
- Realtime connections, subscriptions per connection, dependency breadth, mutation fan-out,
  reconnect/resync rate, and dispatcher lag;
- schedules/Cron rate, overdue age, lease/retry rate, and handler duration;
- file upload/download concurrency, sizes, ranges, grants, committed bytes, free-space floor, and
  pending usage events;
- operational-log admission bytes, hot retention, archive throughput, query windows, and prune lag;
- build/artifact count and cache hit/miss/integrity failure;
- Management bursts for deploy, data console, storage console, and log streaming.

Segment by bounded Product dimensions only. Never use document IDs, object keys, user-supplied
names, arguments, or secret values as metric labels.

## Published hard/default limits

| Boundary | Current compact value |
|---|---:|
| Public Query/Mutation/Action JSON envelope | 2 MiB |
| HTTP header aggregate | 16 KiB and 64 values |
| Application Key | 256 bytes |
| Bearer token | 16 KiB |
| Management concurrent requests | 1024 in `runku-server` composition |
| Environment application file capacity | 10 GiB default |
| One file | 256 MiB default |
| Bytes copied into one Action | 2 MiB default |
| Concurrent uploads/downloads | 16 / 64 default |
| Live upload grants | 4096 default |
| File metadata rows | 100000 default |
| Filesystem post-reservation free floor | 512 MiB default |
| Configuration entries | 512 per Environment |
| Variable / secret value | 16 KiB / 64 KiB |

These are validation/admission ceilings, not performance targets. Lower them to contain risk when
the application needs less. Raising tunable limits consumes memory, descriptors, storage, queue
depth, recovery time, and attack budget; load-test the combined change.

## Resource model

### CPU

Budget Safe V8 execution, JSON/canonical-value processing, TLS proxy, data/index work, Realtime
reruns, and background workers. Measure p50/p95/p99 queue and execution time plus CPU saturation.
One slow Action can occupy admission even if its external service is the bottleneck.

### Memory

Include runtime workers, artifact cache, active request/Realtime state, PostgreSQL connections,
download/upload buffers, DuckDB historical queries, and container/OS overhead. File transfers are
streamed, but concurrency still consumes per-stream state. Keep enough margin for a second Release
during a weighted rollout.

### Disk and I/O

Budget Product SQLite/WAL, artifacts, hot logs, Parquet staging/history, dedicated application
files, PostgreSQL, backup staging, and temporary upgrade/image space. The file free-space floor
protects upload admission only; it is not a general Product/log disk alert. Monitor each filesystem
and forecast exhaustion from growth rate.

### PostgreSQL

Platform Identity is always PostgreSQL in the compact package. Optional Product PostgreSQL moves
only the logical Function store. Size connections, memory, WAL, checkpoint/vacuum behavior, IOPS,
backup throughput, and recovery time. Load-test optimistic conflicts and pool wait, not just simple
read throughput.

### External S3 and NATS

Measure provider request rate/latency/throttling, multipart/incomplete growth, prefix lifecycle,
archive upload throughput, NATS journal bytes/age/replication, and worker lag. External durability
can reduce host data loss but introduces network/provider failure into file/log operation.

## Test matrix

Run at least:

1. steady normal traffic;
2. expected peak and reconnect burst;
3. worst accepted request/file sizes at bounded concurrency;
4. hot-key/index Mutation conflict;
5. Realtime fan-out after relevant commits;
6. schedule/Cron catch-up after downtime;
7. weighted two-Release rollout;
8. log archive/storage slowdown;
9. PostgreSQL/S3/NATS transient failure;
10. backup, restore, and upgrade within the capacity envelope.

For each test record versions, hardware, topology, configuration, workload generator, duration,
warm-up, errors, percentile method, raw output, and recovery behavior. Results are not an SLA.

## Headroom and alerts

Set thresholds from application SLOs and measured knees. Preserve headroom for one dependency
degradation, background catch-up, and maintenance. Alert on user impact/correctness first: no ready
Product, sustained stable errors/latency, admission rejection, pool/storage failure, outbox/schedule
lag, Realtime resync, file quota/free floor, archive frontier stall, or missing telemetry.

An alert must link to the owning runbook and state whether retry is safe. A high queue with no user
impact can be capacity warning; a low-rate corruption or cross-scope failure is a correctness
incident regardless of utilization.

## Scaling decisions

| Signal | Likely first action | Not implied |
|---|---|---|
| Runtime CPU/queue high | optimize Functions/indexes; adjust host limits after load test | horizontal Product writers |
| Data pool/IO high | tune workload/PostgreSQL and move logical store only when justified | migration of all Product state |
| Realtime rerun fan-out high | narrow Query dependencies and client subscription set | pre-commit notifications |
| File disk growth high | tighten quotas/lifecycle or plan external S3 with recovery | metadata-only backup is complete |
| Log hot tier growth high | restore archive throughput then bounded prune | prune past unverified frontier |
| Management deploy burst high | serialize automation and preserve CAS/idempotency | broad permanent operator grants |

Revisit the [production-readiness contract](../self-hosting/production-readiness.md) when a scaling
need crosses the published compact boundary.
