# Operational Log storage and administration

This is the authoritative guide for storing, reading, streaming, retaining, backing up, and
recovering Runku Operational Logs. Start with the standalone profile unless the availability
requirement actually justifies a replicated journal and object storage. DuckDB is embedded in the
Runku process as a query library; it is not another daemon to install or operate.

Operational Logs are diagnostic records. Security audit events and authoritative usage accounting
are separate durable contracts. Never calculate an invoice from a best-effort Function log line.

## Choose the smallest correct profile

| Requirement | Profile | Long-lived processes | Durable log storage |
|---|---|---|---|
| Developer workstation | Development | one `runku dev` process | local SQLite + local Parquet |
| Small dedicated installation | Standalone | one `runku-server serve` process, plus PostgreSQL | persistent Product volume + local Parquet |
| Standalone with off-host history | Standalone S3 | one `runku-server serve` process, plus PostgreSQL | local SQLite + S3-compatible Parquet |
| Zone/node failure after journal admission | HA | serving processes + `runku-server logs-worker` replicas + NATS JetStream | local SQLite + replicated journal + S3-compatible Parquet |

Do not deploy a separate observability service for a small installation. The server already embeds
capture, archival, DuckDB query, retention checks, and live streaming. HA separates the archive
loop only because a local process and disk cannot survive arbitrary node loss.

## Data model and guarantees

Every record has an immutable event ID, an exact Project/Environment scope, and a monotonically
increasing repository cursor (`logc_N`). Queries always require the exact scope. The persisted
archive path repeats that boundary:

```text
<archive>/v1/projects/<project-id>/environments/<environment-id>/segments/
  00000000000000000001-00000000000000001000-<digest>.parquet
  00000000000000000001-00000000000000001000-<digest>.parquet.manifest.json
```

The Parquet object is immutable. Its strict JSON manifest is the commit marker and records the
cursor range, row count, timestamp range, byte size, and SHA-256 digest. Readers reject unknown
fields, malformed IDs, gaps, overlaps, path/scope mismatches, changed objects, and changed
manifests. A manifest is visible only after its Parquet object is written.

The capture boundary is intentionally best effort: Function completion must not be rolled back
because diagnostics are full or unavailable. Once an event has been admitted to the local SQLite
hot store, the following guarantees apply:

- development/standalone survives process restart when the Product directory is persistent;
- standalone does not survive loss of that disk unless it is backed up or uses an off-host archive;
- HA publishes admitted records to JetStream in cursor order and waits for the JetStream PubAck;
- restart resumes at the verified archive frontier and may safely replay only the unarchived tail;
- archive workers ACK only after the immutable manifest is committed and readable;
- a worker crash before ACK causes safe redelivery; an identical replay is verified, not rewritten;
- retention can remove a hot row only when a contiguous committed manifest covers its cursor.

Bytes lost before local admission, a destroyed unbacked standalone disk, and records exceeding a
configured bounded queue are outside those guarantees. Alert on spool drops, repository failures,
journal admission failures, archive lag, and journal capacity. Authoritative billing needs a
transactional idempotent usage event, not this path.

## Development and standalone flow

```text
Function/runtime
      │ nonblocking bounded admission
      ▼
SQLite hot store ─────► live/snapshot API ─────► `runku logs [--follow]`
      │
      │ embedded bounded archive loop
      ▼
Parquet + manifest on filesystem or S3
      │
      └──── embedded DuckDB query ─────────────► historical page
```

The query boundary merges archive records followed by newer hot rows and returns one cursor-ordered
page. DuckDB runs with extension autoinstall/autoload disabled, one thread, and a bounded memory
limit. Local and standalone filesystem history lives below
`.runku/observability-archive/`; the hot database is `.runku/observability.sqlite3`.

### Start and inspect local development

```sh
runku dev
runku doctor
runku logs --limit 100
runku logs archive-status
runku logs --after logc_100 --limit 100
runku logs --follow
```

`archive-status` validates manifest continuity and reports `through`, record count, segment count,
and Parquet bytes. `doctor` validates both the hot database and archive directory. A non-zero result
or `LOCAL_LOG_CORRUPT` is an incident; do not delete the offending object or database to make the
check green.

### Query and correlate

Use the narrowest available filter and persist the last successfully processed cursor:

```sh
runku logs --stream function --level warn --limit 200
runku logs --request req_01... --limit 200
runku logs --invocation inv_01... --limit 200
runku logs --release rel_01... --limit 200
runku logs --after logc_842 --limit 200
```

For an attached server Environment use the same Product-scoped session as the lifecycle commands:

```sh
runku login --url https://management.example.com
runku logs --remote --release rel_01... --limit 200
runku logs archive-status --remote
runku logs --remote --follow
```

Snapshot reads require `logs:read`; follow requires `logs:follow` for the exact Project and
Environment. Follow is one NDJSON streaming HTTP response, not polling every 250 ms. The server
reauthorizes during the stream and closes it after session revocation, expiry, or grant removal.
No operator can select a different scope by modifying only query parameters.

### Standalone filesystem configuration

Filesystem archive is the default and needs no archive variables:

```sh
export RUNKU_PRODUCT_ROOT=/srv/runku/product
export RUNKU_LOG_ARCHIVE_BACKEND=filesystem
runku-server check
runku-server migrate
runku-server serve
```

Mount `/srv/runku/product` on persistent storage and back it up as one Product unit. The embedded
archive directory is created inside that root. Do not use a container writable layer, Kubernetes
`emptyDir`, or a host temporary directory.

### Standalone S3-compatible history

Use this when one server is sufficient but historical logs must survive Product-host disk loss:

```sh
export RUNKU_LOG_ARCHIVE_BACKEND=s3
export RUNKU_LOG_ARCHIVE_S3_BUCKET=runku-operational-logs
export RUNKU_LOG_ARCHIVE_S3_REGION=us-east-1
export RUNKU_LOG_ARCHIVE_S3_PREFIX=installation-a
# Optional for a compatible service:
export RUNKU_LOG_ARCHIVE_S3_ENDPOINT=https://objects.example.com
export RUNKU_LOG_ARCHIVE_S3_VIRTUAL_HOSTED_STYLE=false
runku-server check
runku-server serve
```

Credentials use the object-store environment/workload/instance-role chain. Prefer a workload role
limited to `GetObject`, `ListBucket`, and create-only writes under the configured prefix. Do not put
access keys in command arguments or committed environment files. Plain HTTP is rejected except an
explicit loopback-only test endpoint with `RUNKU_LOG_ARCHIVE_S3_ALLOW_HTTP=true`.

This profile still keeps an unreplicated local hot database. A sudden host loss may omit the newest
rows that were not archived yet. Select HA when that recovery point is unacceptable.

## HA flow

```text
Runku serving process (one active writer per Environment)
  SQLite hot store
       │ cursor order + stable event message ID + PubAck
       ▼
NATS JetStream stream RUNKU_LOGS (file storage, WorkQueue, DiscardNew, replicas=3)
       │ durable pull consumer, explicit ACK, bounded batches
       ▼
runku-server logs-worker (same released image/binary; 2+ replicas allowed)
       │ group by exact Environment, validate contiguous cursors
       ▼
S3-compatible Parquet object → immutable manifest → JetStream double ACK
       │
       └──── DuckDB historical query + hot rows ────► Management logs API
```

This is not a second Runku product. `logs-worker` is a role of the same versioned `runku-server`
artifact. Serving processes never ACK the journal on behalf of storage. Archive workers may run
concurrently because the durable pull consumer distributes deliveries and immutable create/verify
writes make uncertain retries safe.

### HA server configuration

Configure every serving process with the same archive namespace and journal contract:

```sh
export RUNKU_LOG_ARCHIVE_BACKEND=s3
export RUNKU_LOG_ARCHIVE_S3_BUCKET=runku-operational-logs
export RUNKU_LOG_ARCHIVE_S3_REGION=us-east-1
export RUNKU_LOG_ARCHIVE_S3_PREFIX=production
export RUNKU_LOG_JOURNAL_URL=tls://nats.example.internal:4222
export RUNKU_LOG_JOURNAL_REPLICAS=3
export RUNKU_LOG_JOURNAL_CREDENTIALS_FILE=/run/secrets/runku-logs.creds
export RUNKU_LOG_ARCHIVE_BATCH_WAIT_SECONDS=30
runku-server check
runku-server serve
```

Remote NATS must use `tls://`; embedded URL credentials, query strings, and fragments are rejected.
The credentials file must be an absolute, non-empty regular file and not a symlink. Give serving
identities publish-only access to `runku.logs.v1.*.*` plus the minimum JetStream API needed to
validate the named stream. Give workers consume/ACK access to the one durable consumer and object
storage access to the archive prefix. Use distinct identities.

The serving role requires its normal database, identity, management, Product-root, and archive
configuration. The worker role intentionally requires only the journal and S3 archive variables:

```sh
runku-server logs-worker
```

Do not set `RUNKU_LOG_JOURNAL_REPLICAS=1` in a profile advertised as HA. Place a three-replica
JetStream stream across failure domains with persistent volumes. `DiscardNew` is deliberate: a full
journal rejects new admission visibly instead of deleting unarchived records. The archive worker
waits up to 30 seconds by default to consolidate sparse traffic, then commits at most 256 records
per delivery batch. Configure only `1..=60` seconds. This changes historical archive freshness and
object count, not the live log stream.

### Ordering and scaling rule

There must be one active local cursor writer for a given Product Environment. Multiple independent
SQLite roots cannot safely invent cursors for the same Environment. Scale different Environments
across cells/servers, or use a qualified authoritative sequencer before active-active serving of
one Environment. Archive workers may scale horizontally; serving writers for one Environment may
not be duplicated merely to obtain HA.

### HA capacity and alerts

Monitor at least:

- local spool accepted/dropped and writer failures;
- highest hot cursor, highest PubAcked cursor, and their delta;
- JetStream messages/bytes versus configured limits, oldest message age, replica health, and
  storage errors;
- archive consumer pending/redelivery/ACK latency;
- per-Environment archive frontier and age since last committed manifest;
- object-store PUT/GET/LIST latency and errors;
- Parquet segment count/size and DuckDB query latency/memory failures;
- live-stream active connections, authorization closures, and slow-consumer termination.

Page before the journal reaches 70% of either byte or message capacity or when archive lag exceeds
the installation recovery objective. A full journal is not solved by increasing retention blindly:
first restore workers/object storage, confirm the frontier advances, then resize with recorded
capacity evidence.

## Retention

Always dry-run first. Applying requires the exact Environment ID to make a copied command fail
closed. For standalone administration on the Product host:

```sh
runku logs archive-status
runku logs prune --before-micros 1735689600000000 --maximum 1000
runku logs prune --before-micros 1735689600000000 --maximum 1000 \
  --apply --environment env_01...
```

For an attached S3 or HA server, use the authenticated Management path for both checks:

```sh
runku logs archive-status --remote
runku logs prune --remote --before-micros 1735689600000000 --maximum 1000
runku logs prune --remote --before-micros 1735689600000000 --maximum 1000 \
  --apply --environment env_01...
```

Repeat bounded apply calls while `more` is true. The implementation intersects the time cutoff with
the committed archive cursor. Rows newer than the archive frontier remain hot even when their
timestamps match the cutoff. Archive-object expiration is a separate operator policy: never delete
or lifecycle-expire an object or manifest inside the supported history window, and delete a
manifest only as part of a versioned archive-retirement procedure.

## Backup, restore, and disaster recovery

For filesystem standalone, quiesce the Product process or use a storage snapshot that preserves a
consistent `.runku/` tree. Back up both `observability.sqlite3` and
`observability-archive/`; keeping only one tier can create gaps. Restore the entire Product root to
an empty destination, run `runku doctor`, then `runku logs archive-status` and page across the
archive/hot boundary.

For S3/HA, protect the archive bucket with versioning or equivalent immutability, encryption, and a
separate retention policy. Back up the Product hot state and NATS stream according to their own
failure domains. Restore order is object archive, journal, Product state, worker, then serving
admission. Before reopening traffic verify:

1. manifests are contiguous and their objects match size/digest;
2. the first pending journal cursor is at or after the archive frontier;
3. workers drain redeliveries without creating conflicting objects;
4. tiered queries return ordered, non-duplicated records across the boundary;
5. an unauthorized session still receives no snapshot or stream data.

A restore can move a local cursor backward. Stable event IDs and content verification handle
identical replay; a different event for an already committed cursor is corruption and stops that
scope. Do not skip the cursor or hand-edit manifests.

## Failure response

| Symptom | Serving impact | Operator action |
|---|---|---|
| local spool full | Function result continues; diagnostic event may drop | inspect CPU/disk and writer failure counters; restore capacity |
| SQLite unavailable/corrupt | new diagnostic persistence degrades | stop retention, snapshot evidence, restore Product state |
| NATS unavailable/full | hot SQLite remains source for retry | restore quorum/capacity; do not prune hot rows |
| worker crash | pending messages redeliver | restart same-version worker; inspect redelivery and frontier |
| S3 PUT/LIST/GET failure | ACK stops; journal accumulates | restore object path/TLS/credentials; verify frontier before drain |
| manifest gap/conflict | historical reads fail closed for scope | freeze deletion, preserve objects, restore known-good archive |
| Parquet digest mismatch | historical reads fail closed | treat as integrity incident; recover the immutable version |
| DuckDB memory/query failure | historical query fails; serving can continue | narrow query, inspect segment sizing, restore configured resources |
| session/grant revoked | live stream closes | reauthenticate only after authorization is intentionally restored |

Never “repair” an incident by deleting the journal stream, advancing a consumer, changing a cursor,
or rewriting a manifest. Capture configuration, stream/consumer state, archive listing, exact Runku
version, `doctor`, and `archive-status` output before recovery.

## Upgrade and validation

Keep serving processes, workers, CLI, and archive format support on a compatible released version.
For an HA upgrade: validate configuration, upgrade one worker, prove frontier progress and replay,
upgrade remaining workers, then upgrade serving cells. Roll back only within the published
compatibility window; immutable v1 Parquet/manifests remain the recovery boundary.

Repository maintainers can run the fast compile gate without starting services:

```sh
make ci-check
```

The explicit acceptance campaign starts disposable NATS JetStream and MinIO, proves PubAck,
source replay/deduplication, batched archive commit, ACK, and tiered query, then removes only its test
volumes:

```sh
make operational-logs-ha-check
```

Filesystem/DuckDB tests also cover archive replay, safe cursor-bounded retention, cross-scope
isolation, changed manifests, and changed Parquet bytes. The Docker campaign is intentionally not a
regular hosted CI job.
