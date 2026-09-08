# `runku-server` configuration reference

This reference describes the current compact server binary. The released Docker package supplies a
reviewed subset with mounts and defaults; prefer that package for supported Self-Hosted operation.
Unknown or inconsistent configuration fails before listeners accept traffic.

## Commands

`runku-server` accepts exactly one optional command:

| Command | Reads dependencies | Changes durable state | Success |
|---|---|---|---|
| `serve` (default) | all configured dependencies | initializes bootstrap if absent; opens/migrates Product authorities | runs until shutdown |
| `check` | parses configuration; constructs file backend and auth configuration | no intended Product/identity mutation | prints `configuration valid` |
| `migrate` | Platform Identity PostgreSQL; optional Product root + Function PostgreSQL | applies idempotent migrations | prints `migrations applied` |
| `recover-bootstrap` | Platform Identity PostgreSQL and Platform state directory | replaces lost pending first-owner material under explicit confirmation | writes new protected invitation |
| `logs-worker` | NATS journal and S3 archive | consumes/archives operational-log batches | long-running worker |
| `probe-live` | loopback Management listener | none | exit `0` only for HTTP 204 `/health/live` |
| `probe-ready` | loopback Management listener | none | exit `0` only for HTTP 204 `/health/ready` |
| `version` | none | none | prints exact binary version |

Unknown commands/arguments return `SERVER_USAGE_INVALID`. The binary emits stable error codes to
stderr and a non-zero exit; configuration errors do not echo secret values.

## Secret input convention

For inputs read as secrets, set either `NAME=value` or `NAME_FILE=/absolute/path`, never both. A
secret file must be absolute, regular, non-symlinked, non-empty, at most 64 KiB, and contain valid
UTF-8. One trailing LF or CRLF is removed. Empty, surrounding-whitespace, or control-character
values fail closed.

Mounted files are preferred. The path is passed in the environment; the secret value is not.
Protect file creation, ownership, backup, rotation, and deletion independently from Product state.

## Required core settings

| Variable | Required/default | Contract |
|---|---|---|
| `RUNKU_IDENTITY_DATABASE_URL[_FILE]` | required | PostgreSQL URL for Platform Identity; legacy `RUNKU_DATABASE_URL` is accepted but cannot coexist |
| `RUNKU_PLATFORM_IDENTITY_PEPPER[_FILE]` | required | exactly 32 bytes encoded base64url without padding; loss invalidates protected identity material |
| `RUNKU_STATE_DIRECTORY` | required | absolute non-root Platform state directory; contains bootstrap recovery state |
| `RUNKU_MANAGEMENT_LISTEN` | `127.0.0.1:3220` | Management bind address |
| `RUNKU_MANAGEMENT_TLS_TERMINATED` | `false` | `true` asserts an external trusted TLS boundary; non-loopback plaintext is rejected |
| `RUNKU_PUBLIC_MANAGEMENT_URL` | optional | canonical HTTPS origin, or allowed literal-loopback HTTP origin, advertised to clients |
| `RUNKU_PRODUCT_ROOT` | optional | absolute non-root initialized Product directory; without it, only Platform Identity/Management runs |
| `RUNKU_CELL_CONFIG` | optional | absolute strict multi-Environment cell manifest; mutually exclusive with `RUNKU_PRODUCT_ROOT` |

The public Management URL cannot contain credentials, query, fragment, or an unrelated path. CLI
discovery treats Authentication and Management origins as trust configuration and does not follow
redirects.

The compact package sets a Product root and initializes its listener as `127.0.0.1:3210`. Product
HTTP starts lazily after an eligible Channel exists; the Management probe remains the container
readiness signal.

For a shared or dedicated cell member, `RUNKU_CELL_CONFIG` moves each root, host set, optional
Product database secret, origins, and auth config into one versioned manifest. A cell requires
`RUNKU_APPLICATION_LISTEN` plus `RUNKU_APPLICATION_TLS_TERMINATED=true`. See the
[Multi-Environment cell profile](cell-profile.md) for the schema, routing, isolation, capacity, and
single-active-writer constraints.

## Optional Full Node profiles

Full Node is disabled by default. The released server image includes Node 22 for both Linux ARM64
and x86_64. Safe V8 remains enabled in every profile; these settings only attach an executor for
Functions declared as Full Node Actions. `safe`, `node`, and `hybrid` describe Release artifact
composition, while OCI describes an artifact/package format. They are not Environment modes.

| Variable | Default | Contract |
|---|---:|---|
| `RUNKU_FULL_NODE_PROFILE` | `disabled` | `disabled`, `dedicated-host`, or `dedicated-worker`; enabled profiles are rejected for a `shared` cell manifest |
| `RUNKU_FULL_NODE_BINARY` | `/usr/local/bin/node` | absolute Node 20+ executable path |
| `RUNKU_FULL_NODE_MAX_CONCURRENCY` | `1` | bounded worker/admission slots, 1–128 |
| `RUNKU_FULL_NODE_MAX_CONCURRENT_PER_PROJECT` | same as concurrency | worker-only fairness ceiling, 1 through the total slot count |
| `RUNKU_FULL_NODE_HEAP_MEGABYTES` | `256` | per-worker V8 heap, 64–4096 MiB and below declared instance memory |
| `RUNKU_FULL_NODE_INSTANCE_CPU_MILLIS` | required | externally enforced whole-instance CPU declaration |
| `RUNKU_FULL_NODE_INSTANCE_MEMORY_BYTES` | required | externally enforced whole-instance memory declaration |
| `RUNKU_FULL_NODE_INSTANCE_PIDS` | required | externally enforced whole-instance PID declaration |

`dedicated-host` is the compact compatibility profile for a complete server instance belonging to
one Product trust domain. The runtime stores verified read-only artifacts and ephemeral mailboxes below
`PRODUCT_ROOT/.runku/server-node-runtime-v1`. Workers are separate child processes inside the same
container and communicate through the bounded mailbox protocol; no gRPC hop is involved. Healthy
workers are reused and are destroyed after errors, timeout/cancellation, or the reuse ceiling.

`dedicated-worker` moves those Node processes into an independent `runku-server full-node-worker`
container. The cell keeps Safe V8, verifies a Node-capable Release, writes a sanitized immutable
manifest/artifact projection before moving Workspace HEAD, and queues only scoped IDs, deadlines,
and arguments. The worker mounts that projection read-only plus its own writable cache/scratch; it
does not mount Product databases, application keys, Platform Identity state, or the cell's secret
files. Gateway and worker communicate through NATS JetStream rather than gRPC. Because this compact
composition does not include a remote configuration broker, it rejects a Release when any Full
Node Action declares `variable:NAME` or `secret:NAME`; Safe V8 configuration remains available.

| Separate-worker variable | Default | Contract |
|---|---:|---|
| `RUNKU_FULL_NODE_RESOURCE_ROOT` | required | absolute projection root; writable in the cell and read-only in the worker |
| `RUNKU_FULL_NODE_RUNTIME_ROOT` | worker required | absolute worker-private cache/scratch root; must not contain or be contained by the projection |
| `RUNKU_EXECUTION_NATS_URL` | required | `tls://host:port`, or `nats://` only for literal loopback/local composition |
| `RUNKU_EXECUTION_NATS_CREDENTIALS_FILE` | none | optional absolute, non-symlink NATS credentials file |
| `RUNKU_EXECUTION_NATS_REPLICAS` | `1` | exact JetStream stream/KV replica count, 1–5; production external clusters normally use 3 |
| `RUNKU_EXECUTION_NATS_STREAM` | `RUNKU_EXECUTIONS` | exact uppercase/digit/underscore stream namespace |
| `RUNKU_EXECUTION_NATS_SUBJECT_PREFIX` | `runku.execution.v1` | exact lower-case subject namespace |
| `RUNKU_EXECUTION_NATS_CONTROL_BUCKET` | `RUNKU_EXECUTION_STATE` | exact uppercase/digit/underscore KV namespace |
| `RUNKU_FULL_NODE_EXECUTION_CLASS` | `node_host_v1` | exact compatible worker ABI/pool token |

The packaged `full-node-worker` overlay uses one loopback-only NATS instance and is intended for a
single dedicated host. Provider deployments use a private TLS NATS endpoint and distinct
publisher/worker credentials. Queue state is bounded and transient; immutable execution resources
are part of the coordinated platform backup. A stopped worker leaves Safe V8 available, while Node
invocations fail or time out without silently falling back to Safe.

Direct Development publication accepts canonical Node/hybrid ESM bundles. Those bundles carry
compiled application sources and contracts but no `node_modules`; use Node built-ins or code
already bundled into the source graph. Actions with unresolved external npm imports require the
existing package-lock-bound OCI publication path and cannot be made available by installing
packages in the running server container.

Neither profile turns Docker into a hostile multi-tenant sandbox. The separate container materially
reduces accidental access to cell state and gives Node its own cgroup, PID, filesystem, and restart
boundary, but a shared kernel is not a VM-grade isolation boundary. Shared mutually untrusted Node
still requires the Firecracker-oriented executor profile and its separate qualification gate.

## Optional Product logical PostgreSQL

| Variable | Contract |
|---|---|
| `RUNKU_PLATFORM_DATABASE_URL[_FILE]` | separate PostgreSQL database for the attached Environment's documents, indexes, outbox, and schedules |
| `RUNKU_PRODUCT_DATABASE_URL[_FILE]` | legacy alias; cannot coexist with the canonical name |

This URL requires `RUNKU_PRODUCT_ROOT`, must name a different database from Platform Identity, and
is bound atomically to the root's exact Project/Environment scope. It does not move Releases,
Workspaces, Channels, application identity, serving, configuration, Cron, artifacts, object
metadata, or operational logs out of the Product root. See [Product PostgreSQL](product-postgresql.md).

## Product browser and functional identity

| Variable | Default | Contract |
|---|---|---|
| `RUNKU_PRODUCT_ALLOWED_ORIGINS` | none | exact comma-separated browser origins; duplicates, malformed values, or an excessive set fail |
| `RUNKU_PRODUCT_AUTH_CONFIG` | none | Product-root-relative JWT provider JSON; absolute/empty/parent-traversing paths fail |

These settings require a Product root. Origin authorization and JWT principal verification are
independent from Application Keys. Requests without Origin remain eligible server-to-server calls;
they still require all application and function authorization.

## Platform OIDC and managed enrollment

| Variable | Default | Contract |
|---|---|---|
| `RUNKU_PLATFORM_OIDC_CONFIG` | none | path to strict Platform operator OIDC JSON |
| `RUNKU_PLATFORM_MANAGED_ENROLLMENT_TOKEN[_FILE]` | none | shared secret authenticating the configured managed identity gateway |
| `RUNKU_PLATFORM_MANAGED_SOURCE_AUTHORITY` | none | exact authority name for monotonic managed grant reconciliation |

Managed token and source authority must appear together. Platform OIDC is independent from the
Product JWT provider. Use the schema, issuer/JWKS/PKCE/resource constraints, and rotation procedure
in [Platform operator identity](../auth/platform-identity.md); do not infer accepted keys.

## Application Files and Object Storage byte backend

These settings select the shared physical byte boundary for both Action-oriented Application Files
and Runku Object Storage, whose Product route supports a bounded S3-compatible protocol. The two
storage capabilities use disjoint generated namespaces. For
backend choice, capacity math, provider permissions, canaries, backup, restore, migration, and
incident response, use [Storage configuration and limits](storage-configuration.md).

| Variable | Default | Meaning |
|---|---:|---|
| `RUNKU_FILE_STORAGE_BACKEND` | `filesystem` | `filesystem` or `s3` |
| `RUNKU_FILE_STORAGE_FILESYSTEM_ROOT` | Product-owned default | optional absolute non-root dedicated byte directory |
| `RUNKU_FILE_STORAGE_ENVIRONMENT_BYTES` | 10 GiB | committed + reserved Environment byte ceiling |
| `RUNKU_FILE_STORAGE_FILE_BYTES` | 256 MiB | one-file ceiling |
| `RUNKU_FILE_STORAGE_ACTION_BYTES` | 2 MiB | bytes copied into an Action |
| `RUNKU_FILE_STORAGE_CONCURRENT_UPLOADS` | 16 | active streamed uploads |
| `RUNKU_FILE_STORAGE_CONCURRENT_DOWNLOADS` | 64 | active response streams |
| `RUNKU_FILE_STORAGE_MAXIMUM_LIVE_UPLOAD_GRANTS` | 4096 | unexpired grants retained for replay/admission |
| `RUNKU_FILE_STORAGE_MAXIMUM_FILES` | 100000 | ready/deleting metadata rows |
| `RUNKU_FILE_STORAGE_MAXIMUM_PENDING_USAGE_EVENTS` | 1000000 | durable usage events awaiting acknowledgement |
| `RUNKU_FILE_STORAGE_FILESYSTEM_MINIMUM_FREE_BYTES` | 512 MiB | post-reservation disk floor |
| `RUNKU_FILE_STORAGE_UPLOAD_GRANT_TTL_SECONDS` | 900 | upload-grant lifetime |
| `RUNKU_FILE_STORAGE_DOWNLOAD_GRANT_MAX_TTL_SECONDS` | 900 | maximum requested download-grant lifetime |

Limits must be positive and internally ordered: Action ≤ file ≤ Environment. Concurrency is
`1..=10000`; file/grant/event counts have validated ceilings; TTLs are one second through 24 hours.
Changing a limit affects new admission, not the identity or digest of already committed files.

Runku Object Storage bucket quotas are administered per bucket rather than through additional
server environment variables. The Product S3 route admits at most 64 MiB for one PUT or UploadPart,
4 MiB for the completion XML, 10,000 ordered parts, and 5 TiB for the composed object before the
narrower bucket quotas apply. The authenticated Management/console PUT remains 64 MiB. Large
objects therefore use the Product S3 multipart protocol; reverse proxies must stream responses and
must not impose 64 MiB on the completed GET object.

### External S3-compatible byte backend

| Variable | Required/default | Contract |
|---|---|---|
| `RUNKU_FILE_STORAGE_S3_BUCKET` | required | existing bucket |
| `RUNKU_FILE_STORAGE_S3_REGION` | required | signing/region value |
| `RUNKU_FILE_STORAGE_S3_PREFIX` | empty | installation-unique object prefix |
| `RUNKU_FILE_STORAGE_S3_ENDPOINT` | provider default | optional compatible endpoint |
| `RUNKU_FILE_STORAGE_S3_VIRTUAL_HOSTED_STYLE` | `false` | boolean addressing mode |
| `RUNKU_FILE_STORAGE_S3_ALLOW_LOOPBACK_HTTP` | `false` | development-only HTTP exception for loopback endpoints |
| `RUNKU_FILE_STORAGE_S3_ACCESS_KEY_ID[_FILE]` | provider chain | static credential ID when paired with secret |
| `RUNKU_FILE_STORAGE_S3_SECRET_ACCESS_KEY[_FILE]` | provider chain | static secret when paired with ID |
| `RUNKU_FILE_STORAGE_S3_SESSION_TOKEN[_FILE]` | none | optional only with the complete static pair |

An incomplete static pair fails. Without all static fields the backend uses its environment/provider
credential mechanism. Scope the credential to the exact bucket/prefix and required operations.
An external object-store backend is outside the compact backup helper; coordinate its recovery point before declaring a
backup complete.

## Application-file usage sink

| Variable | Default | Contract |
|---|---|---|
| `RUNKU_FILE_USAGE_SINK_URL` | none | HTTPS endpoint for authoritative file usage events |
| `RUNKU_FILE_USAGE_CELL_ID` | none | bounded installation/cell identity paired with URL |
| `RUNKU_FILE_USAGE_SINK_TOKEN[_FILE]` | none | bearer material paired with URL |
| `RUNKU_FILE_USAGE_SINK_ALLOW_LOOPBACK_HTTP` | `false` | permits a loopback-only test sink |
| `RUNKU_FILE_USAGE_INTERVAL_SECONDS` | `5` | flush interval |

The tuple is all-or-none and requires a Product root. Usage facts are durable until acknowledged;
the sink must deduplicate stable event identities. These facts are distinct from operational logs
and must not be reconstructed from them.

## Operational Log archive

| Variable | Default | Contract |
|---|---|---|
| `RUNKU_LOG_ARCHIVE_BACKEND` | `filesystem` | `filesystem` keeps the embedded Product archive; `s3` opens external immutable history |
| `RUNKU_LOG_ARCHIVE_S3_BUCKET` | required for `s3` | existing archive bucket |
| `RUNKU_LOG_ARCHIVE_S3_REGION` | required for `s3` | signing/region value |
| `RUNKU_LOG_ARCHIVE_S3_PREFIX` | empty | unique archive prefix |
| `RUNKU_LOG_ARCHIVE_S3_ENDPOINT` | provider default | optional compatible endpoint |
| `RUNKU_LOG_ARCHIVE_S3_VIRTUAL_HOSTED_STYLE` | `false` | boolean addressing mode |
| `RUNKU_LOG_ARCHIVE_S3_ALLOW_HTTP` | `false` | explicit test-only HTTP policy; prefer HTTPS |

The archive SDK obtains its provider credentials from the process environment/mounted provider
configuration used by the released overlay. Do not reuse application-byte credentials unless the
combined authority is an explicit security decision.

## Optional replicated log journal

| Variable | Default | Contract |
|---|---|---|
| `RUNKU_LOG_JOURNAL_URL` | none | `tls://host:port`, or `nats://` only for loopback |
| `RUNKU_LOG_JOURNAL_REPLICAS` | `3` | JetStream stream replica count |
| `RUNKU_LOG_JOURNAL_CREDENTIALS_FILE` | none | absolute, regular, non-symlinked NATS credentials file |
| `RUNKU_LOG_ARCHIVE_BATCH_WAIT_SECONDS` | `30` | logs-worker wait in `1..=60` |

A journal requires S3 log archive configuration. `logs-worker` reads the same variables and moves
verified batches into the immutable archive. Journal retention and worker lag must be sized
together; NATS is not Product data authority.

## Validation, rotation, and rollback

Before a change:

1. capture the current non-secret configuration hash and exact image digest;
2. create and verify a coordinated backup when the change touches state or keys;
3. run the released package preflight/`runku-server check` with the proposed mounts;
4. change one boundary at a time and restart within the maintenance procedure;
5. verify Management readiness, Product readiness if promoted, identity, one Query, storage, and
   log archive status.

Changing a Channel is application rollback. Changing an image or database schema is a server
upgrade. Replacing a pepper/key can invalidate encrypted/authentication state. These are separate
procedures and cannot substitute for one another.

For stable error-code diagnosis, use [Troubleshooting](../reference/troubleshooting.md). For exact
packaged variable wiring and profile combinations, use the
[Docker standalone guide](../../deployments/docker/README.md).
