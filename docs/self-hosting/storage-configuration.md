# Storage configuration and limits

This guide is for Self-Hosted operators configuring the physical byte boundary used by:

- Application Files authorized by Actions;
- Runku Object Storage buckets, whose Product data plane supports a bounded S3-compatible protocol.

Application developers use the [Application Files API](../functions/file-storage.md). Bucket/key
administrators use [Object Storage](../concepts/object-storage.md). This page covers only physical
backend choice, server parameters, capacity, permissions, backup, changes, and incidents.

## Current compact architecture

The compact `runku-server` owns metadata under the Product root and writes physical bytes through
one configured backend:

```text
Application Files metadata ─┐
                            ├─ filesystem directory OR external S3-compatible bucket/prefix
Object Storage metadata ────┘
```

The two products share the backend configuration but use disjoint generated namespaces. User file
names and logical object keys are never physical paths:

```text
<prefix>/v1/projects/<project>/environments/<environment>/files/<file-id>
<prefix>/v1/projects/<project>/environments/<environment>/object-storage/<bucket-id>/<sha256>
```

`RUNKU_FILE_STORAGE_*` is the stable configuration prefix for this shared physical boundary. Its
name does not mean Runku Object Storage is disabled or uses a separate provider.

## Choose a filesystem or external object-store backend

| Decision | Filesystem | External S3-compatible backend |
|---|---|---|
| simplest supported compact install | yes | requires pre-existing provider service |
| byte durability | mounted host/storage volume | provider configuration |
| compact backup helper | includes dedicated `files/` tree | refuses a false complete backup |
| scaling/failure domain | one mounted storage boundary | provider-dependent |
| credentials | Unix ownership/mode | bucket/prefix credential or provider chain |
| encryption/replication/versioning | volume/host operator | external storage provider operator |
| migration built into Runku | no | no |

Use filesystem when the compact one-host profile and its coordinated offline backup meet the
accepted recovery objective. Use an external S3-compatible backend only when the organization
already operates its availability, encryption, replication, monitoring, lifecycle, and coordinated
restore.

Selecting an external object-store backend does not turn the current compact server into an HA
control/data plane. It only moves
physical application/object bytes outside the host volume.

## Filesystem profile

The released Docker profile sets:

```text
RUNKU_FILE_STORAGE_BACKEND=filesystem
RUNKU_FILE_STORAGE_FILESYSTEM_ROOT=/var/lib/runku/files
```

and mounts that path from `${RUNKU_DATA_DIRECTORY}/files`.

### Filesystem requirements

- absolute path, never `/`;
- dedicated to this installation;
- on creation Runku applies owner-only directory access;
- an existing Unix directory must already deny group/other access;
- must not be a symlink boundary;
- enough free space for active reservations plus the configured safety floor;
- included in the same recovery point as Product metadata.

Do not mount the Product root itself as the file root and do not point two independent Runku
installations at the same unpartitioned directory.

### Free-space admission

Before reserving an upload, Runku protects:

```text
available filesystem bytes after maximum reservation
  >= RUNKU_FILE_STORAGE_FILESYSTEM_MINIMUM_FREE_BYTES
```

The default floor is 512 MiB. This guard complements the logical Environment quota; it does not
replace disk alerts. Leave additional headroom for retained Object Storage versions, temporary
multipart chunks, filesystem metadata, backup staging, and operating-system needs.

## External S3-compatible backend profile

For the packaged distribution select `s3-files` (or `browser-s3-files`) and configure the overlay:

```text
RUNKU_DEPLOYMENT_PROFILE=s3-files
RUNKU_FILE_STORAGE_S3_BUCKET=runku-application-files
RUNKU_FILE_STORAGE_S3_REGION=us-east-1
RUNKU_FILE_STORAGE_S3_PREFIX=installation-01
RUNKU_FILE_STORAGE_S3_ENDPOINT=https://s3.example.com
RUNKU_FILE_STORAGE_S3_VIRTUAL_HOSTED_STYLE=true
```

The bucket must already exist. Runku does not create, encrypt, version, replicate, monitor, back up,
restore, or delete the provider bucket.

### External backend parameters

| Variable | Required/default | Contract |
|---|---|---|
| `RUNKU_FILE_STORAGE_BACKEND` | required value `s3` | selects external adapter |
| `RUNKU_FILE_STORAGE_S3_BUCKET` | required | existing dedicated bucket |
| `RUNKU_FILE_STORAGE_S3_REGION` | required | provider signing region |
| `RUNKU_FILE_STORAGE_S3_PREFIX` | empty in server; required by packaged overlay | unique installation prefix, at most 256 bytes |
| `RUNKU_FILE_STORAGE_S3_ENDPOINT` | provider default | optional compatible endpoint |
| `RUNKU_FILE_STORAGE_S3_VIRTUAL_HOSTED_STYLE` | `false` in server; overlay defaults `true` | `true`/`false` addressing mode |
| `RUNKU_FILE_STORAGE_S3_ALLOW_LOOPBACK_HTTP` | `false` | only permits literal-loopback HTTP for local conformance |
| `RUNKU_FILE_STORAGE_S3_ACCESS_KEY_ID[_FILE]` | provider chain | explicit ID when paired with secret |
| `RUNKU_FILE_STORAGE_S3_SECRET_ACCESS_KEY[_FILE]` | provider chain | explicit secret when paired with ID |
| `RUNKU_FILE_STORAGE_S3_SESSION_TOKEN[_FILE]` | none | optional only with complete static pair |

The configured prefix is trimmed as a logical namespace and must not be empty for production,
start/end with `/`, contain `//`, `.`/`..` path segments, backslash, or NUL. Allocate a new prefix
per exact installation/Environment placement. Never reuse an Application Files/Object Storage
prefix for Operational Log archives.

An endpoint must use HTTPS. The loopback HTTP switch is deliberately unable to authorize a remote
plaintext endpoint.

### Credential source

Set both static access key and secret, optionally session token, or set none and use the supported
provider environment/identity chain. A partial pair or standalone session token fails startup.

For the Docker overlay, place the two values in separate private files:

```text
${RUNKU_SECRETS_DIRECTORY}/file-s3-access-key-id
${RUNKU_SECRETS_DIRECTORY}/file-s3-secret-access-key
```

The compose overlay mounts them and sets only `_FILE` variables. Do not place secrets in `.env`,
image layers, Compose arguments, logs, or Product configuration.

### Provider permission policy

Grant only the configured bucket/prefix and the object/multipart operations required for upload,
verified read/range read, delete, multipart completion/abort, and bounded prefix inspection. Deny
bucket administration and every other prefix. Provider IAM names vary; validate the actual request
campaign against the chosen service instead of copying an AWS-specific policy blindly.

Also configure:

- server-side encryption and key recovery;
- TLS trust and private network policy;
- replication/erasure policy and failure domains;
- cleanup for abandoned multipart uploads;
- request-rate, latency, error, throttling, capacity, version, and incomplete-upload alerts;
- provider-native backup/restore or replicated recovery point.

## Logical quota and admission parameters

These limits govern Application Files. Object Storage additionally enforces each logical bucket's
own object/count/byte quotas. One Product S3 PUT or UploadPart request is capped at 64 MiB; multipart
completion can compose an object up to the smaller of 5 TiB and the bucket quotas. The
Management/console object PUT remains a separate 64 MiB administrative path.

| Variable | Default | Valid contract |
|---|---:|---|
| `RUNKU_FILE_STORAGE_ENVIRONMENT_BYTES` | 10 GiB | positive total committed + reserved Application File bytes |
| `RUNKU_FILE_STORAGE_FILE_BYTES` | 256 MiB | positive, ≤ Environment bytes |
| `RUNKU_FILE_STORAGE_ACTION_BYTES` | 2 MiB | positive, ≤ file bytes |
| `RUNKU_FILE_STORAGE_CONCURRENT_UPLOADS` | 16 | `1..10000` active HTTP streams |
| `RUNKU_FILE_STORAGE_CONCURRENT_DOWNLOADS` | 64 | `1..10000`, held until response body ends/fails/cancels |
| `RUNKU_FILE_STORAGE_MAXIMUM_LIVE_UPLOAD_GRANTS` | 4,096 | `1..1000000` unexpired grants/replay records |
| `RUNKU_FILE_STORAGE_MAXIMUM_FILES` | 100,000 | `1..10000000` ready/deleting metadata rows |
| `RUNKU_FILE_STORAGE_MAXIMUM_PENDING_USAGE_EVENTS` | 1,000,000 | `1..10000000` unacknowledged authoritative events |
| `RUNKU_FILE_STORAGE_FILESYSTEM_MINIMUM_FREE_BYTES` | 512 MiB | non-negative; filesystem must leave floor + one max file |
| `RUNKU_FILE_STORAGE_UPLOAD_GRANT_TTL_SECONDS` | 900 | `1..86400` |
| `RUNKU_FILE_STORAGE_DOWNLOAD_GRANT_MAX_TTL_SECONDS` | 900 | `1..86400` |

The server rejects zero, malformed, out-of-range, or inverted limits before serving. Limit changes
affect future admission; they do not resize existing objects or rewrite metadata.

### Capacity model

Plan at least these separate quantities:

```text
Application Files logical admission
  = committed file bytes + outstanding upload reservations

physical bytes
  >= Application Files bytes
   + current Object Storage content
   + retained Object Storage versions/content-addressed garbage
   + active multipart/temp bytes
   + provider/filesystem overhead
   + recovery and growth headroom
```

Multipart completion does not allocate the complete object in `runku-server`. One composition runs
at a time per physical adapter. It streams verified parts into staging and then streams staging to
the final content address. Each pass uses at most 10,000 provider parts: the writer chunk is
`max(5 MiB, ceil(object bytes / 10,000))`, and one part upload is in flight at a time. This works
beyond a provider's single-request server-side copy ceiling, but at the 5 TiB ceiling one writer
part is about 524.3 MiB. Allow memory for that part, the accumulating writer buffer and the backend
read chunk. An external S3-compatible adapter may temporarily require source parts, staging, and
final provider objects at once. Size provider capacity and incomplete-upload lifecycle for that
peak.

`RUNKU_FILE_STORAGE_ENVIRONMENT_BYTES` does not cap Runku Object Storage buckets. Conversely,
bucket quotas do not reserve disk for Application Files. Sum both product workloads when sizing the
shared backend.

### Choosing values

1. measure expected average/P95/max file and object size;
2. bound one file/object to the smallest legitimate business maximum;
3. set total quotas below tested physical/provider capacity with incident headroom;
4. derive concurrency from measured end-to-end response lifetime, not only request arrival rate;
5. keep grant TTLs short enough to limit bearer exposure and reservation pressure;
6. ensure pending usage capacity covers the longest accepted sink outage;
7. load-test the selected exact backend, proxy, TLS, and object-size distribution.

A configured ceiling is not an SLO. Record measured throughput/latency and alert before saturation.

## Validate before startup

With the final configuration and mounts:

```sh
runku-server check
```

In the Docker package:

```sh
./runku-selfhost start
./runku-selfhost status
```

`check` validates parsing and opens/constructs the backend boundary, but a complete acceptance test
must prove actual operations:

1. create an Application File upload grant;
2. stream a file larger than the direct Action limit;
3. download it and verify length/SHA-256/range;
4. delete it and verify it is unavailable;
5. create a logical bucket and scoped key;
6. use an official AWS-compatible client to PUT/HEAD/GET/list/copy/delete within the allowed prefix;
7. upload at least two parts whose completed object is larger than 64 MiB, list parts, complete,
   retry the identical completion, range-read across a part boundary, and verify the SHA-256;
8. interrupt a multipart upload and abort it, then confirm registry cleanup and account for
   provider-side incomplete/staging bytes;
9. prove another bucket/prefix and disallowed operation are denied;
10. observe expected metrics/logs without credential or object-key leakage.

Repeat the canary after credential rotation, provider policy changes, binary upgrade, and restore.

## Backup and restore

### Filesystem

The supported compact helper stops/quiesces the server and includes the dedicated `files/`
directory with Product metadata and Platform state:

```sh
./runku-selfhost backup /encrypted/backups/runku-2026-09-05 kms://backup-policy/version-7
./runku-selfhost verify-backup /encrypted/backups/runku-2026-09-05
```

That directory contains both Application File and Runku Object Storage physical namespaces. A
restore installs bytes before readiness checks. Verification confirms archive integrity/presence,
but post-restore canaries must still read/checksum/range/delete representative data for both
products.

### External object-store backend

The compact helper fails closed for an `s3-files` profile; it does not silently produce a complete
metadata-only backup. Establish a provider-native recovery point and coordinate its timestamp or
version frontier with the quiesced Product/Platform snapshot.

Restore order:

1. keep Product traffic closed;
2. restore/verify external bucket and exact prefix;
3. restore Runku Product metadata and Platform state from the matching recovery point;
4. restore required secret files/credential identity through the approved process;
5. run configuration/migration/doctor checks;
6. canary metadata + full/range bytes + SHA-256 + delete for Application Files;
7. canary signed S3 current/version reads and a disposable write/delete;
8. compare logical usage, physical capacity, missing-object, and unreferenced-object evidence;
9. reopen traffic only after the accepted consistency checks pass.

Do not roll an external bucket backward independently from Runku metadata. A metadata reference to
a missing/changed physical version surfaces as not found/corrupt; an extra content-addressed blob is
not automatically permission to delete it.

## Change backend or prefix

Runku does not currently provide an online filesystem-to-object-store or prefix migration command. Changing the
backend/prefix without copying and validating all generated objects makes existing metadata point
at missing bytes.

Treat migration as a planned offline data migration:

1. inventory scope, metadata, object counts/bytes/versions, configuration, and current backup;
2. stop new writes and quiesce Runku;
3. create a verified recovery point;
4. copy the complete physical namespace without interpreting generated keys;
5. verify count/size/checksum and preserve provider versions needed by metadata;
6. run `runku-server check` with the new configuration;
7. start privately and run both product canaries;
8. retain the old backend read-only through the rollback window;
9. document when rollback becomes impossible due to new writes.

There is no supported dual-write phase. If the migration procedure cannot guarantee a coordinated
cutover, remain on the current backend.

## Rotation

For external object-store credentials, use overlap when the provider supports multiple principals:

1. create a new least-privilege credential for the same exact prefix;
2. mount new secret files atomically under new paths/content;
3. restart through the maintenance procedure;
4. run complete read/write/delete canaries;
5. revoke the old provider credential;
6. verify denied use of the old identity and inspect access audit.

Changing provider credentials is not the same as rotating Runku Product Object Storage access keys.
Provider credentials are held only by `runku-server`; Runku Storage Product keys are issued to
application clients and use the supported S3-compatible protocol.

## Operational signals and incidents

Alert on:

- logical Environment/bucket utilization and reservation pressure;
- filesystem free bytes/inodes or provider capacity;
- active upload/download saturation and queue/admission rejection;
- grant count and expiry cleanup;
- pending authoritative usage-event backlog;
- backend request latency, timeouts, 4xx/5xx, throttling, multipart abort backlog;
- `FILE_STORAGE_UNAVAILABLE`, `FILE_STORAGE_CORRUPT`, unexpected not-found, or checksum mismatch;
- growing physical bytes not explained by current logical usage/retained-version policy.

An uncertain completion is retried only with the same upload ID and byte-identical completion XML.
The deterministic claim and operation identity reconcile the same result. A failed/cancelled
composition attempts to abort its backend writer; failure after staging commit may leave
unreachable staging bytes. Do not remove those bytes by filename or age alone. Until a published
reachability garbage collector exists, provider lifecycle/manual cleanup must prove that no active
or completing multipart upload and no immutable object version references them.

When corruption/mismatch appears:

1. stop writes and preserve Product/backend/provider audit evidence;
2. identify exact Project, Environment, File/Bucket, immutable version/ETag, and request ID without
   exposing credentials or unbounded object keys as metric labels;
3. verify whether the physical object exists at the recorded version and hash;
4. do not delete unreferenced bytes or edit metadata manually during triage;
5. restore a coordinated recovery point or execute an explicitly reviewed logical repair;
6. rotate provider/Product credentials if confidentiality may be affected.

See [Backup and recovery](../operations/backup-and-recovery.md),
[Capacity planning](../operations/capacity-planning.md), and
[Troubleshooting](../reference/troubleshooting.md) for the surrounding operator workflow.
