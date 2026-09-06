# Runku Object Storage

Status: the provider-independent bucket, object metadata and Product access-key registry is implemented. SQLite
conformance runs in the ordinary crate test; PostgreSQL 16+ runs when
`RUNKU_TEST_POSTGRES_URL` is set. The compact server and authenticated Management API now compose
bucket, object-browser/upload/download/delete, and Product access-key administration over this
authority. Object bytes use the same filesystem/S3 provider boundary as Application Files under a
physically disjoint content-addressed namespace. The attached Product listener also implements a
path-style AWS Signature Version 4 surface for bounded object operations, ListObjectsV2, immutable
version listing/deletion, multipart create/upload/list/complete/abort, COPY, public reads, presigned
URLs, ranges, conditional reads, and bucket CORS over the same authority. Bucket lifecycle rules
execute in bounded batches. The compact coordinated backup includes the authoritative Product
registry plus filesystem object bytes and verifies their archive digest before an empty restore.
The exact supported profile has conformance with the official AWS CLI; it is not a claim to every
AWS S3 service API.

Runku Object Storage is the bucket-and-object surface of Runku Storage. It gives one Project and
Environment logical buckets, object keys, versioning, CORS, quotas, and narrowly scoped Product
access keys. Its data plane is compatible with a supported subset of the Amazon S3 protocol, so
existing S3-compatible clients can connect to Runku; operators administer buckets and credentials
through the Runku Management API.

> **Product naming:** the service applications use is Runku Object Storage. “S3-compatible” in
> this guide describes client/protocol interoperability only. It does not rename the Runku product
> or imply that an application connects to the operator's physical storage provider.

Object Storage is appropriate for static media, imports/exports, build inputs, or integrations that
use an S3-compatible client. For end-user transfers authorized by an Action, use
[Application file storage](../functions/file-storage.md) instead.

## What is supported now

| Operation | Supported | Notes |
|---|---:|---|
| ListObjectsV2 | yes | prefix, delimiter, bounded page |
| HEAD object | yes | current object or immutable version |
| GET object | yes | range and conditional reads supported |
| PUT object | yes | one bounded request; multipart is available separately |
| CopyObject | yes | same logical bucket only |
| DELETE current object | yes | requires Product credential |
| public anonymous GET/HEAD | yes | only for a `publicRead` bucket |
| presigned GET/PUT/etc. | yes | only for operations in this supported subset and key scope |
| version-addressed GET/HEAD | yes | when versioning is enabled |
| multipart upload | yes | create, upload/list parts, complete, and abort within the bounded profile |
| list/delete object versions | yes | immutable version listing and exact-version deletion; no delete-marker resources |
| cross-bucket copy | no | copy within the same bucket |
| lifecycle execution | yes | rules execute in bounded batches |

The endpoint is path-style and uses logical signing region `runku`:

```text
https://<product-origin>/s3/<bucket>/<object-key>
```

A create command supplies the complete initial configuration: private or public-read policy,
bounded CORS rules, versioning, lifecycle periods, and logical quotas. Updates replace that complete
configuration and require the current positive revision. Archive also requires the current
revision, is irreversible in v1, requires an empty current-object namespace, and atomically revokes
all active access-key generations.

## Object authority and transfer ordering

Current keys and immutable versions persist size, SHA-256, Product ETag, content type, bounded user
metadata, and creation time. Current-object lists accept a bounded prefix, optional `/` delimiter,
exclusive full-key cursor, and limit up to 100. Version lists use the exclusive key/version cursor
and the same bound. Prefixes are a presentation over exact keys; they are never filesystem paths.
Administrative put and current delete require an `OperationId`; delete additionally requires the
exact current `ovr_*` version and therefore cannot remove a concurrent replacement. S3 exact-
version deletion is idempotent and promotes the newest remaining immutable version only when the
deleted version was current. Deleting the current key hides it while retaining version evidence;
the profile does not synthesize AWS delete-marker resources.

Upload hashes and validates the bounded bytes, writes the immutable physical content address, and
then commits quota/current/version metadata plus operation and audit in one serializable registry
transaction. A pre-commit conflict may leave an unreachable content-addressed blob, which is safe
for later garbage collection. A lost commit acknowledgement is reconciled through the separate
object operation journal. Download resolves metadata first and then verifies both physical size and
SHA-256 before returning bytes. Delete removes logical visibility but retains immutable physical
content and version evidence for recovery/garbage collection. The authenticated console transport
is bounded to 64 MiB per object; bucket quota may be lower.

Create, update, and archive use an `OperationId`. The repository hashes the complete canonical
client intent and exact scope; server-generated IDs, secret material, and processing timestamps do
not change that digest. An identical retry returns the committed operation; reusing the ID for a
different intent fails with `OBJECT_STORAGE_OPERATION_ID_REUSED`. A commit acknowledgement failure
returns `OBJECT_STORAGE_RESULT_UNCERTAIN`; callers reconcile through exact-scope operation lookup
before retrying.

Lists are stable, ID-ordered pages of at most 100 records. Reads and lists do not create state.
Unknown persisted enum values, malformed JSON, invalid revisions, unsupported schema versions, and
migration checksum drift fail closed.

## Product access keys

An access key belongs to exactly one bucket and carries an immutable label, object-key prefix, and
non-empty subset of `list`, `read`, `write`, and `delete`. It is a Product credential accepted by
the Product bearer and S3-compatible transports; it is never a Cloud, Management API, physical
provider, MinIO, or filesystem credential.

The service generates 256 random bits and returns a secret shaped as
`rk_st_v1_sak_<ULID>.<base64url>` exactly once. For bearer verification, SQL stores a
domain-separated HMAC-SHA256 digest using a deployment-owned 32-byte `SecretDigestKey`. For AWS
Signature Version 4 verification, schema v3 additionally stores the same generation in an
AES-256-GCM envelope whose associated data binds the exact Project, Environment, bucket, key ID,
and generation. The encryption key is domain-separated from the digest key; neither key is stored
in registry tables. Nonces and ciphertext have fixed bounds, authentication failure is corruption,
and diagnostic formatting redacts decrypted and encrypted material.

A successful idempotent replay or operation lookup still returns metadata only. The encrypted
generation is an internal verifier and no API reconstructs the one-time response. If the original
response is lost, the caller must rotate or revoke the key. Generations created before schema v3
remain valid for the bearer form but are intentionally unavailable to S3 verification until the
operator rotates them.

Rotation uses access-key revision CAS. It creates a new generation and keeps the prior generation
valid until an explicit cutoff strictly after the rotation time and no more than 24 hours later.
At the cutoff the prior generation is invalid. Revocation invalidates every generation atomically.
Authorization returns no distinction between malformed, missing, wrong-Environment, expired,
revoked, wrong-prefix, or missing-operation credentials.

The Management transport authenticates the operator for every call, authorizes the exact
Environment, supplies the verified `OperatorId` as actor, and applies request/body limits.
`storage:read` covers bucket/key metadata and operation lookup; `storage:manage` covers create,
replace, archive, issue, rotate, and revoke. Mutation requests require `Idempotency-Key: opn_*` and
pin canonical operation timestamps so an exact transport retry has the same journal digest.
Secret-bearing and non-secret key responses use `Cache-Control: no-store`. The registry does not
accept or store physical provider credentials.

The implemented routes are:

- `GET|POST /v1/projects/{project}/environments/{environment}/buckets`;
- `GET|PUT|DELETE .../buckets/{bkt_*}`;
- `GET|POST .../buckets/{bkt_*}/access-keys`;
- `POST .../access-keys/{sak_*}/rotate|revoke`;
- `GET .../storage-operations/{opn_*}` for uncertain-result reconciliation.
- `GET .../buckets/{bkt_*}/objects?prefix=&delimiter=/&after=&limit=`;
- `GET|PUT|DELETE .../buckets/{bkt_*}/objects/{key}` for verified raw byte transfer and CAS delete;
- `GET .../object-operations/{opn_*}` for uncertain object-result reconciliation.

Raw PUT requires `Idempotency-Key`, `X-Runku-At-Micros`, a bounded `Content-Type`, and may include
up to 32 bounded `X-Runku-Meta-*` headers. DELETE requires the same operation/timestamp headers plus
`X-Runku-Object-Version`. GET returns verified bytes with ETag, SHA-256, and version headers. These
routes are an operator-authenticated administrative surface, separate from the Product S3 route.

## S3-compatible Product route

The attached Product application listener exposes path-style buckets at
`{product-origin}/s3/{bucket}/{key}`. Configure an AWS-compatible client with endpoint
`{product-origin}/s3`, logical region `runku`, `forcePathStyle=true`, access key ID `sak_*`, and the
base64url suffix after the dot in the one-time `rk_st_v1_sak_*.{secret}` response as its secret
access key. The combined `rk_st_*` bearer remains accepted only by the Product bearer verifier.

Header and query-presigned AWS Signature Version 4 validate the canonical method, URI, query,
signed headers, payload hash, `runku/s3/aws4_request` scope, bounded clock skew, key generation,
bucket, prefix, and operation. Current and overlap generations are accepted only through their
configured cutoff; revocation is checked on every request. Semantic content, copy, checksum, and
`x-amz-meta-*` headers must be signed. Verification material and signing keys are redacted and
zeroized after use.

The implemented profile is `ListObjectsV2`, `ListObjectVersions`, `HEAD`, `GET`, bounded
single-request `PUT`, same-bucket `CopyObject`, current/exact-version `DELETE`,
`CreateMultipartUpload`, `UploadPart`, `ListParts`, `ListMultipartUploads`,
`CompleteMultipartUpload`, and `AbortMultipartUpload`. GET/HEAD accept one byte range, standard
ETag/date preconditions, `If-Range`, and an immutable `versionId` when bucket versioning is enabled.
Reads from a `public_read` bucket may be anonymous; listing and every mutation always require
Product credentials. Bucket CORS is evaluated for actual and preflight requests. Product
ETag/version/checksum metadata and sanitized Product request IDs are returned without exposing the
physical adapter. An exact signed single-object retry maps to one deterministic Product operation
ID, so the metadata journal resolves a lost acknowledgement instead of inventing a second logical
intent.

The per-request and completed-object bound is 64 MiB in server composition. A multipart upload has
at most 10,000 ordered parts; every non-final completed part is at least 5 MiB. Parts are immutable
content addresses and may be replaced while the upload is active. Completion first validates the
complete part set, durably claims a digest of the exact completion body, assembles and verifies the
bytes, commits through an object operation deterministically derived from upload ID plus completion
digest, and then marks the upload complete.
An identical completion retry reconciles; a different completion body, part mutation after claim,
abort during completion, or completion after abort fails closed. Bucket lifecycle execution can
expire current objects, expire non-current versions, and abort incomplete uploads in batches of at
most 100 per authenticated request. Physical content-addressed bytes are retained for later safe
garbage collection.

Bucket ACLs, tagging, website hosting, replication, provider administration, cross-bucket copy,
delete-marker resources, `UploadPartCopy`, and unbounded AWS pagination are outside this Runku
profile. Public and presigned URLs are Product routes; a Cloud deployment must wrap this origin
with its exact, revocable opaque Environment route rather than reveal a cell.

The ordinary Rust test uses the in-process router and durable SQLite/filesystem adapters. The
separate external-client gate starts a loopback listener and proves PUT, HEAD, ListObjectsV2,
version-addressed GET, range GET, COPY, multipart create/list/upload/list-parts/complete/download/
abort, version listing, exact-version deletion, current GET, and DELETE with the installed official
AWS CLI:

```sh
make object-storage-s3-client-check
```

The signing region is always `runku`. Runku Product access keys are Runku credentials, not
AWS/provider credentials. They never expose whether the Self-Hosted operator chose a filesystem or
an external S3-compatible object store as the physical byte backend.

SQLite uses one connection, WAL, full synchronous writes, foreign keys, and a busy timeout.
Production composition accepts PostgreSQL 16+ only, with bounded pools, statement/lock/idle
timeouts, and serializable writes. Migration history is append-only and checksum protected.
Schema v3 adds nullable authenticated-encryption fields so existing registries upgrade without
inventing secrets; every newly issued or rotated generation writes both verifier forms atomically.
Schema v4 adds durable multipart upload/part state and completion-claim digests. All are forward-
only migrations; an older binary must not write the registry after a newer schema is adopted.

## Required operator permissions

Bucket and Product-key administration uses a Platform operator session:

| Task | Capability |
|---|---|
| list/get buckets, objects, keys, operation results | `storage:read` |
| create/update/archive buckets | `storage:manage` |
| issue/rotate/revoke Product access keys | `storage:manage` |
| Management upload/delete | `storage:manage` |

Compact filesystem backup format v2 archives `product`, `platform`, and `files` together, verifies
the PostgreSQL dump and state archive digests, requires the Object Storage byte root, and restores
only into empty destinations before `doctor` and readiness checks. The release artifact campaign
adds an actual object byte before backup and verifies it after restore. An external-S3 deployment
still fails the compact backup closed because provider recovery must be coordinated separately.
External-provider backup/restore, physical orphan garbage collection, and a broader SDK/client
matrix retain their own later gates; they are not inferred from the official CLI campaign.

The operator bearer is an `rk_at_v1_*` token. It is valid at the Management origin, not the Runku
Storage data plane. Product access keys are valid at the Product origin, not the Management API.

Set placeholders for the examples:

```sh
export RUNKU_MANAGEMENT_URL="https://management.example.com"
export RUNKU_PRODUCT_URL="https://api.example.com"
export RUNKU_PROJECT_ID="prj_..."
export RUNKU_ENVIRONMENT_ID="env_..."
export RUNKU_ACCESS_TOKEN="rk_at_v1_..."
```

## Create a bucket

Bucket creation is an idempotent Management mutation. Generate one canonical `opn_*` operation ID
and one current non-negative Unix-microsecond timestamp:

```sh
curl --fail-with-body \
  -X POST \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "idempotency-key: opn_01ARZ3NDEKTSV4RRFFQ69G5FAY" \
  -H "content-type: application/json" \
  --data-binary @- \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/buckets" <<'JSON'
{
  "configuration": {
    "name": "media-assets",
    "policy": "private",
    "cors": [],
    "versioning": "enabled",
    "lifecycle": {
      "expireCurrentAfterDays": null,
      "expireNoncurrentAfterDays": null,
      "abortIncompleteAfterDays": null
    },
    "quota": {
      "maxObjectBytes": "67108864",
      "maxTotalBytes": "10737418240",
      "maxObjects": "100000"
    }
  },
  "atMicros": "1800000000000000"
}
JSON
```

The response is HTTP 201 and contains `bucket.bucketId`, complete configuration, revision `1`,
state, timestamps, `operationId`, and `replayed`. Persist the `bkt_*` ID. The human bucket name is
used on the Runku Storage route; the ID is used on Management routes.

### Bucket name rules

A name is unique within the exact Environment and:

- has 3–63 ASCII characters;
- starts with a lowercase letter;
- ends with a lowercase letter or digit;
- contains only lowercase letters, digits, and single hyphens;
- cannot contain `--`.

An archived bucket continues to reserve its name.

## Bucket policy

| Management value | Runku Storage behavior |
|---|---|
| `private` | every list/read/write/delete requires an authorized Product key/signature |
| `publicRead` | anonymous GET/HEAD is allowed; list and every mutation still require a key |

Public read is not a substitute for CORS. CORS controls which browsers may expose a response to
JavaScript; it does not make a private object public.

## Configure CORS

Example browser rule:

```json
{
  "origins": ["https://app.example.com"],
  "methods": ["GET", "HEAD", "PUT"],
  "allowedHeaders": ["content-type", "x-amz-content-sha256", "x-amz-date"],
  "exposedHeaders": ["etag", "x-runku-object-version"],
  "maxAgeSeconds": 3600
}
```

Limits/rules:

- at most 16 rules per bucket;
- each origins/headers list has at most 32 values;
- origin is an exact HTTPS origin with no trailing slash, or the only entry is `*`;
- methods are from `GET`, `HEAD`, `PUT`, `POST`, `DELETE`;
- header names are lowercase ASCII letters/digits/hyphens, at most 128 bytes;
- `*`, when allowed, must be the only value in its list;
- lists are sorted and contain no duplicates;
- `maxAgeSeconds` is `0..86400`.

Update replaces the **complete** bucket configuration, including CORS, policy, versioning,
lifecycle, and quota. Read the current bucket first and pass its positive `expectedRevision`; do not
send a partial patch.

## Versioning and lifecycle

`versioning` is `disabled` or `enabled`. With versioning enabled, a PUT creates an immutable
`ovr_*` version and changes the current object. GET/HEAD can address a returned version ID.

Lifecycle fields accept `null` or an integer from 1 through 36,500 days. Non-current expiry is
invalid when versioning is disabled.

**Current limitation:** Runku stores and validates lifecycle configuration but does not execute it.
Do not rely on these fields to delete objects, old versions, or incomplete multipart uploads. The
Runku Storage data plane does not support multipart in this release.

## Quotas and effective limits

Quota values are unsigned decimal strings to avoid JavaScript precision loss:

| Field | Rule |
|---|---|
| `maxObjectBytes` | positive and ≤ `maxTotalBytes` |
| `maxTotalBytes` | positive and ≤ 2^60 bytes |
| `maxObjects` | positive and ≤ 1,000,000,000 current objects |

The Product server accepts at most 64 MiB in one Runku Storage PUT. The effective per-object limit
is therefore the smaller of `maxObjectBytes` and 64 MiB. Multipart is unavailable, so objects over
that effective limit cannot be uploaded through the current Product route.

Quotas count the logical current namespace. Retained versions still have physical capacity and
recovery consequences even where they no longer count as current objects. Operators must monitor
the physical backend independently.

## Issue a scoped Product access key

Use the `bkt_*` ID from bucket creation:

```sh
export RUNKU_BUCKET_ID="bkt_..."

curl --fail-with-body \
  -X POST \
  -H "authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "idempotency-key: opn_01ARZ3NDEKTSV4RRFFQ69G5FAZ" \
  -H "content-type: application/json" \
  --data-binary @- \
  "${RUNKU_MANAGEMENT_URL}/v1/projects/${RUNKU_PROJECT_ID}/environments/${RUNKU_ENVIRONMENT_ID}/buckets/${RUNKU_BUCKET_ID}/access-keys" <<'JSON'
{
  "configuration": {
    "label": "media-uploader",
    "prefix": "public/",
    "operations": ["list", "read", "write", "delete"]
  },
  "atMicros": "1800000000000001"
}
JSON
```

The successful one-time response contains:

- `key.accessKeyId`: public `sak_*` identifier;
- non-secret configuration/revision/state;
- `secret`: `rk_st_v1_sak_<ULID>.<base64url-secret>`;
- operation ID and replay flag.

Capture the secret exactly once into an appropriate secret manager. Replaying the operation or
querying its operation result returns metadata without the secret. If the response is lost, rotate
or revoke the key; do not inspect storage/database contents to recover it.

### Key parameters

| Field | Contract |
|---|---|
| `label` | trimmed non-empty human label, at most 128 bytes, no controls |
| `prefix` | empty for whole bucket, otherwise at most 1,024 bytes, no leading `/`, controls, or `..` |
| `operations` | non-empty subset of `list`, `read`, `write`, `delete` |

The prefix is immutable for that key. Issue separate keys for separate applications or jobs; do
not share one broad read/write/delete key across unrelated workloads.

## Configure an S3-compatible client

Suppose the Management response was:

```text
rk_st_v1_sak_01ARZ3NDEKTSV4RRFFQ69G5FAV.YOUR_BASE64URL_SECRET
```

Configure:

```sh
export AWS_ACCESS_KEY_ID="sak_01ARZ3NDEKTSV4RRFFQ69G5FAV"
export AWS_SECRET_ACCESS_KEY="YOUR_BASE64URL_SECRET"
export AWS_DEFAULT_REGION="runku"
export AWS_EC2_METADATA_DISABLED="true"
```

The AWS access-key ID is the `sak_*` portion. The AWS secret-access key is only the suffix after
the dot. Do not use the complete `rk_st_*` value as either AWS field.

## AWS CLI example

Upload within the key prefix:

```sh
aws --endpoint-url "${RUNKU_PRODUCT_URL}/s3" \
  s3api put-object \
  --bucket media-assets \
  --key public/logo.png \
  --body ./logo.png \
  --content-type image/png
```

Inspect and download:

```sh
aws --endpoint-url "${RUNKU_PRODUCT_URL}/s3" \
  s3api head-object \
  --bucket media-assets \
  --key public/logo.png

aws --endpoint-url "${RUNKU_PRODUCT_URL}/s3" \
  s3api get-object \
  --bucket media-assets \
  --key public/logo.png \
  ./downloaded-logo.png
```

List the authorized prefix:

```sh
aws --endpoint-url "${RUNKU_PRODUCT_URL}/s3" \
  s3api list-objects-v2 \
  --bucket media-assets \
  --prefix public/
```

Copy within the same bucket and delete current object:

```sh
aws --endpoint-url "${RUNKU_PRODUCT_URL}/s3" \
  s3api copy-object \
  --bucket media-assets \
  --key public/logo-copy.png \
  --copy-source media-assets/public/logo.png

aws --endpoint-url "${RUNKU_PRODUCT_URL}/s3" \
  s3api delete-object \
  --bucket media-assets \
  --key public/logo-copy.png
```

An operation outside the key's bucket/prefix/operation set is unauthorized. Runku rechecks key
state and generation on every request.

## Object key and metadata rules

An object key:

- is 1–1,024 UTF-8 bytes;
- does not start with `/`;
- contains no control character;
- contains no path segment exactly equal to `..`.

`/` creates a logical prefix presentation only. It is not a filesystem traversal or provider path.

Content type is non-empty, at most 255 bytes, with no control characters. A write accepts at most
32 user metadata entries. Metadata names are lowercase ASCII letters/digits/hyphens and at most 128
bytes; values are at most 1,024 bytes and contain no controls. Sign semantic content, copy,
checksum, and `x-amz-meta-*` headers in SigV4 requests.

## List and pagination

ListObjectsV2 supports an exact prefix, optional `/` delimiter, and bounded pages. The Management
object browser similarly returns current objects and `commonPrefixes`; its page limit is `1..100`
and its continuation is the exclusive full object key.

Do not infer authorization from a returned prefix. The access key's immutable prefix is enforced
independently.

## Range, conditional, version, and public reads

GET/HEAD support one byte range, ETag/date preconditions, and `If-Range`. The Product ETag is a
quoted SHA-256 digest. When versioning is enabled, pass `versionId=<ovr_*>` to read that immutable
version.

For `publicRead`, an unsigned GET/HEAD may read an object. Listing and all writes/deletes still need
a Product key. Treat a public URL as permanently discoverable even if you later make the bucket
private; rotate/remove content when secrecy matters.

## Rotate a key

Rotation requires current key revision and can keep the previous generation valid briefly. The
cutoff must be strictly after `atMicros` and no more than 24 hours later. A new one-time secret is
returned only on the original successful response.

Recommended procedure:

1. read the key and record its current revision;
2. rotate with a short overlap and a new operation ID;
3. capture/install the new secret in consumers;
4. verify a signed read/write with the new generation;
5. verify the old generation stops at the cutoff;
6. revoke immediately if compromise is suspected.

Revocation invalidates all generations and has no grace period. Listing keys never returns secret
material.

## Update and archive a bucket

`PUT .../buckets/{bucketId}` is complete replacement under `expectedRevision`; it is not patch.
Preserve fields you do not intend to change. Use a new operation ID for new intent.

`DELETE .../buckets/{bucketId}` archives a bucket under current revision. Archive is irreversible
in v1, requires the current-object namespace to be empty, and revokes all active Product keys.
Delete application objects and verify the list is empty before archival.

## Management object transfer

Operators can list, GET, PUT, and delete objects below:

```text
/v1/projects/{project}/environments/{environment}/buckets/{bucketId}/objects/{key}
```

This is an administrative bearer-authenticated path, not the Runku Storage Product path. PUT/DELETE require
an idempotency key and `X-Runku-At-Micros`; DELETE also requires exact current
`X-Runku-Object-Version`. Use it for bounded console/repair workflows, not as an application data
plane.

## Failures and recovery

| Failure | Response |
|---|---|
| stale bucket/key revision | re-read and reconcile; never overwrite blindly |
| operation response uncertain | query the exact operation endpoint before retrying |
| one-time key response lost | rotate/revoke; secret is intentionally unrecoverable |
| quota/body limit exceeded | reduce object or change reviewed quota; multipart is unavailable |
| signature rejected | verify region `runku`, endpoint path, clock, key split, signed headers, scope |
| object hash/size mismatch | treat as storage corruption and stop serving affected object |
| unsupported protocol operation | redesign around the explicit subset; do not assume complete Amazon S3 parity |

## Self-Hosted backup boundary

With the supported compact **filesystem** profile, the offline `runku-selfhost` backup includes the
dedicated `files/` tree that contains both Application Files and Runku Object Storage bytes,
together with Product metadata. Restore and post-restore Runku Storage canaries remain required.

With the external object-store profile (`s3-files`), the helper fails closed instead of calling a
metadata-only archive complete. The operator must coordinate and test a provider-native
bucket/prefix recovery point with the matching Runku metadata snapshot. Replication, versioning,
encryption, and lifecycle of that external S3-compatible backend remain operator responsibilities.
See [Storage configuration](../self-hosting/storage-configuration.md)
and the [production-readiness contract](../self-hosting/production-readiness.md).

Runku SaaS can validate Runku Object Storage application behavior where the capability is enabled, but it
does not validate the recovery, encryption, capacity, or credential storage of your Self-Hosted
installation.
