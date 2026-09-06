# Logical Object Storage registry

Status: the provider-independent bucket, object metadata and Product access-key registry is implemented. SQLite
conformance runs in the ordinary crate test; PostgreSQL 16+ runs when
`RUNKU_TEST_POSTGRES_URL` is set. The compact server and authenticated Management API now compose
bucket, object-browser/upload/download/delete, and Product access-key administration over this
authority. Object bytes use the same filesystem/S3 provider boundary as Application Files under a
physically disjoint content-addressed namespace. The attached Product listener also implements a
path-style AWS Signature Version 4 surface for bounded single-object operations, ListObjectsV2,
COPY, public reads, presigned URLs, ranges, conditional reads, immutable version reads, and bucket
CORS over the same authority. Native SDK object operations, multipart, version deletion/listing,
lifecycle execution, coordinated backup, and CLI commands remain unimplemented.

This capability is distinct from [Application file storage](../functions/file-storage.md).
Application Files are an Action-oriented upload/download facility. Logical Object Storage is a
Product resource model intended to become the common authority behind future native SDK,
administrative, and S3-compatible surfaces.

## Authority and scope

Every bucket, current object, immutable object version, access key, operation, and audit event carries an exact `ProjectId` plus
`EnvironmentId`. The registry never derives either value from a bucket name, credential, hostname,
or request body. Bucket names are a conservative DNS-label subset and are unique within the exact
Environment; an archived bucket continues to reserve its name.

`runku-object-storage` is a pure domain/service crate. `runku-object-storage-repository` is the only
crate in this capability that knows about SQL. Neither crate contains provider endpoints,
credentials, regions, physical bucket names, or object bytes. `FileObjectStore` supplies only the
physical content-addressed byte boundary; provider enumeration never becomes Product authority.

## Bucket lifecycle

A create command supplies the complete initial configuration: private or public-read policy,
bounded CORS rules, versioning, lifecycle periods, and logical quotas. Updates replace that complete
configuration and require the current positive revision. Archive also requires the current
revision, is irreversible in v1, requires an empty current-object namespace, and atomically revokes
all active access-key generations.

## Object authority and transfer ordering

Current keys and immutable versions persist size, SHA-256, Product ETag, content type, bounded user
metadata, and creation time. Lists accept a bounded prefix, optional `/` delimiter, exclusive full-
key cursor, and limit up to 100. Prefixes are a presentation over exact keys; they are never
filesystem paths. Put and delete require an `OperationId`; delete additionally requires the exact
current `ovr_*` version and therefore cannot remove a concurrent replacement.

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

The implemented subset is `ListObjectsV2`, `HEAD`, `GET`, bounded single-request `PUT`, same-bucket
`CopyObject`, and current-object `DELETE`. GET/HEAD accept one byte range, standard ETag/date
preconditions, `If-Range`, and an immutable `versionId` when bucket versioning is enabled. Reads
from a `public_read` bucket may be anonymous;
listing and every mutation always require Product credentials. Bucket CORS is evaluated for actual
and preflight requests. Product ETag/version/checksum metadata and sanitized Product request IDs are
returned without exposing the physical adapter. An exact signed retry maps to one deterministic
Product operation ID, so the metadata journal resolves a lost acknowledgement instead of inventing
a second logical intent.

The non-multipart body bound is 64 MiB in server composition. Multipart upload/list/abort,
version listing/deletion, lifecycle execution, and cross-bucket copy are not yet part of this
subset and must not be advertised as implemented S3
operations. Public and presigned URLs are Product routes; a Cloud deployment must wrap this origin
with its exact, revocable opaque Environment route rather than reveal a cell.

The ordinary Rust test uses the in-process router and durable SQLite/filesystem adapters. The
separate external-client gate starts a loopback listener and proves PUT, HEAD, ListObjectsV2,
version-addressed GET, range GET, COPY, GET, and DELETE with the installed official AWS CLI:

```sh
make object-storage-s3-client-check
```

## Persistence and recovery

SQLite uses one connection, WAL, full synchronous writes, foreign keys, and a busy timeout.
Production composition accepts PostgreSQL 16+ only, with bounded pools, statement/lock/idle
timeouts, and serializable writes. Migration history is append-only and checksum protected.
Schema v3 adds nullable authenticated-encryption fields so existing registries upgrade without
inventing secrets; every newly issued or rotated generation writes both verifier forms atomically.

Successful state mutation, operation journal entry, and audit event commit in one transaction.
Audit rows are append-only and ordered independently within each Environment. Operators should:

1. Retry `BUSY` or `UNAVAILABLE` with bounded backoff.
2. On `RESULT_UNCERTAIN`, query the exact `OperationId` and scope.
3. If a key operation committed but its one-time secret response was lost, rotate or revoke it;
   never inspect SQL or backups for plaintext because none is persisted and the internal envelope
   is not a recovery API.
4. Treat migration checksum mismatch, malformed persisted configuration, or impossible key state
   as corruption and stop writes until the authoritative database is restored or repaired.

The current backup/restore contract still does not coordinate these bytes, so an operator must not
claim complete Object Storage recovery yet. Filesystem byte round trips run in ordinary tests and
the shared physical S3 adapter retains its opt-in MinIO conformance. The coordinated recovery
point, lifecycle/garbage-collection worker, multipart protocol, and real AWS-compatible client
campaign each require their own later gate.
