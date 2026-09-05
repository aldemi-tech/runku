# Logical Object Storage registry

Status: the provider-independent bucket and Product access-key registry is implemented. SQLite
conformance runs in the ordinary crate test; PostgreSQL 16+ runs when
`RUNKU_TEST_POSTGRES_URL` is set. Object bytes, an S3-compatible HTTP surface, provider adapters,
Management API routes, server composition, and CLI commands are not implemented by this slice.

This capability is distinct from [Application file storage](../functions/file-storage.md).
Application Files are an Action-oriented upload/download facility. Logical Object Storage is a
Product resource model intended to become the common authority behind future native SDK,
administrative, and S3-compatible surfaces.

## Authority and scope

Every bucket, access key, operation, and audit event carries an exact `ProjectId` plus
`EnvironmentId`. The registry never derives either value from a bucket name, credential, hostname,
or request body. Bucket names are a conservative DNS-label subset and are unique within the exact
Environment; an archived bucket continues to reserve its name.

`runku-object-storage` is a pure domain/service crate. `runku-object-storage-repository` is the only
crate in this capability that knows about SQL. Neither crate contains provider endpoints,
credentials, regions, physical bucket names, or object bytes.

## Bucket lifecycle

A create command supplies the complete initial configuration: private or public-read policy,
bounded CORS rules, versioning, lifecycle periods, and logical quotas. Updates replace that complete
configuration and require the current positive revision. Archive also requires the current
revision, is irreversible in v1, and atomically revokes all active access-key generations.

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
non-empty subset of `list`, `read`, `write`, and `delete`. It is a Product credential, not a Cloud,
Management API, provider, MinIO, S3, or filesystem credential.

The service generates 256 random bits and returns a secret shaped as
`rk_st_v1_sak_<ULID>.<base64url>` exactly once. The SQL repository receives and stores only a
domain-separated HMAC-SHA256 digest using a deployment-owned 32-byte `SecretDigestKey`; debug
formatting redacts both the key and returned secret. The digest key is not stored in registry
tables. A successful idempotent replay or operation lookup returns metadata only. If the original
response is lost, the secret is unrecoverable and the caller must rotate or revoke the key.

Rotation uses access-key revision CAS. It creates a new generation and keeps the prior generation
valid until an explicit cutoff strictly after the rotation time and no more than 24 hours later.
At the cutoff the prior generation is invalid. Revocation invalidates every generation atomically.
Authorization returns no distinction between malformed, missing, wrong-Environment, expired,
revoked, wrong-prefix, or missing-operation credentials.

The transport that eventually exposes these APIs remains responsible for authenticating the actor,
authorizing the exact Environment, supplying a canonical actor ID and trustworthy timestamp,
redacting the one-time secret, and applying request/body limits. The registry does not accept or
store physical provider credentials.

## Persistence and recovery

SQLite uses one connection, WAL, full synchronous writes, foreign keys, and a busy timeout.
Production composition accepts PostgreSQL 16+ only, with bounded pools, statement/lock/idle
timeouts, and serializable writes. Migration history is append-only and checksum protected.

Successful state mutation, operation journal entry, and audit event commit in one transaction.
Audit rows are append-only and ordered independently within each Environment. Operators should:

1. Retry `BUSY` or `UNAVAILABLE` with bounded backoff.
2. On `RESULT_UNCERTAIN`, query the exact `OperationId` and scope.
3. If a key operation committed but its one-time secret response was lost, rotate or revoke it;
   never inspect SQL or backups for plaintext because none is persisted.
4. Treat migration checksum mismatch, malformed persisted configuration, or impossible key state
   as corruption and stop writes until the authoritative database is restored or repaired.

The current backup, restore, and availability guarantees are those of the selected registry
database. No object-byte durability claim exists until a provider adapter and its own conformance
campaign are implemented.
