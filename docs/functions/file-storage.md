# Application file storage

Status: **Implemented** for Safe V8 and local Full Node Actions with filesystem storage. Status:
**Conformance** for an S3-compatible MinIO backend. The compact Docker profile supports filesystem
or an operator-provided S3-compatible service. The current distributed Full Node Agent protocol
does not carry storage Platform Ops and rejects the `runku-node-2`/`runku-hybrid-2` contract.

Application file storage keeps large immutable bytes out of Function arguments and document rows.
Metadata and quota reservations are authoritative Environment state; object bytes live in a
dedicated filesystem directory or bucket prefix. File IDs are opaque identifiers, never authority.

## Function API

Only Actions may declare `storage:read` or `storage:write`. An absent capability removes the
corresponding methods and the runtime independently denies the Platform Op. Safe V8 and local Full
Node expose the same `ctx.storage` interface:

```ts
import { action, v } from "@runku/server"

export const beginUpload = action({
  auth: "user",
  visibility: "public",
  capabilities: ["storage:write"],
  args: v.object({ size: v.int64({ minimum: 1n, maximum: 10_000_000n }) }),
  returns: v.any(),
  async handler(ctx, input) {
    return await ctx.storage.createUpload({
      maxBytes: Number(input.size),
      contentType: "image/png",
      // Add `sha256` with a lowercase digest when the exact body is already known.
    })
  },
})

export const beginDownload = action({
  auth: "user",
  visibility: "public",
  capabilities: ["storage:read"],
  args: v.object({ fileId: v.string() }),
  returns: v.any(),
  async handler(ctx, input) {
    // Authorize ownership in application data before delegating access.
    return await ctx.storage.createDownload(input.fileId, { expiresInMicros: 60_000_000n })
  },
})
```

`createUpload` reserves the declared maximum against the Environment quota and returns a one-shot
grant. `createDownload` returns a reusable grant bounded by its short expiry. `getMetadata`,
`createDownload`, and bounded `get` require `storage:read`; `createUpload`, bounded `store`, and
idempotent `delete` require `storage:write`. Direct `store`/`get` are limited to the configured
Action-memory bound (2 MiB by default); use HTTP streaming for larger files.

Authorization of business ownership belongs in the Action. Do not accept an arbitrary File ID and
issue a download grant without checking the caller's principal and an Environment-scoped document
that associates that file with the caller.

## Browser/server transfer API

Resolve grants only through the configured `RunkuClient`. It enforces a same-origin canonical path,
places the secret in the `Authorization` header, never retries an upload, validates response
metadata, and keeps download deadlines active while the response stream is consumed:

```ts
const uploadGrant = (await runku.action("files.beginUpload", { size: BigInt(file.size) })).value
const metadata = await runku.uploadFile(uploadGrant, file, { contentType: "image/png" })

const downloadGrant = (await runku.action("files.beginDownload", {
  fileId: metadata.fileId,
})).value
const response = await runku.downloadFile(downloadGrant)
const downloaded = await response.blob()

// One explicit inclusive-exclusive range is supported.
const partial = await runku.downloadFile(downloadGrant, {
  range: { start: 0n, end: 1024n },
})
```

The raw routes are `PUT /v1/files/uploads/{uploadId}` and
`GET|HEAD /v1/files/downloads/{fileId}`. They accept only `Authorization: Bearer <grant-token>`;
Application Keys and user JWTs do not substitute for a transfer grant. Uploads may include the
exact declared `Content-Type` and `Content-Length`. Content encoding is rejected. Downloads return
`Content-Length`, `ETag` (the SHA-256), `Accept-Ranges: bytes`, `Content-Disposition: attachment`,
and `Cache-Control: private, no-store`. Only a single explicit `bytes=start-end` range is accepted.

## Durable lifecycle and retry

1. `createUpload` durably reserves the maximum size before returning a token.
2. The first valid PUT atomically claims the reservation. Replay returns
   `FILE_STORAGE_CONFLICT`; do not retry it blindly after an uncertain response.
3. Bytes stream to a multipart object while enforcing the declared maximum and optional SHA-256.
4. Only after object completion does immutable metadata become `ready`; the object ETag/version,
   quota, and an authoritative `application_file.committed` usage event commit atomically.
5. `delete` first hides metadata, removes the object, and then removes the metadata row. Repeating
   delete is safe; interrupted deleting/uploading rows are reconciled when the service reopens.

An Action `store` is also an external effect and Actions are never automatically retried. Persist
the returned File ID in an idempotent Mutation or reconcile an uncertain result at application
level. Metadata/object disagreement fails as `FILE_STORAGE_CORRUPT`. Full streams verify byte count
and SHA-256 before completing; range streams are pinned to the exact ETag/version recorded at
commit and verify their returned length. Clients must treat an errored or truncated response body
as failed, even when response headers were already received.

## Limits and attack controls

- All metadata, quota sums, token MAC input, and object keys include exact Project and Environment.
- Tokens use a server-owned, Environment-local HMAC key, bind operation/resource/expiry, have a
  1 KiB hard limit, are constant-time verified, and are never placed in URLs or logs.
- Upload tokens are one-shot. Reservations, committed bytes, per-file size, Action-memory size,
  concurrent uploads, response-lifetime downloads, live files, unexpired grant rows, pending usage
  facts, header count/bytes, request deadline, and backend operation time are bounded. Runtime
  Platform Ops are independently capability-checked
  and budgeted, including frames emitted directly by Full Node code. Expired terminal grant rows
  are pruned during admission and startup; completed deletes remove their metadata row instead of
  accumulating tombstones.
- Filesystem admission preserves a configured free-space floor. Object paths are generated from
  canonical IDs; user filenames and path segments never reach the backend.
- MIME values, decimal sizes, SHA-256, IDs, ranges, duplicate headers, origins, and response headers
  are strictly validated. Unknown versions and storage capability tags fail closed.
- S3-compatible endpoints require HTTPS. Literal loopback HTTP is an explicit conformance/local
  option and cannot authorize a non-loopback endpoint. Credentials are redacted and never exposed
  to Functions or clients.
- The bucket/prefix must be dedicated to application files and credentials must have only required
  multipart read/write/delete/list-abort permissions for that prefix. Configure provider-side
  encryption, access audit, object lifecycle cleanup for abandoned multipart uploads, and network
  restrictions.

## Operator configuration

The compact filesystem profile mounts `${RUNKU_DATA_DIRECTORY}/files` at
`/var/lib/runku/files`; local development uses `.runku/file-storage-objects`. SQLite metadata and
the transfer-token key stay under the Product root.

| Variable | Default/requirement |
|---|---|
| `RUNKU_FILE_STORAGE_BACKEND` | `filesystem`; alternatively `s3` |
| `RUNKU_FILE_STORAGE_FILESYSTEM_ROOT` | optional absolute, non-root dedicated directory; existing Unix roots must already deny group/other access |
| `RUNKU_FILE_STORAGE_ENVIRONMENT_BYTES` | 10 GiB |
| `RUNKU_FILE_STORAGE_FILE_BYTES` | 256 MiB and no larger than Environment quota |
| `RUNKU_FILE_STORAGE_ACTION_BYTES` | 2 MiB and no larger than file limit |
| `RUNKU_FILE_STORAGE_FILESYSTEM_MINIMUM_FREE_BYTES` | 512 MiB |
| `RUNKU_FILE_STORAGE_CONCURRENT_UPLOADS` | 16 |
| `RUNKU_FILE_STORAGE_CONCURRENT_DOWNLOADS` | 64; held until the response body finishes, fails, expires, or is cancelled |
| `RUNKU_FILE_STORAGE_MAXIMUM_LIVE_UPLOAD_GRANTS` | 4096; admission bound for unexpired grants and replay tombstones |
| `RUNKU_FILE_STORAGE_MAXIMUM_FILES` | 100000 ready/deleting files per Environment |
| `RUNKU_FILE_STORAGE_MAXIMUM_PENDING_USAGE_EVENTS` | 1000000 committed usage facts awaiting sink acknowledgement; new commits stop at the bound while deletes remain possible |
| `RUNKU_FILE_STORAGE_UPLOAD_GRANT_TTL_SECONDS` | 900, bounded to 1–86400 |
| `RUNKU_FILE_STORAGE_DOWNLOAD_GRANT_MAX_TTL_SECONDS` | 900, bounded to 1–86400 |
| `RUNKU_FILE_STORAGE_S3_BUCKET`, `_REGION`, `_PREFIX` | required for `s3` |
| `RUNKU_FILE_STORAGE_S3_ENDPOINT` | optional HTTPS S3-compatible endpoint |
| `RUNKU_FILE_STORAGE_S3_VIRTUAL_HOSTED_STYLE` | `false` in the server, profile may set `true` |
| `RUNKU_FILE_STORAGE_S3_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY`, `_SESSION_TOKEN` | optional explicit credentials; each supports a mutually exclusive `_FILE` form |

If explicit S3 credentials are absent, Runku imports only the AWS access/session, web-identity, and
container-relative credential variables needed by the credential chain. Ambient endpoint, TLS,
signing, and metadata-endpoint overrides are not imported. `runku-server check` validates
configuration and opens the filesystem boundary, but backend reachability is ultimately proven by
a successful upload/download/delete canary.

## Authoritative usage export

File commit and delete transitions write positive-byte facts to a bounded SQLite outbox in the same
transaction as Product metadata. The stable kinds are `application_file.committed` and
`application_file.deleted`; billing aggregation applies the sign from the kind and integrates
stored bytes over its price window. Ambiguous delivery replays the same event ID.

`runku-server` can push batches of at most 100 facts to an exact HTTPS sink and acknowledges an
ordered prefix only after HTTP 202. Redirects and ambient proxies are disabled. Literal-loopback
HTTP is an explicit local-conformance exception.

| Variable | Requirement |
|---|---|
| `RUNKU_FILE_USAGE_SINK_URL` | exact HTTPS ingestion URL |
| `RUNKU_FILE_USAGE_CELL_ID` | exact source cell ID assigned by the operator |
| `RUNKU_FILE_USAGE_SINK_TOKEN_FILE` | private mounted bearer; a direct value is local bootstrap only |
| `RUNKU_FILE_USAGE_INTERVAL_SECONDS` | 1–300; default 5 when configured |
| `RUNKU_FILE_USAGE_SINK_ALLOW_LOOPBACK_HTTP` | `false`; local conformance only |

These facts—not S3 metrics, diagnostic logs, or best-effort telemetry—are the authoritative storage
billing input. The sink derives tenant ownership from its own Project/Environment records and
verifies that the reported cell is the current placement.

## Backup, restore, and residual responsibility

Runku does **not** back up application file bytes, configure replication/versioning, manage S3 or
MinIO lifecycle, or provide a disaster-recovery strategy for this feature. The compact
`runku-selfhost backup` archives Product metadata but deliberately excludes the dedicated `files/`
directory and any external bucket. A metadata-only restore is incomplete and may report
`FILE_STORAGE_CORRUPT` or not-found objects.

Operators must back up and restore the filesystem directory at a coordinated recovery point, or
use separately operated MinIO/S3 storage with the required durability, versioning/replication,
retention, encryption, lifecycle, and tested restore process. Restore object bytes before reopening
Product traffic, then canary metadata/download/checksum/delete and compare capacity. Runku cannot
roll an external bucket back during a binary rollback and never deletes objects outside its exact
generated prefix.

## Evidence and diagnosis

- `cargo test -p runku-file-storage` covers filesystem lifecycle, range reads, quota, checksum,
  token tampering, replay, header drift, and deletion.
- `cargo test -p runku-gateway --test file_transfers` covers raw HTTP/CORS/security behavior.
- The runtime tests cover the capability-scoped Safe V8 and local Full Node bridges; the local
  process test invokes an authenticated published Action and consumes its grants over HTTP.
- `scripts/file-storage-evidence.sh` starts digest-pinned MinIO, exercises a multipart upload,
  range download, checksum, and delete, then removes only its uniquely named Compose project.
- `pnpm --dir packages/client test` covers same-origin transfer handling and credential separation.

Treat repeated `FILE_STORAGE_UNAVAILABLE`, `FILE_STORAGE_CORRUPT`, quota failures below expected
usage, filesystem free-space alerts, S3 4xx/5xx, or orphaned multipart growth as incidents. Tokens,
File IDs, object keys, MIME values, and user filenames must not become unbounded metric labels.
