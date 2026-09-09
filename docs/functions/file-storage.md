# Application file storage

Status: **Implemented** for Safe V8 and local Full Node Actions with filesystem storage. Status:
**Conformance** for an S3-compatible MinIO backend. The compact Docker profile supports filesystem
or an operator-provided S3-compatible service. New builds always use the cumulative current runtime
identifier; the unpublished distributed Full Node profiles still fail closed for storage Platform
Ops until their interactive Agent channel has its own conformance gate.

Application Files let an Action authorize upload/download of immutable bytes without placing those
bytes in Function arguments, documents, or the V8 heap. Use this API for avatars, attachments,
exports, and other application-owned files whose access is decided by your Functions.

This capability is different from [Runku Object Storage](../concepts/object-storage.md):

| Need | Use |
|---|---|
| application decides access and gives a browser a short-lived grant | Application Files |
| an S3-compatible tool uses Runku buckets, keys, policies, and Product access keys | Runku Object Storage |

Only Actions can use Application Files. Operators configure the physical filesystem or external
S3-compatible object-store backend and quotas separately in
[Storage configuration](../self-hosting/storage-configuration.md).

## Capabilities and methods

| Capability | Methods added to `ctx.storage` |
|---|---|
| `storage:read` | `getMetadata`, `createDownload`, `get` |
| `storage:write` | `createUpload`, `store`, `delete` |

If neither capability is declared, `ctx.storage` is absent. If only one is declared, only that
method group exists. Query and Mutation cannot declare either storage capability.

## Metadata and grant shapes

```ts
interface FileMetadata {
  fileId: string
  sizeBytes: string
  sha256: string
  contentType: string
  createdAtMicros: string
}

interface FileUploadGrant {
  uploadId: string
  path: string
  token: string
  expiresAtMicros: string
  maxBytes: string
}

interface FileDownloadGrant {
  path: string
  token: string
  expiresAtMicros: string
  metadata: FileMetadata
}
```

Sizes and microsecond timestamps are decimal strings in grant/metadata results. `sha256` is exactly
64 lowercase hexadecimal characters. `fileId` is an opaque Environment-scoped `fil_*` identity;
it is not permission to read the file.

Grant `token` is short-lived bearer authority. Never log it, put it in a URL, store it as file
identity, or send it to a different Product origin.

## Create an upload grant

```ts
import { action, v } from "@runku/server"

const uploadGrant = v.object({
  uploadId: v.string(),
  path: v.string(),
  token: v.string(),
  expiresAtMicros: v.string(),
  maxBytes: v.string(),
})

export const beginAvatarUpload = action({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "storage:write"],
  args: v.object({
    sizeBytes: v.int64({ minimum: 1, maximum: 10_000_000 }),
    sha256: v.optional(v.string({ minLength: 64, maxLength: 64 })),
  }),
  returns: uploadGrant,
  async handler(ctx, input) {
    if (ctx.auth.principal?.kind !== "user") throw new Error("user required")

    return ctx.storage.createUpload({
      maxBytes: Number(input.sizeBytes),
      contentType: "image/png",
      ...(input.sha256 === undefined ? {} : { sha256: input.sha256 }),
    })
  },
})
```

Signature:

```ts
createUpload(options: {
  maxBytes: number
  contentType?: string
  sha256?: string
}): Promise<FileUploadGrant>
```

| Parameter | Required | Contract |
|---|---:|---|
| `maxBytes` | yes | positive integer, no larger than the operator's per-file limit |
| `contentType` | no | printable ASCII media type, at most 255 bytes, exactly one `/`; normalized lowercase |
| `sha256` | no | expected body digest, 64 lowercase hexadecimal characters |

When omitted, content type becomes `application/octet-stream`. When supplied, the upload request
must send the exact declared content type. When `sha256` is supplied, commit succeeds only if the
streamed bytes match it.

Creating the grant durably reserves `maxBytes` against the Environment quota and returns a one-shot
upload token. Do not reserve much more than the expected body: unused reservation temporarily
reduces available quota until the grant completes/expires and reconciliation cleans it.

## Stream the upload with `@runku/client`

```ts
const grant = (await runku.action("files.beginAvatarUpload", {
  sizeBytes: BigInt(file.size),
})).value

const metadata = await runku.uploadFile(grant, file, {
  contentType: "image/png",
})
```

The client uses `PUT grant.path` on the same Product origin and sends only
`Authorization: Bearer <grant.token>`. It does not retry. A success returns HTTP 201 and immutable
metadata.

For a client without the SDK, see [File upload without an SDK](../reference/public-api.md#file-upload-without-an-sdk).

### Upload lifecycle

1. the Action creates a durable reservation and one-shot grant;
2. the first valid PUT claims that grant;
3. Runku streams the body while enforcing content length, maximum, type, deadline, and optional hash;
4. after the physical object completes, metadata/quota/usage fact become durable;
5. a replay of the PUT fails with `FILE_STORAGE_CONFLICT`.

If the connection dies after sending bytes, the outcome is uncertain. Do not blindly replay the
one-shot PUT. Reconcile through application state or request a new business operation after
determining whether the expected File ID/metadata was committed.

## Authorize and create a download grant

Actions cannot read documents directly. Use a nested internal Query to verify that the functional
principal may access the requested File ID before issuing a grant:

```ts
const fileMetadata = v.object({
  fileId: v.string(),
  sizeBytes: v.string(),
  sha256: v.string(),
  contentType: v.string(),
  createdAtMicros: v.string(),
})

const downloadGrant = v.object({
  path: v.string(),
  token: v.string(),
  expiresAtMicros: v.string(),
  metadata: fileMetadata,
})

export const beginDownload = action({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "function:query", "storage:read"],
  args: v.object({ attachmentId: v.documentId("attachments") }),
  returns: downloadGrant,
  async handler(ctx, input) {
    const principal = ctx.auth.principal
    if (principal?.kind !== "user") throw new Error("user required")

    const result = await ctx.runQuery("attachments.authorizeDownload", {
      attachmentId: input.attachmentId,
      principalId: principal.id,
    })
    const authorized = result as { allowed: boolean; fileId: string }
    if (!authorized.allowed) throw new Error("download denied")

    return ctx.storage.createDownload(authorized.fileId, {
      expiresInMicros: 60_000_000n,
    })
  },
})
```

Signature:

```ts
createDownload(
  fileId: FileId | string,
  options: { expiresInMicros: bigint },
): Promise<FileDownloadGrant>
```

`expiresInMicros` must be positive and no larger than the operator-configured maximum (15 minutes
by default, configurable from one second through 24 hours). Prefer the shortest lifetime practical
for the transfer.

Do not issue a grant merely because the caller knows a File ID. Check application ownership,
membership, and revocation state first.

## Stream the download

```ts
const grant = (await runku.action("files.beginDownload", { attachmentId })).value
const response = await runku.downloadFile(grant)
const blob = await response.blob()
```

For allowed cross-origin browser applications, the gateway exposes the response headers the SDK
validates, including `accept-ranges`, `content-disposition`, `content-length`, `content-range`, and
`etag`. A reverse proxy must preserve those headers and the gateway's CORS response.

One inclusive-exclusive range is supported by the SDK:

```ts
const response = await runku.downloadFile(grant, {
  range: { start: 0n, end: 1_048_576n },
})
```

The server returns 200 for a full body or 206 for a range, plus length, content type, ETag/hash,
file identity, range, and private/no-store cache controls. The SDK verifies the response headers;
your code must still consume the body and treat a truncated/errored stream as failure.

For direct HTTP, see [File download without an SDK](../reference/public-api.md#file-download-without-an-sdk).

## Inspect metadata

```ts
const metadata = await ctx.storage.getMetadata(fileId)
```

`getMetadata(fileId)` requires `storage:read` and returns immutable metadata without reading the
bytes. Use it to validate a stored association or report size/type, but authorize the request first.

## Direct bytes inside an Action

For very small generated or consumed values:

```ts
const stored = await ctx.storage.store(
  new TextEncoder().encode("small export"),
  { contentType: "text/plain" },
)

const downloaded = await ctx.storage.get(stored.fileId)
// downloaded.metadata + downloaded.bytes: Uint8Array
```

Signatures:

```ts
store(
  bytes: Uint8Array,
  options?: { contentType?: string; sha256?: string },
): Promise<FileMetadata>

get(fileId: FileId | string): Promise<{
  metadata: FileMetadata
  bytes: Uint8Array
}>
```

Both require a non-empty body and are capped by the direct Action-byte limit: 2 MiB by default.
They also consume Function heap and Platform-operation budget. Use grant streaming for anything
larger or supplied by an end user.

`store` is an Action effect. Because Actions are not automatically retried, persist/reconcile the
returned File ID through an idempotent Mutation.

## Delete a file

```ts
await ctx.storage.delete(fileId)
```

`delete` requires `storage:write` and is idempotent. It first removes logical visibility, then
deletes physical bytes and completes metadata cleanup. Concurrent/new downloads no longer obtain
metadata after logical deletion; an already-open response has its own pinned stream lifecycle.

Deleting the file does not delete your application document that referenced it. Remove/update that
association in an idempotent Mutation according to your business retention policy.

## Default Self-Hosted limits

These are the compact distribution defaults. Operators may select smaller or larger validated
values, so applications should treat limit errors as configuration-aware.

| Boundary | Default | Valid relationship/range |
|---|---:|---|
| total committed + reserved bytes per Environment | 10 GiB | positive |
| one file | 256 MiB | positive and ≤ Environment bytes |
| direct `store`/`get` bytes | 2 MiB | positive and ≤ file bytes |
| concurrent HTTP uploads | 16 | `1..10000` |
| concurrent response-lifetime downloads | 64 | `1..10000` |
| live upload grants/tombstones | 4,096 | `1..1000000` |
| ready/deleting files | 100,000 | `1..10000000` |
| pending authoritative usage events | 1,000,000 | `1..10000000` |
| filesystem free-space floor | 512 MiB | must leave room for one max file |
| upload grant lifetime | 15 minutes | 1 second..24 hours |
| maximum requested download grant lifetime | 15 minutes | 1 second..24 hours |

Quota is enforced on reservations, not only completed bytes. A request can therefore fail before
uploading when active grants already reserve the remaining Environment quota.

## Error and retry guide

| Condition | Meaning | Safe response |
|---|---|---|
| invalid size/type/hash/range/grant | caller contract violation | correct request; do not repeat unchanged |
| quota/limit exceeded | Environment, file, concurrency, grant, file-count, or usage bound reached | release reservations/delete data or ask operator to change policy |
| not found | no ready file in this exact Environment | re-check application association |
| forbidden | capability or grant authority failed | do not substitute another credential type |
| conflict on upload | one-shot grant already claimed/terminal | reconcile; create new business grant only if appropriate |
| timeout/unavailable | backend or request did not complete in time | uploads/stores may be uncertain; do not blind-retry |
| corrupt | metadata, physical length/version, or SHA-256 disagrees | stop serving affected data and escalate to operator |

## Security checklist

- authorize File IDs through Environment-scoped application documents;
- keep transfer grants in memory only and redact them from telemetry;
- restrict accepted content type/size/hash before creating a grant;
- inspect untrusted content in an isolated downstream pipeline when required;
- never use an uploaded filename as a backend path;
- set short download expiries and re-check revocation before each new grant;
- treat file bytes as outside a document Mutation transaction;
- include physical bytes and metadata in the same recovery objective.

## Backup and portability consequences

Filesystem Application Files are included by the supported compact offline backup workflow when
the configured file root is inside the packaged data layout. Bytes in an external S3-compatible
object-store bucket are not copied by that workflow, and the packaged backup fails closed instead
of producing a misleading metadata-only recovery point.

If an operator chooses an external S3-compatible backend, the operator must back up, replicate, and
restore that bucket and coordinate it with Runku metadata before reopening traffic. Application developers
should not assume a committed File ID is recoverable unless the installation's recovery test covers
both metadata and bytes.

Runku SaaS can be used to validate the same Action/grant/client flow. Self-Hosted quota, backend,
encryption, retention, and recovery behavior must be validated on the actual installation.
