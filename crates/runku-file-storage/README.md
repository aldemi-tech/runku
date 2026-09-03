# `runku-file-storage`

Environment-scoped application-file metadata, quota reservation, signed transfer grants, and
filesystem/S3-compatible object adapters.

SQLite schema version 1 is additive state introduced with runtime contract version 2. It stores
upload lifecycle, immutable metadata with the committed backend ETag/version, and an authoritative
usage outbox, never raw bytes or plaintext transfer tokens. Generated object keys use:

```text
{configured-prefix}/v1/projects/{projectId}/environments/{environmentId}/files/{fileId}
```

Unknown schema versions fail closed. Any future schema or key-layout change must add a new decoder
or migration; do not reinterpret version 1 rows/keys. Use the public operator/developer contract in
[`docs/functions/file-storage.md`](../../docs/functions/file-storage.md).

Evidence:

```sh
cargo test -p runku-file-storage
scripts/file-storage-evidence.sh
```

The evidence script uses a digest-pinned MinIO dependency only for conformance. It is not a
production deployment or a backup strategy.
