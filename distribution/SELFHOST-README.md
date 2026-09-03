# Runku Self-Hosted compact installation package

This package installs one Safe V8 Product Environment on a dedicated Linux host with Docker
Compose. One `runku-server` process owns API, background work, Management, live logs, filesystem
Parquet archival, and embedded DuckDB queries. PostgreSQL stores Platform Identity. A TLS reverse
proxy on the host publishes the loopback-only Product and Management listeners.

Read `OPERATOR-GUIDE.md` in this archive before installation. Copy `.env.example` to `.env`, pin
the released image's multi-platform manifest digest, select absolute data/secret directories, and
then run:

```sh
./runku-selfhost configure
./runku-selfhost start
./runku-selfhost status
```

The archive intentionally contains no credential, sample password, TLS private key, IdP, NATS, or
object-store secret. `configure` generates a coordinated local secret set without overwriting an
existing one. Backups exclude external secret files and therefore require the original Platform
Identity pepper during disaster restore.

Use `compose.browser.yaml` only when browser CORS and Product JWT verification are configured. Use
`compose.s3-files.yaml` for application bytes in an independently operated S3-compatible service;
Runku does not provide that service's backup, replication, lifecycle, or recovery. Use
`compose.s3-logs.yaml` for off-host history without another process. Use `compose.ha-logs.yaml` only
with externally operated TLS NATS JetStream and durable S3-compatible storage; the helper selects
the matching compatible-endpoint overlays when a custom HTTPS object endpoint is required.

The current supported compact boundary is one Environment per server process, Safe V8, and one
active Product writer. It does not enable Full Node, multi-Environment orchestration, or
active-active writers for the same Environment.
