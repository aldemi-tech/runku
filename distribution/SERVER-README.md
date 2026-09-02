# Runku Server

This archive contains the Linux `runku-server` binary for the compact Runku Self-Hosted profile.
It composes PostgreSQL-backed Platform Identity and one explicitly attached Product Environment.
The matching OCI image additionally includes the same-version `runku` CLI for controlled init-job
and diagnostic use.

Verify the archive against the release `SHA256SUMS`, keep the binary and image version aligned with
the CLI/SDK version, and read the public operator documentation before starting it:

- `docs/self-hosting/overview.md`
- `docs/auth/platform-identity.md`
- `docs/operations/remote-lifecycle.md`
- `docs/operations/backup-and-recovery.md`

Run `runku-server version` without configuration. `check`, `migrate`, `recover-bootstrap`, and
`serve` require the documented environment configuration. The compact image is non-root and has no
shell or package manager. Mount only explicitly required writable state and application paths.

This profile supports Safe V8. A Full Node trust profile requires its separately qualified Agent,
queue, artifact, OCI, and isolation deployment; it is not silently enabled by this image.
