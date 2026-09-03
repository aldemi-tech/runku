# Docker standalone installation

This is the supported compact Runku Self-Hosted package for a dedicated Linux machine. It runs one
Safe V8 Product Environment and Platform Management in one `runku-server` container. PostgreSQL is
the only required service. Operational Log capture, filesystem Parquet archival, retention,
historical DuckDB query, and live streaming stay inside the Runku process.

The package does not enable Full Node, orchestrate multiple Environments, or make one SQLite Product
Environment active-active. Those are different trust and consistency profiles.

## Topology and network boundary

```text
public application origin ─► host TLS proxy ─► 127.0.0.1:3210 Product API/WS
operator/CLI origin        ─► host TLS proxy ─► 127.0.0.1:3220 Management
                                                    │
                                    one runku-server container
                                      ├─ Safe V8/background/realtime
                                      ├─ Product SQLite + Parquet/DuckDB
                                      ├─ application files ─► dedicated filesystem or external S3
                                      └─ Platform Identity ─► PostgreSQL container
```

Runku deliberately binds Product and Management to loopback. The server container therefore uses
Linux host networking. Only the operator-owned TLS reverse proxy publishes them. Do not forward the
PostgreSQL loopback port or either plaintext Runku listener to another host. This package is not
supported on Docker Desktop networking or a shared container host whose trust boundary does not
match the installation.

## Requirements

- Linux ARM64 or x86_64 with a currently supported Docker Engine and Compose v2;
- a dedicated non-root numeric UID/GID that owns the Runku data directories;
- an exact HTTPS Management origin and a host TLS reverse proxy with WebSocket support;
- `openssl`, `jq`, and `tar` for configuration, backup, and restore;
- an encrypted backup destination or encrypted-volume policy with a stable reference;
- the release archive `runku-selfhost-vX.Y.Z.tar.gz` and its verified `SHA256SUMS` entry;
- the released server image pinned by its multi-platform manifest digest, never `latest`.

The included PostgreSQL 16 image is digest-pinned. If an external PostgreSQL service is selected,
remove the bundled service only after preserving the database URL secret, health ordering, TLS,
backup, version, and restore contracts.

## Install

Verify and extract the release package on the dedicated host:

```sh
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf runku-selfhost-vX.Y.Z.tar.gz
cd runku-selfhost-vX.Y.Z
cp .env.example .env
chmod 0600 .env
```

Edit `.env`:

1. replace the image placeholder with `ghcr.io/aldemi-tech/runku-server:X.Y.Z@sha256:<manifest>`;
2. select absolute data and secret directories;
3. set `RUNKU_UID` and `RUNKU_GID` to their non-root host owner;
4. set the exact public HTTPS Management origin;
5. keep `RUNKU_DEPLOYMENT_PROFILE=standalone` unless an optional profile below is required.

Prepare secrets and persistent directories without starting a server:

```sh
./runku-selfhost configure
```

`configure` creates one random PostgreSQL password, the matching connection URL, and one 256-bit
Platform Identity pepper as `0600` files. It never replaces an existing secret and rejects a
partial set. No secret is placed in `.env`, a command argument, or the container environment.

Initialize the Product repositories, run configuration/migration preflight, and start:

```sh
./runku-selfhost start
./runku-selfhost status
```

Startup is idempotent. It refuses divergent Product identity and waits for PostgreSQL and Management
readiness. The initial-owner invitation is written below the configured data directory at
`platform/bootstrap/initial-owner.code`. Enroll it using the protected procedure in
[Platform operator identity](../../docs/auth/platform-identity.md#enroll-the-initial-owner).

The standalone profile creates a private `${RUNKU_DATA_DIRECTORY}/files` directory, enforces the
configured Environment/file/Action/free-space quotas, and mounts it separately from the Product
root. Select `RUNKU_DEPLOYMENT_PROFILE=s3-files` (or `browser-s3-files` when the browser overlay is
also required) to use `compose.s3-files.yaml` instead. Configure
the bucket/region/unique prefix in `.env`, place access-key ID and secret access key in the named
secret files, and use only an HTTPS S3-compatible endpoint. The bucket must already exist. MinIO or
the selected provider—not Runku—owns encryption, replication, versioning, lifecycle, capacity,
backup, restore, and availability. See
[Application file storage](../../docs/functions/file-storage.md#operator-configuration).

## Publish the listeners through TLS

Configure two exact host-proxy routes:

| Public route | Loopback upstream | Requirements |
|---|---|---|
| Application HTTP/WebSocket | `http://127.0.0.1:3210` | preserve upgrade/streaming, bounded bodies/timeouts |
| Authentication/Management | `http://127.0.0.1:3220` | no caching of auth responses, bounded upload/stream timeouts |

The Product listener starts lazily after the first successful Channel promotion. A proxy failure
before that promotion is expected; Management readiness at port 3220 is the installation probe.
Forward only the headers explicitly trusted by the selected proxy configuration and prevent direct
network access to both upstreams.

## Bring application code and operate it

The Product directory is the Environment's durable application root. Put the application's
`runku/` source there before building on the host, or give CI a protected copy of that root's
non-secret scope metadata. Do not initialize a second unrelated root: remote publication rejects a
different Project identity.

Install the matching CLI on an operator/CI machine, log in, and use the normal lifecycle:

```sh
npm install --global @runku/cli@X.Y.Z
runku login --url https://management.example.com
runku build --root /srv/runku/product
runku publish --remote --root /srv/runku/product \
  --manifest /exact/build/manifest --artifact /exact/build/artifact --expected-head empty
runku release --remote --root /srv/runku/product --release rel_...
runku promote --remote --root /srv/runku/product \
  --channel stable --release rel_... --expected empty
runku status --remote --root /srv/runku/product
runku logs --remote --root /srv/runku/product --follow
```

Use actual JSON output paths and IDs as shown in the
[remote lifecycle runbook](../../docs/operations/remote-lifecycle.md); the shortened placeholders
above are not values to copy.

## Browser application profile

Set `RUNKU_DEPLOYMENT_PROFILE=browser`, an exact comma-separated origin list, and a Product-root-
relative JWT provider descriptor in `.env`. Store the descriptor under the Product directory and
start normally. This selects `compose.browser.yaml`; invalid or parent-traversing paths and malformed
origins fail before Product serving.

OIDC for platform operators is independent. Mount its strict JSON file and add
`RUNKU_PLATFORM_OIDC_CONFIG` through an installation-owned Compose override; do not place its subject
pepper in `.env`.

## Backup and offline verification

Create a uniquely named backup on an already encrypted destination. The second argument records the
external encryption/key-policy reference in the manifest and cannot be `none`:

```sh
./runku-selfhost backup /mnt/encrypted/runku-backup-2026-09-02 kms://backup-policy/version-7
./runku-selfhost verify-backup /mnt/encrypted/runku-backup-2026-09-02
```

Backup briefly stops serving, creates a PostgreSQL custom-format dump, archives Product metadata and
the Platform directory, records SHA-256 checksums and the server version, and restarts only if the
server was previously running. It excludes external secret files, the dedicated `files/` directory,
and every external bucket. Runku does not provide application-file backup or additional durability
strategy. Coordinate a filesystem snapshot or use separately operated MinIO/S3, preserve the
matching Platform pepper/database access material, and test one recovery point as a unit.

`verify-backup` checks the manifest, every digest, archive path safety, Product identity, and
`pg_restore` catalog without changing the installation.

## Total-loss restore

Restore only into a configured installation whose PostgreSQL database, Product directory, and
Platform directory are empty. Supply the original Platform pepper and use the backup directory name
in the explicit confirmation:

```sh
export RUNKU_RESTORE_CONFIRM='restore:runku-backup-2026-09-02'
./runku-selfhost restore /mnt/encrypted/runku-backup-2026-09-02
unset RUNKU_RESTORE_CONFIRM
```

Restore verifies everything before mutation, verifies the pepper fingerprint, stages filesystem
state, restores PostgreSQL in one transaction, runs `runku doctor`, applies the idempotent schema
check, starts the server, and waits for readiness. Afterward, verify operator login, exact Project/
Environment IDs, Application Keys, Channel targets, a Query and idempotent Mutation, Realtime
reconnect, schedules, and logs across the archive/hot boundary.

Before reopening traffic, restore application file bytes from the separately coordinated
filesystem/S3 recovery point. The helper restores their metadata only. Missing or mismatched bytes
make the recovery incomplete and can surface as `FILE_STORAGE_NOT_FOUND` or
`FILE_STORAGE_CORRUPT`.

An older recovery point can resurrect subsequently revoked sessions or invitations. Reconcile and
revoke them before reopening public traffic.

## Upgrade

Obtain the new versioned image manifest digest and choose a new backup directory:

```sh
./runku-selfhost upgrade \
  ghcr.io/aldemi-tech/runku-server:X.Y.Z@sha256:<64-hex-manifest> \
  /mnt/encrypted/runku-pre-upgrade-X.Y.Z \
  kms://backup-policy/version-7
```

The command pulls and launches the exact image for a version check, validates configuration, creates
and verifies the pre-upgrade backup, stops serving, runs migrations, starts the new image, waits for
readiness, and only then atomically persists the image pin in `.env`. It never silently starts an old
binary after a later schema migration. If migration or readiness fails, preserve state and follow
the version's published rollback limit; a Channel rollback is not a database downgrade.

The first supported compact line establishes its upgrade floor. Each later release must publish and
exercise its exact previous-supported-version path before claiming it.

## Optional off-host log history

Use `RUNKU_DEPLOYMENT_PROFILE=s3-logs` when one server is sufficient but historical Operational
Logs must survive loss of its Product disk. Provide the bucket, region, unique prefix, and
`archive-aws-credentials` secret file. For a custom HTTPS object endpoint select
`s3-logs-compatible`; browser variants prefix either name with `browser-`.

This does not add a process or a journal: the server still archives in-process and queries Parquet
with embedded DuckDB. A sudden host loss can omit admitted hot rows that have not reached the
archive frontier. Select the HA log profile only when that recovery point is unacceptable.

## Optional HA Operational Logs

Set `RUNKU_DEPLOYMENT_PROFILE=ha-logs` (or `browser-ha-logs`) and provide:

- a TLS NATS JetStream endpoint with three persistent replicas and separate publisher/worker creds;
- an S3 bucket/prefix with versioning or equivalent immutability;
- an `archive-aws-credentials` shared-credentials secret file with the minimum bucket/prefix access;
- at least two archive workers, configured by `RUNKU_LOG_WORKER_REPLICAS`.

For a custom HTTPS S3-compatible endpoint select `ha-logs-s3-compatible` or
`browser-ha-logs-s3-compatible`. The helper starts the workers from the exact same image. It does not
install NATS or object storage, because their quorum, TLS, storage class, backups, and failure-domain
placement belong to the operator's infrastructure.

The local backup command does not back up the external archive or JetStream. Coordinate their
recovery points and follow [Operational Log storage](../../docs/operations/operational-logs.md).

## Stop and uninstall

```sh
./runku-selfhost uninstall keep-data
```

This removes containers while retaining PostgreSQL, Product data, Platform state, and secrets. To
remove authoritative data and the PostgreSQL volume, first take and verify a backup, then use the
exact installation confirmation:

```sh
export RUNKU_UNINSTALL_CONFIRM='delete:runku'
./runku-selfhost uninstall delete-data
unset RUNKU_UNINSTALL_CONFIRM
```

Secret files are retained even in delete-data mode so an accidental command cannot destroy the last
cryptographic recovery material. Destroy them separately only after the backup/retention decision.

## Security and limitations

- The Runku container is read-only, non-root, capability-free, resource-bounded, and uses a
  distroless release image. PostgreSQL has its own persistent volume and loopback-only host port.
- Pin images by version and digest; verify release checksums and provenance before promotion.
- Backups contain application data, key digests, pending bootstrap material, and audit state. Treat
  them as sensitive even though external peppers are excluded.
- Runku backup/restore excludes application file bytes and does not configure MinIO/S3 durability;
  this remains an explicit operator responsibility.
- The small profile has one active writer and one host failure domain. Use off-host backups or S3
  history according to the required RPO. HA logs protect admitted diagnostics; they do not make the
  Product data path active-active.
- Full Node requires its separately qualified Agent/isolation profile and is never enabled by this
  package.

Troubleshoot with `./runku-selfhost status`, bounded `docker compose logs`, `runku status --remote`,
`runku doctor` while stopped, and the [troubleshooting guide](../../docs/reference/troubleshooting.md).
