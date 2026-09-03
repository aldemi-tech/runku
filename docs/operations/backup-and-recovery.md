# Backup and recovery

> **Application file exclusion:** Runku does not back up application file bytes or manage
> replication/versioning for their filesystem/S3 backend. The compact helper deliberately excludes
> `${RUNKU_DATA_DIRECTORY}/files` and external buckets even though it preserves file metadata under
> the Product root. A valid disaster-recovery plan must coordinate and independently verify that
> backend. See [Application file storage](../functions/file-storage.md#backup-restore-and-residual-responsibility).

Backup is an integrity protocol, not a file-copy feature. A recoverable Runku backup must capture
all authoritative state required to preserve Project/Environment identity, application data,
Release routing, credentials, schedules, and artifact references.

## State inventory

| State | Local implementation | Production-oriented implementation | Class |
|---|---|---|---|
| Project/Environment identity | `.runku/local-state-v1.json` | Management metadata store | Authoritative |
| Documents/indexes/outbox/schedules | `.runku/data.sqlite3` | PostgreSQL | Authoritative; indexes may be rebuildable only by explicit protocol |
| Releases/Channels | `.runku/releases.sqlite3` | PostgreSQL/release repository | Authoritative |
| Workspaces/Dev Revisions | `.runku/development.sqlite3` | PostgreSQL/development repository | Authoritative when development is retained |
| Application Clients/keys | `.runku/identity.sqlite3` + pepper | Identity repository + key protection | Authoritative and sensitive |
| Cron activation/cursors | `.runku/cron.sqlite3` | PostgreSQL | Authoritative for exact scheduling behavior |
| Operational logs/export checkpoints | `.runku/observability.sqlite3` + `.runku/observability-archive/` | hot Product store + filesystem/S3 Parquet; optional NATS journal | Operational evidence; retention policy applies |
| Platform operators/grants/sessions/invitations/audit | not part of local application state | PostgreSQL Platform Identity schema | Authoritative and sensitive |
| Platform credential/OIDC peppers | not part of local application state | Secret provider + coordinated recovery manifest | Authoritative cryptographic material |
| Artifacts | `.runku/artifacts/` and build store | S3-compatible object storage | Authoritative immutable content by digest |
| Application files | `.runku/file-storage-objects/` plus `.runku/file-storage.sqlite3` metadata | dedicated filesystem or S3-compatible prefix plus Product metadata | Authoritative application bytes; Runku backup excludes the byte backend |
| Process locks/caches/scratch | local ephemeral paths | Pod/host ephemeral storage | Reconstructible; never restore as authority |

## Consistent local backup

The implemented safe procedure is offline:

1. stop `runku dev` with SIGINT and wait for a clean exit;
2. confirm no process owns the project root;
3. copy the complete `.runku/` directory preserving private permissions and filenames;
4. do not omit SQLite WAL/SHM companions if they exist;
5. calculate a backup checksum and record timestamp, application root identity, Git commit/CLI
   version, Project ID, and Environment ID outside the backup;
6. store the copy using encryption and access controls appropriate for credential material;
7. restart the original Environment and run `runku doctor`.

Copying one SQLite file while the process is active is not a consistent Environment backup.

## Local restore

1. stop every process using the target application root;
2. preserve the current `.runku/` as incident evidence instead of overwriting it;
3. verify backup checksum, expected source version, and private permissions;
4. restore the complete directory into the same application root;
5. run `runku doctor` before `runku dev`;
6. start the process and verify:
   - Project/Environment IDs;
   - Workspace HEAD and Channel bindings;
   - representative Query and idempotent Mutation;
   - Application Key and JWT behavior;
   - Realtime initial value/reconnect;
   - pending schedule and Cron activation;
   - operational-log cursor expectations.

An older restore may move log cursors and application state backward. Downstream log exporters and
external-effect reconcilers must use event/operation identity, not assume monotonicity across a
disaster restore.

The local backup must include both `.runku/observability.sqlite3` and
`.runku/observability-archive/`. After restore, run `runku logs archive-status` locally (or
`runku logs archive-status --remote` through an attached server) and query a page that
crosses the archive/hot frontier. If history uses S3-compatible storage, preserve bucket versions,
manifests, and objects as one integrity unit; if HA is enabled, restore and reconcile the JetStream
consumer against the archive frontier before allowing hot-log retention. See
[Operational Log storage](operational-logs.md#backup-restore-and-disaster-recovery).

## Packaged compact Docker backup and restore

The release archive implements the coordinated offline procedure for its exact one-Environment
profile:

```sh
./runku-selfhost backup /encrypted/backups/runku-2026-09-02 kms://backup-policy/version-7
./runku-selfhost verify-backup /encrypted/backups/runku-2026-09-02
```

The helper quiesces `runku-server`, dumps Platform Identity PostgreSQL in custom format, archives
the complete Product and Platform directories, writes SHA-256 digests and the exact server version,
and restarts only a previously running server. Verification parses the PostgreSQL catalog and
rejects corrupt digests, unsafe archive paths, missing Product identity, unknown manifest versions,
and a missing encryption-policy reference without changing durable state.

External secret files are deliberately excluded. Preserve the Platform Identity pepper under
separate access control and the same recovery record; restore checks its SHA-256 fingerprint before
changing the empty destination. The database connection credential may be replaced for a new
PostgreSQL service, but the original Platform pepper and Product-root identity material are required.

Restore requires empty Product/Platform directories, an empty PostgreSQL database, and an exact
`restore:<backup-directory-name>` confirmation. It stages and validates filesystem data first,
restores PostgreSQL in one transaction, moves the staged state into place, runs `runku doctor`,
checks/migrates the schema, starts the server, and waits for readiness. Detailed commands and
post-restore application/session checks are in the
[Docker guide](../../deployments/docker/README.md#total-loss-restore).

For the optional HA log overlay, this backup covers local Product hot state and Platform Identity;
it does not copy the external S3 archive or JetStream. Protect and reconcile those systems using the
same recovery point and the log-specific restore order below.

## Corruption response

`doctor` corruption/inconsistency is fail-closed:

- stop writers;
- preserve the original files and diagnostics;
- do not run retention, manual SQL, partial initialization, or file deletion;
- compare backup/checksum/version evidence;
- restore a verified complete backup or perform a deliberately designed logical recovery;
- document accepted data/effect loss and rotate credentials if confidentiality is uncertain.

## Production backup contract

The packaged profile must coordinate PostgreSQL, Product state, and object storage. A backup
manifest must include format version, installation/Project/Environment identities, database
recovery position or snapshot identity, artifact roots, object checksums, configuration revision,
creation time, tool version, encryption metadata, and verification result.

Raw Operational Logs are not stored in Platform Identity PostgreSQL. Their backup roots are the
Product hot SQLite store, filesystem/S3 Parquet objects and manifests, and—in HA—the unconsumed NATS
journal window. Backing up only PostgreSQL does not back up Product logs.

When Platform Identity is enabled, the same recovery point also records the Platform Identity schema
version/checksum, both pepper secret versions, OIDC configuration revision, and whether a pending
bootstrap file exists. Restoring identity tables without their matching peppers leaves operators
unable to authenticate; restoring an older identity snapshot may resurrect later-revoked sessions
or invitations and therefore requires explicit credential reconciliation.

A lost bootstrap file is recoverable only while the database has no operator. The offline
`runku-server recover-bootstrap` operation revokes any pending bootstrap and writes a replacement;
it does not repair a missing pepper, an inconsistent partial restore, or lost owner access after
enrollment. Preserve the original database and audit evidence before using it.

Required operations are plan/dry-run, create, list, verify offline, restore into an empty
installation, and report compatibility. A restore must preserve IDs and keyrings only when the
operator explicitly chooses disaster recovery; a logical clone/import should create new
installation identity and should not copy credentials/secrets by default.

## Disaster-recovery acceptance campaign

No RPO/RTO may be published until this sequence is measured on a supported profile:

```text
clean install → create Projects/Environments → application workload
→ Release promotion + Realtime + pending schedules → backup
→ total loss of serving and storage → empty install → restore
→ integrity verification → existing clients/keys/targets/data/realtime/schedules work
```

Also test partial backup, missing artifact, wrong checksum, incompatible version, interrupted
restore, repeated restore, lost log exporter checkpoint, and key-protection recovery. Record actual
RPO/RTO and workload size; do not infer them from database vendor claims.
