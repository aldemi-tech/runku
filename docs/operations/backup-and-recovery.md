# Backup and recovery

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
| Operational logs/export checkpoints | `.runku/observability.sqlite3` | Operational store | Operational evidence; retention policy applies |
| Artifacts | `.runku/artifacts/` and build store | S3-compatible object storage | Authoritative immutable content by digest |
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

## Corruption response

`doctor` corruption/inconsistency is fail-closed:

- stop writers;
- preserve the original files and diagnostics;
- do not run retention, manual SQL, partial initialization, or file deletion;
- compare backup/checksum/version evidence;
- restore a verified complete backup or perform a deliberately designed logical recovery;
- document accepted data/effect loss and rotate credentials if confidentiality is uncertain.

## Production backup contract

The future packaged profile must coordinate PostgreSQL and object storage. A backup manifest must
include format version, installation/Project/Environment identities, database recovery position or
snapshot identity, artifact roots, object checksums, configuration revision, creation time, tool
version, encryption metadata, and verification result.

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
