# Environment-scoped Function platform PostgreSQL

Runku Server can place the transactional Function data path for its attached Product Environment
in PostgreSQL. This is an optional Self-Hosted profile for operators that need PostgreSQL durability,
concurrency, and database-level isolation while retaining the same Query, Mutation, Realtime,
outbox, and scheduling semantics as local SQLite.

## Exact boundary

`RUNKU_PLATFORM_DATABASE_URL` selects PostgreSQL for Function documents, indexes, idempotent
Mutation operations/results, the transactional outbox, and scheduled invocations. It does not move every
Product repository out of `RUNKU_PRODUCT_ROOT`: Project/Environment identity, Releases, Channels,
Workspaces, Application Clients/keys, Cron metadata, artifacts, file metadata, and the Operational
Log hot tier still use their documented Product-root repositories. Application file bytes and
optional Parquet history retain their independently selected filesystem or S3-compatible backend.

This database is not Platform Identity. `RUNKU_IDENTITY_DATABASE_URL` owns operators, grants,
sessions, invitations, and security audit. Use different databases and credentials even when both
databases are hosted by one PostgreSQL cluster.

In short: the **Identity database** answers “who may operate this installation?”, while the
**Function platform database** holds the documents and transactional work used by application
Functions. `RUNKU_PRODUCT_ROOT` is a filesystem directory for the rest of one Product Environment;
it is not a PostgreSQL URL and it does not merge these databases.

## Configuration

Initialize the Product root with its final exact Project/Environment scope, then provide the
Function platform database through one secret source:

```sh
export RUNKU_PRODUCT_ROOT=/var/lib/runku/product
export RUNKU_PLATFORM_DATABASE_URL_FILE=/run/secrets/platform-database-url

runku-server check
runku-server migrate
runku-server serve
```

The direct `RUNKU_PLATFORM_DATABASE_URL` alternative is supported for secret-injection systems that
do not mount files. `_FILE` does not name a third database and does not change the URL format: its
value is the absolute path of a file whose single line contains the same PostgreSQL URL. Never
configure both forms. The `_FILE` path must identify an absolute, regular,
non-symlinked file of at most 64 KiB containing exactly one non-empty line. The URL must use
`postgres://` or `postgresql://`, include a host, and name one database in its path. `check`
validates shape without connecting; `migrate` and `serve` connect and apply checksum-protected
forward migrations.

Version 0.4.4 accepts `RUNKU_PRODUCT_DATABASE_URL`/`_FILE` as deprecated aliases and
`RUNKU_DATABASE_URL`/`_FILE` as deprecated Identity aliases. They exist only for transition. A
canonical and legacy name configured together for the same role fails closed, even if both contain
the same URL.

Use one database and one least-privilege login role per Environment. Revoke `CONNECT` from `PUBLIC`,
grant it only to that Environment's role, restrict the role to its own database, and constrain
network access to the owning Runku workload. Do not reuse the Platform Identity credential.

On first scoped connection Runku atomically writes a singleton binding containing the exact Project
and Environment IDs. The database rejects a later or concurrent attempt to attach another scope.
Every scoped store operation also checks the process binding before issuing SQL. These guards detect
misconfiguration; PostgreSQL roles, database grants, and network policy remain the isolation
boundary.

## Readiness and failure handling

`/health/ready` checks both Platform Identity and the configured Function platform PostgreSQL
store. A Function platform database outage therefore removes Management readiness and prevents a
server from advertising safe admission. It never falls back to SQLite.

Stable startup failures are:

The `SERVER_PRODUCT_DATABASE_*` prefix predates the clearer configuration names and remains stable
in 0.4.4 so monitoring and automation do not break.

| Code | Meaning | Safe response |
|---|---|---|
| `SERVER_PRODUCT_DATABASE_URL_INVALID` | unsupported or malformed URL scheme | correct secret configuration |
| `SERVER_PRODUCT_DATABASE_WITHOUT_PRODUCT_ROOT` | Function platform database configured without an attached Environment | configure the exact initialized root |
| `SERVER_PRODUCT_DATABASE_NOT_ISOLATED` | Function platform and Identity URLs target the same database | provision a separate database and credential |
| `SERVER_PRODUCT_DATABASE_SCOPE_CONFLICT` | database is bound to or contains rows for another scope | stop; preserve evidence and select the correct empty/restored database |
| `SERVER_PRODUCT_DATABASE_MIGRATION_FAILED` | schema/checksum migration failed | stop writers; inspect version and migration evidence |
| `SERVER_PRODUCT_DATABASE_UNAVAILABLE` | connection, TLS, credentials, capacity, or dependency failed | restore the dependency, then repeat the idempotent startup/migration step |

An uncertain Mutation commit retains its existing operation ID and must be reconciled by replaying
the same logical request. Do not generate a new operation ID or repair documents/outbox/schedules
independently.

## Backup and restore

A recoverable Environment now requires one coordinated recovery point for:

1. the Function platform PostgreSQL database;
2. the complete `RUNKU_PRODUCT_ROOT` while writers are quiesced;
3. application-file bytes and any external Parquet archive;
4. the separate Platform Identity database and its pepper when operator recovery is in scope.

Restore the Function platform database and Product root for the same Project/Environment together.
Restore into an empty database, run `runku-server migrate`, and require readiness plus a representative
authenticated Query, idempotent Mutation replay, Realtime reconnect, pending schedule, lifecycle,
key, file, and log check before reopening traffic.

The compact Docker package continues to use SQLite for Function transactional data and its existing
package-level backup contract. Supplying an external Function platform database outside that
package makes the operator responsible for the coordinated database backup and restore described
here.

## Evidence

The explicit source campaign starts the pinned PostgreSQL 16 fixture, races two different scopes
against one empty migrated database, proves that exactly one binding wins, executes a real
authenticated Mutation through `LocalProcess`, reads the committed document from PostgreSQL,
reopens the winning scope, and rejects the losing scope:

```sh
make product-postgres-check
```

This is component and composition conformance. It does not establish a database vendor SLA,
multi-node serving window, backup RPO/RTO, or general distributed deployment support.
