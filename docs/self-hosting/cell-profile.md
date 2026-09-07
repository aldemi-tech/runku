# Multi-Environment cell profile

Runku 0.5 adds a cell-member composition to the same `runku-server` binary. One operating-system
process can open and keep warm several independent Product Environments, while a `dedicated`
member uses the identical code path with exactly one Environment. This changes process packing;
it does not merge tenant state or weaken Product authorization.

## When to use each mode

| Mode | Environments per process | Intended use |
|---|---:|---|
| compact | one through `RUNKU_PRODUCT_ROOT` | existing standalone Docker installation |
| `shared` cell | 1–64 configured Environments | economical fleet packing for small/medium tenants |
| `dedicated` cell | exactly one | stronger resource and failure-domain isolation |

The ceiling of 64 is a manifest safety bound, not a recommended capacity. Measure resident memory,
runtime concurrency, open files, SQLite contention, background work, and tail latency to choose a
lower placement limit. Every configured Environment is opened at startup, so one member can have
`N` Environments preheated without `N` containers.

## Configuration

Set `RUNKU_CELL_CONFIG` to an absolute path containing a regular, non-symlinked JSON file of at
most 1 MiB. It is mutually exclusive with `RUNKU_PRODUCT_ROOT` and with the singleton Product
variables `RUNKU_PLATFORM_DATABASE_URL[_FILE]`, `RUNKU_PRODUCT_ALLOWED_ORIGINS`, and
`RUNKU_PRODUCT_AUTH_CONFIG`.

```json
{
  "version": 1,
  "mode": "shared",
  "memberId": "member_zone-a-01",
  "environments": [
    {
      "root": "/var/lib/runku/environments/env_alpha",
      "hosts": ["alpha.apps.example.com"],
      "platformDatabaseUrlFile": "/run/secrets/env-alpha-database-url",
      "allowedOrigins": ["https://alpha.example.com"],
      "authConfig": "config/auth.json"
    },
    {
      "root": "/var/lib/runku/environments/env_beta",
      "hosts": ["beta.apps.example.com"],
      "platformDatabaseUrlFile": null,
      "allowedOrigins": [],
      "authConfig": null
    }
  ]
}
```

Each root must already be initialized and must carry its exact Project/Environment identity.
Roots and canonical lowercase hosts must be globally unique inside the member. Each optional
Product PostgreSQL secret must name a database different from Platform Identity and from every
other Environment in the member. `authConfig` is relative to its Product root.

A cell always requires the shared application listener:

```sh
export RUNKU_CELL_CONFIG=/etc/runku/cell.json
export RUNKU_APPLICATION_LISTEN=0.0.0.0:3210
export RUNKU_APPLICATION_TLS_TERMINATED=true
runku-server check
runku-server migrate
runku-server serve
```

`RUNKU_APPLICATION_TLS_TERMINATED=true` is an assertion that a trusted ingress already owns public
HTTPS. The Runku listener itself is HTTP on the provider-private network and must not be exposed
directly to an untrusted network. Changing assignments or hosts requires a validated manifest and
a graceful member restart; 0.5 does not hot-reload the file.

## Routing and isolation

The listener uses exactly one canonical HTTP `Host` header to select an Environment candidate. It
does not trust `Forwarded` or `X-Forwarded-Host`. Missing, duplicate, malformed, or unknown hosts
fail closed. Host selection is only placement: the selected Product gateway still validates the
Environment's own Application Client/key, functional identity, Function visibility/scopes, CORS,
Release target, and data authority. A key from Environment A is rejected by Environment B.

Management routes do not use Host as tenant authority. After Platform operator authentication and
authorization, the exact Project/Environment IDs in the Management path select the matching
Product adapter. Unknown or duplicate scopes fail closed.

## Fleet placement and warm affinity

The manifest is a member-local desired assignment, not a global scheduler. A provider can run many
identical members on EC2, Kubernetes/AKS/GKE, Cloud Run, or Fly.io and route a host to a member that
already lists that Environment. This preserves warm runtimes and caches. Ordinary round-robin is
appropriate only among members that are all valid serving candidates for that exact Environment.

Runku 0.5 does **not** make one Environment active-active. Release, identity, serving,
configuration, Cron, file metadata, and hot operational-log authorities still include
Environment-local SQLite state, and background work is not fenced across members. Therefore an
Environment has one active cell member. A controller may replace that member or keep an unmounted
standby, but it must fence the old writer before mounting/opening the same root on another process.
Shared mode improves packing and restart blast-radius economics; it is not a multi-writer claim.

Safe provider routing follows this order:

1. authenticate/resolve the requested Environment in the control plane;
2. read its current fenced active-member assignment;
3. prefer that warm member and verify its readiness/assignment revision;
4. on failure, fence the old member, attach/recover persistent state, start the replacement, and
   only then publish the new route;
5. never spray writes across members merely because they report healthy.

True same-Environment active-active requires moving or coordinating all remaining SQLite
authorities, distributed scheduler/Cron leases, Realtime fan-out/resync, revision propagation, and
graceful drain. Those remain separate distributed-system acceptance work.

## State, backup, and failure domains

Every Environment retains its own Product root and optional logical PostgreSQL database. Do not
place authoritative roots on ephemeral container filesystems. Back up each Product root, its
Environment database when configured, Platform Identity, and external byte stores at one declared
recovery point. The existing compact backup helper is not a multi-Environment cell backup tool.

A process crash temporarily affects every Environment assigned to that member. Limit placement by
measured capacity and desired blast radius, distribute members across zones, and keep the
controller's assignment/recovery records outside the cell. `shared` and `dedicated` are deployment
choices; moving an Environment between them does not change its Product semantics.
