# Runku CLI guide

The Runku CLI manages an application's development and delivery workflow. It is separate from:

- `runku-server`, the long-running Self-Hosted service operated on infrastructure;
- `@runku/server`, the TypeScript declaration API used inside Functions;
- `@runku/client` and `/v1/query|mutation|action`, which application users call at runtime;
- the Management API, which controllers may use directly for broader administration.

Use this guide to choose a CLI workflow. Use the [CLI reference](../reference/cli.md) for every
flag, JSON output, exit code, and exact retry rule.

## Install

```sh
npm install --global @runku/cli
runku --version
runku --help
```

Supported released targets are macOS ARM64/x86-64, Linux GNU ARM64/x86-64, and Windows
ARM64/x86-64. npm optional dependencies install the matching native executable. Installation with
optional dependencies disabled cannot run the CLI.

## Choose the workflow

| Goal | Commands | State affected |
|---|---|---|
| start a local application backend | `init`, `dev`, `status`, `doctor`, `logs` | `.runku/` in the application root |
| validate/build application declarations | `dev --prepare`, `build` | generated types and immutable local build outputs |
| connect a directory to Self-Hosted | `login`, `link` | protected login store + non-secret root link |
| publish code to a remote Workspace | `publish --remote` | immutable Dev Revision + Workspace HEAD |
| freeze/promote/rollback code | `release --remote`, `promote --remote`, `rollback --remote` | Release/Channel state through Management API |
| inspect remote application state | `status --remote`, `logs --remote` | read-only remote queries |

The CLI does not configure host disks, S3, PostgreSQL, TLS, backups, or process resource limits.
Those belong to [Self-Hosted administration](../self-hosting/deployment-guide.md).

## Local development workflow

From the application root:

```sh
runku init
runku dev --origin http://localhost:3000
```

`init` creates one local Project/Environment/Workspace identity when absent. `dev` builds the
current `runku/` directory, publishes an immutable development revision, moves the local Workspace
only after success, serves the Product API on loopback, and watches for changes.

In another terminal:

```sh
runku status
runku doctor
runku logs --limit 20
```

Stop `dev` with `Ctrl-C` and wait for shutdown. Restarting reuses the same state. Do not delete
`.runku/` as troubleshooting; doing so destroys the local Environment.

See [Local development](../getting-started/local-development.md) for origins, JWT configuration,
application dotenv reconciliation, state/backup, and diagnostic behavior.

## Prepare without serving

Use this in CI or before an explicit build:

```sh
runku dev --prepare
runku build
```

`--prepare` initializes/reconciles local application state and exits. `build` validates schemas,
Functions, capabilities, runtime selection, and contracts; creates immutable outputs; and replaces
the `api` and `serverApi` runtime/declaration pairs below `runku/_generated`; each file is replaced
through an atomic rename. Do not edit generated files.

A build failure does not move a running Workspace from its last valid revision.

## Authenticate to Self-Hosted

```sh
runku login --url https://management.example.com
```

Login authenticates a Platform operator and stores the session in the CLI's protected user store.
It does not create an Application Key or functional user token.

From an application root:

```sh
runku link
```

Interactive `link` lists Environments authorized to the logged-in operator. Automation can pass
the exact Project and Environment IDs. The link records the trusted Management origin and selected
scope; IDs alone do not grant permission.

Use:

```sh
runku login status
runku status --remote
```

to confirm the active operator, Management origin, Project, Environment, and current Release/
Channel state before publishing.

## Publish a development revision

Build first and take paths from JSON stdout:

```sh
runku build
```

Then publish to the linked remote Environment:

```sh
runku publish --remote \
  --manifest /path/from-build/manifest \
  --artifact /path/from-build/artifact \
  --expected-head empty
```

Publishing stores immutable code and moves one Workspace HEAD under compare-and-set. If the
expected head is stale, read remote status and reconcile with the other publisher. Do not keep
retrying with invented state.

## Freeze and promote

```sh
runku release --remote --release rel_...
runku promote --remote \
  --channel preview \
  --release rel_... \
  --expected empty
runku status --remote
```

`release` validates/freezes an immutable Release. `promote` changes traffic policy; it does not
rebuild or mutate Release code. The Channel update requires its expected current state.

Before promotion, verify schema/index/Cron compatibility and application behavior against the
exact Release. A Channel move never migrates or rolls back persistent data automatically.

## Roll back traffic

Use the exact CLI syntax and current expected Channel state from the
[CLI reference](../reference/cli.md). Rollback changes the Channel/serving policy to an eligible
historical Release. It does not:

- undo documents written by newer code;
- remove scheduled work;
- reverse configuration or storage changes;
- downgrade the Self-Hosted server binary/database.

Keep stored-data compatibility for the full rollout and rollback window.

## Local versus remote commands

| Mode | Identity | Authority | Failure evidence |
|---|---|---|---|
| local (default) | local application root | local `.runku/` state | local status/doctor/logs |
| `--remote` | CLI operator session + linked root | Management capabilities in exact scope | HTTP status/code, operation ID, remote state/logs |

Do not assume a command becomes remote merely because `login` exists. Use `--remote` only where
the command explicitly supports it and confirm the linked scope.

## Exit codes and automation

CLI stdout is reserved for the command's documented human/JSON result; stderr carries bounded
diagnostics. Automation must use the command's stable exit-code class and parse JSON only where the
reference promises JSON.

Broad interpretation:

- success: continue only after checking the returned resource/revision;
- usage/validation error: fix input; identical retry is not useful;
- authentication/authorization error: renew or correct scope/grants;
- conflict: read current state and reconcile;
- temporary unavailable/busy: bounded backoff only when operation semantics allow;
- uncertain mutation: query operation/current state using the same operation identity;
- corruption/inconsistency: stop mutation, preserve evidence, follow recovery procedure.

The [CLI reference](../reference/cli.md) is authoritative for each command's exact exit codes.

## Security rules

- never pass bearer/secret material in command arguments when a file/session mechanism exists;
- keep the CLI login store private and revoke lost sessions;
- do not commit `.runku/`, generated dotenv secrets, manifests containing private paths, or logs;
- verify the Self-Hosted Management URL/TLS origin before login/link;
- use separate operator grants for developers and CI publishers;
- use CAS/expected values instead of force-updating remote state;
- record version, origin, Project, Environment, Workspace, operation ID, and Release ID in CI
  evidence without recording tokens or application data.

## What the CLI deliberately does not do

- It does not act as an end-user data client; use the [TypeScript client](../reference/typescript-client.md)
  or [public HTTP API](../reference/public-api.md).
- It does not define Function parameters or schema types; use the
  [Function API reference](../reference/function-api.md).
- It does not replace deployment configuration or backup tooling; use the
  [operator handbook](../operations/operator-handbook.md).
- It does not make unsupported Kubernetes/separated-role assets a released installation profile.

For an end-to-end application delivery procedure, continue with
[Application development workflow](../functions/development-workflow.md).
