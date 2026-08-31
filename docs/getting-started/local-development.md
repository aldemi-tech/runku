# Local development

`runku dev` owns the local backend lifecycle. It discovers the application root, prepares local
credentials, builds the current source, starts SQLite-backed services, watches source files, and
serves the `workspace:local` target.

## Install the CLI from source

```bash
git clone https://github.com/aldemi-tech/runku.git
cd runku
make toolchain
pnpm install --frozen-lockfile
make install-cli
```

## Start an application

From an application containing a `runku/` directory:

```bash
runku dev
```

`--root PATH` is optional. Without it, the current directory is used. `runku dev --prepare` creates
or updates local application configuration without keeping the server running.

The CLI writes generated state under `.runku/`. Application dotenv files receive only the URL,
target, and credentials allowed for their detected framework. Canonical `RUNKU_*` names can be used
with framework-independent build systems.

If an existing dotenv file targets a remote Environment, the CLI asks before replacing it with
local credentials. Non-interactive execution must make that decision explicitly.

## Local storage

Local development uses SQLite because it is zero-configuration and preserves Runku's logical
storage contract. PostgreSQL remains the authoritative production adapter and the only adapter used
for concurrency and distributed-operation claims.

SQLite and PostgreSQL differ physically but expose the same typed values, transactions, indexes,
outbox, scheduling, and Environment scoping.

## Targets

Clients always select an explicit target:

```text
workspace:local
workspace:<name>
release:<release-id>
channel:<channel-name>
```

There is no implicit `latest` target. A preview deployment may select a Workspace for live shared
development or a Release for an immutable test candidate.
