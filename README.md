# Runku

Runku is an open-source, self-hosted Backend-as-a-Service for transactional TypeScript Functions,
typed document data, realtime subscriptions, immutable releases, durable scheduling, and explicit
application identity.

An application defines its schema and `query`, `mutation`, and `action` Functions under `runku/`.
Runku builds an immutable artifact, generates client types, validates every call, executes data
changes transactionally, and refreshes affected subscriptions only after commit.

## Distribution status

Runku is currently pre-release.

| Capability | Current status |
|---|---|
| Versioned CLI for macOS, Linux GNU, and Windows on ARM64/x86_64 | Published from tagged releases through GitHub and npm |
| `@runku/client` and `@runku/server` TypeScript SDKs | Published together with the CLI version |
| Complete SQLite-backed local development process | Implemented and test-covered |
| Safe V8, local Full Node, HTTP, WebSocket, scheduling, identity, logs | Implemented and test-covered |
| PostgreSQL, S3-compatible artifacts, NATS execution queue | Implemented as adapters with conformance gates |
| Remote Workspace protocols and services | Implemented as libraries and integration gates |
| Platform operator bootstrap, sessions, scoped invitations, browser OIDC, and remote lifecycle/logs | Implemented in source with a full PostgreSQL + browser + Product lifecycle campaign |
| Compact `runku-server` binary/image for Linux ARM64/x86_64 | Published from tagged releases; composes one Product Environment and Safe V8 |
| Compact Docker standalone installation | Release-packaged with PostgreSQL, secret files, probes, backup/verify/restore/upgrade, and guarded removal |
| Embedded Operational Log history | SQLite hot tier, filesystem/S3 Parquet, DuckDB query, safe retention, and live stream implemented and test-covered |
| Optional HA Operational Log path | Same-package Compose overlay for externally operated NATS/S3 plus explicit failure-path acceptance |
| General-purpose distributed roles and `runku-agent` package | Not published yet |
| General distributed-role or Kubernetes package | Not published yet |
| Compact offline backup/restore and upgrade procedure | Packaged and tested; 0.3.0 establishes the first supported upgrade floor |
| Rolling multi-node upgrade/support window | Not published yet |

You can use the source checkout for local development and technical evaluation. The compact Docker
package is the supported bounded installation; do not represent the root-level dependency fixtures
or Kubernetes conformance manifests as a production installation. See
[Self-hosting](docs/self-hosting/overview.md) and the
[production-readiness checklist](docs/self-hosting/production-readiness.md) before planning a live
deployment.

## Choose a path

| Goal | Start here |
|---|---|
| Run Runku locally | [Local development](docs/getting-started/local-development.md) |
| Build a first application | [Application tutorial](docs/getting-started/application-tutorial.md) |
| Learn schema and Functions | [`@runku/server`](packages/server/README.md) |
| Call Runku from an application | [`@runku/client`](packages/client/README.md) |
| Add authentication | [Application identity](docs/auth/application-identity.md) |
| Bootstrap operator access | [Platform operator identity](docs/auth/platform-identity.md) |
| Operate a Product through `runku login` | [Authenticated remote lifecycle](docs/operations/remote-lifecycle.md) |
| Use Realtime and transactional data | [Data and Realtime](docs/data/data-and-realtime.md) |
| Publish, promote, or roll back code | [Releases and Workspaces](docs/development/releases-and-workspaces.md) |
| Evaluate self-hosting | [Self-hosting overview](docs/self-hosting/overview.md) |
| Install the compact self-hosted product | [Docker standalone installation](deployments/docker/README.md) |
| Operate or recover a local Environment | [Administration](docs/operations/administration.md) |
| Store and administer logs in standalone or HA | [Operational Log storage](docs/operations/operational-logs.md) |
| Contribute to Runku | [Contributing](CONTRIBUTING.md) |
| Work with an AI coding assistant | [Agent instructions](AGENTS.md) |

## Core model

```text
Project
└── Environment             persistent data, identity, configuration, and operational state
    ├── Release             immutable code, contracts, schema metadata, and artifacts
    ├── Channel             mutable routing pointer to one compatible Release
    └── Workspace           mutable development target whose revisions remain immutable
```

The separation is operational, not cosmetic:

- data belongs to an Environment, not to a deployment of code;
- a Release never changes after publication;
- a Channel promotion changes traffic without rebuilding the Release;
- an explicit Release target is never silently redirected to another Release;
- a request, subscription, nested call, Cron activation, or scheduled invocation pins exact code;
- old and new Releases may share one Environment while their data contracts remain compatible.

Read [Platform model](docs/concepts/platform-model.md) before designing deployment or lifecycle
automation.

## Requirements

For an npm installation and TypeScript application development:

- Node.js 20.18.1 or newer;
- npm, pnpm, or another compatible package manager.

For development from this repository:

- Git;
- `rustup` and the Rust version in [`rust-toolchain.toml`](rust-toolchain.toml);
- pnpm 10.18.1;
- `make` and a POSIX-compatible shell.

Docker is optional and is used by PostgreSQL, object-storage, execution-queue, and OCI conformance
gates. Linux/KVM is required only for the microVM Full Node isolation profile; it is not required
for Safe V8 or ordinary local development.

## Install the CLI

The shortest cross-platform installation is:

```sh
npm install --global @runku/cli
runku --version
```

The npm launcher selects an exact-version native package for macOS, Linux GNU, or Windows on ARM64
or x86_64. Do not disable optional dependencies. Update or remove it with:

```sh
npm update --global @runku/cli
npm uninstall --global @runku/cli
```

GitHub Releases also provide `.tar.gz` archives for macOS/Linux and `.zip` archives for Windows.
Direct archives do not require Node.js to launch the CLI. Verify the selected archive with the
release's `SHA256SUMS` before placing `runku` or `runku.exe` on `PATH`. Exact commands and the
platform table are in [Local development](docs/getting-started/local-development.md#install-the-cli).
The Windows archive also contains `duckdb.dll`; keep it beside `runku.exe`.

To install the current source checkout instead:

```sh
git clone https://github.com/aldemi-tech/runku.git
cd runku
make toolchain
pnpm install --frozen-lockfile
make install-cli
runku --version
```

`make toolchain` installs the exact compiler and components selected by the repository.
`make install-cli` installs the current checkout through Cargo. A source installation identifies
the working tree, not necessarily the bytes of a published release; record its Git commit.

To use an explicit installation root:

```sh
make install-cli CARGO_INSTALL_ROOT="$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
```

Repeat the installation after updating the checkout.

## Run the first backend

The Full Node example provides the shortest tested path through build, persistence, cross-runtime
calls, and scheduling:

```sh
pnpm --dir examples/node-actions dev
```

The command runs `runku dev`. On first start, the CLI:

1. detects the application's `runku/` source directory;
2. initializes a local Project, Environment, and `local` Workspace under `.runku/`;
3. reconciles development Application Clients and keys;
4. writes a local application environment file with URL, target, and permitted credentials;
5. builds and publishes an immutable Dev Revision;
6. starts HTTP, WebSocket, runtime, outbox, scheduler, Cron, and log services on loopback;
7. watches source files while keeping the last valid revision active after a failed build.

Stop it with `Ctrl-C`. The next start reopens the same data, identity, and releases. In another
terminal:

```sh
cd examples/node-actions
runku status
runku doctor
runku logs --limit 20
```

For a browser application with Better Auth, two users, rooms, and Realtime:

```sh
pnpm --dir examples/chat-next dev
```

See the [application tutorial](docs/getting-started/application-tutorial.md) for a source walkthrough
instead of copying an example blindly.

## Everyday local workflow

Run commands from the application root; `--root PATH` is available when automation runs elsewhere.

```sh
runku dev
runku build
runku status
runku doctor
runku logs --follow
```

- `dev` is the normal interactive workflow: initialize, build, publish, serve, and watch.
- `build` creates an immutable package and generated TypeScript contracts without serving it.
- `status` reads Release and Channel state without changing it.
- `doctor` validates local durable state and never repairs it automatically.
- `logs` emits JSON Lines with durable cursors and correlation filters.

`runku init` is only needed before the first `dev` when selecting a non-default Workspace or
listener:

```sh
runku init --workspace integration --listen 127.0.0.1:3310
```

The initialized identity and listener are durable. Runku does not silently replace divergent local
state. The complete command and exit-code reference is in [CLI reference](docs/reference/cli.md).

## Define and call a Function

Every Function declares authentication, external visibility, capabilities, argument contract,
return contract, and handler:

```ts
import { mutation, v } from "@runku/server"
import schema from "./schema.js"

export const create = mutation({
  auth: "user",
  visibility: "public",
  capabilities: ["auth:read", "db:read", "db:write"],
  args: v.object({ title: v.string({ minBytes: 1, maxBytes: 200 }) }),
  returns: v.documentId("notes"),
  async handler(ctx, input) {
    const principal = ctx.auth.principal
    if (principal === null || principal.kind !== "user") throw new Error("user required")
    const id = ctx.db.documentId(schema.tables.notes, ctx.invocation.invocationId)
    await ctx.db.insert(schema.tables.notes, id, {
      ownerId: principal.id,
      title: input.title,
      archived: false,
    })
    return id
  },
})
```

`runku build` generates `runku/_generated/api.d.ts`. The client uses that registry without generated
runtime code:

```ts
import { RunkuClient, typedClient, type CodeTarget } from "@runku/client"
import type { RunkuFunctions } from "./runku/_generated/api.js"

const runku = typedClient<RunkuFunctions>(new RunkuClient({
  baseUrl: process.env.RUNKU_URL!,
  target: process.env.RUNKU_TARGET! as CodeTarget,
  applicationKey: process.env.RUNKU_KEY!,
  getBearer: () => session.accessToken,
}))

const result = await runku.mutation("notes.create", { title: "Read the runbook" })
console.log(result.value)
```

Targets are explicit: `workspace:<ref>`, `release:<rel_...>`, or `channel:<name>`. There is no
`latest` target.

## Data and recovery warning

Local authoritative state lives under `.runku/` in multiple SQLite databases plus immutable
artifacts. Do not edit those files, copy individual databases while `runku dev` is running, or
delete the directory as a repair step.

For a consistent local backup:

1. stop `runku dev` with `Ctrl-C` and wait for a clean exit;
2. copy the complete `.runku/` directory while preserving private permissions;
3. record the Git commit/CLI version, timestamp, and backup checksum;
4. restore into the same application root;
5. run `runku doctor` before serving traffic.

See [Backup and recovery](docs/operations/backup-and-recovery.md) for scope, verification, and
limitations. The compact Docker package coordinates PostgreSQL and Product/Platform filesystem
state. External S3 and NATS recovery remains an installation-owned HA procedure and must not be
inferred from the local backup command.

## Repository quality gates

The hosted pull-request and `main` gate is:

```sh
make ci-check
```

It checks the pinned toolchain, Rust formatting, incomplete-marker policy, compilation of every
workspace target with all features, all three public JavaScript packages, and coordinated release
metadata. It does not link or execute Rust tests, start Runku, build examples, use Docker or a
database, or run benchmarks. This keeps routine automation deterministic and bounded.

The full maintainer gate remains:

```sh
make check
```

It additionally runs strict Clippy, all workspace tests, rustdoc, and executable examples. Run the
full gate, or the narrower behavioral gates affected by a change, before merging changes to runtime,
storage, protocol, concurrency, or security semantics. The gate split changes automation cost, not
the product's test contracts. See [Contributing](CONTRIBUTING.md) and the Makefile.

## Repository layout

| Path | Purpose |
|---|---|
| [`crates/`](crates) | Rust domain, storage, runtime, identity, protocol, gateway, and CLI components |
| [`packages/`](packages) | Public CLI launcher/native packages and TypeScript SDKs |
| [`distribution/`](distribution) | Files embedded in downloadable release archives |
| [`protocol/`](protocol) | Versioned language-independent persisted and wire vectors |
| [`deployments/`](deployments) | Product deployment contracts and explicitly bounded conformance assets |
| [`examples/`](examples) | Executable integration examples, not hidden provisioning paths |
| [`benchmarks/`](benchmarks) | Reproducible regression workloads and non-SLA baselines |
| [`docs/`](docs) | User, operator, security, compatibility, and maintainer knowledge base |
| [`AGENTS.md`](AGENTS.md) | Required context and rules for AI-assisted maintenance |

## Documentation

The [documentation portal](docs/README.md) provides task-oriented reading paths for application
developers, operators, security reviewers, maintainers, and AI assistants. Documentation status is
explicit: implemented behavior, conformance evidence, production-readiness requirements, and
unsupported future assumptions are not interchangeable.

## License

Licensed under the [Apache License 2.0](LICENSE).
