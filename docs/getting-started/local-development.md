# Local development

`runku dev` composes the complete local backend lifecycle. It initializes durable state, reconciles
Application Clients and dotenv configuration, builds/publishes source, starts the gateway and
workers, watches `runku/`, and serves the `workspace:local` target.

## Prerequisites

- Node.js 20.18.1 or newer for npm installation and application tooling;
- pnpm 10.18.1 when working with the included examples;
- Git, `make`, a POSIX shell, and the exact Rust toolchain only when building Runku from source.

## Install the CLI

### npm on every supported platform

```sh
npm install --global @runku/cli
runku --version
```

`@runku/cli` uses an exact-version optional dependency containing the native executable. An install
with `--omit=optional`, `--no-optional`, or an equivalent policy cannot run. The npm launcher itself
requires Node.js; the native executable does not.

| Operating system | Architecture | Rust target | npm native package | Archive |
|---|---|---|---|---|
| macOS | ARM64 | `aarch64-apple-darwin` | `@runku/cli-darwin-arm64` | `.tar.gz` |
| macOS | x86_64 | `x86_64-apple-darwin` | `@runku/cli-darwin-x64` | `.tar.gz` |
| Linux GNU/glibc | ARM64 | `aarch64-unknown-linux-gnu` | `@runku/cli-linux-arm64-gnu` | `.tar.gz` |
| Linux GNU/glibc | x86_64 | `x86_64-unknown-linux-gnu` | `@runku/cli-linux-x64-gnu` | `.tar.gz` |
| Windows | ARM64 | `aarch64-pc-windows-msvc` | `@runku/cli-win32-arm64-msvc` | `.zip` |
| Windows | x86_64 | `x86_64-pc-windows-msvc` | `@runku/cli-win32-x64-msvc` | `.zip` |

Linux musl, Windows 32-bit x86, and other combinations are not release targets. The release gate
compiles and executes `--version` and `--help` natively on every row; broader application behavior
continues to be covered by the ordinary repository CI.

Update and uninstall:

```sh
npm update --global @runku/cli
npm uninstall --global @runku/cli
```

### Direct GitHub archive

Open the GitHub Release matching the required version, download the archive whose target matches
the table, and download `SHA256SUMS`. Verify before extraction:

```sh
# Linux example
sha256sum --check SHA256SUMS --ignore-missing
tar -xzf runku-v0.4.3-x86_64-unknown-linux-gnu.tar.gz
install -m 0755 runku-v0.4.3-x86_64-unknown-linux-gnu/runku "$HOME/.local/bin/runku"
runku --version
```

```sh
# macOS example (verify the named file with the SHA256SUMS value)
shasum -a 256 runku-v0.4.3-aarch64-apple-darwin.tar.gz
tar -xzf runku-v0.4.3-aarch64-apple-darwin.tar.gz
mkdir -p "$HOME/.local/bin"
install -m 0755 runku-v0.4.3-aarch64-apple-darwin/runku "$HOME/.local/bin/runku"
runku --version
```

```powershell
# Windows x86_64 example
Get-FileHash .\runku-v0.4.3-x86_64-pc-windows-msvc.zip -Algorithm SHA256
Expand-Archive .\runku-v0.4.3-x86_64-pc-windows-msvc.zip -DestinationPath .\runku-cli
& .\runku-cli\runku-v0.4.3-x86_64-pc-windows-msvc\runku.exe --version
```

Compare the printed Windows/macOS hash with the exact filename entry in `SHA256SUMS`. Move the
executable to a user-controlled directory on `PATH`; do not overwrite a system-managed binary. On
Windows, keep the archive's `duckdb.dll` in the same directory as `runku.exe`.

### Source checkout

```sh
git clone https://github.com/aldemi-tech/runku.git
cd runku
make toolchain
pnpm install --frozen-lockfile
make install-cli
runku --version
```

For a source installation, also record `git rev-parse HEAD`; the version identifies a published
release only when installed from its immutable tag artifacts.

## Safe application roots

The project root must exist, be a regular non-symlinked directory, and must not be `/` or the user's
home. Declarative sources live in a regular `runku/` directory. Source symlinks and path escapes are
rejected so build inputs remain bounded and reproducible.

Run from the application root:

```sh
runku dev
```

Use `--root PATH` when invoking from elsewhere. On first start, defaults are Workspace `local` and
listener `127.0.0.1:3210`. To choose different values, initialize before first `dev`:

```sh
runku init --workspace integration --listen 127.0.0.1:3310
```

Initialization is idempotent for identical settings and conflicts for divergent settings. Do not
manually edit initialized identity/listener state.

Initialization also creates a private application-file token key. The first Product start opens
`.runku/file-storage.sqlite3` and `.runku/file-storage-objects/`; both are Environment-scoped state.
Local helpers do not back up those bytes. See [Application file storage](../functions/file-storage.md).

External Self-Hosted provisioning automation may bind a new root to an already allocated Product
scope by passing `--project-id prj_* --environment-id env_*` together. This is a provisioning
contract, not a normal developer setting: repeat the exact authorized IDs after an uncertain
response, and treat `LOCAL_STATE_CONFLICT` as evidence to reconcile instead of replacing `.runku/`.
Omitting both flags retains generated local IDs.

For a developer attaching source to an existing remote Environment, authenticate and verify scope
instead of using provisioner initialization directly:

```sh
runku login
runku link --project-id prj_... --environment-id env_...
```

`link` asks the current Management origin to authorize an exact status read before initializing
the directory. It then pins that origin in `.runku/management-link-v1.json`; later `--remote`
commands reject a different login origin for the same root. IDs alone remain non-secret and grant
no access. A denied link creates no local state.

## What starts

The local process opens only a local-development Environment and composes:

- SQLite repositories for documents/indexes/outbox/schedules, Releases/Channels, Workspaces/Dev
  Revisions, Application Clients, Cron, and operational logs;
- filesystem content-addressed artifacts;
- serving/development catalogs and exact target resolution;
- Safe V8 workers and local Full Node when a module selects it;
- HTTP Query/Mutation/Action endpoints and WebSocket Realtime;
- outbox dispatcher, scheduled worker, Cron materializer, source watcher, and catalog refresh;
- `/healthz` liveness and `/readyz` admission readiness.

SIGINT drops readiness, gracefully stops the listener and loops, closes stores, and exits. A project
lease prevents two daemons from sharing one local Environment.

## Durable local layout

The `.runku/` directory contains private authoritative state, including:

| Path | Purpose |
|---|---|
| `local-state-v1.json` | Project/Environment/Workspace identity and loopback listener |
| `management-link-v1.json` | Optional non-secret exact Management origin binding created by authenticated `runku link` |
| `identity-pepper-v1.key` | Private key-verification pepper; never log or copy publicly |
| `data.sqlite3` | Documents, indexes, outbox, scheduled invocations |
| `releases.sqlite3` | Release candidates, lifecycle, Channels |
| `development.sqlite3` | Workspaces, Dev Revisions, HEAD |
| `identity.sqlite3` | Application Clients and credential lifecycle |
| `cron.sqlite3` | Cron activations, cursors, leases |
| `observability.sqlite3` | Operational events and export checkpoint state |
| `artifacts/` | Immutable content-addressed runtime artifacts |
| `builds-v1/` | Immutable source-build outputs and Release-specific types |

Do not commit, edit, partially copy, or delete this directory while the process runs. Stop cleanly
and copy the entire directory for local backup.

## Build, publish, and watch

`dev` discovers `runku/`, produces a coherent source snapshot, builds an immutable package,
publishes artifact-first, and moves Workspace HEAD using compare-and-set. Source fingerprint covers
path and bytes, including additions/removals.

When a source edit fails syntax, policy, configuration, capability, contract, stability, or size
checks, the error is reported and the last valid Dev Revision remains active. Fix the source and
save again; do not repair Workspace pointers manually.

Use explicit build for CI/inspection:

```sh
runku dev --prepare
runku build
```

`--prepare` creates/reconciles local state and application configuration, then exits. `build`
updates `runku/_generated/api.d.ts` and returns immutable output paths in JSON.

## Browser origins and functional identity

Add every admitted browser origin exactly:

```sh
runku dev \
  --origin http://localhost:3000 \
  --origin http://127.0.0.1:5173 \
  --auth-config runku.auth.json
```

Origin applies to HTTP CORS and WebSocket handshake. The auth descriptor is relative to the
application root, contains public issuer/JWKS policy, and must not contain private keys.

## Application environment reconciliation

The CLI writes URL, explicit target, publishable key, and server-only secret according to a detected
frontend convention: Next, Vite, SvelteKit, Vue CLI, or canonical `RUNKU_*` fallback. The SDK itself
never reads environment variables or detects frameworks.

`RUNKU_SECRET_KEY` remains server-only and is not copied to a public prefix. Use
`--public-env-prefix PREFIX` for a custom builder and `--application-env RELATIVE` to select another
dotenv path.

If existing values belong to a different Environment, interactive use asks before replacement.
Non-interactive use fails closed unless `--replace-remote-credentials` is explicitly selected. URL,
target, and keys must always belong to the same Environment.

## Targets

```text
workspace:local
workspace:<name>
release:rel_<id>
channel:<name>
```

Workspace is live development, Release is immutable, and Channel is a mutable compatible routing
pointer. There is no implicit `latest`. A client may override target per call only with another
valid explicit target.

## Health and diagnosis

```sh
runku status
runku doctor
runku logs --limit 20
runku logs --request req_... --stream platform
```

`doctor` is read-only and validates store/path/artifact/Workspace/Cron consistency. A healthy result
does not prove backup freshness, capacity, or zero telemetry loss. See
[Troubleshooting](../reference/troubleshooting.md) for error classes.

## Stop, restart, and reset

Stop with `Ctrl-C` and wait for exit. Restart reuses the same identity, data, credentials, Releases,
and schedules. Deleting `.runku/` creates a new local Environment and permanently destroys the old
one; it is acceptable only when intentionally discarding all local state after a verified backup,
not as routine troubleshooting.

Next: [Build an application](application-tutorial.md) and read the [CLI reference](../reference/cli.md).
