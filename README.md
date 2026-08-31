# Runku

Runku is an open-source, self-hosted Backend-as-a-Service for transactional TypeScript Functions,
typed document data, realtime subscriptions, immutable releases, and durable scheduling.

Applications define a schema and `query`, `mutation`, and `action` Functions. Runku builds a
versioned artifact, generates client types, executes data operations transactionally, and keeps
subscriptions synchronized after commits.

## Core model

```text
Project
└── Environment             persistent data and configuration
    ├── Release             immutable code and contracts
    ├── Channel             mutable production routing pointer
    └── Workspace           mutable development routing pointer
```

Old and new Releases can operate over the same Environment while both contracts remain supported.
A Channel changes the default routing target without silently replacing a Release explicitly
selected by a client.

## Implemented capabilities

- declarative TypeScript schema and Function toolchain;
- typed Query, Mutation, Action, nested invocation, and generated client contracts;
- transactional documents and logical indexes over SQLite or PostgreSQL;
- realtime WebSocket subscriptions driven by a durable outbox;
- immutable Releases, Channels, Workspaces, compatibility checks, and rollback;
- `runAfter`, `runAt`, and Cron over durable scheduled invocations;
- publishable, service, development, guest, JWT, JWKS, and OIDC identity boundaries;
- Safe V8 execution and opt-in Full Node execution for Node.js and npm dependencies;
- operational logs and OTLP log export;
- local and remote collaborative development protocols.

The repository is pre-release. Local development is fully composed by the CLI. Distributed
storage, execution, and Firecracker adapters are present, but production installation profiles are
not certified until a versioned `runku-server`/`runku-agent` distribution is published.

## Requirements

- Rust from [`rust-toolchain.toml`](rust-toolchain.toml);
- Node.js 20.18.1 or newer;
- pnpm 10.18.1;
- Docker for PostgreSQL, S3/NATS conformance, or container-based Full Node tests;
- Linux with KVM, cgroup v2, namespaces, nftables, Firecracker, and jailer for the shared Full Node
  isolation profile.

## Build from source

```bash
make toolchain
pnpm install --frozen-lockfile
make install-cli
runku --version
```

The complete repository gate is:

```bash
make check
```

## Run an example

The Node Actions example exercises Safe V8, Full Node, an external npm dependency, cross-runtime
calls, typed data, persistence, and durable scheduling:

```bash
make install-cli
pnpm --dir examples/node-actions dev
```

The chat example adds Better Auth, publishable-key browser access, service-key server access,
rooms, messages, and realtime synchronization:

```bash
make install-cli
pnpm --dir examples/chat-next dev
```

## Repository layout

| Path | Purpose |
|---|---|
| [`crates/`](crates) | Rust engine, runtime, storage, protocol, gateway, and CLI crates |
| [`packages/`](packages) | `@runku/client` and `@runku/server` TypeScript packages |
| [`protocol/`](protocol) | Versioned protocol vectors and canonical formats |
| [`deployments/`](deployments) | Standalone, container, Kubernetes, and Firecracker integration assets |
| [`examples/`](examples) | Executable application and runtime examples |
| [`benchmarks/`](benchmarks) | Reproducible local regression baselines |
| [`docs/`](docs) | User, operator, security, reference, and internals documentation |

## Documentation

Start with the [documentation index](docs/README.md), then read:

- [Local development](docs/getting-started/local-development.md)
- [Platform model](docs/concepts/platform-model.md)
- [Functions and runtimes](docs/functions/functions-and-runtimes.md)
- [Application identity](docs/auth/application-identity.md)
- [Self-hosting](docs/self-hosting/overview.md)
- [Architecture](docs/internals/architecture.md)

## License

Licensed under the [Apache License 2.0](LICENSE).
