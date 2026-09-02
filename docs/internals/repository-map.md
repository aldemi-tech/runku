# Repository map

Runku is a Rust workspace with public TypeScript packages, language-independent vectors, executable
examples, deployment contracts, and regression benchmarks. Dependency direction flows from pure
domain contracts toward adapters and process composition.

## Core and persistence

| Area | Ownership |
|---|---|
| `runku-core` | Typed IDs, Environment purpose/protection, targets, Channel/Workspace references |
| `runku-value` | Logical value algebra, Stored Value v1, ordered Index Key v1 |
| `runku-contracts` | Function argument/result contract representation and enforcement |
| `runku-data` | Storage-independent snapshots, commits, indexes, outbox, schedules |
| `runku-data-conformance` | Adapter-neutral correctness suite |
| `runku-data-sqlite` | Local implementation |
| `runku-data-postgres` | PostgreSQL implementation and distributed-oriented behavior |
| `runku-schema` | Schema/index catalog and maintenance rules |

Pure crates must not depend on SQL, HTTP, runtime, filesystem, or deployment frameworks.

## Releases, development, and compatibility

| Area | Ownership |
|---|---|
| `runku-releases` | Canonical manifests, artifacts, runtime descriptors, lifecycle values |
| `runku-release-repository` | Durable Release/Channel repository and adapters |
| `runku-artifact-s3` | S3-compatible immutable artifact store |
| `runku-compatibility` | Contract/schema/runtime compatibility reports |
| `runku-development` | Workspace/Dev Revision repository and serving catalog |
| `runku-development-access` | Development credential lifecycle |
| `runku-development-service` | Remote administrative HTTP service |
| `runku-development-client` | Strict remote client, retries, reconciliation |
| `runku-build` | Static source discovery, parsing, contracts, codegen, package/OCI output |

Artifact publication is artifact-first, pointer-last. Mutable routing uses compare-and-set.

## Execution and serving

| Area | Ownership |
|---|---|
| `runku-runtime` | Safe V8 supervisor/workers, Platform Ops, HTTPS broker |
| `runku-node-runtime` | Local/dedicated/container/microVM Node execution adapters |
| `runku-execution` | Query, Mutation, Action, nested invocation coordinators |
| `runku-execution-queue` | NATS-based remote Full Node queue/Agent contracts |
| `runku-realtime` | Outbox dispatch, dependency matching, subscription lifecycle |
| `runku-cron` | Cron materialization, activation, cursor/lease behavior |
| `runku-gateway` | HTTP/WS boundary, identity, routing, admission, envelopes |
| `runku-protocol` | Wire/admin protocol types and canonical conversion |

Gateway tests are the preferred vertical evidence that multiple layers compose correctly.

## Identity and observability

| Area | Ownership |
|---|---|
| `runku-identity` | Application/functional identity policy, keys, guest/JWT validation |
| `runku-identity-provider` | OIDC/JWKS provider integration and bounded cache behavior |
| `runku-identity-repository` | Durable clients/keyrings and adapter conformance |
| `runku-platform-identity` | Operator bootstrap, scoped grants, invitations, sessions, PostgreSQL schema/audit |
| `runku-management-service` | Versioned authenticated Management HTTP boundary and OIDC adapter |
| `runku-observability` | Operational events; SQLite hot tier; filesystem/S3 Parquet; embedded DuckDB query; NATS journal; safe retention |
| `runku-otel` | OTLP mapping, batching, retry, durable checkpoint |

Identity policy belongs before Function execution; observability failure must not corrupt or block
authoritative application state.

## Process and public packages

| Area | Ownership |
|---|---|
| `runku-local` | Local layout, stores, publish/lifecycle, daemon composition, doctor/logs/keys |
| `runku-cli` | Strict parser, stable help/errors/exits, command orchestration |
| `runku-server` | Source composition for Platform Identity plus one optional authenticated Product Environment; not yet the distributed package |
| `packages/server` | Declarative TypeScript SDK consumed statically by the builder |
| `packages/client` | Public HTTP/Realtime client and generated-registry typed view |
| `protocol/` | Compatibility fixtures; existing vectors are immutable contracts |
| `examples/` | End-to-end use through public surfaces |

The Server SDK helper objects are not runtime authority; Rust build/runtime validation remains
authoritative. The client contains no framework configuration discovery.

## Where to add tests

- pure rule/parser: owning crate unit/property tests;
- storage adapter: common conformance plus adapter-specific failure tests;
- wire/persisted change: protocol vector plus decode/encode/rejection tests;
- Function surface: Server SDK type conformance + builder + runtime/gateway vertical;
- client behavior: type conformance + HTTP/WS unit tests + example vertical;
- CLI/process: `runku-local`/`runku-cli` process and filesystem tests;
- deployment: conformance harness with explicit environment and failure scope.

## Generated and durable paths

- `target/`, package `dist/`: generated build output;
- `runku/_generated/api.d.ts`: generated current application contract;
- `.runku/builds-v1/`: immutable local packages;
- `.runku/`: local authoritative Environment state; never fixture/scaffold material;
- `.runku/observability.sqlite3`: hot cursor-ordered Operational Log tier;
- `.runku/observability-archive/`: immutable Parquet segments and strict commit manifests;
- protocol vector JSON/binary fixtures: committed compatibility authority.

Do not edit generated output to change behavior. Change source/generator and prove reproduction.
