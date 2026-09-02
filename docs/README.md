# Runku documentation

This documentation is the operational knowledge base for Runku Self-Hosted. It is organized by
tasks and decisions so an application developer, operator, maintainer, or AI assistant can locate
the authoritative context without reading implementation history.

## Documentation status

Every document must distinguish these states:

| State | Meaning |
|---|---|
| Implemented | Code and executable tests cover the stated behavior |
| Conformance | A component contract was verified in a bounded test environment |
| Production requirement | Required before a deployment profile can be called supported |
| Pre-release limitation | Deliberately not promised by the current source line |

Conformance is not installation support. A requirement is not an implemented feature. A benchmark
is not an SLO. The current product boundary is summarized in the
[root README](../README.md#distribution-status).

## Application developer path

Read in this order:

1. [Local development](getting-started/local-development.md): install the CLI, understand local
   state, start/stop the process, and diagnose startup.
2. [Application tutorial](getting-started/application-tutorial.md): build a schema, Query, Mutation,
   Action, typed client, Realtime subscription, and scheduled operation.
3. [Platform model](concepts/platform-model.md): Project, Environment, Release, Channel, Workspace,
   identity, and code pinning.
4. [Functions and runtimes](functions/functions-and-runtimes.md): declarations, capabilities, Safe
   V8, Full Node, nested calls, HTTPS, scheduling, and failure semantics.
5. [Data and Realtime](data/data-and-realtime.md): values, documents, indexes, transactions, OCC,
   outbox, subscriptions, and resync.
6. [Application identity](auth/application-identity.md): Application Clients, key types, user/service
   identity, JWT/OIDC, browser/server separation, and rotation.
7. [Platform operator identity](auth/platform-identity.md): first-owner bootstrap, `runku login`,
   scoped invitations, sessions, OIDC, PostgreSQL state, and recovery.
8. [Authenticated remote lifecycle](operations/remote-lifecycle.md): use one operator session for
   publish, Release validation, promotion, rollback, historical logs, and streaming logs.
9. [`@runku/server`](../packages/server/README.md) and
   [`@runku/client`](../packages/client/README.md): exact TypeScript APIs and examples.

## Release and CI/CD path

- [Publishing a distribution](maintainers/releases.md): coordinated version, six native CLI
  targets, npm trusted publishing, GitHub assets, fast gates, failure recovery, and verification.
- [Releases and Workspaces](development/releases-and-workspaces.md): development revisions,
  immutable packages, compatibility, promotion, rollback, remote sync, and scheduled-code pinning.
- [CLI reference](reference/cli.md): exact commands, outputs, exit codes, automation expectations,
  and safe retry rules.
- [Compatibility](reference/compatibility.md): support boundaries for CLI, SDK, protocol, manifests,
  storage, runtime, and upgrade behavior.
- [Troubleshooting](reference/troubleshooting.md): symptom-driven diagnosis and evidence collection.

## Operator path

Read the support boundary before designing infrastructure:

1. [Self-hosting overview](self-hosting/overview.md): topology, roles, dependencies, runtime profiles,
   configuration domains, and current packaging state.
2. [Production readiness](self-hosting/production-readiness.md): auditable go/no-go checklist for
   installation, administration, HA, security, recovery, upgrades, and release artifacts.
3. [Docker standalone installation](../deployments/docker/README.md): exact compact-profile install,
   TLS boundary, secrets, backup, restore, upgrade, and removal procedure.
4. [Administration](operations/administration.md): daily checks, lifecycle operations, credentials,
   retention, capacity, maintenance windows, and incident workflow.
5. [Authenticated remote lifecycle](operations/remote-lifecycle.md): exact server/CLI workflow,
   authorization, failures, rollback, logs, and executable acceptance evidence.
6. [Operational Log storage and administration](operations/operational-logs.md): choose standalone
   or HA; configure filesystem/S3/NATS; query, stream, retain, recover, size, and upgrade it.
7. [Observability](operations/observability.md): signal catalog, correlation, privacy, dashboards,
   alerts, capacity indicators, and OTLP behavior.
8. [Backup and recovery](operations/backup-and-recovery.md): local and packaged compact procedures,
   inventory, restore verification, disaster-recovery acceptance, and current limitations.
9. [Security model](security/security-model.md): boundaries, threats, deployment controls, secrets,
   incident response, and residual risk.
10. [Platform operator identity](auth/platform-identity.md): configure and operate management trust.
11. [Deployment assets](../deployments/README.md): standalone, Docker, and Kubernetes profile scope.

## Maintainer and AI-assistant path

AI assistants must begin with [`AGENTS.md`](../AGENTS.md). Human maintainers should use the same
reading order because it records the product invariants and definition of done.

- [System architecture](internals/architecture.md): component and trust boundaries, serving/data/
  runtime/management paths, consistency, scaling, and failure containment.
- [Repository map](internals/repository-map.md): crate ownership, dependency direction, tests,
  generated artifacts, and where to implement a change.
- [Evolving Runku](development/evolving-runku.md): contract classification, versioning, migrations,
  rollout, rollback, security review, and evidence requirements.
- [Documentation maintenance](maintainers/documentation.md): required reading maps, source
  precedence, update matrix, link/example checks, and review rubric.
- [Public protocol vectors](../protocol/README.md): exact persisted and wire compatibility fixtures.
- [Contributing](../CONTRIBUTING.md): toolchain, gates, pull-request contract, and review workflow.

## Examples and evidence

| Resource | What it proves | What it does not prove |
|---|---|---|
| [Realtime chat](../examples/chat-next/README.md) | Browser/server key separation, JWT/OIDC, data, two users, Realtime, restart | General production capacity or IdP coverage |
| [Full Node Actions](../examples/node-actions/README.md) | Node built-ins/npm, Safe↔Node, typed bytes, scheduling, restart | Shared-host production isolation |
| [Storage benchmark](../benchmarks/storage/README.md) | Repeatable local PostgreSQL index baseline | Production SLO or remote database sizing |
| [Runtime benchmark](../benchmarks/runtime/README.md) | Repeatable invocation regression baseline | End-user latency or fleet throughput |
| [Artifact benchmark](../benchmarks/artifacts/README.md) | Local hashing/read/write regression | S3 availability or network performance |
| [Release benchmark](../benchmarks/releases/README.md) | Repository operation regression | Multi-node management capacity |

## Terminology and writing rules

- Capitalized Project, Environment, Release, Channel, Workspace, Function, Query, Mutation, and
  Action refer to Runku domain concepts.
- “Local” means one application root managed by `runku dev`; it does not mean insecure defaults may
  be copied to a networked deployment.
- “Production” is used only for an explicitly supported distribution profile with release artifacts,
  limits, runbooks, upgrade/restore evidence, and a compatibility window.
- Technology names describe an implementation profile. Product manifests and generic operational
  procedures use Runku roles such as API, background, management, and Full Node Agent.
- Commands and API examples must match the current strict parser and exported package surface.
- Unknown, planned, or unsupported behavior must be stated directly; never fill gaps with invented
  configuration.
