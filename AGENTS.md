# Instructions for AI assistants and automated contributors

This repository is the public, open-source Runku Self-Hosted product. Treat the repository as a
product that operators and application teams must be able to understand, run, diagnose, secure,
and evolve without access to private planning documents.

## Mandatory reading before changing anything

Read these files completely for every task:

1. [`README.md`](README.md) for the current distribution boundary and supported user paths.
2. [`docs/README.md`](docs/README.md) for the documentation map and status vocabulary.
3. [`docs/concepts/platform-model.md`](docs/concepts/platform-model.md) for domain invariants.
4. [`docs/internals/architecture.md`](docs/internals/architecture.md) for component boundaries.
5. [`CONTRIBUTING.md`](CONTRIBUTING.md) for gates and compatibility requirements.

Then read the task-specific sources:

| Change area | Required reading |
|---|---|
| CLI/local development | `docs/getting-started/local-development.md`, `docs/reference/cli.md`, `crates/runku-cli/src/lib.rs` |
| Function SDK/build | `docs/functions/functions-and-runtimes.md`, `packages/server/README.md`, `packages/server/src/index.ts`, `crates/runku-build/` |
| Client/protocol/realtime | `packages/client/README.md`, `docs/data/data-and-realtime.md`, `protocol/README.md`, protocol vectors and gateway tests |
| Data/schema/indexes | `docs/data/data-and-realtime.md`, `crates/runku-data/README.md`, `crates/runku-value/README.md`, adapter conformance tests |
| Identity/security | `docs/auth/application-identity.md`, `docs/security/security-model.md`, `SECURITY.md`, identity and gateway tests |
| Release/Workspace | `docs/development/releases-and-workspaces.md`, `docs/reference/compatibility.md`, release/workspace tests |
| Distribution/release automation | `docs/maintainers/releases.md`, `.github/workflows/release.yml`, `scripts/release-*.mjs`, package manifests |
| Self-hosting/deployment | `docs/self-hosting/overview.md`, `docs/self-hosting/production-readiness.md`, `deployments/README.md`, role-specific runbooks |
| Operations | `docs/operations/administration.md`, `docs/operations/observability.md`, `docs/operations/backup-and-recovery.md` |
| Architecture/evolution | `docs/internals/repository-map.md`, `docs/development/evolving-runku.md`, protocol vectors, relevant crate APIs |

Do not claim to have read a file unless you read it completely in the current task.

## Product scope

- Runku is a complete self-hosted product. Installation, administration, application serving,
  identity integration, observability, backup, recovery, and upgrades belong to this product.
- Project, Environment, Release, Channel, Workspace, Function, Application Client, and functional
  identity are product concepts and remain deployment-independent.
- Kubernetes, Docker, PostgreSQL, S3, NATS, V8, Node.js, and microVM technology are implementation
  profiles or dependencies. None of them is the product identity.
- Firecracker is one implementation of the shared untrusted Full Node isolation boundary. Keep its
  name in implementation-specific code, configuration, and security runbooks where precision is
  required. Do not name top-level product manifests, generic roles, or the whole Kubernetes
  topology after Firecracker.

## Current release boundary

The source line is pre-release. Tagged releases publish the cross-platform CLI, TypeScript SDKs,
and a compact Linux `runku-server` binary/non-root image that composes Platform Identity with one
initialized Safe V8 Product Environment. The repository includes distributed adapters and
conformance harnesses, but it does not yet publish separated general-purpose roles, a
`runku-agent` binary, a production Compose profile, or a supported Helm chart.

Never invent installation commands, image names, environment variables, Admin APIs, backup
commands, or stability guarantees. When a required production capability is absent:

1. state the limitation close to the relevant instruction;
2. link to the production-readiness contract;
3. document the acceptance criteria without presenting them as implemented;
4. add implementation only when the task explicitly includes it.

## Sources of truth

Use this precedence when sources disagree:

1. accepted public protocol vectors and persisted-format decoders;
2. executable conformance, integration, security, and failure-path tests;
3. public Rust/TypeScript APIs and strict CLI parser/help;
4. product and operational documentation;
5. examples and comments;
6. aspirational production-readiness requirements.

A discrepancy is a defect. Fix the lower-precedence source or explicitly version the contract.
Do not reconcile contradictions by adding vague wording.

## Non-negotiable invariants

- `Environment = persistent state`, `Release = immutable code`, and `Channel = traffic policy`.
- Every storage, cache, artifact, identity, realtime, queue, and invocation key is scoped to the
  owning Project and Environment.
- A request, subscription, nested call, Cron activation, or scheduled invocation pins one exact
  Release or Dev Revision for its defined lifetime.
- There is no implicit `latest` target.
- Application identity and functional identity are independent authorization axes.
- Public, secret, and development credentials cannot exchange roles.
- Query is snapshot/read-only; Mutation commits document/index/outbox/schedule changes atomically;
  Action may perform effects and is not automatically retried.
- Durable execution is at-least-once. External effects require application-level idempotency.
- Safe V8 is deny-by-default. Capabilities must be declared and mediated.
- Shared untrusted Full Node code requires a VM-grade boundary; Docker alone is not that boundary.
- Unknown wire, persisted, manifest, runtime, or configuration versions fail closed.
- No realtime notification is visible before the corresponding commit.

## Documentation maintenance contract

Every user-visible change must update documentation in the same pull request. At minimum:

- new or changed CLI behavior: `docs/reference/cli.md` and the relevant task guide;
- SDK surface: package README, examples, generated-type narrative, and compatibility notes;
- distribution: install guide, target matrix, package metadata, checksums/provenance, bootstrap,
  retry/recovery, and release-maintainer procedure;
- protocol/persisted format: vectors, `protocol/README.md`, compatibility matrix, migration notes;
- deployment/configuration: deployment README, configuration reference, security model, upgrade and
  rollback procedure;
- operational signal or failure mode: observability catalog, troubleshooting guide, and runbook;
- security boundary: security model, threat/abuse analysis, safe defaults, and residual risk;
- benchmark: workload, environment, raw output location, interpretation, and non-SLA warning.

Documentation must answer:

1. What problem does this capability solve?
2. What prerequisites and permissions does it require?
3. What exact command/API/configuration is used?
4. What durable state does it read or change?
5. What are the success signals?
6. What can fail, how is it detected, and is retry safe?
7. How is it backed up, restored, upgraded, rolled back, or removed?
8. What are the security boundaries and residual risks?
9. Which tests or evidence prove the claim?

Use English for public documentation and user-facing text. Prefer explicit support tables,
procedures, and decision trees over promotional language. Mark examples as examples, conformance
assets as conformance, and unimplemented requirements as readiness criteria.

## Change workflow

1. Inspect `git status` and preserve unrelated user changes.
2. Identify the public contract and its tests before editing.
3. Make the smallest coherent change across code, tests, docs, and vectors.
4. Run the narrowest relevant gate while iterating.
5. Run all gates affected by the contract before handoff.
6. Validate Markdown links and examples when documentation changes.
7. Summarize behavior, compatibility, operational impact, security impact, and commands run.

Useful gates:

```sh
make toolchain-check
make fmt-check
make lint
make test
make docs
make incomplete-check
make sdk-server-check
make sdk-typescript-check
make local-process-check
make gateway-product-check
make release-lifecycle-check
make remote-workspace-check
make check
```

Some gates start Docker dependencies. Read the Makefile target before running it and stop only the
resources created by your task.

## Compatibility and migrations

Classify every change before implementation:

- **internal:** no public/persisted observable effect;
- **compatible additive:** old consumers continue to work without migration;
- **behavioral:** same shape but changed semantics, limits, retry, timing, or authorization;
- **breaking:** old data, clients, SDKs, artifacts, manifests, config, or operations may fail;
- **security fix:** may intentionally reject behavior that worked before.

Breaking and behavioral changes require an explicit versioning/migration decision. Never rewrite
existing protocol vectors or reinterpret durable bytes in place. Use expand → migrate/backfill →
contract for schema evolution and preserve rollback limits in the operator documentation.

## Definition of done

A change is not complete because code compiles. It is complete when:

- success, failure, recovery, concurrency, and adversarial paths are tested proportionally;
- public and persisted formats remain compatible or are explicitly versioned;
- docs enable a new operator or application developer to use and recover the feature;
- secrets and user-controlled cardinality are excluded from diagnostics;
- no accepted work contains `TODO`, `FIXME`, ignored tests, `todo!`, or `unimplemented!`;
- the relevant gates pass and their scope is reported accurately.
