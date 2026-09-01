# Evolving Runku safely

Runku changes must preserve application data, explicit code targeting, authorization boundaries,
and operator recovery. This guide turns a proposed change into a versioned, testable, documented
product evolution.

## 1. State the contract

Before code, write:

- user/operator problem and excluded behavior;
- affected public APIs, CLI, configuration, wire/persisted formats, data, artifacts, runtime, and
  operations;
- invariants and trust boundaries;
- durable state created/changed;
- failure/retry/uncertain-outcome semantics;
- compatibility class and migration/rollback limit;
- success, performance, security, and recovery evidence.

Avoid implementation nouns in the product contract unless users must configure or reason about
them.

## 2. Classify compatibility

| Class | Examples | Required action |
|---|---|---|
| Internal | Refactor behind unchanged trait/tests | Focused tests and architecture consistency |
| Additive compatible | Optional protocol field with safe default | New vectors/tests/docs; old consumer tests |
| Behavioral | Retry, timeout, ordering, auth, limits change | Explicit release note and adversarial tests |
| Breaking | Existing bytes/config/SDK/data rejected | New version + migration/support-window decision |
| Security hardening | Previously accepted unsafe behavior rejected | Advisory/upgrade guidance and regression test |

Unknown versions fail closed. Never reinterpret an existing vector or persisted discriminant.

## 3. Select the owning layer

- pure IDs/targets/domain policy: `runku-core`;
- canonical values/keys: `runku-value`;
- storage contract: `runku-data`, then every adapter/conformance suite;
- release/artifact/routing: release crates and protocol vectors;
- execution semantics: execution/runtime crates plus gateway vertical tests;
- public transport: protocol/gateway/client together;
- declarative source surface: server package, builder, generated contracts, examples;
- local lifecycle: local crate and strict CLI parser;
- remote development: development protocol/service/client and reconciliation tests;
- operational signal: observability/OTLP plus privacy/cardinality/runbook updates.

Do not add a shortcut in CLI, gateway, or one adapter that bypasses the owning contract.

## 4. Design data evolution

Use expand → migrate/backfill → contract:

1. add a representation readable by old/new code;
2. deploy readers/writers compatible with both;
3. migrate in bounded, resumable, observable, idempotent batches;
4. prove coverage and consistency;
5. stop producing the old form;
6. remove old support only after the compatibility window and rollback decision.

Index builds require explicit building/ready/retiring state. Promotion must not select code that
assumes unavailable data/indexes. Garbage collection must prove reachability across Channels,
Workspaces, subscriptions, Cron, and pending scheduled invocations.

## 5. Preserve distributed semantics

For leases, queues, outbox, scheduler, Cron, or Agents, test crash points before and after every
durable write/ack/pointer move. Define ownership, fencing, replay identity, poison handling,
backpressure, cancellation, and uncertain results. At-least-once delivery does not permit publishing
an uncommitted result or running under a different code pin.

## 6. Security review

Identify assets, actors, boundaries, untrusted inputs, privilege transitions, resource exhaustion,
and residual risk. Test cross-Project/Environment IDs, key-role confusion, artifact tampering,
source/path abuse, SSRF/DNS rebinding/redirects, JWT confusion, cache scope, log leakage, malformed
protocol complexity, runtime escape, backup/upgrade tampering, and administrative authorization as
applicable.

Least privilege is structural: capability absent from metadata must also be absent from context,
runtime broker, network, and deployment permissions.

## 7. Test pyramid

- unit/property tests for pure invariants and parsers;
- conformance tests shared by every adapter;
- protocol vectors for byte/wire persistence;
- vertical tests through gateway, identity, routing, runtime, data, and response;
- process tests for CLI, filesystem, locks, signals, restart, and errors;
- adversarial tests for trust boundaries and bounded parsing;
- failure campaigns for dependency/process/node loss and uncertain outcomes;
- clean-room install/upgrade/restore for supported distributions;
- regression benchmarks with exact environment and raw evidence.

A mock-only successful test is insufficient for a persistence, isolation, or recovery claim.

## 8. Documentation/update matrix

Update all affected surfaces in the same change:

| Change | Required docs/evidence |
|---|---|
| CLI | CLI reference, task guide, errors/exits, automation example |
| SDK/build | package README, application tutorial, generated contract, example/gate |
| Protocol | vectors, protocol README, compatibility and migration |
| Storage/schema | data guide, adapter docs, backup/restore, conformance |
| Identity/security | identity guide, security model, rotation/revocation runbook |
| Release lifecycle | releases guide, admin runbook, rollback/compatibility |
| Deployment/config | profile README, configuration, probes, upgrade, security, limits |
| Signal/failure | observability catalog, alert, troubleshooting, recovery runbook |

## 9. Rollout and rollback

Define preflight, ordering, mixed-version behavior, readiness gate, canary/smoke test, observation
window, stop condition, and rollback/forward-fix path. Channel rollback only changes code routing;
it is not a database rollback. Persisted migration and external effects need separate recovery.

## 10. Completion evidence

The pull request reports compatibility/security/operational impact and exact gates. It contains no
accepted-work placeholders or ignored tests. Documentation enables an unfamiliar operator to use,
observe, fail, and recover the capability without reading private context.
