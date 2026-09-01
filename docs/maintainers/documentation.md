# Maintaining the documentation system

Documentation is part of Runku's product contract. It must allow a reader with no private context
to use a feature, evaluate its support, administer state, diagnose failure, recover safely, and
understand compatibility.

## Source precedence

When sources disagree, use: protocol/persisted vectors → executable conformance/integration tests →
public APIs/strict CLI parser → operational documentation → examples/comments → readiness
requirements. Fix contradictions rather than weakening wording.

## Required document shape

A substantial capability document should cover:

- purpose and non-goals;
- prerequisites/support status;
- model and invariants;
- exact API/command/configuration;
- durable state and lifecycle;
- security/trust boundary;
- success/readiness/observability signals;
- failure classes, retry safety, and uncertain outcomes;
- backup/restore/upgrade/rollback/removal impact;
- limits and capacity;
- executable evidence and related references.

Do not repeat every detail in every file. Task guides explain procedures, references define exact
surface, internals explain implementation boundaries, and package READMEs stay close to exported
APIs. Link between them.

## AI-readable maintenance

`AGENTS.md` is the required entry point. Keep its reading map, invariants, sources of truth, update
matrix, gates, and definition of done current. A new subsystem must add its owning path and required
reading. Do not rely on chat history, hidden roadmaps, or institutional memory.

Use explicit tables for status, ownership, compatibility, signals, and failure response. Use stable
domain names consistently. Avoid ambiguous “it”, “production ready”, “secure”, “scalable”, or
“automatic” without named conditions and evidence.

## Update matrix

- CLI parser/help change → CLI reference + task guide + automation/retry notes.
- TypeScript export change → package README + type conformance + application tutorial/example.
- persisted/wire change → vectors + protocol README + compatibility/migration.
- storage/runtime behavior → concept/task guide + failure/recovery + conformance evidence.
- configuration/deployment change → profile, security, observability, upgrade, rollback, support.
- new error/signal → troubleshooting, administration, observability, alert/runbook.

## Review checklist

- Commands were copied from current `--help` or tested APIs.
- Code examples typecheck or are adapted from executable examples.
- Links resolve relative to their document.
- Support state is close to the instruction.
- No conformance asset is presented as a product package.
- No secret/test credential is a recommended default.
- Retry/rollback language accounts for idempotency and uncertain effects.
- Destructive operations name exact scope, backup, and recovery implications.
- Technology-specific detail is nested under the product capability it implements.
- Public prose is English and avoids internal planning language.

## Validation

For documentation-only changes, run at least link validation, `git diff --check`, searches for
stale paths/names, and the narrow SDK/CLI check relevant to examples. Code/API changes require the
full affected gates described in `AGENTS.md` and `CONTRIBUTING.md`.

Record what was not tested. Never state “all tests pass” when only a documentation check ran.
