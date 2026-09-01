# Contributing to Runku

Thank you for helping improve Runku. Changes must preserve the documented protocol, tenant,
transaction, and runtime boundaries.

## Development setup

```bash
make toolchain
pnpm install --frozen-lockfile
make ci-check
```

`make ci-check` matches the bounded hosted gate: formatting, policy checks, compile-only validation
of every Rust target, public package checks, and release metadata. It does not start Runku or run
Rust/integration behavioral tests. `make check` adds strict Clippy, Rust tests, rustdoc, and
executable examples. Smaller behavioral targets in the Makefile are available while iterating, but
a pull request is not ready until every gate relevant to the changed contract passes.

## Engineering rules

- Do not introduce a remote commercial dependency into the self-hosted application path.
- Keep Environment and Project scope explicit at storage, cache, artifact, identity, and realtime
  boundaries.
- Version every persisted or wire format before storing user data.
- Keep Safe V8 capabilities deny-by-default; Node.js access belongs to Full Node isolation.
- Preserve idempotency and at-least-once semantics for durable work.
- Add failure-path and adversarial tests together with successful-path tests.
- Do not commit `TODO`, `FIXME`, ignored tests, `todo!`, or `unimplemented!` for accepted work.
- User-facing documentation, errors, examples, comments, and commit messages must be in English.

## Pull requests

A pull request should explain the affected contract, tests executed, compatibility impact, and any
operational or security consequence. Generated files must be reproducible from committed sources.

Security vulnerabilities must follow [SECURITY.md](SECURITY.md), not the public issue tracker.

## Required context and evidence

Read [`AGENTS.md`](AGENTS.md), the documentation portal, platform model, architecture, and
task-specific contracts. Classify compatibility before editing. Existing vectors/durable formats
are never reinterpreted in place.

Use `make ci-check` for the baseline and focused gates while iterating (`make lint`, affected
SDK/process/vertical targets), then run every affected behavioral gate. Report exact commands and
what was not run; do not describe compile-only validation as behavioral evidence.

Update CLI/task docs, package READMEs, vectors, compatibility, operations, security, and examples in
the same change. Follow [Evolving Runku](docs/development/evolving-runku.md).

Distribution changes additionally run `make cli-package-check`, `make release-package-check`, and
follow [Publishing a distribution](docs/maintainers/releases.md). Tags are created only from a
reviewed clean `main` commit; pull-request CI proves behavior and the tag workflow performs only
native compilation and package/release verification.

A pull request includes problem/scope, contract impact, invariants, failure/security analysis,
migration/rollback limit, exact tests, docs changed, and omitted gates.
