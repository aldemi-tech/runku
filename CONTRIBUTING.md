# Contributing to Runku

Thank you for helping improve Runku. Changes must preserve the documented protocol, tenant,
transaction, and runtime boundaries.

## Development setup

```bash
make toolchain
pnpm install --frozen-lockfile
make check
```

`make check` runs formatting, strict Clippy, Rust tests, rustdoc, TypeScript package checks, and both
examples. Smaller targets in the Makefile are available while iterating, but a pull request is not
ready until the complete relevant gate passes.

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
