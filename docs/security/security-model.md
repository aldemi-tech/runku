# Security model

Runku assumes application code, client input, artifacts, and network peers may be hostile.

## Primary boundaries

- Project and Environment scope is part of every authoritative key.
- Application identity and functional identity are independent checks.
- Safe V8 exposes only declared Platform Ops.
- Full Node never executes inside the Safe V8 process.
- Shared untrusted Full Node uses a microVM boundary, not Docker alone.
- HTTPS egress resolves and pins allowed destinations and denies private infrastructure ranges.
- Artifact size and digest are verified on every trust-boundary read.
- Transactions publish outbox and scheduling state atomically with data changes.
- Public and development credentials cannot exchange roles.

## Secrets

Service and development keys are one-time reveal credentials. Logs, build output, generated types,
browser bundles, traces, and error envelopes must redact them. An IdP private key and any secret
configuration remain server-side.

## Residual risk

The current repository is pre-release and its production packaging and distributed operational
profiles are not certified. Do not run mutually untrusted Full Node code outside the documented
Firecracker boundary. See [SECURITY.md](../../SECURITY.md) to report vulnerabilities.
