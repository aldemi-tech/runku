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
VM-grade microVM boundary. See [SECURITY.md](../../SECURITY.md) to report vulnerabilities.

## Threat classes

- cross-Project/Environment scope confusion;
- Application/Development/functional/admin credential role confusion;
- malformed or high-complexity source, protocol, values, manifests, and artifacts;
- SSRF, DNS rebinding, redirects/private ranges, and oversized HTTPS responses;
- artifact/build dependency tampering and mutable image references;
- JWT issuer/audience/algorithm/key confusion or stale JWKS;
- Realtime authorization drift or pre-commit disclosure;
- OCC/idempotency/replay errors and repeated external effects;
- sandbox escape, resource exhaustion, host/Agent secret exposure;
- secret leakage through logs, errors, bundles, traces, or backups;
- unsafe migration, partial restore, wrong identity, or downgrade.

## Deployment and secret controls

Use TLS, exact origins/hosts/trusted proxies, private dependency networking, least-privilege service
accounts, non-root/read-only images, seccomp/AppArmor guidance, immutable digests, SBOM/provenance/
signatures, encrypted secret providers, NetworkPolicy deny-by-default, bounded resources, and
separate application/management/Agent identities. Gateway never receives KVM/host privileges.

One-time secret material belongs in a secret manager. Rotate with overlap, verify replacement, then
revoke. Never place secrets in CLI arguments, ConfigMaps, image layers, source, generated types,
public env prefixes, logs, traces, errors, or unencrypted backups.

## Incident response and residual risk

Preserve evidence, scope IDs/versions/topology, stop unsafe changes, rotate/revoke credentials,
isolate affected roles without destroying state, reconcile uncertain effects, restore only verified
backups, and add an adversarial regression test. Use private reporting in
[SECURITY.md](../../SECURITY.md).

Component correctness does not certify an installation. Shared untrusted Full Node requires the
documented VM-grade boundary and verified assets. Complete the
[production-readiness contract](../self-hosting/production-readiness.md) before live adoption.
