# Self-Hosted hardening checklist

Use this checklist on the actual installation before production and after any change to topology,
runtime trust, identity provider, storage, proxy, or recovery. A checked item needs retained
evidence, an owner, and a review date.

## Release and host

- [ ] The Self-Hosted archive checksum/provenance and OCI manifest digest match the selected tag.
- [ ] The image is pinned by tag and digest; no deployment references `latest` or another mutable
      target.
- [ ] Docker/host/kernel/PostgreSQL versions and security update ownership are recorded.
- [ ] Runku runs as the configured unprivileged UID/GID with all capabilities dropped,
      `no-new-privileges`, read-only container root, and bounded PIDs/memory/CPU/descriptors.
- [ ] Product, Platform, files, PostgreSQL, secrets, backups, and temporary directories have distinct
      least-privilege ownership; none uses `/`, a home directory, or a symlinked root.
- [ ] Only package-created resources are included in service/uninstall automation.

The compact Safe V8 image does not grant KVM. Do not add host mounts, Docker socket, privileged
mode, or broad device access to resolve an application need.

## Network and TLS

- [ ] Public traffic terminates modern TLS at a controlled proxy with automated certificate expiry
      monitoring.
- [ ] `127.0.0.1:3210` and `127.0.0.1:3220` cannot be reached directly from another host/network.
- [ ] The proxy has two explicit routes: application HTTP/WebSocket and Authentication/Management.
- [ ] Untrusted forwarding headers are stripped; only documented headers are forwarded.
- [ ] Management/auth responses are not cached and login discovery is not redirected.
- [ ] WebSocket upgrade, idle timeout, maximum body/header, and slow-client behavior are tested.
- [ ] Browser origins are exact, minimal, HTTPS in production, and tested for denial; `*` is not
      used as a convenience around credentialed application calls.
- [ ] PostgreSQL, S3, NATS, registry, IdP/JWKS, KMS, backup, and telemetry routes use private/TLS
      connectivity and destination allowlists.

`RUNKU_MANAGEMENT_TLS_TERMINATED=true` asserts a trusted external boundary. Setting it without that
boundary exposes bearer administration over plaintext.

## Secrets and keys

- [ ] Every supported secret uses a mounted `_FILE` or provider-specific protected file where
      available; no secret appears in `.env`, shell history, command arguments, source, image, or
      CI output.
- [ ] Secret files are absolute, regular, non-symlinked, mode `0600` or equivalent, and readable
      only by the intended runtime identity.
- [ ] Platform Identity pepper, Product key material, OIDC subject pepper, database credentials,
      S3/NATS credentials, and backup encryption references have separate inventory and rotation.
- [ ] Backup of encrypted state includes the matching key version while preserving separate access
      control; restore with a mismatched key is tested to fail closed.
- [ ] Environment secret values never appear in Management snapshots/history, logs, debug output,
      metrics, Function results, or error fields.
- [ ] Function code declares only exact `variable:NAME`/`secret:NAME` capabilities; secrets are
      restricted to Actions.
- [ ] Rotation uses overlap only where the protocol supports it, verifies the new credential, then
      revokes the old one and closes the overlap window.

Do not rotate an unrelated root/pepper in response to one leaked application or operator
credential. Revoke at the authority that issued it.

## Platform operator identity

- [ ] The bootstrap invitation was read from the protected file, consumed once, and removed from
      operational workflows.
- [ ] At least two accountable recovery operators exist under approved policy; routine automation
      does not use owner-level access.
- [ ] Grants use the smallest exact Installation/Project/Environment scope and capability set.
- [ ] Invitations have intended lifetime/scope, are delivered out-of-band, and pending material is
      reviewed/revoked.
- [ ] Device sessions are named, listed, expired/revoked, and checked against transactional audit.
- [ ] Managed grant reconciliation has a pinned source authority and monotonic revision; stale or
      divergent updates alert.
- [ ] OIDC pins issuer, audience, algorithm, discovery/JWKS origin, discriminator, optional `typ`,
      resource, and PKCE endpoints; wrong issuer/audience/key/redirect/role is tested.
- [ ] Break-glass login and IdP/JWKS outage procedures work without using Application Keys.

All Management requests reload current grants. Verify revocation during a live `logs follow`
connection, not only during new login.

## Application and functional identity

- [ ] Each browser/native/server application has a separate Application Client and minimum scope.
- [ ] Browser bundles contain only `rk_pub_*`; `rk_sec_*` and `rk_dev_*` remain server/CI-only.
- [ ] Application Key rotation verifies overlap/revoke and does not silently replace another
      Environment's dotenv values.
- [ ] Product JWT providers have exact issuer/audience/algorithm/JWKS/network rules and mapping
      revision ownership.
- [ ] Function `auth` requirements and in-handler document/tenant authorization have negative tests.
- [ ] Guest, user, service, system, application, development, and platform roles cannot be confused
      or exchanged.

## Runtime and code supply chain

- [ ] Safe V8 is the selected profile unless Node is truly required; manifests declare the minimum
      data/network/storage/scheduler/nested-call/configuration capabilities.
- [ ] Builds reject unsupported imports, path escape, symlinked source, unknown versions, and
      unstable snapshots.
- [ ] Artifacts are verified by digest and size on read; old eligible Releases remain immutable.
- [ ] Actions use destination constraints and application-level idempotency for external effects.
- [ ] Nested calls preserve exact Environment/code pin and enforce child visibility/capabilities.
- [ ] Shared untrusted Full Node code is not run in ordinary Docker; a VM-grade boundary is required.

The current compact release does not ship a Full Node Agent. Kubernetes/Firecracker assets in the
repository are conformance/architecture material, not an authorization to expose a production
multi-tenant runtime.

## Data, files, and object storage

- [ ] Every authority is scoped to exact Project/Environment and cross-scope tests pass.
- [ ] PostgreSQL roles/databases are separated between Platform Identity and optional Product logical
      store; Product root remains required and protected.
- [ ] Mutation operation identities, optimistic conflicts, and uncertain responses are reconciled
      rather than blindly replayed.
- [ ] File quotas, Action copy limit, stream concurrency, grant TTL, replay protection, and
      filesystem free-space floor fit the threat model.
- [ ] S3 credentials are limited to the exact bucket/prefix/operations; endpoint HTTP exceptions are
      disabled outside literal-loopback tests.
- [ ] Bucket CORS/policy/access-key lifecycle is narrow and secret material is retained only once.
- [ ] External S3 encryption, versioning, lifecycle, access logs, replication, and recovery point
      are provider-owned and verified.

## Logs, diagnostics, and abuse

- [ ] Logs exclude credentials, secret values, arguments/results, document/file contents, source,
      DSNs, and provider tokens.
- [ ] User-controlled values are excluded from metric labels; Project/Environment/Release/Function
      dimensions have explicit cardinality budgets.
- [ ] Request/invocation/operation IDs and bounded cursors provide correlation without payloads.
- [ ] Log follow is authenticated/re-authorized, and archive/query access has separate grants.
- [ ] Prune cannot cross the verified immutable archive frontier; dry-run/apply is reviewed.
- [ ] Admission, timeouts, body/header sizes, runtime limits, file limits, log limits, and Management
      concurrency are tested against resource exhaustion.
- [ ] Telemetry loss alerts independently; OTLP is not authoritative archive or billing state.

## Backup, restore, and upgrade

- [ ] The recovery manifest covers Platform Identity PostgreSQL, Product root, optional Product
      PostgreSQL, local files or coordinated external S3, logs, and matching key material.
- [ ] Backups are encrypted, off-host, access-controlled, checksum-verified, and retained under an
      explicit policy.
- [ ] Empty-install restore drills verify scope, identity, Channels, data, Realtime, schedules,
      files, logs, and post-restore revocation/replay reconciliation.
- [ ] Upgrade is tested from the previous supported version with production-shaped state.
- [ ] Operators know the exact point after which the old server cannot safely start.
- [ ] A Channel rollback, configuration rollback, credential rotation, forward server fix, and
      full recovery-point restore have separate approvals and procedures.

## Residual-risk acceptance

Record explicitly that the compact profile has one active writer and host failure domain, no rolling
multi-node upgrade, no published general-purpose Kubernetes/Agent package, and no automatic
recovery of provider-owned S3. If any residual risk is unacceptable, keep traffic closed and track
the corresponding [production-readiness](../self-hosting/production-readiness.md) criterion.
