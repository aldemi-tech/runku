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
- Application file grants are Environment-bound, short-lived HMAC bearer credentials; upload
  grants are one-shot and File IDs alone grant no access.
- Transactions publish outbox and scheduling state atomically with data changes.
- Public and development credentials cannot exchange roles.
- Platform operator access uses separate invitation/access/refresh credentials and scoped grants;
  application and development keys never authenticate the Management API.

## Secrets

Service and development keys are one-time reveal credentials. Logs, build output, generated types,
browser bundles, traces, and error envelopes must redact them. An IdP private key and any secret
configuration remain server-side.

Platform invitation (`rk_inv_v1_*`), access (`rk_at_v1_*`), and refresh (`rk_rt_v1_*`) credentials
are also bearer secrets. Only domain-separated HMAC digests are persisted. Bootstrap/session
peppers and the independent OIDC subject pepper belong in the secret provider and coordinated
backup; loss invalidates credentials or external links, while disclosure requires rotation and
session/invitation incident response.

Delegated invitation automation binds one canonical non-secret `opn_*` identity to a SHA-256
fingerprint of the requested operator, exact scope, and expanded capabilities. Exact replay returns
metadata only; it never reconstructs the bearer. An uncertain create is reconciled by operation ID,
then an unavailable code is revoked and replaced. Operation lookup and revocation reload current
`operators:manage` authority for the stored scope, so an Operation ID is never authorization.

Authenticated Product management reloads the operator session and current grants for every
request. Project/Environment path scope is authorized before the Product adapter is reached.
Remote log follow rechecks `logs:follow` during the single streaming connection, so session
revocation or grant removal stops future records. Operator tokens never authorize Product
invocation, and `rk_pub_*`/`rk_sec_*`/`rk_dev_*` never authorize management operations.

Archived logs retain the exact Project/Environment namespace in subjects, object paths, manifests,
and queries. Serving and worker NATS identities are separate; remote NATS requires TLS and rejects
URL credentials. S3 credentials are never accepted through CLI arguments. Immutable manifest
commit, digest verification, create-or-verify replay, and archive-frontier retention prevent a
forged ACK, altered object, or partial write from silently authorizing deletion. Object storage and
journal operators remain privileged trust boundaries and require least privilege, encryption,
audit, and separate failure-domain backups. See
[Operational Log storage](../operations/operational-logs.md).

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
- operator bootstrap theft/replay, invitation privilege escalation, cross-scope grants, session
  replay, external-subject collision, login CSRF/mix-up, callback injection, false browser
  confirmation, authentication/Management endpoint substitution, or application/operator
  credential confusion;
- Realtime authorization drift or pre-commit disclosure;
- OCC/idempotency/replay errors and repeated external effects;
- sandbox escape, resource exhaustion, host/Agent secret exposure;
- secret leakage through logs, errors, bundles, traces, or backups;
- file-grant replay/theft, quota reservation abuse, oversized/chunked uploads, range amplification,
  path traversal, MIME confusion, checksum drift, filesystem exhaustion, orphaned multipart parts,
  or a privileged/compromised object-storage operator;
- unsafe migration, partial restore, wrong identity, or downgrade.

## Deployment and secret controls

Use TLS, exact origins/hosts/trusted proxies, private dependency networking, least-privilege service
accounts, non-root/read-only images, seccomp/AppArmor guidance, immutable digests, SBOM/provenance/
signatures, encrypted secret providers, NetworkPolicy deny-by-default, bounded resources, and
separate application/management/Agent identities. Gateway never receives KVM/host privileges.

Security-sensitive codec and cryptography dependencies are pinned and reviewed with their feature
sets. An update must preserve canonical protocol bytes and must not silently enable optional unsafe
acceleration. Identity, protocol-vector, Realtime, and affected adapter tests are required when a
dependency crosses those boundaries. Runtime randomness continues to come from the
operating-system RNG; deterministic signing material exists only in the public unit-test fixture
described below and is never part of deployment trust.

Run `make security-audit` with a current RustSec database as an explicit networked gate. It remains
separate from the fast compile/package gate. JWT verification uses `jsonwebtoken`'s `aws_lc_rs`
backend; Runku does not retain an unfixed RustCrypto `rsa` implementation merely to generate test
signatures. RSA signing tests use a repository-public, test-only fixture with no deployment trust.
Unmaintained transitive warnings from the pinned Deno/V8/SWC graph must be tracked during upstream
updates even when RustSec reports no exploitable advisory.

One-time secret material belongs in a secret manager. Rotate with overlap, verify replacement, then
revoke. Never place secrets in CLI arguments, ConfigMaps, image layers, source, generated types,
public env prefixes, logs, traces, errors, or unencrypted backups.

Application Functions never receive filesystem paths, bucket endpoints, or S3 credentials. Storage
Platform Ops derive generated object keys from Project, Environment, and canonical File ID; reserve
quota before accepting bytes; enforce byte, file-count, live-grant, Action, concurrency, and
free-space limits; reject encoded bodies, duplicate headers, malformed media
types/checksums/ranges; and verify immutable metadata on read. Transfer tokens belong only in
`Authorization`, never query strings, logs, traces, referrers, analytics, or persisted application
documents. Exact browser CORS origins still apply.

The filesystem/S3 operator remains privileged. Use a dedicated root or prefix, least-privilege
credentials, TLS, encryption at rest, audit, capacity alerts, multipart-abort lifecycle, and an
independently tested backup/restore strategy. Runku does not configure or operate those durability
controls. Filesystem roots must be absolute, non-root, non-symlink directories; an existing Unix
root must already be private because Runku will not change broad directory permissions. See
[Application file storage](../functions/file-storage.md).

The compact Docker profile mounts the PostgreSQL URL and Platform Identity pepper as separate
one-line secret files. Runku rejects simultaneous direct/file sources, relative paths, symlinks,
non-files, empty/oversized/multiline values, and control characters. Docker Compose file-backed
secrets are an injection mechanism, not an encryption system: protect their host directory with a
dedicated owner, `0700` directory permissions, encrypted storage, and backup access controls.

That profile uses Linux host networking only to preserve the Product's loopback listener. Treat the
dedicated host as the installation boundary, firewall the PostgreSQL/Runku loopback ports, and allow
only the host TLS proxy to publish Product and Management. Do not use the profile on an untrusted
shared host or expose its loopback ports through another forwarding mechanism.

The native login client uses exact canonical origins, HTTPS except literal loopback, no proxy, no
redirects, PKCE S256, fresh state, a loopback-only listener, exact callback Host/path/method,
single-valued `state`/`code`/`iss`/`error`, a fixed validated token endpoint, bounded bodies and
timeouts, and shell-free browser launching. External tokens are then verified server-side against
the configured asymmetric algorithm, signature, exact issuer/audience/discriminator, expiry, JWKS
origin policy, and subject namespace. A successful provider callback alone is never presented as a
successful Runku login.

Remote project linking treats Project and Environment IDs as public identifiers, never as proof of
ownership. `runku link` must obtain an authorized exact-scope Management status response before it
creates local state, then pins that canonical Management origin in a non-secret local descriptor.
Later remote commands refuse origin substitution for linked roots. Copying identifiers or locally
minting Application credentials cannot create a remote operator grant or match credential digests
stored by another Environment. A copied project directory remains sensitive because it can contain
source, local data, and other credentials; the link descriptor itself contains none.

Initial-owner recovery is deliberately local and pre-enrollment only. It requires administrative
access to PostgreSQL, the installation pepper, configuration, and protected state directory;
atomically revokes prior pending material and writes a security-audit event. It is never available
through the network Management API and cannot reopen bootstrap after any operator exists.

## Incident response and residual risk

Preserve evidence, scope IDs/versions/topology, stop unsafe changes, rotate/revoke credentials,
isolate affected roles without destroying state, reconcile uncertain effects, restore only verified
backups, and add an adversarial regression test. Use private reporting in
[SECURITY.md](../../SECURITY.md).

Component correctness does not certify an installation. Shared untrusted Full Node requires the
documented VM-grade boundary and verified assets. Complete the
[production-readiness contract](../self-hosting/production-readiness.md) before live adoption.
