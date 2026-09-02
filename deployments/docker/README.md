# Docker deployment profile

Docker supports the tagged compact Safe V8 server image, local/conformance dependencies, and a
dedicated trust-domain execution unit. It is not yet a published distributed Runku Compose
installation.

## Current uses

- `compose.storage.yml`: pinned PostgreSQL for storage/repository/realtime/scheduling conformance;
- `compose.remote-execution.yml`: local NATS JetStream and MinIO/S3 for execution/artifact gates;
- `compose.platform-identity.yml`: pinned PostgreSQL and a disposable Keycloak reference fixture
  for the executable Platform Identity and full authenticated Product lifecycle campaigns;
- Full Node OCI tests: prove image, Node, npm, filesystem, crypto, and restricted TCP contracts;
- `server.Dockerfile`: the digest-pinned non-root compact server image built only by the release
  workflow from natively compiled Linux binaries.

Tagged images are `ghcr.io/aldemi-tech/runku-server:X.Y.Z` for Linux ARM64/x86_64. Pin the resolved
manifest digest in deployment configuration. The image contains `runku-server` as its entrypoint
and the matching `runku` CLI for controlled init/diagnostic jobs; it contains no shell or package
manager and implements the Safe V8 compact profile only.

Read each Makefile target before starting resources. Stop only the Compose project created for the
test. `down -v` destroys its test volumes.

## Security boundary

Docker isolation is acceptable for development/conformance or a complete dedicated tenant unit. It
is not the boundary for mutually untrusted Node workloads sharing a host. Gateway and ordinary Runku
roles remain non-root and do not receive host/KVM privileges.

The current Compose files use local credentials, plaintext loopback endpoints, published ports,
single replicas, and deterministic test storage. Never expose them on a network or reuse credentials.

Run the identity campaign with `make platform-identity-keycloak-check`. It destroys only the
isolated Compose project's disposable volumes on exit. Keycloak runs in development mode and its
imported password/direct grant exist solely to obtain deterministic test evidence; neither is an
installation recommendation. Keycloak is one concrete provider used to exercise Runku's generic
OIDC boundary, not a required component, preferred provider, production example, or certified
integration. Select and qualify the installation's provider with the neutral procedure in
[Platform operator identity](../../docs/auth/platform-identity.md#choose-and-qualify-an-identity-provider).

Run `make platform-lifecycle-keycloak-check` for the heavier browser and Product acceptance gate.
It compiles the CLI/server once, exercises browser Authorization Code + PKCE, and then covers
publish, Release validation, promotion, invocation, log snapshot/stream, replacement, rollback,
scope/capability denial, live session revocation, and re-login. It is intentionally not part of the
fast hosted `ci-check`.

## Requirements for a supported Compose profile

- released digest-pinned Runku images and versioned config schema;
- PostgreSQL/S3 dependencies with TLS, authentication, persistent volumes, backup, and health;
- secret files/provider rather than inline environment values;
- non-root/read-only containers, explicit writable paths, resources, and restart/drain behavior;
- ingress/TLS, origin/host/trusted-proxy policy;
- migrations, startup/readiness/liveness, logs/metrics/traces;
- clean install, upgrade, interrupted upgrade, restore, and uninstall tests.

Until these exist, use Docker assets only for the documented gates.
