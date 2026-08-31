# Self-hosting overview

Runku is designed so the complete application path can operate without an Aldemi or Runku cloud
service.

## Storage profiles

- SQLite is supported for local development and single-process conformance.
- PostgreSQL is the authoritative production data and metadata adapter.
- S3-compatible object storage holds distributed artifacts.
- NATS JetStream provides the distributed Full Node execution queue where that profile is enabled.

## Runtime profiles

- Safe V8 runs inside the Rust process with a deny-by-default Platform Ops surface.
- Dedicated Full Node runs only inside a host, VM, or Pod assigned to one trust domain.
- Shared untrusted Full Node requires Linux/KVM, Firecracker, jailer, namespaces, cgroup v2, and
  default-deny network policy.

Docker alone is not a supported hostile multi-tenant isolation boundary.

## Current distribution status

The source tree contains the storage, gateway, runtime, queue, S3, Full Node, and Firecracker
components and their conformance assets. It does not yet publish a certified general-purpose
`runku-server`, `runku-agent`, Compose production profile, or Helm release. Files under
[`deployments/`](../../deployments) are integration and conformance assets until those binaries are
released.

Do not infer production support from the existence of a test manifest. A supported profile must
include versioned packaging, configuration validation, health endpoints, graceful drain, backup,
restore, upgrade, observability, security limits, and failure-tested runbooks.
