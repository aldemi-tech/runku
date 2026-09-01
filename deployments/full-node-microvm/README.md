# Full Node microVM implementation assets

This directory contains implementation-level assets for the shared-untrusted Full Node Agent
profile. It is not a standalone product installer and is not required by Safe V8 or dedicated Node
deployments.

## Assets

- `runku-init`: minimal guest init that mounts proc/sys/tmp, configures the guest point-to-point
  network from kernel arguments, exports a one-worker IPC token, and starts the Node runner.
- `agent-conformance.Dockerfile`: local Kubernetes/Agent campaign harness. It expects separately
  prepared assets/controller scripts and is not an API/server image.

The controller uses Firecracker/jailer-specific variables and scripts because this profile
implements the VM boundary with that technology. Product-level roles and manifests remain named
Full Node Agent/microVM so the isolation implementation can evolve without redefining Runku.

## Trust boundary

The host Agent/controller are privileged infrastructure. User code executes only inside a verified
guest worker. Kernel, rootfs, VMM, jailer, controller, OCI image reference, and policy are pinned and
verified before readiness. Each worker is single-flight. Timeout, cancellation, protocol loss, or
uncertain result causes destructive replacement before the slot returns to service.

The IPC token is a secret and must not appear in this directory, process arguments visible outside
the boundary, image layers, or logs. Host assets/controller are root-owned and not writable by the
Agent execution identity.

## Guest contract

Kernel arguments provide `runku.ip`, `runku.gateway`, and `runku.token`. The init process rejects
missing values, configures `eth0/30`, installs the default route, and starts
`runner.mjs --serve-tcp 32110`. Guest scratch is tmpfs and is discarded with the worker.

## Network and resources

The controller enforces worker vCPU/memory/cgroup/PID/filesystem limits and egress mode
`none|public|restricted`. Restricted destinations are validated by policy, DNS, port, redirect, and
private-range controls. Host NetworkPolicy/firewall is defense in depth, not a replacement for the
broker/controller policy.

## Conformance and production acceptance

Conformance prepares pinned assets, checks authenticated handshake/prewarm, executes concurrent
work, cancellation/replacement, queue redelivery, CPU/RSS and routing tests, and preserves raw
environment/output. Production support additionally requires published Agent/server images,
versioned config, asset update/rollback, drain, autoscaling, observability, incident runbooks, and
the complete [readiness contract](../../docs/self-hosting/production-readiness.md).
