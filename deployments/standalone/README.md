# Standalone deployment profile

Standalone means a dedicated machine or VM running a complete Runku instance or one product role.
The supported server package and service-unit templates are not published yet; this document fixes
the required trust and host contract.

## Product topology

A small installation may combine API, background, and management into an `all` role with external
PostgreSQL/object storage. A resilient installation separates roles and stores. Full Node is
optional.

## Full Node trust choices

- **Dedicated:** Node executes directly only when the entire machine/VM/service account belongs to
  one trust domain. Do not mix untrusted applications.
- **Shared untrusted:** requires Linux x86_64, KVM, cgroup v2, PID/mount/network namespaces,
  nftables, verified VMM/jailer/kernel/rootfs/controller assets, default-deny egress, and bounded
  single-flight workers.

[`full-node-microvm.env.example`](full-node-microvm.env.example) documents the current microVM controller
contract. It contains no token and is not complete server configuration. Technology-specific names
remain in this implementation-level file because they map to real controller variables.

## Host hardening requirements

- dedicated unprivileged service identities for ordinary roles;
- root-owned configuration/controller/assets; secrets `0600` or secret-manager mounted;
- read-only binaries/assets and explicit data/cache/scratch paths;
- TLS/private dependency networking and exact firewall flows;
- file descriptor, PID, CPU, memory, disk, and cgroup limits;
- time synchronization, log rotation, audit, and backup agents;
- graceful service stop/drain and startup dependency/readiness ordering;
- verified packages, checksums, SBOM, provenance, and update procedure.

## Operational acceptance

Before support: clean install on a documented OS/architecture; service restart/host reboot; process
kill and dependency outage; backup/total-loss restore; `N-1 → N`; capacity/overload; credential and
isolation-asset rotation; uninstall with explicit data-retention choice.
