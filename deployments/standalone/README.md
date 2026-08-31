# Standalone Full Node host

`DedicatedHost` executes Node.js directly only when the machine, VM, or Pod belongs to one trust
domain. It must not mix untrusted tenants.

Shared Full Node hosts require Linux x86_64, KVM, cgroup v2, network namespaces, nftables,
Firecracker, jailer, a pinned kernel and root filesystem, and a root-owned controller. The Agent
must validate the exact artifact digest and network policy before acknowledging work.

[`firecracker.env.example`](firecracker.env.example) documents the environment variables consumed
by the current controller and conformance tooling. It is not a complete production configuration.
