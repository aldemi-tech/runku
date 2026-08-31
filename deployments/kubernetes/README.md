# Kubernetes integration

The manifests in this directory reproduce the distributed Full Node conformance topology: Gateway,
NATS, S3-compatible artifacts, Linux/KVM Agents, replacement, and scale tests.

They contain test placeholders, local credentials, `emptyDir`, and non-production image policies.
Before becoming a supported deployment package they require released server and Agent images,
external secrets, TLS, persistent and replicated dependencies, health probes, disruption budgets,
resource limits, topology constraints, and upgrade-tested packaging.

Only the Agent requires KVM and microVM lifecycle privileges. The Gateway must not receive those
host permissions.
