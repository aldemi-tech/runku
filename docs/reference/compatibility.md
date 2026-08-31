# Compatibility

Runku versions contracts at every boundary that can outlive one process:

- public HTTP and WebSocket protocols;
- canonical values, document IDs, and index keys;
- Release manifests and artifacts;
- runtime and Platform Ops versions;
- Workspace and development administration protocols;
- generated TypeScript API contracts.

Unknown versions fail closed. A client-selected Release is served only while its contract and
runtime remain supported. Channel routing cannot silently replace an explicit incompatible Release.

The source line currently reports version `0.1.0` and has not established a stable compatibility
window. The first stable release will publish an exact CLI, server, agent, SDK, protocol, storage,
and runtime support matrix.
