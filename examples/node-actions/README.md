# Full Node Actions example

This example validates the file-level `"use runku node"` boundary with Node.js `node:crypto` and the
external `pngjs` dependency. It creates and decodes a real PNG, returns canonical bytes through the
public protocol, and composes Safe and Node Functions.

## Run locally

```bash
make install-cli
pnpm install --frozen-lockfile
pnpm --dir examples/node-actions dev
```

Run the complete self-contained gate with:

```bash
make node-example-check
```

The gate verifies invalid and valid Application Keys, contract rejection, typed SDK calls,
`node:crypto`, external npm execution, Safe-to-Node and Node-to-Safe calls, Node-to-Mutation-to-Query,
persistence after restart, and actual `runAfter` execution.

`package-lock.json` remains part of this example because the Full Node OCI builder installs
production dependencies with `npm ci --omit=dev` and lifecycle scripts disabled. pnpm manages the
source workspace and SDK links; the npm lock defines the immutable runtime dependency graph.

PostgreSQL is intentionally not an example dependency. Storage conformance validates PostgreSQL
separately from this runtime scenario.

## Source map and scope

- `images.ts`: Node directive, built-in, npm package, canonical bytes;
- `bridge.ts`: Safe↔Node Action composition and internal visibility;
- `data.ts`: Safe Mutation/Query with OCC persistence;
- scheduling: Node Action creates a pinned Safe Mutation invocation.

Local Node is a development profile; this example does not prove shared-host isolation or a
production Agent. Reinstall the CLI after core changes, use Node 20.18.1+, preserve both workspace
and npm locks, and stop another `runku dev` using the example root. Use `RUNKU_BIN` when the gate
needs an explicit repository-built binary.
