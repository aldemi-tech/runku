# Runku Field Board

This example is an integration lab for `@runku/react`, not a starter template. It exercises:

- Server Component Query preloading and lossless hydration;
- one shared browser Realtime Query subscription;
- browser and Next.js Server Action Mutations;
- browser and server Actions;
- browser and server upload/download through Application File grants;
- generated `api.tasks.list` and `serverApi.files.serviceHealth` references;
- exclusion of internal Functions and service-auth Functions from the browser tree.

From a source checkout, install the pinned toolchain and dependencies, then run:

```sh
make toolchain
pnpm install
pnpm --filter @runku/example-field-board-next dev
```

The example builds and launches the CLI from the current Rust workspace; it does not use an older
global `runku` executable accidentally. Set `RUNKU_BIN` to an exact current binary when a gate or
local workflow has already built one; the launcher rejects a version mismatch before opening local
state. Existing local state is version-sensitive: preserve it and use the same or a compatible CLI
rather than deleting `.runku/` as a repair step.

`runku dev --prepare` creates and reconciles `.env.local`, including the local Application Keys.
The checked-in `.env.example` documents the variable names only; manually copied placeholder keys
are invalid and will be replaced only through the CLI's explicit reconciliation rules.

Open `http://127.0.0.1:3001`. The public board deliberately uses `auth: "none"` so the SDK paths
can be inspected without an identity provider. Do not copy that authorization policy into a private
application. The service-only health Action is emitted only in `serverApi`; invoking it also requires
a valid service bearer, which this compact example does not mint.

Uploads are one-shot and are not automatically retried. The example stores only immutable File IDs
in documents; transfer bearer grants remain short-lived. This example uses Application Files, not
the separate S3-compatible Object Storage surface.
