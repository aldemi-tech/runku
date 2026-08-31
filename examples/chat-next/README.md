# Runku realtime chat

This application demonstrates an external Next.js frontend registering users with Better Auth,
invoking authenticated Runku Functions, persisting rooms and messages, and receiving realtime
updates.

Next.js is an example consumer, not a Runku requirement. The public SDK remains framework-
independent.

## Run locally

From the repository root:

```bash
make install-cli
pnpm install --frozen-lockfile
pnpm --dir examples/chat-next dev
```

`pnpm dev` runs Better Auth migrations, starts Next.js, and starts `runku dev`. No example-specific
Runku provisioning helper is required.

## Security boundary

- Browser requests contain an `rk_pub_*` publishable key and a user JWT.
- The server-only profile bootstrap route uses a separate `rk_sec_*` service key.
- `runku.auth.json` configures JWT/JWKS trust and contains no private key.
- Better Auth owns user registration and sessions; Runku validates issued JWTs.
- Room membership is checked by every room operation.

The room directory is realtime and permits users to join existing rooms without manually copying a
document ID. Each room is bounded to 100 members and retains the 200 most recent messages.

## Validation

```bash
make chat-example-check
make chat-example-e2e-check
```

The E2E gate uses two independent browser sessions, verifies key separation, joins a room from the
directory, exchanges messages in both directions, restarts Runku, checks persistence and realtime
recovery, and confirms that direct unauthenticated protocol access is rejected.
