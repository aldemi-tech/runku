# Application identity

Runku separates application identity from end-user identity. OAuth/OIDC identifies a functional
user; Application Keys identify the software context invoking Runku.

## Application Keys

- `rk_pub_*` identifies browser, mobile, desktop, or other distributable clients. It is not a secret
  and receives only policies explicitly allowed for public applications.
- `rk_sec_*` authenticates trusted server processes, backend integrations, agents, and CI jobs. It
  must never enter a browser bundle or mobile binary.
- `rk_dev_*` authorizes development operations such as Workspace synchronization and freeze. It does
  not invoke application Functions.

An Environment can have multiple named keys. Keys support one-time reveal, overlapping rotation,
independent revocation, and per-key logs and metrics.

## Functional identities

An invocation policy selects one of these modes:

- `none`: no functional identity is required;
- `optional`: no principal is accepted, or a supplied principal must validate;
- `guest`: Runku issues a bounded anonymous identity;
- `user`: an external JWT/OIDC identity is required;
- `service`: a trusted service identity is required.

Runku validates issuers, audience, time bounds, signatures, and JWKS snapshots. It does not replace
the application's identity provider. Better Auth, an enterprise IdP, or another OAuth/OIDC provider
can issue the user token.

## HTTP shape

A public client sends its publishable key using the public application-key header and a guest or
user token using `Authorization: Bearer`. A server client uses its service key from server-only
configuration and may also forward an end-user bearer token when the Function policy requires user
context.

Key type, functional identity, Function visibility, target, and Environment protection are all
checked server-side.

## Client design and browser/server boundary

An Application Client defines kind and maximum scopes; each credential receives a subset. Create
separate clients for browser apps, backends, CI, and integrations so rotation/compromise is bounded.

| Runtime | Credential | Functional token |
|---|---|---|
| Browser/mobile/desktop | `rk_pub_*` | guest or user JWT |
| Trusted BFF/backend | `rk_sec_*` | delegated user JWT or service identity |
| Workspace/Release automation | `rk_dev_*` | not an invocation credential |

A valid JWT without an Application Key is rejected. Never put `rk_sec_*` or `rk_dev_*` in public
environment prefixes, bundles, HTML, URLs, mobile binaries, analytics, or browser storage.

`optional` authentication accepts either no functional principal or a validated one; code must
handle both explicitly.

## JWT/OIDC trust

Configuration fixes issuer, audience, algorithms, JWKS policy, claim mapping, clock tolerance, and
maximum lifetime. Discovery/JWKS access is bounded, HTTPS-only outside loopback, redirect-controlled,
size-limited, and last-known-good cached. Signing-key rotation needs bounded old/new overlap without
algorithm/key-type confusion.

Authentication does not grant ownership. Functions still enforce membership, roles, and
resource-level policy.

## Rotation, revocation, and incident response

1. create a replacement while the old credential remains active;
2. deliver it through the correct public/secret channel;
3. deploy and verify logs for the replacement credential ID;
4. revoke the old key and monitor stale consumers;
5. delete only after revocation and evidence/rollback policy.

Secret material is revealed once and belongs in a secret manager. Lost secret keys are replaced,
not recovered. For exposure, scope by client/credential/Environment, rotate/revoke, inspect
correlated logs, rotate downstream secrets when needed, and preserve evidence. Log pruning does not
revoke credentials or erase exported/backed-up copies.
