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
