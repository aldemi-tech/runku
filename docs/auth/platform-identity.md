# Platform operator identity

Platform Identity authenticates the people and automation that administer a Runku installation.
It is independent from [Application Identity](application-identity.md): `rk_pub_*`, `rk_sec_*`, and
`rk_dev_*` identify application or development clients and never authorize Management API access.
Operator sessions use their own `rk_at_v1_*` access and `rk_rt_v1_*` refresh credentials.

## Implementation and distribution status

The current source tree implements the following coherent management slice:

- PostgreSQL 16+ authoritative storage and a SQLite conformance backend;
- first-owner bootstrap with a server-generated, single-use `rk_inv_v1_*` code;
- delegated invitations with installation, Project, or Environment scope, idempotent issuance
  operations, non-secret reconciliation, and revocation;
- short-lived access tokens, rotating refresh tokens, session listing, and revocation;
- optional external OIDC identity linking using the hardened discovery/JWKS verifier;
- a versioned HTTP API and `runku login` invitation/OIDC-token flows;
- native browser login through OIDC Authorization Code + PKCE and a loopback callback;
- authenticated Workspace publication, Release validation, Channel promotion/rollback, status, and
  historical/streaming Operational Logs for one configured Product Environment;
- transactional security audit records and process-local aggregate counters;
- a tagged `runku-server` Linux binary/image with `check`, `migrate`, `recover-bootstrap`, `serve`,
  and `version` modes;
- an executable PostgreSQL + external-OIDC integration campaign, with Keycloak used as its
  disposable reference fixture.

The compact `runku-server` distribution can compose Platform Identity with one initialized Product Environment
selected by `RUNKU_PRODUCT_ROOT`. That profile exercises the real local repositories, artifact
store, Gateway, runtime, background loops, release lifecycle, and logs behind authenticated remote
management. Tagged releases publish its Linux GNU ARM64/x86_64 archives and non-root OCI image. It
is not the distributed/HA installation package: multi-Environment orchestration, separated roles,
backup windows, and mixed-version support remain governed by the
[production-readiness contract](../self-hosting/production-readiness.md).

## Trust and data flow

```text
first server start ──writes protected invitation file──► initial owner
initial owner ──runku login + invitation──────────────► operator session
operator session ──creates scoped invitation─────────► another operator
external IdP ──signed user JWT──► Runku OIDC verifier ─► linked operator session
operator access token ──capability + scope check──────► Management operation
```

Runku never stores an invitation, access token, or refresh token in recoverable form. PostgreSQL
stores domain-separated HMAC-SHA-256 digests. External OIDC `sub` values are transformed into an
opaque keyed identifier before persistence. The Platform Identity pepper and OIDC subject pepper
are separate 256-bit secrets; changing either one without a planned migration invalidates the
corresponding credentials or identity links.

## Prerequisites

For the implemented source composition you need:

- the repository's pinned Rust toolchain;
- PostgreSQL 16 or newer reachable from the server process;
- an absolute private state directory writable only by the server identity;
- two independently generated 32-byte peppers when OIDC is enabled;
- TLS at a trusted reverse proxy before exposing the Management listener beyond loopback;
- optionally, one OIDC provider with an HTTPS issuer, asymmetric signing keys, exact audience, and
  a stable claim that distinguishes operator tokens.

The database role needs permission to connect, create the Platform Identity tables on first use,
and read/write those tables. Do not share its password with application code or the CLI.

## Configure and start the compact server

Generate the installation credential-verification pepper once and store it in a secret manager:

```sh
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n'
```

The result is URL-safe base64 without padding. Supply it without placing it in a command-line
argument:

```sh
export RUNKU_IDENTITY_DATABASE_URL='postgres://runku_management:REDACTED@postgres.example/runku_identity'
export RUNKU_PLATFORM_IDENTITY_PEPPER='REDACTED_URL_SAFE_BASE64_32_BYTES'
export RUNKU_STATE_DIRECTORY='/var/lib/runku'
export RUNKU_MANAGEMENT_LISTEN='127.0.0.1:3220'

runku-server check
runku-server migrate
runku-server serve
```

`check` parses and validates configuration without connecting to PostgreSQL or changing state.
`migrate` connects, verifies PostgreSQL compatibility, and applies the checksum-protected schema.
It is safe to repeat with the same binary and schema. `serve` also applies the same idempotent
schema check before listening.

Configuration is strict:

| Variable | Required | Contract |
|---|---:|---|
| `RUNKU_IDENTITY_DATABASE_URL` | one source | Platform Identity PostgreSQL URL with host and one database path for operators, grants, sessions, invitations, and audit; sensitive |
| `RUNKU_PLATFORM_IDENTITY_PEPPER` | one source | URL-safe base64, exactly 32 decoded bytes; sensitive |
| `RUNKU_IDENTITY_DATABASE_URL_FILE` | alternative | path to a file containing the same Identity URL; absolute, one-line, regular, non-symlinked |
| `RUNKU_PLATFORM_IDENTITY_PEPPER_FILE` | alternative | absolute one-line regular non-symlink file; mutually exclusive with direct pepper |
| `RUNKU_STATE_DIRECTORY` | yes | absolute path other than `/`; holds bootstrap material |
| `RUNKU_MANAGEMENT_LISTEN` | no | defaults to `127.0.0.1:3220` |
| `RUNKU_MANAGEMENT_TLS_TERMINATED` | no | exact `true` permits a non-loopback listener behind a trusted TLS boundary |
| `RUNKU_PUBLIC_MANAGEMENT_URL` | no | canonical public HTTPS Management origin returned by login discovery; literal-loopback HTTP is local-only |
| `RUNKU_PLATFORM_OIDC_CONFIG` | no | absolute path to a strict JSON file, at most 64 KiB |
| `RUNKU_PRODUCT_ROOT` | no | absolute initialized Product Environment root exposed by authenticated lifecycle routes |
| `RUNKU_PLATFORM_DATABASE_URL` | no | optional Environment-scoped PostgreSQL URL for Function documents, indexes, outbox, and schedules; sensitive; requires Product root |
| `RUNKU_PLATFORM_DATABASE_URL_FILE` | alternative | path to a file containing the same Function platform URL; absolute, one-line, regular, non-symlinked |
| `RUNKU_PRODUCT_ALLOWED_ORIGINS` | no | up to 64 exact comma-separated browser origins; requires Product root |
| `RUNKU_PRODUCT_AUTH_CONFIG` | no | Product-root-relative JWT descriptor without parent traversal; requires Product root |

`RUNKU_IDENTITY_DATABASE_URL` is the database connection string itself. Its `_FILE` alternative is
not another database: its value is only an absolute filesystem path, and Runku reads the connection
string from that file. Use exactly one of those two forms. The same rule applies to the optional
Function platform database and to the pepper. Secret files are bounded to 64 KiB and one canonical
line; missing, empty, oversized, symlinked, multiline, or conflicting inputs fail before a
connection. Unknown OIDC fields, malformed secrets, unsafe database schemes, relative state/config
paths, and a
non-loopback plaintext listener fail before readiness. `RUNKU_MANAGEMENT_TLS_TERMINATED=true` is an
assertion by the operator; Runku cannot verify the reverse proxy. Restrict the backend listener and
configure exact trusted-proxy behavior at the deployment boundary.

The Function platform database is independent of Platform Identity and has its own exact
Environment binding, readiness, least-privilege credential, and coordinated recovery contract. See
[Environment-scoped Function platform PostgreSQL](../self-hosting/product-postgresql.md).

Version 0.4.4 still accepts the deprecated `RUNKU_DATABASE_URL` and
`RUNKU_PRODUCT_DATABASE_URL` names, including their `_FILE` forms, for transition. Do not configure
a canonical and legacy name for the same role at once; even equal values fail closed as
`SERVER_SECRET_CONFIGURATION_CONFLICT`.

## Enroll the initial owner

On the first successful `serve`, while the database contains no operator, the server creates one
bootstrap invitation and writes it to:

```text
$RUNKU_STATE_DIRECTORY/bootstrap/initial-owner.code
```

The file is created with mode `0600` on Unix. Startup is idempotent: if the pending bootstrap exists,
the server requires the original file to remain present and never generates a second valid code.
The bootstrap expires after 24 hours; the next start revokes it and atomically creates a fresh code
while no operator exists.

If the protected file is lost before enrollment, stop the server, preserve the database/logs, and
run the explicit local recovery operation with the same database, pepper, and state directory:

```sh
RUNKU_BOOTSTRAP_RECOVERY_CONFIRM='replace-lost-initial-owner-code' \
  runku-server recover-bootstrap
```

Recovery atomically revokes every pending bootstrap, records `bootstrap.recover` in security audit,
persists one replacement digest, and writes a new `initial-owner.code`. It fails permanently after
the first operator exists. The confirmation is an accident-prevention phrase, not a credential;
authority comes from administrative access to the server configuration, pepper, state directory,
and PostgreSQL. Never expose this command through an HTTP endpoint or unattended startup flag.

Transfer the code through a protected local channel and read it into an environment variable. Do
not paste it into a CLI argument or shared shell history:

```sh
export RUNKU_INITIAL_OWNER_CODE="$(tr -d '\r\n' </var/lib/runku/bootstrap/initial-owner.code)"
runku login \
  --url https://runku.example.com \
  --device operator-laptop \
  --code-env RUNKU_INITIAL_OWNER_CODE
unset RUNKU_INITIAL_OWNER_CODE
```

For a server bound to literal loopback, `http://127.0.0.1:3220` is accepted. Every other origin must
use HTTPS. A successful exchange atomically creates the operator, owner grant, device session, and
audit event, then consumes the invitation. The server removes the bootstrap file on the next
startup after it observes completed initialization.

`runku login` stores one current server session in the platform user configuration directory:

| Platform | Default path |
|---|---|
| Linux | `$XDG_CONFIG_HOME/runku/credentials-v1.json` or `$HOME/.config/runku/credentials-v1.json` |
| macOS | `$HOME/Library/Application Support/runku/credentials-v1.json` |
| Windows | `%APPDATA%\runku\credentials-v1.json` |

`RUNKU_CONFIG_HOME` selects an absolute alternative directory, primarily for isolated automation.
The file is `0600` on Unix and inherits the user's profile ACL on Windows. It contains bearer
credentials, must never be committed or backed up unencrypted, and is currently a protected-file
fallback rather than a native keychain integration. A second login replaces the single stored
profile.

Session schema v2 records the authentication origin separately from the Management origin. Existing
schema v1 profiles remain readable and are upgraded after the next successful refresh or login.

## Invite another operator

Only a context that has `operators:manage` at the requested scope and every capability being
delegated may create an invitation. This prevents a scoped owner from escalating another operator
beyond the creator's own authority.

Until the invitation subcommand is added to the CLI, use the versioned API from a trusted operator
tool. Read the current access token from an approved credential helper; the following variables are
placeholders and must not be printed:

```sh
curl --fail-with-body \
  --request POST \
  --header "Authorization: Bearer $RUNKU_OPERATOR_ACCESS_TOKEN" \
  --header "Idempotency-Key: $RUNKU_INVITATION_OPERATION_ID" \
  --header 'Content-Type: application/json' \
  --data '{
    "operatorName": "release-operator",
    "role": "developer",
    "scope": {
      "kind": "environment",
      "projectId": "prj_00000000000000000000000001",
      "environmentId": "env_00000000000000000000000002"
    }
  }' \
  https://runku.example.com/v1/access/invitations
```

`RUNKU_INVITATION_OPERATION_ID` must be a newly generated canonical `opn_*` Operation ID retained
by the caller as non-secret operation metadata. The first committed response is `201` and contains
`operationId`, `invitationId`, scope, capabilities, timestamps, `code`, `secretShownOnce: true`, and
`replayed: false`; it has `Cache-Control: no-store, max-age=0`. Deliver the code once. It expires
after 30 minutes and cannot be recovered, replayed, or used after revocation.

An exact POST replay with the same operation and request returns `200`, the same non-secret
metadata, `secretShownOnce: false`, and `replayed: true`; it never includes `code`. Reusing an
operation ID for different operator, scope, role/capabilities, or other issuance content returns
`409 PLATFORM_INVITATION_OPERATION_REUSED`.

After an uncertain response, reconcile before producing more bearer material:

```sh
curl --fail-with-body \
  --header "Authorization: Bearer $RUNKU_OPERATOR_ACCESS_TOKEN" \
  "https://runku.example.com/v1/access/invitation-operations/$RUNKU_INVITATION_OPERATION_ID"
```

`404` proves no committed operation exists at the time of the authoritative read. `200` proves the
operation committed and returns metadata but never the code. If the caller did not durably deliver
the original code, revoke the unknown credential and issue a replacement with a new Operation ID:

```sh
curl --fail-with-body \
  --request DELETE \
  --header "Authorization: Bearer $RUNKU_OPERATOR_ACCESS_TOKEN" \
  "https://runku.example.com/v1/access/invitations/$RUNKU_INVITATION_ID"
```

Deleting a pending or already revoked invitation returns `204`, so the exact revocation is safe to
repeat. A consumed invitation returns conflict because revoking its code cannot disable the
operator/session created by consumption. Lookup and revocation reload current authority and require
`operators:manage` at every stored invitation scope. Operation IDs are correlation identities, not
credentials; knowing one never bypasses authorization.

For compatibility, a POST without `Idempotency-Key` retains the previous one-shot behavior. It is
not reconcilable and must not be used by unattended automation or any workflow that could retry
after losing the response.

Supported scope shapes are exact:

```json
{"kind":"installation","projectId":null,"environmentId":null}
{"kind":"project","projectId":"prj_...","environmentId":null}
{"kind":"environment","projectId":"prj_...","environmentId":"env_..."}
```

## Roles, capabilities, and scope

Roles are input conveniences. Runku expands them to explicit durable capabilities; authorization
checks capabilities and scope, not the role label.

| Role | Capabilities |
|---|---|
| `owner` | installation, Project, Environment, operator, Release, Channel, credential, log, usage, and backup management |
| `operator` | Environment, Release, Channel, credential, log, usage, and backup operations; no installation/operator ownership |
| `developer` | read/publish Releases, promote Channels, read credential metadata, read/follow logs |
| `observer` | read Releases, credential metadata, logs, and usage |

An installation grant contains every Project and Environment. A Project grant contains that Project
and its Environments. An Environment grant contains only the exact Project/Environment pair. A
Project or Environment grant never authorizes installation-wide work or a sibling resource.

Every lifecycle request reloads current grants. `releases:publish` protects publication and Release
validation; `channels:promote` protects promotion and rollback; `releases:read` protects status;
`logs:read` protects snapshots; and `logs:follow` protects streaming. The URL's Project and
Environment are checked against both the grant and configured Product Environment before product
state is accessed.

## Configure external OIDC

OIDC is optional. Invitation-only operation has no external IdP dependency. When enabled, create a
strict JSON file readable by the server identity:

```json
{
  "providerId": "workforce-main",
  "issuer": "https://identity.example.com/realms/operators",
  "discoveryUrl": "https://identity.example.com/realms/operators/.well-known/openid-configuration",
  "audience": "https://runku.example.com",
  "allowedOrigins": ["https://identity.example.com"],
  "discriminatorClaim": "runku_actor_type",
  "discriminatorValue": "operator",
  "algorithm": "RS256",
  "requiredType": "JWT",
  "subjectPepper": "REDACTED_URL_SAFE_BASE64_32_BYTES",
  "nativeClient": {
    "authorizationEndpoint": "https://identity.example.com/realms/operators/protocol/openid-connect/auth",
    "tokenEndpoint": "https://identity.example.com/realms/operators/protocol/openid-connect/token",
    "clientId": "runku-cli",
    "scopes": ["openid", "profile"],
    "resource": "https://runku.example.com"
  }
}
```

Set its absolute path in `RUNKU_PLATFORM_OIDC_CONFIG`. The verifier requires:

- exact HTTPS issuer and audience;
- one selected asymmetric algorithm: `RS256`, `PS256`, `ES256`, or `EdDSA`;
- optional exact JWT header `typ`;
- exact discriminator claim/value so an unrelated token class is rejected;
- bounded token lifetime and clock skew;
- discovery and `jwks_uri` origins in the HTTPS allowlist;
- no redirects, bounded bodies/timeouts, DNS/IP controls, cached last-known-good keys, and bounded
  unknown-`kid` refresh.

The IdP authenticates the human; Runku remains authoritative for grants, sessions, and resource
scope. The first OIDC login must consume a Runku invitation so a verified external subject cannot
self-enroll. The normal interactive flow is simply:

```sh
runku login
```

With no prior profile this queries `https://api.runku.app/v1/auth/config`. A self-hosted operator
uses `--url` for the installation's authentication origin. The server advertises invitation,
browser OIDC, and helper-token support plus an optional separate Management origin. If multiple
human methods exist, the CLI asks which to use; `--browser` is an explicit choice, not a normal
requirement. For first identity binding, provide the invitation through a protected environment
variable:

```sh
RUNKU_OPERATOR_INVITATION='rk_inv_v1_...' runku login \
  --url https://runku.example.com \
  --device operator-laptop \
  --browser \
  --code-env RUNKU_OPERATOR_INVITATION
```

The CLI obtains public native-client settings from Runku, generates fresh state and a PKCE S256
verifier, binds an ephemeral `127.0.0.1` callback, opens the authorization URL, checks callback
Host/state and any returned issuer, rejects duplicate security parameters, exchanges the code
against the fixed token endpoint without a client secret, and sends the resulting external access
token only to Runku's OIDC exchange. The browser sees success only after Runku validates that token,
commits the operator session, and persists the local profile. `--no-open` prints the authorization
URL to stderr for headless or controlled-browser automation while retaining those checks.

`nativeClient.resource` is optional. When present, the CLI sends the exact RFC 8707 resource
indicator in both the authorization and token requests. Use it when the provider requires an
explicit protected resource to issue a JWT access token with the configured Runku audience. The
value must be an HTTPS URL (literal-loopback HTTP is conformance-only), and it must agree with the
provider client/resource registration and top-level `audience`. Omitting it preserves the ordinary
OIDC flow for providers that issue the required JWT without a resource indicator.

For workload identity, an approved helper, or deterministic protocol conformance, the external
token can instead be supplied through an allowlisted environment variable:

```sh
RUNKU_OPERATOR_INVITATION='rk_inv_v1_...' \
RUNKU_EXTERNAL_OIDC_TOKEN='eyJ...' \
runku login \
  --url https://runku.example.com \
  --device operator-laptop \
  --code-env RUNKU_OPERATOR_INVITATION \
  --oidc-token-env RUNKU_EXTERNAL_OIDC_TOKEN
```

The invitation, external identity link, operator, grants, first session, and audit event commit in
one transaction. Later logins for the same configured provider and subject omit `--code-env`:

```sh
RUNKU_EXTERNAL_OIDC_TOKEN='eyJ...' runku login \
  --url https://runku.example.com \
  --device operator-laptop \
  --oidc-token-env RUNKU_EXTERNAL_OIDC_TOKEN
```

The external token is never written to Runku's credential file; only the resulting Runku access and
rotating refresh session is stored. Runku does not retain the IdP password, authorization code,
PKCE verifier, or external token.

Changing `providerId` creates a distinct trust namespace. Rotating `subjectPepper` makes existing
links unresolvable. Treat either as an identity migration requiring overlap or re-enrollment; do
not edit database identity rows.

`allowLoopbackHttp: true` permits exactly one literal-loopback HTTP origin for discovery/JWKS. It
exists only for local conformance, requires an HTTPS issuer identifier, and rejects `localhost`,
remote HTTP, or multiple origins. Never use it in a networked deployment.

### Choose and qualify an identity provider

Runku integrates with the OIDC protocol boundary, not with a Keycloak-specific API. Keycloak is
present in this repository because it provides a convenient, disposable way to issue a real signed
token and expose discovery and JWKS documents during a reproducible test. Its use in that test is
not a recommendation, certification, support preference, or claim that it is the best fit for an
installation.

Choose the identity system according to the installation's security, operations, compliance, and
user-lifecycle needs. Examples worth evaluating include
[authentik](https://docs.goauthentik.io/add-secure-apps/providers/oauth2/),
[ZITADEL](https://zitadel.com/docs/apis/openidoauth/endpoints), and
[Dex](https://dexidp.io/docs/) as a federation layer, as well as an OIDC provider the organization
already operates. These examples are illustrative, are not an equivalence matrix, and have not been
certified by Runku. Product features, deployment models, and token behavior change independently;
use each provider's current documentation and validate the exact deployed version.

Evaluate at least the following before selecting a provider:

| Area | Questions the installation must answer |
|---|---|
| Trust ownership | Is the IdP self-managed, managed, or federated, and who owns incidents, upgrades, and recovery? |
| OIDC contract | Does it publish stable discovery metadata, an HTTPS issuer, and an HTTPS `jwks_uri` whose values exactly match issued tokens? |
| Token profile | Can it issue an asymmetrically signed JWT with Runku's exact audience and a dedicated operator discriminator claim? |
| Subject stability | Is `sub` stable for the lifetime of the operator account, including rename, migration, and directory synchronization? |
| Human security | Are MFA, phishing-resistant authentication, enrollment recovery, lockout, and deprovisioning appropriate for platform operators? |
| Key operations | How are signing keys generated, protected, rotated, overlapped, cached, audited, and recovered? |
| Availability | What happens to new logins during an IdP outage, and is invitation-only recovery documented and protected? |
| Organization constraints | Do audit retention, data residency, privacy, licensing, capacity, and support meet local requirements? |
| Client flow | Which approved browser, device, helper, or workload flow obtains the external token without exposing it in shell history? |

Qualify the chosen provider in a non-production environment before enabling it:

1. Record the deployed provider version and retrieve its discovery document over the same network
   path Runku will use.
2. Create a dedicated Runku client/resource audience; do not reuse a token intended for another
   application.
3. Add a dedicated operator discriminator claim/value and verify ordinary application tokens do
   not contain it.
4. Pin the exact issuer, discovery URL, audience, allowed origins, algorithm, optional `typ`, and
   discriminator in `RUNKU_PLATFORM_OIDC_CONFIG`.
5. Exercise first enrollment with an invitation, a later linked login without an invitation,
   `/v1/auth/me`, refresh, session listing, and revocation.
6. Prove rejection of the wrong issuer, audience, discriminator, algorithm, signature, expired
   token, unknown key, redirect, and non-allowlisted JWKS origin.
7. Rotate a signing key under the provider's normal procedure and verify both the intended overlap
   window and removal of the retired key.
8. Document IdP outage, operator removal, compromised-account, subject-change, and Runku
   `subjectPepper` recovery procedures before production use.

Passing this qualification establishes compatibility only for that provider configuration and
version in that installation. It does not transfer to another deployment or make the provider a
Runku dependency. The normative external boundary is the
[OpenID Connect Discovery contract](https://openid.net/specs/openid-connect-discovery-1_0.html)
plus Runku's stricter verifier rules above.

## Sessions and HTTP endpoints

| Method and path | Authentication | Effect and retry |
|---|---|---|
| `GET /v1/auth/config` | none | returns versioned methods and an optional canonical Management origin; never returns secrets |
| `POST /v1/auth/exchange` | single-use invitation in JSON body | creates operator/session atomically; do not replay after success |
| `POST /v1/auth/oidc` | external bearer; invitation required only for first link | verifies OIDC and creates a Runku session |
| `GET /v1/auth/oidc/config` | none | returns exact issuer and public native-client endpoints/ID/scopes/optional RFC 8707 resource; never returns secrets |
| `POST /v1/auth/refresh` | current `rk_rt_v1_*` in JSON body | atomically rotates both tokens; reconcile an uncertain response before retry |
| `GET /v1/auth/me` | `rk_at_v1_*` bearer | reloads current operator and grants; safe to retry |
| `GET /v1/auth/sessions` | `rk_at_v1_*` bearer | lists non-secret sessions owned by the operator; safe to retry |
| `DELETE /v1/auth/sessions/{ops_*}` | `rk_at_v1_*` bearer | revokes own session; another operator requires installation `operators:manage` |
| `POST /v1/access/invitations` | `rk_at_v1_*` + delegated authority | with `Idempotency-Key: opn_*`, atomically creates or replays one issuance; code appears only on create |
| `GET /v1/access/invitation-operations/{opn_*}` | `rk_at_v1_*` + current `operators:manage` at stored scope | reconciles non-secret status; safe to retry |
| `DELETE /v1/access/invitations/{opi_*}` | `rk_at_v1_*` + current `operators:manage` at stored scope | idempotently revokes pending material; never reopens consumed identity |
| `POST /v1/projects/{project}/environments/{environment}/workspace/publish` | `releases:publish` | bounded canonical package publication with explicit Workspace CAS |
| `POST /v1/projects/{project}/environments/{environment}/releases/{release}` | `releases:publish` | validates the candidate and makes it servable |
| `PUT /v1/projects/{project}/environments/{environment}/channels/{channel}` | `channels:promote` | promotes through exact optional CAS |
| `POST /v1/projects/{project}/environments/{environment}/channels/{channel}/rollback` | `channels:promote` | rolls back through required exact-current CAS |
| `GET /v1/projects/{project}/environments/{environment}/status` | `releases:read` | reads a coherent Release/Channel snapshot |
| `GET .../logs` | `logs:read` | reads one bounded exact-scope page |
| `GET .../logs/follow` | `logs:follow` | streams NDJSON and rechecks the session/grant during the connection |
| `GET /health/live` | none | process liveness only |
| `GET /health/ready` | none | bounded authoritative PostgreSQL health |

JSON bodies are limited to 16 KiB; the canonical publication route has the protocol's explicit
manifest/artifact bound. Authorization headers are limited to 16 KiB, JSON rejects unknown fields,
and semantic request concurrency is bounded. Secret-bearing responses use `no-store`. All
Management API access reloads current grants; a stale token does not freeze old authorization
indefinitely.

Default lifetimes are 10 minutes for access, 30 days for rotating refresh, 30 minutes for delegated
invitations, and 24 hours for bootstrap. A successful refresh invalidates the prior refresh token.
Each login creates an independently revocable `ops_*` device session.

## Durable state, backup, and restore

Platform Identity schema v1 owns these PostgreSQL tables:

- `runku_platform_meta` and `runku_platform_migrations`;
- `runku_operators` and `runku_operator_grants`;
- `runku_operator_identities`;
- `runku_operator_invitations`;
- `runku_operator_sessions`;
- `runku_platform_audit`.

Schema v2 append-only adds `runku_operator_invitation_operations`, a revocation timestamp on
delegated invitations, and operation/invitation correlation columns plus an index on security
audit. The operation table stores only the canonical Operation ID, SHA-256 request fingerprint,
explicit installation/Project/Environment scope, invitation ID, creator, and timestamp. Its check
constraint rejects incomplete or mixed scope shapes. It never stores the invitation code or its
raw secret.

Schema versions carry a checksum and fail closed if the recorded version is unknown or its expected
definition differs. PostgreSQL transactions keep operator, grants, identity link, session, and audit
changes atomic.

A recoverable backup must include the complete PostgreSQL database, the Platform Identity pepper,
the OIDC subject pepper and configuration revision, and the pending bootstrap file if initialization
is incomplete. Store database and secret backup material under separate access control but one
coordinated recovery manifest. Restoring PostgreSQL without the original peppers preserves rows but
invalidates every associated credential/link. Restoring peppers without the matching database can
create unsafe identity assumptions and is unsupported.

After restore, start on loopback, verify `/health/ready`, authenticate a designated recovery
operator, inspect sessions and grants, verify OIDC key retrieval, then admit management traffic.
An older restore can resurrect a session or invitation that had later been revoked/consumed. As a
conservative incident response, rotate the peppers or explicitly revoke affected sessions and
pending invitations through the authenticated operator surface. Restore operation IDs with the
matching invitation and audit rows; losing only the operation table removes safe create
reconciliation and is not a valid partial restore.

## Upgrade and rollback

Before upgrading:

1. record the exact source commit and `runku-server version`;
2. back up PostgreSQL and both peppers and verify the backup;
3. run the new binary's `check` against its configuration;
4. run `migrate` during the declared maintenance window;
5. start on a restricted listener and verify liveness, readiness, invitation/session, and OIDC;
6. admit traffic and retain the old binary only within the schema compatibility decision.

Schema v1 initialization and the v1-to-v2 invitation-operation migration are additive. There is no
published mixed-version or downgrade window. After v2 is recorded, move forward; do not use an
older server as an operational rollback even though the added columns/table do not reinterpret v1
rows. Never drop tables or change migration rows to force an older binary to start.

## Failure handling

| Signal | Meaning | Safe response |
|---|---|---|
| `SERVER_CONFIGURATION_MISSING` | a required environment variable is absent/empty | fix configuration; no durable change occurred |
| `SERVER_DATABASE_URL_INVALID` | the Identity URL has an unsupported scheme, no host, or no database name | correct the Identity secret source; no connection was attempted |
| `SERVER_OIDC_CONFIG_INVALID` | unsafe JSON, issuer/origin/algorithm/pepper policy | reject startup; correct config without broadening trust |
| `SERVER_PLATFORM_DATABASE_UNAVAILABLE` | the Identity database connect, version, schema, or migration check failed | preserve logs; verify dependency/schema before retry |
| `SERVER_BOOTSTRAP_FILE_MISSING` | database has a pending bootstrap but protected file is absent | stop; preserve evidence, then restore the matching set or run the explicit recovery operation |
| `SERVER_BOOTSTRAP_RECOVERY_CONFIRMATION_INVALID` | the offline replacement phrase is missing or wrong | verify the intended installation and rerun with the exact documented confirmation |
| `SERVER_BOOTSTRAP_ALREADY_COMPLETE` | recovery was attempted after an operator exists | use an existing owner session or normal scoped operator invitation; bootstrap cannot reopen |
| `SERVER_BOOTSTRAP_RECOVERY_RESULT_UNCERTAIN` | replacement may have committed before the client lost the result | rerun the same offline recovery; it safely revokes any unknown pending replacement and emits one new code |
| `PLATFORM_AUTHENTICATION_FAILED` | malformed, wrong, expired, replayed, or revoked credential | reacquire/refresh; do not weaken authorization |
| `PLATFORM_ACCESS_DENIED` | valid operator lacks capability at exact scope | change the grant deliberately; do not use an application key |
| `PLATFORM_IDENTITY_RESULT_UNCERTAIN` | commit may have succeeded | reconcile session/invitation/audit before creating new secret material |
| `PLATFORM_INVITATION_OPERATION_REUSED` | one `opn_*` was presented with different issuance content | stop; retain both requests as evidence and allocate a new ID only for a deliberate new operation |
| `PLATFORM_IDENTITY_STORAGE_CORRUPT` | schema/persisted invariant failed | stop writes and restore/investigate; never edit rows ad hoc |

`SERVER_PLATFORM_DATABASE_UNAVAILABLE` is an older stable error code: in this table it means the
database selected by `RUNKU_IDENTITY_DATABASE_URL`, not the Function platform database. The code is
preserved in 0.4.4 so existing monitoring does not silently break.

Do not log request bodies, Authorization headers, codes, tokens, peppers, DSNs, raw external
subjects, or configuration file contents. Audit records intentionally retain IDs, operation,
outcome, actor/subject, and time, not bearer material.

## Evidence

The fast gate is:

```sh
make platform-identity-check
```

It runs domain/repository tests on SQLite, Management HTTP tests, CLI parser/process tests, and
strict Clippy for all affected crates. The external-provider campaign is explicit because it starts
containers and a server process. Its target name identifies the concrete fixture rather than the
product's integration boundary:

```sh
make platform-identity-keycloak-check
```

That campaign starts pinned PostgreSQL 16 and Keycloak 26.7.3 on loopback, imports a deterministic
test realm, obtains a real RS256 token, fetches discovery and JWKS through Runku's bounded local
OIDC path, enrolls an invited operator through the CLI, logs in again through the linked identity,
verifies `/me`, proves idempotent invitation replay/conflict and uncertain-response
reconcile/revoke behavior, and rejects credential replay and token tampering.

The complete Product campaign is:

```sh
make platform-lifecycle-keycloak-check
```

It adds an actual browser Authorization Code + PKCE flow and covers publish, replay, Release,
promotion, invocation, historical and streaming logs, replacement, rollback, stale CAS,
capability/scope denial, live session revocation, and linked-identity recovery. The exact commands
and assertions are documented in
[Authenticated remote lifecycle](../operations/remote-lifecycle.md#reproducible-acceptance-campaign).

Keycloak was selected for this exercise because a pinned disposable container and imported realm
make the evidence reproducible without an external account. The campaign evaluates Runku's
standards path against one real provider implementation; it does not compare identity products,
certify Keycloak, validate a production topology, or imply a preferred provider. Keycloak's
development mode, local HTTP endpoint, imported password, and direct grant are confined to this
fixture and must not be copied into a production installation. Use the qualification procedure
above with the identity provider actually selected for the installation.
