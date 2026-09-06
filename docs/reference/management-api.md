# Management API reference

The Management API administers Platform Identity and one exact Product scope. It is intentionally
separate from the public Function API. Every protected call authenticates an `rk_at_v1_*` operator
session, reloads its current grants, and checks one explicit capability at Installation, Project,
or exact Environment scope.

The CLI is the supported interface for login, linking, development lifecycle, status, and logs.
Use the HTTP API for an administration UI/controller only when that integration preserves strict
JSON, operation identity, CAS, one-time secret, and reconciliation rules.

## Origins and transport

Authentication and Management may share an origin or use two canonical HTTPS origins. Discover
them from `GET /v1/auth/config`; do not assume the login URL is the lifecycle URL. The CLI stores
both and rejects redirects, credentials in URLs, ambient proxy substitution, and remote plaintext.

The server listener may use plaintext only on loopback unless a trusted external TLS termination is
explicitly configured. Expose it through a protected administrative route with no response caching,
bounded request/stream timeouts, and no direct listener access.

## Common request contract

Protected requests use exactly one bearer header:

```http
Authorization: Bearer rk_at_v1_...
Accept: application/json
```

State-changing Product routes that advertise idempotency also require exactly one:

```http
Idempotency-Key: opn_...
Content-Type: application/json
```

One `opn_*` identifies one canonical intent. Exact replay returns the original result where the
domain supports it; reuse for different path/body/scope/timestamp fails with conflict. CAS fields
(`expectedRevision`, `expectedHead`, current Release, etc.) prevent a stale operator from silently
overwriting newer state.

If a response is lost or a `RESULT_UNCERTAIN`/timeout-class failure is returned, query the matching
operation endpoint or current resource before retrying. A new operation ID means new intent and can
duplicate a committed change.

JSON uses camelCase and rejects unknown fields. Ordinary bodies are bounded to 16 KiB; Environment
configuration mutations to 72 KiB; Data Admin documents to 12 MiB; publication uses its versioned
manifest/artifact bound; object-console upload has a separate bounded body. Authorization is at
most 16 KiB. Secret-bearing and Data responses use `Cache-Control: no-store`.

## Authentication and operator sessions

| Route | Authentication | Durable effect / retry |
|---|---|---|
| `GET /v1/auth/config` | none | discover methods and optional Management origin; safe read |
| `GET /v1/auth/oidc/config` | none | public native-client OIDC configuration; safe read |
| `POST /v1/auth/exchange` | one-time invitation in body | consume invitation and create session atomically; do not replay blindly |
| `POST /v1/auth/oidc` | external bearer and optional first-link authority | verify/link identity and create/reconcile session |
| `POST /v1/auth/refresh` | rotating refresh token in body | invalidates prior refresh on success; uncertain response needs recovery login/reconciliation |
| `GET /v1/auth/me` | access bearer | reload current operator/grants |
| `GET /v1/auth/resources` | access bearer at authentication origin | bounded linkable Product Environment catalog |
| `GET /v1/auth/sessions` | access bearer | list caller's non-secret sessions |
| `DELETE /v1/auth/sessions/{session_id}` | caller, or installation `operators:manage` | idempotent revocation behavior by session state |
| `PUT /v1/auth/managed/operators/{operator_id}/grants` | authenticated managed gateway | monotonic source-owned grant reconciliation |

Invitation creation, operation lookup, and revocation use `/v1/access/invitations`,
`/v1/access/invitation-operations/{operation_id}`, and
`/v1/access/invitations/{invitation_id}`. Codes and newly issued confidential material are returned
only through their documented one-time response. See
[Platform operator identity](../auth/platform-identity.md) for exact OIDC, invitation, grant, and
session bodies and lifetimes.

## Environment lifecycle

Base path:

```text
/v1/projects/{project_id}/environments/{environment_id}
```

| Route | Capability | Contract |
|---|---|---|
| `GET <base>` | `environments:read` | portable desired/observed state and configuration revisions |
| `POST <base>` | `environments:manage` | create complete exact-scope configuration under idempotency |
| `PUT <base>` | `environments:manage` | replace complete configuration under expected revision |
| `POST <base>/archive` | `environments:manage` | move desired lifecycle to archived; compact Product serving stops |
| `POST <base>/restore` | `environments:manage` | restore desired lifecycle; serving resumes only when eligible Channel exists |
| `GET <base>/environment-operations/{operation_id}` | `environments:read` | reconcile create/update/archive/restore/materialize outcome |

Configuration is a complete portable replacement: name, Project-unique slug, logical region,
purpose, protection, location, and Workspace-target policy. Provider placement is not a Product
field. Desired/observed revisions let an external controller report convergence without claiming a
successful API response implies infrastructure materialization. See
[Environment lifecycle](../concepts/environment-lifecycle.md).

## Workspace, Release, Channel, and serving

| Route | Capability | Purpose |
|---|---|---|
| `POST <base>/workspace/publish` | `releases:publish` | publish canonical package with explicit Workspace HEAD CAS |
| `POST <base>/releases/{release_id}` | `releases:publish` | validate/freeze candidate as servable Release |
| `PUT <base>/channels/{channel}` | `channels:promote` | point a Channel through optional exact CAS |
| `POST <base>/channels/{channel}/rollback` | `channels:promote` | move to an eligible history target with required current CAS |
| `GET <base>/status` | `releases:read` | coherent Release/Channel snapshot |
| `GET/PUT <base>/serving-policy` | read / `channels:promote` | read or completely replace atomic/weighted desired policy |
| `GET <base>/serving-policy-operations/{operation_id}` | `releases:read` | reconcile desired/materialized serving operation |
| `GET <base>/schemas/compatibility` | `releases:read` | schema/index/Cron hash evidence for desired Release set |

`PUT serving-policy` requires `Idempotency-Key`, exact current policy revision, complete canonically
weighted Release set, and mode. The server rejects incompatible sets before persistence and serves
`environment:default` only from the converged policy. An API success for desired intent and an
observed ready revision are distinct signals.

Every selected request/subscription/work item pins one exact Release. See
[Serving policy](../concepts/serving-policy.md) and
[Releases and Workspaces](../development/releases-and-workspaces.md).

## Environment variables and secrets

| Route | Capability | Result |
|---|---|---|
| `GET <base>/configuration` | `configuration:read` | global revision and name-ordered variable/secret-reference entries |
| `GET <base>/configuration/history` | `configuration:read` | newest-first value-free audit page |
| `PUT <base>/configuration/{NAME}` | `configuration:manage` | set/rotate exact name under global CAS and idempotency |
| `DELETE <base>/configuration/{NAME}` | `configuration:manage` | delete exact name under global CAS and idempotency |

Example variable update (replace all placeholders with read/generated canonical values):

```sh
curl --fail-with-body \
  -X PUT \
  -H "Authorization: Bearer ${RUNKU_ACCESS_TOKEN}" \
  -H "Idempotency-Key: opn_..." \
  -H 'Content-Type: application/json' \
  --data '{
    "expectedRevision": 4,
    "kind": "variable",
    "value": "enabled",
    "changedAtMicros": "1780000000000000"
  }' \
  'https://management.example.com/v1/projects/prj_.../environments/env_.../configuration/FEATURE_CHECKOUT'
```

Secret PUT uses `"kind":"secret"`; its value appears only in the request and is never returned.
GET/history expose secret name, kind, revision, and timestamps without plaintext. Names are uppercase
ASCII/digit/underscore, cannot start `_`, and are ≤64 bytes. See
[Environment variables and secrets](../concepts/environment-configuration.md).

## Application Client credentials

| Route group | Read capability | Manage capability |
|---|---|---|
| `GET/POST <base>/application-clients` | `credentials:read` | `credentials:manage` for create |
| `GET/POST <base>/application-clients/{client}/credentials` | `credentials:read` | `credentials:manage` for create |
| `POST .../{credential}/reveal` | `credentials:read` | re-derive only the allowed verified publishable form |
| `POST .../{credential}/rotate` | — | `credentials:manage` |
| `POST .../{credential}/revoke` | — | `credentials:manage` |
| `DELETE .../{credential}` | — | `credentials:manage` |

Public, secret, and development credentials cannot exchange roles. Confidential material appears
only once; store it before acknowledging the operation and reconcile metadata after an uncertain
response. See [Application identity](../auth/application-identity.md).

## Object Storage administration

Routes under `<base>/buckets` manage provider-independent buckets, CORS/policy/lifecycle/quota,
scoped Product storage keys, object catalog, and bounded console transfers.

| Operation | Capability |
|---|---|
| list/get buckets, keys, objects, operation status | `storage:read` |
| create/update/archive bucket; issue/rotate/revoke key | `storage:manage` + operation id/CAS as defined |
| console object GET | `storage:read` |
| console object PUT/DELETE | `storage:manage` + operation id |

The Runku Object Storage Product endpoint is separate and uses scoped bucket access credentials,
not an operator access token. Its protocol is compatible with a bounded S3 subset; native SDK
breadth, multipart, and Product-driven provider backup are not implied. See
[Runku Object Storage](../concepts/object-storage.md).

## Runtime catalogs, data, schedules, and Cron

| Route | Capability | Boundary |
|---|---|---|
| `GET <base>/functions` | `releases:read` | bounded catalog from one verified effective artifact |
| `GET <base>/schema/tables` | `releases:read` | logical table/index catalog without physical IDs |
| `POST <base>/data/query` | `data:read` | bounded logical administrative query |
| `GET <base>/data/documents/{table}/{id}` | `data:read` | one canonical document |
| `POST <base>/data/documents/{table}` | `data:write` | deterministic insert under operation id |
| `PUT/DELETE <base>/data/documents/{table}/{id}` | `data:write` | exact document revision/OCC + operation id |
| `GET <base>/scheduled` | `schedules:read` | bounded durable queue/history without worker identity |
| `GET <base>/crons?target=...` | `cron:read` | code declaration + current activation |
| `PUT <base>/crons/{name}/activation` | `cron:activate` | enable/disable immutable declaration under CAS/idempotency |
| `GET <base>/cron-operations/{operation_id}` | `cron:read` | reconcile activation outcome |

Data Admin bypasses the application's Function API but not schema, scope, identity, CAS, or audit
boundaries. Treat writes as production data changes and use the same backup/change approval.

## Health, metrics, and logs

| Route | Capability | Meaning |
|---|---|---|
| `GET /health/live` | none | Management process liveness only |
| `GET /health/ready` | none | bounded authoritative Platform Identity PostgreSQL readiness |
| `GET <base>/instances/healthz` | `environments:read` | sanitized fixed-component Product health |
| `GET <base>/metrics` | `usage:read` | bounded fixed-name Product aggregate diagnostics |
| `GET <base>/logs` | `logs:read` | bounded exact-scope log page |
| `GET <base>/logs/follow` | `logs:follow` | NDJSON stream with repeated authorization checks |
| `GET <base>/logs/archive-status` | `logs:read` | archive coverage/frontier |
| `POST <base>/logs/prune` | `logs:prune` | bounded retention plan/apply behind verified frontier |

Health is not audit. Metrics are not billing. Logs are not application data authority. A runtime
with no Release can be healthy and `idle`; a log archive outage must block pruning, not Product
Mutation commits.

## Errors, retries, and audit

HTTP maps stable domain errors to invalid input, unauthenticated, forbidden, not found, conflict,
unavailable, uncertain, or internal/corruption classes. Automation branches on stable code and
current durable state. Broad guidance:

- GET reads are safe to retry, recognizing that state can advance;
- CAS conflict requires a new read and human/controller reconciliation;
- unavailable before a known commit can use bounded backoff;
- uncertain mutation requires operation lookup/current-state reconciliation;
- one-time secret responses require secure capture and metadata reconciliation, never issuance with
  a new ID as the first recovery action;
- corruption stops writes and triggers evidence preservation/recovery.

Platform Identity state changes write security audit in the same PostgreSQL transaction. Product
domains keep their own operation/audit records. Audit and logs omit bearer values, invitation codes,
credential secrets, configuration secrets, request bodies, and customer data.

The complete route/capability table and identity lifecycles are in
[Platform operator identity](../auth/platform-identity.md); stable CLI behavior is in
[CLI reference](cli.md).
