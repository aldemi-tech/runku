# Authenticated remote lifecycle

This runbook operates one Product Environment through `runku-server` using the session created by
`runku login`. It covers the complete source acceptance profile: publish, validate, promote,
invoke, inspect/follow logs, publish a replacement, and roll back. Application invocation still
uses `rk_pub_*` or `rk_sec_*`; operator lifecycle operations use `rk_at_v1_*` through the protected
CLI session file. The credentials are intentionally not interchangeable.

## Prepare the Product Environment

The current source composition exposes one initialized Product Environment per server process.
Prepare it once with its own persistent root and listener:

```sh
runku init --root /var/lib/runku/product --workspace local --listen 127.0.0.1:3210
runku dev --root /var/lib/runku/product --prepare
```

`--prepare` initializes repositories and Application Credentials but does not leave a development
server running. Protect the complete root as authoritative product state. Do not put it on an
ephemeral container filesystem.

Configure the Management server with PostgreSQL-backed Platform Identity and the Product root:

```sh
export RUNKU_DATABASE_URL='postgres://runku_management:REDACTED@postgres.example/runku'
export RUNKU_PLATFORM_IDENTITY_PEPPER='REDACTED_URL_SAFE_BASE64_32_BYTES'
export RUNKU_STATE_DIRECTORY='/var/lib/runku/platform'
export RUNKU_PRODUCT_ROOT='/var/lib/runku/product'
export RUNKU_MANAGEMENT_LISTEN='127.0.0.1:3220'

runku-server check
runku-server migrate
runku-server serve
```

The Product Gateway starts lazily after the first successful Channel promotion. The server keeps
the Product process lease, refreshes serving catalogs, and stops the listener/background tasks when
the server exits. A second process cannot own the same Product root.

## Enroll and select an operator session

Bootstrap the first owner with the protected invitation written by the first server start, or
enroll a delegated operator. With an external OIDC provider configured, use the browser flow:

```sh
runku login
```

The CLI offers a prior authentication origin when present, otherwise defaults to
`https://api.runku.app`, discovers the available methods, and displays the Management origin that
will be stored. For a self-hosted installation or automation, make the choice explicit:

```sh
RUNKU_INVITATION='rk_inv_v1_...' runku login \
  --url https://runku.example.com \
  --device release-laptop \
  --browser \
  --code-env RUNKU_INVITATION
unset RUNKU_INVITATION
```

Later logins for the linked external identity omit the invitation. `--browser` forces browser OIDC
but is not required for the interactive flow. Invitation-only installations can select the hidden
prompt or use `--code-env` for automation. The current CLI stores one active profile containing
separate authentication and Management origins. Use an absolute `RUNKU_CONFIG_HOME` when automation
must isolate profiles.

Every `--remote` project command obtains the server origin from that stored profile and the exact
Project/Environment from `--root`. It rejects a malformed/symlinked session file, retries once with
the rotating refresh token after a `401`, safely replaces the file, and never falls back to an
Application or Development key.

## Publish and promote the first Release

Build locally and capture paths/IDs from JSON instead of guessing output names:

```sh
runku build --root /var/lib/runku/product >build.json
manifest_path="$(jq -r .manifestPath build.json)"
artifact_path="$(jq -r .artifactPath build.json)"
release_id="$(jq -r .releaseId build.json)"
```

Remote publication always requires an explicit Workspace HEAD precondition. Use `empty` only for
the first publication of that Workspace:

```sh
runku publish --remote --root /var/lib/runku/product \
  --manifest "$manifest_path" \
  --artifact "$artifact_path" \
  --expected-head empty >publish.json
revision_id="$(jq -r .revisionId publish.json)"

runku release --remote --root /var/lib/runku/product \
  --release "$release_id" >release.json

runku promote --remote --root /var/lib/runku/product \
  --channel stable \
  --release "$release_id" \
  --expected empty >promote.json

runku status --remote --root /var/lib/runku/product
```

Publication sends one bounded canonical binary frame. The server validates Project binding,
manifest canonicality, artifact digest/format, Workspace CAS, and operator scope before moving the
Workspace pointer. Repeating the same package and original precondition returns the same revision
with `replayed: true`; it does not create a second Release. A different package with a stale head
returns exit `4`.

Release validation and promotion are separate. A successful publication is not traffic. A
successful Release outcome must be `servable`; promotion then makes the target `active` and moves
the Channel through repository CAS. Serving refresh is asynchronous, so probes should retry for a
bounded interval until the reported Release ID matches the promoted binding.

Invoke through the Product listener using an Application key and an explicit target:

```sh
curl --fail-with-body \
  --request POST \
  --header 'Content-Type: application/json' \
  --header "x-runku-key: $RUNKU_KEY" \
  --data '{
    "version": 1,
    "target": "channel:stable",
    "function": "version.current",
    "arguments": {"type":"null"}
  }' \
  http://127.0.0.1:3210/v1/query
```

The `rk_at_v1_*` operator token is not accepted by the Product Gateway as an Application key.

## Historical and realtime logs

Read a bounded historical page with exact filters:

```sh
runku logs --remote --root /var/lib/runku/product \
  --release "$release_id" --limit 100

runku logs --remote --root /var/lib/runku/product \
  --after logc_123 --request req_... --stream platform
```

Follow uses one long-lived NDJSON response rather than issuing a client HTTP request every 250 ms:

```sh
runku logs --remote --root /var/lib/runku/product \
  --release "$release_id" --after logc_123 --follow
```

The server performs bounded repository reads behind that connection and reloads the current
operator session/grants before each page. It never accepts Project or Environment IDs from a log
record as authorization input. Revoking the device session, removing `logs:follow`, or disabling
the operator terminates the stream; a client cannot continue seeing new records with authority
that has been withdrawn. Snapshot reads require `logs:read`; streaming additionally requires
`logs:follow`.

Operational events include exact Project, Environment, Release, Function, request, invocation,
Application Client, and credential attribution. They exclude keys, JWTs, arguments, results, and
secret values. The Product profile stores its authoritative local log journal in its dedicated
SQLite database; OTLP export can ship records to an external log backend without putting log
payloads in Platform Identity PostgreSQL.

## Promote a replacement and roll back

After changing source, build a new immutable package and use the observed previous revision:

```sh
runku build --root /var/lib/runku/product >build-v2.json
runku publish --remote --root /var/lib/runku/product \
  --manifest "$(jq -r .manifestPath build-v2.json)" \
  --artifact "$(jq -r .artifactPath build-v2.json)" \
  --expected-head "$revision_id" >publish-v2.json

release_v2="$(jq -r .releaseId build-v2.json)"
runku release --remote --root /var/lib/runku/product \
  --release "$release_v2" --against stable
runku promote --remote --root /var/lib/runku/product \
  --channel stable --release "$release_v2" --expected "$release_id"
```

Rollback requires the exact current Channel binding and never bypasses compatibility:

```sh
runku rollback --remote --root /var/lib/runku/product \
  --channel stable \
  --expected "$release_v2" \
  --to "$release_id"
```

Rollback changes future target resolution. Already pinned invocations/scheduled work remain on
their exact Release, and data/schema effects are not undone. If the expected Release is stale,
exit `4` means another operator changed the Channel; re-read status before taking further action.

## Authorization and failure behavior

| Operation | Required capability | Exact scope |
|---|---|---|
| status | `releases:read` | URL Project/Environment |
| publish and Release validation | `releases:publish` | URL Project/Environment |
| promote and rollback | `channels:promote` | URL Project/Environment |
| log snapshot | `logs:read` | URL Project/Environment |
| log follow | `logs:follow` | URL Project/Environment, rechecked during stream |

Authentication occurs before the Product adapter is called. A valid operator without the
capability receives `403`; a malformed/expired/revoked session receives `401`; a different
configured Product scope is not opened. Product errors are sanitized as invalid, not found,
conflict, unavailable, or corruption without leaking paths, tokens, source, or stored values.

Recovery rules:

- `401` after the single refresh attempt: run `runku login` again; do not replace it with `rk_sec`;
- `403`: request the minimum missing capability at the intended scope;
- exit `4`: fetch status/current Workspace state and reconcile CAS intent;
- exit `5`: verify server/storage health and retry exact idempotent bytes with bounded backoff;
- exit `6`: stop writes, preserve Product state, and follow corruption/restore procedures;
- interrupted publish: repeat the same canonical package and precondition, then inspect `replayed`;
- interrupted promotion/rollback: read status before retrying.

## Reproducible acceptance campaign

Run the complete Docker/browser/runtime gate explicitly:

```sh
make platform-lifecycle-keycloak-check
```

The campaign builds the CLI/server once, starts disposable PostgreSQL and an OIDC provider,
drives an actual browser, and proves:

- invitation bootstrap without an IdP;
- Authorization Code + PKCE, an incorrect password rejection, and invitation-bound enrollment;
- invitation replay rejection and linked-identity re-login;
- authenticated publish/replay/release/promote/invoke/log snapshot/log stream;
- a second Release, exact Channel CAS, and rollback behavior;
- missing authentication, insufficient capability, and cross-Environment denial;
- live log-stream termination after session revocation and recovery through OIDC re-login.

The repository uses Keycloak only as a disposable standards fixture. The exercised product
contract is OIDC; provider selection and qualification remain an installation decision. This gate
is intentionally separate from `ci-check` because it starts Docker, a browser, a runtime, and the
full lifecycle. Routine CI only compiles and performs bounded static/package validation.
