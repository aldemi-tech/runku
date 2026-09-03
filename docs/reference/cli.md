# CLI reference

The `runku` CLI manages the implemented local Environment lifecycle and remote Workspace client
operations. Arguments are strict: unknown flags, duplicated singleton flags, missing values,
unexpected positional arguments, and malformed IDs fail with exit code `2`.

All project commands default `--root` to the current directory. Run automation from the application
root or pass one explicit absolute/relative project path.

## Output contract

Successful non-streaming commands write compact JSON to stdout. Log queries write JSON Lines.
Failures write a stable machine code plus a human explanation to stderr:

```text
error: LOCAL_PROCESS_STATE_INVALID
message: Runku could not open a valid initialized project in the selected directory.
hint: Run from the project root or pass --root PATH. ...
```

Automation should branch on exit code and `error:` code, not localized/human text. Secret material,
source content, user arguments, manifests, artifacts, pepper, and user-supplied paths are not
interpolated into generic failure explanations.

## Exit codes

| Code | Class | Retry/response guidance |
|---:|---|---|
| 0 | Complete | Parse stdout and persist relevant IDs/cursors |
| 1 | Internal/process/signal failure | Preserve evidence; retry only after understanding state |
| 2 | CLI usage invalid | Fix arguments; retrying unchanged input is unsafe/noisy |
| 3 | Input, state, source, or package invalid | Correct input or restore valid state |
| 4 | Conflict/compatibility/CAS failure | Re-read current state and reconcile intent |
| 5 | Dependency/listener temporarily unavailable | Retry with bounded backoff after dependency check |
| 6 | Durable corruption/inconsistency | Stop writes, preserve state, diagnose/restore |
| 7 | Authentication failed | Replace/rotate the expected credential; do not broaden access |
| 8 | Policy denied | Correct target, Environment protection, scope, or operation |
| 9 | Outcome uncertain | Reconcile by operation ID/current state before any retry |

## Platform operator login

### `runku login`

```sh
runku login

RUNKU_INVITATION_CODE='rk_inv_v1_...' runku login \
  --url https://api.example.com \
  --code-env RUNKU_INVITATION_CODE

RUNKU_OIDC_TOKEN='eyJ...' runku login \
  --url https://api.example.com \
  --device operator-laptop \
  --oidc-token-env RUNKU_OIDC_TOKEN

RUNKU_INVITATION_CODE='rk_inv_v1_...' runku login \
  --url https://api.example.com \
  --device operator-laptop \
  --browser \
  --code-env RUNKU_INVITATION_CODE
```

With no flags, `login` uses `https://api.runku.app`. If a prior profile exists, an interactive
terminal first asks whether to reuse its authentication server. The CLI calls `/v1/auth/config`,
shows the separate authentication and Management origins, and offers the methods actually
advertised by that server. Browser OIDC is the recommended default when both OIDC and an invitation
are available. Invitation input is hidden; automation should continue to use `--code-env`.

`--url` overrides the authentication origin, not necessarily the Management origin. Discovery may
return one canonical `managementEndpoint`; otherwise the queried origin serves both roles. New
profiles store both origins: refresh/login requests return to the authentication server, while
lifecycle and log requests go only to the Management server. Both must be exact HTTPS origins or
literal-loopback HTTP origins. Redirects, URL credentials, paths, queries, fragments, ambient proxy
configuration, and remote plaintext HTTP are rejected.

`login` exchanges one server-issued, single-use bootstrap/operator invitation or a configured
external OIDC token for an independently revocable device session. First OIDC enrollment supplies
an invitation plus browser/external authentication; a linked identity omits the invitation.
`--device` overrides the bounded audit label derived from the local computer name. `--code-env`
must name an uppercase `RUNKU_*` variable; the code is never accepted as an argument.

The response is bounded to 16 KiB and must contain valid `rk_at_v1_*` and `rk_rt_v1_*` credentials.
One current profile is stored in the platform user configuration directory as described in
[Platform operator identity](../auth/platform-identity.md#enroll-the-initial-owner). On Unix the
file is created with mode `0600`; on Windows it inherits the profile directory ACL. The current
source implementation uses this protected file fallback, not a native OS keychain.

Success prints only operator, authentication server, Management server, and session IDs as JSON.
The invitation is consumed
atomically; do not repeat it after a successful response. Exit `7` means the code was invalid,
expired, consumed, or rejected. Exit `5` means the endpoint/transport was unavailable and the
caller must determine whether the server committed before trying to enroll new material.

When selected interactively, browser OIDC needs no `--browser` flag. `--browser` forces that method;
`--no-open` is its controlled/headless companion. Authorization Code + PKCE uses fresh state and
verifier values, an ephemeral loopback callback, exact Host/state checks, optional authorization
response issuer binding, a fixed token endpoint, and no redirects. The callback page reports
success only after the external token has passed Runku verification and the Runku session has been
persisted. `--oidc-token-env` remains available for an approved helper or workload identity.
Neither flow stores the external IdP token.

If the server advertises an RFC 8707 resource indicator, the CLI includes that exact value in both
authorization and token exchange. This lets a provider mint a JWT access token whose audience is
the selected Runku resource; the CLI cannot override the server-selected value.

This command authenticates a platform operator. It does not create or use `rk_pub_*`, `rk_sec_*`,
or `rk_dev_*` credentials. See the complete
[operator identity runbook](../auth/platform-identity.md).

## Initialization and local serving

### `runku dev`

```sh
runku dev [--root PATH] \
  [--origin http(s)://HOST[:PORT]]... \
  [--prebuilt] \
  [--auth-config RELATIVE] \
  [--application-env RELATIVE] \
  [--public-env-prefix PREFIX] \
  [--prepare] \
  [--replace-remote-credentials]
```

Normal behavior without flags:

- discovers `runku/`;
- initializes missing local state with Workspace `local` and `127.0.0.1:3210`;
- reconciles reserved local Application Clients and dotenv values;
- builds and publishes current source;
- starts the local product process and watches source changes.

`--origin` is repeatable and admits exact browser origins. `--prebuilt` serves already-published
state without reading application source. `--prepare` prepares durable state/credentials and exits.
`--replace-remote-credentials` is the explicit non-interactive authorization to replace foreign
Environment values in the selected application dotenv; omit it to fail closed.

### `runku init`

```sh
runku init [--root PATH] [--workspace REF] [--listen LOOPBACK:PORT] \
  [--project-id prj_* --environment-id env_*]
```

Use only before first `dev` when changing defaults. Initialization is idempotent for identical
settings and conflicts for divergent settings. The listener must be loopback; port `0` is accepted
only when selected explicitly. The project root must be safe, existing, regular, non-symlinked, and
must not be filesystem root or the user's home.

An external Self-Hosted provisioner that already owns the Product scope may supply
`--project-id` and `--environment-id` together. Both are required or both must be omitted. Exact
repetition recovers safely after a lost response and returns the existing IDs; a different requested
scope fails with `LOCAL_STATE_CONFLICT` without replacing state. IDs are not credentials, but the
provisioner must still derive them from its authorized durable control record rather than accepting
unverified request input. Ordinary local development omits both flags and continues to generate a
fresh scope.

### `runku link`

```sh
runku login
runku link [--root PATH] [--workspace REF] [--listen LOOPBACK:PORT] \
  --project-id prj_* --environment-id env_*
```

Use `link` for a customer- or operator-controlled directory that will issue remote lifecycle
commands. It loads the current `runku login` profile and performs an authenticated `status` request
against the exact Project/Environment before creating any local Runku state. Authentication,
current grants, and the selected installation's configured Product scope must all succeed. A
rejected request leaves an uninitialized directory unchanged.

On success, Runku initializes the exact scope and writes the non-secret
`.runku/management-link-v1.json` descriptor. The descriptor pins the canonical Management origin
for later `--remote` commands, preventing a subsequent login profile from silently redirecting the
linked root to another installation. It contains no access/refresh token or Application key.

Identical repetition is safe. A different Project, Environment, Workspace, listener, or Management
origin returns `PLATFORM_LINK_CONFLICT` without replacing state. Revoking the operator session or
grant still blocks subsequent requests; the descriptor records where the root was linked, not a
durable authorization grant. Direct `init --project-id/--environment-id` remains the trusted
provisioner primitive and does not prove remote ownership.

### `runku doctor`

```sh
runku doctor [--root PATH]
```

Read-only verification of state paths/permissions, stores, identity pepper, Workspace HEAD,
candidate manifest consistency, artifact integrity, and Cron activation consistency. `doctor`
never repairs pointers, replaces files, initializes missing state, or deletes evidence.

### `runku status`

```sh
runku status [--root PATH]
```

Returns one coherent Release/Channel snapshot. Use before promotion, rollback, compatibility
investigation, or retrying a conflict. `--remote` reads it through the current operator session and
requires `releases:read` at the root's exact Environment.

## Build and lifecycle

### `runku build`

```sh
runku build [--root PATH]

# Reproducible metadata tuple; all three flags must be supplied together:
runku build [--root PATH] \
  --release-id rel_* --build-id bld_* --created-at-micros I64
```

Discovers `runku/`, validates source/metadata/capabilities/contracts, produces an immutable package
under `.runku/builds-v1/rel_*`, preserves Release-specific generated types, and updates
`runku/_generated/api.d.ts`. Consume the `manifestPath` and `artifactPath` from its JSON result;
never guess filenames or edit build output.

### `runku publish`

```sh
runku publish [--root PATH] \
  --manifest FILE --artifact FILE \
  [--workspace REF] [--actor LABEL] [--expected-head empty|drv_*]
```

Add `--remote` to use the current `runku login` session. Remote publication requires
`--expected-head empty|drv_*`; it never infers a mutable server HEAD. `--root` supplies the exact
Project/Environment context and package metadata, while the stored session supplies the server and
operator authority.

Validates and persists the artifact before updating a Workspace pointer. `--expected-head` turns
the update into an operator-visible compare-and-set. A stale expectation returns conflict; re-read
state before deciding whether to publish a newer package or retry exact bytes.

### `runku release`

```sh
runku release [--root PATH] --release rel_* [--against CHANNEL]
```

Add `--remote` to validate through the Management API with `releases:publish` at the root's exact
Environment.

Validates a published candidate and makes it explicitly servable if lifecycle and compatibility
checks pass. `--against` selects a Channel compatibility baseline.

### `runku promote`

```sh
runku promote [--root PATH] \
  --channel CHANNEL --release rel_* [--expected empty|rel_*]
```

Add `--remote` to use the operator session and `channels:promote`. Keep `--expected` in automation.

Moves or creates a Channel after compatibility validation. Use `--expected` in automation to avoid
overwriting a concurrent operator decision.

### `runku rollback`

```sh
runku rollback [--root PATH] \
  --channel CHANNEL --expected rel_current --to rel_previous
```

Add `--remote` to perform the same exact-current CAS through the Management API.

Moves a Channel back only if it still points to the observed Release. Rollback changes routing, not
data. It cannot reverse a schema/data migration and does not change already-pinned scheduled work or
subscriptions.

## Application Clients and keys

### Clients

```sh
runku client create [--root PATH] \
  --name NAME --kind public|confidential --scope SCOPE... [--client-id app_*]
runku client list [--root PATH]
```

Client scopes are maximum grants for keys below that client. Use separate clients for browser,
backend, CI, and independently deployed integrations.

### Keys

```sh
runku key create [--root PATH] --client app_* --label LABEL \
  --scope SCOPE... [--key-id crd_*] [--expires-at-micros I64]
runku key list [--root PATH] --client app_*
runku key reveal [--root PATH] --client app_* --key crd_*
runku key rotate [--root PATH] --client app_* --key crd_* --label LABEL \
  [--new-key-id crd_*] [--expires-at-micros I64]
runku key revoke [--root PATH] --key crd_*
runku key delete [--root PATH] --key crd_*
```

Confidential key material is one-time reveal. Publishable keys can be re-derived through `reveal`.
Rotation creates a replacement with the source scopes and leaves the old key active so rollout can
overlap. After consumers use the replacement, revoke the old credential; deletion requires a
revoked key and tombstones it.

## Development access and remote Workspaces

### Development keys

```sh
runku workspace key create [--root PATH] --actor ACTOR --label LABEL \
  [--key-id dvk_*] [--expires-at-micros I64]
runku workspace key list [--root PATH]
runku workspace key rotate [--root PATH] --key dvk_* --label LABEL \
  [--new-key-id dvk_*] [--expires-at-micros I64]
runku workspace key revoke [--root PATH] --key dvk_*
runku workspace key delete [--root PATH] --key dvk_*
```

The revealed external token has the `rk_dev_*` form and authorizes development operations only.

### Sync

```sh
RUNKU_DEV_KEY='rk_dev_...' runku workspace sync \
  [--root PATH] \
  --url https://runku.example \
  --workspace dev/team/branch \
  --token-env RUNKU_DEV_KEY \
  [--expected-head empty|drv_*] [--create]
```

The token is read from the named environment variable and must not appear in arguments, logs, or
shell history. The client uses exact-origin HTTPS, bounded payloads, byte-exact retry for uncertain
requests, and state reconciliation. `--create` authorizes creating a missing Workspace;
`--expected-head` protects shared updates.

### Freeze a remote Release

```sh
RUNKU_DEV_KEY='rk_dev_...' runku workspace freeze \
  --url https://runku.example \
  --release rel_* \
  --token-env RUNKU_DEV_KEY \
  [--against rel_*]
```

Freezes an immutable Release through the remote development administrative protocol. Reconcile an
uncertain result by querying remote state before repeating or publishing another candidate.

## Operational logs

### Query/follow

```sh
runku logs [--root PATH] [--after logc_N] [--limit 1..1000] \
  [--stream platform|function] [--level debug|info|warn|error] \
  [--function fnc_*] [--request req_*] [--invocation inv_*] \
  [--client app_*] [--credential crd_*] [--release rel_*] [--follow]
```

`--after` is an exclusive durable cursor. Store the last processed cursor before continuing.
Filters are exact and can be combined. Local `--follow` polls the local repository until
interrupted. `--remote` uses the stored operator session; snapshot mode requires `logs:read`.
`--remote --follow` opens one NDJSON streaming response and requires `logs:follow`. The server
rechecks current session/grants during the stream, so revocation terminates it rather than waiting
for the original access-token lifetime.

### Retention

```sh
runku logs archive-status [--root PATH] [--remote]
runku logs prune [--root PATH] [--remote] --before-micros I64 [--maximum 1..10000]
runku logs prune [--root PATH] [--remote] --before-micros I64 [--maximum 1..10000] \
  --apply --environment env_*
```

`archive-status` verifies the immutable manifest chain and prints its contiguous cursor, rows,
segments, and bytes. Add `--remote` for an attached self-hosted/S3/HA Environment using the current
operator session and `logs:read`. Without `--apply`, pruning is a dry run. Applying requires the
exact Environment confirmation and deletes at most the bounded batch. Add `--remote` to use the
current operator session with `logs:prune` against the attached Environment. Repeat while the result
reports more matches. Deletion is additionally bounded by the committed archive cursor; an
unarchived hot row is never deleted merely because its timestamp matches.

For filesystem/S3 selection, HA journaling, worker operation, backup, and incident handling, see
[Operational Log storage](../operations/operational-logs.md).

### OTLP export

```sh
runku logs export-otlp [--root PATH] --config RELATIVE [--once]
```

The strict config is relative to the project root. `--once` exports one bounded batch; without it,
the exporter follows. Checkpointing is durable. Collector failure must not block serving.

## Automation checklist

- pin the source commit/CLI version;
- pass `--root` explicitly in multi-repository jobs;
- store IDs and cursors from JSON instead of scraping human text;
- use expected/CAS flags for shared mutable pointers;
- never pass keys through CLI arguments when a token environment option exists;
- treat exit `9` as reconciliation, not a blind retry;
- preserve build bytes for retry; do not rebuild between uncertain publish attempts;
- run `doctor` and save logs before destructive recovery;
- keep secret stdout from creation operations out of shared CI logs.
