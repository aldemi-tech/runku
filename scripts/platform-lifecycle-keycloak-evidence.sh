#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repository_root/compose.platform-identity.yml"
compose_project="runku-platform-lifecycle-evidence"
evidence_dir="$(mktemp -d "${TMPDIR:-/tmp}/runku-platform-lifecycle.XXXXXX")"
server_pid=""
metadata_pid=""
login_pid=""
follow_pid=""

cleanup() {
  exit_status="$?"
  for pid in "$follow_pid" "$login_pid" "$server_pid" "$metadata_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ "$exit_status" != 0 && -f "$evidence_dir/runku-server.log" ]]; then
    printf '%s\n' 'sanitized runku-server log:' >&2
    cat "$evidence_dir/runku-server.log" >&2
  fi
  docker compose --project-name "$compose_project" --file "$compose_file" down --volumes --remove-orphans >/dev/null 2>&1 || true
  if [[ "${RUNKU_KEEP_EVIDENCE:-false}" == true ]]; then
    printf 'evidence retained at %s\n' "$evidence_dir" >&2
  else
    case "$evidence_dir" in
      */runku-platform-lifecycle.*) rm -rf -- "$evidence_dir" ;;
    esac
  fi
}
trap cleanup EXIT INT TERM

for command in cargo curl docker node openssl python3; do
  command -v "$command" >/dev/null || {
    printf 'missing required command: %s\n' "$command" >&2
    exit 1
  }
done
docker info >/dev/null
test -d "$repository_root/examples/chat-next/node_modules/@playwright/test"

run_browser_flow() {
  if [[ -n "${RUNKU_PLAYWRIGHT_RUNNER:-}" && -n "${RUNKU_PLAYWRIGHT_SCRIPT:-}" ]]; then
    node "$RUNKU_PLAYWRIGHT_RUNNER" "$RUNKU_PLAYWRIGHT_SCRIPT"
  else
    node "$repository_root/scripts/platform-lifecycle-browser.mjs"
  fi
}

printf '%s\n' 'starting disposable PostgreSQL and OIDC fixtures'
docker compose --project-name "$compose_project" --file "$compose_file" up --detach
for attempt in {1..90}; do
  if curl --fail --silent --show-error \
    http://127.0.0.1:18080/realms/runku-test/.well-known/openid-configuration \
    >"$evidence_dir/keycloak-discovery.json"; then
    break
  fi
  if [[ "$attempt" == 90 ]]; then
    docker compose --project-name "$compose_project" --file "$compose_file" logs platform-keycloak >&2
    exit 1
  fi
  sleep 2
done
curl --fail --silent --show-error \
  http://127.0.0.1:18080/realms/runku-test/protocol/openid-connect/certs \
  >"$evidence_dir/jwks.json"

cat >"$evidence_dir/discovery.json" <<'JSON'
{
  "issuer": "https://identity.runku.test/realms/runku-test",
  "jwks_uri": "http://127.0.0.1:18081/jwks.json"
}
JSON
python3 -m http.server 18081 --bind 127.0.0.1 --directory "$evidence_dir" \
  >"$evidence_dir/metadata-server.log" 2>&1 &
metadata_pid="$!"
for attempt in {1..20}; do
  curl --fail --silent http://127.0.0.1:18081/discovery.json >/dev/null && break
  [[ "$attempt" != 20 ]] || exit 1
  sleep 1
done

identity_pepper="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n')"
subject_pepper="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n')"
mkdir -p "$evidence_dir/state" "$evidence_dir/owner-cli" "$evidence_dir/alice-cli" \
  "$evidence_dir/observer-cli" "$evidence_dir/product" "$evidence_dir/foreign"
cp -R "$repository_root/tests/fixtures/platform-lifecycle/v1/runku" "$evidence_dir/product/runku"
cat >"$evidence_dir/oidc.json" <<JSON
{
  "providerId": "keycloak-runku-test",
  "issuer": "https://identity.runku.test/realms/runku-test",
  "discoveryUrl": "http://127.0.0.1:18081/discovery.json",
  "audience": "runku-management",
  "allowedOrigins": ["http://127.0.0.1:18081"],
  "discriminatorClaim": "runku_actor_type",
  "discriminatorValue": "operator",
  "algorithm": "RS256",
  "requiredType": "JWT",
  "subjectPepper": "$subject_pepper",
  "allowLoopbackHttp": true,
  "nativeClient": {
    "authorizationEndpoint": "http://127.0.0.1:18080/realms/runku-test/protocol/openid-connect/auth",
    "tokenEndpoint": "http://127.0.0.1:18080/realms/runku-test/protocol/openid-connect/token",
    "clientId": "runku-management",
    "scopes": ["openid", "profile"]
  }
}
JSON

cd "$repository_root"
printf '%s\n' 'building the server and CLI once'
cargo build --quiet --package runku-server --package runku-cli
runku_bin="$repository_root/target/debug/runku"
server_bin="$repository_root/target/debug/runku-server"

"$runku_bin" init --root "$evidence_dir/product" --listen 127.0.0.1:18310 \
  >"$evidence_dir/product-init.json"
"$runku_bin" dev --root "$evidence_dir/product" --prepare \
  >"$evidence_dir/product-prepare.json"
"$runku_bin" init --root "$evidence_dir/foreign" --listen 127.0.0.1:18311 \
  >"$evidence_dir/foreign-init.json"
project_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["projectId"])' "$evidence_dir/product-init.json")"
environment_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["environmentId"])' "$evidence_dir/product-init.json")"
foreign_project="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["projectId"])' "$evidence_dir/foreign-init.json")"
foreign_environment="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["environmentId"])' "$evidence_dir/foreign-init.json")"

RUNKU_IDENTITY_DATABASE_URL='postgres://runku_platform:runku_platform_test@127.0.0.1:15432/runku_platform' \
RUNKU_PLATFORM_IDENTITY_PEPPER="$identity_pepper" \
RUNKU_STATE_DIRECTORY="$evidence_dir/state" \
RUNKU_MANAGEMENT_LISTEN='127.0.0.1:18220' \
RUNKU_PLATFORM_OIDC_CONFIG="$evidence_dir/oidc.json" \
RUNKU_PRODUCT_ROOT="$evidence_dir/product" \
  "$server_bin" serve >"$evidence_dir/runku-server.log" 2>&1 &
server_pid="$!"
for attempt in {1..30}; do
  curl --fail --silent http://127.0.0.1:18220/health/ready >/dev/null && break
  [[ "$attempt" != 30 ]] || exit 1
  sleep 1
done

printf '%s\n' 'enrolling the initial owner without an external IdP'
bootstrap_code="$(tr -d '\r\n' <"$evidence_dir/state/bootstrap/initial-owner.code")"
RUNKU_BOOTSTRAP_CODE="$bootstrap_code" RUNKU_CONFIG_HOME="$evidence_dir/owner-cli" \
  "$runku_bin" login --url http://127.0.0.1:18220 --device initial-owner \
    --code-env RUNKU_BOOTSTRAP_CODE >"$evidence_dir/owner-login.json"
owner_access="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["accessToken"])' "$evidence_dir/owner-cli/credentials-v1.json")"

curl --fail --silent --show-error --request POST \
  --header "Authorization: Bearer $owner_access" \
  --header 'Content-Type: application/json' \
  --data "{\"operatorName\":\"alice\",\"role\":\"developer\",\"scope\":{\"kind\":\"environment\",\"projectId\":\"$project_id\",\"environmentId\":\"$environment_id\"}}" \
  http://127.0.0.1:18220/v1/access/invitations >"$evidence_dir/alice-invitation.json"
alice_invitation="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["code"])' "$evidence_dir/alice-invitation.json")"

printf '%s\n' 'completing browser OIDC Authorization Code + PKCE login'
RUNKU_ALICE_INVITATION="$alice_invitation" RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" \
  "$runku_bin" login --url http://127.0.0.1:18220 --device alice-browser \
    --browser --no-open --code-env RUNKU_ALICE_INVITATION \
    >"$evidence_dir/alice-login.json" 2>"$evidence_dir/alice-login.stderr" &
login_pid="$!"
authorization_url=""
for attempt in {1..100}; do
  authorization_url="$(sed -n 's/^authorization URL: //p' "$evidence_dir/alice-login.stderr" | tail -n 1)"
  [[ -z "$authorization_url" ]] || break
  [[ "$attempt" != 100 ]] || exit 1
  sleep 0.1
done
RUNKU_TEST_AUTHORIZATION_URL="$authorization_url" \
  run_browser_flow
wait "$login_pid"
login_pid=""
alice_access="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["accessToken"])' "$evidence_dir/alice-cli/credentials-v1.json")"
alice_session="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["sessionId"])' "$evidence_dir/alice-cli/credentials-v1.json")"

reuse_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  --request POST --header 'Content-Type: application/json' \
  --data "{\"code\":\"$alice_invitation\",\"deviceName\":\"replay\"}" \
  http://127.0.0.1:18220/v1/auth/exchange)"
[[ "$reuse_status" == 401 ]]

build_and_read() {
  build_file="$1"
  "$runku_bin" build --root "$evidence_dir/product" >"$build_file"
  manifest_path="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["manifestPath"])' "$build_file")"
  artifact_path="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["artifactPath"])' "$build_file")"
  release_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["releaseId"])' "$build_file")"
}

invoke_version() {
  expected="$1"
  output="$2"
  for attempt in {1..40}; do
    response="$(curl --fail --silent --show-error \
      --request POST --header 'Content-Type: application/json' \
      --header "x-runku-key: $application_key" \
      --data '{"version":1,"target":"channel:stable","function":"version.current","arguments":{"type":"null"}}' \
      http://127.0.0.1:18310/v1/query)"
    printf '%s' "$response" >"$output"
    if python3 -c 'import json,sys; value=json.load(open(sys.argv[1]))["result"]; assert value == {"type":"string","value":sys.argv[2]}, value' "$output" "$expected" 2>/dev/null; then
      return 0
    fi
    [[ "$attempt" != 40 ]] || return 1
    sleep 0.25
  done
}

application_key="$(sed -n 's/^RUNKU_KEY=//p' "$evidence_dir/product/.env.local")"
[[ "$application_key" == rk_pub_v1_* ]]

printf '%s\n' 'publishing, releasing, promoting, invoking, and reading logs for release v1'
build_and_read "$evidence_dir/build-v1.json"
release_v1="$release_id"
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" publish --remote \
  --root "$evidence_dir/product" --manifest "$manifest_path" --artifact "$artifact_path" \
  --expected-head empty >"$evidence_dir/publish-v1.json"
revision_v1="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["revisionId"])' "$evidence_dir/publish-v1.json")"
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" publish --remote \
  --root "$evidence_dir/product" --manifest "$manifest_path" --artifact "$artifact_path" \
  --expected-head empty >"$evidence_dir/publish-v1-replay.json"
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["replayed"] is True' "$evidence_dir/publish-v1-replay.json"
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" release --remote \
  --root "$evidence_dir/product" --release "$release_v1" >"$evidence_dir/release-v1.json"
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" promote --remote \
  --root "$evidence_dir/product" --channel stable --release "$release_v1" --expected empty \
  >"$evidence_dir/promote-v1.json"
for attempt in {1..40}; do
  curl --fail --silent http://127.0.0.1:18310/readyz >/dev/null && break
  [[ "$attempt" != 40 ]] || exit 1
  sleep 0.25
done
invoke_version v1 "$evidence_dir/invoke-v1.json"
for attempt in {1..20}; do
  RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" logs --remote \
    --root "$evidence_dir/product" --release "$release_v1" >"$evidence_dir/logs-v1.ndjson"
  grep -q "$release_v1" "$evidence_dir/logs-v1.ndjson" && break
  [[ "$attempt" != 20 ]] || exit 1
  sleep 0.25
done

printf '%s\n' 'following one streaming log connection'
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" logs --remote --follow \
  --root "$evidence_dir/product" --after "$(tail -n 1 "$evidence_dir/logs-v1.ndjson" | python3 -c 'import json,sys; print(json.load(sys.stdin)["cursor"])')" \
  >"$evidence_dir/log-follow.ndjson" 2>"$evidence_dir/log-follow.stderr" &
follow_pid="$!"
invoke_version v1 "$evidence_dir/invoke-v1-follow.json"
for attempt in {1..40}; do
  grep -q "$release_v1" "$evidence_dir/log-follow.ndjson" && break
  [[ "$attempt" != 40 ]] || exit 1
  sleep 0.25
done
kill "$follow_pid"
wait "$follow_pid" 2>/dev/null || true
follow_pid=""

printf '%s\n' 'publishing v2, promoting it, and rolling back to v1'
cp "$repository_root/tests/fixtures/platform-lifecycle/v2/runku/version.ts" "$evidence_dir/product/runku/version.ts"
build_and_read "$evidence_dir/build-v2.json"
release_v2="$release_id"
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" publish --remote \
  --root "$evidence_dir/product" --manifest "$manifest_path" --artifact "$artifact_path" \
  --expected-head "$revision_v1" >"$evidence_dir/publish-v2.json"
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" release --remote \
  --root "$evidence_dir/product" --release "$release_v2" --against stable >"$evidence_dir/release-v2.json"
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" promote --remote \
  --root "$evidence_dir/product" --channel stable --release "$release_v2" --expected "$release_v1" \
  >"$evidence_dir/promote-v2.json"
invoke_version v2 "$evidence_dir/invoke-v2.json"
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" rollback --remote \
  --root "$evidence_dir/product" --channel stable --expected "$release_v2" --to "$release_v1" \
  >"$evidence_dir/rollback-v1.json"
invoke_version v1 "$evidence_dir/invoke-after-rollback.json"

printf '%s\n' 'restarting the server and proving persisted Channel serving recovers automatically'
kill "$server_pid"
wait "$server_pid"
server_pid=""
RUNKU_IDENTITY_DATABASE_URL='postgres://runku_platform:runku_platform_test@127.0.0.1:15432/runku_platform' \
RUNKU_PLATFORM_IDENTITY_PEPPER="$identity_pepper" \
RUNKU_STATE_DIRECTORY="$evidence_dir/state" \
RUNKU_MANAGEMENT_LISTEN='127.0.0.1:18220' \
RUNKU_PLATFORM_OIDC_CONFIG="$evidence_dir/oidc.json" \
RUNKU_PRODUCT_ROOT="$evidence_dir/product" \
  "$server_bin" serve >>"$evidence_dir/runku-server.log" 2>&1 &
server_pid="$!"
for attempt in {1..40}; do
  if curl --fail --silent http://127.0.0.1:18220/health/ready >/dev/null \
    && curl --fail --silent http://127.0.0.1:18310/readyz >/dev/null; then
    break
  fi
  [[ "$attempt" != 40 ]] || exit 1
  sleep 0.25
done
invoke_version v1 "$evidence_dir/invoke-after-server-restart.json"

set +e
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" promote --remote \
  --root "$evidence_dir/product" --channel stable --release "$release_v2" --expected "$release_v2" \
  >"$evidence_dir/stale-promote.stdout" 2>"$evidence_dir/stale-promote.stderr"
stale_status="$?"
set -e
[[ "$stale_status" == 4 ]]

printf '%s\n' 'validating authenticated archive inspection and bounded retention authority'
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" logs archive-status --remote \
  --root "$evidence_dir/product" >"$evidence_dir/alice-archive-status.json"
set +e
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" logs prune --remote \
  --root "$evidence_dir/product" --before-micros 9223372036854775807 --maximum 100 \
  >"$evidence_dir/alice-prune.stdout" 2>"$evidence_dir/alice-prune.stderr"
alice_prune_status="$?"
set -e
[[ "$alice_prune_status" == 8 ]]
RUNKU_CONFIG_HOME="$evidence_dir/owner-cli" "$runku_bin" logs prune --remote \
  --root "$evidence_dir/product" --before-micros 9223372036854775807 --maximum 100 \
  >"$evidence_dir/owner-prune-dry-run.json"
RUNKU_CONFIG_HOME="$evidence_dir/owner-cli" "$runku_bin" logs prune --remote \
  --root "$evidence_dir/product" --before-micros 9223372036854775807 --maximum 100 \
  --apply --environment "$environment_id" >"$evidence_dir/owner-prune-apply.json"
python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); assert value["applied"] is True; assert value["environmentId"] == sys.argv[2]' \
  "$evidence_dir/owner-prune-apply.json" "$environment_id"

printf '%s\n' 'validating missing auth, exact-scope isolation, capability denial, and revocation'
missing_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "http://127.0.0.1:18220/v1/projects/$project_id/environments/$environment_id/status")"
[[ "$missing_status" == 401 ]]
foreign_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  --header "Authorization: Bearer $alice_access" \
  "http://127.0.0.1:18220/v1/projects/$foreign_project/environments/$foreign_environment/status")"
[[ "$foreign_status" == 403 ]]

curl --fail --silent --show-error --request POST \
  --header "Authorization: Bearer $owner_access" --header 'Content-Type: application/json' \
  --data "{\"operatorName\":\"observer\",\"role\":\"observer\",\"scope\":{\"kind\":\"environment\",\"projectId\":\"$project_id\",\"environmentId\":\"$environment_id\"}}" \
  http://127.0.0.1:18220/v1/access/invitations >"$evidence_dir/observer-invitation.json"
observer_invitation="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["code"])' "$evidence_dir/observer-invitation.json")"
RUNKU_OBSERVER_INVITATION="$observer_invitation" RUNKU_CONFIG_HOME="$evidence_dir/observer-cli" \
  "$runku_bin" login --url http://127.0.0.1:18220 --device observer \
    --code-env RUNKU_OBSERVER_INVITATION >"$evidence_dir/observer-login.json"
set +e
RUNKU_CONFIG_HOME="$evidence_dir/observer-cli" "$runku_bin" promote --remote \
  --root "$evidence_dir/product" --channel stable --release "$release_v2" \
  >"$evidence_dir/observer-promote.stdout" 2>"$evidence_dir/observer-promote.stderr"
observer_status="$?"
set -e
[[ "$observer_status" == 8 ]]

RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" logs --remote --follow \
  --root "$evidence_dir/product" >"$evidence_dir/revoked-follow.ndjson" \
  2>"$evidence_dir/revoked-follow.stderr" &
follow_pid="$!"
sleep 0.5
curl --fail --silent --show-error --request DELETE \
  --header "Authorization: Bearer $owner_access" \
  "http://127.0.0.1:18220/v1/auth/sessions/$alice_session" >/dev/null
set +e
wait "$follow_pid"
revoked_follow_status="$?"
set -e
follow_pid=""
[[ "$revoked_follow_status" == 7 ]]
set +e
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" status --remote \
  --root "$evidence_dir/product" >"$evidence_dir/revoked-status.stdout" 2>"$evidence_dir/revoked-status.stderr"
revoked_status="$?"
set -e
[[ "$revoked_status" == 7 ]]

printf '%s\n' 're-enrolling the linked OIDC identity after revocation'
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" login \
  --url http://127.0.0.1:18220 --device alice-browser-recovered --browser --no-open \
  >"$evidence_dir/alice-relogin.json" 2>"$evidence_dir/alice-relogin.stderr" &
login_pid="$!"
authorization_url=""
for attempt in {1..100}; do
  authorization_url="$(sed -n 's/^authorization URL: //p' "$evidence_dir/alice-relogin.stderr" | tail -n 1)"
  [[ -z "$authorization_url" ]] || break
  [[ "$attempt" != 100 ]] || exit 1
  sleep 0.1
done
RUNKU_TEST_AUTHORIZATION_URL="$authorization_url" run_browser_flow
wait "$login_pid"
login_pid=""
RUNKU_CONFIG_HOME="$evidence_dir/alice-cli" "$runku_bin" status --remote \
  --root "$evidence_dir/product" >"$evidence_dir/recovered-status.json"

printf '%s\n' 'platform lifecycle Keycloak evidence passed:'
printf '%s\n' '  invitation bootstrap without IdP: passed'
printf '%s\n' '  browser OIDC Authorization Code + PKCE, including rejected password: passed'
printf '%s\n' '  invitation binding and replay rejection: passed'
printf '%s\n' '  authenticated publish/replay/release/promote/invoke/logs: passed'
printf '%s\n' '  one-connection realtime log follow: passed'
printf '%s\n' '  second release promotion and exact rollback: passed'
printf '%s\n' '  persisted Channel serving after server restart: passed'
printf '%s\n' '  archive inspection and bounded retention authorization: passed'
printf '%s\n' '  stale CAS, missing auth, capability, and cross-scope denial: passed'
printf '%s\n' '  live stream revocation and CLI session denial: passed'
printf '%s\n' '  linked OIDC identity recovery after revocation: passed'
