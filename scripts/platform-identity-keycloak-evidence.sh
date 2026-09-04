#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repository_root/compose.platform-identity.yml"
compose_project="runku-platform-identity-evidence"
evidence_dir="$(mktemp -d "${TMPDIR:-/tmp}/runku-platform-identity.XXXXXX")"
server_pid=""
metadata_pid=""

cleanup() {
  exit_status="$?"
  if [[ "$exit_status" != 0 && -f "$evidence_dir/runku-server.log" ]]; then
    printf '%s\n' 'sanitized runku-server log:' >&2
    cat "$evidence_dir/runku-server.log" >&2
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ -n "$metadata_pid" ]]; then
    kill "$metadata_pid" 2>/dev/null || true
    wait "$metadata_pid" 2>/dev/null || true
  fi
  docker compose --project-name "$compose_project" --file "$compose_file" down --volumes --remove-orphans >/dev/null 2>&1 || true
  case "$evidence_dir" in
    */runku-platform-identity.*) rm -rf -- "$evidence_dir" ;;
  esac
}
trap cleanup EXIT INT TERM

for command in cargo curl docker openssl python3; do
  command -v "$command" >/dev/null || {
    printf 'missing required command: %s\n' "$command" >&2
    exit 1
  }
done
docker info >/dev/null

printf '%s\n' 'starting PostgreSQL and Keycloak fixtures'
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
  if curl --fail --silent http://127.0.0.1:18081/discovery.json >/dev/null; then
    break
  fi
  if [[ "$attempt" == 20 ]]; then
    exit 1
  fi
  sleep 1
done

identity_pepper="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n')"
subject_pepper="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n')"
mkdir -p "$evidence_dir/state" "$evidence_dir/cli"
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
printf '%s\n' 'building Runku server and CLI'
cargo build --quiet --package runku-server --package runku-cli

start_runku_server() {
  RUNKU_IDENTITY_DATABASE_URL='postgres://runku_platform:runku_platform_test@127.0.0.1:15432/runku_platform' \
  RUNKU_PLATFORM_IDENTITY_PEPPER="$identity_pepper" \
  RUNKU_STATE_DIRECTORY="$evidence_dir/state" \
  RUNKU_MANAGEMENT_LISTEN='127.0.0.1:18220' \
  RUNKU_PLATFORM_OIDC_CONFIG="$evidence_dir/oidc.json" \
    "$repository_root/target/debug/runku-server" serve >>"$evidence_dir/runku-server.log" 2>&1 &
  server_pid="$!"
}

wait_for_runku_server() {
  for attempt in {1..30}; do
    if curl --fail --silent http://127.0.0.1:18220/health/live >/dev/null; then
      return
    fi
    if [[ "$attempt" == 30 ]]; then
      cat "$evidence_dir/runku-server.log" >&2
      exit 1
    fi
    sleep 1
  done
}

start_runku_server
wait_for_runku_server

printf '%s\n' 'recovering a deliberately lost pending bootstrap file'
original_bootstrap_code="$(tr -d '\r\n' <"$evidence_dir/state/bootstrap/initial-owner.code")"
kill "$server_pid"
wait "$server_pid" || true
server_pid=""
rm -- "$evidence_dir/state/bootstrap/initial-owner.code"
RUNKU_IDENTITY_DATABASE_URL='postgres://runku_platform:runku_platform_test@127.0.0.1:15432/runku_platform' \
RUNKU_PLATFORM_IDENTITY_PEPPER="$identity_pepper" \
RUNKU_STATE_DIRECTORY="$evidence_dir/state" \
RUNKU_MANAGEMENT_LISTEN='127.0.0.1:18220' \
RUNKU_PLATFORM_OIDC_CONFIG="$evidence_dir/oidc.json" \
RUNKU_BOOTSTRAP_RECOVERY_CONFIRM='replace-lost-initial-owner-code' \
  "$repository_root/target/debug/runku-server" recover-bootstrap \
  >"$evidence_dir/recover-bootstrap.log"
bootstrap_code="$(tr -d '\r\n' <"$evidence_dir/state/bootstrap/initial-owner.code")"
[[ "$bootstrap_code" != "$original_bootstrap_code" ]]
start_runku_server
wait_for_runku_server
old_bootstrap_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  --request POST \
  --header 'Content-Type: application/json' \
  --data "{\"code\":\"$original_bootstrap_code\",\"deviceName\":\"recovered-old-code\"}" \
  http://127.0.0.1:18220/v1/auth/exchange)"
[[ "$old_bootstrap_status" == 401 ]]

printf '%s\n' 'enrolling initial owner through runku login'
RUNKU_BOOTSTRAP_CODE="$bootstrap_code" \
RUNKU_CONFIG_HOME="$evidence_dir/cli" \
  "$repository_root/target/debug/runku" login \
    --url http://127.0.0.1:18220 \
    --device evidence-owner \
    --code-env RUNKU_BOOTSTRAP_CODE \
    >"$evidence_dir/owner-login.json"

owner_access="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["accessToken"])' "$evidence_dir/cli/credentials-v1.json")"
printf '%s\n' 'creating a scoped operator invitation'
curl --fail --silent --show-error \
  --request POST \
  --header "Authorization: Bearer $owner_access" \
  --header 'Content-Type: application/json' \
  --data '{"operatorName":"alice","role":"operator","scope":{"kind":"installation","projectId":null,"environmentId":null}}' \
  http://127.0.0.1:18220/v1/access/invitations \
  >"$evidence_dir/alice-invitation.json"
alice_invitation="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["code"])' "$evidence_dir/alice-invitation.json")"

printf '%s\n' 'obtaining a real Keycloak RS256 access token'
curl --fail --silent --show-error \
  --request POST \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=password' \
  --data-urlencode 'client_id=runku-management' \
  --data-urlencode 'username=alice' \
  --data-urlencode 'password=runku-alice-test-password' \
  http://127.0.0.1:18080/realms/runku-test/protocol/openid-connect/token \
  >"$evidence_dir/keycloak-token.json"
keycloak_access="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["access_token"])' "$evidence_dir/keycloak-token.json")"

printf '%s\n' 'enrolling the invited Keycloak identity'
RUNKU_ALICE_INVITATION="$alice_invitation" \
RUNKU_KEYCLOAK_TOKEN="$keycloak_access" \
RUNKU_CONFIG_HOME="$evidence_dir/cli" \
  "$repository_root/target/debug/runku" login \
  --url http://127.0.0.1:18220 \
  --device alice-keycloak \
  --code-env RUNKU_ALICE_INVITATION \
  --oidc-token-env RUNKU_KEYCLOAK_TOKEN \
  >"$evidence_dir/alice-login.json"
alice_access="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["accessToken"])' "$evidence_dir/cli/credentials-v1.json")"
alice_operator="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operatorId"])' "$evidence_dir/alice-login.json")"

printf '%s\n' 'verifying Runku session identity and linked re-login'
curl --fail --silent --show-error \
  --header "Authorization: Bearer $alice_access" \
  http://127.0.0.1:18220/v1/auth/me \
  >"$evidence_dir/alice-me.json"
me_operator="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operatorId"])' "$evidence_dir/alice-me.json")"
[[ "$alice_operator" == "$me_operator" ]]

RUNKU_KEYCLOAK_TOKEN="$keycloak_access" \
RUNKU_CONFIG_HOME="$evidence_dir/cli" \
  "$repository_root/target/debug/runku" login \
  --url http://127.0.0.1:18220 \
  --device alice-keycloak-second \
  --oidc-token-env RUNKU_KEYCLOAK_TOKEN \
  >"$evidence_dir/alice-second-login.json"
second_operator="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operatorId"])' "$evidence_dir/alice-second-login.json")"
[[ "$alice_operator" == "$second_operator" ]]

printf '%s\n' 'verifying replay and tamper rejection'
reuse_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  --request POST \
  --header 'Content-Type: application/json' \
  --data "{\"code\":\"$alice_invitation\",\"deviceName\":\"replay\"}" \
  http://127.0.0.1:18220/v1/auth/exchange)"
[[ "$reuse_status" == 401 ]]

tampered_token="${keycloak_access%?}x"
tampered_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  --request POST \
  --header "Authorization: Bearer $tampered_token" \
  --header 'Content-Type: application/json' \
  --data '{"deviceName":"tampered","invitationCode":null}' \
  http://127.0.0.1:18220/v1/auth/oidc)"
[[ "$tampered_status" == 401 ]]

printf '%s\n' 'platform identity Keycloak evidence passed:'
printf '%s\n' '  lost bootstrap recovery and previous-code revocation: passed'
printf '%s\n' '  invitation CLI bootstrap: passed'
printf '%s\n' '  Keycloak RS256 discovery/JWKS verification: passed'
printf '%s\n' '  invitation-bound OIDC enrollment: passed'
printf '%s\n' '  linked OIDC re-login preserves operator identity: passed'
printf '%s\n' '  single-use invitation replay rejection: passed'
printf '%s\n' '  tampered external bearer rejection: passed'
