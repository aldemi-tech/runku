#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" != 4 ]]; then
  printf 'usage: selfhost-artifact-evidence.sh VERSION SERVER_ARCHIVE CLI_ARCHIVE SELFHOST_ARCHIVE\n' >&2
  exit 2
fi
version="$1"
server_archive="$2"
cli_archive="$3"
selfhost_archive="$4"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
evidence="$(mktemp -d "${TMPDIR:-/tmp}/runku-selfhost-artifact.XXXXXX")"
package="$evidence/runku-selfhost-v$version"
image="runku-selfhost-evidence:$version"
project="runku-selfhost-evidence-${GITHUB_RUN_ID:-$$}"

cleanup() {
  status=$?
  if [[ -x "$package/runku-selfhost" && -f "$package/.env" ]]; then
    RUNKU_SELFHOST_ENV="$package/.env" RUNKU_UNINSTALL_CONFIRM="delete:$project" \
      "$package/runku-selfhost" uninstall delete-data >/dev/null 2>&1 || true
  fi
  docker image rm "$image" >/dev/null 2>&1 || true
  if [[ "$status" != 0 || "${RUNKU_KEEP_EVIDENCE:-false}" == true ]]; then
    printf 'self-host evidence retained at %s\n' "$evidence" >&2
  else
    rm -rf -- "$evidence"
  fi
}
trap cleanup EXIT INT TERM

for command in curl docker jq openssl sed tar; do
  command -v "$command" >/dev/null || { printf 'missing required command: %s\n' "$command" >&2; exit 1; }
done
[[ "$(uname -s)" == Linux ]] || { printf 'the compact artifact campaign requires Linux host networking\n' >&2; exit 1; }

mkdir -p "$evidence/server" "$evidence/cli" "$evidence/image"
tar -xzf "$server_archive" -C "$evidence/server"
tar -xzf "$cli_archive" -C "$evidence/cli"
tar -xzf "$selfhost_archive" -C "$evidence"
server_bin="$(find "$evidence/server" -type f -name runku-server -print -quit)"
runku_bin="$(find "$evidence/cli" -type f -name runku -print -quit)"
[[ -x "$server_bin" && -x "$runku_bin" && -x "$package/runku-selfhost" ]]
[[ "$($server_bin version)" == "runku-server $version" ]]
[[ "$($runku_bin --version)" == "runku $version" ]]

cp "$server_bin" "$evidence/image/runku-server"
cp "$runku_bin" "$evidence/image/runku"
docker build --file "$repository_root/deployments/docker/server.Dockerfile" \
  --build-arg "RUNKU_VERSION=$version" --build-arg "RUNKU_REVISION=${GITHUB_SHA:-local}" \
  --tag "$image" "$evidence/image"

mkdir -p "$evidence/data/product" "$evidence/data/platform" "$evidence/secrets" "$evidence/cli-session"
cp -R "$repository_root/tests/fixtures/platform-lifecycle/v1/runku" "$evidence/data/product/runku"
sed \
  -e "s#^COMPOSE_PROJECT_NAME=.*#COMPOSE_PROJECT_NAME=$project#" \
  -e 's#^RUNKU_DEPLOYMENT_PROFILE=.*#RUNKU_DEPLOYMENT_PROFILE=standalone#' \
  -e "s#^RUNKU_SERVER_IMAGE=.*#RUNKU_SERVER_IMAGE=$image#" \
  -e "s#^RUNKU_DATA_DIRECTORY=.*#RUNKU_DATA_DIRECTORY=$evidence/data#" \
  -e "s#^RUNKU_SECRETS_DIRECTORY=.*#RUNKU_SECRETS_DIRECTORY=$evidence/secrets#" \
  -e "s#^RUNKU_UID=.*#RUNKU_UID=$(id -u)#" \
  -e "s#^RUNKU_GID=.*#RUNKU_GID=$(id -g)#" \
  -e 's#^RUNKU_POSTGRES_PORT=.*#RUNKU_POSTGRES_PORT=25432#' \
  -e 's#^RUNKU_PUBLIC_MANAGEMENT_URL=.*#RUNKU_PUBLIC_MANAGEMENT_URL=http://127.0.0.1:3220#' \
  "$package/.env.example" >"$package/.env"
chmod 0600 "$package/.env"

export RUNKU_SELFHOST_ENV="$package/.env"
export RUNKU_SELFHOST_ALLOW_UNPINNED_IMAGE_FOR_TESTS=true
"$package/runku-selfhost" start
"$package/runku-selfhost" start
"$package/runku-selfhost" status

owner_code="$(tr -d '\r\n' <"$evidence/data/platform/bootstrap/initial-owner.code")"
RUNKU_OWNER_CODE="$owner_code" RUNKU_CONFIG_HOME="$evidence/cli-session" \
  "$runku_bin" login --url http://127.0.0.1:3220 --device artifact-owner \
    --code-env RUNKU_OWNER_CODE >"$evidence/login.json"

"$runku_bin" build --root "$evidence/data/product" >"$evidence/build.json"
manifest="$(jq -r .manifestPath "$evidence/build.json")"
artifact="$(jq -r .artifactPath "$evidence/build.json")"
release="$(jq -r .releaseId "$evidence/build.json")"
RUNKU_CONFIG_HOME="$evidence/cli-session" "$runku_bin" publish --remote \
  --root "$evidence/data/product" --manifest "$manifest" --artifact "$artifact" \
  --expected-head empty >"$evidence/publish.json"
RUNKU_CONFIG_HOME="$evidence/cli-session" "$runku_bin" release --remote \
  --root "$evidence/data/product" --release "$release" >"$evidence/release.json"
RUNKU_CONFIG_HOME="$evidence/cli-session" "$runku_bin" promote --remote \
  --root "$evidence/data/product" --channel stable --release "$release" --expected empty \
  >"$evidence/promote.json"

application_key="$(sed -n 's/^RUNKU_KEY=//p' "$evidence/data/product/.env.local")"
for attempt in {1..40}; do
  if curl --fail --silent http://127.0.0.1:3210/readyz >/dev/null; then break; fi
  [[ "$attempt" != 40 ]] || exit 1
  sleep 0.25
done
curl --fail --silent --show-error --request POST \
  --header 'Content-Type: application/json' --header "x-runku-key: $application_key" \
  --data '{"version":1,"target":"channel:stable","function":"version.current","arguments":{"type":"null"}}' \
  http://127.0.0.1:3210/v1/query >"$evidence/invoke-before-backup.json"
jq -e '.result == {"type":"string","value":"v1"}' "$evidence/invoke-before-backup.json" >/dev/null
RUNKU_CONFIG_HOME="$evidence/cli-session" "$runku_bin" logs --remote \
  --root "$evidence/data/product" --release "$release" >"$evidence/logs.ndjson"

backup="$evidence/backup"
"$package/runku-selfhost" backup "$backup" evidence-encrypted-volume
"$package/runku-selfhost" verify-backup "$backup"
RUNKU_UNINSTALL_CONFIRM="delete:$project" "$package/runku-selfhost" uninstall delete-data
"$package/runku-selfhost" configure
RUNKU_RESTORE_CONFIRM='restore:backup' "$package/runku-selfhost" restore "$backup"

RUNKU_CONFIG_HOME="$evidence/cli-session" "$runku_bin" status --remote \
  --root "$evidence/data/product" >"$evidence/status-after-restore.json"
curl --fail --silent --show-error --request POST \
  --header 'Content-Type: application/json' --header "x-runku-key: $application_key" \
  --data '{"version":1,"target":"channel:stable","function":"version.current","arguments":{"type":"null"}}' \
  http://127.0.0.1:3210/v1/query >"$evidence/invoke-after-restore.json"
jq -e '.result == {"type":"string","value":"v1"}' "$evidence/invoke-after-restore.json" >/dev/null

printf '%s\n' 'self-host release artifact evidence passed:'
printf '%s\n' '  clean package/image setup and mounted-secret configuration: passed'
printf '%s\n' '  invitation login and authenticated publish/release/promote: passed'
printf '%s\n' '  Product invocation and scoped logs: passed'
printf '%s\n' '  coordinated backup and offline verification: passed'
printf '%s\n' '  empty-install restore, persisted session, and automatic Channel serving: passed'
