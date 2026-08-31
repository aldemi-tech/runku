#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "$0")/.." && pwd)
fixture_root="$project_root/crates/runku-gateway/tests/fixtures/full_node"
host_port=${RUNKU_HOST_NODE_TEST_PORT:-55433}
container_name="runku-host-node-evidence-$$"

if [[ -n "${RUNKU_HOST_NODE_TEST_IP:-}" ]]; then
  host_ip=$RUNKU_HOST_NODE_TEST_IP
elif command -v ipconfig >/dev/null 2>&1; then
  default_interface=$(route -n get default 2>/dev/null | awk '$1 == "interface:" { print $2; exit }')
  host_ip=$(ipconfig getifaddr "$default_interface" 2>/dev/null || true)
else
  host_ip=$(hostname -I 2>/dev/null | awk '{print $1}')
fi
if [[ -z "${host_ip:-}" ]]; then
  echo "Unable to determine a non-loopback host IP; set RUNKU_HOST_NODE_TEST_IP." >&2
  exit 1
fi

cleanup() {
  docker rm --force "$container_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cd "$fixture_root"
npm ci --omit=dev --ignore-scripts

cd "$project_root"
cargo test -p runku-runtime --test runtime_conformance \
  safe_v8_rejects_node_crypto_and_has_no_node_authority --locked -- --exact
cargo test -p runku-node-runtime --test local_runtime --locked -- --exact
cargo test -p runku-node-runtime --test host_runtime \
  dedicated_host_runs_node_crypto_image_and_controls_failures --locked -- --exact
cargo test -p runku-node-runtime --test host_runtime \
  dedicated_host_rejects_public_egress_and_missing_cache --locked -- --exact
cargo test -p runku-node-runtime --test host_runtime \
  dedicated_host_rejects_valid_contract_substitution_in_cache --locked -- --exact

docker run --detach --name "$container_name" \
  --publish "$host_ip:$host_port:5432" \
  --env POSTGRES_DB=runku_test \
  --env POSTGRES_USER=runku \
  --env POSTGRES_PASSWORD=runku_host_test_only \
  postgres:16-alpine@sha256:20edbde7749f822887a1a022ad526fde0a47d6b2be9a8364433605cf65099416
for _attempt in {1..30}; do
  if docker exec "$container_name" pg_isready -U runku -d runku_test >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
docker exec "$container_name" pg_isready -U runku -d runku_test >/dev/null

RUNKU_HOST_NODE_POSTGRES_URL="postgres://runku:runku_host_test_only@$host_ip:$host_port/runku_test" \
RUNKU_HOST_NODE_POSTGRES_DESTINATION="$host_ip" \
RUNKU_HOST_NODE_POSTGRES_PORT="$host_port" \
RUNKU_HOST_NODE_MODULES="$fixture_root/node_modules" \
cargo test -p runku-node-runtime --test host_runtime \
  dedicated_host_connects_to_external_postgres_under_exact_policy --locked -- --exact
cleanup

RUNKU_FULL_NODE_DOCKER_TEST=1 cargo test -p runku-gateway --test product_vertical \
  full_node_channel_promotion_and_rollback_use_exact_oci_artifacts --locked -- --exact --nocapture
RUNKU_FULL_NODE_DOCKER_TEST=1 cargo test -p runku-gateway --test product_vertical \
  full_node_docker_enforces_crypto_image_tcp_filesystem_memory_and_deadline \
  --locked -- --exact --nocapture
