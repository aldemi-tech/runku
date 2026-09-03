#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repository_root/compose.remote-execution.yml"
minio_port="${RUNKU_MINIO_PORT:-59000}"
compose_project="runku-file-storage-evidence-$$"

cleanup() {
  docker compose -p "$compose_project" -f "$compose_file" down --volumes --remove-orphans >/dev/null
}
trap cleanup EXIT

docker compose -p "$compose_project" -f "$compose_file" up -d minio
for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:${minio_port}/minio/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:${minio_port}/minio/health/ready" >/dev/null
docker compose -p "$compose_project" -f "$compose_file" run --rm minio-init

RUNKU_TEST_S3_ENDPOINT="http://127.0.0.1:${minio_port}" \
RUNKU_TEST_FILE_S3_BUCKET="runku-files" \
RUNKU_TEST_S3_ACCESS_KEY="runku_test" \
RUNKU_TEST_S3_SECRET_KEY="runku_test_secret" \
  cargo test -p runku-file-storage --test minio_conformance --locked -- --nocapture

cargo test -p runku-file-storage --locked
cargo test -p runku-gateway --test file_transfers --locked
cargo test -p runku-runtime --test runtime_conformance action_file_storage_is_capability_scoped_and_typed --locked -- --exact
cargo test -p runku-node-runtime --test local_runtime declarative_full_node_action_uses_the_machine_node_binary --locked -- --exact
cargo test -p runku-local process_action_grants_drive_authenticated_http_file_transfer --locked
pnpm --dir packages/client test
