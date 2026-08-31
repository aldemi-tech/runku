#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repository_root/compose.remote-execution.yml"
nats_port="${RUNKU_NATS_PORT:-54222}"
minio_port="${RUNKU_MINIO_PORT:-59000}"

cleanup() {
  docker compose -f "$compose_file" down --volumes --remove-orphans >/dev/null
}
trap cleanup EXIT

docker compose -f "$compose_file" up -d nats minio

for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:${minio_port}/minio/health/ready" >/dev/null \
    && nc -z 127.0.0.1 "$nats_port"; then
    break
  fi
  sleep 1
done

curl --fail --silent "http://127.0.0.1:${minio_port}/minio/health/ready" >/dev/null
nc -z 127.0.0.1 "$nats_port"
docker compose -f "$compose_file" run --rm minio-init

RUNKU_TEST_S3_ENDPOINT="http://127.0.0.1:${minio_port}" \
RUNKU_TEST_S3_BUCKET="runku-artifacts" \
RUNKU_TEST_S3_ACCESS_KEY="runku_test" \
RUNKU_TEST_S3_SECRET_KEY="runku_test_secret" \
  cargo test -p runku-artifact-s3 --test minio_conformance --locked -- --nocapture

RUNKU_TEST_NATS_URL="nats://127.0.0.1:${nats_port}" \
  cargo test -p runku-execution-queue --test nats_conformance --locked -- --nocapture

RUNKU_TEST_NATS_URL="nats://127.0.0.1:${nats_port}" \
  cargo test -p runku-node-runtime --test remote_execution \
    nats_gateway_agent_result_and_cancellation_vertical --locked -- --exact --nocapture

cargo test -p runku-artifact-s3 -p runku-execution-queue --locked
cargo clippy -p runku-artifact-s3 -p runku-execution-queue --all-targets -- -D warnings
