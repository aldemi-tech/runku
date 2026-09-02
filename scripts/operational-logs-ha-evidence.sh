#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repository_root/compose.remote-execution.yml"
nats_port="${RUNKU_NATS_PORT:-54222}"
minio_port="${RUNKU_MINIO_PORT:-59000}"
worker_pid=""
worker_log="$(mktemp)"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  docker compose -f "$compose_file" down --volumes --remove-orphans >/dev/null
  rm -f "$worker_log"
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
cargo build -p runku-server --locked

RUNKU_LOG_ARCHIVE_BACKEND=s3 \
RUNKU_LOG_ARCHIVE_S3_BUCKET=runku-artifacts \
RUNKU_LOG_ARCHIVE_S3_REGION=us-east-1 \
RUNKU_LOG_ARCHIVE_S3_PREFIX=operational-log-conformance \
RUNKU_LOG_ARCHIVE_S3_ENDPOINT="http://127.0.0.1:${minio_port}" \
RUNKU_LOG_ARCHIVE_S3_ALLOW_HTTP=true \
RUNKU_LOG_JOURNAL_URL="nats://127.0.0.1:${nats_port}" \
RUNKU_LOG_JOURNAL_REPLICAS=1 \
RUNKU_LOG_ARCHIVE_BATCH_WAIT_SECONDS=1 \
AWS_ACCESS_KEY_ID=runku_test \
AWS_SECRET_ACCESS_KEY=runku_test_secret \
  "$repository_root/target/debug/runku-server" logs-worker >"$worker_log" 2>&1 &
worker_pid=$!

RUNKU_TEST_NATS_URL="nats://127.0.0.1:${nats_port}" \
RUNKU_TEST_S3_ENDPOINT="http://127.0.0.1:${minio_port}" \
RUNKU_TEST_S3_BUCKET="runku-artifacts" \
RUNKU_TEST_S3_ACCESS_KEY="runku_test" \
RUNKU_TEST_S3_SECRET_KEY="runku_test_secret" \
RUNKU_TEST_EXTERNAL_LOG_WORKER=true \
  cargo test -p runku-observability --test journal_conformance --locked -- --nocapture
