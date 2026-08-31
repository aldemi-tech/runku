#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
remote_compose="$repository_root/compose.remote-execution.yml"
remote_project="runku-bench-remote"
nats_port="${RUNKU_NATS_PORT:-54222}"
minio_port="${RUNKU_MINIO_PORT:-59000}"
iterations="${RUNKU_BENCH_ITERATIONS:-20}"
warmups="${RUNKU_BENCH_WARMUPS:-3}"
concurrency="${RUNKU_BENCH_CONCURRENCY:-4}"
concurrent_requests="${RUNKU_BENCH_CONCURRENT_REQUESTS:-32}"
request_timeout_secs="${RUNKU_BENCH_REQUEST_TIMEOUT_SECS:-15}"
routing_requests="${RUNKU_BENCH_ROUTING_REQUESTS:-$concurrent_requests}"
open_loop_rps="${RUNKU_BENCH_OPEN_LOOP_RPS:-0}"
open_loop_duration_secs="${RUNKU_BENCH_OPEN_LOOP_DURATION_SECS:-0}"
host_modes="${RUNKU_BENCH_HOST_MODES:-safe_v8,node_local,node_host,remote_host}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
output_directory="${RUNKU_BENCH_OUTPUT_DIR:-$repository_root/benchmarks/full-node/$timestamp}"
asset_directory="${RUNKU_FIRECRACKER_ASSET_DIR:-}"
image_reference="registry.invalid/runku/performance@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

mkdir -p "$output_directory"
cleanup() {
  docker compose -p "$remote_project" -f "$remote_compose" down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

{
  date -u +"timestamp=%Y-%m-%dT%H:%M:%SZ"
  uname -a
  rustc --version
  cargo --version
  node --version
} > "$output_directory/environment.txt"

docker compose -p "$remote_project" -f "$remote_compose" up -d nats minio
for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:${minio_port}/minio/health/ready" >/dev/null \
    && nc -z 127.0.0.1 "$nats_port"; then
    break
  fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:${minio_port}/minio/health/ready" >/dev/null
nc -z 127.0.0.1 "$nats_port"
docker compose -p "$remote_project" -f "$remote_compose" run --rm minio-init >/dev/null

if test -n "$asset_directory"; then
  image_reference="$(cat "$asset_directory/image-reference.txt")"
fi

RUNKU_PERFORMANCE_BENCHMARK=1 \
RUNKU_BENCH_MODES="$host_modes" \
RUNKU_BENCH_OCI_IMAGE="$image_reference" \
RUNKU_BENCH_ITERATIONS="$iterations" \
RUNKU_BENCH_WARMUPS="$warmups" \
RUNKU_BENCH_CONCURRENCY="$concurrency" \
RUNKU_BENCH_CONCURRENT_REQUESTS="$concurrent_requests" \
RUNKU_BENCH_REQUEST_TIMEOUT_SECS="$request_timeout_secs" \
RUNKU_BENCH_ROUTING_REQUESTS="$routing_requests" \
RUNKU_BENCH_OPEN_LOOP_RPS="$open_loop_rps" \
RUNKU_BENCH_OPEN_LOOP_DURATION_SECS="$open_loop_duration_secs" \
RUNKU_TEST_NATS_URL="nats://127.0.0.1:${nats_port}" \
RUNKU_TEST_S3_ENDPOINT="http://127.0.0.1:${minio_port}" \
RUNKU_TEST_S3_BUCKET="runku-artifacts" \
RUNKU_TEST_S3_ACCESS_KEY="runku_test" \
RUNKU_TEST_S3_SECRET_KEY="runku_test_secret" \
  cargo test -p runku-node-runtime --test performance_benchmark \
    --release --locked -- --exact full_execution_flow_performance_baseline --nocapture \
    2>&1 | tee "$output_directory/host.log"

node "$repository_root/scripts/full-node-performance-report.mjs" \
  "$output_directory" "$output_directory/host.log"

if test -n "$asset_directory"; then
  if test "$(uname -s)" != Linux || test "$(id -u)" -ne 0; then
    echo "Firecracker assets were supplied but the benchmark is not root on Linux" >&2
    exit 2
  fi
  RUNKU_FIRECRACKER_ASSET_DIR="$asset_directory" \
  RUNKU_BENCH_OUTPUT_DIR="$output_directory/firecracker" \
  RUNKU_TEST_NATS_URL="nats://127.0.0.1:${nats_port}" \
  RUNKU_TEST_S3_ENDPOINT="http://127.0.0.1:${minio_port}" \
  RUNKU_TEST_S3_BUCKET="runku-artifacts" \
  RUNKU_TEST_S3_ACCESS_KEY="runku_test" \
  RUNKU_TEST_S3_SECRET_KEY="runku_test_secret" \
    "$repository_root/scripts/firecracker-production-benchmark.sh"
fi

echo "benchmark_output=$output_directory"
