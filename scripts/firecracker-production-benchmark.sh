#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
asset_directory="${RUNKU_FIRECRACKER_ASSET_DIR:?RUNKU_FIRECRACKER_ASSET_DIR is required}"
workers="${RUNKU_BENCH_CONCURRENCY:-4}"
memory_mib="${RUNKU_FIRECRACKER_MEMORY_MIB:-256}"
iterations="${RUNKU_BENCH_ITERATIONS:-1000}"
warmups="${RUNKU_BENCH_WARMUPS:-25}"
requests="${RUNKU_BENCH_CONCURRENT_REQUESTS:-1000}"
routing_requests="${RUNKU_BENCH_ROUTING_REQUESTS:-500}"
open_loop_rps="${RUNKU_BENCH_OPEN_LOOP_RPS:-100}"
open_loop_duration="${RUNKU_BENCH_OPEN_LOOP_DURATION_SECS:-5}"
repetitions="${RUNKU_BENCH_REPETITIONS:-3}"
cpu_set="${RUNKU_BENCH_CPUSET:-}"
nats_url="${RUNKU_TEST_NATS_URL:?RUNKU_TEST_NATS_URL is required}"
s3_endpoint="${RUNKU_TEST_S3_ENDPOINT:?RUNKU_TEST_S3_ENDPOINT is required}"
s3_bucket="${RUNKU_TEST_S3_BUCKET:-runku-artifacts}"
s3_access_key="${RUNKU_TEST_S3_ACCESS_KEY:?RUNKU_TEST_S3_ACCESS_KEY is required}"
s3_secret_key="${RUNKU_TEST_S3_SECRET_KEY:?RUNKU_TEST_S3_SECRET_KEY is required}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
output_directory="${RUNKU_BENCH_OUTPUT_DIR:-/var/tmp/runku-firecracker-results/$timestamp}"
state_root="/var/tmp/runku-firecracker-production-$timestamp"
token_file="$state_root/ipc-token"
controller="$repository_root/scripts/runku-firecracker-controller.sh"
test_binary="$asset_directory/performance_benchmark"
image_reference="$(cat "$asset_directory/image-reference.txt")"
runner_endpoints=()

if test "$(id -u)" != 0 || test "$(uname -s)" != Linux || test "$(uname -m)" != x86_64; then
  echo "Firecracker production conformance requires root on x86_64 Linux" >&2
  exit 2
fi
test -r /dev/kvm
test -x "$controller"
test -x "$test_binary"
test -r "$asset_directory/SHA256SUMS"
(
  cd "$asset_directory"
  sha256sum --check --status SHA256SUMS
)
if test "$workers" -lt 1 || test "$workers" -gt 32; then
  echo "RUNKU_BENCH_CONCURRENCY must be between 1 and 32" >&2
  exit 2
fi
if test -n "$cpu_set" && ! [[ "$cpu_set" =~ ^[0-9,-]+$ ]]; then
  echo "RUNKU_BENCH_CPUSET is invalid" >&2
  exit 2
fi

mkdir -p "$output_directory" "$state_root"
chmod 0700 "$state_root"
openssl rand -hex 32 > "$token_file"
chmod 0600 "$token_file"
token="$(cat "$token_file")"
for index in $(seq 0 $((workers - 1))); do
  third_octet=$((220 + index))
  runner_endpoints+=("172.31.${third_octet}.2:32110")
done
addresses="$(IFS=,; echo "${runner_endpoints[*]}")"

controller_command() {
  env \
    RUNKU_FIRECRACKER_ASSET_DIR="$asset_directory" \
    RUNKU_FIRECRACKER_STATE_ROOT="$state_root" \
    RUNKU_FIRECRACKER_TOKEN_FILE="$token_file" \
    RUNKU_FIRECRACKER_WORKERS="$workers" \
    RUNKU_FIRECRACKER_MEMORY_MIB="$memory_mib" \
    RUNKU_FIRECRACKER_CPUSET="$cpu_set" \
    RUNKU_FIRECRACKER_IMAGE_REFERENCE="$image_reference" \
    RUNKU_FIRECRACKER_EGRESS_MODE=none \
    RUNKU_FIRECRACKER_EGRESS_ALLOW= \
    RUNKU_FIRECRACKER_EGRESS_DENY= \
    "$controller" "$1" "$2"
}

cleanup() {
  for index in $(seq 0 $((workers - 1))); do
    controller_command shutdown "$index" >/dev/null 2>&1 || true
  done
  if test -d "$state_root"; then
    find "$state_root" -depth -delete >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

boot_started_millis="$(($(date +%s%N) / 1000000))"
for index in $(seq 0 $((workers - 1))); do
  controller_command ensure "$index"
done
boot_ready_millis="$(($(date +%s%N) / 1000000))"

{
  date -u +"timestamp=%Y-%m-%dT%H:%M:%SZ"
  uname -a
  lscpu
  free -b
  "$asset_directory/firecracker" --version
  "$asset_directory/jailer" --version
  printf 'isolation=jailer+mount_namespace+pid_namespace+network_namespace+cgroup_v2\n'
  printf 'image=%s\n' "$image_reference"
  printf 'workers=%s memory_mib_per_worker=%s\n' "$workers" "$memory_mib"
  printf 'shared_cpu_set=%s\n' "${cpu_set:-unrestricted}"
  printf 'boot_to_ready_micros=%s\n' "$(((boot_ready_millis - boot_started_millis) * 1000))"
} > "$output_directory/environment-before.txt"

for repetition in $(seq 1 "$repetitions"); do
  benchmark_command=("$test_binary")
  if test -n "$cpu_set"; then
    benchmark_command=(taskset --cpu-list "$cpu_set" "$test_binary")
  fi
  env \
    RUNKU_FIRECRACKER_ASSET_DIR="$asset_directory" \
    RUNKU_FIRECRACKER_STATE_ROOT="$state_root" \
    RUNKU_FIRECRACKER_TOKEN_FILE="$token_file" \
    RUNKU_FIRECRACKER_WORKERS="$workers" \
    RUNKU_FIRECRACKER_MEMORY_MIB="$memory_mib" \
    RUNKU_FIRECRACKER_CPUSET="$cpu_set" \
    RUNKU_PERFORMANCE_BENCHMARK=1 \
    RUNKU_BENCH_MODES=node_firecracker_warm,remote_firecracker_warm \
    RUNKU_BENCH_OCI_IMAGE="$image_reference" \
    RUNKU_BENCH_FIRECRACKER_ENDPOINTS="$addresses" \
    RUNKU_BENCH_FIRECRACKER_TOKEN="$token" \
    RUNKU_BENCH_FIRECRACKER_CONTROLLER="$controller" \
    RUNKU_BENCH_ITERATIONS="$iterations" \
    RUNKU_BENCH_WARMUPS="$warmups" \
    RUNKU_BENCH_CONCURRENCY="$workers" \
    RUNKU_BENCH_CONCURRENT_REQUESTS="$requests" \
    RUNKU_BENCH_ROUTING_REQUESTS="$routing_requests" \
    RUNKU_BENCH_OPEN_LOOP_RPS="$open_loop_rps" \
    RUNKU_BENCH_OPEN_LOOP_DURATION_SECS="$open_loop_duration" \
    RUNKU_BENCH_REQUEST_TIMEOUT_SECS=120 \
    RUNKU_TEST_NATS_URL="$nats_url" \
    RUNKU_TEST_S3_BUCKET="$s3_bucket" \
    RUNKU_TEST_S3_ENDPOINT="$s3_endpoint" \
    RUNKU_TEST_S3_ACCESS_KEY="$s3_access_key" \
    RUNKU_TEST_S3_SECRET_KEY="$s3_secret_key" \
    "${benchmark_command[@]}" --exact full_execution_flow_performance_baseline --nocapture \
    > "$output_directory/repetition-$repetition.log" 2>&1
done

{
  free -b
  for index in $(seq 0 $((workers - 1))); do
    id="runku-worker-$index"
    pid_file="$state_root/jailer/firecracker/$id/root/firecracker.pid"
    pid="$(cat "$pid_file")"
    ps -o pid,rss,%cpu,etime,cmd -p "$pid"
    awk '/^(Rss|Pss|Private_Clean|Private_Dirty):/ { print "id='"$id"' " $0 }' \
      "/proc/$pid/smaps_rollup"
    cgroup="/sys/fs/cgroup/runku-firecracker/$id"
    for metric in cpu.stat memory.current memory.peak memory.events pids.current pids.peak io.stat; do
      if test -f "$cgroup/$metric"; then
        printf 'id=%s cgroup.%s.begin\n' "$id" "$metric"
        cat "$cgroup/$metric"
        printf 'id=%s cgroup.%s.end\n' "$id" "$metric"
      fi
    done
  done
} > "$output_directory/environment-after.txt"

grep -h '^RUNKU_BENCHMARK_REPORT ' "$output_directory"/repetition-*.log > "$output_directory/reports.jsonl"
grep -h '^RUNKU_FIRECRACKER_CONFORMANCE ' "$output_directory"/repetition-*.log > "$output_directory/conformance.jsonl"
test "$(wc -l < "$output_directory/reports.jsonl")" -eq "$repetitions"
test "$(wc -l < "$output_directory/conformance.jsonl")" -eq "$repetitions"

echo "benchmark_output=$output_directory"
