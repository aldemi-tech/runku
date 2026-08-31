#!/usr/bin/env bash
set -euo pipefail

# Kubernetes conformance entrypoint. A product Agent binary is intentionally outside this harness.

asset_directory="${RUNKU_FIRECRACKER_ASSET_DIR:-/opt/runku/assets}"
controller="${RUNKU_BENCH_FIRECRACKER_CONTROLLER:-/opt/runku/runku-firecracker-controller.sh}"
state_root="${RUNKU_FIRECRACKER_STATE_ROOT:-/var/lib/runku/firecracker}"
workers="${RUNKU_BENCH_CONCURRENCY:-2}"
token_file="$state_root/ipc-token"
runner_endpoints=()

test "$(id -u)" = 0
test "$(uname -s)" = Linux
test "$(uname -m)" = x86_64
test -r /dev/kvm
test -x "$controller"
test -x "$asset_directory/performance_benchmark"
test "$workers" -ge 1 && test "$workers" -le 32

mkdir -p "$state_root"
chmod 0700 "$state_root"
openssl rand -hex 32 > "$token_file"
chmod 0600 "$token_file"
token="$(cat "$token_file")"
image_reference="$(cat "$asset_directory/image-reference.txt")"
for index in $(seq 0 $((workers - 1))); do
  third_octet=$((220 + index))
  runner_endpoints+=("172.31.${third_octet}.2:32110")
done
addresses="$(IFS=,; echo "${runner_endpoints[*]}")"

exec env \
  RUNKU_FIRECRACKER_ASSET_DIR="$asset_directory" \
  RUNKU_FIRECRACKER_STATE_ROOT="$state_root" \
  RUNKU_FIRECRACKER_TOKEN_FILE="$token_file" \
  RUNKU_FIRECRACKER_WORKERS="$workers" \
  RUNKU_BENCH_FIRECRACKER_ENDPOINTS="$addresses" \
  RUNKU_BENCH_FIRECRACKER_TOKEN="$token" \
  RUNKU_BENCH_FIRECRACKER_CONTROLLER="$controller" \
  RUNKU_BENCH_OCI_IMAGE="$image_reference" \
  RUNKU_KUBERNETES_EXECUTION_AGENT=1 \
  "$asset_directory/performance_benchmark" \
    --exact kubernetes_distributed_execution_agent --nocapture
