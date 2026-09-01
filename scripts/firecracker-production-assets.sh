#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="${RUNKU_FIRECRACKER_VERSION:-v1.16.1}"
archive_sha256="${RUNKU_FIRECRACKER_ARCHIVE_SHA256:-382a02a869e4d6d5cb14c40577f9545e8458021ea8b0b2d3fc10ec14d9c242e6}"
kernel_key="${RUNKU_FIRECRACKER_KERNEL_KEY:-firecracker-ci/20260826-761f88fbb951-0/x86_64/vmlinux-6.18.44}"
kernel_sha256="${RUNKU_FIRECRACKER_KERNEL_SHA256:-435466ec838656f59e464ce941e7fe9f3697d5da6a73c5e5dad60dae5ad93ceb}"
dns_servers="${RUNKU_FIRECRACKER_DNS_SERVERS:-1.1.1.1,1.0.0.1}"
registry_port="${RUNKU_OCI_REGISTRY_PORT:-55001}"
output_directory="${RUNKU_FIRECRACKER_OUTPUT_DIR:-$repository_root/benchmarks/full-node/firecracker-assets}"
registry_name="runku-firecracker-assets-registry"
registry_volume="runku-firecracker-assets-registry"
repository="127.0.0.1:${registry_port}/runku/firecracker-production"
base_image="docker.io/library/node:22.19.0-bookworm-slim@sha256:4a4884e8a44826194dff92ba316264f392056cbe243dcc9fd3551e71cea02b90"
busybox_image="docker.io/library/busybox:1.36.1-musl@sha256:3c6ae8008e2c2eedd141725c30b20d9c36b026eb796688f88205845ef17aa213"
published_image=""
node_container=""
busybox_container=""
staging=""

if test "$(uname -s)" != "Linux" || test "$(uname -m)" != "x86_64"; then
  echo "asset construction requires x86_64 Linux" >&2
  exit 2
fi
[[ "$dns_servers" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(,[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)*$ ]]
[[ "$archive_sha256" =~ ^[0-9a-f]{64}$ ]]
[[ "$kernel_key" =~ ^firecracker-ci/[0-9]{8}-[0-9a-f-]+/x86_64/vmlinux-[0-9]+\.[0-9]+\.[0-9]+$ ]]
[[ "$kernel_sha256" =~ ^[0-9a-f]{64}$ ]]
for command in cargo curl docker mkfs.ext4 sha256sum sudo tar truncate; do
  command -v "$command" >/dev/null || { echo "missing command: $command" >&2; exit 2; }
done
sudo -n true

cleanup() {
  test -z "$node_container" || docker rm -f "$node_container" >/dev/null 2>&1 || true
  test -z "$busybox_container" || docker rm -f "$busybox_container" >/dev/null 2>&1 || true
  test -z "$published_image" || docker image rm "$published_image" >/dev/null 2>&1 || true
  if test -n "$staging" && test -d "$staging"; then
    sudo find "$staging" -depth -delete >/dev/null 2>&1 || true
  fi
  docker rm -f "$registry_name" >/dev/null 2>&1 || true
  docker volume rm "$registry_volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
mkdir -p "$output_directory"

docker volume create "$registry_volume" >/dev/null
docker run --detach --name "$registry_name" \
  --publish "127.0.0.1:${registry_port}:5000" \
  --mount "type=volume,src=${registry_volume},dst=/var/lib/registry" \
  registry:2.8.3@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373 \
  >/dev/null
for _ in $(seq 1 30); do
  curl --fail --silent "http://127.0.0.1:${registry_port}/v2/" >/dev/null && break
  sleep 1
done
curl --fail --silent "http://127.0.0.1:${registry_port}/v2/" >/dev/null

RUNKU_TEST_OCI_BUILDER="$(command -v docker)" \
RUNKU_TEST_NODE_BASE_IMAGE="$base_image" \
RUNKU_TEST_OCI_REPOSITORY="$repository" \
  cargo test -p runku-build \
    node_oci::tests::publisher_builds_pushes_and_executes_real_oci_image \
    --release --locked -- --exact --nocapture \
    > "$output_directory/publisher.log" 2>&1
published_image="$(sed -n 's/^RUNKU_PUBLISHED_IMAGE //p' "$output_directory/publisher.log" | tail -n 1)"
case "$published_image" in
  "${repository}@sha256:"*) ;;
  *) echo "publisher did not emit an immutable OCI image" >&2; exit 1 ;;
esac
printf '%s\n' "$published_image" > "$output_directory/image-reference.txt"
docker pull "$published_image" >/dev/null

staging="$(mktemp -d)"
node_container="$(docker create "$published_image")"
busybox_container="$(docker create "$busybox_image")"
docker export "$node_container" | sudo tar -xpf - -C "$staging"
docker cp "$busybox_container:/bin/busybox" "$output_directory/busybox"
sudo install -m 0755 "$output_directory/busybox" "$staging/bin/busybox"
sudo install -m 0755 "$repository_root/deployments/full-node-microvm/runku-init" "$staging/sbin/runku-init"
sudo install -d -m 0755 "$staging/tmp" "$staging/proc" "$staging/sys"
resolver_file="$output_directory/resolv.conf"
: > "$resolver_file"
old_ifs="$IFS"
IFS=','
read -r -a resolvers <<< "$dns_servers"
IFS="$old_ifs"
for resolver in "${resolvers[@]}"; do
  printf 'nameserver %s\n' "$resolver" >> "$resolver_file"
done
sudo install -m 0644 "$resolver_file" "$staging/etc/resolv.conf"
rm -f "$resolver_file"
rootfs_staging="$output_directory/rootfs.ext4.staging"
truncate -s 768M "$rootfs_staging"
sudo mkfs.ext4 -q -F -d "$staging" "$rootfs_staging"
mv "$rootfs_staging" "$output_directory/rootfs.ext4"

archive="$output_directory/firecracker.tgz"
curl -fL "https://github.com/firecracker-microvm/firecracker/releases/download/${version}/firecracker-${version}-x86_64.tgz" -o "$archive"
printf '%s  %s\n' "$archive_sha256" "$archive" | sha256sum --check --status
tar -xzf "$archive" -C "$output_directory"
install -m 0755 \
  "$output_directory/release-${version}-x86_64/firecracker-${version}-x86_64" \
  "$output_directory/firecracker"
install -m 0755 \
  "$output_directory/release-${version}-x86_64/jailer-${version}-x86_64" \
  "$output_directory/jailer"

s3="https://s3.amazonaws.com/spec.ccfc.min"
curl -fL "$s3/$kernel_key" -o "$output_directory/vmlinux"
printf '%s  %s\n' "$kernel_sha256" "$output_directory/vmlinux" | sha256sum --check --status
printf '%s\n' "$kernel_key" > "$output_directory/kernel-source.txt"

cargo test -p runku-node-runtime --test performance_benchmark --release --locked --no-run
test_binary="$(
  find "$repository_root/target/release/deps" -maxdepth 1 -type f \
    -name 'performance_benchmark-*' -perm -u+x -print0 | xargs -0 ls -1t | head -n 1
)"
test -n "$test_binary"
install -m 0755 "$test_binary" "$output_directory/performance_benchmark"

rm -f "$archive" "$output_directory/busybox"
find "$output_directory/release-${version}-x86_64" -depth -delete
{
  date -u +"timestamp=%Y-%m-%dT%H:%M:%SZ"
  printf 'firecracker_version=%s\n' "$version"
  printf 'firecracker_archive_sha256=%s\n' "$archive_sha256"
  printf 'image=%s\n' "$published_image"
  printf 'kernel=%s\n' "$kernel_key"
  printf 'kernel_sha256=%s\n' "$kernel_sha256"
  printf 'dns_servers=%s\n' "$dns_servers"
  "$output_directory/firecracker" --version
  rustc --version
  cargo --version
  docker --version
} > "$output_directory/build-environment.txt"
(
  cd "$output_directory"
  find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 \
    | sort -z | xargs -0 sha256sum > SHA256SUMS
)

echo "firecracker_assets=$output_directory"
