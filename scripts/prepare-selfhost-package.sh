#!/bin/sh
set -eu

test "$#" -eq 2 || { printf 'usage: prepare-selfhost-package.sh VERSION OUTPUT_DIRECTORY\n' >&2; exit 2; }
version=$1
output=$2
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || {
  printf 'invalid release version: %s\n' "$version" >&2
  exit 2
}

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
archive="runku-selfhost-v$version"
stage="$output/archive/$archive"
mkdir -p "$stage" "$output/release"
cp "$repository/deployments/docker/compose.yaml" "$stage/compose.yaml"
cp "$repository/deployments/docker/compose.browser.yaml" "$stage/compose.browser.yaml"
cp "$repository/deployments/docker/compose.ha-logs.yaml" "$stage/compose.ha-logs.yaml"
cp "$repository/deployments/docker/compose.ha-s3-compatible.yaml" "$stage/compose.ha-s3-compatible.yaml"
cp "$repository/deployments/docker/compose.s3-logs.yaml" "$stage/compose.s3-logs.yaml"
cp "$repository/deployments/docker/compose.s3-files.yaml" "$stage/compose.s3-files.yaml"
cp "$repository/deployments/docker/compose.s3-compatible.yaml" "$stage/compose.s3-compatible.yaml"
cp "$repository/deployments/docker/.env.example" "$stage/.env.example"
cp "$repository/deployments/docker/runku-selfhost" "$stage/runku-selfhost"
cp "$repository/distribution/SELFHOST-README.md" "$stage/README.md"
sed "s#](../../docs/#](https://github.com/aldemi-tech/runku/blob/v$version/docs/#g" \
  "$repository/deployments/docker/README.md" >"$stage/OPERATOR-GUIDE.md"
cp "$repository/LICENSE" "$stage/LICENSE"
chmod 0555 "$stage/runku-selfhost"

grep -Fqx "RUNKU_SERVER_IMAGE=ghcr.io/aldemi-tech/runku-server:$version@sha256:REPLACE_WITH_64_HEX_CHARACTERS" "$stage/.env.example" || {
  printf '.env.example is not coordinated with version %s\n' "$version" >&2
  exit 1
}
test -z "$(find "$stage" -type f \( -name '*password*' -o -name '*.creds' -o -name '*.key' \) -print -quit)" || {
  printf 'self-host package unexpectedly contains secret-like files\n' >&2
  exit 1
}
tar -czf "$output/release/$archive.tar.gz" -C "$output/archive" "$archive"
