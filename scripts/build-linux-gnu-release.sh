#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <runku-cli|runku-server> <version>" >&2
  exit 64
fi

readonly package="$1"
readonly version="$2"
readonly builder_image="rust:1.98-bullseye@sha256:4730e387a220a08a365c77da3096544dde214f9d796c16284d4be45438cad4a9"
readonly rust_toolchain="1.98.0"

case "$package" in
  runku-cli)
    readonly binary="runku"
    readonly version_argument="--version"
    readonly expected_version="runku ${version}"
    ;;
  runku-server)
    readonly binary="runku-server"
    readonly version_argument="version"
    readonly expected_version="runku-server ${version}"
    ;;
  *)
    echo "unsupported Linux release package: $package" >&2
    exit 64
    ;;
esac

case "$(uname -m)" in
  aarch64 | arm64)
    readonly platform="linux/arm64"
    readonly expected_target="aarch64-unknown-linux-gnu"
    ;;
  x86_64 | amd64)
    readonly platform="linux/amd64"
    readonly expected_target="x86_64-unknown-linux-gnu"
    ;;
  *)
    echo "unsupported Linux release architecture: $(uname -m)" >&2
    exit 64
    ;;
esac

readonly workspace="$(pwd -P)"
readonly cargo_cache="${RUNKU_RELEASE_CARGO_CACHE:-${HOME}/.cargo}"
readonly target_dir="${RUNKU_RELEASE_TARGET_DIR:-${workspace}/target}"
mkdir -p "$cargo_cache" "$target_dir"

builder_target="$({
  docker run --rm --pull=always --platform "$platform" \
    -e "RUSTUP_TOOLCHAIN=${rust_toolchain}" \
    "$builder_image" rustc -vV
} | sed -n 's/^host: //p')"
if [[ "$builder_target" != "$expected_target" ]]; then
  echo "Linux release builder target ${builder_target:-<missing>} does not match $expected_target" >&2
  exit 1
fi

docker run --rm --pull=never --platform "$platform" \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp/runku-release-home \
  -e "RUSTUP_TOOLCHAIN=${rust_toolchain}" \
  -e CARGO_HOME=/cargo \
  -e CARGO_TARGET_DIR=/target \
  -e CARGO_BUILD_JOBS=2 \
  -e CARGO_INCREMENTAL=0 \
  -e CARGO_PROFILE_RELEASE_DEBUG=0 \
  -e CARGO_PROFILE_RELEASE_STRIP=symbols \
  -v "${cargo_cache}:/cargo" \
  -v "${target_dir}:/target" \
  -v "${workspace}:/work:ro" \
  -w /work \
  "$builder_image" \
  cargo build --package "$package" --release --locked

actual_version="$(
  docker run --rm --pull=never --platform "$platform" \
    -v "${target_dir}/release/${binary}:/artifact:ro" \
    "$builder_image" /artifact "$version_argument"
)"
if [[ "$actual_version" != "$expected_version" ]]; then
  echo "unexpected ${binary} baseline version output: $actual_version" >&2
  exit 1
fi

echo "$binary $version passed the pinned GNU/Linux glibc 2.31 runtime baseline ($expected_target)"
