export const releasePlatforms = Object.freeze([
  Object.freeze({
    target: "aarch64-apple-darwin",
    packageName: "@runku/cli-darwin-arm64",
    packageDirectory: "packages/cli-darwin-arm64",
    os: "darwin",
    cpu: "arm64",
    binaryName: "runku",
    archiveFormat: "tar.gz",
  }),
  Object.freeze({
    target: "x86_64-apple-darwin",
    packageName: "@runku/cli-darwin-x64",
    packageDirectory: "packages/cli-darwin-x64",
    os: "darwin",
    cpu: "x64",
    binaryName: "runku",
    archiveFormat: "tar.gz",
  }),
  Object.freeze({
    target: "aarch64-unknown-linux-gnu",
    packageName: "@runku/cli-linux-arm64-gnu",
    packageDirectory: "packages/cli-linux-arm64-gnu",
    os: "linux",
    cpu: "arm64",
    libc: "glibc",
    binaryName: "runku",
    archiveFormat: "tar.gz",
  }),
  Object.freeze({
    target: "x86_64-unknown-linux-gnu",
    packageName: "@runku/cli-linux-x64-gnu",
    packageDirectory: "packages/cli-linux-x64-gnu",
    os: "linux",
    cpu: "x64",
    libc: "glibc",
    binaryName: "runku",
    archiveFormat: "tar.gz",
  }),
  Object.freeze({
    target: "aarch64-pc-windows-msvc",
    packageName: "@runku/cli-win32-arm64-msvc",
    packageDirectory: "packages/cli-win32-arm64-msvc",
    os: "win32",
    cpu: "arm64",
    binaryName: "runku.exe",
    archiveFormat: "zip",
  }),
  Object.freeze({
    target: "x86_64-pc-windows-msvc",
    packageName: "@runku/cli-win32-x64-msvc",
    packageDirectory: "packages/cli-win32-x64-msvc",
    os: "win32",
    cpu: "x64",
    binaryName: "runku.exe",
    archiveFormat: "zip",
  }),
])

export function platformForTarget(target) {
  return releasePlatforms.find((platform) => platform.target === target) ?? null
}

export function platformForPackage(packageName) {
  return releasePlatforms.find((platform) => platform.packageName === packageName) ?? null
}
