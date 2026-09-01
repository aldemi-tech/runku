const PACKAGE_BY_PLATFORM = Object.freeze({
  "darwin-arm64": "@runku/cli-darwin-arm64",
  "darwin-x64": "@runku/cli-darwin-x64",
  "linux-arm64": "@runku/cli-linux-arm64-gnu",
  "linux-x64": "@runku/cli-linux-x64-gnu",
  "win32-arm64": "@runku/cli-win32-arm64-msvc",
  "win32-x64": "@runku/cli-win32-x64-msvc",
})

export function nativePackageName(platform = process.platform, architecture = process.arch) {
  return PACKAGE_BY_PLATFORM[`${platform}-${architecture}`] ?? null
}

export function nativeBinaryName(platform = process.platform) {
  return platform === "win32" ? "runku.exe" : "runku"
}

export const supportedPlatforms = Object.freeze(Object.keys(PACKAGE_BY_PLATFORM))
