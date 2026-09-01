#!/usr/bin/env node

import { createRequire } from "node:module"
import { dirname, join } from "node:path"
import { spawnSync } from "node:child_process"
import process from "node:process"
import { nativeBinaryName, nativePackageName, supportedPlatforms } from "../lib/platform.js"

const packageName = nativePackageName()
if (packageName === null) {
  fail(
    `Runku does not publish a CLI binary for ${process.platform}-${process.arch}. ` +
      `Supported platforms: ${supportedPlatforms.join(", ")}.`,
  )
}

const require = createRequire(import.meta.url)
let packageJson
try {
  packageJson = require.resolve(`${packageName}/package.json`)
} catch {
  fail(
    `The native package ${packageName} is missing. Reinstall @runku/cli without omitting ` +
      "optional dependencies, or download the matching archive from the Runku GitHub release.",
  )
}

const binary = join(dirname(packageJson), "bin", nativeBinaryName())
const result = spawnSync(binary, process.argv.slice(2), {
  stdio: "inherit",
  windowsHide: false,
})

if (result.error) {
  fail(`Runku could not start the native CLI: ${result.error.message}`)
}
if (result.signal !== null) {
  if (process.platform !== "win32") process.kill(process.pid, result.signal)
  process.exit(1)
}
process.exit(result.status ?? 1)

function fail(message) {
  process.stderr.write(`error: RUNKU_CLI_LAUNCH_FAILED\nmessage: ${message}\n`)
  process.exit(1)
}
