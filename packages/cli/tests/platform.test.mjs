import assert from "node:assert/strict"
import test from "node:test"
import { nativeBinaryName, nativePackageName, supportedPlatforms } from "../lib/platform.js"

const cases = [
  ["darwin", "arm64", "@runku/cli-darwin-arm64", "runku"],
  ["darwin", "x64", "@runku/cli-darwin-x64", "runku"],
  ["linux", "arm64", "@runku/cli-linux-arm64-gnu", "runku"],
  ["linux", "x64", "@runku/cli-linux-x64-gnu", "runku"],
  ["win32", "arm64", "@runku/cli-win32-arm64-msvc", "runku.exe"],
  ["win32", "x64", "@runku/cli-win32-x64-msvc", "runku.exe"],
]

test("maps every published platform to its native package", () => {
  for (const [platform, architecture, packageName, binary] of cases) {
    assert.equal(nativePackageName(platform, architecture), packageName)
    assert.equal(nativeBinaryName(platform), binary)
  }
  assert.deepEqual(supportedPlatforms, cases.map(([platform, architecture]) => `${platform}-${architecture}`))
})

test("rejects platforms without a published artifact", () => {
  assert.equal(nativePackageName("freebsd", "x64"), null)
  assert.equal(nativePackageName("linux", "ia32"), null)
})
