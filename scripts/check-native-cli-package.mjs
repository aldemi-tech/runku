import { accessSync, constants, readFileSync } from "node:fs"
import { join } from "node:path"
import process from "node:process"
import { platformForPackage } from "./release-platforms.mjs"

const packageDirectory = process.cwd()
const packageJson = JSON.parse(readFileSync(join(packageDirectory, "package.json"), "utf8"))
const platform = platformForPackage(packageJson.name)
if (platform === null) throw new Error(`${packageJson.name} is not a declared Runku CLI target`)

for (const path of [
  join(packageDirectory, "bin", platform.binaryName),
  join(packageDirectory, "LICENSE"),
  join(packageDirectory, "README.md"),
]) {
  accessSync(path, constants.R_OK)
}

process.stdout.write(`${packageJson.name}@${packageJson.version} contains ${platform.binaryName}\n`)
