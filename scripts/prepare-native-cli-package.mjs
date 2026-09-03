import { appendFileSync, chmodSync, copyFileSync, mkdirSync, readFileSync, rmSync } from "node:fs"
import { basename, join, resolve } from "node:path"
import process from "node:process"
import { fileURLToPath } from "node:url"
import { platformForTarget } from "./release-platforms.mjs"

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url))
const [target, binaryArgument, outputArgument] = process.argv.slice(2)
const platform = platformForTarget(target)
if (platform === null) throw new Error(`unsupported release target: ${target ?? "<missing>"}`)
if (!binaryArgument || !outputArgument) {
  throw new Error("usage: prepare-native-cli-package.mjs TARGET BINARY OUTPUT_DIRECTORY")
}

const binary = resolve(binaryArgument)
if (basename(binary) !== platform.binaryName) {
  throw new Error(`binary for ${target} must be named ${platform.binaryName}`)
}

const packageDirectory = join(repositoryRoot, platform.packageDirectory)
const packageBinDirectory = join(packageDirectory, "bin")
rmSync(packageBinDirectory, { recursive: true, force: true })
mkdirSync(packageBinDirectory, { recursive: true })
copyExecutable(binary, join(packageBinDirectory, platform.binaryName))
copyRuntimeLibraries(packageBinDirectory)
copyFileSync(join(repositoryRoot, "LICENSE"), join(packageDirectory, "LICENSE"))
copyFileSync(
  join(repositoryRoot, "packages", "cli-native-README.md"),
  join(packageDirectory, "README.md"),
)

const version = JSON.parse(
  readFileSync(join(repositoryRoot, "packages", "cli", "package.json"), "utf8"),
).version
const archiveBase = `runku-v${version}-${target}`
const archiveDirectory = join(resolve(outputArgument), "archive", archiveBase)
rmSync(archiveDirectory, { recursive: true, force: true })
mkdirSync(archiveDirectory, { recursive: true })
copyExecutable(binary, join(archiveDirectory, platform.binaryName))
copyRuntimeLibraries(archiveDirectory)
copyFileSync(join(repositoryRoot, "LICENSE"), join(archiveDirectory, "LICENSE"))
copyFileSync(
  join(repositoryRoot, "distribution", "CLI-README.md"),
  join(archiveDirectory, "README.md"),
)

writeOutput("package_directory", platform.packageDirectory)
writeOutput("archive_base", archiveBase)
writeOutput("archive_directory", archiveDirectory)
writeOutput("archive_format", platform.archiveFormat)
process.stdout.write(`prepared ${platform.packageName} and ${archiveBase}\n`)

function copyExecutable(source, destination) {
  copyFileSync(source, destination)
  if (platform.os !== "win32") chmodSync(destination, 0o755)
}

function copyRuntimeLibraries(destinationDirectory) {
  for (const library of platform.runtimeLibraries ?? []) {
    copyFileSync(join(binary, "..", library), join(destinationDirectory, library))
  }
}

function writeOutput(name, value) {
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`)
}
