import { appendFileSync, readFileSync } from "node:fs"
import process from "node:process"
import { releasePlatforms } from "./release-platforms.mjs"

const repositoryRoot = new URL("../", import.meta.url)
const rootPackage = readJson("package.json")
const cliPackage = readJson("packages/cli/package.json")
const clientPackage = readJson("packages/client/package.json")
const serverPackage = readJson("packages/server/package.json")
const version = cliPackage.version

assertVersion("root package", rootPackage.version, version)
assertVersion("@runku/client", clientPackage.version, version)
assertVersion("@runku/server", serverPackage.version, version)

const cargoManifest = read("crates/runku-cli/Cargo.toml")
const cargoVersion = cargoManifest.match(/^version = "([^"]+)"$/m)?.[1]
assertVersion("runku-cli Cargo package", cargoVersion, version)
const serverCargoManifest = read("crates/runku-server/Cargo.toml")
const serverCargoVersion = serverCargoManifest.match(/^version = "([^"]+)"$/m)?.[1]
assertVersion("runku-server Cargo package", serverCargoVersion, version)

const cliSource = read("crates/runku-cli/src/lib.rs")
if (!cliSource.includes(`runku ${version}\\n`)) {
  throw new Error(`crates/runku-cli/src/lib.rs does not report runku ${version}`)
}

const expectedOptionalDependencies = Object.fromEntries(
  releasePlatforms.map((platform) => [platform.packageName, version]),
)
assertEqual(
  "@runku/cli optionalDependencies",
  cliPackage.optionalDependencies,
  expectedOptionalDependencies,
)

const launcherSource = read("packages/cli/lib/platform.js")
const releaseWorkflow = read(".github/workflows/release.yml")
const cliWorkflow = section(releaseWorkflow, "  cli-binaries:", "  server-binaries:")
const serverWorkflow = section(releaseWorkflow, "  server-binaries:", "  server-image:")
const installGuide = read("docs/getting-started/local-development.md")

for (const platform of releasePlatforms) {
  const nativePackage = readJson(`${platform.packageDirectory}/package.json`)
  assertVersion(platform.packageName, nativePackage.version, version)
  assertEqual(`${platform.packageName} name`, nativePackage.name, platform.packageName)
  assertEqual(`${platform.packageName} os`, nativePackage.os, [platform.os])
  assertEqual(`${platform.packageName} cpu`, nativePackage.cpu, [platform.cpu])
  assertEqual(
    `${platform.packageName} libc`,
    nativePackage.libc,
    platform.libc === undefined ? undefined : [platform.libc],
  )
  assertContains(
    "CLI launcher mapping",
    launcherSource,
    `"${platform.os}-${platform.cpu}": "${platform.packageName}"`,
  )
  assertOccursOnce("CLI release workflow target", cliWorkflow, `target: ${platform.target}`)
  assertContains("installation target table", installGuide, `\`${platform.target}\``)
  assertContains("installation package table", installGuide, `\`${platform.packageName}\``)
}

for (const target of ["aarch64-unknown-linux-gnu", "x86_64-unknown-linux-gnu"]) {
  assertOccursOnce("server release workflow target", serverWorkflow, `target: ${target}`)
}
assertContains("server image workflow", releaseWorkflow, "ghcr.io/aldemi-tech/runku-server:${VERSION}")
assertContains(
  "server image base",
  read("deployments/docker/server.Dockerfile"),
  "gcr.io/distroless/cc-debian12:nonroot@sha256:",
)
assertContains("source install smoke version", read("Makefile"), `runku ${version.replaceAll(".", "\\.")}`)
assertContains(
  "self-host image example",
  read("deployments/docker/.env.example"),
  `ghcr.io/aldemi-tech/runku-server:${version}@sha256:REPLACE_WITH_64_HEX_CHARACTERS`,
)
assertContains("self-host release package", releaseWorkflow, "prepare-selfhost-package.sh")

const tagIndex = process.argv.indexOf("--tag")
if (tagIndex !== -1) {
  const tag = process.argv[tagIndex + 1]
  if (tag !== `v${version}`) {
    throw new Error(`release tag ${tag ?? "<missing>"} must equal v${version}`)
  }
}

if (process.env.GITHUB_OUTPUT) {
  appendFileSync(process.env.GITHUB_OUTPUT, `version=${version}\n`)
}
process.stdout.write(`release metadata is coherent for v${version}\n`)

function read(path) {
  return readFileSync(new URL(path, repositoryRoot), "utf8")
}

function readJson(path) {
  return JSON.parse(read(path))
}

function assertVersion(label, actual, expected) {
  if (actual !== expected) {
    throw new Error(`${label} version ${actual ?? "<missing>"} must equal ${expected}`)
  }
}

function assertEqual(label, actual, expected) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label} is ${JSON.stringify(actual)}; expected ${JSON.stringify(expected)}`)
  }
}

function assertContains(label, source, expected) {
  if (!source.includes(expected)) throw new Error(`${label} does not contain ${expected}`)
}

function assertOccursOnce(label, source, expected) {
  const occurrences = source.split(expected).length - 1
  if (occurrences !== 1) throw new Error(`${label} must contain ${expected} exactly once`)
}

function section(source, start, end) {
  const startIndex = source.indexOf(start)
  const endIndex = source.indexOf(end, startIndex + start.length)
  if (startIndex === -1 || endIndex === -1) {
    throw new Error(`release workflow section ${start}..${end} is missing`)
  }
  return source.slice(startIndex, endIndex)
}
