import { appendFileSync, readFileSync } from "node:fs"
import process from "node:process"

const repositoryRoot = new URL("../", import.meta.url)
const clientPackage = readJson("packages/client/package.json")
const reactPackage = readJson("packages/react/package.json")
const version = clientPackage.version

assertVersion("@runku/react", reactPackage.version, version)
assertVersion(
  "@runku/react peer @runku/client",
  reactPackage.peerDependencies["@runku/client"],
  version,
)

const tagIndex = process.argv.indexOf("--tag")
if (tagIndex !== -1) {
  const tag = process.argv[tagIndex + 1]
  if (tag !== `sdk-v${version}`) {
    throw new Error(`SDK release tag ${tag ?? "<missing>"} must equal sdk-v${version}`)
  }
}

if (process.env.GITHUB_OUTPUT) {
  appendFileSync(process.env.GITHUB_OUTPUT, `version=${version}\n`)
}
process.stdout.write(`frontend SDK release metadata is coherent for sdk-v${version}\n`)

function readJson(path) {
  return JSON.parse(readFileSync(new URL(path, repositoryRoot), "utf8"))
}

function assertVersion(label, actual, expected) {
  if (actual !== expected) {
    throw new Error(`${label} version ${actual ?? "<missing>"} must equal ${expected}`)
  }
}
