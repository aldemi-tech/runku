import { createHash } from "node:crypto"
import { readdirSync, readFileSync } from "node:fs"
import { join, resolve } from "node:path"
import { spawnSync } from "node:child_process"
import process from "node:process"
import { releasePlatforms } from "./release-platforms.mjs"

const directory = resolve(process.argv[2] ?? "")
if (!process.argv[2]) throw new Error("usage: publish-npm.mjs DIRECTORY")

const tarballs = findTarballs(directory).map(describeTarball)

const expectedNames = new Set([
  ...releasePlatforms.map((platform) => platform.packageName),
  "@runku/client",
  "@runku/server",
  "@runku/cli",
])
const actualNames = new Set(tarballs.map((tarball) => tarball.name))
if (
  tarballs.length !== expectedNames.size ||
  JSON.stringify([...actualNames].sort()) !== JSON.stringify([...expectedNames].sort())
) {
  throw new Error(`npm artifact set is incomplete: ${JSON.stringify([...actualNames].sort())}`)
}

tarballs.sort(
  (left, right) =>
    publishOrder(left.name) - publishOrder(right.name) || left.name.localeCompare(right.name),
)

for (const tarball of tarballs) {
  const spec = `${tarball.name}@${tarball.version}`
  const existing = run("npm", ["view", spec, "dist.integrity", "--json"], { allowFailure: true })
  if (existing.status === 0) {
    const publishedIntegrity = JSON.parse(existing.stdout.trim())
    if (publishedIntegrity !== tarball.integrity) {
      throw new Error(`${spec} already exists with different bytes`)
    }
    process.stdout.write(`verified existing ${spec}\n`)
    continue
  }
  if (!isMissingPackage(existing)) {
    throw new Error(`could not determine whether ${spec} exists: ${existing.stderr.trim()}`)
  }
  if (hasPublishedDistTag(tarball.name, tarball.version)) {
    process.stdout.write(`verified existing ${spec} while registry metadata propagates\n`)
    continue
  }

  run("npm", ["publish", tarball.path, "--access", "public"])
  const published = await waitForPublishedState(tarball)
  if (published && published !== tarball.integrity) {
    throw new Error(`${spec} registry integrity does not match the tarball`)
  }
  process.stdout.write(`published and verified ${spec}\n`)
}

function describeTarball(path) {
  const manifest = JSON.parse(run("tar", ["-xOzf", path, "package/package.json"]).stdout)
  const integrity = `sha512-${createHash("sha512").update(readFileSync(path)).digest("base64")}`
  return { path, name: manifest.name, version: manifest.version, integrity }
}

function findTarballs(parent) {
  return readdirSync(parent, { withFileTypes: true }).flatMap((entry) => {
    const path = join(parent, entry.name)
    if (entry.isDirectory()) return findTarballs(path)
    return entry.isFile() && entry.name.endsWith(".tgz") ? [path] : []
  })
}

function publishOrder(name) {
  if (name.startsWith("@runku/cli-")) return 0
  if (name === "@runku/client" || name === "@runku/server") return 1
  if (name === "@runku/cli") return 2
  return 3
}

function isMissingPackage(result) {
  return `${result.stdout}\n${result.stderr}`.includes("E404")
}

async function waitForPublishedState(tarball) {
  const spec = `${tarball.name}@${tarball.version}`
  for (let attempt = 1; attempt <= 10; attempt += 1) {
    const result = run("npm", ["view", spec, "dist.integrity", "--json"], { allowFailure: true })
    if (result.status === 0) return JSON.parse(result.stdout.trim())
    if (!isMissingPackage(result)) throw new Error(`registry verification failed for ${spec}`)
    if (hasPublishedDistTag(tarball.name, tarball.version)) return null
    if (attempt < 10) await new Promise((resolvePromise) => setTimeout(resolvePromise, 1_000))
  }
  throw new Error(`${spec} was not acknowledged by the registry after publication`)
}

function hasPublishedDistTag(name, version) {
  const result = run("npm", ["dist-tag", "ls", name], { allowFailure: true })
  if (result.status !== 0) {
    if (isMissingPackage(result)) return false
    throw new Error(`could not inspect dist-tags for ${name}: ${result.stderr.trim()}`)
  }
  return result.stdout
    .split("\n")
    .map((line) => line.slice(line.indexOf(":") + 1).trim())
    .includes(version)
}

function run(command, argumentsValue, options = {}) {
  const result = spawnSync(command, argumentsValue, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  })
  if (result.status !== 0 && !options.allowFailure) {
    if (result.stderr) process.stderr.write(result.stderr)
    throw new Error(`${command} ${argumentsValue.join(" ")} exited ${result.status ?? "without status"}`)
  }
  return result
}
