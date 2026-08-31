import assert from "node:assert/strict"
import { createHash } from "node:crypto"
import { spawn, spawnSync } from "node:child_process"
import { once } from "node:events"
import { readFile } from "node:fs/promises"
import test from "node:test"
import { RunkuClient, RunkuError } from "@runku/client"
import { PNG } from "pngjs"

const binary = process.env.RUNKU_BIN ?? "runku"
const projectRoot = new URL("..", import.meta.url)

function prepare() {
  const help = spawnSync(binary, ["--help"], { encoding: "utf8" })
  if (help.error?.code === "ENOENT") {
    throw new Error(
      `RunKu CLI was not found (${JSON.stringify(binary)}). `
      + "Install it from the repository root with `make install-cli` or set RUNKU_BIN.",
    )
  }
  if (help.status !== 0 || !help.stdout.includes("[--prepare]")) {
    throw new Error(
      `The selected RunKu CLI (${JSON.stringify(binary)}) is outdated and does not support `
      + "`runku dev --prepare`. Reinstall it from the repository root with `make install-cli`, "
      + "or point RUNKU_BIN to the current binary.",
    )
  }
  const result = spawnSync(binary, ["dev", "--prepare"], {
    cwd: projectRoot,
    encoding: "utf8",
  })
  if (result.status !== 0) {
    throw new Error(`runku dev --prepare failed:\n${result.stderr.trim()}`)
  }
}

async function environment() {
  const source = await readFile(new URL(".env.local", projectRoot), "utf8")
  return Object.fromEntries(
    source
      .split(/\r?\n/u)
      .filter((line) => /^[A-Z][A-Z0-9_]*=/u.test(line))
      .map((line) => {
        const separator = line.indexOf("=")
        return [line.slice(0, separator), line.slice(separator + 1)]
      }),
  )
}

async function waitUntilReady(baseUrl, child) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (child.exitCode !== null) throw new Error(`runku dev exited with ${child.exitCode}`)
    try {
      const response = await fetch(`${baseUrl}/readyz`)
      if (response.ok) return
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error("runku dev did not become ready")
}

async function startRuntime(baseUrl) {
  const child = spawn(binary, ["dev"], {
    cwd: projectRoot,
    stdio: ["ignore", "ignore", "pipe"],
  })
  let stderr = ""
  child.stderr.setEncoding("utf8")
  child.stderr.on("data", (chunk) => { stderr += chunk })
  await waitUntilReady(baseUrl, child)
  return { child, stderr: () => stderr }
}

async function stopRuntime(runtime) {
  if (runtime.child.exitCode === null) {
    runtime.child.kill("SIGINT")
    await Promise.race([
      once(runtime.child, "exit"),
      new Promise((_, reject) => {
        setTimeout(() => reject(new Error("runku dev did not stop")), 5_000)
      }),
    ])
  }
  assert.equal(runtime.child.exitCode, 0, runtime.stderr())
}

function tamperCanonicalKey(key) {
  const replacement = ["A", "Q", "g", "w"].find((candidate) => candidate !== key.at(-1))
  assert.notEqual(replacement, undefined)
  return `${key.slice(0, -1)}${replacement}`
}

function isRunkuFailure(error, status, code) {
  return error instanceof RunkuError && error.status === status && error.code === code
}

async function waitForStoredValue(client, key, expected) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const result = await client.action("images.readStored", { key })
    if (result.value === expected) return
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  assert.fail(`scheduled value ${JSON.stringify(expected)} was not persisted`)
}

test("Node Actions are executable, isolated and composable in local development", async (t) => {
  prepare()
  const env = await environment()
  assert.match(env.RUNKU_KEY ?? "", /^rk_pub_v1_/u)
  let runtime = await startRuntime(env.RUNKU_URL)
  const client = new RunkuClient({
    baseUrl: env.RUNKU_URL,
    target: env.RUNKU_TARGET,
    applicationKey: env.RUNKU_KEY,
  })

  try {
    await t.test("rejects invalid application keys and direct calls to internal Functions", async () => {
      const invalidClient = new RunkuClient({
        baseUrl: env.RUNKU_URL,
        target: env.RUNKU_TARGET,
        applicationKey: tamperCanonicalKey(env.RUNKU_KEY),
      })
      await assert.rejects(
        invalidClient.action("images.checkerboard", {
          width: 1n,
          height: 1n,
          seed: "unauthorized",
        }),
        (error) => error instanceof RunkuError
          && error.status === 401
          && error.code.startsWith("APPLICATION_CREDENTIAL_"),
      )
      await assert.rejects(
        client.action("bridge.echo", "external-bypass"),
        (error) => isRunkuFailure(error, 403, "FUNCTION_INTERNAL"),
      )
      await assert.rejects(
        client.action("images.checkerboard", { width: 0n, height: 1n, seed: "invalid" }),
        (error) => error instanceof RunkuError && error.status === 400,
      )
    })

    await t.test("executes node:crypto and pngjs through Safe-to-Node routing", async () => {
      const envelope = await client.action("bridge.render", {
        width: 32n,
        height: 24n,
        seed: "runku-full-node",
      })
      const result = envelope.value
      assert.equal(result.runtimeFunction, "images.checkerboard")
      assert.equal(result.sha256, createHash("sha256").update(result.png).digest("hex"))
      const decoded = PNG.sync.read(Buffer.from(result.png))
      assert.equal(decoded.width, 32)
      assert.equal(decoded.height, 24)
      assert.deepEqual([...result.png.subarray(0, 8)], [137, 80, 78, 71, 13, 10, 26, 10])
    })

    await t.test("keeps concurrent Node Action results independent", async () => {
      const results = await Promise.all(
        Array.from({ length: 8 }, (_, index) => client.action("images.checkerboard", {
          width: BigInt(16 + index),
          height: 16n,
          seed: `concurrent-${index}`,
        })),
      )
      assert.equal(new Set(results.map(({ value }) => value.sha256)).size, results.length)
      for (const [index, { value }] of results.entries()) {
        assert.equal(PNG.sync.read(Buffer.from(value.png)).width, 16 + index)
        assert.equal(value.runtimeFunction, "images.checkerboard")
      }
    })

    await t.test("routes Node-to-Safe Action, Mutation and Query operations", async () => {
      const roundTrip = await client.action("images.roundTripToSafe", "bridge")
      assert.equal(roundTrip.value, "safe:node:bridge")
      const persisted = await client.action("images.writeAndRead", {
        key: "node-platform-op",
        value: "committed-through-safe-mutation",
      })
      assert.equal(persisted.value, "committed-through-safe-mutation")
    })

    await t.test("preserves Safe data across a local runtime restart", async () => {
      await stopRuntime(runtime)
      runtime = await startRuntime(env.RUNKU_URL)
      const persisted = await client.action("images.readStored", { key: "node-platform-op" })
      assert.equal(persisted.value, "committed-through-safe-mutation")
    })

    await t.test("executes runAfter from Node against an internal Safe Mutation", async () => {
      const input = { key: "scheduled-node-platform-op", value: "executed-after-delay" }
      const scheduled = await client.action("images.scheduleWrite", input)
      assert.match(scheduled.value, /^sch_[0-7][0-9A-HJKMNP-TV-Z]{25}$/u)
      assert.deepEqual(scheduled.metadata, { kind: "action", schedulesCreated: 1n })
      await waitForStoredValue(client, input.key, input.value)
    })
  } finally {
    await stopRuntime(runtime)
  }
})
