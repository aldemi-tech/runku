import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"

const readJson = async (name) => JSON.parse(await readFile(new URL(`../${name}`, import.meta.url), "utf8"))

test("the local identity descriptor is bounded and user-scoped", async () => {
  const config = await readJson("runku.auth.json")
  assert.equal(config.version, 1)
  assert.equal(config.allowedOrigin, "http://127.0.0.1:3000")
  assert.equal(config.issuer, "https://chat.local.runku")
  assert.deepEqual(config.audiences, ["runku-chat-local"])
  assert.equal(config.discriminatorClaim, "token_use")
  assert.equal(config.discriminatorValue, "user")
  assert.deepEqual(config.algorithms, ["EdDSA"])
  assert.equal(config.maxTokenTtlSeconds, 900)
})

test("the declarative source exposes seven authenticated functions", async () => {
  const modules = await Promise.all(
    ["profiles.ts", "rooms.ts", "messages.ts"].map((name) =>
      readFile(new URL(`../runku/${name}`, import.meta.url), "utf8"),
    ),
  )
  const definitions = modules.flatMap((source) =>
    [...source.matchAll(/export const ([a-zA-Z][a-zA-Z0-9_]*) = (query|mutation|action)\(\{/g)],
  )
  assert.equal(definitions.length, 7)
  for (const source of modules) {
    const count = [...source.matchAll(/export const [a-zA-Z][a-zA-Z0-9_]* = (?:query|mutation|action)\(\{/g)].length
    assert.equal((source.match(/auth: "user"/g) ?? []).length, count)
    assert.equal((source.match(/visibility: "public"/g) ?? []).length, count)
    assert.equal((source.match(/"auth:read"/g) ?? []).length, count)
    assert.equal((source.match(/returns:/g) ?? []).length, count)
  }

  const schema = await readFile(new URL("../runku/schema.ts", import.meta.url), "utf8")
  const model = await readFile(new URL("../runku/model.ts", import.meta.url), "utf8")
  assert.match(schema, /profiles: defineTable\(profile\)\.index\("by_principal"/)
  assert.match(schema, /rooms: defineTable\(room\)\s+\.index\("by_owner"/)
  assert.match(schema, /\.index\("by_name", \["name"\]\)/)
  assert.match(model, /v\.documentId\("rooms"\)/)
  assert.match(model, /v\.pick\(profile, \["displayName"\]\)/)
  assert.match(model, /v\.pick\(room, \["name"\]\)/)
  for (const source of modules) assert.doesNotMatch(source, /tbl_[0-9A-Z]+/)
  for (const source of modules) assert.doesNotMatch(source, /as unknown as|type (?:Profile|Room|Member|Message)\b/)

  const send = modules[2]
  const join = modules[1]
  assert.match(join, /ctx\.db\.documentId\(schema\.tables\.rooms, ctx\.invocation\.invocationId\)/)
  assert.match(send, /slice\(-200\)/)
  assert.match(join, /members\.length >= 100/)
  assert.match(join, /ctx\.db\.scan\(schema\.indexes\.rooms\.by_name, \{ limit: 100 \}\)/)

  const client = await readFile(new URL("../src/lib/runku.ts", import.meta.url), "utf8")
  const page = await readFile(new URL("../src/app/page.tsx", import.meta.url), "utf8")
  assert.match(client, /typedClient<RunkuFunctions>/)
  assert.match(page, /RunkuFunctionResult<"rooms\.get">/)
  assert.match(page, /RunkuFunctionResult<"rooms\.list">/)
  assert.match(page, /realtime\.subscribe\("rooms\.list", null,/)
  assert.match(page, /data-testid="room-directory"/)
  assert.match(page, /documentId\("rooms",/)
  assert.match(page, /The submitted data has an invalid format\./)
  assert.match(page, /SDK_REALTIME_DISCONNECTED/)
  assert.doesNotMatch(page, /as unknown as|new RunkuId|ulid/)
})

test("browser extension attributes do not create a false hydration issue", async () => {
  const layout = await readFile(new URL("../src/app/layout.tsx", import.meta.url), "utf8")
  assert.match(layout, /<body suppressHydrationWarning>/)
})

test("development starts without an application-specific setup wrapper", async () => {
  const packageJson = await readJson("package.json")
  assert.equal(packageJson.packageManager, "pnpm@10.18.1")
  assert.equal(packageJson.engines.node, ">=20.18.1")
  assert.equal(packageJson.scripts.predev, "runku dev --prepare && pnpm auth:migrate")
  assert.equal(packageJson.scripts["auth:migrate"], "tsx src/lib/migrate-auth.ts")
  assert.equal(packageJson.devDependencies.auth, undefined)
  assert.match(packageJson.scripts.dev, /runku dev /)
  assert.doesNotMatch(packageJson.scripts.dev, /--watch|runku init/)
  assert.equal(packageJson.scripts.setup, undefined)
  assert.equal(packageJson.scripts["runku:init"], undefined)
  await assert.rejects(readFile(new URL("../scripts/init-runku.mjs", import.meta.url), "utf8"))
  await assert.rejects(readFile(new URL("../scripts/migrate-auth.ts", import.meta.url), "utf8"))
  const migration = await readFile(new URL("../src/lib/migrate-auth.ts", import.meta.url), "utf8")
  assert.match(migration, /loadEnvFile/)
  assert.match(migration, /getMigrations/)
  await assert.rejects(readFile(new URL("../src/lib/provision-runku-keys.ts", import.meta.url), "utf8"))
  await readFile(new URL("../../../pnpm-lock.yaml", import.meta.url), "utf8")
})
