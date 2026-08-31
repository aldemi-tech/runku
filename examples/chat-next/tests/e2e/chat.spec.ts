import { expect, test, type BrowserContext, type Page } from "@playwright/test"
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process"
import { access } from "node:fs/promises"
import path from "node:path"

let runkuProcess: ChildProcessWithoutNullStreams | null = null
let runkuLogs = ""

const runkuBinary = process.env.RUNKU_BIN ?? path.resolve("../../target/release/runku")

async function waitForRunku(): Promise<void> {
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    if (runkuProcess?.exitCode !== null) {
      throw new Error(`runku dev exited before becoming ready:\n${runkuLogs.slice(-4_000)}`)
    }
    try {
      const response = await fetch("http://127.0.0.1:3210/readyz", {
        signal: AbortSignal.timeout(1_000),
      })
      if (response.ok) return
    } catch {
      // The listener is expected to be unavailable while the local release is assembled.
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`runku dev did not become ready:\n${runkuLogs.slice(-4_000)}`)
}

async function startRunku(): Promise<void> {
  await access(runkuBinary)
  runkuLogs = ""
  runkuProcess = spawn(
    runkuBinary,
    [
      "dev",
      "--origin", "http://127.0.0.1:3000",
      "--auth-config", "runku.auth.json",
    ],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] },
  )
  runkuProcess.stdout.on("data", (chunk: Buffer) => { runkuLogs += chunk.toString() })
  runkuProcess.stderr.on("data", (chunk: Buffer) => { runkuLogs += chunk.toString() })
  await waitForRunku()
}

async function stopRunku(): Promise<void> {
  const child = runkuProcess
  runkuProcess = null
  if (child === null || child.exitCode !== null) return
  await new Promise<void>((resolve) => {
    const force = setTimeout(() => child.kill("SIGKILL"), 5_000)
    child.once("exit", () => {
      clearTimeout(force)
      resolve()
    })
    child.kill("SIGINT")
  })
}

async function register(context: BrowserContext, name: string, email: string): Promise<Page> {
  await context.addInitScript(() => {
    const originalFetch = globalThis.fetch
    const calls: string[] = []
    Object.defineProperty(globalThis, "__runkuFetchCalls", { value: calls })
    globalThis.fetch = async (...argumentsValue) => {
      const input = argumentsValue[0]
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url
      try {
        const response = await originalFetch(...argumentsValue)
        if (url.includes(":3210")) calls.push(`${url} -> ${response.status}`)
        return response
      } catch (cause) {
        if (url.includes(":3210")) calls.push(`${url} -> ${String(cause)}`)
        throw cause
      }
    }
  })
  const page = await context.newPage()
  const failedRequests: string[] = []
  page.on("requestfailed", (request) => {
    failedRequests.push(`${request.method()} ${request.url()}: ${request.failure()?.errorText ?? "unknown"}`)
  })
  await page.goto("/")
  await page.getByTestId("auth-name").fill(name)
  await page.getByTestId("auth-email").fill(email)
  await page.getByTestId("auth-password").fill("correct-horse-battery-staple")
  await page.getByTestId("auth-submit").click()
  try {
    await expect(page.getByTestId("create-room")).toBeEnabled({ timeout: 20_000 })
  } catch (cause) {
    const alerts = await page.getByTestId("app-error").allTextContents()
    const diagnostics = await page.evaluate(async () => {
      const calls = (globalThis as typeof globalThis & { __runkuFetchCalls?: string[] }).__runkuFetchCalls
      try {
        const readiness = await fetch("http://127.0.0.1:3210/readyz")
        return { calls, readiness: `${readiness.status}:${await readiness.text()}` }
      } catch (error) {
        return { calls, readiness: String(error) }
      }
    })
    throw new Error(
      `profile bootstrap failed; alerts=${JSON.stringify(alerts)}; `
      + `runkuExit=${String(runkuProcess?.exitCode)}; requests=${JSON.stringify(failedRequests)}; `
      + `browser=${JSON.stringify(diagnostics)}; runkuLogs=${runkuLogs.slice(-4_000)}`,
      { cause },
    )
  }
  await expect(page.getByTestId("app-error")).toHaveCount(0)
  return page
}

test.beforeAll(async () => {
  await startRunku()
})

test.afterAll(async () => {
  await stopRunku()
})

test("unauthenticated HTTP and Realtime calls cannot read or mutate chat state", async ({ page }) => {
  await page.goto("/")
  const outcomes = await page.evaluate(async () => {
    async function invoke(path: string, body: object, applicationKey?: string) {
      const headers: Record<string, string> = {
        accept: "application/json",
        "content-type": "application/json",
      }
      if (applicationKey !== undefined) headers["x-runku-key"] = applicationKey
      const response = await fetch(`http://127.0.0.1:3210${path}`, {
        method: "POST",
        credentials: "omit",
        headers,
        body: JSON.stringify(body),
      })
      const payload = await response.json() as {
        error?: { code?: string }
      }
      return {
        status: response.status,
        code: payload.error?.code ?? null,
        cacheControl: response.headers.get("cache-control"),
      }
    }

    function exchange(messages: readonly object[]): Promise<readonly string[]> {
      return new Promise((resolve, reject) => {
        const socket = new WebSocket("ws://127.0.0.1:3210/v1/realtime", ["runku.realtime.v1"])
        const codes: string[] = []
        const timeout = window.setTimeout(() => {
          socket.close()
          reject(new Error("Realtime security probe timed out"))
        }, 3_000)
        socket.onopen = () => socket.send(JSON.stringify(messages[0]))
        socket.onmessage = (event) => {
          const message = JSON.parse(String(event.data)) as { type?: string; code?: string }
          codes.push(message.code ?? message.type ?? "unknown")
          if (codes.length < messages.length) {
            socket.send(JSON.stringify(messages[codes.length]))
          } else {
            window.clearTimeout(timeout)
            socket.close()
            resolve(codes)
          }
        }
        socket.onerror = () => {
          window.clearTimeout(timeout)
          reject(new Error("Realtime security probe socket failed"))
        }
      })
    }

    const query = await invoke("/v1/query", {
      version: 1,
      target: "workspace:local",
      function: "rooms.list",
      arguments: { type: "null" },
    })
    const mutation = await invoke("/v1/mutation", {
      version: 1,
      target: "workspace:local",
      function: "rooms.create",
      arguments: {
        type: "object",
        value: [{ key: "name", value: { type: "string", value: "INYECCION-SIN-AUTH" } }],
      },
      operationId: "opn_7ZZZZZZZZZZZZZZZZZZZZZZZZZ",
    })
    const invalidApplication = await invoke("/v1/query", {
      version: 1,
      target: "workspace:local",
      function: "rooms.list",
      arguments: { type: "null" },
    }, "rk_pub_v1_7ZZZZZZZZZZZZZZZZZZZZZZZZZ_AAAAAAAAAAAAAAAAAAAAAA")
    const subscribeBeforeAuth = await exchange([{
      type: "subscribe",
      version: 1,
      requestId: "req_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      target: "workspace:local",
      function: "rooms.list",
      arguments: { type: "null" },
    }])
    const anonymousSubscribe = await exchange([
      {
        type: "authenticate",
        version: 1,
        requestId: "req_01ARZ3NDEKTSV4RRFFQ69G5FAW",
        applicationKey: null,
        bearer: null,
      },
      {
        type: "subscribe",
        version: 1,
        requestId: "req_01ARZ3NDEKTSV4RRFFQ69G5FAX",
        target: "workspace:local",
        function: "rooms.list",
        arguments: { type: "null" },
      },
    ])
    const mutationOverRealtime = await exchange([
      {
        type: "authenticate",
        version: 1,
        requestId: "req_01ARZ3NDEKTSV4RRFFQ69G5FAY",
        applicationKey: null,
        bearer: null,
      },
      {
        type: "mutation",
        version: 1,
        requestId: "req_01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        target: "workspace:local",
        function: "rooms.create",
        arguments: { type: "null" },
      },
    ])
    return { query, mutation, invalidApplication, subscribeBeforeAuth, anonymousSubscribe, mutationOverRealtime }
  })

  expect(outcomes.query).toEqual({
    status: 401,
    code: "APPLICATION_CREDENTIAL_REQUIRED",
    cacheControl: "no-store",
  })
  expect(outcomes.mutation).toEqual({
    status: 401,
    code: "APPLICATION_CREDENTIAL_REQUIRED",
    cacheControl: "no-store",
  })
  expect(outcomes.invalidApplication).toEqual({
    status: 401,
    code: "APPLICATION_CREDENTIAL_INVALID",
    cacheControl: "no-store",
  })
  expect(outcomes.subscribeBeforeAuth).toEqual(["REALTIME_AUTH_REQUIRED"])
  expect(outcomes.anonymousSubscribe).toEqual(["authentication_accepted", "APPLICATION_CREDENTIAL_REQUIRED"])
  expect(outcomes.mutationOverRealtime).toEqual(["authentication_accepted", "PROTOCOL_REQUEST_INVALID"])
})

test("two identities exchange realtime messages and recover after a Runku restart", async ({ browser }) => {
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`
  const roomName = `MVP Room ${suffix}`
  const aliceContext = await browser.newContext()
  const bobContext = await browser.newContext()
  const applicationKeys: string[] = []
  const bearerTokens: string[] = []
  for (const context of [aliceContext, bobContext]) {
    context.on("request", (request) => {
      if (request.url().startsWith("http://127.0.0.1:3210/")) {
        void request.allHeaders().then((headers) => {
          const key = headers["x-runku-key"]
          if (key !== undefined) applicationKeys.push(key)
          const authorization = headers.authorization
          if (authorization?.startsWith("Bearer ")) bearerTokens.push(authorization.slice(7))
        })
      }
    })
  }

  try {
    const alice = await register(aliceContext, "Alice", `alice-${suffix}@example.test`)
    await alice.reload()
    await expect(alice.getByTestId("create-room")).toBeEnabled({ timeout: 20_000 })

    await alice.getByTestId("room-name").fill(roomName)
    await alice.getByTestId("create-room").click()
    const roomId = await alice.getByTestId("active-room-id").textContent()
    expect(roomId).toMatch(/^doc_[0-9A-HJKMNP-TV-Z]{26}$/)
    await expect.poll(() => applicationKeys.length).toBeGreaterThan(0)
    await expect.poll(() => bearerTokens.length).toBeGreaterThan(0)
    const directProfile = await alice.evaluate(async ({ key, bearer }) => {
      const response = await fetch("http://127.0.0.1:3210/v1/mutation", {
        method: "POST",
        headers: {
          authorization: `Bearer ${bearer}`,
          "content-type": "application/json",
          "x-runku-key": key,
        },
        body: JSON.stringify({
          version: 1,
          target: "workspace:local",
          function: "profiles.upsert",
          arguments: {
            type: "object",
            value: [{ key: "displayName", value: { type: "string", value: "Alice" } }],
          },
          operationId: "opn_7ZZZZZZZZZZZZZZZZZZZZZZZZY",
        }),
      })
      const payload = await response.json() as { error?: { code?: string } }
      return { status: response.status, code: payload.error?.code ?? null }
    }, { key: applicationKeys[0], bearer: bearerTokens[0] })
    expect(directProfile).toEqual({ status: 500, code: "RUNTIME_JAVASCRIPT_ERROR" })

    const bob = await register(bobContext, "Bob", `bob-${suffix}@example.test`)
    const listedRoom = bob.getByTestId("room-directory-item").filter({ hasText: roomName })
    await expect(listedRoom).toBeVisible()
    await listedRoom.getByRole("button", { name: `Join ${roomName}`, exact: true }).click()
    await expect(bob.getByTestId("active-room-name")).toHaveText(roomName)
    await expect(listedRoom).toContainText("2 participants")

    await alice.getByTestId("message-body").fill("Hello from Alice")
    await alice.getByTestId("send-message").click()
    await expect(bob.getByText("Hello from Alice")).toBeVisible()

    await bob.getByTestId("message-body").fill("Hello from Bob")
    await bob.getByTestId("send-message").click()
    await expect(alice.getByText("Hello from Bob")).toBeVisible()

    await stopRunku()
    await startRunku()

    await alice.getByTestId("message-body").fill("Still here after the restart")
    await alice.getByTestId("send-message").click()
    await expect(bob.getByText("Still here after the restart")).toBeVisible({ timeout: 20_000 })
    await expect(alice.getByText("Hello from Alice")).toBeVisible()
    await expect(alice.getByText("Hello from Bob")).toBeVisible()
    await expect(alice.getByTestId("app-error")).toHaveCount(0)
    await expect(bob.getByTestId("app-error")).toHaveCount(0)
    expect(applicationKeys.every((key) => key.startsWith("rk_pub_v1_"))).toBe(true)
    expect(applicationKeys.some((key) => key.startsWith("rk_sec_v1_"))).toBe(false)
    expect(await alice.locator("html").textContent()).not.toContain("rk_sec_v1_")

    await bob.getByRole("button", { name: "Sign out" }).click()
    await expect(bob.getByTestId("auth-submit")).toBeVisible()
  } finally {
    await aliceContext.close()
    await bobContext.close()
  }
})
