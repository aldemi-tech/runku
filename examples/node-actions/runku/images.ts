"use runku node"

import { createHash } from "node:crypto"
import { action, v } from "@runku/server"
import { PNG } from "pngjs"
import { imageRequest, imageResult, storedKey, storedValue } from "./contracts.js"

export const checkerboard = action({
  auth: "none",
  visibility: "public",
  capabilities: [],
  args: imageRequest,
  returns: imageResult,
  handler(ctx, input) {
    const width = Number(input.width)
    const height = Number(input.height)
    const seed = createHash("sha256").update(input.seed).digest()
    const image = new PNG({ width, height, colorType: 6 })

    for (let y = 0; y < height; y += 1) {
      for (let x = 0; x < width; x += 1) {
        const offset = (width * y + x) * 4
        const shade = seed[(x + y) % seed.length] ?? 0
        const alternate = (Math.floor(x / 8) + Math.floor(y / 8)) % 2 === 0
        image.data[offset] = alternate ? shade : 255 - shade
        image.data[offset + 1] = seed[(x * 3 + y) % seed.length] ?? 0
        image.data[offset + 2] = alternate ? 255 - shade : shade
        image.data[offset + 3] = 255
      }
    }

    const png = new Uint8Array(PNG.sync.write(image))
    return {
      png,
      sha256: createHash("sha256").update(png).digest("hex"),
      runtimeFunction: ctx.invocation.functionName,
    }
  },
})

export const roundTripToSafe = action({
  auth: "none",
  visibility: "public",
  capabilities: ["function:action"],
  args: v.string(),
  returns: v.string(),
  async handler(ctx, input) {
    const result = await ctx.runAction("bridge.echo", `node:${input}`)
    if (typeof result !== "string") throw new TypeError("bridge.echo returned a non-string value")
    return result
  },
})

export const writeAndRead = action({
  auth: "none",
  visibility: "public",
  capabilities: ["function:mutation", "function:query"],
  args: v.object({ key: v.string(), value: v.string() }),
  returns: v.string(),
  async handler(ctx, input) {
    await ctx.runMutation("data.put", input)
    const result = await ctx.runQuery("data.get", { key: input.key })
    if (typeof result !== "string") throw new TypeError("data.get did not observe the committed value")
    return result
  },
})

export const readStored = action({
  auth: "none",
  visibility: "public",
  capabilities: ["function:query"],
  args: storedKey,
  returns: v.union(v.null(), v.string()),
  async handler(ctx, input) {
    const result = await ctx.runQuery("data.get", input)
    if (result !== null && typeof result !== "string") {
      throw new TypeError("data.get returned an invalid value")
    }
    return result
  },
})

export const scheduleWrite = action({
  auth: "none",
  visibility: "public",
  capabilities: ["scheduler:create"],
  args: storedValue,
  returns: v.string(),
  handler(ctx, input) {
    return ctx.scheduler.runAfter(50n, "data.put", input)
  },
})
