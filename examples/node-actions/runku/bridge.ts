import { action, type Infer, v } from "@runku/server"
import { imageRequest, imageResult } from "./contracts.js"

export const render = action({
  auth: "none",
  visibility: "public",
  capabilities: ["function:action"],
  args: imageRequest,
  returns: imageResult,
  handler(ctx, input) {
    return ctx.runAction("images.checkerboard", input) as Promise<Infer<typeof imageResult>>
  },
})

export const echo = action({
  auth: "none",
  visibility: "internal",
  capabilities: [],
  args: v.string(),
  returns: v.string(),
  handler(_ctx, input) {
    return `safe:${input}`
  },
})
