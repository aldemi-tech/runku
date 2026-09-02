import { query, v } from "@runku/server"

export const current = query({
  auth: "none",
  visibility: "public",
  capabilities: [],
  args: v.null(),
  returns: v.string(),
  handler() {
    console.info("platform lifecycle release v1")
    return "v1"
  },
})
