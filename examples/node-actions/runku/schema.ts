import { defineSchema, defineTable } from "@runku/server"
import { storedValue } from "./contracts.js"

export default defineSchema({
  values: defineTable(storedValue).index("by_key", ["key"]),
})
