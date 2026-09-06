import { defineSchema, defineTable } from "@runku/server"
import { task } from "./model"

export default defineSchema({
  tasks: defineTable(task).index("by_title", ["title"]),
})
