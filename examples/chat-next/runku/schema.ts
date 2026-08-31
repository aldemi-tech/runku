import { defineSchema, defineTable } from "@runku/server"
import { profile, room } from "./model"

export default defineSchema({
  profiles: defineTable(profile).index("by_principal", ["principalId"]),
  rooms: defineTable(room)
    .index("by_owner", ["ownerId"])
    .index("by_name", ["name"]),
})
