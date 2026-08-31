import { v } from "@runku/server"

export const imageRequest = v.object({
  width: v.int64({ minimum: 1, maximum: 256 }),
  height: v.int64({ minimum: 1, maximum: 256 }),
  seed: v.string({ minBytes: 1, maxBytes: 128 }),
})

export const imageResult = v.object({
  png: v.bytes({ minBytes: 67, maxBytes: 1_048_576 }),
  sha256: v.string({ minBytes: 64, maxBytes: 64 }),
  runtimeFunction: v.string({ minBytes: 1, maxBytes: 128 }),
})

export const storedValue = v.object({
  key: v.string({ minBytes: 1, maxBytes: 64 }),
  value: v.string({ minBytes: 1, maxBytes: 256 }),
})

export const storedKey = v.pick(storedValue, ["key"])
