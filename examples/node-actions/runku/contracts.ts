import { v } from "@runku/server"

export const imageRequest = v.object({
  width: v.int64({ minimum: 1, maximum: 256 }),
  height: v.int64({ minimum: 1, maximum: 256 }),
  seed: v.string({ minLength: 1, maxLength: 128 }),
})

export const imageResult = v.object({
  png: v.bytes({ minBytes: 67, maxBytes: 1_048_576 }),
  sha256: v.string({ minLength: 64, maxLength: 64 }),
  runtimeFunction: v.string({ minLength: 1, maxLength: 128 }),
})

export const storedValue = v.object({
  key: v.string({ minLength: 1, maxLength: 64 }),
  value: v.string({ minLength: 1, maxLength: 256 }),
})

export const storedKey = v.pick(storedValue, ["key"])
