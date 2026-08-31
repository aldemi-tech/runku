import "server-only"

import { RunkuClient, typedClient, type CodeTarget } from "@runku/client"
import type { RunkuFunctions } from "../../runku/_generated/api"

const baseUrl = process.env.RUNKU_URL ?? process.env.NEXT_PUBLIC_RUNKU_URL ?? "http://127.0.0.1:3210"
const target = (process.env.RUNKU_TARGET ?? process.env.NEXT_PUBLIC_RUNKU_TARGET ?? "workspace:local") as CodeTarget
function requiredApplicationKey(): string {
  const key = process.env.RUNKU_SECRET_KEY
  if (key === undefined) {
    throw new Error("RUNKU_SECRET_KEY is required by the trusted Runku server client")
  }
  return key
}

const applicationKey = requiredApplicationKey()

export function runkuServer(bearer: string) {
  return typedClient<RunkuFunctions>(new RunkuClient({
    baseUrl,
    target,
    applicationKey,
    getBearer: () => bearer,
    timeoutMs: 10_000,
    maxAttempts: 3,
    retryDelayMs: 100,
  }))
}
