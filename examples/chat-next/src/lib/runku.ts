"use client"

import { RunkuClient, typedClient, type CodeTarget } from "@runku/client"
import type { RunkuFunctions } from "../../runku/_generated/api"

import { authClient } from "@/lib/auth-client"

const baseUrl = process.env.NEXT_PUBLIC_RUNKU_URL ?? "http://127.0.0.1:3210"
const target = (process.env.NEXT_PUBLIC_RUNKU_TARGET ?? "workspace:local") as CodeTarget
const applicationKey = process.env.NEXT_PUBLIC_RUNKU_KEY

if (applicationKey === undefined) {
  throw new Error("NEXT_PUBLIC_RUNKU_KEY is required by the browser Runku client")
}

let pendingBearer: Promise<string> | null = null

async function issueBearer(): Promise<string> {
  const result = await authClient.token()
  if (result.error !== null || result.data?.token === undefined) {
    throw new Error("The authenticated session did not issue a bearer token")
  }
  return result.data.token
}

async function getBearer(): Promise<string> {
  pendingBearer ??= issueBearer().finally(() => {
    pendingBearer = null
  })
  return pendingBearer
}

const client = new RunkuClient({
  baseUrl,
  target,
  applicationKey,
  getBearer,
  timeoutMs: 10_000,
  maxAttempts: 3,
  retryDelayMs: 100,
})

export const runku = typedClient<RunkuFunctions>(client)

export async function bootstrapProfile(displayName: string): Promise<void> {
  const bearer = await getBearer()
  const response = await fetch("/api/runku/profile", {
    method: "POST",
    headers: { authorization: `Bearer ${bearer}`, "content-type": "application/json" },
    body: JSON.stringify({ displayName }),
  })
  if (!response.ok) throw new Error(`Profile bootstrap failed (${response.status})`)
}
