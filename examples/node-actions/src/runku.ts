import { RunkuClient, typedClient, type CodeTarget } from "@runku/client"
import type { RunkuFunctions } from "../runku/_generated/api.js"

export function createRunkuClient(environment: NodeJS.ProcessEnv = process.env) {
  const { RUNKU_KEY: applicationKey, RUNKU_TARGET: target, RUNKU_URL: baseUrl } = environment
  if (baseUrl === undefined || target === undefined || applicationKey === undefined) {
    throw new Error("RUNKU_URL, RUNKU_TARGET and RUNKU_KEY are required")
  }
  return typedClient<RunkuFunctions>(
    new RunkuClient({
      baseUrl,
      target: target as CodeTarget,
      applicationKey,
    }),
  )
}

export async function createExampleImage(): Promise<Uint8Array> {
  const result = await createRunkuClient().action("images.checkerboard", {
    width: 64n,
    height: 64n,
    seed: "documented-example",
  })
  return result.value.png
}
