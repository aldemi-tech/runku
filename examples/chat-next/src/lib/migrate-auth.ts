import { existsSync } from "node:fs"
import { loadEnvFile } from "node:process"

import { getMigrations } from "better-auth/db/migration"

for (const envFile of [".env.local", ".env"]) {
  if (existsSync(envFile)) {
    loadEnvFile(envFile)
  }
}

const { auth } = await import("./auth")
const { runMigrations } = await getMigrations(auth.options)
await runMigrations()
