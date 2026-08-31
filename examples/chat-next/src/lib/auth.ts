import { mkdirSync } from "node:fs"
import path from "node:path"
import Database from "better-sqlite3"
import { betterAuth } from "better-auth"
import { jwt } from "better-auth/plugins"

const secret = process.env.BETTER_AUTH_SECRET
if (secret === undefined || secret.length < 32) {
  throw new Error("BETTER_AUTH_SECRET must contain at least 32 characters")
}

const databasePath = process.env.BETTER_AUTH_DATABASE_PATH ?? ".data/auth.sqlite3"
mkdirSync(path.dirname(databasePath), { recursive: true, mode: 0o700 })

export const auth = betterAuth({
  appName: "Runku Chat",
  baseURL: process.env.BETTER_AUTH_URL ?? "http://127.0.0.1:3000",
  secret,
  database: new Database(databasePath),
  trustedOrigins: ["http://127.0.0.1:3000"],
  emailAndPassword: {
    enabled: true,
    minPasswordLength: 10,
    maxPasswordLength: 128,
  },
  rateLimit: {
    enabled: true,
    window: 60,
    max: 100,
  },
  advanced: {
    database: {
      joins: true,
    },
  },
  plugins: [
    jwt({
      jwks: {
        keyPairConfig: { alg: "EdDSA", crv: "Ed25519" },
        rotationInterval: 60 * 60 * 24 * 30,
        gracePeriod: 60 * 60 * 24 * 30,
      },
      jwt: {
        issuer: "https://chat.local.runku",
        audience: "runku-chat-local",
        expirationTime: "15m",
        definePayload: () => ({ token_use: "user" }),
      },
    }),
  ],
})
