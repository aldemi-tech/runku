"use client"

import { createAuthClient } from "better-auth/react"
import { jwtClient } from "better-auth/client/plugins"

export const authClient = createAuthClient({
  baseURL: "http://127.0.0.1:3000",
  plugins: [jwtClient()],
})
