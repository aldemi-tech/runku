import { defineConfig, devices } from "@playwright/test"

const authSecret = "runku-chat-e2e-secret-with-at-least-32-characters"

export default defineConfig({
  testDir: "./tests/e2e",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: [["list"]],
  use: {
    baseURL: "http://127.0.0.1:3000",
    ...devices["Desktop Chrome"],
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "pnpm dev:next",
    url: "http://127.0.0.1:3000",
    reuseExistingServer: false,
    timeout: 120_000,
    env: {
      BETTER_AUTH_SECRET: authSecret,
      BETTER_AUTH_DATABASE_PATH: ".data/auth.e2e.sqlite3",
      BETTER_AUTH_URL: "http://127.0.0.1:3000",
      NEXT_PUBLIC_RUNKU_URL: "http://127.0.0.1:3210",
      NEXT_PUBLIC_RUNKU_TARGET: "workspace:local",
    },
  },
})
