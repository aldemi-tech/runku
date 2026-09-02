import { createRequire } from "node:module"

const require = createRequire(new URL("../examples/chat-next/package.json", import.meta.url))
const { chromium } = require("@playwright/test")

const authorizationUrl = process.env.RUNKU_TEST_AUTHORIZATION_URL
if (!authorizationUrl) throw new Error("RUNKU_TEST_AUTHORIZATION_URL is required")

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage()
try {
  await page.goto(authorizationUrl, { waitUntil: "domcontentloaded" })
  await page.getByLabel("Username or email").fill("alice")
  await page.getByLabel("Password", { exact: true }).fill("deliberately-wrong")
  await page.locator("form").evaluate(form => { form.action = form.action.replace("https://identity.runku.test", "http://127.0.0.1:18080") })
  await page.getByRole("button", { name: "Sign In" }).click()
  await page.getByLabel("Password", { exact: true }).waitFor()
  if (page.url().startsWith("http://127.0.0.1:") && page.url().includes("/callback")) {
    throw new Error("the IdP accepted an invalid password")
  }
  await page.getByLabel("Username or email").fill("alice")
  await page.getByLabel("Password", { exact: true }).fill("runku-alice-test-password")
  await page.locator("form").evaluate(form => { form.action = form.action.replace("https://identity.runku.test", "http://127.0.0.1:18080") })
  await page.getByRole("button", { name: "Sign In" }).click()
  await page.getByText("Runku login complete.").waitFor({ timeout: 30_000 })
} finally {
  await browser.close()
}
