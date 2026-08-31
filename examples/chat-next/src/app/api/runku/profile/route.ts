import { NextResponse } from "next/server"

import { runkuServer } from "@/lib/runku-server"

export const runtime = "nodejs"

function bearer(request: Request): string | null {
  const authorization = request.headers.get("authorization")
  if (authorization === null || !authorization.startsWith("Bearer ")) return null
  const token = authorization.slice(7)
  return token.length >= 16 && token.length <= 16 * 1024 ? token : null
}

export async function POST(request: Request) {
  const token = bearer(request)
  if (token === null) {
    return NextResponse.json({ error: "AUTH_REQUIRED" }, { status: 401 })
  }
  if (request.headers.get("content-type")?.split(";", 1)[0] !== "application/json") {
    return NextResponse.json({ error: "CONTENT_TYPE_INVALID" }, { status: 415 })
  }
  let body: unknown
  try {
    body = await request.json()
  } catch {
    return NextResponse.json({ error: "REQUEST_INVALID" }, { status: 400 })
  }
  if (body === null || typeof body !== "object" || Array.isArray(body)
    || Object.keys(body).length !== 1 || !("displayName" in body)
    || typeof body.displayName !== "string") {
    return NextResponse.json({ error: "REQUEST_INVALID" }, { status: 400 })
  }
  const displayName = body.displayName.trim()
  const bytes = new TextEncoder().encode(displayName).byteLength
  if (bytes < 1 || bytes > 48) {
    return NextResponse.json({ error: "DISPLAY_NAME_INVALID" }, { status: 400 })
  }
  try {
    await runkuServer(token).mutation("profiles.upsert", { displayName })
    return new NextResponse(null, { status: 204 })
  } catch (error) {
    const status = error !== null && typeof error === "object" && "status" in error
      && typeof error.status === "number" ? error.status : 502
    return NextResponse.json({ error: "RUNKU_PROFILE_BOOTSTRAP_FAILED" }, { status })
  }
}
