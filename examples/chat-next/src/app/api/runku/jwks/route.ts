import { NextResponse } from "next/server"

import { auth } from "@/lib/auth"

export async function GET(): Promise<NextResponse> {
  const jwks = await auth.api.getJwks()
  return NextResponse.json(
    {
      keys: jwks.keys.map((key) => ({ ...key, use: "sig" })),
    },
    {
      headers: {
        "Cache-Control": "public, max-age=30",
      },
    },
  )
}
