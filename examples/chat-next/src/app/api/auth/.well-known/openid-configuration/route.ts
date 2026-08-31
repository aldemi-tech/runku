import { NextResponse } from "next/server"

const metadata = {
  issuer: "https://chat.local.runku",
  jwks_uri: "http://127.0.0.1:3000/api/runku/jwks",
  id_token_signing_alg_values_supported: ["EdDSA"],
  response_types_supported: ["id_token"],
  subject_types_supported: ["public"],
} as const

export function GET(): NextResponse {
  return NextResponse.json(metadata, {
    headers: {
      "Cache-Control": "public, max-age=30",
    },
  })
}
