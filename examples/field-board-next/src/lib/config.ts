import type { CodeTarget } from "@runku/client";

export const baseUrl = process.env.NEXT_PUBLIC_RUNKU_URL ?? "http://127.0.0.1:5173";
export const target = (process.env.NEXT_PUBLIC_RUNKU_TARGET ?? process.env.RUNKU_TARGET ?? "environment:default") as CodeTarget;

export function publishableKey(): string {
  const value = process.env.NEXT_PUBLIC_RUNKU_KEY;
  if (value === undefined) throw new Error("NEXT_PUBLIC_RUNKU_KEY is required");
  return value;
}
