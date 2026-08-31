import { createHash } from "node:crypto";
import { deflateSync } from "node:zlib";
import pg from "pg";

export function echo(context, input) {
  return { argument: input, function: context.invocation.function, release: process.env.RELEASE_LABEL };
}

export function encrypt(_context, input) {
  return createHash("sha256").update(input).digest("hex");
}

export function image(_context, input) {
  const compressed = deflateSync(Buffer.from(input));
  return new Uint8Array(Buffer.concat([Buffer.from("89504e470d0a1a0a", "hex"), compressed]));
}

export async function postgres(_context, connectionString) {
  const client = new pg.Client({ connectionString, connectionTimeoutMillis: 1000 });
  await client.connect();
  try {
    const result = await client.query("select 'tcp-ok'::text as value");
    return result.rows[0].value;
  } finally {
    await client.end();
  }
}

export function loop() {
  for (;;) {}
}

export function memory() {
  const value = Buffer.alloc(512 * 1024 * 1024, 7);
  return value.length;
}

export async function writeRoot() {
  try {
    await import("node:fs/promises").then((fs) => fs.writeFile("/opt/runku/forbidden", "x"));
    return "writable";
  } catch (error) {
    return error.code ?? "denied";
  }
}
