import { pathToFileURL } from "node:url";

const brand = Symbol("runku.node.value");
const timestampBrand = "timestamp";
const typedIdBrand = "typed_id";

function branded(kind, value) {
  return Object.freeze({ [brand]: kind, value, toString() { return String(value); } });
}

globalThis.Runku = Object.freeze({
  timestamp(value) { return branded(timestampBrand, BigInt(value)); },
  id(value) { return branded(typedIdBrand, String(value)); },
});

function decode(wire, depth = 0) {
  if (depth > 64 || wire === null || typeof wire !== "object") throw new TypeError("invalid wire value");
  switch (wire.type) {
    case "null": return null;
    case "boolean": return wire.value;
    case "int64": return BigInt(wire.value);
    case "float64": {
      const bytes = Buffer.allocUnsafe(8);
      bytes.writeBigUInt64BE(BigInt(`0x${wire.value}`));
      return bytes.readDoubleBE();
    }
    case "string": return wire.value;
    case "bytes": return new Uint8Array(Buffer.from(wire.value, "base64url"));
    case "timestamp": return branded(timestampBrand, BigInt(wire.value));
    case "typed_id": return branded(typedIdBrand, wire.value);
    case "array": return wire.value.map((value) => decode(value, depth + 1));
    case "object": return Object.fromEntries(wire.value.map((entry) => [entry.key, decode(entry.value, depth + 1)]));
    default: throw new TypeError("unknown wire value");
  }
}

function encode(value, depth = 0, seen = new WeakSet()) {
  if (depth > 64) throw new TypeError("result nesting limit");
  if (value === null) return { type: "null" };
  if (typeof value === "boolean") return { type: "boolean", value };
  if (typeof value === "bigint") return { type: "int64", value: value.toString() };
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("non-finite result");
    const bytes = Buffer.allocUnsafe(8);
    bytes.writeDoubleBE(value);
    return { type: "float64", value: bytes.readBigUInt64BE().toString(16).padStart(16, "0") };
  }
  if (typeof value === "string") return { type: "string", value };
  if (typeof value !== "object") throw new TypeError("unsupported result");
  if (value[brand] === timestampBrand) return { type: "timestamp", value: value.value.toString() };
  if (value[brand] === typedIdBrand) return { type: "typed_id", value: String(value.value) };
  if (value instanceof Uint8Array) return { type: "bytes", value: Buffer.from(value).toString("base64url") };
  if (seen.has(value)) throw new TypeError("cyclic result");
  seen.add(value);
  let encoded;
  if (Array.isArray(value)) {
    encoded = { type: "array", value: value.map((item) => encode(item, depth + 1, seen)) };
  } else {
    const keys = Object.keys(value).sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right)));
    encoded = { type: "object", value: keys.map((key) => ({ key, value: encode(value[key], depth + 1, seen) })) };
  }
  seen.delete(value);
  return encoded;
}

const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);

try {
  const request = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  if (request.protocolVersion !== 1) throw new Error("unsupported protocol");
  const exportName = request.function.split(".").at(-1);
  const implementation = await import(`${pathToFileURL("/opt/runku/handler.mjs").href}?invocation=${encodeURIComponent(request.invocationId)}`);
  const handler = implementation[exportName];
  if (typeof handler !== "function") throw new TypeError("handler export missing");
  const context = Object.freeze({ invocation: Object.freeze({
    releaseId: request.releaseId,
    invocationId: request.invocationId,
    function: request.function,
  }) });
  const value = await handler(context, decode(request.arguments));
  process.stdout.write(JSON.stringify({ protocolVersion: 1, ok: true, value: encode(value) }));
} catch {
  process.stdout.write(JSON.stringify({ protocolVersion: 1, ok: false, error: { code: "FUNCTION_FAILED" } }));
}
