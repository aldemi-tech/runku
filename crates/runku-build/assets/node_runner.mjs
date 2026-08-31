import { watch } from "node:fs";
import { mkdir, readFile, readdir, rename, rm, unlink, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { pathToFileURL } from "node:url";

const readResourceUsage = process.resourceUsage.bind(process);
const readMemoryUsage = process.memoryUsage.bind(process);

function performanceSnapshot(start) {
  if (!start) return undefined;
  const end = readResourceUsage();
  return {
    userCpuMicros: Math.max(0, end.userCPUTime - start.userCPUTime),
    systemCpuMicros: Math.max(0, end.systemCPUTime - start.systemCPUTime),
    peakMemoryBytes: end.maxRSS * 1024,
    memoryBytes: readMemoryUsage().rss,
  };
}

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

function floatValue(wire) {
  const bytes = Buffer.allocUnsafe(8);
  bytes.writeBigUInt64BE(BigInt(`0x${wire.value}`));
  return bytes.readDoubleBE();
}

function validates(contract, wire, depth = 0, budget = { steps: 0 }) {
  if (++budget.steps > 200000 || depth > 64 || !contract || !wire) return false;
  switch (contract.type) {
    case "any": return true;
    case "null": return wire.type === "null";
    case "boolean": return wire.type === "boolean";
    case "int64": {
      if (wire.type !== "int64") return false;
      const value = BigInt(wire.value);
      return (contract.minimum === undefined || value >= BigInt(contract.minimum)) &&
        (contract.maximum === undefined || value <= BigInt(contract.maximum));
    }
    case "float64": {
      if (wire.type !== "float64") return false;
      const value = floatValue(wire);
      return Number.isFinite(value) && (contract.minimum === undefined || value >= contract.minimum) &&
        (contract.maximum === undefined || value <= contract.maximum);
    }
    case "string": {
      if (wire.type !== "string") return false;
      const size = Buffer.byteLength(wire.value, "utf8");
      return (contract.minimum_bytes === undefined || size >= contract.minimum_bytes) &&
        (contract.maximum_bytes === undefined || size <= contract.maximum_bytes);
    }
    case "bytes": {
      if (wire.type !== "bytes") return false;
      const size = Buffer.from(wire.value, "base64url").length;
      return (contract.minimum_bytes === undefined || size >= contract.minimum_bytes) &&
        (contract.maximum_bytes === undefined || size <= contract.maximum_bytes);
    }
    case "timestamp": return wire.type === "timestamp";
    case "typed_id": return wire.type === "typed_id" &&
      (contract.kind === undefined || wire.value.startsWith(`${contract.kind}_`));
    case "document_id": return wire.type === "typed_id" && wire.value.startsWith("doc_");
    case "array": return wire.type === "array" &&
      (contract.minimum_items === undefined || wire.value.length >= contract.minimum_items) &&
      (contract.maximum_items === undefined || wire.value.length <= contract.maximum_items) &&
      wire.value.every((value) => validates(contract.items, value, depth + 1, budget));
    case "object": {
      if (wire.type !== "object") return false;
      const values = new Map(wire.value.map((entry) => [entry.key, entry.value]));
      const fields = Object.keys(contract.fields);
      if (values.size !== wire.value.length || [...values.keys()].some((key) => !(key in contract.fields))) return false;
      return fields.every((key) => values.has(key)
        ? validates(contract.fields[key], values.get(key), depth + 1, budget)
        : (contract.optional ?? []).includes(key));
    }
    case "union": return contract.variants.some((variant) => validates(variant, wire, depth + 1, budget));
    default: return false;
  }
}

async function requestBytes() {
  if (process.argv[2] === "--request-base64url" && process.argv[3]) {
    return Buffer.from(process.argv[3], "base64url");
  }
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks);
}

const contractCache = new Map();

async function contract(digest) {
  let value = contractCache.get(digest);
  if (!value) {
    value = JSON.parse(await readFile(`/opt/runku/contracts/${digest}.json`, "utf8"));
    contractCache.set(digest, value);
  }
  return value;
}

async function executeRequest(request) {
  let performanceStart;
  try {
  if (request.protocolVersion !== 1 || !/^[a-f0-9]{64}$/.test(request.implementationHash)) {
    throw new Error("unsupported protocol");
  }
  if (request.collectPerformance === true) performanceStart = readResourceUsage();
  const argumentsContract = await contract(request.argumentsContractHash);
  const resultContract = await contract(request.resultContractHash);
  if (!validates(argumentsContract, request.arguments)) throw new TypeError("invalid arguments");
  const implementation = await import(pathToFileURL(`/opt/runku/functions/${request.implementationHash}.mjs`).href);
  const exportName = request.function.split(".").at(-1);
  const handler = implementation[exportName];
  if (typeof handler !== "function") throw new TypeError("handler export missing");
  const context = Object.freeze({ invocation: Object.freeze({
    releaseId: request.releaseId,
    invocationId: request.invocationId,
    function: request.function,
  }) });
  const encoded = encode(await handler(context, decode(request.arguments)));
  if (!validates(resultContract, encoded)) throw new TypeError("invalid result");
  return {
    protocolVersion: 1,
    ok: true,
    value: encoded,
    performance: performanceSnapshot(performanceStart),
  };
  } catch {
    return {
    protocolVersion: 1,
    ok: false,
    error: { code: "FUNCTION_FAILED" },
    performance: performanceSnapshot(performanceStart),
    };
  }
}

async function resetInvocationState(mailboxDirectory) {
  const keep = new Set([1, process.pid]);
  for (let pass = 0; pass < 4; pass += 1) {
    for (const entry of await readdir("/proc")) {
      if (!/^\d+$/.test(entry)) continue;
      const pid = Number(entry);
      if (!keep.has(pid)) {
        try { process.kill(pid, "SIGKILL"); } catch {}
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  for (const entry of await readdir("/tmp")) {
    if (!mailboxDirectory || `/tmp/${entry}` !== mailboxDirectory) {
      await rm(`/tmp/${entry}`, { recursive: true, force: true });
    }
  }
}

const maximumFrameBytes = 2 * 1024 * 1024;

async function* frames(socket) {
  let buffered = Buffer.alloc(0);
  for await (const chunk of socket) {
    buffered = buffered.length === 0 ? chunk : Buffer.concat([buffered, chunk]);
    for (;;) {
      if (buffered.length < 4) break;
      const length = buffered.readUInt32BE(0);
      if (length === 0 || length > maximumFrameBytes) throw new TypeError("invalid frame");
      if (buffered.length < 4 + length) break;
      yield buffered.subarray(4, 4 + length);
      buffered = buffered.subarray(4 + length);
    }
  }
  if (buffered.length !== 0) throw new TypeError("truncated frame");
}

async function writeFrame(socket, bytes) {
  if (bytes.length === 0 || bytes.length > maximumFrameBytes) throw new TypeError("invalid frame");
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32BE(bytes.length);
  const headerAccepted = socket.write(header);
  const bodyAccepted = socket.write(bytes);
  if (!headerAccepted || !bodyAccepted) {
    await new Promise((resolve, reject) => {
      socket.once("drain", resolve);
      socket.once("error", reject);
    });
  }
}

async function serveTcp(port, token) {
  if (!Number.isInteger(port) || port < 1024 || port > 65535 || !token || token.length > 256) {
    throw new TypeError("invalid tcp server configuration");
  }
  const server = createServer((socket) => {
    socket.setNoDelay(true);
    void (async () => {
      let authenticated = false;
      try {
        for await (const frame of frames(socket)) {
          if (!authenticated) {
            if (frame.toString("utf8") !== token) throw new TypeError("invalid handshake");
            authenticated = true;
            await writeFrame(socket, Buffer.from("READY"));
            continue;
          }
          const request = JSON.parse(frame.toString("utf8"));
          const response = await executeRequest(request);
          await resetInvocationState(undefined);
          await writeFrame(socket, Buffer.from(JSON.stringify(response)));
        }
      } catch {
        socket.destroy();
      }
    })();
  });
  server.maxConnections = 1;
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen({ host: "0.0.0.0", port, backlog: 1 }, resolve);
  });
}

function waitForChange(directory, fallbackMilliseconds) {
  return new Promise((resolve) => {
    const watcher = watch(directory, () => {
      watcher.close();
      clearTimeout(fallback);
      resolve();
    });
    const fallback = setTimeout(() => {
      watcher.close();
      resolve();
    }, fallbackMilliseconds);
  });
}

async function serve(directory) {
  const requests = `${directory}/requests`;
  const responses = `${directory}/responses`;
  await mkdir(requests, { recursive: true });
  await mkdir(responses, { recursive: true });
  await writeFile(`${directory}/ready`, `${process.pid}\n`, { flag: "wx" });
  const active = new Set();
  let idleDelay = 1;
  for (;;) {
    const changed = waitForChange(requests, idleDelay);
    let discovered = false;
    for (const file of await readdir(requests)) {
      const match = /^(inv_[0-9A-Z]{26})\.json$/.exec(file);
      if (!match || active.has(file)) continue;
      discovered = true;
      active.add(file);
      void (async () => {
        const requestPath = `${requests}/${file}`;
        try {
          const request = JSON.parse(await readFile(requestPath, "utf8"));
          await unlink(requestPath);
          if (request.invocationId !== match[1]) throw new TypeError("invocation mismatch");
          const response = await executeRequest(request);
          await resetInvocationState(directory);
          const staging = `${responses}/.${match[1]}.staging`;
          await writeFile(staging, JSON.stringify(response), { flag: "wx" });
          await rename(staging, `${responses}/${match[1]}.json`);
        } catch {
          const staging = `${responses}/.${match[1]}.staging`;
          const response = { protocolVersion: 1, ok: false, error: { code: "FUNCTION_FAILED" } };
          try {
            await rm(staging, { force: true });
            await writeFile(staging, JSON.stringify(response), { flag: "wx" });
            await rename(staging, `${responses}/${match[1]}.json`);
          } catch {}
        } finally {
          active.delete(file);
        }
      })();
    }
    await changed;
    idleDelay = discovered ? 1 : Math.min(50, idleDelay * 2);
  }
}

if (process.argv[2] === "--serve-tcp" && process.argv[3]) {
  await serveTcp(Number(process.argv[3]), process.env.RUNKU_IPC_TOKEN);
} else if (process.argv[2] === "--serve-directory" && process.argv[3]) {
  await serve(process.argv[3]);
} else {
  const request = JSON.parse((await requestBytes()).toString("utf8"));
  process.stdout.write(JSON.stringify(await executeRequest(request)));
}
