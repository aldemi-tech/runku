import { pathToFileURL } from "node:url";
import { watch } from "node:fs";
import { mkdir, readFile, readdir, rename, rm, unlink, writeFile } from "node:fs/promises";

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
      if (!/^[0-9a-f]{16}$/.test(wire.value)) throw new TypeError("invalid float");
      const bytes = Buffer.allocUnsafe(8);
      bytes.writeBigUInt64BE(BigInt(`0x${wire.value}`));
      const value = bytes.readDoubleBE();
      if (!Number.isFinite(value)) throw new TypeError("non-finite float");
      return value;
    }
    case "string": return wire.value;
    case "bytes": return new Uint8Array(Buffer.from(wire.value, "base64url"));
    case "timestamp": return branded(timestampBrand, BigInt(wire.value));
    case "typed_id": return branded(typedIdBrand, wire.value);
    case "array": return wire.value.map((value) => decode(value, depth + 1));
    case "object": {
      const output = Object.create(null);
      for (const entry of wire.value) output[entry.key] = decode(entry.value, depth + 1);
      return output;
    }
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
  if (value instanceof Uint8Array) {
    return { type: "bytes", value: Buffer.from(value).toString("base64url") };
  }
  if (seen.has(value)) throw new TypeError("cyclic result");
  seen.add(value);
  let encoded;
  if (Array.isArray(value)) {
    encoded = { type: "array", value: value.map((item) => encode(item, depth + 1, seen)) };
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new TypeError("unsupported object");
    const keys = Object.keys(value).sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right)));
    encoded = {
      type: "object",
      value: keys.map((key) => ({ key, value: encode(value[key], depth + 1, seen) })),
    };
  }
  seen.delete(value);
  return encoded;
}

function platformContext(request, channel) {
  const capabilities = new Set(request.capabilities ?? []);
  const context = {
    invocation: Object.freeze({
      releaseId: request.releaseId,
      invocationId: request.invocationId,
      functionName: request.function,
      function: request.function,
    }),
    cooperate: () => new Promise((resolve) => setImmediate(resolve)),
    log: Object.freeze({
      trace: async (...values) => console.error(...values),
      debug: async (...values) => console.error(...values),
      info: async (...values) => console.error(...values),
      warn: async (...values) => console.error(...values),
      error: async (...values) => console.error(...values),
    }),
  };
  if (capabilities.has("function:query")) {
    context.runQuery = (func, args) => channel.call("functionCall", { kind: "query", function: func, arguments: encode(args) });
  }
  if (capabilities.has("function:mutation")) {
    context.runMutation = (func, args) => channel.call("functionCall", { kind: "mutation", function: func, arguments: encode(args) });
  }
  if (capabilities.has("function:action")) {
    context.runAction = (func, args) => channel.call("functionCall", { kind: "action", function: func, arguments: encode(args) });
  }
  if (capabilities.has("scheduler:create")) {
    context.scheduler = Object.freeze({
      runAfter: (micros, func, args, options = {}) => channel.call("schedule", {
        function: func, arguments: encode(args), time: { kind: "after", micros: BigInt(micros).toString() },
        idempotencyKey: options.idempotencyKey,
      }, "text"),
      runAt: (micros, func, args, options = {}) => channel.call("schedule", {
        function: func, arguments: encode(args), time: { kind: "at", micros: BigInt(micros).toString() },
        idempotencyKey: options.idempotencyKey,
      }, "text"),
    });
  }
  const storage = {};
  if (capabilities.has("storage:write")) {
    storage.createUpload = (options) => channel.call("storageCreateUpload", {
      maxBytes: options?.maxBytes,
      contentType: options?.contentType,
      sha256: options?.sha256,
    }, "json");
    storage.store = (bytes, options = {}) => {
      if (!(bytes instanceof Uint8Array)) throw new TypeError("storage bytes must be Uint8Array");
      return channel.call("storageStore", {
        bytes: Buffer.from(bytes).toString("base64url"),
        contentType: options.contentType,
        sha256: options.sha256,
      }, "json");
    };
    storage.delete = (fileId) => channel.call("storageDelete", {
      fileId: String(fileId),
    }, "json");
  }
  if (capabilities.has("storage:read")) {
    storage.getMetadata = (fileId) => channel.call("storageMetadata", {
      fileId: String(fileId),
    }, "json");
    storage.createDownload = (fileId, options) => channel.call("storageCreateDownload", {
      fileId: String(fileId),
      expiresInMicros: BigInt(options?.expiresInMicros).toString(),
    }, "json");
    storage.get = async (fileId) => {
      const result = await channel.call("storageGet", { fileId: String(fileId) }, "json");
      return Object.freeze({
        metadata: Object.freeze(result.metadata),
        bytes: new Uint8Array(Buffer.from(result.bytes, "base64url")),
      });
    };
  }
  if (Object.keys(storage).length) context.storage = Object.freeze(storage);
  return Object.freeze(context);
}

async function executeRequest(request, modulePath, exportName, channel) {
  let performanceStart;
  try {
  if (request.protocolVersion !== 1) throw new Error("unsupported protocol");
  if (request.collectPerformance === true) performanceStart = readResourceUsage();
  const implementation = await import(pathToFileURL(modulePath).href);
  const handler = implementation[exportName];
  if (typeof handler !== "function") throw new TypeError("handler export missing");
  const context = platformContext(request, channel);
  const value = await handler(context, decode(request.arguments));
  return {
    protocolVersion: 1,
    ok: true,
    value: encode(value),
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

async function serve(directory, imageRoot) {
  const requests = `${directory}/requests`;
  const responses = `${directory}/responses`;
  await mkdir(requests, { recursive: true });
  await mkdir(responses, { recursive: true });
  await writeFile(`${directory}/ready`, `${process.pid}\n`, { flag: "wx" });
  const active = new Set();
  let idleDelay = 1;
  for (;;) {
    const changed = new Promise((resolve) => {
      const watcher = watch(requests, () => {
        watcher.close();
        clearTimeout(fallback);
        resolve();
      });
      const fallback = setTimeout(() => {
        watcher.close();
        resolve();
      }, idleDelay);
    });
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
          if (request.invocationId !== match[1] || !/^[a-f0-9]{64}$/.test(request.implementationHash)) {
            throw new TypeError("invalid invocation envelope");
          }
          const exportName = request.function.split(".").at(-1);
          const response = await executeRequest(
            request,
            `${imageRoot}/${request.implementationHash}.mjs`,
            exportName,
            undefined,
          );
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

async function readFrame(iterator) {
  const header = Buffer.alloc(4);
  let headerOffset = 0;
  if (iterator.pending?.length) {
    const count = Math.min(4, iterator.pending.length);
    iterator.pending.copy(header, 0, 0, count);
    iterator.pending = iterator.pending.subarray(count);
    headerOffset = count;
  }
  while (headerOffset < 4) {
    const { value, done } = await iterator.next();
    if (done) throw new TypeError("truncated frame");
    const chunk = Buffer.from(value);
    const needed = 4 - headerOffset;
    chunk.copy(header, headerOffset, 0, Math.min(needed, chunk.length));
    headerOffset += Math.min(needed, chunk.length);
    if (chunk.length > needed) iterator.pending = chunk.subarray(needed);
  }
  const length = header.readUInt32BE(0);
  if (length === 0 || length > 2 * 1024 * 1024) throw new TypeError("invalid frame");
  const body = Buffer.alloc(length);
  let offset = 0;
  if (iterator.pending?.length) {
    const count = Math.min(length, iterator.pending.length);
    iterator.pending.copy(body, 0, 0, count);
    iterator.pending = iterator.pending.subarray(count);
    offset = count;
  }
  while (offset < length) {
    const { value, done } = await iterator.next();
    if (done) throw new TypeError("truncated frame");
    const chunk = Buffer.from(value);
    const count = Math.min(length - offset, chunk.length);
    chunk.copy(body, offset, 0, count);
    offset += count;
    if (chunk.length > count) iterator.pending = chunk.subarray(count);
  }
  return body;
}

async function writeStdoutFrame(message) {
  const bytes = Buffer.from(JSON.stringify(message));
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32BE(bytes.length);
  if (!process.stdout.write(Buffer.concat([header, bytes]))) {
    await new Promise((resolve) => process.stdout.once("drain", resolve));
  }
}

async function executeFramed(modulePath, exportName) {
  const iterator = process.stdin[Symbol.asyncIterator]();
  iterator.pending = Buffer.alloc(0);
  const request = JSON.parse((await readFrame(iterator)).toString("utf8"));
  let nextCallId = 1;
  const pending = new Map();
  const channel = {
    async call(type, payload, mode = "value") {
      const callId = nextCallId++;
      const response = new Promise((resolve, reject) => pending.set(callId, { resolve, reject, mode }));
      await writeStdoutFrame({ type, callId, ...payload });
      return response;
    },
  };
  void (async () => {
    for (;;) {
      const response = JSON.parse((await readFrame(iterator)).toString("utf8"));
      if (response.type !== "opResult" || !Number.isSafeInteger(response.callId)) throw new TypeError("invalid op response");
      const waiter = pending.get(response.callId);
      if (!waiter) throw new TypeError("unknown op response");
      pending.delete(response.callId);
      if (!response.ok) waiter.reject(Object.assign(new Error(response.error), { code: response.error }));
      else if (waiter.mode === "text") waiter.resolve(response.text);
      else if (waiter.mode === "json") waiter.resolve(response.json);
      else waiter.resolve(decode(response.value));
    }
  })().catch(() => {
    for (const waiter of pending.values()) waiter.reject(new Error("platform channel closed"));
    pending.clear();
  });
  const response = await executeRequest(request, modulePath, exportName, channel);
  await writeStdoutFrame({ type: "result", response });
  process.stdin.destroy();
}

if (process.argv[1] === "--serve-directory") {
  await serve(process.argv[2], process.argv[3]);
} else {
  await executeFramed(process.argv[1], process.argv[2]);
}
