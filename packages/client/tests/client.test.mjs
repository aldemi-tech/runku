import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  RunkuClient,
  RunkuError,
  RunkuId,
  RunkuTimestamp,
  decodeValue,
  documentId,
  encodeValue,
} from "../dist/index.js";

const REQUEST_ID = "req_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const RELEASE_ID = "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const PUBLISHABLE_KEY = "rk_pub_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV_AAAAAAAAAAAAAAAAAAAAAA";

function response(result, metadata = { kind: "query", snapshotSequence: null }) {
  return new Response(JSON.stringify({
    version: 1,
    status: "ok",
    requestId: REQUEST_ID,
    releaseId: RELEASE_ID,
    result,
    metadata,
  }), { status: 200, headers: { "content-type": "application/json" } });
}

function failure(status, code, retryable) {
  return new Response(JSON.stringify({
    version: 1,
    status: "error",
    requestId: REQUEST_ID,
    error: { code, message: "The service is temporarily unavailable.", retryable },
  }), { status, headers: { "content-type": "application/json" } });
}

test("Wire Value v1 round-trips every JavaScript representation losslessly", () => {
  const values = [
    null,
    true,
    -(1n << 63n),
    (1n << 63n) - 1n,
    1.25,
    "mañana",
    new Uint8Array([0, 1, 254, 255]),
    new RunkuTimestamp(-123456n),
    new RunkuId("document_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
    [1n, "nested"],
    { z: 2n, a: 1n },
  ];
  for (const value of values) {
    const decoded = decodeValue(encodeValue(value));
    if (value instanceof Uint8Array) assert.deepEqual(decoded, value);
    else if (value instanceof RunkuTimestamp) assert.equal(decoded.micros, value.micros);
    else if (value instanceof RunkuId) assert.equal(decoded.value, value.value);
    else if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      assert.deepEqual(Object.entries(decoded), [["a", 1n], ["z", 2n]]);
    } else assert.deepEqual(decoded, value);
  }
  assert.throws(() => encodeValue(Number.NaN));
  assert.throws(() => encodeValue(-0));
  assert.throws(() => decodeValue({ type: "int64", value: "01" }), RunkuError);
  assert.throws(() => decodeValue({
    type: "object",
    value: [{ key: "z", value: { type: "null" } }, { key: "a", value: { type: "null" } }],
  }), RunkuError);
});

test("Document IDs validate canonical shape and retain their wire value", () => {
  const id = documentId("rooms", "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV");
  assert.equal(id.value, "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV");
  assert.equal(id.toString(), id.value);
  assert.throws(() => documentId("Rooms", id.value), TypeError);
  assert.throws(() => documentId("rooms", "opn_01ARZ3NDEKTSV4RRFFQ69G5FAV"), TypeError);
});

test("Wire Value matches the protocol golden vector shared with Rust", () => {
  const golden = JSON.parse(readFileSync(
    new URL("../../../protocol/v1/public-wire-vectors.json", import.meta.url),
    "utf8",
  ));
  assert.deepEqual(encodeValue(decodeValue(golden.queryCall.arguments)), golden.queryCall.arguments);
});

test("Query sends explicit target and independently resolved credentials", async () => {
  const calls = [];
  let bearerCalls = 0;
  const client = new RunkuClient({
    baseUrl: "https://api.example",
    target: "release:rel_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    applicationKey: PUBLISHABLE_KEY,
    getBearer: () => { bearerCalls += 1; return "signed.jwt"; },
    fetch: async (url, init) => {
      calls.push({ url, init, body: JSON.parse(init.body) });
      return response({ type: "string", value: "ready" });
    },
  });
  const result = await client.query("users.me", null, { target: "workspace:debug/issue-42" });
  assert.equal(result.value, "ready");
  assert.equal(result.releaseId, RELEASE_ID);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "https://api.example/v1/query");
  assert.equal(calls[0].body.target, "workspace:debug/issue-42");
  assert.equal(calls[0].init.headers.get("x-runku-key"), PUBLISHABLE_KEY);
  assert.equal(calls[0].init.headers.get("authorization"), "Bearer signed.jwt");
  assert.equal(bearerCalls, 1);
});

test("Default fetch keeps the browser global receiver", async () => {
  const originalFetch = globalThis.fetch;
  let receiver;
  globalThis.fetch = function () {
    receiver = this;
    return Promise.resolve(response({ type: "string", value: "ready" }));
  };
  try {
    const client = new RunkuClient({
      baseUrl: "https://api.example",
      target: "channel:stable",
      applicationKey: PUBLISHABLE_KEY,
    });
    const result = await client.query("users.me", null);
    assert.equal(result.value, "ready");
    assert.equal(receiver, globalThis);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Mutation retries with one stable generated OperationId", async () => {
  const bodies = [];
  const client = new RunkuClient({
    baseUrl: "https://api.example/",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    maxAttempts: 3,
    retryDelayMs: 0,
    fetch: async (_url, init) => {
      bodies.push(JSON.parse(init.body));
      return bodies.length === 1
        ? failure(503, "STORAGE_UNAVAILABLE", true)
        : response(
          { type: "int64", value: "42" },
          { kind: "mutation", commitSequence: "7", replayed: true, attempts: 1 },
        );
    },
  });
  const result = await client.mutation("notes.create", 42n);
  assert.equal(result.value, 42n);
  assert.equal(result.metadata.commitSequence, 7n);
  assert.equal(bodies.length, 2);
  assert.match(bodies[0].operationId, /^opn_[0-9A-HJKMNP-TV-Z]{26}$/);
  assert.equal(bodies[0].operationId, bodies[1].operationId);
});

test("Action uses its endpoint and is never automatically retried", async () => {
  let calls = 0;
  const client = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    maxAttempts: 5,
    retryDelayMs: 0,
    fetch: async (url) => {
      calls += 1;
      assert.equal(url, "https://api.example/v1/action");
      return failure(503, "RUNTIME_UNAVAILABLE", true);
    },
  });
  await assert.rejects(
    client.action("email.send", null),
    (error) => error instanceof RunkuError && error.code === "RUNTIME_UNAVAILABLE" && error.retryable,
  );
  assert.equal(calls, 1);
});

test("File transfers use same-origin bearer grants without application credentials", async () => {
  const uploadId = "upl_01ARZ3NDEKTSV4RRFFQ69G5FAV";
  const fileId = "fil_01ARZ3NDEKTSV4RRFFQ69G5FAV";
  const metadata = {
    fileId,
    sizeBytes: "3",
    sha256: "a".repeat(64),
    contentType: "text/plain",
    createdAtMicros: "1",
  };
  const calls = [];
  const client = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    fetch: async (url, init) => {
      calls.push({ url, init });
      if (init.method === "PUT") {
        return new Response(JSON.stringify({ version: 1, status: "ok", file: metadata }), {
          status: 201,
          headers: { "content-type": "application/json", "x-runku-request-id": REQUEST_ID },
        });
      }
      return new Response("abc", {
        status: 206,
        headers: {
          "accept-ranges": "bytes",
          "content-disposition": "attachment",
          "content-length": "3",
          "content-range": "bytes 0-2/3",
          "content-type": "text/plain",
          etag: `"${metadata.sha256}"`,
        },
      });
    },
  });
  const uploaded = await client.uploadFile({
    uploadId,
    path: `/v1/files/uploads/${uploadId}`,
    token: "upload.secret",
    expiresAtMicros: "999999999999999",
    maxBytes: "3",
  }, new Uint8Array([97, 98, 99]), { contentType: "text/plain" });
  assert.equal(uploaded.fileId, fileId);
  const downloaded = await client.downloadFile({
    path: `/v1/files/downloads/${fileId}`,
    token: "download.secret",
    expiresAtMicros: "999999999999999",
    metadata,
  }, { range: { start: 0n, end: 3n } });
  assert.equal(await downloaded.text(), "abc");
  assert.equal(calls[0].url, `https://api.example/v1/files/uploads/${uploadId}`);
  assert.equal(calls[0].init.headers.get("authorization"), "Bearer upload.secret");
  assert.equal(calls[0].init.headers.get("x-runku-key"), null);
  assert.equal(calls[1].init.headers.get("range"), "bytes=0-2");
  await assert.rejects(client.uploadFile({
    uploadId,
    path: "https://evil.example/upload",
    token: "secret",
    expiresAtMicros: "1",
    maxBytes: "3",
  }, "abc"), TypeError);
});

test("Abort, response limits, malformed envelopes, and config fail closed", async () => {
  let abortCalls = 0;
  const aborting = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    timeoutMs: 5,
    maxAttempts: 5,
    fetch: async (_url, init) => {
      abortCalls += 1;
      return await new Promise((_resolve, reject) => {
        init.signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
      });
    },
  });
  await assert.rejects(
    aborting.query("slow.query", null),
    (error) => error instanceof RunkuError && error.code === "SDK_TIMEOUT",
  );
  assert.equal(abortCalls, 1);

  const oversized = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    fetch: async () => new Response(new Uint8Array(2 * 1024 * 1024 + 1), {
      headers: { "content-type": "application/json" },
    }),
  });
  await assert.rejects(
    oversized.query("large.query", null),
    (error) => error instanceof RunkuError && error.code === "SDK_RESPONSE_LIMIT_EXCEEDED",
  );

  const malformed = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    fetch: async () => new Response("{}", { status: 200 }),
  });
  await assert.rejects(
    malformed.query("bad.query", null),
    (error) => error instanceof RunkuError && error.code === "SDK_RESPONSE_INVALID",
  );
  assert.throws(() => new RunkuClient({ baseUrl: "https://user@api.example", target: "channel:stable" }));
  assert.throws(() => new RunkuClient({ baseUrl: "http://api.example", target: "channel:stable" }));
  assert.throws(() => new RunkuClient({ baseUrl: "http://127.0.0.1:4200", target: "workspace:local" }));
  assert.doesNotThrow(() => new RunkuClient({
    baseUrl: "http://127.0.0.1:4200",
    target: "workspace:local",
    applicationKey: PUBLISHABLE_KEY,
  }));
  assert.throws(() => new RunkuClient({ baseUrl: "https://api.example", target: "latest" }));
  assert.throws(() => new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: "rk_sec_v1_not-a-real-key",
  }));

  const maximumSequence = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    fetch: async () => response(
      { type: "null" },
      { kind: "query", snapshotSequence: "18446744073709551615" },
    ),
  });
  assert.equal((await maximumSequence.query("large.sequence", null)).metadata.snapshotSequence, (1n << 64n) - 1n);

  const wrongMetadata = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    fetch: async () => response(
      { type: "null" },
      { kind: "action", schedulesCreated: "0" },
    ),
  });
  await assert.rejects(
    wrongMetadata.query("bad.metadata", null),
    (error) => error instanceof RunkuError && error.code === "SDK_RESPONSE_INVALID",
  );
});
