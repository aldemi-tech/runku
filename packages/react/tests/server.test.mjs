import assert from "node:assert/strict";
import test from "node:test";

import { RunkuClient, functionReference } from "@runku/client";
import { createRunkuReactServer } from "../dist/server.js";

const reference = functionReference("tasks.list", "query", "user");
const key = "rk_pub_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV_AAAAAAAAAAAAAAAAAAAAAA";

test("preloadQuery deduplicates a request and emits serializable hydration state", async () => {
  let calls = 0;
  const client = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: key,
    fetch: async () => {
      calls += 1;
      return new Response(JSON.stringify({
        version: 1,
        status: "ok",
        requestId: "req_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        releaseId: "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        result: { type: "array", value: [{ type: "string", value: "Ship SDK" }] },
        metadata: { kind: "query", snapshotSequence: "9" },
      }), { headers: { "content-type": "application/json" } });
    },
  });
  const server = createRunkuReactServer(client, "user_123");
  const [first, second] = await Promise.all([
    server.preloadQuery(reference, { board: "today" }),
    server.preloadQuery(reference, { board: "today" }),
  ]);
  assert.equal(calls, 1);
  assert.deepEqual(first.value, ["Ship SDK"]);
  assert.deepEqual(second.value, ["Ship SDK"]);
  const state = server.dehydrate();
  assert.equal(state.identityKey, "user_123");
  assert.equal(state.queries.length, 1);
  assert.doesNotThrow(() => JSON.stringify(state));
  assert.equal(state.queries[0].result.snapshotSequence, "9");
});
