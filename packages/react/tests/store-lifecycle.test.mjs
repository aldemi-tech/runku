import assert from "node:assert/strict";
import test from "node:test";

import { functionReference } from "@runku/client";
import { RunkuStore } from "../dist/store.js";

const reference = functionReference("tasks.list", "query", "public");

function fakeClient() {
  const realtimeClients = [];
  return {
    realtimeClients,
    realtime() {
      const state = { closed: false, subscriptions: [], unsubscribeCount: 0 };
      realtimeClients.push(state);
      return {
        subscribe(_reference, _argumentsValue, options) {
          if (state.closed) throw new Error("The Realtime client is closed");
          const subscription = {
            ready: Promise.resolve({
              releaseId: "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV",
              deliveryRevision: 1n,
              value: ["loaded"],
              resultHash: "0".repeat(64),
              snapshotSequence: 1n,
              authorizedUntilMicros: 2_000_000_000_000_000n,
            }),
            subscriptionId: "sub_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            async unsubscribe() { state.unsubscribeCount += 1; },
          };
          state.subscriptions.push(subscription);
          options.onValue({
            releaseId: "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            deliveryRevision: 1n,
            value: ["loaded"],
            resultHash: "0".repeat(64),
            snapshotSequence: 1n,
            authorizedUntilMicros: 2_000_000_000_000_000n,
          });
          return subscription;
        },
        close() { state.closed = true; },
      };
    },
  };
}

test("provider cleanup can be followed by a Strict Mode resubscription", () => {
  const client = fakeClient();
  const store = new RunkuStore(client, "anonymous", null);
  let notifications = 0;

  const stopFirst = store.listen(
    "tasks.list",
    reference,
    null,
    {},
    () => { notifications += 1; },
  );
  assert.equal(client.realtimeClients.length, 1);
  assert.equal(store.entry("tasks.list").snapshot.status, "success");

  store.close();
  stopFirst();

  const stopSecond = store.listen(
    "tasks.list",
    reference,
    null,
    {},
    () => { notifications += 1; },
  );
  assert.equal(client.realtimeClients.length, 2);
  assert.equal(client.realtimeClients[0].closed, true);
  assert.equal(client.realtimeClients[1].closed, false);
  assert.deepEqual(store.entry("tasks.list").snapshot.data, ["loaded"]);
  assert.equal(notifications, 2);

  stopSecond();
  assert.equal(client.realtimeClients[1].unsubscribeCount, 1);
  store.close();
});
