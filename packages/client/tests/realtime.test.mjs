import assert from "node:assert/strict";
import test from "node:test";

import { RunkuClient, RunkuError } from "../dist/index.js";

const RELEASE_ID = "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const SUBSCRIPTION_ID = "sub_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const DIGEST = "0".repeat(64);
const PUBLISHABLE_KEY = "rk_pub_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV_AAAAAAAAAAAAAAAAAAAAAA";

class FakeSocket {
  readyState = 0;
  binaryType = "blob";
  onopen = null;
  onmessage = null;
  onerror = null;
  onclose = null;
  sent = [];
  subscribeCount = 0;

  constructor(owner) {
    this.owner = owner;
    queueMicrotask(() => {
      this.readyState = 1;
      this.onopen?.(new Event("open"));
    });
  }

  send(data) {
    const command = JSON.parse(data);
    this.sent.push(command);
    if (command.type === "authenticate") {
      this.owner.authentications.push(command);
      this.deliver({ type: "authentication_accepted", version: 1, requestId: command.requestId });
    } else if (command.type === "subscribe") {
      this.subscribeCount += 1;
      this.owner.subscriptions.push(command);
      this.deliver({
        type: "state",
        version: 1,
        requestId: command.requestId,
        subscriptionId: SUBSCRIPTION_ID,
        releaseId: RELEASE_ID,
        deliveryRevision: "1",
        value: { type: "string", value: `initial-${this.owner.sockets.length}` },
        resultHash: DIGEST,
        snapshotSequence: "1",
        authorizedUntilMicros: "2000000000000000",
      });
    } else if (command.type === "unsubscribe") {
      this.deliver({
        type: "unsubscribed",
        version: 1,
        requestId: command.requestId,
        subscriptionId: command.subscriptionId,
      });
    }
  }

  close(code = 1000, reason = "") {
    if (this.readyState === 3) return;
    this.readyState = 3;
    queueMicrotask(() => this.onclose?.({ code, reason }));
  }

  disconnect() { this.close(1012, "restart"); }

  deliver(value) {
    queueMicrotask(() => this.onmessage?.({ data: JSON.stringify(value) }));
  }
}

function harness() {
  const state = { sockets: [], authentications: [], subscriptions: [] };
  state.factory = (url, protocols) => {
    assert.equal(url, "wss://api.example/v1/realtime");
    assert.deepEqual(protocols, ["runku.realtime.v1"]);
    const socket = new FakeSocket(state);
    state.sockets.push(socket);
    return socket;
  };
  return state;
}

test("Realtime authenticates, decodes state, resyncs and unsubscribes", async () => {
  const network = harness();
  let bearerCalls = 0;
  const values = [];
  const client = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    getBearer: () => { bearerCalls += 1; return `token-${bearerCalls}`; },
    webSocketFactory: network.factory,
  });
  const realtime = client.realtime({ reconnectInitialDelayMs: 0, reconnectMaximumDelayMs: 1 });
  const subscription = realtime.subscribe("messages.list", null, {
    onValue: (state) => values.push(state),
  });
  const initial = await subscription.ready;
  assert.equal(initial.value, "initial-1");
  assert.equal(initial.deliveryRevision, 1n);
  assert.equal(subscription.subscriptionId, SUBSCRIPTION_ID);
  assert.equal(network.authentications[0].bearer, "token-1");

  network.sockets[0].deliver({
    type: "resync_required",
    version: 1,
    subscriptionId: SUBSCRIPTION_ID,
    code: "REALTIME_DELIVERY_LAGGED",
  });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(network.sockets[0].subscribeCount, 2);
  assert.equal(values.length, 2);

  await subscription.unsubscribe();
  assert.equal(subscription.subscriptionId, null);
  assert.equal(network.sockets[0].sent.at(-1).type, "unsubscribe");
  realtime.close();
});

test("Reconnect refreshes bearer and performs authoritative resubscribe", async () => {
  const network = harness();
  let bearerCalls = 0;
  const values = [];
  const errors = [];
  const client = new RunkuClient({
    baseUrl: "https://api.example",
    target: "workspace:debug/fix",
    applicationKey: PUBLISHABLE_KEY,
    getBearer: () => { bearerCalls += 1; return `fresh-${bearerCalls}`; },
    webSocketFactory: network.factory,
  });
  const realtime = client.realtime({ reconnectInitialDelayMs: 0, reconnectMaximumDelayMs: 1 });
  const subscription = realtime.subscribe("messages.list", null, {
    onValue: (state) => values.push(state.value),
    onError: (error) => errors.push(error),
  });
  await subscription.ready;
  network.sockets[0].disconnect();
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(network.sockets.length, 2);
  assert.equal(network.authentications[1].bearer, "fresh-2");
  assert.equal(network.subscriptions[1].target, "workspace:debug/fix");
  assert.deepEqual(values, ["initial-1", "initial-2"]);
  assert.ok(errors.some((error) => error instanceof RunkuError && error.code === "SDK_REALTIME_DISCONNECTED"));
  await subscription.unsubscribe();
  realtime.close();
});

test("Malformed server messages close fail-closed", async () => {
  const network = harness();
  const client = new RunkuClient({
    baseUrl: "https://api.example",
    target: "channel:stable",
    applicationKey: PUBLISHABLE_KEY,
    webSocketFactory: network.factory,
  });
  const realtime = client.realtime({ reconnectInitialDelayMs: 100, reconnectMaximumDelayMs: 100 });
  const subscription = realtime.subscribe("messages.list", null, { onValue: () => undefined });
  await subscription.ready;
  network.sockets[0].deliver({ type: "state", version: 1 });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(network.sockets[0].readyState, 3);
  await subscription.unsubscribe();
  realtime.close();
});
