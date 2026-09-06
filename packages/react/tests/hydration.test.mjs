import assert from "node:assert/strict";
import test from "node:test";

import React from "react";
import { renderToString } from "react-dom/server";
import { RunkuClient, encodeValue, functionReference } from "@runku/client";
import { RunkuHydrationBoundary, RunkuProvider, useQuery } from "../dist/index.js";

const reference = functionReference("tasks.list", "query", "user");
const client = new RunkuClient({
  baseUrl: "https://api.example",
  target: "channel:stable",
  applicationKey: "rk_pub_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV_AAAAAAAAAAAAAAAAAAAAAA",
  fetch: async () => { throw new Error("server render must not refetch hydrated data"); },
  webSocketFactory: () => { throw new Error("server render must not open Realtime"); },
});

function Probe() {
  const query = useQuery(reference, null);
  return React.createElement("span", null, `${query.status}:${query.data?.[0]}:${query.isStale}`);
}

test("hydrated Query state is the server snapshot without opening transports", () => {
  const key = JSON.stringify([null, "tasks.list", encodeValue(null)]);
  const state = {
    version: 1,
    identityKey: "user_123",
    queries: [{
      key,
      target: null,
      functionName: "tasks.list",
      arguments: encodeValue(null),
      result: {
        requestId: "req_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        releaseId: "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        value: encodeValue(["hydrated"]),
        snapshotSequence: "4",
      },
    }],
  };
  const html = renderToString(React.createElement(
    RunkuHydrationBoundary,
    { state },
    React.createElement(RunkuProvider, { client, identityKey: "user_123" }, React.createElement(Probe)),
  ));
  assert.match(html, /success:hydrated:true/);
});
