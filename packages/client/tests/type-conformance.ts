import { RunkuClient, documentId, typedClient } from "../src/index.js";

interface GeneratedFunctions {
  readonly "queries.user": {
    readonly kind: "query";
    readonly visibility: "public";
    readonly arguments: { readonly id: string };
    readonly result: { readonly name: string } | null;
  };
  readonly "mutations.rename": {
    readonly kind: "mutation";
    readonly visibility: "public";
    readonly arguments: { readonly id: string; readonly name: string };
    readonly result: boolean;
  };
  readonly "internal.audit": {
    readonly kind: "mutation";
    readonly visibility: "internal";
    readonly arguments: null;
    readonly result: null;
  };
}

declare const client: RunkuClient;
const typed = typedClient<GeneratedFunctions>(client);
const query = typed.query("queries.user", { id: "one" });
const mutation = typed.mutation("mutations.rename", { id: "one", name: "Ada" });
const realtime = typed.realtime();
const subscription = realtime.subscribe("queries.user", { id: "one" }, {
  onValue(state) {
    state.value?.name satisfies string | undefined;
  },
});
const fileUpload = typed.uploadFile({
  uploadId: "upl_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  path: "/v1/files/uploads/upl_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  token: "token",
  expiresAtMicros: "1",
  maxBytes: "3",
}, new Uint8Array([1, 2, 3]));
const roomId = documentId("rooms", "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV");
roomId.toString() satisfies string;

new RunkuClient({
  baseUrl: "https://api.example",
  target: "channel:stable",
  applicationKey: "rk_pub_v1_01ARZ3NDEKTSV4RRFFQ69G5FAV_AAAAAAAAAAAAAAAAAAAAAA",
});
// @ts-expect-error every external Runku client requires an Application Key
new RunkuClient({ baseUrl: "https://api.example", target: "channel:stable" });

// @ts-expect-error mutation names cannot be called as queries
void typed.query("mutations.rename", { id: "one", name: "Ada" });
// @ts-expect-error required generated fields cannot be omitted
void typed.mutation("mutations.rename", { id: "one" });
// @ts-expect-error internal Functions are absent from the public typed client
void typed.mutation("internal.audit", null);
// @ts-expect-error realtime accepts public queries only
void realtime.subscribe("mutations.rename", { id: "one", name: "Ada" }, { onValue() {} });
// @ts-expect-error realtime arguments come from the generated query contract
void realtime.subscribe("queries.user", { name: "Ada" }, { onValue() {} });

void [query, mutation, subscription, fileUpload];
