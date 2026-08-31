import { documentId, type TypedRunkuClient } from "@runku/client"
import type { RunkuFunctions } from "../runku/_generated/api"

declare const runku: TypedRunkuClient<RunkuFunctions>

void runku.mutation("profiles.upsert", { displayName: "Ada" })
void runku.query("rooms.get", {
  roomId: documentId("rooms", "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
})

// @ts-expect-error unknown Function names are rejected before an HTTP request exists
void runku.mutation("ssssss", { displayName: "Ada" })
// @ts-expect-error query names cannot be invoked through the mutation method
void runku.mutation("rooms.get", {
  roomId: documentId("rooms", "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
})
// @ts-expect-error generated arguments preserve required fields
void runku.mutation("profiles.upsert", {})
void runku.mutation("rooms.join", {
  // @ts-expect-error a document ID for another table cannot cross the schema boundary
  roomId: documentId("profiles", "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
})
