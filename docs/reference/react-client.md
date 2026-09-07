# React and Next.js integration

`@runku/react` binds the typed `@runku/client` to React and Next.js. Use it for live Query state,
Mutation and Action execution state, Application File transfers, request-local server calls, and
lossless server-to-client hydration.

## Prerequisites

- configure a stable `RunkuClient` as described in the [TypeScript client](typescript-client.md);
- run `runku build` so the application has generated Function references;
- keep public browser references separate from server-only service references;
- decide which stable, non-secret value partitions cached data by functional identity.

Install the React binding together with the client:

```sh
pnpm add @runku/react@0.5.0 @runku/client@0.5.0
```

The coordinated 0.5.0 distribution retains Public Protocol v1 and includes the CLI runtime
Function-reference generator.

## Generated Function references

`runku build` writes two runtime/type pairs below `runku/_generated`:

| Generated module | Intended consumer | Functions included |
|---|---|---|
| `api.js` / `api.d.ts` | browser and trusted server UI code | public Functions except `auth: "service"` |
| `server.js` / `server.d.ts` | server-only modules | every externally callable public Function, including service-only Functions |

Internal Functions are never emitted into either external tree. A logical Function name separated
by `.` or `/` becomes nested properties: `tasks.list` is `api.tasks.list`.

Never import `server.js` into a Client Component or browser bundle. Doing so would expose a
server-only API description even though it would not by itself provide a credential. The auth
policy is also carried by the generated `FunctionReference` type, so React browser hooks reject a
service-authenticated reference at compile time. The gateway remains the authorization boundary.

## Create the provider

Create one stable client outside render, then place the provider above all Runku hooks:

```tsx
"use client"

import { RunkuClient } from "@runku/client"
import { RunkuProvider } from "@runku/react"

const client = new RunkuClient({
  baseUrl: "https://api.example.com",
  applicationKey: "rk_pub_v1_...",
  getBearer: async () => sessionStorage.getItem("functional-token"),
})

export function RunkuRoot({ userId, children }) {
  return (
    <RunkuProvider client={client} identityKey={userId ?? "anonymous"}>
      {children}
    </RunkuProvider>
  )
}
```

`identityKey` is required, non-empty, stable, and non-secret. Change it whenever the effective
guest/user/service identity changes. Runku then closes the old Realtime connection and discards
the old query cache so one identity cannot inherit another identity's values.

## Read a live Query

```tsx
"use client"

import { useQuery } from "@runku/react"
import { api } from "../runku/_generated/api.js"

export function Tasks() {
  const tasks = useQuery(api.tasks.list, { board: "today" })

  if (tasks.status === "pending") return <p>Loading…</p>
  if (tasks.status === "error") return <button onClick={() => tasks.refetch()}>Retry</button>

  return <TaskList tasks={tasks.data} refreshing={tasks.isStale} />
}
```

`useQuery(reference, arguments, options?)` returns:

| Field | Meaning |
|---|---|
| `status` | `pending`, `success`, or `error` |
| `data` | decoded Query value, or `undefined` before success |
| `error` | current `RunkuError`, otherwise `null` |
| `releaseId` | exact Release that produced the value |
| `snapshotSequence` | Query snapshot sequence when supplied |
| `isStale` | hydrated/last value exists but Realtime has not confirmed current state |
| `refetch()` | performs an authoritative HTTP Query and replaces the cached value |

Equivalent canonical Function, arguments, and target values share one Realtime subscription.
The last subscriber unmounting unsubscribes that shared connection. Treat `isStale` as a display
and refresh signal; do not present hydrated state as freshly confirmed while it is true.

React Strict Mode may replay effect setup and cleanup in development. `RunkuProvider` closes the
current Realtime transport during cleanup and lazily opens a fresh one when the replay subscribes;
the Query snapshot remains in the same identity-partitioned store. An identity or client change
still creates a different store and cannot reuse the previous identity's cache.

## Run a Mutation

```tsx
const createTask = useMutation(api.tasks.create)

async function submit(title: string) {
  const result = await createTask.mutate({ title })
  console.log(result.value)
}
```

The hook returns `status`, `data`, `result`, `error`, `mutate(arguments, options?)`, and `reset()`.
The underlying client preserves a stable Operation ID within its safe Mutation retry cycle. A new
call to `mutate` is new intent unless you explicitly reuse the appropriate operation ID.

## Run an Action

```tsx
const exportTasks = useAction(api.tasks.export)

async function startExport() {
  await exportTasks.execute({ board: "today" })
}
```

The returned state has the same idle/pending/success/error lifecycle plus `execute` and `reset`.
Actions are not automatically retried. If the response is uncertain, reconcile application state
before executing the Action again; external effects may already have happened.

## Transfer Application Files

`useUploadFile()` and `useDownloadFile()` expose the matching `RunkuClient` helpers:

```tsx
const uploadFile = useUploadFile()
const downloadFile = useDownloadFile()

const uploadGrant = fileUploadGrant((await beginUpload.execute(input)).value)
await uploadFile(uploadGrant, file, {
  contentType: file.type,
})

const downloadGrant = fileDownloadGrant((await beginDownload.execute(input)).value)
const response = await downloadFile(downloadGrant)
```

The grant comes from an authorized Action and is itself short-lived authority. Do not log, persist,
cache globally, or place it in analytics. These hooks implement Runku Application File grants; they
are not a generic Runku Object Storage client or presigner.

## Next.js server usage

Create the server facade once per request. Never share it across requests or identities:

```tsx
import "server-only"

import { createRunkuReactServer } from "@runku/react/server"
import { serverApi } from "../runku/_generated/server.js"

const runku = createRunkuReactServer(serverClient, session.user.id)

const result = await runku.preloadQuery(serverApi.tasks.list, { board: "today" })
await runku.mutation(serverApi.tasks.create, { title: "Review" })
await runku.action(serverApi.tasks.export, null)

const state = runku.dehydrate()
```

The facade exposes `query`, `preloadQuery`, `mutation`, `action`, `uploadFile`, `downloadFile`,
`realtime`, and `dehydrate`. Use `realtime` only in a long-lived server process with an injected
WebSocket factory, not for a request-scoped Server Component. Concurrent equivalent `preloadQuery`
calls in one facade share the pending request.
Only preloaded Queries enter the dehydrated state.

Wrap the client provider with the matching hydration boundary:

```tsx
<RunkuHydrationBoundary state={state}>
  <RunkuProvider client={client} identityKey={session.user.id}>
    <Tasks />
  </RunkuProvider>
</RunkuHydrationBoundary>
```

Hydration uses Runku Wire Value v1, not plain JavaScript serialization. Timestamps, typed IDs,
bytes, and signed 64-bit integers therefore cross the React Server Component boundary without
losing their Runku types. The boundary rejects a state version or `identityKey` that does not match
the active provider.

## Failure and security checklist

- render all three Query states and all four Mutation/Action states;
- keep one stable `RunkuClient`; recreating it during render recreates connections and caches;
- change `identityKey` on login, logout, account switch, or functional-role switch;
- never import `serverApi` or secret Application credentials into a browser module;
- use `isStale` to distinguish hydrated data from Realtime-confirmed data;
- do not retry Actions merely because React remounted or a network response was lost;
- do not log file grants, bearer tokens, Application secret keys, or serialized hydration state;
- use the exact generated reference so argument/result types and Function kind remain enforced.

For transport parameters, retry settings, targets, and error codes, see the
[TypeScript client reference](typescript-client.md). For Function authentication and return types,
see the [Function API reference](function-api.md).
