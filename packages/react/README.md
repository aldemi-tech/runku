# `@runku/react`

React and Next.js bindings for `@runku/client`. The package provides typed Function references,
Realtime Query hooks, Mutation and Action hooks, file transfer helpers, and request-local server
utilities with hydration.

Install the exact frontend SDK pair together:

```sh
pnpm add @runku/react@0.5.0 @runku/client@0.5.0
```

Version 0.5.0 uses Public Protocol v1 and ships with the coordinated CLI runtime
Function-reference generation.

## Generated Function references

`runku build` writes two runtime/type pairs below `runku/_generated`:

- `api.js` and `api.d.ts`: browser-safe public Functions. Functions with `auth: "service"` are
  excluded.
- `server.js` and `server.d.ts`: every externally callable public Function, including service-only
  Functions. Keep imports from this module in server-only files.

Internal Functions are callable only by trusted nested Function calls and are never emitted into
either external tree. Function names separated by `.` or `/` become nested properties, so a
Function named `tasks.list` is addressed as `api.tasks.list`. Authentication is part of the
reference type; browser hooks reject an `auth: "service"` reference even if one is imported from
the wrong module.

```tsx
"use client";

import { useMutation, useQuery } from "@runku/react";
import { api } from "../runku/_generated/api.js";

export function Tasks() {
  const tasks = useQuery(api.tasks.list, { board: "today" });
  const createTask = useMutation(api.tasks.create);
  // Arguments and results are inferred from the generated reference.
  return null;
}
```

## Provider and identity isolation

Create one stable `RunkuClient`, then provide a non-secret `identityKey` that changes whenever the
effective user, guest, or service identity changes. Changing it closes the old Realtime connection
and drops the old cache.

```tsx
<RunkuProvider client={client} identityKey={session.user.id}>
  {children}
</RunkuProvider>
```

`useQuery` shares one Realtime subscription per canonical Function/arguments/target key. Its
hydrated value is marked `isStale` until Realtime confirms the current state. `useMutation` keeps a
stable Operation ID within the underlying client's retry cycle. `useAction` is never automatically
retried. `useUploadFile` and `useDownloadFile` consume short-lived grants returned by storage
Actions; the grant itself is authority and must not be logged or persisted.

The provider supports the development effect replay performed by React Strict Mode. Cleanup closes
the current Realtime transport; if React subscribes the same mounted store again, the provider opens
a fresh transport while retaining only that identity's cached Query snapshots.

## Next.js server usage

Create the facade per request. It exposes every operation available to browser code plus
`preloadQuery` and `dehydrate`.

```tsx
import "server-only";
import { createRunkuReactServer } from "@runku/react/server";
import { serverApi } from "../runku/_generated/server.js";

const runku = createRunkuReactServer(serverClient, session.user.id);
const result = await runku.preloadQuery(serverApi.tasks.list, { board: "today" });
await runku.mutation(serverApi.tasks.create, { title: "Review" });
await runku.action(serverApi.tasks.export, null);
const state = runku.dehydrate();
```

Pass `state` through a `RunkuHydrationBoundary` surrounding `RunkuProvider`. Hydration uses Runku
Wire Value v1 rather than JavaScript class instances, so timestamps, IDs, bytes, and 64-bit integers
cross the React Server Component boundary losslessly.

The server facade also delegates `uploadFile`, `downloadFile`, and `realtime`. Use Realtime only in
a long-lived server process with an injected WebSocket factory, not as a request-scoped Server
Component subscription. It does not make Action effects
retry-safe: callers remain responsible for Action idempotency. The current SDK supports Runku
Application File grants. Native S3 multipart operations and a generic object-storage presigner are
not part of this package.
