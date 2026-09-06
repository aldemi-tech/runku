import type {
  CallOptions,
  FileDownloadGrant,
  FileDownloadOptions,
  FileMetadata,
  FileUploadGrant,
  FileUploadOptions,
  FunctionReference,
  MutationOptions,
  RunkuClient,
  RunkuRealtimeClient,
  RunkuResult,
  RunkuValue,
  RealtimeClientOptions,
} from "@runku/client";

import {
  dehydrateResult,
  queryKey,
  type RunkuDehydratedState,
} from "./shared.js";

export type { DehydratedQuery, RunkuDehydratedState } from "./shared.js";

export interface RunkuReactServer {
  readonly client: RunkuClient;
  query<A extends RunkuValue, R extends RunkuValue>(
    reference: FunctionReference<"query", A, R>,
    argumentsValue: A,
    options?: CallOptions,
  ): Promise<RunkuResult<R>>;
  preloadQuery<A extends RunkuValue, R extends RunkuValue>(
    reference: FunctionReference<"query", A, R>,
    argumentsValue: A,
    options?: CallOptions,
  ): Promise<RunkuResult<R>>;
  mutation<A extends RunkuValue, R extends RunkuValue>(
    reference: FunctionReference<"mutation", A, R>,
    argumentsValue: A,
    options?: MutationOptions,
  ): Promise<RunkuResult<R>>;
  action<A extends RunkuValue, R extends RunkuValue>(
    reference: FunctionReference<"action", A, R>,
    argumentsValue: A,
    options?: CallOptions,
  ): Promise<RunkuResult<R>>;
  uploadFile(grant: FileUploadGrant, body: BodyInit, options?: FileUploadOptions): Promise<FileMetadata>;
  downloadFile(grant: FileDownloadGrant, options?: FileDownloadOptions): Promise<Response>;
  /** Opens Realtime for a long-lived server process; avoid request-scoped RSC subscriptions. */
  realtime(options?: RealtimeClientOptions): RunkuRealtimeClient;
  dehydrate(): RunkuDehydratedState;
}

/** Creates a request-local server facade. Never share it between identities or requests. */
export function createRunkuReactServer(client: RunkuClient, identityKey: string): RunkuReactServer {
  if (identityKey.length === 0) throw new TypeError("identityKey must not be empty");
  const queries = new Map<string, RunkuDehydratedState["queries"][number]>();
  const pending = new Map<string, Promise<RunkuResult<RunkuValue>>>();
  const preloadQuery = async <A extends RunkuValue, R extends RunkuValue>(
    reference: FunctionReference<"query", A, R>,
    argumentsValue: A,
    options: CallOptions = {},
  ): Promise<RunkuResult<R>> => {
    const key = queryKey(reference, argumentsValue, options);
    let promise = pending.get(key) as Promise<RunkuResult<R>> | undefined;
    if (promise === undefined) {
      promise = client.query(reference, argumentsValue, options);
      pending.set(key, promise as unknown as Promise<RunkuResult<RunkuValue>>);
    }
    const result = await promise;
    queries.set(key, dehydrateResult(key, reference, argumentsValue, options, result));
    return result;
  };
  return {
    client,
    query: (reference, argumentsValue, options) => client.query(reference, argumentsValue, options),
    preloadQuery,
    mutation: (reference, argumentsValue, options) => client.mutation(reference, argumentsValue, options),
    action: (reference, argumentsValue, options) => client.action(reference, argumentsValue, options),
    uploadFile: (grant, body, options) => client.uploadFile(grant, body, options),
    downloadFile: (grant, options) => client.downloadFile(grant, options),
    realtime: (options) => client.realtime(options),
    dehydrate: () => Object.freeze({
      version: 1,
      identityKey,
      queries: Object.freeze([...queries.values()]),
    }),
  };
}
