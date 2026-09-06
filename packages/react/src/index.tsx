"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import type {
  BrowserFunctionAuth,
  CallOptions,
  FileDownloadGrant,
  FileDownloadOptions,
  FileMetadata,
  FileUploadGrant,
  FileUploadOptions,
  FunctionReference,
  MutationOptions,
  RunkuClient,
  RunkuResult,
  RunkuValue,
} from "@runku/client";

import {
  queryKey,
  type RunkuDehydratedState,
} from "./shared.js";
import {
  RunkuStore,
  type QueryState,
} from "./store.js";

export type { RunkuDehydratedState } from "./shared.js";
export type { QueryState, QueryStatus } from "./store.js";

export interface UseQueryResult<T extends RunkuValue> extends QueryState<T> {
  readonly refetch: () => Promise<RunkuResult<T>>;
}

export interface OperationState<T extends RunkuValue> {
  readonly status: "idle" | "pending" | "success" | "error";
  readonly data: T | undefined;
  readonly result: RunkuResult<T> | undefined;
  readonly error: unknown;
}

export interface UseMutationResult<A extends RunkuValue, R extends RunkuValue>
  extends OperationState<R> {
  readonly mutate: (argumentsValue: A, options?: MutationOptions) => Promise<RunkuResult<R>>;
  readonly reset: () => void;
}

export interface UseActionResult<A extends RunkuValue, R extends RunkuValue>
  extends OperationState<R> {
  readonly execute: (argumentsValue: A, options?: CallOptions) => Promise<RunkuResult<R>>;
  readonly reset: () => void;
}

const StoreContext = createContext<RunkuStore | null>(null);
const HydrationContext = createContext<RunkuDehydratedState | null>(null);

export interface RunkuHydrationBoundaryProps {
  readonly state: RunkuDehydratedState;
  readonly children: ReactNode;
}

export function RunkuHydrationBoundary({ state, children }: RunkuHydrationBoundaryProps) {
  return <HydrationContext.Provider value={state}>{children}</HydrationContext.Provider>;
}

export interface RunkuProviderProps {
  readonly client: RunkuClient;
  /** Stable, non-secret identity/cache partition such as a user ID or `anonymous`. */
  readonly identityKey: string;
  readonly children: ReactNode;
}

export function RunkuProvider({ client, identityKey, children }: RunkuProviderProps) {
  const hydration = useContext(HydrationContext);
  const store = useMemo(
    () => new RunkuStore(client, identityKey, hydration),
    [client, identityKey, hydration],
  );
  useEffect(() => () => store.close(), [store]);
  return <StoreContext.Provider value={store}>{children}</StoreContext.Provider>;
}

export function useRunkuClient(): RunkuClient {
  return useStore().client;
}

export function useQuery<A extends RunkuValue, R extends RunkuValue>(
  reference: FunctionReference<"query", A, R, BrowserFunctionAuth>,
  argumentsValue: A,
  options: CallOptions = {},
): UseQueryResult<R> {
  const store = useStore();
  const key = queryKey(reference, argumentsValue, options);
  const stableArguments = useMemo(() => argumentsValue, [key]);
  const stableOptions = useMemo<CallOptions>(() => ({
    ...(options.target === undefined ? {} : { target: options.target }),
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  }), [options.target, options.signal]);
  const subscribe = useCallback(
    (listener: () => void) => store.listen(key, reference, stableArguments, stableOptions, listener),
    [store, key, reference, stableArguments, stableOptions],
  );
  const getSnapshot = useCallback(
    () => store.entry(key).snapshot as QueryState<R>,
    [store, key],
  );
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const refetch = useCallback(
    () => store.refetch(key, reference, stableArguments, stableOptions),
    [store, key, reference, stableArguments, stableOptions],
  );
  return useMemo(() => ({ ...snapshot, refetch }), [snapshot, refetch]);
}

export function useMutation<A extends RunkuValue, R extends RunkuValue>(
  reference: FunctionReference<"mutation", A, R, BrowserFunctionAuth>,
): UseMutationResult<A, R> {
  const client = useRunkuClient();
  const [state, setState] = useState<OperationState<R>>(idleState);
  const mutate = useCallback(async (argumentsValue: A, options?: MutationOptions) => {
    setState(pendingState);
    try {
      const result = await client.mutation(reference, argumentsValue, options);
      setState({ status: "success", data: result.value, result, error: null });
      return result;
    } catch (error) {
      setState({ status: "error", data: undefined, result: undefined, error });
      throw error;
    }
  }, [client, reference]);
  const reset = useCallback(() => setState(idleState), []);
  return { ...state, mutate, reset };
}

export function useAction<A extends RunkuValue, R extends RunkuValue>(
  reference: FunctionReference<"action", A, R, BrowserFunctionAuth>,
): UseActionResult<A, R> {
  const client = useRunkuClient();
  const [state, setState] = useState<OperationState<R>>(idleState);
  const execute = useCallback(async (argumentsValue: A, options?: CallOptions) => {
    setState(pendingState);
    try {
      const result = await client.action(reference, argumentsValue, options);
      setState({ status: "success", data: result.value, result, error: null });
      return result;
    } catch (error) {
      setState({ status: "error", data: undefined, result: undefined, error });
      throw error;
    }
  }, [client, reference]);
  const reset = useCallback(() => setState(idleState), []);
  return { ...state, execute, reset };
}

export function useUploadFile(): (
  grant: FileUploadGrant,
  body: BodyInit,
  options?: FileUploadOptions,
) => Promise<FileMetadata> {
  const client = useRunkuClient();
  return useCallback((grant, body, options) => client.uploadFile(grant, body, options), [client]);
}

export function useDownloadFile(): (
  grant: FileDownloadGrant,
  options?: FileDownloadOptions,
) => Promise<Response> {
  const client = useRunkuClient();
  return useCallback((grant, options) => client.downloadFile(grant, options), [client]);
}

function useStore(): RunkuStore {
  const store = useContext(StoreContext);
  if (store === null) throw new Error("Runku hooks must be used inside RunkuProvider");
  return store;
}

const idleState: OperationState<never> = Object.freeze({
  status: "idle",
  data: undefined,
  result: undefined,
  error: null,
});

const pendingState: OperationState<never> = Object.freeze({
  status: "pending",
  data: undefined,
  result: undefined,
  error: null,
});
