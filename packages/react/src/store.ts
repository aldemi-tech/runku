import type {
  BrowserFunctionAuth,
  CallOptions,
  FunctionReference,
  RunkuClient,
  RunkuError,
  RunkuRealtimeClient,
  RunkuRealtimeSubscription,
  RunkuResult,
  RunkuValue,
} from "@runku/client";

import {
  hydrateValue,
  type RunkuDehydratedState,
} from "./shared.js";

export type QueryStatus = "pending" | "success" | "error";

export interface QueryState<T extends RunkuValue> {
  readonly status: QueryStatus;
  readonly data: T | undefined;
  readonly error: RunkuError | null;
  readonly releaseId: string | null;
  readonly snapshotSequence: bigint | null;
  readonly isStale: boolean;
}

interface QueryEntry<T extends RunkuValue = RunkuValue> {
  snapshot: QueryState<T>;
  listeners: Set<() => void>;
  subscription: RunkuRealtimeSubscription<T> | null;
}

const PENDING: QueryState<RunkuValue> = Object.freeze({
  status: "pending",
  data: undefined,
  error: null,
  releaseId: null,
  snapshotSequence: null,
  isStale: false,
});
const MAX_CACHED_QUERIES = 500;

/** Internal query store. It is emitted for package tests but is not a public package export. */
export class RunkuStore {
  readonly client: RunkuClient;
  readonly entries = new Map<string, QueryEntry>();
  #realtime: RunkuRealtimeClient | null = null;

  constructor(client: RunkuClient, identityKey: string, state: RunkuDehydratedState | null) {
    this.client = client;
    if (identityKey.length === 0) throw new TypeError("identityKey must not be empty");
    if (state !== null) {
      if (state.version !== 1 || state.identityKey !== identityKey) {
        throw new TypeError("Runku hydration state does not match the active identity");
      }
      if (state.queries.length > MAX_CACHED_QUERIES) {
        throw new TypeError("Runku hydration state exceeds the query cache limit");
      }
      for (const query of state.queries) {
        this.entries.set(query.key, {
          snapshot: Object.freeze({
            status: "success",
            data: hydrateValue(query),
            error: null,
            releaseId: query.result.releaseId,
            snapshotSequence: query.result.snapshotSequence === null
              ? null
              : BigInt(query.result.snapshotSequence),
            isStale: true,
          }),
          listeners: new Set(),
          subscription: null,
        });
      }
    }
  }

  entry(key: string): QueryEntry {
    let entry = this.entries.get(key);
    if (entry === undefined) {
      if (this.entries.size >= MAX_CACHED_QUERIES) {
        const evictable = [...this.entries].find(([, candidate]) => candidate.listeners.size === 0);
        if (evictable === undefined) throw new Error("Runku query cache limit reached");
        this.entries.delete(evictable[0]);
      }
      entry = { snapshot: PENDING, listeners: new Set(), subscription: null };
      this.entries.set(key, entry);
    }
    return entry;
  }

  listen<A extends RunkuValue, R extends RunkuValue>(
    key: string,
    reference: FunctionReference<"query", A, R, BrowserFunctionAuth>,
    argumentsValue: A,
    options: CallOptions,
    listener: () => void,
  ): () => void {
    const entry = this.entry(key);
    entry.listeners.add(listener);
    if (entry.subscription === null) {
      entry.subscription = this.realtime().subscribe(reference, argumentsValue, {
        ...options,
        onValue: (state) => {
          entry.snapshot = Object.freeze({
            status: "success",
            data: state.value,
            error: null,
            releaseId: state.releaseId,
            snapshotSequence: state.snapshotSequence,
            isStale: false,
          });
          this.emit(entry);
        },
        onError: (error) => {
          entry.snapshot = Object.freeze({
            ...entry.snapshot,
            status: "error",
            error,
            isStale: true,
          });
          this.emit(entry);
        },
      });
      void entry.subscription.ready.catch(() => undefined);
    }
    return () => {
      entry.listeners.delete(listener);
      if (entry.listeners.size === 0 && entry.subscription !== null) {
        const subscription = entry.subscription;
        entry.subscription = null;
        void subscription.unsubscribe();
      }
    };
  }

  async refetch<A extends RunkuValue, R extends RunkuValue>(
    key: string,
    reference: FunctionReference<"query", A, R, BrowserFunctionAuth>,
    argumentsValue: A,
    options: CallOptions,
  ): Promise<RunkuResult<R>> {
    const result = await this.client.query(reference, argumentsValue, options);
    const entry = this.entry(key) as QueryEntry<R>;
    if (result.metadata.kind !== "query") throw new TypeError("Runku returned non-query metadata");
    entry.snapshot = Object.freeze({
      status: "success",
      data: result.value,
      error: null,
      releaseId: result.releaseId,
      snapshotSequence: result.metadata.snapshotSequence,
      isStale: false,
    });
    this.emit(entry);
    return result;
  }

  close(): void {
    const realtime = this.#realtime;
    this.#realtime = null;
    realtime?.close();
    for (const entry of this.entries.values()) {
      entry.subscription = null;
      entry.listeners.clear();
    }
  }

  private realtime(): RunkuRealtimeClient {
    this.#realtime ??= this.client.realtime();
    return this.#realtime;
  }

  private emit(entry: QueryEntry): void {
    for (const listener of entry.listeners) listener();
  }
}
