import {
  decodeValue,
  encodeValue,
  type CallOptions,
  type CodeTarget,
  type FunctionReference,
  type RunkuResult,
  type RunkuValue,
} from "@runku/client";

export interface DehydratedQuery {
  readonly key: string;
  readonly target: CodeTarget | null;
  readonly functionName: string;
  readonly arguments: unknown;
  readonly result: {
    readonly requestId: string;
    readonly releaseId: string;
    readonly value: unknown;
    readonly snapshotSequence: string | null;
  };
}

export interface RunkuDehydratedState {
  readonly version: 1;
  readonly identityKey: string;
  readonly queries: readonly DehydratedQuery[];
}

export function queryKey<A extends RunkuValue, R extends RunkuValue>(
  reference: FunctionReference<"query", A, R>,
  argumentsValue: A,
  options: CallOptions = {},
): string {
  return JSON.stringify([
    options.target ?? null,
    reference.name,
    encodeValue(argumentsValue),
  ]);
}

export function dehydrateResult<A extends RunkuValue, T extends RunkuValue>(
  key: string,
  reference: FunctionReference<"query", A, T>,
  argumentsValue: A,
  options: CallOptions,
  result: RunkuResult<T>,
): DehydratedQuery {
  if (result.metadata.kind !== "query") throw new TypeError("Only query results can be dehydrated");
  return Object.freeze({
    key,
    target: options.target ?? null,
    functionName: reference.name,
    arguments: encodeValue(argumentsValue),
    result: Object.freeze({
      requestId: result.requestId,
      releaseId: result.releaseId,
      value: encodeValue(result.value),
      snapshotSequence: result.metadata.snapshotSequence?.toString() ?? null,
    }),
  });
}

export function hydrateValue(entry: DehydratedQuery): RunkuValue {
  return decodeValue(entry.result.value);
}
