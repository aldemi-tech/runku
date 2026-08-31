const MAX_ENVELOPE_BYTES = 2 * 1024 * 1024;
const MAX_DEPTH = 64;
const MAX_CONTAINER_ITEMS = 10_000;
const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const BASE64URL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
const textEncoder = new TextEncoder();
const ULID_PATTERN = "[0-7][0-9A-HJKMNP-TV-Z]{25}";

export type CodeTarget = `release:rel_${string}` | `channel:${string}` | `workspace:${string}`;

export class RunkuTimestamp {
  readonly micros: bigint;

  constructor(micros: bigint) {
    ensureI64(micros, "timestamp");
    this.micros = micros;
    Object.freeze(this);
  }
}

export class RunkuId {
  readonly value: string;

  constructor(value: string) {
    if (!new RegExp(`^[a-z0-9]{1,16}_${ULID_PATTERN}$`).test(value)) {
      throw new TypeError("Runku typed ID is not canonical");
    }
    this.value = value;
    Object.freeze(this);
  }

  toString(): string {
    return this.value;
  }
}

declare const documentTableBrand: unique symbol;

/** A canonical document identity statically associated with one schema table. */
export type DocumentId<TableName extends string> = RunkuId & {
  readonly [documentTableBrand]: TableName;
};

/** Validates a wire document ID and associates it with its expected table at compile time. */
export function documentId<TableName extends string>(
  tableName: TableName,
  value: string,
): DocumentId<TableName> {
  if (!/^[a-z][A-Za-z0-9_]{0,63}$/.test(tableName)) {
    throw new TypeError("Runku table name is invalid");
  }
  const id = new RunkuId(value);
  if (!id.value.startsWith("doc_")) {
    throw new TypeError("Runku document ID is not canonical");
  }
  return id as DocumentId<TableName>;
}

export type RunkuValue =
  | null
  | boolean
  | bigint
  | number
  | string
  | Uint8Array
  | RunkuTimestamp
  | RunkuId
  | readonly RunkuValue[]
  | { readonly [key: string]: RunkuValue };

export interface RunkuClientConfig {
  readonly baseUrl: string;
  readonly target: CodeTarget;
  readonly applicationKey: string;
  readonly getBearer?: () => string | null | undefined | Promise<string | null | undefined>;
  readonly timeoutMs?: number;
  readonly maxAttempts?: number;
  readonly retryDelayMs?: number;
  readonly fetch?: typeof globalThis.fetch;
  readonly webSocketFactory?: RunkuWebSocketFactory;
}

export interface RunkuWebSocketLike {
  readonly readyState: number;
  binaryType: BinaryType;
  onopen: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

export type RunkuWebSocketFactory = (
  url: string,
  protocols: readonly string[],
) => RunkuWebSocketLike;

export interface RealtimeClientOptions {
  readonly reconnectInitialDelayMs?: number;
  readonly reconnectMaximumDelayMs?: number;
}

export interface RealtimeSubscribeOptions<T extends RunkuValue = RunkuValue> extends CallOptions {
  readonly onValue: (state: RunkuRealtimeState<T>) => void;
  readonly onError?: (error: RunkuError) => void;
}

export interface RunkuRealtimeState<T extends RunkuValue = RunkuValue> {
  readonly subscriptionId: string;
  readonly releaseId: string;
  readonly deliveryRevision: bigint;
  readonly value: T;
  readonly resultHash: string;
  readonly snapshotSequence: bigint | null;
  readonly authorizedUntil: RunkuTimestamp;
}

export interface RunkuRealtimeSubscription<T extends RunkuValue = RunkuValue> {
  readonly ready: Promise<RunkuRealtimeState<T>>;
  readonly subscriptionId: string | null;
  unsubscribe(): Promise<void>;
}

export interface CallOptions {
  readonly signal?: AbortSignal;
  readonly target?: CodeTarget;
}

export interface MutationOptions extends CallOptions {
  readonly operationId?: string;
}

export type RunkuMetadata =
  | Readonly<{ kind: "query"; snapshotSequence: bigint | null }>
  | Readonly<{
      kind: "mutation";
      commitSequence: bigint | null;
      replayed: boolean;
      attempts: number;
    }>
  | Readonly<{ kind: "action"; schedulesCreated: bigint }>;

export interface RunkuResult<T extends RunkuValue = RunkuValue> {
  readonly requestId: string;
  readonly releaseId: string;
  readonly value: T;
  readonly metadata: RunkuMetadata;
}

/** Structural contract entry emitted by `runku build`. */
export interface RunkuFunctionContract<
  K extends "query" | "mutation" | "action" = "query" | "mutation" | "action",
  A extends RunkuValue = RunkuValue,
  R extends RunkuValue = RunkuValue,
  V extends "public" | "internal" = "public" | "internal",
> {
  readonly kind: K;
  readonly visibility: V;
  readonly arguments: A;
  readonly result: R;
}

type FunctionNameOfKind<Registry, Kind extends RunkuFunctionContract["kind"]> = {
  [Name in keyof Registry]: Registry[Name] extends {
    readonly kind: Kind;
    readonly visibility: "public";
  }
    ? Name
    : never;
}[keyof Registry] & string;

type FunctionArguments<Registry, Name extends keyof Registry> = Registry[Name] extends {
  readonly arguments: infer Arguments extends RunkuValue;
} ? Arguments : never;

type FunctionResult<Registry, Name extends keyof Registry> = Registry[Name] extends {
  readonly result: infer Result extends RunkuValue;
} ? Result : never;

/** Compile-time typed view over one ordinary `RunkuClient`; no generated runtime code is used. */
export interface TypedRunkuClient<Registry> {
  query<Name extends FunctionNameOfKind<Registry, "query">>(
    functionName: Name,
    argumentsValue: FunctionArguments<Registry, Name>,
    options?: CallOptions,
  ): Promise<RunkuResult<FunctionResult<Registry, Name>>>;
  mutation<Name extends FunctionNameOfKind<Registry, "mutation">>(
    functionName: Name,
    argumentsValue: FunctionArguments<Registry, Name>,
    options?: MutationOptions,
  ): Promise<RunkuResult<FunctionResult<Registry, Name>>>;
  action<Name extends FunctionNameOfKind<Registry, "action">>(
    functionName: Name,
    argumentsValue: FunctionArguments<Registry, Name>,
    options?: CallOptions,
  ): Promise<RunkuResult<FunctionResult<Registry, Name>>>;
  realtime(options?: RealtimeClientOptions): TypedRunkuRealtimeClient<Registry>;
}

/** Typed realtime view restricted to public query contracts. */
export interface TypedRunkuRealtimeClient<Registry> {
  subscribe<Name extends FunctionNameOfKind<Registry, "query">>(
    functionName: Name,
    argumentsValue: FunctionArguments<Registry, Name>,
    options: RealtimeSubscribeOptions<FunctionResult<Registry, Name>>,
  ): RunkuRealtimeSubscription<FunctionResult<Registry, Name>>;
  close(): void;
}

/** Returns a zero-cost typed view using the registry emitted by `runku build`. */
export function typedClient<Registry>(client: RunkuClient): TypedRunkuClient<Registry> {
  return client as unknown as TypedRunkuClient<Registry>;
}

export class RunkuError extends Error {
  readonly code: string;
  readonly retryable: boolean;
  readonly status: number;
  readonly requestId: string | null;

  constructor(input: {
    code: string;
    message: string;
    retryable: boolean;
    status: number;
    requestId: string | null;
  }) {
    super(input.message);
    this.name = "RunkuError";
    this.code = input.code;
    this.retryable = input.retryable;
    this.status = input.status;
    this.requestId = input.requestId;
    Object.setPrototypeOf(this, new.target.prototype);
  }

  override toString(): string {
    return `${this.name}: ${this.code}${this.requestId === null ? "" : ` (${this.requestId})`}`;
  }
}

type CallKind = "query" | "mutation" | "action";

export class RunkuClient {
  readonly #baseUrl: string;
  readonly #target: CodeTarget;
  readonly #applicationKey: string;
  readonly #getBearer: RunkuClientConfig["getBearer"];
  readonly #timeoutMs: number;
  readonly #maxAttempts: number;
  readonly #retryDelayMs: number;
  readonly #fetch: typeof globalThis.fetch;
  readonly #webSocketFactory: RunkuWebSocketFactory | undefined;

  constructor(config: RunkuClientConfig) {
    this.#baseUrl = validateBaseUrl(config.baseUrl);
    this.#target = validateTarget(config.target);
    this.#applicationKey = validateApplicationKey(config.applicationKey);
    this.#getBearer = config.getBearer;
    this.#timeoutMs = boundedInteger(config.timeoutMs ?? 30_000, 1, 300_000, "timeoutMs");
    this.#maxAttempts = boundedInteger(config.maxAttempts ?? 2, 1, 5, "maxAttempts");
    this.#retryDelayMs = boundedInteger(config.retryDelayMs ?? 50, 0, 10_000, "retryDelayMs");
    const fetchImplementation = config.fetch ?? globalThis.fetch;
    this.#fetch = fetchImplementation?.bind(globalThis);
    this.#webSocketFactory = config.webSocketFactory;
    if (typeof this.#fetch !== "function") throw new TypeError("Fetch API is unavailable");
  }

  realtime(options: RealtimeClientOptions = {}): RunkuRealtimeClient {
    return new RunkuRealtimeClient({
      baseUrl: this.#baseUrl,
      target: this.#target,
      applicationKey: this.#applicationKey,
      ...(this.#getBearer === undefined ? {} : { getBearer: this.#getBearer }),
      ...(this.#webSocketFactory === undefined ? {} : { webSocketFactory: this.#webSocketFactory }),
      ...options,
    });
  }

  async query<T extends RunkuValue = RunkuValue>(
    functionName: string,
    argumentsValue: RunkuValue,
    options: CallOptions = {},
  ): Promise<RunkuResult<T>> {
    return this.#call<T>("query", functionName, argumentsValue, options, undefined);
  }

  async mutation<T extends RunkuValue = RunkuValue>(
    functionName: string,
    argumentsValue: RunkuValue,
    options: MutationOptions = {},
  ): Promise<RunkuResult<T>> {
    const operationId = options.operationId === undefined
      ? generateOperationId()
      : validateOperationId(options.operationId);
    return this.#call<T>("mutation", functionName, argumentsValue, options, operationId);
  }

  async action<T extends RunkuValue = RunkuValue>(
    functionName: string,
    argumentsValue: RunkuValue,
    options: CallOptions = {},
  ): Promise<RunkuResult<T>> {
    return this.#call<T>("action", functionName, argumentsValue, options, undefined);
  }

  async #call<T extends RunkuValue>(
    kind: CallKind,
    functionName: string,
    argumentsValue: RunkuValue,
    options: CallOptions,
    operationId: string | undefined,
  ): Promise<RunkuResult<T>> {
    const target = options.target === undefined ? this.#target : validateTarget(options.target);
    const envelope: Record<string, unknown> = {
      version: 1,
      target,
      function: validateFunctionName(functionName),
      arguments: encodeValue(argumentsValue),
    };
    if (operationId !== undefined) envelope.operationId = operationId;
    const body = JSON.stringify(envelope);
    if (textEncoder.encode(body).byteLength > MAX_ENVELOPE_BYTES) {
      throw localError("SDK_REQUEST_LIMIT_EXCEEDED", "The request exceeds the client limit.");
    }
    const attempts = kind === "action" ? 1 : this.#maxAttempts;
    const lifecycle = abortLifecycle(options.signal, this.#timeoutMs);
    try {
      let lastError: RunkuError | undefined;
      for (let attempt = 1; attempt <= attempts; attempt += 1) {
        try {
          let bearer: string | undefined;
          try {
            const resolved = await this.#getBearer?.();
            bearer = resolved === null || resolved === undefined
              ? undefined
              : validateOptionalCredential(resolved, 16 * 1024, "bearer");
          } catch {
            throw localError("SDK_CREDENTIAL_INVALID", "The bearer credential could not be resolved.");
          }
          const headers = new Headers({ accept: "application/json", "content-type": "application/json" });
          headers.set("x-runku-key", this.#applicationKey);
          if (bearer !== undefined) headers.set("authorization", `Bearer ${bearer}`);
          const response = await this.#fetch(`${this.#baseUrl}/v1/${kind}`, {
            method: "POST",
            headers,
            body,
            signal: lifecycle.signal,
          });
          validateContentType(response);
          const bytes = await readBounded(response);
          const decoded = decodeJson(bytes);
          const headerRequestId = response.headers.get("x-runku-request-id");
          if (response.status === 200) return decodeSuccess<T>(decoded, kind, headerRequestId);
          const error = decodeFailure(decoded, response.status, headerRequestId);
          if (!error.retryable || attempt === attempts) throw error;
          lastError = error;
        } catch (error) {
          if (lifecycle.signal.aborted) throw abortError(lifecycle.timedOut());
          const normalized = error instanceof RunkuError
            ? error
            : localError("SDK_NETWORK_ERROR", "The network request failed.", true);
          if (!normalized.retryable || attempt === attempts) throw normalized;
          lastError = normalized;
        }
        try {
          await delay(this.#retryDelayMs * attempt, lifecycle.signal);
        } catch {
          throw abortError(lifecycle.timedOut());
        }
      }
      throw lastError ?? localError("SDK_INTERNAL_ERROR", "The client request failed.");
    } finally {
      lifecycle.dispose();
    }
  }
}

interface RunkuRealtimeClientConfig extends RealtimeClientOptions {
  readonly baseUrl: string;
  readonly target: CodeTarget;
  readonly applicationKey: string;
  readonly getBearer?: RunkuClientConfig["getBearer"];
  readonly webSocketFactory?: RunkuWebSocketFactory;
}

interface SubscriptionRecord {
  readonly localId: string;
  readonly functionName: string;
  readonly argumentsWire: WireValue;
  readonly target: CodeTarget;
  readonly onValue: (state: RunkuRealtimeState) => void;
  readonly onError: (error: RunkuError) => void;
  readonly resolveReady: (state: RunkuRealtimeState) => void;
  readonly rejectReady: (error: RunkuError) => void;
  serverId: string | null;
  pendingRequestId: string | null;
  active: boolean;
  readySettled: boolean;
}

export class RunkuRealtimeClient {
  readonly #baseUrl: string;
  readonly #target: CodeTarget;
  readonly #applicationKey: string;
  readonly #getBearer: RunkuClientConfig["getBearer"];
  readonly #factory: RunkuWebSocketFactory;
  readonly #initialDelayMs: number;
  readonly #maximumDelayMs: number;
  readonly #subscriptions = new Map<string, SubscriptionRecord>();
  readonly #byServerId = new Map<string, SubscriptionRecord>();
  readonly #pending = new Map<string, SubscriptionRecord>();
  #socket: RunkuWebSocketLike | null = null;
  #connectPromise: Promise<void> | null = null;
  #resolveAuthentication: (() => void) | null = null;
  #rejectAuthentication: ((error: RunkuError) => void) | null = null;
  #authenticationRequestId: string | null = null;
  #closed = false;
  #reconnectAttempt = 0;
  #reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(config: RunkuRealtimeClientConfig) {
    this.#baseUrl = validateBaseUrl(config.baseUrl);
    this.#target = validateTarget(config.target);
    this.#applicationKey = validateApplicationKey(config.applicationKey);
    this.#getBearer = config.getBearer;
    this.#initialDelayMs = boundedInteger(config.reconnectInitialDelayMs ?? 100, 0, 60_000, "reconnectInitialDelayMs");
    this.#maximumDelayMs = boundedInteger(config.reconnectMaximumDelayMs ?? 10_000, 1, 300_000, "reconnectMaximumDelayMs");
    if (this.#initialDelayMs > this.#maximumDelayMs) throw new TypeError("reconnect delays are inverted");
    this.#factory = config.webSocketFactory ?? defaultWebSocketFactory;
  }

  subscribe<T extends RunkuValue = RunkuValue>(
    functionName: string,
    argumentsValue: T,
    options: RealtimeSubscribeOptions<T>,
  ): RunkuRealtimeSubscription<T> {
    if (this.#closed) throw localError("SDK_REALTIME_CLOSED", "The Realtime client is closed.");
    const localId = generateResourceId("req");
    let resolveReady: (state: RunkuRealtimeState<T>) => void = () => undefined;
    let rejectReady: (error: RunkuError) => void = () => undefined;
    const ready = new Promise<RunkuRealtimeState<T>>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    const record: SubscriptionRecord = {
      localId,
      functionName: validateFunctionName(functionName),
      argumentsWire: encodeValue(argumentsValue),
      target: options.target === undefined ? this.#target : validateTarget(options.target),
      onValue: (state) => options.onValue(state as RunkuRealtimeState<T>),
      onError: options.onError ?? (() => undefined),
      resolveReady: (state) => resolveReady(state as RunkuRealtimeState<T>),
      rejectReady,
      serverId: null,
      pendingRequestId: null,
      active: true,
      readySettled: false,
    };
    this.#subscriptions.set(localId, record);
    if (options.signal?.aborted === true) void this.#unsubscribe(record);
    else options.signal?.addEventListener("abort", () => { void this.#unsubscribe(record); }, { once: true });
    void this.#ensureConnected().then(
      () => this.#sendSubscribe(record),
      (error: unknown) => this.#report(record, normalizeRealtimeError(error)),
    );
    const owner = this;
    return {
      ready,
      get subscriptionId(): string | null { return record.serverId; },
      unsubscribe: () => owner.#unsubscribe(record),
    };
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#reconnectTimer !== null) clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = null;
    const error = localError("SDK_REALTIME_CLOSED", "The Realtime client is closed.");
    for (const record of this.#subscriptions.values()) {
      record.active = false;
      if (!record.readySettled) record.rejectReady(error);
    }
    this.#subscriptions.clear();
    this.#pending.clear();
    this.#byServerId.clear();
    this.#socket?.close(1000, "client closed");
    this.#socket = null;
  }

  async #ensureConnected(): Promise<void> {
    if (this.#closed) throw localError("SDK_REALTIME_CLOSED", "The Realtime client is closed.");
    if (this.#socket?.readyState === 1 && this.#authenticationRequestId === null) return;
    if (this.#connectPromise !== null) return this.#connectPromise;
    this.#connectPromise = this.#connect();
    try { await this.#connectPromise; } finally { this.#connectPromise = null; }
  }

  async #connect(): Promise<void> {
    const socket = this.#factory(realtimeUrl(this.#baseUrl), ["runku.realtime.v1"]);
    this.#socket = socket;
    socket.binaryType = "arraybuffer";
    socket.onmessage = (event) => this.#onMessage(event.data);
    socket.onclose = () => this.#onClose();
    socket.onerror = () => undefined;
    await new Promise<void>((resolve, reject) => {
      socket.onopen = () => resolve();
      const fail = (): void => reject(localError("SDK_REALTIME_NETWORK_ERROR", "The Realtime connection failed.", true));
      const original = socket.onclose;
      socket.onclose = (event) => { original?.(event); fail(); };
    });
    let bearer: string | undefined;
    try {
      const resolved = await this.#getBearer?.();
      bearer = resolved === null || resolved === undefined
        ? undefined
        : validateOptionalCredential(resolved, 16 * 1024, "bearer");
    } catch {
      socket.close(1008, "credential unavailable");
      throw localError("SDK_CREDENTIAL_INVALID", "The bearer credential could not be resolved.");
    }
    const requestId = generateResourceId("req");
    this.#authenticationRequestId = requestId;
    const authenticated = new Promise<void>((resolve, reject) => {
      this.#resolveAuthentication = resolve;
      this.#rejectAuthentication = reject;
    });
    socket.send(JSON.stringify({
      type: "authenticate",
      version: 1,
      requestId,
      applicationKey: this.#applicationKey,
      bearer: bearer ?? null,
    }));
    await authenticated;
    this.#reconnectAttempt = 0;
  }

  #onMessage(data: unknown): void {
    if (typeof data !== "string" || textEncoder.encode(data).byteLength > 64 * 1024) {
      this.#socket?.close(1008, "invalid message");
      return;
    }
    let message: RealtimeMessage;
    try { message = decodeRealtimeMessage(JSON.parse(data) as unknown); }
    catch { this.#socket?.close(1008, "invalid message"); return; }
    if (message.type === "authentication_accepted") {
      if (message.requestId !== this.#authenticationRequestId) { this.#socket?.close(1008, "invalid auth correlation"); return; }
      this.#authenticationRequestId = null;
      this.#resolveAuthentication?.();
      this.#resolveAuthentication = null;
      this.#rejectAuthentication = null;
      return;
    }
    if (message.type === "state") {
      const record = (message.requestId === null ? this.#byServerId.get(message.subscriptionId) : this.#pending.get(message.requestId));
      if (record === undefined || !record.active) return;
      if (message.requestId !== null) {
        this.#pending.delete(message.requestId);
        record.pendingRequestId = null;
        if (record.serverId !== null) this.#byServerId.delete(record.serverId);
        record.serverId = message.subscriptionId;
        this.#byServerId.set(message.subscriptionId, record);
      }
      const state = message.state;
      record.onValue(state);
      if (!record.readySettled) { record.readySettled = true; record.resolveReady(state); }
      return;
    }
    if (message.type === "resync_required") {
      const record = this.#byServerId.get(message.subscriptionId);
      if (record !== undefined && record.active) {
        this.#byServerId.delete(message.subscriptionId);
        record.serverId = null;
        this.#sendSubscribe(record);
      }
      return;
    }
    if (message.type === "error") {
      const record = message.requestId === null
        ? (message.subscriptionId === null ? undefined : this.#byServerId.get(message.subscriptionId))
        : this.#pending.get(message.requestId);
      const error = localError(message.code, "The Realtime operation failed.", message.retryable);
      if (record !== undefined) {
        if (message.requestId !== null) { this.#pending.delete(message.requestId); record.pendingRequestId = null; }
        this.#report(record, error);
      }
    }
  }

  #sendSubscribe(record: SubscriptionRecord): void {
    if (!record.active || record.pendingRequestId !== null || this.#socket?.readyState !== 1) return;
    const requestId = generateResourceId("req");
    record.pendingRequestId = requestId;
    this.#pending.set(requestId, record);
    this.#socket.send(JSON.stringify({
      type: "subscribe",
      version: 1,
      requestId,
      target: record.target,
      function: record.functionName,
      arguments: record.argumentsWire,
    }));
  }

  async #unsubscribe(record: SubscriptionRecord): Promise<void> {
    if (!record.active) return;
    record.active = false;
    this.#subscriptions.delete(record.localId);
    if (record.pendingRequestId !== null) this.#pending.delete(record.pendingRequestId);
    if (!record.readySettled) {
      record.readySettled = true;
      record.rejectReady(localError("SDK_ABORTED", "The subscription was cancelled."));
    }
    if (record.serverId !== null) {
      const serverId = record.serverId;
      record.serverId = null;
      this.#byServerId.delete(serverId);
      if (this.#socket?.readyState === 1) {
        this.#socket.send(JSON.stringify({
          type: "unsubscribe",
          version: 1,
          requestId: generateResourceId("req"),
          subscriptionId: serverId,
        }));
      }
    }
  }

  #report(record: SubscriptionRecord, error: RunkuError): void {
    record.onError(error);
    if (!record.readySettled && !error.retryable) {
      record.readySettled = true;
      record.rejectReady(error);
    }
  }

  #onClose(): void {
    const error = localError("SDK_REALTIME_DISCONNECTED", "The Realtime connection was interrupted.", true);
    this.#rejectAuthentication?.(error);
    this.#resolveAuthentication = null;
    this.#rejectAuthentication = null;
    this.#authenticationRequestId = null;
    this.#socket = null;
    this.#pending.clear();
    this.#byServerId.clear();
    for (const record of this.#subscriptions.values()) {
      record.pendingRequestId = null;
      record.serverId = null;
      if (record.active) record.onError(error);
    }
    this.#queueReconnect();
  }

  #queueReconnect(): void {
    if (this.#closed || this.#reconnectTimer !== null
        || ![...this.#subscriptions.values()].some((record) => record.active)) return;
    const delayMs = Math.min(this.#maximumDelayMs, this.#initialDelayMs * 2 ** Math.min(this.#reconnectAttempt, 16));
    this.#reconnectAttempt += 1;
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      void this.#ensureConnected().then(
        () => { for (const record of this.#subscriptions.values()) this.#sendSubscribe(record); },
        () => this.#queueReconnect(),
      );
    }, delayMs);
  }
}

type RealtimeMessage =
  | Readonly<{ type: "authentication_accepted"; requestId: string }>
  | Readonly<{
      type: "state";
      requestId: string | null;
      subscriptionId: string;
      state: RunkuRealtimeState;
    }>
  | Readonly<{
      type: "error";
      requestId: string | null;
      subscriptionId: string | null;
      code: string;
      retryable: boolean;
    }>
  | Readonly<{ type: "resync_required"; subscriptionId: string }>
  | Readonly<{ type: "unsubscribed" }>
  | Readonly<{ type: "pong" }>;

function decodeRealtimeMessage(value: unknown): RealtimeMessage {
  if (!isRecord(value) || value.version !== 1 || typeof value.type !== "string") throw protocolError();
  switch (value.type) {
    case "authentication_accepted":
      if (!hasExactKeys(value, ["type", "version", "requestId"])) throw protocolError();
      return { type: "authentication_accepted", requestId: parseResourceId(value.requestId, "req") };
    case "state": {
      if (!hasExactKeys(value, [
        "type", "version", "requestId", "subscriptionId", "releaseId", "deliveryRevision",
        "value", "resultHash", "snapshotSequence", "authorizedUntilMicros",
      ])) throw protocolError();
      const requestId = value.requestId === null ? null : parseResourceId(value.requestId, "req");
      const subscriptionId = parseResourceId(value.subscriptionId, "sub");
      const releaseId = parseResourceId(value.releaseId, "rel");
      const deliveryRevision = parseU64(value.deliveryRevision);
      if (deliveryRevision === 0n || typeof value.resultHash !== "string" || !/^[0-9a-f]{64}$/.test(value.resultHash)) {
        throw protocolError();
      }
      const snapshotSequence = value.snapshotSequence === null ? null : parseU64(value.snapshotSequence);
      const authorizedMicros = parseI64(value.authorizedUntilMicros, true);
      if (authorizedMicros < 0n) throw protocolError();
      return {
        type: "state",
        requestId,
        subscriptionId,
        state: {
          subscriptionId,
          releaseId,
          deliveryRevision,
          value: decodeValue(value.value),
          resultHash: value.resultHash,
          snapshotSequence,
          authorizedUntil: new RunkuTimestamp(authorizedMicros),
        },
      };
    }
    case "error": {
      if (!hasExactKeys(value, [
        "type", "version", "requestId", "subscriptionId", "deliveryRevision", "code", "retryable",
      ])) throw protocolError();
      if (typeof value.code !== "string" || !/^[A-Z][A-Z0-9_]{0,63}$/.test(value.code)
          || typeof value.retryable !== "boolean") throw protocolError();
      if (value.deliveryRevision !== null && parseU64(value.deliveryRevision) === 0n) throw protocolError();
      return {
        type: "error",
        requestId: value.requestId === null ? null : parseResourceId(value.requestId, "req"),
        subscriptionId: value.subscriptionId === null ? null : parseResourceId(value.subscriptionId, "sub"),
        code: value.code,
        retryable: value.retryable,
      };
    }
    case "resync_required":
      if (!hasExactKeys(value, ["type", "version", "subscriptionId", "code"])
          || typeof value.code !== "string" || !/^[A-Z][A-Z0-9_]{0,63}$/.test(value.code)) throw protocolError();
      return { type: "resync_required", subscriptionId: parseResourceId(value.subscriptionId, "sub") };
    case "unsubscribed":
      if (!hasExactKeys(value, ["type", "version", "requestId", "subscriptionId"])) throw protocolError();
      parseResourceId(value.requestId, "req");
      parseResourceId(value.subscriptionId, "sub");
      return { type: "unsubscribed" };
    case "pong":
      if (!hasExactKeys(value, ["type", "version", "requestId"])) throw protocolError();
      parseResourceId(value.requestId, "req");
      return { type: "pong" };
    default:
      throw protocolError();
  }
}

function defaultWebSocketFactory(url: string, protocols: readonly string[]): RunkuWebSocketLike {
  if (typeof globalThis.WebSocket !== "function") {
    throw localError("SDK_REALTIME_UNAVAILABLE", "The WebSocket API is unavailable.");
  }
  return new globalThis.WebSocket(url, [...protocols]);
}

function realtimeUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = "/v1/realtime";
  return url.toString();
}

function normalizeRealtimeError(error: unknown): RunkuError {
  return error instanceof RunkuError
    ? error
    : localError("SDK_REALTIME_NETWORK_ERROR", "The Realtime connection failed.", true);
}

function parseResourceId(value: unknown, prefix: string): string {
  if (typeof value !== "string" || !new RegExp(`^${prefix}_${ULID_PATTERN}$`).test(value)) throw protocolError();
  return value;
}

type WireValue = Record<string, unknown>;

export function encodeValue(value: RunkuValue, depth = 0): WireValue {
  if (depth > MAX_DEPTH) throw new TypeError("Runku value exceeds depth limit");
  if (value === null) return { type: "null" };
  if (typeof value === "boolean") return { type: "boolean", value };
  if (typeof value === "bigint") {
    ensureI64(value, "int64");
    return { type: "int64", value: value.toString() };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || Object.is(value, -0)) throw new TypeError("Runku float must be finite and not negative zero");
    const view = new DataView(new ArrayBuffer(8));
    view.setFloat64(0, value, false);
    return { type: "float64", value: view.getBigUint64(0, false).toString(16).padStart(16, "0") };
  }
  if (typeof value === "string") {
    if (!isUnicodeScalarString(value)) throw new TypeError("Runku string is not valid Unicode");
    return { type: "string", value };
  }
  if (value instanceof Uint8Array) {
    if (value.byteLength > MAX_ENVELOPE_BYTES) throw new TypeError("Runku bytes exceed value limit");
    return { type: "bytes", value: encodeBase64Url(value) };
  }
  if (value instanceof RunkuTimestamp) return { type: "timestamp", value: value.micros.toString() };
  if (value instanceof RunkuId) return { type: "typed_id", value: value.value };
  if (Array.isArray(value)) {
    if (value.length > MAX_CONTAINER_ITEMS) throw new TypeError("Runku array exceeds item limit");
    return { type: "array", value: value.map((item) => encodeValue(item, depth + 1)) };
  }
  if (typeof value === "object") {
    const objectValue = value as { readonly [key: string]: RunkuValue };
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new TypeError("Runku object must be plain");
    const keys = Object.keys(value);
    if (keys.length > MAX_CONTAINER_ITEMS) throw new TypeError("Runku object exceeds item limit");
    if (keys.some((key) => !isUnicodeScalarString(key))) throw new TypeError("Runku object key is not valid Unicode");
    keys.sort(compareUtf8);
    return {
      type: "object",
      value: keys.map((key) => ({ key, value: encodeValue(objectValue[key] as RunkuValue, depth + 1) })),
    };
  }
  throw new TypeError("Unsupported Runku value");
}

export function decodeValue(wire: unknown, depth = 0): RunkuValue {
  if (depth > MAX_DEPTH || !isRecord(wire) || typeof wire.type !== "string") {
    throw protocolError();
  }
  const exact = (keys: readonly string[]): void => {
    const actual = Object.keys(wire).sort();
    if (actual.length !== keys.length || actual.some((key, index) => key !== [...keys].sort()[index])) {
      throw protocolError();
    }
  };
  switch (wire.type) {
    case "null": exact(["type"]); return null;
    case "boolean": exact(["type", "value"]); if (typeof wire.value !== "boolean") throw protocolError(); return wire.value;
    case "int64": exact(["type", "value"]); return parseI64(wire.value, false);
    case "float64": {
      exact(["type", "value"]);
      if (typeof wire.value !== "string" || !/^[0-9a-f]{16}$/.test(wire.value)) throw protocolError();
      const view = new DataView(new ArrayBuffer(8));
      view.setBigUint64(0, BigInt(`0x${wire.value}`), false);
      const value = view.getFloat64(0, false);
      if (!Number.isFinite(value) || Object.is(value, -0)) throw protocolError();
      return value;
    }
    case "string": exact(["type", "value"]); if (typeof wire.value !== "string" || !isUnicodeScalarString(wire.value)) throw protocolError(); return wire.value;
    case "bytes": exact(["type", "value"]); if (typeof wire.value !== "string") throw protocolError(); return decodeBase64Url(wire.value);
    case "timestamp": exact(["type", "value"]); return new RunkuTimestamp(parseI64(wire.value, true));
    case "typed_id": exact(["type", "value"]); if (typeof wire.value !== "string") throw protocolError(); return new RunkuId(wire.value);
    case "array": {
      exact(["type", "value"]);
      if (!Array.isArray(wire.value) || wire.value.length > MAX_CONTAINER_ITEMS) throw protocolError();
      return wire.value.map((item) => decodeValue(item, depth + 1));
    }
    case "object": {
      exact(["type", "value"]);
      if (!Array.isArray(wire.value) || wire.value.length > MAX_CONTAINER_ITEMS) throw protocolError();
      const output: Record<string, RunkuValue> = Object.create(null) as Record<string, RunkuValue>;
      let previous: string | undefined;
      for (const entry of wire.value) {
        if (!isRecord(entry) || Object.keys(entry).sort().join(",") !== "key,value"
            || typeof entry.key !== "string" || !isUnicodeScalarString(entry.key)) throw protocolError();
        if (previous !== undefined && compareUtf8(previous, entry.key) >= 0) throw protocolError();
        previous = entry.key;
        output[entry.key] = decodeValue(entry.value, depth + 1);
      }
      return output;
    }
    default: throw protocolError();
  }
}

function decodeSuccess<T extends RunkuValue>(
  value: unknown,
  expectedKind: CallKind,
  headerRequestId: string | null,
): RunkuResult<T> {
  if (!isRecord(value) || !hasExactKeys(value, ["version", "status", "requestId", "releaseId", "result", "metadata"])
      || value.version !== 1 || value.status !== "ok"
      || typeof value.requestId !== "string" || !new RegExp(`^req_${ULID_PATTERN}$`).test(value.requestId)
      || typeof value.releaseId !== "string" || !new RegExp(`^rel_${ULID_PATTERN}$`).test(value.releaseId)
      || (headerRequestId !== null && headerRequestId !== value.requestId)) throw protocolError();
  return Object.freeze({
    requestId: value.requestId,
    releaseId: value.releaseId,
    value: decodeValue(value.result) as T,
    metadata: decodeMetadata(value.metadata, expectedKind),
  });
}

function decodeMetadata(value: unknown, expectedKind: CallKind): RunkuMetadata {
  if (!isRecord(value) || value.kind !== expectedKind) throw protocolError();
  switch (expectedKind) {
    case "query":
      if (!hasExactKeys(value, ["kind", "snapshotSequence"])) throw protocolError();
      return Object.freeze({
        kind: "query",
        snapshotSequence: value.snapshotSequence === null ? null : parseU64(value.snapshotSequence),
      });
    case "mutation":
      if (!hasExactKeys(value, ["kind", "commitSequence", "replayed", "attempts"])
          || typeof value.replayed !== "boolean" || typeof value.attempts !== "number"
          || !Number.isInteger(value.attempts) || value.attempts < 1 || value.attempts > 255) throw protocolError();
      return Object.freeze({
        kind: "mutation",
        commitSequence: value.commitSequence === null ? null : parseU64(value.commitSequence),
        replayed: value.replayed,
        attempts: value.attempts,
      });
    case "action":
      if (!hasExactKeys(value, ["kind", "schedulesCreated"])) throw protocolError();
      return Object.freeze({ kind: "action", schedulesCreated: parseU64(value.schedulesCreated) });
  }
}

function decodeFailure(value: unknown, status: number, headerRequestId: string | null): RunkuError {
  if (!isRecord(value) || !hasExactKeys(value, ["version", "status", "requestId", "error"])
      || value.version !== 1 || value.status !== "error"
      || typeof value.requestId !== "string" || !new RegExp(`^req_${ULID_PATTERN}$`).test(value.requestId)
      || (headerRequestId !== null && headerRequestId !== value.requestId)
      || !isRecord(value.error) || !hasExactKeys(value.error, ["code", "message", "retryable"])
      || typeof value.error.code !== "string"
      || !/^[A-Z][A-Z0-9_]{0,63}$/.test(value.error.code)
      || typeof value.error.message !== "string" || value.error.message.length === 0
      || textEncoder.encode(value.error.message).byteLength > 128 || !isUnicodeScalarString(value.error.message)
      || /\p{Cc}/u.test(value.error.message)
      || typeof value.error.retryable !== "boolean" || status < 400 || status > 599) throw protocolError();
  return new RunkuError({
    code: value.error.code,
    message: value.error.message,
    retryable: value.error.retryable,
    status,
    requestId: value.requestId,
  });
}

async function readBounded(response: Response): Promise<Uint8Array> {
  if (response.body === null) throw protocolError();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    length += next.value.byteLength;
    if (length > MAX_ENVELOPE_BYTES) {
      await reader.cancel();
      throw localError("SDK_RESPONSE_LIMIT_EXCEEDED", "The response exceeds the client limit.");
    }
    chunks.push(next.value);
  }
  const output = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

function validateContentType(response: Response): void {
  const contentType = response.headers.get("content-type");
  if (contentType === null || contentType.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
    throw protocolError();
  }
  const length = response.headers.get("content-length");
  if (length !== null) {
    if (!/^(?:0|[1-9][0-9]*)$/.test(length)) throw protocolError();
    if (BigInt(length) > BigInt(MAX_ENVELOPE_BYTES)) {
      throw localError("SDK_RESPONSE_LIMIT_EXCEEDED", "The response exceeds the client limit.");
    }
  }
}

function decodeJson(bytes: Uint8Array): unknown {
  if (bytes.byteLength === 0) throw protocolError();
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown;
  } catch {
    throw protocolError();
  }
}

function validateBaseUrl(input: string): string {
  let url: URL;
  try { url = new URL(input); } catch { throw new TypeError("baseUrl is invalid"); }
  if ((url.protocol !== "https:" && url.protocol !== "http:") || url.username !== "" || url.password !== ""
      || url.search !== "" || url.hash !== "" || (url.pathname !== "/" && url.pathname !== "")) {
    throw new TypeError("baseUrl must be an HTTP(S) origin");
  }
  if (url.protocol === "http:"
      && url.hostname !== "localhost" && url.hostname !== "127.0.0.1" && url.hostname !== "[::1]") {
    throw new TypeError("plain HTTP is only allowed for loopback development");
  }
  return url.origin;
}

function validateTarget(input: CodeTarget): CodeTarget {
  const bytes = textEncoder.encode(input).byteLength;
  const validRelease = new RegExp(`^release:rel_${ULID_PATTERN}$`).test(input);
  const validChannel = /^channel:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(input) && bytes <= 71;
  const workspace = input.startsWith("workspace:") ? input.slice(10) : "";
  const validWorkspace = workspace.length > 0 && textEncoder.encode(workspace).byteLength <= 100
    && workspace.split("/").every((part) => /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(part));
  if (!validRelease && !validChannel && !validWorkspace) throw new TypeError("target is not canonical");
  return input;
}

function validateFunctionName(value: string): string {
  if (textEncoder.encode(value).byteLength > 128 || !/^[A-Za-z][A-Za-z0-9_.\/-]*$/.test(value)) {
    throw new TypeError("function name is not canonical");
  }
  return value;
}

function validateOperationId(value: string): string {
  if (!new RegExp(`^opn_${ULID_PATTERN}$`).test(value)) throw new TypeError("operationId is not canonical");
  return value;
}

function generateOperationId(): string {
  return generateResourceId("opn");
}

function generateResourceId(prefix: "opn" | "req"): string {
  const random = new Uint8Array(10);
  if (globalThis.crypto === undefined || typeof globalThis.crypto.getRandomValues !== "function") {
    throw localError("SDK_CRYPTO_UNAVAILABLE", "Web Crypto is unavailable.");
  }
  globalThis.crypto.getRandomValues(random);
  const timestamp = BigInt(Date.now());
  if (timestamp < 0n || timestamp > 0xffffffffffffn) throw new TypeError("clock cannot produce an operation ID");
  let randomness = 0n;
  for (const byte of random) randomness = (randomness << 8n) | BigInt(byte);
  let value = (timestamp << 80n) | randomness;
  let encoded = "";
  for (let index = 0; index < 26; index += 1) {
    encoded = (CROCKFORD[Number(value & 31n)] as string) + encoded;
    value >>= 5n;
  }
  return `${prefix}_${encoded}`;
}

function validateOptionalCredential(value: string | undefined, maximum: number, name: string): string | undefined {
  if (value === undefined) return undefined;
  if (value.length === 0 || textEncoder.encode(value).byteLength > maximum || /[^\x21-\x7e]/.test(value)) {
    throw new TypeError(`${name} is invalid`);
  }
  return value;
}

function validateApplicationKey(value: string): string {
  const publishable = new RegExp(`^rk_pub_v1_${ULID_PATTERN}_[A-Za-z0-9_-]{21}[AQgw]$`);
  const secret = new RegExp(`^rk_sec_v1_${ULID_PATTERN}\\.[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$`);
  if (!publishable.test(value) && !secret.test(value)) throw new TypeError("application key is not canonical");
  return value;
}

function boundedInteger(value: number, minimum: number, maximum: number, name: string): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) throw new TypeError(`${name} is outside limits`);
  return value;
}

function ensureI64(value: bigint, name: string): void {
  if (value < -(1n << 63n) || value > (1n << 63n) - 1n) throw new TypeError(`${name} is outside i64`);
}

function parseI64(value: unknown, timestamp: boolean): bigint {
  if (typeof value !== "string" || !/^(?:0|-?[1-9][0-9]*)$/.test(value) || value === "-0") throw protocolError();
  const parsed = BigInt(value);
  ensureI64(parsed, timestamp ? "timestamp" : "int64");
  return parsed;
}

function parseU64(value: unknown): bigint {
  if (typeof value !== "string" || !/^(?:0|[1-9][0-9]*)$/.test(value)) throw protocolError();
  const parsed = BigInt(value);
  if (parsed > (1n << 64n) - 1n) throw protocolError();
  return parsed;
}

function compareUtf8(left: string, right: string): number {
  const a = textEncoder.encode(left);
  const b = textEncoder.encode(right);
  const length = Math.min(a.length, b.length);
  for (let index = 0; index < length; index += 1) {
    const difference = (a[index] as number) - (b[index] as number);
    if (difference !== 0) return difference;
  }
  return a.length - b.length;
}

function isUnicodeScalarString(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) return false;
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      return false;
    }
  }
  return true;
}

function encodeBase64Url(bytes: Uint8Array): string {
  let output = "";
  for (let index = 0; index < bytes.length; index += 3) {
    const a = bytes[index] as number;
    const hasB = index + 1 < bytes.length;
    const hasC = index + 2 < bytes.length;
    const b = hasB ? bytes[index + 1] as number : 0;
    const c = hasC ? bytes[index + 2] as number : 0;
    output += BASE64URL[a >> 2] as string;
    output += BASE64URL[((a & 3) << 4) | (b >> 4)] as string;
    if (hasB) output += BASE64URL[((b & 15) << 2) | (c >> 6)] as string;
    if (hasC) output += BASE64URL[c & 63] as string;
  }
  return output;
}

function decodeBase64Url(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]*$/.test(value) || value.length % 4 === 1) throw protocolError();
  const output: number[] = [];
  let bits = 0;
  let count = 0;
  for (const character of value) {
    const index = BASE64URL.indexOf(character);
    if (index < 0) throw protocolError();
    bits = (bits << 6) | index;
    count += 6;
    if (count >= 8) {
      count -= 8;
      output.push((bits >> count) & 255);
    }
  }
  if (count > 0 && (bits & ((1 << count) - 1)) !== 0) throw protocolError();
  const decoded = Uint8Array.from(output);
  if (encodeBase64Url(decoded) !== value) throw protocolError();
  return decoded;
}

function abortLifecycle(parent: AbortSignal | undefined, timeoutMs: number): {
  signal: AbortSignal;
  timedOut: () => boolean;
  dispose: () => void;
} {
  const controller = new AbortController();
  let timeoutWon = false;
  const parentAbort = (): void => controller.abort(parent?.reason);
  if (parent?.aborted === true) parentAbort();
  else parent?.addEventListener("abort", parentAbort, { once: true });
  const timeout = setTimeout(() => { timeoutWon = true; controller.abort(); }, timeoutMs);
  return {
    signal: controller.signal,
    timedOut: () => timeoutWon,
    dispose: () => { clearTimeout(timeout); parent?.removeEventListener("abort", parentAbort); },
  };
}

function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (milliseconds === 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, milliseconds);
    const abort = (): void => { clearTimeout(timeout); reject(abortError(false)); };
    signal.addEventListener("abort", abort, { once: true });
  });
}

function abortError(timedOut: boolean): RunkuError {
  return localError(
    timedOut ? "SDK_TIMEOUT" : "SDK_ABORTED",
    timedOut ? "The client deadline elapsed." : "The request was aborted.",
  );
}

function protocolError(): RunkuError {
  return localError("SDK_RESPONSE_INVALID", "The server response is invalid.");
}

function localError(code: string, message: string, retryable = false): RunkuError {
  return new RunkuError({ code, message, retryable, status: 0, requestId: null });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
}
