/** Canonical timestamp in signed microseconds. */
export interface RunkuTimestamp {
  readonly value: bigint
  toString(): string
}

/** Canonical typed Runku identifier. */
export interface RunkuId {
  readonly value: string
  toString(): string
}

declare const documentTableBrand: unique symbol
declare const tableReferenceBrand: unique symbol
declare const indexReferenceBrand: unique symbol

/** Canonical Document ID statically associated with one logical schema table. */
export type DocumentId<TableName extends string> = RunkuId & {
  readonly [documentTableBrand]: TableName
}

/** Values accepted and returned by the declarative `runku-js-1` runtime. */
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
  | { readonly [key: string]: RunkuValue }

/** Platform capability names implemented by the safe runtime. */
export type ImplementedCapability =
  | "db:read"
  | "db:write"
  | "auth:read"
  | "function:query"
  | "function:mutation"
  | "function:action"
  | "network:https"
  | "scheduler:create"

export type QueryCapability = Extract<
  ImplementedCapability,
  "db:read" | "auth:read" | "function:query"
>
export type MutationCapability = Extract<
  ImplementedCapability,
  | "db:read"
  | "db:write"
  | "auth:read"
  | "function:query"
  | "function:mutation"
  | "scheduler:create"
>
export type ActionCapability = Extract<
  ImplementedCapability,
  | "auth:read"
  | "function:query"
  | "function:mutation"
  | "function:action"
  | "network:https"
  | "scheduler:create"
>

export interface InvocationMetadata {
  readonly projectId: string
  readonly environmentId: string
  readonly releaseId: string
  readonly requestId: string
  readonly invocationId: string
  readonly functionId: string
  readonly functionName: string
  readonly functionType: "query" | "mutation" | "action"
  readonly capabilities: readonly string[]
  readonly httpsEnabled: boolean
  readonly dataEnabled: boolean
  readonly dataWriteEnabled: boolean
  readonly schedulerEnabled: boolean
  readonly functionQueryEnabled: boolean
  readonly functionMutationEnabled: boolean
  readonly functionActionEnabled: boolean
  readonly authEnabled: boolean
}

export interface ApplicationContext {
  readonly clientId: string
  readonly credentialId: string
  readonly assurance: "declared" | "verified"
  readonly scopes: readonly string[]
  readonly configurationRevision: bigint
}

export interface PrincipalContext {
  readonly id: string
  readonly kind: "guest" | "user" | "service" | "system"
  readonly providerId: string
  readonly scopes: readonly string[]
  readonly authTime: RunkuTimestamp | null
  readonly expiresAt: RunkuTimestamp | null
  readonly mappingRevision: bigint
}

export interface AuthContext {
  readonly application: ApplicationContext | null
  readonly principal: PrincipalContext | null
}

export interface DataDocument<
  T extends RunkuValue = RunkuValue,
  TableName extends string = string,
> {
  readonly tableId: string
  readonly documentId: DocumentId<TableName>
  readonly revision: bigint
  readonly commitSequence: bigint
  readonly createdAt: RunkuTimestamp
  readonly updatedAt: RunkuTimestamp
  readonly value: T
}

export interface DataIndexEntry<TableName extends string = string> {
  readonly indexId: string
  readonly key: Uint8Array
  readonly tableId: string
  readonly documentId: DocumentId<TableName>
  readonly documentRevision: bigint
  readonly commitSequence: bigint
}

export interface DataRangeBound {
  readonly kind: "inclusive" | "exclusive"
  readonly key: Uint8Array
}

export interface QueryDatabase {
  get<Name extends string, T extends RunkuValue, I extends string>(
    table: TableReference<Name, T, I>,
    documentId: DocumentId<NoInfer<Name>>,
  ): Promise<DataDocument<T, Name> | null>
  documentId<Name extends string, T extends RunkuValue, I extends string>(
    table: TableReference<Name, T, I>,
    stableKey: string,
  ): DocumentId<Name>
  scan<Name extends string, I extends string, IndexName extends I>(
    index: IndexReference<Name, IndexName>,
    options: {
      readonly lower?: DataRangeBound | null
      readonly upper?: DataRangeBound | null
      readonly limit: number
    },
  ): Promise<readonly DataIndexEntry<Name>[]>
}

export interface MutationReadDatabase {
  get<Name extends string, T extends RunkuValue, I extends string>(
    table: TableReference<Name, T, I>,
    documentId: DocumentId<NoInfer<Name>>,
  ): Promise<DataDocument<T, Name> | null>
  documentId<Name extends string, T extends RunkuValue, I extends string>(
    table: TableReference<Name, T, I>,
    stableKey: string,
  ): DocumentId<Name>
}

export interface MutationWriteDatabase {
  insert<Name extends string, T extends RunkuValue, I extends string>(
    table: TableReference<Name, T, I>,
    documentId: DocumentId<NoInfer<Name>>,
    value: NoInfer<T>,
  ): Promise<void>
  replace<Name extends string, T extends RunkuValue, I extends string>(
    table: TableReference<Name, T, I>,
    documentId: DocumentId<NoInfer<Name>>,
    expectedRevision: bigint,
    value: NoInfer<T>,
  ): Promise<void>
  delete<Name extends string, T extends RunkuValue, I extends string>(
    table: TableReference<Name, T, I>,
    documentId: DocumentId<NoInfer<Name>>,
    expectedRevision: bigint,
  ): Promise<void>
}

export interface HttpsRequest {
  readonly method: "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE"
  readonly url: string
  readonly headers?: Readonly<Record<string, readonly string[]>>
  readonly body?: Uint8Array
  readonly idempotencyKey?: string
}

export interface HttpsResponse {
  readonly status: number
  readonly headers: Readonly<Record<string, readonly string[]>>
  readonly body: Uint8Array
}

export interface HttpsClient {
  request(input: HttpsRequest): Promise<HttpsResponse>
}

export interface ScheduleOptions {
  readonly idempotencyKey?: string
}

export interface Scheduler {
  runAfter(
    delayMicros: bigint,
    functionName: string,
    argumentsValue: RunkuValue,
    options?: ScheduleOptions,
  ): Promise<string>
  runAt(
    timestampMicros: bigint,
    functionName: string,
    argumentsValue: RunkuValue,
    options?: ScheduleOptions,
  ): Promise<string>
}

/** Bounded best-effort structured Function logger. */
export interface FunctionLogger {
  debug(message: string, fields?: Readonly<Record<string, RunkuValue>>): Promise<void>
  info(message: string, fields?: Readonly<Record<string, RunkuValue>>): Promise<void>
  warn(message: string, fields?: Readonly<Record<string, RunkuValue>>): Promise<void>
  error(message: string, fields?: Readonly<Record<string, RunkuValue>>): Promise<void>
}

interface BaseContext {
  readonly invocation: InvocationMetadata
  readonly cooperate: () => Promise<void>
  readonly log: FunctionLogger
}

type AuthFeature<C extends ImplementedCapability> = "auth:read" extends C
  ? { readonly auth: AuthContext }
  : Record<never, never>

type QueryDataFeature<C extends QueryCapability> = "db:read" extends C
  ? { readonly db: QueryDatabase }
  : Record<never, never>

type MutationDataFeature<C extends MutationCapability> =
  ("db:read" extends C ? MutationReadDatabase : Record<never, never>) &
    ("db:write" extends C ? MutationWriteDatabase : Record<never, never>) extends infer D
    ? [keyof D] extends [never]
      ? Record<never, never>
      : { readonly db: D }
    : never

type HttpsFeature<C extends ActionCapability> = "network:https" extends C
  ? { readonly https: HttpsClient }
  : Record<never, never>

type SchedulerFeature<C extends MutationCapability | ActionCapability> =
  "scheduler:create" extends C ? { readonly scheduler: Scheduler } : Record<never, never>

type RunQueryFeature<C extends ImplementedCapability> = "function:query" extends C
  ? {
      readonly runQuery: (functionName: string, argumentsValue: RunkuValue) => Promise<RunkuValue>
    }
  : Record<never, never>

type RunMutationFeature<C extends ImplementedCapability> = "function:mutation" extends C
  ? {
      readonly runMutation: (functionName: string, argumentsValue: RunkuValue) => Promise<RunkuValue>
    }
  : Record<never, never>

type RunActionFeature<C extends ImplementedCapability> = "function:action" extends C
  ? {
      readonly runAction: (functionName: string, argumentsValue: RunkuValue) => Promise<RunkuValue>
    }
  : Record<never, never>

export type QueryContext<C extends QueryCapability> = BaseContext &
  AuthFeature<C> &
  QueryDataFeature<C> &
  RunQueryFeature<C>

export type MutationContext<C extends MutationCapability> = BaseContext &
  AuthFeature<C> &
  MutationDataFeature<C> &
  SchedulerFeature<C> &
  RunQueryFeature<C> &
  RunMutationFeature<C>

export type ActionContext<C extends ActionCapability> = BaseContext &
  AuthFeature<C> &
  HttpsFeature<C> &
  SchedulerFeature<C> &
  RunQueryFeature<C> &
  RunMutationFeature<C> &
  RunActionFeature<C>

export type QueryHandler<C extends QueryCapability, A extends RunkuValue, R extends RunkuValue> = (
  context: QueryContext<C>,
  argumentsValue: A,
) => R | Promise<R>

export type MutationHandler<
  C extends MutationCapability,
  A extends RunkuValue,
  R extends RunkuValue,
> = (context: MutationContext<C>, argumentsValue: A) => R | Promise<R>

export type ActionHandler<C extends ActionCapability, A extends RunkuValue, R extends RunkuValue> = (
  context: ActionContext<C>,
  argumentsValue: A,
) => R | Promise<R>

/** Statically extractable value contract used by the Runku source compiler. */
export interface Validator<T extends RunkuValue = RunkuValue> {
  readonly __runkuValidator: true
  readonly __value?: T
}

export interface OptionalValidator<T extends RunkuValue = RunkuValue> extends Validator<T> {
  readonly __runkuOptional: true
}

/** Infers the canonical TypeScript value represented by one validator. */
export type Infer<V extends Validator> = V extends Validator<infer T> ? T : never
type ValidatorValue<V extends Validator> = Infer<V>
type ValidatorShape = Readonly<Record<string, Validator>>
type OptionalKeys<S extends ValidatorShape> = {
  [K in keyof S]-?: S[K] extends OptionalValidator ? K : never
}[keyof S]
type ObjectValue<S extends ValidatorShape> = {
  readonly [K in Exclude<keyof S, OptionalKeys<S>>]: ValidatorValue<S[K]>
} & {
  readonly [K in OptionalKeys<S>]?: ValidatorValue<S[K]>
}

export interface ObjectValidator<S extends ValidatorShape> extends Validator<ObjectValue<S>> {
  readonly __shape?: S
}

export interface BoundOptions {
  readonly minimum?: number
  readonly maximum?: number
}

export interface ByteBoundOptions {
  readonly minBytes?: number
  readonly maxBytes?: number
}

export interface ItemBoundOptions {
  readonly minItems?: number
  readonly maxItems?: number
}

/** Declarative validators understood statically by `runku build`. */
export const v = Object.freeze({
  any: (): Validator<RunkuValue> => validator(),
  null: (): Validator<null> => validator(),
  boolean: (): Validator<boolean> => validator(),
  int64: (_options: BoundOptions = {}): Validator<bigint> => validator(),
  float64: (_options: BoundOptions = {}): Validator<number> => validator(),
  string: (_options: ByteBoundOptions = {}): Validator<string> => validator(),
  bytes: (_options: ByteBoundOptions = {}): Validator<Uint8Array> => validator(),
  timestamp: (): Validator<RunkuTimestamp> => validator(),
  id: <const K extends string>(_kind?: K): Validator<RunkuId> => validator(),
  documentId: <const TableName extends string>(
    _table: TableName,
  ): Validator<DocumentId<TableName>> => validator(),
  array: <V extends Validator>(
    _items: V,
    _options: ItemBoundOptions = {},
  ): Validator<readonly ValidatorValue<V>[]> => validator(),
  object: <S extends ValidatorShape>(_fields: S): ObjectValidator<S> => validator(),
  pick: <S extends ValidatorShape, const K extends readonly (keyof S & string)[]>(
    _object: ObjectValidator<S>,
    _keys: K,
  ): ObjectValidator<Pick<S, K[number]>> => validator(),
  union: <const V extends readonly [Validator, Validator, ...Validator[]]>(
    ..._variants: V
  ): Validator<ValidatorValue<V[number]>> => validator(),
  optional: <V extends Validator>(_value: V): OptionalValidator<ValidatorValue<V>> =>
    Object.freeze({ __runkuValidator: true, __runkuOptional: true }) as OptionalValidator<
      ValidatorValue<V>
    >,
})

function validator<T extends RunkuValue>(): Validator<T> {
  return Object.freeze({ __runkuValidator: true }) as Validator<T>
}

export type FunctionAuth = "none" | "optional" | "guest" | "user" | "service"
export type FunctionVisibility = "public" | "internal"

export interface FunctionDefinition<
  K extends "query" | "mutation" | "action",
  A extends RunkuValue,
  R extends RunkuValue,
> {
  readonly __runkuFunction: K
  readonly __arguments?: A
  readonly __result?: R
}

type CommonDefinition<A extends Validator, R extends Validator> = {
  readonly auth: FunctionAuth
  readonly visibility: FunctionVisibility
  readonly args: A
  readonly returns: R
}

export function query<
  const C extends readonly QueryCapability[],
  A extends Validator,
  R extends Validator,
>(
  definition: CommonDefinition<A, R> & {
    readonly capabilities: C
    readonly handler: QueryHandler<C[number], ValidatorValue<A>, ValidatorValue<R>>
  },
): FunctionDefinition<"query", ValidatorValue<A>, ValidatorValue<R>> {
  return functionDefinition("query", definition)
}

export function mutation<
  const C extends readonly MutationCapability[],
  A extends Validator,
  R extends Validator,
>(
  definition: CommonDefinition<A, R> & {
    readonly capabilities: C
    readonly handler: MutationHandler<C[number], ValidatorValue<A>, ValidatorValue<R>>
  },
): FunctionDefinition<"mutation", ValidatorValue<A>, ValidatorValue<R>> {
  return functionDefinition("mutation", definition)
}

export function action<
  const C extends readonly ActionCapability[],
  A extends Validator,
  R extends Validator,
>(
  definition: CommonDefinition<A, R> & {
    readonly capabilities: C
    readonly handler: ActionHandler<C[number], ValidatorValue<A>, ValidatorValue<R>>
  },
): FunctionDefinition<"action", ValidatorValue<A>, ValidatorValue<R>> {
  return functionDefinition("action", definition)
}

export interface CronDefinition {
  readonly __runkuCron: true
}

/** Statically compiled UTC schedule. Its export name becomes the stable Cron name. */
export function cron(definition: {
  readonly schedule: string
  readonly function: string
  readonly args: RunkuValue
}): CronDefinition {
  return Object.freeze({ __runkuCron: true, ...definition })
}

/** Explicit constructors for non-JSON values embedded in Cron arguments. */
export const value = Object.freeze({
  int64: (input: bigint): bigint => input,
  float64: (input: number): number => input,
  timestamp: (micros: bigint): RunkuTimestamp => Runku.timestamp(micros),
  id: (input: string): RunkuId => Runku.id(input),
  bytes: (input: readonly number[]): Uint8Array => new Uint8Array(input),
})

function functionDefinition<K extends "query" | "mutation" | "action", A extends RunkuValue, R extends RunkuValue>(
  kind: K,
  definition: object,
): FunctionDefinition<K, A, R> {
  return Object.freeze({ __runkuFunction: kind, ...definition }) as FunctionDefinition<K, A, R>
}

export interface IndexDeclaration {
  readonly name: string
  readonly fields: readonly string[]
}

export interface TableDefinition<
  T extends RunkuValue = RunkuValue,
  I extends string = never,
> {
  readonly __runkuTable: true
  readonly document: Validator<T>
  readonly indexes: readonly IndexDeclaration[]
  index<const N extends string>(
    name: N,
    fields: readonly (keyof T & string)[],
  ): TableDefinition<T, I | N>
}

export function defineTable<V extends Validator>(document: V): TableDefinition<ValidatorValue<V>, never> {
  const indexes: IndexDeclaration[] = []
  const table = {
    __runkuTable: true,
    document: document as unknown as Validator<ValidatorValue<V>>,
    indexes,
    index<N extends string>(
      name: N,
      fields: readonly (keyof ValidatorValue<V> & string)[],
    ): TableDefinition<ValidatorValue<V>, N> {
      indexes.push(Object.freeze({ name, fields: Object.freeze([...fields]) }))
      return table as TableDefinition<ValidatorValue<V>, N>
    },
  }
  return table as TableDefinition<ValidatorValue<V>, never>
}

type TableMap = Readonly<Record<string, TableDefinition<RunkuValue, string>>>
export type TableReference<
  Name extends string,
  T extends RunkuValue,
  IndexName extends string = never,
> = string & {
  readonly [tableReferenceBrand]: {
    readonly name: Name
    readonly document: T
    readonly indexes: IndexName
  }
}

export type IndexReference<TableName extends string, IndexName extends string> = string & {
  readonly [indexReferenceBrand]: {
    readonly table: TableName
    readonly index: IndexName
  }
}

type TableValue<T> = T extends TableDefinition<infer V, string> ? V : never
type TableReferences<T extends TableMap> = {
  readonly [K in keyof T & string]: TableReference<K, TableValue<T[K]>, IndexNames<T[K]>>
}
type IndexNames<T> = T extends TableDefinition<RunkuValue, infer I> ? I : never
type IndexReferences<T extends TableMap> = {
  readonly [K in keyof T & string]: {
    readonly [I in IndexNames<T[K]>]: IndexReference<K, I>
  }
}

export interface SchemaDefinition<T extends TableMap = TableMap> {
  readonly __runkuSchema: true
  readonly definitions: T
  readonly tables: TableReferences<T>
  readonly indexes: IndexReferences<T>
}

export function defineSchema<const T extends TableMap>(definitions: T): SchemaDefinition<T> {
  const tables = Object.fromEntries(Object.keys(definitions).map((name) => [name, name]))
  const indexes = Object.fromEntries(
    Object.entries(definitions).map(([name, table]) => [
      name,
      Object.fromEntries(table.indexes.map((index) => [index.name, `${name}.${index.name}`])),
    ]),
  )
  return Object.freeze({ __runkuSchema: true, definitions, tables, indexes }) as SchemaDefinition<T>
}

export interface RunkuGlobals {
  timestamp(micros: bigint | number | string): RunkuTimestamp
  id(value: string): RunkuId
}

declare global {
  const Runku: RunkuGlobals
}
