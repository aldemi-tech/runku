import {
  op_runku_cooperate,
  op_runku_data_get,
  op_runku_data_document_id,
  op_runku_data_insert,
  op_runku_data_replace,
  op_runku_data_delete,
  op_runku_data_scan,
  op_runku_https,
  op_runku_function_query,
  op_runku_function_mutation,
  op_runku_function_action,
  op_runku_log,
  op_runku_schedule_after,
  op_runku_schedule_at,
  op_runku_storage_create_upload,
  op_runku_storage_store,
  op_runku_storage_metadata,
  op_runku_storage_create_download,
  op_runku_storage_get,
  op_runku_storage_delete,
} from "ext:core/ops";

const ObjectCtor = Object;
const ObjectCreate = Object.create;
const ObjectDefineProperty = Object.defineProperty;
const ObjectFreeze = Object.freeze;
const ObjectGetPrototypeOf = Object.getPrototypeOf;
const ObjectKeys = Object.keys;
const ArrayCtor = Array;
const ArrayIsArray = Array.isArray;
const ArrayFrom = Array.from;
const ArrayMap = Array.prototype.map;
const ArraySort = Array.prototype.sort;
const Uint8ArrayCtor = Uint8Array;
const Uint8ArrayFrom = Uint8Array.from;
const BigIntCtor = BigInt;
const BigIntToString = BigInt.prototype.toString;
const NumberIsFinite = Number.isFinite;
const StringCtor = String;
const WeakSetCtor = WeakSet;
const WeakSetAdd = WeakSet.prototype.add;
const WeakSetDelete = WeakSet.prototype.delete;
const WeakSetHas = WeakSet.prototype.has;
const ReflectApply = Reflect.apply;
const brand = Symbol("runku.platform-js-1.value");
const timestampBrand = "timestamp";
const typedIdBrand = "typed_id";
const maxDepth = 64;
const maxNodes = 100_000;

function freezeHeaders(headers) {
  const output = ObjectCreate(null);
  const keys = ObjectKeys(headers);
  ReflectApply(ArraySort, keys, []);
  for (const key of keys) {
    const values = headers[key];
    if (!ArrayIsArray(values)) throw new TypeError("HTTPS header values must be arrays");
    ObjectDefineProperty(output, key, {
      value: ObjectFreeze(ReflectApply(ArrayMap, values, [(value) => StringCtor(value)])),
      enumerable: true,
    });
  }
  return ObjectFreeze(output);
}

function freezeAuth(auth) {
  const application = auth.application === null ? null : ObjectFreeze({
    clientId: auth.application.clientId,
    credentialId: auth.application.credentialId,
    assurance: auth.application.assurance,
    scopes: ObjectFreeze(ReflectApply(ArrayFrom, ArrayCtor, [auth.application.scopes])),
    configurationRevision: BigIntCtor(auth.application.configurationRevision),
  });
  const principal = auth.principal === null ? null : ObjectFreeze({
    id: auth.principal.id,
    kind: auth.principal.kind,
    providerId: auth.principal.providerId,
    scopes: ObjectFreeze(ReflectApply(ArrayFrom, ArrayCtor, [auth.principal.scopes])),
    authTime: auth.principal.authTime === null
      ? null
      : branded(timestampBrand, BigIntCtor(auth.principal.authTime)),
    expiresAt: auth.principal.expiresAt === null
      ? null
      : branded(timestampBrand, BigIntCtor(auth.principal.expiresAt)),
    mappingRevision: BigIntCtor(auth.principal.mappingRevision),
  });
  return ObjectFreeze({ application, principal });
}

async function httpsRequest(input) {
  if (input === null || typeof input !== "object" || ArrayIsArray(input)) {
    throw new TypeError("HTTPS request must be an object");
  }
  const body = input.body === undefined
    ? []
    : ReflectApply(ArrayFrom, ArrayCtor, [input.body]);
  const wire = {
    method: input.method,
    url: input.url,
    headers: input.headers === undefined ? ObjectCreate(null) : input.headers,
    body,
    idempotencyKey: input.idempotencyKey,
  };
  const response = await op_runku_https(wire);
  return ObjectFreeze({
    status: response.status,
    headers: freezeHeaders(response.headers),
    body: ReflectApply(Uint8ArrayFrom, Uint8ArrayCtor, [response.body]),
  });
}

function storageOptions(input, name) {
  if (input === null || typeof input !== "object" || ArrayIsArray(input)) {
    throw new TypeError(`${name} options must be an object`);
  }
  return input;
}

async function storageCreateUpload(input) {
  const options = storageOptions(input, "storage upload");
  return ObjectFreeze(await op_runku_storage_create_upload({
    maxBytes: options.maxBytes,
    contentType: options.contentType,
    sha256: options.sha256,
  }));
}

async function storageStore(bytes, input = {}) {
  const options = storageOptions(input, "storage store");
  return ObjectFreeze(await op_runku_storage_store({
    bytes: ReflectApply(ArrayFrom, ArrayCtor, [bytes]),
    contentType: options.contentType,
    sha256: options.sha256,
  }));
}

async function storageMetadata(fileId) {
  return ObjectFreeze(await op_runku_storage_metadata(StringCtor(fileId)));
}

async function storageCreateDownload(fileId, input) {
  const options = storageOptions(input, "storage download");
  const result = await op_runku_storage_create_download({
    fileId: StringCtor(fileId),
    expiresInMicros: StringCtor(options.expiresInMicros),
  });
  result.metadata = ObjectFreeze(result.metadata);
  return ObjectFreeze(result);
}

async function storageGet(fileId) {
  const result = await op_runku_storage_get(StringCtor(fileId));
  return ObjectFreeze({
    metadata: ObjectFreeze(result.metadata),
    bytes: ReflectApply(Uint8ArrayFrom, Uint8ArrayCtor, [result.bytes]),
  });
}

async function storageDelete(fileId) {
  await op_runku_storage_delete(StringCtor(fileId));
}

function decodeDocument(document) {
  if (document === null) return null;
  return ObjectFreeze({
    tableId: document.tableId,
    documentId: branded(typedIdBrand, document.documentId),
    revision: BigIntCtor(document.revision),
    commitSequence: BigIntCtor(document.commitSequence),
    createdAt: branded(timestampBrand, BigIntCtor(document.createdAt)),
    updatedAt: branded(timestampBrand, BigIntCtor(document.updatedAt)),
    value: decode(document.value),
  });
}

async function dataGet(tableId, documentId) {
  const document = await op_runku_data_get({
    tableId: StringCtor(tableId),
    documentId: StringCtor(documentId),
  });
  return decodeDocument(document);
}

function dataDocumentId(tableId, stableKey) {
  const value = op_runku_data_document_id({
    tableId: StringCtor(tableId),
    stableKey: StringCtor(stableKey),
  });
  return branded(typedIdBrand, value);
}

function encodeDataBound(bound) {
  if (bound === undefined || bound === null) return null;
  if (typeof bound !== "object" || ArrayIsArray(bound)) {
    throw new TypeError("data range bound must be an object");
  }
  return {
    kind: bound.kind,
    key: ReflectApply(ArrayFrom, ArrayCtor, [bound.key]),
  };
}

async function dataScan(indexId, options) {
  if (options === null || typeof options !== "object" || ArrayIsArray(options)) {
    throw new TypeError("data scan options must be an object");
  }
  const entries = await op_runku_data_scan({
    indexId: StringCtor(indexId),
    lower: encodeDataBound(options.lower),
    upper: encodeDataBound(options.upper),
    limit: options.limit,
  });
  const output = ReflectApply(ArrayMap, entries, [(entry) => ObjectFreeze({
    indexId: entry.indexId,
    key: ReflectApply(Uint8ArrayFrom, Uint8ArrayCtor, [entry.key]),
    tableId: entry.tableId,
    documentId: branded(typedIdBrand, entry.documentId),
    documentRevision: BigIntCtor(entry.documentRevision),
    commitSequence: BigIntCtor(entry.commitSequence),
  })]);
  return ObjectFreeze(output);
}

async function dataInsert(tableId, documentId, value) {
  await op_runku_data_insert({
    tableId: StringCtor(tableId),
    documentId: StringCtor(documentId),
    value: encode(value),
  });
}

async function dataReplace(tableId, documentId, expectedRevision, value) {
  await op_runku_data_replace({
    tableId: StringCtor(tableId),
    documentId: StringCtor(documentId),
    expectedRevision: StringCtor(expectedRevision),
    value: encode(value),
  });
}

async function dataDelete(tableId, documentId, expectedRevision) {
  await op_runku_data_delete({
    tableId: StringCtor(tableId),
    documentId: StringCtor(documentId),
    expectedRevision: StringCtor(expectedRevision),
  });
}

function scheduleOptions(options) {
  if (options === undefined) return undefined;
  if (options === null || typeof options !== "object" || ArrayIsArray(options)) {
    throw new TypeError("schedule options must be an object");
  }
  return options.idempotencyKey;
}

async function scheduleAfter(delayMicros, functionName, argumentsValue, options) {
  return await op_runku_schedule_after({
    function: StringCtor(functionName),
    arguments: encode(argumentsValue),
    timeMicros: StringCtor(delayMicros),
    idempotencyKey: scheduleOptions(options),
  });
}

async function scheduleAt(timestampMicros, functionName, argumentsValue, options) {
  return await op_runku_schedule_at({
    function: StringCtor(functionName),
    arguments: encode(argumentsValue),
    timeMicros: StringCtor(timestampMicros),
    idempotencyKey: scheduleOptions(options),
  });
}

async function callFunction(op, functionName, argumentsValue) {
  const result = await op({
    function: StringCtor(functionName),
    arguments: encode(argumentsValue),
  });
  return decode(result);
}

async function runQuery(functionName, argumentsValue) {
  return await callFunction(op_runku_function_query, functionName, argumentsValue);
}

async function runMutation(functionName, argumentsValue) {
  return await callFunction(op_runku_function_mutation, functionName, argumentsValue);
}

async function runAction(functionName, argumentsValue) {
  return await callFunction(op_runku_function_action, functionName, argumentsValue);
}

async function functionLog(level, message, fields) {
  await op_runku_log({
    level,
    message: StringCtor(message),
    fields: fields === undefined ? null : encode(fields),
  });
}

function branded(kind, value) {
  const result = ObjectCreate(null);
  ObjectDefineProperty(result, brand, { value: kind });
  ObjectDefineProperty(result, "value", { value, enumerable: true });
  ObjectDefineProperty(result, "toString", {
    value() { return StringCtor(this.value); },
  });
  return ObjectFreeze(result);
}

function decode(wire, depth = 0, state = { nodes: 0 }) {
  if (depth > maxDepth || ++state.nodes > maxNodes || wire === null || typeof wire !== "object") {
    throw new TypeError("invalid Runku input value");
  }
  switch (wire.type) {
    case "null": return null;
    case "boolean": return wire.value;
    case "int64": return BigIntCtor(wire.value);
    case "float64": return wire.value;
    case "string": return wire.value;
    case "bytes": return ReflectApply(Uint8ArrayFrom, Uint8ArrayCtor, [wire.value]);
    case "timestamp": return branded(timestampBrand, BigIntCtor(wire.value));
    case "typed_id": return branded(typedIdBrand, wire.value);
    case "array":
      return ReflectApply(ArrayMap, wire.value, [(item) => decode(item, depth + 1, state)]);
    case "object": {
      const result = ObjectCreate(null);
      const keys = ObjectKeys(wire.value);
      ReflectApply(ArraySort, keys, []);
      for (const key of keys) {
        ObjectDefineProperty(result, key, {
          value: decode(wire.value[key], depth + 1, state),
          enumerable: true,
          configurable: true,
          writable: true,
        });
      }
      return result;
    }
    default: throw new TypeError("unsupported Runku input value");
  }
}

function encode(value, depth = 0, state = { nodes: 0, seen: new WeakSetCtor() }) {
  if (depth > maxDepth || ++state.nodes > maxNodes) {
    throw new TypeError("Runku result exceeds structural limits");
  }
  if (value === null) return { type: "null" };
  switch (typeof value) {
    case "boolean": return { type: "boolean", value };
    case "bigint": return { type: "int64", value: ReflectApply(BigIntToString, value, []) };
    case "number":
      if (!NumberIsFinite(value)) throw new TypeError("Runku number must be finite");
      return { type: "float64", value };
    case "string": return { type: "string", value };
    case "undefined":
    case "function":
    case "symbol":
      throw new TypeError("unsupported Runku result value");
    default: break;
  }
  if (value[brand] === timestampBrand) {
    return { type: "timestamp", value: ReflectApply(BigIntToString, value.value, []) };
  }
  if (value[brand] === typedIdBrand) {
    return { type: "typed_id", value: value.value };
  }
  if (value instanceof Uint8ArrayCtor) {
    return { type: "bytes", value: ReflectApply(ArrayFrom, ArrayCtor, [value]) };
  }
  if (ReflectApply(WeakSetHas, state.seen, [value])) throw new TypeError("cyclic Runku result");
  ReflectApply(WeakSetAdd, state.seen, [value]);
  if (ArrayIsArray(value)) {
    const result = {
      type: "array",
      value: ReflectApply(ArrayMap, value, [(item) => encode(item, depth + 1, state)]),
    };
    ReflectApply(WeakSetDelete, state.seen, [value]);
    return result;
  }
  const prototype = ObjectGetPrototypeOf(value);
  if (prototype !== null && prototype !== ObjectCtor.prototype) {
    throw new TypeError("custom object prototypes are not supported");
  }
  const encoded = ObjectCreate(null);
  const keys = ObjectKeys(value);
  ReflectApply(ArraySort, keys, []);
  for (const key of keys) {
    ObjectDefineProperty(encoded, key, {
      value: encode(value[key], depth + 1, state),
      enumerable: true,
      configurable: true,
      writable: true,
    });
  }
  ReflectApply(WeakSetDelete, state.seen, [value]);
  return { type: "object", value: encoded };
}

const Runku = ObjectFreeze({
  timestamp(micros) { return branded(timestampBrand, BigIntCtor(micros)); },
  id(value) { return branded(typedIdBrand, StringCtor(value)); },
});
ObjectDefineProperty(globalThis, "Runku", {
  value: Runku,
  configurable: false,
  enumerable: true,
  writable: false,
});

for (const denied of ["Deno", "WebAssembly", "SharedArrayBuffer", "Atomics"]) {
  ObjectDefineProperty(globalThis, denied, {
    value: undefined,
    configurable: false,
    enumerable: false,
    writable: false,
  });
}

ObjectDefineProperty(globalThis, "__runkuPlatformInvoke", {
  value: async function invoke(handler, wireArguments, metadata) {
    if (typeof handler !== "function") throw new TypeError("default export must be a function");
    const auth = metadata.auth;
    delete metadata.auth;
    if (ArrayIsArray(metadata.capabilities)) ObjectFreeze(metadata.capabilities);
    const context = {
      invocation: ObjectFreeze(metadata),
      cooperate: ObjectFreeze(() => op_runku_cooperate()),
      log: ObjectFreeze({
        debug: ObjectFreeze((message, fields) => functionLog("debug", message, fields)),
        info: ObjectFreeze((message, fields) => functionLog("info", message, fields)),
        warn: ObjectFreeze((message, fields) => functionLog("warn", message, fields)),
        error: ObjectFreeze((message, fields) => functionLog("error", message, fields)),
      }),
    };
    if (metadata.authEnabled === true) {
      if (auth === null) throw new TypeError("authorized identity is required");
      context.auth = freezeAuth(auth);
    }
    if (metadata.httpsEnabled === true) {
      context.https = ObjectFreeze({ request: ObjectFreeze(httpsRequest) });
    }
    if (metadata.dataEnabled === true) {
      const database = {
        get: ObjectFreeze(dataGet),
        documentId: ObjectFreeze(dataDocumentId),
      };
      if (metadata.functionType === "query") database.scan = ObjectFreeze(dataScan);
      context.db = ObjectFreeze(database);
    }
    if (metadata.dataWriteEnabled === true) {
      const database = context.db === undefined ? {} : {
        get: context.db.get,
        documentId: context.db.documentId,
      };
      if (context.db !== undefined && context.db.scan !== undefined) {
        database.scan = context.db.scan;
      }
      database.insert = ObjectFreeze(dataInsert);
      database.replace = ObjectFreeze(dataReplace);
      database.delete = ObjectFreeze(dataDelete);
      context.db = ObjectFreeze(database);
    }
    if (metadata.schedulerEnabled === true) {
      context.scheduler = ObjectFreeze({
        runAfter: ObjectFreeze(scheduleAfter),
        runAt: ObjectFreeze(scheduleAt),
      });
    }
    if (metadata.storageReadEnabled === true || metadata.storageWriteEnabled === true) {
      const storage = {};
      if (metadata.storageReadEnabled === true) {
        storage.getMetadata = ObjectFreeze(storageMetadata);
        storage.createDownload = ObjectFreeze(storageCreateDownload);
        storage.get = ObjectFreeze(storageGet);
      }
      if (metadata.storageWriteEnabled === true) {
        storage.createUpload = ObjectFreeze(storageCreateUpload);
        storage.store = ObjectFreeze(storageStore);
        storage.delete = ObjectFreeze(storageDelete);
      }
      context.storage = ObjectFreeze(storage);
    }
    if (metadata.functionQueryEnabled === true) {
      context.runQuery = ObjectFreeze(runQuery);
    }
    if (metadata.functionMutationEnabled === true) {
      context.runMutation = ObjectFreeze(runMutation);
    }
    if (metadata.functionActionEnabled === true) {
      context.runAction = ObjectFreeze(runAction);
    }
    const ctx = ObjectFreeze(context);
    return await ReflectApply(handler, undefined, [ctx, decode(wireArguments)]);
  },
  configurable: true,
  enumerable: false,
  writable: false,
});

ObjectDefineProperty(globalThis, "__runkuPlatformEncode", {
  value: encode,
  configurable: true,
  enumerable: false,
  writable: false,
});
