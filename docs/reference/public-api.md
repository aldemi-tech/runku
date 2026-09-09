# Public HTTP API without an SDK

Use the public HTTP API when the application is written in a language without a Runku client, when
integrating an HTTP-only system, or when verifying the exact wire contract with `curl`. This API
invokes application Functions and transfers Function-authorized files. It does not administer the
Runku installation.

The caller is responsible for canonical value encoding, response validation, timeouts, Mutation
operation identity, and retry safety. TypeScript applications should normally use the
[TypeScript client](typescript-client.md).

## Base URL and endpoints

The base URL is the Product/application origin exposed by Self-Hosted Runku, for example
`https://api.example.com`.

| Method and path | Purpose | Authentication |
|---|---|---|
| `POST /v1/query` | invoke one public Query | Application Key; functional bearer when required |
| `POST /v1/query/follow` | stream one Query's authoritative state as NDJSON | same as Query |
| `POST /v1/mutation` | invoke one public Mutation | same, plus operation ID in body |
| `POST /v1/action` | invoke one public Action | Application Key; functional bearer when required |
| `GET /v1/realtime` | open Realtime WebSocket | credentials in protocol authentication message |
| `PUT /v1/files/uploads/{uploadId}` | consume one upload grant | upload grant bearer only |
| `GET /v1/files/downloads/{fileId}` | consume one download grant | download grant bearer only |
| `HEAD /v1/files/downloads/{fileId}` | inspect authorized file headers | download grant bearer only |
| `GET /healthz` | process liveness | deployment policy |
| `GET /readyz` | admission readiness | deployment policy |

Unknown routes, methods, versions, envelope fields, and value fields fail closed.

## Credentials

A normal Function request sends:

```http
Content-Type: application/json
Accept: application/json
X-Runku-Key: rk_pub_v1_...
Authorization: Bearer <functional-token>
```

`X-Runku-Key` is the Application Client credential. `Authorization` is optional at the transport
level but required when the Function declares `auth: "guest"`, `"user"`, or `"service"`. They are
independent authorization axes. Neither one is a Platform Management credential.

Do not send `Authorization` for an `auth: "none"` Function with the expectation that it will be
used: the functional principal is deliberately discarded. For `auth: "optional"`, send no bearer
or a valid bearer; an invalid token never degrades to anonymous.

## Choose an exact target

Every request body contains one target:

| Target | Meaning |
|---|---|
| `environment:default` | current converged compatible Environment serving policy |
| `channel:<name>` | current explicit Channel binding/policy |
| `release:rel_*` | exact immutable Release |
| `workspace:<name>` | current immutable development revision for an authorized Workspace |

Target resolution pins exact code for the request. There is no missing-target or `latest`
fallback. Prefer a Channel in user-facing production clients; use an exact Release for controlled
verification and a Workspace only in development.

## Query request

```sh
curl --fail-with-body \
  -X POST "https://api.example.com/v1/query" \
  -H "accept: application/json" \
  -H "content-type: application/json" \
  -H "x-runku-key: rk_pub_v1_REPLACE_ME" \
  -H "authorization: Bearer REPLACE_ME" \
  --data-binary @- <<'JSON'
{
  "version": 1,
  "target": "channel:stable",
  "function": "notes.get",
  "arguments": {
    "type": "object",
    "value": [
      {
        "key": "id",
        "value": {
          "type": "typed_id",
          "value": "doc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
        }
      }
    ]
  }
}
JSON
```

The Query envelope has exactly `version`, `target`, `function`, and `arguments`.

## Mutation request and operation identity

A Mutation has one additional required field, `operationId`:

```sh
curl --fail-with-body \
  -X POST "https://api.example.com/v1/mutation" \
  -H "accept: application/json" \
  -H "content-type: application/json" \
  -H "x-runku-key: rk_pub_v1_REPLACE_ME" \
  -H "authorization: Bearer REPLACE_ME" \
  --data-binary @- <<'JSON'
{
  "version": 1,
  "target": "channel:stable",
  "function": "notes.create",
  "arguments": {
    "type": "object",
    "value": [
      {"key": "body", "value": {"type": "string", "value": "Exact content"}},
      {"key": "priority", "value": {"type": "int64", "value": "2"}},
      {"key": "title", "value": {"type": "string", "value": "Runbook"}}
    ]
  },
  "operationId": "opn_01ARZ3NDEKTSV4RRFFQ69G5FAY"
}
JSON
```

Generate a fresh canonical `opn_*` ULID for new intent and persist it with the caller's job/request.
If the response is lost, repeat the **exact** request with the same operation ID. A committed replay
returns the original result. Never use a new operation ID to guess whether an uncertain commit
happened, and never reuse an ID with changed target, Function, scope, caller, or arguments.

## Action request

```sh
curl --fail-with-body \
  -X POST "https://api.example.com/v1/action" \
  -H "accept: application/json" \
  -H "content-type: application/json" \
  -H "x-runku-key: rk_sec_v1_REPLACE_ME" \
  -H "authorization: Bearer REPLACE_ME" \
  --data-binary @- <<'JSON'
{
  "version": 1,
  "target": "release:rel_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "function": "exports.start",
  "arguments": {
    "type": "object",
    "value": [
      {"key": "format", "value": {"type": "string", "value": "csv"}}
    ]
  }
}
JSON
```

The Action envelope is identical to Query. It has no protocol operation ID because an Action may
perform non-transactional effects. Do not automatically repeat an Action after timeout, HTTP 5xx,
connection loss, or an unreadable response. Reconcile each downstream effect through the
application's own stable idempotency key.

## Canonical value encoding

Function arguments and results are lossless tagged JSON values. The wrapper is mandatory even for
ordinary JSON strings/booleans.

| Application value | Wire JSON |
|---|---|
| null | `{"type":"null"}` |
| boolean | `{"type":"boolean","value":true}` |
| int64 | `{"type":"int64","value":"-42"}` |
| float64 | `{"type":"float64","value":"3ff8000000000000"}` |
| string | `{"type":"string","value":"Runku"}` |
| bytes `00 ff` | `{"type":"bytes","value":"AP8"}` |
| timestamp | `{"type":"timestamp","value":"1700000000123456"}` |
| typed/document ID | `{"type":"typed_id","value":"doc_..."}` |
| array | `{"type":"array","value":[...wire values...]}` |
| object | `{"type":"object","value":[{"key":"a","value":...}]}` |

### Integer and timestamp

Encode a signed 64-bit integer as its shortest base-10 string. No leading plus, no leading zero
except `"0"`, and no value outside `-9223372036854775808..9223372036854775807`.

Timestamp uses the same signed decimal rule and represents Unix epoch microseconds, not
milliseconds or an ISO string.

### Float

Encode the exact IEEE-754 binary64 bits as 16 lowercase hexadecimal characters. For example,
`1.5` is `3ff8000000000000`. NaN, positive/negative infinity, and non-canonical negative zero are
rejected. Do not serialize the decimal spelling of the float.

### Bytes

Use URL-safe Base64 without `=` padding. `+` and `/` from standard Base64 are not accepted, and the
server rejects alternate encodings of the same bytes.

### Typed IDs

Pass the complete canonical Runku ID, including its kind prefix and underscore. A document ID is
encoded with wire type `typed_id`; the Function validator enforces the expected table.

### Objects

An object is an array of `{key,value}` entries, strictly sorted by the unsigned UTF-8 bytes of
`key`. Duplicate or unsorted keys are invalid. This preserves a single canonical representation
across programming languages. Function `v.object()` validation then rejects undeclared fields.

### Structural limits

The public envelope is at most 2 MiB, value nesting depth is at most 64, and an individual array or
object contains at most 10,000 entries. The Function/schema validator can impose smaller limits.
All envelope/value objects reject unknown fields.

## Successful response

All Function kinds return HTTP 200 with the exact common fields:

```json
{
  "version": 1,
  "status": "ok",
  "requestId": "req_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "releaseId": "rel_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "result": {"type": "string", "value": "ready"},
  "metadata": {"kind": "query", "snapshotSequence": "42"}
}
```

`result` is one canonical tagged value. `metadata` is kind-specific:

```json
{"kind":"query","snapshotSequence":"42"}
```

`snapshotSequence` is `null` when the Query made no data read.

```json
{"kind":"mutation","commitSequence":"43","replayed":false,"attempts":1}
```

`commitSequence` is `null` for a no-write Mutation. `attempts` is positive and includes internal
optimistic-concurrency reruns.

```json
{"kind":"action","schedulesCreated":"0"}
```

Sequence/count fields are canonical unsigned decimal strings so clients do not lose precision.
Always record `requestId` and `releaseId` in bounded diagnostic context.

## Error response

Failures use the actual HTTP status and a sanitized envelope:

```json
{
  "version": 1,
  "status": "error",
  "requestId": "req_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "error": {
    "code": "AUTH_POLICY_DENIED",
    "message": "The request is not permitted.",
    "retryable": false
  }
}
```

| HTTP status | Class | Typical caller action |
|---:|---|---|
| 400 | invalid request | fix encoding, target, Function name, or arguments |
| 401 | unauthenticated | obtain/refresh the correct credential |
| 403 | forbidden | do not retry without an authorization change |
| 404 | not found | verify target, Function visibility, and resource |
| 409 | conflict | reconcile operation/current state |
| 410 | retired Release | select a supported target |
| 413 | limit exceeded | reduce request/work; identical retry will fail |
| 429 | rate limited | honor bounded backoff/application policy |
| 503 | busy/unavailable | retry Query/Mutation only when `retryable` is true |
| 504 | deadline | Query may retry; Mutation keeps operation ID; Action is uncertain |
| 500 | internal | record request ID; follow `retryable` and escalate persistent errors |

Branch on `code` and `retryable`, not `message`. The message is deliberately generic. A response
includes `X-Runku-Request-Id`; after invocation allocation it may also include
`X-Runku-Invocation-Id`. Absence of an invocation ID does not prove an Action had no effect when
the response itself was lost.

## Retry policy for a custom client

| Call | Safe automatic policy |
|---|---|
| Query | bounded retry only for transport failure or error with `retryable: true` |
| Mutation | same, always preserving exact `operationId` and body |
| Action | no automatic retry |
| upload grant PUT | no automatic retry |
| download GET | application-controlled retry while grant remains valid |

Use one total deadline across attempts. Bound response bytes before JSON decoding. Validate
`Content-Type`, status, exact response fields, canonical values, IDs, metadata kind, and correlation
headers. Reject success with a mismatched metadata kind.

## File upload without an SDK

An Action with `storage:write` returns a one-shot grant containing `path`, `token`, expiry, upload
ID, and maximum bytes. Use its exact path on the same Product origin:

```sh
curl --fail-with-body \
  -X PUT "https://api.example.com/v1/files/uploads/upl_REPLACE_ME" \
  -H "authorization: Bearer REPLACE_WITH_GRANT_TOKEN" \
  -H "accept: application/json" \
  -H "content-type: image/png" \
  --data-binary @avatar.png
```

Do not send the Application Key or user bearer in place of the grant. The optional content type
must equal the value declared when the Action created the grant. A successful upload returns HTTP
201 and immutable file metadata. The token is consumed once; an interrupted request has uncertain
outcome and must be reconciled at application level.

## File download without an SDK

```sh
curl --fail-with-body \
  "https://api.example.com/v1/files/downloads/fil_REPLACE_ME" \
  -H "authorization: Bearer REPLACE_WITH_GRANT_TOKEN" \
  -H "range: bytes=0-1023" \
  --output download.bin
```

The grant is short-lived. GET returns 200 or 206; HEAD returns the same metadata headers without a
body. Only one explicit byte range is supported. Verify `Content-Length`, ETag/SHA-256, file ID,
content type, range, and that the stream ends successfully. Grant tokens are credentials: never put
them in query strings, logs, analytics, or persistent filenames.

## Realtime without an SDK

For an HTTP-only transport, send the exact Query envelope and normal Query credentials to
`POST /v1/query/follow` with `Accept: application/x-ndjson`. A successful response is an NDJSON
stream whose `state`, `error`, and `resync_required` objects use the same version-1 server-message
contract as the WebSocket protocol. The initial `state` has a request ID; later states keep the
same subscription ID and increase `deliveryRevision`. Decode `value` as a canonical value and
replace the local result; it is a state stream, not an event log.

Bound each line and pending decoder buffer. On `resync_required`, stream termination, authorization
expiry, or transport loss, discard continuity assumptions and create a fresh HTTP follow request.
The server removes the subscription when the HTTP consumer disconnects. This route uses the same
dependency registry and committed-outbox reruns as WebSocket Realtime; it is not polling.

Connect to `wss://<product-origin>/v1/realtime` with WebSocket subprotocol
`runku.realtime.v1`. The client must implement strict versioned JSON messages for authentication,
subscribe/unsubscribe, state, error, resynchronization, ping/pong, reconnect, and authorization
expiry. Each message is bounded to 64 KiB.

Implementing Realtime correctly requires preserving delivery revisions, accepting a fresh snapshot
after `resync_required`, reauthenticating after reconnect, and never inventing missed events. Unless
your language has a conforming Runku client, start with request/response HTTP and add Realtime only
after validating these behaviors against accepted protocol fixtures.

## Self-Hosted verification checklist

Before accepting a custom client:

1. verify Query, Mutation replay, and Action no-retry behavior against a non-production Environment;
2. test every canonical scalar, nested arrays/objects, boundary sizes, and malformed alternatives;
3. test missing, invalid, expired, and wrong-kind credentials independently;
4. prove exact target pinning and record returned Release IDs;
5. simulate timeout after a Mutation commit and recover with the same operation ID;
6. simulate an uncertain Action and reconcile without blind retry;
7. validate TLS, CORS, proxy body/header/time limits, and correlation-header forwarding;
8. repeat the contract tests against Runku SaaS when useful, but keep Self-Hosted infrastructure,
   identity, capacity, storage, and recovery acceptance on the actual installation.
