# Environment variables and secrets

Each Environment owns one durable, exact-scope configuration registry. It stores readable
variables and encrypted secret material independently from Release artifacts. A configuration
change never mutates an immutable Release: Functions opt in by declaring exact names in their
manifest, and the runtime resolves the current Environment value when the Function asks for it.

## Function contract

Declare `variable:NAME` for non-secret values and `secret:NAME` for secrets. Names contain only
uppercase ASCII letters, digits, and `_`, are at most 64 bytes, and cannot start with `_`. A digit
is accepted in the first position by the current version.
Variables are available to Query, Mutation, and Action; secrets are available only to Action.

```ts
import { action, query, v } from "@runku/server"

export const checkoutEnabled = query({
  auth: "none",
  visibility: "public",
  capabilities: ["variable:FEATURE_CHECKOUT_V3"],
  args: v.null(),
  returns: v.string(),
  handler: (ctx) => ctx.env.get("FEATURE_CHECKOUT_V3"),
})

export const sendPayment = action({
  auth: "service",
  visibility: "internal",
  capabilities: ["variable:STRIPE_WEBHOOK_URL", "secret:PAYMENTS_API_KEY"],
  args: v.null(),
  returns: v.null(),
  async handler(ctx) {
    const endpoint = await ctx.env.get("STRIPE_WEBHOOK_URL")
    const key = await ctx.secrets.get("PAYMENTS_API_KEY")
    // Use values without logging or returning secret material.
    void endpoint
    void key
    return null
  },
})
```

`ctx.env` and `ctx.secrets` are absent unless the Function declares a matching capability. Their
`get(name)` methods recheck the exact kind and name in the host, so casting TypeScript or forging a
Full Node platform message cannot broaden access. Nested calls keep the exact Environment and
attach only the child Function's declared names. Every new build targets the cumulative current
runtime contract: `runku-js`, `runku-node`, or `runku-hybrid`, according to artifact class.
Capability selection no longer emits numeric runtime variants. Readers still accept already-
persisted `*-1`, `*-2`, and `*-3` manifests; those identifiers are compatibility inputs, not
separately evolving runtimes.

Safe V8 and local Full Node resolve configuration through the exact-name broker. OCI, dedicated
host, Docker, and Firecracker Full Node execution resolve exactly the declared names before handing
the authenticated invocation to the isolated runner, zeroize temporary plaintext after encoding,
and fail closed when the exact Environment broker is absent. The distributed queue/agent test
proves Gateway-to-agent resolution and verifies that secret material is absent from diagnostics.

## Management API

All routes use the explicit Project/Environment path and a current `rk_at_*` operator session.

| Method and path | Capability | Result |
|---|---|---|
| `GET .../configuration` | `configuration:read` | current global revision and name-ordered safe entries |
| `GET .../configuration/history?limit=50&beforeSequence=...` | `configuration:read` | newest-first immutable value-free audit page |
| `PUT .../configuration/{NAME}` | `configuration:manage` + `Idempotency-Key: opn_*` | create/update/rotate under global revision CAS |
| `DELETE .../configuration/{NAME}` | `configuration:manage` + `Idempotency-Key: opn_*` | delete under global revision CAS |

A PUT body contains `expectedRevision`, `kind`, `value`, and caller-pinned
`changedAtMicros`. DELETE contains `expectedRevision` and `changedAtMicros`. Every real mutation
increments the Environment configuration revision exactly once. An identical operation replay
returns the original safe result even if later mutations changed the same name; reusing its
operation ID for different intent fails closed.

Variable values are present in authorized list and mutation responses. Secret values are accepted
only in PUT and never appear in snapshots, mutation responses, history, Debug output, logs, or
audit. Secret values are encrypted with AES-256-GCM using an external, domain-separated deployment
key and authenticated with Project, Environment, name, and revision. The current compact/server
composition derives that key from protected Product key material and stores registry rows in the
Product identity SQLite database.

Limits are 512 entries per Environment, 16 KiB per variable, 64 KiB per secret, and 100 history
events per page. The HTTP configuration body limit is 72 KiB. Unknown kinds, malformed names,
wrong revisions, kind-confused reads, corrupt ciphertext, and unknown schema checksums fail closed.

## Recovery and rollout

The registry, operation journal, audit, and encrypted secret envelopes live in Product persistent
state and are included by the compact coordinated backup. The external root key/pepper must be
backed up through its separate protected secret procedure; SQL/SQLite bytes alone cannot decrypt
secrets. Restore the persistent state and matching key material together, then verify snapshot,
history, a declared variable read, and a declared secret read without printing either secret.

The registry is currently composed through the compact/server SQLite authority. A generic
PostgreSQL repository for this configuration domain is not claimed. A SaaS deployment can expose
the same Product-level Management contract, but its current service behavior must be validated
independently and does not change the Self-Hosted state-ownership or recovery requirements above.
