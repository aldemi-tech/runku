# Validate the product model in Runku SaaS

Runku SaaS is useful when an application team wants to exercise the Product contract before it
operates Runku capacity itself. The SaaS path validates application code and the shared Product
model; it does **not** prove that a particular self-hosted topology, backup plan, security policy,
or capacity envelope is ready.

## What is shared and what is not

| Concern | Shared Product contract | SaaS-specific or operator-owned concern |
|---|---|---|
| Project, Environment, Release, Channel, Workspace | Same concepts and exact-target rules | Organization, billing, provisioning, and placement are service concerns |
| Functions and generated client contracts | Same manifest and public protocol versions within the published compatibility window | Available runtime profiles can be restricted by the selected service capacity |
| Data, Realtime, schedules, files | Same application semantics | Quotas, retention, regional capacity, and service limits are plan/deployment policy |
| Application and functional identity | Same independent authorization axes | Provider enrollment and domain configuration depend on the service setup |
| Operator identity and lifecycle | Same scoped Management capabilities | SaaS can reconcile operator grants from its membership authority |
| Infrastructure | Not part of application code | SaaS operates it; Self-Hosted operators own it |

Never use successful SaaS behavior as evidence that an unlisted Self-Hosted feature is shipped.
Check [Capability and support status](../concepts/capability-status.md) and the selected tag's release
notes first.

## Prerequisites

- install the CLI version required by the target service;
- have an active SaaS account and access to one Project/Environment;
- use a separate application directory or commit all local changes before linking;
- know whether the Environment permits Workspace targets and which Channel is safe for testing.

The CLI defaults to `https://api.runku.app`. A managed deployment can use browser OIDC and derive
the operator's current grants from its identity and membership authority; no bootstrap invitation
belongs in that flow.

## Link an application root

From an empty or already initialized application root:

```sh
npm install --global @runku/cli@X.Y.Z
runku login
runku link
runku status --remote
```

Interactive `link` lists only resources visible to the current session and verifies the selected
Environment before writing local state. CI supplies both IDs explicitly:

```sh
runku link --project-id prj_... --environment-id env_...
```

The resulting `.runku/management-link-v1.json` contains the canonical Management origin and exact
scope, but no token or Application Key. Treat a different Management origin or scope as a new link;
the CLI rejects silent replacement.

Success means `status --remote` returns the same Project/Environment IDs and a coherent serving
revision. Failure before the descriptor is written leaves an uninitialized root unchanged.

## Validate development and delivery

Use the same immutable build output that would be published to Self-Hosted:

```sh
runku build
runku publish --remote \
  --manifest /exact/path/from-build-output/manifest \
  --artifact /exact/path/from-build-output/artifact \
  --expected-head empty
runku release --remote --release rel_...
runku promote --remote --channel preview --release rel_... --expected empty
runku status --remote
```

The paths, IDs, and expected revision come from prior JSON responses; the placeholders above are
not literal values. Use a non-production Channel for the first validation. If the response to a
mutating operation is lost, reconcile by its operation ID or current state before retrying. Never
guess whether a publish or promotion committed.

Validate these behaviors explicitly:

1. a failed Workspace build does not replace the last valid Dev Revision;
2. freezing creates an immutable Release rather than renaming a Workspace revision;
3. promotion changes Channel traffic policy and preserves the prior Release;
4. rollback moves the Channel to a known eligible history entry without rebuilding code;
5. incompatible schema/index/Cron contracts are rejected before a mixed serving set is persisted;
6. logs and request/invocation identifiers correlate the exact Release that ran.

## Validate the application protocol

Obtain a purpose-appropriate Application Client credential through the authorized administration
surface. Public browser code receives only a publishable key; confidential server code keeps a
secret key outside bundles and logs. Configure `@runku/client` explicitly with the application
base URL, an exact `channel:`, `release:`, or authorized `workspace:` target, and the key.

Exercise at least:

- one Query and its snapshot/read-only behavior;
- one idempotent Mutation, including retry with the same operation identity;
- one Action whose external effect has application-level idempotency;
- one Realtime subscription across a committed relevant Mutation and a reconnect;
- one scheduled execution and, when used, one Cron activation;
- one file upload/download using one-shot grants without recording the grant token.

Record function result, stable error code, `x-runku-request-id`, optional
`x-runku-invocation-id`, exact code target, and relevant log cursor. Do not record credentials,
arguments containing secrets, or document/file contents.

## What still must be validated Self-Hosted

SaaS cannot validate your TLS proxy, host ownership, secret mounts, PostgreSQL recovery, Product
root and external-S3 consistency, disk pressure behavior, log archive frontier, upgrade procedure,
or incident access. Before production, complete the [deployment guide](../self-hosting/deployment-guide.md),
[hardening checklist](../security/hardening-checklist.md), and
[production-readiness review](../self-hosting/production-readiness.md) on the actual installation.
