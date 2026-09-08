# Capability and support status

This page distinguishes features an application/operator can use from contracts that are only
validated for future deployment profiles. It applies to the current pre-release distribution. A
tagged release contains only the artifacts and profiles named in that release.

## Status vocabulary

| Label | Operational meaning |
|---|---|
| **Supported profile** | Shipped as a versioned artifact with exact install, recovery, upgrade, and verification procedures |
| **Implemented** | Available in the stated product path; it may still live inside a bounded profile |
| **Conformance** | A provider or distributed boundary passes its contract harness, but Runku does not yet package the complete topology |
| **Readiness criterion** | Required acceptance evidence, not a claim that the capability is available |
| **Not shipped** | Deliberately outside the current distribution boundary |

## Shipped product boundary

| Capability | Status | What is available now | Important boundary |
|---|---|---|---|
| Cross-platform `runku` CLI | Supported profile | Tagged macOS, Linux GNU, and Windows ARM64/x86_64 binaries plus `@runku/cli` | Use the CLI guide/reference, not server commands |
| TypeScript SDKs | Supported profile | `@runku/server` declarations/validators and `@runku/client` HTTP, Realtime, and file transfer client | SDK and protocol versions must match the server compatibility window |
| Local application process | Implemented | One complete SQLite-backed Environment through `runku dev` | Development convenience; not a multi-node production topology |
| Compact self-hosted server | Supported profile | Linux ARM64/x86_64 non-root image and one-Environment Docker Compose package | One active writer and one host failure domain |
| Platform Identity | Implemented in compact profile | PostgreSQL-backed owner bootstrap, invitations, sessions, scoped grants, optional OIDC, and managed grant reconciliation | Management exposure requires loopback or trusted TLS termination |
| Product gateway | Implemented | Query, Mutation, Action, Realtime, file transfers, and the Runku Object Storage path | Calls require exact application identity and code targeting |
| Safe V8 | Implemented | Deny-by-default TypeScript/JavaScript runtime with manifest-declared capabilities and deadlines | No ambient Node, filesystem, environment, or network authority |
| Full Node, local | Implemented for development | Node built-ins/npm, local machine runtime, immutable OCI descriptor generation, and hybrid calls | Local Node trusts the developer machine; it is not a tenant boundary |
| Full Node, separate trusted worker | Implemented | NATS queue/control, immutable read-only resource projection, dedicated container/cgroup/cache | Shares the host kernel; not a hostile multi-tenant boundary |
| Full Node, shared untrusted | Conformance | Firecracker-oriented execution evidence behind the same queue/control-plane contract | The compact package does not ship the VM-grade Agent profile |
| General distributed roles | Not shipped | Internal conformance exists for some boundaries | No supported `runku-agent`, generic role package, or active-active Product topology |
| Kubernetes | Conformance | Dependency and Full Node Agent conformance manifests | No supported Helm chart or general Kubernetes installation |

## Application-development capabilities

| Capability | Status | Contract | Important boundary |
|---|---|---|---|
| Schema and typed values | Implemented | Canonical null, boolean, signed 64-bit integer, finite float, string, bytes, timestamp, typed ID, array, and object values | Exact validators, schema and stored-value limits apply |
| Query | Implemented | Snapshot/read-only execution; may call Queries and collect dependencies | Function index range encoding/pagination is currently limited |
| Mutation | Implemented | OCC retry around one atomic document/index/outbox/schedule commit; operation-ID replay | No index scan and no external effects |
| Action | Implemented | May coordinate files/scheduling/nested calls; never automatically retried by the client | Available effects depend on deployment capabilities |
| Nested calls | Implemented | Same Environment and exact code pin; type/capability matrix is enforced | Bounded call count, depth, concurrency, and deadline |
| Durable scheduling | Implemented | At-least-once; scheduled work stores an exact Release or Dev Revision | Handler effects require idempotency |
| Cron | Implemented | Declarations with durable operator activation overrides | Targets Mutation/Action; delivery is at-least-once |
| Realtime | Implemented | Query subscriptions, dependency-driven post-commit reruns, reconnect, reauth, and explicit resync | Authoritative state refresh, not an event log |
| Application files | Implemented | Action-issued one-shot upload/download grants; streamed bytes, quota, and range support | Recovery of an external object-store backend is operator-coordinated |
| Runku Object Storage | Implemented, bounded | Bucket/key authority, console transfers, provider-independent bytes, and an S3-compatible SigV4 protocol subset | No multipart, version listing/deletion, lifecycle execution, native SDK, or CLI commands |
| Environment variables/secrets | Implemented | Environment-scoped CAS registry; encrypted secrets; exact manifest-gated reads | Secrets are Action-only |
| HTTPS from Actions | Not available in compact profile | Public `network:https` contract exists | Compact invocation returns `ACTION_HTTPS_UNAVAILABLE`; use only a profile/SaaS Environment that explicitly exposes a broker |

## Identity and management capabilities

Runku uses independent authorization axes. Passing one does not grant the other.

| Plane | Credential examples | Purpose | Cannot be exchanged for |
|---|---|---|---|
| Application | `rk_pub_v1_*`, `rk_sec_v1_*` | Identifies browser/native/server application code and caps application scopes | Platform operator or development authority |
| Functional principal | guest token, application JWT, internal service principal | Identifies the user/guest/service evaluated by a Function auth policy | Application identity or Platform administration |
| Development | `rk_dev_v1_*` | Publishes immutable Dev Revisions to authorized Workspaces | Function invocation or Platform operator session |
| Platform operator | one-time invitation, `rk_at_v1_*`, `rk_rt_v1_*`, configured OIDC token | Administers exact Installation/Project/Environment resources | Application invocation without a separate application credential |
| Runku Object Storage | bucket access key and secret, presigned request | Authorizes the bounded Runku Product path through its S3-compatible protocol | Function, development, or Platform authority |

Management capabilities are persisted as explicit grants at Installation, Project, or exact
Environment scope. Presentation roles expand to capabilities before persistence. See
[Identity map](../auth/identity-map.md) and [Management API](../reference/management-api.md).

## Data and storage profiles

| State or dependency | Compact supported choice | Additional implemented choice | Operator consequence |
|---|---|---|---|
| Platform Identity | PostgreSQL 16 | — | Back up with Product state and matching peppers |
| Function logical store | Product-root SQLite | Scope-bound PostgreSQL 16 | PostgreSQL replaces only documents/indexes/outbox/schedules, not the whole Product root |
| Release, Workspace, application identity, Environment, serving, Cron, configuration metadata | Product-root SQLite authorities | Repository-level PostgreSQL conformance exists for several domains | The compact package remains one writer even when Function data uses PostgreSQL |
| Release artifacts | Product-root content-addressed filesystem | S3-compatible adapter conformance | Digest and size are verified on read |
| Application file/object bytes | Dedicated filesystem directory | External S3-compatible prefix | Filesystem bytes are in compact backup; external-backend recovery must be coordinated separately |
| Operational logs | SQLite hot tier and filesystem/S3 Parquet archive | Optional NATS JetStream plus S3 archive worker | HA logs improve diagnostic durability, not Product data HA |

## How to use this matrix

For a local evaluation, follow [Local development](../getting-started/local-development.md). For a
self-hosted installation, use only the [compact deployment guide](../self-hosting/deployment-guide.md)
and apply the [production-readiness contract](../self-hosting/production-readiness.md). To exercise
the same Project/Environment/Release/Channel concepts without operating infrastructure first, use
the [SaaS validation path](../getting-started/saas-validation.md); do not infer a self-hosted feature
from SaaS behavior unless it is also listed here and in the tagged release notes.

## Interpreting status

Treat a capability as supported only in the exact distribution/profile stated. SaaS availability,
a conformance result, or the presence of configuration fields does not make a feature available in
the compact Self-Hosted package. Follow the linked user/operator guide and its limitations.
