---
slug: /docs
title: Runku documentation
description: Use and administer Runku Self-Hosted.
---

# Use and administer Runku

This documentation explains how to build applications on Runku and operate Runku Self-Hosted. It
is organized by responsibility so application code, CLI workflows, and server administration do
not blur together.

## Choose your path

| You want to… | Start here | You will work with |
|---|---|---|
| build an application backend | [Application tutorial](getting-started/application-tutorial.md) | schemas, Query/Mutation/Action, data, auth, client |
| look up an exact Function parameter/type/limit | [Function API reference](reference/function-api.md) | `@runku/server` declarations and handler context |
| call Runku from TypeScript | [TypeScript client](reference/typescript-client.md) | `@runku/client`, typed calls, Realtime, file grants |
| use React or Next.js | [React and Next.js integration](reference/react-client.md) | hooks, SSR hydration, server calls, generated references |
| call Runku without an SDK | [Public HTTP API](reference/public-api.md) | canonical HTTP/JSON, credentials, retries |
| use the command line | [CLI guide](cli/overview.md) | local dev, login/link, publish, promote, rollback |
| install Self-Hosted | [Deployment guide](self-hosting/deployment-guide.md) | compact Docker package, TLS, state, readiness |
| operate an installation | [Operator handbook](operations/operator-handbook.md) | health, logs, backup, upgrades, incidents |
| configure the Runku Storage backend | [Storage configuration](self-hosting/storage-configuration.md) | filesystem/external object-store bytes, quota, capacity, recovery |

## Application development

Follow this route when writing the backend and the application that consumes it:

1. [Application tutorial](getting-started/application-tutorial.md) — complete first application.
2. [Schema and data types](functions/schema-and-types.md) — every validator, table/index rule,
   naming constraint, size/depth limit, and rollout concern.
3. [Query, Mutation, and Action](functions/query-mutation-action.md) — choose the correct semantic
   operation, define authentication/visibility/capabilities, handle retries/effects/scheduling.
4. [Function API reference](reference/function-api.md) — exact declaration fields, handler
   parameters, context methods, capability matrix, and runtime limits.
5. [Documents and indexes](data/documents-and-indexes.md) — IDs, reads/writes, revisions, OCC,
   index ordering, scan constraints, pagination limits, and Realtime dependencies.
6. [Data and Realtime](data/data-and-realtime.md) — broader transaction, outbox, subscription,
   resync, and administrative-data contract.
7. [Application identity](auth/application-identity.md) — Application Clients, functional identity,
   public/secret/development credentials, JWT/OIDC, browser/server separation, and rotation.
8. [Environment variables and secrets](concepts/environment-configuration.md) — declare and read
   exact configuration capabilities safely.
9. [Application Files](functions/file-storage.md) — Action permissions, grant parameters, streaming,
   direct bytes, lifecycle, limits, and recovery consequences.
10. [TypeScript client](reference/typescript-client.md) or [HTTP without an SDK](reference/public-api.md)
    — consume the application API.

Use [Application development workflow](functions/development-workflow.md) for build, Dev Revision,
Release, Channel, compatibility, testing, and delivery as one task flow.

## Storage products

Runku exposes two distinct application-facing storage capabilities:

| Capability | Application interface | Administration | Best for |
|---|---|---|---|
| Application Files | Action `ctx.storage` + short-lived HTTP grants | Self-Hosted byte quota/backend | user attachments and authorized transfers |
| Runku Object Storage | Runku Product route with S3-compatible protocol | buckets, policies, CORS, quotas, Product access keys | application objects and compatible tooling |

Read [Application Files](functions/file-storage.md) or
[Runku Object Storage](concepts/object-storage.md) for usage. Operators should separately
read [Storage configuration and limits](self-hosting/storage-configuration.md), because the
physical filesystem or external object-store credentials, capacity, backup, and migration are
installation concerns.

## CLI

The [CLI guide](cli/overview.md) covers:

- installation and supported platforms;
- `init`, `dev`, `build`, `status`, `doctor`, and local logs;
- Self-Hosted `login` and Environment `link`;
- remote publish, Release freeze, Channel promotion, and rollback;
- local versus remote authority, CAS conflicts, exit-code behavior, and security.

Use the [CLI reference](reference/cli.md) when you need exact syntax and flags. The CLI does not
define Function parameters and is not the `runku-server` administration/configuration interface.

## Self-Hosted installation

Read in this order:

1. [Self-hosting overview](self-hosting/overview.md) — current supported distribution and fit.
2. [Compact deployment guide](self-hosting/deployment-guide.md) — plan, install, expose TLS,
   initialize ownership, publish, verify, and remove.
3. [Docker package](../deployments/docker/README.md) — exact package commands and profiles.
4. [Server configuration](self-hosting/server-configuration.md) — `runku-server` commands,
   listeners, identity, Product database, browser auth, logs, and validation.
5. [Storage configuration](self-hosting/storage-configuration.md) — filesystem or external
   object-store parameters, capacity, credentials, canaries, backup, restore, and backend changes.
6. [Function data PostgreSQL](self-hosting/product-postgresql.md) — optional Environment-scoped
   document/index/outbox/schedule database and its recovery boundary.
7. [Production readiness](self-hosting/production-readiness.md) — explicit go/no-go criteria.

The current supported package is a compact non-root `runku-server` plus Docker Compose for one
initialized Safe V8 Product Environment. Repository assets for separated general-purpose roles,
Kubernetes, and VM-isolated shared Full Node are not a supported Helm/cluster distribution.

## Operate Self-Hosted

Use these guides after installation:

1. [Operator handbook](operations/operator-handbook.md) — routine checks, safe changes, rollout,
   incident triage, restart, restore decisions, and evidence.
2. [Administration](operations/administration.md) — Environment lifecycle, credentials, retention,
   capacity, maintenance, and incidents.
3. [Observability](operations/observability.md) — signal catalog, dashboards, alerts, correlation,
   privacy, and OTLP behavior.
4. [Operational logs](operations/operational-logs.md) — standalone/HA storage, query/follow,
   archive frontier, retention, recovery, sizing, and upgrades.
5. [Backup and recovery](operations/backup-and-recovery.md) — state inventory, compact commands,
   restore verification, external dependencies, and disaster recovery.
6. [Upgrades and rollback](operations/upgrades.md) — server/database/package change procedure.
7. [Capacity planning](operations/capacity-planning.md) — workload model, resource signals,
   saturation tests, and limits.
8. [Troubleshooting](reference/troubleshooting.md) — symptom-first diagnosis.

Application Release rollback and Self-Hosted server rollback are different operations. A Channel
move does not restore data, change the binary, or reverse an external effect.

## Identity and security

- [Identity map](auth/identity-map.md) distinguishes Application Client, functional principal,
  Platform operator, file grant, Product storage key, and physical provider credential.
- [Application identity](auth/application-identity.md) covers runtime callers.
- [Platform operator identity](auth/platform-identity.md) covers owners, invitations, OIDC,
  sessions, grants, and recovery.
- [Security model](security/security-model.md) covers trust boundaries, threats, controls, secrets,
  and residual risk.
- [Hardening checklist](security/hardening-checklist.md) is the deployment acceptance checklist.

Credentials cannot exchange roles. In particular, an Application Key cannot call the Management
API, an operator token cannot replace a Function bearer/file grant, and a Runku Storage Product key
is not the physical external-backend credential held by `runku-server`.

## Administration APIs

Use the [Management API reference](reference/management-api.md) only when building an operator UI or
controller. It covers Platform sessions/capabilities, exact Project/Environment scope, CAS,
idempotency, one-time secret responses, lifecycle, Releases/Channels, configuration, credentials,
storage, data administration, schedules/Cron, logs, and errors.

Application clients use the [Public HTTP API](reference/public-api.md). Never expose the Management
origin/token as an application backend API.

## Product model

The durable vocabulary is:

- **Environment = persistent state**;
- **Release = immutable code**;
- **Channel = traffic policy**;
- **Workspace = mutable pointer to immutable development revisions**.

Read [Platform model](concepts/platform-model.md),
[Environment lifecycle](concepts/environment-lifecycle.md),
[Serving policy](concepts/serving-policy.md), and
[Releases and Workspaces](development/releases-and-workspaces.md) when operating delivery/routing.
Every request, subscription, nested call, Cron activation, and scheduled invocation pins exact
code for its defined lifetime; there is no implicit `latest`.

## Support status vocabulary

Documentation uses these states precisely:

| State | Meaning |
|---|---|
| Implemented | available in the current product path described |
| Conformance | a bounded contract/test exists; this alone is not installation support |
| Production requirement | acceptance criterion that must be met before production use |
| Pre-release limitation | capability or guarantee deliberately not promised by the current line |

Start with [Capability and support status](concepts/capability-status.md) before relying on optional
runtimes, distributed topology, HTTPS egress, storage backends, or backup guarantees.

## Validate in Runku SaaS

[SaaS validation](getting-started/saas-validation.md) can shorten application contract validation:
schemas, Functions, clients, identity behavior, canonical calls, Release targeting, and supported
storage flows can be compared there when enabled.

SaaS validation does not prove Self-Hosted TLS, proxy behavior, physical Runku Storage backend
policy, PostgreSQL, capacity, backup/restore, upgrade, or incident readiness. Repeat those
acceptance tests on the actual Self-Hosted installation.
