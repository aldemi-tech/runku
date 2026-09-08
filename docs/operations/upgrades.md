# Upgrade and rollback

An application promotion and a Runku server upgrade are different operations. A Channel rollback
changes code traffic policy. It cannot undo a server migration, Product data change, configuration
rotation, or external effect.

## Upgrade contract

Upgrade only to a tagged version whose release materials provide:

- exact server image manifest digest and package checksums/provenance;
- CLI/SDK/protocol/persisted-format compatibility window;
- previous-supported-version upgrade evidence;
- migration behavior, downtime expectation, and rollback floor;
- known limitations and required operator actions.

Runku is pre-release. The first supported compact distribution establishes its upgrade floor; do
not invent an older upgrade path from source compatibility alone.

## Preflight

1. Read current version, image digest, Environment scope, Channels, serving revision, and state
   backend inventory.
2. Confirm every Application/CLI/SDK version remains inside the target compatibility window.
3. Resolve schema/index/Cron rollout separately; keep an eligible application Release available.
4. Review new/removed configuration and secret inputs. Prepare mounts without replacing current
   key material.
5. Confirm free capacity for image pull, migration, backup staging, database WAL, and rollback
   evidence.
6. Create an isolated staging restore and exercise the upgrade there with production-shaped state.
7. Schedule a maintenance window because the compact profile has one active writer and no rolling
   multi-node upgrade.

## Execute the packaged upgrade

From the current versioned package directory:

```sh
./runku-selfhost upgrade \
  ghcr.io/aldemi-tech/runku-server:X.Y.Z@sha256:<64-hex-manifest> \
  /mnt/encrypted/runku-pre-upgrade-X.Y.Z \
  kms://backup-policy/version-7
```

The helper validates the candidate image/version/configuration, creates and verifies the
pre-upgrade recovery point, stops serving, applies migrations, starts the new image, waits for
readiness, and only then persists the new image pin in `.env`.

Never edit `.env` to the new image before the helper completes: the durable pin is the statement of
which version owns the migrated state. Never use a mutable tag or unverified digest.

## Verify before reopening traffic

- `./runku-selfhost status` and public Management discovery are healthy;
- operator login/refresh/grants and exact Project/Environment identity are unchanged;
- Channel targets, serving policy revision/observation, artifact integrity, and eligible rollback
  history are coherent;
- Query, idempotent Mutation replay, Action policy, Realtime reconnect, schedule/Cron, files, and
  logs work through public TLS;
- Product root and optional logical PostgreSQL agree on exact scope;
- external-S3 bytes, log archive frontier, pending usage events, and worker lag are healthy;
- no new stable error, migration warning, or secret exposure appears.

Record the final version/digest, migration result, recovery point, verification evidence, and any
temporary compatibility mode that must later be removed.

## Failure decision tree

| Point of failure | State assumption | Response |
|---|---|---|
| candidate image pull/version/check | old state untouched | correct provenance/configuration and rerun preflight |
| backup/verification | no upgrade authorized | preserve failure evidence; repair recovery path first |
| stop before migration | old schema expected | restart old version only if helper/release procedure confirms no migration began |
| migration returns failure | schema may be partially/forward changed despite transaction design | stop writes; follow exact release recovery/rollback floor |
| new readiness fails after migration | new schema may be authoritative | do not silently start old binary; diagnose or restore per release procedure |
| post-start application regression | server healthy, application behavior changed | decide Channel rollback versus server recovery using exact evidence |

If migration/readiness fails, the helper preserves state and does not silently fall back to an older
binary. Starting old code against newer persisted formats can convert a recoverable upgrade into
corruption.

## Rollback classes

| Need | Mechanism | Data effect |
|---|---|---|
| Application code regression | Channel rollback to eligible immutable Release | no automatic data reversal |
| Configuration regression | new CAS mutation restoring prior approved value | increments configuration revision; secret value must come from protected source |
| Credential issue | overlap/rotate/revoke in owning identity authority | no server binary change |
| Server regression before migration | release-documented old image restart | only when persisted format remains compatible |
| Server regression after irreversible migration | restore complete pre-upgrade recovery point or forward fix | downtime and post-restore reconciliation required |

Restore rollback loses durable changes after the recovery point and may resurrect revoked sessions,
invitations, and queued work. It requires explicit business authorization, complete state scope,
and reconciliation before traffic.

## SDK and client rollout

Upgrade server, CLI, `@runku/server`, and `@runku/client` according to the published matrix rather
than as one unexamined bundle. Generated types belong to the built Release. Rebuild application code
when server SDK contracts change and retain older clients/Releases for the documented compatibility
window. Unknown protocol/manifest/runtime versions fail closed.

The `dedicated-host` Full Node setting is an operator-selected capability, not a persisted-format
migration. Before enabling it on an upgraded image, prove the image's pinned Node binary and one
direct Node bundle on the target architecture. Rolling back to an image without that profile is
safe for Product bytes, but all Node Releases must be drained or treated as unavailable first;
Safe Releases remain independently eligible.

## Remove old material

Only after the observation and rollback window:

- expire temporary compatibility settings;
- revoke temporary credentials and sessions;
- prune old images/packages under host policy;
- remove a Release from eligibility only after no Channel, schedule, Cron, or recovery plan needs it;
- retain pre-upgrade recovery evidence for the approved period.

Do not delete old artifacts merely because their Channel weight is zero.
