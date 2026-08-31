# Releases and Workspaces

## Development flow

`runku dev` publishes source snapshots to `workspace:local`. Remote development uses an `rk_dev_*`
credential and compare-and-swap Workspace revisions so multiple developers can share one
Environment without silently overwriting each other.

A failed source build does not replace the currently served Dev Revision. The CLI reports the build
failure and continues watching.

## Immutable deployment

A Release contains a canonical manifest, typed contracts, runtime descriptors, and content-addressed
artifacts. Publishing the same inputs produces the same build identity. Artifact reads verify size
and digest.

Promotion changes a Channel pointer after compatibility and readiness checks. Rollback selects a
previous immutable Release; it does not rebuild source.

## Shared data

Workspaces and Releases in one Environment intentionally operate over the same data. This supports
debugging or previewing a fix against representative shared state. Protection rules can prohibit
Workspace targets and development synchronization in production Environments.

## Scheduled work

Scheduled invocations pin the exact Release or Dev Revision that created them. A later Channel move
does not change pending work. Cron activation similarly materializes work from a versioned manifest.
