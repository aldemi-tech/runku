# Publishing Runku artifacts

Runku has two explicit release tracks. A `vX.Y.Z` distribution tag coordinates the public CLI,
`@runku/server`, frontend SDKs, compact Linux server, image, and self-host archive. An
`sdk-vX.Y.Z` tag publishes only the exact-version `@runku/client` and `@runku/react` pair when the
frontend bindings need to move without changing CLI, Rust, server, image, or persisted contracts.

The distribution path builds six native CLI executables and two native server executables,
packages the same CLI bytes for GitHub and npm, publishes ten npm packages, publishes one
multi-platform server image, generates checksums/SBOM/provenance, and creates the GitHub Release
only after npm and the image are complete. It also publishes one compact self-host installation
archive.

This procedure publishes irreversible external state. Run it only from a reviewed, clean commit on
`main`; never from an uncommitted working tree or a fork.

An OCI-only candidate track exists for remote conformance before the coordinated distribution is
published. It builds the native Linux CLI and server for both supported architectures and publishes
only `ghcr.io/aldemi-tech/runku-server:X.Y.Z-candidate-SHORT_COMMIT`, its architecture assembly
tags, and `sha-COMMIT`. It does not create a Git tag or GitHub Release and does not pack or publish
any npm package. Candidate tags are immutable test inputs; production must pin the resulting digest.

## Published artifacts

### Coordinated distribution

Version `X.Y.Z` produces:

- `@runku/client@X.Y.Z`;
- `@runku/react@X.Y.Z`;
- `@runku/server@X.Y.Z`;
- `@runku/cli@X.Y.Z`;
- six exact-version `@runku/cli-*` native packages;
- four `.tar.gz` archives for macOS/Linux and two `.zip` archives for Windows;
- two `runku-server` `.tar.gz` archives for Linux GNU ARM64/x86_64;
- `runku-selfhost-vX.Y.Z.tar.gz` with the versioned compact Compose profile, overlays, operator
  helper, and offline guide;
- `ghcr.io/aldemi-tech/runku-server:X.Y.Z` as a non-root ARM64/x86_64 image, plus an immutable
  `sha-COMMIT` tag and architecture assembly tags;
- `SHA256SUMS`, GitHub artifact attestations, npm integrity, and npm provenance.

### Frontend SDK pair

Version `X.Y.Z` under tag `sdk-vX.Y.Z` produces only:

- `@runku/client@X.Y.Z`;
- `@runku/react@X.Y.Z` with an exact `@runku/client@X.Y.Z` peer;
- npm integrity and provenance for both packages.

It does not create a GitHub Release, CLI/native package, Rust crate, server archive/image, or
self-host package. The SDK release notes must name the compatible Public Protocol/server range and
whether the currently published CLI can generate every documented reference artifact.

Each Windows ZIP and native npm package contains both `runku.exe` and its exact `duckdb.dll`.
The release job downloads that runtime from the DuckDB version pinned in `Cargo.lock`, verifies the
architecture-specific SHA-256 pinned in the workflow, runs the CLI before packaging it, and fails if
either packaged file is absent. A DuckDB dependency update must update and independently verify the
versioned Windows archive URLs and both digests in the same review.

The native target list lives in `scripts/release-platforms.mjs`. The npm launcher mapping, package
optional dependencies, native package metadata, workflow matrix, compatibility table, and install
guide must describe exactly the same six targets.

## Required authority and one-time setup

The release owner needs:

1. write/tag/release permission for `aldemi-tech/runku`;
2. publish permission for the npm organization scope `@runku`;
3. npm account two-factor authentication;
4. GitHub Actions permission to mint OIDC tokens and attestations;
5. repository immutable releases enabled after validating the first release process;
6. protected release tags so an unreviewed commit cannot trigger publication.

Every current package, including `@runku/react` since its 0.4.8 bootstrap, uses trusted publishing.
The normal workflow contains no npm token or repository secret. Never add
`NPM_REACT_BOOTSTRAP_TOKEN`, `NPM_TOKEN`, or `NODE_AUTH_TOKEN` to the release path. If a future new
package name must be created, bootstrap it with a short-lived granular token, establish trusted
publishing immediately, remove all token wiring, and revoke the token before considering setup
complete.

Configure every package in npm with:

| Setting | Value |
|---|---|
| Provider | GitHub Actions |
| Organization | `aldemi-tech` |
| Repository | `runku` |
| Workflow | `release.yml` |
| Allowed action | `npm publish` |

Require two-factor authentication and disallow traditional write tokens for each package. Do not
copy a personal npm session file into the repository or print credentials in Actions logs.

## Distribution version preparation

Runku uses one version for the CLI, all three SDKs, and native packages during the `0.x` line. Update:

- the root, CLI, client, server, and six native `package.json` files;
- `crates/runku-cli/Cargo.toml`;
- the version in CLI help;
- changelog, compatibility, install examples, and upgrade/rollback notes.

Then run:

```sh
pnpm install --frozen-lockfile
pnpm check:packages
pnpm check:release
pnpm audit --audit-level=high
cargo build --package runku-cli --release --locked
target/release/runku --version
target/release/runku --help
make selfhost-package-check
git diff --check
```

`pnpm-workspace.yaml` records two exact, temporary `image-size` CVE exceptions for trusted
repository-owned documentation images. Version `2.0.2` is the current upstream release and has no
patched version for those denial-of-service advisories. Do not broaden these exceptions or treat
them as runtime/package acceptance: every other high or critical JavaScript advisory remains a
release blocker, and the exceptions must be removed when upstream publishes a fixed release.

`scripts/verify-release.mjs` rejects divergent package/Cargo/help versions, native package metadata,
launcher dependencies, and a tag that is not exactly `vX.Y.Z`.

## Frontend SDK version preparation

An SDK-only release changes only `packages/client/package.json`, `packages/react/package.json`, the
exact React peer version, lockfile, changelog, compatibility notes, and affected SDK docs. Keep the
root, CLI/native packages, `@runku/server`, Rust crates, compact server, and image version unchanged.

Run:

```sh
pnpm install --frozen-lockfile
pnpm check:sdk-release
pnpm --filter @runku/client check
pnpm --filter @runku/react check
pnpm --dir examples/field-board-next check
make docs
git diff --check
```

Pack both packages and validate the exact artifact set without publication:

```sh
package_root=$(mktemp -d)
npm pack ./packages/client --pack-destination "$package_root"
npm pack ./packages/react --pack-destination "$package_root"
node scripts/publish-npm.mjs "$package_root" --sdk --dry-run
```

`scripts/verify-sdk-release.mjs` rejects mismatched client/React/peer versions and requires the tag
to equal `sdk-vX.Y.Z`.

Before the first tag or after changing the platform matrix, run the workflow manually from `main`:

```sh
gh workflow run release.yml --ref main
```

A manual run builds, smoke-checks, archives, and packs every target but skips npm and GitHub
publication. It also builds a local Linux image from the fresh archives and exercises clean compact
setup, invitation login, publish/release/promote/invoke/logs, coordinated backup, offline
verification, empty-install restore, preserved operator session, and automatic serving after
restart. Use it to prove all native runners and the release-shaped self-host package before creating
irreversible registry versions.

After all local gates pass, publish an OCI-only candidate from the exact reviewed commit when a
remote cell is required for the remaining campaign:

```sh
gh workflow run release.yml --ref COMMIT_OR_BRANCH -f candidate_image=true
```

This is a publication action because the candidate image becomes externally visible. Record the
workflow run, candidate tag, multi-platform digest, source commit, SBOM/provenance result, and remote
campaign evidence. Do not use a candidate run to infer that npm packages or the final distribution
exist. The final `vX.Y.Z` tag remains forbidden until every required local and remote scenario has
passed and the release owner separately approves the coordinated publication.

The retained `0.5.1` candidate record is
[`v0.5.1-candidate-evidence.md`](v0.5.1-candidate-evidence.md). Future candidate records must retain
the same distinction between Product conformance, hosted artifact completion, and final publication.
The local-only `0.5.2` preparation record is
[`v0.5.2-candidate-evidence.md`](v0.5.2-candidate-evidence.md); it explicitly does not qualify or
authorize a remote image or coordinated publication.
The local `0.5.3` queryable-data and database campaign is recorded in
[`v0.5.3-candidate-evidence.md`](v0.5.3-candidate-evidence.md); its remaining hosted artifact gates
and publication boundary are explicit.

## Trigger and workflow

After review and CI success:

```sh
git tag -s vX.Y.Z -m "Runku vX.Y.Z"
git push origin vX.Y.Z
```

For the frontend-only track, create a separate signed tag instead:

```sh
git tag -s sdk-vX.Y.Z -m "Runku frontend SDK X.Y.Z"
git push origin sdk-vX.Y.Z
```

`.github/workflows/release.yml` runs these jobs:

1. `metadata` validates the immutable tag/version relationship.
2. `sdk-packages` installs the locked JavaScript workspace and runs the four focused package
   checks. It packs client/React for SDK tags and additionally packs server/CLI for distribution
   tags.
3. `selfhost-package` creates the compact installation archive and statically validates its Compose
   model without starting services.
4. Six `cli-binaries` jobs run concurrently on native ARM64/x86_64 macOS, Linux, and Windows
   runners. Linux builds execute in a digest-pinned Debian Bullseye container and must run on its
   glibc 2.31 runtime; the other platforms build natively. Each job checks `--version`, `--help`,
   package content, and archive creation.
5. Two `server-binaries` jobs build and smoke-check `runku-server` on the same pinned Linux GNU
   glibc 2.31 baseline for ARM64 and x86_64, then produce downloadable archives plus exact image
   inputs. C++ compilation is bounded to two concurrent jobs so DuckDB cannot exhaust runner RAM.
6. On a manual pre-tag run, `selfhost-artifact-smoke` assembles the Linux x86_64 image and runs the
   bounded install/lifecycle/disaster-restore campaign. It is skipped on tags because the reviewed
   commit already supplied this behavioral evidence.
7. `server-image` combines those server bytes with the matching native CLI bytes into a digest-
   pinned distroless image and publishes both architectures with BuildKit SBOM and provenance
   attestations. A distribution tag creates the version/commit manifests. An explicitly requested
   candidate run creates only the immutable `X.Y.Z-candidate-SHORT_COMMIT` and commit manifests.
8. For a distribution tag, `publish-npm` verifies the complete ten-package set, publishes native
   packages first and the launcher last, and compares registry integrity with the local tarballs.
9. `github-release` generates checksums, attests assets, and publishes the release after npm and
   the server image pass.

For `sdk-v*`, the same workflow runs metadata and focused package checks, packs only client/React,
and runs `publish-sdk-npm`. Distribution binaries, images, archives, and GitHub Release jobs are
skipped.

The release workflow intentionally does not run `make check`, examples, databases, a server
lifecycle, HTTP/WebSocket flows, benchmarks, Clippy, rustdoc, or the Rust test suite. Its Docker
work only assembles already native-validated server/CLI bytes into the OCI image. The hosted
`make ci-check` gate proves compile and package coherence; maintainers run the focused behavioral
gates required by the changed contract before merge. Repeating those gates after tagging increases
release latency without changing the source. The release gate proves native compilation, launch,
metadata, package shape, byte identity, and publication.

Cargo registry, Git dependencies, and the target directory are cached per exact native target and
lock/toolchain hash. macOS Intel deliberately caches only Cargo registry/Git inputs: its large
`target` tree took longer to upload than it saved and could consume the job's post-build timeout
after the required artifact had already passed. Matrix jobs remain independent and `fail-fast` is
disabled so one platform failure does not hide evidence from the other five.

## Distribution success verification

The workflow is complete only when:

```sh
npm view @runku/cli@X.Y.Z version dist.integrity
npm view @runku/client@X.Y.Z version dist.integrity
npm view @runku/react@X.Y.Z version dist.integrity
npm view @runku/server@X.Y.Z version dist.integrity
gh release view vX.Y.Z --repo aldemi-tech/runku
docker buildx imagetools inspect ghcr.io/aldemi-tech/runku-server:X.Y.Z
```

Also verify one clean npm install and one direct archive on each supported operating-system family:

```sh
npm install --global @runku/cli@X.Y.Z
runku --version
```

For direct assets, download `SHA256SUMS`, verify the exact filename, extract, and run `--version`.
Confirm npm displays provenance and GitHub displays the release/asset attestation.
Pull the server image by its reported digest on both Linux architecture families, run
`runku-server version`, and execute the documented compact-profile smoke campaign before promotion.

For an SDK-only release, verify:

```sh
npm view @runku/client@X.Y.Z version dist.integrity
npm view @runku/react@X.Y.Z version dist.integrity peerDependencies
```

Then install both exact versions into an empty temporary project, import `@runku/react` and
`@runku/react/server`, and run a TypeScript check. Confirm npm displays provenance. The React
bootstrap publication is the one permitted exception; configure trusted publishing immediately
afterward so subsequent SDK versions carry normal OIDC provenance.

## Failure and safe retry

The workflow may be rerun for the same tag. `scripts/publish-npm.mjs`, including `--sdk` mode,
behaves as follows:

- missing name/version: publish it;
- existing name/version with identical SHA-512 integrity: verify and skip it;
- existing name/version with different bytes: fail closed;
- incomplete or unexpected tarball set: fail before publishing.

This permits recovery from a network failure after some packages were accepted. It never replaces
published bytes. Do not delete/recreate the tag, rebuild a different commit under the same version,
or use `npm unpublish` as rollback.

If a released defect is discovered:

1. stop promotion/document the affected version;
2. deprecate it in npm when appropriate;
3. prepare a new patch version;
4. describe impact, upgrade, and any state rollback limit;
5. run the normal release procedure.

The GitHub Release is created last. If npm succeeds and GitHub creation fails, rerun the workflow;
the npm integrity checks skip identical packages and the final job can publish the existing assets.

## Adding or removing a platform

A platform change affects the public compatibility contract. In one reviewed change:

1. prove the Rust/V8/native dependency graph supports the target;
2. add or remove its native runner and package metadata;
3. update `release-platforms.mjs`, launcher mapping/dependencies/tests, workflow matrix, lockfile,
   install table, compatibility, and release documentation;
4. run a native `--version`/`--help` check;
5. state whether existing installations continue receiving updates.

Do not publish a target built through emulation while documenting it as natively validated.
