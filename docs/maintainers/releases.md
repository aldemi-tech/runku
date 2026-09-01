# Publishing a Runku distribution

One repository tag coordinates the public CLI and TypeScript SDK release. The release workflow
builds six native executables, packages the same bytes for GitHub and npm, publishes nine npm
packages, generates checksums and provenance, and creates the GitHub Release only after npm is
complete.

This procedure publishes irreversible external state. Run it only from a reviewed, clean commit on
`main`; never from an uncommitted working tree or a fork.

## Published artifacts

Version `X.Y.Z` produces:

- `@runku/client@X.Y.Z`;
- `@runku/server@X.Y.Z`;
- `@runku/cli@X.Y.Z`;
- six exact-version `@runku/cli-*` native packages;
- four `.tar.gz` archives for macOS/Linux and two `.zip` archives for Windows;
- `SHA256SUMS`, GitHub artifact attestations, npm integrity, and npm provenance.

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

All nine current package names already use trusted publishing. The normal workflow contains no npm
token or repository secret. A future new package name must exist before npm can select its trusted
publisher; bootstrap only that new package with a short-lived granular token in a reviewed,
temporary workflow change, then remove the token wiring immediately after configuring trust. Never
leave `NPM_TOKEN` or `NODE_AUTH_TOKEN` in the normal release path.

After the first successful publication, configure every package in npm with:

| Setting | Value |
|---|---|
| Provider | GitHub Actions |
| Organization | `aldemi-tech` |
| Repository | `runku` |
| Workflow | `release.yml` |
| Allowed action | `npm publish` |

Then require two-factor authentication, disallow traditional write tokens for each package, delete
the `NPM_TOKEN` GitHub secret, and revoke the bootstrap token in npm. Do not copy a personal npm
session file into the repository or print it in Actions logs.

## Version preparation

Runku uses one version for the CLI, both SDKs, and native packages during the `0.x` line. Update:

- the root, CLI, client, server, and six native `package.json` files;
- `crates/runku-cli/Cargo.toml`;
- the version in CLI help;
- changelog, compatibility, install examples, and upgrade/rollback notes.

Then run:

```sh
pnpm install --frozen-lockfile
pnpm check:packages
pnpm check:release
cargo build --package runku-cli --release --locked
target/release/runku --version
target/release/runku --help
git diff --check
```

`scripts/verify-release.mjs` rejects divergent package/Cargo/help versions, native package metadata,
launcher dependencies, and a tag that is not exactly `vX.Y.Z`.

Before the first tag or after changing the platform matrix, run the workflow manually from `main`:

```sh
gh workflow run release.yml --ref main
```

A manual run builds, smoke-checks, archives, and packs every target but skips npm and GitHub
publication. Use it to prove all native runners before creating irreversible registry versions.

## Trigger and workflow

After review and CI success:

```sh
git tag -s vX.Y.Z -m "Runku vX.Y.Z"
git push origin vX.Y.Z
```

`.github/workflows/release.yml` runs these jobs:

1. `metadata` validates the immutable tag/version relationship.
2. `sdk-packages` installs the locked JavaScript workspace, runs the three focused package checks,
   and packs client, server, and CLI launcher tarballs.
3. Six `cli-binaries` jobs run concurrently on native ARM64/x86_64 macOS, Linux, and Windows
   runners. Each builds only `runku-cli --release --locked`, then checks `--version`, `--help`,
   package content, and archive creation.
4. `publish-npm` verifies the complete nine-package set, publishes native packages first and the
   launcher last, and compares registry integrity with the local tarballs.
5. `github-release` generates checksums, attests assets, and publishes the release after npm passes.

The release workflow intentionally does not run `make check`, examples, Docker, databases, a local
server, HTTP/WebSocket flows, benchmarks, Clippy, rustdoc, or the Rust test suite. The hosted
`make ci-check` gate proves compile and package coherence; maintainers run the focused behavioral
gates required by the changed contract before merge. Repeating those gates after tagging increases
release latency without changing the source. The release gate proves native compilation, launch,
metadata, package shape, byte identity, and publication.

Cargo registry, Git dependencies, and the target directory are cached per exact native target and
lock/toolchain hash. Matrix jobs remain independent and `fail-fast` is disabled so one platform
failure does not hide evidence from the other five.

## Success verification

The workflow is complete only when:

```sh
npm view @runku/cli@X.Y.Z version dist.integrity
npm view @runku/client@X.Y.Z version dist.integrity
npm view @runku/server@X.Y.Z version dist.integrity
gh release view vX.Y.Z --repo aldemi-tech/runku
```

Also verify one clean npm install and one direct archive on each supported operating-system family:

```sh
npm install --global @runku/cli@X.Y.Z
runku --version
```

For direct assets, download `SHA256SUMS`, verify the exact filename, extract, and run `--version`.
Confirm npm displays provenance and GitHub displays the release/asset attestation.

## Failure and safe retry

The workflow may be rerun for the same tag. `scripts/publish-npm.mjs` behaves as follows:

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
