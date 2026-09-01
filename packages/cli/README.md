# `@runku/cli`

Installs the native Runku command-line interface for the current operating system and architecture.
The package contains only a small Node.js launcher; the executable is supplied by an exact-version
platform package from the same Runku release.

```sh
npm install --global @runku/cli
runku --version
```

Supported combinations are macOS, Linux GNU, and Windows on ARM64 and x86_64. Node.js is required
to launch an npm installation. Direct archives from the matching GitHub Release execute without
Node.js.

Do not install with optional dependencies disabled. If an installation policy omits optional
dependencies, use a direct release archive and verify it against `SHA256SUMS`.

Update or remove the global installation with:

```sh
npm update --global @runku/cli
npm uninstall --global @runku/cli
```

The CLI manages durable application state. Updating the executable never migrates or deletes an
application's `.runku/` directory automatically. Read the release notes and compatibility matrix
before changing versions.
