# Runku CLI archive

This archive contains the native `runku` executable for the operating system and architecture in
the filename.

1. Verify the archive against `SHA256SUMS` from the same GitHub Release.
2. Extract the archive.
3. Move `runku` (`runku.exe` on Windows) to a directory on `PATH`.
4. Run `runku --version` and confirm that it matches the Release version.

The executable does not update or remove `.runku/` application state automatically. Read the
Release notes and compatibility documentation before upgrading or downgrading.
