# runku-value

Pure logical value model and canonical Stored Value v1 and Index Key v1 encodings. The crate is
independent of SQL, filesystems, HTTP, JavaScript runtimes, and external services.

Canonical ordering, numeric edge cases, UTF-8 object keys, nesting/container/byte limits, typed IDs,
and malformed/unknown-version rejection are compatibility/security boundaries. Existing vectors are
immutable; a format change requires a new version and migration decision.
