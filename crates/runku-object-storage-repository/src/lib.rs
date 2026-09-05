//! Checksum-migrated SQLite/PostgreSQL logical Object Storage repository.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;

pub use adapter::{ObjectStorageRepositoryConfig, RepositoryRole, SqlObjectStorageRepository};
