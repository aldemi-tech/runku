//! Durable Release Repository implemented over `SQLite` and `PostgreSQL`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;

pub use adapter::{RepositoryConfig, RepositoryRole, SqlReleaseRepository};
