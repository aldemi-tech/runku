//! Authoritative `PostgreSQL` adapter for Runku's `LogicalStore`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;
mod migration;

pub use adapter::{PostgresStore, PostgresStoreConfig};
