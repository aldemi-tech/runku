//! Local-only `SQLite` adapter for Runku's `LogicalStore`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;
mod migration;

pub use adapter::{
    EnvironmentExportV1, ExportedOutboxRecord, SqliteRole, SqliteStore, SqliteStoreConfig,
};
