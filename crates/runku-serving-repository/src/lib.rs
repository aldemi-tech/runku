//! Durable weighted serving-policy registry over SQLite and PostgreSQL.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;

pub use adapter::{ServingRepositoryConfig, ServingRepositoryRole, SqlServingPolicyRepository};
