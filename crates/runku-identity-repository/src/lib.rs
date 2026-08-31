//! Durable SQL adapters for Runku Application Clients and keyrings.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapter;

pub use adapter::{IdentityRepositoryConfig, RepositoryRole, SqlApplicationIdentityRepository};
