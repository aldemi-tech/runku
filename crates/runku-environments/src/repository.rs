//! Durable Environment repository boundary.

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId, ProjectId};

use crate::{
    Environment, EnvironmentCommand, EnvironmentError, EnvironmentOperation,
    EnvironmentOperationResult, EnvironmentPage, EnvironmentPageRequest,
};

/// Physical backend selected by composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvironmentRepositoryBackend {
    /// Embedded local/test SQLite.
    SQLite,
    /// Authoritative PostgreSQL.
    PostgreSQL,
}

/// Bounded process-local Environment repository telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EnvironmentRepositoryTelemetrySnapshot {
    /// Newly committed commands.
    pub commands: u64,
    /// Exact operation-journal replays.
    pub replays: u64,
    /// CAS, slug, or operation-identity conflicts.
    pub conflicts: u64,
    /// Exact Environment reads.
    pub reads: u64,
    /// Project list queries.
    pub lists: u64,
    /// Operation lookup queries.
    pub operation_reads: u64,
    /// Retryable repository failures.
    pub retryable_errors: u64,
    /// Current physical pool size.
    pub pool_size: u32,
    /// Current idle physical connections.
    pub pool_idle: u32,
}

/// Durable Environment registry contract.
#[async_trait]
pub trait EnvironmentRepository: Send + Sync {
    /// Returns the selected physical backend.
    fn backend(&self) -> EnvironmentRepositoryBackend;

    /// Applies one exact-scope idempotent command atomically.
    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &EnvironmentCommand,
    ) -> Result<EnvironmentOperationResult, EnvironmentError>;

    /// Gets one exact Project/Environment without creating state on absence.
    async fn get(&self, scope: EnvironmentScope) -> Result<Option<Environment>, EnvironmentError>;

    /// Lists one bounded stable page for an exact Project.
    async fn list(
        &self,
        project_id: ProjectId,
        request: EnvironmentPageRequest,
    ) -> Result<EnvironmentPage, EnvironmentError>;

    /// Reconciles one uncertain command by exact scope and operation identity.
    async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<EnvironmentOperation>, EnvironmentError>;

    /// Performs a lightweight backend health query.
    async fn health(&self) -> Result<(), EnvironmentError>;

    /// Returns bounded process-local telemetry.
    fn telemetry(&self) -> EnvironmentRepositoryTelemetrySnapshot;
}
