//! Durable serving-policy repository boundary.

use async_trait::async_trait;
use runku_core::{EnvironmentScope, OperationId};

use crate::{
    ServingAuditPage, ServingAuditPageRequest, ServingCommand, ServingOperation,
    ServingOperationResult, ServingPolicyError, ServingPolicyRecord,
};

/// Physical backend selected by composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServingRepositoryBackend {
    /// Embedded local/test SQLite.
    SQLite,
    /// Authoritative PostgreSQL.
    PostgreSQL,
}

/// Bounded process-local repository telemetry without tenant labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ServingRepositoryTelemetrySnapshot {
    /// Newly committed commands.
    pub commands: u64,
    /// Exact operation-journal replays.
    pub replays: u64,
    /// CAS, operation identity, or policy conflicts.
    pub conflicts: u64,
    /// Exact policy reads.
    pub reads: u64,
    /// Exact operation lookups.
    pub operation_reads: u64,
    /// Audit page reads.
    pub audit_reads: u64,
    /// Retryable repository failures.
    pub retryable_errors: u64,
    /// Current physical pool size.
    pub pool_size: u32,
    /// Current idle physical connections.
    pub pool_idle: u32,
}

/// Durable exact-Environment serving-policy contract.
#[async_trait]
pub trait ServingPolicyRepository: Send + Sync {
    /// Returns the selected physical backend.
    fn backend(&self) -> ServingRepositoryBackend;

    /// Applies one exact-scope idempotent command and audit event atomically.
    async fn apply(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        command: &ServingCommand,
    ) -> Result<ServingOperationResult, ServingPolicyError>;

    /// Gets one exact policy without creating state on absence.
    async fn get(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Option<ServingPolicyRecord>, ServingPolicyError>;

    /// Reconciles one uncertain command by exact scope and operation identity.
    async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ServingOperation>, ServingPolicyError>;

    /// Lists one bounded immutable audit page for the exact scope.
    async fn audit(
        &self,
        scope: EnvironmentScope,
        request: ServingAuditPageRequest,
    ) -> Result<ServingAuditPage, ServingPolicyError>;

    /// Performs a lightweight backend health query.
    async fn health(&self) -> Result<(), ServingPolicyError>;

    /// Returns bounded aggregate process-local telemetry.
    fn telemetry(&self) -> ServingRepositoryTelemetrySnapshot;
}
