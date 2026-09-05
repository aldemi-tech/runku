//! Storage-independent Environment lifecycle service.

use std::{fmt, sync::Arc};

use runku_core::{EnvironmentScope, OperationId, ProjectId};
use runku_value::TimestampMicros;

use crate::{
    Environment, EnvironmentCommand, EnvironmentConfiguration, EnvironmentError,
    EnvironmentMaterializationOutcome, EnvironmentOperation, EnvironmentOperationResult,
    EnvironmentPage, EnvironmentPageRequest, EnvironmentRepository, EnvironmentRepositoryBackend,
    EnvironmentRepositoryTelemetrySnapshot,
};

/// Shared Environment lifecycle service used by local and remote adapters.
#[derive(Clone)]
pub struct EnvironmentService {
    repository: Arc<dyn EnvironmentRepository>,
}

impl fmt::Debug for EnvironmentService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnvironmentService")
            .field("backend", &self.repository.backend())
            .finish_non_exhaustive()
    }
}

impl EnvironmentService {
    /// Creates a service over one authoritative repository.
    #[must_use]
    pub fn new(repository: Arc<dyn EnvironmentRepository>) -> Self {
        Self { repository }
    }

    /// Returns the selected repository backend.
    #[must_use]
    pub fn backend(&self) -> EnvironmentRepositoryBackend {
        self.repository.backend()
    }

    /// Checks repository availability without mutating Environment state.
    ///
    /// # Errors
    ///
    /// Returns repository availability or corruption failures unchanged.
    pub async fn health(&self) -> Result<(), EnvironmentError> {
        self.repository.health().await
    }

    /// Returns bounded aggregate repository telemetry.
    #[must_use]
    pub fn telemetry(&self) -> EnvironmentRepositoryTelemetrySnapshot {
        self.repository.telemetry()
    }

    /// Creates one pending Environment record idempotently.
    ///
    /// # Errors
    ///
    /// Returns validation, conflict, availability, uncertainty, or corruption failures unchanged.
    pub async fn create(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        configuration: EnvironmentConfiguration,
        created_at: TimestampMicros,
    ) -> Result<EnvironmentOperationResult, EnvironmentError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &EnvironmentCommand::Create {
                    configuration,
                    created_at,
                },
            )
            .await
    }

    /// Replaces desired configuration using an exact revision precondition.
    ///
    /// # Errors
    ///
    /// Returns validation, CAS, availability, uncertainty, or corruption failures unchanged.
    pub async fn update(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        expected_revision: u64,
        configuration: EnvironmentConfiguration,
        updated_at: TimestampMicros,
    ) -> Result<EnvironmentOperationResult, EnvironmentError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &EnvironmentCommand::Update {
                    expected_revision,
                    configuration,
                    updated_at,
                },
            )
            .await
    }

    /// Records one materializer outcome for the exact desired revision.
    ///
    /// # Errors
    ///
    /// Returns validation, CAS, not-found, availability, or uncertainty failures unchanged.
    pub async fn materialize(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        expected_revision: u64,
        outcome: EnvironmentMaterializationOutcome,
        observed_at: TimestampMicros,
    ) -> Result<EnvironmentOperationResult, EnvironmentError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &EnvironmentCommand::Materialize {
                    expected_revision,
                    outcome,
                    observed_at,
                },
            )
            .await
    }

    /// Gets one exact Environment without side effects.
    ///
    /// # Errors
    ///
    /// Returns repository availability or corruption failures unchanged.
    pub async fn get(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Option<Environment>, EnvironmentError> {
        self.repository.get(scope).await
    }

    /// Lists one bounded Project page.
    ///
    /// # Errors
    ///
    /// Returns invalid limits, repository availability, or corruption failures unchanged.
    pub async fn list(
        &self,
        project_id: ProjectId,
        request: EnvironmentPageRequest,
    ) -> Result<EnvironmentPage, EnvironmentError> {
        self.repository.list(project_id, request).await
    }

    /// Looks up one exact-scope operation after an uncertain result.
    ///
    /// # Errors
    ///
    /// Returns repository availability or corruption failures unchanged.
    pub async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<EnvironmentOperation>, EnvironmentError> {
        self.repository.operation(scope, operation_id).await
    }
}
