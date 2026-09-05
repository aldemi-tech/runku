//! Storage-independent serving-policy service.

use std::{fmt, sync::Arc};

use runku_core::{EnvironmentScope, OperationId, OperatorId};
use runku_value::TimestampMicros;

use crate::{
    ServingAuditPage, ServingAuditPageRequest, ServingCommand, ServingMaterializationOutcome,
    ServingOperation, ServingOperationResult, ServingPolicy, ServingPolicyError,
    ServingPolicyRecord, ServingPolicyRepository, ServingRepositoryBackend,
    ServingRepositoryTelemetrySnapshot,
};

/// Shared serving-policy service used by provider-independent compositions.
#[derive(Clone)]
pub struct ServingPolicyService {
    repository: Arc<dyn ServingPolicyRepository>,
}

impl fmt::Debug for ServingPolicyService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServingPolicyService")
            .field("backend", &self.repository.backend())
            .finish_non_exhaustive()
    }
}

impl ServingPolicyService {
    /// Creates a service over one authoritative repository.
    #[must_use]
    pub fn new(repository: Arc<dyn ServingPolicyRepository>) -> Self {
        Self { repository }
    }

    /// Returns the selected repository backend.
    #[must_use]
    pub fn backend(&self) -> ServingRepositoryBackend {
        self.repository.backend()
    }

    /// Creates or replaces the complete desired policy using exact revision CAS.
    ///
    /// # Errors
    ///
    /// Returns validation, compatibility, conflict, availability, or uncertainty failures.
    pub async fn set_desired(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        actor: OperatorId,
        expected_revision: Option<u64>,
        policy: ServingPolicy,
        changed_at: TimestampMicros,
    ) -> Result<ServingOperationResult, ServingPolicyError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &ServingCommand::SetDesired {
                    actor,
                    expected_revision,
                    policy,
                    changed_at,
                },
            )
            .await
    }

    /// Records application of one exact desired policy revision by a trusted materializer.
    ///
    /// # Errors
    ///
    /// Returns validation, CAS, not-found, availability, or uncertainty failures.
    pub async fn materialize(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
        expected_revision: u64,
        outcome: ServingMaterializationOutcome,
        observed_at: TimestampMicros,
    ) -> Result<ServingOperationResult, ServingPolicyError> {
        self.repository
            .apply(
                scope,
                operation_id,
                &ServingCommand::Materialize {
                    expected_revision,
                    outcome,
                    observed_at,
                },
            )
            .await
    }

    /// Gets one exact desired/observed policy without side effects.
    ///
    /// # Errors
    ///
    /// Returns repository availability or corruption failures.
    pub async fn get(
        &self,
        scope: EnvironmentScope,
    ) -> Result<Option<ServingPolicyRecord>, ServingPolicyError> {
        self.repository.get(scope).await
    }

    /// Looks up one exact-scope operation after an uncertain result.
    ///
    /// # Errors
    ///
    /// Returns repository availability or corruption failures.
    pub async fn operation(
        &self,
        scope: EnvironmentScope,
        operation_id: OperationId,
    ) -> Result<Option<ServingOperation>, ServingPolicyError> {
        self.repository.operation(scope, operation_id).await
    }

    /// Lists one bounded exact-scope immutable audit page.
    ///
    /// # Errors
    ///
    /// Returns invalid limits, repository availability, or corruption failures.
    pub async fn audit(
        &self,
        scope: EnvironmentScope,
        request: ServingAuditPageRequest,
    ) -> Result<ServingAuditPage, ServingPolicyError> {
        self.repository.audit(scope, request).await
    }

    /// Checks repository availability without mutating policy state.
    ///
    /// # Errors
    ///
    /// Returns availability or corruption failures.
    pub async fn health(&self) -> Result<(), ServingPolicyError> {
        self.repository.health().await
    }

    /// Returns bounded aggregate repository telemetry.
    #[must_use]
    pub fn telemetry(&self) -> ServingRepositoryTelemetrySnapshot {
        self.repository.telemetry()
    }
}
