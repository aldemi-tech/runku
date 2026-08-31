//! Composite tenant/environment scopes.

use crate::{EnvironmentId, ProjectId};

/// Composite tenant boundary required by application data and serving configuration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EnvironmentScope {
    project_id: ProjectId,
    environment_id: EnvironmentId,
}

impl EnvironmentScope {
    /// Creates an explicit Project/Environment scope.
    #[must_use]
    pub const fn new(project_id: ProjectId, environment_id: EnvironmentId) -> Self {
        Self {
            project_id,
            environment_id,
        }
    }

    /// Returns the owning Project.
    #[must_use]
    pub const fn project_id(self) -> ProjectId {
        self.project_id
    }

    /// Returns the state-owning Environment.
    #[must_use]
    pub const fn environment_id(self) -> EnvironmentId {
        self.environment_id
    }
}
