//! Environment classification and authoritative target policy.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CodeTarget, EnvironmentId};

/// Operational intent of an Environment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentPurpose {
    /// Interactive development and shared debugging data.
    Development,
    /// Pre-production review, either stable or live.
    Preview,
    /// Long-lived pre-production validation.
    Staging,
    /// User-facing production state.
    Production,
}

/// Server-authoritative protection level for management and target operations.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentProtection {
    /// Developer operations are allowed subject to authorization.
    Open,
    /// Destructive operations require additional administrative authorization.
    Protected,
    /// Development mutation of code/data is forbidden through ordinary workflows.
    Production,
}

/// Physical or operational location of an Environment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentLocation {
    /// Runs on the developer machine.
    Local,
    /// Operated by the Runku `SaaS` fleet.
    Managed,
    /// Operated by the customer using the Product Base.
    SelfHosted,
}

/// Trusted metadata used to validate code targeting and development sync.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EnvironmentDescriptor {
    id: EnvironmentId,
    purpose: EnvironmentPurpose,
    protection: EnvironmentProtection,
    location: EnvironmentLocation,
    workspace_targets_enabled: bool,
}

impl EnvironmentDescriptor {
    /// Creates a validated descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`EnvironmentPolicyError`] when production purpose/protection is combined with
    /// Workspace targeting.
    pub const fn new(
        id: EnvironmentId,
        purpose: EnvironmentPurpose,
        protection: EnvironmentProtection,
        location: EnvironmentLocation,
        workspace_targets_enabled: bool,
    ) -> Result<Self, EnvironmentPolicyError> {
        if workspace_targets_enabled
            && (matches!(purpose, EnvironmentPurpose::Production)
                || matches!(protection, EnvironmentProtection::Production))
        {
            return Err(EnvironmentPolicyError::ProductionWorkspaceConfiguration);
        }
        Ok(Self {
            id,
            purpose,
            protection,
            location,
            workspace_targets_enabled,
        })
    }

    /// Creates the standard local-development policy.
    #[must_use]
    pub const fn local_development(id: EnvironmentId) -> Self {
        Self {
            id,
            purpose: EnvironmentPurpose::Development,
            protection: EnvironmentProtection::Open,
            location: EnvironmentLocation::Local,
            workspace_targets_enabled: true,
        }
    }

    /// Creates the standard production policy.
    #[must_use]
    pub const fn production(id: EnvironmentId, location: EnvironmentLocation) -> Self {
        Self {
            id,
            purpose: EnvironmentPurpose::Production,
            protection: EnvironmentProtection::Production,
            location,
            workspace_targets_enabled: false,
        }
    }

    /// Returns the Environment identifier.
    #[must_use]
    pub const fn id(self) -> EnvironmentId {
        self.id
    }

    /// Returns the declared operational purpose.
    #[must_use]
    pub const fn purpose(self) -> EnvironmentPurpose {
        self.purpose
    }

    /// Returns the server-authoritative protection level.
    #[must_use]
    pub const fn protection(self) -> EnvironmentProtection {
        self.protection
    }

    /// Returns where the Environment is operated.
    #[must_use]
    pub const fn location(self) -> EnvironmentLocation {
        self.location
    }

    /// Returns whether this validated policy permits Workspace targets in principle.
    #[must_use]
    pub const fn workspace_targets_enabled(self) -> bool {
        self.workspace_targets_enabled
    }

    /// Validates a client-supplied Code Target against trusted Environment metadata.
    ///
    /// # Errors
    ///
    /// Returns [`TargetPolicyError::WorkspaceNotAllowed`] for a Workspace target when the
    /// Environment does not explicitly allow it. Release and Channel targets are accepted by this
    /// foundational policy; lifecycle/servability checks belong to the Release Router.
    pub const fn validate_target(&self, target: &CodeTarget) -> Result<(), TargetPolicyError> {
        if matches!(target, CodeTarget::Workspace(_)) && !self.workspace_targets_enabled {
            return Err(TargetPolicyError::WorkspaceNotAllowed);
        }
        Ok(())
    }

    /// Validates whether ordinary `runku dev` synchronization may target this Environment.
    ///
    /// # Errors
    ///
    /// Returns [`TargetPolicyError::DevelopmentSyncNotAllowed`] unless Workspace targeting is
    /// enabled and the Environment is not production by purpose or protection.
    pub const fn validate_development_sync(&self) -> Result<(), TargetPolicyError> {
        if !self.workspace_targets_enabled
            || matches!(self.purpose, EnvironmentPurpose::Production)
            || matches!(self.protection, EnvironmentProtection::Production)
        {
            return Err(TargetPolicyError::DevelopmentSyncNotAllowed);
        }
        Ok(())
    }
}

/// Error returned for an internally contradictory Environment policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EnvironmentPolicyError {
    /// Production purpose or protection cannot enable Workspace targeting.
    #[error("production environments cannot enable workspace targets")]
    ProductionWorkspaceConfiguration,
}

impl EnvironmentPolicyError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        "ENVIRONMENT_POLICY_INVALID"
    }

    /// Policy configuration errors require operator action and are not retryable unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

/// Error returned when a target or development operation violates Environment protection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TargetPolicyError {
    /// Workspace routing is disabled for the Environment.
    #[error("workspace code targets are not allowed for this environment")]
    WorkspaceNotAllowed,
    /// Ordinary development synchronization is forbidden.
    #[error("development synchronization is not allowed for this environment")]
    DevelopmentSyncNotAllowed,
}

impl TargetPolicyError {
    /// Stable machine-readable public error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::WorkspaceNotAllowed => "WORKSPACE_TARGET_NOT_ALLOWED",
            Self::DevelopmentSyncNotAllowed => "DEVELOPMENT_SYNC_NOT_ALLOWED",
        }
    }

    /// Policy errors require a different target or operation and are not retryable unchanged.
    #[must_use]
    pub const fn retryable(self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::{ChannelName, ReleaseId, WorkspaceRef};
    use ulid::Ulid;

    fn environment_id() -> EnvironmentId {
        EnvironmentId::from_ulid(Ulid::from(1_u128))
    }

    #[test]
    fn production_rejects_workspace_and_dev_sync() -> Result<(), Box<dyn Error>> {
        let environment =
            EnvironmentDescriptor::production(environment_id(), EnvironmentLocation::Managed);
        let workspace = CodeTarget::Workspace("debug/bug-241".parse::<WorkspaceRef>()?);

        assert_eq!(
            environment.validate_target(&workspace),
            Err(TargetPolicyError::WorkspaceNotAllowed)
        );
        assert_eq!(
            environment.validate_development_sync(),
            Err(TargetPolicyError::DevelopmentSyncNotAllowed)
        );
        Ok(())
    }

    #[test]
    fn production_accepts_release_and_channel_at_foundational_gate() -> Result<(), Box<dyn Error>> {
        let environment =
            EnvironmentDescriptor::production(environment_id(), EnvironmentLocation::SelfHosted);
        let release = CodeTarget::Release(ReleaseId::from_ulid(Ulid::from(2_u128)));
        let channel = CodeTarget::Channel("stable".parse::<ChannelName>()?);

        assert_eq!(environment.validate_target(&release), Ok(()));
        assert_eq!(environment.validate_target(&channel), Ok(()));
        Ok(())
    }

    #[test]
    fn contradictory_production_policy_cannot_be_constructed() {
        let result = EnvironmentDescriptor::new(
            environment_id(),
            EnvironmentPurpose::Preview,
            EnvironmentProtection::Production,
            EnvironmentLocation::Managed,
            true,
        );
        assert_eq!(
            result,
            Err(EnvironmentPolicyError::ProductionWorkspaceConfiguration)
        );
    }

    #[test]
    fn local_development_accepts_all_target_kinds() -> Result<(), Box<dyn Error>> {
        let environment = EnvironmentDescriptor::local_development(environment_id());
        let targets = [
            CodeTarget::Release(ReleaseId::from_ulid(Ulid::from(3_u128))),
            CodeTarget::Channel("canary".parse::<ChannelName>()?),
            CodeTarget::Workspace("local/worktree".parse::<WorkspaceRef>()?),
        ];

        for target in targets {
            assert_eq!(environment.validate_target(&target), Ok(()));
        }
        assert_eq!(environment.validate_development_sync(), Ok(()));
        Ok(())
    }

    #[test]
    fn preview_can_explicitly_disable_live_workspaces() -> Result<(), Box<dyn Error>> {
        let environment = EnvironmentDescriptor::new(
            environment_id(),
            EnvironmentPurpose::Preview,
            EnvironmentProtection::Protected,
            EnvironmentLocation::Managed,
            false,
        )?;
        let workspace = CodeTarget::Workspace("preview/stable".parse::<WorkspaceRef>()?);
        assert_eq!(
            environment.validate_target(&workspace),
            Err(TargetPolicyError::WorkspaceNotAllowed)
        );
        Ok(())
    }

    #[test]
    fn preview_can_explicitly_enable_live_workspaces() -> Result<(), Box<dyn Error>> {
        let environment = EnvironmentDescriptor::new(
            environment_id(),
            EnvironmentPurpose::Preview,
            EnvironmentProtection::Protected,
            EnvironmentLocation::Managed,
            true,
        )?;
        let workspace = CodeTarget::Workspace("preview/live".parse::<WorkspaceRef>()?);
        assert_eq!(environment.validate_target(&workspace), Ok(()));
        assert_eq!(environment.validate_development_sync(), Ok(()));
        Ok(())
    }

    #[test]
    fn environment_axes_have_stable_wire_values() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            serde_json::to_string(&EnvironmentPurpose::Development)?,
            "\"development\""
        );
        assert_eq!(
            serde_json::to_string(&EnvironmentProtection::Production)?,
            "\"production\""
        );
        assert_eq!(
            serde_json::to_string(&EnvironmentLocation::SelfHosted)?,
            "\"self_hosted\""
        );
        Ok(())
    }

    #[test]
    fn policy_errors_have_stable_codes_and_are_not_retryable() {
        assert_eq!(
            EnvironmentPolicyError::ProductionWorkspaceConfiguration.code(),
            "ENVIRONMENT_POLICY_INVALID"
        );
        assert!(!EnvironmentPolicyError::ProductionWorkspaceConfiguration.retryable());
        assert_eq!(
            TargetPolicyError::WorkspaceNotAllowed.code(),
            "WORKSPACE_TARGET_NOT_ALLOWED"
        );
        assert!(!TargetPolicyError::WorkspaceNotAllowed.retryable());
    }
}
