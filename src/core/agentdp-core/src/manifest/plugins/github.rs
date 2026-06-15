use serde::{Deserialize, Serialize};

use crate::provisioning::host_input::HostInputRequirements;

use super::AuthMode;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GitHub {
    pub auth: AuthMode,
    #[serde(default)]
    pub setup_git: bool,
}

impl GitHub {
    pub(super) fn host_input_requirements(&self, requirements: &mut HostInputRequirements) {
        match self.auth {
            AuthMode::Mediated => {
                requirements.allow_mediated_secret_hosts(
                    ["GITHUB_TOKEN", "GH_TOKEN", "GITHUB_PAT"],
                    ["api.github.com", "github.com", "objects.githubusercontent.com"],
                );
            }
            AuthMode::CopyFromHost => requirements.copy_custom_env(["GITHUB_TOKEN", "GH_TOKEN", "GITHUB_PAT"]),
        }
    }
}
