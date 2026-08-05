use serde::{Deserialize, Serialize};

use crate::provisioning::host_input::{HostInputFile, HostInputFileSource, HostInputGuestPath, HostInputRequirements};

use super::AuthMode;
use super::mediated_json_auth::MediatedJsonAuthTransform;

const CLAUDE_AUTH_PATH_ENV: &str = "AGENTDP_CLAUDE_AUTH_PATH";
const CLAUDE_CONFIG_DIR_ENV: &str = "CLAUDE_CONFIG_DIR";
const CLAUDE_AUTH_GUEST_PATH: &str = ".claude/.credentials.json";
const CLAUDE_AUTH_HOME_PATH: &str = ".credentials.json";
const CLAUDE_AUTH_DEFAULT_HOME_PATH: &str = ".claude/.credentials.json";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Claude {
    #[serde(default)]
    pub yolo: bool,
    pub auth: AuthMode,
    pub auth_source: Option<ClaudeAuthSource>,
}

impl Claude {
    pub(super) fn host_input_requirements(&self, requirements: &mut HostInputRequirements) {
        match self.auth {
            AuthMode::Mediated => match self.auth_source.unwrap_or_default() {
                ClaudeAuthSource::HostAuth => {
                    requirements.add_file(HostInputFile::with_transform(
                        "Claude auth",
                        claude_auth_source(),
                        claude_auth_guest_path(),
                        "0600",
                        &CLAUDE_AUTH_TRANSFORM,
                    ));
                }
                ClaudeAuthSource::Env => {
                    requirements.allow_mediated_secret_hosts(
                        ["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"],
                        CLAUDE_AUTH_HOSTS,
                    );
                }
            },
            AuthMode::CopyFromHost => {
                requirements.copy_custom_env(["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"]);
                requirements.add_file(HostInputFile::copy(
                    "Claude auth",
                    claude_auth_source(),
                    claude_auth_guest_path(),
                    "0600",
                ));
            }
        }
    }
}

const CLAUDE_AUTH_HOSTS: [&str; 3] = ["api.anthropic.com", "console.anthropic.com", "claude.ai"];

static CLAUDE_AUTH_TRANSFORM: MediatedJsonAuthTransform = MediatedJsonAuthTransform {
    name: "claude-auth",
    prefix: "CLAUDE_AUTH",
    hosts: &CLAUDE_AUTH_HOSTS,
    normalize_expiry: false,
    jwt_access_token_placeholder: false,
    omit_refresh_token: false,
    managed_credential: None,
};

fn claude_auth_guest_path() -> HostInputGuestPath {
    HostInputGuestPath::AgentHomeRelative(CLAUDE_AUTH_GUEST_PATH.to_owned())
}

fn claude_auth_source() -> HostInputFileSource {
    HostInputFileSource::HomeRelative {
        path_env: Some(CLAUDE_AUTH_PATH_ENV.to_owned()),
        home_env: Some(CLAUDE_CONFIG_DIR_ENV.to_owned()),
        home_relative_path: CLAUDE_AUTH_HOME_PATH.to_owned(),
        default_home_relative_path: CLAUDE_AUTH_DEFAULT_HOME_PATH.to_owned(),
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ClaudeAuthSource {
    #[default]
    HostAuth,
    Env,
}
