use serde::{Deserialize, Serialize};

use crate::provisioning::host_input::{HostInputFile, HostInputFileSource, HostInputGuestPath, HostInputRequirements};

use super::AuthMode;
use super::mediated_json_auth::MediatedJsonAuthTransform;

const CODEX_AUTH_PATH_ENV: &str = "AGENTDP_CODEX_AUTH_PATH";
const CODEX_HOME_ENV: &str = "CODEX_HOME";
const CODEX_AUTH_GUEST_PATH: &str = ".codex/auth.json";
const CODEX_AUTH_HOME_PATH: &str = "auth.json";
const CODEX_AUTH_DEFAULT_HOME_PATH: &str = ".codex/auth.json";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Codex {
    #[serde(default)]
    pub yolo: bool,
    #[serde(default = "default_version")]
    pub version: String,
    #[serde(default)]
    pub session: CodexSession,
    pub auth: AuthMode,
    pub auth_source: Option<CodexAuthSource>,
}

impl Codex {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        super::super::validate_non_empty("plugins.codex.version", &self.version, errors);
    }

    pub(super) fn host_input_requirements(&self, requirements: &mut HostInputRequirements) {
        match self.auth {
            AuthMode::Mediated => match self.auth_source.unwrap_or_default() {
                CodexAuthSource::HostAuth => {
                    requirements.add_file(HostInputFile::with_transform(
                        "Codex auth",
                        codex_auth_source(),
                        codex_auth_guest_path(),
                        "0600",
                        &CODEX_AUTH_TRANSFORM,
                    ));
                }
                CodexAuthSource::Env => requirements.allow_mediated_secret_hosts(
                    ["OPENAI_API_KEY", "CODEX_API_KEY"],
                    ["api.openai.com", "chatgpt.com"],
                ),
            },
            AuthMode::CopyFromHost => {
                requirements.copy_custom_env(["OPENAI_API_KEY", "CODEX_API_KEY"]);
                requirements.add_file(HostInputFile::copy(
                    "Codex auth",
                    codex_auth_source(),
                    codex_auth_guest_path(),
                    "0600",
                ));
            }
        }
    }
}

fn default_version() -> String {
    "latest".to_owned()
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CodexSession {
    #[default]
    Guestd,
    None,
}

static CODEX_AUTH_TRANSFORM: MediatedJsonAuthTransform = MediatedJsonAuthTransform {
    name: "codex-auth",
    prefix: "CODEX_AUTH",
    hosts: &["api.openai.com", "chatgpt.com"],
    normalize_expiry: true,
};

fn codex_auth_guest_path() -> HostInputGuestPath {
    HostInputGuestPath::AgentHomeRelative(CODEX_AUTH_GUEST_PATH.to_owned())
}

fn codex_auth_source() -> HostInputFileSource {
    HostInputFileSource::HomeRelative {
        path_env: Some(CODEX_AUTH_PATH_ENV.to_owned()),
        home_env: Some(CODEX_HOME_ENV.to_owned()),
        home_relative_path: CODEX_AUTH_HOME_PATH.to_owned(),
        default_home_relative_path: CODEX_AUTH_DEFAULT_HOME_PATH.to_owned(),
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CodexAuthSource {
    #[default]
    HostAuth,
    Env,
}
