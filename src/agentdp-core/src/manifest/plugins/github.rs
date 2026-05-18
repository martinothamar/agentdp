use serde::Deserialize;

use super::AuthMode;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GitHub {
    pub auth: AuthMode,
    #[serde(default)]
    pub setup_git: bool,
}
