use serde::Deserialize;

use super::AuthMode;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Codex {
    #[serde(default)]
    pub yolo: bool,
    pub auth: AuthMode,
}
