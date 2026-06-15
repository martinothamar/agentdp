use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Go {
    #[serde(default)]
    pub from_mise: bool,
    #[serde(default)]
    pub tools: Vec<String>,
}

impl Go {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        super::super::validate_non_empty_values("plugins.go.tools", &self.tools, errors);
    }
}
