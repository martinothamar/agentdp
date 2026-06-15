use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DotNet {
    #[serde(default)]
    pub from_mise: bool,
    #[serde(default)]
    pub tools: Vec<String>,
}

impl DotNet {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        super::super::validate_non_empty_values("plugins.dotnet.tools", &self.tools, errors);
    }
}
