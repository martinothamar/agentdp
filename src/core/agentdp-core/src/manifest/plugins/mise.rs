use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Mise {
    #[serde(default)]
    pub packages: Vec<String>,
}

impl Mise {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        super::super::validate_non_empty_values("plugins.mise.packages", &self.packages, errors);
    }
}
