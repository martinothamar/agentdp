use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CodeServer {
    pub settings: Option<String>,
    #[serde(default)]
    pub extensions: Vec<String>,
    #[serde(default)]
    pub remove_extensions: Vec<String>,
    #[serde(default)]
    pub trusted_domains: Vec<String>,
    #[serde(default)]
    pub restart_after_bootstrap: bool,
}

impl CodeServer {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        if let Some(settings) = &self.settings {
            super::super::validate_relative_path("plugins.code_server.settings", settings, errors);
        }
        super::super::validate_non_empty_values("plugins.code_server.extensions", &self.extensions, errors);
        super::super::validate_non_empty_values(
            "plugins.code_server.remove_extensions",
            &self.remove_extensions,
            errors,
        );
        super::super::validate_non_empty_values("plugins.code_server.trusted_domains", &self.trusted_domains, errors);
    }
}
