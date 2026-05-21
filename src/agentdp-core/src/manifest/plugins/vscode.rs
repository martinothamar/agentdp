use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VsCode {
    pub settings: Option<String>,
    #[serde(default)]
    pub extensions: Vec<String>,
    #[serde(default)]
    pub trusted_domains: Vec<String>,
    #[serde(default)]
    pub restart_after_bootstrap: bool,
}

impl VsCode {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        if let Some(settings) = &self.settings {
            super::super::validate_relative_path("plugins.vscode.settings", settings, errors);
        }
        super::super::validate_non_empty_values("plugins.vscode.extensions", &self.extensions, errors);
        super::super::validate_non_empty_values("plugins.vscode.trusted_domains", &self.trusted_domains, errors);
    }
}
