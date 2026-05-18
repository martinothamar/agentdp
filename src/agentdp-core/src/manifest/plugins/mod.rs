pub mod codex;
pub mod docker;
pub mod github;
pub mod mise;
pub mod vscode;

use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Plugins {
    pub docker: Option<docker::Docker>,
    pub mise: Option<mise::Mise>,
    pub codex: Option<codex::Codex>,
    pub github: Option<github::GitHub>,
    pub vscode: Option<vscode::VsCode>,
}

impl Plugins {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        if let Some(mise) = &self.mise {
            mise.validate(errors);
        }
        if let Some(vscode) = &self.vscode {
            vscode.validate(errors);
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    Mediated,
    CopyFromHost,
}
