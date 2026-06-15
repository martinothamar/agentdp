use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Browser {
    pub playwright: Option<Playwright>,
}

impl Browser {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        if let Some(playwright) = &self.playwright {
            playwright.validate(errors);
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Playwright {
    #[serde(default)]
    pub install: PlaywrightInstall,
    #[serde(default = "default_browser_package")]
    pub browser_package: String,
    #[serde(default = "default_executable_path")]
    pub executable_path: String,
    #[serde(default = "default_npm_packages")]
    pub npm_packages: Vec<String>,
    #[serde(default = "default_mcp_package")]
    pub mcp_package: String,
    #[serde(default = "default_viewport")]
    pub viewport: String,
    #[serde(default = "default_codex_mcp")]
    pub codex_mcp: bool,
}

impl Playwright {
    fn validate(&self, errors: &mut Vec<String>) {
        super::super::validate_non_empty(
            "plugins.browser.playwright.browser_package",
            &self.browser_package,
            errors,
        );
        super::super::validate_non_empty(
            "plugins.browser.playwright.executable_path",
            &self.executable_path,
            errors,
        );
        super::super::validate_non_empty_values("plugins.browser.playwright.npm_packages", &self.npm_packages, errors);
        super::super::validate_non_empty("plugins.browser.playwright.mcp_package", &self.mcp_package, errors);
        super::super::validate_non_empty("plugins.browser.playwright.viewport", &self.viewport, errors);
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PlaywrightInstall {
    #[default]
    NpmGlobal,
}

fn default_browser_package() -> String {
    "chromium".to_owned()
}

fn default_executable_path() -> String {
    "/usr/bin/chromium".to_owned()
}

fn default_npm_packages() -> Vec<String> {
    vec!["playwright@latest".to_owned(), "@playwright/test@latest".to_owned()]
}

fn default_mcp_package() -> String {
    "@playwright/mcp@latest".to_owned()
}

fn default_viewport() -> String {
    "1440x900".to_owned()
}

const fn default_codex_mcp() -> bool {
    true
}
