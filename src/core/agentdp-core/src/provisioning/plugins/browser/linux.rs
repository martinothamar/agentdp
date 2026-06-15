use crate::manifest::GuestOs;
use crate::manifest::plugins::browser::{Browser, Playwright, PlaywrightInstall};

use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

const PLAYWRIGHT_MCP_COMMAND: &str = "playwright-mcp";

pub(super) fn apply(plugin: &Browser, builder: &mut ProvisioningBuilder<'_>) {
    let Some(playwright) = &plugin.playwright else {
        return;
    };
    playwright.apply(builder);
}

impl Playwright {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        let browser_dependency = install_browser_packages(builder, self);
        builder.require_mise_package("node@lts");
        builder.add_agent_shell_env(render_playwright_env(self));

        match self.install {
            PlaywrightInstall::NpmGlobal => {
                builder.add_base_user_step(
                    "plugin.browser.playwright_npm",
                    "Install Playwright npm tools",
                    [browser_dependency, "plugin.mise"],
                    [BootstrapStepResource::AgentHome, BootstrapStepResource::NpmGlobal],
                    render_npm_global_install(self),
                );
            }
        }
    }
}

fn install_browser_packages(builder: &mut ProvisioningBuilder<'_>, playwright: &Playwright) -> &'static str {
    match builder.guest_os() {
        GuestOs::Archlinux => {
            builder.add_package(playwright.browser_package.clone());
            builder.add_package("noto-fonts");
            "system.packages"
        }
        GuestOs::Rocky9 => {
            builder.add_base_system_step(
                "plugin.browser.rocky",
                "Install Rocky browser packages",
                ["system.packages"],
                [BootstrapStepResource::PackageManager],
                render_rocky_browser_install(playwright),
            );
            "plugin.browser.rocky"
        }
    }
}

fn render_rocky_browser_install(playwright: &Playwright) -> String {
    let mut script = shell::ShellScript::new();
    script.line("dnf -y install epel-release || dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm");
    script.line(format!(
        "dnf -y install {} google-noto-sans-fonts",
        shell::single_quote(&playwright.browser_package)
    ));
    script.line("if [ ! -x /usr/bin/chromium ] && [ -x /usr/bin/chromium-browser ]; then ln -sf /usr/bin/chromium-browser /usr/bin/chromium; fi");
    script.render()
}

pub(super) fn apply_codex_integration(
    plugins: &crate::manifest::plugins::Plugins,
    builder: &mut ProvisioningBuilder<'_>,
) {
    let Some(browser) = &plugins.browser else {
        return;
    };
    let Some(playwright) = &browser.playwright else {
        return;
    };
    if playwright.codex_mcp && plugins.codex.is_some() {
        builder.add_instance_user_step(
            "plugin.browser.codex_mcp",
            "Configure Codex Playwright MCP",
            ["plugin.codex.config", "plugin.browser.playwright_npm"],
            [BootstrapStepResource::AgentHome],
            render_codex_mcp_config(playwright),
        );
    }
}

fn render_playwright_env(playwright: &Playwright) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "export PLAYWRIGHT_MCP_EXECUTABLE_PATH={}",
        shell::single_quote(&playwright.executable_path)
    ));
    script.line(format!(
        "export PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH={}",
        shell::single_quote(&playwright.executable_path)
    ));
    script.line("export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1");
    script.render()
}

fn render_npm_global_install(playwright: &Playwright) -> String {
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "if [ ! -x {} ]; then",
        shell::single_quote(&playwright.executable_path)
    ));
    script.line(format!(
        "  echo {} >&2",
        shell::single_quote(&format!(
            "{} is missing; Playwright requires plugins.browser.playwright.browser_package",
            playwright.executable_path
        ))
    ));
    script.line("  exit 1");
    script.line("fi");
    script.line("npm config set prefix \"$HOME/.local\"");
    script.line("npm install -g \\");
    let mut packages = playwright.npm_packages.clone();
    if !packages.iter().any(|package| package == &playwright.mcp_package) {
        packages.push(playwright.mcp_package.clone());
    }
    script.line(format!(
        "  {}",
        packages
            .iter()
            .map(|package| shell::single_quote(package))
            .collect::<Vec<_>>()
            .join(" ")
    ));
    script.line("node -e \"const fs = require('fs'); const browser = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH; if (!browser || !fs.existsSync(browser)) { process.exit(1); }\"");
    script.line(format!("{PLAYWRIGHT_MCP_COMMAND} --help >/dev/null"));
    script.render()
}

fn render_codex_mcp_config(playwright: &Playwright) -> String {
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 \"$HOME/.codex\"");
    script.line("config=\"$HOME/.codex/config.toml\"");
    script.line("touch \"$config\"");
    script.line("tmp=\"$(mktemp)\"");
    script.line("awk '");
    script.line("  /^\\[mcp_servers\\.playwright\\]$/ { skip = 1; next }");
    script.line("  skip && /^\\[/ { skip = 0 }");
    script.line("  !skip { print }");
    script.line("' \"$config\" >\"$tmp\"");
    script.line("cat \"$tmp\" >\"$config\"");
    script.line("rm -f \"$tmp\"");
    script.line("cat >>\"$config\" <<'EOF'");
    script.blank();
    script.line("[mcp_servers.playwright]");
    script.line(format!("command = {}", toml_string(PLAYWRIGHT_MCP_COMMAND)));
    script.line("startup_timeout_sec = 120");
    script.line(format!(
        "args = [\"--browser=chromium\", {}, {}]",
        toml_string(&format!("--executable-path={}", playwright.executable_path)),
        toml_string(&format!("--viewport-size={}", playwright.viewport))
    ));
    script.line(format!(
        "env = {{ PLAYWRIGHT_MCP_EXECUTABLE_PATH = {}, PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH = {}, PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = \"1\", PLAYWRIGHT_MCP_VIEWPORT_SIZE = {} }}",
        toml_string(&playwright.executable_path),
        toml_string(&playwright.executable_path),
        toml_string(&playwright.viewport)
    ));
    script.line("EOF");
    script.render()
}

fn toml_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}
