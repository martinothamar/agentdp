use crate::manifest::GuestOs;
use crate::manifest::plugins::AuthMode;
use crate::manifest::plugins::github::GitHub;

use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

pub(super) fn apply(plugin: &GitHub, builder: &mut ProvisioningBuilder<'_>) {
    install_github_cli(builder);
    match plugin.auth {
        AuthMode::Mediated | AuthMode::CopyFromHost if plugin.setup_git => {
            builder.add_instance_user_step(
                "plugin.github.setup_git",
                "Configure GitHub git credentials",
                ["system.agent_user"],
                [BootstrapStepResource::AgentHome],
                render_mediated_setup_git(builder.runtime_env_path()),
            );
        }
        AuthMode::Mediated | AuthMode::CopyFromHost => {}
    }
}

fn install_github_cli(builder: &mut ProvisioningBuilder<'_>) {
    match builder.guest_os() {
        GuestOs::Archlinux => builder.add_package("github-cli"),
        GuestOs::Rocky9 => builder.add_base_system_step(
            "plugin.github.cli",
            "Install GitHub CLI",
            ["system.packages"],
            [BootstrapStepResource::PackageManager],
            render_rocky_github_cli_install(),
        ),
    }
}

fn render_rocky_github_cli_install() -> String {
    let mut script = shell::ShellScript::new();
    script.line("if ! command -v gh >/dev/null 2>&1; then");
    script.line("  dnf -y install dnf-plugins-core");
    script.line("  dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo || true");
    script.line("  dnf -y install gh");
    script.line("fi");
    script.render()
}

fn render_mediated_setup_git(custom_env_path: &str) -> String {
    let mut command = shell::ShellScript::new();
    command.line("install -d -m 0700 \"$HOME/.config/gh\"");
    command.line(shell_expand_line("agentdp_gh_token=\"", "GITHUB_PAT", "\""));
    command.line(shell_expand_line(
        "if [ -n \"",
        "GH_TOKEN",
        "\" ]; then agentdp_gh_token=\"$GH_TOKEN\"; fi",
    ));
    command.line(shell_expand_line(
        "if [ -n \"",
        "GITHUB_TOKEN",
        "\" ]; then agentdp_gh_token=\"$GITHUB_TOKEN\"; fi",
    ));
    command.line("if [ -n \"$agentdp_gh_token\" ] && ! gh auth status -h github.com >/dev/null 2>&1; then");
    command.line("  printf '%s\\n' \"$agentdp_gh_token\" | gh auth login --with-token");
    command.line("fi");
    command.line("if [ -f \"$HOME/.config/gh/hosts.yml\" ]; then");
    command.line("  gh auth setup-git >/dev/null || true");
    command.line("fi");
    command.line("git config --global credential.https://github.com.username x-access-token || true");
    command.line(
        "git config --global credential.https://github.com.helper '!f() { test \"$1\" = get || exit 0; token=\"$GITHUB_TOKEN\"; if [ -z \"$token\" ]; then token=\"$GH_TOKEN\"; fi; if [ -z \"$token\" ]; then token=\"$GITHUB_PAT\"; fi; test -n \"$token\" || exit 0; printf \"%s\\n\" \"username=x-access-token\" \"password=$token\"; }; f' || true",
    );
    command.line("git config --global url.https://github.com/.insteadOf git@github.com: || true");
    command.line("unset agentdp_gh_token");

    let mut script = shell::ShellScript::new();
    script.line(format!("custom_env={}", shell::single_quote(custom_env_path)));
    script.line("if [ -f \"$custom_env\" ]; then");
    script.line("  set -a");
    script.line("  # shellcheck source=/dev/null");
    script.line("  . \"$custom_env\"");
    script.line("  set +a");
    script.line("fi");
    script.block(&command.render());
    script.line("unset GITHUB_TOKEN GH_TOKEN GITHUB_PAT");
    script.line("unset custom_env");
    script.render()
}

fn shell_expand_line(prefix: &str, name: &str, suffix: &str) -> String {
    format!("{prefix}${{{name}:-}}{suffix}")
}
