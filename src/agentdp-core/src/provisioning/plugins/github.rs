use crate::manifest::plugins::AuthMode;
use crate::manifest::plugins::github::GitHub;

use super::Plugin;
use crate::provisioning::bootstrap::{CUSTOM_ENV_PATH, ProvisioningBuilder};
use crate::provisioning::shell;

impl Plugin for GitHub {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        builder.add_package("github-cli");
        match self.auth {
            AuthMode::CopyFromHost if self.setup_git => builder.add_agent_shell(
                "if [ -f \"$HOME/.config/gh/hosts.yml\" ]; then gh auth setup-git >/dev/null || true; fi",
            ),
            AuthMode::Mediated if self.setup_git => {
                builder.add_root_shell(render_mediated_setup_git());
            }
            _ => {}
        }
    }
}

fn render_mediated_setup_git() -> String {
    let mut command = shell::ShellScript::new();
    command.line("install -d -m 0700 \"$HOME/.config/gh\"");
    command.line(format!(
        "if [ -n \"${{{}:-}}\" ] && ! gh auth status -h github.com >/dev/null 2>&1; then",
        "GITHUB_PAT"
    ));
    command.line("  printf '%s\\n' \"$GITHUB_PAT\" | gh auth login --with-token");
    command.line("fi");
    command.line("if [ -f \"$HOME/.config/gh/hosts.yml\" ]; then");
    command.line("  gh auth setup-git >/dev/null || true");
    command.line("fi");

    let mut script = shell::ShellScript::new();
    script.line(format!("AGENTDP_CUSTOM_ENV={}", shell::single_quote(CUSTOM_ENV_PATH)));
    script.line("if [ -f \"$AGENTDP_CUSTOM_ENV\" ]; then");
    script.line("  set -a");
    script.line("  # shellcheck source=/dev/null");
    script.line("  . \"$AGENTDP_CUSTOM_ENV\"");
    script.line("  set +a");
    script.line("fi");
    script.line(format!("run_agent {}", shell::single_quote(&command.render())));
    script.line("unset GITHUB_PAT");
    script.render()
}
