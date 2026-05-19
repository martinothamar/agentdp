use super::Plugin;
use crate::manifest::plugins::{codex::Codex, github::GitHub};
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::{shell, templates};

pub(super) struct PrLoop<'a> {
    _codex: &'a Codex,
    _github: &'a GitHub,
}

impl<'a> PrLoop<'a> {
    pub(super) const fn new(codex: &'a Codex, github: &'a GitHub) -> Self {
        Self {
            _codex: codex,
            _github: github,
        }
    }
}

impl Plugin for PrLoop<'_> {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        builder.add_package("mise");
        builder.add_package("tmux");
        builder.add_root_shell(format!(
            "if command -v loginctl >/dev/null 2>&1; then\n  loginctl enable-linger {} || true\nfi",
            shell::single_quote(&builder.agent_user().name)
        ));
        builder.add_agent_shell(format!("mise use --global {}", shell::single_quote("node@lts")));
        builder.add_agent_shell(install_pr_loop_tools());
        builder.add_agent_shell(
            "agentdp-codex-session\nexport XDG_RUNTIME_DIR=\"${XDG_RUNTIME_DIR:-/run/user/$(id -u)}\"\nsystemctl --user daemon-reload || true\nsystemctl --user enable --now agentdp-codex-session.service agentdp-pr-subscriber.service || true",
        );
    }
}

fn install_pr_loop_tools() -> String {
    let scripts = [
        ("agentdp-codex-session", templates::AGENTDP_CODEX_SESSION),
        ("agentdp-pr", templates::AGENTDP_PR),
        ("agentdp-pr-subscriber", templates::AGENTDP_PR_SUBSCRIBER),
    ];

    let mut script = shell::ShellScript::new();
    script
        .line("install -d -m 0755 \"$HOME/.local/bin\" \"$HOME/.config/systemd/user\" \"$HOME/.local/state/agentdp\"");
    for (name, contents) in scripts {
        script.line(format!("cat >\"$HOME/.local/bin/{name}\" <<'EOF'"));
        script.block(contents);
        script.line("EOF");
        script.line(format!("chmod 0755 \"$HOME/.local/bin/{name}\""));
    }
    let services = [
        (
            "agentdp-codex-session.service",
            templates::AGENTDP_CODEX_SESSION_SERVICE,
        ),
        (
            "agentdp-pr-subscriber.service",
            templates::AGENTDP_PR_SUBSCRIBER_SERVICE,
        ),
    ];
    for (name, contents) in services {
        script.line(format!("cat >\"$HOME/.config/systemd/user/{name}\" <<'EOF'"));
        script.block(contents);
        script.line("EOF");
    }
    script.render()
}
