use crate::manifest::plugins::codex::{Codex, CodexSession};
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

const GUESTCTL_SKILL: &str = include_str!("../resources/guestctl-skill.md");

pub(super) fn apply(plugin: &Codex, builder: &mut ProvisioningBuilder<'_>) {
    builder.require_mise_package("node@lts");
    let code_dir = builder.code_dir();
    builder.add_base_user_step(
        "plugin.codex.install",
        "Install Codex",
        ["plugin.mise"],
        [BootstrapStepResource::AgentHome, BootstrapStepResource::NpmGlobal],
        render_codex_install(plugin.yolo, &plugin.version),
    );
    let mut config = shell::ShellScript::new();
    config.block(&trust_code_dir(code_dir));
    config.block(&install_guestctl_skill());
    builder.add_instance_user_step(
        "plugin.codex.config",
        "Configure Codex",
        ["plugin.codex.install"],
        [BootstrapStepResource::AgentHome],
        config.render(),
    );
    if plugin.session == CodexSession::Guestd {
        builder.add_base_system_step(
            "plugin.codex.session",
            "Configure Codex session service",
            ["system.guest_tooling"],
            [BootstrapStepResource::Systemd],
            enable_daemon_codex_session_management(),
        );
    }
}

fn trust_code_dir(code_dir: &str) -> String {
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 \"$HOME/.codex\"");
    script.line("config=\"$HOME/.codex/config.toml\"");
    script.line("touch \"$config\"");
    script.line(format!(
        "if ! grep -Fqx {} \"$config\"; then",
        shell::single_quote(&format!("[projects.\"{code_dir}\"]"))
    ));
    script.line("  {");
    script.line("    printf '\\n'");
    script.line(format!(
        "    printf '%s\\n' {}",
        shell::single_quote(&format!("[projects.\"{code_dir}\"]"))
    ));
    script.line("    printf '%s\\n' 'trust_level = \"trusted\"'");
    script.line("  } >>\"$config\"");
    script.line("fi");
    script.render()
}

fn render_codex_install(yolo: bool, version: &str) -> String {
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 \"$HOME/.local/bin\" \"$HOME/.local/share/agentdp/codex\"");
    // A normal prefix install hoists the platform package beside @openai/codex,
    // making this directory usable as VS Code's Codex SDK root as well as the
    // source of the CLI. npm's global layout nests that package too deeply.
    script.line(format!(
        "npm install --prefix \"$HOME/.local/share/agentdp/codex\" {}",
        shell::single_quote(&format!("@openai/codex@{version}"))
    ));
    if yolo {
        script.block(CODEX_WRAPPER);
    } else {
        script.line(
            "ln -sf \"$HOME/.local/share/agentdp/codex/node_modules/@openai/codex/bin/codex.js\" \"$HOME/.local/bin/codex\"",
        );
    }
    script.render()
}

const CODEX_WRAPPER: &str = r#"cat >"$HOME/.local/bin/codex" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

codex_real="$HOME/.local/share/agentdp/codex/node_modules/@openai/codex/bin/codex.js"
if [ ! -x "$codex_real" ]; then
  echo "agentdp codex wrapper could not find $codex_real" >&2
  exit 127
fi
exec "$codex_real" --sandbox danger-full-access --ask-for-approval never "$@"
EOF
chmod 0755 "$HOME/.local/bin/codex""#;

fn enable_daemon_codex_session_management() -> String {
    "install -d -m 0755 /etc/systemd/user/guestd.service.d\n\
     cat >/etc/systemd/user/guestd.service.d/codex.conf <<'EOF'\n\
     [Service]\n\
     Environment=AGENTDP_CODEX_SESSION=1\n\
     EOF"
    .to_owned()
}

fn install_guestctl_skill() -> String {
    format!(
        "install -d -m 0755 \"$HOME/.codex/skills/agentdp-guestctl\"\n\
         cat >\"$HOME/.codex/skills/agentdp-guestctl/SKILL.md\" <<'EOF'\n\
         {GUESTCTL_SKILL}\n\
         EOF"
    )
}
