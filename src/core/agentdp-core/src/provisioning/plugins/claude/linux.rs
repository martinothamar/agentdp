use crate::manifest::plugins::claude::Claude;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

const GUESTCTL_SKILL: &str = include_str!("../resources/guestctl-skill.md");

pub(super) fn apply(plugin: &Claude, builder: &mut ProvisioningBuilder<'_>) {
    builder.require_mise_package("node@lts");
    let code_dir = builder.code_dir();
    builder.add_base_user_step(
        "plugin.claude.install",
        "Install Claude Code",
        ["plugin.mise"],
        [BootstrapStepResource::AgentHome, BootstrapStepResource::NpmGlobal],
        render_claude_install(plugin.yolo),
    );
    let mut config = shell::ShellScript::new();
    config.block(&trust_code_dir(code_dir));
    if plugin.yolo {
        config.block(&accept_bypass_permissions());
    }
    config.block(&install_guestctl_skill());
    builder.add_instance_user_step(
        "plugin.claude.config",
        "Configure Claude Code",
        ["plugin.claude.install"],
        [BootstrapStepResource::AgentHome],
        config.render(),
    );
    builder.add_base_system_step(
        "plugin.claude.session",
        "Configure Claude Code session service",
        ["system.guest_tooling"],
        [BootstrapStepResource::Systemd],
        enable_daemon_claude_session_management(),
    );
}

fn trust_code_dir(code_dir: &str) -> String {
    let mut script = shell::ShellScript::new();
    script.line("trust_js=\"$(mktemp)\"");
    script.line("cat >\"$trust_js\" <<'EOF'");
    script.block(TRUST_CODE_DIR_JS);
    script.line("EOF");
    script.line(format!("node \"$trust_js\" {}", shell::single_quote(code_dir)));
    script.line("rm -f \"$trust_js\"");
    script.render()
}

const TRUST_CODE_DIR_JS: &str = r#"const fs = require("fs");
const os = require("os");
const path = os.homedir() + "/.claude.json";
let config = {};
try { config = JSON.parse(fs.readFileSync(path, "utf8")); } catch {}
config.hasCompletedOnboarding = true;
config.projects = config.projects || {};
const dir = process.argv[2];
config.projects[dir] = Object.assign({}, config.projects[dir], { hasTrustDialogAccepted: true });
fs.writeFileSync(path, JSON.stringify(config, null, 2) + "\n");"#;

fn accept_bypass_permissions() -> String {
    let mut script = shell::ShellScript::new();
    script.line("bypass_js=\"$(mktemp)\"");
    script.line("cat >\"$bypass_js\" <<'EOF'");
    script.block(ACCEPT_BYPASS_JS);
    script.line("EOF");
    script.line("node \"$bypass_js\"");
    script.line("rm -f \"$bypass_js\"");
    script.render()
}

const ACCEPT_BYPASS_JS: &str = r#"const fs = require("fs");
const os = require("os");
const dir = os.homedir() + "/.claude";
fs.mkdirSync(dir, { recursive: true });
const path = dir + "/settings.json";
let settings = {};
try { settings = JSON.parse(fs.readFileSync(path, "utf8")); } catch {}
settings.skipDangerousModePermissionPrompt = true;
fs.writeFileSync(path, JSON.stringify(settings, null, 2) + "\n");"#;

fn render_claude_install(yolo: bool) -> String {
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 \"$HOME/.local/bin\" \"$HOME/.local/share/agentdp/claude\"");
    script.line("npm install -g --prefix \"$HOME/.local/share/agentdp/claude\" @anthropic-ai/claude-code@latest");
    if yolo {
        script.block(CLAUDE_WRAPPER);
    } else {
        script.line("ln -sf \"$HOME/.local/share/agentdp/claude/bin/claude\" \"$HOME/.local/bin/claude\"");
    }
    script.render()
}

const CLAUDE_WRAPPER: &str = r#"cat >"$HOME/.local/bin/claude" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

claude_real="$HOME/.local/share/agentdp/claude/bin/claude"
if [ ! -x "$claude_real" ]; then
  echo "agentdp claude wrapper could not find $claude_real" >&2
  exit 127
fi
exec "$claude_real" --dangerously-skip-permissions "$@"
EOF
chmod 0755 "$HOME/.local/bin/claude""#;

fn enable_daemon_claude_session_management() -> String {
    "install -d -m 0755 /etc/systemd/user/guestd.service.d\n\
     cat >/etc/systemd/user/guestd.service.d/claude.conf <<'EOF'\n\
     [Service]\n\
     Environment=AGENTDP_CLAUDE_SESSION=1\n\
     EOF"
    .to_owned()
}

fn install_guestctl_skill() -> String {
    format!(
        "install -d -m 0755 \"$HOME/.claude/skills/agentdp-guestctl\"\n\
         cat >\"$HOME/.claude/skills/agentdp-guestctl/SKILL.md\" <<'EOF'\n\
         {GUESTCTL_SKILL}\n\
         EOF"
    )
}
