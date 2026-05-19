use crate::manifest::plugins::codex::Codex;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::{CODE_DIR, shell};

impl Plugin for Codex {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        builder.add_package("mise");
        builder.add_agent_shell(format!(
            "mise use --global {}\n{}",
            shell::single_quote("node@lts"),
            trust_code_dir()
        ));
        if self.yolo {
            builder.add_agent_shell("if [ ! -x \"$HOME/.local/share/agentdp/codex/bin/codex\" ]; then npm install -g --prefix \"$HOME/.local/share/agentdp/codex\" @openai/codex@latest; fi");
            builder.add_agent_shell(CODEX_WRAPPER);
        } else {
            builder
                .add_agent_shell("if ! command -v codex >/dev/null 2>&1; then npm install -g @openai/codex@latest; fi");
        }
    }
}

fn trust_code_dir() -> String {
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 \"$HOME/.codex\"");
    script.line("config=\"$HOME/.codex/config.toml\"");
    script.line("touch \"$config\"");
    script.line(format!(
        "if ! grep -Fqx {} \"$config\"; then",
        shell::single_quote(&format!("[projects.\"{CODE_DIR}\"]"))
    ));
    script.line("  {");
    script.line("    printf '\\n'");
    script.line(format!(
        "    printf '%s\\n' {}",
        shell::single_quote(&format!("[projects.\"{CODE_DIR}\"]"))
    ));
    script.line("    printf '%s\\n' 'trust_level = \"trusted\"'");
    script.line("  } >>\"$config\"");
    script.line("fi");
    script.render()
}

const CODEX_WRAPPER: &str = r#"install -d -m 0755 "$HOME/.local/bin"
cat >"$HOME/.local/bin/codex" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

codex_real="$HOME/.local/share/agentdp/codex/bin/codex"
if [ ! -x "$codex_real" ]; then
  echo "agentdp codex wrapper could not find $codex_real" >&2
  exit 127
fi
exec "$codex_real" --sandbox danger-full-access --ask-for-approval never "$@"
EOF
chmod 0755 "$HOME/.local/bin/codex""#;
