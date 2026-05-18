use crate::manifest::plugins::codex::Codex;

use super::Plugin;
use crate::provisioning::bootstrap::ProvisioningBuilder;

impl Plugin for Codex {
    fn apply(&self, builder: &mut ProvisioningBuilder<'_>) {
        builder.add_package("npm");
        if self.yolo {
            builder.add_agent_shell("if [ ! -x \"$HOME/.local/share/agentdp/codex/bin/codex\" ]; then npm install -g --prefix \"$HOME/.local/share/agentdp/codex\" @openai/codex@latest; fi");
            builder.add_agent_shell(CODEX_WRAPPER);
        } else {
            builder
                .add_agent_shell("if ! command -v codex >/dev/null 2>&1; then npm install -g @openai/codex@latest; fi");
        }
    }
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
