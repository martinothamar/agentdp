#!/usr/bin/env bash
set -euo pipefail
AGENTDP_USER={{agent_user}}
AGENTDP_HOME={{agent_home}}
AGENTDP_CODE_DIR={{code_dir}}
AGENTDP_AGENT_ENV=/usr/local/bin/agentdp-agent-env
export HOME="${HOME:-/root}"
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"
mkdir -p "$AGENTDP_HOME" "$AGENTDP_CODE_DIR"
chown -R "$AGENTDP_USER:$AGENTDP_USER" "$AGENTDP_HOME"

if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files sshd.service >/dev/null 2>&1; then
  systemctl enable --now sshd.service
fi
