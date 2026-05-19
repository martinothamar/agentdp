#!/usr/bin/env bash
set -euo pipefail

session="${AGENTDP_TMUX_SESSION:-agentdp}"
state_dir="${AGENTDP_STATE_DIR:-$HOME/.local/state/agentdp}"
pane_file="${AGENTDP_CODEX_PANE_FILE:-$state_dir/codex-pane-id}"
workdir="${AGENTDP_CODE_DIR:-$HOME/code}"

mkdir -p "$state_dir"

if ! tmux has-session -t "$session" >/dev/null 2>&1; then
  tmux new-session -d -s "$session" -c "$workdir" "codex resume --last || codex"
fi

pane_id="$(tmux display-message -p -t "$session:0.0" '#{pane_id}')"
tmp="$pane_file.tmp"
printf '%s\n' "$pane_id" >"$tmp"
mv "$tmp" "$pane_file"
printf '%s\n' "$pane_id"
