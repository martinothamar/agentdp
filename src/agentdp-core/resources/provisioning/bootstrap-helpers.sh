if command -v sudo >/dev/null 2>&1; then
  install -d -m 0750 /etc/sudoers.d
  printf '%s ALL=(ALL) NOPASSWD:ALL\n' "$AGENTDP_USER" >/etc/sudoers.d/agentdp-agent
  chmod 0440 /etc/sudoers.d/agentdp-agent
fi

run_agent() {
  local command=$1
  runuser -u "$AGENTDP_USER" --preserve-environment -- "$AGENTDP_AGENT_ENV" bash -lc "$command"
}

run_agent_args() {
  runuser -u "$AGENTDP_USER" --preserve-environment -- "$AGENTDP_AGENT_ENV" "$@"
}

clone_repo() {
  local url=$1
  local path=$2
  for attempt in $(seq 1 6); do
    run_agent_args git clone "$url" "$path" && return 0
    sleep $((attempt * 2))
  done
  run_agent_args git clone "$url" "$path"
}
