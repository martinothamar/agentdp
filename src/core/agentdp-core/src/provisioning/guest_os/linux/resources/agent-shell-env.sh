PATH="${PATH:-/usr/bin:/bin}"

agentdp_prepend_path() {
  agentdp_path_value=
  agentdp_old_ifs=$IFS
  IFS=:
  for agentdp_path_entry in ${PATH:-}; do
    if [ -n "$agentdp_path_entry" ] && [ "$agentdp_path_entry" != "$1" ]; then
      agentdp_path_value="${agentdp_path_value:+$agentdp_path_value:}$agentdp_path_entry"
    fi
  done
  IFS=$agentdp_old_ifs
  PATH="$1${agentdp_path_value:+:$agentdp_path_value}"
}

agentdp_prepend_path "/bin"
agentdp_prepend_path "/usr/bin"
agentdp_prepend_path "{{usr_local_bin}}"
export PATH

export AGENTDP_CODE_DIR={{code_dir}}
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
{{agent_runtime_env}}
{{agent_ca_env}}

if [ "${HOME:-}" = "{{agent_home_raw}}" ] || [ "${USER:-}" = "{{agent_user_raw}}" ]; then
  agentdp_prepend_path "{{agent_home_raw}}/.dotnet/tools"
  agentdp_prepend_path "{{agent_home_raw}}/.cargo/bin"
  agentdp_prepend_path "{{agent_home_raw}}/go/bin"
  agentdp_prepend_path "{{agent_home_raw}}/.local/bin"
{{agent_plugin_env}}
  export PATH
fi

unset agentdp_path_entry agentdp_path_value agentdp_old_ifs
unset -f agentdp_prepend_path
