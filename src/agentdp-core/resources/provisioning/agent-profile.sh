if [ "${HOME:-}" = "{{agent_home_raw}}" ] || [ "${USER:-}" = "{{agent_user_raw}}" ]; then
  export AGENTDP_HOME={{agent_home}}
  export AGENTDP_CODE_DIR={{code_dir}}
  if [ -d "{{agent_home_raw}}/.local/share/mise/dotnet-root" ]; then
    export DOTNET_ROOT="{{agent_home_raw}}/.local/share/mise/dotnet-root"
    export DOTNET_ROOT_X64="$DOTNET_ROOT"
  fi

  agentdp_prepend_path() {
    case ":${PATH:-}:" in
      *":$1:"*) ;;
      *) PATH="$1${PATH:+:$PATH}" ;;
    esac
  }

  agentdp_prepend_path "/opt/mise/shims"
  agentdp_prepend_path "{{agent_home_raw}}/.dotnet/tools"
  agentdp_prepend_path "{{agent_home_raw}}/.cargo/bin"
  agentdp_prepend_path "{{agent_home_raw}}/go/bin"
  agentdp_prepend_path "{{agent_home_raw}}/.local/share/mise/shims"
  agentdp_prepend_path "{{agent_home_raw}}/.local/bin"
  export PATH
  unset -f agentdp_prepend_path
fi
