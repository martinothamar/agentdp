#!/usr/bin/env sh
export HOME={{agent_home}}
export AGENTDP_HOME={{agent_home}}
export AGENTDP_CODE_DIR={{code_dir}}
export PATH="/usr/local/bin:/usr/bin:/bin${PATH:+:$PATH}"
if [ -r /etc/profile.d/agentdp-agent.sh ]; then
  . /etc/profile.d/agentdp-agent.sh
fi
exec "$@"
