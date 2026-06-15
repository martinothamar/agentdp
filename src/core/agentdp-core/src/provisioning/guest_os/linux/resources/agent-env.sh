#!/usr/bin/env sh
export HOME={{agent_home}}
. {{agent_shell_env}}
exec "$@"
