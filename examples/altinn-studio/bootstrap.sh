#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name
  for name in \
    STUDIO_PROD_API_KEY \
    STUDIO_STAGING_API_KEY \
    STUDIO_DEV_API_KEY
  do
    if [ -z "${!name:-}" ]; then
      echo "$name is missing from the agent bootstrap environment" >&2
      exit 1
    fi
  done
}

section() {
  printf '%s\n%s\n\n' "---------------------" "$1"
}

prepare_workspace() {
  section "Preparing workspace:"

  mkdir -p "$AGENTDP_CODE_DIR/.reference/" "$AGENTDP_CODE_DIR/.scripts/" "$AGENTDP_CODE_DIR/apps/"
}

install_studioctl() {
  section "Installing and configuring studioctl:"

  curl -sSL https://altinn.studio/designer/api/v1/studioctl/install.sh | sh

  printf '%s\n' "$STUDIO_PROD_API_KEY" | studioctl auth login --env prod --with-token
  unset STUDIO_PROD_API_KEY
  printf '%s\n' "$STUDIO_STAGING_API_KEY" | studioctl auth login --env staging --with-token
  unset STUDIO_STAGING_API_KEY
  printf '%s\n' "$STUDIO_DEV_API_KEY" | studioctl auth login --env dev --with-token
  unset STUDIO_DEV_API_KEY

  studioctl shell alias --shell bash
  sudo "$HOME/.local/bin/studioctl" env hosts add
}

clone_apps() {
  section "Cloning test apps:"

  pushd "$AGENTDP_CODE_DIR/apps/"
  studioctl app clone --env dev ttd/testprefix
  studioctl app clone ttd/martinotest
  popd
}

install_tools() {
  section "Installing tools:"

  curl -fsSL https://raw.githubusercontent.com/rtk-ai/rtk/refs/heads/master/install.sh | sh

  rtk --version
  rtk init -g --codex
}

altinn_studio_bootstrap() {
  set -euo pipefail

  require_env
  prepare_workspace
  install_studioctl
  clone_apps
  install_tools
}

altinn_studio_bootstrap
