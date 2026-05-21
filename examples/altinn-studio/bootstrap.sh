#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name
  for name in \
    GIT_USER_NAME \
    GIT_USER_EMAIL \
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

configure_git() {
  section "Configuring git:"

  git config --global user.name "$GIT_USER_NAME"
  git config --global user.email "$GIT_USER_EMAIL"
  unset GITHUB_PAT

  mkdir -p "$AGENTDP_CODE_DIR/.cache/" "$AGENTDP_CODE_DIR/apps/"
}

remove_copilot() {
  section "Removing Copilot:"

  for extension in github.copilot github.copilot-chat; do
    code-server --uninstall-extension "$extension" >/dev/null 2>&1 || true
  done
  rm -rf "$HOME/.local/share/code-server/extensions/github.copilot"*
  gh extension remove github/gh-copilot >/dev/null 2>&1 || true
}

configure_playwright() {
  section "Configuring Playwright:"

  local chromium_path
  chromium_path="$(command -v chromium)"
  if [ -z "$chromium_path" ]; then
    echo "chromium is missing; Playwright MCP requires the system chromium package" >&2
    exit 1
  fi

  sudo tee /etc/profile.d/agentdp-playwright.sh >/dev/null <<EOF
export PLAYWRIGHT_MCP_EXECUTABLE_PATH="$chromium_path"
export PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$chromium_path"
export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
EOF

  export PLAYWRIGHT_MCP_EXECUTABLE_PATH="$chromium_path"
  export PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$chromium_path"
  export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

  npm config set prefix "$HOME/.local"
  npm install -g playwright@latest @playwright/test@latest @playwright/mcp@latest
  node -e "const fs = require('fs'); const browser = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH; if (!browser || !fs.existsSync(browser)) { process.exit(1); }"
  npx --yes @playwright/mcp@latest --help >/dev/null
}

install_go_tools() {
  section "Installing Go tools:"

  go install sigs.k8s.io/kind@latest
  go install code.gitea.io/tea@latest
}

install_studioctl() {
  section "Installing and configuring studioctl:"

  local studio_repo="$AGENTDP_CODE_DIR/altinn-studio"
  if [ ! -d "$studio_repo/.git" ]; then
    git clone https://github.com/martinothamar-agent/altinn-studio.git "$studio_repo"
  fi

  make -C "$studio_repo/src/cli" user-install

  printf '%s\n' "$STUDIO_PROD_API_KEY" | studioctl auth login --env prod --with-token
  unset STUDIO_PROD_API_KEY
  printf '%s\n' "$STUDIO_STAGING_API_KEY" | studioctl auth login --env staging --with-token
  unset STUDIO_STAGING_API_KEY
  printf '%s\n' "$STUDIO_DEV_API_KEY" | studioctl auth login --env dev --with-token
  unset STUDIO_DEV_API_KEY
}

configure_studioctl_environment() {
  section "Configuring studioctl environment:"

  studioctl shell alias --shell bash
  sudo "$HOME/.local/bin/studioctl" env hosts add
}

install_altinn_studio_node_dependencies() {
  section "Installing Altinn Studio Node dependencies:"

  corepack enable
  cd "$AGENTDP_CODE_DIR/altinn-studio"
  corepack yarn install
}

clone_apps() {
  section "Cloning test apps:"

  pushd "$AGENTDP_CODE_DIR/apps/"
  studioctl app clone --env dev ttd/testprefix
  studioctl app clone ttd/martinotest
  popd
}

altinn_studio_bootstrap() {
  set -euo pipefail

  require_env
  configure_git
  remove_copilot
  configure_playwright
  install_go_tools
  install_studioctl
  configure_studioctl_environment
  install_altinn_studio_node_dependencies
  clone_apps
}

run_agent "$(declare -f require_env section configure_git remove_copilot configure_playwright install_go_tools install_studioctl configure_studioctl_environment install_altinn_studio_node_dependencies clone_apps altinn_studio_bootstrap); altinn_studio_bootstrap"
