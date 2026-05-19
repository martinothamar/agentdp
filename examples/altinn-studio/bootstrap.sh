#!/usr/bin/env bash
set -euo pipefail

altinn_studio_bootstrap() {
  set -euo pipefail

  if [ -z "${GITHUB_PAT:-}" ]; then
    echo "GITHUB_PAT is missing from the agent bootstrap environment" >&2
    exit 1
  fi

  if [ -z "${STUDIO_PROD_API_KEY:-}" ]; then
    echo "STUDIO_PROD_API_KEY is missing from the agent bootstrap environment" >&2
    exit 1
  fi

  if [ -z "${STUDIO_DEV_API_KEY:-}" ]; then
    echo "STUDIO_DEV_API_KEY is missing from the agent bootstrap environment" >&2
    exit 1
  fi

  echo "---------------------"
  echo "Configuring git:"
  echo ""
  install -d -m 0700 "$HOME/.config/gh"
  printf '%s\n' "$GITHUB_PAT" | gh auth login --with-token
  gh auth setup-git >/dev/null
  git config --global user.name "$GIT_USER_NAME"
  git config --global user.email "$GIT_USER_EMAIL"
  unset GITHUB_PAT
  mkdir -p "$AGENTDP_CODE_DIR/.cache/"
  mkdir -p "$AGENTDP_CODE_DIR/apps/"
  echo "---------------------"

  echo "---------------------"
  echo "Installing and configuring studioctl:"
  echo ""
  studio_repo="$AGENTDP_CODE_DIR/altinn-studio"
  if [ ! -d "$studio_repo/.git" ]; then
    git clone https://github.com/martinothamar-agent/altinn-studio.git "$studio_repo"
  fi
  mise use --global go@latest dotnet@10
  make -C "$studio_repo/src/cli" user-install
  printf '%s\n' "$STUDIO_PROD_API_KEY" | studioctl auth login --env prod --with-token
  unset STUDIO_PROD_API_KEY
  printf '%s\n' "$STUDIO_STAGING_API_KEY" | studioctl auth login --env staging --with-token
  unset STUDIO_STAGING_API_KEY
  printf '%s\n' "$STUDIO_DEV_API_KEY" | studioctl auth login --env dev --with-token
  unset STUDIO_DEV_API_KEY
  echo "---------------------"

  echo "---------------------"
  echo "Installing and configuring studioctl:"
  echo ""
  pushd "$AGENTDP_CODE_DIR/apps/"
  studioctl app clone --env dev ttd/testprefix
  studioctl app clone ttd/martinotest
  popd
  echo "---------------------"

}

run_agent "$(declare -f altinn_studio_bootstrap); altinn_studio_bootstrap"
