#!/usr/bin/env bash
set -euo pipefail

section() {
  printf '%s\n%s\n\n' "---------------------" "$1"
}

configure_agentdp_repo() {
  section "Configuring agentdp repo:"

  local repo="$AGENTDP_CODE_DIR/agentdp"
  if [ ! -d "$repo/.git" ]; then
    git clone https://github.com/martinothamar/agentdp.git "$repo"
  fi

  git -C "$repo" config core.preloadIndex true
  git -C "$repo" config core.untrackedCache true
  git -C "$repo" update-index --test-untracked-cache >/dev/null 2>&1 || true
}

verify_dev_tools() {
  section "Verifying dev tools:"

  command -v rustc
  command -v cargo
  command -v cargo-nextest
  command -v just
  command -v rg
  command -v qemu-img
  command -v qemu-system-x86_64
  command -v docker
  command -v gh
}

verify_nested_kvm() {
  section "Verifying nested KVM:"

  if [ ! -e /dev/kvm ]; then
    printf '%s\n' "/dev/kvm is missing. This dev agent needs nested virtualization so it can run agentdp's QEMU/KVM backend."
    exit 1
  fi

  if [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then
    printf '%s\n' "/dev/kvm exists but is not readable/writable by $USER. Check nested virtualization and kvm group access."
    ls -l /dev/kvm
    id
    exit 1
  fi

  qemu-system-x86_64 -accel help | grep -qx 'kvm'
}

agentdp_dev_bootstrap() {
  set -euo pipefail

  configure_agentdp_repo
  verify_dev_tools
  verify_nested_kvm
}

agentdp_dev_bootstrap
