#!/usr/bin/env bash
set -euo pipefail

allowed_url="${AGENTDP_NETWORK_ALLOWED_URL:-https://example.com}"
blocked_url="${AGENTDP_NETWORK_BLOCKED_URL:-https://www.microsoft.com}"
tmp_dir="${TMPDIR:-/tmp}/agentdp-network-smoke-$(id -u)"

mkdir -p "$tmp_dir"

echo "== dns =="
getent ahostsv4 example.com | head -n 3

echo "== allowed https =="
curl -fsSI --connect-timeout 5 --max-time 20 "$allowed_url" >"$tmp_dir/allowed.headers"
head -n 1 "$tmp_dir/allowed.headers"

echo "== denied https =="
if curl -fsSI --connect-timeout 3 --max-time 8 "$blocked_url" >"$tmp_dir/blocked.out" 2>"$tmp_dir/blocked.err"; then
  echo "unexpectedly reached blocked URL: $blocked_url" >&2
  head -n 5 "$tmp_dir/blocked.out" >&2
  exit 1
fi

head -n 5 "$tmp_dir/blocked.err" || true

echo "== allowed https after deny =="
curl -fsSI --connect-timeout 5 --max-time 20 "$allowed_url" >"$tmp_dir/allowed-after-deny.headers"
head -n 1 "$tmp_dir/allowed-after-deny.headers"

echo "== git https =="
git ls-remote --exit-code https://github.com/octocat/Hello-World.git HEAD >"$tmp_dir/git-ls-remote.out"
head -n 1 "$tmp_dir/git-ls-remote.out"

echo "network-smoke-ok"
