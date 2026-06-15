#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "auth-smoke: $*" >&2
  exit 1
}

test -f "$HOME/.codex/auth.json" || fail "missing Codex auth file"
if jq -e '
  .. | objects | to_entries[]
  | select((.key | test("^(access_token|refresh_token|id_token)$"))
      and (.value | type == "string")
      and ((.value | contains("AGENTDP_SECRET_CODEX_AUTH_")) | not))
' "$HOME/.codex/auth.json" >/dev/null; then
  fail "Codex auth file appears to contain real token values"
fi

if ! grep -q "AGENTDP_SECRET_CODEX_AUTH_" "$HOME/.codex/auth.json"; then
  fail "Codex auth file does not contain mediated placeholders"
fi

test -f /run/agentdp/.env || fail "missing mediated custom env"
if grep -Eq '^(GITHUB_TOKEN|GH_TOKEN|GITHUB_PAT)=.*(github_pat_|ghp_)' /run/agentdp/.env; then
  fail "GitHub token appears to be copied into guest env"
fi
grep -Eq '^(GITHUB_TOKEN|GH_TOKEN|GITHUB_PAT)=AGENTDP_SECRET_' /run/agentdp/.env \
  || fail "GitHub token placeholder missing from guest env"

set -a
source /run/agentdp/.env
set +a
gh auth status -h github.com >/dev/null
gh api user --jq .login >/dev/null

codex --version >/dev/null
codex_output=$(
  printf '%s\n' "Reply with exactly: auth-smoke-ok" \
    | timeout 120s codex exec --ephemeral --skip-git-repo-check -
)
grep -q "auth-smoke-ok" <<<"$codex_output" \
  || fail "Codex authenticated request did not return expected smoke response"

echo "auth-smoke: ok"
