# Auth Test Agent

This disposable agent validates mediated host authentication.

Useful commands:

```bash
/data/home/auth-smoke.sh
gh auth status -h github.com
codex --version
printf '%s\n' "Reply with exactly: auth-smoke-ok" | codex exec --ephemeral --skip-git-repo-check -
```

Expected behavior:

- `/run/agentdp/.env` contains a GitHub token placeholder, not the real token.
- `$HOME/.codex/auth.json` contains Codex auth placeholders, not real token values.
- GitHub API and Codex authenticated traffic succeeds through mediated secret substitution.
