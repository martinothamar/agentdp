# Auth Test Agent

Slim mediated-auth smoke agent for GitHub and Codex.

Before creating it, create a local ignored `.env` beside `agent.yaml`:

```bash
GITHUB_PAT=...
```

Codex auth is read from the host Codex login file, normally `~/.codex/auth.json`.
Override with `AGENTDP_CODEX_AUTH_PATH` if needed.

Useful commands:

```bash
agentctl create auth-0
agentctl up auth-0
agentctl exec auth-0 -- /data/home/auth-smoke.sh
agentctl shell auth-0
agentctl down auth-0
agentctl rm auth-0
```
