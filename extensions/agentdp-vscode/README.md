# AgentDP for VS Code

Connects VS Code Insiders to one AgentDP control plane and keeps its ready
Agent Host instances synchronized with the native Agents Window. Run
`AgentDP: Add Server` once and enter the AgentDP host's Tailscale HTTPS URL.
Tailscale identity and ACLs protect the connection; the extension does not
store or send a separate bearer token.

The extension owns the `AgentDP: ` remote-host name prefix. Reconciliation
replaces entries in that namespace and preserves hosts with other names.

The currently pinned Insiders build mixes Copilot subscription promotions into
remote model pickers. Run `scripts/patch-vscode-insiders-agent-host.sh` from the
AgentDP repository after installing or updating that exact desktop build, then
restart VS Code Insiders. The script verifies the build and bundle checksums and
fails instead of patching an unknown version.
