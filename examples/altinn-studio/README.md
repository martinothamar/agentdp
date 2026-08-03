# Altinn Studio agent

This three-replica agent is the primary Altinn Studio development environment.
Each VM has 100 GB of storage and retains the full package, toolchain, browser,
container, repository, credential, and project context defined by this example.

VS Code Insiders' Agents Window is the user interface. Each instance runs the
pinned VS Code Agent Host and Codex app-server; it does not run code-server or
an AgentDP-managed tmux Codex session. The AgentDP VS Code extension discovers
ready instances through the control plane and registers their Tailscale-served
AHP endpoints automatically.

AgentDP owns the Agent Host runtime, artifact checksum, and version-specific
overlay. The manifest only enables `agent_host: {}`. Codex is separately pinned
to the version tested with that runtime and uses `session: none` because Agent
Host owns session creation, persistence, and resumption.

The pinned Agent Host is restricted to the guest's Codex installation. Its
bundled Copilot and default Claude providers are disabled, protected resources
are not advertised, and AHP bearer-token authentication is rejected. Codex,
GitHub, and Studio credentials continue to use AgentDP's mediated guest
credentials; desktop VS Code credentials are never forwarded into the VM.

The existing `guestctl pr` workflow remains available. `guestd` delivers queued
pull-request events through AHP to the unique matching Codex session and keeps
each event on disk until the marked turn completes.

Copy `sample.env` to the ignored `.env`, populate the required host values, and
apply `agent.yaml`. The local `bootstrap.sh`, seeded Codex instructions and
skills, all nine Altinn repositories, and the test-app workspace are preserved
from the original agent.
