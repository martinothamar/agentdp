# TODO

Been using the altinn-studio agent for a couple of days, here are improvements I want:

- [x] AGENTS.md improvement
  - Use this user-level AGENTS.md and copy it into a section in the altinn-studio AGENTS.md
  - Part of the development workflow should be considering whether the change needs to be reflected in user-level documentation (altinn-studio-docs repo)
- [x] PR subscriber/events improvements - summary updates are too spammy
  - Rename to `<pr_events>`, it should be event oriented and not summary oriented. The agent can read the current status if needed on its own
  - Each event should correspond to a single, compressed line in the update message
  - Dont include CI workflow/job success events, as they are not actionable. Keep failing workflow/jobs and comments
  - Only send events when it is not already working (debounce/coalesce events until idle/turn finished)
  - Agents should use `gh` cli directly for creation, but what is currently a PR creation hook/wrapper should instead just have explicity commands for PR registration: `register`, `unregister`, `list` (list subscribed PRs). Agents should be encouraged to unregister from PRs on demand or when PRs are merged/completed
- [x] "playwright mcp is configured for a missing chrome binary in this environment"
  - Playwright library and playwright mcp should be configured to and use the same installed chromium browser
  - Add a doctor check or bootstrap validation so misconfiguration fails early
- [x] dotnet tool install --global ilspycmd
  - Add to the altinn-studio bootstrap/tooling install list.
  - Ensure `$HOME/.dotnet/tools` is on PATH for both shell sessions and code-server terminals.
- [ ] When I select image/png files in the code-server file explorer sidebar, they are just blank and dont render, not sure why
- [ ] VM filesystem is currently btrfs, but I think it makes more sense to use standard ext4 as that is more usual. Do this if it is feasible/minimal change
- [x] yarn install in altinn-studio repo on bootstrap
- [x] `corepack enable` on bootstrap
- [x] Add `host.docker.internal` to /etc/hosts on bootstrap
- [x] Run `studioctl shell alias` on bootstrap (to add s -> studioctl alias)
- [x] Run `studioctl env hosts add` on bootstrap (requests sudo due to /etc/hosts)
- [x] Install `dotnet tool install --global dotnet-ef` on bootstrap
- [x] Install `actionlint` in the agent so GitHub Actions workflow edits can be validated locally
- [x] Install tea/gitea CLI so that the agent can interact with app repos (create forks etc)
- [x] Add upstream remotes for repos that are forks (likely through repository configuration in the yaml, like `fork: true`)
- [x] Add *github.com and *altinn.studio domains as trusted domains in the code-server installation

Future consideration:


- [ ] copy/paste / domain name / tls?
  - Remote browser clipboard APIs usually need a secure context or explicit browser permission.
  - Check whether using a stable LAN hostname with TLS fixes code-server clipboard behavior.
  - Document the fallback behavior if copy/paste remains browser-dependent.
- [ ] periodic hang
  - Capture where it hangs: `agentctl` client, agentdp-server, QEMU, SSH command, cloud-init polling, or code-server.
  - Add diagnostics before changing behavior: server pid/socket/lock status, last request, process tree, and relevant logs.
  - Revisit Windows port conflicts/excluded ranges and stale server socket handling as likely suspects.
- [ ] gists in token?
  - Check whether the GitHub token available in the agent includes `gist` scope.
  - Decide whether gist support is required or should be explicitly unavailable.
- [ ] screenshots / gifs?
  - Decide where captures should be stored by convention, probably under repo-local artifacts or `/data/home`.
  - Prefer tools that can be invoked by the agent without opening extra host windows.
