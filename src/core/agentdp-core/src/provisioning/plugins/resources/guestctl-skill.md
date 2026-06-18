---
name: agentdp-guestctl
description: Use inside agentdp guests to manage GitHub PR event notification registrations/subscriptions and inspect guest helper health.
---

# agentdp guestctl

`guestctl` is available inside this agent VM and talks to the user-owned `guestd` daemon.

Use `guestctl ping` to verify the guest helper daemon is reachable.

Use `guestctl pr register [pr-url-or-number]` from a repository to watch a PR. Registration records the current PR state first, so only later check failures, reviews, and comments are prompted into the agent tmux pane.

Use `guestctl pr list` to show registered PRs.

Use `guestctl pr unregister [pr-url-or-number]` to stop watching a PR.
