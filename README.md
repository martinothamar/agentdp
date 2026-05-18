# agentdp

Agent developer platform for long-running, topic-based software engineering agents.

The first implementation target is the workflow proven in `common/ai/harness`: one agent instance gets a dedicated Arch Linux QEMU VM with Docker Engine, code-server, Codex CLI, seeded home files, seeded harness instructions, and one or more repository checkouts under `HOME/code`.

## Components

- `src/agentdp-server`: the local per-user server. Owns orchestration, provisioning, VM lifecycle, state, networking, bootstrap, and credential mediation.
- `src/agentdp-cli`: the first frontend crate. It builds the `agentctl` binary for creating, starting, stopping, removing, inspecting, and shelling into agents.
- `src/agentweb`: future web frontend. It should call the same `agentdp-server` API as `agentctl`.
- `src/agentdp-core`: shared Rust library for manifests, backend identity, state models, and platform API types.
  - `platform`: module. OS-specific host integration: paths, services, process launching, networking primitives, and dependency checks.
  - `installation`: module. installation and upgrade flow. Starts with `agentctl`; optional frontends are installed later.
- `src/agentdp-protocol`: shared JSONL request/response contract used by interfaces and server processes.
- `src/examples/`: example agent folders.

`agentdp-server` is the local control-plane process. `agentctl` and `agentweb` are interfaces on top of it. The `platform` module name is reserved for OS-level host differences.

Future Kubernetes hosting should use separate crates such as `agentdp-operator`
for the Kubernetes controller and `agentdp-kube-plugin` for a Kubernetes-specific
CLI/plugin. The local server deliberately avoids the "operator" crate name so
that term remains available for the Kubernetes controller model.

## Initial Scope

- Rust implementation.
- Linux/WSL2 host support only.
- QEMU/KVM is required.
- Single backend: QEMU/KVM with Arch Linux cloud image.
- `archlinux` is the only supported guest OS initially.
- Backend selection is represented as a manifest-derived `BackendKind`; the local server dispatches lifecycle work through a narrow backend facade.
- Codex CLI remains the agent runtime for now.
- code-server is installed and started by default as the inspect/debug desktop.
- Docker Engine is installed and started by default inside the VM.
- Host-side credential mediation is preferred over copying secrets into the guest.
- `agentctl` reads `agent.yaml` or `agent.yml` from the current directory, unless `-f <path>` is supplied.

## Non-Goals For The First Cut

- No smolvm backend.
- No Kata backend.
- No Kubernetes integration.
- No agentweb implementation.
- No generic plugin system.
- No snapshot/clone workflow until the VM disk/state model is stable.

## Agent Manifest

The manifest should be YAML, not TOML. It should contain only configuration used by the current agent workflow. It names the desired guest OS; concrete image sources are selected by the backend, not embedded in the manifest.

Example:

```yaml
version: 1
name: altinn-studio

image:
  os: archlinux

user:
  name: agent

resources:
  cpus: 4
  memory: 16G
  storage: 80G

network:
  mode: user
  allow: all
  ports:
    code-server:
      guest: 4090
      protocol: tcp
    ssh:
      guest: 22
      protocol: tcp

bootstrap:
  packages:
    - base-devel
    - git
    - openssh
    - curl
    - wget
    - jq
    - ripgrep
    - fd
    - tmux
    - chromium

  repos:
    - url: https://github.com/martinothamar-agent/altinn-studio.git
    - url: https://github.com/martinothamar-agent/altinn-studio-docs.git
    - name: <name override>
      url: <git remote>
      path: <relative path override>

  shell:
    - curl -sSL https://altinn.studio/designer/api/v1/studioctl/install.sh | sh
    - npm install -g playwright@latest @playwright/test@latest
    - playwright install chromium

  healthchecks:
    - name: code-server
      tcp: 127.0.0.1:4090
      timeout: 30m

plugins:
  docker:
    compose: true
    buildx: true
    healthcheck: true
  mise:
    packages:
      - node@lts
      - python@latest
      - go@latest
      - dotnet@8
      - dotnet@9
      - dotnet@10
  codex:
    yolo: true
    auth: copy-from-host
  github:
    auth: mediated
    setup_git: false
  vscode:
    settings: data/home/.local/share/code-server/User/settings.json
    extensions:
      - EditorConfig.EditorConfig
      - redhat.vscode-yaml
      - ms-dotnettools.csdevkit
```

Defaults owned by the platform:

- VM disk interface is `virtio`.
- Supported guest OS values are named values known to support systemd; initially only `archlinux`.
- `image.os: archlinux` resolves to a backend-neutral cloud image catalog entry; QEMU maps that entry to its qcow2 source and cache key.
- The guest has a normal agent user separate from root. The user receives passwordless sudo for management commands, and `plugins.docker` grants Docker access when enabled.
- Host port assignment is an instance creation concern.
- The manifest declares guest ports; `agentctl create` can map host ports to those named guest ports.
- `network.allow` accepts either `all` or a list of allowed hostnames. The first QEMU backend allows normal user-mode guest egress; stricter enforcement is not implemented yet.
- Tool-specific setup lives under `plugins`; each plugin translates its config into the common Linux provisioning plan.
- `plugins.codex.auth` accepts `mediated` or `copy-from-host`.
- `plugins.github.auth` accepts `mediated` or `copy-from-host`.
  Mediation is the target model. `copy-from-host` is a temporary trusted-host
  mode for the first useful local workflow.
- code-server listens inside the guest on the `code-server` manifest port and is exposed by `agentdp-server`.
- Docker Engine runs inside the guest.
- `/data/home` is the agent home.
- Repository `name` and `path` default to the repository name parsed from the Git remote.
- Repository `path` values are always relative to `HOME/code` when specified.
- Agent-local seed files live next to the manifest and are copied into the VM on create/bootstrap.
  The default source directory is `data/home` beside `agent.yaml`.
- A sibling `bootstrap.sh` is copied to a root-only temporary location and
  sourced before repository checkout, with generated bootstrap helpers such as
  `run_agent` in scope. A sibling `.env` is copied beside it, exported while
  `bootstrap.sh` runs, and removed immediately after the custom bootstrap hook
  finishes.

## CLI Shape

`agentctl` should keep the current harness command vocabulary, but the manifest path replaces the named agent argument.
All commands accept global `-v`/`--verbose` for verbose diagnostic logging to stderr.

```text
agentctl create <instance> [--port <name>:<host-port>] [-f agent.yaml]
agentctl up <instance> [-f agent.yaml]
agentctl down <instance> [-f agent.yaml]
agentctl rm <instance> [-f agent.yaml]
agentctl ps [-f agent.yaml] [--json]
agentctl status <instance> [-f agent.yaml]
agentctl exec <instance> [-f agent.yaml] -- <command...>
agentctl shell <instance> [-f agent.yaml]
agentctl logs <instance> [-f agent.yaml]
```

If `-f` is omitted, `agentctl` searches the current directory for `agent.yaml` or `agent.yml`.

Instance identity is `(manifest name, instance name)`. For example, `altinn-studio/pr-0`.

`create` allocates durable resources: VM disk, instance metadata, copied manifest, seed files, generated secrets, and host port mappings. `up` starts and reconciles an existing instance; it must not allocate a new identity or silently replace create-time choices.

`agentctl` talks to `agentdp-server` over a local Unix domain socket using a small JSONL protocol implemented with synchronous Rust I/O. `agentctl` should auto-start `agentdp-server` on demand when the socket is missing or stale.

The socket lives directly under the user-local agentdp runtime directory selected by the installation/platform modules:

```text
<runtime>/agentdp-server.sock
```

`agentdp-server` should run as a per-user process by default. QEMU can run unprivileged when the user has access to `/dev/kvm`, and named host port forwarding does not require root for high ports. If a future networking mode needs privileged host operations, put that behind a narrow helper instead of making the whole server root.

Request/response framing is one JSON object per line. The protocol should stay boring and inspectable:

```jsonl
{"id":"cmd_j4f8n2","method":"instance.create","params":{"manifest":"/path/agent.yaml","instance":"pr-0"}}
{"id":"cmd_j4f8n2","event":"bootstrap.phase","data":{"phase":"packages","status":"running"}}
{"id":"cmd_j4f8n2","ok":true,"result":{"name":"altinn-studio/pr-0"}}
{"id":"cmd_j4f8n2","ok":false,"error":{"code":"healthcheck_failed","message":"healthcheck code-server timed out"}}
```

Every command gets a generated command identifier. Responses and streaming events use the same identifier so `agentctl`, `agentweb`, and logs can correlate progress with the originating command.

Error responses use stable machine-readable `code` values and human-readable `message` values.

The first create path allocates durable VM inputs but does not start QEMU yet: `agentctl create <instance>` calls `instance.create`, validates the manifest, generates a per-instance SSH key, renders the shared provisioning plan, ensures the cached base image exists, creates the instance qcow2 disk, writes QEMU seed artifacts, and persists instance state.

The first up path starts an already-created instance from persisted state: `agentctl up <instance>` calls `instance.up`, reads `runtime.json` and the copied instance manifest, builds the QEMU command from create-time choices, launches `qemu-system-x86_64`, records the QEMU pid and running status, waits for backend provisioning to finish, then waits for configured healthchecks. For QEMU, backend provisioning means cloud-init has completed. TCP healthchecks probe forwarded host ports; command healthchecks run inside the guest over the per-instance SSH key.

The first status path reads persisted instance state and reports the stored lifecycle status, recorded QEMU pid/process state, stale runtime state, disk path, seed media path, monitor/QMP paths, and host port mappings.

The first down path stops only a tracked QEMU process: `agentctl down <instance>` calls `instance.down`, reads `runtime.json`, checks the recorded QEMU pid, terminates it when the process is still running, waits briefly for exit, removes stale QEMU pid/monitor/QMP files, and records stopped status. If runtime state says running but the recorded pid is already gone, `down` treats that as stale state and reconciles the instance to stopped. It is idempotent for instances that are already stopped or only created.

`rm` removes stopped or created instances, but refuses instances whose runtime status is still running. Run `agentctl down <instance>` first so the server can stop or reconcile the tracked process before deleting durable instance state.

The server also supports `provisioning.plan` as an internal preview method. It accepts an absolute manifest path and optional instance name, validates the manifest, renders the shared provisioning plan, and returns backend-specific preview details under a tagged `backend` result. For the initial QEMU backend, it resolves QEMU image metadata and writes QEMU seed preview artifacts under the instance's generated work directory:

```jsonl
{"id":"cmd_j4f8n2","method":"provisioning.plan","params":{"manifest":"/path/agent.yaml","instance":"pr-0"}}
```

## Backend Boundary

The manifest names the desired guest OS. The server resolves that to a `BackendKind`
and dispatches through a small backend facade. Instance state stores opaque
backend state under a tagged `backend` field; code outside the backend should not
reach into QEMU-specific fields.

The first implementation has only `qemu`, so an enum-based facade is simpler
than a trait object. If a second local backend is added, promote only the proven
shared operations into a trait at that point.

Keep backend-specific details in the QEMU backend:

- mapping catalog images to concrete QEMU image sources and cache keys.
- qcow2 base image download/cache.
- NoCloud seed generation and delivery.
- disk creation and resizing.
- QEMU process supervision.
- SSH readiness checks.
- interactive shell command generation.
- backend-specific doctor checks.
- code-server and Docker service startup.
- host port and forwarded service wiring.

## State Model

Agent state should be VM-shaped from the beginning.

```text
~/.local/share/agentdp/
  images/
  instances/
    altinn-studio/
      pr-0/
        manifest.yaml
        disk.qcow2
        runtime.json
        generated/
          qemu/
            seed.img
            seed/
              meta-data
              user-data
            scripts/
              bootstrap.sh
        logs/
```

Cached base images live under the user-local cache directory, for example `<cache>/images/archlinux-x86_64-cloudimg.qcow2`. Downloads should use a temporary `.part` path before replacing the final cache path.

The VM disk is the primary persistence boundary. This keeps future backup, snapshot, and clone semantics close to normal VM operations.

`agentdp-server` owns state locking. Mutating operations on the same instance must be serialized so concurrent `agentctl` calls cannot corrupt disks, metadata, or QEMU process state.

`runtime.json` should contain enough metadata for status, reconciliation, and debugging:

- manifest name and instance name
- manifest source path
- input hash over manifest YAML and source seed files
- current VM process ID when running
- QEMU monitor/socket paths
- assigned host port mappings
- current lifecycle status
- last completed bootstrap phase
- timestamps for create, last start, last stop, and last successful healthcheck
- last error code and message when applicable

## Installation

Installing agentdp starts with `agentctl`. The CLI is the bootstrapper for the rest of the platform:

- Install or upgrade `agentctl`.
- Initialize user-local platform directories.
- Check host dependencies.
- Install and register per-user `agentdp-server`.
- Start or connect to `agentdp-server`.
- Later: install optional frontends such as `agentweb`.

`agentweb` should never be required for platform setup. It is an optional interface on top of the same `agentdp-server` API used by `agentctl`.

The first supported host target is Linux/WSL2. Other OS path rules are documented now so the platform boundary is clear, but they are not implementation targets for the first cut.

User-local directories are selected by the `platform` module:

```text
Linux:
  data:   $XDG_DATA_HOME/agentdp   or ~/.local/share/agentdp
  config: $XDG_CONFIG_HOME/agentdp or ~/.config/agentdp
  cache:  $XDG_CACHE_HOME/agentdp  or ~/.cache/agentdp
  runtime:$XDG_RUNTIME_DIR/agentdp or ~/.local/state/agentdp/run
  logs:   $XDG_STATE_HOME/agentdp  or ~/.local/state/agentdp

macOS:
  data:   ~/Library/Application Support/agentdp
  config: ~/Library/Application Support/agentdp
  cache:  ~/Library/Caches/agentdp
  runtime:~/Library/Application Support/agentdp/run
  logs:   ~/Library/Logs/agentdp

Windows:
  data:   %LOCALAPPDATA%\agentdp
  config: %APPDATA%\agentdp
  cache:  %LOCALAPPDATA%\agentdp\cache
  runtime:%LOCALAPPDATA%\agentdp\run
  logs:   %LOCALAPPDATA%\agentdp\logs
```

The first installer can be simple: build `agentctl` and `agentdp-server`, then let `agentctl self install` copy them to the user-local bin directory selected by the installation module. If an `agentdp-server` process is already running, self-install refreshes it after replacing the binary: it asks newer servers to shut down gracefully and falls back to terminating the pinged PID for older servers.

`agentctl doctor` should be available early. It verifies the host before VM work starts:

- Linux/WSL2 host detection
- user-local directory writability
- `agentdp-server` socket health via `server.ping`
- selected backend prerequisites via `server.doctor`

For the initial QEMU backend, `server.doctor` checks:

- `/dev/kvm` availability and permissions
- `qemu-system-x86_64`
- `qemu-img`
- `curl`
- `ssh`
- `ssh-keygen`

When `agentdp-server` is missing or stale, `agentctl doctor` may start it as a detached per-user process and then retry the ping. The first implementation resolves the `agentdp-server` executable from `AGENTDP_SERVER_PATH`, then from a sibling binary next to the running `agentctl`, then from `PATH`.

## Platform Module

All host OS branching belongs in the `platform` module. Other crates should depend on platform capabilities, not `cfg(target_os)` checks scattered through orchestration code.

The module should own:

- User-local directory resolution.
- Binary lookup and dependency checks.
- Process spawning and signal handling.
- Background service registration for `agentdp-server`.
- Host firewall and port forwarding helpers.
- Hypervisor capability checks.
- OS-specific QEMU command construction where unavoidable.
- Path normalization for manifests, seed folders, and state.

This keeps `agentdp-server` focused on desired state and lifecycle orchestration instead of host-specific mechanics.

## Home And Harness Seed

The current seed behavior should carry forward:

- Copy agent-owned files from the manifest directory into `/data/home` during create/bootstrap.
- Generate Codex harness information inside the guest.
- Include OS image, CPUs, RAM, storage, network mode, installed packages, Docker availability, repositories, and bootstrap shell commands.
- Keep `AGENTS.md`, Codex skills, code-server settings, and similar agent-authored files in source control beside `agent.yaml`.

Source seed files and generated files are separate:

- Source seed files live beside `agent.yaml` and are user-authored.
- Generated files live under the instance `generated/` directory in agentdp state.
- `agentdp-server` regenerates generated files from the manifest and runtime metadata.
- Users should edit source seed files, not generated files.

Bootstrap must be idempotent and phase-based. `up` should be able to resume after a failed bootstrap phase without rebuilding the VM:

- base system packages
- plugin-provided tools
- repository checkout/update
- home seed
- generated harness information
- code-server extensions and settings
- manifest shell hook
- healthchecks

`up` blocks until configured healthchecks pass or time out.

## Credential Model

Do not copy long-lived credentials into the VM by default.

The first networking implementation can use QEMU user networking with explicit host port forwards. Full host-mediated networking is the next stage, after the VM lifecycle and CLI are stable.

Target model:

- `agentdp-server` owns host-side credentials.
- Guest tools see normal CLIs and normal network endpoints.
- A mediated host networking layer injects credentials only into approved outbound requests.
- Git SSH access is proxied through the host, similar to Gondolin's SSH egress model.
- GitHub API access is mediated for `gh` and HTTPS API calls.
- Codex CLI auth is mediated too. The guest may see a fake auth file or fake token, but real credentials stay host-side and are substituted only by `agentdp-server`.
- The guest should not be able to read raw PATs, Codex tokens, or host SSH private keys from disk, env, or memory.

This is a core platform feature, but it can start narrow: GitHub and Codex first.

Temporary local mode:

- `plugins.codex.auth: copy-from-host` copies host `~/.codex/auth.json` into
  `/data/home/.codex/auth.json` when the host file exists.
- `plugins.github.auth: copy-from-host` copies host
  `${XDG_CONFIG_HOME:-~/.config}/gh/hosts.yml` into
  `/data/home/.config/gh/hosts.yml` when the host file exists. If
  `setup_git: true`, bootstrap runs `gh auth setup-git`.
- Agent-specific temporary auth can be implemented in the sibling `bootstrap.sh`
  using values from a sibling `.env`. This keeps PAT-specific setup out of the
  generic manifest schema while credential mediation is still missing.
- `copy-from-host` is intentionally explicit in the manifest because it places
  host credentials inside the VM during bootstrap. Replace it with `mediated`
  once credential mediation exists for that tool.

## Implementation Order

1. Workspace crates: `agentdp-core`, `agentdp-protocol`, `agentdp-server`, `agentdp-cli`.
2. `platform` module for user-local paths, host checks, and OS-specific process/service behavior.
3. `installation` flow for `agentctl self install`.
4. Per-user `agentdp-server` auto-start and UDS JSONL protocol.
5. YAML manifest parser and validation.
6. Local `agentdp-server` API with QEMU backend.
7. `agentctl create/up/down/rm/ps/status/exec/shell/logs`.
8. Arch VM provisioning with Docker, seeded home, and plugin-provided tools such as code-server, Codex CLI, and mise.
9. Bootstrap phases and healthchecks.
10. `agentctl doctor`.
11. QEMU user networking with named guest port forwards.
12. Mediated GitHub/Codex credential flow.
13. agentweb once the server API has proven useful from the CLI.

## Design Bias

- Prefer one working QEMU backend over premature backend breadth.
- Keep manifests declarative; keep runtime state out of manifests.
- Keep ports, process IDs, sockets, and generated disks in instance state.
- Make `agentctl --json` available for every command where machine consumption is useful.
- Treat code-server as an inspection surface, not the primary automation loop.
- Preserve the option to add another backend later without shaping the first version around it.
