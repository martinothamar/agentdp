# agentdp

> [!NOTE]
> This is an experiment in building an agent platform. LLMs have been used in generating code, as such this is part slop.
> Lots of things have changed and been refactored since the "idea"-stage so things may seem at least in part incosistent.
>
> I have currently tested the latest state only on Linux. Been a while since I tested Windows and haven't tested macOS at all.

Agent developer platform for long-running, project-based software engineering agents.

Using `agentctl` to manage agents:

![agentctl](img/agentctl.gif)

Historical recording of me driving two [altinn-studio agent instances](examples/altinn-studio/agent.yaml) through the previous code-server/tmux workflow:

https://github.com/user-attachments/assets/4f0f7a27-5d3f-4a09-88cf-691574362c19


## Features

Current:

- Agents provisioned and configured through "desired state" manifest descriptions of their environment: installed OS, tools, auth, plugins
- `agentctl` CLI for managing agents (ps, status, apply, delete)
- QEMU virtualization backend with cloud-init for first bootstrap (creates full vm for a given agent manifest)
  - Creates a base image with instance (depending on `replicas` in manifest) disks layered on top
- Guest-side daemon and CLI (`guestd`, `guestctl`) for control-plane/automation and agent tooling
  - GH PR listener/subscription manager for auto-prompting agent session based on GH PR events 
- Support Arch Linux and Rocky 9 as guest OS
- Plugins for `gh`, `codex`, `docker`, `mise`, `tailscale`, browser (including Playwright MCP) and more
- VS Code Agents Window integration through discovered, Tailscale-served Agent Host Protocol endpoints
- Custom network stack for MITM proxying, allowing "mediated auth", meaning your agent never sees secrets; only placeholders
  - Automatically inject and register MITM CAs with guest OS, docker/podman containers 
- OS-agnostic runtime, provisioning and guest-side modules to allow for future crossplatform support host- and guest-side for Windows, macOS and Linux
  - Windows and Linux tested host-side, only Linux guest-side

Ideas:

- Extensibility around network MITM
  - Essentially some form of middleware perhaps in various parts of the ingress/egress stack, e.g. for auditability or blocking
  - Auditability: seeing which hosts or endpoints the agent communicates with, or doing some prompt/context analysis (is Anthropic trying to exfiltrate data through Claude Code?)
  - Blocking: detecting sensitive data in context/prompt, though never going to be bulletproof
- Support for multiple OSes both guest- and host-side; Windows, Linux distros, macOS (?)
- Support for multiple types of virtualization/runtime backends; QEMU, cloud-hypervisor, containers, lima, ...
- Richer reverse proxying for serving instance tools through stable per-agent/per-instance tailnet subdomains with host-header routing
- Richer web UI workflows for browsing manifests, creating instances, inspecting readiness, tailing logs, and managing route exposure
- Broader mediated networking policy and protocol support
- Kubernetes orchestration and efficient scheduling, i.e. k8s controller layered on top of server
  - Locally: 1 server manages N agents
  - Cloud: 1 controller schedules and orchestrates N pods each containing 1 server and 1 agent (server still wanted for automation)
- Guest-side plugin model
  - Could GH PR component in guestd become a plugin?


## Altinn Studio example

The [altinn-studio agent example](examples/altinn-studio/agent.yaml) has been the main driver/user of the code in this repo.
I personally use it installed on a cachyOS desktop PC with 3 replicas running. Reflections:
* Its a very technically capable agent, can take full advantage of "computer use" advances in models
  * Debugging and taking process dumps to [root-cause and fix hangs/deadlocks](https://github.com/Altinn/app-lib-dotnet/pull/1798)
  * Use advanced tools like [`strace` to debug and prove various facts important to decision-making](https://github.com/Altinn/app-lib-dotnet/pull/1811#issuecomment-4716265043)
  * Use browsers to do [complex e2e-testing](https://github.com/Altinn/altinn-studio/pull/19078#issuecomment-4632994414) and [provide screenshots to visualize changes](https://github.com/Altinn/altinn-studio-docs/pull/2939)
  * In one instance the agent independently installed Podman itself so it could verify some behavior between Docker and Podman on Linux
  * [Analyze deeply diffs/changes when updating dependencies](https://github.com/Altinn/altinn-studio/pull/19283#issuecomment-4753967011)
  * Takeaway: running agents in proper VM sandboxes is has major benefits; it can both run --yolo and make use of tools and APIs not accessible in lesser sandboxes like container or microVMs
* There are benefits to driving agents through public interfaces like GH issues/PRs; the prompting becomes visible to team-members and anyone involved, which means that the understanding of why some end result was reached is made clear, and team-members can cooperate in prompting to improve designs and decision-making. While we do also use ADRs for tracking important decisions, this is also very useful for the portion of works that is not architectural but still carries important historical and domain/project context. I suspect software-archeology will be more pleasant in these cases where agents are used (though it can be verbose and have a lot of back-and-forth)
* I'm still the bottleneck, rarely able to keep more than 3 agents running contiuously at the same time (though its nice to have more instances available for the occasional side-quest)
* Giving the agents broader context including full set of relevant repos where session cwd = entire code directory and describing the "state of the world" in terms of what the team is focusing on and rough architectural descriptions goes a long way. For instance, we have been doing a poly-repo -> monorepo migration and so providing this information in AGENTS.md has led to me having to explain things a lot less. Its also more self-driven in explorying the various contracts and coupling between modules and services across repositories which is very useful
* Using open source infra and having IaC like e.g. GitOps is a major benefit. Both agents and human engineers do a lot better when there is awareness of the infastructure that binds and underpins the software they write

Configuration
* Codex app-server through the pinned VS Code Agent Host
* Mediated/MITM auth and networking, no access to secrets for gh, codex, studioctl
* VS Code Insiders Agents Window as UI, backed by the pinned VS Code Agent Host and Codex
* Tailscale Serve and the AgentDP VS Code extension for automatic remote-instance discovery
* lots of tools: browser, playwright, languages/compilers/toolchains, docker (normal install including daemon and systemd), kind (Kubernetes In Docker)


## Architecture

```
- src/    
  - apps/
    - agentdp-cli/          CLI frontend 
    - agentdp-guest         guestd (guest-side daemon) and guestctl (tool for agents to interact with certain tools)
    - agentdp-server        Agent manager/control-plane
  - backends/               Backend implementations for the sandbox
    - agentdp-qemu/         QEMU modules, e.g. QMP implementation
  - core/
    - agentdp-core/         Core domain models/types, e.g. manifest serialization/deserialization, cloud-init and guest OS config generation based on manifest, plugins etc
    - agentdp-protocol      Protocol types for IPC/integration between client<->server, server<->guestd (typically JSONL over UDS)
  - libs/                   Library/crate-things (TODO: perhaps a bit blurry in terms of core vs libs)
    - agentdp-base64        Base64 impl
    - agentdp-crypto        Integration points to rustls and related crates
    - agentdp-ds            Datastructures (spsc, ringbuffers, ...)
    - agentdp-network       Implementation of user-space networking stack based on the excellent smoltcp
    - agentdp-platform      Unified OS-level APIs for both host- and guest-side
    - agentdp-rand          PRNG
- tests/                    tests exercising public APIs only, preferred type of tests
  - agentdp-network-tests   Harness for deterministic simulation testing for the user-space networking stack (agentdp-network)
  - agentdp-test-support    Shared modules/fixtures for testing
```

### Building blocks

* Client (cli, frontend)
* Server (controller/manager of agents)
* Sandbox (e.g. QEMU VM)
* Harness (e.g. VS Code Agent Host + Codex + tooling in guestd/guestctl)

### General

- Written in Rust
  - Great type-system, good for low-level as well as high-level programming, errors-as-values, doesn't lie
  - Strict clippy-linting, e.g. denied unsafe/expect as much as possible
- Layered to maximize reuse of building-blocks (e.g. server should be able to run locally on dev-machines and in k8s in cloud-context)
- Minimal use of dependencies
  - This has always been a good principle to have, even moreso now with AI agents being able to generate good code for well-scoped tasks where there are good references/specs (base64 as example)

- Threat model:
  - The host, host user, and local `agentdp-server` are trusted.
  - Agent guest trust is a user decision. A user may copy credentials into the guest and let the agent use them directly, or keep real credentials on the host and expose only placeholders through mediated networking.
  - Mediated networking is the stricter mode: guests receive placeholders, real host secrets stay on the host, and the host substitutes them only for allowed destinations.
  - External network destinations are untrusted unless allowed by manifest policy.
  - Other local OS users are not the primary threat model for local development, but state and socket paths should still avoid accidental disclosure, collisions, and stale-resource failures.
  - Host-side persistence of runtime state is acceptable for local development when owned by the trusted user.


### Operator-pattern

Based on ideas from Kubernetes, which in turn is based on control-theory (think PID-loops and whatnot),
this is a good pattern for creating self-healing processes that are able to function independently and build automations.
Should be used across controller, server, guestd.

The [agent/runtime.rs](src/apps/agentdp-server/src/agent/runtime.rs) module is implemented as such,
essentially a controller loop with state machines and long-running work spawned as async tasks (e.g. bootstrapping a QEMU VM).
The idea was that this would mesh well with the k8s deployment eventually and that we could easily automate things such as disk resizing, automated sleep etc..
It could also be self-healing in the case of crashes.


### OS-specifics

Given the goals of (theoretically) supporting Windows, macOS and Linux both host- and guest-side,
I try to create unified APIs in the platform crate, while still having e.g. guest-specific OS submodule layers where it makes sense.


### Network

The network crate is the main/scariest part of the "data plane" of this project, directly interacting with guest/agent network traffic on (OSI L2-L3 upto 7 currently for the QEMU transport).
Here I have about as much testing code as I have source code, where all impurity is injected as part of "NetworkRuntime" capabilities (reactor backend, clock, ...).
The core of it is an eventloop orchestrating smoltcp as a virtual device/gateway consuming and emitting ethernet frames from and to the guest transport.
There are also benchmarks measuring ~2-4 gbps no my machine, which is pretty terrible considering iperf as a pipeline reaches 100+ gbps. Work to be done here, including on mem allocation strategy.


### Concurrency

- Tokio single-thread runtime where async is used (basically anywhere but network crate)
- Hand-rolled eventloop + statemachines in network crate (needs determinism, as such it doesnt assume anything about execution context/environment)
- Rc and RefCell over Arc and Mutex
  - lots of datastructures are simple to make a lot more performant, e.g. custom spsc over std/tokio mpsc (see ds crate)


### Testing

- Integration-level testing preferred (independent from implementation details)
  - Calling CLIs, UDS/JSON protocol, deterministic simulation testing of network
- Miri for unsafe/UB
- Loom for testing concurrent permutations (different interleavings and reentrancy)
- cargo-mutants (?)
- Benchmarks and allocations tests for hard perf invariants (e.g. dataplane part of network crate)
- Unittesting for invariants that arent easily tested on the integration-level


#### IPC

Mostly chosen JSONL over Unix Domain Sockets, as it is supported on all Windows 11 versions even (unified API in platform-crate).


## Background

This project started in part due to curiosity and as a learning experiment, and partly due to some frustration
with startups, "frontier labs" and "big tech" trying to capture/own both the creation of intelligence (models)
and the agents making use of them.

It seemed weird to me that in a world where agents are "virtual employees", that someone else should be
managing and running them, especially in medium/large companies where there is existing infrastructure, access control etc..
Wouldn't that be effectively like outsourcing your core value production?
When faced with build-or-buy decisions, one of the most important decision drivers are the consideration of whether
the thing to build/buy is "core to the organization". To me, if agents are "virtual employees", 
then they certainly are a core part of the organization and so I would be biased towards "build" for sure.

So this was an effort to learn more about the underpinnings and (potential) building blocks of agents,
and an attempt to show that companies and organizations can and should own their own agents.

A very basic version of what is now in this repo took 2 full days to build. That was essentially a manifest-driven
way to describe an agent, the PC/VM and its tools. Consisted of a QEMU VM with Code Server for editor/IDE and Codex CLI in a tmux session,
with a little JS glue to poll github PR events into the tmux Codex CLI session through `send-keys`.

What followed were 2-3 weeks of afternoon work to try to get this looking more like an actual "agent developer platform",
which you are now free to explore here in this repository.
