# self-dev

Development harness for working on `agentdp` from inside an agent VM.

This instance is intended for nested virtualization. It installs QEMU inside
the VM and checks that `/dev/kvm` is available so inner `agentctl up` lifecycle
tests can use the normal QEMU/KVM backend.

Create and start an instance:

```powershell
agentctl create -f agent.yaml dev-0 --port code_server:3090
agentctl up -f agent.yaml dev-0
```

Open code-server on:

```text
http://127.0.0.1:3090
```

Codex runs in the `agentdp` tmux session with the local YOLO wrapper:

```text
--sandbox danger-full-access --ask-for-approval never
```

The VM clones `https://github.com/martinothamar/agentdp.git` into:

```text
/data/home/code/agentdp
```

The clone runs from `bootstrap.sh` after GitHub auth has been registered with
git. GitHub and Codex auth use mediated placeholders, matching the
altinn-studio example, while the VM keeps normal QEMU user networking so nested
agent lifecycle tests stay close to a plain VM.

If the host has unpushed work that the VM needs, push it first or transfer a patch before continuing inside the VM.
