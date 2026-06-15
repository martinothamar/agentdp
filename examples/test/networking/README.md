# Networking test agent

Manual, smoke, and stress test harness for the mediated networking stack.

The manifest uses a strict allow-list so the instance exercises host-side DNS, TCP forwarding, direct TLS interception, and deny behavior. It is safe to run from Windows or Linux hosts as long as the installed backend can run QEMU instances.

WebSocket support has two paths:

- Direct `ws://`/`wss://` traffic uses the generic mediated TCP egress path.
- TLS-intercepted `wss://` traffic must preserve HTTP `101 Switching Protocols` upgrades after host policy checks.

The VM-level WebSocket smoke test connects directly to `wss://ws.postman-echo.com/raw` through mediated networking.

## Start

From this directory:

```powershell
agentctl create -f agent.yaml net-0 --port ssh:4220
agentctl up -f agent.yaml net-0
```

For two instances:

```powershell
agentctl create -f agent.yaml net-0 --port ssh:4220
agentctl create -f agent.yaml net-1 --port ssh:4221
agentctl up -f agent.yaml net-0
agentctl up -f agent.yaml net-1
```

## Run Checks

```powershell
agentctl exec -f agent.yaml net-0 -- /data/home/network-smoke.sh
agentctl exec -f agent.yaml net-0 -- /data/home/network-websocket-smoke.js
agentctl exec -f agent.yaml net-0 -- /data/home/network-stress.sh 50 8
```

`network-smoke.sh` exercises DNS, allowed/denied HTTPS, and Git-over-HTTPS.
`network-websocket-smoke.js` exercises a raw WSS upgrade and echo.
`network-stress.sh` repeats allowed/denied HTTPS checks with bounded concurrency.

Use `net-1` for a second agent or a Linux-host comparison run.

## Stop

```powershell
agentctl down -f agent.yaml net-0
agentctl down -f agent.yaml net-1
agentctl rm -f agent.yaml net-0
agentctl rm -f agent.yaml net-1
```
