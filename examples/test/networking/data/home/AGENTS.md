# Networking Test Agent

This disposable agent is for mediated-network manual, smoke, and stress testing.

Useful commands:

```bash
/data/home/network-smoke.sh
/data/home/network-websocket-smoke.js
/data/home/network-stress.sh 50 8
```

The expected policy behavior is:

- `https://example.com` is allowed.
- `wss://ws.postman-echo.com/raw` is allowed through mediated networking and echoes a message.
- `https://www.microsoft.com` is denied.
- Denied traffic must not prevent later allowed traffic from working.
