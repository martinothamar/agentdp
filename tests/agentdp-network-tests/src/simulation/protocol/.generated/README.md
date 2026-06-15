Generated TLS fixtures for network simulation tests.

These PEM files are test-only certificate authorities and server identities for
deterministic local TLS scenarios. They are not runtime secrets.

Regenerate with:

```sh
cargo run -p agentdp-network-tests --bin generate-network-test-tls-fixtures
```
