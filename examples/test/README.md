# Test agents

Disposable agent definitions for manual, smoke, and stress testing.

These examples are intentionally smaller than `examples/altinn-studio` and are meant to exercise one subsystem at a time. Use short-lived instance names and remove them after testing.

## Mediated CA troubleshooting

The Podman plugin configures default CA env and a read-only CA bind mount. Image-defined env or explicit user `--env` flags can still override those defaults. For images that set their own CA bundle, pass the agentdp CA explicitly:

```sh
podman run \
  --env CURL_CA_BUNDLE=/run/agentdp/ca/ca-bundle.pem \
  --env SSL_CERT_FILE=/run/agentdp/ca/ca-bundle.pem \
  --volume /var/lib/agentdp/ca/ca-bundle.pem:/run/agentdp/ca/ca-bundle.pem:ro \
  IMAGE
```
