# AGENTS.md

Repository-local guidance for agentdp work.

- Prefer the Makefile/Justfile targets for routine verification: `make fmt`, `make lint`, `make build`, `make test`, and `make install`.
- Prefer e2e/integration tests for user-visible CLI behavior.
- Snapshot tests auto-update during `make test`; review changed snapshot files through normal `git diff`.
- For install verification, run `make install`, then test the installed `agentctl` directly from `PATH`.
- Do not override `HOME`, `XDG_*`, `CARGO_HOME`, or `RUSTUP_HOME` for normal verification. Use custom environment variables only when explicitly testing platform path-resolution behavior.

`make` and `just` can be used interchangeably.
