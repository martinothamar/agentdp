# AGENTS.md

Repository-local guidance for agentdp work.

## Commands

- Run commands from the repository root.
- Do not invoke Justfile targets directly. Use the Justfile as the command source of truth, then run the equivalent recipe commands yourself.
- Prefer `rtk` for commands with compacted output.
- Format: `cargo fmt --all` and `cargo fmt --manifest-path fuzz/Cargo.toml`.
- Lint: `rtk cargo clippy --workspace --all-targets` and `rtk cargo +nightly check --manifest-path fuzz/Cargo.toml --bins`.
- Build: `rtk cargo build --workspace`.
- Test: `AGENTDP_UPDATE_SNAPSHOTS=always rtk cargo nextest run --workspace -E 'not test(::e2e_tests::)'`.
- Loom concurrency test: `rtk cargo nextest run -p agentdp-ds --features loom --release`.
- Miri unsafe-memory test: `rtk cargo +nightly miri nextest run -p agentdp-ds`.
- E2E test: `AGENTDP_UPDATE_SNAPSHOTS=always rtk cargo nextest run --workspace -E 'test(::e2e_tests::)' --no-tests pass`.
- Coverage: run the `cargo llvm-cov` commands from the Justfile `coverage-network` recipe.
- Dependencies: `rtk cargo tree --workspace`.
- Guest tools: build `agentdp-guest` for `x86_64-unknown-linux-musl`, create `target/x86_64-unknown-linux-musl/release/agentdp-guest-tools`, then copy `guestd` and `guestctl` there.
- Install: prepare guest tools, build release `agentdp-cli`, `agentdp-server`, and `agentdp-guest`, then run `AGENTDP_GUEST_TOOL_DIR=target/x86_64-unknown-linux-musl/release/agentdp-guest-tools target/release/agentctl self install`; test installed `agentctl` from `PATH`.
- Setup: run the install/toolchain commands from the Justfile `setup` recipe only when explicitly requested.
- Fuzz smoke: run the `cargo +nightly fuzz run ... -- -runs=1` commands from the relevant Justfile fuzz recipe.
- Mutants: `rtk cargo mutants --workspace`.

## Guidance

- Prefer e2e/integration tests for user-visible CLI behavior.
- Snapshot tests auto-update during the full test gate; review changed snapshot files through normal `rtk git diff`.
- Do not override `HOME`, `XDG_*`, `CARGO_HOME`, or `RUSTUP_HOME` for normal verification. Use custom environment variables only when explicitly testing platform path-resolution behavior.
- Use `rtk cargo nextest` for targeted test runs, e.g. `rtk cargo nextest run -p agentdp-network`.
- Keep command guidance in this file synchronized with the Justfile.
