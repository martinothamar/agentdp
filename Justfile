default:
    just --list

linux-guest-target := "x86_64-unknown-linux-musl"
guest-tool-dir := "target/" + linux-guest-target + "/release/agentdp-guest-tools"

fmt:
    cargo fmt --all
    cargo fmt --manifest-path fuzz/Cargo.toml

lint:
    cargo clippy --workspace --all-targets
    cargo +nightly check --manifest-path fuzz/Cargo.toml --bins

build:
    cargo build --workspace

test:
    AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run --workspace -E 'not test(::e2e_tests::)'

test-loom:
    cargo nextest run -p agentdp-ds --features loom --release

test-miri:
    cargo +nightly miri nextest run -p agentdp-ds

test-e2e:
    AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run --workspace --exclude agentdp-network-tests -E 'test(::e2e_tests::)' --no-tests pass

test-randomized-network profile="debug":
    @case "{{ profile }}" in debug|release) ;; *) echo "profile must be debug or release" >&2; exit 2;; esac; \
    profile_flag=""; \
    if [ "{{ profile }}" = "release" ]; then profile_flag="--release"; fi; \
    AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run $profile_flag -p agentdp-network-tests randomized_https_wss_workloads_preserve_public_behavior randomized_hot_dataplane_https_requests_preserve_public_behavior randomized_hot_concurrent_dataplane_https_requests_preserve_public_behavior

generate-network-test-tls-fixtures:
    cargo run -p agentdp-network-tests --bin generate-network-test-tls-fixtures

stress-randomized-network seconds="600" profile="debug":
    @case "{{ profile }}" in debug|release) ;; *) echo "profile must be debug or release" >&2; exit 2;; esac; \
    profile_flag=""; \
    if [ "{{ profile }}" = "release" ]; then profile_flag="--release"; fi; \
    AGENTDP_NETWORK_RANDOMIZED_LONG_SECONDS={{ seconds }} AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run $profile_flag -p agentdp-network-tests randomized_https_wss_workloads_long_run --run-ignored only

stress-randomized-network-hot seconds="600" profile="debug":
    @case "{{ profile }}" in debug|release) ;; *) echo "profile must be debug or release" >&2; exit 2;; esac; \
    profile_flag=""; \
    if [ "{{ profile }}" = "release" ]; then profile_flag="--release"; fi; \
    AGENTDP_NETWORK_RANDOMIZED_SECONDS={{ seconds }} AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run $profile_flag -p agentdp-network-tests randomized_hot_dataplane_https_requests_preserve_public_behavior

stress-randomized-network-hot-concurrent seconds="600" profile="debug":
    @case "{{ profile }}" in debug|release) ;; *) echo "profile must be debug or release" >&2; exit 2;; esac; \
    profile_flag=""; \
    if [ "{{ profile }}" = "release" ]; then profile_flag="--release"; fi; \
    AGENTDP_NETWORK_RANDOMIZED_SECONDS={{ seconds }} AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run $profile_flag -p agentdp-network-tests randomized_hot_concurrent_dataplane_https_requests_preserve_public_behavior

stress-randomized-network-all seconds="600" profile="debug":
    @case "{{ profile }}" in debug|release) ;; *) echo "profile must be debug or release" >&2; exit 2;; esac; \
    profile_flag=""; \
    if [ "{{ profile }}" = "release" ]; then profile_flag="--release"; fi; \
    AGENTDP_NETWORK_RANDOMIZED_LONG_SECONDS={{ seconds }} AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run $profile_flag -p agentdp-network-tests randomized_https_wss_workloads_long_run --run-ignored only; \
    AGENTDP_NETWORK_RANDOMIZED_SECONDS={{ seconds }} AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run $profile_flag -p agentdp-network-tests randomized_hot_dataplane_https_requests_preserve_public_behavior; \
    AGENTDP_NETWORK_RANDOMIZED_SECONDS={{ seconds }} AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run $profile_flag -p agentdp-network-tests randomized_hot_concurrent_dataplane_https_requests_preserve_public_behavior

coverage-network:
    cargo llvm-cov nextest --package agentdp-network --no-report -E 'not test(::e2e_tests::)' --no-tests pass
    cargo llvm-cov report --package agentdp-network --html --output-dir target/llvm-cov/agentdp-network
    cargo llvm-cov report --package agentdp-network --text --show-missing-lines --output-path target/llvm-cov/agentdp-network.txt
    cargo llvm-cov report --package agentdp-network --summary-only --fail-under-lines 80
    @echo "coverage report: target/llvm-cov/agentdp-network/html/index.html"
    @echo "coverage text: target/llvm-cov/agentdp-network.txt"

[windows]
wsl_fmt:
    WINDOWS_REPO='{{justfile_directory()}}' wsl -e bash -lc 'cd "$(wslpath -a "$WINDOWS_REPO")" && cargo fmt --all && cargo fmt --manifest-path fuzz/Cargo.toml'

[windows]
wsl_lint:
    WINDOWS_REPO='{{justfile_directory()}}' wsl -e bash -lc 'cd "$(wslpath -a "$WINDOWS_REPO")" && CARGO_TARGET_DIR=target/wsl cargo clippy --workspace --all-targets && CARGO_TARGET_DIR=target/wsl cargo +nightly check --manifest-path fuzz/Cargo.toml --bins'

[windows]
wsl_build:
    WINDOWS_REPO='{{justfile_directory()}}' wsl -e bash -lc 'cd "$(wslpath -a "$WINDOWS_REPO")" && CARGO_TARGET_DIR=target/wsl cargo build --workspace'

[windows]
wsl_test:
    WINDOWS_REPO='{{justfile_directory()}}' wsl -e bash -lc 'cd "$(wslpath -a "$WINDOWS_REPO")" && CARGO_TARGET_DIR=target/wsl AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run --workspace -E "not test(::e2e_tests::)"'

[windows]
wsl_test_e2e:
    WINDOWS_REPO='{{justfile_directory()}}' wsl -e bash -lc 'cd "$(wslpath -a "$WINDOWS_REPO")" && CARGO_TARGET_DIR=target/wsl AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run --workspace --exclude agentdp-network-tests -E "test(::e2e_tests::)" --no-tests pass'

deps:
    cargo tree --workspace

[windows]
prepare-guest-tools:
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts/prepare-guest-tools.ps1 -OutputDir "{{guest-tool-dir}}"

[unix]
prepare-guest-tools:
    cargo build --release -p agentdp-guest --target {{linux-guest-target}}
    mkdir -p "{{guest-tool-dir}}"
    cp "target/{{linux-guest-target}}/release/guestd" "{{guest-tool-dir}}/guestd"
    cp "target/{{linux-guest-target}}/release/guestctl" "{{guest-tool-dir}}/guestctl"

[windows]
install: prepare-guest-tools
    cargo build --release -p agentdp-cli -p agentdp-server -p agentdp-guest
    AGENTDP_GUEST_TOOL_DIR="{{guest-tool-dir}}" target/release/agentctl self install

[unix]
install: prepare-guest-tools
    cargo build --release -p agentdp-cli -p agentdp-server -p agentdp-guest
    AGENTDP_GUEST_TOOL_DIR="{{guest-tool-dir}}" target/release/agentctl self install

setup:
    cargo install --locked cargo-nextest
    cargo install --locked cargo-llvm-cov
    cargo install --locked cargo-mutants --version 27.0.0
    cargo install --locked cargo-fuzz --version 0.13.1
    cargo install --locked cross
    rustup component add llvm-tools-preview
    rustup toolchain add nightly
    rustup component add miri --toolchain nightly

fuzz-smoke:
    cargo +nightly fuzz run base64_encode -- -runs=1
    cargo +nightly fuzz run protocol_decode -- -runs=1
    cargo +nightly fuzz run server_guest_frame_decode -- -runs=1
    cargo +nightly fuzz run manifest_parse_validate -- -runs=1
    cargo +nightly fuzz run cloud_init_render -- -runs=1
    cargo +nightly fuzz run network_secret_bindings -- -runs=1

fuzz-network-smoke:
    cargo +nightly fuzz run network_secret_bindings -- -runs=1

mutants:
    cargo mutants --workspace
