LINUX_GUEST_TARGET := x86_64-unknown-linux-musl
GUEST_TOOL_DIR := target/$(LINUX_GUEST_TARGET)/release/agentdp-guest-tools

.PHONY: fmt lint build test deps install prepare-guest-tools setup fuzz-smoke fuzz-network-smoke mutants

fmt:
	cargo fmt --all
	cargo fmt --manifest-path fuzz/Cargo.toml

lint:
	cargo clippy --workspace --all-targets
	cargo +nightly check --manifest-path fuzz/Cargo.toml --bins

build:
	cargo build --workspace

test:
	AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run --workspace

deps:
	cargo tree --workspace

prepare-guest-tools:
	cargo build --release -p agentdp-guest --target $(LINUX_GUEST_TARGET)
	mkdir -p "$(GUEST_TOOL_DIR)"
	cp target/$(LINUX_GUEST_TARGET)/release/guestd "$(GUEST_TOOL_DIR)/guestd"
	cp target/$(LINUX_GUEST_TARGET)/release/guestctl "$(GUEST_TOOL_DIR)/guestctl"

install: prepare-guest-tools
	cargo build --release -p agentdp-cli -p agentdp-server -p agentdp-guest
	AGENTDP_GUEST_TOOL_DIR="$(GUEST_TOOL_DIR)" target/release/agentctl self install

setup:
	cargo install --locked cargo-nextest
	cargo install --locked cargo-llvm-cov
	cargo install --locked cargo-mutants --version 27.0.0
	cargo install --locked cargo-fuzz --version 0.13.1
	cargo install --locked cross
	rustup component add llvm-tools-preview
	rustup toolchain add nightly

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
