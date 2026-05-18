.PHONY: fmt lint build test deps install setup

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets

build:
	cargo build --workspace

test:
	AGENTDP_UPDATE_SNAPSHOTS=always cargo nextest run --workspace

deps:
	cargo tree --workspace

install:
	cargo build --release -p agentdp-cli -p agentdp-server
	target/release/agentctl self install

setup:
	cargo install --locked cargo-nextest
	cargo install --locked cargo-llvm-cov
	cargo install --locked cargo-mutants
	cargo install --locked cargo-fuzz
	cargo install --locked cross
	rustup component add llvm-tools-preview
	rustup toolchain add nightly
