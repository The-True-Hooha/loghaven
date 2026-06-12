BIN := loghaven

.PHONY: build release check test clean run run-fg install fmt lint help init status stop

build:
	cargo build

release:
	cargo build --release

check:
	cargo check

test:
	cargo test

fmt:
	cargo fmt

lint:
	cargo clippy

clean:
	cargo clean

init:
	cargo run -- init

init-force:
	cargo run -- init --force

run:
	cargo run -- run

run-fg:
	cargo run -- run --foreground

status:
	cargo run -- status

stop:
	cargo run -- stop

stop-force:
	cargo run -- stop --force

install: release
	cargo install --path .

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  build      dev build"
	@echo "  release    optimized release build"
	@echo "  check      type check only (fast)"
	@echo "  test       run tests"
	@echo "  fmt        format code"
	@echo "  lint       clippy lint"
	@echo "  clean      remove build artifacts"
	@echo "  init       initialize config"
	@echo "  init-force overwrite existing config"
	@echo "  run        run daemon in background"
	@echo "  run-fg     run daemon in foreground"
	@echo "  status     check daemon status"
	@echo "  stop       graceful stop"
	@echo "  stop-force force kill daemon"
	@echo "  install    install binary to cargo bin"
