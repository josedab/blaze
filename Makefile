.PHONY: build check test lint fmt doc bench bench-tpch clean release all setup fix quick watch bump-patch bump-minor bump-major examples ci

# Default: format + test (always works for new contributors)
all: fmt test

# Build in debug mode
build:
	cargo build

# Build in release mode
release:
	cargo build --release

# Run all checks (fmt + lint + test)
check: fmt-check lint test

# Run tests (core only)
test:
	cargo test

# Run tests including all extension modules
test-all:
	cargo test --features all-extensions

# Run clippy lints (advisory, does not block by default)
lint:
	cargo clippy

# Run clippy with strict warnings-as-errors
lint-strict:
	cargo clippy -- -D warnings

# Run clippy on all features
lint-all:
	cargo clippy --features all-extensions

# Format code
fmt:
	cargo fmt

# Check formatting without modifying files
fmt-check:
	cargo fmt --check

# Auto-fix: format + apply clippy suggestions
fix:
	cargo fmt
	cargo clippy --fix --allow-dirty --allow-staged

# Quick iteration: check syntax + run tests (no full build)
quick:
	cargo check
	cargo test

# Watch mode: re-run checks on file changes (requires cargo-watch)
watch:
	cargo watch -x check -x test

# Generate documentation
doc:
	cargo doc --no-deps --open

# Run benchmarks
bench:
	cargo bench

# Run TPC-H benchmarks only
bench-tpch:
	cargo bench --bench tpch_benchmarks

# Clean build artifacts
clean:
	cargo clean

# Run examples
examples:
	cargo run --example basic_queries
	cargo run --example advanced_queries

# Run the same checks as CI (use before submitting a PR)
ci: fmt-check lint-strict test test-all
	@cargo clippy --features all-extensions -- -D warnings
	@RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --quiet
	@echo ""
	@echo "✅ All CI checks passed locally!"

# Run the CLI
run:
	cargo run

# First-time setup for contributors
setup:
	@echo "==> Installing Rust toolchain..."
	rustup show
	@echo "==> Installing cargo-watch (optional, for 'make watch')..."
	-cargo install cargo-watch 2>/dev/null || echo "  (skipped, install manually if needed)"
	@echo "==> Installing pre-commit hook..."
	@mkdir -p .git/hooks
	@cp scripts/pre-commit .git/hooks/pre-commit 2>/dev/null || \
		echo '#!/bin/sh\ncargo fmt --check || { echo "Run cargo fmt first"; exit 1; }' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "==> Building project..."
	cargo build
	@echo "==> Running tests..."
	cargo test
	@echo ""
	@echo "✅ Setup complete! Run 'make' to format + test."

# Quick iteration: build + test
dev: build test

# Version bumping
bump-patch:
	./scripts/bump-version.sh patch

bump-minor:
	./scripts/bump-version.sh minor

bump-major:
	./scripts/bump-version.sh major
