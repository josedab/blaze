.PHONY: build check test lint fmt doc bench bench-tpch clean clean-deep release all setup fix quick watch bump-patch bump-minor bump-major examples example deps deps-why ci new-module

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

# Run clippy on all features (enforces deny(clippy::unwrap_used) on extension modules)
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

# Deep clean: remove all generated artifacts and report reclaimed space
clean-deep:
	@echo "Removing build and generated artifacts..."
	@du -sh target/ wasm/pkg/ node/dist/ 2>/dev/null || true
	rm -rf target/ wasm/pkg/ node/dist/
	@echo "✅ Deep clean complete."

# Run examples
examples:
	cargo run --example basic_queries
	cargo run --example advanced_queries

# Run a specific example: make example NAME=basic_queries
example:
	@if [ -z "$(NAME)" ]; then echo "Usage: make example NAME=<example_name>"; echo "Available: basic_queries, advanced_queries"; exit 1; fi
	cargo run --example $(NAME)

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
	@echo "==> Installing cargo-watch (for 'make watch')..."
	@cargo install cargo-watch --quiet 2>/dev/null || echo "  ⚠ cargo-watch install failed (optional). Install manually: cargo install cargo-watch"
	@echo "==> Installing git hooks..."
	@mkdir -p .git/hooks
	@cp scripts/pre-commit .git/hooks/pre-commit 2>/dev/null || \
		echo '#!/bin/sh\ncargo fmt --check || { echo "Run cargo fmt first"; exit 1; }' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@cp scripts/commit-msg .git/hooks/commit-msg 2>/dev/null || true
	@chmod +x .git/hooks/commit-msg 2>/dev/null || true
	@echo "==> Building project..."
	cargo build
	@echo "==> Running tests..."
	cargo test
	@echo ""
	@echo "✅ Setup complete! Run 'make' to format + test."

# Quick iteration: build + test
dev: build test

# Show top-level dependency tree
deps:
	cargo tree --depth 1 --edges normal

# Show why a specific package is included: make deps-why PKG=datafusion
deps-why:
	@if [ -z "$(PKG)" ]; then echo "Usage: make deps-why PKG=<package_name>"; exit 1; fi
	cargo tree -i $(PKG)

# Version bumping
bump-patch:
	./scripts/bump-version.sh patch

bump-minor:
	./scripts/bump-version.sh minor

bump-major:
	./scripts/bump-version.sh major

# Scaffold a new module: make new-module NAME=my_feature [TYPE=file|dir]
new-module:
	@if [ -z "$(NAME)" ]; then echo "Usage: make new-module NAME=<module_name> [TYPE=file|dir]"; exit 1; fi
	@./scripts/new-module.sh $(NAME) $(or $(TYPE),dir)
