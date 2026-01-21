# Contributing to Blaze

Thank you for your interest in contributing to Blaze! This guide covers everything you need to get started.

## Quick Start

```bash
# Clone and build
git clone https://github.com/blaze-db/blaze.git
cd blaze

# First-time setup (installs hooks, verifies toolchain)
make setup

# Or manually:
cargo build        # ~10s debug build
cargo test         # Run 500+ tests (~0.2s)
cargo fmt          # Format code
```

## Prerequisites

- **Rust 1.92.0+** (pinned in `rust-toolchain.toml`, installed automatically by `rustup`)
- **Git** for version control

Optional:
- `cargo-watch` for live-reload development (`make watch`)

## Development Workflow

### Day-to-Day Commands

| Command | What it Does | When to Use |
|---------|-------------|-------------|
| `make` | Format + test | Before every commit |
| `make quick` | Syntax check + test | Fast iteration |
| `make fix` | Auto-fix formatting + clippy | Clean up before PR |
| `make watch` | Re-run checks on save | During development |
| `make lint` | Run clippy (advisory) | Check for issues |
| `make test-all` | Test all feature flags | Before submitting PR |

### Making Changes

1. **Create a branch**: `git checkout -b my-feature`
2. **Make your changes** following the conventions below
3. **Run `make`** to format and test
4. **Commit** with a descriptive message
5. **Push and open a PR**

### Pre-commit Hook

A pre-commit hook is installed by `make setup` that runs `cargo fmt --check` before each commit.
To install manually:

```bash
cp scripts/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Code Conventions

### Error Handling

Use `BlazeError` and `Result<T>` from `src/error.rs`:

```rust
use crate::error::{BlazeError, Result};

fn my_function() -> Result<T> {
    let value = something()?;  // Use ? for propagation
    Err(BlazeError::analysis("Column not found"))
}
```

### Testing

Tests live alongside code in `#[cfg(test)]` modules:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_my_feature() {
        // ...
    }
}
```

Run specific tests:
```bash
cargo test test_name            # Single test
cargo test module::tests::      # Module tests
```

### Style

- Run `cargo fmt` before committing
- Add comments only where logic is non-obvious
- Follow existing patterns in the codebase

## Project Structure

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture documentation.

Key directories:
- `src/` — Core query engine
- `src/planner/` — SQL planning and optimization
- `src/executor/` — Query execution operators
- `src/storage/` — Storage providers (memory, CSV, Parquet)
- `tests/` — Integration tests
- `examples/` — Example programs
- `benches/` — Benchmarks

## Feature Flags

Blaze uses feature flags for optional extensions. Core tests run without flags; use `--features all-extensions` for the full suite:

```bash
cargo test                          # Core tests only
cargo test --features all-extensions # All tests
```

## Need Help?

- **Architecture questions**: See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Bug reports**: [Open an issue](https://github.com/blaze-db/blaze/issues/new)
- **Detailed contributing guide**: [website/docs/contributing.md](website/docs/contributing.md)
