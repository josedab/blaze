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
cargo build        # ~4-5min first build, ~10s incremental
cargo test         # Run 900+ tests (~0.2s)
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
| `make ci` | Run all CI checks locally | Before submitting PR |
| `make watch` | Re-run checks on save | During development |
| `make lint` | Run clippy (advisory) | Check for issues |
| `make test-all` | Test all feature flags | Before submitting PR |

### Making Changes

1. **Create a branch** using the naming convention below
2. **Make your changes** following the conventions below
3. **Run `make`** to format and test
4. **Commit** using [Conventional Commits](#commit-message-conventions) format
5. **Push and open a PR**

### Branch Naming

Use descriptive branch names with a category prefix:

| Prefix | Purpose |
|--------|---------|
| `feature/` | New features or enhancements |
| `fix/` | Bug fixes |
| `docs/` | Documentation changes |
| `chore/` | Maintenance, CI, dependencies |
| `refactor/` | Code refactoring |
| `test/` | Adding or updating tests |

**Examples:**
```
feature/lateral-join-support
fix/null-handling-in-hash-join
docs/update-contributing-guide
chore/upgrade-arrow-v58
refactor/extract-common-storage-logic
test/add-window-function-coverage
```

### Pre-commit Hook

A pre-commit hook is installed by `make setup` that runs `cargo fmt --check` and `cargo clippy -- -D warnings` before each commit.
To install manually:

```bash
cp scripts/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Commit Message Conventions

We follow the [Conventional Commits](https://www.conventionalcommits.org/) standard. Each commit message should have the format:

```
<type>(<optional scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | When to Use |
|------|-------------|
| `feat` | New feature or functionality |
| `fix` | Bug fix |
| `docs` | Documentation only changes |
| `style` | Formatting, missing semicolons (no code change) |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf` | Performance improvement |
| `test` | Adding or updating tests |
| `chore` | Build process, CI, or tooling changes |
| `ci` | CI configuration changes |

### Examples

```
feat(parser): add support for LATERAL JOIN syntax
fix(executor): handle NULL values in hash join probe
docs: update CONTRIBUTING.md with commit conventions
test(planner): add optimizer rule coverage for predicate pushdown
refactor(storage): extract common CSV/Parquet read logic
perf(executor): use SIMD for batch filtering
chore: update arrow dependency to v57
```

### Breaking Changes

Append `!` after the type/scope for breaking changes, and include a `BREAKING CHANGE:` footer:

```
feat(api)!: rename Connection::new() to Connection::in_memory()

BREAKING CHANGE: Connection::new() has been renamed to Connection::in_memory()
for clarity. Update all call sites.
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

## Debugging

Blaze uses `tracing` with `env-filter`. Enable debug logging with:

```bash
RUST_LOG=blaze=debug cargo run
RUST_LOG=blaze::executor=debug,blaze::planner=info cargo run
RUST_LOG=blaze=trace cargo run   # very verbose
```

## Pull Request Process

### Submitting a PR

1. Ensure your branch is up to date with `main`
2. Run `make ci` to verify all checks pass locally
3. Open a PR with a clear title using [Conventional Commits](#commit-message-conventions) format
4. Fill out the PR description with: what changed, why, and how to test
5. Link any related issues (e.g., `Fixes #123`)

### Review Process

- **Review timeline**: Maintainers aim to provide initial feedback within 3 business days
- **Who reviews**: At least one maintainer must approve before merge
- **Requesting review**: PRs are automatically assigned to maintainers. If you haven't heard back, leave a comment pinging `@blaze-db/maintainers`

### Merge Requirements

- All CI checks pass (formatting, clippy, tests)
- At least one maintainer approval
- No unresolved review comments
- Branch is up to date with `main`

### After Submitting

- **Be patient** — maintainers review in order of submission
- **Respond to feedback** — push fixes as new commits (don't force-push during review)
- **Squash on merge** — PRs are squash-merged; your commit messages will be combined

## Getting Started — Good First Issues

New to the project? Here are some areas that are great for first-time contributors:

### Starter Areas

| Area | Description | Difficulty |
|------|-------------|------------|
| **Add scalar functions** | Implement missing SQL functions (e.g., `REPEAT`, `POSITION`, `INITCAP`) in `src/planner/` | Easy |
| **Improve error messages** | Add context or suggestions to error messages in `src/error.rs` | Easy |
| **Add test coverage** | Write integration tests for untested SQL features in `tests/` | Easy |
| **Documentation** | Fix typos, add examples, improve API docs | Easy |
| **Add output formats** | Extend output formatting in `src/output.rs` (e.g., Markdown tables) | Medium |
| **Optimizer rules** | Implement new optimization rules in `src/planner/optimizer.rs` | Medium |

### Finding Issues

- Look for issues labeled [`good first issue`](https://github.com/blaze-db/blaze/labels/good%20first%20issue) on GitHub
- Issues labeled [`help wanted`](https://github.com/blaze-db/blaze/labels/help%20wanted) are also welcoming to outside contributors
- If you want to work on something not yet filed, open an issue first to discuss the approach

### Tips for First-Time Contributors

1. Start by reading [ARCHITECTURE.md](docs/ARCHITECTURE.md) to understand the codebase
2. Run `make setup` to get your environment ready
3. Pick a small, well-defined issue
4. Ask questions in the issue thread if anything is unclear

## Need Help?

- **Architecture questions**: See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Bug reports**: [Open an issue](https://github.com/blaze-db/blaze/issues/new)
- **Detailed contributing guide**: [website/docs/contributing.md](website/docs/contributing.md)
