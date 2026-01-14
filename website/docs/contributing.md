---
sidebar_position: 9
---

# Contributing

Thank you for your interest in contributing to Blaze! This guide will help you get started.

## Ways to Contribute

### Report Bugs

Found a bug? Please [open an issue](https://github.com/blaze-db/blaze/issues/new) with:

- **Title:** Clear, concise description
- **Environment:** Blaze version, OS, Rust version
- **Steps to reproduce:** Minimal code example
- **Expected behavior:** What should happen
- **Actual behavior:** What actually happens
- **Error messages:** Complete error output

### Suggest Features

Have an idea? [Open a feature request](https://github.com/blaze-db/blaze/issues/new) with:

- **Use case:** What problem does it solve?
- **Proposed solution:** How would it work?
- **Alternatives:** Other approaches considered
- **Examples:** Code snippets showing usage

### Improve Documentation

Documentation improvements are always welcome:

- Fix typos and grammatical errors
- Add missing examples
- Clarify confusing explanations
- Translate documentation

### Contribute Code

Code contributions follow this process:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Development Setup

### Prerequisites

- Rust 1.70 or later
- Git

### Clone and Build

```bash
# Fork on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/blaze.git
cd blaze

# Build
cargo build

# Run tests
cargo test

# Run clippy
cargo clippy

# Format code
cargo fmt
```

### Project Structure

```
blaze/
├── src/
│   ├── lib.rs              # Library entry point
│   ├── main.rs             # CLI binary
│   ├── error.rs            # Error types
│   ├── catalog/            # Table catalog
│   ├── executor/           # Query execution
│   ├── planner/            # Query planning
│   ├── sql/                # SQL parsing
│   ├── storage/            # Data storage
│   └── types/              # Type system
├── examples/               # Example code
├── benches/                # Benchmarks
├── tests/                  # Integration tests
└── website/                # Documentation site
```

## Coding Guidelines

### Code Style

- Follow Rust conventions and idioms
- Run `cargo fmt` before committing
- Run `cargo clippy` and address warnings
- Keep functions focused and small
- Write self-documenting code

### Error Handling

Use `BlazeError` and `Result<T>`:

```rust
use crate::error::{BlazeError, Result};

fn my_function() -> Result<T> {
    // Use ? for propagation
    let value = something_fallible()?;

    // Create errors with helper methods
    Err(BlazeError::analysis("Column not found"))
}
```

### Documentation

- Add doc comments to public APIs
- Include examples in documentation
- Keep comments up-to-date with code

```rust
/// Executes a SQL query and returns the results.
///
/// # Arguments
///
/// * `sql` - The SQL query string to execute
///
/// # Returns
///
/// A vector of Arrow RecordBatches containing the query results.
///
/// # Errors
///
/// Returns an error if the query is invalid or execution fails.
///
/// # Example
///
/// ```rust
/// let conn = Connection::in_memory()?;
/// let results = conn.query("SELECT 1 + 1 AS answer")?;
/// ```
pub fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
    // implementation
}
```

### Testing

- Write tests for new functionality
- Maintain test coverage
- Test edge cases and error conditions

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_query() {
        let conn = Connection::in_memory().unwrap();
        let result = conn.query("SELECT 1").unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_error_handling() {
        let conn = Connection::in_memory().unwrap();
        let result = conn.query("INVALID SQL");
        assert!(matches!(result, Err(BlazeError::Parse { .. })));
    }
}
```

## Pull Request Process

### Before Submitting

1. **Create an issue first** for non-trivial changes
2. **Discuss the approach** before writing code
3. **Keep PRs focused** - one feature/fix per PR

### PR Checklist

- [ ] Code compiles without errors
- [ ] All tests pass (`cargo test`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] New code has tests
- [ ] Documentation is updated
- [ ] Commit messages are clear

### Commit Messages

Follow conventional commits:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `test`: Tests
- `refactor`: Code refactoring
- `perf`: Performance improvement
- `chore`: Maintenance

Examples:
```
feat(parser): add support for HAVING clause
fix(executor): handle NULL values in aggregate functions
docs(readme): update installation instructions
test(planner): add tests for window functions
```

### Review Process

1. Submit PR with clear description
2. CI runs tests automatically
3. Maintainers review the code
4. Address feedback with new commits
5. Maintainers merge when approved

## Adding Features

### Adding a New SQL Function

1. **Add the function implementation** in `planner/physical_expr.rs`:

```rust
#[derive(Debug)]
pub struct MyFunctionExpr {
    args: Vec<Arc<dyn PhysicalExpr>>,
}

impl PhysicalExpr for MyFunctionExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // Implementation
    }
    fn name(&self) -> &str { "my_function" }
}
```

2. **Export the function** in `planner/mod.rs`

3. **Handle in binder** in `planner/binder.rs`:

```rust
match function_name.to_uppercase().as_str() {
    "MY_FUNCTION" => {
        // Create MyFunctionExpr
    }
    // ...
}
```

4. **Add tests**

5. **Update documentation**

### Adding a New Physical Operator

1. **Add operator struct** in `executor/operators.rs`

2. **Add variant** to `PhysicalPlan` in `planner/physical_plan.rs`

3. **Update methods** in PhysicalPlan: `schema()`, `children()`, `format_indent()`

4. **Handle in executor** in `executor/mod.rs`:

```rust
PhysicalPlan::MyOperator { ... } => {
    // Execute operator
}
```

5. **Handle in planner** in `planner/physical_planner.rs`

6. **Add tests** and **documentation**

## Benchmarking

### Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- query_execution
```

### Adding Benchmarks

Add benchmarks in `benches/`:

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use blaze::Connection;

fn benchmark_query(c: &mut Criterion) {
    let conn = Connection::in_memory().unwrap();
    // Setup...

    c.bench_function("my_query", |b| {
        b.iter(|| {
            conn.query("SELECT ...").unwrap()
        })
    });
}

criterion_group!(benches, benchmark_query);
criterion_main!(benches);
```

## Documentation

### Building Docs

```bash
# Rust docs
cargo doc --open

# Website
cd website
npm install
npm run start
```

### Writing Documentation

- Use clear, concise language
- Include code examples that compile
- Show expected output
- Link to related sections
- Follow existing style

## Code of Conduct

### Our Standards

- Be respectful and inclusive
- Welcome newcomers
- Accept constructive criticism
- Focus on what's best for the community

### Unacceptable Behavior

- Harassment or discrimination
- Trolling or insulting comments
- Personal attacks
- Publishing others' private information

### Enforcement

Violations may result in temporary or permanent ban from the project.

## Recognition

Contributors are recognized in:
- Release notes
- CONTRIBUTORS file
- README acknowledgments

Thank you for contributing to Blaze!

## Questions?

- **GitHub Discussions:** [Ask questions](https://github.com/blaze-db/blaze/discussions)
- **Issues:** [Report problems](https://github.com/blaze-db/blaze/issues)
