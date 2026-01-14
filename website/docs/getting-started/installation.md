---
sidebar_position: 1
---

# Installation

This guide covers installing Blaze for Rust and Python projects.

## Rust

### Requirements

- Rust 1.70 or later
- Cargo (included with Rust)

### Add to Your Project

Add Blaze to your `Cargo.toml`:

```bash
cargo add blaze
```

Or manually add the dependency:

```toml title="Cargo.toml"
[dependencies]
blaze = "0.1"
```

### Verify Installation

Create a simple test to verify the installation:

```rust title="src/main.rs"
use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;
    let results = conn.query("SELECT 1 + 1 AS answer")?;

    for batch in results {
        println!("Rows: {}", batch.num_rows());
    }

    Ok(())
}
```

Run it:

```bash
cargo run
```

You should see `Rows: 1` printed.

## Python

### Requirements

- Python 3.8 or later
- pip or conda

### Install from PyPI

```bash
pip install blaze-db
```

### Install from Source

To install from source with the latest features:

```bash
git clone https://github.com/blaze-db/blaze.git
cd blaze
pip install maturin
maturin develop --features python
```

### Verify Installation

```python
import blaze

conn = blaze.Connection()
results = conn.query("SELECT 1 + 1 AS answer")
print(results)
```

## Build from Source

For development or to get the latest features:

```bash
# Clone the repository
git clone https://github.com/blaze-db/blaze.git
cd blaze

# Build in release mode
cargo build --release

# Run tests
cargo test

# Run the CLI
cargo run
```

### Build Options

```bash
# Debug build (faster compilation, slower runtime)
cargo build

# Release build with optimizations
cargo build --release

# Build with Python bindings
cargo build --release --features python

# Run clippy for linting
cargo clippy

# Format code
cargo fmt
```

## Feature Flags

Blaze supports optional features that can be enabled in `Cargo.toml`:

| Feature | Description | Default |
|---------|-------------|---------|
| `python` | Python bindings via PyO3 | Off |

Enable features:

```toml title="Cargo.toml"
[dependencies]
blaze = { version = "0.1", features = ["python"] }
```

## Platform Support

Blaze is tested on:

| Platform | Architecture | Status |
|----------|-------------|--------|
| Linux | x86_64 | Fully supported |
| Linux | aarch64 | Fully supported |
| macOS | x86_64 | Fully supported |
| macOS | Apple Silicon | Fully supported |
| Windows | x86_64 | Supported |

## Troubleshooting

### Compilation Errors

If you encounter compilation errors, ensure you have the latest Rust toolchain:

```bash
rustup update stable
```

### Missing System Dependencies

On Linux, you may need to install additional development libraries:

```bash
# Ubuntu/Debian
sudo apt-get install build-essential

# Fedora/RHEL
sudo dnf groupinstall "Development Tools"
```

### Python Binding Issues

If Python bindings fail to build:

```bash
# Ensure maturin is installed
pip install maturin

# Build with verbose output
maturin develop --features python -v
```

## Next Steps

Now that Blaze is installed, continue to the [Quick Start](/docs/getting-started/quick-start) guide to run your first queries.
