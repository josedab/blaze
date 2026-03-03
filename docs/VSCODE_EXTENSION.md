# VS Code SQL Extension

Blaze includes an LSP (Language Server Protocol) implementation for SQL editing.

## Features

- SQL syntax highlighting
- Autocomplete for table names, column names, and functions
- Signature help for function parameters
- Inline error diagnostics
- Document symbols

## Setup

The LSP server is built into the Blaze binary:

```bash
cargo build --release
```

### VS Code Configuration

Add to `.vscode/settings.json`:
```json
{
    "blaze.lsp.path": "./target/release/blaze",
    "blaze.lsp.args": ["--lsp"]
}
```

## Future Plans

A dedicated VS Code extension package will be published to the VS Code Marketplace.
