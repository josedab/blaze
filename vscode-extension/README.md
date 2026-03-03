# Blaze SQL for Visual Studio Code

SQL language support for the [Blaze query engine](https://github.com/blaze-db/blaze).

## Features

- SQL syntax highlighting (DDL, DML, functions, window functions)
- Language server integration for autocomplete and diagnostics
- Comment toggling and bracket matching

## Requirements

- Blaze binary installed and in PATH (or configure `blaze-sql.serverPath`)

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `blaze-sql.serverPath` | `blaze` | Path to the Blaze binary |
| `blaze-sql.serverArgs` | `["--lsp"]` | Arguments for the LSP server |

## Building

```bash
cd vscode-extension
npm install
npm run compile
```

## Publishing

```bash
npx vsce package
npx vsce publish
```
