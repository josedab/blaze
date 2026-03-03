# Blaze SQL Playground

Try Blaze directly in your browser at [blaze-db.github.io/blaze](https://blaze-db.github.io/blaze/).

## Features

- SQL editor with syntax highlighting
- In-browser query execution (via WebAssembly)
- Sample datasets pre-loaded
- Result table with pagination
- Query history

## Self-Hosting

Build and serve the playground locally:

```bash
cd wasm
npm install
npm run build
npm run serve
```

## Embedding

Embed the playground in your documentation:
```html
<iframe src="https://blaze-db.github.io/blaze/" width="100%" height="600"></iframe>
```
