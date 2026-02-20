# WASM Binding Examples

## Files

- **`quickstart.html`** — Minimal example: create tables, query, load JSON data
- **`playground.html`** — Full interactive SQL editor with drag-and-drop file loading

## Quickstart

```bash
# From the wasm/ directory
npm install
npm run build

# Serve examples locally
npx serve examples/
# Open http://localhost:3000/quickstart.html
```

### Usage in your own project

```js
import { initBlaze, BlazeDB } from '@aspect-build/blaze-wasm';

await initBlaze();
const db = new BlazeDB();

// Execute DDL
db.execute('CREATE TABLE users (id INT, name VARCHAR)');

// Query data
const result = db.query('SELECT * FROM users');
console.log(result.rows);      // Array of row objects
console.log(result.columns);   // Column names
console.log(result.rowCount);  // Number of rows

// Load data from JSON or CSV strings
db.registerJson('products', [{ id: 1, name: 'Widget' }]);
db.registerCsv('sales', 'id,amount\n1,100\n2,200');

// Get Arrow IPC binary (for use with Apache Arrow JS)
const arrowBuf = db.queryArrow('SELECT * FROM users');

db.close();
```
