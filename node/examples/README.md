# Node.js Binding Examples

## Files

- **`quickstart.mjs`** — Minimal ESM quickstart: tables, queries, JSON loading
- **`demo.js`** — Comprehensive CommonJS demo with aggregations, joins, window functions

## Quickstart

```bash
# From the node/ directory
npm install
npm run build

# Run the quickstart
node examples/quickstart.mjs

# Or the full demo
node examples/demo.js
```

### Usage in your own project

```js
import { BlazeDB } from '@blaze-sql/node';

const db = await BlazeDB.create();

// Execute DDL
db.execute('CREATE TABLE users (id INT, name VARCHAR)');

// Query
const result = db.query('SELECT * FROM users');
console.log(result.rows);       // Array of row objects
console.log(result.rowCount);   // Number of rows

// Load files (Node.js only)
await db.registerCsv('sales', './data/sales.csv');
await db.registerParquet('events', './data/events.parquet');

// Load JSON data
db.registerJson('config', [{ key: 'timeout', value: 30 }]);

// Arrow IPC output (returns Node.js Buffer)
const arrowBuf = db.queryArrow('SELECT * FROM users');

db.close();
```

### File watching

```js
const db = await BlazeDB.create({
  baseDir: './data',
  watchFiles: true,   // auto-reload tables when files change
});
```
