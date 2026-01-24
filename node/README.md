# @blaze-sql/node

Node.js bindings for the Blaze SQL query engine. Extends the WASM package with filesystem integration for reading Parquet, CSV, and JSON files.

## Features

- **File System Integration**: Read data directly from CSV, JSON, and Parquet files
- **File Watching**: Auto-reload tables when source files change
- **Full SQL Support**: All features from `@blaze-sql/wasm` plus Node.js-specific APIs
- **Arrow Integration**: Export results as Arrow buffers for efficient data processing

## Installation

```bash
npm install @blaze-sql/node
# or
yarn add @blaze-sql/node
# or
pnpm add @blaze-sql/node
```

## Quick Start

```typescript
import { BlazeDB } from '@blaze-sql/node';

async function main() {
  // Create a connection
  const db = await BlazeDB.create();

  // Register data from files
  await db.registerCsv('users', './data/users.csv');
  await db.registerJsonFile('orders', './data/orders.json');

  // Or register data directly
  await db.registerJson('products', [
    { id: 1, name: 'Widget', price: 9.99 },
    { id: 2, name: 'Gadget', price: 19.99 },
  ]);

  // Run queries
  const result = db.query(`
    SELECT u.name, COUNT(o.id) as order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.name
    ORDER BY order_count DESC
  `);

  console.log(result.rows);

  // Cleanup
  db.close();
}

main();
```

## API Reference

### Creating a Connection

```typescript
// Basic creation
const db = await BlazeDB.create();

// With configuration
const db = await BlazeDB.create({
  baseDir: './data',      // Base directory for relative paths
  watchFiles: true,       // Auto-reload on file changes
  maxMemory: 512 * 1024 * 1024,  // 512 MB limit
});
```

### Registering Data from Files

```typescript
// CSV files
await db.registerCsv('users', './users.csv', {
  encoding: 'utf-8',
  delimiter: ',',
  header: true,
});

// JSON files
await db.registerJsonFile('orders', './orders.json');

// Parquet files (requires apache-arrow)
await db.registerParquet('events', './events.parquet');
```

### Registering Data Directly

```typescript
// JSON data
await db.registerJson('products', [
  { id: 1, name: 'Widget', price: 9.99 },
]);

// CSV string
db.registerCsv('data', 'id,name,value\n1,foo,100\n2,bar,200');
```

### Executing Queries

```typescript
// Execute and get results
const result = db.query('SELECT * FROM users WHERE active = true');
console.log(result.rows);
console.log(`${result.rowCount} rows in ${result.executionTimeMs}ms`);

// Execute with options
const result = db.query('SELECT * FROM users', {
  maxRows: 100,
  includeSchema: true,
});

// Get results as Arrow buffer
const arrowBuffer = db.queryArrow('SELECT * FROM users');
// Use with apache-arrow: tableFromIPC(arrowBuffer)
```

### Managing Tables

```typescript
// List tables
const tables = db.listTables();
// ['users', 'orders', 'products']

// Get table schema
const schema = db.getTableSchema('users');
// { columns: [{ name: 'id', type: 'Int64', nullable: false }, ...] }

// Drop table
db.dropTable('temp_data');

// Clear all tables
db.clear();
```

### Statistics

```typescript
const stats = db.getStats();
console.log(`Queries: ${stats.queriesExecuted}`);
console.log(`Memory: ${stats.memoryUsed} bytes`);
console.log(`Tables: ${stats.tableCount}`);
```

## File Watching

Enable auto-reload when source files change:

```typescript
const db = await BlazeDB.create({ watchFiles: true });

// This table will auto-reload when users.csv changes
await db.registerCsv('users', './users.csv');
```

## Working with Parquet

Parquet support requires the `apache-arrow` package:

```bash
npm install apache-arrow
```

```typescript
import { BlazeDB } from '@blaze-sql/node';
import { tableFromIPC } from 'apache-arrow';

const db = await BlazeDB.create();

// Read Parquet file
await db.registerParquet('events', './events.parquet');

// Query and get Arrow output
const arrowBuffer = db.queryArrow('SELECT * FROM events');
const table = tableFromIPC(arrowBuffer);
```

## Error Handling

```typescript
try {
  const result = db.query('SELECT * FROM nonexistent_table');
} catch (error) {
  console.error('Query failed:', error.message);
}
```

## TypeScript Support

Full TypeScript support with type definitions included:

```typescript
import { BlazeDB, QueryResult, TableSchema } from '@blaze-sql/node';

const db = await BlazeDB.create();
const result: QueryResult = db.query('SELECT 1');
const schema: TableSchema | null = db.getTableSchema('users');
```

## License

Apache-2.0
